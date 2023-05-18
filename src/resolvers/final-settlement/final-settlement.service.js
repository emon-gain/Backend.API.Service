import { size } from 'lodash'
import { CustomError } from '../common'

import {
  appHelper,
  finalSettlementHelper,
  invoiceHelper,
  payoutHelper,
  contractHelper
} from '../helpers'
import {
  appQueueService,
  contractService,
  invoiceService,
  paymentService,
  payoutService
} from '../services'

const generateContractFinalSettlement = async (
  contractInfo = {},
  userId,
  session
) => {
  // Check need to final settlement; if no need to final settlement then return true
  if (
    !(await checkProcessAndChangeFinalSettlementStatusToCompleted(
      contractInfo,
      session
    ))
  ) {
    // Final settlement not needed
    return true
  }
  await contractService.updateContract(
    {
      _id: contractInfo._id
    },
    {
      $set: {
        finalSettlementStatus: 'in_progress',
        isFinalSettlementDone: true
      }
    },
    session
  )
  await appQueueService.createAnAppQueue(
    {
      event: 'generate_final_settlement',
      action: 'create_tenant_invoice_for_rent_invoice_correction',
      params: {
        contractId: contractInfo._id,
        userId
      },
      destination: 'lease',
      priority: 'immediate'
    },
    session
  )
}

export const generateFinalSettlement = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId'])
  const { body = {}, session } = req
  const { contractId, partnerId, userId } = body
  const contractQuery = {
    _id: contractId,
    partnerId,
    status: 'closed',
    isFinalSettlementDone: { $ne: true }
  }
  const contractInfo = await contractHelper.getAContract(
    contractQuery,
    session,
    ['partner', 'partnerSetting']
  )
  if (
    !size(contractInfo) ||
    !size(contractInfo.partner) ||
    !size(contractInfo.partnerSetting)
  ) {
    throw new CustomError(400, 'Please provide valid lease')
  }
  await generateContractFinalSettlement(contractInfo, userId, session)
  return {
    msg: 'Final settlement process started successfully',
    code: 200
  }
}

export const createRentInvoiceForCorrections = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['contractId', 'userId'], body)
  const { contractId, userId } = body
  const contractInfo = await contractHelper.getAContract(
    {
      _id: contractId
    },
    session,
    ['partner', 'partnerSetting']
  )
  if (
    !size(contractInfo) ||
    !size(contractInfo.partner) ||
    !size(contractInfo.partnerSetting)
  ) {
    throw new CustomError(400, 'Please provide valid lease')
  }
  await createRentCorrectionInvoice(contractInfo, userId, session)
  await appQueueService.createAnAppQueue(
    {
      event: 'generate_final_settlement',
      action: 'adjust_landlord_payout_send_landlord_invoice',
      params: {
        contractId: contractInfo._id,
        partnerId: contractInfo.partnerId,
        propertyId: contractInfo.propertyId,
        userId
      },
      destination: 'lease',
      priority: 'immediate'
    },
    session
  )
  return {
    msg: 'Rent invoices for corrections created successfully',
    code: 200
  }
}

export const createRentCorrectionInvoice = async (
  contract = {},
  userId,
  session
) => {
  const { rentalMeta } = contract
  const corrections = await finalSettlementHelper.getRentCorrections(contract)
  let results = []
  if (size(corrections)) {
    const promiseArr = []
    const today = await invoiceHelper.getInvoiceDate(
      new Date(),
      contract.partnerSetting
    )
    const invoiceData = await invoiceHelper.getBasicInvoiceDataForTenant(
      contract,
      today
    )
    invoiceData.isPendingCorrection = true
    let noContractUpdate = true
    let noSerialIdQueue = true
    let i = 0
    const totalCorrection = corrections.length - 1
    let hold = false
    for (const correction of corrections) {
      if (i === totalCorrection) {
        noContractUpdate = false
        noSerialIdQueue = false
      }
      const body = {
        contract,
        correction,
        enabledNotification: !!(rentalMeta && rentalMeta.enabledNotification),
        hold,
        invoiceData,
        noContractUpdate,
        noSerialIdQueue,
        partner: contract.partner,
        partnerSetting: contract.partnerSetting,
        today,
        userId
      }
      promiseArr.push(invoiceService.createACorrectionInvoice(body, session))
      hold = true
      i++
    }

    if (size(promiseArr)) results = await Promise.all(promiseArr)
  }
  return results
}

export const checkProcessAndChangeFinalSettlementStatusToCompleted = async (
  contractInfo,
  session
) => {
  if (!size(contractInfo)) return false
  console.log(
    'Checking the final settlement process status to complete',
    contractInfo?._id
  )
  const { _id: contractId, partnerId, propertyId } = contractInfo
  let isNeedFSUpdate = false

  isNeedFSUpdate =
    await finalSettlementHelper.checkNotInProgressFinalSettlementStatus(
      contractInfo,
      session
    )

  if (!isNeedFSUpdate) {
    isNeedFSUpdate =
      await finalSettlementHelper.checkInProgressFinalSettlementStatus(
        {
          contractId,
          partnerId,
          propertyId
        },
        session
      )
  }

  // If invoices paid, landlord invoices adjust, refund payments, rent invoice creation for correction
  // And payouts process are complete then update final settlement status will be completed
  // Otherwise don`t complete final settlement process
  if (!isNeedFSUpdate) {
    console.log(
      '====> Passed all conditions to update final settlement, isNeedFSUpdate:',
      isNeedFSUpdate,
      '<===='
    )
    const query = {
      _id: contractId,
      finalSettlementStatus: { $ne: 'completed' }
    }
    const data = {
      $set: {
        finalSettlementStatus: 'completed',
        isFinalSettlementDone: true
      }
    }

    await contractService.updateContract(query, data, session)
    console.log(`-- Updated contract as finalSettlement completed.`)
  }
  return isNeedFSUpdate
}

export const linkedBetweenFinalSettlementClaimsAndPayouts = async (
  params,
  session
) => {
  const { contractId, partnerId } = params
  const unbalancedFinalSettlementClaimsQuery = {
    contractId,
    partnerId,
    invoiceType: 'landlord_invoice',
    isFinalSettlement: true,
    isPayable: true,
    remainingBalance: { $ne: 0 }
  }
  const payoutsQuery = {
    contractId,
    partnerId,
    status: 'estimated',
    amount: { $gt: 0 }
  }
  const multiplyValueForPayout = -1
  const multiplyValueForLandlord = 1
  const unbalanceFinalSettlementClaims = await invoiceHelper.getInvoices(
    unbalancedFinalSettlementClaimsQuery,
    session,
    {
      sort: { invoiceSerialId: 1 }
    }
  )
  let unbalancedEstimatedPayouts = await payoutHelper.getPayoutsWithSort(
    payoutsQuery,
    { serialId: 1 },
    session
  )
  unbalancedEstimatedPayouts = JSON.parse(
    JSON.stringify(unbalancedEstimatedPayouts)
  )

  if (
    size(unbalanceFinalSettlementClaims) &&
    size(unbalancedEstimatedPayouts)
  ) {
    const promiseArr = []
    for (const unbalanceFinalSettlementClaim of unbalanceFinalSettlementClaims) {
      let finalSettlementRemainingBalance =
        unbalanceFinalSettlementClaim.remainingBalance || 0
      let finalSettlementNewUpdateData = {
        totalBalanced: unbalanceFinalSettlementClaim.totalBalanced || 0,
        remainingBalance: unbalanceFinalSettlementClaim.remainingBalance || 0,
        invoiceContent: unbalanceFinalSettlementClaim.invoiceContent
      }
      if (size(unbalancedEstimatedPayouts)) {
        for (const unbalancedEstimatedPayout of unbalancedEstimatedPayouts) {
          if (!unbalancedEstimatedPayout.amount > 0) continue
          if (finalSettlementRemainingBalance > 0) {
            const newMeta = unbalancedEstimatedPayout.meta || []
            let newAmount = unbalancedEstimatedPayout.amount || 0
            let amount =
              newAmount >= finalSettlementRemainingBalance
                ? finalSettlementRemainingBalance
                : newAmount
            const newMetaInfo = {
              type: 'final_settlement_invoiced_cancelled',
              landlordInvoiceId: unbalanceFinalSettlementClaim._id
            }
            if (amount > 0) {
              amount = await appHelper.convertTo2Decimal(amount)

              newMetaInfo.amount = amount * multiplyValueForPayout
              newMeta.push(newMetaInfo)

              finalSettlementRemainingBalance =
                finalSettlementRemainingBalance - amount
              newAmount = await appHelper.convertTo2Decimal(
                newAmount + amount * multiplyValueForPayout || 0
              )

              //add relation between final settlement claim and payout
              //reduce payout amount for final settlement claim amount balanced in payout
              //add final settlement claim id in payout meta
              unbalancedEstimatedPayout.meta = newMeta
              unbalancedEstimatedPayout.amount = newAmount
              const payoutUpdateData = {
                meta: newMeta,
                amount: newAmount
              }
              if (
                newAmount === 0 &&
                (!unbalancedEstimatedPayout.isFinalSettlement ||
                  (await payoutHelper.isFinalSettlementPayoutWillBeCompleted(
                    unbalancedEstimatedPayout,
                    session
                  )))
              ) {
                payoutUpdateData.status = 'completed'
                payoutUpdateData.paymentStatus = 'balanced'
                promiseArr.push(
                  payoutService.afterUpdateProcessForNewlyCompletedPayout(
                    unbalancedEstimatedPayout,
                    newMeta,
                    session
                  )
                )
              }
              promiseArr.push(
                payoutService.updateAPayout(
                  {
                    _id: unbalancedEstimatedPayout._id,
                    partnerId: unbalancedEstimatedPayout.partnerId
                  },
                  {
                    $set: payoutUpdateData
                  },
                  session
                )
              )
              promiseArr.push(
                payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
                  unbalancedEstimatedPayout,
                  {
                    ...unbalancedEstimatedPayout,
                    ...payoutUpdateData
                  },
                  session
                )
              )
              finalSettlementNewUpdateData =
                payoutHelper.getDistributedBalanceAmount({
                  invoiceUpdateData: finalSettlementNewUpdateData,
                  newPayout: {
                    amount,
                    payoutId: unbalancedEstimatedPayout._id,
                    isFinalSettlement: true
                  },
                  multiplyLandlord: multiplyValueForLandlord
                })
            }
          } else break
        }

        if (size(finalSettlementNewUpdateData)) {
          const updatedInvoice = JSON.parse(
            JSON.stringify(unbalanceFinalSettlementClaim)
          )
          updatedInvoice.totalBalanced =
            finalSettlementNewUpdateData.totalBalanced
          const returnData =
            await invoiceService.updateInvoiceStatusWhenTotalPaidOrTotalBalancedChange(
              unbalanceFinalSettlementClaim,
              updatedInvoice,
              session
            )
          promiseArr.push(
            invoiceService.updateInvoice(
              {
                _id: unbalanceFinalSettlementClaim._id,
                partnerId: unbalanceFinalSettlementClaim.partnerId
              },
              {
                $set: {
                  ...finalSettlementNewUpdateData,
                  ...returnData.setData
                },
                $unset: {
                  ...returnData.unsetData
                }
              },
              session
            )
          )
        }
      }
    }
    if (size(promiseArr)) {
      await Promise.all(promiseArr)
    }
  }
}

//Is called when isFinalSettlement true for a payout
export const linkedBetweenFinalSettlementsAndLastPayouts = async (
  params,
  session
) => {
  const { contractId, partnerId } = params
  const payoutsQuery = {
    contractId,
    partnerId,
    status: 'estimated'
  }
  const payoutInfo = await payoutHelper.getLastPayout(
    payoutsQuery,
    { serialId: -1 },
    session
  )
  if (!payoutInfo) return
  const newPayoutId = payoutInfo._id
  let balancedAmount = payoutInfo.amount || 0
  let newPayoutMeta = payoutInfo.meta || []
  const multiplyValueForPayout = -1
  const multiplyValueForLandlord = 1
  const unbalancedFinalSettlementClaimsQuery = {
    contractId,
    partnerId,
    invoiceType: 'landlord_invoice',
    isFinalSettlement: true,
    isPayable: true,
    remainingBalance: { $ne: 0 }
  }
  const unbalancedFinalSettlements = await invoiceHelper.getInvoices(
    unbalancedFinalSettlementClaimsQuery,
    session,
    {
      sort: { invoiceSerialId: 1 }
    }
  )

  if (size(unbalancedFinalSettlements)) {
    const promiseArr = []
    for (const unbalancedFinalSettlement of unbalancedFinalSettlements) {
      const amount = Math.abs(unbalancedFinalSettlement.remainingBalance || 0)
      const newMetaInfo = {
        type: 'final_settlement_invoiced_cancelled',
        landlordInvoiceId: unbalancedFinalSettlement._id
      }

      if (amount > 0) {
        newMetaInfo.amount = await appHelper.convertTo2Decimal(
          amount * multiplyValueForPayout
        )
        newPayoutMeta = payoutHelper.getPayoutNewMeta(
          newPayoutMeta,
          newMetaInfo
        )

        balancedAmount = balancedAmount + amount * multiplyValueForPayout || 0

        let finalSettlementNewUpdateData = {
          totalBalanced: unbalancedFinalSettlement.totalBalanced || 0,
          remainingBalance: unbalancedFinalSettlement.remainingBalance || 0,
          invoiceContent: unbalancedFinalSettlement.invoiceContent
        }

        finalSettlementNewUpdateData =
          await payoutHelper.getDistributedBalanceAmount({
            invoiceUpdateData: finalSettlementNewUpdateData,
            newPayout: {
              amount,
              payoutId: newPayoutId,
              isFinalSettlement: true
            },
            multiplyLandlord: multiplyValueForLandlord
          })
        const updateInvoice = JSON.parse(
          JSON.stringify(unbalancedFinalSettlement)
        )
        updateInvoice.totalBalanced = finalSettlementNewUpdateData.totalBalanced
        const returnData =
          await invoiceService.updateInvoiceStatusWhenTotalPaidOrTotalBalancedChange(
            unbalancedFinalSettlement,
            updateInvoice,
            session
          )
        promiseArr.push(
          invoiceService.updateInvoice(
            {
              _id: unbalancedFinalSettlement._id,
              partnerId
            },
            {
              $set: {
                ...finalSettlementNewUpdateData,
                ...returnData.setData
              },
              $unset: {
                ...returnData.unsetData
              }
            },
            session
          )
        )
      }
    }
    if (size(promiseArr)) await Promise.all(promiseArr)
    // Add relation between cancelled final settlements and payout
    // Reduce payout amount for cancelled final settlements amount balanced in payout
    // Add cancelled final settlement id in payout meta
    const payoutUpdateData = {
      meta: newPayoutMeta,
      amount: await appHelper.convertTo2Decimal(balancedAmount)
    }
    if (
      balancedAmount === 0 &&
      (!payoutInfo.isFinalSettlement ||
        (await payoutHelper.isFinalSettlementPayoutWillBeCompleted(
          {
            ...payoutInfo,
            meta: newPayoutMeta
          },
          session
        )))
    ) {
      payoutUpdateData.status = 'completed'
      payoutUpdateData.paymentStatus = 'balanced'
      await payoutService.afterUpdateProcessForNewlyCompletedPayout(
        payoutInfo,
        newPayoutMeta,
        session
      )
    }
    const updatedPayout = await payoutService.updateAPayout(
      { _id: newPayoutId, partnerId },
      {
        $set: payoutUpdateData
      },
      session
    )
    await payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
      payoutInfo,
      updatedPayout,
      session
    )
    // Since this is last payout, so we don't need after update implementation when amount < 0
  }
}
export const generateRefundPaymentToTenantOnLeaseTermination = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['contractId', 'userId'], body)
  const { contractId, userId } = body
  const pipeline = refundPaymentInfoPipeline(contractId)
  const refundPaymentInfo = await contractHelper.getContractByAggregate(
    pipeline
  )
  if (!size(refundPaymentInfo)) {
    throw new CustomError(404, 'Lease not found with contractId: ' + contractId)
  }
  const tenantId =
    refundPaymentInfo && refundPaymentInfo.tenantId
      ? refundPaymentInfo.tenantId
      : ''
  let refundableAmount = 0
  const refundData = {}
  const {
    creditedTotalAmount = 0,
    invoiceTotalAmount = 0,
    lostTotalAmount = 0
  } = refundPaymentInfo
  const invoiceTotal =
    invoiceTotalAmount - lostTotalAmount + creditedTotalAmount
  if (
    size(refundPaymentInfo) &&
    refundPaymentInfo.paymentTotal > invoiceTotal &&
    size(refundPaymentInfo.lastPayment)
  ) {
    refundableAmount = refundPaymentInfo.paymentTotal - invoiceTotal // add refundable amount to last payment of the tenant
    refundData.amount = refundableAmount
    refundData.contractId = refundPaymentInfo._id
    refundData.propertyId = refundPaymentInfo.propertyId

    await paymentService.createRefundPaymentForFinalSettlement(
      {
        paymentId: refundPaymentInfo.lastPayment?._id,
        partnerId: refundPaymentInfo.lastPayment?.partnerId,
        refundPaymentData: refundData,
        userId
      },
      session
    )
  } else {
    console.log('No refund payment on lease termination for tenantId', tenantId)
  }
  // Implementation of findUnbalancedLandlordInvoicesAndMakePayable just an update operation so I don't create another app queue for this. Apologies.
  await invoiceService.updateInvoices(
    {
      contractId,
      invoiceType: 'landlord_invoice',
      isPayable: false,
      remainingBalance: { $gt: 0 },
      status: { $nin: ['paid', 'credited', 'lost', 'balanced', 'cancelled'] }
    },
    { $set: { isPayable: true } },
    session
  )
  await appQueueService.insertInQueue(
    {
      event: 'generate_final_settlement',
      action: 'forward_over_paid_invoices_amount',
      params: {
        contractId,
        partnerId: refundPaymentInfo.partnerId
      },
      destination: 'lease',
      priority: 'immediate'
    },
    session
  )
  return {
    code: 200,
    msg: 'Success'
  }
}

const refundPaymentInfoPipeline = (contractId) => [
  {
    $match: {
      _id: contractId
    }
  },
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      as: 'invoicePayments',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                {
                  $not: { $eq: ['$isFinalSettlement', true] }
                },
                {
                  $not: { $eq: ['$refundPaymentStatus', 'paid'] }
                }
              ]
            }
          }
        },
        {
          $unwind: '$invoices'
        },
        {
          $lookup: {
            from: 'invoices',
            localField: 'invoices.invoiceId',
            foreignField: '_id',
            as: 'paymentInvoice'
          }
        },
        appHelper.getUnwindPipeline('paymentInvoice'),
        {
          $match: {
            $expr: {
              $not: {
                $eq: ['$paymentInvoice.isFinalSettlement', true]
              }
            }
          }
        },
        {
          $group: {
            _id: null,
            total: { $sum: '$invoices.amount' }
          }
        }
      ]
    }
  },
  appHelper.getUnwindPipeline('invoicePayments'),
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      as: 'invoiceTotalAmountInfo',
      pipeline: [
        {
          $match: {
            $expr: { $eq: ['$invoiceType', 'invoice'] }
          }
        },
        {
          $group: {
            _id: null,
            invoiceTotalAmount: { $sum: '$invoiceTotal' },
            creditedTotalAmount: { $sum: '$creditedAmount' },
            lostTotalAmount: { $sum: '$lostMeta.amount' },
            totalPaidAmount: { $sum: '$totalPaid' }
          }
        }
      ]
    }
  },
  appHelper.getUnwindPipeline('invoiceTotalAmountInfo'),
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      as: 'lastPayment',
      pipeline: [
        {
          $sort: { createdAt: -1 }
        },
        {
          $limit: 1
        }
      ]
    }
  },
  appHelper.getUnwindPipeline('lastPayment'),
  {
    $project: {
      _id: 1,
      lastPayment: 1,
      paymentTotal: { $ifNull: ['$invoicePayments.total', 0] },
      invoiceTotal: {
        $subtract: [
          { $ifNull: ['$invoiceTotalAmountInfo.invoiceTotalAmount', 0] },
          {
            $add: [
              { $ifNull: ['$invoiceTotalAmountInfo.lostTotalAmount', 0] },
              { $ifNull: ['$invoiceTotalAmountInfo.creditedTotalAmount', 0] }
            ]
          }
        ]
      },
      invoiceTotalPaid: {
        $ifNull: ['$invoiceTotalAmountInfo.totalPaidAmount', 0]
      },
      propertyId: 1,
      tenantId: '$rentalMeta.tenantId',
      partnerId: 1,
      invoiceTotalAmount: {
        $ifNull: ['$invoiceTotalAmountInfo.invoiceTotalAmount', 0]
      },
      creditedTotalAmount: {
        $ifNull: ['$invoiceTotalAmountInfo.creditedTotalAmount', 0]
      },
      lostTotalAmount: {
        $ifNull: ['$invoiceTotalAmountInfo.lostTotalAmount', 0]
      }
    }
  }
]

export const findUnbalancedLandlordInvoicesAndMakePayable = async (
  contractId
) => {
  try {
    const invoiceUpdate = await invoiceService.updateInvoice(
      {
        contractId,
        invoiceType: 'landlord_invoice',
        isPayable: false,
        remainingBalance: { $gt: 0 },
        status: { $nin: ['paid', 'credited', 'lost', 'balanced', 'cancelled'] }
      },
      { $set: { isPayable: true } }
    )
    console.log('Invoice data after update', invoiceUpdate)
    return {
      msg: 'Success'
    }
  } catch (e) {
    console.log('Error when updating unbalanced invoice', e)
  }
}

export const adjustLandlordPayoutOrSendLandlordInvoice = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.compactObject(body, true)
  appHelper.checkRequiredFields(
    ['contractId', 'partnerId', 'propertyId', 'userId'],
    body
  )
  const { contractId, partnerId, propertyId, userId } = body
  const contractInfo = await contractHelper.getAContract(
    {
      _id: contractId,
      partnerId,
      propertyId
    },
    session,
    ['partnerSetting']
  )
  if (!size(contractInfo) || !size(contractInfo.partnerSetting)) {
    throw new CustomError(400, 'Please provide valid lease: ' + contractId)
  }
  try {
    const partnerSetting = contractInfo.partnerSetting
    const data = {
      contractId,
      isFinalSettlement: true,
      partnerId,
      partnerSetting,
      propertyId
    }
    await payoutService.adjustBetweenPayoutsAndLandlordInvoices(data, session)
    await linkedBetweenFinalSettlementClaimsAndPayouts({
      contractId,
      partnerId
    })
    const notAdjustedFinalSettlementInvoiceAmount =
      await finalSettlementHelper.getNotAdjustedAmountForFinalSettlementInvoices(
        {
          contractId,
          partnerId,
          propertyId
        },
        session
      )
    if (notAdjustedFinalSettlementInvoiceAmount !== 0) {
      const estimatedPayout = await payoutHelper.getPayout(
        {
          status: 'estimated',
          contractId,
          partnerId,
          propertyId
        },
        session
      )
      if (size(estimatedPayout)) {
        await linkedBetweenFinalSettlementsAndLastPayouts({
          contractId,
          partnerId
        })
      } else {
        await payoutService.createPayoutForFinalSettlement(
          contractInfo,
          userId,
          session
        )
      }
    }
    await appQueueService.insertInQueue(
      {
        event: 'generate_final_settlement',
        action: 'find_negative_estimated_payout_and_create_landlord_invoice',
        params: {
          contractId: contractInfo._id,
          partnerId,
          propertyId,
          userId
        },
        destination: 'lease',
        priority: 'immediate'
      },
      session
    )
    return {
      code: 200,
      msg: 'Adjust payout or landlord invoice done'
    }
  } catch (e) {
    console.log('Error happened when adjusting payout and landlord invoice', e)
    throw new Error(e)
  }
}

export const cancelFinalSettlementService = async (body = {}, req) => {
  const { session, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['contractId'], body)
  const { contractId } = body
  let { partnerId } = body
  if (size(user?.roles) && user.roles.includes('lambda_manager')) {
    user.partnerId = partnerId
  }
  partnerId = user.partnerId
  if (!size(partnerId)) {
    throw new CustomError('400', 'Required partnerId')
  }

  const contractInfo = await contractHelper.getAContract({
    _id: contractId,
    partnerId
  })

  if (!size(contractInfo)) {
    throw new CustomError('404', 'Contract not found')
  }

  if (contractInfo.status !== 'closed' || !contractInfo.isFinalSettlementDone) {
    throw new CustomError('400', 'Final settlement not available for cancel')
  }

  const query = {
    contractId,
    partnerId,
    invoiceType: 'landlord_invoice',
    isFinalSettlement: true,
    isPayable: true
  }

  await invoiceService.updateInvoice(
    {
      status: { $nin: ['paid', 'cancelled'] },
      isPartiallyPaid: { $ne: true },
      ...query
    },
    {
      $set: { status: 'cancelled' }
    },
    session
  )

  await invoiceService.updateInvoiceWithPipeline(
    { isPartiallyPaid: true, ...query },
    [
      {
        $set: {
          status: 'cancelled',
          remainingBalance: {
            $round: [
              {
                $subtract: [
                  {
                    $ifNull: ['$invoiceTotal', 0]
                  },
                  {
                    $ifNull: ['$totalPaid', 0]
                  }
                ]
              },
              2
            ]
          }
        }
      }
    ],
    session
  )

  await linkedBetweenFinalSettlementClaimsAndPayouts(
    {
      contractId,
      partnerId
    },
    session
  )

  await linkedBetweenFinalSettlementsAndLastPayouts(
    { contractId, partnerId },
    session
  )

  const contractUpdated = await contractService.updateContract(
    {
      _id: contractId,
      partnerId
    },
    { $set: { isFinalSettlementDone: false, finalSettlementStatus: 'new' } },
    session
  )

  if (!size(contractUpdated)) {
    throw new CustomError('404', 'Could not update final settlement status')
  }

  return {
    msg: 'Successfully canceled the final settlement'
  }
}

export const overPaidInvoicesAmountForwardedToNonePaidInvoices = async (
  req
) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['contractId', 'partnerId'], body)
  const { contractId, partnerId } = body
  try {
    await appQueueService.createAppQueueForMatchPayment({
      action: 'forward_overpaid_to_non_paid_invoice_for_final_settlement',
      contractId,
      partnerId
    })
    return {
      code: 200,
      msg: 'Success'
    }
  } catch (e) {
    throw new Error(e)
  }
}
export const checkFinalSettlementsAndUpdateStatusToCompleted = async (
  contractId,
  session
) => {
  console.log('Checking final settlement contractId', contractId)

  const needFinalSettlement =
    await finalSettlementHelper.checkIfFinalSettlementNeeded(contractId)

  if (!needFinalSettlement) {
    await contractService.updateContract(
      {
        _id: contractId,
        status: 'closed',
        finalSettlementStatus: { $ne: 'completed' }
      },
      { finalSettlementStatus: 'completed', isFinalSettlementDone: true },
      session
    )

    console.log(
      'Successfully final settlement completed contractId',
      contractId
    )
    return true
  }

  return false
}

export const checkDailyFinalSettlementsAndUpdateStatusToCompleted = async (
  req
) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['contractId'], body)
  const { contractId } = body

  await checkFinalSettlementsAndUpdateStatusToCompleted(contractId, session)
  return {
    msg: 'Success',
    code: 200
  }
}
