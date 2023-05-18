import { clone, includes, map, pick, size } from 'lodash'
import { CustomError } from '../common'
import { PayoutCollection } from '../models'
import {
  appHelper,
  invoiceHelper,
  invoicePaymentHelper,
  logHelper,
  partnerPayoutHelper,
  partnerSettingHelper,
  payoutHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  contractService,
  correctionService,
  finalSettlementService,
  invoiceService,
  invoiceSummaryService,
  logService,
  partnerPayoutService,
  partnerSettingService,
  payoutService,
  transactionService
} from '../services'

export const updateAPayout = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedPayout = await PayoutCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  return updatedPayout
}

export const createLogForUpdatedPayout = async (payout, session, options) => {
  if (!size(payout)) return false

  const logData = logHelper.prepareLogDataForPayout(
    {
      action: 'updated_payout',
      context: 'payout',
      ...options
    },
    payout
  )
  const createdLogData = await logService.createLog(logData, session)
  console.log(`-- Log created for payout`)
  return createdLogData
}

export const updateLastPayoutInfo = async (params = {}, session) => {
  if (!size(params)) return false

  const {
    amount,
    lastPayoutAmount,
    lastPayoutId,
    meta: lastPayoutMeta,
    nextPayoutId,
    payoutMetaType,
    status
  } = params || {}

  if (
    lastPayoutId &&
    nextPayoutId &&
    includes(
      ['unpaid_earlier_payout', 'unpaid_expenses_and_commissions'],
      payoutMetaType
    )
  ) {
    const metaTotal = await payoutHelper.getPayoutMetaTotal(lastPayoutMeta)
    const newMetaTotal = await appHelper.convertTo2Decimal(
      metaTotal + lastPayoutAmount
    )

    const payoutUpdatingSetData = {}
    if (
      includes(['completed', 'in_progress'], status) ||
      (status === 'estimated' && amount < 0 && metaTotal !== newMetaTotal)
    ) {
      payoutUpdatingSetData.amount = 0
      payoutUpdatingSetData.status = 'completed'
    }

    const payoutUpdatingData = {
      $push: {
        meta: {
          type: 'moved_to_next_payout',
          amount: lastPayoutAmount,
          payoutId: nextPayoutId
        }
      }
    }
    if (size(payoutUpdatingSetData))
      payoutUpdatingData['$set'] = payoutUpdatingSetData

    const updatedPayout = await updateAPayout(
      { _id: lastPayoutId },
      payoutUpdatingData,
      session
    )
    if (updatedPayout?._id) {
      let updatedPayoutDataForUpdatingSummary = updatedPayout
      if (
        updatedPayout.status === 'completed' &&
        updatedPayout.paymentStatus !== 'balanced' &&
        updatedPayout.amount === 0
      ) {
        const paymentStatusUpdatedPayout = await updateAPayout(
          { _id: updatedPayout._id },
          { $set: { paymentStatus: 'balanced' } },
          session
        )
        updatedPayoutDataForUpdatingSummary =
          size(paymentStatusUpdatedPayout) && paymentStatusUpdatedPayout._id
            ? paymentStatusUpdatedPayout
            : updatedPayout
      }

      const previous = await payoutHelper.getPayout(
        { _id: updatedPayout._id },
        null
      )
      await checkPayoutUpdatedDataToUpdateInvoiceSummary(
        previous,
        updatedPayoutDataForUpdatingSummary,
        session
      )
    }
  }
}

const addLastUnpaidPayoutInNextPayout = async (
  lastPayoutData = {},
  payoutMetaType,
  session
) => {
  if (!(size(lastPayoutData) && payoutMetaType)) return false

  const {
    _id: lastPayoutId,
    amount: lastPayoutAmount,
    contractId: lastPayoutContractId,
    partnerId,
    serialId
  } = lastPayoutData || {}
  const nextPayoutData =
    (await payoutHelper.getPayout(
      {
        contractId: lastPayoutContractId,
        partnerId,
        serialId: { $gt: serialId },
        status: 'estimated'
      },
      session,
      { serialId: 1 }
    )) || {}
  if (
    includes(
      ['unpaid_earlier_payout', 'unpaid_expenses_and_commissions'],
      payoutMetaType
    )
  ) {
    const {
      _id: nextPayoutId,
      amount: nextPayoutAmount,
      contractId: nextPayoutContractId,
      estimatedAmount
    } = nextPayoutData || {}
    const totalEstimated = estimatedAmount + lastPayoutAmount || 0
    const totalPayout = nextPayoutAmount + lastPayoutAmount || 0
    console.log('-- Trying to add last unpaid payout In next payout')
    const updatedPayout = await updateAPayout(
      { _id: nextPayoutId, contractId: nextPayoutContractId },
      {
        $push: {
          meta: {
            type: payoutMetaType,
            amount: lastPayoutAmount,
            payoutId: lastPayoutId
          }
        },
        $set: {
          amount: await appHelper.convertTo2Decimal(totalPayout),
          estimatedAmount: await appHelper.convertTo2Decimal(totalEstimated)
        }
      }
    )
    console.log('-- Moved last unpaid payout amount to next payout')
    if (updatedPayout?._id) {
      const nextPayoutDataBeforeUpdate = await payoutHelper.getPayout(
        { _id: updatedPayout._id },
        null
      )
      await checkPayoutUpdatedDataToUpdateInvoiceSummary(
        nextPayoutDataBeforeUpdate,
        updatedPayout,
        session
      )
      await updateLastPayoutInfoForCreditInvoice(
        {
          lastPayoutAmount,
          lastPayoutData,
          nextPayoutData: updatedPayout
        },
        session
      )
    }
  }
}

const checkPayoutStatusToHandleFailedPayout = async (doc, session) => {
  if (doc?.status && doc?.status !== 'failed') return false

  const { numberOfFails = 0 } = doc
  const { retryFailedPayouts = {} } =
    (await partnerSettingHelper.getSettingByPartnerId(
      doc.partnerId,
      session
    )) || {}
  const { days, enabled = false } = retryFailedPayouts || {}
  // If partner enabled to retry payout then make payout status to estimated else move payout to next payout
  if (days && days > numberOfFails && enabled) {
    const updatedPayout = await updateAPayout(
      { _id: doc._id },
      {
        $inc: { numberOfFails: 1 },
        $set: {
          status: 'estimated',
          sentToNETS: false
        },
        $unset: { advancedPayout: 1, paymentStatus: 1 }
      },
      session
    )

    console.log('-- Retry Payout process started')
    if (updatedPayout?._id)
      await createLogForUpdatedPayout(updatedPayout, session)
  } else {
    await addLastUnpaidPayoutInNextPayout(doc, 'unpaid_earlier_payout', session)
  }
}

export const checkPayoutUpdatedDataToUpdateInvoiceSummary = async (
  previous,
  doc,
  session
) => {
  if (!size(doc)) return false

  const {
    _id: payoutId,
    amount,
    invoiceId,
    meta: payoutMeta,
    partnerId,
    status
  } = doc || {}
  const queryData = { partnerId }
  const updateData = {}
  if (invoiceId && partnerId && payoutId) {
    queryData.invoiceId = invoiceId
    updateData.payoutId = payoutId
    const landlordInvoiceIds = map(payoutMeta, 'landlordInvoiceId')
    const { commissionInvoiceIds, correctionInvoiceIds } =
      (await invoiceHelper.getCommissionAndCorrectionInvoiceIdsForQuery(
        {
          _id: { $in: landlordInvoiceIds }
        },
        session
      )) || {}

    if (size(commissionInvoiceIds)) {
      updateData.commissionsAmount =
        (await appHelper.convertTo2Decimal(
          payoutHelper.getLandlordInvoiceTotalFromPayoutMeta(
            payoutMeta,
            commissionInvoiceIds
          )
        )) * -1 // For making positive amount, multiply meta amount with -1
    }
    if (size(correctionInvoiceIds)) {
      updateData.correctionsAmount = await appHelper.convertTo2Decimal(
        payoutHelper.getLandlordInvoiceTotalFromPayoutMeta(
          payoutMeta,
          correctionInvoiceIds
        )
      )
    }

    updateData.payoutAmount = await appHelper.convertTo2Decimal(amount || 0)
  }

  if (
    status === 'completed' &&
    size(previous?.status) &&
    status !== previous.status
  ) {
    queryData.payoutId = payoutId
    updateData.isPaid = true
  }

  if (size(queryData) && size(updateData)) {
    const isUpdated = await invoiceSummaryService.updateInvoiceSummary(
      queryData,
      { $set: updateData },
      session
    )
    if (isUpdated)
      console.log('-- Updated Invoice Summary with payout information')
  }
}

const checkPayoutStatusToUpdateCorrectionStatus = async (
  previous,
  doc,
  session
) => {
  if (!(size(previous) && size(doc))) return false

  const { _id: payoutId, correctionsIds, status } = doc || {}
  if (
    size(correctionsIds) &&
    payoutId &&
    status === 'completed' &&
    status !== previous.status
  ) {
    await correctionService.updateCorrections(
      { _id: { $in: correctionsIds } },
      { $set: { payoutId, status } },
      session
    )
    console.log(`-- Correction status updated to ${status}`)
  }
}

const checkPayoutStatusToSendNotificationAndCreateTransaction = async (
  partnerId,
  payoutId,
  session
) => {
  if (!(partnerId && payoutId)) {
    console.log(
      'PartnerId or PayoutId is required to creation payout notification and transaction queue'
    )
    return false
  }

  const appQueuesData = [
    {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_payout',
      params: {
        partnerId,
        collectionId: payoutId,
        collectionNameStr: 'payouts'
      },
      priority: 'regular'
    }
  ]

  if (await transactionHelper.isTransactionEnabledForPartner(partnerId)) {
    console.log("=== Transaction is enabled for this payout's partner ===")
    appQueuesData.push({
      action: 'add_payout_regular_transaction',
      destination: 'accounting',
      event: 'add_new_transaction',
      params: {
        partnerId,
        payoutIds: [payoutId],
        transactionEvent: 'regular'
      },
      priority: 'regular'
    })
  }
  console.log('-- Payout notification sent and transaction added for payout')
  const queues = await appQueueService.insertAppQueueItems(
    appQueuesData,
    session
  )
  console.log(
    `=== Created #${size(
      queues
    )} queues to send payout notification and transaction. payoutId: ${payoutId} ===`
  )
}

export const initPayoutAfterUpdateProcessForStatus = async (
  updatedPayout,
  session
) => {
  if (!size(updatedPayout))
    throw new CustomError(404, 'Updated payout info is required')

  const {
    _id: payoutId,
    contractId,
    advancedPayout,
    amount,
    partnerId,
    paymentStatus,
    status
  } = updatedPayout

  const oldPayout = await payoutHelper.getPayout({ _id: payoutId })
  console.log('present advancedPayout', advancedPayout)
  console.log('present paymentStatus', paymentStatus)
  console.log('present status', status)

  if (!size(oldPayout)) throw new CustomError(404, 'Payout info not found')

  const {
    advancedPayout: preAdvancedPayout,
    paymentStatus: prePaymentStatus,
    status: preStatus
  } = oldPayout

  console.log('previous advancedPayout', preAdvancedPayout)
  console.log('previous paymentStatus', prePaymentStatus)
  console.log('previous status', preStatus)

  if (status && preStatus && status !== preStatus) {
    if (includes(['completed', 'failed', 'in_progress'], status))
      await createLogForUpdatedPayout(updatedPayout, session)

    if (status === 'failed')
      await checkPayoutStatusToHandleFailedPayout(updatedPayout, session)

    if (status === 'completed')
      await checkPayoutStatusToUpdateCorrectionStatus(
        oldPayout,
        updatedPayout,
        session
      )
    // Update contract for payout to landlord before tenant's payment - max:0 (ex: 2/3) months
    if (advancedPayout && advancedPayout !== preAdvancedPayout) {
      // Incrementing the noOfPayoutMonth +1 in contract
      const contract = await contractService.updateContract(
        { _id: contractId },
        { $inc: { noOfPayoutMonth: 1 } },
        session
      )
      size(contract)
        ? console.log(
            `Incremented noOfPayoutMonth for this payout's contract. contractId: ${contractId}. noOfPayoutMonth: ${contract.noOfPayoutMonth}`
          )
        : console.log(
            `Unable to increment noOfPayoutMonth for this payout's contract. contractId: ${contractId}, payoutId: ${payoutId}`
          )
    }
  }

  if (
    amount > 0 &&
    status === 'completed' &&
    paymentStatus === 'paid' &&
    paymentStatus !== prePaymentStatus
  ) {
    await checkPayoutStatusToSendNotificationAndCreateTransaction(
      partnerId,
      payoutId,
      session
    )
  }

  await checkPayoutUpdatedDataToUpdateInvoiceSummary(
    oldPayout,
    updatedPayout,
    session
  )

  return true
}

export const updatePayouts = async (query, data, session) => {
  const response = await PayoutCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  return response
}

export const createLogForAddedNewPayout = async (action, payout, session) => {
  if (payout) {
    const options = { action, context: 'payout' }
    const logData = logHelper.prepareLogDataForPayout(options, payout)
    await logService.createLog(logData, session)
  }
}

export const createLogForUpdatingHoldStatusOrDate = async (
  action,
  data,
  session
) => {
  const { previousPayout, payout, payoutDate } = data
  if (previousPayout && payout && payoutDate) {
    const options = { action, context: 'payout' }
    const logData = logHelper.prepareLogDataForPayout(options, payout)
    logData.isChangeLog = true
    logData.changes = [
      {
        field: 'payoutDate',
        type: 'date',
        oldDate: previousPayout.payoutDate || '',
        newDate: payout.payoutDate || ''
      }
    ]
    await logService.createLog(logData, session)
  }
}

export const createPayout = async (data, session) => {
  if (!size(data)) throw new CustomError(404, 'No data found to create payout')
  const [payout] = await PayoutCollection.create([data], { session })
  if (!size(payout)) throw new CustomError(400, 'Unable to create payout')
  return payout
}

export const createMultiplePayouts = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found to create payout')
  }

  const [payout] = await PayoutCollection.create(data, { session })
  if (!size(payout)) {
    throw new CustomError(400, 'Unable to create payout')
  }

  return payout
}

export const createEstimatedPayout = async (data, session) => {
  const { invoice, partnerSetting } = data
  data.payoutData = await payoutHelper.getPayoutCreationData(
    {
      invoice,
      isFinalSettlement: data.isFinalSettlement,
      meta: data.meta || []
    },
    session
  )
  data.dueDate = invoice.dueDate
  const payoutData = await payoutHelper.preparePayoutData(data, session)
  const payout = await createPayout(payoutData, session)
  console.log(`--- Created a new Payout for invoiceId: ${invoice._id} ---`)
  await createLogForAddedNewPayout(
    'added_new_payout',
    payout.toObject(),
    session
  )
  // After insert process starts
  await appQueueService.createAppQueueForAddingSerialId(
    'payouts',
    payout,
    session
  )
  // await appQueueService.createAppQueueForAddingPayoutBankReference(
  //   payout.partnerId,
  //   session
  // )
  await checkPayoutUpdatedDataToUpdateInvoiceSummary({}, payout, session)
  if (size(payout.meta)) {
    const payoutUpdateArr = []
    for (const meta of payout.meta) {
      if (
        meta.type === 'unpaid_earlier_payout' ||
        meta.type === 'unpaid_expenses_and_commissions'
      ) {
        const payoutInfo = await payoutHelper.getPayout(
          { _id: meta.payoutId },
          session
        )
        if (size(payoutInfo)) {
          const updateArr = await updateLastPayoutInfoForCreditInvoice(
            {
              nextPayoutData: payout,
              lastPayoutData: payoutInfo,
              lastPayoutAmount: meta.amount
            },
            session
          )
          payoutUpdateArr.push(...updateArr)
        }
      }
    }
    if (size(payoutUpdateArr)) await Promise.all(payoutUpdateArr)
  }
  await addLinkBetweenPayoutAndLandlordInvoices(
    {
      payout,
      isFirstAdjustLandlordCreditNote: true,
      isAdjustAll: !!payout.isFinalSettlement,
      partnerSetting
    },
    session
  )
  await finalSettlementService.linkedBetweenFinalSettlementClaimsAndPayouts(
    {
      contractId: payout.contractId,
      partnerId: payout.partnerId
    },
    session
  )
  if (payout.isFinalSettlement) {
    await finalSettlementService.linkedBetweenFinalSettlementsAndLastPayouts(
      {
        contractId: payout.contractId,
        partnerId: payout.partnerId
      },
      session
    )
  }
  return payout
}

export const addEstimatedPayout = async (req) => {
  const { body, session } = req
  const { invoiceId, isFinalSettlement } = body
  if (!invoiceId) {
    throw new CustomError(400, `invoiceId is required!`)
  }
  const payoutExist = await payoutHelper.getPayout({ invoiceId }, session)
  if (payoutExist) {
    throw new CustomError(
      405,
      `Payout is already created for invoice with _id: ${invoiceId}`
    )
  }
  const invoice = await invoiceHelper.getInvoice(
    {
      _id: invoiceId
    },
    session,
    ['contract']
  )
  if (!invoice) {
    throw new CustomError(
      404,
      `Could not find any invoice with _id: ${invoiceId}`
    )
  }
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    invoice.partnerId
  )
  const data = {
    contract: invoice.contract,
    invoice,
    isFinalSettlement,
    partnerSetting
  }
  const payout = await createEstimatedPayout(data, session)
  return [payout]
}

export const updatePayout = async (req) => {
  const { body, session } = req
  const { partnerId, payoutId, payoutDate, holdPayout } = body
  if (!partnerId || !payoutId) {
    throw new CustomError(
      400,
      'Bad request, partnerId and payoutId are required!'
    )
  }
  const query = { _id: payoutId, partnerId }
  let payout = await payoutHelper.getPayout(query, session)
  if (!payout) {
    throw new CustomError(
      404,
      `Could not find any payout with _id: ${payoutId} and partnerId: ${partnerId}`
    )
  }
  const previousPayout = payout.toObject()
  if (payoutDate) {
    payout.payoutDate = payoutDate
  }
  if (holdPayout) {
    payout.holdPayout = holdPayout
  }
  payout = (await payout.save()).toObject()
  console.log(
    `--- Updated the Payout with _id: ${payoutId} and partnerId: ${partnerId} ---`
  )
  const data = {
    previousPayout,
    payout,
    payoutDate
  }
  await createLogForUpdatingHoldStatusOrDate(
    'updated_payout_info',
    data,
    session
  )
  return payout
}

export const adjustEstimatedPayout = async (data) => {
  const { payout, invoice } = data
  const totalPayoutAmount = payout.amount + invoice.payoutableAmount
  payout.meta = payout.meta || []
  payout.meta.push({
    type: 'rent_invoice',
    amount: invoice.payoutableAmount,
    invoiceId: invoice._id
  })
  payout.estimatedAmount = await appHelper.convertTo2Decimal(
    totalPayoutAmount || 0
  )
  payout.amount = await appHelper.convertTo2Decimal(totalPayoutAmount || 0)
  const result = await payout.save()
  return result
}

export const adjustOrAddEstimatedPayout = async (invoiceId) => {
  const session = await require('mongoose').startSession()
  session.startTransaction()
  let result = {}
  try {
    if (!invoiceId) {
      throw new CustomError(400, `invoiceId is required!`)
    }
    const invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
    if (!invoice) {
      throw new CustomError(
        404,
        `Could not find any invoice with _id: ${invoiceId}`
      )
    }
    const query = pick(invoice, ['contractId', 'partnerId', 'propertyId'])
    query.status = 'estimated'
    const payout = await payoutHelper.getPayout(query, session)
    const data = { invoice, session }
    if (payout && invoice.isPendingCorrection) {
      data.payout = payout
      result = await adjustEstimatedPayout(data, session)
    } else {
      data.body = { invoiceId }
      ;[result] = await addEstimatedPayout(data, session) // Todo: add this method to app-queue
    }
    await session.commitTransaction()
  } catch (err) {
    await session.abortTransaction()
    throw new CustomError(err.statusCode || 500, err.message)
  } finally {
    session.endSession()
  }
  return result
}

/**
 * Add relation between landlord invoice and payout
 * Reduce payout amount for landlord invoice amount balanced in payout
 * Add landlord invoice id in payout meta
 */
export const addLinkBetweenLandlordInvoiceAndPayouts = async (
  landlordInvoice,
  partnerSetting,
  session
) => {
  const { invoiceType, remainingBalance = 0 } = landlordInvoice
  const multiplyPayout = remainingBalance > 0 ? -1 : 1
  const multiplyLandlord = remainingBalance > 0 ? 1 : -1
  let balancedAmount = Math.abs(remainingBalance)
  let invoiceUpdateData = pick(landlordInvoice, [
    'totalBalanced',
    'remainingBalance',
    'commissionsMeta',
    'addonsMeta'
  ])
  const unbalancedPayouts = await payoutHelper.getUnbalancedPayouts(
    landlordInvoice,
    partnerSetting,
    session
  )
  let needUpdateInvoice = false
  const promiseArr = []
  for (let i = 0; i < unbalancedPayouts.length; i++) {
    const payout = unbalancedPayouts[i]
    if (balancedAmount > 0) {
      const newMetaInfo = {
        type: 'landlord_invoice',
        landlordInvoiceId: landlordInvoice._id
      }
      let newAmount = payout.amount || 0
      let amount = 0
      if (invoiceType === 'landlord_credit_note' || multiplyPayout === 1) {
        amount = balancedAmount
      } else {
        amount = balancedAmount >= newAmount ? newAmount : balancedAmount
      }
      if (amount > 0) {
        newMetaInfo.amount = amount * multiplyPayout
        newAmount = await appHelper.convertTo2Decimal(
          newAmount + amount * multiplyPayout || 0
        )
        // Process when payout amount === 0
        const payoutUpdateData = {
          amount: newAmount
        }
        const newMetaArr = [...payout.meta, newMetaInfo]
        if (
          newAmount === 0 &&
          (!payout.isFinalSettlement ||
            (await payoutHelper.isFinalSettlementPayoutWillBeCompleted(
              {
                ...payout,
                meta: [...payout.meta, newMetaInfo]
              },
              session
            )))
        ) {
          payoutUpdateData.status = 'completed'
          payoutUpdateData.paymentStatus = 'balanced'
          await payoutService.afterUpdateProcessForNewlyCompletedPayout(
            payout,
            newMetaArr,
            session
          )
        }
        promiseArr.push(
          updateAPayout(
            {
              _id: payout._id
            },
            {
              $set: payoutUpdateData,
              $push: { meta: newMetaInfo }
            },
            session
          )
        )
        promiseArr.push(
          payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
            payout,
            {
              ...payout,
              ...payoutUpdateData,
              meta: newMetaArr
            },
            session
          )
        )
        console.log(
          `====> Checking payout adjustment with landlord invoices for payoutId: ${
            payout?._id
          }, payoutUpdatingData: ${JSON.stringify({
            ...payoutUpdateData,
            meta: newMetaInfo
          })} <====`
        )
        balancedAmount -= amount
        // To get info from last unbalanced payouts
        if (balancedAmount <= 0 || i === unbalancedPayouts.length - 1) {
          const data = {
            invoiceUpdateData,
            newPayout: { amount, payoutId: payout._id },
            multiplyLandlord
          }
          invoiceUpdateData = await payoutHelper.getDistributedBalanceAmount(
            data
          )
          needUpdateInvoice = true
        }
      }
    }
  }
  if (size(promiseArr)) {
    await Promise.all(promiseArr)
  }
  let updatedInvoice = landlordInvoice
  if (needUpdateInvoice) {
    const updateInvoice = JSON.parse(JSON.stringify(landlordInvoice))
    console.log('invoiceUpdateData', invoiceUpdateData)
    updateInvoice.totalBalanced = invoiceUpdateData.totalBalanced
    updateInvoice.remainingBalance = invoiceUpdateData.remainingBalance
    console.log('updateInvoice', updateInvoice)
    const returnData =
      await invoiceService.updateInvoiceStatusWhenTotalPaidOrTotalBalancedChange(
        landlordInvoice,
        updateInvoice,
        session
      )
    console.log('returnData', returnData)
    updatedInvoice = await invoiceService.updateInvoice(
      {
        _id: landlordInvoice._id
      },
      {
        $set: {
          ...invoiceUpdateData,
          ...returnData.setData
        },
        $unset: {
          ...returnData.unsetData
        }
      },
      session
    )
  }
  console.log(
    `====> Checking payout adjustment with landlord invoice for landlordInvoiceId: ${landlordInvoice?._id} <====`
  )
  return { landlordInvoice: updatedInvoice, unbalancedPayouts }
}

export const linkLandlordInvoicesOrCreditNotes = async (data, session) => {
  const { isAdjustAll, partnerSetting, payout, unbalancedInvoiceQuery } = data
  console.log(
    `====> Started payout adjustment with landlord invoices for payoutId: ${payout?._id} <====`
  )
  const unbalancedInvoices =
    (await payoutHelper.getUnbalancedInvoices(
      {
        payout,
        partnerSetting,
        unbalancedInvoiceQuery,
        isAdjustAll
      },
      session
    )) || []
  // For taking landlord invoice connected to the payout invoice first
  unbalancedInvoices.sort(function (x) {
    return x.rentInvoiceId === payout.invoiceId ? -1 : 0
  })

  let balancedAmount = payout.amount || 0
  let newPayoutMeta = payout.meta || []

  const promiseArr = []
  for (const invoice of unbalancedInvoices) {
    const { invoiceType, remainingBalance = 0 } = invoice
    if (
      balancedAmount > 0 ||
      (isAdjustAll && balancedAmount <= 0) ||
      (balancedAmount < 0 && remainingBalance < 0) ||
      (invoiceType === 'landlord_credit_note' &&
        balancedAmount === 0 &&
        payout.isFinalSettlement &&
        !size(newPayoutMeta))
    ) {
      console.log(
        `====> Started payout adjustment with landlord invoice for landlordInvoiceId: ${invoice?._id} <====`
      )
      const amount =
        payoutHelper.prepareAmountData({
          balancedAmount,
          isAdjustAll,
          remainingBalance
        }) || 0

      if (amount > 0) {
        const newMetaAmount = remainingBalance < 0 ? amount : amount * -1
        const newMetaInfo = {
          amount: await appHelper.convertTo2Decimal(newMetaAmount),
          landlordInvoiceId: invoice._id,
          type: 'landlord_invoice'
        }
        newPayoutMeta = payoutHelper.getPayoutNewMeta(
          newPayoutMeta,
          newMetaInfo
        )
        const invoiceUpdateData =
          await payoutHelper.getDistributedBalanceAmount({
            invoiceUpdateData: invoice,
            multiplyLandlord: remainingBalance > 0 ? 1 : -1,
            newPayout: { amount, payoutId: payout._id }
          })
        const updatedInvoice = JSON.parse(JSON.stringify(invoice))
        updatedInvoice.totalBalanced = invoiceUpdateData.totalBalanced
        const returnData =
          await invoiceService.updateInvoiceStatusWhenTotalPaidOrTotalBalancedChange(
            invoice,
            updatedInvoice,
            session
          )
        promiseArr.push(
          invoiceService.updateInvoice(
            {
              _id: invoice._id
            },
            {
              $set: {
                ...invoiceUpdateData,
                ...returnData.setData
              },
              $unset: {
                ...returnData.unsetData
              }
            },
            session
          )
        )

        // Update balance amount so that next invoice can calculate properly
        balancedAmount += newMetaAmount

        console.log(
          `====> Ended payout adjustment with landlord invoice for landlordInvoiceId: ${
            invoice?._id
          }, landlordUpdatingData: ${JSON.stringify({
            ...invoiceUpdateData,
            ...returnData
          })}, payoutUpdatingData: ${JSON.stringify({
            amount: balancedAmount,
            meta: newMetaInfo
          })} <====`
        )
      }
    }
  }
  if (size(promiseArr)) await Promise.all(promiseArr)
  if (size(unbalancedInvoices)) {
    const payoutUpdateData = {
      meta: newPayoutMeta,
      amount: await appHelper.convertTo2Decimal(balancedAmount)
    }
    if (
      balancedAmount === 0 &&
      (!payout.isFinalSettlement ||
        (await payoutHelper.isFinalSettlementPayoutWillBeCompleted(
          {
            ...payout,
            meta: newPayoutMeta
          },
          session
        )))
    ) {
      payoutUpdateData.status = 'completed'
      payoutUpdateData.paymentStatus = 'balanced'
      await payoutService.afterUpdateProcessForNewlyCompletedPayout(
        payout,
        newPayoutMeta,
        session
      )
    }
    const updatedPayout = await updateAPayout(
      {
        _id: payout._id
      },
      {
        $set: payoutUpdateData
      },
      session
    )
    console.log(
      `====> Ended payout adjustment with landlord invoices for payoutId: ${
        payout?._id
      }, payoutUpdatingData: ${JSON.stringify(payoutUpdateData)} <====`
    )

    await payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
      payout,
      updatedPayout,
      session
    )
    if (updatedPayout?.amount < 0) {
      await updateNextEstimatedPayoutInfo(
        {
          payoutData: updatedPayout,
          payoutMetaType: 'unpaid_expenses_and_commissions'
        },
        session
      )
    }
  }
}

export const addLinkBetweenPayoutAndLandlordInvoices = async (
  data,
  session
) => {
  const {
    isAdjustAll,
    isFirstAdjustLandlordCreditNote,
    partnerSetting,
    payout
  } = data
  const orQuery = [
    { invoiceType: 'landlord_invoice', remainingBalance: { $lt: 0 } },
    { invoiceType: 'landlord_credit_note', remainingBalance: { $lt: 0 } }
  ]
  const unbalancedInvoiceQuery = {
    contractId: payout.contractId,
    partnerId: payout.partnerId
  }
  if (isAdjustAll) {
    orQuery.push({
      invoiceType: 'landlord_invoice',
      remainingBalance: { $gt: 0 }
    })
    orQuery.push({
      invoiceType: 'landlord_credit_note',
      remainingBalance: { $gt: 0 }
    })
  } else if (isFirstAdjustLandlordCreditNote && payout.amount > 0) {
    orQuery.push(
      { invoiceType: 'landlord_invoice', remainingBalance: { $gt: 0 } },
      { invoiceType: 'landlord_credit_note', remainingBalance: { $gt: 0 } }
    )
  }
  unbalancedInvoiceQuery.$or = orQuery
  const params = {
    payout,
    unbalancedInvoiceQuery,
    isAdjustAll,
    partnerSetting
  }
  await linkLandlordInvoicesOrCreditNotes(params, session)
}

export const adjustBetweenPayoutsAndLandlordInvoices = async (
  data,
  session
) => {
  const {
    contractId,
    isFinalSettlement,
    partnerId,
    partnerSetting,
    propertyId
  } = data
  const query = {
    status: 'estimated',
    contractId,
    partnerId,
    propertyId
  }
  const estimatedPayouts = await payoutHelper.getPayouts(query, session, [
    'invoice'
  ])
  const totalIndex = size(estimatedPayouts)
  for (const [index, payout] of estimatedPayouts.entries()) {
    const isAdjustAll = isFinalSettlement && totalIndex === index + 1
    // Since next payouts amount can be updated by current payout
    const payoutInfo = await payoutHelper.getPayout(
      {
        _id: payout._id,
        status: 'estimated'
      },
      session
    )
    if (size(payoutInfo)) {
      const params = {
        isAdjustAll,
        isFirstAdjustLandlordCreditNote: true,
        partnerSetting,
        payout
      }
      await addLinkBetweenPayoutAndLandlordInvoices(params, session)
    }
  }
}

export const addInvoicePaidInfoInPayout = async (invoice, session) => {
  console.log('addInvoicePaidInfoInPayout')
  if (size(invoice) && invoice._id) {
    const { _id: invoiceId, partnerId, contractId } = invoice
    const query = { invoiceId, partnerId, contractId }
    const invoicePaidOn = invoice.lastPaymentDate || new Date()
    const updateData = { invoicePaid: true, invoicePaidOn }
    const invoicePaidOnDate = (
      await appHelper.getActualDate(invoice.partnerId, true, invoicePaidOn)
    )
      .startOf('day')
      .toDate()
    const payoutQuery = clone(query)
    payoutQuery.payoutDate = { $lt: invoicePaidOnDate }
    const payout = await payoutHelper.getPayout(payoutQuery, session)
    let payoutDate = payout?.payoutDate ? payout.payoutDate : ''
    if (payoutDate) {
      const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
        partnerId,
        session
      )
      while (payoutDate < invoicePaidOnDate) {
        payoutDate = (
          await appHelper.getActualDate(partnerSetting, true, payoutDate)
        )
          .add(1, 'months')
          .toDate()
      }
      updateData.newPayoutDate = payoutDate
      updateData.invoicePaidAfterPayoutDate = true
    }
    console.log('updateAPayout', updateData)
    return await updateAPayout(query, updateData, session)
  } else console.log('Missing invoice to update payout', invoice)
}

export const removeInvoicePaidInfoFromPayout = async (invoice, session) => {
  if (size(invoice) && invoice._id) {
    const { _id, partnerId, contractId } = invoice
    const payoutQuery = {
      invoiceId: _id,
      partnerId,
      contractId,
      status: 'estimated',
      invoicePaid: { $exists: true },
      invoicePaidOn: { $exists: true }
    }
    const updateData = {
      invoicePaid: 1,
      invoicePaidOn: 1,
      invoicePaidAfterPayoutDate: 1
    }
    const payout = await payoutHelper.getPayout(payoutQuery, session)
    if (size(payout)) {
      const { _id: payoutId, newPayoutDate } = payout
      if (newPayoutDate) updateData.newPayoutDate = 1
      console.log('updateData', updateData)
      await updateAPayout({ _id: payout._id }, { $unset: updateData }, session)
      console.log(
        `=== Removed invoice paid info from payout. payoutId: ${payoutId} ===`
      )
    }
  }
}

export const setInvoicePaidInFinalSettlementPayout = async (
  invoice = {},
  session,
  isIgnoreCurrentInvoiceId = false
) => {
  const {
    _id: invoiceId,
    contractId,
    partnerId,
    invoiceType,
    lastPaymentDate
  } = invoice
  if (invoiceType === 'invoice') {
    let query = {
      contractId,
      partnerId,
      isFinalSettlement: true,
      invoicePaid: { $ne: true },
      amount: { $gt: 0 }
    }
    const payout = await payoutHelper.getPayout(query, session)
    if (size(payout)) {
      query = {
        contractId,
        status: { $nin: ['paid', 'credited'] },
        invoiceType: 'invoice'
      }

      if (isIgnoreCurrentInvoiceId) query._id = { $ne: invoiceId }

      const unpaidInvoices = await invoiceHelper.getInvoices(query, session)
      delete query.status
      const lastPaidInvoice = !isIgnoreCurrentInvoiceId
        ? await invoiceHelper.getLastPaidInvoice(query, session)
        : null

      if (
        !size(unpaidInvoices) &&
        ((isIgnoreCurrentInvoiceId && lastPaymentDate) ||
          (lastPaidInvoice && lastPaidInvoice.lastPaymentDate))
      ) {
        query = { _id: payout._id, contractId }
        const data = {
          invoicePaid: true,
          invoicePaidOn: isIgnoreCurrentInvoiceId
            ? lastPaymentDate
            : lastPaidInvoice.lastPaymentDate
        }
        const updatedPayout = await updateAPayout(query, data, session)
        return updatedPayout
      }
    }
  }
}

export const addInvoiceLostInfoInPayout = async (req) => {
  const { body, session } = req
  const { invoiceId, partnerId, contractId } = body
  const query = {
    _id: invoiceId,
    partnerId,
    contractId
  }
  const invoice = await invoiceHelper.getInvoice(query, session)
  const { lostMeta = {} } = invoice || {}
  const invoiceLostOnDate = lostMeta.date

  if (partnerId && invoiceId && invoiceLostOnDate) {
    query.status = 'estimated'
    query.invoiceId = invoiceId
    delete query._id
    const payout = (await payoutHelper.getPayout(query, session)) || {}
    const payoutId = payout._id || ''
    let payoutDate = payout.payoutDate || ''
    const updateData = { invoiceLost: true, invoiceLostOn: invoiceLostOnDate }

    if (payoutId && payoutDate) {
      const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
        partnerId,
        session
      )
      while (payoutDate < invoiceLostOnDate) {
        payoutDate = await appHelper.getActualDate(
          partnerSetting,
          true,
          payoutDate
        )
        payoutDate = payoutDate.add(1, 'months').toDate()
      }
      if (payoutDate) {
        updateData.newPayoutDate = payoutDate
      }
      const updatedPayout = await updateAPayout(query, updateData, session)
      return updatedPayout
    }
  }
}

export const addPayoutTransaction = async (
  payout,
  transactionEvent,
  session
) => {
  const isExistsPayoutTransaction =
    await payoutHelper.isExistsPayoutTransaction(payout, session)
  if (isExistsPayoutTransaction) {
    throw new CustomError(405, 'Transaction already exists')
  }
  const transactionData = await payoutHelper.prepareTransactionData(
    payout,
    transactionEvent,
    session
  )
  const addedTransaction = await transactionService.createTransaction(
    transactionData,
    session
  )
  return addedTransaction
}

//For Payments Lambda #10482

const validateInputDataForApprovePendingPayouts = async (body) => {
  appHelper.checkRequiredFields(['partnerId'], body)
  const { pendingPayoutIds } = body
  if (!size(pendingPayoutIds))
    throw new CustomError(404, 'No pending payouts selected')

  const wrongPayoutsQuery = {
    _id: { $in: pendingPayoutIds },
    status: { $ne: 'pending_for_approval' }
  }
  const wrongPayouts = await payoutHelper.getPayouts(wrongPayoutsQuery)
  if (size(wrongPayouts)) throw new CustomError(400, 'Wrong payouts')
}

const updatePartnerPayoutForApprovePendingPayouts = async (body, session) => {
  const { partnerId, pendingPayoutIds } = body
  let partnerPayoutId = ''
  const partnerPayoutQuery = {
    partnerId,
    type: 'payout',
    status: 'pending_for_approval',
    hasPayouts: true,
    payoutIds: { $exists: false }
  }
  const partnerPayoutOptions = {
    sort: { createdAt: -1 },
    limit: 1
  }
  const partnerPayout = await partnerPayoutHelper.getPartnerPayouts(
    partnerPayoutQuery,
    partnerPayoutOptions
  )
  if (size(partnerPayout)) {
    const payoutParams = {
      status: 'waiting_for_signature',
      eventStatus: 'added_payout_ids',
      eventNote: `Added ${pendingPayoutIds.length} payouts for getting approval`,
      payoutIds: pendingPayoutIds,
      hasPayouts: true
    }
    const updateData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(payoutParams)
    const updated = await partnerPayoutService.updateAPartnerPayout(
      { _id: partnerPayout[0]._id },
      updateData,
      session
    )
    if (!size(updated)) {
      throw new CustomError(
        405,
        `Could not update partner payout status to waiting_for_signature`
      )
    }
    partnerPayoutId = partnerPayout[0]._id
  } else {
    const partnerPayoutsData = {
      partnerId,
      type: 'payout',
      status: 'waiting_for_signature',
      events: [
        {
          status: 'added_payout_ids',
          note: `Added ${pendingPayoutIds.length} payouts for getting approval`,
          createdAt: new Date()
        }
      ],
      payoutIds: pendingPayoutIds,
      hasPayouts: true
    }
    const partnerPayout = await partnerPayoutService.insertAPartnerPayout(
      partnerPayoutsData,
      session
    )
    if (!size(partnerPayout)) {
      throw new CustomError(
        405,
        `Could not update partner payout status to waiting_for_signature`
      )
    }
    partnerPayoutId = partnerPayout[0]._id
  }
  return partnerPayoutId
}

const createAQueueForCreateEsigningDoc = async (body, session) => {
  const { partnerId, pendingPayoutIds, partnerPayoutId, userId } = body
  const queueData = {
    action: 'generate_pending_payout_esigning_doc',
    event: 'payout_approved',
    destination: 'payments',
    priority: 'immediate',
    params: {
      partnerId,
      partnerPayoutId,
      pendingPayoutIds,
      userId
    }
  }
  console.log('queueData ', queueData)
  await appQueueService.insertInQueue(queueData, session)
}

//For Payment Lambda #10482
export const approvePendingPayouts = async (req) => {
  const { body, session, user } = req
  appHelper.validatePartnerAppRequestData(req)
  const isValidSSN = await appHelper.validateUserSSN(user.userId, session)
  if (!isValidSSN) {
    throw new CustomError(
      400,
      'Please add right norwegian national identification to approve payouts'
    )
  }
  await validateInputDataForApprovePendingPayouts(body)
  await invoicePaymentHelper.canApproveDirectRemittances(user)
  const { partnerId, pendingPayoutIds = [] } = body
  const updated = await updatePayouts(
    {
      _id: { $in: pendingPayoutIds },
      status: 'pending_for_approval',
      partnerId
    },
    { $set: { status: 'waiting_for_signature' } },
    session
  )
  if (
    !(
      size(updated) &&
      updated.nModified &&
      updated.nModified === pendingPayoutIds.length
    )
  ) {
    throw new CustomError(
      405,
      'Could not update payouts status to waiting_for_signature'
    )
  }
  const partnerPayoutId = await updatePartnerPayoutForApprovePendingPayouts(
    body,
    session
  )
  body.partnerPayoutId = partnerPayoutId
  await createAQueueForCreateEsigningDoc(body, session)
  return 'Success'
}

export const getEsigningDoc = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId', 'partnerPayoutId'], body)
  const { directRemittanceApprovalUserIds, esigningPdfContent } =
    await payoutHelper.prepareEsigningDoc(body)
  console.log('esigningPdfContent ', esigningPdfContent)
  const { partnerId, userId, partnerPayoutId } = body
  return {
    partnerId,
    userId,
    directRemittanceApprovalUserIds,
    esigningPdfContent,
    partnerPayoutId
  }
}

export const addAppQueueForPayoutAndPaymentEsigning = async (req) => {
  const { body, session } = req
  const appQueueData = await payoutHelper.prepareAppQueueDataForEsigning(body)
  const [addedAppQueue] = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  return addedAppQueue
}

export const updatePartnerPayout = async (params, session) => {
  const {
    directRemittanceSigningMeta,
    directRemittanceSigningStatus,
    partnerPayoutId,
    status,
    eventStatus,
    eventNote,
    payoutIds,
    hasPayouts,
    paymentIds,
    hasRefundPayments
  } = params
  const updateData = {}
  const set = {}

  if (size(directRemittanceSigningMeta)) {
    set.directRemittanceSigningMeta = directRemittanceSigningMeta
  }
  if (size(directRemittanceSigningStatus)) {
    set.directRemittanceSigningStatus = directRemittanceSigningStatus
  }
  if (status) set.status = status
  if (payoutIds) set.payoutIds = payoutIds
  if (hasPayouts) set.hasPayouts = true
  if (paymentIds) set.paymentIds = paymentIds
  if (hasRefundPayments) set.hasRefundPayments = true

  if (size(set)) updateData['$set'] = set

  updateData['$push'] = {
    events: { status: eventStatus, createdAt: new Date(), note: eventNote }
  }
  await partnerPayoutService.updateAPartnerPayout(
    { _id: partnerPayoutId },
    updateData,
    session
  )
  return true
}

export const downloadPayout = async (req) => {
  const { body, session, user } = req
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId

  const {
    accountId,
    agentId,
    bankReferenceId,
    branchId,
    bookingDateRange,
    createdAtDateRange,
    paymentStatus,
    payoutDateRange,
    hasPaused,
    propertyId,
    searchKeyword,
    sentToNETSOnDateRange,
    sort = { createdAt: -1 },
    status,
    tenantId
  } = body
  const params = {}

  if (branchId) {
    appHelper.validateId({ branchId })
    params.branchId = branchId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    params.agentId = agentId
  }
  if (accountId) {
    appHelper.validateId({ accountId })
    params.accountId = accountId
  }
  if (propertyId) {
    appHelper.validateId({ propertyId })
    params.propertyId = propertyId
  }
  if (tenantId) {
    appHelper.validateId({ tenantId })
    params.tenantId = tenantId
  }
  appHelper.validateSortForQuery(sort)

  if (size(createdAtDateRange)) {
    params.createdAtDateRange = {
      startDate: new Date(createdAtDateRange.startDate),
      endDate: new Date(createdAtDateRange.endDate)
    }
  }
  if (size(status)) params.status = status
  if (paymentStatus) params.paymentStatus = [paymentStatus]
  if (size(bookingDateRange)) {
    params.bookingDate = {
      startDate: new Date(bookingDateRange.startDate),
      endDate: new Date(bookingDateRange.endDate)
    }
  }
  if (size(sentToNETSOnDateRange)) {
    params.sentToNETSOn = {
      startDate: new Date(sentToNETSOnDateRange.startDate),
      endDate: new Date(sentToNETSOnDateRange.endDate)
    }
  }
  if (size(payoutDateRange)) {
    params.payoutDate = {
      startDate: new Date(payoutDateRange.startDate),
      endDate: new Date(payoutDateRange.endDate)
    }
  }
  if (hasPaused) params.hasPaused = hasPaused
  if (bankReferenceId) params.bankReferenceId = bankReferenceId
  if (searchKeyword) params.searchKeyword = searchKeyword

  params.userId = userId
  params.partnerId = partnerId
  params.sort = sort
  params.downloadProcessType = 'download_payouts'
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'
  const preparedQuery = await payoutHelper.preparePayoutsQuery(params)
  await appHelper.isMoreOrLessThanTargetRows(PayoutCollection, preparedQuery, {
    moduleName: 'Payouts',
    rejectEmptyList: true
  })
  const queueData = {
    action: 'download_email',
    event: 'download_email',
    priority: 'immediate',
    destination: 'excel-manager',
    status: 'new',
    params
  }

  const payoutQueue = await appQueueService.createAnAppQueue(queueData, session)
  if (size(payoutQueue)) {
    return {
      status: 200,
      message:
        'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
    }
  } else throw new CustomError(404, `Unable to download payout`)
}

export const updatePayoutStatusAsEstimated = async (req) => {
  const { body, session, user } = req
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { payoutId, status } = body
  appHelper.validateId({ payoutId })
  if (!size(status)) {
    throw new CustomError(400, 'Payout status is required')
  }

  const updatedPayout = await updateAPayout(
    { _id: payoutId, status },
    { $set: { status: 'estimated' } },
    session
  )

  if (size(updatedPayout)) {
    return updatedPayout
  } else {
    throw new CustomError(400, 'Failed to update payout status')
  }
}

export const updatePayoutForLambda = async (req) => {
  const { body, session, user } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['payoutUpdateData'], body)
  const { payoutUpdateData = [] } = body
  if (!size(payoutUpdateData)) return false

  const updatedPayouts = []
  for (const payoutData of payoutUpdateData) {
    const {
      bookingDate,
      bankRef = '',
      feedbackInfo = {},
      paymentStatus = '',
      payoutId = '',
      status = ''
    } = payoutData || {}

    const updatingPushData = {}
    const updatingSetData = {}

    if (bankRef) updatingSetData.bankRef = bankRef
    if (bookingDate) updatingSetData.bookingDate = bookingDate
    if (size(feedbackInfo)) updatingPushData.feedbackStatusLog = feedbackInfo
    if (paymentStatus) updatingSetData.paymentStatus = paymentStatus
    if (status) updatingSetData.status = status

    const updatingData = {}

    if (size(updatingPushData)) updatingData['$push'] = updatingPushData
    if (size(updatingSetData)) updatingData['$set'] = updatingSetData

    if (payoutId && size(updatingData)) {
      const updatedPayoutData = await updateAPayout(
        { _id: payoutId },
        updatingData,
        session
      )

      if (size(updatedPayoutData) && size(feedbackInfo)) {
        await payoutService.createLogForUpdatedPayout(
          updatedPayoutData,
          session,
          {
            payoutFeedbackHistory: feedbackInfo
          }
        )
        if (status) await createLogForUpdatedPayout(updatedPayoutData, session)

        updatedPayouts.push(updatedPayoutData)
      }
    }
  }

  return !!size(updatedPayouts)
}

export const updatePayoutsStatusByCreditTransferData = async (
  creditTransferData = [],
  session
) => {
  if (!size(creditTransferData))
    throw new CustomError(
      400,
      'CreditTransferData is required to update payout status'
    )

  const updatedPayouts = []
  for (const creditTransferObj of creditTransferData) {
    console.log(
      '====> Checking credit transfer info for updating payout, creditTransferObj:',
      creditTransferObj,
      '<===='
    )
    const { bankRef, bookingDate, payoutId, status } = creditTransferObj || {}

    const payoutUpdatingSetData = {}
    const payoutUpdatingUnsetData = {}
    if (status === 'ACCP' || status === 'booked') {
      payoutUpdatingSetData.status = 'completed'
      payoutUpdatingSetData.paymentStatus = bookingDate ? 'paid' : 'pending'
      if (status === 'booked') payoutUpdatingSetData.bookingDate = bookingDate
      if (bankRef) payoutUpdatingSetData.bankRef = bankRef
    } else if (status === 'RJCT') {
      payoutUpdatingSetData.status = 'failed'
      payoutUpdatingUnsetData.paymentStatus = 1
    }

    console.log(
      '====> Checking payout updating data for credit transfer object, payoutUpdatingSetData:',
      payoutUpdatingSetData,
      '<===='
    )
    if (payoutId && size(payoutUpdatingSetData)) {
      const updatedPayout = await updateAPayout(
        {
          _id: payoutId,
          bookingDate: { $exists: false }
        },
        { $set: payoutUpdatingSetData, $unset: payoutUpdatingUnsetData },
        session
      )
      if (updatedPayout?._id) {
        await initPayoutAfterUpdateProcessForStatus(updatedPayout, session)

        updatedPayouts.push(updatedPayout)
      }
    }
  }

  console.log(
    '-- Updated all payouts status, bookingDate, paymentStatus based on credit transfer info'
  )

  return updatedPayouts
}

export const addInvoiceCreditedTagInPayout = async (invoiceId, session) => {
  if (!invoiceId) return false
  const updatedPayout = await updateAPayout(
    { invoiceId },
    { $set: { invoiceCredited: true } },
    session
  )
  return updatedPayout
}

export const updateNextEstimatedPayoutInfo = async (params, session) => {
  const { payoutData, payoutMetaType } = params
  let lastPayoutAmount = payoutData.amount
  let lastPayoutId = payoutData._id
  let nextEstimatedPayouts = await payoutHelper.getPayoutsWithSort(
    {
      partnerId: payoutData.partnerId,
      propertyId: payoutData.propertyId,
      contractId: payoutData.contractId,
      status: 'estimated',
      serialId: { $gt: payoutData.serialId }
    },
    { serialId: 1 },
    session
  )
  if (size(nextEstimatedPayouts)) {
    nextEstimatedPayouts.unshift(payoutData)
    nextEstimatedPayouts = JSON.parse(JSON.stringify(nextEstimatedPayouts))
    const payoutUpdateArr = []
    let i = 1
    while (lastPayoutAmount < 0 && i < nextEstimatedPayouts.length) {
      const nextPayoutData = nextEstimatedPayouts[i]
      const meta = {
        type: payoutMetaType,
        amount: lastPayoutAmount,
        payoutId: lastPayoutId
      }
      const totalPayout = nextPayoutData.amount + lastPayoutAmount
      const totalEstimated = nextPayoutData.estimatedAmount + lastPayoutAmount
      payoutUpdateArr.push(
        updateAPayout(
          { _id: nextPayoutData._id },
          {
            $set: {
              amount: await appHelper.convertTo2Decimal(totalPayout),
              estimatedAmount: await appHelper.convertTo2Decimal(totalEstimated)
            },
            $push: {
              meta
            }
          },
          session
        )
      )
      nextPayoutData.amount = totalPayout
      nextPayoutData.estimatedAmount = totalEstimated
      nextPayoutData.meta = [...(nextPayoutData.meta || []), meta]
      // For updating last payout data
      const lastPayoutData = nextEstimatedPayouts[i - 1]
      const updateArr = await updateLastPayoutInfoForCreditInvoice(
        { nextPayoutData, lastPayoutData, lastPayoutAmount },
        session
      )
      payoutUpdateArr.push(...updateArr)
      // End
      lastPayoutAmount = totalPayout
      lastPayoutId = nextPayoutData._id
      i++
    }
    if (size(payoutUpdateArr)) await Promise.all(payoutUpdateArr)
  }
}

const updateLastPayoutInfoForCreditInvoice = async (params, session) => {
  const { lastPayoutAmount, lastPayoutData, nextPayoutData } = params
  const payoutUpdateArr = []
  const metaTotal = await payoutHelper.getPayoutMetaTotal(lastPayoutData.meta)
  const newMetaTotal = await appHelper.convertTo2Decimal(
    metaTotal + lastPayoutAmount * -1
  )
  const lastPayoutNewMeta = {
    type: 'moved_to_next_payout',
    amount: lastPayoutAmount * -1,
    payoutId: nextPayoutData._id
  }
  const lastPayoutUpdateData = {
    amount: 0,
    status: 'completed'
  }
  if (lastPayoutData.paymentStatus !== 'balanced') {
    lastPayoutUpdateData.paymentStatus = 'balanced'
  }
  let setStatus = false
  let setMeta = false
  if (
    !['completed', 'in_progress'].includes(lastPayoutData.status) ||
    (lastPayoutData.status === 'estimated' &&
      lastPayoutData.amount < 0 &&
      metaTotal !== newMetaTotal)
  ) {
    setStatus = true
    payoutUpdateArr.push(
      updateAPayout(
        { _id: lastPayoutData._id },
        {
          $set: lastPayoutUpdateData,
          $push: {
            meta: lastPayoutNewMeta
          }
        },
        session
      )
    )
  } else if (
    ['completed', 'in_progress'].includes(lastPayoutData.status) &&
    metaTotal !== newMetaTotal
  ) {
    setMeta = true
    payoutUpdateArr.push(
      updateAPayout(
        { _id: lastPayoutData._id },
        {
          $push: {
            meta: lastPayoutNewMeta
          }
        },
        session
      )
    )
  } else {
    return []
  }
  let lastPayoutInstantUpdateData = JSON.parse(JSON.stringify(lastPayoutData))
  if (setStatus) {
    lastPayoutInstantUpdateData = {
      ...lastPayoutInstantUpdateData,
      ...lastPayoutUpdateData,
      meta: [...lastPayoutData.meta, lastPayoutNewMeta]
    }
    payoutUpdateArr.push(
      createLogForUpdatedPayout(lastPayoutInstantUpdateData, session, {
        context: 'payout',
        partnerId: lastPayoutData.partnerId,
        collectionId: lastPayoutData._id
      })
    )
    if (size(lastPayoutData.correctionsIds)) {
      payoutUpdateArr.push(
        correctionService.updateCorrections(
          {
            _id: {
              $in: lastPayoutData.correctionsIds
            }
          },
          { $set: { status: 'paid', payoutId: lastPayoutData._id } },
          session
        )
      )
    }
  } else if (setMeta) {
    lastPayoutInstantUpdateData = {
      ...lastPayoutInstantUpdateData,
      meta: [...lastPayoutData.meta, lastPayoutNewMeta]
    }
  }
  payoutUpdateArr.push(
    payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
      lastPayoutData,
      lastPayoutInstantUpdateData,
      session
    )
  )
  return payoutUpdateArr
}

export const afterUpdateProcessForNewlyCompletedPayout = async (
  payoutInfo = {},
  newMeta = [],
  session
) => {
  const payoutUpdateData = {
    amount: 0,
    status: 'completed',
    paymentStatus: 'balanced',
    meta: newMeta
  }
  const lastPayoutInstantUpdateData = {
    ...JSON.parse(JSON.stringify(payoutInfo)),
    ...payoutUpdateData
  }
  await createLogForUpdatedPayout(lastPayoutInstantUpdateData, session, {
    context: 'payout',
    partnerId: payoutInfo.partnerId,
    collectionId: payoutInfo._id
  })
  if (size(payoutInfo.correctionsIds)) {
    await correctionService.updateCorrections(
      {
        _id: {
          $in: payoutInfo.correctionsIds
        }
      },
      { $set: { status: 'paid', payoutId: payoutInfo._id } },
      session
    )
  }
}

// These are the after update process of payouts
// const checkPayoutCorrectionIdsToUpdateCorrections = async (
//   previous,
//   doc,
//   session
// ) => {
//   if (!(size(previous) && size(doc))) return false
//
//   const { _id: payoutId, correctionsIds } = doc || {}
//   const { correctionsIds: previousCorrectionIds } = previous || {}
//   if (payoutId && size(previousCorrectionIds) !== size(correctionsIds))
//     await correctionService.updateCorrections(
//       { _id: { $in: correctionsIds } },
//       { $set: { payoutId } },
//       session
//     )
// }
// await checkPayoutStatusToCreateLog(previous, doc, session)
// await checkPayoutStatusToHandleFailedPayout(previous, doc, session)
// await checkPayoutUpdatedDataToUpdateInvoiceSummary(previous, doc, session)
// TODO: Have to add last payout unpaid expenses and commissions in next payout from v2 services
// TODO: await addLastPayoutUnpaidExpensesAndCommissionsInNextPayout(previous, doc, session)
// TODO: Have to check payout amount and status after 1 minute so that updateLastPayoutInfo trigger after all the commission created
// TODO: await insertSQSMsgForUpdatePayout(doc, session)
// await checkPayoutStatusToUpdateCorrectionStatus(previous, doc, session)
// await checkPayoutStatusToSendNotificationAndCreateTransaction(
//   previous,
//   doc,
//   session
// )

export const updateInvoiceLostInfoInPayout = async (invoice = {}, session) => {
  const { _id, contractId, lostMeta, partnerId } = invoice
  const query = {
    contractId,
    invoiceId: _id,
    partnerId,
    status: 'estimated'
  }

  const payout = await payoutHelper.getPayout(query, session)
  if (!payout || !payout?.payoutDate) return

  let payoutDate = payout.payoutDate
  const invoiceLostOnDate = (await appHelper.getActualDate(partnerId, true))
    .startOf('day')
    .toDate()

  const updateData = { invoiceLost: true, invoiceLostOn: lostMeta.date }

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId,
    session
  )

  while (payoutDate < invoiceLostOnDate) {
    payoutDate = (
      await appHelper.getActualDate(partnerSetting, true, payoutDate)
    )
      .add(1, 'months')
      .toDate()
  }

  if (payoutDate) updateData.newPayoutDate = payoutDate

  const updatedPayout = await updateAPayout(query, updateData, session)
  return updatedPayout
}

export const updatePayoutPauseStatus = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['holdPayout', 'payoutId'])
  const { body, session } = req
  await payoutHelper.validateUpdatePayoutPauseStatus(body)
  const { holdPayout = false, partnerId, payoutId } = body
  const payout = await updateAPayout(
    { _id: payoutId, partnerId },
    { $set: { holdPayout } },
    session
  )
  return payout
}

export const createEstimatedPayoutService = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['invoiceId'], body)
  const { invoiceId, isFinalSettlement, meta } = body
  const invoice = await invoiceHelper.getInvoice(
    {
      _id: invoiceId
    },
    undefined,
    ['contract', 'partnerSetting']
  )
  if (!size(invoice)) {
    throw new CustomError(404, 'Invoice not found')
  } else if (!size(invoice.contract)) {
    throw new CustomError(404, 'Invoice contract not found')
  } else if (!size(invoice.partnerSetting)) {
    throw new CustomError(404, 'Partner setting not found')
  }
  const contract = invoice.contract
  const params = {
    contract,
    invoice,
    isFinalSettlement:
      isFinalSettlement &&
      !!(contract.rentalMeta && contract.rentalMeta.status === 'closed'),
    meta,
    partnerSetting: invoice.partnerSetting,
    userId: invoice.createdBy
  }
  await createEstimatedPayout(params, session)
  return {
    result: true
  }
}

export const createOrAdjustEstimatedPayoutService = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['invoiceId'], body)
  const { invoiceId, isFinalSettlement } = body
  const invoice = await invoiceHelper.getInvoice(
    {
      _id: invoiceId
    },
    session,
    ['contract', 'partnerSetting']
  )
  if (!size(invoice)) {
    throw new CustomError(404, 'Invoice not found')
  } else if (!size(invoice.contract)) {
    throw new CustomError(404, 'Invoice contract not found')
  } else if (!size(invoice.partnerSetting)) {
    throw new CustomError(404, 'Partner setting not found')
  }
  const contract = invoice.contract
  const payout = await payoutHelper.getPayout(
    {
      contractId: invoice.contractId,
      partnerId: invoice.partnerId,
      status: 'estimated'
    },
    session
  )
  if (size(payout)) {
    await invoiceService.adjustEstimatedPayout(payout, invoice, session)
  } else {
    const params = {
      contract,
      invoice,
      isFinalSettlement:
        isFinalSettlement &&
        !!(contract.rentalMeta && contract.rentalMeta.status === 'closed'),
      partnerSetting: invoice.partnerSetting,
      userId: invoice.createdBy
    }
    await createEstimatedPayout(params, session)
  }
  return {
    result: true
  }
}

export const updateEstimatedPayoutDate = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['payoutDate', 'payoutId'])
  const { body, session } = req
  const { payoutDate, partnerId, payoutId } = body
  const previousPayout = await payoutHelper.getPayout({
    _id: payoutId,
    partnerId,
    status: 'estimated'
  })
  if (!previousPayout) throw new CustomError(404, 'Payout not found')
  const updatedPayout = await payoutService.updateAPayout(
    { _id: payoutId, partnerId },
    {
      payoutDate
    },
    session
  )
  const options = { action: 'updated_payout_info', context: 'payout' }
  const logData = logHelper.prepareLogDataForPayout(options, updatedPayout)
  logData.isChangeLog = true
  logData.changes = [
    {
      field: 'payoutDate',
      type: 'text',
      oldDate: previousPayout.payoutDate || '',
      newDate: payoutDate
    }
  ]
  await logService.createLog(logData, session)

  return {
    _id: payoutId,
    payoutDate: updatedPayout.payoutDate
  }
}

export const createPayoutForFinalSettlement = async (
  contractInfo = {},
  userId,
  session
) => {
  const partnerSetting = contractInfo.partnerSetting
  let payoutData = await payoutHelper.getPayoutCreationDataForFinalSettlement(
    contractInfo,
    session
  )
  const data = {
    contract: contractInfo,
    isFinalSettlement: true,
    partnerSetting,
    payoutData,
    dueDate: new Date(),
    userId
  }
  payoutData = await payoutHelper.preparePayoutData(data, session)
  const payout = await createPayout(payoutData, session)
  console.log(
    `--- Created a new Payout for contractId: ${contractInfo._id} ---`
  )
  await createLogForAddedNewPayout(
    'added_new_payout',
    payout.toObject(),
    session
  )
  // await appQueueService.createAppQueueForAddingPayoutBankReference(
  //   payout.partnerId,
  //   session
  // )
  // After insert process starts
  await appQueueService.createAppQueueForAddingSerialId(
    'payouts',
    payout,
    session
  )
  await checkPayoutUpdatedDataToUpdateInvoiceSummary({}, payout, session)
  // Since there is no meta in payout data, so we don't need updateLastPayoutInfo implementation
  await addLinkBetweenPayoutAndLandlordInvoices(
    {
      payout,
      isFirstAdjustLandlordCreditNote: true,
      isAdjustAll: true,
      partnerSetting
    },
    session
  )
  await finalSettlementService.linkedBetweenFinalSettlementClaimsAndPayouts(
    {
      contractId: payout.contractId,
      partnerId: payout.partnerId
    },
    session
  )
  await finalSettlementService.linkedBetweenFinalSettlementsAndLastPayouts(
    {
      contractId: payout.contractId,
      partnerId: payout.partnerId
    },
    session
  )
}

export const addBankReferences = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { limit = 50, partnerId } = body
  let isCompleted = true
  const totalPayouts = await payoutHelper.countPayouts(
    {
      partnerId,
      bankReferenceId: {
        $exists: false
      }
    },
    session
  )
  if (totalPayouts > limit) {
    isCompleted = false
  }
  await addPayoutBankReferences(partnerId, limit, session)
  return {
    isCompleted
  }
}

const addPayoutBankReferences = async (partnerId, limit, session) => {
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  let lastBankReference = partnerSetting?.lastBankReference
  const payouts = await payoutHelper.getPayoutsWithOptions(
    {
      partnerId,
      bankReferenceId: {
        $exists: false
      }
    },
    {
      limit
    },
    session
  )
  const promiseArr = []
  for (const payout of payouts) {
    // Generate bank reference id for next time find the payment transfer transaction information
    lastBankReference = payoutHelper.getRandomBankReference(lastBankReference)
    promiseArr.push(
      updateAPayout(
        {
          _id: payout._id
        },
        {
          $set: {
            bankReferenceId: lastBankReference
          }
        },
        session
      )
    )
  }
  if (size(promiseArr)) {
    await Promise.all(promiseArr)
    await partnerSettingService.updateAPartnerSetting(
      {
        _id: partnerSetting._id
      },
      { lastBankReference },
      session
    )
  }
}
