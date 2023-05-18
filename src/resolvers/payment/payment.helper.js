import {
  assign,
  assignIn,
  clone,
  concat,
  difference,
  differenceWith,
  each,
  extend,
  filter,
  find,
  indexOf,
  isEqual,
  pick,
  reverse,
  size,
  sumBy
} from 'lodash'
import moment from 'moment-timezone'

import {
  AppInvoiceCollection,
  InvoiceCollection,
  InvoicePaymentCollection
} from '../models'
import {
  appHelper,
  contractHelper,
  depositInsuranceHelper,
  finalSettlementHelper,
  invoiceHelper,
  partnerHelper,
  partnerSettingHelper,
  transactionHelper
} from '../helpers'

import { appPermission, CustomError } from '../common'

export const getPaymentById = async (query, session) => {
  const payment = await InvoicePaymentCollection.findById(query).session(
    session
  )
  return payment
}

export const getPayment = async (query, session) => {
  const payment = await InvoicePaymentCollection.findOne(query).session(session)
  return payment
}

export const getPayments = async (
  query,
  session,
  sort = { paymentDate: 1 }
) => {
  const payments = await InvoicePaymentCollection.find(query)
    .sort(sort)
    .session(session)
  return payments
}

export const getPaymentsWithProjection = async (query, session, options) => {
  const { projection = {}, sort = { paymentDate: 1 } } = options || {}
  const payments = await InvoicePaymentCollection.find(query, projection)
    .sort(sort)
    .session(session)
  return payments
}

export const countPayments = async (query, session) => {
  const countedPayments = await InvoicePaymentCollection.find(query)
    .session(session)
    .countDocuments()
  return countedPayments
}

export const getUnManagedInvoicesWithPayments = async (data, session) => {
  const aggregationPipelines = [
    {
      $match: {
        contractId: data.contractId,
        partnerId: data.partnerId,
        invoiceId: { $exists: true },
        invoices: { $exists: false }
      }
    },
    { $sort: { paymentDate: 1 } },
    {
      $addFields: {
        invoiceTypes: {
          $concatArrays: [
            [],
            {
              $cond: [
                {
                  $and: [
                    { $eq: ['$paymentType', 'manual'] },
                    {
                      $eq: [
                        {
                          $and: [
                            { $eq: ['$paymentType', 'bank'] },
                            { $ne: ['$status', 'new'] },
                            { $ifNull: ['$meta.kidNumber', false] }
                          ]
                        },
                        false
                      ]
                    }
                  ]
                },
                ['invoice'],
                ['invoice', 'landlord_invoice']
              ]
            }
          ]
        }
      }
    },
    {
      $lookup: {
        from: 'invoices',
        localField: 'invoiceId',
        foreignField: '_id',
        let: { invoiceTypes: '$invoiceTypes' },
        pipeline: [
          { $match: { $expr: { $in: ['$invoiceType', '$$invoiceTypes'] } } }
        ],
        as: 'invoice'
      }
    },
    { $unwind: '$invoice' },
    {
      $addFields: {
        totalDue: {
          $subtract: [
            {
              $add: [
                '$invoice.invoiceTotal',
                {
                  $cond: [
                    { $ifNull: ['$invoice.creditedAmount', false] },
                    '$invoice.creditedAmount',
                    0
                  ]
                }
              ]
            },
            '$invoice.totalPaid'
          ]
        }
      }
    },
    {
      $addFields: {
        totalDue: {
          $cond: [{ $lt: ['$totalDue', 0] }, 0, '$totalDue']
        }
      }
    },
    {
      $group: {
        _id: '$invoice._id',
        invoice: { $first: '$invoice' },
        invoiceTotalDue: { $first: '$totalDue' },
        payments: { $push: '$$ROOT' }
      }
    }
  ]

  return await InvoicePaymentCollection.aggregate(aggregationPipelines).session(
    session
  )
}

export const forwardRemainingAmount = async (data, newlyPaidInvoices) => {
  const { remaining, payment, nonPaidInvoices } = data
  if (size(nonPaidInvoices)) {
    const invoice = nonPaidInvoices.shift() // Shift method removes the first item of array
    const lastInvoice = !size(nonPaidInvoices)
    const creditedAmount = invoice.creditedAmount || 0
    let totalDue = invoice.invoiceTotal - invoice.totalPaid + creditedAmount
    totalDue = await appHelper.convertTo2Decimal(totalDue || 0)
    const invoiceInfo = { invoiceId: invoice._id }
    if (lastInvoice || remaining < totalDue) {
      invoiceInfo.amount = remaining
      invoice.totalPaid += remaining
      if (remaining > totalDue) {
        invoiceInfo.remaining = await appHelper.convertTo2Decimal(
          remaining - totalDue || 0
        )
      }
      data.remaining = 0
    } else {
      invoiceInfo.amount = totalDue
      invoice.totalPaid += totalDue
      data.remaining = await appHelper.convertTo2Decimal(
        remaining - totalDue || 0
      )
    }
    if (remaining < totalDue) {
      nonPaidInvoices.unshift(invoice) // Unshift method adds an item at first index to array
    }
    invoice.lastPaymentDate = payment.paymentDate
    const invoiceInPayment = payment.invoices.find(
      (item) => item.invoiceId === invoice._id
    )
    if (invoiceInPayment) {
      invoiceInPayment.amount += invoiceInfo.amount
      invoiceInPayment.remaining = invoiceInfo.remaining
    } else {
      payment.invoices.push(invoiceInfo)
    }
    newlyPaidInvoices.push(invoice)
    if (data.remaining > 0 && !lastInvoice) {
      await forwardRemainingAmount(data, newlyPaidInvoices) // Recursion call here
    }
  } else {
    return false
  }
}

export const getTotalDueAmountOfAnInvoice = (invoice) => {
  const { creditedAmount = 0, invoiceTotal, totalPaid } = invoice
  const due = invoiceTotal - totalPaid + (creditedAmount || 0)
  return due < 0 ? 0 : due
}

export const getPaymentsWithRemaining = async (data, session) => {
  const query = {
    contractId: data.contractId,
    partnerId: data.partnerId,
    'invoices.remaining': { $exists: true }
  }
  const payments = await getPayments(query, session)
  return payments
}

export const getNonPaidInvoices = async (data, session) => {
  const query = {
    contractId: data.contractId,
    partnerId: data.partnerId,
    invoiceType: 'invoice',
    status: { $nin: ['credited', 'paid', 'lost'] },
    isNonRentInvoice: data.isNonRentInvoice ? true : { $ne: true }
  }
  if (data.earlierInvoiceId) {
    query.earlierInvoiceId = {
      $ne: data.earlierInvoiceId
    }
  }
  return invoiceHelper.getInvoices(query, session, {
    sort: { invoiceStartOn: 1 }
  })
}

export const getOverPaidInvoiceById = async (invoiceId, session) => {
  const query = { _id: invoiceId, isOverPaid: true }
  const overPaidInvoice = await invoiceHelper.getInvoice(query, session)
  return overPaidInvoice
}

export const getRemainingInvoiceIdAndAmount = (payment) => {
  const invoices = payment.invoices.filter((invoice) => invoice.remaining)
  return invoices
}
export const getPaymentInvoicesArrayWithoutRemaining = (payment) => {
  const invoices = (payment.invoices = payment.invoices
    .map((invoice) => {
      // Move out overpaid remaining amount
      if (invoice.remaining) {
        return {
          invoiceId: invoice.invoiceId,
          amount: invoice.amount - invoice.remaining
        }
      }
      return invoice
    })
    .filter((invoice) => invoice.amount > 0))

  return invoices
}

export const getCreditedOverPaidInvoicesWithPayments = async (
  data,
  session
) => {
  const aggregationPipelines = [
    {
      $match: {
        contractId: data.contractId,
        partnerId: data.partnerId,
        invoiceType: 'invoice',
        isOverPaid: true,
        creditedAmount: { $lt: 0 }
      }
    },
    {
      $lookup: {
        from: 'invoice-payments',
        localField: '_id',
        foreignField: 'invoices.invoiceId',
        as: 'payment'
      }
    },
    {
      $unwind: '$payment'
    },
    {
      $addFields: {
        invoiceTotalAmount: {
          $subtract: [
            { $add: ['$invoiceTotal', { $ifNull: ['$creditedAmount', 0] }] },
            { $ifNull: ['$lostMeta.amount', 0] }
          ]
        },
        invoicePaymentInfo: {
          $first: {
            $filter: {
              input: '$payment.invoices',
              as: 'invoice',
              cond: { $eq: ['$$invoice.invoiceId', '$_id'] }
            }
          }
        },
        paymentId: '$payment._id',
        payment: '$$REMOVE'
      }
    },
    {
      $addFields: {
        invoiceTotalAmount: {
          $cond: [{ $gt: ['$invoiceTotalAmount', 0] }, '$invoiceTotalAmount', 0]
        }
      }
    },
    {
      $group: {
        _id: '$_id',
        invoiceTotalAmount: { $first: '$invoiceTotalAmount' },
        invoices: { $push: '$invoicePaymentInfo' },
        payments: {
          $push: { invoice: '$invoicePaymentInfo', paymentId: '$paymentId' }
        }
      }
    }
  ]

  return await invoiceHelper.getInvoicesViaAggregation(
    aggregationPipelines,
    session
  )
}

export const isExistsPaymentTransaction = async (params, session) => {
  const {
    payment,
    transactionAmount,
    isDifferentAmount,
    callFromUpgradeScript
  } = params
  const { _id, partnerId, type, propertyId, invoices = [] } = payment
  let { amount } = payment
  const [paymentInvoice] = invoices
  const { invoiceId = '' } = paymentInvoice
  if (transactionAmount) {
    amount = await appHelper.convertTo2Decimal(transactionAmount)
  }
  const transactionQuery = {
    partnerId,
    paymentId: _id,
    invoiceId,
    propertyId,
    amount,
    type
  }
  let existsTransaction = await transactionHelper.getTransaction(
    transactionQuery,
    session
  )
  if (existsTransaction && isDifferentAmount) {
    existsTransaction = false
  }
  if (existsTransaction) {
    transactionQuery.amount = amount * -1
    const existsNegativePaymentTransaction =
      !!(await transactionHelper.getTransaction(transactionQuery, session))
    const aggregateQuery = [
      { $match: { partnerId, paymentId: _id, type } },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: '$amount' }
        }
      }
    ]
    const [allTransactions = {}] =
      await transactionHelper.getTransactionByAggregate(aggregateQuery, session)
    const { totalAmount = '' } = allTransactions
    if (
      existsNegativePaymentTransaction ||
      callFromUpgradeScript ||
      !totalAmount
    ) {
      existsTransaction = false
    }
  }
  return existsTransaction
}

export const prepareTransactionData = async (params, session) => {
  const {
    payment = {},
    paymentAmount,
    callFromUpgradeScript,
    transactionEvent
  } = params
  let transactionData = pick(payment, [
    'partnerId',
    'contractId',
    'tenantId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'createdBy'
  ])
  const {
    _id = '',
    partnerId = '',
    meta = {},
    invoices = [],
    isManualRefund = false,
    type = ''
  } = payment
  let { amount } = payment
  const [paymentInvoice = {}] = invoices
  const { invoiceId = '' } = paymentInvoice
  const { cdTrAccountNumber = '' } = meta
  let bankAccountNumber = cdTrAccountNumber ? cdTrAccountNumber : ''
  let accountingType = 'rent_payment'
  if (isManualRefund && size(meta)) {
    bankAccountNumber = ''
  }
  const isBrokerPartner = await partnerHelper.isBrokerPartner(
    partnerId,
    session
  )
  const isFinalSettlementInvoice = await invoiceHelper.isFinalSettlementInvoice(
    invoiceId,
    session
  )
  if (isBrokerPartner && isFinalSettlementInvoice) {
    accountingType = 'final_settlement_payment'
  }
  if (paymentAmount) {
    amount = await appHelper.convertTo2Decimal(paymentAmount)
  }
  const accountingParams = {
    partnerId: payment.partnerId,
    accountingType,
    options: {
      invoiceId: payment.invoiceId,
      bankAccountNumber
    }
  }
  const paymentTransactionData =
    await transactionHelper.getAccountingDataForTransaction(
      accountingParams,
      session
    )
  if (!size(paymentTransactionData)) {
    return {}
  }
  transactionData.invoiceId = invoiceId
  transactionData.amount = amount
  transactionData.paymentId = _id
  transactionData.createdAt = payment.bookingDate || payment.paymentDate
  transactionData.type = type
  transactionData.transactionEvent = transactionEvent
  if (callFromUpgradeScript) {
    transactionData.isAddedFromUpgradeScript = callFromUpgradeScript
  }
  if (transactionData.createdAt) {
    transactionData.period =
      await transactionHelper.getFormattedTransactionPeriod(
        transactionData.createdAt,
        partnerId
      )
  }
  transactionData = extend(transactionData, paymentTransactionData)
  return transactionData
}

export const getPaymentsForTransaction = async (
  partnerId,
  paymentIds,
  session
) => {
  const paymentQuery = { status: 'registered', type: 'payment', partnerId }
  const refundPaymentQuery = {
    partnerId,
    type: 'refund',
    refundStatus: 'completed',
    refundPaymentStatus: 'paid'
  }
  if (size(paymentIds)) {
    paymentQuery._id = { $in: paymentIds }
    refundPaymentQuery._id = { $in: paymentIds }
  }
  const query = {
    $or: [paymentQuery, refundPaymentQuery]
  }
  const allPayments = (await getPayments(query, session)) || []
  return allPayments
}

export const getPaymentIdsForLegacyTransaction = async (partnerId) => {
  const query = {
    partnerId,
    $or: [
      { status: 'registered', type: 'payment' },
      {
        type: 'refund',
        refundStatus: 'completed',
        refundPaymentStatus: 'paid'
      }
    ]
  }
  const payments = await InvoicePaymentCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: null,
        paymentIds: { $addToSet: '$_id' }
      }
    }
  ])
  const [paymentInfo = {}] = payments || []
  const { paymentIds = [] } = paymentInfo
  return paymentIds
}

export const checkRequiredDataInMatchPaymentsReq = (body) => {
  appHelper.checkRequiredFields(['action', 'event', 'params', 'queueId'], body)
  const { params, queueId } = body
  appHelper.checkRequiredFields(['contractId', 'partnerId'], params)
  const { contractId, partnerId } = params

  appHelper.validateId({ contractId })
  appHelper.validateId({ partnerId })
  appHelper.validateId({ queueId })
}

export const checkRequiredDataInIdentifyBankPaymentReq = (body) => {
  appHelper.checkRequiredFields(['action', 'event', 'params', 'queueId'], body)
  const { params, queueId } = body
  appHelper.checkRequiredFields(['paymentId'], params)
  const { paymentId } = params

  appHelper.validateId({ paymentId })
  appHelper.validateId({ queueId })
}

export const identifyDIBankPayment = async (params, newData) => {
  const { cdTrAccountNumber = '', kidNumber = '' } = params
  const bankAccountNumber = await appHelper.getSettingsInfoByFieldName(
    'bankAccountNumber'
  )

  if (kidNumber && cdTrAccountNumber === bankAccountNumber) {
    console.log('=== Matching deposit insurance by kidNumber ===')
    const depositInsurance = await depositInsuranceHelper.getADepositInsurance(
      {
        kidNumber
      },
      null,
      ['appInvoice']
    )
    if (
      size(depositInsurance) &&
      depositInsurance._id &&
      size(depositInsurance.appInvoice)
    ) {
      console.log('=== Payment matched with deposit insurance ===')
      newData.accountId = depositInsurance.accountId
      newData.agentId = depositInsurance.agentId
      newData.appInvoiceId = depositInsurance.appInvoice._id
      newData.appPartnerId = depositInsurance.partnerId
      newData.branchId = depositInsurance.branchId
      newData.contractId = depositInsurance.contractId
      newData.depositInsuranceId = depositInsurance._id
      newData.isDepositInsurancePayment = true
      newData.propertyId = depositInsurance.propertyId
      newData.status = 'registered'
      newData.tenantId = depositInsurance.tenantId
      console.log(
        '=== Set: status as registered and others ids from depositInsurance in the payment ==='
      )
    }
  }
}

export const setCommonIdsFromInvoice = (newData, invoice) => {
  newData.accountId = invoice.accountId
  newData.agentId = invoice.agentId
  newData.branchId = invoice.branchId
  newData.contractId = invoice.contractId
  newData.invoiceId = invoice._id
  newData.partnerId = invoice.partnerId
  newData.propertyId = invoice.propertyId
  newData.tenantId = invoice.tenantId

  if (invoice.isFinalSettlement)
    newData.isFinalSettlement = invoice.isFinalSettlement
}

export const identifyBankPaymentByKidNumberAndAccountNumber = async (
  params,
  newData
) => {
  const { invoiceAccountNumber, kidNumber, partnerId } = params
  let invoices = {}

  const invoiceQuery = {
    kidNumber,
    partnerId,
    status: { $nin: ['credited', 'paid', 'lost', 'cancelled'] },
    $or: [
      { invoiceType: 'invoice' },
      {
        invoiceType: 'landlord_invoice',
        isPayable: true
      }
    ]
  }

  // Matching both Paid to account number & KID! Ref #13742
  if (invoiceAccountNumber) {
    invoiceQuery.invoiceAccountNumber = invoiceAccountNumber
  }
  const options = { sort: { dueDate: 1 } }

  // Getting All invoices with status filter
  invoices = await invoiceHelper.getInvoices(invoiceQuery, null, options)

  if (!size(invoices)) {
    // Getting All invoices without status filter
    delete invoiceQuery.status
    invoices = await invoiceHelper.getInvoices(invoiceQuery, null, options)
  }

  if (size(invoices)) {
    let lastInvoiceInfo = {}

    // Since we got the invoices, check which invoice is not paid yet and add the payment to that.
    for (const invoice of invoices) {
      if (!newData.invoiceId) {
        // Don't check next invoices if we get any unpaid invoice on dueDate ascending order.
        const { invoiceTotal, totalPaid } = invoice
        if (invoiceTotal > totalPaid) {
          // This invoice still unpaid, So adding the payment to this
          setCommonIdsFromInvoice(newData, invoice)
        } else lastInvoiceInfo = invoice
      }
    }

    // Check if we got the appropriate invoice, if not then add the payment to last invoice
    if (!newData.invoiceId) setCommonIdsFromInvoice(newData, lastInvoiceInfo)

    // If We have the required data. Make payment status 'registered'
    if (newData.invoiceId && newData.partnerId) newData.status = 'registered'
  }
}

const invoiceTotalDueAmountForFinalSettlement = async (contractId) => {
  const contractInfo = await contractHelper.getAContract({ _id: contractId })

  if (size(contractInfo)) {
    const { totalDue } = await contractHelper.getContractInvoiceInfo(
      contractInfo
    )

    return await appHelper.convertTo2Decimal(totalDue)
  }
}

export const isItAfterFinalSettlementPayment = async (newData) => {
  // ## Getting total rent amount that is due for final settlement
  const { contractId, invoiceId, partnerId } = newData
  let totalDueRentAmount = 0
  if (contractId) {
    totalDueRentAmount = await invoiceTotalDueAmountForFinalSettlement(
      contractId
    )
  }
  console.log('=== Check contract final settlement is done or not done! ===')
  // ## Check contract final settlement is done or not done?

  // ## If contract final settlement is done then payment except only for
  // final settlement landlord invoice and final settlement rent invoice for
  // which finalSettlementStatus is "in_progress" and has got a due rent amount to pay.

  // ## Otherwise payment will not be excepted for other invoices based on contract.
  // change payment status to "unspecified"
  if (
    contractId &&
    partnerId &&
    (await finalSettlementHelper.isDoneFinalSettlement(contractId, partnerId))
  ) {
    console.log("=== Final settlement is done for this payment's contract. ===")
    if (invoiceId) {
      const { invoiceType = '' } = await invoiceHelper.getInvoice({
        _id: invoiceId
      })

      if (invoiceType && invoiceType === 'landlord_invoice') {
        if (!(await finalSettlementHelper.isPayableLandlordInvoice(invoiceId)))
          newData = {
            status: 'unspecified',
            partnerId
          }

        return newData
      }

      if (invoiceType && invoiceType === 'invoice' && totalDueRentAmount <= 0) {
        if (
          !(await finalSettlementHelper.isFinalSettlementInProgress(
            contractId,
            partnerId
          ))
        )
          newData = {
            status: 'unspecified',
            partnerId
          }

        return newData
      }
    }
  } // End of isDoneFinalSettlement block
  console.log(
    "=== Final settlement is not done for this payment's contract ==="
  )
  return newData
}

export const checkRequiredDataInAddManualPaymentReq = (body) => {
  appHelper.checkRequiredFields(
    ['amount', 'paymentDate', 'paymentFor', 'userId'],
    body
  )

  const { amount, paymentFor } = body
  if (amount <= 0) {
    throw new CustomError(400, 'Amount must be greater than 0')
  }

  if (paymentFor === 'invoice') {
    appHelper.checkRequiredFields(
      ['invoiceId', 'partnerId', 'paymentToAccountNumber'],
      body
    )
    const { invoiceId, partnerId } = body
    appHelper.validateId({ invoiceId })
    appHelper.validateId({ partnerId })
  } else if (paymentFor === 'appInvoice') {
    appHelper.checkRequiredFields(['appInvoiceId'], body)
    const { appInvoiceId } = body
    appHelper.validateId({ appInvoiceId })
  }
}

export const isDoneFinalSettlementBasedOnInvoiceId = async (
  invoiceId,
  partnerId
) => {
  const invoice =
    invoiceId && partnerId
      ? await invoiceHelper.getInvoice({ _id: invoiceId, partnerId })
      : {}

  if (size(invoice)) {
    const { contractId, invoiceType, isPayable, partnerId } = invoice
    if (invoiceType === 'landlord_invoice' && isPayable) return false
    else if (contractId && partnerId)
      return await finalSettlementHelper.isDoneFinalSettlement(
        contractId,
        partnerId
      )
  } else return false
}

export const findPartnerSettingsByACNumber = async (
  cdTrAccountNumber,
  newData
) => {
  // Finding partnerId from partnerSetting by cdTrAccountNumber
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
    $or: [
      { 'bankPayment.firstMonthACNo': cdTrAccountNumber },
      { 'bankPayment.afterFirstMonthACNo': cdTrAccountNumber },
      { 'landlordBankPayment.firstMonthACNo': cdTrAccountNumber },
      { 'landlordBankPayment.afterFirstMonthACNo': cdTrAccountNumber },
      { 'bankAccounts.accountNumber': cdTrAccountNumber }
    ]
  })
  const { partnerId = '' } = partnerSetting || {}
  if (partnerId) newData.partnerId = partnerId
}

export const isDoneFinalSettlementBasedOnPaymentId = async (
  partnerId,
  paymentId
) => {
  const payment = await getPayment({ _id: paymentId, partnerId })

  if (size(payment)) {
    const { invoiceId } = payment
    if (invoiceId)
      return await isDoneFinalSettlementBasedOnInvoiceId(invoiceId, partnerId)
    else return false
  }

  throw new CustomError(404, `Payment doesn't exists`)
}

export const getRemovedPaymentLogData = async (paymentId, partnerId) => {
  let paymentInfo = await getPayment({ _id: paymentId, partnerId })
  const visibility = ['payment']
  let logData = { partnerId, context: 'payment' }

  if (size(paymentInfo)) {
    logData.paymentId = paymentInfo._id

    paymentInfo = pick(paymentInfo, [
      'accountId',
      'agentId',
      'amount',
      'branchId',
      'invoiceId',
      'propertyId',
      'tenantId'
    ])

    logData = assign(logData, paymentInfo) // Extend log data.

    if (paymentInfo.accountId) visibility.push('account')
    if (paymentInfo.propertyId) visibility.push('property')
    if (paymentInfo.tenantId) visibility.push('tenant')
    if (paymentInfo.invoiceId) visibility.push('invoice')

    logData.visibility = visibility

    logData.meta = [{ field: 'paymentAmount', value: paymentInfo.amount }]

    return logData
  } else return {}
}

export const prepareRefundPaymentCreationData = async (params) => {
  const {
    invoiceId = '',
    paymentInfo,
    paymentRefundData,
    refundedAmount,
    userId
  } = params
  console.log(`=== Preparing refund payment. paymentInfo: ${paymentInfo}`)
  console.log('=== paymentRefundData:', paymentRefundData)
  console.log('=== refundedAmount:', refundedAmount)
  let refundPaymentData = clone(paymentRefundData)
  let refundAmount = refundedAmount

  const paymentData = pick(paymentInfo, [
    'accountId',
    'agentId',
    'branchId',
    'contractId',
    'invoiceId',
    'partnerId',
    'propertyId',
    'tenantId',
    'tenants'
  ])
  const refundPaymentInvoices = []
  const paymentInvoices = paymentInfo?.invoices || []

  if (size(paymentInfo) && refundAmount > paymentInfo?.amount) return false // RefundingAmount can't be greater than the amount of the payment'

  refundPaymentData = assignIn(refundPaymentData, paymentData)
  refundPaymentData.type = 'refund'
  refundPaymentData.paymentId = paymentInfo._id
  refundPaymentData.amount =
    (await appHelper.convertTo2Decimal(refundPaymentData.refundAmount || 0)) *
    -1 // Making negative amount to create a refund payment
  console.log(`=== refundPaymentData.amount: ${refundPaymentData?.amount}`)
  if (!refundPaymentData.paymentDate) refundPaymentData.paymentDate = new Date()

  if (refundPaymentData.isManualRefund) {
    const metaData = {}

    if (paymentInfo.paymentType === 'manual' && size(paymentInfo.meta)) {
      if (paymentInfo.meta.cdTrAccountNumber)
        metaData.dbTrAccountNumber = paymentInfo.meta.cdTrAccountNumber
      if (paymentInfo.meta.cdTrName)
        metaData.dbTrName = paymentInfo.meta.cdTrName
      if (paymentInfo.meta.cdTrAddress)
        metaData.dbTrAddress = paymentInfo.meta.cdTrAddress
    }

    if (refundPaymentData.refundToAccountNumber) {
      metaData.cdTrAccountNumber = refundPaymentData.refundToAccountNumber

      delete refundPaymentData.refundToAccountNumber
    }

    if (size(metaData)) refundPaymentData.meta = metaData
  }

  if (invoiceId) {
    const paymentInvoice = find(paymentInvoices, { invoiceId })

    refundPaymentData.invoiceId = invoiceId

    if (refundAmount > 0) {
      refundPaymentInvoices.push({
        invoiceId: paymentInvoice.invoiceId,
        amount: clone(refundAmount) * -1
      })
    }
  } else {
    reverse(paymentInvoices)

    for (const invoice of paymentInvoices) {
      if (refundAmount > 0 && invoice.amount >= refundAmount) {
        refundPaymentInvoices.push({
          invoiceId: invoice.invoiceId,
          amount: clone(refundAmount) * -1
        })
        refundAmount = 0
      } else if (refundAmount > 0 && invoice.amount < refundAmount) {
        refundPaymentInvoices.push({
          invoiceId: invoice.invoiceId,
          amount: invoice.amount * -1
        })
        refundAmount = refundAmount - invoice.amount
      }
    }
  }

  refundPaymentData.invoices = refundPaymentInvoices
  if (userId) {
    refundPaymentData.createdBy = userId
  }
  return refundPaymentData
}

export const getInvoiceRefundedAmount = async (
  invoiceId,
  partnerId,
  paymentId
) => {
  if (!invoiceId) return false
  // If paymentId exist then calculate the amount that has not been refunded from the payment of the invoice
  const invoiceInfo = await invoiceHelper.getInvoice({
    _id: invoiceId,
    partnerId
  })
  const query = {
    partnerId,
    type: 'payment',
    invoices: { $elemMatch: { invoiceId, amount: { $gt: 0 } } }
  }
  const paymentQuery = paymentId ? assign(query, { _id: paymentId }) : query
  const payments = await getPayments(paymentQuery, null, { createdAt: -1 })
  let refundPaymentIds = []
  const totalPaid = invoiceInfo?.totalPaid || 0
  let totalRefundAmount = paymentId ? 0 : totalPaid

  if (size(payments)) {
    each(payments, (payment) => {
      refundPaymentIds = concat(refundPaymentIds, payment.refundPaymentIds)

      if (paymentId && payment._id === paymentId) {
        const paymentInvoice = find(payment.invoices, { invoiceId })

        if (size(paymentInvoice) && paymentInvoice.amount)
          totalRefundAmount = paymentInvoice.amount
      }
    })
  }

  if (size(refundPaymentIds)) {
    const refundQuery = {
      partnerId,
      _id: { $in: refundPaymentIds },
      type: 'refund',
      refundPaymentStatus: { $ne: 'paid' },
      refundStatus: 'estimated'
    }
    const refundPaymentQuery = paymentId
      ? assign(refundQuery, { paymentId })
      : refundQuery
    const refundPayments = (await getPayments(refundPaymentQuery)) || {}
    const refundedAmount = size(refundPayments)
      ? sumBy(refundPayments, 'amount')
      : 0

    if (refundedAmount) totalRefundAmount += refundedAmount
  }

  return totalRefundAmount
}

export const validatedUserInputDataForInvoiceRefundPaymentCreationReq = async (
  body
) => {
  appHelper.checkRequiredFields(
    ['invoiceId', 'partnerId', 'paymentRefundData', 'userId'],
    body
  )

  const { partnerId, paymentRefundData, userId } = body

  if (!size(paymentRefundData))
    throw new CustomError(400, 'Required paymentRefundData')

  const { paymentType } = paymentRefundData

  if (!paymentType)
    throw new CustomError(400, 'Missing paymentType in paymentRefundData')

  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  ) {
    // Only partner admin can do this request
    throw new CustomError(400, 'Permission denied')
  }
}

export const getPaymentsToRefund = async (
  invoiceId,
  partnerId,
  refundAmount
) => {
  if (!invoiceId || refundAmount <= 0) return {}
  let totalRefundedAmount = clone(refundAmount)
  let payments = []

  const invoicePayments = await getPayments(
    {
      partnerId,
      type: 'payment',
      $or: [{ invoiceId }, { invoices: { $elemMatch: { invoiceId } } }]
    },
    null,
    { amount: -1 }
  )

  if (size(invoicePayments)) {
    for (const payment of invoicePayments) {
      const refundAmountForInvoice = await getInvoiceRefundedAmount(
        invoiceId,
        partnerId,
        payment._id
      )

      // if invoice refund amount is equal to total refund amount then refund will be done from that payment only

      if (totalRefundedAmount > 0) {
        if (refundAmountForInvoice === refundAmount) {
          payments = [payment]
          return payments
        } else if (refundAmountForInvoice > refundAmount) {
          // If invoice refund amount is greater than total refund amount then refund will be done from that payment only
          payments = [payment]
        } else {
          payments.push(payment)
          totalRefundedAmount -= refundAmountForInvoice
        }
      }
    }
  }

  return payments
}

export const checkRequiredDataInDIPaymentUpdateReq = (body) => {
  appHelper.checkRequiredFields(
    ['amount', 'appInvoiceId', 'paymentDate', 'paymentId', 'userId'],
    body
  )
  const { appInvoiceId, paymentId } = body
  appHelper.validateId({ appInvoiceId })
  appHelper.validateId({ paymentId })
}

export const checkRequiredDataInRefundPaymentUpdateReq = (body) => {
  appHelper.checkRequiredFields(['partnerId', 'paymentId', 'userId'], body)
  const { partnerId, paymentId } = body
  appHelper.validateId({ partnerId })
  appHelper.validateId({ paymentId })
}

export const checkRequiredDataInManualPaymentUpdateReq = (body) => {
  appHelper.checkRequiredFields(
    [
      'amount',
      'invoiceId',
      'partnerId',
      'paymentDate',
      'paymentId',
      'paymentReason',
      'paymentToAccountNumber',
      'userId'
    ],
    body
  )
  const { invoiceId, partnerId, paymentId } = body
  appHelper.validateId({ partnerId })
  appHelper.validateId({ paymentId })
  appHelper.validateId({ invoiceId })
}

export const prepareDataForManualPaymentUpdate = async (params) => {
  const {
    amount,
    invoiceId,
    partnerId,
    paymentId,
    paymentDate,
    paymentReason,
    paymentToAccountNumber,
    userId
  } = params

  // Only partner admin can do this request
  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  )
    throw new CustomError(400, 'Permission denied')
  // Payment must be exists
  const payment = await getPayment({
    _id: paymentId,
    partnerId
  })

  if (!size(payment)) throw new CustomError(404, 'Payment does not exists')

  if (payment.type !== 'payment')
    throw new CustomError(400, 'Invalid payment type found')

  // Invoice must be exists
  const invoice = await invoiceHelper.getInvoice({
    _id: invoiceId
  })
  if (!size(invoice)) throw new CustomError(404, 'Invoice does not exists')

  if (await isDoneFinalSettlementBasedOnInvoiceId(invoiceId, partnerId))
    throw new CustomError(405, 'Final settlement is done for this payment')

  // Payment updating log will be created only if 'paymentDate' or 'amount' or 'paymentToAccountNumber' is changed.
  let haveToCreateLog = !(
    moment(payment.paymentDate).isSame(paymentDate) &&
    payment.amount === amount &&
    paymentToAccountNumber === payment.meta?.cdTrAccountNumber
  )

  // Have to update Transactions createdAt date, if paymentDate has been changed
  const previousPaymentDate = await appHelper.getActualDate(
    partnerId,
    true,
    payment.paymentDate
  )
  const docPaymentDate = await appHelper.getActualDate(
    partnerId,
    true,
    paymentDate
  )
  const haveToUpdateTransaction = !previousPaymentDate.isSame(docPaymentDate)

  let updatingData = { amount, paymentDate, paymentReason }

  if (payment.invoiceId !== invoiceId) {
    // Trying to move the payment into another Invoice (Set payment data for new invoiceId)
    const { invoiceType, isFinalSettlement, isPayable } = invoice
    updatingData = { amount, invoiceId, paymentDate, paymentReason }

    const invoiceData = pick(invoice, [
      'accountId',
      'agentId',
      'branchId',
      'contractId',
      'propertyId',
      'tenantId',
      'tenants'
    ])
    updatingData = assign(updatingData, invoiceData) // Merging updatingData with invoiceData

    if (payment.status === 'unspecified') {
      updatingData.paymentType = 'manual'
      updatingData.status = 'registered'
    }

    if (
      invoiceType === 'landlord_invoice' &&
      (isFinalSettlement || isPayable)
    ) {
      updatingData.isFinalSettlement = true
    }

    haveToCreateLog = true // We have to create payment updating log, for new invoice payment
  }

  const updatingMeta = {}

  if (paymentToAccountNumber !== payment.meta?.cdTrAccountNumber) {
    // If user changed paymentToAccountNumber
    const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })

    if (!size(partnerSetting))
      throw new CustomError(404, 'PartnerSetting does not exists')

    const partner = await partnerHelper.getAPartner({ _id: partnerId })
    if (!size(partner)) throw new CustomError(404, 'Partner does not exists')

    const isDirectPartner = partner.accountType === 'direct'

    updatingMeta['meta.cdTrAccountNumber'] = paymentToAccountNumber

    if (isDirectPartner && partnerSetting.companyInfo) {
      const { companyName, officeAddress } = partnerSetting.companyInfo
      if (companyName) updatingMeta['meta.cdTrName'] = companyName
      if (officeAddress) updatingMeta['meta.cdTrAddress'] = officeAddress
    } else {
      const { bankAccounts } = partnerSetting

      if (size(bankAccounts)) {
        const bankAccount = find(bankAccounts, {
          accountNumber: paymentToAccountNumber
        })

        if (size(bankAccount)) {
          const { orgAddress, orgName } = bankAccount
          if (orgAddress) updatingMeta['meta.cdTrAddress'] = orgAddress
          if (orgName) updatingMeta['meta.cdTrName'] = orgName
        }
      }
    }
  }

  if (size(updatingMeta)) updatingData = { ...updatingData, ...updatingMeta }

  const data =
    payment.invoiceId !== invoiceId
      ? { $set: updatingData, $unset: { invoices: 1 } }
      : { $set: updatingData }

  return {
    data,
    haveToCreateLog,
    haveToUpdateTransaction,
    oldPaymentInfo: payment
  }
}

export const getChangedPaymentInvoicesArray = (docInvoices, prevInvoices) => {
  let changeInvoices = differenceWith(docInvoices, prevInvoices, isEqual)

  if (size(docInvoices) > size(prevInvoices))
    changeInvoices = differenceWith(docInvoices, prevInvoices, isEqual)
  else if (size(docInvoices) < size(prevInvoices))
    changeInvoices = difference(docInvoices, prevInvoices, isEqual)

  return changeInvoices
}

const getPaymentTotalBasedOnInvoice = async (invoiceId, paymentId) => {
  if (!invoiceId) return 0

  let newPaymentTotal = 0

  const paymentQuery = {
    type: 'payment',
    invoices: { $elemMatch: { invoiceId, amount: { $gt: 0 } } }
  }

  if (paymentId) paymentQuery._id = { $ne: paymentId }

  const payments = await getPayments(paymentQuery)

  if (size(payments)) {
    for (const payment of payments) {
      const paymentInvoiceInfo = find(
        payment.invoices || [],
        (paymentInvoice) =>
          size(paymentInvoice) && paymentInvoice?.invoiceId === invoiceId
      )

      if (paymentInvoiceInfo?.amount)
        newPaymentTotal = newPaymentTotal + paymentInvoiceInfo.amount
    }
  }

  return newPaymentTotal
}

export const getPaymentInvoiceInfo = async (params) => {
  const { invoiceInfo = {}, isLastIndex, paymentId, type } = params
  console.log('params', params)
  let paymentAmount = params.paymentAmount || 0

  const invoiceId = invoiceInfo?._id
  const invoiceTotal = invoiceInfo?.invoiceTotal || 0
  const totalPaid = invoiceInfo?.totalPaid || 0
  console.log('invoiceId', invoiceId)
  console.log('invoiceTotal', invoiceTotal)
  console.log('totalPaid', totalPaid)

  let dueTotal = invoiceTotal - totalPaid
  console.log('dueTotal 1', dueTotal)
  let paidAmount = 0

  if (type === 'editPayment') dueTotal = invoiceTotal
  console.log('dueTotal 2', dueTotal)

  if (invoiceId && paymentId) {
    const paymentTotal = await getPaymentTotalBasedOnInvoice(
      invoiceId,
      paymentId
    )

    if (paymentTotal > 0) dueTotal = invoiceTotal - paymentTotal
  }

  let remaining = 0

  if ((paymentAmount <= dueTotal && dueTotal > 0) || isLastIndex) {
    // If payment amount is less than due amount, then paid all amount in a invoice OR
    // If this last invoice, then all paymentAmount in a invoice
    console.log('=== Paid all amount in this invoice ===')
    paidAmount = clone(paymentAmount)
    paymentAmount = 0
    if (isLastIndex) {
      remaining = paidAmount - dueTotal
      if (remaining > paidAmount) {
        remaining = paidAmount
      }
    }
  } else {
    paymentAmount = paymentAmount - dueTotal
    paidAmount = dueTotal
  }
  console.log('dueTotal 3', dueTotal)
  console.log('paidAmount', paidAmount)

  console.log('remaining', remaining)
  const invoice = { invoiceId }
  invoice.amount = await appHelper.convertTo2Decimal(paidAmount)

  if (remaining > 0)
    invoice.remaining = await appHelper.convertTo2Decimal(remaining)

  return { invoice, paidAmount, paymentAmount }
}

export const getPaymentNewInvoicesArray = (
  paymentInvoices,
  paymentNewInvoicesData
) => {
  let invoicesData = []
  let newRemaining = 0
  let newAmount = 0
  let newInvoiceId = ''
  let isExistsInvoiceId = false

  if (size(paymentNewInvoicesData)) {
    newAmount = paymentNewInvoicesData.amount || 0
    newInvoiceId = paymentNewInvoicesData.invoiceId || ''
    newRemaining = paymentNewInvoicesData.remaining || 0
  }

  if (size(paymentInvoices)) {
    invoicesData = filter(paymentInvoices, (invoiceInfo) => {
      if (
        size(invoiceInfo) &&
        newInvoiceId &&
        invoiceInfo.invoiceId === newInvoiceId
      ) {
        invoiceInfo.amount = invoiceInfo.amount + newAmount

        if (newRemaining > 0)
          invoiceInfo.remaining = (invoiceInfo.remaining || 0) + newRemaining

        isExistsInvoiceId = true
      }

      return invoiceInfo
    })

    if (!isExistsInvoiceId && newAmount > 0)
      invoicesData.push(paymentNewInvoicesData)
  } else invoicesData = [paymentNewInvoicesData]

  return invoicesData
}

export const prepareInvoicesArrayDataForEditPayment = async (
  oldPayment,
  updatedPayment
) => {
  const paymentId = updatedPayment._id
  const partnerId = updatedPayment.partnerId
  const contractId = updatedPayment.contractId
  const invoices = updatedPayment.invoices
  let newInvoicesArray = []
  let paymentAmount = updatedPayment.amount
  // Prepare new invoices array for payment

  if (updatedPayment.amount > oldPayment.amount) {
    // If edit payment amount grater than previous payment amount
    for (const [index, invoice] of invoices.entries()) {
      const invoiceId = invoice.invoiceId
      const paidAmount = invoice.amount
      let currentPayment = clone(paymentAmount)
      const isLastIndex = size(invoices) === index + 1

      if (paymentAmount > 0) paymentAmount = paymentAmount - paidAmount

      if (isLastIndex) {
        const invoiceInfo = await invoiceHelper.getInvoice({ _id: invoiceId })

        const unpaidInvoiceQuery = {
          contractId,
          partnerId,
          invoiceType: 'invoice',
          _id: { $ne: invoiceId },
          status: { $nin: ['credited', 'paid'] }
        }

        if (
          size(invoiceInfo) &&
          invoiceInfo?.invoiceType === 'landlord_invoice' &&
          (invoiceInfo?.isFinalSettlement || invoiceInfo?.isPayable)
        ) {
          unpaidInvoiceQuery.invoiceType = 'landlord_invoice'
          if (invoiceInfo.isFinalSettlement)
            unpaidInvoiceQuery.isFinalSettlement = true
          if (invoiceInfo.isPayable) unpaidInvoiceQuery.isPayable = true
        } else {
          unpaidInvoiceQuery.isNonRentInvoice = invoiceInfo.isNonRentInvoice
            ? true
            : { $ne: true }
        }

        const unPaidInvoices = await invoiceHelper.getInvoices(
          unpaidInvoiceQuery,
          null,
          {
            sort: { invoiceSerialId: 1 }
          }
        )
        const invoiceTotal = invoiceInfo?.invoiceTotal || 0
        const isPaid = !!(
          size(invoiceInfo) &&
          indexOf(['paid', 'credited'], invoiceInfo?.status) !== -1
        )

        // If has remaining, that means no unpaid invoices in this contract
        if (!size(unPaidInvoices)) {
          const newInvoiceObj = {
            invoiceId,
            amount: await appHelper.convertTo2Decimal(clone(currentPayment))
          }
          const remaining = clone(currentPayment) - invoiceTotal

          if (remaining > 0) newInvoiceObj.remaining = remaining

          paymentAmount = 0
          currentPayment = 0
          newInvoicesArray = getPaymentNewInvoicesArray(
            clone(newInvoicesArray),
            clone(newInvoiceObj)
          )
        } else if (currentPayment > 0) {
          paymentAmount = clone(currentPayment)

          // Payment move to paid invoice if its last invoice
          if (isPaid) {
            const paymentInvoiceInfo = await getPaymentInvoiceInfo({
              invoiceInfo: clone(invoiceInfo),
              isLastIndex: false,
              paymentAmount: clone(paymentAmount),
              paymentId,
              type: 'editPayment'
            })

            paymentAmount = paymentInvoiceInfo.paymentAmount
            newInvoicesArray = getPaymentNewInvoicesArray(
              clone(newInvoicesArray),
              clone(paymentInvoiceInfo.invoice)
            )
          }

          // Payment move to paid invoice or unpaid invoice if its last invoice
          if (
            paymentAmount > 0 &&
            (!isPaid || (!size(unPaidInvoices) && isPaid))
          ) {
            const isLastIndexInvoice = size(unPaidInvoices) > 0
            const paymentInvoiceInfo = await getPaymentInvoiceInfo({
              invoiceInfo: clone(invoiceInfo),
              isLastIndex: isLastIndexInvoice,
              paymentAmount: clone(paymentAmount),
              paymentId,
              type: 'editPayment'
            })

            if (paymentInvoiceInfo.paidAmount > 0) {
              paymentAmount = paymentInvoiceInfo.paymentAmount
              newInvoicesArray = getPaymentNewInvoicesArray(
                clone(newInvoicesArray),
                clone(paymentInvoiceInfo.invoice)
              )
            }
          }

          // Payment move to unpaid invoices
          if (size(unPaidInvoices) && paymentAmount > 0) {
            for (const [newIndex, invoiceObj] of unPaidInvoices.entries()) {
              const isLastIndexForUnpaidInvoice =
                size(unPaidInvoices) === newIndex + 1
              const paymentInvoiceInfo = await getPaymentInvoiceInfo({
                invoiceInfo: clone(invoiceObj),
                isLastIndex: isLastIndexForUnpaidInvoice,
                paymentAmount: clone(paymentAmount),
                paymentId,
                type: 'editPayment'
              })

              if (paymentInvoiceInfo.paidAmount > 0) {
                paymentAmount = paymentInvoiceInfo.paymentAmount
                newInvoicesArray = getPaymentNewInvoicesArray(
                  clone(newInvoicesArray),
                  clone(paymentInvoiceInfo.invoice)
                )
              }
            }
          }
        }
      } else {
        newInvoicesArray = getPaymentNewInvoicesArray(
          clone(newInvoicesArray),
          clone(invoice)
        )
      }
    }
  } else if (updatedPayment.amount < oldPayment.amount) {
    // If edit payment amount less than previous payment amount
    for (const invoice of invoices) {
      const amount = clone(invoice.amount)
      let newAmount = 0

      if (paymentAmount > 0 && amount > 0 && paymentAmount >= amount)
        newAmount = clone(amount)
      else if (paymentAmount > 0 && amount > 0 && paymentAmount < amount)
        newAmount = clone(paymentAmount)

      if (newAmount > 0) {
        const lastObj = {
          invoiceId: invoice.invoiceId,
          amount: await appHelper.convertTo2Decimal(newAmount)
        }

        paymentAmount = clone(paymentAmount) - clone(newAmount)

        newInvoicesArray.push(lastObj)
      }
    }
  }

  return { newInvoicesArray }
}

export const getPaymentStatusForPartnerDashboard = async (query) => {
  const result = await InvoicePaymentCollection.aggregate([
    {
      $match: {
        ...query,
        type: 'refund',
        refundStatus: {
          $in: ['pending_for_approval', 'waiting_for_signature']
        }
      }
    },
    {
      $addFields: {
        pendingPaymentApprovalCount: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'refund'] },
                { $eq: ['$refundStatus', 'pending_for_approval'] }
              ]
            },
            1,
            0
          ]
        },
        paymentAwaitingSignatureCount: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'refund'] },
                { $eq: ['$refundStatus', 'waiting_for_signature'] }
              ]
            },
            1,
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        pendingPaymentApprovalCount: {
          $sum: '$pendingPaymentApprovalCount'
        },
        paymentAwaitingSignatureCount: {
          $sum: '$paymentAwaitingSignatureCount'
        }
      }
    }
  ])
  const [paymentStatus = {}] = result || []
  const { paymentAwaitingSignatureCount = 0, pendingPaymentApprovalCount = 0 } =
    paymentStatus
  return {
    paymentAwaitingSignatureCount,
    pendingPaymentApprovalCount
  }
}

export const getPaymentsJSONDataForTestIncomingPayment = async (
  params = {}
) => {
  if (!size(params)) return []

  const {
    isAppInvoice = false,
    isInvoice = false,
    limit = 1,
    partnerId,
    sort = { createdAt: 1 }
  } = params || {}

  appHelper.validateSortForQuery(sort)

  const collection = isAppInvoice
    ? AppInvoiceCollection
    : isInvoice
    ? InvoiceCollection
    : undefined

  const pipeline = [
    {
      $match: {
        invoiceType: isAppInvoice ? 'app_invoice' : 'invoice',
        partnerId,
        status: { $nin: ['credited', 'paid'] }
      }
    },
    { $sort: sort },
    { $limit: limit },
    {
      $lookup: {
        from: 'partner_settings',
        localField: 'partnerId',
        foreignField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $addFields: {
        isAppInvoice,
        partnerSettings: { $first: '$partnerSettings' },
        todayDate: {
          $dateToString: {
            date: new Date(),
            format: '%Y-%m-%d'
          }
        }
      }
    },
    {
      $addFields: {
        document: {
          Id: 'Id',
          CreDtTm: new Date(),
          Acct: {
            Id: {
              Othr: {
                Id: '$invoiceAccountNumber',
                SchmeNm: { Cd: 'BBAN' }
              }
            },
            Ccy: 'NOK',
            Ownr: {
              Nm: 'Unite Living AS',
              Id: {
                OrgId: {
                  Othr: {
                    Id: '00916861923',
                    SchmeNm: { Cd: 'CUST' }
                  }
                }
              }
            },
            Svcr: {
              FinInstnId: { BIC: 'DNBANOKK' }
            }
          },
          Ntry: {
            NtryRef: 'NtryRef',
            Amt: {
              '@Ccy': 'NOK',
              '#text': '$invoiceTotal'
            },
            CdtDbtInd: 'CRDT',
            Sts: 'BOOK',
            BookgDt: { Dt: '$todayDate' },
            ValDt: { Dt: '$todayDate' },
            AcctSvcrRef: '08004106559',
            BkTxCd: {
              Domn: {
                Cd: 'PMNT',
                Fmly: {
                  Cd: 'RCDT',
                  SubFmlyCd: 'VCOM'
                }
              },
              Prtry: {
                Cd: '230',
                Issr: 'NETS'
              }
            },
            NtryDtls: {
              TxDtls: {
                Refs: {
                  AcctSvcrRef: '261039152',
                  PmtInfId: '81540269733'
                },
                AmtDtls: {
                  TxAmt: {
                    Amt: {
                      '@Ccy': 'NOK',
                      '#text': '$invoiceTotal'
                    }
                  }
                },
                RltdPties: {
                  Dbtr: {
                    Nm: '$isAppInvoice'
                      ? 'Unite Living AS'
                      : '$partnerSettings.companyInfo.companyName'
                  },
                  DbtrAcct: {
                    Id: {
                      Othr: {
                        Id: '26101859478',
                        SchmeNm: { Cd: 'BBAN' }
                      }
                    }
                  },
                  CdtrAcct: {
                    Id: {
                      Othr: {
                        Id: '$invoiceAccountNumber',
                        SchmeNm: { Cd: 'BBAN' }
                      }
                    }
                  }
                },
                RmtInf: {
                  Strd: {
                    RfrdDocAmt: {
                      RmtdAmt: {
                        '@Ccy': 'NOK',
                        '#text': '$invoiceTotal'
                      }
                    },
                    CdtrRefInf: {
                      Tp: {
                        CdOrPrtry: { Cd: 'SCOR' }
                      },
                      Ref: '$kidNumber'
                    }
                  }
                },
                RltdDts: { IntrBkSttlmDt: '$todayDate' }
              }
            }
          }
        }
      }
    },
    { $group: { _id: null, documents: { $push: '$document' } } }
  ]
  const [data] = (await collection.aggregate(pipeline)) || []
  const { documents } = data || {}

  return documents?.map((document) => {
    const messageId = Math.floor(100000 + Math.random() * 90000000000)
    document.Id = messageId + '-1'
    document.Ntry.NtryRef = messageId + '-1-1'
    return document
  })
}
