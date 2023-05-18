import nid from 'nid'
import {
  compact,
  clone,
  each,
  extend,
  includes,
  indexOf,
  map,
  pick,
  size,
  uniq
} from 'lodash'
import moment from 'moment-timezone'
import {
  AccountCollection,
  InvoicePaymentCollection,
  PayoutCollection
} from '../models'
import {
  accountHelper,
  appHelper,
  appQueueHelper,
  commissionHelper,
  contractHelper,
  dashboardHelper,
  fileHelper,
  invoiceHelper,
  invoicePaymentHelper,
  notificationTemplateHelper,
  partnerSettingHelper,
  partnerPayoutHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import { CustomError, appPermission } from '../common'

export const getUniqueFieldValues = async (field, query) =>
  (await PayoutCollection.distinct(field, query)) || []

export const getPayout = async (query, session, sort = { createdAt: 1 }) => {
  const payout = await PayoutCollection.findOne(query)
    .session(session)
    .sort(sort)
  return payout
}

export const getPayouts = async (query, session, populate = []) => {
  const payouts = await PayoutCollection.find(query)
    .populate(populate)
    .session(session)
    .sort({ serialId: 1 })
  return payouts
}

export const getPayoutsWithOptions = async (query, options = {}, session) => {
  const { limit = 50, skip = 0, sort = { createdAt: 1 } } = options
  return await PayoutCollection.find(query)
    .skip(skip)
    .sort(sort)
    .limit(limit)
    .session(session)
}

export const getPayoutsWithSort = async (query, sort, session) =>
  await PayoutCollection.find(query).sort(sort).session(session)

export const getAggregatedPayouts = async (pipeline = [], session) =>
  await PayoutCollection.aggregate(pipeline).session(session)

const getInvoicePipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            invoiceSerialId: 1
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo')
]

const getPropertyPipelineForPayoutList = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            imageUrl: 1,
            location: {
              name: 1,
              postalCode: 1,
              city: 1,
              country: 1
            },
            listingTypeId: 1,
            propertyTypeId: 1,
            apartmentId: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo')
]

const getContractPipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            holdPayout: 1
          }
        }
      ],
      as: 'contractInfo'
    }
  },
  appHelper.getUnwindPipeline('contractInfo')
]

export const getPayoutForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getInvoicePipeline(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    ...getPropertyPipelineForPayoutList(),
    ...getContractPipeline(),
    {
      $project: {
        _id: 1,
        serialId: 1,
        invoiceInfo: 1,
        status: 1,
        numberOfFails: 1,
        paymentStatus: 1,
        holdPayout: {
          $cond: [
            {
              $and: [
                { $eq: ['$status', 'estimated'] },
                {
                  $or: [
                    { $eq: ['$holdPayout', true] },
                    { $eq: ['$contractInfo.holdPayout', true] }
                  ]
                }
              ]
            },
            true,
            false
          ]
        },
        accountInfo: 1,
        propertyInfo: 1,
        tenantInfo: 1,
        sentToNETSOn: 1,
        bookingDate: 1,
        payoutDate: 1,
        amount: 1,
        bankReferenceId: 1,
        createdAt: 1
      }
    }
  ]
  const payouts = (await PayoutCollection.aggregate(pipeline)) || []
  return payouts
}

export const countPayouts = async (query, session) => {
  const countedPayouts = await PayoutCollection.find(query)
    .session(session)
    .countDocuments()
  return countedPayouts
}

export const getPayoutById = async (id, session) => {
  const payout = await PayoutCollection.findById(id).session(session)
  return payout
}

export const getLastPayout = async (query, sort = {}, session) => {
  const lastPayout = await PayoutCollection.findOne(query)
    .session(session)
    .sort(sort)
  return lastPayout
}

export const getLastFailedPayoutMeta = async (invoice, session) => {
  const { partnerId, propertyId, contractId } = invoice
  const query = {
    partnerId,
    propertyId,
    contractId,
    status: 'failed',
    amount: { $gt: 0 }
  }
  const lastFailedPayout = await getLastPayout(
    query,
    { createdAt: -1 },
    session
  )
  let lastFailedPayoutMeta = {}
  if (size(lastFailedPayout)) {
    lastFailedPayoutMeta = {
      type: 'unpaid_earlier_payout',
      amount: await appHelper.convertTo2Decimal(lastFailedPayout.amount || 0),
      payoutId: lastFailedPayout._id
    }
  }
  return lastFailedPayoutMeta
}

export const getLastNegativePayoutMeta = async (invoice, session) => {
  const { partnerId, propertyId, contractId } = invoice
  const query = {
    partnerId,
    propertyId,
    contractId,
    amount: { $lt: 0 }
  }
  const lastNegativePayout = await getLastPayout(
    query,
    { createdAt: -1 },
    session
  )
  let lastNegativePayoutMeta = {}
  if (lastNegativePayout) {
    lastNegativePayoutMeta = {
      type: 'unpaid_expenses_and_commissions',
      amount: await appHelper.convertTo2Decimal(lastNegativePayout.amount || 0),
      payoutId: lastNegativePayout._id
    }
  }
  return lastNegativePayoutMeta
}

export const getPayoutMetaTotal = async (payoutMeta) => {
  let total = 0
  if (size(payoutMeta)) {
    each(payoutMeta, (meta) => {
      total += meta.amount || 0
    })
    total = await appHelper.convertTo2Decimal(total)
  }
  return total * 1
}

export const getLastPayoutMissingMeta = async (invoice, session) => {
  const query = pick(invoice, ['partnerId', 'propertyId', 'contractId'])
  const lastPayoutInfo = await getLastPayout(query, { serialId: -1 }, session)
  const movedToNextMetaInfo = {}
  if (lastPayoutInfo) {
    const payoutMeta = lastPayoutInfo.meta || []
    const payoutTotal = lastPayoutInfo.amount || 0
    const metaTotal = getPayoutMetaTotal(payoutMeta)
    const missingAmount = await appHelper.convertTo2Decimal(
      payoutTotal - metaTotal
    )
    if (missingAmount) {
      movedToNextMetaInfo.type =
        missingAmount > 0
          ? 'unpaid_expenses_and_commissions'
          : 'unpaid_earlier_payout'
      movedToNextMetaInfo.amount = missingAmount * -1
      movedToNextMetaInfo.payoutId = lastPayoutInfo._id
    }
  }
  return movedToNextMetaInfo
}

export const getTotalPayoutAmount = async (payoutMeta) => {
  let totalPayoutAmount = 0
  for (const meta of payoutMeta) {
    totalPayoutAmount += meta.amount
  }
  totalPayoutAmount = await appHelper.convertTo2Decimal(totalPayoutAmount)
  return totalPayoutAmount || 0
}

export const getPayoutCreationData = async (params, session) => {
  const { invoice, isFinalSettlement, meta = [] } = params || {}
  const payoutData = pick(invoice, [
    'partnerId',
    'branchId',
    'agentId',
    'accountId',
    'propertyId',
    'tenantId',
    'contractId',
    'createdAt',
    'tenants'
  ])
  const payoutMeta = [
    {
      type:
        invoice.invoiceType === 'credit_note'
          ? 'credit_rent_invoice'
          : 'rent_invoice',
      amount: invoice.payoutableAmount,
      invoiceId: invoice._id
    },
    ...(meta || [])
  ]
  const lastFailedPayoutMeta = await getLastFailedPayoutMeta(invoice, session)
  if (size(lastFailedPayoutMeta)) {
    payoutMeta.push(lastFailedPayoutMeta)
  }
  const lastNegativePayoutMeta = await getLastNegativePayoutMeta(
    invoice,
    session
  )
  if (size(lastNegativePayoutMeta)) {
    payoutMeta.push(lastNegativePayoutMeta)
  }
  const lastPayoutMissingMeta = await getLastPayoutMissingMeta(invoice, session)
  if (size(lastPayoutMissingMeta)) {
    payoutMeta.push(lastPayoutMissingMeta)
  }
  const totalPayoutAmount = await getTotalPayoutAmount(payoutMeta)
  payoutData.estimatedAmount = totalPayoutAmount
  payoutData.amount = totalPayoutAmount
  payoutData.invoiceId = invoice._id
  payoutData.meta = payoutMeta
  if (isFinalSettlement) {
    // Check all invoices are paid or credited then invoicePaid will be true according to this contract
    const foundUnpaidInvoices = await invoiceHelper.getInvoice(
      {
        contractId: payoutData.contractId,
        status: { $nin: ['paid', 'credited'] },
        invoiceType: 'invoice'
      },
      session
    )
    const lastPaidInvoiceInfo = await invoiceHelper.getLastPaidInvoice(
      {
        contractId: payoutData.contractId,
        invoiceType: 'invoice'
      },
      session
    )
    if (
      !foundUnpaidInvoices &&
      lastPaidInvoiceInfo &&
      lastPaidInvoiceInfo.lastPaymentDate
    ) {
      payoutData.invoicePaid = true
      payoutData.invoicePaidOn = lastPaidInvoiceInfo.lastPaymentDate
    }
  }
  return payoutData
}

export const getPayoutDate = async (
  standardPayoutDate,
  dueDate,
  partnerSetting
) => {
  // If standardPayoutDate is greater then total days of month, then the date should be end of month.
  const invoiceDueDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    dueDate
  )
  let payoutDay = standardPayoutDate
  const daysInCurrentMonth = invoiceDueDate.daysInMonth()
  // Payout date out of this month's max date.
  if (standardPayoutDate > daysInCurrentMonth) {
    payoutDay = daysInCurrentMonth
  }
  // Payout can't be done before the invoice due date.
  // In this case, it'll be on next month of the due date.
  if (payoutDay > parseInt(invoiceDueDate.format('D'))) {
    return invoiceDueDate.set('date', payoutDay).toDate()
  }
  const nextMonth = invoiceDueDate.add(1, 'month')
  const daysInNextMonth = nextMonth.daysInMonth()
  if (standardPayoutDate >= daysInNextMonth) {
    payoutDay = daysInNextMonth
  } else {
    payoutDay = standardPayoutDate
  }
  return nextMonth.set('date', payoutDay).toDate()
}

export const getRandomBankReference = (lastBankReference) => {
  if (!lastBankReference) {
    return 'AA11111'
  }
  let i = lastBankReference.length
  let result = lastBankReference
  const alphabet = 'abcdefghijklmnopqrstuvwxyz'
  const { length } = alphabet
  while (i >= 0) {
    const last = lastBankReference.charAt(--i)
    let next = ''
    let carry = false
    if (isNaN(last)) {
      const index = alphabet.indexOf(last.toLowerCase())
      if (index === -1) {
        next = last
        carry = true
      } else {
        const isUpperCase = last === last.toUpperCase()
        next = alphabet.charAt((index + 1) % length)
        if (isUpperCase) {
          next = next.toUpperCase()
        }
        carry = index + 1 >= length
        if (carry && i === 0) {
          const added = isUpperCase ? 'A' : 'a'
          result = added + next + result.slice(1)
          break
        }
      }
    } else {
      next = +last + 1
      if (next > 9) {
        next = 0
        carry = true
      }
      if (carry && i === 0) {
        result = `1${next}${result.slice(1)}`
        break
      }
    }
    result = result.slice(0, i) + next + result.slice(i + 1)
    if (!carry) {
      break
    }
  }
  return result
}

export const preparePayoutForApphealth = async (req) => {
  const { body } = req
  const { contractId } = body
  const pipeline = preparePipelineForPayoutAppHealth(contractId)
  const payout = await PayoutCollection.aggregate(pipeline)
  return payout
}

export const preparePayoutData = async (data) => {
  const {
    contract,
    invoice,
    isFinalSettlement,
    partnerSetting,
    payoutData,
    userId
  } = data || {}
  let { dueDate } = data
  let standardPayoutDate = 1
  if (isFinalSettlement) {
    dueDate = await appHelper.getActualDate(partnerSetting, false)
    payoutData.createdAt = await appHelper.getActualDate(partnerSetting, false)
  }
  if (contract && contract.monthlyPayoutDate) {
    standardPayoutDate = contract.monthlyPayoutDate
  } else if (partnerSetting && partnerSetting.standardPayoutDate) {
    ;({ standardPayoutDate } = partnerSetting)
  }
  payoutData.payoutDate = await getPayoutDate(
    standardPayoutDate,
    dueDate,
    partnerSetting
  )
  if (payoutData.amount === 0) {
    payoutData.status = 'completed'
  } else {
    payoutData.status = 'estimated'
  }
  payoutData.createdBy = userId
  if (invoice?.status === 'paid') {
    payoutData.invoicePaid = true
    payoutData.invoicePaidOn = invoice?.lastPaymentDate || new Date()
  }
  return payoutData
}

export const getPayoutCreationDataForFinalSettlement = async (
  contractInfo = {},
  session
) => {
  const payoutData = pick(contractInfo, [
    'partnerId',
    'branchId',
    'agentId',
    'accountId',
    'propertyId'
  ])
  payoutData.tenantId = contractInfo.rentalMeta?.tenantId || ''
  payoutData.contractId = contractInfo._id
  payoutData.estimatedAmount = 0
  payoutData.amount = 0
  payoutData.isFinalSettlement = true
  // Check all invoices are paid or credited then invoicePaid will be true according to this contract
  const foundUnpaidInvoices = await invoiceHelper.getInvoice(
    {
      contractId: payoutData.contractId,
      status: { $nin: ['paid', 'credited'] },
      invoiceType: 'invoice'
    },
    session
  )
  const lastPaidInvoiceInfo = await invoiceHelper.getLastPaidInvoice(
    {
      contractId: payoutData.contractId,
      invoiceType: 'invoice'
    },
    session
  )
  if (
    !foundUnpaidInvoices &&
    lastPaidInvoiceInfo &&
    lastPaidInvoiceInfo.lastPaymentDate
  ) {
    payoutData.invoicePaid = true
    payoutData.invoicePaidOn = lastPaidInvoiceInfo.lastPaymentDate
  }
  return payoutData
}

export const getLandlordInvoicePeriod = async (
  landlordInvoice,
  partnerSetting,
  session
) => {
  let landlordInvoicePeriod = null
  if (!landlordInvoice) {
    return landlordInvoicePeriod
  }
  if (size(landlordInvoice.commissionsIds)) {
    const query = { _id: { $in: landlordInvoice.commissionsIds } }
    const commission = await commissionHelper.getCommission(query, session, [
      'invoice'
    ])
    if (size(commission?.invoice)) {
      const invoice = commission.invoice
      if (invoice && invoice.invoiceStartOn) {
        const invoiceStartOn = await appHelper.getActualDate(
          partnerSetting,
          true,
          invoice.invoiceStartOn
        )
        landlordInvoicePeriod = invoiceStartOn.format('YYYYMM') // Date format must be "YYYYMM". Otherwise we will not compare with another period.
      }
    }
  }
  if (
    !landlordInvoicePeriod &&
    size(landlordInvoice.correctionsIds) &&
    landlordInvoice.createdAt
  ) {
    const createdAt = await appHelper.getActualDate(
      partnerSetting,
      true,
      landlordInvoice.createdAt
    )
    landlordInvoicePeriod = createdAt.format('YYYYMM') // Date format must be "YYYYMM". Otherwise we will not compare with another period.
  }
  return landlordInvoicePeriod
}

export const getPayoutPeriod = async (payout, partnerSetting, session) => {
  let payoutPeriod = ''
  if (!payout) {
    return payoutPeriod
  }
  const invoiceId = payout.invoiceId || ''
  if (invoiceId) {
    let invoice = payout.invoice
    if (!size(invoice)) {
      invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
    }
    if (invoice && invoice.invoiceStartOn) {
      const invoiceStartOn = await appHelper.getActualDate(
        partnerSetting,
        true,
        invoice.invoiceStartOn
      )
      payoutPeriod = invoiceStartOn.format('YYYYMM') // Date format must be "YYYYMM". Otherwise we will not compare with another period.
    }
  }
  return payoutPeriod
}

export const getUnbalancedPayouts = async (
  landlordInvoice,
  partnerSetting,
  session
) => {
  const { contractId, partnerId } = landlordInvoice
  const unbalancedQuery = {
    contractId,
    partnerId,
    status: 'estimated'
  }
  const landlordInvoicePeriod = await getLandlordInvoicePeriod(
    landlordInvoice,
    partnerSetting,
    session
  )
  const timezone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const unbalancedPayouts = await getAggregatedPayouts(
    [
      {
        $match: unbalancedQuery
      },
      {
        $lookup: {
          from: 'invoices',
          localField: 'invoiceId',
          foreignField: '_id',
          as: 'invoiceInfo'
        }
      },
      {
        $unwind: {
          path: '$invoiceInfo',
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $addFields: {
          payoutPeriod: {
            $cond: [
              { $ifNull: ['$invoiceInfo.invoiceStartOn', false] },
              {
                $dateToString: {
                  format: '%Y%m',
                  date: '$invoiceInfo.invoiceStartOn',
                  timezone
                }
              },
              null
            ]
          },
          landlordInvoicePeriod,
          correctionSize: size(landlordInvoice.correctionsIds)
        }
      },
      {
        $match: {
          landlordInvoicePeriod: {
            $ne: null
          },
          payoutPeriod: {
            $ne: null
          },
          $or: [
            { correctionSize: { $gt: 0 } },
            {
              payoutPeriod: {
                $gte: landlordInvoicePeriod
              }
            }
          ]
        }
      },
      {
        $sort: { payoutPeriod: 1 }
      }
    ],
    session
  )
  return unbalancedPayouts
}

export const getBalancedData = async (newMetaData, params) => {
  const { newPayoutId, multiplyLandlord } = params
  let { newBalancedAmount } = params
  newBalancedAmount *= multiplyLandlord
  for (const info of newMetaData) {
    const remainingAmount =
      ((info.total || 0) - (info.totalBalanced || 0)) * multiplyLandlord
    let newAmount = 0
    let isExistsPayoutId = false
    const newPayoutsData = []
    let newPayoutIds = info.payoutsIds || []
    if (newBalancedAmount > 0 && remainingAmount > 0) {
      newAmount =
        newBalancedAmount >= remainingAmount
          ? remainingAmount
          : newBalancedAmount
      newAmount = await appHelper.convertTo2Decimal(
        newAmount * multiplyLandlord
      )
    }
    if (newAmount !== 0) {
      if (size(info.payouts)) {
        for (const newInfo of info.payouts) {
          if (newInfo.payoutId === newPayoutId && !newInfo.isAdjustedBalance) {
            newInfo.amount += newAmount
            isExistsPayoutId = true
          }
          if (newInfo) newPayoutsData.push(newInfo)
        }
        if (!isExistsPayoutId && newAmount) {
          newPayoutsData.push({ payoutId: newPayoutId, amount: newAmount })
        }
      } else {
        newPayoutsData.push({ payoutId: newPayoutId, amount: newAmount })
      }
      newPayoutIds.push(clone(newPayoutId))
      newPayoutIds = uniq(newPayoutIds)
      info.payouts = clone(newPayoutsData)
      info.payoutsIds = clone(newPayoutIds)
      info.totalBalanced = (info.totalBalanced || 0) + (newAmount || 0)
      newBalancedAmount -= newAmount
    }
  }
  return { newMetaData, newBalancedAmount }
}

export const getDistributedBalanceAmount = async (params) => {
  const { invoiceUpdateData, newPayout, multiplyLandlord } = params
  const { commissionsMeta, addonsMeta, invoiceContent } = invoiceUpdateData
  const { isFinalSettlement } = newPayout
  let newTotalBalanced = invoiceUpdateData.totalBalanced || 0
  let newRemainingBalance = invoiceUpdateData.remainingBalance || 0
  const newPayoutId = newPayout.payoutId || ''
  let newBalancedAmount = (newPayout.amount || 0) * multiplyLandlord
  newTotalBalanced = await appHelper.convertTo2Decimal(
    newTotalBalanced + newBalancedAmount || 0
  )
  newRemainingBalance = await appHelper.convertTo2Decimal(
    newRemainingBalance - newBalancedAmount || 0
  )
  let landlordNewUpdateInfo = {
    totalBalanced: newTotalBalanced,
    remainingBalance: newRemainingBalance
  }
  let isBalancedCalculated = false
  const data = { newPayoutId, multiplyLandlord }
  if (size(commissionsMeta)) {
    data.newBalancedAmount = newBalancedAmount
    const newMetaInfo = await getBalancedData(commissionsMeta, data)
    landlordNewUpdateInfo.commissionsMeta = newMetaInfo.newMetaData
    ;({ newBalancedAmount } = newMetaInfo)
    isBalancedCalculated = true
  }
  if (newBalancedAmount !== 0 && size(addonsMeta)) {
    data.newBalancedAmount = newBalancedAmount
    const newMetaInfo = await getBalancedData(addonsMeta, data)
    landlordNewUpdateInfo.addonsMeta = newMetaInfo.newMetaData
    ;({ newBalancedAmount } = newMetaInfo)
    isBalancedCalculated = true
  }
  if (isFinalSettlement && newBalancedAmount !== 0 && size(invoiceContent)) {
    data.newBalancedAmount = newBalancedAmount
    const newMetaInfo = await getBalancedData(invoiceContent, data)
    landlordNewUpdateInfo.invoiceContent = newMetaInfo.newMetaData
    isBalancedCalculated = true
  }
  if (!isBalancedCalculated) {
    landlordNewUpdateInfo = invoiceUpdateData
  }
  return landlordNewUpdateInfo
}

export const getUnbalancedInvoices = async (data, session) => {
  const { payout, partnerSetting, unbalancedInvoiceQuery, isAdjustAll } = data
  unbalancedInvoiceQuery.isFinalSettlement = { $ne: true }
  const payoutPeriod = await getPayoutPeriod(payout, partnerSetting)
  const timezone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const unbalancedInvoices = await invoiceHelper.getAggregatedInvoices(
    [
      {
        $match: unbalancedInvoiceQuery
      },
      {
        $lookup: {
          from: 'commissions',
          localField: 'commissionsIds',
          foreignField: '_id',
          as: 'commissions'
        }
      },
      {
        $addFields: {
          commission: {
            $first: {
              $ifNull: ['$commissions', []]
            }
          }
        }
      },
      {
        $lookup: {
          from: 'invoices',
          localField: 'commission.invoiceId',
          foreignField: '_id',
          as: 'commissionInvoice'
        }
      },
      {
        $unwind: {
          path: '$commissionInvoice',
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $addFields: {
          landlordInvoicePeriod: {
            $switch: {
              branches: [
                {
                  case: {
                    $ifNull: ['$commissionInvoice.invoiceStartOn', false]
                  },
                  then: '$commissionInvoice.invoiceStartOn'
                },
                {
                  case: {
                    $gt: [{ $size: { $ifNull: ['$correctionsIds', []] } }, 0]
                  },
                  then: '$createdAt'
                }
              ],
              default: null
            }
          }
        }
      },
      {
        $addFields: {
          landlordInvoicePeriod: {
            $cond: [
              { $ifNull: ['$landlordInvoicePeriod', false] },
              {
                $dateToString: {
                  format: '%Y%m',
                  date: '$landlordInvoicePeriod',
                  timezone
                }
              },
              null
            ]
          },
          payoutPeriod,
          isAdjustAllForFinalSettlement: isAdjustAll
        }
      },
      {
        $match: {
          landlordInvoicePeriod: {
            $ne: null
          },
          $or: [
            { isAdjustAllForFinalSettlement: true },
            {
              payoutPeriod: {
                $ne: null
              },
              $or: [
                {
                  'correctionsIds.0': {
                    $exists: true
                  }
                },
                {
                  landlordInvoicePeriod: {
                    $lte: payoutPeriod
                  }
                }
              ]
            }
          ]
        }
      },
      {
        $sort: { landlordInvoicePeriod: 1 }
      },
      {
        $addFields: {
          rentInvoiceId: '$commission.invoiceId'
        }
      }
    ],
    session
  )
  return unbalancedInvoices
}

export const getPayoutNewMeta = (payoutNewMeta = [], newMetaInfo = {}) => {
  let isExistsLandlordInvoiceId = false
  const { landlordInvoiceId, type } = newMetaInfo
  const newAmount = newMetaInfo.amount || 0
  if (size(payoutNewMeta)) {
    for (const info of payoutNewMeta) {
      if (type === info.type && landlordInvoiceId === info.landlordInvoiceId) {
        info.amount += newAmount
        isExistsLandlordInvoiceId = true
      }
    }
    if (!isExistsLandlordInvoiceId && newAmount !== 0) {
      payoutNewMeta.push(newMetaInfo)
    }
  } else {
    payoutNewMeta.push(newMetaInfo)
  }
  return payoutNewMeta
}

export const prepareAmountData = (params) => {
  const { balancedAmount, isAdjustAll, remainingBalance } = params || {}
  const absBalancedAmount = Math.abs(clone(balancedAmount))
  const absRemainingBalance = Math.abs(clone(remainingBalance))

  let amount
  if (
    (absBalancedAmount !== 0 && absBalancedAmount >= absRemainingBalance) ||
    (absBalancedAmount === 0 && remainingBalance < 0) ||
    remainingBalance < 0 ||
    isAdjustAll
  ) {
    amount = absRemainingBalance
  } else if (
    (absBalancedAmount !== 0 && absBalancedAmount < absRemainingBalance) ||
    (absBalancedAmount === 0 && remainingBalance > 0)
  ) {
    amount = balancedAmount
  }

  return amount
}

export const getPayoutsForTransaction = async (
  payoutIds,
  partnerId,
  session
) => {
  const query = {
    _id: { $in: payoutIds },
    status: 'completed',
    paymentStatus: 'paid',
    amount: { $gt: 0 },
    partnerId
  }
  const payouts = await getPayouts(query, session)
  return payouts
}

export const isExistsPayoutTransaction = async (payout, session) => {
  const { partnerId, _id, invoiceId, propertyId, amount } = payout
  const query = {
    partnerId,
    payoutId: _id,
    invoiceId,
    propertyId,
    amount,
    type: 'payout'
  }
  const transaction = await transactionHelper.getTransaction(query, session)
  return !!transaction
}

export const prepareTransactionData = async (payout, transactionEvent) => {
  const transactionData = pick(payout, [
    'partnerId',
    'contractId',
    'tenantId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'invoiceId',
    'amount',
    'createdBy'
  ])
  const { partnerId, meta, _id, bookingDate } = payout
  let { invoiceId } = payout
  if (!invoiceId && size(meta)) {
    const landlordInvoiceIds = map(meta, 'landlordInvoiceId')
    if (size(landlordInvoiceIds)) invoiceId = landlordInvoiceIds[0]
  }
  const params = {
    partnerId,
    accountingType: 'payout_to_landlords',
    options: { invoiceId }
  }
  const payoutTransactionData =
    await transactionHelper.getAccountingDataForTransaction(params)
  if (!size(payoutTransactionData)) {
    return {}
  }
  transactionData.payoutId = _id
  transactionData.type = 'payout'
  transactionData.createdAt = bookingDate
  transactionData.period = transactionData.createdAt
    ? await transactionHelper.getFormattedTransactionPeriod(
        transactionData.createdAt,
        partnerId
      )
    : ''
  transactionData.transactionEvent = transactionEvent
  return extend(transactionData, payoutTransactionData)
}

const getRefundPayments = async (pendingPaymentIds) => {
  const pipeline = [
    {
      $match: { _id: { $in: pendingPaymentIds } }
    },
    {
      $lookup: {
        from: 'invoices',
        as: 'invoiceInfo',
        let: { invoiceId: '$invoiceId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [{ $eq: ['$_id', '$$invoiceId'] }]
              }
            }
          },
          {
            $project: {
              invoiceSerialId: 1
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'listings',
        as: 'propertyInfo',
        let: { propertyId: '$propertyId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [{ $eq: ['$_id', '$$propertyId'] }]
              }
            }
          },
          {
            $project: {
              location: 1
            }
          }
        ]
      }
    },
    {
      $unwind: { path: '$invoiceInfo', preserveNullAndEmptyArrays: true }
    },
    {
      $unwind: { path: '$propertyInfo', preserveNullAndEmptyArrays: true }
    },
    {
      $project: {
        invoiceId: '$invoiceInfo.invoiceSerialId',
        propertyName: {
          $reduce: {
            input: {
              $filter: {
                input: [
                  '$propertyInfo.location.name',
                  '$propertyInfo.location.postalCode',
                  '$propertyInfo.location.city',
                  '$propertyInfo.location.country'
                ],
                as: 'value',
                cond: {
                  $and: [{ $ne: ['$$value', null] }, { $ne: ['$$value', ''] }]
                }
              }
            },
            initialValue: '',
            in: {
              $cond: {
                if: { $eq: ['$$value', ''] },
                then: { $concat: ['$$value', '$$this'] },
                else: { $concat: ['$$value', ', ', '$$this'] }
              }
            }
          }
        },
        amount: '$amount'
      }
    }
  ]
  const payments = await invoicePaymentHelper.getInvoicePaymentByAggregation(
    pipeline
  )
  if (!size(payments)) {
    throw new CustomError(405, `No payments found for creating esigning doc`)
  }
  let totalAmount = 0
  for (const payment of payments) {
    totalAmount += payment.amount
  }
  return { totalAmount, refunds: payments }
}

const getEsigningPdfContent = async (pendingPaymentIds) => {
  const template = await notificationTemplateHelper.getNotificationTemplate({
    isEsignPdfForPaymentsApproval: true,
    category: 'pending_payments_esign_pdf'
  })
  if (!size(template)) {
    throw new CustomError(405, 'Template not found')
  }
  const baseTemplate =
    size(template.content) && size(template.content['no'])
      ? template.content['no']
      : ''
  const { totalAmount, refunds } = await getRefundPayments(pendingPaymentIds)
  const esigningPdfContent = appHelper.SSR(baseTemplate, {
    total_amount: totalAmount,
    refunds
  })
  return esigningPdfContent
}

const prepareEsiningDocForPayments = async (body) => {
  const { userId, pendingPaymentIds } = body
  const esigningPdfContent = await getEsigningPdfContent(pendingPaymentIds)
  return { directRemittanceApprovalUserIds: [userId], esigningPdfContent }
}

//For Payment lambda #10482
export const prepareEsigningDoc = async (body) => {
  const { partnerId, userId, pendingPayoutIds, pendingPaymentIds } = body
  if (size(pendingPaymentIds) && size(pendingPayoutIds)) {
    throw new CustomError(405, 'Only one type doc preparation is allowed')
  }
  if (size(pendingPaymentIds)) {
    return await prepareEsiningDocForPayments(body)
  }
  let totalPayouts = 0

  const payouts = await PayoutCollection.aggregate([
    {
      $match: {
        _id: { $in: pendingPayoutIds }
      }
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        amount: 1,
        payoutId: '$serialId',
        property: 1
      }
    },
    {
      $addFields: {
        propertyName: {
          $reduce: {
            input: {
              $filter: {
                input: [
                  '$property.location.name',
                  '$property.location.postalCode',
                  '$property.location.city',
                  '$property.location.country'
                ],
                as: 'value',
                cond: {
                  $and: [{ $ne: ['$$value', null] }, { $ne: ['$$value', ''] }]
                }
              }
            },
            initialValue: '',
            in: {
              $cond: {
                if: { $eq: ['$$value', ''] },
                then: { $concat: ['$$value', '$$this'] },
                else: { $concat: ['$$value', ', ', '$$this'] }
              }
            }
          }
        }
      }
    }
  ])
  for (const payout of payouts) {
    totalPayouts += payout.amount || 0
  }

  const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const directRemittanceApproval =
    size(partnerSettings) && size(partnerSettings.directRemittanceApproval)
      ? partnerSettings.directRemittanceApproval
      : {}

  const template = await notificationTemplateHelper.getNotificationTemplate({
    isEsignPdfForPayoutsApproval: true,
    category: 'pending_payouts_esign_pdf'
  })
  const baseTemplate =
    size(template) && size(template.content) && size(template.content['no'])
      ? template.content['no']
      : ''
  const esigningPdfContent = appHelper.SSR(baseTemplate, {
    total_payouts: totalPayouts,
    payouts
  })

  let directRemittanceApprovalUserIds = []
  if (
    size(directRemittanceApproval) &&
    directRemittanceApproval.isEnableMultipleSigning
  ) {
    directRemittanceApprovalUserIds = size(directRemittanceApproval.persons)
      ? directRemittanceApproval.persons
      : []
  } else {
    directRemittanceApprovalUserIds = [userId]
  }

  return { directRemittanceApprovalUserIds, esigningPdfContent }
}

export const preparePayoutsQuery = async (params) => {
  let query = {}

  if (size(params)) {
    const partnerId = params.partnerId
    query.partnerId = partnerId

    //Set status filters in query
    const payoutStatus = compact(params.status)
    if (size(payoutStatus)) query.status = { $in: payoutStatus }

    //Set payment status filters in query
    if (indexOf(payoutStatus, 'completed') !== -1) {
      const paymentStatus = compact(params.paymentStatus)

      if (size(paymentStatus)) query.paymentStatus = { $in: paymentStatus }
    }

    if (params.hasPaused === 'yes') query.holdPayout = true
    else if (params.hasPaused === 'no') query.holdPayout = { $ne: true }

    //Set branch filters in query
    if (params.branchId) {
      query.branchId = params.branchId
    }

    //Set agent filters in query
    if (params.agentId) query.agentId = params.agentId

    //Set account filters in query
    if (params.accountId) query.accountId = params.accountId

    if (params.context && params.context === 'landlordDashboard') {
      const accountIds =
        uniq(
          map(
            await accountHelper.getAccounts({ personId: params.userId }),
            '_id'
          )
        ) || []

      if (size(accountIds)) query.accountId = { $in: accountIds }
    }

    //Set property filters in query
    if (params.propertyId) query.propertyId = params.propertyId

    //Set tenant filters in query
    if (params.tenantId) {
      query.$or = [
        { tenantId: params.tenantId },
        { tenants: { $elemMatch: { tenantId: params.tenantId } } }
      ]
    }

    //Set amount filters in query.
    if (params.searchKeyword) {
      if (!isNaN(parseInt(params.searchKeyword))) {
        query = {
          partnerId,
          $or: [
            { amount: parseInt(params.searchKeyword) },
            { serialId: parseInt(params.searchKeyword) }
          ]
        }
      } else
        query = {
          partnerId,
          bankReferenceId: new RegExp(params.searchKeyword, 'i')
        }
    }

    //Set created at date range filters in query
    if (
      params.createdAtDateRange &&
      params.createdAtDateRange.startDate &&
      params.createdAtDateRange.endDate
    ) {
      query.createdAt = {
        $gte: params.createdAtDateRange.startDate,
        $lte: params.createdAtDateRange.endDate
      }
    }

    //Set dateRange filters in query
    if (
      params.payoutDate &&
      params.payoutDate.startDate &&
      params.payoutDate.endDate
    ) {
      query.payoutDate = {
        $gte: params.payoutDate.startDate,
        $lte: params.payoutDate.endDate
      }
    }

    //Set payout date range in query for export data
    if (
      params.download &&
      params.payoutDate &&
      params.payoutDate.startDate_string &&
      params.payoutDate.endDate_string
    ) {
      const payoutDateRange = await appHelper.getDateRangeFromStringDate(
        partnerId,
        params.payoutDate
      )

      if (
        payoutDateRange &&
        payoutDateRange.startDate &&
        payoutDateRange.endDate
      ) {
        query.payoutDate = {
          $gte: payoutDateRange.startDate,
          $lte: payoutDateRange.endDate
        }
      }
    }

    if (
      params.sentToNETSOn &&
      params.sentToNETSOn.startDate &&
      params.sentToNETSOn.endDate
    ) {
      query.sentToNETSOn = {
        $gte: params.sentToNETSOn.startDate,
        $lte: params.sentToNETSOn.endDate
      }
    }

    //Set payout sentToNETSOn range in query for export data
    if (
      params.download &&
      params.sentToNETSOn &&
      params.sentToNETSOn.startDate_string &&
      params.sentToNETSOn.endDate_string
    ) {
      const sentToNETSOnRange = await appHelper.getDateRangeFromStringDate(
        partnerId,
        params.sentToNETSOn
      )

      if (
        sentToNETSOnRange &&
        sentToNETSOnRange.startDate &&
        sentToNETSOnRange.endDate
      ) {
        query.sentToNETSOn = {
          $gte: sentToNETSOnRange.startDate,
          $lte: sentToNETSOnRange.endDate
        }
      }
    }

    if (
      params.bookingDate &&
      params.bookingDate.startDate &&
      params.bookingDate.endDate
    ) {
      query.bookingDate = {
        $gte: params.bookingDate.startDate,
        $lte: params.bookingDate.endDate
      }
    }

    //Set payout sentToNETSOn range in query for export data
    if (
      params.download &&
      params.bookingDate &&
      params.bookingDate.startDate_string &&
      params.bookingDate.endDate_string
    ) {
      const bookingDateRange = await appHelper.getDateRangeFromStringDate(
        partnerId,
        params.bookingDate
      )

      if (
        bookingDateRange &&
        bookingDateRange.startDate &&
        bookingDateRange.endDate
      ) {
        query.bookingDate = {
          $gte: bookingDateRange.startDate,
          $lte: bookingDateRange.endDate
        }
      }
    }

    if (params.contractId) query.contractId = params.contractId

    if (params.contractId && params.leaseSerial) {
      const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
        params.contractId,
        params.leaseSerial
      )
      query.invoiceId = { $in: invoiceIds }
    }

    if (params.hasPaused === 'yes') query.holdPayout = true
    else if (params.hasPaused === 'no') query.holdPayout = { $ne: true }
  }

  return query
}

export const getPayoutForExcelManager = async (queryData) => {
  const { query, options, dateFormat, timeZone } = queryData
  const { sort, skip, limit } = options

  const pipeline = [
    {
      $match: query
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
        foreignField: '_id',
        as: 'tenant'
      }
    },
    {
      $unwind: {
        path: '$tenant',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'invoices',
        localField: 'invoiceId',
        foreignField: '_id',
        as: 'invoice'
      }
    },
    {
      $unwind: {
        path: '$invoice',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        as: 'account'
      }
    },
    {
      $unwind: {
        path: '$account',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        payoutId: '$serialId',
        invoiceId: '$invoice.invoiceSerialId',
        objectId: '$property.serial',
        property: {
          $concat: [
            { $ifNull: ['$property.location.name', ''] },
            {
              $cond: [
                { $ifNull: ['$property.location.postalCode', false] },
                { $concat: [', ', '$property.location.postalCode'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.city', false] },
                { $concat: [', ', '$property.location.city'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.country', false] },
                { $concat: [', ', '$property.location.country'] },
                ''
              ]
            }
          ]
        },
        apartmentId: '$property.apartmentId',
        tenantId: '$tenant.serial',
        tenant: '$tenant.name',
        status: 1,
        paidDate: {
          $dateToString: {
            format: dateFormat,
            date: '$bookingDate',
            timezone: timeZone
          }
        },
        amount: {
          $ifNull: ['$amount', 0]
        },
        estimated: {
          $dateToString: {
            format: dateFormat,
            date: '$payoutDate',
            timezone: timeZone
          }
        },
        account: '$account.name',
        sentToNETSOn: {
          $dateToString: {
            format: dateFormat,
            date: '$sentToNETSOn',
            timezone: timeZone
          }
        }
      }
    }
  ]
  const payoutData = await PayoutCollection.aggregate(pipeline)
  return payoutData || []
}

export const payoutDataForExcelCreator = async (params, options) => {
  const { partnerId = {}, userId = {} } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const userInfo = await userHelper.getAnUser({ _id: params.userId })
  const userLanguage = userInfo.getLanguage()
  const payoutsQuery = await preparePayoutsQuery(params)
  const dataCount = await countPayouts(payoutsQuery)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'

  const queryData = {
    query: payoutsQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage
  }
  const payouts = await getPayoutForExcelManager(queryData)
  if (size(payouts)) {
    for (const payout of payouts) {
      payout.status = appHelper.translateToUserLng(
        'payouts.status.' + payout.status,
        userLanguage
      )
    }
  }
  return {
    data: payouts,
    total: dataCount
  }
}

export const queryForPayoutExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_payouts') {
    const payoutData = await payoutDataForExcelCreator(queueInfo.params, {
      skip,
      limit,
      sort
    })
    return payoutData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const percentCalculation = (commissionAmount, taxPercentage) => {
  if (taxPercentage) {
    const result =
      (parseFloat(commissionAmount) * parseFloat(taxPercentage)) / 100
    return result
  }
  return 0
}

export const prepareLandlordReportQuery = async (params, userId) => {
  const {
    partnerId = '',
    accountId = '',
    context = '',
    dateRange = null
  } = params
  const query = { partnerId, status: 'completed' }

  //Set accountId for landlord Dashboard reports download
  if (context && context === 'landlordReports' && !accountId) {
    const accountIds = await AccountCollection.distinct('_id', {
      personId: userId,
      partnerId
    })
    query.accountId = { $in: accountIds }
  } else if (accountId) {
    query.accountId = accountId
  }

  //Set download date range in query
  if (size(dateRange)) {
    const startDate = (
      await appHelper.getActualDate(partnerId, true, dateRange.startDate_string)
    )
      .startOf('day')
      .toDate()
    const endDate = (
      await appHelper.getActualDate(partnerId, true, dateRange.endDate_string)
    )
      .endOf('day')
      .toDate()

    query.bookingDate = { $gte: startDate, $lte: endDate }
  }

  return query
}

const getInvoicePipelineForLandlordReport = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      as: 'invoice'
    }
  },
  {
    $unwind: {
      path: '$invoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getPropertyPipelineForLandlordReport = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'invoice.propertyId',
      foreignField: '_id',
      as: 'property'
    }
  },
  {
    $unwind: {
      path: '$property',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getInvoicePaymentPipelineForLandlordReport = () => [
  {
    $lookup: {
      from: 'invoice-payments',
      let: {
        partnerId: '$invoice.partnerId',
        propertyId: '$invoice.propertyId',
        invoiceId: '$invoiceId'
      },
      localField: 'invoice.contractId',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$partnerId', '$$partnerId'] },
                { $eq: ['$propertyId', '$$propertyId'] },
                { $eq: ['$type', 'payment'] }
                // { $eq: ['$invoices.invoiceId', '$$invoiceId'] }
              ]
            }
          }
        },
        {
          $unwind: {
            path: '$invoices',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $match: {
            $expr: {
              $eq: ['$invoices.invoiceId', '$$invoiceId']
            }
          }
        },
        {
          $group: {
            _id: 'null',
            totalPayment: {
              $sum: '$invoices.amount'
            }
          }
        }
      ],
      as: 'payment'
    }
  },
  {
    $unwind: {
      path: '$payment',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getLandlordInvoicePipelineForLandlordReport = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'meta.landlordInvoiceId',
      foreignField: '_id',
      as: 'landlordInvoice'
    }
  },
  {
    $unwind: {
      path: '$landlordInvoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getInvoiceAndCommissionDetailsPipeline = () => [
  {
    $addFields: {
      addonMetaPayouts: {
        $filter: {
          input: '$landlordInvoice.addonsMeta',
          as: 'addonMeta',
          cond: {
            $and: [
              { $ifNull: ['$$addonMeta.payouts', false] },
              { $gt: [{ $size: '$$addonMeta.payouts' }, 0] },
              { $eq: ['$$addonMeta.type', 'addon'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$addonMetaPayouts',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      payouts: {
        $filter: {
          input: '$addonMetaPayouts.payouts',
          as: 'payout',
          cond: {
            $eq: ['$$payout.payoutId', '$_id']
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$payouts',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      assignmentTaxPercentage: {
        $add: [
          {
            $divide: [{ $ifNull: ['$addonMetaPayouts.taxPercentage', 0] }, 100]
          },
          1
        ]
      }
    }
  },
  {
    $addFields: {
      assignmentAddonExclVat: {
        $divide: ['$payouts.amount', '$assignmentTaxPercentage']
      }
    }
  },
  {
    $addFields: {
      assignmentAddonVat: {
        $subtract: [
          { $ifNull: ['$payouts.amount', 0] },
          '$assignmentAddonExclVat'
        ]
      }
    }
  },
  {
    $group: {
      _id: { _id: '$_id', landlordInvoiceId: '$meta.landlordInvoiceId' },
      mainId: { $first: '$_id' },
      landlordInvoiceId: { $first: '$meta.landlordInvoiceId' },
      meta: { $first: '$meta' },
      totalAssignmentAddon: {
        $sum: '$payouts.amount'
      },
      assignmentAddonVat: {
        $sum: '$assignmentAddonVat'
      },
      assignmentAddonExclVat: {
        $sum: '$assignmentAddonExclVat'
      },
      commissionsMeta: {
        $first: '$landlordInvoice.commissionsMeta'
      },
      invoice: {
        $first: '$invoice'
      },
      property: {
        $first: '$property'
      },
      payoutDate: {
        $first: '$payoutDate'
      },
      payment: {
        $first: '$payment'
      },
      amount: {
        $first: '$amount'
      },
      createdAt: {
        $first: '$createdAt'
      }
    }
  },
  {
    $unwind: {
      path: '$commissionsMeta',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      commissionPayouts: {
        $filter: {
          input: '$commissionsMeta.payouts',
          as: 'payout',
          cond: {
            $eq: ['$$payout.payoutId', '$mainId']
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$commissionPayouts',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      commissionTaxPercentage: {
        $add: [
          {
            $divide: [{ $ifNull: ['$commissionsMeta.taxPercentage', 0] }, 100]
          },
          1
        ]
      }
    }
  },
  {
    $addFields: {
      commissionExclVat: {
        $divide: ['$commissionPayouts.amount', '$commissionTaxPercentage']
      }
    }
  },
  {
    $addFields: {
      commissionVat: {
        $subtract: [
          { $ifNull: ['$commissionPayouts.amount', 0] },
          '$commissionExclVat'
        ]
      }
    }
  },
  {
    $group: {
      _id: { mainId: '$mainId', landlordInvoiceId: '$landlordInvoiceId' },
      mainId: { $first: '$mainId' },
      meta: { $first: '$meta' },
      totalAssignmentAddon: {
        $first: '$totalAssignmentAddon'
      },
      assignmentAddonVat: {
        $first: '$assignmentAddonVat'
      },
      assignmentAddonExclVat: {
        $first: '$assignmentAddonExclVat'
      },
      managementVat: {
        $sum: {
          $cond: [
            { $eq: ['$commissionsMeta.type', 'rental_management_contract'] },
            '$commissionVat',
            0
          ]
        }
      },
      managementExclVat: {
        $sum: {
          $cond: [
            { $eq: ['$commissionsMeta.type', 'rental_management_contract'] },
            '$commissionExclVat',
            0
          ]
        }
      },
      totalManagement: {
        $sum: {
          $cond: [
            { $eq: ['$commissionsMeta.type', 'rental_management_contract'] },
            { $ifNull: ['$commissionPayouts.amount', 0] },
            0
          ]
        }
      },
      assignmentAddonVatCommission: {
        $sum: {
          $cond: [
            { $ne: ['$commissionsMeta.type', 'rental_management_contract'] },
            '$commissionVat',
            0
          ]
        }
      },
      assignmentAddonExclVatCommission: {
        $sum: {
          $cond: [
            { $ne: ['$commissionsMeta.type', 'rental_management_contract'] },
            '$commissionExclVat',
            0
          ]
        }
      },
      totalAssignmentAddonCommission: {
        $sum: {
          $cond: [
            { $ne: ['$commissionsMeta.type', 'rental_management_contract'] },
            { $ifNull: ['$commissionPayouts.amount', 0] },
            0
          ]
        }
      },
      invoice: {
        $first: '$invoice'
      },
      property: {
        $first: '$property'
      },
      payoutDate: {
        $first: '$payoutDate'
      },
      payment: {
        $first: '$payment'
      },
      amount: {
        $first: '$amount'
      },
      createdAt: {
        $first: '$createdAt'
      }
    }
  }
]

const getTenantPipelineForLandlordReport = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'invoice.tenantId',
      foreignField: '_id',
      as: 'tenant'
    }
  },
  {
    $unwind: {
      path: '$tenant',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getPayoutDateMonthYearPipeline = (timezone) => [
  {
    $addFields: {
      month: {
        $let: {
          vars: {
            monthsInString: [
              '',
              'Jan',
              'Feb',
              'Mar',
              'Apr',
              'May',
              'Jun',
              'Jul',
              'Aug',
              'Sep',
              'Oct',
              'Nov',
              'Dec'
            ]
          },
          in: {
            $arrayElemAt: [
              '$$monthsInString',
              {
                $toInt: {
                  $dateToString: {
                    date: '$payoutDate',
                    format: '%m',
                    timezone,
                    onNull: 0
                  }
                }
              }
            ]
          }
        }
      },
      year: {
        $substr: [
          {
            $dateToString: {
              date: '$payoutDate',
              format: '%Y',
              timezone,
              onNull: ''
            }
          },
          2,
          -1
        ]
      }
    }
  }
]

const getFinalProjectPipelineForLandlordReport = () => [
  {
    $project: {
      propertyAddress: '$property.location.name',
      propertyApartmentId: '$property.apartmentId',
      tenantName: '$tenant.name',
      periodOfPayout: {
        $cond: [
          { $ifNull: ['$payoutDate', false] },
          { $concat: ['$month', '-', '$year'] },
          ''
        ]
      },
      payments: '$payment.totalPayment',
      totalPayout: '$amount',
      monthlyRentAmount: 1,
      invoicedExclVat: 1,
      invoicedVat: 1,
      totalInvoiced: 1,
      outstanding: {
        $subtract: ['$totalInvoiced', { $ifNull: ['$payment.totalPayment', 0] }]
      },
      assignmentAddonExclVat: 1,
      assignmentAddonVat: 1,
      totalAssignmentAddon: 1,
      managementExclVat: 1,
      managementVat: 1,
      totalManagement: 1,
      unpaidCorrectionsAndCommissions: 1,
      unpaidEarlierPayout: 1,
      movedToNextPayout: 1,
      createdAt: 1
    }
  }
]

const getMetaTypePipelineForLandlordReport = () => [
  {
    $group: {
      _id: '$mainId',
      invoiceCreditedAmount: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'credit_rent_invoice'] },
            { $ifNull: ['$meta.amount', 0] },
            0
          ]
        }
      },
      unpaidEarlierPayout: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'unpaid_earlier_payout'] },
            { $ifNull: ['$meta.amount', 0] },
            0
          ]
        }
      },
      unpaidCorrectionsAndCommissions: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'unpaid_expenses_and_commissions'] },
            { $ifNull: ['$meta.amount', 0] },
            0
          ]
        }
      },
      movedToNextPayout: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'moved_to_next_payout'] },
            { $ifNull: ['$meta.amount', 0] },
            0
          ]
        }
      },
      totalAssignmentAddon: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'landlord_invoice'] },
            {
              $add: ['$totalAssignmentAddon', '$totalAssignmentAddonCommission']
            },
            0
          ]
        }
      },
      assignmentAddonVat: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'landlord_invoice'] },
            {
              $add: ['$assignmentAddonVat', '$assignmentAddonVatCommission']
            },
            0
          ]
        }
      },
      assignmentAddonExclVat: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'landlord_invoice'] },
            {
              $add: [
                '$assignmentAddonExclVat',
                '$assignmentAddonExclVatCommission'
              ]
            },
            0
          ]
        }
      },
      managementVat: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'landlord_invoice'] },
            '$managementVat',
            0
          ]
        }
      },
      managementExclVat: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'landlord_invoice'] },
            '$managementExclVat',
            0
          ]
        }
      },
      totalManagement: {
        $sum: {
          $cond: [
            { $eq: ['$meta.type', 'landlord_invoice'] },
            '$totalManagement',
            0
          ]
        }
      },
      invoice: {
        $first: '$invoice'
      },
      property: {
        $first: '$property'
      },
      payoutDate: {
        $first: '$payoutDate'
      },
      payment: {
        $first: '$payment'
      },
      amount: {
        $first: '$amount'
      },
      createdAt: {
        $first: '$createdAt'
      }
    }
  }
]

const getLandlordReportDataForExcelCreator = async (paramsData) => {
  const { query, options, timezone } = paramsData
  const { skip, limit } = options
  const landlordReport = await PayoutCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: {
        createdAt: 1
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getInvoicePipelineForLandlordReport(),
    ...getPropertyPipelineForLandlordReport(),
    ...getInvoicePaymentPipelineForLandlordReport(),
    {
      $unwind: {
        path: '$meta',
        preserveNullAndEmptyArrays: true
      }
    },
    ...getLandlordInvoicePipelineForLandlordReport(),
    // Start of landlord invoice calculation
    ...getInvoiceAndCommissionDetailsPipeline(),
    // End of landlord invoice calculation
    ...getMetaTypePipelineForLandlordReport(),
    {
      $addFields: {
        totalInvoiced: {
          $cond: [
            { $eq: ['$invoice.invoiceType', 'credit_note'] },
            {
              $add: [
                { $multiply: ['$invoice.payoutableAmount', -1] },
                '$invoiceCreditedAmount'
              ]
            },
            {
              $add: ['$invoice.payoutableAmount', '$invoiceCreditedAmount']
            }
          ]
        }
      }
    },
    {
      $addFields: {
        monthlyRentContent: {
          $first: {
            $filter: {
              input: { $ifNull: ['$invoice.invoiceContent', []] },
              as: 'content',
              cond: {
                $eq: ['$$content.type', 'monthly_rent']
              }
            }
          }
        }
      }
    },
    {
      $addFields: {
        monthlyRentAmount: {
          $ifNull: ['$monthlyRentContent.total', 0]
        },
        taxPercentage: {
          $add: [
            {
              $divide: [
                { $ifNull: ['$monthlyRentContent.taxPercentage', 0] },
                100
              ]
            },
            1
          ]
        }
      }
    },
    {
      $addFields: {
        invoicedExclVat: {
          $divide: ['$totalInvoiced', '$taxPercentage']
        }
      }
    },
    {
      $addFields: {
        invoicedVat: {
          $subtract: ['$totalInvoiced', '$invoicedExclVat']
        }
      }
    },
    ...getTenantPipelineForLandlordReport(),
    ...getPayoutDateMonthYearPipeline(timezone),
    ...getFinalProjectPipelineForLandlordReport(),
    {
      $sort: {
        createdAt: 1
      }
    }
  ])
  return landlordReport
}

export const landlordDataForExcelCreator = async (params, options) => {
  const { partnerId = '', userId = '' } = params
  appHelper.checkRequiredFields(['partnerId', 'userId'], params)
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const isPartnerLandlord = await appPermission.isPartnerLandlord(
    userId,
    partnerId
  )
  const isPartnerAdmin = await appPermission.isPartnerAdmin(userId, partnerId)
  const isPartnerAgent = await appPermission.isPartnerAgent(userId, partnerId)
  const isPartnerAccounting = await appPermission.isPartnerAccounting(
    userId,
    partnerId
  )

  if (
    isPartnerLandlord ||
    isPartnerAdmin ||
    isPartnerAgent ||
    isPartnerAccounting
  ) {
    const landlordPayoutQuery = await prepareLandlordReportQuery(params, userId)
    const dataCount = await countPayouts(landlordPayoutQuery)
    const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })
    const timezone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
    const paramsData = {
      query: landlordPayoutQuery,
      options,
      timezone
    }
    const landlordPayouts = await getLandlordReportDataForExcelCreator(
      paramsData
    )
    return {
      data: landlordPayouts,
      total: dataCount
    }
  } else {
    throw new CustomError(403, 'Invalid role permission')
  }
}

export const queryForLandlordReportExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.checkRequiredFields(['queueId'], query)
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_landlord_reports') {
    const payoutData = await landlordDataForExcelCreator(
      queueInfo.params,
      options
    )
    return payoutData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const prepareQueryDataForPayoutsQuery = async (query) => {
  const {
    accountId,
    agentId,
    amount,
    bankReferenceId,
    bookingDateRange,
    branchId,
    contractId,
    createdAtDateRange,
    hasPaused,
    leaseSerial,
    partnerId,
    paymentStatus,
    payoutDateRange,
    propertyId,
    sentToNETSOnDateRange,
    serialId,
    status,
    searchKeyword = '',
    tenantId
  } = query
  let preparedQuery = {}
  if (partnerId) preparedQuery.partnerId = partnerId
  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    preparedQuery.createdAt = {
      $gte: moment(startDate).toDate(),
      $lte: moment(endDate).toDate()
    }
  }
  if (size(status)) {
    preparedQuery.status = {
      $in: status
    }
  }
  if (paymentStatus) preparedQuery.paymentStatus = paymentStatus
  if (size(bookingDateRange)) {
    const { startDate, endDate } = bookingDateRange
    preparedQuery.bookingDate = {
      $gte: moment(startDate).toDate(),
      $lte: moment(endDate).toDate()
    }
  }
  if (size(sentToNETSOnDateRange)) {
    const { startDate, endDate } = sentToNETSOnDateRange
    preparedQuery.sentToNETSOn = {
      $gte: moment(startDate).toDate(),
      $lte: moment(endDate).toDate()
    }
  }
  if (size(payoutDateRange)) {
    const { startDate, endDate } = payoutDateRange
    preparedQuery.payoutDate = {
      $gte: moment(startDate).toDate(),
      $lte: moment(endDate).toDate()
    }
  }
  if (branchId) preparedQuery.branchId = branchId
  if (agentId) preparedQuery.agentId = agentId
  if (accountId) preparedQuery.accountId = accountId
  if (propertyId) preparedQuery.propertyId = propertyId
  if (tenantId) {
    preparedQuery.$or = [{ tenantId }, { 'tenants.tenantId': tenantId }]
  }
  if (bankReferenceId) preparedQuery.bankReferenceId = bankReferenceId

  if (hasPaused === 'yes') preparedQuery.holdPayout = true
  else if (hasPaused === 'no') preparedQuery.holdPayout = { $ne: true }

  if (size(searchKeyword)) {
    if (!isNaN(parseInt(searchKeyword))) {
      preparedQuery = {
        partnerId,
        $or: [
          { amount: parseInt(searchKeyword) },
          { serialId: parseInt(searchKeyword) }
        ]
      }
    } else
      preparedQuery = {
        partnerId,
        bankReferenceId: new RegExp(searchKeyword, 'i')
      }
  }
  if (query.hasOwnProperty('serialId')) preparedQuery.serialId = serialId
  if (query.hasOwnProperty('amount')) preparedQuery.amount = amount
  // For lease filter
  if (contractId) preparedQuery.contractId = contractId

  if (contractId && leaseSerial) {
    const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
      contractId,
      leaseSerial
    )
    preparedQuery.invoiceId = { $in: invoiceIds }
  }
  return preparedQuery
}

export const queryForGetPayouts = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  const { options, query } = body
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)
  const { requestFrom = '', propertyId = '' } = query

  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }

  body.query = await prepareQueryDataForPayoutsQuery(query)
  const payouts = await getPayoutForQuery(body)

  const filteredDocuments = await countPayouts(body.query)
  const totalDocuments = await countPayouts(totalDocumentsQuery)

  const esignInfo = await partnerPayoutHelper.getAllPayoutsEsignInfo(
    partnerId,
    propertyId
  )

  return {
    data: payouts,
    metaData: { filteredDocuments, totalDocuments },
    actions: { esignInfo }
  }
}

export const getFailedPayoutInfoForDashboard = async (partnerType = '') => {
  const pipeline = []
  const match = { $match: { status: 'failed' } }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: {
      _id: null,
      countedFailedPayouts: { $sum: 1 }
    }
  }
  pipeline.push(group)
  const [listingInfo] = await PayoutCollection.aggregate(pipeline)
  return listingInfo
}

const getPayoutsSummary = async (query) => {
  const [summary] = await PayoutCollection.aggregate([
    {
      $match: query
    },
    {
      $facet: {
        total: [
          {
            $group: {
              _id: null,
              totalPaused: {
                $sum: { $cond: [{ $eq: ['$holdPayout', true] }, '$amount', 0] }
              },
              totalAmount: { $sum: '$amount' }
            }
          }
        ],
        statusSummary: [
          {
            $group: {
              _id: '$status',
              amount: { $sum: '$amount' }
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$total',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        totalAmount: '$total.totalAmount',
        totalPaused: '$total.totalPaused',
        statusSummary: 1
      }
    }
  ])
  return summary
}

export const queryPayoutsSummary = async (req) => {
  const { body, user } = req
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  appHelper.checkRequiredFields(['partnerId'], body)
  const preparedQuery = await prepareQueryDataForPayoutsQuery(body)
  return await getPayoutsSummary(preparedQuery)
}

export const getPayoutIdsForLegacyTransaction = async (partnerId) => {
  const payouts = await PayoutCollection.aggregate([
    {
      $match: {
        status: 'completed',
        paymentStatus: 'paid',
        amount: { $gt: 0 },
        partnerId
      }
    },
    {
      $group: {
        _id: null,
        payoutIds: { $addToSet: '$_id' }
      }
    }
  ])
  const [payoutInfo = {}] = payouts || []
  const { payoutIds = [] } = payoutInfo
  return payoutIds
}

const getPayoutSignersInfo = async (partnerId, sendToUserIds) => {
  const signers = []
  const v1PartnerUrl = await appHelper.getPartnerURL(partnerId, true)
  const signersMeta = { ui: { language: 'en' } }
  // const userRedirectUrl = redirectUrl + '/esigning-success'
  const v1RedirectUrl = v1PartnerUrl + '/esigning-success'

  console.log('Checking partnerId: ', partnerId)
  const v2SubDomain = await appHelper.getPartnerURL(partnerId)
  console.log('Checking v2SubDomain: ', v2SubDomain)
  const v2_url = `${v2SubDomain}/esigning-success`
  console.log('Checking v2_url: ', v2_url)
  const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${v1RedirectUrl}`
  console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
  const userRedirectUrl = appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`

  signersMeta['signatureType'] = {
    signatureMethods: ['NO_BANKID'],
    mechanism: 'pkisignature'
  }

  each(sendToUserIds, (userId) => {
    const signersMetaForUser = clone(signersMeta)
    signersMetaForUser['redirectSettings'] = {
      redirectMode: 'redirect',
      success: userRedirectUrl,
      cancel: userRedirectUrl + '&signingStatus=cancel',
      error: userRedirectUrl + '&signingStatus=error'
    }

    signersMetaForUser['externalSignerId'] = userId
    signersMetaForUser['tags'] = ['user']

    signers.push(signersMetaForUser)
  })

  return signers
}

const updateDataByFileType = async (dataForIdfy, fileInfo) => {
  const {
    _id,
    directRemittanceApprovalUserIds,
    partnerId,
    partnerPayoutId,
    type
  } = fileInfo
  const userLang = await appHelper.getUserLanguageByPartnerId(partnerId)
  if (type === 'payouts_approval_esigning_pdf') {
    dataForIdfy.title = appHelper.translateToUserLng(
      'payouts.pending_payouts_approval_esigning',
      userLang
    )
    dataForIdfy.description = appHelper.translateToUserLng(
      'payouts.pending_payouts_approval_to_be_signed',
      userLang
    )
    dataForIdfy.externalId = `payoutsApproval-${partnerPayoutId}-${_id}`
    dataForIdfy.dataToSign = {
      fileName: 'payouts-approve.pdf'
    }
    dataForIdfy.advanced = {
      tags: ['pendingPayoutApproval'],
      requiredSignatures: size(directRemittanceApprovalUserIds) >= 2 ? 2 : 1
    }
  } else if (fileInfo && fileInfo.type === 'payments_approval_esigning_pdf') {
    dataForIdfy.title = appHelper.translateToUserLng(
      'payments.pending_payments_approval_esigning',
      userLang
    )
    dataForIdfy.description = appHelper.translateToUserLng(
      'payments.pending_payments_approval_to_be_signed',
      userLang
    )
    dataForIdfy.externalId = `paymentsApproval-${partnerPayoutId}-${_id}`
    dataForIdfy.dataToSign = {
      fileName: 'payments-approve.pdf'
    }
    dataForIdfy.advanced = { tags: ['pendingPaymentApproval'] }
  }
  return dataForIdfy
}

const prepareDataForIdfy = async (fileInfo) => {
  const contactEmail =
    process.env.STAGE === 'production'
      ? 'contact-us@uniteliving.com'
      : `contact-us.${process.env.STAGE}@uniteliving.com`

  const { directRemittanceApprovalUserIds = [], partnerId } = fileInfo
  let dataForIdfy = {
    contactDetails: { email: contactEmail },
    signers: await getPayoutSignersInfo(
      partnerId,
      directRemittanceApprovalUserIds
    ),
    signatureType: { mechanism: 'bank_id' }
  }
  dataForIdfy = await updateDataByFileType(dataForIdfy, fileInfo)
  return dataForIdfy
}

export const prepareAppQueueDataForEsigning = async (params, session) => {
  const { fileId } = params
  const fileInfo = await fileHelper.getAFile({ _id: fileId }, session)
  const { partnerId, partnerPayoutId, type } = fileInfo
  const dataForIdfy = await prepareDataForIdfy(fileInfo, session)
  const fileKey = fileHelper.getFileKey(fileInfo)
  const appQueueParams = {
    partnerId,
    processType: 'create_document',
    dataForIdfy,
    eSignType: type === 'payouts_approval_esigning_pdf' ? 'payout' : 'payment',
    fileType: type ? type : 'payouts_approval_esigning_pdf',
    docId: partnerPayoutId ? partnerPayoutId : '',
    fileKey
  }
  return {
    action: 'create_e_signing_document',
    event: 'handle_e_signing',
    priority: 'immediate', // TODO: replace priority with regular
    destination: 'esigner',
    params: appQueueParams
  }
}

export const getCreditTransferInfo = async (
  partnerId,
  payoutId,
  contractId
) => {
  const payoutInfo = await getPayout({ _id: payoutId, partnerId })
  const newCreditTransferData = {}
  const contractInfo = await contractHelper.getAContract({ _id: contractId })
  const invoiceInfo =
    size(payoutInfo) && payoutInfo.invoiceId
      ? await invoiceHelper.getInvoice({ _id: payoutInfo.invoiceId })
      : ''
  let newDebtorAccountId =
    size(invoiceInfo) && invoiceInfo.invoiceAccountNumber
      ? invoiceInfo.invoiceAccountNumber
      : ''
  if (!(payoutInfo && contractInfo)) return false

  //find invoice account number and set debtor account id for final settlement payout
  if (payoutInfo.isFinalSettlement && !newDebtorAccountId) {
    const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })
    const booleanParams = {
      isFirstInvoice: false,
      isLandlordInvoice: false
    }
    newDebtorAccountId =
      (await invoiceHelper.getInvoiceAccountNumber(
        contractInfo,
        partnerSettings,
        booleanParams
      )) || ''
  }
  newCreditTransferData.accountId = payoutInfo.accountId
  newCreditTransferData.payoutId = payoutId
  newCreditTransferData.paymentInstrId = nid(17)
  newCreditTransferData.paymentEndToEndId = nid(17)
  newCreditTransferData.creditorAccountId = contractInfo.payoutTo
  newCreditTransferData.debtorAccountId = newDebtorAccountId
  newCreditTransferData.amount = payoutInfo.amount
  newCreditTransferData.status = 'new'
  newCreditTransferData.contractId = contractId
  newCreditTransferData.paymentReferenceId = payoutInfo.bankReferenceId
  return newCreditTransferData
}

export const getPayoutsCreditTransferData = async (payoutIds) => {
  if (!size(payoutIds)) return false

  const pipeline = [
    { $match: { _id: { $in: payoutIds } } },
    {
      $lookup: {
        as: 'contract',
        from: 'contracts',
        localField: 'contractId',
        foreignField: '_id'
      }
    },
    { $unwind: { path: '$contract', preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        as: 'invoice',
        from: 'invoices',
        localField: 'invoiceId',
        foreignField: '_id'
      }
    },
    { $unwind: { path: '$invoice', preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        as: 'partner',
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id'
      }
    },
    { $unwind: { path: '$partner', preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        as: 'partnerSetting',
        from: 'partner_settings',
        localField: 'partnerId',
        foreignField: 'partnerId'
      }
    },
    { $unwind: { path: '$partnerSetting', preserveNullAndEmptyArrays: true } },
    {
      $addFields: {
        isFinalSettlement: {
          $cond: [{ $ifNull: ['$isFinalSettlement', false] }, true, false]
        },
        partnerInvoiceAccountNumber: {
          $cond: [
            { $eq: ['$partner.accountType', 'direct'] },
            '$contract.rentalMeta.invoiceAccountNumber',
            {
              $cond: [
                { $eq: ['$invoice.isFirstInvoice', true] },
                '$partnerSetting.bankPayment.firstMonthACNo',
                '$partnerSetting.bankPayment.afterFirstMonthACNo'
              ]
            }
          ]
        }
      }
    },
    {
      $addFields: {
        newDebtorAccountId: {
          $cond: [
            { $ifNull: ['$invoice.invoiceAccountNumber', false] },
            '$invoice.invoiceAccountNumber',
            {
              $cond: [
                { $eq: ['$isFinalSettlement', true] },
                '$partnerInvoiceAccountNumber',
                null
              ]
            }
          ]
        }
      }
    },
    {
      $project: {
        _id: 0,
        accountId: 1,
        payoutId: '$_id',
        creditorAccountId: '$contract.payoutTo',
        debtorAccountId: '$newDebtorAccountId',
        amount: 1,
        status: 'new',
        contractId: 1,
        paymentReferenceId: '$bankReferenceId'
      }
    }
  ]
  const creditTransferData = await PayoutCollection.aggregate(pipeline)
  return map(creditTransferData, (creditTransfer) => {
    creditTransfer.paymentInstrId = nid(17)
    creditTransfer.paymentEndToEndId = nid(17)
    return creditTransfer
  })
}

const prepareCollectionAndBankRefField = (feature) => {
  const data = {
    payout: {
      collection: PayoutCollection,
      field: 'bankReferenceId'
    },
    payment: {
      collection: InvoicePaymentCollection,
      field: 'meta.bankRef'
    }
  }
  return data[feature]
}

const prepareBankReferencesQuery = (query) => {
  const { partnerId, field = '', searchString } = query
  const preparedQuery = {
    partnerId,
    [field]: { $exists: true }
  }
  if (searchString) {
    preparedQuery[field] = new RegExp(searchString, 'i')
  }
  return preparedQuery
}

const countBankReferences = async ({ collection, field, query }) => {
  const bankRefs = (await collection.distinct(field, query)) || []
  return bankRefs.length
}

export const queryBankReferencesDropdown = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query = {}, options } = body
  appHelper.checkRequiredFields(['feature'], query)
  const { partnerId } = user
  query.partnerId = partnerId
  const { feature } = query
  const { collection, field } = prepareCollectionAndBankRefField(feature)
  query.field = field
  const preparedQuery = prepareBankReferencesQuery(query)
  const bankReferences =
    (await collection.aggregate([
      {
        $match: preparedQuery
      },
      {
        $group: {
          _id: '$' + field
        }
      },
      {
        $sort: {
          _id: 1
        }
      },
      {
        $skip: options.skip
      },
      {
        $limit: options.limit
      }
    ])) || []
  const totalDocuments = await countBankReferences({
    collection,
    field,
    query: { partnerId, [field]: { $exists: true } }
  })
  const filteredDocuments = await countBankReferences({
    collection,
    field,
    query: preparedQuery
  })
  return {
    data: bankReferences.map((item) => item._id),
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

const getEventsPipeline = () => [
  {
    $addFields: {
      eventAll: [
        {
          $cond: [
            { $ifNull: ['$createdAt', false] },
            { event: 'created', date: '$createdAt' },
            null
          ]
        },
        {
          $cond: [
            { $ifNull: ['$payoutDate', false] },
            { event: 'estimated', date: '$payoutDate' },
            null
          ]
        },
        {
          $cond: [
            { $ifNull: ['$sentToNETSOn', false] },
            { event: 'sent_to_nets', date: '$sentToNETSOn' },
            null
          ]
        },
        {
          $cond: [
            { $ifNull: ['$bookingDate', false] },
            { event: 'paid_by_bank', date: '$bookingDate' },
            null
          ]
        }
      ]
    }
  },
  {
    $addFields: {
      events: {
        $filter: {
          input: '$eventAll',
          as: 'event',
          cond: {
            $ifNull: ['$$event', false]
          }
        }
      }
    }
  }
]

const getMetaInvoicePipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'meta.invoiceId',
      foreignField: '_id',
      as: 'metaInvoice'
    }
  },
  {
    $unwind: {
      path: '$metaInvoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getMetaLandlordInvoicePipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'meta.landlordInvoiceId',
      foreignField: '_id',
      pipeline: [
        {
          $match: {
            $expr: {
              $in: [
                '$invoiceType',
                ['landlord_invoice', 'landlord_credit_note']
              ]
            }
          }
        }
      ],
      as: 'metaLandlordInvoice'
    }
  },
  {
    $unwind: {
      path: '$metaLandlordInvoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getUnpaidPayoutPipeline = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: 'meta.payoutId',
      foreignField: '_id',
      as: 'unpaidPayout'
    }
  },
  {
    $unwind: {
      path: '$unpaidPayout',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getMetaCommissionPipeline = () => [
  {
    $lookup: {
      from: 'commissions',
      localField: 'meta.commissionId',
      foreignField: '_id',
      as: 'metaCommission'
    }
  },
  {
    $unwind: {
      path: '$metaCommission',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getMetaCorrectionPipeline = () => [
  {
    $lookup: {
      from: 'expenses',
      localField: 'meta.correctionsIds',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            createdAt: 1,
            amount: 1
          }
        }
      ],
      as: 'metaCorrections'
    }
  }
]

const getMetaAddonsPipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'metaContract'
    }
  },
  {
    $unwind: {
      path: '$metaContract',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'products_services',
      localField: 'meta.addonsIds',
      foreignField: '_id',
      let: { metaContract: '$metaContract' },
      pipeline: [
        {
          $addFields: {
            contractAddon: {
              $first: {
                $filter: {
                  input: { $ifNull: ['$$metaContract.addons', []] },
                  as: 'addon',
                  cond: {
                    $eq: ['$$addon.addonId', '$_id']
                  }
                }
              }
            }
          }
        },
        {
          $addFields: {
            total: '$contractAddon.total'
          }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            total: 1
          }
        }
      ],
      as: 'metaAddons'
    }
  }
]

const getInvoiceSummaryPipeline = () => [
  {
    $unwind: {
      path: '$meta',
      preserveNullAndEmptyArrays: true
    }
  },
  ...getMetaInvoicePipeline(),
  ...getUnpaidPayoutPipeline(),
  ...getMetaCommissionPipeline(),
  ...getMetaCorrectionPipeline(),
  ...getMetaLandlordInvoicePipeline(),
  ...getMetaAddonsPipeline(),
  {
    $addFields: {
      metaType: '$meta.type',
      metaAmount: '$meta.amount',
      metaSerialId: {
        $switch: {
          branches: [
            {
              case: {
                $or: [
                  { $eq: ['$meta.type', 'rent_invoice'] },
                  { $eq: ['$meta.type', 'credit_rent_invoice'] }
                ]
              },
              then: '$metaInvoice.invoiceSerialId'
            },
            {
              case: {
                $or: [
                  { $eq: ['$meta.type', 'unpaid_earlier_payout'] },
                  { $eq: ['$meta.type', 'unpaid_expenses_and_commissions'] },
                  { $eq: ['$meta.type', 'moved_to_next_payout'] }
                ]
              },
              then: '$unpaidPayout.serialId'
            },
            {
              case: {
                $or: [
                  { $eq: ['$meta.type', 'brokering_commission'] },
                  { $eq: ['$meta.type', 'management_commission'] },
                  { $eq: ['$meta.type', 'credit_brokering_commission'] },
                  { $eq: ['$meta.type', 'credit_management_commission'] }
                ]
              },
              then: '$metaCommission.serialId'
            },
            {
              case: {
                $or: [
                  { $eq: ['$meta.type', 'landlord_invoice'] },
                  { $eq: ['$meta.type', 'final_settlement_invoiced'] },
                  { $eq: ['$meta.type', 'final_settlement_invoiced_cancelled'] }
                ]
              },
              then: '$metaLandlordInvoice.invoiceSerialId'
            }
          ],
          default: null
        }
      }
    }
  },
  {
    $group: {
      _id: '$_id',
      metaInfo: {
        $push: {
          type: '$metaType',
          amount: '$metaAmount',
          serialId: '$metaSerialId',
          correctionsInfo: '$metaCorrections',
          invoiceId: '$meta.invoiceId',
          landlordInvoiceId: '$meta.landlordInvoiceId',
          payoutId: '$meta.payoutId',
          commissionId: '$meta.commissionId',
          addonsInfo: '$metaAddons'
        }
      },
      serialId: { $first: '$serialId' },
      holdPayout: { $first: '$holdPayout' },
      bankRef: { $first: '$bankRef' },
      numberOfFails: { $first: '$numberOfFails' },
      status: { $first: '$status' },
      paymentStatus: { $first: '$paymentStatus' },
      amount: { $first: '$amount' },
      propertyInfo: { $first: '$propertyInfo' },
      accountInfo: { $first: '$accountInfo' },
      agentInfo: { $first: '$agentInfo' },
      branchInfo: { $first: '$branchInfo' },
      tenantsInfo: { $first: '$tenantsInfo' },
      events: { $first: '$events' },
      contractId: { $first: '$contractId' }
    }
  }
]

const lookupTenantsInfo = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenants.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'user'
          }
        },
        {
          $unwind: {
            path: '$user',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$user.profile.avatarKey'
            )
          }
        }
      ],
      as: 'tenantsInfo'
    }
  }
]

const getPayoutDetails = async (query) => {
  const pipeline = [
    {
      $match: query
    },
    ...appHelper.getCommonPropertyInfoPipeline(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...lookupTenantsInfo(),
    ...getEventsPipeline(),
    ...getInvoiceSummaryPipeline(),
    ...getContractPipeline(),
    {
      $project: {
        _id: 1,
        serialId: 1,
        holdPayout: {
          $cond: [
            {
              $and: [
                { $eq: ['$status', 'estimated'] },
                {
                  $or: [
                    { $eq: ['$holdPayout', true] },
                    { $eq: ['$contractInfo.holdPayout', true] }
                  ]
                }
              ]
            },
            true,
            false
          ]
        },
        numberOfFails: 1,
        bankRef: 1,
        status: 1,
        paymentStatus: 1,
        amount: 1,
        propertyInfo: {
          _id: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          },
          listingTypeId: 1,
          propertyTypeId: 1,
          apartmentId: 1,
          imageUrl: 1
        },
        accountInfo: 1,
        agentInfo: 1,
        branchInfo: 1,
        tenantsInfo: 1,
        events: 1,
        metaInfo: 1
      }
    }
  ]
  const [details = {}] = (await PayoutCollection.aggregate(pipeline)) || []
  return details
}

export const queryPayoutDetails = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['payoutId'], body)
  const { partnerId, userId, roles = [] } = user
  const { requestFromPartnerPublic } = body
  body.partnerId = partnerId
  const query = {
    _id: body.payoutId,
    partnerId
  }
  if (roles.includes('partner_landlord') && requestFromPartnerPublic === true) {
    const accountIds = await accountHelper.getAccountIdsByQuery({
      personId: userId,
      partnerId
    })
    query.accountId = {
      $in: accountIds
    }
  }
  return await getPayoutDetails(query)
}

export const getDataForIdfy = async (req) => {
  const { body } = req
  const { query } = body
  const { fileId } = query
  const fileInfo = await fileHelper.getAFile({ _id: fileId })
  const { partnerId, partnerPayoutId, type } = fileInfo
  const dataForIdfy = await prepareDataForIdfy(fileInfo)
  const fileKey = fileHelper.getFileKey(fileInfo)
  return {
    partnerId,
    processType: 'create_document',
    dataForIdfy,
    eSignType: type === 'payouts_approval_esigning_pdf' ? 'payout' : 'payment',
    fileType: type ? type : 'payouts_approval_esigning_pdf',
    docId: partnerPayoutId ? partnerPayoutId : '',
    fileKey
  }
}

export const payoutForApphealthTransactions = async (partnerId) => {
  const pipeline = preparePipelineForPayoutTransactionAppHealth(partnerId)
  const payoutAmount = await PayoutCollection.aggregate(pipeline)
  return payoutAmount[0] || {}
}

const preparePipelineForPayoutTransactionAppHealth = (partnerId) => [
  { $match: { status: 'completed', paymentStatus: 'paid', partnerId } },
  {
    $project: {
      amount: 1
    }
  },
  {
    $lookup: {
      from: 'transactions',
      localField: '_id',
      foreignField: 'payoutId',
      as: 'transactions',
      pipeline: [
        {
          $match: {
            type: 'payout'
          }
        },
        {
          $project: {
            amount: 1,
            type: 1,
            payoutId: 1,
            subType: 1,
            totalRounded: {
              $cond: {
                if: { $eq: ['$subType', 'rounded_amount'] },
                then: '$amount',
                else: 0
              }
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      totalRoundedAmount: {
        $sum: '$transactions.totalRounded'
      },
      transactions: '$transactions._id',
      transactionAmounts: {
        $sum: '$transactions.amount'
      }
    }
  },
  {
    $addFields: {
      transactionAmounts: {
        $subtract: ['$transactionAmounts', '$totalRoundedAmount']
      }
    }
  },
  {
    $addFields: {
      missMatchTransactionsAmount: {
        $abs: {
          $subtract: ['$transactionAmounts', '$amount']
        }
      }
    }
  },
  {
    $group: {
      _id: null,
      totalTransactions: {
        $sum: '$transactionAmounts'
      },
      totalPayout: {
        $sum: '$amount'
      },
      missingAmount: {
        $sum: '$missMatchTransactionsAmount'
      },
      missingTransactionsInPayout: {
        $push: {
          $cond: {
            if: {
              $gte: [
                {
                  $abs: '$missMatchTransactionsAmount'
                },
                1
              ]
            },
            then: {
              payoutId: '$_id',
              payoutAmount: '$amount',
              transactions: '$transactions',
              transactionAmounts: '$transactionAmounts'
            },
            else: '$$REMOVE'
          }
        }
      }
    }
  }
]

export const preparePipelineForPayoutAppHealth = (contractId) => {
  const pipeline = [
    {
      $match: { contractId }
    },

    {
      $addFields: {
        payoutMetaAmount: {
          $round: [{ $sum: '$meta.amount' }, 2]
        }
      }
    },
    {
      $lookup: {
        from: 'contracts',
        localField: 'contractId',
        foreignField: '_id',
        as: 'contract'
      }
    },
    {
      $unwind: {
        path: '$contract',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'invoices',
        localField: 'contractId',
        foreignField: 'contractId',
        as: 'invoices',
        let: {
          contract: '$contract'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $in: [
                  '$invoiceType',
                  [
                    'invoice',
                    'credit_note',
                    'landlord_invoice',
                    'landlord_credit_note'
                  ]
                ]
              }
            }
          },
          {
            $addFields: {
              totalFees: {
                $sum: '$feesMeta.total'
              },
              balanceLeftForInvoice: {
                $cond: {
                  if: {
                    $and: [
                      { $eq: ['$contract.status', 'closed'] },
                      { $ne: ['$status', 'balanced'] },
                      {
                        $in: [
                          '$invoiceType',
                          ['landlord_invoice', 'landlord_credit_note']
                        ]
                      }
                    ]
                  },
                  then: {
                    $subtract: ['$invoiceTotal', '$totalBalanced']
                  },
                  else: 0
                }
              }
            }
          },
          {
            $group: {
              _id: null,
              total: { $sum: '$invoiceTotal' },
              roundedtotal: { $sum: '$roundedAmount' },
              feesMeta: { $push: '$feesMeta' },
              totals: { $push: '$invoiceTotal' },
              rentInvoiceTotal: {
                $sum: {
                  $cond: {
                    if: { $in: ['$invoiceType', ['invoice', 'credit_note']] },
                    then: '$invoiceTotal',
                    else: 0
                  }
                }
              },
              remainingBalanceTotal: {
                $sum: {
                  $cond: {
                    if: {
                      $ne: ['$isFinalSettlement', true]
                    },
                    then: '$remainingBalance',
                    else: 0
                  }
                }
              },
              finalSettlementTotal: {
                $sum: {
                  $cond: {
                    if: {
                      $eq: ['$isFinalSettlement', true]
                    },
                    then: '$invoiceTotal',
                    else: 0
                  }
                }
              },
              landlordInvoiceTotal: {
                $sum: {
                  $cond: {
                    if: {
                      $and: [
                        {
                          $in: [
                            '$invoiceType',
                            ['landlord_invoice', 'landlord_credit_note']
                          ]
                        },
                        {
                          $ne: ['$isFinalSettlement', true]
                        }
                      ]
                    },
                    then: '$invoiceTotal',
                    else: 0
                  }
                }
              },
              invoiceSerialIds: { $push: '$invoiceSerialId' },
              propertyId: { $first: '$propertyId' },
              totalFees: {
                $sum: '$totalFees'
              },

              balanceLeftInvoicesIds: {
                $push: {
                  $cond: {
                    if: { $gt: ['$balanceLeftForInvoice', 0.5] },
                    then: '$invoiceSerialId',
                    else: '$$REMOVE'
                  }
                }
              },
              balanceLeft: {
                $sum: {
                  $cond: {
                    if: {
                      $and: [
                        { $eq: ['$contract.status', 'closed'] },
                        { $ne: ['$status', 'balanced'] },
                        {
                          $in: [
                            '$invoiceType',
                            ['landlord_invoice', 'landlord_credit_note']
                          ]
                        }
                      ]
                    },
                    then: {
                      $subtract: ['$invoiceTotal', '$totalBalanced']
                    },
                    else: 0
                  }
                }
              }
            }
          },
          {
            $addFields: {
              rentInvoiceTotalWithoutFees: {
                $subtract: [
                  '$rentInvoiceTotal',
                  {
                    $add: ['$totalFees', '$roundedtotal']
                  }
                ]
              }
            }
          },
          {
            $addFields: {
              expectedPayoutTotal: {
                $round: [
                  {
                    $subtract: [
                      '$rentInvoiceTotalWithoutFees',
                      '$landlordInvoiceTotal'
                    ]
                  },
                  2
                ]
              }
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$meta',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        metaInvoiceId: {
          $cond: {
            if: {
              $in: ['$meta.type', ['rent_invoice', 'credit_rent_invoice']]
            },
            then: '$meta.invoiceId',
            else: '$meta.landlordInvoiceId'
          }
        }
      }
    },
    {
      $lookup: {
        from: 'invoices',
        localField: 'metaInvoiceId',
        foreignField: '_id',
        as: 'metaInvoices',
        let: {
          meta: '$meta'
        },
        pipeline: [
          {
            $addFields: {
              totalFees: {
                $sum: '$feesMeta.total'
              }
            }
          },
          {
            $addFields: {
              invoiceWithoutTotalFees: {
                $subtract: ['$invoiceTotal', '$totalFees']
              },
              metaTotal: {
                $cond: {
                  if: {
                    $in: [
                      '$$meta.type',
                      ['rent_invoice', 'credit_rent_invoice']
                    ]
                  },
                  then: '$$meta.amount',
                  else: {
                    $multiply: ['$$meta.amount', -1]
                  }
                }
              },
              metaTotalRent: {
                $cond: {
                  if: {
                    $in: [
                      '$$meta.type',
                      ['rent_invoice', 'credit_rent_invoice']
                    ]
                  },
                  then: '$$meta.amount',
                  else: 0
                }
              },
              metaTotalLandlord: {
                $cond: {
                  if: {
                    $in: [
                      '$$meta.type',
                      ['landlord_invoice', 'landlord_credit_note']
                    ]
                  },
                  then: '$$meta.amount',
                  else: 0
                }
              }
            }
          },
          {
            $project: {
              invoiceSerialId: 1,
              invoiceTotal: 1,
              totalFees: 1,
              invoiceWithoutTotalFees: 1,
              feesMeta: 1,
              metaTotalDiff: {
                $subtract: ['$invoiceWithoutTotalFees', '$metaTotal']
              },
              metaTotalRent: 1,
              metaTotalRentDiff: {
                $cond: {
                  if: {
                    $and: [
                      {
                        $in: [
                          '$$meta.type',
                          ['rent_invoice', 'credit_rent_invoice']
                        ]
                      }
                    ]
                  },
                  then: {
                    $subtract: ['$invoiceWithoutTotalFees', '$metaTotalRent']
                  },
                  else: 0
                }
              },
              metaTotalLandlordDiff: {
                $cond: {
                  if: {
                    $and: [
                      {
                        $in: [
                          '$$meta.type',
                          ['landlord_invoice', 'landlord_credit_note']
                        ]
                      }
                    ]
                  },
                  then: {
                    $subtract: [
                      '$invoiceWithoutTotalFees',
                      {
                        $abs: '$metaTotalLandlord'
                      }
                    ]
                  },
                  else: 0
                }
              },
              invoiceType: 1,
              metaTotal: 1
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$metaInvoices',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: '$contractId',
        landlordMetaAmount: {
          $sum: {
            $cond: {
              if: { $eq: ['$meta.type', 'landlord_invoice'] },
              then: '$meta.amount',
              else: 0
            }
          }
        },
        metaInvoices: {
          $push: {
            $cond: {
              if: { $gt: ['$metaInvoices.metaTotalDiff', 0] },
              then: '$metaInvoices',
              else: '$$REMOVE'
            }
          }
        },
        metaInvoicesRent: {
          $push: {
            $cond: {
              if: { $gt: ['$metaInvoices.metaTotalRentDiff', 0] },
              then: '$metaInvoices',
              else: '$$REMOVE'
            }
          }
        },
        metaInvoicesLandlord: {
          $push: {
            $cond: {
              if: { $gt: ['$metaInvoices.metaTotalLandlordDiff', 0] },
              then: '$metaInvoices',
              else: '$$REMOVE'
            }
          }
        },
        // metaInvoices: {
        //     $push: "$metaInvoices"
        // }
        metaInvoicesSerialIds: {
          $addToSet: '$metaInvoices.invoiceSerialId'
        },
        metaAmount: {
          $sum: '$meta.amount'
        },
        actualPayout: {
          $addToSet: {
            payoutId: '$_id',
            amount: '$amount'
          }
        },
        wrongPayoutMeta: {
          $addToSet: {
            $cond: {
              if: {
                $gt: [
                  {
                    $abs: {
                      $subtract: ['$payoutMetaAmount', '$amount']
                    }
                  },
                  0.5
                ]
              },
              then: {
                payoutMetaAmount: '$payoutMetaAmount',
                amount: '$amount',
                payoutSerialId: '$serialId',
                payoutId: '$_id'
              },
              else: '$$REMOVE'
            }
          }
        },
        rentInvoiceAmountMisMatch: {
          $addToSet: {
            $cond: {
              if: {
                $and: [
                  {
                    $gte: [
                      {
                        $abs: '$metaInvoices.metaTotalRentDiff'
                      },
                      1
                    ]
                  },
                  {
                    $in: [
                      '$metaInvoices.invoiceType',
                      ['invoice', 'credit_note']
                    ]
                  }
                ]
              },
              then: '$metaInvoices',
              else: '$$REMOVE'
            }
          }
        },
        invoices: {
          $first: '$invoices'
        },
        rentInvoiceMetaTotal: {
          $sum: {
            $cond: {
              if: {
                $in: ['$meta.type', ['rent_invoice', 'credit_rent_invoice']]
              },
              then: '$meta.amount',
              else: 0
            }
          }
        }
      }
    },
    {
      $unwind: {
        path: '$invoices',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        landlordInvoiceAmountMisMatch: {
          $subtract: [
            '$invoices.landlordInvoiceTotal',
            {
              $abs: '$landlordMetaAmount'
            }
          ]
        }
      }
    },
    {
      $addFields: {
        landlordInvoiceAmountMisMatch: {
          $cond: {
            if: {
              $ne: [
                {
                  $round: ['$landlordInvoiceAmountMisMatch', 0]
                },
                {
                  $round: ['$invoices.remainingBalanceTotal', 0]
                }
              ]
            },
            then: '$landlordInvoiceAmountMisMatch',
            else: 0
          }
        }
      }
    },
    {
      $addFields: {
        missingSerialIds: {
          $cond: {
            if: { $gt: ['$landlordInvoiceAmountMisMatch', 0] },
            then: {
              $setDifference: [
                '$invoices.invoiceSerialIds',
                '$metaInvoicesSerialIds'
              ]
            },
            else: []
          }
        },
        actualPayoutTotal: {
          $sum: '$actualPayout.amount'
        }
      }
    },
    {
      $addFields: {
        absPayoutDiff: {
          $abs: {
            $subtract: ['$actualPayoutTotal', '$invoices.expectedPayoutTotal']
          }
        }
      }
    },
    {
      $addFields: {
        actualPayoutTotal: {
          $round: [
            {
              $subtract: [
                '$actualPayoutTotal',
                '$invoices.finalSettlementTotal'
              ]
            },
            2
          ]
        }
      }
    },
    {
      $addFields: {
        payoutDiff: {
          $subtract: ['$actualPayoutTotal', '$invoices.expectedPayoutTotal']
        }
      }
    },
    {
      $addFields: {
        //   testDiff: {

        //   }
        payoutDiff: {
          $cond: {
            if: {
              $gt: [
                {
                  $abs: {
                    $subtract: [
                      '$absPayoutDiff',
                      {
                        $abs: '$invoices.remainingBalanceTotal'
                      }
                    ]
                  }
                },
                1
              ]
            },
            then: '$payoutDiff',
            else: 0
          }
        }
      }
    },
    {
      $addFields: {
        payoutDiff: {
          $cond: {
            if: {
              $gt: ['$absPayoutDiff', 1]
            },
            then: '$payoutDiff',
            else: 0
          }
        }
      }
    },
    {
      $addFields: {
        payoutDiff: {
          $cond: {
            if: {
              $gte: [
                {
                  $subtract: [
                    '$rentInvoiceMetaTotal',
                    '$invoice.expectedPayoutTotal'
                  ]
                },
                1
              ]
            },
            then: '$payoutDiff',
            else: 0
          }
        }
      }
    },
    {
      $addFields: {
        payoutDiff: {
          $abs: {
            $round: ['$payoutDiff', 2]
          }
        },
        rentInvoiceDiff: {
          $subtract: [
            '$invoices.rentInvoiceTotalWithoutFees',
            '$rentInvoiceMetaTotal'
          ]
        }
      }
    }
  ]
  return pipeline
}

export const getLandlordInvoiceTotalFromPayoutMeta = (
  payoutMeta = [],
  landlordInvoiceIds = []
) => {
  let total = 0

  for (const metaInfo of payoutMeta) {
    if (includes(landlordInvoiceIds, metaInfo.landlordInvoiceId))
      total += metaInfo.amount || 0
  }

  return total
}

export const isFinalSettlementPayoutWillBeCompleted = async (
  payoutInfo = {},
  session
) => {
  let willBeCompleted = false
  if (payoutInfo.isFinalSettlement && size(payoutInfo.meta)) {
    const unbalanceInvoice = await invoiceHelper.getInvoice(
      {
        contractId: payoutInfo.contractId,
        partnerId: payoutInfo.partnerId,
        propertyId: payoutInfo.propertyId,
        isFinalSettlement: { $ne: true },
        remainingBalance: { $ne: 0 },
        invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] }
      },
      session
    )

    if (!unbalanceInvoice) willBeCompleted = true
  }
  return willBeCompleted
}

export const validateUpdatePayoutPauseStatus = async (body) => {
  const { partnerId, payoutId } = body
  const payout = await getPayout({ _id: payoutId, partnerId })
  if (!size(payout)) throw new CustomError(404, 'Payout not found')
  if (payout.status !== 'estimated') {
    throw new CustomError(400, 'Payout not available for update')
  }
  const contract = await contractHelper.getContractById(payout.contractId)
  if (!size(contract)) throw new CustomError(400, 'Contract not found')
  if (contract.holdPayout) {
    throw new CustomError(400, 'Payout not available for update')
  }
  return body
}

export const getPendingPayoutsList = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body = {}, user } = req
  const { query = {}, options = {}, partnerId } = body
  const { requestFrom = '', propertyId = '' } = query
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)

  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }

  const isAllowed =
    await invoicePaymentHelper.getPermissionForApprovingPayoutsOrPayments(user)
  if (!isAllowed) {
    throw new CustomError(403, 'User is not permitted!')
  }

  body.query = await prepareQueryDataForPayoutsQuery(query)
  body.query.status = { $in: ['pending_for_approval'] }

  const payouts = await getPayoutForQuery(body)
  const filteredDocuments = await countPayouts(body.query)
  const totalDocuments = await countPayouts(totalDocumentsQuery)

  const esignInfo = await partnerPayoutHelper.getAllPayoutsEsignInfo(partnerId)

  return {
    data: payouts,
    metaData: { filteredDocuments, totalDocuments },
    actions: { esignInfo }
  }
}

export const getPayoutStatusForPartnerDashboard = async (query) => {
  const result = await PayoutCollection.aggregate([
    {
      $match: {
        ...query,
        status: {
          $in: ['pending_for_approval', 'estimated']
        }
      }
    },
    {
      $addFields: {
        pendingPayouts: {
          $cond: [{ $eq: ['$status', 'pending_for_approval'] }, 1, 0]
        },
        pausedPayouts: {
          $cond: [
            {
              $and: [
                { $eq: ['$status', 'estimated'] },
                { $eq: ['$holdPayout', true] }
              ]
            },
            1,
            0
          ]
        },
        waitingForSignaturePayoutCount: {
          $cond: [
            {
              $and: [{ $eq: ['$status', 'waiting_for_signature'] }]
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
        pendingPayoutCount: {
          $sum: '$pendingPayouts'
        },
        pausedPayoutsCount: {
          $sum: '$pausedPayouts'
        },
        waitingForSignaturePayoutCount: {
          $sum: '$waitingForSignaturePayoutCount'
        }
      }
    }
  ])
  const [payoutStatus = {}] = result || []
  const {
    pausedPayoutsCount = 0,
    pendingPayoutCount = 0,
    waitingForSignaturePayoutCount = 0
  } = payoutStatus
  return {
    pausedPayoutsCount,
    pendingPayoutCount,
    waitingForSignaturePayoutCount
  }
}
