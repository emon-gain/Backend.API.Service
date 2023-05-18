import { each, includes, indexOf, omit, pick, size } from 'lodash'
import { InvoiceSummaryCollection, PayoutCollection } from '../models'
import {
  accountHelper,
  appHelper,
  invoiceHelper,
  payoutHelper,
  tenantHelper
} from '../helpers'

export const getInvoiceSummary = async (query, session) => {
  const summary = await InvoiceSummaryCollection.findOne(query).session(session)
  return summary
}

export const getInvoiceSummaries = async (query, session) => {
  const summaries = await InvoiceSummaryCollection.find(query).session(session)
  return summaries
}

export const getTotalFeeByInvoiceFeesMeta = (invoiceFeesMetaData) => {
  let totalFee = 0
  if (size(invoiceFeesMetaData)) {
    each(invoiceFeesMetaData, (feesMetaInfo) => {
      totalFee += feesMetaInfo.amount || 0
      totalFee += feesMetaInfo.tax || 0
    })
  }
  return totalFee
}

export const prepareInvoiceSummaryData = (invoice = {}) => {
  const invoiceFeesMeta = invoice.feesMeta || []
  const summaryData = pick(invoice, [
    'accountId',
    'agentId',
    'correctionsIds',
    'createdBy',
    'branchId',
    'dueDate',
    'invoiceSerialId',
    'partnerId',
    'propertyId',
    'tenantId',
    'tenants'
  ])
  summaryData.invoiceAmount = invoice.invoiceTotal || 0
  summaryData.feesAmount = getTotalFeeByInvoiceFeesMeta(invoiceFeesMeta)
  summaryData.isPaid = false
  summaryData.invoiceId = invoice._id
  return summaryData
}

const getTenantPipeline = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            // For property details page
            pipeline: [
              {
                $lookup: {
                  from: 'userReport',
                  localField: '_id',
                  foreignField: 'reportedUser',
                  as: 'reports'
                }
              }
            ],
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
            ),
            reported: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$user.reports', []] } }, 0] },
                true,
                false
              ]
            }
          }
        }
      ],
      as: 'tenantInfo'
    }
  },
  {
    $unwind: { path: '$tenantInfo', preserveNullAndEmptyArrays: true }
  }
]

const getAccountPipeline = () => [
  {
    $lookup: {
      from: 'accounts',
      localField: 'accountId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'personId',
            foreignField: '_id',
            as: 'person'
          }
        },
        { $unwind: { path: '$person', preserveNullAndEmptyArrays: true } },
        {
          $lookup: {
            from: 'organizations',
            localField: 'organizationId',
            foreignField: '_id',
            as: 'organization'
          }
        },
        {
          $unwind: { path: '$organization', preserveNullAndEmptyArrays: true }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: {
              $cond: [
                { $eq: ['$type', 'person'] },
                appHelper.getUserAvatarKeyPipeline('$person.profile.avatarKey'),
                appHelper.getOrganizationLogoPipeline('$organization.image')
              ]
            }
          }
        }
      ],
      as: 'accountInfo'
    }
  },
  {
    $unwind: { path: '$accountInfo', preserveNullAndEmptyArrays: true }
  }
]

const getAgentPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agentId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'agentInfo'
    }
  },
  {
    $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true }
  }
]

export const getBranchPipeline = (userId = '') => [
  {
    $lookup: {
      from: 'branches',
      localField: 'branchId',
      foreignField: '_id',
      let: { userId },
      pipeline: [
        {
          $project: {
            _id: 1,
            name: 1,
            isBranchAdminUser: {
              $cond: [
                {
                  $eq: ['$adminId', '$$userId']
                },
                true,
                false
              ]
            }
          }
        }
      ],
      as: 'branchInfo'
    }
  },
  {
    $unwind: { path: '$branchInfo', preserveNullAndEmptyArrays: true }
  }
]

const getPropertyPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              postalCode: 1
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
  {
    $unwind: {
      path: '$propertyInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

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
            totalPaid: 1,
            totalDue: {
              $subtract: [
                {
                  $add: [
                    { $ifNull: ['$invoiceTotal', 0] },
                    { $ifNull: ['$creditedAmount', 0] }
                  ]
                },
                {
                  $add: [
                    { $ifNull: ['$totalPaid', 0] },
                    { $ifNull: ['$lostMeta.amount', 0] }
                  ]
                }
              ]
            },
            invoiceType: 1,
            contractId: 1,
            status: 1,
            invoiceSent: 1,
            isNonRentInvoice: 1,
            isPartiallyPaid: 1,
            isDefaulted: 1,
            secondReminderSentAt: 1,
            firstReminderSentAt: 1,
            isOverPaid: 1,
            isPartiallyCredited: 1,
            isPartiallyBalanced: 1,
            delayDate: 1,
            evictionNoticeSent: 1,
            evictionDueReminderSent: 1,
            vippsStatus: 1,
            enabledNotification: 1,
            isCollectionNoticeSent: {
              $cond: [
                { $ifNull: ['$collectionNoticeSentAt', false] },
                true,
                false
              ]
            },
            lastPaymentDate: 1,
            compelloStatus: 1
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  {
    $unwind: {
      path: '$invoiceInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getPayoutPipeline = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: 'payoutId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            status: 1,
            paymentStatus: 1
          }
        }
      ],
      as: 'payoutInfo'
    }
  },
  {
    $unwind: {
      path: '$payoutInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getFinalProjectPipeline = () => ({
  $project: {
    _id: 1,
    tenantInfo: 1,
    accountInfo: 1,
    agentInfo: 1,
    branchInfo: 1,
    propertyInfo: 1,
    invoiceInfo: 1,
    payoutInfo: 1,
    invoiceSerialId: 1,
    isFinalSettlementDone: '$contractInfo.isFinalSettlementDone',
    isSendInvoice: 1,
    dueDate: 1,
    createdAt: 1,
    invoiceAmount: 1,
    totalPaid: '$invoiceInfo.totalPaid',
    totalDue: '$invoiceInfo.totalDue',
    commissionsIds: 1,
    commissionsAmount: 1,
    correctionsIds: 1,
    correctionsAmount: 1,
    payoutAmount: 1,
    showRefundOption: 1,
    paymentId: '$invoicePaymentInfo._id',
    // For property details page
    feesAmount: 1,
    isBranchAdminUser: '$branchInfo.isBranchAdminUser'
  }
})

export const showRefundOption = (localField) => [
  {
    $lookup: {
      from: 'invoice-payments',
      let: { invoiceId: '$' + localField },
      localField,
      foreignField: 'invoices.invoiceId',
      as: 'invoicePaymentInfo',
      pipeline: [
        {
          $addFields: {
            isMainInvoice: {
              $cond: [{ $eq: ['$invoiceId', '$$invoiceId'] }, true, false]
            },
            matchedInvoice: {
              $first: {
                $filter: {
                  input: '$invoices',
                  as: 'invoice',
                  cond: { $eq: ['$$invoice.invoiceId', '$$invoiceId'] }
                }
              }
            }
          }
        },
        {
          $match: {
            $expr: {
              $or: [
                { $eq: ['$isMainInvoice', true] },
                { $gt: ['$matchedInvoice.amount', 0] }
              ]
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      showRefundOption: {
        $cond: [
          { $gt: [{ $size: { $ifNull: ['$invoicePaymentInfo', []] } }, 0] },
          true,
          false
        ]
      },
      invoicePaymentInfo: {
        $first: {
          $ifNull: ['$invoicePaymentInfo', []]
        }
      }
    }
  }
]

export const getInvoiceNotificationLogDetails = () => [
  {
    $lookup: {
      from: 'notification_logs',
      foreignField: 'invoiceId',
      localField: 'invoiceId',
      as: 'notificationLogs',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$event', 'send_invoice']
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      isSendInvoice: {
        $cond: [{ $gt: [{ $size: '$notificationLogs' }, 0] }, true, false]
      }
    }
  }
]

export const getInvoiceSummariesForQuery = async (params = {}) => {
  const { query, options, userId } = params
  const pipeline = [
    { $match: query },
    { $sort: options.sort },
    { $skip: options.skip },
    { $limit: options.limit },
    ...getTenantPipeline(),
    ...getAccountPipeline(),
    ...getAgentPipeline(),
    ...getBranchPipeline(userId),
    ...getPropertyPipeline(),
    ...getInvoicePipeline(),
    ...getInvoiceNotificationLogDetails(),
    ...getPayoutPipeline(),
    ...appHelper.getCommonContractInfoPipeline('invoiceInfo.contractId'),
    ...showRefundOption('invoiceId'),
    getFinalProjectPipeline()
  ]
  const invoiceSummaries =
    (await InvoiceSummaryCollection.aggregate(pipeline)) || []
  return invoiceSummaries
}

export const countInvoiceSummaries = async (query, session) => {
  const numberOfInvoiceSummaries = await InvoiceSummaryCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfInvoiceSummaries
}

export const prepareStatusQueryForSummary = (status) => {
  const invoicesStatusQuery = []
  const isPartiallyPaid = includes(status, 'partially_paid')
  const isOverpaid = includes(status, 'overpaid')
  const isDefaulted = includes(status, 'defaulted')
  const isSent = includes(status, 'sent')
  if (isPartiallyPaid) invoicesStatusQuery.push({ isPartiallyPaid: true })
  if (isOverpaid) invoicesStatusQuery.push({ isOverPaid: true })
  if (isDefaulted) invoicesStatusQuery.push({ isDefaulted: true })
  if (includes(status, 'created')) {
    invoicesStatusQuery.push({
      status: { $in: ['new', 'created'] },
      invoiceSent: { $ne: true }
    })
  }
  if (isSent) {
    invoicesStatusQuery.push({
      invoiceSent: true,
      status: { $in: ['created', 'new'] },
      isPartiallyPaid: { $ne: true },
      isDefaulted: { $ne: true }
    })
  }
  if (indexOf(status, 'paid') !== -1)
    invoicesStatusQuery.push({ status: 'paid' })

  if (indexOf(status, 'overdue') !== -1)
    invoicesStatusQuery.push({ status: 'overdue' })

  if (indexOf(status, 'lost') !== -1)
    invoicesStatusQuery.push({ lostMeta: { $exists: true } })

  if (indexOf(status, 'credited') !== -1)
    invoicesStatusQuery.push({ status: 'credited' })

  if (indexOf(status, 'partially_credited') !== -1)
    invoicesStatusQuery.push({ isPartiallyCredited: true })

  if (indexOf(status, 'balanced') !== -1)
    invoicesStatusQuery.push({ status: 'balanced' })

  if (indexOf(status, 'partially_balanced') !== -1)
    invoicesStatusQuery.push({ isPartiallyBalanced: true })

  if (indexOf(status, 'fees_paid') !== -1)
    invoicesStatusQuery.push({ feesPaid: true })

  if (indexOf(status, 'fees_due') !== -1)
    invoicesStatusQuery.push({
      feesMeta: { $exists: true },
      invoiceType: { $ne: 'credit_note' },
      status: { $ne: 'credited' },
      feesPaid: { $ne: true }
    })

  if (indexOf(status, 'eviction_notice') !== -1)
    invoicesStatusQuery.push({
      evictionNoticeSent: true,
      status: { $nin: ['paid', 'credited', 'lost'] }
    })

  if (indexOf(status, 'eviction_notice_due') !== -1)
    invoicesStatusQuery.push({
      evictionDueReminderSent: true,
      status: { $nin: ['paid', 'credited', 'lost'] }
    })

  return invoicesStatusQuery
}

export const prepareQueryForInvoiceSummary = async (query = {}) => {
  const {
    amount,
    contractId,
    createdDateRange,
    evictionNoticeSent,
    invoiceDueDateRange,
    invoicePeriod,
    invoiceStatus,
    kidNumber,
    leaseSerial,
    partnerId,
    payoutStatus,
    searchKeyword = '',
    tenantId,
    vippsStatus = []
  } = query
  const invoiceQuery = {}
  const payoutQuery = {}

  let { compelloStatus = [] } = query
  const eInvoiceType = process.env.E_INVOICE_TYPE
  if (eInvoiceType === 'compello') {
    compelloStatus = query.vippsStatus
  }

  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: new Date(createdDateRange.startDate),
      $lte: new Date(createdDateRange.endDate)
    }
  }

  if (size(invoiceDueDateRange)) {
    appHelper.validateCreatedAtForQuery(invoiceDueDateRange)
    query.dueDate = {
      $gte: new Date(invoiceDueDateRange.startDate),
      $lte: new Date(invoiceDueDateRange.endDate)
    }
  }

  // Lease serial filter
  if (contractId && leaseSerial) {
    const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
      contractId,
      leaseSerial
    )
    query.invoiceId = {
      $in: invoiceIds
    }
  }

  if (tenantId) query.$or = [{ tenantId }, { 'tenants.tenantId': tenantId }]
  //set vipps status
  const vippsInvoiceStatuses = []
  if (vippsStatus.includes('sent')) {
    vippsInvoiceStatuses.push('sent', 'created', 'pending')
  }
  if (vippsStatus.includes('approved')) {
    vippsInvoiceStatuses.push('approved')
  }
  if (vippsStatus.includes('failed')) {
    vippsInvoiceStatuses.push(
      'sending',
      'sending_failed',
      'failed',
      'rejected',
      'expired',
      'deleted',
      'revoked'
    )
  }

  // Set compello status
  const compelloInvoiceStatuses = []
  if (size(compelloStatus)) {
    if (compelloStatus.includes('sent')) {
      compelloInvoiceStatuses.push('sent', 'created', 'pending')
    }
    if (compelloStatus.includes('approved')) {
      compelloInvoiceStatuses.push('approved')
    }
    if (compelloStatus.includes('failed')) {
      compelloInvoiceStatuses.push(
        'sending',
        'sending_failed',
        'failed',
        'rejected',
        'expired',
        'deleted',
        'revoked'
      )
    }
  }

  if (size(compelloInvoiceStatuses) || size(vippsInvoiceStatuses)) {
    invoiceQuery.$and = [
      {
        $or: [
          { compelloStatus: { $in: compelloInvoiceStatuses } },
          { vippsStatus: { $in: vippsInvoiceStatuses } }
        ]
      }
    ]
  }
  if (size(invoicePeriod)) {
    appHelper.validateCreatedAtForQuery(invoicePeriod)
    const startDate = new Date(invoicePeriod.startDate)
    const endDate = new Date(invoicePeriod.endDate)
    invoiceQuery.$or = [
      {
        // When invoiceStartOn in range of filter-duration
        invoiceStartOn: { $gte: startDate, $lte: endDate }
      },
      {
        // When invoiceEndOn in range of filter-duration
        invoiceEndOn: { $gte: startDate, $lte: endDate }
      },
      {
        // When filter-duration in range of period. it is for today, thisWeek and lastWeek.
        invoiceStartOn: { $lte: startDate },
        invoiceEndOn: { $gte: endDate }
      }
    ]
  }

  if (size(invoiceStatus)) {
    const invoiceStatusQuery = prepareStatusQueryForSummary(invoiceStatus)
    if (invoiceQuery.$or) {
      invoiceQuery['$and'] = [{ $or: invoiceStatusQuery }]
    } else {
      invoiceQuery.$or = invoiceStatusQuery
    }
  }

  if (size(payoutStatus)) {
    payoutQuery.status = { $in: payoutStatus }
  }

  if (evictionNoticeSent === true || evictionNoticeSent === false) {
    if (evictionNoticeSent) {
      invoiceQuery.evictionNoticeSent = { $eq: true }
    } else {
      invoiceQuery.evictionNoticeSent = { $ne: true }
    }
  }
  if (kidNumber) {
    const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
      kidNumber: { $regex: kidNumber, $options: 'i' }
    })
    query.invoiceId = { $in: invoiceIds }
  }
  if (amount) query.invoiceAmount = amount
  if (size(searchKeyword)) {
    searchKeyword.trim()
    const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
      kidNumber: { $regex: searchKeyword, $options: 'i' }
    })

    query['$or'] = [
      { invoiceSerialId: parseInt(searchKeyword) },
      { invoiceAmount: parseInt(searchKeyword) },
      { invoiceId: { $in: invoiceIds } }
    ]
  }

  if (size(invoiceQuery)) {
    const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
      ...invoiceQuery,
      partnerId
    })
    query.invoiceId = { $in: invoiceIds }
  }
  if (size(payoutQuery)) {
    const payoutIds = await payoutHelper.getUniqueFieldValues('_id', {
      ...payoutQuery,
      partnerId
    })
    query.payoutId = { $in: payoutIds }
  }

  const preparedQuery = omit(query, [
    'amount',
    'compelloStatus',
    'context',
    'contractId',
    'createdDateRange',
    'evictionNoticeSent',
    'invoiceDueDateRange',
    'invoicePeriod',
    'invoiceStatus',
    'KidNumber',
    'leaseSerial',
    'payoutStatus',
    'requestFrom',
    'searchKeyword',
    'tenantId',
    'vippsStatus'
  ])
  return preparedQuery
}

export const queryInvoiceSummaries = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.validateId({ partnerId })
  const { query, options } = body
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)
  const { context = '', propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  // For landlord dashboard
  if (context === 'landlordDashboard') {
    const accountIds = await accountHelper.getAccountIdsByQuery({
      personId: userId
    })
    query.accountId = { $in: accountIds }
  }
  // For tenant dashboard
  else if (context === 'tenantDashboard' && size(propertyId)) {
    appHelper.validateId({ propertyId })
    const tenantInfo = await tenantHelper.getATenant({ userId, partnerId })
    const tenantId = tenantInfo?._id || ''
    query.$or = [{ tenantId }, { tenants: { $elemMatch: { tenantId } } }]
  }

  const preparedQuery = await prepareQueryForInvoiceSummary(query)
  body.query = preparedQuery
  body.userId = userId
  const invoiceSummaries = await getInvoiceSummariesForQuery(body)
  const totalDocuments = await countInvoiceSummaries(totalDocumentsQuery)
  const filteredDocuments = await countInvoiceSummaries(preparedQuery)
  return {
    data: invoiceSummaries,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const getInvoicePipelineForSummaryInfo = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      as: 'invoice'
    }
  },
  {
    $addFields: {
      invoice: {
        $filter: {
          input: { $ifNull: ['$invoice', []] },
          as: 'singleInvoice',
          cond: {
            $eq: ['$$singleInvoice.invoiceType', 'invoice']
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$invoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getRentCorrectionPipeline = () => [
  {
    $lookup: {
      from: 'expenses',
      localField: 'correctionsIds',
      foreignField: '_id',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$addTo', 'rent_invoice'] },
                { $not: { $eq: ['$isNonRent', true] } }
              ]
            }
          }
        }
      ],
      as: 'rentCorrections'
    }
  },
  {
    $addFields: {
      rentCorrectionAmount: {
        $reduce: {
          input: { $ifNull: ['$rentCorrections', []] },
          initialValue: 0,
          in: { $add: ['$$value', '$$this.amount'] }
        }
      }
    }
  }
]

const prepareQueryForTotalPayoutAmount = (query = {}) => {
  const { accountId, agentId, partnerId, payoutStatus, propertyId, tenantId } =
    query
  const preparedQuery = {}
  if (agentId) preparedQuery.agentId = agentId
  if (partnerId) preparedQuery.partnerId = partnerId
  if (tenantId)
    preparedQuery.$or = [
      { tenantId },
      { tenants: { $elemMatch: { tenantId } } }
    ]
  if (accountId) preparedQuery.accountId = accountId
  if (propertyId) preparedQuery.propertyId = propertyId
  if (payoutStatus) preparedQuery.status = payoutStatus
  return preparedQuery
}

const getPayoutAmountForInvoiceSummary = async (query) => {
  const preparedQuery = prepareQueryForTotalPayoutAmount(query)
  const [payoutInfo = {}] = await PayoutCollection.aggregate([
    { $match: preparedQuery },
    {
      $group: {
        _id: null,
        totalPayoutAmount: { $sum: '$amount' }
      }
    }
  ])
  return payoutInfo.totalPayoutAmount || 0
}

export const queryInvoiceSummaryInfo = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  const { queryData } = body
  queryData.partnerId = partnerId
  const preparedQuery = await prepareQueryForInvoiceSummary(queryData)
  const totalPayoutAmount = await getPayoutAmountForInvoiceSummary(queryData)
  const pipeline = [
    { $match: preparedQuery },
    ...getInvoicePipelineForSummaryInfo(),
    ...getRentCorrectionPipeline(),
    {
      $group: {
        _id: null,
        totalInvoiceAmount: { $sum: '$invoice.invoiceTotal' },
        totalFeesAmount: { $sum: '$feesAmount' },
        totalRentCorrectionAmount: { $sum: '$rentCorrectionAmount' },
        totalCommissionsAmount: { $sum: '$commissionsAmount' },
        totalCorrectionsAmount: { $sum: '$correctionsAmount' },
        totalPaidAmount: { $sum: '$invoice.totalPaid' },
        totalLostAmount: { $sum: '$invoice.lostMeta.amount' },
        totalCreditedAmount: { $sum: '$invoice.creditedAmount' },
        totalBalancedAmount: { $sum: '$invoice.totalBalanced' }
      }
    },
    {
      $addFields: {
        totalDueAmount: {
          $subtract: [
            {
              $add: [
                { $ifNull: ['$totalInvoiceAmount', 0] },
                { $ifNull: ['$totalCreditedAmount', 0] }
              ]
            },
            {
              $add: [
                { $ifNull: ['$totalPaidAmount', 0] },
                { $ifNull: ['$totalLostAmount', 0] },
                { $ifNull: ['$totalBalancedAmount', 0] }
              ]
            }
          ]
        },
        totalInvoiceAmount: {
          $add: [
            { $ifNull: ['$totalInvoiceAmount', 0] },
            { $ifNull: ['$totalCreditedAmount', 0] }
          ]
        },
        totalPayoutAmount
      }
    }
  ]
  const [invoiceSummaryInfo = {}] = await InvoiceSummaryCollection.aggregate(
    pipeline
  )
  return invoiceSummaryInfo
}
