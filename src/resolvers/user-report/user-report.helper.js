import { clone, find, map, size, slice } from 'lodash'
import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  appQueueHelper,
  listingHelper,
  tenantHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import {
  AccountCollection,
  TransactionCollection,
  UserReportCollection
} from '../models'

const subTypesData = [
  'administration_eviction_notice_fee',
  'administration_eviction_notice_fee_move_to',
  'collection_notice_fee',
  'collection_notice_fee_move_to',
  'eviction_notice_fee',
  'eviction_notice_fee_move_to',
  'invoice_fee',
  'invoice_reminder_fee',
  'reminder_fee_move_to',
  'rounded_amount',
  'unpaid_administration_eviction_notice',
  'unpaid_collection_notice',
  'unpaid_eviction_notice',
  'unpaid_reminder'
]

export const getAnUserReport = async (query, session) => {
  const userReport = await UserReportCollection.findOne(query).session(session)
  return userReport
}

export const prepareUserReportCreationData = async (reportedUserId) => {
  const user = await userHelper.getAnUser({ _id: reportedUserId })
  if (!size(user)) throw new CustomError(404, "User doesn't exists")
  const existingAdminReport = await getAnUserReport({
    reportedByAdmin: true,
    reportedUser: reportedUserId
  })
  if (size(existingAdminReport)) throw new CustomError(400, 'Already reported')
  return {
    userReportCreationData: {
      reportedByAdmin: true,
      reportedUser: reportedUserId
    }
  }
}

export const prepareUserReportDeletionQuery = async (reportedUserId) => {
  const user = await userHelper.getAnUser({ _id: reportedUserId })
  if (!size(user)) throw new CustomError(404, "User doesn't exists")
  else
    return {
      query: {
        reportedByAdmin: true,
        reportedUser: reportedUserId
      }
    }
}

export const getUserReports = async (query, session) => {
  const userReports = await UserReportCollection.find(query).session(session)
  return userReports
}

export const checkRequiredFieldsForUserReportCreationOrDeletion = (body) => {
  appHelper.checkRequiredFields(['reportedUserId'], body)
  const { reportedUserId = '' } = body
  appHelper.validateId({ reportedUserId })
}

export const getReportQuery = async (params) => {
  const query = {}
  const partnerId = params.partnerId ? params.partnerId : ''
  if (size(params) && partnerId) {
    //set partner id in query
    query.partnerId = partnerId

    //set branch id in query
    if (params.branchId) query.branchId = params.branchId

    //set agent id in query
    if (params.agentId) query.agentId = params.agentId

    //set account id in query
    if (params.accountId) query.accountId = params.accountId

    //set property id in query
    if (params.propertyId) query.propertyId = params.propertyId

    //set tenant id in query
    if (params.tenantId) query.tenantId = params.tenantId

    //set last accounting period
    if (params.lastEnquiryDate)
      query.createdAt = {
        $lte: (
          await appHelper.getActualDate(partnerId, true, params.lastEnquiryDate)
        )
          .endOf('day')
          .toDate()
      }
  }

  return query
}

export const getTotalInfo = (totalInfoList, findById, compareId) =>
  find(totalInfoList, function (totalInfo) {
    return totalInfo && totalInfo._id && totalInfo._id[findById] === compareId
  })

export const getTenantName = async (tenantId) => {
  const tenantInfo = tenantId
    ? await tenantHelper.getATenant({ _id: tenantId })
    : false
  const tenantName = size(tenantInfo) && tenantInfo.name ? tenantInfo.name : ''
  return tenantName
}

export const getAccountName = async (accountId) => {
  const accountInfo = accountId
    ? await accountHelper.getAnAccount({ _id: accountId })
    : false
  const accountName = accountInfo && accountInfo.name ? accountInfo.name : ''

  return accountName
}

const getTenantsQueryForReportList = (params) => {
  const tenantsQuery = {}

  if (params) {
    if (params.tenantId) tenantsQuery._id = params.tenantId
    if (params.partnerId) tenantsQuery.partnerId = params.partnerId
    // TODO:: Implemented like V1
    if (params.lastEnquiryDate)
      tenantsQuery.createdAt = { $lte: params.lastEnquiryDate }
  }

  return tenantsQuery
}

const getAccountsQueryForReportList = async (params) => {
  const accountsQuery = {}

  if (params) {
    if (params.partnerId) accountsQuery.partnerId = params.partnerId
    if (params.branchId) accountsQuery.branchId = params.branchId
    if (params.agentId) accountsQuery.agentId = params.agentId

    const accountIds = []
    if (params.accountId) accountIds.push(params.accountId)
    if (params.propertyId && !params.accountId) {
      const property = await listingHelper.getListingById(params.propertyId)
      if (size(property) && property.accountId)
        accountIds.push(property.accountId)
    }
    if (params.tenantId && !params.accountId) {
      const tenant = await tenantHelper.getATenant({ _id: params.tenantId })
      const accountIdsFromTenants = map(tenant?.properties, 'accountId')

      if (size(accountIdsFromTenants)) accountIds.push(...accountIdsFromTenants)
    }
    if (size(accountIds)) accountsQuery._id = { $in: accountIds }
    if (params.lastEnquiryDate)
      accountsQuery.createdAt = { $lte: new Date(params.lastEnquiryDate) }
  }

  return accountsQuery
}

export const getReportsList = async (params) => {
  const query = await getReportQuery(params)
  const invoiceQuery = clone(query)
  const paymentQuery = clone(query)
  const landlordInvoiceQuery = clone(query)
  const payoutQuery = clone(query)
  const finalSettlementQuery = clone(query)
  let groupBy = { tenantId: '$tenantId' }
  let groupByField = 'tenantId'
  const reportList = []
  const totalRowInfo = { finalAccounting: true }
  const excludedSubtypes = subTypesData
  let invoiceProjectQuery = {
    tenantId: 1,
    amount: {
      $cond: {
        if: { $eq: ['$subType', 'loss_recognition'] },
        then: { $multiply: ['$amount', -1] },
        else: '$amount'
      }
    },
    createdAt: 1
  }

  invoiceQuery['$or'] = [
    { type: { $in: ['invoice', 'credit_note'] } },
    { type: 'correction', subType: { $in: ['addon', 'rounded_amount'] } }
  ]

  paymentQuery.type = { $in: ['payment', 'refund'] }
  paymentQuery.landlordPayment = { $ne: true }

  payoutQuery.type = 'payout'

  //Set landlord invoice query
  landlordInvoiceQuery['$or'] = [
    { type: 'commission' },
    { type: 'correction', subType: { $in: ['payout_addon'] } }
  ]

  finalSettlementQuery.type = 'payment'
  finalSettlementQuery.landlordPayment = true

  if (params && params.reportType === 'landlord') {
    groupBy = { accountId: '$accountId' }
    groupByField = 'accountId'

    excludedSubtypes.push('loss_recognition')

    invoiceQuery['$or'] = [
      {
        type: { $in: ['invoice', 'credit_note'] },
        subType: { $nin: excludedSubtypes }
      },
      { type: 'correction', subType: { $in: ['addon'] } }
    ]

    //Set filter in invoice project query
    invoiceProjectQuery = {
      accountId: 1,
      amount: 1,
      createdAt: 1
    }
  }

  let tenantsList = []
  let accountsList = []

  const invoicesTotalInfo = await TransactionCollection.aggregate([
    { $match: invoiceQuery },
    { $project: invoiceProjectQuery },
    {
      $group: {
        _id: groupBy,
        totalAmount: { $sum: '$amount' },
        createdAt: { $first: '$createdAt' }
      }
    }
  ])
  let paymentsTotalInfo = ''
  let landlordInvoicesTotalInfo = ''
  let payoutsTotalInfo = ''
  let finalSettlementTotalInfo = ''

  if (params && params.reportType === 'tenant') {
    const tenantsQuery = getTenantsQueryForReportList(params)
    tenantsList = await tenantHelper.getTenantsWithProjection({
      query: tenantsQuery,
      options: {
        sort: {
          name: 1
        }
      },
      projection: '_id name serial userId'
    })

    paymentsTotalInfo = await transactionHelper.getTransctionForReport(
      paymentQuery,
      groupBy
    )
  }

  if (params && params.reportType === 'landlord') {
    const accountsQuery = await getAccountsQueryForReportList(params)
    accountsList = await accountHelper.getAccountsWithProjection({
      query: accountsQuery,
      options: {
        sort: {
          name: 1
        }
      },
      projection: '_id name serial personId'
    })
    landlordInvoicesTotalInfo = await transactionHelper.getTransctionForReport(
      landlordInvoiceQuery,
      groupBy
    )

    payoutsTotalInfo = await transactionHelper.getTransctionForReport(
      payoutQuery,
      groupBy
    )

    finalSettlementTotalInfo = await transactionHelper.getTransctionForReport(
      finalSettlementQuery,
      groupBy
    )
  }

  let totalBalance = 0,
    totalRent = 0,
    totalPayments = 0,
    totalPayouts = 0,
    totalInvoiced = 0
  if (params && params.reportType === 'tenant' && size(tenantsList)) {
    for (const tenant of tenantsList) {
      const tenantId = tenant?._id || ''
      const tenantName = tenant?.name || ''
      const tenantSerial = tenant?.serial || 0
      const userData = tenant.user
      const avatarKey = userHelper.getAvatar(userData)
      const report = {}
      let newTotalDue = 0
      const invoiceInfo = size(invoicesTotalInfo)
        ? getTotalInfo(clone(invoicesTotalInfo), groupByField, clone(tenantId))
        : false
      const paymentInfo = paymentsTotalInfo
        ? getTotalInfo(clone(paymentsTotalInfo), groupByField, clone(tenantId))
        : false

      if (size(invoiceInfo) || size(paymentInfo)) {
        // Adding tenantId
        report.tenantId = tenantId
        // Adding tenant name
        report.name = tenantName
        report.serial = tenantSerial
        report.avatarKey = avatarKey
        // Adding total invoice amount
        report.invoiced = clone(invoiceInfo?.totalAmount || 0)
        totalInvoiced += clone(invoiceInfo?.totalAmount || 0)

        // Generating total due amount
        newTotalDue = clone(invoiceInfo?.totalAmount || 0)
        const newPaymentTotalAmount = paymentInfo
          ? paymentInfo?.totalAmount || 0
          : 0
        newTotalDue -= newPaymentTotalAmount
        newTotalDue =
          parseFloat(parseFloat(clone(newTotalDue) * 1).toFixed(2)) * 1 || 0

        // Adding total payments amount
        report.payments = newPaymentTotalAmount
        totalPayments += clone(newPaymentTotalAmount)

        // Adding total balance amount
        report.totalBalance = clone(newTotalDue)
        totalBalance += clone(newTotalDue)
        // transaction date
        report.createdAt = invoiceInfo?.createdAt || ''

        // Adding report to report list
        if (size(report) && (report.invoiced || report.payments))
          reportList.push(report)
      }
    }
  } else if (params && params.reportType === 'landlord' && size(accountsList)) {
    for (const account of accountsList) {
      const accountId = account?._id || ''
      const accountName = account?.name || ''
      const serialNumber = account?.serial || 0
      const userData = account.person
      const avatarKey = userHelper.getAvatar(userData)
      const report = {}
      let newTotalDue = 0
      const invoiceInfo = invoicesTotalInfo
        ? getTotalInfo(clone(invoicesTotalInfo), groupByField, clone(accountId))
        : false
      const landlordInvoiceInfo = landlordInvoicesTotalInfo
        ? getTotalInfo(
            clone(landlordInvoicesTotalInfo),
            groupByField,
            clone(accountId)
          )
        : false
      const payoutInfo = payoutsTotalInfo
        ? getTotalInfo(clone(payoutsTotalInfo), groupByField, clone(accountId))
        : false
      const finalSettlementInfo = finalSettlementTotalInfo
        ? getTotalInfo(
            clone(finalSettlementTotalInfo),
            groupByField,
            clone(accountId)
          )
        : false
      if (
        size(invoiceInfo) ||
        size(landlordInvoiceInfo) ||
        size(payoutInfo) ||
        size(finalSettlementInfo)
      ) {
        // Adding accountId
        report.accountId = accountId
        // Adding account name
        report.name = accountName
        // Account serial
        report.serial = serialNumber
        // avatar key
        report.avatarKey = avatarKey
        // Adding total rent amount
        report.rent = clone(invoiceInfo?.totalAmount || 0)
        totalRent += clone(invoiceInfo?.totalAmount || 0)

        // Generating total due amount
        newTotalDue = clone(invoiceInfo?.totalAmount || 0)
        const newLandlordInvoiceTotalAmount = landlordInvoiceInfo
          ? landlordInvoiceInfo?.totalAmount || 0
          : 0
        let newPayoutTotalAmount = payoutInfo ? payoutInfo?.totalAmount || 0 : 0
        const newFinalSettlementTotalAmount = finalSettlementInfo
          ? finalSettlementInfo?.totalAmount || 0
          : 0
        // Deduct landlord paid payment from payouts
        newPayoutTotalAmount -= newFinalSettlementTotalAmount
        newTotalDue -= newLandlordInvoiceTotalAmount
        newTotalDue -= newPayoutTotalAmount

        // Added total payouts amount
        report.payouts = newPayoutTotalAmount
        totalPayouts += clone(newPayoutTotalAmount)

        // Adding total invoiced amount for landlord
        report.invoiced = newLandlordInvoiceTotalAmount
        totalInvoiced += clone(report.invoiced)

        // Adding total balance amount
        newTotalDue =
          parseFloat(parseFloat(clone(newTotalDue) * 1).toFixed(2)) * 1
        report.totalBalance = clone(newTotalDue)
        totalBalance += clone(newTotalDue)
        // transaction date
        report.createdAt = invoiceInfo?.createdAt || ''

        // Adding report to report list
        if (size(report) && (report.rent || report.invoiced || report.payouts))
          reportList.push(report)
      }
    }
  }

  totalRowInfo.totalBalance =
    parseFloat(parseFloat(clone(totalBalance) * 1).toFixed(2)) * 1
  totalRowInfo.totalInvoiced =
    parseFloat(parseFloat(clone(totalInvoiced) * 1).toFixed(2)) * 1
  totalRowInfo.totalRent =
    parseFloat(parseFloat(clone(totalRent) * 1).toFixed(2)) * 1
  totalRowInfo.totalPayments =
    parseFloat(parseFloat(clone(totalPayments) * 1).toFixed(2)) * 1
  totalRowInfo.totalPayouts =
    parseFloat(parseFloat(clone(totalPayouts) * 1).toFixed(2)) * 1

  reportList.push(totalRowInfo)

  return JSON.parse(JSON.stringify(reportList))
}

export const reportDataForExcelCreator = async (params, skip, limit) => {
  const { partnerId = {}, userId = {}, reportType = '' } = params
  await appHelper.validateId({ partnerId })
  await appHelper.validateId({ userId })
  const reportLists = (await getReportsList(params)) || []
  //Remove last row temporary. It's totals amount of the sheet
  reportLists.pop()
  const dataList = slice(reportLists, skip, limit + skip)
  const dataCount = size(reportLists)
  const rowData = []
  if (size(dataList)) {
    for (const dataItem of dataList) {
      const itemObj = {
        name: dataItem && dataItem.name ? dataItem.name : ''
      }
      if (reportType === 'tenant') {
        itemObj.invoiced =
          size(dataItem) && dataItem.invoiced ? dataItem.invoiced : 0
        itemObj.payments = dataItem && dataItem.payments ? dataItem.payments : 0
      } else {
        itemObj.rent = dataItem && dataItem.rent ? dataItem.rent : 0
        itemObj.invoiced = dataItem && dataItem.invoiced ? dataItem.invoiced : 0
        itemObj.payouts = dataItem && dataItem.payouts ? dataItem.payouts : 0
      }
      itemObj.totalBalance =
        dataItem && dataItem.totalBalance ? dataItem.totalBalance : 0

      rowData.push(itemObj)
    }
  }

  return {
    data: rowData,
    total: dataCount
  }
}

export const queryReportsForExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  const { skip, limit } = options
  appHelper.checkRequiredFields(['queueId'], query)
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_report') {
    const reportData = await reportDataForExcelCreator(
      queueInfo.params,
      skip,
      limit
    )
    return reportData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const queryReportsForTenantBalance = async (req) => {
  const { body, user = {} } = req
  const { options, query } = body
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['partnerId'], user)
  appHelper.validateId({ partnerId })
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  const params = validateParamsForBalanceReport(query)

  const reportQuery = prepareBalanceReportQuery(params, 'tenant_report')
  const tenantBalanceReport = await balanceReportQueryForTenant(
    reportQuery,
    options
  )
  const totalDocuments = await countTenantBalanceReport({
    query: { partnerId }
  })
  const filteredDocuments = await countTenantBalanceReport(reportQuery)

  return {
    data: tenantBalanceReport,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const countTenantBalanceReport = async (reportQuery = {}) => {
  const { query = {} } = reportQuery
  const countDocument = await transactionHelper.countTransactionsForAField(
    'tenantId',
    query
  )
  return countDocument
}

const validateParamsForBalanceReport = (query) => {
  const {
    accountId,
    agentId,
    branchId,
    lastEnquiryDate,
    propertyId,
    partnerId,
    tenantId
  } = query
  const params = {}
  if (accountId) {
    appHelper.validateId({ accountId })
    params.accountId = accountId
  }
  if (partnerId) {
    appHelper.validateId({ partnerId })
    params.partnerId = partnerId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    params.agentId = agentId
  }
  if (branchId) {
    appHelper.validateId({ branchId })
    params.branchId = branchId
  }
  if (propertyId) {
    appHelper.validateId({ propertyId })
    params.propertyId = propertyId
  }
  if (tenantId) {
    appHelper.validateId({ tenantId })
    params.tenantId = tenantId
  }
  if (lastEnquiryDate) {
    params.lastEnquiryDate = lastEnquiryDate
  }

  return params
}

export const queryReportsForLandlordBalance = async (req) => {
  const { body, user = {} } = req
  const { query, options } = body
  const { userId, partnerId } = user

  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.validateSortForQuery(options.sort)
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  query.partnerId = partnerId

  const params = validateParamsForBalanceReport(query)

  const accountsQuery = await getAccountsQueryForReportList(params)
  const { transactionQuery } = prepareParamsForBalanceReportQuery(params)
  const landlordReport = await queryForLandlordBalanceReport(
    accountsQuery,
    options,
    transactionQuery
  )
  const totalDocuments = await transactionHelper.countTransactionsForAField(
    'accountId',
    { partnerId }
  )

  const reportQuery = prepareBalanceReportQuery(params, 'landlord_report')
  const filteredDocuments = await transactionHelper.countTransactionsForAField(
    'accountId',
    reportQuery.query
  )

  return {
    data: landlordReport,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const queryForLandlordBalanceReport = async (
  accountsQuery,
  options,
  transactionQuery
) => {
  const { limit, skip, sort } = options

  const excludedSubtypes = subTypesData
  excludedSubtypes.push('loss_recognition')

  const pipeline = [
    {
      $match: accountsQuery
    },
    ...transactionPipelineForLandlordBalanceReport(
      excludedSubtypes,
      transactionQuery
    ),
    ...groupPipelineForLandlordBalanceReport(excludedSubtypes),
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...userInfoPipelineForLandlordBalanceReport(),
    ...organizationInfoPipelineForLandlordBalanceReport(),
    {
      $project: {
        accountId: '$_id',
        avatarKey: {
          $cond: [
            { $eq: ['$accountType', 'person'] },
            appHelper.getUserAvatarKeyPipeline('$userInfo.profile.avatarKey'),
            appHelper.getOrganizationLogoPipeline('$organizationInfo.image')
          ]
        },
        createdAt: 1,
        invoiced: '$landlordInvoiceTotalAmount',
        name: 1,
        payouts: {
          $subtract: ['$payoutTotalAmount', '$finalSattlementTotalAmount']
        },
        rent: '$invoiceTotalAmount',
        serial: 1
      }
    },
    {
      $addFields: {
        paymentAmount: {
          $add: ['$invoiced', '$payouts']
        }
      }
    },
    {
      $addFields: {
        totalBalance: {
          $subtract: ['$rent', '$paymentAmount']
        }
      }
    }
  ]

  const landlordReport = await AccountCollection.aggregate(pipeline)

  return landlordReport || []
}

const transactionPipelineForLandlordBalanceReport = (
  excludedSubtypes,
  transactionQuery
) => [
  {
    $lookup: {
      from: 'transactions',
      localField: '_id',
      foreignField: 'accountId',
      pipeline: [
        {
          $match: {
            $expr: { $and: transactionQuery }
          }
        }
      ],
      as: 'transactionInfo'
    }
  },
  {
    $unwind: {
      path: '$transactionInfo',
      preserveNullAndEmptyArrays: false
    }
  },
  {
    $match: {
      $or: [
        { 'transactionInfo.type': { $in: ['commission', 'payout'] } },
        {
          'transactionInfo.type': 'payment',
          'transactionInfo.landlordPayment': true
        },
        {
          'transactionInfo.type': 'correction',
          'transactionInfo.subType': { $in: ['addon', 'payout_addon'] }
        },
        {
          'transactionInfo.type': { $in: ['invoice', 'credit_note'] },
          'transactionInfo.subType': {
            $nin: excludedSubtypes
          }
        }
      ]
    }
  }
]

const groupPipelineForLandlordBalanceReport = (excludedSubtypes) => [
  {
    $group: {
      _id: '$_id',
      serial: { $first: '$serial' },
      personId: { $first: '$personId' },
      organizationId: { $first: '$organizationId' },
      accountType: { $first: '$type' },
      name: { $first: '$name' },
      createdAt: { $first: '$createdAt' },
      invoiceTotalAmount: {
        $sum: {
          $cond: [
            {
              $or: [
                {
                  $and: [
                    {
                      $in: ['$transactionInfo.type', ['invoice', 'credit_note']]
                    },
                    {
                      $not: [
                        {
                          $in: ['$transactionInfo.subType', excludedSubtypes]
                        }
                      ]
                    }
                  ]
                },
                {
                  $and: [
                    { $eq: ['$transactionInfo.type', 'correction'] },
                    { $eq: ['$transactionInfo.subType', 'addon'] }
                  ]
                }
              ]
            },
            '$transactionInfo.amount',
            0
          ]
        }
      },

      // Landlord invoice total amount
      landlordInvoiceTotalAmount: {
        $sum: {
          $cond: [
            {
              $or: [
                {
                  $eq: ['$transactionInfo.type', 'commission']
                },
                {
                  $and: [
                    { $eq: ['$transactionInfo.type', 'correction'] },
                    { $eq: ['$transactionInfo.subType', 'payout_addon'] }
                  ]
                }
              ]
            },
            '$transactionInfo.amount',
            0
          ]
        }
      },

      // Payout total amount
      payoutTotalAmount: {
        $sum: {
          $cond: [
            {
              $eq: ['$transactionInfo.type', 'payout']
            },
            '$transactionInfo.amount',
            0
          ]
        }
      },

      // Final sattlement total info
      finalSattlementTotalAmount: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$transactionInfo.type', 'payment'] },
                { $eq: ['$transactionInfo.landlordPayment', true] }
              ]
            },
            '$transactionInfo.amount',
            0
          ]
        }
      }
    }
  }
]

const userInfoPipelineForLandlordBalanceReport = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'personId',
      foreignField: '_id',
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo')
]

const organizationInfoPipelineForLandlordBalanceReport = () => [
  {
    $lookup: {
      from: 'organizations',
      localField: 'organizationId',
      foreignField: '_id',
      as: 'organizationInfo'
    }
  },
  appHelper.getUnwindPipeline('organizationInfo')
]

const transactionGroupPipelineForTenantBalanceReport = () => [
  {
    $group: {
      _id: '$tenantId',
      invoiced: {
        $sum: {
          $cond: [
            {
              $ifNull: [
                {
                  $or: [
                    {
                      $in: ['$type', ['invoice', 'credit_note']]
                    },
                    {
                      $and: [
                        { $eq: ['$type', 'correction'] },
                        {
                          $in: ['$subType', ['addon', 'rounded_amount']]
                        }
                      ]
                    }
                  ]
                },
                false
              ]
            },
            '$amount',
            0
          ]
        }
      },
      // payments total
      payments: {
        $sum: {
          $cond: [
            {
              $ifNull: [{ $in: ['$type', ['payment', 'refund']] }, false]
            },
            '$amount',
            0
          ]
        }
      }
    }
  }
]

const balanceReportQueryForTenant = async (reportQuery, options) => {
  const { query } = reportQuery
  const { limit, sort, skip } = options

  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        amount: {
          $cond: {
            if: { $eq: ['$subType', 'loss_recognition'] },
            then: { $multiply: ['$amount', -1] },
            else: '$amount'
          }
        }
      }
    },
    // Group transaction
    ...transactionGroupPipelineForTenantBalanceReport(),
    {
      $lookup: {
        from: 'tenants',
        localField: '_id',
        foreignField: '_id',
        as: 'tenantInfo'
      }
    },
    appHelper.getUnwindPipeline('tenantInfo'),
    {
      $project: {
        createdAt: '$tenantInfo.createdAt',
        invoiced: 1,
        name: '$tenantInfo.name',
        payments: 1,
        serial: '$tenantInfo.serial',
        tenantId: '$tenantInfo._id',
        totalBalance: { $subtract: ['$invoiced', '$payments'] },
        userId: '$tenantInfo.userId'
      }
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
    {
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        pipeline: [
          {
            $project: {
              avatarKey:
                appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
            }
          }
        ],
        as: 'userInfo'
      }
    },
    {
      $unwind: {
        path: '$userInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        avatarKey: '$userInfo.avatarKey'
      }
    }
  ]

  const tenantBalanceReport = await TransactionCollection.aggregate(pipeline)

  return tenantBalanceReport || []
}

const prepareParamsForBalanceReportQuery = (params) => {
  const query = {}
  const transactionQuery = []
  const partnerId = params.partnerId ? params.partnerId : ''
  if (size(params) && partnerId) {
    //set partner id in query
    query.partnerId = partnerId
    transactionQuery.push({ $eq: ['$partnerId', partnerId] })
    if (params.branchId) {
      query.branchId = params.branchId
      transactionQuery.push({ $eq: ['$branchId', params.branchId] })
    }
    if (params.agentId) {
      query.agentId = params.agentId
      transactionQuery.push({ $eq: ['$agentId', params.agentId] })
    }
    if (params.accountId) {
      query.accountId = params.accountId
      transactionQuery.push({ $eq: ['$accountId', params.accountId] })
    }
    if (params.propertyId) {
      query.propertyId = params.propertyId
      transactionQuery.push({ $eq: ['$propertyId', params.propertyId] })
    }
    if (params.tenantId) {
      query.tenantId = params.tenantId
      transactionQuery.push({ $eq: ['$tenantId', params.tenantId] })
    }
    if (params.lastEnquiryDate) {
      query.createdAt = { $lte: new Date(params.lastEnquiryDate) }
      transactionQuery.push({
        $lte: ['$createdAt', new Date(params.lastEnquiryDate)]
      })
    }
  }
  return { transactionQuery, query }
}

const prepareBalanceReportQuery = (params, context = '') => {
  const { query } = prepareParamsForBalanceReportQuery(params)
  if (context === 'landlord_report') {
    const excludedSubtypes = subTypesData
    excludedSubtypes.push('loss_recognition')

    query['$or'] = [
      { type: { $in: ['commission', 'payout'] } },
      {
        type: 'payment',
        landlordPayment: true
      },
      { type: 'correction', subType: { $in: ['addon', 'payout_addon'] } },
      {
        type: { $in: ['invoice', 'credit_note'] },
        subType: {
          $nin: excludedSubtypes
        }
      }
    ]
  } else {
    query['$or'] = [
      { type: { $in: ['invoice', 'credit_note'] } },
      {
        $and: [
          { type: 'correction' },
          { subType: { $in: ['addon', 'rounded_amount'] } }
        ]
      },
      {
        $and: [
          { type: { $in: ['payment', 'refund'] } },
          { landlordPayment: { $ne: true } }
        ]
      }
    ]
  }
  return { query }
}

const getTenantBalanceReportSummary = async (query) => {
  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        amount: {
          $cond: {
            if: { $eq: ['$subType', 'loss_recognition'] },
            then: { $multiply: ['$amount', -1] },
            else: '$amount'
          }
        }
      }
    },
    {
      $group: {
        _id: null,
        totalInvoiced: {
          $sum: {
            $cond: [
              {
                $or: [
                  {
                    $in: ['$type', ['invoice', 'credit_note']]
                  },
                  {
                    $and: [
                      { $eq: ['$type', 'correction'] },
                      {
                        $in: ['$subType', ['addon', 'rounded_amount']]
                      }
                    ]
                  }
                ]
              },
              '$amount',
              0
            ]
          }
        },
        // payments total
        totalPayments: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $in: ['$type', ['payment', 'refund']] },
                  { $not: { $eq: ['$landlordPayment', true] } }
                ]
              },
              '$amount',
              0
            ]
          }
        }
      }
    },
    {
      $addFields: {
        totalBalance: { $subtract: ['$totalInvoiced', '$totalPayments'] }
      }
    }
  ]
  const [summary = {}] = await TransactionCollection.aggregate(pipeline)

  return summary
}

export const tenantBalanceReportSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const { query } = prepareBalanceReportQuery(body, 'tenant_report')
  const reportSummary = await getTenantBalanceReportSummary(query)
  return reportSummary
}

const getLandlordBalanceReportSummary = async (
  accountsQuery,
  transactionQuery
) => {
  const excludedSubtypes = subTypesData
  excludedSubtypes.push('loss_recognition')

  const pipeline = [
    {
      $match: accountsQuery
    },
    ...transactionPipelineForLandlordBalanceReport(
      excludedSubtypes,
      transactionQuery
    ),
    {
      $group: {
        _id: null,
        invoiceTotalAmount: {
          $sum: {
            $cond: [
              {
                $or: [
                  {
                    $and: [
                      {
                        $in: [
                          '$transactionInfo.type',
                          ['invoice', 'credit_note']
                        ]
                      },
                      {
                        $not: [
                          {
                            $in: ['$transactionInfo.subType', excludedSubtypes]
                          }
                        ]
                      }
                    ]
                  },
                  {
                    $and: [
                      { $eq: ['$transactionInfo.type', 'correction'] },
                      { $eq: ['$transactionInfo.subType', 'addon'] }
                    ]
                  }
                ]
              },
              '$transactionInfo.amount',
              0
            ]
          }
        },

        // Landlord invoice total amount
        landlordInvoiceTotalAmount: {
          $sum: {
            $cond: [
              {
                $or: [
                  {
                    $eq: ['$transactionInfo.type', 'commission']
                  },
                  {
                    $and: [
                      { $eq: ['$transactionInfo.type', 'correction'] },
                      { $eq: ['$transactionInfo.subType', 'payout_addon'] }
                    ]
                  }
                ]
              },
              '$transactionInfo.amount',
              0
            ]
          }
        },

        // Payout total amount
        payoutTotalAmount: {
          $sum: {
            $cond: [
              {
                $eq: ['$transactionInfo.type', 'payout']
              },
              '$transactionInfo.amount',
              0
            ]
          }
        },

        // Final sattlement total info
        finalSattlementTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$transactionInfo.type', 'payment'] },
                  { $eq: ['$transactionInfo.landlordPayment', true] }
                ]
              },
              '$transactionInfo.amount',
              0
            ]
          }
        }
      }
    },
    {
      $project: {
        totalInvoiced: '$landlordInvoiceTotalAmount',
        totalPayouts: {
          $subtract: ['$payoutTotalAmount', '$finalSattlementTotalAmount']
        },
        totalRent: '$invoiceTotalAmount'
      }
    },
    {
      $addFields: {
        paymentAmount: {
          $add: ['$totalPayouts', '$totalInvoiced']
        }
      }
    },
    {
      $addFields: {
        totalBalance: {
          $subtract: ['$totalRent', '$paymentAmount']
        }
      }
    }
  ]

  const [landlordReport = {}] = await AccountCollection.aggregate(pipeline)
  return landlordReport
}

export const landlordBalanceReportSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const accountsQuery = await getAccountsQueryForReportList(body)
  const { transactionQuery } = prepareParamsForBalanceReportQuery(body)
  return await getLandlordBalanceReportSummary(accountsQuery, transactionQuery)
}
