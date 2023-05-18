import { map, sortedUniq, compact } from 'lodash'
import moment from 'moment-timezone'

import {
  appHealthHelper,
  appHelper,
  contractHelper,
  depositAccountHelper,
  depositInsuranceHelper,
  listingHelper,
  notificationLogHelper,
  partnerHelper,
  partnerSettingHelper,
  partnerUsageHelper,
  paymentHelper,
  payoutHelper,
  userHelper
} from '../helpers'
import { IntegrationCollection, InvoiceCollection } from '../models'

// This function must be called from appropriate position
export const preparePipelineForPartner = (pipeline = [], partnerType = '') => {
  const lookup = {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  }
  const finalMatch = {
    $match: {
      'partner.accountType': partnerType
    }
  }
  if (partnerType !== 'all') pipeline.push(lookup, finalMatch)
}

export const getUnitPerAgentInfo = async (queryData) => {
  const requiredFields = ['dateRange', 'partnerType']
  appHelper.checkRequiredFields(requiredFields, queryData)
  const { dateRange } = queryData
  const { startDate, endDate } =
    (await appHelper.autoDateGenerator({
      eventName: dateRange
    })) || {}
  const query = {}
  if (startDate && endDate) {
    query.createdAt = { $gte: startDate, $lte: endDate }
  }
  const params = { ...queryData, query }
  const upaInfo = await partnerUsageHelper.getDashboardUPAForPartner(params)
  const { finalSummary, upaForPartners } = upaInfo
  return {
    countedTotalAgents: finalSummary[0]?.countedTotalAgents || 0,
    countedTotalProperties: finalSummary[0]?.countedTotalProperties || 0,
    countedTotalPartners: finalSummary[0]?.countedTotalPartners || 0,
    totalUpaOfPartners: finalSummary[0]?.totalUpaOfPartners || 0,
    upaForPartners
  }
}

export const getUnitPerAgentGraphData = async (queryData) => {
  appHelper.checkRequiredFields(['dateRange', 'partnerType'], queryData)
  const { dateRange, partnerType } = queryData
  const query = {}
  const { startDate, endDate } =
    (await appHelper.autoDateGenerator({
      eventName: dateRange
    })) || {}
  if (startDate && endDate) {
    query.createdAt = { $gte: startDate, $lte: endDate }
  }
  const upaGraphData = await partnerUsageHelper.getUPAGraphDataForDashboard(
    query,
    dateRange,
    partnerType
  )

  return upaGraphData
}

export const getAppHealthInfo = async (queryData) => {
  appHelper.checkRequiredFields(['partnerType'], queryData)
  const { createdAtDateRange, partnerType } = queryData
  const { startDate, endDate } = createdAtDateRange || {}

  const query = {
    $or: [
      { type: 'transaction' },
      { type: 'sqs' },
      { type: 'notifications' },
      { type: 'accuracy', context: { $in: ['invoice', 'payout'] } }
    ],
    createdAt: { $gte: new Date(startDate), $lte: new Date(endDate) }
  }
  console.log('query', query)
  const transactionInfo = await appHealthHelper.getAppHeathInfoForDashboard(
    query,
    partnerType
  )
  return (
    transactionInfo || {
      totalTransactionErrors: 0,
      totalTransactions: 0,
      totalSQSErrors: 0,
      totalSQSs: 0,
      totalNotificationErrors: 0,
      totalNotifications: 0,
      totalInvoiceAndPayoutErrors: 0,
      totalInvoicesAndPayouts: 0
    }
  )
}

export const getUserInfo = async () => {
  const countedUsers = await userHelper.countUsers({})
  return { countedUsers }
}

export const getFailedPayoutInfo = async (queryData) => {
  appHelper.checkRequiredFields(['partnerType'], queryData)
  const { partnerType } = queryData
  const failedPayoutInfo = await payoutHelper.getFailedPayoutInfoForDashboard(
    partnerType
  )
  return failedPayoutInfo || { countedFailedPayouts: 0 }
}

export const getListingInfo = async (queryData) => {
  appHelper.checkRequiredFields(['partnerType'], queryData)
  const { partnerType } = queryData
  const query = { $or: [{ listed: true }, { 'finn.isShareAtFinn': true }] }
  const listingInfo = await listingHelper.getListingInfoForDashboard(
    query,
    partnerType
  )
  return (
    listingInfo || {
      countedFinnListings: 0,
      countedUniteListings: 0,
      countedTotalListings: 0
    }
  )
}

export const getActivePartnerInfo = async (queryData) => {
  appHelper.checkRequiredFields(['dateRange', 'partnerType'], queryData)
  const { dateRange, partnerType } = queryData
  const query = {}
  if (partnerType !== 'all') query.accountType = partnerType
  const { startDate, endDate } =
    (await appHelper.autoDateGenerator({
      eventName: dateRange
    })) || {}
  if (startDate && endDate) query.createdAt = { $gte: startDate, $lte: endDate }

  const activePartnerInfo =
    await partnerHelper.getActivePartnerInfoForDashboard(query, dateRange)
  return (
    activePartnerInfo || {
      countedTotalPartners: 0,
      countedBrokerPartners: 0,
      countedDirectPartners: 0,
      activePartnerGraphData: []
    }
  )
}

export const getActivePropertyInfo = async (queryData) => {
  appHelper.checkRequiredFields(['dateRange', 'partnerType'], queryData)
  const { dateRange, partnerType } = queryData
  const query = {}
  const { startDate, endDate } =
    (await appHelper.autoDateGenerator({
      eventName: dateRange
    })) || {}
  if (startDate && endDate) {
    query.createdAt = { $gte: startDate, $lte: endDate }
  }
  const activePropertyInfo =
    await partnerUsageHelper.getActivePropertyInfoForDashboard(
      query,
      partnerType,
      dateRange
    )
  return {
    countedProperties: appHelper.convertToInt(
      activePropertyInfo?.countedProperties || 0
    ),
    activePropertyGraphData: activePropertyInfo?.activePropertyGraphData || []
  }
}

const prepareQueryForPogoIntegratedPartnersCount = (accountType) => {
  const preparedQuery = {
    type: 'power_office_go',
    status: 'integrated'
  }
  if (accountType === 'broker') {
    preparedQuery.accountId = { $exists: false }
  } else if (accountType === 'direct') {
    preparedQuery.accountId = { $exists: true }
  }
  return preparedQuery
}

export const getPartnerUsageInfo = async (queryData) => {
  appHelper.checkRequiredFields(['dateRange', 'partnerType'], queryData)
  const { dateRange, partnerType } = queryData
  const query = {}
  const { startDate, endDate } =
    (await appHelper.autoDateGenerator({
      eventName: dateRange
    })) || {}
  if (startDate && endDate) {
    query.createdAt = { $gte: startDate, $lte: endDate }
  }
  const partnerUsageInfo =
    await partnerUsageHelper.getPartnerUsageInfoForDashboard(query, partnerType)
  const [agentInfo = {}] = partnerUsageInfo.agent
  const [otherInfo = {}] = partnerUsageInfo.other
  const preparedQuery = prepareQueryForPogoIntegratedPartnersCount(partnerType)
  const countedPogo = await partnerHelper.countIntegratedPartners({
    ...preparedQuery,
    ...query
  })
  return {
    countedActiveAgents: appHelper.convertToInt(
      agentInfo.countedActiveAgents || 0
    ),
    countedCompello: otherInfo.countedCompello || 0,
    countedEsigns: otherInfo.countedEsigns || 0,
    countedSms: otherInfo.countedSms || 0,
    countedVipps: otherInfo.countedVipps || 0,
    countedDeposits: otherInfo.countedDeposits || 0,
    countedFinns: otherInfo.countedFinns || 0,
    countedCreditRatings: otherInfo.countedCreditRatings || 0,
    countedPogo
  }
}

const getPartnerUseInPogoGraphDataForDashboard = async (
  query,
  dateRange,
  partnerType
) => {
  delete query.type
  const preparedQuery = prepareQueryForPogoIntegratedPartnersCount(partnerType)
  const graphData = await IntegrationCollection.aggregate([
    {
      $match: {
        ...preparedQuery
      }
    },
    {
      $sort: {
        createdAt: 1
      }
    },
    {
      $group: {
        _id: '$partnerId',
        createdAt: { $first: '$createdAt' }
      }
    },
    {
      $match: {
        ...query
      }
    },
    {
      $group: {
        _id: {
          $dateToString: {
            date: '$createdAt',
            format: appHelper.getDateFormatString(dateRange)
          }
        },
        countedTotal: {
          $sum: 1
        }
      }
    },
    {
      $project: {
        _id: 0,
        date: '$_id',
        countedTotal: 1
      }
    }
  ])
  return graphData
}

export const getPartnerUsageGraphData = async (queryData) => {
  const requiredFields = ['dateRange', 'partnerType', 'partnerUsageType']
  appHelper.checkRequiredFields(requiredFields, queryData)
  const { dateRange, partnerType, partnerUsageType } = queryData
  const query = { type: partnerUsageType }
  const { startDate, endDate } =
    (await appHelper.autoDateGenerator({
      eventName: dateRange
    })) || {}
  if (startDate && endDate) {
    query.createdAt = { $gte: startDate, $lte: endDate }
  }
  let graphData = []
  if (partnerUsageType === 'pogo') {
    graphData = await getPartnerUseInPogoGraphDataForDashboard(
      query,
      dateRange,
      partnerType
    )
  } else {
    graphData = await partnerUsageHelper.getPartnerUsageGraphDataForDashboard(
      query,
      dateRange,
      partnerType
    )
  }
  return graphData
}

export const getRetentionRate = async (queryData) => {
  appHelper.checkRequiredFields(['partnerType'], queryData)
  const { partnerType } = queryData
  const retentionRate = await contractHelper.getRetentionRateForDashboard(
    partnerType
  )
  const retentionRateForMonths = []
  if (retentionRate) {
    for (let i = 2; i <= 13; i++) {
      retentionRateForMonths.push({
        month: moment()
          .subtract(13 - i, 'month')
          .format('MMM'),
        rate:
          (retentionRate[`month${i}pep`] - retentionRate[`month${i}npp`]) /
          (retentionRate[`month${i - 1}pep`] || 1), // Formula: retentionRate = (PEP - NPP) / PSP
        npp: retentionRate[`month${i}npp`],
        pep: retentionRate[`month${i}pep`],
        psp: retentionRate[`month${i - 1}pep`] // PSP = PEP of previous month
      })
    }
  }
  return retentionRateForMonths
}

export const prepareQueryForPartnerDashboard = (params = {}) => {
  const { accountId, agentId, branchId, partnerId, propertyId } = params
  const query = {
    partnerId
  }
  if (accountId) query.accountId = accountId
  if (agentId) query.agentId = agentId
  if (branchId) query.branchId = branchId
  if (propertyId) query.propertyId = propertyId
  return query
}

const getMovingInOutStatus = async (query) => {
  let resultData = {}
  const { partnerId } = query
  const partnerSettingsInfo = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const isEnableMovingInOutProtocol =
    partnerSettingsInfo?.propertySettings?.movingInOutProtocol || false
  if (isEnableMovingInOutProtocol) {
    const awaitingSigningStatus =
      await listingHelper.getMovingInOutAwaitingSigningStatus(query)
    const notCreatedMovingInOutStatus =
      await contractHelper.getNotCreatedMovingInOutLease(
        partnerSettingsInfo,
        query
      )
    resultData = {
      ...awaitingSigningStatus,
      ...notCreatedMovingInOutStatus
    }
  }
  return resultData
}

export const getPartnerDashboardAwaitingStatus = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const preparedQuery = prepareQueryForPartnerDashboard(body)

  const awaitingAssignmentAndLeaseSigning =
    await contractHelper.getAssignmentAndLeaseESignStatus(preparedQuery)
  const movingStatus = await getMovingInOutStatus(preparedQuery)
  const deposits = await depositAccountHelper.getDepositAmountAwaitStatus(
    preparedQuery
  )
  const depositInsuranceStatus =
    await depositInsuranceHelper.getDepositInsuranceStatusForPartnerDashboard(
      preparedQuery
    )
  const paymentStatus = await paymentHelper.getPaymentStatusForPartnerDashboard(
    preparedQuery
  )
  const payoutStatus = await payoutHelper.getPayoutStatusForPartnerDashboard(
    preparedQuery
  )
  // TODO:: Need to write test cases for added new field.
  // depositInsuranceStatus - awaitingDepositInsuranceCount, partiallyPaidDepositInsuranceCount
  // payoutStatus - pausedPayoutsCount, waitingForSignaturePayoutCount
  // paymentStatus - paymentAwaitingSignatureCount
  // deposits - partiallyPaidDepositAccountCount

  return {
    awaitingAssignmentAndLeaseSigning,
    depositStatus: {
      ...deposits,
      ...depositInsuranceStatus
    },
    movingStatus,
    payments: {
      ...payoutStatus,
      ...paymentStatus
    }
  }
}

const getAllFailedStatus = async (query) => {
  const { partnerId } = query
  const partnerSettingInfo = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const compareDate = (await appHelper.getActualDate(partnerSettingInfo, true))
    .subtract(7, 'days')
    .startOf('day')
    .toDate()
  const totalFailedPayouts = await payoutHelper.countPayouts({
    ...query,
    status: 'failed',
    sentToNETSOn: {
      $gt: compareDate
    }
  })
  const totalFailedRefunds = await paymentHelper.countPayments({
    ...query,
    type: 'refund',
    refundStatus: 'failed',
    createdAt: {
      $gt: compareDate
    }
  })
  const totalFailedEmails = await notificationLogHelper.countNotificationLogs({
    ...query,
    type: 'email',
    status: { $in: ['rejected', 'bounced', 'soft-bounced', 'deferred'] },
    createdAt: { $gt: compareDate }
  })
  const totalFailedSms = await notificationLogHelper.countNotificationLogs({
    ...query,
    type: 'sms',
    status: 'failed',
    createdAt: { $gt: compareDate }
  })
  return {
    totalFailedEmails,
    totalFailedPayouts,
    totalFailedRefunds,
    totalFailedSms
  }
}

export const getPartnerDashboardFailedStatus = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const preparedQuery = prepareQueryForPartnerDashboard(body)
  return await getAllFailedStatus(preparedQuery)
}

const getChartLabelsInfo = (chartInfo, periodName = 'thisMonth') => {
  let labels = []
  if (periodName === 'thisMonth' || periodName === 'lastMonth') {
    const lastDateOfMonth = moment().endOf('month').toDate().getDate()
    for (let i = 0; i < lastDateOfMonth; i++) {
      labels[i] = i + 1
    }
  } else if (periodName === 'thisYear' || periodName === 'lastYear') {
    labels = [
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December'
    ]
  } else if (periodName === 'total') {
    labels = map(chartInfo.labelsData, 'label')
    labels = sortedUniq(compact(labels))
  }
  return labels
}

const prepareQueryForPartnerDashboardChartInfo = (params, dateRange) => {
  const {
    accountId,
    agentId,
    branchId,
    filterByDateOption,
    partnerId,
    periodName,
    propertyId
  } = params
  const query = {
    partnerId
  }

  if (accountId) query.accountId = accountId
  if (agentId) query.agentId = agentId
  if (branchId) query.branchId = branchId
  if (propertyId) query.propertyId = propertyId
  if (periodName && periodName !== 'total') {
    query[filterByDateOption] = {
      $gte: dateRange.startDate,
      $lte: dateRange.endDate
    }
  }
  return query
}

const prepareLabelForChart = (filterByDate, periodName, timeZone) => {
  let labelTime = {}
  if (periodName === 'thisMonth' || periodName === 'lastMonth') {
    labelTime = {
      $dayOfMonth: { date: filterByDate, timezone: timeZone }
    }
  } else if (periodName === 'thisYear' || periodName === 'lastYear') {
    labelTime = {
      $month: { date: filterByDate, timezone: timeZone }
    }
  } else if (periodName === 'total') {
    labelTime = {
      $dateToString: {
        date: filterByDate,
        format: '%Y',
        timezone: timeZone
      }
    }
  }
  return labelTime
}

const getProjectPipelineForPartnerDashboardChart = () => [
  {
    $project: {
      totalInvoiceAmount: {
        $cond: [
          {
            $in: ['$invoiceType', ['credit_note', 'invoice']]
          },
          {
            $sum: { $ifNull: ['$invoiceTotal', 0] }
          },
          0
        ]
      },
      totalInvoiceDueAmount: {
        $cond: [
          {
            $and: [{ $eq: ['$invoiceType', 'invoice'] }]
          },
          {
            $subtract: [
              {
                $sum: [
                  { $ifNull: ['$invoiceTotal', 0] },
                  { $ifNull: ['$creditedAmount', 0] }
                ]
              },
              {
                $sum: [
                  { $ifNull: ['$totalPaid', 0] },
                  { $ifNull: ['$lostMeta.amount', 0] }
                ]
              }
            ]
          },
          0
        ]
      },
      totalInvoiceOverDueAmount: {
        $cond: [
          {
            $and: [
              { $eq: ['$invoiceType', 'invoice'] },
              { $eq: ['$status', 'overdue'] }
            ]
          },
          {
            $subtract: [
              {
                $sum: [
                  { $ifNull: ['$invoiceTotal', 0] },
                  { $ifNull: ['$creditedAmount', 0] }
                ]
              },
              { $ifNull: ['$totalPaid', 0] }
            ]
          },
          0
        ]
      },
      totalPaid: {
        $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$totalPaid', 0]
      },
      lostMeta: {
        $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$lostMeta', 0]
      },
      landLordTotalInvoiceAmount: {
        $cond: [
          {
            $in: ['$invoiceType', ['landlord_invoice', 'landlord_credit_note']]
          },
          { $ifNull: ['$invoiceTotal', 0] },
          0
        ]
      },
      createdAt: 1,
      dueDate: 1
    }
  }
]

const queryChartInfoDataForPartnerDashboard = async (
  query,
  pipelineLabelData
) => {
  const pipeline = [
    {
      $match: query || {}
    },
    ...getProjectPipelineForPartnerDashboardChart(),
    {
      $addFields: {
        label: pipelineLabelData
      }
    },
    {
      $group: {
        _id: {
          label: '$label'
        },
        partnerId: {
          $first: '$partnerId'
        },
        totalInvoiceAmount: { $sum: '$totalInvoiceAmount' },
        totalInvoiceDueAmount: { $sum: '$totalInvoiceDueAmount' },
        totalInvoiceOverDueAmount: { $sum: '$totalInvoiceOverDueAmount' },
        totalPaidAmount: { $sum: '$totalPaid' },
        totalLostAmount: { $sum: '$lostMeta.amount' },
        landLordTotalInvoiceAmount: { $sum: '$landLordTotalInvoiceAmount' }
      }
    },
    {
      $sort: {
        '_id.label': 1
      }
    },
    {
      $group: {
        _id: null,
        partnerId: {
          $first: '$partnerId'
        },
        totalInvoiceAmount: { $sum: '$totalInvoiceAmount' },
        totalInvoiceDueAmount: { $sum: '$totalInvoiceDueAmount' },
        totalInvoiceOverDueAmount: { $sum: '$totalInvoiceOverDueAmount' },
        totalLostAmount: { $sum: '$totalLostAmount' },
        totalPaidAmount: { $sum: '$totalPaidAmount' },
        landLordTotalInvoiceAmount: { $sum: '$landLordTotalInvoiceAmount' },
        labels: {
          $push: {
            label: '$_id.label',
            landLordTotalInvoiceAmount: '$landLordTotalInvoiceAmount',
            totalInvoiceAmount: '$totalInvoiceAmount',
            totalInvoiceDueAmount: '$totalInvoiceDueAmount',
            totalInvoiceOverDueAmount: '$totalInvoiceOverDueAmount',
            totalLostAmount: '$totalLostAmount',
            totalPaidAmount: '$totalPaidAmount'
          }
        }
      }
    }
  ]
  const result = await InvoiceCollection.aggregate(pipeline)
  const [chartInfo = {}] = result || []
  const {
    labels = [],
    landLordTotalInvoiceAmount = 0,
    totalInvoiceAmount = 0,
    totalInvoiceDueAmount = 0,
    totalInvoiceOverDueAmount = 0,
    totalLostAmount = 0,
    totalPaidAmount = 0
  } = chartInfo

  return {
    labelsData: labels,
    landLordTotalInvoiceAmount,
    totalInvoiceAmount,
    totalInvoiceDueAmount,
    totalInvoiceOverDueAmount,
    totalLostAmount,
    totalPaidAmount
  }
}

const prepareParamDataForChartInfo = async (body) => {
  const { filterByDateOption, partnerId, periodName } = body

  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const timeZone = partnerSettings?.dateTimeSettings?.timezone
    ? partnerSettings.dateTimeSettings.timezone
    : 'Europe/Oslo'
  const filterByDate = filterByDateOption
    ? '$' + filterByDateOption
    : '$createdAt'
  const pipelineLabelData = prepareLabelForChart(
    filterByDate,
    periodName,
    timeZone
  )
  const dateRange =
    (await appHelper.autoDateGenerator({
      eventName: periodName,
      partnerIdOrSettings: partnerSettings
    })) || {}
  const query = prepareQueryForPartnerDashboardChartInfo(body, dateRange)
  return { dateRange, timeZone, pipelineLabelData, query }
}

const prepareChartInfoData = (chartInfo, periodName) => {
  const labelsInfo = getChartLabelsInfo(chartInfo, periodName)
  const labels = labelsInfo
  const { labelsData } = chartInfo

  const result = {}
  const invoiceDueData = []
  const invoiceLostData = []
  const invoicePaidData = []

  let position = 0
  for (let i = 1; i <= labels.length; i++) {
    let findInvoiceInfo
    if (periodName === 'total') {
      findInvoiceInfo = labelsData.find((info) => labels[i - 1] === info.label)
      if (findInvoiceInfo) {
        invoicePaidData[position] = findInvoiceInfo.totalPaidAmount
        invoiceDueData[position] = findInvoiceInfo.totalInvoiceDueAmount
        invoiceLostData[position] = findInvoiceInfo.totalLostAmount
      } else {
        invoicePaidData[position] = 0
        invoiceDueData[position] = 0
        invoiceLostData[position] = 0
      }
    } else {
      findInvoiceInfo = labelsData.find((info) => i === info.label)
      if (findInvoiceInfo) {
        invoicePaidData[position] = findInvoiceInfo.totalPaidAmount
        invoiceDueData[position] = findInvoiceInfo.totalInvoiceDueAmount
        invoiceLostData[position] = findInvoiceInfo.totalLostAmount
      } else {
        invoicePaidData[position] = 0
        invoiceDueData[position] = 0
        invoiceLostData[position] = 0
      }
    }
    position = position + 1
  }

  result.totalInvoiced = chartInfo.totalInvoiceAmount
  result.totalDue = chartInfo.totalInvoiceDueAmount
  result.totalOverDue = chartInfo.totalInvoiceOverDueAmount
  result.totalLandlordInvoices = chartInfo.landLordTotalInvoiceAmount
  result.labels = labels
  result.invoicePaidChartData = invoicePaidData
  result.invoiceDueChartData = invoiceDueData
  result.invoiceLostChartData = invoiceLostData
  return result
}

export const getPartnerDashboardChartInfo = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId

  const { filterByDateOption, periodName } = body
  if (!filterByDateOption) body.filterByDateOption = 'createdAt'
  if (!periodName) body.periodName = 'thisMonth'

  const { pipelineLabelData, query } = await prepareParamDataForChartInfo(body)
  const chartInfo = await queryChartInfoDataForPartnerDashboard(
    query,
    pipelineLabelData
  )
  return prepareChartInfoData(chartInfo, periodName)
}
