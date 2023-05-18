import { has, indexOf, intersection, isArray, size } from 'lodash'

import { PowerOfficeLogCollection, TenantCollection } from '../models'
import {
  accountHelper,
  appHelper,
  integrationHelper,
  partnerHelper,
  partnerSettingHelper,
  tenantHelper,
  transactionHelper
} from '../helpers'

export const getPowerOfficeLog = async (query, options, session) => {
  const powerOfficeLogs = await PowerOfficeLogCollection.find(
    query,
    options
  ).session(session)
  return powerOfficeLogs
}

export const getAPowerOfficeLog = async (query, session) => {
  const powerOfficeLog = await PowerOfficeLogCollection.findOne(query).session(
    session
  )
  return powerOfficeLog
}

export const countPowerOfficeLogs = async (query) => {
  const totalPowerOfficeLog = await PowerOfficeLogCollection.countDocuments(
    query
  )
  return totalPowerOfficeLog
}

//For lambda accounting bridge pogo #10175
export const getPowerOfficeLogQuery = (params) => {
  const query = {}
  if (size(params._id)) query._id = params._id
  if (size(params.partnerId)) query.partnerId = params.partnerId
  if (size(params.accountId)) query.accountId = params.accountId
  if (size(params.tenantId)) query.tenantId = params.tenantId
  if (size(params.type)) query.type = params.type
  if (size(params.status)) query.status = params.status
  if (size(params.powerOfficeLogIds))
    query._id = { $in: params.powerOfficeLogIds }

  return query
}

export const getPowerOfficeLogUpdateData = async (
  data,
  powerOfficeLogQuery,
  session
) => {
  const updateData = {}

  if (size(data) && size(data.status)) updateData.status = data.status
  if (size(data) && size(data.accountId)) updateData.accountId = data.accountId
  if (size(data) && size(data.tenantId)) updateData.tenantId = data.tenantId
  if (size(data) && size(data.transactionIds))
    updateData.transactionIds = data.transactionIds
  if (size(data) && size(data.type)) updateData.type = data.type
  if (size(data) && size(data.errorType)) updateData.errorType = data.errorType
  if (size(data) && has(data, 'hasError')) updateData.hasError = data.hasError
  if (size(data) && size(data.powerOfficeId))
    updateData.powerOfficeId = data.powerOfficeId
  if (size(data) && size(data.retries)) updateData.retries = data.retries
  if (size(data) && size(data.powerOfficeVoucherId))
    updateData.powerOfficeVoucherId = data.powerOfficeVoucherId
  if (size(data) && size(data.processingAt))
    updateData.processingAt = data.processingAt
  if (size(data) && size(data.errors)) updateData.errorsMeta = data.errors
  if (size(data) && size(data.transactionDate))
    updateData.transactionDate = data.transactionDate

  //Update powerOfficeEvents array
  const newStatus =
    size(data) &&
    isArray(data.powerOfficeEvents) &&
    size(data.powerOfficeEvents[0].status)
      ? data.powerOfficeEvents[0].status
      : ''
  const statusList = ['new', 'processing', 'processed']
  if (size(newStatus) && indexOf(statusList, newStatus) !== -1) {
    const powerOfficeLogInfo =
      (await getAPowerOfficeLog(powerOfficeLogQuery, session)) || {}
    let powerOfficeEvents =
      size(powerOfficeLogInfo) && size(powerOfficeLogInfo.powerOfficeEvents)
        ? powerOfficeLogInfo.powerOfficeEvents
        : []

    //Add new event createAt for event status
    const newObject = {
      status: newStatus,
      createdAt: new Date()
    }

    powerOfficeEvents = [newObject, ...powerOfficeEvents]
    updateData.powerOfficeEvents = powerOfficeEvents
  }

  return updateData
}

export const createPowerOfficeLogFieldNameForApi = (powerOfficeLog) => {
  const powerOfficeLogData = {
    _id: powerOfficeLog._id,
    status: powerOfficeLog.status,
    type: powerOfficeLog.type,
    errorType: powerOfficeLog.errorType,
    tenantId: powerOfficeLog.tenantId,
    accountId: powerOfficeLog.accountId,
    transactionIds: powerOfficeLog.transactionIds,
    powerOfficeId: powerOfficeLog.powerOfficeId,
    hasError: powerOfficeLog.hasError,
    errors: powerOfficeLog.errorsMeta,
    retries: powerOfficeLog.retries,
    powerOfficeEvents: powerOfficeLog.powerOfficeEvents,
    powerOfficeVoucherId: powerOfficeLog.powerOfficeVoucherId,
    processingAt: powerOfficeLog.processingAt
  }

  return powerOfficeLogData
}

//For lambda accounting bridge pogo #10175
export const queryPowerOfficeLog = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const query = getPowerOfficeLogQuery(body)
  const powerOfficeLogs = await getPowerOfficeLog(query)
  const powerOfficeLogsDataForApi = []
  for (const powerOfficeLog of powerOfficeLogs) {
    powerOfficeLogsDataForApi.push(
      createPowerOfficeLogFieldNameForApi(powerOfficeLog)
    )
  }
  return {
    data: powerOfficeLogsDataForApi,
    metaData: {
      filteredDocuments: powerOfficeLogs.length
    }
  }
}

const getAccountLogForPOGO = async (query = {}) => {
  const totalAccountQuery = {
    ...query,
    serial: { $exists: true }
  }
  const modifiedAccountQuery = {
    ...totalAccountQuery,
    'powerOffice.syncedAt': { $exists: true },
    lastUpdate: { $exists: true },
    $expr: {
      $gte: ['$lastUpdate', '$powerOffice.syncedAt']
    }
  }
  const totalAccounts = await accountHelper.countAccounts(totalAccountQuery)
  const totalModifiedAccounts = await accountHelper.countAccounts(
    modifiedAccountQuery
  )

  return {
    totalModifiedAccounts,
    totalAccounts
  }
}

const getTenantLogForPOGO = async (params = {}, query = {}) => {
  const { accountId, isBrokerPartner } = params
  const totalTenantQuery = {
    ...query,
    serial: { $exists: true }
  }
  let tenantConditionalObj = {
    $and: [
      { $ifNull: ['$$powerOfficeArray.syncedAt', false] },
      { $lte: ['$$powerOfficeArray.syncedAt', '$lastUpdate'] }
    ]
  }
  const modifiedTenantQuery = {
    ...query,
    lastUpdate: { $exists: true },
    powerOffice: { $exists: true }
  }
  if (!isBrokerPartner) {
    totalTenantQuery.properties = { $elemMatch: { accountId } }
    tenantConditionalObj = {
      $and: [
        { $ifNull: ['$$powerOfficeArray.syncedAt', false] },
        { $lte: ['$$powerOfficeArray.syncedAt', '$lastUpdate'] },
        { $eq: ['$$powerOfficeArray.accountId', accountId] }
      ]
    }
    modifiedTenantQuery.powerOffice = {
      $exists: true,
      $elemMatch: { accountId }
    }
  }
  const updatedTenantsPipeline = [
    { $match: modifiedTenantQuery },
    { $sort: { createdAt: -1 } },
    {
      $project: {
        powerOffice: {
          $first: {
            $filter: {
              input: { $ifNull: ['$powerOffice', []] },
              as: 'powerOfficeArray',
              cond: tenantConditionalObj
            }
          }
        }
      }
    },
    {
      $match: {
        powerOffice: {
          $exists: true
        }
      }
    }
  ]
  const updatedTenants =
    (await TenantCollection.aggregate(updatedTenantsPipeline)) || []

  const totalTenants = await tenantHelper.countTenants(totalTenantQuery)

  return {
    tenantsModified: updatedTenants.length,
    totalTenants
  }
}

const getTransactionLogForPOGO = async (params = {}, query = {}) => {
  const {
    fromDate,
    fromDateForPartner,
    todayDate,
    transactionSuccessIds,
    transactionErrorIds
  } = params

  let fromDateData = {
    createdAt: {
      $lte: todayDate
    }
  }

  if (fromDate) {
    fromDateData = {
      createdAt: {
        $gte: fromDateForPartner,
        $lte: todayDate
      }
    }
  }

  const transactionElMatchQuery = {
    ...query,
    ...fromDateData
  }
  const totalTransactionIds =
    await transactionHelper.getTransactionsUniqueFieldValue(
      '_id',
      transactionElMatchQuery
    )

  const successIds = intersection(totalTransactionIds, transactionSuccessIds)
  const errorIds = intersection(totalTransactionIds, transactionErrorIds)

  return {
    totalTransactions: totalTransactionIds.length,
    transactionsAdded: successIds.length,
    transactionError: errorIds.length
  }
}

const getErrorSuccessInfoForPOGO = async (query = {}) => {
  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        successAccount: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'account'] },
                { $eq: ['$status', 'success'] }
              ]
            },
            1,
            0
          ]
        },
        errorAccount: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'account'] },
                { $eq: ['$status', 'error'] }
              ]
            },
            1,
            0
          ]
        },
        successTenant: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'tenant'] },
                { $eq: ['$status', 'success'] }
              ]
            },
            1,
            0
          ]
        },
        errorTenant: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'tenant'] },
                { $eq: ['$status', 'error'] }
              ]
            },
            1,
            0
          ]
        },
        successTransactionIds: {
          $cond: [
            {
              $and: [
                { $eq: ['$status', 'success'] },
                { $eq: ['$type', 'transaction'] }
              ]
            },
            '$transactionIds',
            '$$REMOVE'
          ]
        },
        errorTransactionIds: {
          $cond: [
            {
              $and: [
                { $eq: ['$status', 'error'] },
                { $eq: ['$type', 'transaction'] }
              ]
            },
            '$transactionIds',
            '$$REMOVE'
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        totalSuccessAccount: {
          $sum: '$successAccount'
        },
        totalErrorAccount: {
          $sum: '$errorAccount'
        },
        totalSuccessTenant: {
          $sum: '$successTenant'
        },
        totalErrorTenant: {
          $sum: '$errorTenant'
        },
        successTransactionIds: {
          $push: '$successTransactionIds'
        },
        errorTransactionIds: {
          $push: '$errorTransactionIds'
        }
      }
    },
    {
      $addFields: {
        transactionSuccessIds: {
          $reduce: {
            input: '$successTransactionIds',
            initialValue: [],
            in: {
              $concatArrays: ['$$value', '$$this']
            }
          }
        },
        transactionErrorIds: {
          $reduce: {
            input: '$errorTransactionIds',
            initialValue: [],
            in: {
              $concatArrays: ['$$value', '$$this']
            }
          }
        }
      }
    }
  ]
  const [errorSuccessInfo = {}] =
    (await PowerOfficeLogCollection.aggregate(pipeline)) || []

  return errorSuccessInfo
}

const getPogoLogForPartnerApp = async (params) => {
  const { accountId = '', isBrokerPartner = true, partnerId } = params
  const integrationQuery = { partnerId, type: 'power_office_go' }
  if (!isBrokerPartner) integrationQuery.accountId = accountId
  const integrationData = await integrationHelper.getAnIntegration(
    integrationQuery
  )
  const fromDate = integrationData?.fromDate
  const isPowerOfficeIntegrated = !!(
    integrationData?.applicationKey && integrationData?.clientKey
  )
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const fromDateForPartner = await appHelper.getActualDate(
    partnerSettings,
    false,
    fromDate
  )
  const result = {}
  if (isPowerOfficeIntegrated) result.keyIntegrationView = true
  const query = { partnerId }

  const todayDate = await appHelper.getActualDate(partnerSettings, false)

  const { totalModifiedAccounts, totalAccounts } = await getAccountLogForPOGO(
    query
  )
  const { tenantsModified, totalTenants } = await getTenantLogForPOGO(
    {
      accountId,
      isBrokerPartner
    },
    query
  )

  if (!isBrokerPartner) query.accountId = accountId
  const {
    totalSuccessAccount,
    totalErrorAccount,
    totalSuccessTenant,
    totalErrorTenant,
    transactionSuccessIds,
    transactionErrorIds
  } = await getErrorSuccessInfoForPOGO(query)

  const { totalTransactions, transactionsAdded, transactionError } =
    await getTransactionLogForPOGO(
      {
        fromDate,
        fromDateForPartner,
        todayDate,
        transactionSuccessIds,
        transactionErrorIds
      },
      query
    )

  result.accountsAdded = totalSuccessAccount
  result.accountsError = totalErrorAccount
  result.accountsModified = totalModifiedAccounts
  result.totalAccounts = totalAccounts
  result.tenantsAdded = totalSuccessTenant
  result.tenantsError = totalErrorTenant
  result.tenantsModified = tenantsModified
  result.totalTenants = totalTenants
  result.transactionsAdded = transactionsAdded
  result.transactionError = transactionError
  result.totalTransactions = totalTransactions

  return result
}

export const queryPowerOfficeLogForPartnerApp = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  if (!isBrokerPartner) appHelper.checkRequiredFields(['accountId'], body)
  body.isBrokerPartner = isBrokerPartner
  body.partnerId = partnerId
  return getPogoLogForPartnerApp(body)
}

export const getTenantInfo = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      as: 'tenantInfo'
    }
  },
  appHelper.getUnwindPipeline('tenantInfo')
]

export const getAccountInfo = () => [
  {
    $lookup: {
      from: 'accounts',
      localField: 'accountId',
      foreignField: '_id',
      as: 'accountInfo'
    }
  },
  appHelper.getUnwindPipeline('accountInfo')
]

export const getTransactionInfo = () => [
  appHelper.getUnwindPipeline('transactionIds'),
  {
    $lookup: {
      from: 'transactions',
      localField: 'transactionIds',
      foreignField: '_id',
      as: 'transactionInfo'
    }
  },
  appHelper.getUnwindPipeline('transactionInfo')
]

const groupSerialIds = () => [
  {
    $group: {
      _id: '$_id',
      type: {
        $first: '$type'
      },
      status: {
        $first: '$status'
      },
      errorType: {
        $first: '$errorType'
      },
      errorsMeta: {
        $first: '$errorsMeta'
      },
      createdAt: {
        $first: '$createdAt'
      },
      accountSerialId: {
        $first: '$accountInfo.serial'
      },
      tenantSerialId: {
        $first: '$tenantInfo.serial'
      },
      transactionSerialIds: {
        $push: '$transactionInfo.serialId'
      }
    }
  }
]

const getLogDetailsForPartnerApp = async (params) => {
  const { options, query } = params
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
    ...getTenantInfo(),
    ...getAccountInfo(),
    ...getTransactionInfo(),
    ...groupSerialIds(),
    {
      $project: {
        _id: 1,
        createdAt: 1,
        errorsMeta: 1,
        errorType: 1,
        serial: {
          $switch: {
            branches: [
              {
                case: { $eq: ['$type', 'tenant'] },
                then: ['$tenantSerialId']
              },
              {
                case: { $eq: ['$type', 'account'] },
                then: ['$accountSerialId']
              },
              {
                case: { $eq: ['$type', 'transaction'] },
                then: '$transactionSerialIds'
              }
            ],
            default: []
          }
        },
        status: 1,
        type: 1
      }
    },
    {
      $sort: sort
    }
  ]
  return (await PowerOfficeLogCollection.aggregate(pipeline)) || []
}

export const queryLogDetailsForPartnerApp = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query = {} } = body
  appHelper.checkRequiredFields(['status', 'type'], query)
  query.partnerId = partnerId
  const logDetails = await getLogDetailsForPartnerApp(body)
  const filteredDocuments = await countPowerOfficeLogs(query)
  const totalDocuments = await countPowerOfficeLogs({ partnerId })
  return {
    data: logDetails,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}
