import { indexOf, size } from 'lodash'
import { TenantCollection, XledgerLogCollection } from '../models'
import {
  appHelper,
  accountHelper,
  integrationHelper,
  partnerHelper,
  powerOfficeLogHelper,
  tenantHelper,
  transactionHelper
} from '../helpers'
import { CustomError } from '../../resolvers/common'
export const getAXledgerLog = async (query, session) =>
  await XledgerLogCollection.findOne(query).session(session)

export const getXledgerLogQuery = (params) => {
  const { data, partnerId } = params
  const {
    accountId,
    status,
    tenantId,
    type,
    transactionDate,
    transactionIds,
    xledgerLogIds
  } = data
  const query = {}
  if (size(accountId)) query.accountId = accountId
  if (size(partnerId)) query.partnerId = partnerId
  if (size(status)) query.status = status
  if (size(tenantId)) query.tenantId = tenantId
  if (size(type)) query.type = type
  if (size(transactionDate)) query.transactionDate = transactionDate
  if (size(transactionIds)) query.transactionIds = { $in: transactionIds }
  if (size(xledgerLogIds)) query._id = { $in: xledgerLogIds }

  return query
}

export const getXledgerLogUpdateData = async (data) => {
  const updateData = {}
  const pushData = {}
  const setData = {}
  if (size(data) && size(data.accountId)) setData.accountId = data.accountId
  //Update errors array
  if (size(data.errors)) {
    setData.errorsMeta = data.errors
  }

  if (size(data) && size(data.hasError)) {
    setData.hasError = true
    if (data.lastUpdatedAt) setData.lastUpdatedAt = data.lastUpdatedAt
    else setData.lastUpdatedAt = new Date()
  }
  if (size(data) && size(data.status)) setData.status = data.status
  if (size(data) && size(data.type)) setData.type = data.type

  //Update xledgerEvents array
  const newStatus = size(data.xledgerEvent?.status)
    ? data.xledgerEvent.status
    : ''
  const statusList = ['new', 'processing', 'processed']
  if (size(newStatus) && indexOf(statusList, newStatus) !== -1) {
    //Add new event createAt for event status
    const newObject = {
      status: newStatus,
      createdAt: new Date()
    }
    pushData.xledgerEvents = newObject
    if (size(data.xledgerEvent?.note)) {
      pushData.xledgerEvents.note = data.xledgerEvent.note
    }
  }
  if (size(data) && size(data.transactionIds))
    setData.transactionIds = data.transactionIds
  if (size(data) && size(data.xledgerVoucherId))
    setData.xledgerVoucherId = data.xledgerVoucherId

  if (size(pushData)) {
    updateData.$push = pushData
  }
  if (size(setData)) {
    updateData.$set = setData
  }
  return updateData
}

export const prepareXledgerInfoUpdateData = (updatedData) => {
  const { accountId, code, hasError, hasUpdateError, id, syncedAt } =
    updatedData

  const updateData = {}
  if (accountId) updateData['xledger.accountId'] = accountId
  if (code) updateData['xledger.code'] = code
  if (hasError) updateData['xledger.hasError'] = hasError
  if (hasUpdateError) updateData['xledger.hasUpdateError'] = hasUpdateError
  if (id) updateData['xledger.id'] = id
  if (syncedAt) updateData['xledger.syncedAt'] = syncedAt

  return updateData
}

export const prepareTenantXledgerInfoUpdateData = (updatedData) => {
  const { accountId, code, hasError, hasUpdateError, id, syncedAt } =
    updatedData

  const pushData = {}
  const setData = {}
  if (accountId) {
    pushData.accountId = accountId
    setData['xledger.$.accountId'] = accountId
  }
  if (code) {
    pushData.code = code
    setData['xledger.$.code'] = code
  }
  if (hasError) {
    pushData.hasError = hasError
    setData['xledger.$.hasError'] = hasError
  }
  if (hasUpdateError) {
    pushData.hasUpdateError = hasUpdateError
    setData['xledger.$.hasUpdateError'] = hasUpdateError
  }
  if (id) {
    pushData.id = id
    setData['xledger.$.id'] = id
  }
  if (syncedAt) {
    pushData.syncedAt = syncedAt
    setData['xledger.$.syncedAt'] = syncedAt
  }

  return {
    pushData,
    setData
  }
}

export const getXledgerStatusForPartnerApp = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const partner = await partnerHelper.getAPartner(
    { _id: partnerId },
    null,
    'partnerSetting'
  )
  if (!(size(partner) && size(partner.partnerSetting)))
    throw new CustomError(404, 'Partner not found!!')
  body.partner = partner
  body.partnerSetting = partner.partnerSetting
  body.isBrokerPartner = partner.accountType === 'broker'
  body.partnerId = partnerId
  return await getXledgerStatusForPartnerAppQuery(body)
}

const getXledgerStatusForPartnerAppQuery = async (params) => {
  const {
    accountId = '',
    isBrokerPartner = true,
    partnerId = '',
    partner = {},
    partnerSetting = {}
  } = params
  const query = {
    partnerId
  }
  if (isBrokerPartner === false) query.accountId = accountId
  const {
    totalAccounts,
    totalModifiedAccounts,
    totalTenants,
    totalModifiedTenants,
    totalTransactions
  } = await getTotalInfo({ query, isBrokerPartner, partnerSetting, partner })

  const {
    totalSyncedAccount,
    totalErrorAccount,
    totalUpdateErrorAccount,
    totalSyncedTenant,
    totalErrorTenant,
    totalUpdateErrorTenant,
    totalSyncedTransaction,
    totalErrorTransaction,
    totalSyncedModifiedAccount,
    totalSyncedModifiedTenant
  } = await getErrorSuccessUpdateErrorInfoForXledger(query)
  return {
    totalAccounts,
    totalModifiedAccounts,
    totalTenants,
    totalModifiedTenants,
    totalTransactions,
    totalSyncedAccount,
    totalErrorAccount,
    totalUpdateErrorAccount,
    totalSyncedTenant,
    totalErrorTenant,
    totalUpdateErrorTenant,
    totalSyncedTransaction,
    totalErrorTransaction,
    totalSyncedModifiedAccount,
    totalSyncedModifiedTenant
  }
}

const getTotalInfo = async (params) => {
  const { query = {}, isBrokerPartner, partnerSetting = {} } = params
  const { partnerId, accountId } = query
  const accountQuery = {
    partnerId,
    serial: { $exists: true }
  }
  const modifiedAccountQuery = {
    partnerId,
    'xledger.syncedAt': { $exists: true },
    lastUpdate: { $exists: true },
    $expr: {
      $gte: ['$lastUpdate', '$xledger.syncedAt']
    }
  }

  const modifiedTenantQuery = {
    partnerId,
    lastUpdate: { $exists: true },
    xledger: { $exists: true }
  }
  let tenantConditionalObj = {
    $and: [
      { $ifNull: ['$$xledgerArray.syncedAt', false] },
      { $lte: ['$$xledgerArray.syncedAt', '$lastUpdate'] }
    ]
  }
  let totalAccounts = 0
  let totalModifiedAccounts = 0
  if (isBrokerPartner === true) {
    totalAccounts = await accountHelper.countAccounts(accountQuery)
    totalModifiedAccounts = await accountHelper.countAccounts(
      modifiedAccountQuery
    )
  } else {
    tenantConditionalObj = {
      $and: [
        { $ifNull: ['$$xledgerArray.syncedAt', false] },
        { $lte: ['$$xledgerArray.syncedAt', '$lastUpdate'] },
        { $eq: ['$$xledgerArray.accountId', accountId] }
      ]
    }
    modifiedTenantQuery.xledger = {
      $exists: true,
      $elemMatch: { accountId }
    }
  }
  const totalTenants = await tenantHelper.countTenants({
    partnerId,
    serial: {
      $exists: true
    }
  })

  const updatedTenantsPipeline = [
    { $match: modifiedTenantQuery },
    {
      $project: {
        xledger: {
          $first: {
            $filter: {
              input: { $ifNull: ['$xledger', []] },
              as: 'xledgerArray',
              cond: tenantConditionalObj
            }
          }
        }
      }
    },
    {
      $match: {
        xledger: {
          $exists: true
        }
      }
    }
  ]
  const updatedTenants =
    (await TenantCollection.aggregate(updatedTenantsPipeline)) || []

  const integrationQuery = { partnerId, type: 'xledger' }
  const integrationData = await integrationHelper.getAnIntegration(
    integrationQuery
  )
  const fromDate = integrationData?.fromDate
  const todayDate = (await appHelper.getActualDate(partnerSetting, true))
    .startOf('day')
    .toDate()

  const totalTransactionQuery = {
    ...query
  }
  if (fromDate) {
    totalTransactionQuery.createdAt = {
      $gte: await appHelper.getActualDate(partnerSetting, false, fromDate),
      $lte: todayDate
    }
  } else totalTransactionQuery.createdAt = { $lte: todayDate }

  const totalTransactions = await transactionHelper.countTransactions(
    totalTransactionQuery
  )
  return {
    totalAccounts,
    totalModifiedAccounts,
    totalTenants,
    totalModifiedTenants: updatedTenants.length,
    totalTransactions
  }
}

const getErrorSuccessUpdateErrorInfoForXledger = async (query = {}) => {
  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        syncedAccount: {
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
        errorUpdateAccount: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'update_account'] },
                { $eq: ['$status', 'error'] }
              ]
            },
            1,
            0
          ]
        },
        modifiedSyncedAccount: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'update_account'] },
                { $eq: ['$status', 'success'] }
              ]
            },
            1,
            0
          ]
        },
        syncedTenant: {
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
        errorUpdateTenant: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'update_tenant'] },
                { $eq: ['$status', 'error'] }
              ]
            },
            1,
            0
          ]
        },
        updateSyncedTenant: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'update_tenant'] },
                { $eq: ['$status', 'success'] }
              ]
            },
            1,
            0
          ]
        },
        syncedTransaction: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'transaction'] },
                { $eq: ['$status', 'success'] }
              ]
            },
            { $size: { $ifNull: ['$transactionIds', []] } },
            0
          ]
        },
        errorTransaction: {
          $cond: [
            {
              $and: [
                { $eq: ['$type', 'transaction'] },
                { $eq: ['$status', 'error'] }
              ]
            },
            { $size: { $ifNull: ['$transactionIds', []] } },
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        totalSyncedAccount: {
          $sum: '$syncedAccount'
        },
        totalErrorAccount: {
          $sum: '$errorAccount'
        },
        totalUpdateErrorAccount: {
          $sum: '$errorUpdateAccount'
        },
        totalSyncedTenant: {
          $sum: '$syncedTenant'
        },
        totalErrorTenant: {
          $sum: '$errorTenant'
        },
        totalUpdateErrorTenant: {
          $sum: '$errorUpdateTenant'
        },

        totalSyncedTransaction: {
          $sum: '$syncedTransaction'
        },
        totalErrorTransaction: {
          $sum: '$errorTransaction'
        },

        totalSyncedModifiedAccount: {
          $sum: '$modifiedSyncedAccount'
        },
        totalSyncedModifiedTenant: {
          $sum: '$updateSyncedTenant'
        }
      }
    }
  ]
  const [errorSuccessUpdateErrorInfo = {}] =
    (await XledgerLogCollection.aggregate(pipeline)) || []
  return errorSuccessUpdateErrorInfo
}

export const logDetailsForPartnerApp = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query = {} } = body
  appHelper.checkRequiredFields(['context', 'status', 'type'], query)
  query.partnerId = partnerId
  body.query = query
  const { logDetails, filteredDocuments, totalDocuments } =
    await getLogDetailsForPartnerApp(body)
  return {
    data: logDetails,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

const getLogDetailsForPartnerApp = async (params) => {
  const { options, query } = params
  const {
    accountId,
    context = '',
    partnerId = '',
    status = '',
    type = ''
  } = query
  const { limit, skip, sort } = options
  const prepareQuery = {
    partnerId,
    status,
    type
  }
  const isDirect = await partnerHelper.isDirectPartner(partnerId)
  if (isDirect) {
    appHelper.checkRequiredFields(['accountId'], query)
    prepareQuery.accountId = accountId
  }
  const pipeline = [
    {
      $match: prepareQuery
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

    ...powerOfficeLogHelper.getTenantInfo(),
    ...powerOfficeLogHelper.getAccountInfo(),
    ...powerOfficeLogHelper.getTransactionInfo(),
    ...groupSerialIds(),
    {
      $project: {
        _id: 1,
        createdAt: 1,
        errorsMeta: 1,
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
                case: { $eq: ['$type', 'update_tenant'] },
                then: ['$tenantSerialId']
              },
              {
                case: { $eq: ['$type', 'update_account'] },
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
  let logDetails = []
  let filteredDocuments = 0
  let totalDocuments = 0
  if (context === 'xledger') {
    logDetails = (await XledgerLogCollection.aggregate(pipeline)) || []
    filteredDocuments = await countXledgerLogs(prepareQuery)
    totalDocuments = await countXledgerLogs({ partnerId })
  }
  return {
    logDetails,
    filteredDocuments,
    totalDocuments
  }
}

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

export const countXledgerLogs = async (query) =>
  await XledgerLogCollection.countDocuments(query)
