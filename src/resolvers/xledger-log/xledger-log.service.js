import { map, size } from 'lodash'
import moment from 'moment-timezone'
import { appHelper, partnerHelper, xledgerLogHelper } from '../helpers'
import { accountService, tenantService, transactionService } from '../services'
import { XledgerLogCollection } from '../models'
import { CustomError } from '../common'

export const createXledgerLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId', 'data'], body)
  const { data = {} } = body
  appHelper.checkRequiredFields(['type'], data)
  const xledgerLogQuery = xledgerLogHelper.getXledgerLogQuery(body)
  const existingLog = await xledgerLogHelper.getAXledgerLog(xledgerLogQuery)
  if (size(existingLog)) {
    throw new CustomError(400, 'Xledger log already exist')
  }
  const insertData = xledgerLogQuery
  const xledgerEvent = { createdAt: new Date() }
  const status = xledgerLogQuery.status
  if (size(status)) {
    xledgerEvent.status = status
  } else {
    insertData.status = 'processing'
    xledgerEvent.status = 'new'
  }
  if (size(xledgerEvent)) insertData.xledgerEvents = [xledgerEvent]
  insertData.processingAt = new Date()
  if (size(data.transactionIds)) {
    insertData.transactionIds = data.transactionIds
  }
  const createdLog = await insertXledgerLog(insertData, session)
  if (!size(createdLog)) throw new CustomError(400, 'Not inserted xledger log')
  return {
    _id: createdLog._id,
    accountId: createdLog.accountId,
    partnerId: createdLog.partnerId,
    status: createdLog.status,
    tenantId: createdLog.tenantId,
    transactionIds: createdLog.transactionIds,
    type: createdLog.type
  }
}

export const insertXledgerLog = async (data, session) => {
  const [xledgerLog] = await XledgerLogCollection.create([data], {
    session
  })
  if (!size(xledgerLog))
    throw new CustomError(400, 'Unable to create xledger log')

  return xledgerLog
}

export const resetXledgerLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  const partnerInfo = (await partnerHelper.getPartnerById(partnerId)) || {}
  if (!size(partnerInfo))
    throw new CustomError(
      404,
      'Partner info not found with partnerId: ' + partnerId
    )
  const isDirectPartner = partnerInfo.accountType === 'direct'
  const tenantsQuery = { partnerId, 'xledger.hasError': { $exists: true } }
  let tenantsUpdateData = {}
  if (isDirectPartner) {
    tenantsUpdateData = {
      $pull: {
        xledger: { hasError: true }
      }
    }
  } else {
    tenantsUpdateData = {
      $unset: { xledger: 1 }
    }
  }

  //To check if tenant and account synced in xledger but status processing in xledgerLog
  const completedXledgerLogs = await XledgerLogCollection.aggregate([
    {
      $match: {
        partnerId,
        status: 'processing'
      }
    },
    ...getAccountPipelineForResetXledgerLog(),
    ...getTenantPipelineForResetXledgerLog(isDirectPartner),
    ...getTransactionPipelineForResetXledgerLog(),
    ...getFinalMatchPipelineForResetXledgerLog()
  ])
  const completedXledgerLogIds = map(completedXledgerLogs, '_id')
  await updateXledgerLogs(
    {
      _id: {
        $in: completedXledgerLogIds
      }
    },
    { status: 'success' },
    session
  )
  await accountService.updateAccounts(
    {
      partnerId,
      'xledger.hasError': true
    },
    { $unset: { xledger: 1 } },
    session
  )
  await tenantService.updateTenants(tenantsQuery, tenantsUpdateData, session)
  const transactionsQuery = {
    partnerId,
    'xledger.hasError': true
  }
  await transactionService.updateTransaction(
    transactionsQuery,
    { $unset: { xledger: 1 } },
    session
  )
  // Remove the processing and error logs which is created at least 3 minutes ago
  // To set this time, you must check the lambda timeout. Now the timeout is 1.5 minutes in lambda.
  // It must be larger then the lambda timeout.
  const startDate = moment().subtract(3, 'minutes').toDate()
  const xledgerLogQuery = {
    _id: { $nin: completedXledgerLogIds },
    partnerId,
    createdAt: { $lte: startDate },
    status: { $in: ['error', 'processing'] }
  }

  await deleteXledgerLogs(xledgerLogQuery, session)

  await accountService.updateAccounts(
    {
      partnerId,
      'xledger.hasUpdateError': true
    },
    { $unset: { 'xledger.hasUpdateError': 1 } },
    session
  )

  await tenantService.updateTenants(
    {
      partnerId,
      xledger: {
        $elemMatch: {
          hasUpdateError: true
        }
      }
    },
    {
      $unset: {
        'xledger.$.hasUpdateError': 1
      }
    },
    session
  )

  return {
    result: true
  }
}

const getAccountPipelineForResetXledgerLog = () => [
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
  }
]

const getTenantPipelineForResetXledgerLog = (isDirectPartner) => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      let: { accountId: '$accountId' },
      pipeline: [
        {
          $addFields: {
            xledgerInfo: {
              $cond: [
                { $eq: [isDirectPartner, true] },
                {
                  $first: {
                    $filter: {
                      input: { $ifNull: ['$xledger', []] },
                      as: 'xledgerInfo',
                      cond: {
                        $eq: ['$$xledgerInfo.accountId', '$$accountId']
                      }
                    }
                  }
                },
                {
                  $first: { $ifNull: ['$xledger', []] }
                }
              ]
            }
          }
        },
        {
          $addFields: {
            syncedDone: {
              $and: [
                { $ifNull: ['$xledgerInfo.id', false] },
                { $ifNull: ['$xledgerInfo.syncedAt', false] }
              ]
            }
          }
        }
      ],
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

const getTransactionPipelineForResetXledgerLog = () => [
  {
    $addFields: { transactionId: { $first: '$transactionIds' } }
  },
  {
    $lookup: {
      from: 'transactions',
      localField: 'transactionId',
      foreignField: '_id',
      as: 'transaction'
    }
  },
  {
    $unwind: {
      path: '$transaction',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getFinalMatchPipelineForResetXledgerLog = () => [
  {
    $match: {
      $or: [
        {
          type: 'tenant',
          'tenant.syncedDone': true
        },
        {
          type: 'update_tenant',
          'tenant.xledgerInfo': {
            $exists: true
          },
          'tenant.xledgerInfo.hasUpdateError': {
            $exists: false
          }
        },
        {
          type: 'account',
          'account.xledger': { $exists: true },
          'account.xledger.id': { $exists: true },
          'account.xledger.syncedAt': { $exists: true },
          'account.xledger.hasError': { $exists: false }
        },
        {
          type: 'update_account',
          'account.xledger': { $exists: true },
          'account.xledger.id': { $exists: true },
          'account.xledger.syncedAt': { $exists: true },
          'account.xledger.hasUpdateError': { $exists: false }
        },
        {
          type: 'transaction',
          'transaction.xledger.xledgerLogId': { $exists: true },
          'transaction.xledger.hasError': { $exists: false }
        }
      ]
    }
  }
]

export const updateAXledgerLog = async (query, data, session) => {
  const updatedXledgeLog = await XledgerLogCollection.findOneAndUpdate(
    query,
    data,
    { session, new: true, runValidators: true }
  )
  return updatedXledgeLog
}

export const updateXledgerLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['xledgerLogId', 'partnerId'], body)
  const { xledgerLogId, partnerId } = body
  const xledgerLogQuery = { _id: xledgerLogId, partnerId }

  const updateData = await xledgerLogHelper.getXledgerLogUpdateData(body.data)

  if (!size(updateData)) {
    throw new CustomError(400, 'Nothing to update')
  }
  const updatedXledgerLog = await updateAXledgerLog(
    xledgerLogQuery,
    updateData,
    session
  )
  if (!size(updatedXledgerLog)) {
    throw new CustomError(400, 'Unable to update xledger log')
  }

  return {
    _id: updatedXledgerLog._id,
    accountId: updatedXledgerLog.accountId,
    partnerId: updatedXledgerLog.partnerId,
    status: updatedXledgerLog.status,
    tenantId: updatedXledgerLog.tenantId,
    transactionIds: updatedXledgerLog.transactionIds,
    type: updatedXledgerLog.type
  }
}

export const updateXledgerLogs = async (query, data, session) =>
  await XledgerLogCollection.updateMany(query, data, {
    session,
    runValidators: true
  })

export const updateXledgerInfoByContext = async (req) => {
  const { queryData, session, updateData, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['context'], queryData)
  const { accountId, context, partnerId, tenantId, xledgerDbId } = queryData
  const updatingData = xledgerLogHelper.prepareXledgerInfoUpdateData(updateData)
  if (!size(updatingData) && context !== 'transaction') {
    throw new CustomError(400, 'Nothing to update')
  }
  if (context === 'tenant') {
    appHelper.checkRequiredFields(['tenantId'], queryData)
    const { pushData, setData } =
      xledgerLogHelper.prepareTenantXledgerInfoUpdateData(updateData)
    if (xledgerDbId) {
      await tenantService.updateATenant(
        { _id: tenantId, xledger: { $elemMatch: { id: xledgerDbId } } },
        { $set: setData }
      )
    } else {
      await tenantService.updateATenant(
        { _id: tenantId },
        {
          $push: {
            xledger: pushData
          }
        }
      )
    }
  } else if (context === 'account') {
    appHelper.checkRequiredFields(['accountId'], queryData)

    if (size(updatingData)) {
      await accountService.updateAnAccount(
        { _id: accountId },
        {
          $set: updatingData
        }
      )
    }
  } else if (context === 'transaction') {
    if (size(updateData)) {
      appHelper.checkRequiredFields(
        ['xledgerLogId', 'transactions'],
        updateData
      )
      const { xledgerLogId, transactions } = updateData
      for (const item of transactions) {
        const { creditTrDbId, debitTrDbId, hasError, syncedAt, transactionId } =
          item
        const newUpdateData = {}
        if (creditTrDbId) newUpdateData['xledger.creditTrDbId'] = creditTrDbId
        if (debitTrDbId) newUpdateData['xledger.debitTrDbId'] = debitTrDbId
        if (hasError) newUpdateData['xledger.hasError'] = hasError
        if (syncedAt) newUpdateData['xledger.syncedAt'] = syncedAt
        if (xledgerLogId) newUpdateData['xledger.xledgerLogId'] = xledgerLogId

        await transactionService.updateATransaction(
          { _id: transactionId, partnerId },
          { $set: newUpdateData },
          session
        )
      }
    }
  }
  return {
    result: true
  }
}

const deleteXledgerLogs = async (query, session) => {
  const response = await XledgerLogCollection.deleteMany(query, session)
  return response
}
