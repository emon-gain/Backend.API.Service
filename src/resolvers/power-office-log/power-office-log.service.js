import moment from 'moment-timezone'
import { compact, indexOf, map, size } from 'lodash'

import { appHelper, partnerHelper, powerOfficeLogHelper } from '../helpers'
import {
  accountService,
  appQueueService,
  tenantService,
  transactionService
} from '../services'
import { PowerOfficeLogCollection } from '../models'
import { CustomError } from '../common/error'

const deletePowerOfficeLog = async (query, session) => {
  const response = await PowerOfficeLogCollection.deleteMany(query).session(
    session
  )
  return response
}

export const updateAPowerOfficeLog = async (query, data, session) => {
  const updatedPowerOfficeLog = await PowerOfficeLogCollection.findOneAndUpdate(
    query,
    data,
    { session, new: true, runValidators: true }
  )
  return updatedPowerOfficeLog
}

export const updatePowerOfficeLogs = async (query, data, session) => {
  const response = await PowerOfficeLogCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  return response
}

export const removePowerOfficeLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const query = powerOfficeLogHelper.getPowerOfficeLogQuery(body)
  // Remove the processing logs after which is created at least 3 minutes ago
  // To set this time, you must check the lambda timeout. Now the timeout is 1.5 minutes in lambda.
  // It must be larger then the lambda timeout.
  const startDate = moment().subtract(3, 'minutes').toDate()
  query.createdAt = { $lte: startDate }
  const removedPowerOfficeLogs = await deletePowerOfficeLog(query, session)
  return removedPowerOfficeLogs
}

export const resetPowerOfficeLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { action, directPartnerAccountId, partnerId, type } = body
  const partnerInfo = await partnerHelper.getPartnerById(partnerId, session)
  if (
    size(partnerInfo) &&
    partnerInfo.accountType === 'direct' &&
    !size(directPartnerAccountId)
  ) {
    appHelper.checkRequiredFields(['directPartnerAccountId'], body)
  }
  if (type) {
    appHelper.checkRequiredFields(['action'], body)
  }
  const tenantsQuery = { partnerId }
  let tenantsUpdateData = {}
  let syncedTenantsQuery = {}

  if (size(directPartnerAccountId)) {
    tenantsUpdateData = {
      $pull: {
        powerOffice: { hasError: true, accountId: directPartnerAccountId }
      }
    }
    syncedTenantsQuery = {
      'tenant.powerOffice': {
        $elemMatch: {
          id: { $exists: true },
          code: { $exists: true },
          accountId: directPartnerAccountId
        }
      }
    }
  } else {
    tenantsQuery['powerOffice.hasError'] = true
    tenantsUpdateData = {
      $unset: { powerOffice: 1 }
    }
    syncedTenantsQuery = {
      'tenant.powerOffice': {
        $elemMatch: {
          id: { $exists: true },
          code: { $exists: true }
        }
      }
    }
  }

  //To check if tenant and account synced in POGO but status processing in powerOfficeLog
  const completedPowerOfficeLogs = await PowerOfficeLogCollection.aggregate([
    {
      $match: {
        partnerId,
        status: 'processing'
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
    },
    {
      $match: {
        $or: [
          syncedTenantsQuery,
          {
            'account.powerOffice': { $exists: true },
            $or: [
              { 'account.powerOffice.hasError': { $exists: false } },
              { 'account.powerOffice.hasError': false }
            ]
          },
          {
            'transaction.powerOffice.powerOfficeLogId': { $exists: true },
            'transaction.powerOffice.hasError': { $exists: false }
          }
        ]
      }
    }
  ])
  const completedPowerOfficeLogIds = map(completedPowerOfficeLogs, '_id')
  await updatePowerOfficeLogs(
    {
      _id: {
        $in: completedPowerOfficeLogIds
      }
    },
    { status: 'success' },
    session
  )
  if (
    (!action && !type) ||
    (action === 'start_account_integration' && type === 'one_after_error')
  ) {
    await accountService.updateAccounts(
      {
        partnerId,
        $and: [
          { 'powerOffice.hasError': { $exists: true } },
          { 'powerOffice.hasError': true }
        ]
      },
      { $unset: { powerOffice: 1 } },
      session
    )
  }

  if (
    (!action && !type) ||
    (action === 'start_tenant_integration' && type === 'one_after_error')
  ) {
    await tenantService.updateTenants(tenantsQuery, tenantsUpdateData, session)
  }
  // Remove the processing and error logs after which is created at least 3 minutes ago
  // To set this time, you must check the lambda timeout. Now the timeout is 1.5 minutes in lambda.
  // It must be larger then the lambda timeout.
  const startDate = moment().subtract(3, 'minutes').toDate()

  const powerOfficeLogQuery = {
    _id: { $nin: completedPowerOfficeLogIds },
    partnerId,
    createdAt: { $lte: startDate }
  }

  if (size(directPartnerAccountId)) {
    powerOfficeLogQuery.accountId = directPartnerAccountId
  }

  if (type === 'one_after_error' && action) {
    powerOfficeLogQuery.status = { $in: ['error', 'processing'] }
    if (action === 'start_account_integration') {
      powerOfficeLogQuery.type = 'account'
    } else if (action === 'start_tenant_integration') {
      powerOfficeLogQuery.type = 'tenant'
    }
    await deletePowerOfficeLog(powerOfficeLogQuery, session) //remove error and processing for account and tenant
  } else if (!type && !action) {
    //Don't reset transactions on after error.
    const transactionsQuery = {
      partnerId,
      'powerOffice.hasError': { $exists: true }
    }

    const appQueueRemoveQuery = {
      destination: 'accounting-pogo',
      status: 'failed',
      'params.partnerId': partnerId
    }

    if (size(directPartnerAccountId)) {
      transactionsQuery.accountId = directPartnerAccountId
      appQueueRemoveQuery['params.directPartnerAccountId'] =
        directPartnerAccountId
    }

    await transactionService.updateTransaction(
      transactionsQuery,
      { $unset: { powerOffice: 1 } },
      session
    )

    powerOfficeLogQuery.status = { $in: ['error', 'processing'] }
    powerOfficeLogQuery.type = {
      $in: [
        'account',
        'tenant',
        'transaction',
        'update_account',
        'update_tenant'
      ]
    }
    await deletePowerOfficeLog(powerOfficeLogQuery, session)
    //To delete failed app_queues for start new integration
    await appQueueService.removeAppQueueItems(appQueueRemoveQuery, session)
  }
  return {
    data: { success: true }
  }
}

export const insertPowerOfficeLog = async (data, session) => {
  const response = await PowerOfficeLogCollection.create([data], {
    session
  })
  return response
}

export const createPowerOfficeLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)

  const powerOfficeLogQuery = powerOfficeLogHelper.getPowerOfficeLogQuery(body)
  const insertData = await powerOfficeLogHelper.getPowerOfficeLogUpdateData(
    body.data,
    {},
    session
  )

  //If empty and has missing data
  if (!size(insertData)) {
    // return false;
    throw new CustomError(404, 'Missing insert data')
  }

  //prepare power office log query to check if the account/tenant is already in processing
  if (size(insertData.accountId))
    powerOfficeLogQuery.accountId = insertData.accountId
  if (size(insertData.tenantId))
    powerOfficeLogQuery.tenantId = insertData.tenantId
  if (size(insertData.type)) powerOfficeLogQuery.type = insertData.type
  if (size(insertData.transactionDate)) {
    powerOfficeLogQuery.transactionDate = insertData.transactionDate
    if (size(insertData.transactionIds))
      powerOfficeLogQuery.transactionIds = { $in: insertData.transactionIds }

    const powerOffice = await powerOfficeLogHelper.getAPowerOfficeLog(
      powerOfficeLogQuery,
      session
    )
    if (size(powerOffice)) {
      throw new CustomError(400, 'Power office log already exist')
    }
  } else if (
    indexOf(
      ['transaction', 'update_tenant', 'update_account'],
      insertData.type
    ) === -1
  ) {
    const previousPowerOfficeLogData =
      await powerOfficeLogHelper.getAPowerOfficeLog(
        powerOfficeLogQuery,
        session
      )
    if (size(previousPowerOfficeLogData)) {
      if (
        previousPowerOfficeLogData.status === 'error' &&
        previousPowerOfficeLogData.retries < 5
      ) {
        return powerOfficeLogHelper.createPowerOfficeLogFieldNameForApi(
          previousPowerOfficeLogData
        )
      } else {
        throw new CustomError(
          400,
          'Previous power office log data exist with status not error'
        )
      }
    }
  }

  if (size(powerOfficeLogQuery.partnerId))
    insertData.partnerId = powerOfficeLogQuery.partnerId

  const powerOfficeEvent = { createdAt: new Date() }
  const status = insertData.status

  if (size(status)) {
    powerOfficeEvent.status = status
  } else {
    insertData.status = 'new'
    powerOfficeEvent.status = 'new'
  }

  insertData.retries = 1
  if (size(powerOfficeEvent)) insertData.powerOfficeEvents = [powerOfficeEvent]
  //Implementation of before insert hook
  if (
    insertData.type === 'transaction' &&
    size(insertData.transactionIds) &&
    size(insertData.partnerId)
  ) {
    const powerOfficeLogExistenceQuery = {
      transactionIds: { $in: compact(insertData.transactionIds) },
      partnerId: insertData.partnerId,
      type: 'transaction'
    }
    if (size(insertData.accountId)) {
      powerOfficeLogExistenceQuery.accountId = insertData.accountId
    }
    const logExists = await powerOfficeLogHelper.getAPowerOfficeLog(
      powerOfficeLogExistenceQuery,
      session
    )

    if (size(logExists)) {
      throw new CustomError(400, 'Power office log already exist')
    }
  }
  const created = await insertPowerOfficeLog(insertData, session)
  if (!size(created)) {
    throw new CustomError(400, 'Not inserted power office log')
  }

  return powerOfficeLogHelper.createPowerOfficeLogFieldNameForApi(created[0])
}

export const updatePowerOfficeLog = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)

  const powerOfficeLogQuery = powerOfficeLogHelper.getPowerOfficeLogQuery(body)
  const updateData = await powerOfficeLogHelper.getPowerOfficeLogUpdateData(
    body.data,
    powerOfficeLogQuery,
    session
  )
  //Query params
  if (!size(powerOfficeLogQuery) && !size(updateData)) {
    throw new CustomError(404, 'Missing query and update data')
  }
  const updatedPowerOfficeLog = await updateAPowerOfficeLog(
    powerOfficeLogQuery,
    {
      $set: updateData
    },
    session
  )
  if (!size(updatedPowerOfficeLog)) {
    throw new CustomError(400, 'Power office log not updated')
  }

  return powerOfficeLogHelper.createPowerOfficeLogFieldNameForApi(
    updatedPowerOfficeLog
  )
}
