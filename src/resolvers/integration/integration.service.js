import { size } from 'lodash'

import { CustomError } from '../common'
import {
  appHelper,
  appQueueHelper,
  integrationHelper,
  partnerHelper
} from '../helpers'
import { appQueueService } from '../services'
import { IntegrationCollection } from '../models'

export const createAnIntegration = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found to create integration')
  }
  const [createdIntegration] = await IntegrationCollection.create([data], {
    session
  })
  if (!size(createdIntegration)) {
    throw new CustomError(404, `Unable to create integration`)
  }
  return createdIntegration
}

export const updateAnIntegration = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found to update integration')
  }
  const response = await IntegrationCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })

  if (!size(response)) {
    throw new CustomError(404, `Unable to update integration`)
  }

  return response
}

export const prepareInsertData = async (body = {}, isDirect) => {
  const {
    accountId,
    applicationKey,
    clientKey,
    fromDate,
    partnerId,
    projectDepartmentType,
    tenantAccountType
  } = body

  const updateData = {
    applicationKey,
    clientKey,
    fromDate,
    projectDepartmentType,
    status: 'pending',
    tenantAccountType
  }

  const insertData = {
    ...updateData,
    enabledPowerOfficeIntegration: true,
    partnerId,
    type: 'power_office_go'
  }

  const query = { partnerId, type: 'power_office_go' }

  if (accountId && isDirect) {
    insertData.accountId = accountId
    query.accountId = accountId

    const isGlobalIntegration = await integrationHelper.getAnIntegration({
      partnerId,
      isGlobal: true
    })

    if (!isGlobalIntegration) insertData.isGlobal = true
  }

  return {
    insertData,
    query,
    updateData
  }
}

export const createIntegration = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const isDirect = await partnerHelper.isDirectPartner(partnerId)
  let requiredFields = [
    'applicationKey',
    'clientKey',
    'fromDate',
    'projectDepartmentType',
    'tenantAccountType'
  ]
  if (isDirect) {
    requiredFields = [
      'accountId',
      'applicationKey',
      'clientKey',
      'fromDate',
      'projectDepartmentType'
    ]
  }
  appHelper.checkRequiredFields(requiredFields, body)
  body.partnerId = partnerId
  const { insertData, query, updateData } = await prepareInsertData(
    body,
    isDirect
  )
  const { accountId, applicationKey, clientKey } = body
  const accessTokenQuery = {
    accountId,
    applicationKey,
    clientKey,
    partnerId
  }
  if (!process.env.CI) {
    await integrationHelper.getAuthorizationToken(accessTokenQuery)
  }
  let integrationInfo = {}
  const isIntegrationExist = !!(await integrationHelper.getAnIntegration(query))

  if (isIntegrationExist) {
    integrationInfo = await updateAnIntegration(query, updateData)
  } else {
    integrationInfo = await createAnIntegration(insertData)
  }

  let globalMappingIntegration = {}

  if (isDirect) {
    globalMappingIntegration =
      (await integrationHelper.getAnIntegration({
        partnerId,
        type: 'power_office_go',
        isGlobal: true
      })) || {}
  }

  integrationInfo.mapAccounts = size(integrationInfo.mapAccounts)
    ? integrationInfo.mapAccounts
    : size(globalMappingIntegration.mapAccounts)
    ? globalMappingIntegration.mapAccounts
    : []

  integrationInfo.mapBranches = size(integrationInfo.mapBranches)
    ? integrationInfo.mapBranches
    : size(globalMappingIntegration.mapBranches)
    ? globalMappingIntegration.mapBranches
    : []

  integrationInfo.mapGroups = size(integrationInfo.mapGroups)
    ? integrationInfo.mapGroups
    : size(globalMappingIntegration.mapGroups)
    ? globalMappingIntegration.mapGroups
    : []

  return integrationInfo
}

const prepareUpdateDataForSubLedgerSeries = (params) => {
  const { subledger } = params
  const {
    accountSubledgerSeries,
    tenantAccountType,
    tenantSubledgerSeries,
    type,
    subLedgerSeries
  } = subledger
  const updateData = {}
  const updateSetData = { tenantAccountType }

  if (type === 'tenant') {
    if (subLedgerSeries) updateSetData.tenantSubledgerSeries = subLedgerSeries
    else updateData.$unset = { tenantSubledgerSeries: 1 }
  }

  if (type === 'account') {
    if (subLedgerSeries) updateSetData.accountSubledgerSeries = subLedgerSeries
    else updateData.$unset = { accountSubledgerSeries: 1 }
  }

  if (!type && (tenantSubledgerSeries || accountSubledgerSeries)) {
    if (tenantSubledgerSeries)
      updateSetData.tenantSubledgerSeries = tenantSubledgerSeries

    if (accountSubledgerSeries)
      updateSetData.accountSubledgerSeries = accountSubledgerSeries
  }
  updateData.$set = updateSetData
  return updateData
}

const prepareUpdateDataForAddMapAccount = (params) => {
  const { mapAccount } = params
  if (!size(mapAccount))
    throw new CustomError(401, 'Could not find resource to update')

  return { $addToSet: { mapAccounts: mapAccount } }
}

const prepareUpdateDataForAddMapBranch = (params) => {
  const { mapBranch } = params
  if (!size(mapBranch))
    throw new CustomError(401, 'Could not find resource to update')

  return { $addToSet: { mapBranches: mapBranch } }
}

const prepareUpdateDataForAddMapGroup = (params) => {
  const { mapGroup } = params
  if (!size(mapGroup))
    throw new CustomError(401, 'Could not find resource to update')

  return { $addToSet: { mapGroups: mapGroup } }
}

const prepareUpdateDataForRemoveMapAccount = (data) => {
  const { ledgerAccountId } = data
  if (!ledgerAccountId)
    throw new CustomError(401, 'Could not find resource to update')

  return { $pull: { mapAccounts: { accountingId: ledgerAccountId } } }
}

const prepareUpdateDataForRemoveMapBranch = (params) => {
  const { branchSerialId } = params
  if (!branchSerialId)
    throw new CustomError(401, 'Could not find resource to update')

  return { $pull: { mapBranches: { branchSerialId } } }
}

const prepareUpdateDataForRemoveMapGroup = (params) => {
  const { propertyGroupId } = params
  if (!propertyGroupId)
    throw new CustomError(401, 'Could not find resource to update')

  return { $pull: { mapGroups: { propertyGroupId } } }
}

export const prepareUpdateData = (params) => {
  const { updateType, data } = params

  const modifiersObj = {
    addOrRemoveSubLedgerSeries: prepareUpdateDataForSubLedgerSeries,
    addMapAccount: prepareUpdateDataForAddMapAccount,
    addMapBranch: prepareUpdateDataForAddMapBranch,
    addMapGroup: prepareUpdateDataForAddMapGroup,
    removeMapAccount: prepareUpdateDataForRemoveMapAccount,
    removeMapBranch: prepareUpdateDataForRemoveMapBranch,
    removeMapGroup: prepareUpdateDataForRemoveMapGroup
  }
  if (modifiersObj[updateType]) return modifiersObj[updateType](data)

  return {}
}

export const prepareSubLedgerSeriesQuery = (data) => {
  const { partnerId } = data
  return { partnerId, type: 'power_office_go' }
}

const prepareAddMapQuery = (data) => {
  const { isDirectPartner, partnerId } = data
  const query = { partnerId, type: 'power_office_go' }
  if (isDirectPartner) query.isGlobal = true
  return query
}

const prepareRemoveQuery = (data) => {
  const { isDirectPartner, partnerId } = data
  const query = { partnerId, type: 'power_office_go' }
  if (isDirectPartner) {
    query.isGlobal = true
  } else {
    query.isGlobal = { $exists: false }
  }
  return query
}

export const prepareQueryData = (params) => {
  const { updateType, data } = params

  const queryObj = {
    addOrRemoveSubLedgerSeries: prepareSubLedgerSeriesQuery,
    addMapAccount: prepareAddMapQuery,
    addMapBranch: prepareAddMapQuery,
    addMapGroup: prepareAddMapQuery,
    removeMapAccount: prepareRemoveQuery,
    removeMapBranch: prepareRemoveQuery,
    removeMapGroup: prepareRemoveQuery
  }
  return queryObj[updateType](data)
}

export const updateOrRemoveIntegration = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  appHelper.checkRequiredFields(['data', 'updateType'], body)
  body.data.partnerId = partnerId
  body.data.isDirectPartner = await partnerHelper.isDirectPartner(partnerId)

  const query = prepareQueryData(body)
  const updateData = await prepareUpdateData(body)
  if (!size(updateData)) {
    throw new CustomError(401, 'Could not find any data to update')
  }
  return await updateAnIntegration(query, updateData, session)
}

export const enableOrDisablePogo = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  appHelper.checkRequiredFields(['enabledPowerOfficeIntegration'], body)
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) {
    appHelper.checkRequiredFields(
      ['accountId', 'enabledPowerOfficeIntegration'],
      body
    )
  }
  const { accountId, enabledPowerOfficeIntegration } = body
  const updateData = { enabledPowerOfficeIntegration }
  if (enabledPowerOfficeIntegration) updateData.status = 'pending'
  else updateData.status = 'disabled'

  const integrationQuery = {
    partnerId,
    type: 'power_office_go'
  }
  const partnerInfo = await partnerHelper.getAPartner({
    _id: partnerId,
    accountType: 'direct'
  })
  if (accountId && size(partnerInfo)) {
    integrationQuery.accountId = accountId
  }

  return await updateAnIntegration(integrationQuery, {
    $set: updateData
  })
}

export const enableOrDisableIntegration = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'context',
    'enabledIntegration'
  ])
  const { body = {} } = req
  const { accountId, context, enabledIntegration, partnerId } = body
  const integrationQuery = {
    partnerId,
    type: context
  }
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) {
    appHelper.checkRequiredFields(['accountId'], body)
    integrationQuery.accountId = accountId
  }
  const updateData = { enabledIntegration }
  if (enabledIntegration) updateData.status = 'pending'
  else updateData.status = 'disabled'

  const updatedIntegration = await updateAnIntegration(integrationQuery, {
    $set: updateData
  })
  const {
    _id,
    enabledIntegration: updatedEnabledIntegration,
    status
  } = updatedIntegration
  return {
    _id,
    enabledIntegration: updatedEnabledIntegration,
    status
  }
}

export const updateIntegrationFromLambda = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['integrationId'], body)
  const { integrationId } = body
  const preparedQuery = { _id: integrationId }
  const updateData = integrationHelper.prepareDataToUpdateIntegration(body)
  if (!size(updateData)) throw new CustomError(400, 'Nothing to update')
  const updatedIntegration = await updateAnIntegration(
    preparedQuery,
    updateData
  )
  if (!size(updatedIntegration))
    throw new CustomError(404, 'Integration not found')
  return {
    result: true
  }
}

export const checkPogoIntegrationStatusForPartnerAop = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body = {} } = req
  const { accountId, partnerId } = body
  const isDirect = await partnerHelper.isDirectPartner(partnerId)
  const params = {
    partnerId,
    partnerType: isDirect ? 'direct' : 'broker'
  }
  const query = {
    action: 'check_integration_status',
    'params.partnerId': partnerId,
    status: {
      $nin: ['completed', 'failed']
    },
    destination: 'accounting-pogo'
  }
  if (isDirect) {
    appHelper.checkRequiredFields(['accountId'], body)
    params.directPartnerAccountId = accountId
    query['params.directPartnerAccountId'] = accountId
  }
  // To check if another queue exists or not
  const existAppQueue = await appQueueHelper.getAnAppQueue(query)
  if (size(existAppQueue))
    throw new CustomError(
      400,
      'Another integration status checking process is running. Please try again later.'
    )
  const appQueueData = {
    action: 'check_integration_status',
    event: 'check_integration_status',
    priority: 'immediate',
    destination: 'accounting-pogo',
    params
  }
  await appQueueService.createAnAppQueue(appQueueData)
  return {
    result: true
  }
}

export const checkIntegrationStatusForPartnerAop = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['context'])
  const { body = {} } = req
  const { accountId, context, partnerId } = body
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  const query = {
    action: 'check_integration_status',
    'params.partnerId': partnerId,
    status: {
      $nin: ['completed', 'failed']
    },
    destination: context
  }
  const params = { partnerId }
  if (isDirectPartner) {
    appHelper.checkRequiredFields(['accountId'], body)
    query['params.accountId'] = accountId
    params.accountId = accountId
  }
  // To check if another queue exists or not
  const existAppQueue = await appQueueHelper.getAnAppQueue(query)
  if (size(existAppQueue))
    throw new CustomError(
      400,
      'Another integration status checking process is running. Please try again later.'
    )
  const appQueueData = {
    action: 'check_integration_status',
    event: 'check_integration_status',
    priority: 'immediate',
    destination: context,
    params
  }
  await appQueueService.createAnAppQueue(appQueueData)
  return {
    result: true
  }
}

export const createIntegrationForXledger = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const requiredFields = ['clientKey', 'fromDate']
  const isDirect = await partnerHelper.isDirectPartner(partnerId)
  if (isDirect) {
    requiredFields.push('accountId')
  } else {
    requiredFields.push('tenantAccountType')
  }
  appHelper.checkRequiredFields(requiredFields, body)

  body.partnerId = partnerId
  const { clientKey } = body
  const { companyDbId, ownerDbId } =
    await integrationHelper.checkXledgerTokenValidity(clientKey)
  body.companyDbId = companyDbId
  body.ownerDbId = ownerDbId
  const { insertData, query, updateData } =
    await integrationHelper.prepareInsertDataForXledger(body, isDirect)

  let integrationInfo = {}
  const isXledgerIntegrationExist = !!(await integrationHelper.getAnIntegration(
    query
  ))

  if (isXledgerIntegrationExist) {
    integrationInfo = await updateAnIntegration(query, updateData)
  } else {
    integrationInfo = await createAnIntegration(insertData)
  }

  return integrationInfo
}

export const updateOrRemoveIntegrationItem = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  const requiredFields = ['data', 'type', 'updateType']
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) requiredFields.push('accountId')
  appHelper.checkRequiredFields(requiredFields, body)
  body.partnerId = partnerId

  const query = integrationHelper.preparedQueryForIntegration(body)
  if (isDirectPartner) query.isGlobal = true
  const integrationInfo = await integrationHelper.getAnIntegration(query)
  if (!size(integrationInfo))
    throw new CustomError(404, 'Integration info not found')
  const updateData =
    await integrationHelper.prepareUpdateDataForUpdateIntegrationItem(
      body,
      integrationInfo
    )
  if (!size(updateData)) {
    throw new CustomError(401, 'Could not find any data to update')
  }
  return await updateAnIntegration(query, updateData)
}
