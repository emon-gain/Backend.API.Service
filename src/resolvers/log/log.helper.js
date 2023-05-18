import { uniq, size, pick, assign, clone, union } from 'lodash'
import moment from 'moment-timezone'

import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  contractHelper,
  invoiceHelper,
  paymentHelper,
  organizationHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import { LogCollection } from '../models'
import settings from '../../../settings.json'

export const getLogVisibility = (options, dataInfo) => {
  const { accountId = '', invoiceId = '', propertyId = '' } = dataInfo || {}
  let { tenantId = '' } = dataInfo || {}
  let visibility = options.context ? [options.context] : []

  if (options.collectionName === 'contract') {
    const { rentalMeta = {} } = dataInfo || {}
    tenantId =
      size(rentalMeta) && rentalMeta.tenantId ? rentalMeta.tenantId : ''
  }

  if (
    options.collectionName === 'notificationLogs' &&
    dataInfo.event === 'send_task_notification'
  )
    visibility.push('task')

  if (accountId) visibility.push('account')
  if (invoiceId) visibility.push('invoice')
  if (propertyId) visibility.push('property')
  if (tenantId) visibility.push('tenant')

  if (
    (dataInfo && dataInfo.annualStatementId) ||
    (options && options.collectionName === 'annual_statements')
  )
    visibility = ['property', 'account']

  return uniq(visibility)
}

export const countLogs = async (query, session) => {
  const noOfLogs = await LogCollection.countDocuments(query).session(session)
  return noOfLogs
}

export const getLogForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const logs = await LogCollection.find(query)
    .populate([
      'account',
      'agent',
      'branch',
      'comment',
      'contract',
      'correction',
      'file',
      'invoice',
      'partner',
      'payout',
      'property',
      'task',
      'tenant'
    ])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return logs
}

export const queryLogs = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)

  const logs = await getLogForQuery(body)
  const filteredDocuments = await countLogs(query)
  const totalDocuments = await countLogs({})
  return {
    data: logs,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const prepareAgentOrBankAccountChangeLogData = (options, account) => {
  const { fieldName = '', previousDoc = {}, oldText, newText } = options
  const type = fieldName === 'agentId' ? 'foreignKey' : 'text'
  const oldTextForAgent =
    size(previousDoc) && previousDoc[fieldName] ? previousDoc[fieldName] : ''
  const changes = []
  if (fieldName) {
    const data = {
      field: fieldName,
      type,
      oldText: fieldName === 'bankAccountNumbers' ? oldText : oldTextForAgent,
      newText: fieldName === 'bankAccountNumbers' ? newText : account[fieldName]
    }
    changes.push(data)
  }
  return changes
}

export const preparePropertyIdsAndMeta = async (contractIds) => {
  const metaData = []
  const propertyIds = []
  const contracts =
    (await contractHelper.getContracts({
      _id: {
        $in: contractIds
      }
    })) || []
  for (const contractInfo of contracts) {
    propertyIds.push(contractInfo.propertyId)
    metaData.push({
      field: 'assignmentSerial',
      value: contractInfo.assignmentSerial,
      contractId: contractInfo._id,
      propertyId: contractInfo.propertyId
    })
  }
  return { propertyIds, metaData }
}

export const prepareChangesFieldsLogData = async (params, session) => {
  const { account, changesFields, accountPersonInfo, personId } = params
  let { previousDoc } = params
  const newChangesArray = []
  for (const fieldInfo of changesFields) {
    let currentDoc = account
    if (fieldInfo.organizationId) {
      previousDoc = fieldInfo.previousOrganizationData
      currentDoc = await organizationHelper.getAnOrganization(
        { _id: fieldInfo.organizationId, personId },
        session
      )
    } else if (fieldInfo.personId) {
      currentDoc = accountPersonInfo
    }
    const fieldData = {
      field: fieldInfo.fieldName,
      type: 'text',
      oldText:
        size(previousDoc) && previousDoc[fieldInfo.fieldName]
          ? previousDoc[fieldInfo.fieldName]
          : '',
      newText: currentDoc[fieldInfo.fieldName]
    }
    if (fieldInfo.personId) {
      if (fieldInfo.fieldName === 'email') {
        fieldData.oldText = fieldInfo.prevPersonEmail || ''
        fieldData.newText = currentDoc.getEmail()
      } else if (fieldInfo.fieldName === 'phoneNumber') {
        fieldData.oldText = fieldInfo.prevPersonPhone
        fieldData.newText = currentDoc.getPhone()
      } else if (fieldInfo.fieldName === 'name') {
        fieldData.oldText = fieldInfo.prevPersonName
        fieldData.newText = currentDoc.getName()
      } else if (fieldInfo.fieldName === 'norwegianNationalIdentification') {
        fieldData.oldText = fieldInfo.prevPersonNorwegianNationalIdentification
        fieldData.newText =
          currentDoc.getNorwegianNationalIdentification() || ''
      } else if (fieldInfo.fieldName === 'city') {
        fieldData.oldText = fieldInfo.accountPrevCity
        fieldData.newText = currentDoc.getCity() || ''
      } else if (fieldInfo.fieldName === 'zipCode') {
        fieldData.oldText = fieldInfo.accountPrevZipCode
        fieldData.newText = currentDoc.getZipCode() || ''
      } else if (fieldInfo.fieldName === 'country') {
        fieldData.oldText = fieldInfo.accountPrevCountry
        fieldData.newText = currentDoc.getCountry() || ''
      } else {
        fieldData.oldText = fieldInfo.prevPersonAddress || ''
        fieldData.newText = currentDoc.getHometown()
      }
    }
    newChangesArray.push(fieldData)
  }
  return newChangesArray || []
}

export const prepareAccountUpdatedLogData = async (
  action,
  options = {},
  session
) => {
  if (!(action && size(options))) {
    throw new CustomError(400, 'Bad request, Required data missing')
  }
  const {
    collectionId = null,
    partnerId = null,
    personId = null,
    landlordPartnerId = null,
    tenantPartnerId = null,
    previousDoc = {},
    context = '',
    logData = {},
    isResend,
    isDbUpgrade,
    createdAt,
    changesFields = [],
    isVisibleInProperty,
    contractIds,
    accountType
  } = options
  let logUpdatingData = size(logData)
    ? logData
    : pick(options, [
        'partnerId',
        'context',
        'landlordPartnerId',
        'tenantPartnerId'
      ])
  logUpdatingData.action = action
  const query = { partnerId }
  landlordPartnerId ? (query.landlordPartnerId = landlordPartnerId) : ''
  tenantPartnerId ? (query.tenantPartnerId = tenantPartnerId) : ''
  if (partnerId || landlordPartnerId || tenantPartnerId) {
    if (collectionId && action === 'updated_account') {
      query._id = collectionId
      logUpdatingData.isChangeLog = true
      let account = await accountHelper.getAnAccount(query, session)
      const accountPersonInfo =
        size(account) && account.personId
          ? await userHelper.getAnUser({ _id: account.personId }, session)
          : {}
      if (size(previousDoc) && size(changesFields)) {
        // Set changes
        const data = {
          account,
          previousDoc,
          changesFields,
          accountPersonInfo,
          personId
        }
        logUpdatingData.changes = await prepareChangesFieldsLogData(
          data,
          session
        )
      } else {
        // Prepare change log data for AgentId or Bank account numbers (set changes)
        logUpdatingData.changes = prepareAgentOrBankAccountChangeLogData(
          options,
          account
        )
      }
      logUpdatingData.accountId = account._id
      logUpdatingData.visibility = context ? [context] : []
      account = pick(account, ['agentId', 'branchId', 'address'])
      logUpdatingData = assign(logUpdatingData, account) // Extend log data.
      if (isVisibleInProperty && size(contractIds)) {
        // Set visibility, propertyIds & meta
        const { propertyIds, metaData } = await preparePropertyIdsAndMeta(
          contractIds
        )
        if (size(propertyIds)) {
          logUpdatingData.visibility.push('property')
          logUpdatingData.propertyIds = propertyIds
          logUpdatingData.meta = metaData
        }
      }
      if (accountType === 'organization' && previousDoc.type !== 'person') {
        // Set meta
        logUpdatingData.meta = [{ field: 'accountType', value: accountType }] // For determining whether is updating for account or contact person
      }
    }
    isResend ? (logUpdatingData.isResend = true) : ''
    isDbUpgrade && createdAt ? (logUpdatingData.createdAt = createdAt) : ''
    return logUpdatingData // Return
  }
}

export const getPersonChangeFields = (
  presentPersonInfo,
  previousPersonInfo
) => {
  const personChangeFields = []
  if (previousPersonInfo.getHometown() !== presentPersonInfo.getHometown()) {
    personChangeFields.push({
      fieldName: 'address',
      type: 'text',
      personId: presentPersonInfo._id,
      prevPersonAddress: previousPersonInfo.getHometown()
    })
  }
  if (previousPersonInfo.getPhone() !== presentPersonInfo.getPhone()) {
    personChangeFields.push({
      fieldName: 'phoneNumber',
      type: 'text',
      personId: presentPersonInfo._id,
      prevPersonPhone: previousPersonInfo.getPhone()
    })
  }
  if (
    previousPersonInfo.getNorwegianNationalIdentification() !==
    presentPersonInfo.getNorwegianNationalIdentification()
  ) {
    personChangeFields.push({
      fieldName: 'norwegianNationalIdentification',
      type: 'text',
      personId: presentPersonInfo._id,
      prevPersonNorwegianNationalIdentification:
        previousPersonInfo.getNorwegianNationalIdentification()
    })
  }
  if (previousPersonInfo.getEmail() !== presentPersonInfo.getEmail()) {
    personChangeFields.push({
      fieldName: 'email',
      type: 'text',
      personId: presentPersonInfo._id,
      prevPersonEmail: previousPersonInfo.getEmail()
    })
  }
  return { personChangeFields }
}

export const getAccountChangeFields = (params) => {
  const { newAccount, previousAccount, presentPersonInfo, previousPersonInfo } =
    params
  const accountChangeFields = []
  const { type } = newAccount
  if (
    previousAccount.getCity() !== newAccount.getCity() ||
    (size(presentPersonInfo) &&
      size(previousPersonInfo) &&
      previousPersonInfo.getCity() !== presentPersonInfo.getCity())
  ) {
    const personFieldData = { fieldName: 'city', type: 'text' }
    type === 'person'
      ? (personFieldData.accountPrevCity = previousPersonInfo.getCity())
      : (personFieldData.accountPrevCity = previousAccount.getCity())
    type === 'person' ? (personFieldData.personId = presentPersonInfo._id) : ''
    accountChangeFields.push(personFieldData)
  }
  if (
    previousAccount.getZipCode() !== newAccount.getZipCode() ||
    (size(presentPersonInfo) &&
      size(previousPersonInfo) &&
      previousPersonInfo.getZipCode() !== presentPersonInfo.getZipCode())
  ) {
    const personFieldData = { fieldName: 'zipCode', type: 'text' }
    type === 'person'
      ? (personFieldData.accountPrevZipCode = previousPersonInfo.getZipCode())
      : (personFieldData.accountPrevZipCode = previousAccount.getZipCode())
    type === 'person' ? (personFieldData.personId = presentPersonInfo._id) : ''
    accountChangeFields.push(personFieldData)
  }
  if (
    previousAccount.getCountry() !== newAccount.getCountry() ||
    (size(presentPersonInfo) &&
      size(previousPersonInfo) &&
      previousPersonInfo.getCountry() !== presentPersonInfo.getCountry())
  ) {
    const personFieldData = { fieldName: 'country', type: 'text' }
    type === 'person'
      ? (personFieldData.accountPrevCountry = previousPersonInfo.getCountry())
      : (personFieldData.accountPrevCountry = previousAccount.getCountry())
    type === 'person' ? (personFieldData.personId = presentPersonInfo._id) : ''
    accountChangeFields.push(personFieldData)
  }
  if (previousAccount.serial !== newAccount.serial) {
    accountChangeFields.push({ fieldName: 'serial', type: 'text' })
  }
  return { accountChangeFields }
}

export const prepareChangeFieldOptionsData = (params) => {
  const {
    organizationId,
    newAccount,
    previousAccount,
    newOrganization,
    previousOrganization,
    presentPersonInfo,
    previousPersonInfo
  } = params
  const changeFieldOptions = []
  if (size(presentPersonInfo) && size(previousPersonInfo)) {
    const { personChangeFields } = getPersonChangeFields(
      presentPersonInfo,
      previousPersonInfo
    )
    changeFieldOptions.push(...personChangeFields)
  }
  if (size(newAccount) && size(previousAccount)) {
    const accountChangeFieldsData = {
      newAccount,
      previousAccount,
      presentPersonInfo,
      previousPersonInfo
    }
    const { accountChangeFields } = getAccountChangeFields(
      accountChangeFieldsData
    )
    changeFieldOptions.push(...accountChangeFields)
  }
  if (
    size(newOrganization) &&
    size(previousOrganization) &&
    previousOrganization.orgId !== newOrganization.orgId
  ) {
    changeFieldOptions.push({
      fieldName: 'orgId',
      type: 'text',
      previousOrganizationData: previousOrganization,
      organizationId
    })
  }
  if (size(newAccount) && newAccount.type === 'organization') {
    if (
      size(previousAccount) &&
      previousAccount.address !== newAccount.address
    ) {
      changeFieldOptions.push({ fieldName: 'address', type: 'text' })
    }
    if (
      size(previousPersonInfo) &&
      size(presentPersonInfo) &&
      previousPersonInfo.getName() !== presentPersonInfo.getName()
    ) {
      changeFieldOptions.push({
        fieldName: 'name',
        type: 'text',
        personId: presentPersonInfo._id,
        prevPersonName: previousPersonInfo.getName()
      })
    }
  }
  return { changeFieldOptions }
}

const prepareLogMetaForPayout = (payout = {}, options = {}) => {
  const { action, payoutFeedbackHistory } = options || {}

  const logMetaData = []
  if (action === 'added_new_payout' && payout.estimatedAmount) {
    logMetaData.push({
      field: 'estimatedAmount',
      value: payout.estimatedAmount
    })
  }
  if (action === 'updated_payout') {
    if (size(payoutFeedbackHistory)) {
      if (payoutFeedbackHistory.status)
        logMetaData.push({
          field: 'status',
          value: payoutFeedbackHistory.status
        })
      if (payoutFeedbackHistory.reason)
        logMetaData.push({
          field: 'reason',
          value: payoutFeedbackHistory.reason
        })
    } else {
      logMetaData.push({ field: 'status', value: payout.status })

      if (payout.numberOfFails) {
        logMetaData.push({
          field: 'numberOfFails',
          value: payout.numberOfFails
        })
      } else logMetaData.push({ field: 'amount', value: payout.amount })
    }
  }

  return logMetaData
}

export const prepareLogDataForPayout = (options, payout) => {
  const { action, context } = options
  let logData = { action }
  if (payout) {
    logData.payoutId = payout._id
    logData.context = context
    const payoutLogData = pick(payout, [
      'accountId',
      'partnerId',
      'propertyId',
      'agentId',
      'tenantId',
      'branchId',
      'invoiceId'
    ])
    logData = assign(logData, payoutLogData) // Extend log data.
    logData.meta = prepareLogMetaForPayout(payout, options)
    logData.visibility = getLogVisibility(options, payout)
    logData.createdBy = 'SYSTEM'
  }
  return logData
}

export const getTenantChangesFieldsArray = async (
  tenantInfo,
  options,
  session
) => {
  const { previousDoc = {}, changesFields = [] } = options
  const changesArray = []
  if (size(changesFields)) {
    for (const field of changesFields) {
      let userInfo = {}
      const { fieldName, personId = '' } = field
      let oldText =
        previousDoc && previousDoc[fieldName] ? previousDoc[fieldName] : ''
      let newText = tenantInfo[fieldName]
      personId
        ? (userInfo = await userHelper.getAnUser({ _id: personId }, session))
        : ''
      if (size(userInfo)) {
        if (fieldName === 'email') {
          oldText = field.previousEmail
          newText = userInfo.getEmail()
        } else if (fieldName === 'phoneNumber') {
          oldText = field.previousPhone
          newText = userInfo.getPhone()
        } else if (fieldName === 'name') {
          oldText = field.previousName
          newText = userInfo.getName()
        } else if (fieldName === 'norwegianNationalIdentification') {
          oldText = field.previousNID
          newText = userInfo.getNorwegianNationalIdentification()
        }
      } else if (fieldName === 'billingAddress') {
        oldText = field.previousBillingAddress
        newText = tenantInfo ? tenantInfo.getAddress() : ''
      } else if (fieldName === 'serial') {
        oldText = field.previousSerial
        newText = tenantInfo ? tenantInfo.getSerialId() : ''
      } else if (fieldName === 'city') {
        oldText = field.previousCity
        newText = tenantInfo ? tenantInfo.getCity() : ''
      } else if (fieldName === 'zipCode') {
        oldText = field.previousZipCode
        newText = tenantInfo ? tenantInfo.getZipCode() : ''
      } else if (fieldName === 'country') {
        oldText = field.previousCountry
        newText = tenantInfo ? tenantInfo.getCountry() : ''
      }
      const changeData = {
        field: fieldName,
        type: 'text',
        oldText,
        newText
      }
      changesArray.push(changeData)
    }
  }
  return changesArray
}

export const actionChangeLogForTenant = async (params, session) => {
  const { query, logUpdatingData, options } = params
  const { collectionName, collectionId } = options
  const logData = clone(logUpdatingData)
  if (size(query) && collectionName === 'tenant') {
    const tenantInfo = await tenantHelper.getATenant(query, session)
    logData.isChangeLog = true
    logData.visibility = getLogVisibility(options, tenantInfo)
    logData.tenantId = collectionId
    const changesArray = await getTenantChangesFieldsArray(
      tenantInfo,
      options,
      session
    )
    size(changesArray) ? (logData.changes = changesArray) : ''
  }
  return logData
}

export const prepareTenantUpdatedLogData = async (action, options, session) => {
  if (!(action && size(options))) {
    throw new CustomError(400, 'Bad request, Required data missing')
  }
  const {
    collectionId = null,
    partnerId = null,
    landlordPartnerId = null,
    tenantPartnerId = null,
    logData = {},
    isResend,
    isDbUpgrade,
    createdAt
  } = options
  const logUpdatingData = size(logData)
    ? logData
    : pick(options, [
        'partnerId',
        'context',
        'landlordPartnerId',
        'tenantPartnerId'
      ])
  logUpdatingData.action = action
  const query = { partnerId }
  landlordPartnerId ? (query.landlordPartnerId = landlordPartnerId) : ''
  tenantPartnerId ? (query.tenantPartnerId = tenantPartnerId) : ''
  isResend ? (logUpdatingData.isResend = true) : ''
  isDbUpgrade && createdAt ? (logUpdatingData.createdAt = createdAt) : ''
  if (partnerId || landlordPartnerId || tenantPartnerId) {
    if (collectionId && action === 'updated_tenant') {
      query._id = collectionId
      const params = {
        query,
        logUpdatingData,
        options
      }
      const tenantLogData = await actionChangeLogForTenant(params, session)
      return tenantLogData // Return
    }
  }
}

export const prepareLogDataForRemovedEvictionCase = async (
  removedData,
  session
) => {
  const { invoiceId = '', leaseSerial } = removedData
  const invoice = (await invoiceHelper.getInvoiceById(invoiceId, session)) || {}
  const logData = pick(removedData, [
    'contractId',
    'partnerId',
    'propertyId',
    'accountId',
    'tenantId',
    'taskId',
    'invoiceId'
  ])
  logData.meta = []
  logData.meta.push({
    field: 'invoiceSerialId',
    value: invoice.invoiceSerialId
  })
  logData.meta.push({ field: 'leaseSerial', value: leaseSerial })
  logData.action = 'removed_eviction_case'
  logData.collectionName = 'contract'
  logData.context = 'property'
  logData.visibility = getLogVisibility({ context: 'property' }, removedData)
  return logData
}

export const prepareInvoiceLogData = (
  invoice,
  options,
  createdBy = 'SYSTEM'
) => {
  const { action, context } = options || {}
  if (!size(invoice)) {
    return false
  }
  const logData = pick(invoice, [
    'partnerId',
    'accountId',
    'propertyId',
    'agentId',
    'tenantId',
    'branchId',
    'contractId'
  ])
  logData.invoiceId = invoice._id
  logData.action = action
  logData.createdBy = createdBy
  logData.context = context || 'invoice'
  logData.meta = prepareInvoiceMetaData(logData, options)
  logData.visibility = getLogVisibility(options, invoice)
  if (options?.errorText) logData.errorText = options.errorText
  return logData
}

const prepareInvoiceMetaData = function (logData, options) {
  const metaData = []

  if (
    logData?.action === 'invoice_sent_to_vipps' ||
    logData?.action === 'invoice_sent_to_vipps_error'
  )
    metaData.push({ field: 'sendTo', value: 'tenant' })

  if (
    logData?.action === 'invoice_sent_to_vipps_error' &&
    options?.errorTextKey
  )
    metaData.push({ field: 'errorTextKey', value: options.errorTextKey })

  return metaData
}

export const prepareInvoiceDelayDateLogData = (invoice, options) => {
  const logData = prepareInvoiceLogData(invoice, options)
  const { previousDoc = {} } = options
  if (!logData) {
    return false
  }
  logData.isChangeLog = true
  logData.changes = [
    {
      field: 'delayDate',
      type: 'date',
      newDate: invoice.delayDate,
      oldDate: previousDoc.delayDate
    }
  ]
  return logData
}

export const prepareRemovedInvoiceLostLogData = (data, options) => {
  const { invoice = {}, lostMeta = {} } = data
  const logData = prepareInvoiceLogData(invoice, options)
  if (!logData && !lostMeta.amount) {
    return false
  }
  logData.meta = [
    {
      field: 'amount',
      value: lostMeta.amount
    }
  ]
  return logData
}

export const prepareLogDataForNewPayment = async (params, session) => {
  const { logData, options, query } = params

  const payment = await paymentHelper.getPayment(query, session)

  if (!size(payment)) throw new CustomError(404, "Payment doesn't exists")

  const { _id: paymentId, amount, meta: paymentMeta } = payment

  logData.paymentId = paymentId

  const commonIds = pick(payment, [
    'accountId',
    'amount',
    'agentId',
    'branchId',
    'invoiceId',
    'propertyId',
    'tenantId'
  ])

  const newLogData = assign(logData, commonIds) // newLogData = Merged commonIds with logData.
  let metaData = []

  if (amount) metaData = [{ field: 'paymentAmount', value: amount }]

  if (size(paymentMeta)) {
    const { cdTrAccountNumber, dbTrAccountNumber, dbTrName } = paymentMeta

    if (dbTrName)
      metaData.push({ field: 'paymentAccountName', value: dbTrName })

    if (dbTrAccountNumber)
      metaData.push({ field: 'toAccountNumber', value: dbTrAccountNumber })

    if (cdTrAccountNumber)
      metaData.push({ field: 'fromAccountNumber', value: cdTrAccountNumber })
  }

  newLogData.meta = metaData
  newLogData.visibility = getLogVisibility(options, payment)

  return newLogData
}

export const prepareLogDataForRemovePayment = async (params) => {
  const { action, options } = params

  const { logData, partnerId } = options

  if (!partnerId)
    throw new CustomError(404, `Required partnerId for ${action} log creation`)

  if (!size(logData))
    throw new CustomError(
      404,
      `Prepared logData is missing for ${action} log creation`
    )

  logData.action = action

  return logData
}

export const prepareLogDataForCanceledRefundPayment = async (
  params,
  session
) => {
  const { logData, options, query } = params

  const payment = await paymentHelper.getPayment(query, session)

  if (!size(payment)) throw new CustomError(404, "Payment doesn't exists")

  const { _id: paymentId, amount } = payment

  logData.paymentId = paymentId

  const commonIds = pick(payment, [
    'accountId',
    'amount',
    'agentId',
    'branchId',
    'invoiceId',
    'propertyId',
    'tenantId'
  ])

  const newLogData = assign(logData, commonIds) // newLogData = Merged commonIds with logData.
  const metaData = amount ? [{ field: 'paymentAmount', value: amount }] : []

  newLogData.meta = metaData
  newLogData.visibility = getLogVisibility(options, payment)

  return newLogData
}

export const prepareLogDataForUpdateLease = async (contractInfo, options) => {
  const { _id, rentalMeta, accountId, propertyId, agentId, branchId } =
    contractInfo
  const { action = 'updated_lease', fieldName, newVal, oldVal } = options
  let type = 'text'
  const logData = {
    accountId,
    action,
    agentId,
    branchId,
    contractId: _id,
    isChangeLog: true,
    tenantId: rentalMeta.tenantId,
    partnerId: contractInfo.partnerId,
    propertyId
  }
  const visibility = getLogVisibility(logData, contractInfo)
  logData.visibility = union(visibility, ['tenant'])
  if (contractInfo.leaseSerial)
    logData.meta = [{ field: 'leaseSerial', value: contractInfo.leaseSerial }]
  if (fieldName === 'lastCpiDate' || fieldName === 'contractEndDate')
    type = 'date'

  if (options.CPIBasedIncrement)
    logData.meta.push({ field: 'basedOnCPI', value: 'true' })
  console.log(`changes adding -> ${fieldName}, ${oldVal}, ${newVal}`)
  logData.changes = [
    {
      field: fieldName,
      type,
      oldText: `${oldVal}`,
      newText: `${newVal}`
    }
  ]
  return logData
}

const prepareActivityLogsQuery = async (query = {}) => {
  const { context, leaseSerial, partnerId, showMyActivities, userId } = query
  let preparedQuery = {}
  if (partnerId) {
    preparedQuery.$or = [
      { partnerId },
      { landlordPartnerId: partnerId, isMovingInOutProtocolTaskLog: true },
      { tenantPartnerId: partnerId, isMovingInOutProtocolTaskLog: true }
    ]
  }
  let creditRattingQuery = {}
  if (showMyActivities) preparedQuery.agentId = userId
  if (query.accountId) preparedQuery.accountId = query.accountId
  if (query.propertyId)
    preparedQuery = {
      $or: [{ propertyId: query.propertyId }, { propertyIds: query.propertyId }]
    }
  if (query.tenantId) {
    preparedQuery.tenantId = query.tenantId
    creditRattingQuery = {
      partnerId,
      tenantId: query.tenantId,
      context: 'creditRating'
    }
  }
  if (query.invoiceId) preparedQuery.invoiceId = query.invoiceId
  if (query.paymentId) preparedQuery.paymentId = query.paymentId
  if (query.payoutId) preparedQuery.payoutId = query.payoutId
  if (query.correctionId) preparedQuery.correctionId = query.correctionId
  // Field commissionId not exist in log schema
  if (query.commissionId) preparedQuery.commissionId = query.commissionId
  if (query.taskId) preparedQuery.taskId = query.taskId
  if (
    context &&
    context !== 'dashboard' &&
    context !== 'landlordDashboard' &&
    context !== 'tenantDashboard'
  )
    preparedQuery.visibility = context
  if (query.isChangeLog) preparedQuery.isChangeLog = query.isChangeLog
  if (query.contractId) preparedQuery.contractId = query.contractId
  if (query.appInvoiceId) preparedQuery.appInvoiceId = query.appInvoiceId
  // Field depositInsuranceId not exist in log schema
  if (query.depositInsuranceId)
    preparedQuery.depositInsuranceId = query.depositInsuranceId
  if (leaseSerial) {
    preparedQuery.meta = {
      $elemMatch: {
        field: 'leaseSerial',
        value: leaseSerial.toString()
      }
    }
  }
  // Now in v2 this should not be in this way for tenant or lordlord, I think we need to check if user is tenant or landlord
  if (context === 'landlordDashboard' || context === 'tenantDashboard') {
    if (context === 'landlordDashboard') {
      const accountIds = await accountHelper.getAccountIdsByQuery({
        partnerId,
        personId: userId
      })
      preparedQuery.accountId = { $in: accountIds }
      preparedQuery.context = { $in: ['invoice', 'payment', 'payout'] }
    } else {
      preparedQuery.context = { $in: ['invoice', 'payment'] }
    }
    if (preparedQuery.taskId) {
      if (context === 'landlordDashboard') {
        preparedQuery.landlordPartnerId = partnerId
      } else if (context === 'tenantDashboard') {
        preparedQuery.tenantPartnerId = partnerId
      }
      preparedQuery.context = { $in: ['comment', 'task'] }
      delete preparedQuery.partnerId
      delete preparedQuery.accountId
      delete preparedQuery.tenantId
    }
  }
  let logQuery = preparedQuery
  if (size(creditRattingQuery)) {
    logQuery = { $or: [creditRattingQuery, preparedQuery] }
  }
  return logQuery
}

const getCommentPipeline = () => [
  {
    $lookup: {
      from: 'comments',
      localField: 'commentId',
      foreignField: '_id',
      as: 'commentInfo'
    }
  },
  appHelper.getUnwindPipeline('commentInfo')
]

const getChangesPipeline = () => [
  appHelper.getUnwindPipeline('changes'),
  {
    $addFields: {
      collectionName: {
        $switch: {
          branches: [
            {
              case: {
                $in: [
                  '$changes.field',
                  ['assignTo', 'agentId', 'representativeId', 'janitorId']
                ]
              },
              then: 'users'
            },
            {
              case: { $eq: ['$changes.field', 'accountId'] },
              then: 'accounts'
            },
            {
              case: { $eq: ['$changes.field', 'propertyId'] },
              then: 'listings'
            },
            {
              case: { $eq: ['$changes.field', 'tenantId'] },
              then: 'tenants'
            },
            {
              case: { $eq: ['$changes.field', 'invoiceId'] },
              then: 'invoices'
            },
            {
              case: { $eq: ['$changes.field', 'branchId'] },
              then: 'branches'
            }
          ],
          default: ''
        }
      }
    }
  },
  {
    $lookup: {
      from: 'users',
      localField: 'changes.oldId',
      foreignField: '_id',
      as: 'oldAssignee'
    }
  },
  appHelper.getUnwindPipeline('oldAssignee'),
  {
    $lookup: {
      from: 'users',
      localField: 'changes.newId',
      foreignField: '_id',
      as: 'newAssignee'
    }
  },
  appHelper.getUnwindPipeline('newAssignee'),
  {
    $lookup: {
      from: 'users',
      localField: 'changes.oldText',
      foreignField: '_id',
      as: 'oldUser'
    }
  },
  appHelper.getUnwindPipeline('oldUser'),
  {
    $lookup: {
      from: 'users',
      localField: 'changes.newText',
      foreignField: '_id',
      as: 'newUser'
    }
  },
  appHelper.getUnwindPipeline('newUser'),
  {
    $lookup: {
      from: 'accounts',
      localField: 'changes.oldText',
      foreignField: '_id',
      as: 'oldAccount'
    }
  },
  appHelper.getUnwindPipeline('oldAccount'),
  {
    $lookup: {
      from: 'accounts',
      localField: 'changes.newText',
      foreignField: '_id',
      as: 'newAccount'
    }
  },
  appHelper.getUnwindPipeline('newAccount'),
  {
    $lookup: {
      from: 'listings',
      localField: 'changes.oldText',
      foreignField: '_id',
      as: 'oldProperty'
    }
  },
  appHelper.getUnwindPipeline('oldProperty'),
  {
    $lookup: {
      from: 'listings',
      localField: 'changes.newText',
      foreignField: '_id',
      as: 'newProperty'
    }
  },
  appHelper.getUnwindPipeline('newProperty'),
  {
    $lookup: {
      from: 'tenants',
      localField: 'changes.oldText',
      foreignField: '_id',
      as: 'oldTenant'
    }
  },
  appHelper.getUnwindPipeline('oldTenant'),
  {
    $lookup: {
      from: 'tenants',
      localField: 'changes.newText',
      foreignField: '_id',
      as: 'newTenant'
    }
  },
  appHelper.getUnwindPipeline('newTenant'),
  {
    $lookup: {
      from: 'invoices',
      localField: 'changes.oldText',
      foreignField: '_id',
      as: 'oldInvoice'
    }
  },
  appHelper.getUnwindPipeline('oldInvoice'),
  {
    $lookup: {
      from: 'invoices',
      localField: 'changes.newText',
      foreignField: '_id',
      as: 'newInvoice'
    }
  },
  appHelper.getUnwindPipeline('newInvoice'),
  {
    $lookup: {
      from: 'branches',
      localField: 'changes.oldText',
      foreignField: '_id',
      as: 'oldBranch'
    }
  },
  appHelper.getUnwindPipeline('oldBranch'),
  {
    $lookup: {
      from: 'branches',
      localField: 'changes.newText',
      foreignField: '_id',
      as: 'newBranch'
    }
  },
  appHelper.getUnwindPipeline('newBranch'),
  {
    $addFields: {
      'changes.oldValue': {
        $switch: {
          branches: [
            {
              case: {
                $not: { $ifNull: ['$changes', false] }
              },
              then: '$$REMOVE'
            },
            {
              case: { $eq: ['$collectionName', 'accounts'] },
              then: '$oldAccount.name'
            },
            {
              case: { $eq: ['$collectionName', 'tenants'] },
              then: '$oldTenant.name'
            },
            {
              case: { $eq: ['$collectionName', 'branches'] },
              then: '$oldBranch.name'
            },
            {
              case: { $eq: ['$collectionName', 'users'] },
              then: '$oldUser.profile.name'
            },
            {
              case: { $eq: ['$collectionName', 'invoices'] },
              then: '$oldInvoice.invoiceSerialId'
            },
            {
              case: {
                $eq: ['$collectionName', 'listings']
              },
              then: '$oldProperty.location.name'
            }
          ],
          default: ''
        }
      },
      'changes.newValue': {
        $switch: {
          branches: [
            {
              case: {
                $not: { $ifNull: ['$changes', false] }
              },
              then: '$$REMOVE'
            },
            {
              case: { $eq: ['$collectionName', 'accounts'] },
              then: '$newAccount.name'
            },
            {
              case: { $eq: ['$collectionName', 'tenants'] },
              then: '$newTenant.name'
            },
            {
              case: { $eq: ['$collectionName', 'branches'] },
              then: '$newBranch.name'
            },
            {
              case: { $eq: ['$collectionName', 'users'] },
              then: '$newUser.profile.name'
            },
            {
              case: { $eq: ['$collectionName', 'invoices'] },
              then: '$newInvoice.invoiceSerialId'
            },
            {
              case: {
                $eq: ['$collectionName', 'listings']
              },
              then: '$newProperty.location.name'
            }
          ],
          default: ''
        }
      },
      'changes.oldAssigneeName': {
        $cond: [
          {
            $not: { $ifNull: ['$changes', false] }
          },
          '$$REMOVE',
          '$oldAssignee.profile.name'
        ]
      },
      'changes.newAssigneeName': {
        $cond: [
          {
            $not: { $ifNull: ['$changes', false] }
          },
          '$$REMOVE',
          '$newAssignee.profile.name'
        ]
      }
    }
  },
  {
    $addFields: {
      changes: {
        $cond: [{ $gt: ['$changes', {}] }, '$changes', '$$REMOVE']
      }
    }
  },
  {
    $group: {
      _id: '$_id',
      changes: {
        $push: '$changes'
      },
      createdBy: {
        $first: '$createdBy'
      },
      context: {
        $first: '$context'
      },
      action: {
        $first: '$action'
      },
      createdAt: {
        $first: '$createdAt'
      },
      commentId: {
        $first: '$commentId'
      },
      propertyId: {
        $first: '$propertyId'
      },
      tenantId: {
        $first: '$tenantId'
      },
      payoutId: {
        $first: '$payoutId'
      },
      taskId: {
        $first: '$taskId'
      },
      invoiceId: {
        $first: '$invoiceId'
      },
      accountId: {
        $first: '$accountId'
      },
      paymentId: {
        $first: '$paymentId'
      },
      messageId: {
        $first: '$messageId'
      },
      fileId: {
        $first: '$fileId'
      },
      meta: {
        $first: '$meta'
      },
      notificationLogId: {
        $first: '$notificationLogId'
      },
      agentId: {
        $first: '$agentId'
      },
      contractId: {
        $first: '$contractId'
      },
      correctionId: {
        $first: '$correctionId'
      },
      reason: {
        $first: '$reason'
      },
      movingId: {
        $first: '$movingId'
      },
      annualStatementId: {
        $first: '$annualStatementId'
      },
      isResend: {
        $first: '$isResend'
      },
      errorText: {
        $first: '$errorText'
      },
      isChangeLog: {
        $first: '$isChangeLog'
      }
    }
  }
]

const getCreatedByPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'createdBy',
      foreignField: '_id',
      as: 'createdByInfo'
    }
  },
  appHelper.getUnwindPipeline('createdByInfo')
]

const getPropertyPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo')
]

const getTenantPipeline = () => [
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

const getPayoutPipeline = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: 'payoutId',
      foreignField: '_id',
      as: 'payoutInfo'
    }
  },
  appHelper.getUnwindPipeline('payoutInfo')
]

const getTaskPipeline = () => [
  {
    $lookup: {
      from: 'tasks',
      localField: 'taskId',
      foreignField: '_id',
      as: 'taskInfo'
    }
  },
  appHelper.getUnwindPipeline('taskInfo')
]

const getInvoicePipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo')
]

const getAppInvoicePipeline = () => [
  {
    $lookup: {
      from: 'app_invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      as: 'appInvoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('appInvoiceInfo')
]

const getAccountPipeline = () => [
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

const getConversationMessagePipeline = () => [
  {
    $lookup: {
      from: 'conversation-messages',
      localField: 'messageId',
      foreignField: '_id',
      as: 'conversationMessageInfo'
    }
  },
  appHelper.getUnwindPipeline('conversationMessageInfo')
]

const getFilePipeline = () => {
  const cdn = appHelper.getCDNDomain()
  const folder = settings.S3.Directives['Files'].folder
  return [
    {
      $lookup: {
        from: 'files',
        localField: 'fileId',
        foreignField: '_id',
        as: 'fileInfo'
      }
    },
    // To match those files which don't have taskId
    {
      $addFields: {
        taskId: {
          $ifNull: ['$taskId', '$$REMOVE']
        }
      }
    },
    {
      $addFields: {
        fileInfo: {
          $first: {
            $filter: {
              input: { $ifNull: ['$fileInfo', []] },
              as: 'file',
              cond: {
                $eq: ['$$file.taskId', '$taskId']
              }
            }
          }
        }
      }
    },
    {
      $addFields: {
        findFileNameMeta: {
          $first: {
            $filter: {
              input: { $ifNull: ['$meta', []] },
              as: 'metaObj',
              cond: {
                $eq: ['$$metaObj.field', 'fileName']
              }
            }
          }
        }
      }
    },
    {
      $addFields: {
        extension: {
          $last: {
            $split: [{ $ifNull: ['$fileInfo.name', ''] }, '.']
          }
        },
        filePartnerId: {
          $switch: {
            branches: [
              {
                case: { $ifNull: ['$fileInfo.partnerId', false] },
                then: '$fileInfo.partnerId'
              },
              {
                case: { $ifNull: ['$fileInfo.landlordPartnerId', false] },
                then: '$fileInfo.landlordPartnerId'
              },
              {
                case: { $ifNull: ['$fileInfo.tenantPartnerId', false] },
                then: '$fileInfo.tenantPartnerId'
              }
            ],
            default: null
          }
        },
        'fileInfo.title': {
          $ifNull: ['$findFileNameMeta.value', '$fileInfo.title']
        }
      }
    },
    {
      $addFields: {
        'fileInfo.fileSrc': {
          $cond: [
            {
              $in: ['$extension', ['jpg', 'png', 'jpeg', 'gif', 'svg']]
            },
            {
              $concat: [
                cdn,
                '/',
                folder,
                '/',
                '$filePartnerId',
                '/',
                '$fileInfo.context',
                '/',
                '$fileInfo.name'
              ]
            },
            null
          ]
        }
      }
    }
  ]
}

const getMetaInfoPipeline = () => [
  {
    $addFields: {
      findLeaseSerialMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'leaseSerial']
            }
          }
        }
      },
      findAssignmentSerialMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'assignmentSerial']
            }
          }
        }
      },
      findAccountIdMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'accountId']
            }
          }
        }
      },
      findTenantIdMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'tenantId']
            }
          }
        }
      },
      findSendToMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'sendTo']
            }
          }
        }
      },
      findErrorTextKeyMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'errorTextKey']
            }
          }
        }
      },
      findBankAccountNumberMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'bankAccountNumbers']
            }
          }
        }
      },
      findAddonIdMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$meta', []] },
            as: 'metaObj',
            cond: {
              $eq: ['$$metaObj.field', 'addonId']
            }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'accounts',
      localField: 'findAccountIdMeta.value',
      foreignField: '_id',
      as: 'metaAccountInfo'
    }
  },
  appHelper.getUnwindPipeline('metaAccountInfo'),
  {
    $lookup: {
      from: 'tenants',
      localField: 'findTenantIdMeta.value',
      foreignField: '_id',
      as: 'metaTenantInfo'
    }
  },
  appHelper.getUnwindPipeline('metaTenantInfo'),
  {
    $lookup: {
      from: 'products_services',
      localField: 'findAddonIdMeta.value',
      foreignField: '_id',
      as: 'metaAddonInfo'
    }
  },
  appHelper.getUnwindPipeline('metaAddonInfo'),
  {
    $addFields: {
      'metaInfo.leaseSerial': '$findLeaseSerialMeta.value',
      'metaInfo.assignmentSerial': '$findAssignmentSerialMeta.value',
      'metaInfo.sendTo': '$findSendToMeta.value',
      'metaInfo.toEmail': '$findSendToMeta.toEmail',
      'metaInfo.errorTextKey': '$findErrorTextKeyMeta.value',
      'metaInfo.bankAccountNumber': '$findBankAccountNumberMeta.value',
      'metaInfo.accountInfo': {
        $cond: [
          { $eq: ['$findSendToMeta.value', 'account'] },
          '$metaAccountInfo',
          null
        ]
      },
      'metaInfo.tenantInfo': {
        $cond: [
          { $eq: ['$findSendToMeta.value', 'tenant'] },
          '$metaTenantInfo',
          null
        ]
      },
      'metaInfo.addonInfo': '$metaAddonInfo'
    }
  }
]

const getPdfFilePipeline = () => {
  const getAttachmentMetaAllowedAction = [
    'sent_payout_email',
    'sent_eviction_notice_email',
    'send_welcome_lease_email',
    'send_welcome_lease_sms',
    'sent_tenant_lease_esigning_email',
    'send_landlord_annual_statement_email',
    'send_landlord_annual_statement_sms',
    'sent_next_schedule_payout_email',
    'sent_invoice_email',
    'sent_app_invoice_email',
    'sent_assignment_email',
    'sent_deposit_insurance_created_email',
    'sent_CPI_settlement_notice_email'
  ]
  return [
    {
      $addFields: {
        pdfTypes: {
          $switch: {
            branches: [
              {
                case: {
                  $eq: ['$action', 'sent_due_reminder_email']
                },
                then: ['pre_reminder_pdf', 'pre_reminder_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_first_reminder_email']
                },
                then: ['first_reminder_pdf', 'first_reminder_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_second_reminder_email']
                },
                then: ['second_reminder_pdf', 'second_reminder_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_collection_notice_email']
                },
                then: [
                  'collection_notice_pdf',
                  'collection_notice_attachment_pdf'
                ]
              },
              // {
              //   case: {
              //     $eq: ['$action', 'sent_eviction_notice_email']
              //   },
              //   then: ['eviction_notice_attachment_pdf']
              // },
              // {
              //   case: {
              //     $eq: ['$action', 'sent_eviction_due_reminder_notice_email']
              //   },
              //   then: ['eviction_due_reminder_notice_attachment_pdf']
              // },
              {
                case: {
                  $eq: ['$action', 'sent_payout_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_assignment_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_tenant_lease_esigning_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $and: [
                    { $eq: ['$action', 'sent_invoice_email'] },
                    { $eq: ['$invoiceInfo.invoiceType', 'credit_note'] }
                  ]
                },
                then: ['credit_note_attachment_pdf', 'credit_note_pdf']
              },
              {
                case: {
                  $and: [
                    { $eq: ['$action', 'sent_invoice_email'] },
                    {
                      $not: { $eq: ['$invoiceInfo.invoiceType', 'credit_note'] }
                    }
                  ]
                },
                then: ['invoice_attachment_pdf', 'invoice_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_app_invoice_email']
                },
                then: ['app_invoice_pdf']
              },
              {
                case: {
                  $and: [
                    {
                      $in: [
                        '$action',
                        [
                          'sent_landlord_invoice_email',
                          'sent_final_settlement_email'
                        ]
                      ]
                    },
                    {
                      $eq: ['$invoiceInfo.invoiceType', 'landlord_credit_note']
                    }
                  ]
                },
                then: [
                  'landlord_credit_note_attachment_pdf',
                  'landlord_credit_note_pdf'
                ]
              },
              {
                case: {
                  $and: [
                    {
                      $in: [
                        '$action',
                        [
                          'sent_landlord_invoice_email',
                          'sent_final_settlement_email'
                        ]
                      ]
                    },
                    {
                      $not: {
                        $eq: [
                          '$invoiceInfo.invoiceType',
                          'landlord_credit_note'
                        ]
                      }
                    }
                  ]
                },
                then: [
                  'landlord_invoice_attachment_pdf',
                  'landlord_invoice_pdf'
                ]
              },
              {
                case: {
                  $eq: ['$action', 'sent_CPI_settlement_notice_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_next_schedule_payout_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'send_welcome_lease_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'send_welcome_lease_sms']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'send_landlord_annual_statement_email']
                },
                then: ['lease_statement_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'send_landlord_annual_statement_sms']
                },
                then: ['lease_statement_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_deposit_insurance_created_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $eq: ['$action', 'sent_deposit_insurance_created_email']
                },
                then: ['email_attachment_pdf']
              },
              {
                case: {
                  $in: [
                    '$action',
                    ['invoice_sent_to_vipps', 'invoice_sent_to_vipps_error']
                  ]
                },
                then: ['invoice_attachment_pdf', 'invoice_pdf']
              },
              {
                case: {
                  $in: [
                    '$action',
                    [
                      'invoice_sent_to_compello',
                      'invoice_sent_to_compello_error'
                    ]
                  ]
                },
                then: ['invoice_attachment_pdf', 'invoice_pdf']
              }
            ],
            default: []
          }
        }
      }
    },
    {
      $addFields: {
        pdfList: {
          $switch: {
            branches: [
              {
                case: {
                  $in: [
                    '$action',
                    [
                      'sent_payout_email',
                      'sent_assignment_email',
                      'sent_tenant_lease_esigning_email',
                      'sent_CPI_settlement_notice_email',
                      'sent_next_schedule_payout_email',
                      'send_welcome_lease_email',
                      'send_welcome_lease_sms',
                      'send_landlord_annual_statement_email',
                      'send_landlord_annual_statement_sms',
                      'sent_deposit_insurance_created_email'
                    ]
                  ]
                },
                then: []
              },
              {
                case: {
                  $eq: ['$action', 'sent_app_invoice_email']
                },
                then: '$appInvoiceInfo.pdf'
              }
            ],
            default: '$invoiceInfo.pdf'
          }
        }
      }
    },
    {
      $lookup: {
        from: 'notification_logs',
        localField: 'notificationLogId',
        foreignField: '_id',
        as: 'notificationLogInfo'
      }
    },
    appHelper.getUnwindPipeline('notificationLogInfo'),
    {
      $lookup: {
        from: 'users',
        localField: 'notificationLogInfo.toUserId',
        foreignField: '_id',
        as: 'notificationLogAgentInfo'
      }
    },
    appHelper.getUnwindPipeline('notificationLogAgentInfo'),
    {
      $addFields: {
        notificationLogPdfs: {
          $cond: [
            { $in: ['$action', getAttachmentMetaAllowedAction] },
            { $ifNull: ['$notificationLogInfo.attachmentsMeta', []] },
            []
          ]
        }
      }
    },
    {
      $addFields: {
        pdfList: {
          $filter: {
            input: { $ifNull: ['$pdfList', []] },
            as: 'file',
            cond: {
              $in: ['$$file.type', '$pdfTypes']
            }
          }
        }
      }
    },
    {
      $addFields: {
        pdf: {
          $concatArrays: ['$pdfList', '$notificationLogPdfs']
        }
      }
    },
    {
      $lookup: {
        from: 'files',
        localField: 'pdf.fileId',
        foreignField: '_id',
        as: 'pdfFilesInfo'
      }
    }
  ]
}

const getAgentInfoPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agentId',
      foreignField: '_id',
      as: 'agentInfo'
    }
  },
  appHelper.getUnwindPipeline('agentInfo')
]

const getPaymentInfoPipeline = () => [
  {
    $lookup: {
      from: 'invoice-payments',
      localField: 'paymentId',
      foreignField: '_id',
      as: 'paymentInfo'
    }
  },
  appHelper.getUnwindPipeline('paymentInfo'),
  {
    $lookup: {
      from: 'invoices',
      localField: 'paymentInfo.invoiceId',
      foreignField: '_id',
      as: 'paymentInvoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('paymentInvoiceInfo'),
  {
    $lookup: {
      from: 'app_invoices',
      localField: 'paymentInfo.appInvoiceId',
      foreignField: '_id',
      as: 'paymentAppInvoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('paymentAppInvoiceInfo'),
  {
    $lookup: {
      from: 'accounts',
      localField: 'paymentInfo.accountId',
      foreignField: '_id',
      as: 'paymentAccountInfo'
    }
  },
  appHelper.getUnwindPipeline('paymentAccountInfo'),
  {
    $addFields: {
      'paymentInfo.accountInfo': {
        $cond: [
          { $ifNull: ['$paymentInfo', false] },
          '$paymentAccountInfo',
          '$$REMOVE'
        ]
      },
      'paymentInfo.invoiceInfo': {
        $cond: [
          { $ifNull: ['$paymentInvoiceInfo', false] },
          '$paymentInvoiceInfo',
          '$$REMOVE'
        ]
      },
      'paymentInfo.appInvoiceInfo': {
        $cond: [
          { $ifNull: ['$paymentAppInvoiceInfo', false] },
          '$paymentAppInvoiceInfo',
          '$$REMOVE'
        ]
      }
    }
  }
]

const getContractInfoPipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'contractInfo'
    }
  },
  appHelper.getUnwindPipeline('contractInfo')
]

const getCorrectionInfoPipeline = () => [
  {
    $lookup: {
      from: 'expenses',
      localField: 'correctionId',
      foreignField: '_id',
      as: 'correctionInfo'
    }
  },
  appHelper.getUnwindPipeline('correctionInfo')
]

const getMovingInfoPipeline = () => [
  {
    $lookup: {
      from: 'property_items',
      localField: 'movingId',
      foreignField: '_id',
      as: 'movingInfo'
    }
  },
  appHelper.getUnwindPipeline('movingInfo')
]

const getAnnualStatementPipeline = () => [
  {
    $lookup: {
      from: 'annual_statements',
      localField: 'annualStatementId',
      foreignField: '_id',
      as: 'annualStatementInfo'
    }
  },
  appHelper.getUnwindPipeline('annualStatementInfo')
]

const getDepositAccountPipeline = () => [
  {
    $lookup: {
      from: 'deposit_accounts',
      localField: 'contractId',
      foreignField: 'contractId',
      as: 'depositAccountInfo'
    }
  },
  {
    $addFields: {
      depositAccountInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$depositAccountInfo', []] },
            as: 'depositAccount',
            cond: {
              $eq: ['$$depositAccount.tenantId', '$tenantId']
            }
          }
        }
      }
    }
  }
]

const getActivityLogs = async (body) => {
  const { options, query } = body
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
    ...getChangesPipeline(),
    ...getCommentPipeline(),
    ...getCreatedByPipeline(),
    ...getPropertyPipeline(),
    ...getTenantPipeline(),
    ...getPayoutPipeline(),
    ...getTaskPipeline(),
    ...getInvoicePipeline(),
    ...getAppInvoicePipeline(),
    ...getAccountPipeline(),
    ...getConversationMessagePipeline(),
    ...getFilePipeline(),
    ...getMetaInfoPipeline(),
    ...getPdfFilePipeline(),
    ...getAgentInfoPipeline(),
    ...getPaymentInfoPipeline(),
    ...getContractInfoPipeline(),
    ...getCorrectionInfoPipeline(),
    ...getMovingInfoPipeline(),
    ...getAnnualStatementPipeline(),
    ...getDepositAccountPipeline(),
    {
      $sort: sort
    },
    ...getFinalProjectPipeline()
  ]
  const logs = (await LogCollection.aggregate(pipeline)) || []
  return logs
}

const getFinalProjectPipeline = () => [
  {
    $project: {
      _id: 1,
      changes: 1,
      context: 1,
      action: 1,
      createdAt: 1,
      isResend: 1,
      createdByInfo: {
        _id: 1,
        avatarKey: appHelper.getUserAvatarKeyPipeline(
          '$createdByInfo.profile.avatarKey'
        ),
        name: '$createdByInfo.profile.name'
      },
      createdBy: 1,
      commentInfo: {
        _id: 1,
        content: 1
      },
      propertyInfo: {
        _id: 1,
        locationName: '$propertyInfo.location.name',
        title: 1
      },
      tenantId: 1,
      tenantInfo: {
        _id: 1,
        name: 1
      },
      payoutInfo: {
        _id: 1,
        serialId: 1
      },
      taskInfo: {
        _id: 1,
        title: 1
      },
      invoiceInfo: {
        _id: 1,
        creditReason: 1,
        invoiceSerialId: 1,
        invoiceType: 1,
        isFinalSettlement: 1,
        isPayable: 1,
        dueDate: 1,
        invoiceTotal: 1
      },
      appInvoiceInfo: {
        _id: 1,
        invoiceSerialId: '$appInvoiceInfo.serialId',
        invoiceType: 1
      },
      accountInfo: {
        _id: 1,
        name: 1,
        serial: 1
      },
      conversationMessageInfo: {
        _id: 1,
        content: 1
      },
      fileInfo: {
        _id: 1,
        name: 1,
        title: 1,
        fileSrc: 1
      },
      pdfFilesInfo: {
        _id: 1,
        name: 1,
        title: 1,
        type: 1
      },
      notificationLogAgentInfo: {
        _id: 1,
        name: '$notificationLogAgentInfo.profile.name'
      },
      metaInfo: {
        leaseSerial: 1,
        assignmentSerial: 1,
        sendTo: 1,
        toEmail: 1,
        errorTextKey: 1,
        bankAccountNumber: 1,
        accountInfo: {
          _id: 1,
          name: 1
        },
        tenantInfo: {
          _id: 1,
          name: 1
        },
        addonInfo: {
          _id: 1,
          name: 1
        }
      },
      meta: {
        field: 1,
        value: 1,
        contractId: 1,
        propertyId: 1,
        toEmail: 1
      },
      agentInfo: {
        _id: 1,
        name: '$agentInfo.profile.name'
      },
      paymentInfo: {
        _id: 1,
        amount: 1,
        paymentType: 1,
        type: 1,
        accountInfo: {
          _id: 1,
          name: 1
        },
        invoiceInfo: {
          _id: 1,
          invoiceSerialId: 1
        },
        appInvoiceInfo: {
          _id: 1,
          serialId: 1
        },
        isDepositInsurancePayment: 1
      },
      reason: 1,
      contractInfo: {
        _id: 1,
        assignmentSerial: 1,
        leaseSerial: 1,
        assignmentSigningMeta: 1,
        leaseSigningMeta: '$contractInfo.rentalMeta.leaseSigningMeta'
      },
      correctionInfo: {
        _id: 1,
        correctionSerialId: 1
      },
      movingInfo: {
        _id: 1,
        movingSigningMeta: 1
      },
      annualStatementInfo: {
        _id: 1,
        statementYear: 1,
        status: 1,
        createdAt: 1
      },
      depositAccountInfo: {
        _id: 1,
        bankAccountNumber: 1
      },
      errorText: 1,
      isChangeLog: 1,
      notificationLogId: 1,
      notificationLogInfo: {
        _id: 1,
        type: 1,
        status: 1,
        msgOpenCount: 1,
        msgClickCount: 1,
        toUserId: 1
      }
    }
  }
]

export const queryActivityLogs = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { roles } = user
  if (!roles.includes('app_admin')) {
    appHelper.checkRequiredFields(['partnerId'], user)
    appHelper.validateId({ partnerId: user.partnerId })
  }
  const { partnerId, userId } = user
  const { options, query } = body
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  query.userId = userId
  const preparedQuery = await prepareActivityLogsQuery(query)
  body.query = preparedQuery
  const logs = await getActivityLogs(body)
  const filteredDocuments = await countLogs(preparedQuery)
  const totalDocuments = await countLogs({
    $or: [
      { partnerId },
      { landlordPartnerId: partnerId, isMovingInOutProtocolTaskLog: true },
      { tenantPartnerId: partnerId, isMovingInOutProtocolTaskLog: true }
    ]
  })
  return {
    data: logs,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const prepareLogDataForUpdatePayment = async (params, session) => {
  const { logData, options, query } = params

  const payment = await paymentHelper.getPayment(query, session)

  if (!size(payment)) throw new CustomError(404, "Payment doesn't exists")

  const { previousDoc = {} } = options

  const { _id: paymentId } = payment

  const paymentMeta = payment.meta || {}
  const previousMeta = previousDoc?.meta || {}
  const currentAccountNumber = paymentMeta?.cdTrAccountNumber || ''
  const previousAccountNumber = previousMeta?.cdTrAccountNumber || ''

  const paymentData = pick(payment, [
    'accountId',
    'agentId',
    'branchId',
    'invoiceId',
    'propertyId',
    'tenantId'
  ])

  const newLogData = assign(logData, paymentData) // Extend log data with paymentData

  if (size(previousDoc) && size(payment)) {
    const changes = []

    if (currentAccountNumber !== previousAccountNumber) {
      changes.push({
        field: 'accountNumber',
        oldText: previousAccountNumber,
        newText: currentAccountNumber,
        type: 'number'
      })
    }

    if (payment.amount !== previousDoc?.amount) {
      changes.push({
        field: 'amount',
        oldText: previousDoc.amount,
        newText: payment.amount || 0,
        type: 'number'
      })
    }

    if (payment.invoiceId !== previousDoc.invoiceId) {
      changes.push({
        field: 'invoiceId',
        oldText: previousDoc.invoiceId,
        newText: payment.invoiceId,
        type: 'foreignKey'
      })
    }

    if (
      moment(payment.paymentDate).format('YYYY-MM-DD') !==
      moment(previousDoc.paymentDate).format('YYYY-MM-DD')
    ) {
      changes.push({
        field: 'paymentDate',
        oldText: previousDoc.paymentDate,
        newText: payment.paymentDate,
        type: 'date'
      })
    }

    newLogData.isChangeLog = true
    newLogData.paymentId = paymentId
    newLogData.visibility = getLogVisibility(options, payment)

    if (size(changes)) newLogData.changes = changes
  }

  return newLogData
}

export const prepareLogDataForUpdateRefundPayment = async (params, session) => {
  const { logData, options, query } = params

  const payment = await paymentHelper.getPayment(query, session)

  if (!size(payment)) throw new CustomError(404, "Payment doesn't exists")

  const { refundPaymentFeedbackHistory = {} } = options

  const { _id: paymentId } = payment

  logData.paymentId = paymentId

  const paymentData = pick(payment, [
    'accountId',
    'agentId',
    'branchId',
    'invoiceId',
    'propertyId',
    'tenantId'
  ])

  const newLogData = assign(logData, paymentData) // Extend log data with paymentData

  if (size(refundPaymentFeedbackHistory)) {
    const metaData = []

    if (refundPaymentFeedbackHistory.status)
      metaData.push({
        field: 'status',
        value: refundPaymentFeedbackHistory.status
      })

    if (refundPaymentFeedbackHistory.reason)
      metaData.push({
        field: 'reason',
        value: refundPaymentFeedbackHistory.reason
      })

    if (size(metaData)) newLogData.meta = metaData
  } else {
    newLogData.meta = [
      { field: 'amount', value: payment.amount },
      { field: 'status', value: payment.refundStatus }
    ]

    if (payment.numberOfFails)
      newLogData.Meta = [
        { field: 'status', value: payment.refundStatus },
        {
          field: 'numberOfFails',
          value: payment.numberOfFails
        }
      ]
  }

  newLogData.visibility = getLogVisibility(options, payment)

  return newLogData
}

export const prepareInvoiceLostLogData = (invoice, createdBy) => {
  if (!size(invoice)) return false
  const logData = pick(invoice, [
    'accountId',
    'agentId',
    'branchId',
    'contractId',
    'partnerId',
    'propertyId',
    'tenantId'
  ])
  logData.invoiceId = invoice._id
  logData.action = 'lost_invoice'
  logData.createdBy = createdBy || 'SYSTEM'
  logData.context = 'invoice'
  logData.visibility = getLogVisibility(
    {
      context: 'invoice'
    },
    invoice
  )
  return logData
}

export const prepareCompelloInvoiceLogData = (
  invoice,
  options = {},
  createdBy = 'SYSTEM'
) => {
  const { action, context } = options
  if (!size(invoice)) return []
  const metaData = []
  const logData = pick(invoice, [
    'accountId',
    'agentId',
    'branchId',
    'contractId',
    'partnerId',
    'propertyId',
    'tenantId'
  ])

  if (
    options?.action === 'invoice_sent_to_compello' ||
    options?.action === 'invoice_sent_to_compello_error'
  ) {
    metaData.push({ field: 'sendTo', value: 'tenant' })
  }

  if (
    options?.action === 'invoice_sent_to_compello_error' &&
    options?.errorTextKey
  ) {
    metaData.push({ field: 'errorTextKey', value: options.errorTextKey })
  }
  logData.invoiceId = invoice._id
  logData.action = action
  logData.createdBy = createdBy
  logData.context = context || 'invoice'
  logData.meta = metaData
  logData.visibility = getLogVisibility(options, invoice)
  if (options?.errorText) logData.errorText = options.errorText
  return logData
}
