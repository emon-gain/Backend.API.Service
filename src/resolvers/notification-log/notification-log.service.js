import { indexOf, omit, size, sortBy } from 'lodash'
import moment from 'moment-timezone'
import { CustomError } from '../common'
import { NotificationLogCollection } from '../models'
import {
  accountHelper,
  appHelper,
  appQueueHelper,
  fileHelper,
  invoiceHelper,
  notificationLogHelper,
  partnerHelper,
  partnerSettingHelper,
  propertyHelper,
  tenantHelper
} from '../helpers'
import {
  appQueueService,
  appInvoiceService,
  contractService,
  invoiceService,
  logService,
  partnerUsageService
} from '../services'

export const createANotificationLog = async (data, session) => {
  if (!size(data))
    throw new CustomError(404, `Unable to create notification log`)

  const notificationLog = await NotificationLogCollection.create([data], {
    session
  })

  if (!size(notificationLog))
    throw new CustomError(404, `Unable to create notification log`)

  return notificationLog
}

export const createNotificationLogs = async (data, session) => {
  if (!size(data))
    throw new CustomError(404, `Unable to create notification log`)

  const notificationLogs = await NotificationLogCollection.insertMany(data, {
    session,
    runValidators: true
  })

  if (!size(notificationLogs))
    throw new CustomError(404, `Unable to create notification log`)

  return notificationLogs
}

export const updateNotificationLogs = async (query, data, session) => {
  if (!(size(data) && size(query))) {
    throw new CustomError(404, 'Can not update notification log')
  }
  const updatedNotificationLogs = await NotificationLogCollection.updateMany(
    query,
    data,
    {
      session,
      runValidators: true
    }
  )
  return updatedNotificationLogs
}

export const updateNotificationLogsByActionType = async (options, session) => {
  appHelper.checkRequiredFields(['status', 'type'], options)
  const { limit = 20, status, type } = options
  const params = {
    query: { type, status: 'ready' },
    options: { limit, sort: { createdAt: 1 } }
  }
  const notificationLogs =
    await notificationLogHelper.getNotificationLogsForQuery(params)
  const query = { _id: { $in: notificationLogs.map((log) => log._id) } }

  const setData = { status }
  if (status === 'processing') {
    setData.processStartedAt = new Date()
  }
  const updateData = {
    $set: setData,
    $push: {
      events: {
        status,
        createdAt: new Date()
      }
    }
  }
  const response = await updateNotificationLogs(query, updateData, session)
  let updatedNotificationLogs = []
  if (response.nModified >= 1) {
    updatedNotificationLogs = await notificationLogHelper.getNotificationLogs(
      query,
      session,
      { _id: 1 }
    )
  }
  return updatedNotificationLogs
}

export const updateNotificationLog = async (query, data, session) => {
  if (!(size(data) && size(query))) {
    throw new CustomError(404, 'Can not update notification log')
  }
  const updatedNotificationLog =
    await NotificationLogCollection.findOneAndUpdate(query, data, {
      runValidators: true,
      new: true,
      session
    })

  return updatedNotificationLog
}

export const createLogForSentEmailAndSms = async (notificationLog, session) => {
  const logData = await notificationLogHelper.prepareCreateLogData(
    notificationLog,
    session
  )

  if (!logData) return {}

  const log = await logService.createLog(logData, session)
  return log
}

export const addPartnerUsagesForSentSMS = async (notificationLog, session) => {
  const partnerUsageData =
    notificationLogHelper.preparePartnerUsageData(notificationLog)
  const addedUsage = await partnerUsageService.createAPartnerUsage(
    partnerUsageData,
    session
  )
  return addedUsage
}

export const updateNotificationLogByOption = async (options, session) => {
  const updatedNotificationLogs = []
  for (const option of options) {
    const { notificationLogId = '', updateData = {} } = option
    const { status = '' } = updateData
    if (status === 'sent') {
      updateData.sentAt = new Date()
    }
    const updateDataWithModifier = { $set: updateData }
    if (status) {
      updateDataWithModifier.$push = {
        events: {
          status,
          createdAt: new Date()
        }
      }
    }
    const updatedNotificationLog = await updateNotificationLog(
      { _id: notificationLogId, status: 'processing' },
      updateDataWithModifier,
      session
    )

    if (size(updatedNotificationLog)) {
      updatedNotificationLogs.push(updatedNotificationLog)
    }
  }
  return updatedNotificationLogs
}

export const updateNotificationLogForLambdaService = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { updateType, optionsObject, optionsArray } = body
  let updatedNotificationLogs = []
  if (updateType === 'statusUpdate' && size(optionsObject)) {
    updatedNotificationLogs = await updateNotificationLogsByActionType(
      optionsObject,
      session
    )
  } else if (updateType !== 'statusUpdate' && size(optionsArray)) {
    updatedNotificationLogs = await updateNotificationLogByOption(
      optionsArray,
      session
    )
    await handleMultipleAfterUpdateProcesses(updatedNotificationLogs, session)
  }
  return updatedNotificationLogs
}

const isNotificationLogAlreadyCreated = async (params) => {
  const { notificationLogsData, queueId, totalNotificationLogs = 0 } = params
  const appQueue = await appQueueHelper.getQueueItemById(queueId)
  if (!size(appQueue)) return true // Do nothing if appQueue doesn't exists
  const { notificationLogInfos = [] } = appQueue
  const notificationLogsCreated = size(notificationLogInfos) || 0

  const isCreated =
    totalNotificationLogs -
      notificationLogsCreated -
      size(notificationLogsData) !==
    0

  return isCreated
}

export const prepareDataAndCreateNotificationLogsForLambda = async (
  params,
  session
) => {
  const {
    collectionId,
    collectionNameStr,
    event,
    fromName,
    notificationLogsData,
    partnerId
  } = params

  const isCreated = await isNotificationLogAlreadyCreated(params)

  if (isCreated) {
    console.log('Notification log already created')
    return { ids: null }
  } // Do nothing if NotificationLog already created

  const { collectionName = '', fieldName = '' } =
    appHelper.getCollectionNameAndFieldNameByString(collectionNameStr)

  if (!collectionName) {
    console.log('Invalid collectionNameStr found')
    return { ids: null }
  }

  let notificationLogOptions = { partnerId }
  if (fieldName) notificationLogOptions[fieldName] = collectionId

  const insertedIds = []
  const collectionData = await collectionName
    .findOne({ _id: collectionId })
    .session(session)

  if (!size(collectionData)) {
    console.log(
      `collectionData doesn't exists for ${collectionNameStr}, collectionId: ${collectionId}`
    )
    return { ids: null }
  }

  if (collectionData.accountId)
    notificationLogOptions.accountId = collectionData.accountId
  if (collectionData.agentId)
    notificationLogOptions.agentId = collectionData.agentId
  if (collectionData.branchId)
    notificationLogOptions.branchId = collectionData.branchId
  if (collectionData.contractId)
    notificationLogOptions.contractId = collectionData.contractId
  if (collectionData.propertyId)
    notificationLogOptions.propertyId = collectionData.propertyId

  notificationLogOptions = JSON.parse(JSON.stringify(notificationLogOptions))

  for (const notificationLogData of notificationLogsData) {
    const notificationLogDataWithOptions = {
      ...notificationLogOptions,
      ...notificationLogData
    }
    if (
      size(notificationLogDataWithOptions) &&
      !notificationLogDataWithOptions.tenantId &&
      collectionData.tenantId
    ) {
      notificationLogDataWithOptions.tenantId = collectionData.tenantId
    } else if (
      size(notificationLogDataWithOptions) &&
      !notificationLogDataWithOptions.tenantId &&
      collectionNameStr === 'contracts' &&
      collectionData.rentalMeta &&
      collectionData.rentalMeta.tenantId
    ) {
      notificationLogDataWithOptions.tenantId =
        collectionData.rentalMeta.tenantId
    }

    // Adding already created attachments meta into notification log
    if (notificationLogDataWithOptions?.type === 'email') {
      const attachmentsMetasParams = {
        collectionData,
        collectionNameStr,
        event,
        notificationLogData: notificationLogDataWithOptions
      }

      const attachmentsMetas = await notificationLogHelper.getAttachmentsMetas(
        attachmentsMetasParams,
        session
      )

      if (event === 'send_invoice') {
        const {
          isExceedAttachedFileSize,
          attachmentVars: correctionAttachmentsMetas = []
        } =
          (await notificationLogHelper.getCorrectionInvoiceAttachmentInfo(
            collectionData,
            notificationLogData?.sendToUserId
          )) || {}

        if (!isExceedAttachedFileSize && size(correctionAttachmentsMetas))
          attachmentsMetas.push(...correctionAttachmentsMetas)

        if (collectionData && collectionData.isCorrectionInvoice)
          notificationLogData.isCorrectionInvoice = true
      }

      if (size(attachmentsMetas)) {
        notificationLogDataWithOptions.attachmentsMeta = attachmentsMetas
        notificationLogDataWithOptions.totalAttachment = size(attachmentsMetas)
      }
    }

    if (notificationLogDataWithOptions.status) {
      notificationLogDataWithOptions.events = [
        {
          status: notificationLogDataWithOptions.status,
          createdAt: new Date()
        }
      ]
    }

    notificationLogDataWithOptions.fromName = fromName

    const createdNotificationLog = await createANotificationLog(
      notificationLogDataWithOptions,
      session
    )
    const { status = '', type = '' } = createdNotificationLog[0]
    if (status === 'ready') {
      if (type === 'email' || type === 'sms')
        await createLogForSentEmailAndSms(createdNotificationLog[0], session)

      if (type === 'email' && size(createdNotificationLog[0].attachmentsMeta)) {
        await invoiceService.addFileIdsInInvoice(
          createdNotificationLog[0],
          session
        )
      }
    }
    insertedIds.push(createdNotificationLog[0]._id)
  }

  return { ids: insertedIds }
}

export const createNotificationLogsForLambdaService = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.compactObject(body)
  appHelper.checkRequiredFields(
    [
      'collectionId',
      'collectionNameStr',
      'event',
      'notificationLogsData',
      'partnerId',
      'queueId',
      'fromName'
    ],
    body
  )
  const createdNotificationLogIds =
    await prepareDataAndCreateNotificationLogsForLambda(body, session)
  return createdNotificationLogIds
}

export const addInvoiceSentInfoInInvoice = async (notificationLog, session) => {
  const { event = '', invoiceId = '', partnerId = '' } = notificationLog

  if (!(invoiceId && partnerId)) return false

  const events = [
    'send_invoice',
    'send_credit_note',
    'send_landlord_invoice',
    'send_landlord_credit_note',
    'send_final_settlement',
    'send_deposit_insurance_payment_reminder'
  ]
  const query = { _id: invoiceId, partnerId }
  const invoiceInfo =
    invoiceId && partnerId ? await invoiceHelper.getInvoice(query, session) : {}

  if (
    invoiceId &&
    partnerId &&
    size(invoiceInfo) &&
    !invoiceInfo.invoiceSent &&
    indexOf(events, event) !== -1
  ) {
    const updateData = { invoiceSent: true, invoiceSentAt: new Date() }

    if (event === 'send_deposit_insurance_payment_reminder') {
      await appInvoiceService.updateAppInvoice(
        query,
        { $set: updateData },
        session
      )
    } else {
      if (
        invoiceInfo.isReadyCreditedContent &&
        invoiceInfo.status === 'credited' &&
        !invoiceInfo.isSentCredited
      ) {
        query.status = 'credited'
        updateData.isSentCredited = true
        updateData.isReadyCreditedContent = false
      }

      await invoiceService.updateInvoice(query, { $set: updateData }, session)
    }
  }
}

export const updateContractAfterLeaseWelcomeMailSent = async (
  notificationLog,
  session
) => {
  const { contractId, createdAt, partnerId, sentAt } = notificationLog

  const leaseWelcomeEmailSendDate = sentAt
    ? (await appHelper.getActualDate(partnerId, true, sentAt)).toDate()
    : (await appHelper.getActualDate(partnerId, true, createdAt)).toDate()

  const query = { _id: contractId }
  const updateData = {
    $set: { 'rentalMeta.leaseWelcomeEmailSentAt': leaseWelcomeEmailSendDate },
    $unset: { 'rentalMeta.leaseWelcomeEmailSentInProgress': 1 }
  }

  await contractService.updateContract(query, updateData, session)
}

export const initAfterUpdateProcess = async (
  updatedNotificationLog = {},
  previous = {},
  session
) => {
  const {
    _id: notificationLogId,
    event,
    isResend,
    partnerId,
    status,
    type
  } = updatedNotificationLog
  // For sms
  if (
    partnerId &&
    status === 'sent' &&
    type === 'sms' &&
    (previous.status === 'processing' || isResend)
  ) {
    console.log(
      '====> Adding partner usage for sms count for notificationLog:',
      { notificationLogId, oldStatus: previous.status, newStatus: status },
      '<===='
    )
    await addPartnerUsagesForSentSMS(updatedNotificationLog, session)
  }
  // For email
  if (
    type === 'email' &&
    ['failed', 'queued', 'sent'].includes(status) &&
    previous.status === 'processing'
  ) {
    console.log(
      '====> Adding notification sent at time in invoice for notificationLog:',
      { notificationLogId, oldStatus: previous.status, newStatus: status },
      '<===='
    )
    await addInvoiceSentInfoInInvoice(updatedNotificationLog, session)
    if (event === 'send_welcome_lease') {
      console.log(
        '====> Updating welcome lease notification sending status for notificationLog:',
        {
          notificationLogId,
          event,
          contractId: updatedNotificationLog.contractId
        },
        '<===='
      )
      await updateContractAfterLeaseWelcomeMailSent(
        updatedNotificationLog,
        session
      )
    }
  }
}

export const handleMultipleAfterUpdateProcesses = async (
  updatedNotificationLogs = [],
  session
) => {
  if (!size(updatedNotificationLogs)) return false
  updatedNotificationLogs = sortBy(updatedNotificationLogs, '_id')
  const prevNotificationLogs = await notificationLogHelper.getNotificationLogs(
    { _id: { $in: updatedNotificationLogs.map(({ _id }) => _id) } },
    null,
    { _id: 1 }
  ) // Without session it returns previous
  for (let i = 0; i < updatedNotificationLogs.length; i++) {
    await initAfterUpdateProcess(
      updatedNotificationLogs[i],
      prevNotificationLogs[i],
      session
    )
  }
}

export const updateStatusAndIncrementRetry = async (
  timeBeforeSomeMinutes,
  session
) => {
  const query = {
    status: 'processing',
    processStartedAt: { $lte: timeBeforeSomeMinutes },
    $or: [{ retryCount: { $lt: 10 } }, { retryCount: { $exists: false } }]
  }
  const data = {
    $push: { events: { status: 'ready', createdAt: new Date() } },
    $set: { status: 'ready' },
    $inc: { retryCount: 1 }
  }
  const response = await updateNotificationLogs(query, data, session)
  let updatedNotificationLogs = []
  if (response.nModified >= 1) {
    const getQuery = { status: 'ready', retryCount: { $lte: 10 } }
    updatedNotificationLogs = await notificationLogHelper.getNotificationLogs(
      getQuery,
      session,
      { _id: 1 }
    )
  }
  return updatedNotificationLogs
}

export const retryFailedNotificationLogs = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)

  const { time } = body
  const timeBeforeSomeMinutes = moment().subtract(time, 'minutes').toDate()

  return updateStatusAndIncrementRetry(timeBeforeSomeMinutes, session)
}

export const updateNotificationLogWithSNSResponse = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { events, type } = body
  const { SESMsgIds } = events
  if (!(size(SESMsgIds) && type)) {
    return false
  }
  const { query, updateData } =
    notificationLogHelper.prepareQueryAndDataForSNSResponse(
      events,
      SESMsgIds,
      type
    )
  await updateNotificationLogs(query, updateData, session)

  return notificationLogHelper.getNotificationLogs(query, session)
}

export const updateNotificationLogAttachments = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { params = [] } = body

  if (!size(params))
    throw new CustomError(400, "Didn't get required data to update")

  let numberOfUpdate = 0

  for (const param of params) {
    const { attachmentFileId = '', fileId = '', status = '' } = param
    const updateQuery = {
      attachmentsMeta: {
        $elemMatch: { id: attachmentFileId }
      }
    }
    const updateData = {
      $unset: { 'attachmentsMeta.$.content': 1 },
      $set: {
        'attachmentsMeta.$.status': 'moved_to_attachment',
        'attachmentsMeta.$.fileId': fileId
      }
    }

    if (status) {
      updateData.$set.status = status
      updateData.$push = {
        events: {
          status,
          createdAt: new Date()
        }
      }
    }

    const updatedNotificationLog = await updateNotificationLog(
      updateQuery,
      updateData,
      session
    )
    if (updatedNotificationLog?.status === 'ready') {
      await createLogForSentEmailAndSms(updatedNotificationLog, session)
      await invoiceService.addFileIdsInInvoice(updatedNotificationLog, session)
    }

    if (size(updatedNotificationLog)) numberOfUpdate = numberOfUpdate + 1
  }

  return { numberOfUpdate }
}

export const createNotificationLogForLambdaService = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(
    ['content', 'event', 'fromName', 'status', 'subject', 'toEmail', 'type'],
    body
  )
  const createdNotificationLog = await createANotificationLog(body, session)
  return createdNotificationLog
}

export const createNotificationLogAndUpdateQueue = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(
    [
      'content',
      'event',
      'fromName',
      'queueId',
      'status',
      'subject',
      'toEmail',
      'type'
    ],
    body
  )
  const { queueId } = body

  const notificationLogUpdatingData = omit(body, ['queueId'])

  const [notificationLog] = await createANotificationLog(
    notificationLogUpdatingData,
    session
  )

  const { _id: logId, type, sendTo, toUserId = '' } = notificationLog

  const queueUpdatingData = {
    status: 'completed',
    errorDetails: null,
    totalNotificationLogs: 1,
    notificationLogIds: [logId],
    notificationLogInfos: [
      {
        logId,
        type,
        sendTo
      }
    ]
  }
  if (toUserId)
    queueUpdatingData.notificationLogInfos[0].sendToUserId = toUserId

  await appQueueService.updateAnAppQueue(
    { _id: queueId },
    queueUpdatingData,
    session
  )

  return { result: true }
}

export const sendMailToAll = async (req) => {
  const { body, session, user = {} } = req
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  if (!size(body)) throw new CustomError(400, 'Input data can not be empty')
  const { context, sendTo, notificationType } = body
  body.partnerId = partnerId
  const queueParams =
    notificationLogHelper.prepareQueueParamsForSendMailToAll(body)
  let tenantIds = []
  let accountIds = []
  const isBrokerPartner = await partnerHelper.getAPartner({
    _id: partnerId,
    accountType: 'broker'
  })
  console.log('=== isBrokerPartner', !!isBrokerPartner)
  console.log('=== notificationType', notificationType)
  console.log('=== context', context)

  if (context === 'tenant') {
    const tenantQuery =
      await notificationLogHelper.prepareTenantQueryForEmailSendToAll(body)
    console.log('===> tenantQuery', { ...tenantQuery })
    if (notificationType === 'email')
      tenantIds = await tenantHelper.getTenantIdsByQuery(tenantQuery)
    else {
      const tenants = await tenantHelper.getTenantsByAggregate(tenantQuery)
      console.log('tenants after aggregate ==> ', tenants)
      const [tenant = {}] = tenants || []
      const { tenantIdsWithPhoneNumbers = [] } = tenant
      console.log(
        'tenantIdsWithPhoneNumbers after aggregate ==> ',
        tenantIdsWithPhoneNumbers
      )
      tenantIds = tenantIdsWithPhoneNumbers
    }
  } else if (context === 'account') {
    const accountQuery =
      await notificationLogHelper.prepareAccountQueryForEmailSendToAll(
        body,
        size(isBrokerPartner)
      )
    console.log('===> accountQuery', { ...accountQuery })
    if (notificationType === 'email')
      accountIds = await accountHelper.getAccountIdsByQuery(accountQuery)
    else {
      const accounts = await accountHelper.getAccountsByAggregate(accountQuery) // Returns accountIds which have phone number
      console.log('accounts after aggregate ==> ', accounts)
      const [account = {}] = accounts || []
      const { accountIdsWithPhoneNumbers = [] } = account
      console.log(
        'accountIdsWithPhoneNumbers after aggregate ==> ',
        accountIdsWithPhoneNumbers
      )
      accountIds = accountIdsWithPhoneNumbers
    }
  } else if (context === 'property') {
    const { preparedQuery: queryData } =
      await propertyHelper.preparePropertiesQueryFromFilterData(body)

    const property = await propertyHelper.getPropertyAndAccountIds(queryData)
    console.log('property ', property)
    const tenantQuery = {
      partnerId,
      $and: [
        {
          properties: {
            $elemMatch: {
              status: 'active',
              propertyId: { $in: property[0]?.propertyIds }
            }
          }
        }
      ]
    }
    console.log('=== sendTo', sendTo)
    if (sendTo === 'tenants') {
      if (notificationType === 'email') {
        const { tenantIdsWithActiveLease } =
          await tenantHelper.getTenantIdsBasedOnLeaseStatus(tenantQuery)
        console.log('=== tenantIdsWithActiveLease', tenantIdsWithActiveLease)
        tenantIds = tenantIdsWithActiveLease
      } else {
        const tenants =
          await tenantHelper.getTenantIdsBasedOnActiveLeaseAndPhoneNumbers(
            tenantQuery
          )
        console.log('tenants after aggregate ==> ', tenants)
        const [tenant = {}] = tenants || []
        const { tenantIdsWithPhoneNumbers = [] } = tenant
        console.log(
          'tenantIdsWithPhoneNumbers after aggregate ==> ',
          tenantIdsWithPhoneNumbers
        )
        tenantIds = tenantIdsWithPhoneNumbers
      }
    } else if (size(isBrokerPartner) && sendTo === 'accounts') {
      if (notificationType === 'email') accountIds = property[0]?.accountIds
      else {
        const accountQuery = { _id: { $in: property[0]?.accountIds } }
        const accounts = await accountHelper.getAccountsByAggregate(
          accountQuery
        ) // Returns accountIds which have phone number
        console.log('accounts after aggregate ==> ', accounts)
        const [account = {}] = accounts || []
        const { accountIdsWithPhoneNumbers = [] } = account
        console.log(
          'accountIdsWithPhoneNumbers after aggregate ==> ',
          accountIdsWithPhoneNumbers
        )
        accountIds = accountIdsWithPhoneNumbers
      }
    } else if (sendTo === 'all') {
      if (size(isBrokerPartner)) {
        if (notificationType === 'email') accountIds = property[0]?.accountIds
        else {
          const accountQuery = { _id: { $in: property[0]?.accountIds } }
          const accounts = await accountHelper.getAccountsByAggregate(
            accountQuery
          ) // Returns accountIds which have phone number
          console.log('accounts after aggregate ==> ', accounts)
          const [account = {}] = accounts || []
          const { accountIdsWithPhoneNumbers = [] } = account
          console.log(
            'accountIdsWithPhoneNumbers after aggregate ==> ',
            accountIdsWithPhoneNumbers
          )
          accountIds = accountIdsWithPhoneNumbers
        }
      }
      if (notificationType === 'email') {
        const { tenantIdsWithActiveLease } =
          await tenantHelper.getTenantIdsBasedOnLeaseStatus(tenantQuery)
        tenantIds = tenantIdsWithActiveLease
      } else {
        const tenants =
          await tenantHelper.getTenantIdsBasedOnActiveLeaseAndPhoneNumbers(
            tenantQuery
          )
        console.log('tenants after aggregate ==> ', JSON.stringify(tenants))
        const [tenant = {}] = tenants || []
        const { tenantIdsWithPhoneNumbers = [] } = tenant
        console.log(
          'tenantIdsWithPhoneNumbers after aggregate ==> ',
          tenantIdsWithPhoneNumbers
        )
        tenantIds = tenantIdsWithPhoneNumbers
      }
    }
    console.log('=== Final accountIds', accountIds)
    console.log('=== Final tenantIds', tenantIds)
  }
  let createQueue
  if (size(accountIds) || size(tenantIds)) {
    queueParams.accountIds = size(accountIds) ? accountIds : []
    queueParams.tenantIds = size(tenantIds) ? tenantIds : []
    const prepareInputData = {
      params: queueParams,
      destination: 'notifier',
      priority: 'regular',
      action: 'send_notification',
      event: 'send_email_to_all'
    }
    if (notificationType === 'sms') prepareInputData.event = 'send_sms_to_all'
    createQueue = await appQueueService.createAnAppQueue(
      prepareInputData,
      session
    )
  } else {
    throw new CustomError(404, 'Ids not found')
  }

  if (size(createQueue)) {
    return {
      message: 'Email send to all successfully',
      status: 200
    }
  }
}

export const createNotificationLogsAndUpdateAppQueue = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['notificationLogsData', 'queueId'], body)

  const { notificationLogsData, queueId } = body
  appHelper.validateId({ queueId })

  if (!size(notificationLogsData))
    throw new CustomError(400, 'Invalid notificationLogsData')

  const createdNotificationLogsData = await createNotificationLogs(
    notificationLogsData,
    session
  )

  if (size(createdNotificationLogsData)) {
    const notificationLogInfos = []
    for (const notificationLog of createdNotificationLogsData) {
      // Adding activity logs
      await createLogForSentEmailAndSms(notificationLog, session)
      // Adding notification log data for pushing into app queue
      notificationLogInfos.push({
        logId: notificationLog._id,
        sendTo: notificationLog.sendTo,
        sendToUserId: notificationLog.toUserId,
        type: notificationLog.type
      })
    }

    const updatedAppQueueInfo = size(notificationLogInfos)
      ? await appQueueService.updateAnAppQueue(
          { _id: queueId },
          {
            $addToSet: { notificationLogInfos: { $each: notificationLogInfos } }
          },
          session
        )
      : {}

    return size(updatedAppQueueInfo) ? createdNotificationLogsData : []
  } else throw new CustomError(400, 'Could not create notification logs!')
}

export const sendNotificationLog = async (notificationLogData, session) => {
  const partnerId = notificationLogData.partnerId
  const partnerSettingsQuery = {
    partnerId: partnerId ? partnerId : { $exists: false }
  }
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting(
    partnerSettingsQuery
  )
  const fromPhoneNumber = partnerSettings?.smsSettings?.smsSenderName || null
  const updateData = { status: 'ready' }

  if (notificationLogData && notificationLogData.type === 'sms') {
    if (fromPhoneNumber) updateData.fromPhoneNumber = fromPhoneNumber
  } else if (
    notificationLogData &&
    notificationLogData.type === 'email' &&
    notificationLogData.toEmail
  ) {
    const attachmentsMeta = []

    if (
      notificationLogData.isResend &&
      size(notificationLogData.attachmentsMeta)
    ) {
      for (const attachmentsMetaInfo of notificationLogData.attachmentsMeta) {
        if (attachmentsMetaInfo.fileId) {
          const file = await fileHelper.getAFile({
            _id: attachmentsMetaInfo.fileId
          })
          const key = fileHelper.getFileKey(file)
          attachmentsMetaInfo.fileKey = key
          attachmentsMeta.push(attachmentsMetaInfo)
        }
      }
    }

    if (size(attachmentsMeta)) updateData.attachmentsMeta = attachmentsMeta
  }
  return await updateNotificationLog(
    {
      _id: notificationLogData._id
    },
    {
      $set: updateData,
      $push: {
        events: {
          status: 'ready',
          createdAt: new Date()
        }
      }
    },
    session
  )
}

export const resendEmailOrSms = async (req) => {
  const { body = {}, session, user = {} } = req
  const { roles = [] } = user
  if (!roles?.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }
  const {
    invoiceId,
    notificationLogId,
    partnerId,
    toEmail,
    toPhoneNumber,
    userId
  } = body

  if (!invoiceId && !notificationLogId)
    throw new CustomError(400, 'Please provide notificationLogId or invoiceId')
  const query = {}
  if (notificationLogId) query._id = notificationLogId
  if (invoiceId) query.invoiceId = invoiceId
  if (partnerId) query.partnerId = partnerId
  const notificationLog = await notificationLogHelper.getNotificationLog(query)
  if (!size(notificationLog))
    throw new CustomError(404, 'Notification log not found')
  const insertData =
    await notificationLogHelper.prepareNewNotificationLogDataToResendEmailOrSms(
      notificationLog,
      {
        toEmail,
        toPhoneNumber
      }
    )
  insertData.createdBy = userId
  const [insertedNewLogData] = await createANotificationLog(insertData, session)
  await createLogForSentEmailAndSms(insertedNewLogData, session)
  return {
    result: true
  }
}
