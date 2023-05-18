import { find, isEmpty, map, size } from 'lodash'
import moment from 'moment-timezone'
import nid from 'nid'

import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  appInvoiceHelper,
  appQueueHelper,
  contractHelper,
  depositInsuranceHelper,
  integrationHelper,
  notificationLogHelper,
  partnerHelper,
  partnerSettingHelper,
  transactionHelper
} from '../helpers'
import { AppQueueCollection } from '../models'
import { depositInsuranceService } from '../services'

export const insertInQueue = async (data, session) => {
  const [queueItem] = await AppQueueCollection.create([data], { session })
  return queueItem
}

export const removeAppQueueItems = async (query, session) => {
  const response = await AppQueueCollection.deleteMany(query).session(session)
  return response
}

export const insertAppQueueItems = async (data = [], session) => {
  data.forEach((element) => {
    element._id = nid(17)
  })

  const appQueues = await AppQueueCollection.insertMany(data, {
    session,
    runValidators: true
  })

  return appQueues
}

export const createAnAppQueue = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for app-queue creation')
  }

  data.createdAt = new Date()

  const createdAppQueue = await AppQueueCollection.create([data], { session })
  if (isEmpty(createdAppQueue)) {
    throw new CustomError(404, 'Unable to create app-queue')
  }
  return createdAppQueue
}

export const createMultipleAppQueues = async (data, session) => {
  if (!size(data))
    throw new CustomError(404, 'No data found for app-queues creation')

  const createdAppQueues = await AppQueueCollection.create(data, { session })

  if (isEmpty(createdAppQueues))
    throw new CustomError(400, 'Unable to create app-queues')

  return createdAppQueues
}

export const updateQueues = async (query, updateData) => {
  const updatedQueue = await AppQueueCollection.updateMany(query, updateData)
  if (updatedQueue.nModified === 0) {
    throw new CustomError(500, 'Queue cannot update')
  }
  return true
}

export const queueOnFlight = async (queueIds, updateData, session) => {
  const response = await AppQueueCollection.updateMany(
    {
      _id: {
        $in: queueIds
      }
    },
    {
      $set: updateData
    },
    { session }
  )

  return response.nModified
}

export const updateAnAppQueue = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for app app-queue update')
  }
  const updatedAppQueue = await AppQueueCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!size(updatedAppQueue)) {
    throw new CustomError(404, `Unable to update app queue`)
  }
  return updatedAppQueue
}

export const updateAnAppQueueWithSort = async (params, session) => {
  const { data, query, sort } = params || {}
  if (!(size(data) && size(query) && size(sort))) {
    throw new CustomError(404, 'Missing required data to update an app queue')
  }

  return AppQueueCollection.findOneAndUpdate(query, data, {
    new: true,
    runValidators: true,
    session,
    sort
  })
}

export const updateAppQueues = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for app app-queues update')
  }
  const updatedAppQueues = await AppQueueCollection.updateMany(query, data, {
    session,
    runValidators: true
  })

  if (updatedAppQueues.nModified < 1) {
    throw new CustomError(404, `Unable to update app queues`)
  }

  data.status ? (query.status = data.status) : delete query.status

  return await appQueueHelper.getAppQueues(query, session)
}

export const updateAppQueueItems = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for app app-queues update')
  }
  const updatedAppQueues = await AppQueueCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  return updatedAppQueues
}

const validateDataForAppQueueUpdate = (body) => {
  const { queryData = {}, updatingData = {} } = body
  const { queueId = '' } = queryData
  if (queueId) {
    appHelper.validateId({ queueId })
  }
  if (!size(updatingData))
    throw new CustomError(400, 'Updating data is required')
}

const validateDataForAppQueuesUpdate = (body) => {
  const { queryData = {}, updatingData = {} } = body
  const { queueIds = [] } = queryData

  if (!size(queueIds)) throw new CustomError(400, "queueIds won't be empty")
  if (!size(updatingData))
    throw new CustomError(400, 'Updating data is required')

  for (const queueId of queueIds) {
    appHelper.validateId({ queueId })
  }
}

export const appQueueUpdate = async (req) => {
  const { body, session } = req
  validateDataForAppQueueUpdate(body)
  const { queryData = {}, updatingData = {} } = body
  const {
    queueId = '',
    status = '',
    partnerId = '',
    event = '',
    ignoreQueueIds = []
  } = queryData
  const query = {}
  const isQCompleted = await AppQueueCollection.findOne({
    _id: queueId,
    status: 'completed'
  })
  if (isQCompleted) {
    console.log(
      'This app queue is already completed and will not be updated further',
      queueId,
      isQCompleted
    )
    return isQCompleted
  }
  if (queueId) query._id = queueId
  else if (size(ignoreQueueIds)) query._id = { $nin: ignoreQueueIds }

  if (partnerId) query['params.partnerId'] = partnerId
  if (event) query.event = event
  if (status) query.status = status

  const updateData = appQueueHelper.prepareAppQueueUpdateData(updatingData)
  const updatedAppQueue = await updateAnAppQueue(query, updateData, session)

  const { sequentialCategory, status: updatedStatus } = updatedAppQueue || {}
  console.log(
    '====> Checking sequential queue for queuing next queue, sequentialCategory:',
    sequentialCategory,
    ', updatedStatus:',
    updatedStatus,
    '<===='
  )
  if (sequentialCategory && updatedStatus === 'completed') {
    console.log(
      '====> Checking sequential queue for queuing next queue, queue:',
      await appQueueHelper.getAppQueuesWithOptions(
        { sequentialCategory, status: 'hold' },
        { limit: 1, sort: { createdAt: 1 } },
        session
      ),
      '<===='
    )
    console.log(
      '====> Checking queue updating response for sequential queue:',
      await updateAnAppQueueWithSort(
        {
          data: { $set: { status: 'new' } },
          query: { sequentialCategory, status: 'hold' },
          sort: { createdAt: 1 }
        },
        session
      ),
      '<===='
    )
  }

  return updatedAppQueue
}

export const appQueuesUpdate = async (req) => {
  const { body, session } = req
  validateDataForAppQueuesUpdate(body)
  const { queryData = {}, updatingData = {} } = body
  const { queueIds = [], status = '' } = queryData

  const query = { _id: { $in: queueIds } }
  if (status) query.status = status

  const updateData = appQueueHelper.prepareAppQueueUpdateData(updatingData)

  // There is no need to use await in returning method
  return updateAppQueues(query, updateData, session)
}

export const appQueuesUpdateToNew = async (req) => {
  const { body, session } = req
  const { queryData = {} } = body
  const { status = '' } = queryData

  if (!status) throw new CustomError(400, "status won't be empty")

  const fiveMinutesAgo = moment(new Date()).subtract(5, 'minutes').toDate()

  const query = {
    isSequential: { $ne: true },
    sequentialCategory: { $exists: false },
    status,
    noOfRetry: {
      $lte: 4
    },
    flightAt: {
      $lte: fiveMinutesAgo
    }
  }

  const updatedAppQueue = await AppQueueCollection.updateMany(
    query,
    [
      {
        $set: {
          noOfRetry: {
            $add: ['$noOfRetry', 1]
          },
          priority: 'regular',
          status: 'new',
          history: {
            $concatArrays: [
              {
                $ifNull: ['$history', []]
              },
              [
                {
                  status: '$status',
                  flightAt: '$flightAt',
                  noOfRetry: '$noOfRetry',
                  errorDetails: '$errorDetails'
                }
              ]
            ]
          }
        }
      }
    ],
    {
      session
    }
  )
  console.log(updatedAppQueue.nModified)

  return { numberOfUpdate: updatedAppQueue?.nModified }
}

export const createAppQueue = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appQueueHelper.validateDataForAddingAppQueue(body)
  const [queue] = await createAnAppQueue(body, session)
  return queue
}

export const createAppQueues = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appQueueHelper.validateDataForAddingAppQueues(body)
  const { data } = body
  const createdAppQueues = await insertAppQueueItems(data, session)
  return createdAppQueues
}

export const cleanUpQueueService = async (req) => {
  const { session } = req
  const query = {
    $and: [
      {
        status: { $in: ['on_flight', 'sent', 'processing'] },
        isSequential: { $ne: true },
        sequentialCategory: { $exists: false }
      },
      {
        flightAt: { $lte: moment(new Date()).subtract(20, 'minutes').toDate() }
      },
      { noOfRetry: { $lte: 4 } } // 0-4
    ]
  }
  await AppQueueCollection.updateMany(
    query,
    [
      {
        $set: {
          noOfRetry: {
            $add: ['$noOfRetry', 1]
          },
          status: 'new',
          history: {
            $concatArrays: [
              {
                $ifNull: ['$history', []]
              },
              [
                {
                  status: '$status',
                  flightAt: '$flightAt',
                  noOfRetry: '$noOfRetry',
                  errorDetails: '$errorDetails'
                }
              ]
            ]
          }
        }
      }
    ],
    {
      session
    }
  )
  return { msg: 'Queue Cleaned Up', code: 201 }
}

export const createQueueItemsForXledgerIntegratedPartners = async (req) => {
  const { session, body, user } = req
  appHelper.checkUserId(user.userId)
  const query = {
    type: 'xledger',
    status: 'integrated'
  }
  const integratedData =
    await integrationHelper.getIntegratedPartnersToStartSyncProcess(query, body)
  const queueData = []
  for (const data of integratedData) {
    queueData.push({
      action: 'start_integration',
      event: 'start_integration',
      priority: 'immediate',
      destination: 'xledger',
      params: {
        partnerId: data.partnerId,
        partnerType: data?.partner?.accountType
      }
    })
  }
  let numberOfCreatedQueue = 0
  if (size(queueData)) {
    const response = await insertAppQueueItems(queueData, session)
    numberOfCreatedQueue = queueData.length
    if (!size(response)) {
      throw new CustomError(
        400,
        'Unable to create app queues to start integration'
      )
    }
  }
  return { numberOfCreatedQueue }
}
export const createQueueToStartIntegrationOrExternalIdPartnerCheckService =
  async (req) => {
    const { session } = req
    const { body } = req
    const { dataType, filter } = body
    let query = {}
    let action = ''
    let event = ''
    let partnerQuery = {}
    if (size(dataType) && dataType === 'pogo_integrated_partners') {
      partnerQuery = {
        dataType: 'pogo_integrated_partners'
      }
      event = 'integration'
      action = 'start_integration'
    } else if (size(filter) && filter === 'external_id_invalid') {
      partnerQuery = {
        filter: 'external_id_invalid'
      }
      event = 'external_id_check'
      action = 'external_id_partner_check'
    }
    query = await partnerHelper.preparePartnersQueryBasedOnFilters(
      partnerQuery,
      session
    )
    const partners = size(query) ? await partnerHelper.getPartners(query) : []
    const queueData = []
    for (const partner of partners) {
      queueData.push({
        action,
        event,
        priority: 'immediate',
        destination: 'accounting-pogo',
        params: {
          partnerId: partner._id,
          partnerType: partner.accountType
        }
      })
    }
    const queues = await insertAppQueueItems(queueData, session)
    return queues
  }

export const createQueueItemsForExternalIdTransactionCheckService = async (
  req
) => {
  const { body, session } = req
  const { partnerId } = body
  const query = await accountHelper.prepareAccountsQuery(
    {
      dataType: 'integrated_accounts',
      partnerId
    },
    session
  )
  delete query.dataType
  const integratedAccounts = await accountHelper.getAccounts(query, session)
  const queueData = []
  for (const account of integratedAccounts) {
    queueData.push({
      action: 'external_id_transaction_check',
      event: 'transaction',
      destination: 'accounting-pogo',
      priority: 'immediate',
      params: {
        partnerId,
        partnerType: 'direct',
        directPartnerAccountId: account._id
      }
    })
  }
  const queues = await insertAppQueueItems(queueData, session)
  return queues
}

const validateDataOfAppQueuesUpdateForLambda = (body) => {
  const { params = [] } = body
  if (!size(params)) throw new CustomError(400, 'Missing required data')

  for (const param of params) {
    const { notificationLogIds = [], queueId = '', status = '' } = param
    if (!queueId) throw new CustomError(400, 'Missing queueId')
    if (!status) throw new CustomError(400, 'Missing status')

    appHelper.validateId({ queueId })

    if (size(notificationLogIds))
      notificationLogIds.forEach((notificationLogId) => {
        appHelper.validateId({ notificationLogId })
      })
  }
}

export const updateAppQueuesDataForNotifierLambda = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)

  validateDataOfAppQueuesUpdateForLambda(body)

  const { params = [] } = body

  let numberOfUpdated = 0

  for (const param of params) {
    const {
      errorDetails = {},
      notificationLogIds = [],
      queueId = '',
      status = '',
      totalNotificationLogs = 0
    } = param

    const notificationLogs = size(notificationLogIds)
      ? await notificationLogHelper.getNotificationLogs(
          { _id: { $in: notificationLogIds } },
          session
        )
      : []

    const notificationLogInfos = size(notificationLogs)
      ? map(notificationLogs, (log) => ({
          logId: log?._id,
          type: log?.type,
          sendTo: log?.sendTo,
          sendToUserId: log?.toUserId
        }))
      : []

    const updatingData = {
      $set: {
        status,
        totalNotificationLogs,
        errorDetails
      },
      $push: { notificationLogInfos: { $each: notificationLogInfos } }
    }

    if (status === 'completed') {
      updatingData.$set['completedAt'] = new Date()
      updatingData.$set['errorDetails'] = null
    }

    await updateAnAppQueue({ _id: queueId }, updatingData, session)

    numberOfUpdated = numberOfUpdated + 1
  }

  return { numberOfUpdated }
}

export const updateAppQueueToCompleted = async (queueId, session) => {
  const response = await updateAnAppQueue(
    { _id: queueId, status: 'processing' },
    { $set: { status: 'completed', completedAt: new Date() } },
    session
  )

  if (size(response))
    console.log(`## Updated appQueue to completed. queueId: ${response?._id}`)

  const { sequentialCategory, status } = response

  if (sequentialCategory && status === 'completed') {
    const updatedQ = await updateAnAppQueueWithSort(
      {
        data: { $set: { status: 'new' } },
        query: { sequentialCategory, status: 'hold' },
        sort: { createdAt: 1 }
      },
      session
    )
    size(updatedQ)
      ? console.log(
          `=== Updated sequential queue status to new. queueId: ${updatedQ._id} ===`
        )
      : console.log(`=== No sequential queue found with status 'hold' ===`)
  }

  return !!response
}

export const createAppQueueForPaymentTransaction = async (params, session) => {
  const { action, amount, isDifferentAmount, payment } = params

  if (!size(payment))
    throw new CustomError(
      404,
      'PaymentData is required to create payment transaction'
    )

  const { _id: paymentId, partnerId } = payment
  if (await transactionHelper.isTransactionEnabledForPartner(partnerId)) {
    const appQueueData = {
      action: 'add_payment_regular_transaction',
      destination: 'accounting',
      event: 'add_new_transaction',
      params: {
        paymentIds: [paymentId],
        partnerId,
        transactionEvent: 'regular',
        paymentAmount: amount || undefined,
        removalPaymentData: action === 'remove' ? payment : undefined,
        previousPaymentData: action === 'update' ? payment : undefined,
        isDifferentAmount: isDifferentAmount || undefined
      },
      priority: 'regular'
    }

    const [appQueue] = await createAnAppQueue(appQueueData, session)
    console.log('-- Transaction added for payment')
    console.log(
      `// Created appQueue for payment transaction. queueId: ${appQueue._id}, paymentId: ${paymentId} ===`
    )
  } else
    console.log(
      `// Transaction is not enabled for this partner. partnerId: ${partnerId} ===`
    )
}

export const createAppQueueForMatchPayment = async (params, session) => {
  const { action, contractId, partnerId } = params

  if (!(contractId || partnerId))
    throw new CustomError(
      404,
      'Missing required data to create appQueue for Match payment'
    )

  const event = 'match_payments_with_invoices'

  console.log('=== Trying to create appQueue for match payment ===')
  const appQueueData = {
    action,
    destination: 'payments',
    event,
    isSequential: true,
    params: { contractId, partnerId },
    priority: 'immediate',
    sequentialCategory: `match_payments_with_invoices_${contractId}`
  }

  const [appQueue] = await addSequentialAppQueues([appQueueData], session)
  console.log(
    `=== Created appQueue for match payment. queueId: ${appQueue._id}, partnerId: ${partnerId} ===`
  )
}

export const createAppQueueToSendDepositInsuranceCreatingRequest = async (
  contractId,
  session
) => {
  if (contractId) {
    const contract = await contractHelper.getAContract({ _id: contractId })
    if (!size(contract)) {
      console.log(
        `Contract doesn't exists to send DI creating request. for contractId: ${contractId}`
      )
      throw new CustomError(
        404,
        `Contract doesn't exists to send DI creating request. for contractId: ${contractId}`
      )
    }
    const { accountId, partnerId, rentalMeta = {} } = contract
    const { tenantId } = rentalMeta || {}

    const depositInsurance = await depositInsuranceHelper.getADepositInsurance({
      contractId
    })
    if (!size(depositInsurance)) {
      console.log(
        `DepositInsurance doesn't exists to send DI creating request. for contractId: ${contractId}`
      )
      return false
    }

    if (depositInsurance._id && accountId && tenantId) {
      const appQueueData = {
        action: 'handle_deposit_insurance',
        destination: 'deposit-insurance',
        event: 'create_deposit_insurance',
        params: {
          accountId,
          contractId,
          depositInsuranceId: depositInsurance._id,
          partnerId,
          tenantId
        },
        priority: 'immediate'
      }
      const [appQueue] = await createAnAppQueue(appQueueData, session)

      if (size(appQueue)) {
        console.log(
          `=== Successfully created appQueue for creating deposit insurance, appQueueId: ${appQueue._id}, contractId: ${contractId}, partnerId: ${partnerId} ===`
        )

        const depositInsurance =
          await depositInsuranceService.updateADepositInsurance(
            { contractId },
            { $set: { status: 'sent' }, $unset: { creationResult: 1 } },
            session
          )
        if (size(depositInsurance))
          console.log(
            `=== Updated depositInsurance status to sent and removed old creationResult, depositInsuranceId: ${depositInsurance._id} ===`
          )
      }
    } else {
      console.log(
        `=== Missing accountId or tenantId in the contract. for contractId: ${contractId} ===`
      )
    }
  } else {
    console.log(
      '=== Unable to send deposit insurance creating request missing contractId ==='
    )
  }
}

export const makeDIDueAndSendDINotification = async (req) => {
  const { body, session } = req
  const { appInvoiceId } = body

  if (!appInvoiceId) throw new CustomError(400, 'Missing appInvoiceId')

  const appInvoice = await appInvoiceHelper.getAppInvoice({ _id: appInvoiceId })
  if (!size(appInvoice)) throw new CustomError(404, 'AppInvoice not found')

  const { depositInsuranceId } = appInvoice
  if (!depositInsuranceId)
    throw new CustomError(404, 'DepositInsuranceId not found in appInvoice')

  const depositInsurance =
    await depositInsuranceService.updateADepositInsurance(
      { _id: depositInsuranceId },
      { $set: { status: 'due' } },
      session
    )
  if (size(depositInsurance)) {
    console.log(`=== Updated DI status to 'due' ===`)
    return await createAppQueueToSendDepositInsurancePaymentNotification(
      appInvoice,
      session
    )
  }
}

export const createAppQueueToSendDepositInsurancePaymentNotification = async (
  appInvoice,
  session
) => {
  if (!size(appInvoice)) {
    console.log(`Required data is missing to send DI payment notification`)
    throw new CustomError(
      404,
      'Required data is missing to send DI payment notification'
    )
  }

  const { depositInsuranceId, partnerId } = appInvoice

  const partnerSetting = partnerId
    ? await partnerSettingHelper.getAPartnerSetting({ partnerId })
    : {}
  if (!size(partnerSetting)) {
    console.log(
      `PartnerSetting doesn't exists. for partnerId: ${partnerId} to send DI payment notification`
    )
    throw new CustomError(
      404,
      `PartnerSetting doesn't exists. for partnerId: ${partnerId} to send DI payment notification`
    )
  }
  console.log(
    '=== DepositInsurance Setting',
    partnerSetting.depositInsuranceSetting
  )
  const isNotificationEnabled =
    partnerSetting?.depositInsuranceSetting?.paymentReminder?.enabled || false

  if (partnerId && isNotificationEnabled) {
    const appQueueData = {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_deposit_insurance_payment_reminder',
      params: {
        collectionId: appInvoice._id,
        collectionNameStr: 'app_invoices',
        partnerId
      },
      priority: 'regular'
    }
    const isCreated = await createAnAppQueue(appQueueData, session)

    if (size(isCreated)) {
      console.log(
        '--- Successfully notified tenant for deposit insurance payment depositInsuranceId:',
        depositInsuranceId,
        ', partnerId:',
        partnerId,
        ', queueId:',
        isCreated[0]._id
      )
    } else {
      console.log(
        "--- Couldn't notified tenant for deposit insurance payment depositInsuranceId:",
        depositInsuranceId,
        ', partnerId:',
        partnerId
      )
    }
    return isCreated
  } else
    console.log(
      `Notification is disable for this partner. depositInsuranceId: ${depositInsuranceId}, partnerId: ${partnerId}`
    )
}

export const createAppQueueForProcessingEvictionCase = async (
  invoice = {},
  session
) => {
  const { _id: invoiceId, contractId, partnerId } = invoice || {}
  const isAppQueueExists = !!size(
    await appQueueHelper.getAppQueues(
      {
        action: 'process_eviction_case_for_contract',
        'params.contractId': contractId,
        'params.partnerId': partnerId,
        status: { $nin: ['completed', 'failed'] }
      },
      session
    )
  )
  const partnerSettingsInfo = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const enabledCreateEvictionPackage = !!(
    partnerSettingsInfo?.evictionDueReminderNotice &&
    partnerSettingsInfo?.evictionDueReminderNotice?.isCreateEvictionPackage
  )

  if (
    enabledCreateEvictionPackage &&
    contractId &&
    partnerId &&
    !isAppQueueExists
  ) {
    const appQueueData = {
      action: 'process_eviction_case_for_contract',
      event: 'process_eviction_case_for_contract',
      destination: 'invoice',
      params: { contractId, partnerId },
      priority: 'regular'
    }
    const queue = await insertInQueue(appQueueData, session)
    console.log(
      `=== Created appQueue for processing eviction case. invoiceId: 
        ${invoiceId}, queueId: ${queue._id} ===`
    )
  }
}

export const createAppQueueForAddingSerialId = async (
  collectionNameStr = '',
  collectionData = {},
  session
) => {
  const { partnerId } = collectionData || {}
  const query = {
    action: 'add_serialIds',
    status: { $nin: ['completed', 'failed'] },
    'params.collectionNameStr': collectionNameStr,
    'params.partnerId': partnerId
  }
  let sequentialCategory = `add_${collectionNameStr}_serial_ids_${partnerId}`
  const appQueueParams = {
    collectionNameStr,
    partnerId
  }

  if (collectionNameStr === 'invoices') {
    const { accountId, isFinalSettlement, partnerId } = collectionData || {}
    const partnerInfo = (await partnerHelper.getPartnerById(partnerId)) || {}

    if (
      partnerInfo.accountType === 'direct' &&
      partnerInfo.enableInvoiceStartNumber
    ) {
      appQueueParams.accountId = accountId
      appQueueParams.isAccountWiseSerialId = !!accountId
      sequentialCategory = `add_${collectionNameStr}_serial_ids_${accountId}`
      query['params.accountId'] = accountId
      query['params.isAccountWiseSerialId'] = !!accountId
    } else if (isFinalSettlement) {
      appQueueParams.isFinalSettlementInvoice = true
      query['params.isFinalSettlementInvoice'] = isFinalSettlement
    }
  }
  const existingInvoice = await appQueueHelper.getAnAppQueue(query, session)
  if (size(existingInvoice)) {
    return
  }
  const appQueueData = {
    action: 'add_serialIds',
    destination: 'invoice',
    event: 'add_serialIds',
    isSequential: true,
    params: appQueueParams,
    priority: 'immediate',
    sequentialCategory,
    status: 'new'
  }
  await addSequentialAppQueues([appQueueData], session)
}

export const createAppQueueForAddingPayoutBankReference = async (
  partnerId,
  session
) => {
  const query = {
    action: 'add_bank_reference',
    status: { $nin: ['completed', 'failed'] },
    'params.partnerId': partnerId
  }
  const sequentialCategory = `add_bank_reference_${partnerId}`
  const existingInvoice = await appQueueHelper.getAnAppQueue(query, session)
  if (size(existingInvoice)) {
    return
  }
  const appQueueData = {
    action: 'add_bank_reference',
    delaySeconds: Math.floor(Math.random() * 61) + 120,
    destination: 'invoice',
    event: 'add_bank_reference',
    isSequential: true,
    params: {
      partnerId
    },
    priority: 'immediate',
    sequentialCategory,
    status: 'new'
  }
  await addSequentialAppQueues([appQueueData], session)
}

export const createAnAppQueueToCreateLandlordCreditNote = async (
  params = {},
  session
) => {
  const { creditNoteId = '', contractId = '', hold, partnerId = '' } = params
  const appQueueData = {
    action: 'create_landlord_credit_note',
    destination: 'invoice',
    event: 'create_landlord_credit_note',
    isSequential: true,
    params: {
      contractId,
      creditNoteId,
      partnerId
    },
    priority: 'immediate',
    sequentialCategory: `create_and_update_contract_invoices_${contractId}`
  }
  if (hold) {
    appQueueData.status = 'hold'
  }
  await addSequentialAppQueues([appQueueData], session)
}

export const createAnAppQueueToCreateEstimatedPayout = async (
  params = {},
  session
) => {
  const {
    contractId = '',
    hold,
    invoiceId = '',
    isFinalSettlement,
    meta,
    partnerId
  } = params
  const appQueueData = {
    action: 'create_estimated_payout',
    destination: 'invoice',
    event: 'create_estimated_payout',
    isSequential: true,
    params: {
      contractId,
      invoiceId,
      isFinalSettlement,
      meta,
      partnerId
    },
    priority: 'immediate',
    sequentialCategory: `create_and_update_contract_invoices_${contractId}`
  }
  if (hold) appQueueData.status = 'hold'
  await addSequentialAppQueues([appQueueData], session)
}

export const createAnAppQueueToCreateOrAdjustEstimatedPayout = async (
  params = {},
  session
) => {
  const {
    contractId = '',
    hold,
    invoiceId = '',
    isFinalSettlement,
    partnerId
  } = params
  const appQueueData = {
    action: 'create_or_adjust_estimated_payout',
    destination: 'invoice',
    event: 'create_or_adjust_estimated_payout',
    isSequential: true,
    params: {
      contractId,
      invoiceId,
      isFinalSettlement,
      partnerId
    },
    priority: 'immediate',
    sequentialCategory: `create_and_update_contract_invoices_${contractId}`
  }
  if (hold) appQueueData.status = 'hold'
  await addSequentialAppQueues([appQueueData], session)
}

export const createAnAppQueueToAddInvoiceCommissions = async (
  params = {},
  session
) => {
  const {
    adjustmentNotNeeded = false,
    contractId = '',
    hold,
    invoiceId = '',
    partnerId = ''
  } = params
  const appQueueData = {
    action: 'add_invoice_commissions',
    destination: 'invoice',
    event: 'add_invoice_commissions',
    isSequential: true,
    params: {
      adjustmentNotNeeded,
      contractId,
      invoiceId,
      partnerId
    },
    priority: 'immediate',
    sequentialCategory: `create_and_update_contract_invoices_${contractId}`
  }
  if (hold) appQueueData.status = 'hold'
  await addSequentialAppQueues([appQueueData], session)
}

export const createAppQueueForAppInvoicePdf = async (appInvoice, session) => {
  const appQueueData = {
    action: 'add_app_queue_for_pdf_creation',
    destination: 'invoice',
    event: 'init_after_process_of_app_invoice_creation',
    params: {
      invoiceId: appInvoice._id,
      status: appInvoice.status
    },
    priority: 'immediate'
  }
  await insertInQueue(appQueueData, session)
  return true
}

export const createAnAppQueueToCheckCommissionChanges = async (
  params = {},
  session
) => {
  const { contractId, hold, partnerId } = params
  const appQueueData = {
    action: 'check_commission_changes_and_add_history',
    destination: 'invoice',
    event: 'check_commission_changes_and_add_history',
    isSequential: true,
    params: {
      contractId,
      partnerId
    },
    priority: 'immediate',
    sequentialCategory: `create_and_update_contract_invoices_${contractId}`
  }
  if (hold) appQueueData.status = 'hold'
  await addSequentialAppQueues([appQueueData], session)
}

export const createAppQueueForCreateRentInvoice = async (
  params = {},
  session,
  priority = 'immediate'
) => {
  console.log(
    '=== Creating app queue for making rent invoices. params:',
    params
  )
  const { contractId, enabledNotification, partnerId, today, userId } = params
  const appQueueData = {
    action: 'create_rent_invoice',
    destination: 'invoice',
    event: 'create_rent_invoice',
    params: {
      contractId,
      enabledNotification,
      partnerId,
      today,
      userId
    },
    priority
  }
  const queue = await insertInQueue(appQueueData, session)
  console.log(
    `=== Created app queue for making rent invoices. queueId: ${queue._id},contractId: ${contractId} ===`
  )
}

export const addSequentialAppQueues = async (queues, session) => {
  if (!size(queues)) {
    throw new CustomError(400, 'Missing required app queues data')
  }

  const sequentialCategories = []

  for (const queue of queues) {
    appQueueHelper.validateDataForAddingAppQueue(queue)

    if (queue.sequentialCategory) {
      let isSequentialCategoryExists = find(
        sequentialCategories,
        (category) => category === queue.sequentialCategory
      )

      if (!isSequentialCategoryExists) {
        const [existingAppQueue] = await appQueueHelper.getAppQueuesWithOptions(
          {
            sequentialCategory: queue.sequentialCategory,
            status: { $nin: ['completed', 'failed'] }
          },
          { limit: 1, sort: { createdAt: 1 } },
          session
        )
        if (existingAppQueue?._id) isSequentialCategoryExists = true
      }

      if (isSequentialCategoryExists) queue.status = 'hold'

      sequentialCategories.push(queue.sequentialCategory)
    }
  }

  return createMultipleAppQueues(queues, session)
}

export const addSequentialAppQueuesForRequest = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['data'], body)

  return addSequentialAppQueues(body.data, session)
}

export const createAppQueueForCreateCreditNote = async (
  params = {},
  session
) => {
  const {
    contractId,
    enabledNotification,
    hold,
    invoiceId,
    notUpdateDefaultedContract,
    partnerId,
    terminationDate,
    userId,
    requestFrom
  } = params
  const appQueueData = {
    action: 'create_credit_note',
    destination: 'invoice',
    event: 'create_credit_note',
    isSequential: true,
    params: {
      contractId,
      enabledNotification,
      invoiceId,
      notUpdateDefaultedContract,
      partnerId,
      terminationDate,
      userId,
      requestFrom
    },
    sequentialCategory: 'credit_note_creation_process_' + contractId,
    priority: 'immediate'
  }
  if (hold) appQueueData.status = 'hold'
  await addSequentialAppQueues([appQueueData], session)
  console.log(
    `Create an app queue for create credit note of invoiceId: ` + invoiceId
  )
}

export const cleanUpSequentialAppQueues = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)

  const { cleanUpOnlyFailedQueues = false, limit = 50 } = body
  const query = {
    $or: [
      { flightAt: { $exists: false } },
      {
        flightAt: {
          $lte: moment(new Date())
            .subtract(cleanUpOnlyFailedQueues ? 5 : 20, 'minutes')
            .toDate()
        }
      }
    ],
    status: cleanUpOnlyFailedQueues
      ? { $in: ['failed', 'hold'] }
      : { $in: ['on_flight', 'sent', 'processing'] },
    updatedAt: {
      $lte: moment(new Date())
        .subtract(cleanUpOnlyFailedQueues ? 5 : 20, 'minutes')
        .toDate()
    }
  }

  query.isSequential = true
  query.noOfRetry = { $lte: 4 }
  query.sequentialCategory = { $exists: true }
  console.log('query', JSON.stringify(query))
  const appQueues = await appQueueHelper.getAppQueuesWithOptions(
    query,
    {
      limit,
      sort: { createdAt: 1 }
    },
    session
  )
  console.log('===> appQueues', appQueues.length, appQueues)
  if (size(appQueues)) {
    const sequentialCategories = []
    for (const appQueue of appQueues) {
      const {
        _id: queueId,
        errorDetails = null,
        noOfRetry,
        sequentialCategory,
        status
      } = appQueue
      let { updatingStatus = 'new' } = appQueue

      const flightAt = appQueue?.flightAt || appQueue?.createdAt
      const history = { status, flightAt, noOfRetry }
      if (size(errorDetails)) history.errorDetails = errorDetails

      if (sequentialCategory) {
        let isSequentialCategoryExists = size(
          find(
            sequentialCategories,
            (category) => category === sequentialCategory
          )
        )
        if (!isSequentialCategoryExists) {
          const [existingAppQueue] =
            await appQueueHelper.getAppQueuesWithOptions(
              {
                _id: { $ne: queueId },
                sequentialCategory,
                status: { $nin: ['completed', 'failed', 'hold'] }
              },
              { limit: 1, sort: { createdAt: 1 } },
              session
            )

          if (existingAppQueue?._id) {
            console.log(
              '=== ExistingAppQueueId: ===',
              existingAppQueue._id,
              'QueueId:',
              queueId
            )
            isSequentialCategoryExists = true
          }
        }
        if (isSequentialCategoryExists && updatingStatus === 'new') {
          updatingStatus = 'hold'
        }
        console.log('=== IsSequentialCategoryExists: ===', {
          queueId,
          isSequentialCategoryExists
        })
        sequentialCategories.push(sequentialCategory)
      }
      console.log('=== Queue status will be: ===', { queueId, updatingStatus })
      const updateData = {
        $set: { status: updatingStatus }
      }
      if (status !== updatingStatus) {
        updateData['$inc'] = { noOfRetry: 1 }
        updateData['$push'] = { history }
      }
      await updateAnAppQueue({ _id: queueId }, updateData, session)
    }
    return { isCompleted: false }
  } else return { isCompleted: true }
}

export const createAppQueueForRemoveFilesFromS3 = async (files, session) => {
  if (!size(files)) return false

  const appQueue = {
    action: 'remove_multiple_object_from_s3',
    event: 'remove_multiple_object_from_s3',
    destination: 'lease',
    params: { files },
    priority: 'regular',
    status: 'new'
  }
  return await createAnAppQueue(appQueue, session)
}
