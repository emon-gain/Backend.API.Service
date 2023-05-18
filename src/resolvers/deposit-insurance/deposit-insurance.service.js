import { assign, pick, size } from 'lodash'
import { DepositInsuranceCollection } from '../models'
import { CustomError } from '../common'
import {
  appInvoiceService,
  appQueueService,
  contractService,
  logService
} from '../services'
import {
  appHelper,
  contractHelper,
  depositInsuranceHelper,
  logHelper
} from '../helpers'

export const updateADepositInsurance = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update deposit_insurance')
  }
  const response = await DepositInsuranceCollection.findOneAndUpdate(
    query,
    data,
    {
      runValidators: true,
      new: true,
      session
    }
  )
  if (!size(response))
    throw new CustomError(404, `Unable to update deposit_insurance`)

  return response
}

export const updateMultipleDepositInsurances = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update deposit insurances')
  }
  const response = await DepositInsuranceCollection.updateMany(query, data, {
    runValidators: true,
    session
  })
  if (!size(response))
    throw new CustomError(404, `Unable to update deposit insurances`)

  return response
}

export const createDepositInsuranceCreateLog = async (
  action,
  options,
  session
) => {
  const { collectionId = '', collectionName = '', partnerId = '' } = options

  if (action && partnerId) {
    let logData = pick(options, ['context', 'partnerId'])
    const query = { partnerId }
    logData.action = action
    if (collectionId) {
      query._id = collectionId
      if (collectionName === 'contract') {
        logData.contractId = collectionId
        const contractInfo = await contractHelper.getAContract(query, session)
        const newLogData = pick(contractInfo, [
          'accountId',
          'agentId',
          'branchId',
          'propertyId'
        ])
        const { rentalMeta = {} } = contractInfo || {}
        if (size(rentalMeta) && rentalMeta.tenantId)
          newLogData.tenantId = rentalMeta.tenantId
        logData = assign(logData, newLogData)
        logData.visibility = logHelper.getLogVisibility(options, contractInfo)
      }
    }

    await logService.createLog(logData, session)
  }
}

export const prepareDataAndInsertAQueueForDISentNotification = async (
  data,
  session
) => {
  const { params } =
    (await depositInsuranceHelper.getParamsForQueueCreationOfDINotification(
      data
    )) || {}

  if (!size(params)) return {}
  else {
    const queueData = {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_deposit_insurance_created',
      params,
      priority: 'immediate'
    }
    await appQueueService.insertInQueue(queueData, session)
  }
}

export const updateDepositInsuranceCreationStatus = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['queueId', 'insuranceData'], body)
  const { insuranceData = {}, queueId = '' } = body
  appHelper.validateId({ queueId })
  if (!size(insuranceData))
    throw new CustomError(400, "Can't be empty insuranceData")
  const { query, updatingData } =
    await depositInsuranceHelper.prepareDepositInsuranceQueryAndUpdatingData(
      body
    )
  const depositInsuranceInfo =
    await depositInsuranceHelper.getADepositInsurance(query, session)
  if (!size(depositInsuranceInfo))
    throw new CustomError(400, "depositInsurance doesn't exists")

  const updatedDepositInsurance = await updateADepositInsurance(
    query,
    updatingData,
    session
  )
  if (
    depositInsuranceInfo.status !== 'registered' &&
    updatedDepositInsurance.status === 'registered'
  ) {
    // CreateDepositInsuranceCreateLog
    const { contractId = '', partnerId = '' } = updatedDepositInsurance
    const action = 'deposit_insurance_created'
    const options = {
      collectionId: contractId,
      collectionName: 'contract',
      context: 'property',
      partnerId
    }
    await createDepositInsuranceCreateLog(action, options, session)
    await prepareDataAndInsertAQueueForDISentNotification(
      updatedDepositInsurance,
      session
    )
  }

  return updatedDepositInsurance
}

export const removePaymentInfoBasedOnPayment = async (payment, session) => {
  if (!size(payment)) throw new CustomError(404, 'Required payment data')

  const { _id: paymentId, amount, depositInsuranceId } = payment
  if (!depositInsuranceId) return false

  const depositInsurance = await depositInsuranceHelper.getADepositInsurance(
    { _id: depositInsuranceId },
    session
  )
  if (!size(depositInsurance))
    throw new CustomError(404, 'DepositInsurance not found')

  const { payments = [], totalPaymentAmount } = depositInsurance

  // Removing old payment info from payments array
  const newPaymentsArray = payments?.filter((obj) => obj.id !== paymentId)

  if (size(payments) === size(newPaymentsArray)) return false // If no payments match do nothing

  const data = {
    totalPaymentAmount: totalPaymentAmount - amount,
    payments: newPaymentsArray
  }

  await updateADepositInsurance(
    { _id: depositInsuranceId },
    { $set: data },
    session
  )
  console.log(
    `=== Removed paymentsInfo and updated totalPaymentAmount from depositInsurance. depositInsuranceId: ${depositInsuranceId} ===`
  )
}

const createDepositInsurance = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(400, 'No deposit insurance data found to insert')
  }
  const addedDepositInsurance = await DepositInsuranceCollection.create(
    [data],
    {
      session
    }
  )
  return addedDepositInsurance
}

const addDepositInsurance = async (body, contract, session) => {
  await depositInsuranceHelper.checkRequirementsForAddingDI(body)
  const insertData = depositInsuranceHelper.prepareDataForDepositInsurance(
    body,
    contract
  )
  const addedData = await createDepositInsurance(insertData, session)
  if (!size(addedData))
    throw new CustomError(404, 'Could not create Deposit insurance')
  return addedData
}

const addDepositInsuranceIdToLease = async (
  body,
  depositInsuranceId,
  session
) => {
  const { contractId = '' } = body
  const contract = await contractService.updateContract(
    { _id: contractId },
    { $set: { 'rentalMeta.depositInsuranceId': depositInsuranceId } },
    session
  )
  return contract
}

export const addDepositInsuranceDataForLambda = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['contractId', 'partnerId'], body)
  const { contractId } = body
  const pipeline =
    depositInsuranceHelper.preparePipelineForDepositInsurance(contractId)
  const contract = (await contractHelper.getContractByAggregate(pipeline)) || {}
  const [depositInsurance] = await addDepositInsurance(body, contract, session)
  await addDepositInsuranceIdToLease(body, depositInsurance._id, session)
  await appInvoiceService.createAppInvoiceForDepositInsurance(
    depositInsurance,
    contract,
    session
  )
  return depositInsurance
}

const updateDepositInsurancesAndCreateAppQueues = async (
  depositInsurances,
  session
) => {
  const appQueuesData = []
  for (let i = 0; i < size(depositInsurances); i++) {
    const {
      _id: depositInsuranceId,
      appInvoiceId,
      notificationSendingDate,
      partnerId
    } = depositInsurances[i] || {}

    if (appInvoiceId && depositInsuranceId && partnerId) {
      appQueuesData.push({
        action: 'send_notification',
        destination: 'notifier',
        event: 'send_deposit_insurance_payment_reminder',
        params: {
          collectionId: appInvoiceId,
          collectionNameStr: 'app_invoices',
          partnerId
        },
        priority: 'regular'
      })

      await updateADepositInsurance(
        { _id: depositInsuranceId },
        { $set: { paymentReminderSentAt: notificationSendingDate } },
        session
      )
    } else {
      console.log(
        '+++ Could not find required data to create queue for sending deposit insurance payment reminder for:',
        depositInsurances[i],
        '+++'
      )
      continue
    }
  }

  const depositInsurancesCount = size(depositInsurances)
  if (size(appQueuesData) !== depositInsurancesCount) {
    throw new CustomError(
      405,
      'Something went wrong when preparing queues data'
    )
  }

  const createdAppQueuesData = await appQueueService.createMultipleAppQueues(
    appQueuesData,
    session
  )
  if (size(createdAppQueuesData) !== size(appQueuesData)) {
    throw new CustomError(405, 'Could not create all app queues')
  }

  return depositInsurancesCount
}

export const createQForSendingDepositInsurancePaymentReminder = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { query, options } = body

  const depositInsurances =
    await depositInsuranceHelper.getDepositInsuranceForPaymentReminder(
      query,
      options
    )
  const depositInsurancesCount = size(depositInsurances) || 0
  if (!depositInsurancesCount) {
    console.log(
      '====> No deposit insurance found for sending payment reminder <===='
    )
    return 0
  }

  return updateDepositInsurancesAndCreateAppQueues(depositInsurances, session)
}
