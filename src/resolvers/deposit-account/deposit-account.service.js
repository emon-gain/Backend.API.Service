import { map, isNumber, pick, size } from 'lodash'
import nid from 'nid'

import { CustomError } from '../common'
import {
  appHelper,
  contractHelper,
  fileHelper,
  depositAccountHelper,
  partnerSettingHelper,
  tenantHelper
} from '../helpers'
import { DepositAccountCollection, FileCollection } from '../models'
import {
  appQueueService,
  fileService,
  contractService,
  logService,
  partnerUsageService,
  tenantService
} from '../services'

export const createADepositAccount = async (data, session) => {
  if (!size(data))
    throw new CustomError(404, 'No data found for deposit account creation')

  const [createdAccount] = await DepositAccountCollection.create([data], {
    session
  })

  if (!size(createdAccount))
    throw new CustomError(404, 'Unable to create deposit account')

  console.log(
    `=== Deposit account created. depositAccountId: ${createdAccount._id} ===`
  )

  return createdAccount
}

export const updateADepositAccount = async (query, data, session) => {
  if (!size(query))
    throw new CustomError(404, 'Query not found to update depositAccount')
  if (!size(data))
    throw new CustomError(404, 'Data not found to update depositAccount')

  const depositAccount = await DepositAccountCollection.findOneAndUpdate(
    query,
    data,
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return depositAccount
}

export const createAppQueuesToRetrieveTenantDepositAccountPDF = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  appHelper.checkRequiredFields(['contractId', 'partnerId', 'queueId'], body)
  const { contractId, partnerId, queueId } = body

  if (!(contractId && partnerId && queueId))
    throw new CustomError(400, 'Missing required data')

  const populate = [
    { path: 'account', populate: ['organization', 'person'] },
    { path: 'partner', populate: ['partnerSetting'] }
  ]
  const contract = await contractHelper.getAContract(
    { _id: contractId, partnerId },
    session,
    populate
  )
  if (!contract?._id) throw new CustomError(404, 'Could not find contract!')

  const isEnabledDepositAccountProcess =
    await depositAccountHelper.isEnabledDepositAccountProcess({
      actionType: 'esigning_lease_pdf',
      contractInfoOrId: contract,
      partnerInfoOrId: contract?.partner
    })
  if (!isEnabledDepositAccountProcess) {
    throw new CustomError(
      400,
      'Deposit account is not enabled for this contract'
    )
  }
  console.log('isEnabledDepositAccountProcess', isEnabledDepositAccountProcess)
  const { rentalMeta = {} } = contract
  const {
    depositAmount = 0,
    enabledJointlyLiable = false,
    enabledJointDepositAccount = false,
    tenantId = '',
    tenants = []
  } = rentalMeta || {}

  let depositAmountForEachTenant = 0
  let tenantIds = []
  if (
    depositAmount &&
    enabledJointlyLiable &&
    !enabledJointDepositAccount &&
    size(tenants)
  ) {
    const allTenantIds = map(tenants, 'tenantId')
    depositAmountForEachTenant = depositAmount / size(allTenantIds)
    tenantIds = allTenantIds
  } else if (depositAmount && tenantId) {
    depositAmountForEachTenant = depositAmount
    tenantIds = [tenantId]
  }
  console.log('Ok. tenantIds', tenantIds)
  const tenantList =
    (await tenantHelper.getTenants({ _id: { $in: tenantIds } }, session, [
      'user'
    ])) || []
  if (!size(tenantList)) throw new CustomError(404, 'Could not find tenants')
  console.log('tenantList', { tenantList })
  const appQueuesData = []
  const qIds = []
  for (const tenant of tenantList) {
    const { _id: tenantId } = tenant || {}
    if (tenantId) {
      const tenantDataForBank =
        depositAccountHelper.prepareTenantBankContractObject(
          tenant,
          contract,
          depositAmountForEachTenant
        )

      const _id = nid(17)
      qIds.push(_id)

      appQueuesData.push({
        _id,
        action: 'update_kyc_form_for_tenant',
        destination: 'lease',
        event: 'handle_deposit_account_process',
        isSequential: true,
        params: {
          contractId,
          partnerId,
          tenantDataForBank,
          tenantId
        },
        priority: 'immediate',
        sequentialCategory: `update_kyc_form_for_tenant_${contractId}`
      })
    }
  }

  console.log(`#${size(qIds)} queues creating. queueIds: ${qIds}`)

  const response = await appQueueService.createMultipleAppQueues(
    appQueuesData,
    session
  )
  console.log(`response: ${response}`)
  const ids = map(response, '_id')
  console.log(`#${size(ids)} queues created. queueIds: ${ids}`)

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed. queueId: ${queueId} ===`)
  await appQueueService.updateAppQueueToCompleted(queueId, session)

  return { ids: qIds }
}

const createAppQueueForSubmitKycForm = async (params, session) => {
  const { contractId, formData, referenceNumber, partnerId, tenantId } = params
  const appQueueData = {
    action: 'submit_deposit_account_kyc_form',
    destination: 'lease',
    event: 'submit_deposit_account_kyc_form',
    params: {
      contractId,
      formData,
      partnerId,
      referenceNumber,
      tenantId
    },
    status: 'new',
    priority: 'immediate'
  }
  const appQueue = await appQueueService.createAnAppQueue(appQueueData, session)
  return appQueue
}

export const submitKycForm = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['referenceNumber'], body)
  const { partnerId, referenceNumber } = body

  const preparedData = await depositAccountHelper.prepareKycFormData(body)
  const { formData, signingUrl, tenantId } = preparedData
  await createAppQueueForSubmitKycForm(preparedData, session)

  const today = (await appHelper.getActualDate(partnerId, true))
    .startOf('day')
    .toDate()

  const updatedKycForm = {
    'depositAccountMeta.kycForms.$.isFormSubmitted': true,
    'depositAccountMeta.kycForms.$.formData': formData,
    'depositAccountMeta.kycForms.$.createdAt': today
  }

  const isUpdated = await tenantService.updateTenant(
    {
      _id: tenantId,
      partnerId,
      'depositAccountMeta.kycForms': { $elemMatch: { referenceNumber } }
    },
    { $set: updatedKycForm },
    session
  )
  if (!isUpdated) throw new CustomError(404, 'Could not update tenant')
  return {
    signingUrl
  }
}

const prepareDepositAccountCreationData = (params) => {
  const {
    bankAccountNumber,
    bankNotificationId,
    bankNotificationType,
    branchId,
    contractId,
    depositAmount,
    partnerId,
    propertyId,
    referenceNumber,
    tenantId
  } = params

  const data = {}

  if (bankAccountNumber) data.bankAccountNumber = bankAccountNumber
  if (bankNotificationId) data.bankNotificationId = bankNotificationId
  if (bankNotificationType) data.bankNotificationType = bankNotificationType
  if (branchId) data.branchId = branchId
  if (contractId) data.contractId = contractId
  if (isNumber(depositAmount)) data.depositAmount = depositAmount
  if (partnerId) data.partnerId = partnerId
  if (propertyId) data.propertyId = propertyId
  if (referenceNumber) data.referenceNumber = referenceNumber
  if (tenantId) data.tenantId = tenantId

  return data
}

const addPartnerUsageForDepositAccount = async (depositAccount, session) => {
  if (!size(depositAccount))
    throw new CustomError(
      404,
      'Could not find depositAccount to create partner usage'
    )

  const usageData = {
    branchId: depositAccount.branchId,
    createdAt: depositAccount.createdAt,
    partnerId: depositAccount.partnerId,
    total: 1,
    type: 'deposit_account'
  }

  const [partnerUsage] = await partnerUsageService.createAPartnerUsage(
    usageData,
    session
  )

  if (!size(partnerUsage)) {
    console.log(
      `=== Unable to create PartnerUsage for deposit account. depositAccountId: ${depositAccount._id} ===`
    )
    throw new CustomError(404, 'Unable to create PartnerUsage')
  }
  console.log(
    `=== PartnerUsage created for deposit account. partnerUsageId: ${partnerUsage._id}, depositAccountId: ${depositAccount._id} ===`
  )
  return true
}

const sendDACreationNotificationToTenant = async (depositAccount, session) => {
  if (!size(depositAccount))
    throw new CustomError(
      404,
      'Could not find depositAccount to send depositAccount creation notification to tenant'
    )

  const {
    _id: depositAccountId,
    contractId,
    partnerId,
    tenantId
  } = depositAccount

  const partnerSetting = partnerId
    ? await partnerSettingHelper.getAPartnerSetting({ partnerId })
    : null

  if (!size(partnerSetting))
    throw new CustomError(
      404,
      `Could not find partnerSetting for this partner. partnerId: ${partnerId}`
    )

  if (partnerSetting?.notifications?.depositAccount) {
    const queueData = {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_deposit_account_created',
      params: {
        partnerId,
        collectionId: contractId,
        collectionNameStr: 'contracts',
        options: { tenantId, depositAccountId }
      },
      priority: 'immediate'
    }

    const [queue] = await appQueueService.createAnAppQueue(queueData, session)
    console.log(
      `=== Created appQueue to send deposit account creation notification. queueId: ${queue._id}, depositAccountId: ${depositAccountId} ===`
    )
    return true
  } else {
    console.log(
      `=== DepositAccount is not enabled for this partner. partnerSettingId: ${partnerSetting._id} ===`
    )
    return false
  }
}

const sendDAPaymentNotificationToTenant = async (
  depositAccount,
  incomingPaymentData,
  session
) => {
  if (!size(depositAccount))
    throw new CustomError(
      404,
      'Could not find depositAccount to send depositAccount payment notification to tenant'
    )

  if (!size(incomingPaymentData))
    throw new CustomError(
      404,
      'Could not find incomingPaymentData to send depositAccount payment notification to tenant'
    )

  const {
    _id: depositAccountId,
    contractId,
    partnerId,
    tenantId
  } = depositAccount

  const partnerSetting = partnerId
    ? await partnerSettingHelper.getAPartnerSetting({ partnerId })
    : null

  if (!size(partnerSetting))
    throw new CustomError(
      404,
      `Could not find partnerSetting for this partner. partnerId: ${partnerId}`
    )

  if (partnerSetting?.notifications?.depositAccount) {
    const queueData = {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_deposit_incoming_payment',
      params: {
        partnerId,
        collectionId: contractId,
        collectionNameStr: 'contracts',
        options: { tenantId, incomingPaymentData }
      },
      priority: 'immediate'
    }

    const [queue] = await appQueueService.createAnAppQueue(queueData, session)
    console.log(
      `=== Created appQueue to send deposit account payment notification. queueId: ${queue._id}, depositAccountId: ${depositAccountId} ===`
    )
    return true
  } else {
    console.log(
      `=== DepositAccount is not enabled for this partner. partnerSettingId: ${partnerSetting._id} ===`
    )
    return false
  }
}

const createDepositAccountCreationLog = async (depositAccount, session) => {
  if (!size(depositAccount))
    throw new CustomError(
      404,
      'Could not find depositAccount to create depositAccount creation log'
    )

  const { contractId, partnerId, tenantId } = depositAccount

  const contract = await contractHelper.getAContract({
    _id: contractId,
    partnerId
  })

  if (size(contract)) {
    const logData = {
      accountId: contract.accountId || undefined,
      agentId: contract.agentId || undefined,
      action: 'deposit_account_created',
      branchId: contract.branchId || undefined,
      context: 'property',
      contractId,
      isChangeLog: false,
      meta: [
        {
          field: 'leaseSerial',
          value: contract.leaseSerial
        },
        {
          field: 'tenantId',
          value: tenantId
        }
      ],
      partnerId,
      propertyId: contract.propertyId || undefined,
      tenantId,
      visibility: ['property', 'account', 'tenant']
    }
    const log = await logService.createLog(logData, session)

    if (!size(log)) {
      console.log(
        `=== Unable to create log for deposit account creation. depositAccountId: ${depositAccount._id} ===`
      )
      throw new CustomError(
        404,
        'Unable to create deposit account creation log'
      )
    }
    console.log(
      `=== Log created for deposit account creation. logId: ${log._id}, depositAccountId: ${depositAccount._id} ===`
    )
    return true
  } else {
    console.log(
      `=== Contract not found. contractId: ${contractId}, partnerId: ${partnerId} ===`
    )
    return false
  }
}

export const createADepositAccountForLambda = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  appHelper.checkRequiredFields(
    ['contractId', 'partnerId', 'referenceNumber', 'queueId'],
    body
  )

  const { queueId } = body

  const data = prepareDepositAccountCreationData(body)

  const createdDepositAccount = await createADepositAccount(data, session)

  // Creating DepositAccount creation log
  await createDepositAccountCreationLog(
    createdDepositAccount.toObject(),
    session
  )

  // Send deposit_account created notification to tenant
  await sendDACreationNotificationToTenant(
    createdDepositAccount.toObject(),
    session
  )

  // Add deposit account to partner usage
  await addPartnerUsageForDepositAccount(
    createdDepositAccount.toObject(),
    session
  )

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  await appQueueService.updateAppQueueToCompleted(queueId, session)

  return createdDepositAccount
}

export const updateADepositAccountForLambda = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  appHelper.checkRequiredFields(
    ['depositAccountId', 'payments', 'queueId', 'totalPaymentAmount'],
    body
  )

  const { depositAccountId, payments, queueId, totalPaymentAmount } = body

  if (!(size(payments) && depositAccountId && queueId))
    throw new CustomError(400, 'Missing required fields in the req body')

  if (!isNumber(totalPaymentAmount))
    throw new CustomError(400, 'TotalPaymentAmount is required')

  const query = { _id: depositAccountId }

  const depositAccount = await depositAccountHelper.getDepositAccount(query)
  if (!size(depositAccount))
    throw new CustomError(404, 'DepositAccount not found')

  console.log(
    '=== Old totalPaymentAmount is: ',
    depositAccount.totalPaymentAmount
  )
  console.log('=== New payment to be add: ', totalPaymentAmount)
  const data = { $inc: { totalPaymentAmount }, $push: { payments } }

  const updatedDepositAccount = await updateADepositAccount(
    query,
    data,
    session
  )

  if (!size(updatedDepositAccount))
    throw new CustomError(404, 'Unable to update deposit account')

  console.log(
    `=== Updated deposit account. depositAccountId: ${depositAccountId} totalPaymentAmount: ${
      updatedDepositAccount.totalPaymentAmount
    }, payments: ${JSON.stringify(payments)} ===`
  )
  // Send deposit_account payment notification to tenant
  await sendDAPaymentNotificationToTenant(
    updatedDepositAccount,
    payments,
    session
  )

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  await appQueueService.updateAppQueueToCompleted(queueId, session)

  return updatedDepositAccount
}

export const addFileAndUpdateContract = async (req) => {
  const { body, session } = req
  const { contractId, tenantId, fileData, fileKey } = body
  try {
    console.log('Method called addFileAndUpdateContract', body)
    const updatedFile = await FileCollection.findOneAndUpdate(
      {
        tenantId,
        contractId,
        type: 'deposit_account_contract_pdf',
        isExistingFile: { $exists: false }
      },
      {
        $set: { isExistingFile: true }
      },
      {
        new: true,
        runValidators: true,
        session
      }
    )
    console.log('=== File Updated ===', updatedFile)
    const [file] = await fileService.createAFile(fileData, session)
    console.log('=== File Created ===', file, file?._id)
    const contractUpdated = await contractService.updateContract(
      {
        _id: contractId,
        'rentalMeta.tenantLeaseSigningStatus.tenantId': tenantId
      },
      {
        $set: {
          'rentalMeta.tenantLeaseSigningStatus.$.attachmentFileId': file?._id
        }
      },
      session
    )
    console.log('=== Contract Updated ===', contractUpdated)
    const contractData =
      (
        await contractHelper.getContractDataWithFile(
          contractId,
          tenantId,
          session
        )
      )[0] || {}
    console.log('=== Contract Found ===', contractData)
    const userLang = contractData?.userLang
    const tenantSignerInfo = contractData?.tenantSignerInfo
    const tenantLeaseSigningStatus = contractData?.tenantLeaseSigningStatus
    const dataForIdfy = {
      title: await appHelper.translateToUserLng(
        'deposit_accounts.contract_title',
        userLang
      ),
      fileName: 'deposit_account_contract.pdf',
      description: await appHelper.translateToUserLng(
        'deposit_accounts.contract_description',
        userLang
      ),
      signers: [tenantSignerInfo?.idfySignerId],
      convertToPdf: true,
      type: 'sign'
    }
    const documentId = contractData?.idfyLeaseDocId
    const sqsParams = {
      partnerId: contractData?.partner?._id,
      processType: 'create_document',
      eSignType: 'deposit_account',
      dataForIdfy,
      fileKey,
      fileType: 'deposit_account_contract_pdf',
      documentId,
      docId: contractId,
      tenantId,
      tenantLeaseSigningStatus,
      callBackParams: {
        callBackAction: 'deposit_account_e_signing_initialisation_process',
        callBackDestination: 'lease',
        callBackEvent: 'deposit_account_e_signing_initialisation_process',
        callBackPriority: 'regular'
      }
    }
    const appQueueData = await appQueueService.createAnAppQueue(
      {
        event: 'create_document',
        action: 'handle_e_signing',
        priority: 'regular',
        isSequential: true,
        destination: 'esigner',
        sequentialCategory: `create_deposit_account_attachment${contractId}`,
        params: sqsParams
      },
      session
    )
    console.log('=== App queue created ===', appQueueData)
    return {
      msg: 'Success'
    }
  } catch (e) {
    console.log('Error when api called addFileAndUpdateContract', e)
    throw new Error(e)
  }
}

export const uploadIdfySignedFileToS3Service = async (req) => {
  const { body = {}, session } = req
  const { contractId, documentId, esignType, queueId } = body
  const directive = 'Files'
  try {
    const contractData = await contractHelper.contractWithFileForDepositAccount(
      contractId
    )
    console.log(
      `Fetched data for contract id uploadIdfySignedFileToS3Service ${contractId}`,
      contractData
    )
    const getContractData = contractData[0]

    console.log(
      `Fetched data for contract id uploadIdfySignedFileToS3Service ${contractId}`,
      getContractData
    )
    const partnerId = getContractData?.partnerId
    const tenantId = getContractData?.rentalMeta?.tenantId
    const appQueueData = []
    for (let i = 0; i < getContractData.files.length; i++) {
      const singleData = getContractData.files[i]
      console.log('Attachment id is', singleData?.signer?.idfyAttachmentId)
      const appQueueParams = {
        partnerId,
        processType: 'fetch_or_upload_document',
        subProcessType: 'uploadIdfySignedFileToS3',
        esignType,
        documentId,
        contractId,
        paramsAndOptions: fileHelper.getFileUploadParamsAndOptions({
          directive,
          existingFileName: singleData?.name,
          fileDirectory: '',
          partnerId,
          subFolder: '',
          type: 'deposit_account_contracts'
        }),
        attachmentId: singleData?.signer?.idfyAttachmentId,
        attachmentFileId: singleData?._id,
        tenantId,
        callBackParams: {
          callBackAction: 'handle_fetched_or_uploaded_s3_document',
          callBackDestination: 'lease',
          callBackEvent: 'handle_deposit_account_process',
          callBackIsSequential: true,
          callBackSequentialCategory: `esigning_deposit_account_pdf_${contractId}`,
          callBackPriority: 'immediate'
        },
        actions: ['status', 'file_upload']
      }
      const singleAppQueue = {
        event: 'fetch_or_upload_document',
        action: 'handle_e_signing',
        params: appQueueParams,
        destination: 'esigner',
        priority: 'regular'
      }
      appQueueData.push(singleAppQueue)
    }
    const appQueue = await appQueueService.createMultipleAppQueues(
      appQueueData,
      session
    )
    console.log(`App queues create for contractId ${contractId}`, appQueue)
    await appQueueService.updateAppQueueToCompleted(queueId, session)
    const appQueueIds = []
    for (let i = 0; i < appQueue?.length; i++) {
      appQueueIds.push(appQueue[i]._id)
    }
    console.log('Total app queue created', appQueueIds.length)
    return appQueueIds || []
  } catch (e) {
    console.log('Error when creating app queue for upload signed da', e)
    throw new Error(`Error when creating app queue for upload signed da ${e}`)
  }
}

export const createTestNotification = async (req) => {
  const stage = process.env.STAGE || ''
  console.log('=== Stage', stage)
  if (!(stage && ['local', 'dev', 'test', 'demo'].includes(stage))) {
    throw new CustomError(405, 'Invalid env found')
  }
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'type'])
  const { body, session, user } = req
  const { partnerId = '' } = user || {}
  if (!partnerId) throw new CustomError(400, 'PartnerId does not exists')
  const { contractId, type, amount = 0 } = body
  const tenants = await depositAccountHelper.getTenantsTestNotificationCreate(
    contractId,
    type
  )
  if (!size(tenants)) throw new CustomError(404, 'Tenant not found.')
  console.log(`=== Tenants`, JSON.stringify(tenants))
  if (type === 'createTestNotification') {
    for (const tenant of tenants) {
      const randomAccount = Math.floor(Math.random() * 1000000000).toString()
      const tenantId = tenant.tenantId || ''
      console.log('===> TenantId', tenantId)
      const kycData = await depositAccountHelper.getTenantDepositKycData({
        partnerId,
        tenantId,
        contractId
      })
      const referenceNumber = kycData?.referenceNumber || ''
      console.log('=== referenceNumber', referenceNumber)
      const formData = { accountNumber: randomAccount, referenceNumber }
      const params = { partnerId, contractId, tenantId, formData }
      if (referenceNumber) {
        const contract = await contractService.updateContract(
          { _id: contractId },
          {
            $set: { 'rentalMeta.isDepositAccountCreationTestProcessing': true }
          },
          session
        )
        console.log(
          '=== Updated contract info',
          contract?.rentalMeta?.isDepositAccountCreationTestProcessing
        )
        await createAppQueueForTestNotification(params, session)
      } else
        console.log(
          '=== No referenceNumber number found while creating DA, For tenantId',
          tenantId
        )
    }
    return { result: true }
  } else if (type === 'incomingPaymentTestNotification') {
    if (!isNumber(amount)) throw new CustomError(400, 'Amount must be a number')
    if (amount <= 0) throw new CustomError(400, 'Invalid amount')

    for (const tenant of tenants) {
      const randomAccount = Math.floor(Math.random() * 1000000000).toString()
      const tenantId = tenant.tenantId || ''
      const kycData = await depositAccountHelper.getTenantDepositKycData({
        partnerId,
        tenantId,
        contractId
      })
      const referenceNumber = kycData?.referenceNumber || ''
      console.log('=== referenceNumber', referenceNumber)
      const depositAccount = depositAccountHelper.getDepositAccount({
        partnerId,
        tenantId,
        contractId
      })
      const totalPaymentAmount = depositAccount?.totalPaymentAmount || 0
      const currentBalance = totalPaymentAmount + amount
      const formData = {
        paymentReference: randomAccount,
        referenceNumber,
        paymentAmount: amount,
        currentBalance
      }
      const params = { partnerId, contractId, tenantId, formData }
      if (referenceNumber) {
        const contract = await contractService.updateContract(
          { _id: contractId },
          {
            $set: { 'rentalMeta.isDepositAccountPaymentTestProcessing': true }
          },
          session
        )
        console.log(
          '=== Updated contract info',
          contract?.rentalMeta?.isDepositAccountPaymentTestProcessing
        )
        console.log(
          `=== currentBalance: ${currentBalance}, amount: ${amount}, referenceNumber: ${referenceNumber}`
        )
        await createAppQueueForTestNotificationOfIncomingPayment(
          params,
          session
        )
      } else
        console.log(
          '=== No referenceNumber number found while adding payment, For tenantId',
          tenantId
        )
    }
    return { result: true }
  } else throw new CustomError(400, 'Invalid type found')
}
const createAppQueueForTestNotification = async (params, session) => {
  const { partnerId, contractId, tenantId, formData } = params
  const appQueueData = {
    action: 'create_deposit_account_test_notification',
    destination: 'lease',
    event: 'test_deposit_account',
    params: {
      contractId,
      formData,
      partnerId,
      tenantId
    },
    status: 'new',
    priority: 'immediate'
  }
  const [createdQ] = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  console.log(
    '=== Created Q to create_deposit_account_test_notification. qId:',
    createdQ?._id
  )
}

const createAppQueueForTestNotificationOfIncomingPayment = async (
  params,
  session
) => {
  const { partnerId, contractId, tenantId, formData } = params
  const appQueueData = {
    action: 'incoming_payment_test_notification',
    destination: 'lease',
    event: 'test_deposit_account',
    params: {
      contractId,
      formData,
      partnerId,
      tenantId
    },
    status: 'new',
    priority: 'immediate'
  }
  const [createdQ] = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  console.log(
    '=== Created Q to incoming_payment_test_notification. qId:',
    createdQ?._id
  )
}

const prepareDataByContractInfo = async (contractId, session) => {
  const contract = await contractHelper.getAContract(
    { _id: contractId },
    session
  )
  const data = pick(contract, [
    '_id',
    'accountId',
    'propertyId',
    'agentId',
    'branchId'
  ])
  return data
}

const prepareContractMetaData = (contractInfo, tenantId) => [
  { field: 'leaseSerial', value: contractInfo.leaseSerial },
  {
    field: 'tenantId',
    value: tenantId
  }
]

const prepareLogData = async (body) => {
  const { contractId, tenantId, partnerId } = body
  const contractData = await prepareDataByContractInfo(contractId)
  const visibility = ['account', 'property', 'tenant']
  const meta = prepareContractMetaData(contractData, tenantId)
  return {
    action: 'sent_request_for_deposit_account_creation',
    partnerId,
    tenantId,
    meta,
    contractId: contractData._id,
    propertyId: contractData.propertyId,
    agentId: contractData.agentId,
    branchId: contractData.branchId,
    visibility,
    context: 'property'
  }
}

export const createLogForDepositAccount = async (req) => {
  console.log('=== started creating log ====')
  const { body, session } = req
  console.log('checking body ', body)
  const logData = await prepareLogData(body)
  console.log('checking logData ', logData)
  const { _id = '' } = await logService.createLog(logData, session)
  console.log('checking _id ', _id)
  return _id ? true : false
}
