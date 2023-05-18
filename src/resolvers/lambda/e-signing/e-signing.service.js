import { find, head, indexOf, map, size } from 'lodash'
import nid from 'nid'

import { CustomError } from '../../common'
import {
  appHelper,
  appQueueHelper,
  contractHelper,
  depositAccountHelper,
  depositInsuranceHelper,
  eSigningHelper,
  fileHelper,
  partnerHelper,
  partnerPayoutHelper,
  propertyItemHelper
} from '../../helpers'
import {
  accountService,
  appQueueService,
  contractService,
  fileService,
  partnerPayoutService,
  propertyItemService
} from '../../services'
import { checkRequiredFields } from '../../app/app.helper'
import { sendTenantLeaseESigningNotificationAndAddSendESigningTagInContract } from '../../contract/contract.service'

const getPartnerIdFromFileInfo = async (fileId = '') => {
  const { partnerId = '' } = (await fileHelper.getAFile({ _id: fileId })) || {}
  return partnerId
}

export const createAnAppQueueForDirectRemittanceESigningDocument = async (
  params,
  session
) => {
  const { queueId, queueParams } = params
  // Preparing appQueue data and creating appQueue
  const {
    documentId = '',
    fileId = '',
    idfyResData = {},
    partnerPayoutId = ''
  } = queueParams
  const { document = {}, status = null } = idfyResData || {}

  if (status && size(document?.signers)) {
    if (!fileId) throw new CustomError(400, 'Missing fileId')
    const partnerId = await getPartnerIdFromFileInfo(fileId)
    if (!partnerId) throw new CustomError(404, 'No partnerId found in fileInfo')
    const queueData = {
      action: 'handle_partner_payout_dividing_by_payouts',
      destination: 'payments',
      event: 'handle_partner_payout_dividing_by_payouts',
      params: { documentId, fileId, idfyResData, partnerId, partnerPayoutId },
      priority: 'regular'
    }
    // Creating appQueue for payment lambda
    const [createdAppQueue] = await appQueueService.createAnAppQueue(
      queueData,
      session
    )
    if (size(createdAppQueue)) {
      console.log(
        `## Created appQueue for payment lambda. queueId: ${createdAppQueue._id}, action: ${createdAppQueue.action}`
      )
      // Updating lambda appQueue to completed
      return await appQueueService.updateAppQueueToCompleted(queueId, session)
    }
  } else throw new CustomError(400, 'Missing documentStatus or documentSigners')
}

export const addESigningDocumentInfo = async (req) => {
  const { body, session } = req
  eSigningHelper.checkRequiredDataBeforeStartESigningProgress(body)
  const { idfyResData, queueId } = body

  const queueInfo = await appQueueHelper.getAQueueItem(
    { _id: queueId },
    session
  )
  if (!size(queueInfo)) throw new CustomError(404, "AppQueue doesn't exists")

  const { params = {} } = queueInfo
  if (!size(params))
    throw new CustomError(404, "AppQueue params doesn't exists")

  const { subProcessType = '' } = params
  let response = false
  if (subProcessType === 'payout_payment_e_signing_document') {
    response = await createAnAppQueueForDirectRemittanceESigningDocument(
      { queueId, queueParams: { idfyResData, ...params } },
      session
    )
  } else throw new CustomError(400, 'Invalid subProcessType found')
  // Todo :Will be implement later: 6 subProcessType are pending
  //  Ref :Meteor: export const addESigningDocumentInfo = function (params, req, res, next)

  return { result: response }
}

const sendDirectRemittanceApprovalESigningNotification = async (
  queueInfo,
  idfyResponse,
  session
) => {
  const { documentId = '', signers = [] } = idfyResponse
  if (!(documentId && size(signers)))
    throw new CustomError(400, 'Missing required fields in idfyResponse')

  const { queueId, params = {} } = queueInfo
  const { docId: partnerPayoutId, fileType, partnerId } = params
  if (!(partnerPayoutId && fileType && partnerId))
    throw new CustomError(404, 'Missing required params')

  const partnerPayoutQuery =
    fileType === 'payouts_approval_esigning_pdf'
      ? { _id: partnerPayoutId, partnerId, payoutIds: { $exists: true } }
      : fileType === 'payments_approval_esigning_pdf'
      ? { _id: partnerPayoutId, partnerId, paymentIds: { $exists: true } }
      : {}

  if (!size(partnerPayoutQuery))
    throw new CustomError(404, 'Invalid fileType found')

  const partnerPayout = await partnerPayoutHelper.getAPartnerPayout(
    partnerPayoutQuery,
    session
  )
  if (!size(partnerPayout))
    throw new CustomError(404, "Partner payout doesn't exists")

  const directRemittanceSigningStatus = []
  for (const signer of signers) {
    const {
      id: idfySignerId,
      externalSignerId: userId,
      url: eSigningUrl
    } = signer || {}
    if (!eSigningUrl) {
      console.log(`+++ Missing required e-signing URL: ${{ ...signer }} +++`)
      continue
    }

    const appQueueData = {
      action: 'send_notification',
      destination: 'notifier',
      priority: 'immediate',
      status: 'new'
    }
    if (fileType === 'payouts_approval_esigning_pdf') {
      appQueueData.event = 'send_payouts_approval_esigning'
      appQueueData.params = {
        partnerId,
        collectionId: head(partnerPayout?.payoutIds) || '',
        collectionNameStr: 'payouts',
        options: {
          userId,
          payoutsApprovalESigningURL: signer.url
        }
      }
    } else if (fileType === 'payments_approval_esigning_pdf') {
      appQueueData.event = 'send_payments_approval_esigning'
      appQueueData.params = {
        partnerId,
        collectionId: head(partnerPayout?.paymentIds) || '',
        collectionNameStr: 'payments',
        options: {
          userId,
          paymentsApprovalESigningURL: eSigningUrl
        }
      }
    }

    directRemittanceSigningStatus.push({
      idfySignerId,
      internalUrl: nid(17),
      signingUrl: eSigningUrl,
      signed: false,
      userId
    })

    // Creating an appQueue to send email to the signer with approvalEsigningUrl
    const [createdQueue] = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    if (size(createdQueue)) {
      console.log(
        `## Creating an appQueue to send email to the signer with approvalEsigningUrl. CreatedQueueId:
        ${createdQueue._id}`
      )
    }
  }

  // Set payments or payouts ApprovalESigningURL in partnerPayout collection
  const partnerPayoutUpdatingData = {
    directRemittanceESigningInitiatedAt: new Date(),
    directRemittanceIDFYDocumentId: documentId,
    directRemittanceSigningStatus
  }
  const response = await partnerPayoutService.updateAPartnerPayout(
    { _id: partnerPayoutId },
    { $set: partnerPayoutUpdatingData },
    session
  )
  if (size(response)) {
    console.log(
      '## Updated approvalEsigningUrl in partnerPayout collection. partnerPayoutId:',
      partnerPayoutId,
      'updating data:',
      JSON.stringify(partnerPayoutUpdatingData)
    )
  }
  // Updating lambda appQueue to completed
  return await appQueueService.updateAppQueueToCompleted(queueId, session)
}

const updateContractESigningInitialisationInfo = async (
  queueInfo,
  idfyResponse,
  session
) => {
  const { documentId = '', signers = [] } = idfyResponse

  if (!(documentId && size(signers)))
    throw new CustomError(400, 'Missing required fields in idfyResponse')

  const { queueId, params = {} } = queueInfo
  const { docId: contractId, eSignType, fileType, partnerId } = params
  console.log('params', params)
  checkRequiredFields(['docId', 'fileType', 'partnerId'], params)

  if (
    !(
      fileType === 'esigning_assignment_pdf' ||
      fileType === 'esigning_lease_pdf'
    )
  )
    throw new CustomError(405, 'Wrong fileType found')

  const contract = await contractHelper.getAContract({ _id: contractId })
  if (!size(contract)) throw new CustomError(404, "Contract doesn't exists")

  const { queryData, updatingData } =
    await eSigningHelper.prepareDataForAssignmentOrLeaseSigningStatusInitialization(
      idfyResponse,
      fileType
    )

  // Only update assignment and account status to "in_progress", if assignment status is "new"
  if (fileType === 'esigning_assignment_pdf' && contract.status === 'new') {
    // Updating account status from new to in_progress
    console.log(
      `=== Updating account status to in_progress. accountId: ${contract?.accountId} ===`
    )
    await accountService.updateAnAccount(
      { _id: contract.accountId },
      { $set: { status: 'in_progress' } },
      session
    )
  } else delete updatingData.status
  console.log(queryData, updatingData)
  const updatedContract = await contractService.updateContract(
    queryData,
    updatingData,
    session
  )

  console.log(
    `=== Updated ${eSignType} with IDFY documents creation info. contractId: ${updatedContract._id}`
  )

  console.log(`=== depositType: ${contract?.rentalMeta?.depositType} ===`)

  if (contract?.rentalMeta?.depositType === 'deposit_insurance') {
    const isEnabledDepositInsuranceProcess =
      await depositInsuranceHelper.isEnabledDepositInsuranceProcess(contract)
    console.log(
      `=== isEnabledDepositInsuranceProcess : ${isEnabledDepositInsuranceProcess} ===`
    )
    if (isEnabledDepositInsuranceProcess) {
      console.log('=== DepositInsurance enabled for this lease ===')
      const appQueueData = {
        action: 'init_deposit_insurance_pdf_generation',
        destination: 'lease',
        event: 'handle_deposit_insurance_process',
        params: {
          context: 'deposit_insurance',
          contractId: updatedContract._id,
          idfyDocId: documentId,
          partnerId,
          propertyId: updatedContract.propertyId,
          tenantId: updatedContract.rentalMeta?.tenantId || ''
        },
        priority: 'immediate',
        status: 'new'
      }
      const [createdQueue] = await appQueueService.createAnAppQueue(
        appQueueData,
        session
      )
      if (size(createdQueue))
        console.log(
          `## Creating an appQueue to start process of deposit insurance for lease. CreatedQueueId:
        ${createdQueue._id}`
        )
    }
  } else if (contract?.rentalMeta?.depositType === 'deposit_account') {
    const isEnabledDepositAccountProcess =
      await depositAccountHelper.isEnabledDepositAccountProcess({
        actionType: 'esigning_lease_pdf',
        contractInfoOrId: updatedContract._id,
        partnerInfoOrId: updatedContract.partnerId
      })
    console.log(
      `=== isEnabledDepositAccountProcess : ${isEnabledDepositAccountProcess} ===`
    )
    if (isEnabledDepositAccountProcess) {
      console.log('=== DepositAccount enabled for this lease ===')
      const appQueueData = {
        action: 'init_deposit_account_process',
        destination: 'lease',
        event: 'handle_deposit_account_process',
        params: {
          contractId: updatedContract._id,
          idfyDocId: documentId,
          partnerId
        },
        priority: 'immediate',
        status: 'new'
      }
      const [createdQueue] = await appQueueService.createAnAppQueue(
        appQueueData,
        session
      )
      if (size(createdQueue))
        console.log(
          `## Creating an appQueue to start process of deposit account for lease. CreatedQueueId:
        ${createdQueue._id}`
        )
    }
  } else if (
    contract?.rentalMeta?.depositType === 'no_deposit' &&
    !size(contract?.rentalMeta?.tenantLeaseSigningStatus) &&
    size(updatedContract.rentalMeta?.tenantLeaseSigningStatus)
  ) {
    console.log('=== No Deposit enabled for this lease ===')
    await sendTenantLeaseESigningNotificationAndAddSendESigningTagInContract(
      updatedContract,
      session
    )
    console.log('=== Allowing file deletion permissions ===')
    const fileInfo = await fileService.addORRemoveFileInUseTag(
      { contractId: updatedContract._id, type: 'esigning_lease_pdf' },
      { isFileInUse: false },
      session
    )
    if (size(fileInfo)) {
      console.log(
        `Granted lease file deletion permission for type: 'esigning_lease_pdf'`
      )
    }
  } else if (
    !size(contract.agentAssignmentSigningStatus) &&
    !size(contract.landlordAssignmentSigningStatus) &&
    size(updatedContract.agentAssignmentSigningStatus) &&
    size(updatedContract.landlordAssignmentSigningStatus)
  ) {
    console.log('=== Sending assignment eSigning notification ===')
    const appQueueData = {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_assignment_esigning',
      params: {
        partnerId,
        collectionId: updatedContract._id,
        collectionNameStr: 'contracts'
      },
      priority: 'immediate',
      status: 'new'
    }
    const [createdQueue] = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    if (size(createdQueue))
      console.log(
        `## Creating an appQueue to send emails to the signers with document E-SigningUrl for assignment. CreatedQueueId:
        ${createdQueue._id}`
      )
    console.log('=== Allowing file deletion permissions ===')
    const fileInfo = await fileService.addORRemoveFileInUseTag(
      { contractId: updatedContract._id, type: 'esigning_assignment_pdf' },
      { isFileInUse: false },
      session
    )
    if (size(fileInfo)) {
      console.log(
        `Granted assignment file deletion permission for type: 'esigning_assignment_pdf'`
      )
    }
  }

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  const response = await appQueueService.updateAppQueueToCompleted(
    queueId,
    session
  )

  return !!response
}

const updateMovingESigningInitialisationInfo = async (
  queueInfo,
  idfyResponse,
  session
) => {
  const { documentId = '', signers = [] } = idfyResponse

  if (!(documentId && size(signers)))
    throw new CustomError(400, 'Missing required fields in idfyResponse')

  const { queueId, params = {} } = queueInfo
  const { docId: propertyItemId, eSignType, fileType, partnerId } = params

  checkRequiredFields(['docId', 'fileType', 'partnerId'], params)

  if (
    !(
      fileType === 'esigning_moving_in_pdf' ||
      fileType === 'esigning_moving_out_pdf'
    )
  )
    throw new CustomError(405, 'Wrong fileType found')

  const propertyItem = await propertyItemHelper.getAPropertyItem(
    {
      _id: propertyItemId
    },
    null,
    ['partner']
  )
  if (!size(propertyItem))
    throw new CustomError(
      404,
      `PropertyItem not found. propertyItemId: ${propertyItemId}`
    )

  const { partner } = propertyItem
  if (!size(partner))
    throw new CustomError(
      404,
      `partner not found in this propertyItem. propertyItemId: ${propertyItemId}`
    )
  const { queryData, updatingData } =
    await eSigningHelper.prepareDataForMovingSigningStatusInitialization(
      idfyResponse,
      fileType
    )

  const updatedPropertyItem = await propertyItemService.updateAPropertyItem(
    queryData,
    updatingData,
    session
  )

  if (!size(updatedPropertyItem))
    throw new CustomError(404, 'Unable to update propertyItem')

  console.log(
    `=== Updated ${eSignType} with IDFY documents creation info. propertyItemId: ${updatedPropertyItem._id}`
  )

  const { accountType } = partner

  if (!accountType)
    throw new CustomError(
      404,
      `AccountType not found in partner of this propertyItem. propertyItemId: ${propertyItemId}`
    )

  if (!(accountType === 'broker' || accountType === 'direct'))
    throw new CustomError(
      404,
      `Invalid accountType not found in partner of this propertyItem. propertyItemId: ${propertyItemId}`
    )

  const appQueueData = {
    action: 'send_notification',
    destination: 'notifier',
    params: {
      partnerId,
      collectionId: updatedPropertyItem.contractId,
      collectionNameStr: 'contracts',
      options: { movingId: updatedPropertyItem._id }
    },
    priority: 'immediate',
    status: 'new'
  }
  const { type } = updatedPropertyItem

  if (
    !size(propertyItem.tenantSigningStatus) &&
    size(updatedPropertyItem.tenantSigningStatus)
  ) {
    let event = ''
    if (type === 'in') event = 'send_tenant_moving_in_esigning'
    else if (type === 'out') event = 'send_tenant_moving_out_esigning'
    appQueueData.event = event

    const [createdQueue] = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    if (size(createdQueue))
      console.log(
        `## Creating an appQueue to send emails to the tenants with document E-SigningUrl for moving_${type}. CreatedQueueId:
        ${createdQueue._id}`
      )
  }
  if (
    accountType === 'broker' &&
    !size(propertyItem.agentSigningStatus) &&
    size(updatedPropertyItem.agentSigningStatus)
  ) {
    let event = ''
    if (type === 'in') event = 'send_agent_moving_in_esigning'
    else if (type === 'out') event = 'send_agent_moving_out_esigning'
    appQueueData.event = event

    const [createdQueue] = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    if (size(createdQueue))
      console.log(
        `## Creating an appQueue to send emails to the agent with document E-SigningUrl for moving_${type}. CreatedQueueId:
        ${createdQueue._id}`
      )
  } else if (
    accountType === 'direct' &&
    !size(propertyItem.landlordSigningStatus) &&
    size(updatedPropertyItem.landlordSigningStatus)
  ) {
    let event = ''
    if (type === 'in') event = 'send_landlord_moving_in_esigning'
    else if (type === 'out') event = 'send_landlord_moving_out_esigning'
    appQueueData.event = event

    const [createdQueue] = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    if (size(createdQueue))
      console.log(
        `## Creating an appQueue to send emails to the landlord with document E-SigningUrl for moving_${type}. CreatedQueueId:
        ${createdQueue._id}`
      )
  }

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  const response = await appQueueService.updateAppQueueToCompleted(
    queueId,
    session
  )

  return !!response
}

const updateDepositAccountESigningInitialisationInfo = async (
  queueInfo,
  idfyResponse,
  session
) => {
  const { queueId, params } = queueInfo
  const { docId, tenantId } = params

  if (!(queueId && size(params)))
    throw new CustomError(400, 'Missing required params in appQueue')
  if (!size(idfyResponse))
    throw new CustomError(
      400,
      'Missing idfyResponse data in deposit account eSigning initialisation process'
    )

  // Todo :DA:
  //  1. Add idfyAttachmentId in lease.  updateTenantLeaseSigningStatus(Meteor)
  //  After update check => send tenant notification if all tenantLeaseSigningStatus has idfyAttachmentId
  console.log('Params are', params)
  const contractUpdateData = await contractService.updateContract(
    {
      _id: docId,
      'rentalMeta.tenantLeaseSigningStatus.tenantId': tenantId
    },
    {
      $set: {
        'rentalMeta.tenantLeaseSigningStatus.$.idfyAttachmentId':
          idfyResponse.id
      }
    },
    session
  )
  console.log('Update contract data is', contractUpdateData)
  await sendEsignNotification(contractUpdateData, session)
  //TODO send e signing notification

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  const response = await appQueueService.updateAppQueueToCompleted(
    queueId,
    session
  )

  return !!response
}

const sendEsignNotification = async (contract, session) => {
  const rentalMeta = contract.rentalMeta
  const enabledJointlyLiable = rentalMeta?.enabledJointlyLiable
  const tenantsListLeaseSigning = rentalMeta?.tenantLeaseSigningStatus
  const isJointDepositAccount =
    rentalMeta && rentalMeta.enabledJointDepositAccount
  const tenantsIds =
    enabledJointlyLiable && !isJointDepositAccount
      ? map(tenantsListLeaseSigning, 'tenantId')
      : [rentalMeta.tenantId]
  console.log('Tenant ids are', tenantsIds)
  const isNotAttachedIdfyFile = find(
    tenantsListLeaseSigning,
    (tenantInfo) =>
      indexOf(tenantsIds, tenantInfo.tenantId) !== -1 &&
      !tenantInfo.idfyAttachmentId
  )
  console.log('isNotAttachedIdfyFile', isNotAttachedIdfyFile)
  const enabledSendEsignNotification = !!(
    (await isEnabledDepositAccountProcess({
      partnerId: contract.partnerId,
      contractRentalMeta: contract.rentalMeta,
      actionType: 'esigning_lease_pdf'
    })) && !rentalMeta.isSendEsignNotify
  )
  console.log(
    'Is enable send deposit account process',
    enabledSendEsignNotification
  )
  if (enabledSendEsignNotification && !size(isNotAttachedIdfyFile)) {
    const appQueue = await appQueueService.createAnAppQueue(
      {
        destination: 'notifier',
        action: 'send_notification',
        event: 'send_tenant_lease_esigning',
        params: {
          partnerId: contract.partnerId,
          collectionId: contract._id,
          collectionNameStr: 'contracts'
        },
        priority: 'immediate'
      },
      session
    )
    console.log('App queue created', appQueue)
    const now = new Date()
    console.log(now)
    const contractUpdated = await contractService.updateContractWithUpdateOne(
      { _id: contract._id },
      {
        $set: { 'rentalMeta.isSendEsignNotify': true }
      },
      session
    )
    console.log(
      `Contract updated for contractId ${contract._id} ${contractUpdated}`
    )
  }
}

const isEnabledDepositAccountProcess = async (params, session) => {
  const {
    partnerId,
    actionType,
    isDepositAmountPaid,
    hasRentalContract,
    contractRentalMeta
  } = params
  const partnerInfo =
    (await partnerHelper.getAPartner({ _id: partnerId }, session)) || {}
  console.log('Partner found', partnerInfo)
  const enableDepositAccountOfPartner =
    partnerInfo?.enableDepositAccount || false
  const isSignatureMechanismByBank =
    contractRentalMeta?.leaseSignatureMechanism === 'bank_id'
  const enableDepositAccountOfContract =
    contractRentalMeta?.enabledDepositAccount || false
  const isLandlordSigned =
    contractRentalMeta?.landlordLeaseSigningStatus?.signed || false
  const isDepositAccountActivated =
    contractRentalMeta.depositType === 'deposit_account'

  const isEnabledDepositProcess = !!(
    enableDepositAccountOfPartner &&
    isSignatureMechanismByBank &&
    enableDepositAccountOfContract &&
    isDepositAccountActivated
  )
  console.log('isEnabledDepositProcess', isEnabledDepositProcess)

  if (
    actionType &&
    actionType === 'esigning_lease_pdf' &&
    isEnabledDepositProcess
  )
    return true
  else if (
    actionType &&
    actionType === 'active' &&
    isLandlordSigned &&
    hasRentalContract &&
    isEnabledDepositProcess &&
    !isDepositAmountPaid
  )
    return true
}
const addIDFYAttachmentIdAndSendNotificationToTenants = async (
  queueInfo,
  idfyResponse,
  session
) => {
  const { queueId, params } = queueInfo

  if (!(queueId && size(params)))
    throw new CustomError(400, 'Missing required params in appQueue')
  if (!size(idfyResponse))
    throw new CustomError(
      400,
      'Missing idfyResponse data in deposit account eSigning initialisation process'
    )

  appHelper.checkRequiredFields(
    ['docId', 'eSignType', 'fileType', 'idfyRes', 'partnerId'],
    params
  )

  const { docId, eSignType, fileType, idfyRes, partnerId } = params

  if (!(docId && eSignType && fileType && size(idfyRes) && partnerId))
    throw new CustomError(404, 'Missing required parameter in queue params')

  const idfyAttachmentId = idfyRes.id
  if (!idfyAttachmentId)
    throw new CustomError(404, 'Missing idfyAttachmentId in idfyRes')

  // Adding idfyAttachmentId in each tenantLeaseSigningStatus object
  const updatedContract = await contractService.updateContract(
    { _id: docId },
    {
      $set: {
        'rentalMeta.tenantLeaseSigningStatus.$[].idfyAttachmentId':
          idfyAttachmentId
      }
    },
    session
  )
  if (!size(updatedContract))
    throw new CustomError(
      404,
      'Unable to add idfyAttachmentId in each tenantLeaseSigningStatus object'
    )

  console.log(
    `Added idfyAttachmentId in each tenantLeaseSigningStatus object. contractId: ${updatedContract._id}`
  )
  //  After update check => send tenant notification if all tenantLeaseSigningStatus has idfyAttachmentId
  const tenantLeaseSigningStatus =
    updatedContract.rentalMeta?.tenantLeaseSigningStatus || []
  const isNotAttachedIdfyIdExists = find(
    tenantLeaseSigningStatus,
    (tenantInfo) => !tenantInfo?.idfyAttachmentId
  )
  const isSendEsignNotify = updatedContract.rentalMeta?.isSendEsignNotify
  if (!(size(isNotAttachedIdfyIdExists) && isSendEsignNotify)) {
    // If all tenantLeaseSigningStatus has 'idfyAttachmentId' and 'isSendEsignNotify' is false. then we will send tenants signing email
    const isDepositInsuranceProcessEnabled =
      await depositInsuranceHelper.isEnabledDepositInsuranceProcess(
        updatedContract.toObject()
      )
    console.log(
      `=== isDepositInsuranceProcessEnabled: ${isDepositInsuranceProcessEnabled} ===`
    )
    if (isDepositInsuranceProcessEnabled) {
      await sendTenantLeaseESigningNotificationAndAddSendESigningTagInContract(
        updatedContract.toObject(),
        session
      )
    }
    // Removing fileInUse tag from the files
    await fileService.addORRemoveFileInUseTag(
      {
        contractId: updatedContract._id,
        type: {
          $in: ['esigning_lease_pdf', 'esigning_deposit_insurance_pdf']
        },
        isFileInUse: true
      },
      { $set: { isFileInUse: false } },
      session
    )
  }
  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  const response = await appQueueService.updateAppQueueToCompleted(
    queueId,
    session
  )

  return !!response
}

export const updateESigningDocumentInfo = async (req) => {
  const { body, session } = req

  eSigningHelper.checkRequiredDataBeforeStartESigningProgress(body)

  const { idfyResData, queueId } = body

  const queueInfo = await appQueueHelper.getAQueueItem({ _id: queueId })
  if (!size(queueInfo)) throw new CustomError(404, "AppQueue doesn't exists")

  const { params = {} } = queueInfo
  if (!size(params))
    throw new CustomError(404, "AppQueue params doesn't exists")

  const { eSignType = '' } = params

  let response = false
  if (eSignType === 'payout' || eSignType === 'payment') {
    response = await sendDirectRemittanceApprovalESigningNotification(
      { queueId, params },
      idfyResData,
      session
    )
  } else if (eSignType === 'assignment' || eSignType === 'lease') {
    response = await updateContractESigningInitialisationInfo(
      { queueId, params },
      idfyResData,
      session
    )
  } else if (eSignType === 'moving_in' || eSignType === 'moving_out') {
    response = await updateMovingESigningInitialisationInfo(
      { queueId, params },
      idfyResData,
      session
    )
  } else if (eSignType === 'deposit_account') {
    response = await updateDepositAccountESigningInitialisationInfo(
      { queueId, params },
      idfyResData,
      session
    )
  } else if (eSignType === 'deposit_insurance') {
    response = await addIDFYAttachmentIdAndSendNotificationToTenants(
      { queueId, params },
      idfyResData,
      session
    )
  } else throw new CustomError(400, 'Invalid eSignType found')
  // Todo :Will be implement later: 4 eSignType are pending
  //  Ref :Meteor: export const updateESigningInitialisationInfo = function (params, req, res, next)

  return { result: response }
}

const prepareLeaseStatusDataAndCreateQToCreateInvoice = async (
  contractId,
  session
) => {
  const contract = await contractHelper.getAContract({ _id: contractId })
  if (!size(contract)) throw new CustomError(404, 'Contract not found')

  const { partnerId, propertyId, rentalMeta, status: contractStatus } = contract

  if (!partnerId)
    throw new CustomError(404, 'PartnerId not found in the contract')
  if (!size(rentalMeta))
    throw new CustomError(404, 'RentalMeta not found in the contract')

  const {
    contractStartDate,
    enabledJointlyLiable,
    enabledJointDepositAccount,
    status: rentalStatus
  } = rentalMeta

  let initAfterUpdateHook = false

  if (enabledJointDepositAccount && enabledJointlyLiable)
    initAfterUpdateHook = true

  const updatingData = { draftLeaseDoc: false } // Don't show document preparing message

  updatingData['rentalMeta.hasLeasePadesFile'] = true
  updatingData['rentalMeta.leasePadesFileCreatedAt'] = new Date()

  // Update contract status after PAdES completed
  if (rentalStatus !== 'closed') {
    const todayDate = (
      await appHelper.getActualDate(partnerId, true, new Date())
    )
      .endOf('day')
      .toDate()

    const startDate = contractStartDate
      ? (
          await appHelper.getActualDate(partnerId, true, contractStartDate)
        ).toDate()
      : ''
    console.log(
      'Update contract status after PAdES completed',
      contractStartDate
    )
    let updatedContract = {}
    console.log('Started contract status update', rentalStatus)
    if (startDate && startDate > todayDate) {
      if (rentalStatus !== 'upcoming') {
        updatedContract =
          await contractService.updateContractStatusAndCreateRentInvoice(
            { partnerId, propertyId, contractId, status: 'upcoming' },
            session
          )
      }
    } else if (startDate && startDate <= todayDate) {
      if (rentalStatus !== 'active' && contractStatus !== 'active') {
        updatedContract =
          await contractService.updateContractStatusAndCreateRentInvoice(
            { partnerId, propertyId, contractId, status: 'active' },
            session
          )
      }
    }
    if (size(updatedContract)) {
      await contractService.contractStatusUpdateAfterHooksProcess(
        { previousContract: contract, updatedContract },
        session
      )
    }

    const depositType = rentalMeta.depositType
    console.log(`=== DepositType: ${depositType}===`)
    // Todo :DA: will Update later
    // if depositType === 'deposit_account' then create a queue to start process called submit_for_creating_deposit_account
    const contractRentalMeta = contract?.rentalMeta
    let tenantIds = []

    if (
      contractRentalMeta.enabledJointlyLiable &&
      !contractRentalMeta.enabledJointDepositAccount
    ) {
      tenantIds = map(contractRentalMeta['tenants'], 'tenantId') || []
    } else {
      tenantIds = [contractRentalMeta.tenantId] || []
    }

    if (depositType === 'deposit_account') {
      const singleAppQueueData = {
        event: 'handle_deposit_account_process',
        action: 'init_submit_for_creating_deposit_account',
        params: {
          contractId,
          partnerId,
          tenantIds
        },
        destination: 'lease',
        priority: 'regular'
      }
      await appQueueService.createAnAppQueue(singleAppQueueData)
    }
    /*
      Please create queue with all bank data. so that we can process those in lease lambda
      RF. submitForCreatingDepositAccount(meteor)
      queue {event: handle_deposit_account_process, action: 'create_deposit_account, isSequential: true, sequentialCategory: `create_deposit_account${contractId}`  }
     */

    // if depositType === 'deposit_insurance' then create a queue to start process called create_deposit_insurance_data (meteor)
    if (depositType === 'deposit_insurance') {
      const queueData = {
        event: 'add_deposit_insurance_data',
        action: 'add_deposit_insurance_data',
        params: {
          contractId,
          partnerId
        },
        destination: 'deposit-insurance',
        priority: 'immediate'
      }
      const [queue] = await appQueueService.createAnAppQueue(queueData, session)
      console.log(
        `=== Created queue to start deposit insurance process. queueId: ${queue._id} ===`
      )
    }
  }

  return { contract, updatingData, initAfterUpdateHook }
}

export const updateLeaseStatusAndCreateInvoice = async (req) => {
  const { body, session } = req

  appHelper.checkRequiredFields(['contractId', 'queueId'], body)
  const { contractId = '', queueId = '' } = body || {}
  if (!(contractId && queueId))
    throw new CustomError(400, 'Missing required data')

  const { updatingData } =
    await prepareLeaseStatusDataAndCreateQToCreateInvoice(contractId, session)

  if (!size(updatingData)) throw new CustomError(400, 'UpdatingData not found')

  // Update contract
  console.log(
    `=== Updating contract with updatingData: ${updatingData}, contractId: ${contractId} ===`
  )
  const updatedContract = await contractService.updateContract(
    { _id: contractId },
    updatingData,
    session
  )

  if (!size(updatedContract))
    throw new CustomError(404, 'Unable to update contract')

  console.log(`Updated contractInfo. contractId: ${updatedContract._id}`)

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  const response = await appQueueService.updateAppQueueToCompleted(
    queueId,
    session
  )

  return { result: !!response }
}

const prepareDepositInsuranceIDFYAttachmentCreationData = async (
  signers,
  userLang = 'no'
) => {
  if (!(size(signers) && userLang))
    throw new CustomError(404, 'Signers info missing in params')

  const dataForIdfy = {
    title: appHelper.translateToUserLng(
      `deposit_insurances.contract_insurance_signing`,
      userLang
    ),
    fileName: 'esigning_contract_insurance.pdf',
    description: appHelper.translateToUserLng(
      `deposit_insurances.contract_insurance_description`,
      userLang
    ),
    signers,
    convertToPdf: true,
    type: 'sign'
  }

  return dataForIdfy
}

export const addDIAttachmentIdAndIDFYCreationProcess = async (req) => {
  const { body, session } = req

  appHelper.checkRequiredFields(
    [
      'contractId',
      'fileId',
      'fileKey',
      'fileType',
      'idfyDocId',
      'partnerId',
      'queueId'
    ],
    body
  )
  const {
    contractId,
    fileId,
    fileKey,
    fileType,
    idfyDocId,
    partnerId,
    queueId
  } = body || {}
  if (
    !(
      contractId &&
      fileId &&
      fileKey &&
      fileType &&
      idfyDocId &&
      partnerId &&
      queueId
    )
  )
    throw new CustomError(400, 'Missing required data')

  const contract = await contractHelper.getAContract(
    { _id: contractId },
    null,
    [{ path: 'partner', populate: ['owner'] }]
  )

  if (!size(contract)) throw new CustomError(404, 'Contract not found')

  if (!size(contract.partner)) throw new CustomError(404, 'Partner not found')

  const userLang = contract.partner?.owner?.profile?.language || 'no'

  const tenantLeaseSigningStatus =
    contract.rentalMeta?.tenantLeaseSigningStatus || []
  const tenantsSignerInfo = size(tenantLeaseSigningStatus)
    ? map(tenantLeaseSigningStatus, 'idfySignerId')
    : []

  if (!size(tenantsSignerInfo))
    throw new CustomError(404, 'TenantsSignerInfo not found in contract')

  // Adding attachmentFileId in each tenantLeaseSigningStatus object
  const updatedContract = await contractService.updateContract(
    { _id: contractId },
    {
      $set: {
        'rentalMeta.tenantLeaseSigningStatus.$[].attachmentFileId': fileId
      }
    },
    session
  )

  if (!size(updatedContract))
    throw new CustomError(
      404,
      'Unable to add attachmentFileId in each tenantLeaseSigningStatus object'
    )

  console.log(
    `Added attachmentFileId in each tenantLeaseSigningStatus object. contractId: ${updatedContract._id}`
  )

  // Preparing data and send DI attachment creation request to IDFY (eSigner)

  const dataForIdfy = await prepareDepositInsuranceIDFYAttachmentCreationData(
    tenantsSignerInfo,
    userLang
  )

  const appQueueData = {
    action: 'handle_e_signing',
    destination: 'esigner',
    event: 'create_document',
    params: {
      callBackParams: {
        callBackAction: 'deposit_insurance_e_signing_initialisation_process',
        callBackDestination: 'lease',
        callBackEvent: 'deposit_insurance_e_signing_initialisation_process'
      },
      dataForIdfy,
      docId: contractId,
      documentId: idfyDocId,
      eSignType: 'deposit_insurance',
      fileKey,
      fileType,
      partnerId,
      processType: 'create_document',
      tenantId: contract.rentalMeta?.tenantId || ''
    },
    priority: 'immediate'
  }

  const [appQueueInfo] = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )

  console.log(
    `=== Created appQueue to send DI attachment creation request to IDFY. queueId: ${appQueueInfo._id} ===`
  )

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  await appQueueService.updateAppQueueToCompleted(queueId, session)

  return { queueId: appQueueInfo._id }
}

export const createQueuesToUploadDISignedFileFromIDFY = async (req) => {
  const { body, session } = req

  appHelper.checkRequiredFields(
    ['callBackParams', 'contractId', 'documentId', 'partnerId', 'queueId'],
    body
  )
  const { callBackParams, contractId, documentId, partnerId, queueId } =
    body || {}
  if (
    !(size(callBackParams) && contractId && documentId && partnerId && queueId)
  )
    throw new CustomError(400, 'Missing required data in req body')

  const contract = await contractHelper.getAContract({ _id: contractId })

  if (!size(contract)) throw new CustomError(404, 'Contract not found')

  const tenantLeaseSigningStatus =
    contract.rentalMeta?.tenantLeaseSigningStatus || []

  if (!size(tenantLeaseSigningStatus))
    throw new CustomError(404, 'TenantsSignerInfo not found in contract')

  const queueIds = []

  for (const signer of tenantLeaseSigningStatus) {
    if (signer.signed && signer.idfyAttachmentId && signer.attachmentFileId) {
      const {
        idfyAttachmentId: attachmentId,
        attachmentFileId,
        tenantId
      } = signer

      const fileQuery = {
        _id: attachmentFileId,
        contractId,
        partnerId,
        type: 'esigning_deposit_insurance_pdf'
      }

      const file = await fileHelper.getAFile(fileQuery)
      const fileTitle = file?.title || ''
      console.log(`=== fileTitle: ${fileTitle} ===`)

      const paramsAndOptions = fileHelper.getFileUploadParamsAndOptions({
        directive: 'Files',
        existingFileName: fileTitle,
        partnerId,
        type: 'esigning_deposit_insurance'
      })
      const appQueueData = {
        action: 'handle_e_signing',
        destination: 'esigner',
        event: 'fetch_or_upload_document',
        params: {
          attachmentFileId,
          attachmentId,
          callBackParams,
          contractId,
          documentId,
          eSignType: 'esigning_deposit_insurance_pdf',
          paramsAndOptions,
          partnerId,
          processType: 'fetch_or_upload_document',
          propertyId: contract.propertyId,
          subProcessType: 'uploadIdfySignedFileToS3',
          tenantId,
          actions: ['status', 'file_upload']
        },
        priority: 'immediate'
      }

      const [appQueueInfo] = await appQueueService.createAnAppQueue(
        appQueueData,
        session
      )

      console.log(
        `=== Created appQueue to send DI attachment creation request to IDFY. queueId: ${appQueueInfo._id} ===`
      )
      queueIds.push(appQueueInfo._id)
    }
  }

  // Updating lambda appQueue to completed
  console.log(`=== Updating AppQueue to completed ===`)
  await appQueueService.updateAppQueueToCompleted(queueId, session)

  return { queueIds }
}
