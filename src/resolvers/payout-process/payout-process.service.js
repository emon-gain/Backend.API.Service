import { each, size } from 'lodash'
import nid from 'nid'

import { CustomError } from '../common'

import { PayoutProcessCollection } from '../models'

import {
  appHelper,
  invoicePaymentHelper,
  partnerPayoutHelper,
  partnerSettingHelper,
  payoutHelper,
  payoutProcessHelper,
  settingHelper
} from '../helpers'
import {
  appQueueService,
  invoicePaymentService,
  partnerPayoutService,
  payoutProcessService,
  payoutService
} from '../services'

const getRejectOrBookedInfo = function (creditTransferData) {
  let totalReject = 0,
    totalBooked = 0,
    totalTransfer = 0

  each(creditTransferData, (creditTransfer) => {
    if (creditTransfer.status === 'booked') totalBooked++
    else if (creditTransfer.status === 'RJCT') totalReject++
  })

  totalTransfer = totalReject + totalBooked

  return { totalReject, totalBooked, totalTransfer }
}

const prepareUpdateDataForStatusUpdate = (
  payoutProcess,
  newRejectOrBookedInfo = {}
) => {
  let status = null,
    eventStatus = null
  const updateData = {}

  const { status: currentStatus, creditTransferInfo } = payoutProcess
  const totalCreditTransfer = size(creditTransferInfo)

  if (currentStatus === 'ACCP') {
    status = 'accepted'
    eventStatus = 'nets_accepted'
  } else if (currentStatus === 'RJCT') {
    status = 'error'
    eventStatus = 'nets_rejected'
  } else if (currentStatus === 'PART') {
    status = 'partially_completed'
    eventStatus = 'nets_partially_accepted'
  } else if (currentStatus === 'ACTC') {
    status = 'validated'
    eventStatus = 'nets_received'
  } else if (
    currentStatus === 'completed' &&
    totalCreditTransfer === newRejectOrBookedInfo.totalTransfer
  ) {
    if (
      newRejectOrBookedInfo.totalTransfer === newRejectOrBookedInfo.totalBooked
    )
      status = 'completed'
    else if (
      newRejectOrBookedInfo.totalTransfer === newRejectOrBookedInfo.totalReject
    )
      status = 'failed'
    else if (
      newRejectOrBookedInfo.totalBooked &&
      newRejectOrBookedInfo.totalReject
    )
      status = 'partially_completed'
  }
  if (currentStatus === 'ASICE_OK') {
    status = 'asice_approved'
    eventStatus = 'asice_approved'
  }

  if (status) {
    console.log(`-- Payout process status updated to ${status}`)
    updateData['$set'] = { status }
  }

  if (eventStatus) {
    updateData['$push'] = {
      events: { status: eventStatus, createdAt: new Date() }
    }
  }
  return updateData
}

const updatePayoutProcessInfoForCreditTransfer = async (
  { doc = {}, newRejectOrBookedInfo = {} },
  session
) => {
  let status = null,
    bookedAt = ''

  const totalCreditTransfer = size(doc.creditTransferInfo)
  if (totalCreditTransfer === newRejectOrBookedInfo.totalTransfer) {
    status = 'completed'
    bookedAt =
      newRejectOrBookedInfo.totalTransfer === newRejectOrBookedInfo.totalBooked
        ? new Date()
        : ''
  } else if (
    newRejectOrBookedInfo.totalTransfer &&
    totalCreditTransfer !== newRejectOrBookedInfo.totalTransfer
  )
    status = 'PART'
  if (status) {
    const payoutProcessUpdateData = { status }

    if (bookedAt) payoutProcessUpdateData.bookedAt = bookedAt

    const query = {
      _id: doc._id,
      partnerId: doc.partnerId
    }
    const updateData = { $set: payoutProcessUpdateData }
    console.log(`-- Updating payout process status to ${status}`)
    return await updateAPayoutProcess(query, updateData, session)
  }
}

const prepareAndUpdatePartnerPayoutForPPStatus = async (
  { doc = {}, previous = {}, newRejectOrBookedInfo = {} },
  session
) => {
  if (!(size(doc) && size(previous))) return false

  const { partnerPayoutId, status: currentStatus = '' } = doc || {}
  const { status: prevStatus = '' } = previous || {}
  console.log(
    '====> Checking partner payout updating params:',
    JSON.stringify({ currentStatus, newRejectOrBookedInfo, prevStatus }),
    '<===='
  )
  if (partnerPayoutId && currentStatus !== prevStatus) {
    const updateData = prepareUpdateDataForStatusUpdate(
      doc,
      newRejectOrBookedInfo
    )
    console.log(
      '====> Checking partner payout updating data:',
      JSON.stringify(updateData || {}),
      '<===='
    )
    await partnerPayoutService.updateAPartnerPayout(
      { _id: partnerPayoutId },
      updateData,
      session
    )
    console.log(`-- Updated partner payout status to ${updateData.$set.status}`)
  }
}

const checkCreditTransferDataAndUpdatePPStatus = async (
  {
    doc = {},
    newRejectOrBookedInfo = {},
    oldRejectOrBookedInfo = {},
    previous = {}
  },
  session
) => {
  if (!(size(doc) && size(previous))) return false
  console.log(
    '====> Checking reject or booked info of payout process:',
    { newRejectOrBookedInfo, oldRejectOrBookedInfo },
    '<===='
  )
  if (
    newRejectOrBookedInfo.totalTransfer &&
    oldRejectOrBookedInfo.totalTransfer !== newRejectOrBookedInfo.totalTransfer
  ) {
    const statusUpdatedPayoutProcess =
      await updatePayoutProcessInfoForCreditTransfer(
        { doc, newRejectOrBookedInfo },
        session
      )
    if (size(statusUpdatedPayoutProcess))
      await prepareAndUpdatePartnerPayoutForPPStatus(
        {
          doc: statusUpdatedPayoutProcess,
          previous,
          newRejectOrBookedInfo
        },
        session
      )
  }
}

export const updateAPayoutProcess = async (query, data, session) => {
  const payoutProcess = await PayoutProcessCollection.findOneAndUpdate(
    query,
    data,
    {
      new: true,
      runValidators: true,
      session
    }
  )
  return payoutProcess
}

export const updateAPayoutProcessWithAfterUpdate = async (
  query,
  data,
  session
) => {
  const updatedPayoutProcess = await updateAPayoutProcess(query, data, session)
  if (!updatedPayoutProcess?._id)
    throw new CustomError(404, 'Could not update payout process')

  const {
    creditTransferInfo = [],
    payoutIds = [],
    paymentIds = []
  } = updatedPayoutProcess || {}
  const prevPayoutProcess =
    (await payoutProcessHelper.getPayoutProcess({
      _id: updatedPayoutProcess._id
    })) || {}
  const newRejectOrBookedInfo = getRejectOrBookedInfo(
    updatedPayoutProcess?.creditTransferInfo || []
  )
  const oldRejectOrBookedInfo = getRejectOrBookedInfo(
    prevPayoutProcess?.creditTransferInfo || []
  )
  if (prevPayoutProcess?.status !== updatedPayoutProcess?.status) {
    await prepareAndUpdatePartnerPayoutForPPStatus(
      {
        doc: updatedPayoutProcess,
        previous: prevPayoutProcess,
        newRejectOrBookedInfo
      },
      session
    )
  }
  if (size(creditTransferInfo) && data?.['$set']?.creditTransferInfo) {
    console.log(
      '====> Credit transfer data updated so update payout process status now <===='
    )
    await checkCreditTransferDataAndUpdatePPStatus(
      {
        doc: updatedPayoutProcess,
        newRejectOrBookedInfo,
        oldRejectOrBookedInfo,
        previous: prevPayoutProcess
      },
      session
    )
    if (size(payoutIds)) {
      await payoutService.updatePayoutsStatusByCreditTransferData(
        creditTransferInfo,
        session
      )
    }
    if (size(paymentIds)) {
      await invoicePaymentService.updateRefundPaymentStatusByCreditTransferData(
        creditTransferInfo,
        session
      )
    }
  }

  return updatedPayoutProcess
}

//For Payments Lambda #10482
export const updatePayoutProcess = async (req) => {
  const { body, user, session } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['payoutProcessId', 'partnerId'], body)
  const query = payoutProcessHelper.prepareQueryToUpdatePayoutProcess(body)
  const payoutProcessInfo = await payoutProcessHelper.getPayoutProcess(query)
  if (!size(payoutProcessInfo))
    throw new CustomError(404, 'Could not find payout process info')
  const updateData = payoutProcessHelper.prepareDataToUpdatePayoutProcess(
    body,
    payoutProcessInfo
  )
  console.log(
    '====> Checking payout process updating data:',
    JSON.stringify(updateData || {}),
    '<===='
  )
  const payoutProcess = await updateAPayoutProcessWithAfterUpdate(
    query,
    updateData,
    session
  )
  if (!size(payoutProcess)) {
    throw new CustomError(400, 'Payout process not updated')
  }

  console.log(
    '-- Feedback status log, sent file name, file status updated on payout process'
  )

  return payoutProcess
}

const preparePayoutProcessCreatingParams = async (params, session) => {
  const { partnerId, partnerPayout, partnerPayoutId } = params
  if (!(partnerId && size(partnerPayout) && partnerPayoutId)) {
    throw new CustomError(405, 'Missing required data')
  }

  const { payoutIds, paymentIds: refundPaymentIds, type } = partnerPayout || {}

  const creditTransferPreparingMethod = {
    payout: payoutHelper.getPayoutsCreditTransferData,
    refund_payment: invoicePaymentHelper.getCreditTransferInfoForRefundPayment
  }
  const collectionIds = type === 'refund_payment' ? refundPaymentIds : payoutIds
  const creditTransferData = await creditTransferPreparingMethod[type](
    collectionIds
  )

  if (!size(creditTransferData)) {
    const payoutParams = {
      partnerPayoutId,
      status: 'failed',
      eventStatus: 'failed',
      eventNote: 'Something went wrong when preparing the credit transfer info.'
    }
    await payoutService.updatePartnerPayout(payoutParams, session)
    return false
  }

  const payoutProcessCreatingParams = {
    partnerId,
    creditTransferData,
    partnerPayoutId
  }

  if (type === 'payout') payoutProcessCreatingParams.payoutIds = payoutIds
  if (type === 'refund_payment') {
    const settings = await partnerSettingHelper.getAPartnerSetting(
      { partnerId },
      session
    )
    payoutProcessCreatingParams.refundPaymentIds = refundPaymentIds
    payoutProcessCreatingParams.settings = settings
  }

  return payoutProcessCreatingParams
}

export const updatePayoutAndPayoutProcess = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(
    [
      'directRemittanceSigningMeta',
      'directRemittanceSigningStatus',
      'partnerId',
      'partnerPayoutId'
    ],
    body
  )
  const {
    directRemittanceSigningMeta,
    directRemittanceSigningStatus,
    partnerPayoutId,
    partnerId
  } = body
  if (
    !(
      size(directRemittanceSigningStatus) &&
      size(directRemittanceSigningMeta?.signers) &&
      partnerPayoutId &&
      partnerId
    )
  ) {
    throw new CustomError(400, 'Invalid input data!')
  }

  const partnerPayout = await partnerPayoutHelper.getAPartnerPayout(
    { _id: partnerPayoutId, partnerId },
    session
  )
  const { paymentIds: refundPaymentIds, payoutIds, type } = partnerPayout || {}

  const partnerPayoutUpdatingData = {
    $push: {
      events: {
        status: 'approved',
        createdAt: new Date(),
        note: 'Payouts process will be started now'
      }
    },
    $set: {
      directRemittanceSigningMeta,
      directRemittanceSigningStatus,
      status: 'approved'
    }
  }
  await partnerPayoutService.updateAPartnerPayout(
    { _id: partnerPayoutId },
    partnerPayoutUpdatingData,
    session
  )

  console.log('-- Updated partner payout status to approved')

  if (type === 'payout' && size(payoutIds)) {
    const { nModified: payoutUpdatedCount } =
      (await payoutService.updatePayouts(
        {
          _id: { $in: payoutIds },
          status: 'waiting_for_signature'
        },
        { $set: { status: 'approved' } },
        session
      )) || {}
    if (payoutUpdatedCount !== size(payoutIds))
      throw new CustomError(404, 'Could not update approved payouts')
    console.log(
      '-- Updated payouts status from waiting for signature to approved'
    )
  }
  if (type === 'refund_payment' && size(refundPaymentIds)) {
    const { nModified: paymentUpdatedCount } =
      (await invoicePaymentService.updateInvoicePayments(
        {
          _id: { $in: refundPaymentIds },
          refundStatus: 'waiting_for_signature'
        },
        { $set: { refundStatus: 'approved' } },
        session
      )) || {}
    if (paymentUpdatedCount !== size(refundPaymentIds))
      throw new CustomError(404, 'Could not update refund payments')
    console.log(
      '-- Updated refund payments status from waiting for signature to approved'
    )
  }
  const params = {
    partnerId,
    partnerPayout,
    partnerPayoutId
  }
  const payoutProcessCreatingParams =
    (await preparePayoutProcessCreatingParams(params, session)) || {}
  if (!size(payoutProcessCreatingParams))
    throw new CustomError('Could not prepare payout process creating params')

  await payoutService.updatePartnerPayout(
    {
      partnerPayoutId,
      status: 'processing',
      eventStatus: 'ready',
      eventNote: `Ready to create payout process data for ${
        type === 'payout' ? 'payout' : 'refund payment'
      }`
    },
    session
  )

  return await payoutProcessService.createPayoutProcess(
    payoutProcessCreatingParams,
    session
  )
}

//TODO: remove when final implementation is done
// const addAppQueueForGenerateSignature = async (params) => {
//   const {
//     payoutProcessId,
//     payoutProcessData,
//     partnerId,
//     partnerSetting,
//     partnerPayoutId
//   } = params
//
//   const payoutFileName = 'ISO.PAIN001.' + payoutProcessId + '.xml'
//   const approvalFileName = 'ApprovalData.' + payoutProcessId + '.xml'
//   const payoutXmlFileObject = InvoicesHelpers.prepareXmlFileData(
//     payoutProcessData,
//     partnerSetting
//   )
//   const approvalXmlFileData = InvoicesHelpers.prepareApprovalXmlFileData(
//     partnerId,
//     payoutProcessData,
//     payoutFileName,
//     partnerPayoutId
//   )
//   const asicManifestData = InvoicesHelpers.prepareAsicManifestXmlFileData(
//     payoutXmlFileObject,
//     approvalXmlFileData,
//     payoutFileName,
//     approvalFileName
//   )
//   const draftPayoutFilesUploaded = uploadDraftPayoutFiles(
//     partnerId,
//     payoutProcessId,
//     partnerPayoutId,
//     payoutFileName,
//     approvalFileName,
//     payoutXmlFileObject,
//     approvalXmlFileData,
//     asicManifestData
//   )
//
//   if (
//     draftPayoutFilesUploaded &&
//     draftPayoutFilesUploaded.success &&
//     draftPayoutFilesUploaded.subFolder &&
//     draftPayoutFilesUploaded.fileKeys
//   ) {
//     const params = {
//       partnerId,
//       subFolder: draftPayoutFilesUploaded.subFolder,
//       fileKeys: draftPayoutFilesUploaded.fileKeys,
//       payoutFileName,
//       approvalFileName,
//       payoutProcessId,
//       partnerPayoutId,
//       processType: 'payout_approval_signature',
//       processMessage
//     }
//
//     const sqsId = LambdaSqsHelpers.createSqsMessage(
//       params,
//       'generate_signature'
//     )
//
//     log.debug(
//       `Lambda SQS Id: ${sqsId}, partnerId: ${partnerId} triggered for generating signature. partnerPayoutId: ${partnerPayoutId}`
//     )
//
//     return sqsId
//   }
// }

const addAppQueuesForPrepareXmlFiles = async (
  payoutProcessData,
  processMessage,
  session
) => {
  const { appInfo = {} } = (await settingHelper.getSettingInfo()) || {}
  const appQueueData = {
    event: 'updated_payout_process',
    action: 'prepare_xml_file',
    destination: 'payments',
    priority: 'regular',
    params: {
      partnerId: payoutProcessData.partnerId,
      partnerPayoutId: payoutProcessData?.partnerPayoutId || undefined,
      payoutProcessInfo: payoutProcessData,
      processMessage,
      appInfo
    }
  }
  const [addedAppQueue] = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  return addedAppQueue
}

export const createPayoutProcess = async (params, session) => {
  const {
    partnerId,
    payoutIds,
    creditTransferData: creditTransferInfo,
    partnerPayoutId,
    refundPaymentIds
  } = params
  const payoutExecuteDate = new Date()

  const payoutProcessData = {
    partnerId,
    groupHeaderMsgId: nid(17),
    paymentInfoId: nid(17),
    requestExecuteDate: payoutExecuteDate,
    creditTransferInfo,
    status: 'new',
    sentFileStatus: 'not_created',
    partnerPayoutId
  }
  let processMessage = 'payout'

  if (size(payoutIds)) payoutProcessData.payoutIds = payoutIds

  if (size(refundPaymentIds)) {
    payoutProcessData.paymentIds = refundPaymentIds
    processMessage = 'refund payment'
  }
  const [payoutProcess] = await createPayoutProcesses(
    payoutProcessData,
    session
  )
  console.log('-- Payout process is in progress')
  const {
    _id: payoutProcessId,
    payoutIds: addedPayoutIds,
    paymentIds: addedPaymentIds
  } = payoutProcess
  if (!payoutProcessId) return false
  if (size(addedPayoutIds)) {
    const updateData = {
      $set: {
        sentToNETS: true,
        status: 'in_progress',
        sentToNETSOn: new Date()
      }
    }
    for (const payoutId of addedPayoutIds) {
      const updatedPayout = await payoutService.updateAPayout(
        { _id: payoutId },
        updateData,
        session
      )
      if (updatedPayout?._id)
        await payoutService.createLogForUpdatedPayout(updatedPayout, session)
    }
  } else if (size(addedPaymentIds)) {
    const updateData = {
      $set: {
        sentToNETS: true,
        refundStatus: 'in_progress',
        sentToNETSOn: new Date()
      }
    }
    for (const paymentId of addedPaymentIds) {
      const updatedRefundPayment =
        await invoicePaymentService.updateAnInvoicePayment(
          { _id: paymentId },
          updateData,
          session
        )
      if (updatedRefundPayment?._id)
        await invoicePaymentService.createRefundPaymentUpdatedLog(
          updatedRefundPayment,
          session
        )
    }
  }
  await partnerPayoutService.updateAPartnerPayout(
    { _id: partnerPayoutId },
    { $set: { payoutProcessId } },
    session
  )
  payoutProcessData.payoutProcessId = payoutProcessId
  const { _id: addedAppQueueId } = await addAppQueuesForPrepareXmlFiles(
    payoutProcessData,
    processMessage,
    session
  )
  console.log('-- Started processing XML files')
  return !!addedAppQueueId
}

const createPayoutProcesses = async (data, session) => {
  const payoutProcesses = await PayoutProcessCollection.create([data], {
    session
  })
  return payoutProcesses
}

export const updatePayoutProcessForPaymentLambda = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkUserId(user.userId)
  const { updateData: payoutProcessUpdatingArr } = body
  appHelper.checkRequiredFields(['updateData'], body)

  if (!size(payoutProcessUpdatingArr))
    throw new CustomError(400, 'Invalid updating data for payout process')

  const updatedPayoutProcesses = []
  for (const payoutProcessUpdatingObj of payoutProcessUpdatingArr) {
    const query = payoutProcessHelper.prepareQueryToUpdatePayoutProcess(
      payoutProcessUpdatingObj
    )
    const payoutProcessInfo = await payoutProcessHelper.getPayoutProcess(query)
    const updateData = payoutProcessHelper.prepareDataToUpdatePayoutProcess(
      payoutProcessUpdatingObj,
      payoutProcessInfo
    )
    console.log(
      '====> Checking payout process updating data:',
      JSON.stringify(updateData || {}),
      '<===='
    )
    const updatedPayoutProcess = await updateAPayoutProcessWithAfterUpdate(
      query,
      updateData,
      session
    )
    updatedPayoutProcesses.push(updatedPayoutProcess)
  }

  return !!size(updatedPayoutProcesses)
}
