import { clone, find, indexOf, isEmpty, map, size } from 'lodash'
import { CustomError } from '../common'
import { FileCollection } from '../models'
import {
  appHelper,
  appQueueHelper,
  fileHelper,
  invoiceHelper,
  notificationLogHelper,
  partnerSettingHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  contractService,
  fileService,
  invoiceService,
  logService,
  propertyItemService,
  propertyRoomService
} from '../services'

export const createAFile = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for file creation')
  }
  const createdFile = await FileCollection.create([data], { session })
  if (isEmpty(createdFile)) {
    throw new CustomError(404, 'Unable to create file')
  }
  return createdFile
}

export const createFiles = async (data = [], session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for file creation')
  }
  const createdFile = await FileCollection.insertMany(data, {
    session,
    runValidators: true
  })
  if (isEmpty(createdFile)) {
    throw new CustomError(404, 'Unable to create file')
  }
  return createdFile
}

export const updateAFile = async (query, data, session) => {
  if (!(size(data) && size(query)))
    throw new CustomError(400, 'Missing required data to update file')

  const updatedFile = await FileCollection.findOneAndUpdate(query, data, {
    new: true,
    runValidators: true,
    session
  })
  if (!size(updatedFile)) throw new CustomError(404, 'Could not update file')

  return updatedFile
}

export const addFile = async (req) => {
  try {
    const { body, session, user = {} } = req
    const { userId } = user
    appHelper.checkUserId(userId)
    appHelper.checkRequiredFields(
      ['title', 'name', 'directive', 'context'],
      body
    )
    const fileData = await fileHelper.prepareFileData(body, userId)
    const result = await createFiles([fileData], session)
    if (size(result)) {
      return result[0]
    }
  } catch (error) {
    throw new CustomError(
      error.statusCode || 404,
      error.message || 'Unable to create file',
      error
    )
  }
}

export const deleteFile = async (req) => {
  const { session, body } = req
  fileHelper.validatePutAndDeleteRequest(clone(body))
  const { _id } = body
  const deletedFile = await FileCollection.findOneAndDelete(
    { _id },
    { session }
  )
  if (!size(deletedFile)) {
    throw new CustomError(304, 'item not found')
  }
  return deletedFile
}

export const deleteAFile = async (fileId, session) => {
  const deletedFile = await FileCollection.findOneAndDelete(
    { _id: fileId },
    { session }
  )
  if (!size(deletedFile)) {
    throw new CustomError(404, 'item not found')
  }
  return deletedFile
}

export const deleteMultipleFile = async (fileIds, session) => {
  const deletedFiles = await FileCollection.deleteMany(
    {
      _id: {
        $in: fileIds
      }
    },
    { session }
  )
  return deletedFiles
}

const prepareFileQuery = (params = {}) => {
  const { fileId, partnerId } = params || {}
  return partnerId ? { _id: fileId, partnerId } : { _id: fileId }
}

const prepareFileUpdatingData = (params = {}) => {
  const { event, eventStatus, status } = params || {}

  const updatingAddToSetData = {}
  const updatingSetData = {}

  if (size(event)) updatingAddToSetData.events = event
  if (size(eventStatus)) updatingSetData.eventStatus = eventStatus
  if (size(status)) updatingSetData.status = status

  const updatingData = {}
  if (size(updatingAddToSetData))
    updatingData['$addToSet'] = updatingAddToSetData
  if (size(updatingSetData)) updatingData['$set'] = updatingSetData

  return updatingData
}

export const updateFileForLambda = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['fileId'], body)

  const query = prepareFileQuery(body)
  const updatingData = prepareFileUpdatingData(body)

  return await updateAFile(query, updatingData, session)
}

export const prepareDataAndCreateFilesForNotificationAttachments = async (
  attachmentFileIds,
  session
) => {
  const fileIdsWithAttachmentFileIds = []

  for (const attachmentFileId of attachmentFileIds) {
    const query = {
      attachmentsMeta: {
        $elemMatch: {
          id: { $in: attachmentFileId },
          fileKey: { $exists: true }
        }
      }
    }

    const notificationLogInfo =
      (await notificationLogHelper.getNotificationLog(query, session)) || {}

    if (size(notificationLogInfo)) {
      const {
        _id,
        accountId,
        agentId,
        attachmentsMeta = [],
        contractId,
        invoiceId,
        partnerId,
        propertyId,
        tenantId
      } = notificationLogInfo

      const attachmentInfo =
        find(
          attachmentsMeta,
          (attachment) => attachment.id === attachmentFileId
        ) || {}
      const { name: attachmentFileTitle } = attachmentInfo
      const attachmentFileName = `${attachmentFileId}.pdf`
      const jsonFileName = `${attachmentFileId}.json`

      const fileData = {
        accountId,
        agentId,
        contractId,
        invoiceId,
        jsonFileName,
        partnerId,
        propertyId,
        tenantId,
        context: 'attachments',
        directive: 'Files',
        name: attachmentFileName,
        notificationLogId: _id,
        size: 0,
        status: 'processed',
        title: attachmentFileTitle,
        type:
          size(attachmentInfo) && attachmentInfo.type
            ? attachmentInfo.type
            : 'email_attachment_pdf'
      }

      const [createdFile] = await createAFile(fileData, session)

      fileIdsWithAttachmentFileIds.push({
        attachmentFileId,
        fileId: createdFile._id
      })
    }
  }

  return fileIdsWithAttachmentFileIds
}

export const addFileForNotificationAttachments = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { attachmentFileIds = [] } = body

  if (!size(attachmentFileIds))
    throw new CustomError(400, "Didn't get required data to create")

  const fileIdsWithAttachmentFileIds =
    await prepareDataAndCreateFilesForNotificationAttachments(
      attachmentFileIds,
      session
    )
  return fileIdsWithAttachmentFileIds
}

export const addFileEvent = async (params = {}, session) => {
  const { fileId, status, note } = params
  const result = await FileCollection.findOneAndUpdate(
    { _id: fileId },
    {
      $push: {
        events: {
          status,
          createdAt: new Date(),
          note
        }
      }
    },
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!size(result)) throw new CustomError(404, 'File is not found')
  return result
}

export const prepareAndCreateFileForPdfGeneration = async (
  params = {},
  session
) => {
  let { content } = params
  if (!content) throw new CustomError(400, 'Pdf content is not found!')
  // Removing max-width range from template html content
  content = fileHelper.getRemovedMaxWidthContent(content)
  content =
    '<!DOCTYPE html><html><head><meta charset="UTF-8"></head>' +
    content +
    '</html>'

  const fileData = await fileHelper.prepareFileDataForPdfGeneration(params)
  const restrictionTypes = [
    'esigning_assignment_pdf',
    'lease_pdf',
    'esigning_moving_in_pdf',
    'esigning_lease_pdf',
    'assignment_pdf',
    'esigning_deposit_insurance_pdf',
    'esigning_moving_out_pdf',
    'deposit_account_contract_pdf'
  ]
  if (restrictionTypes.includes(fileData.type)) fileData.isFileInUse = true
  console.log('Checking fileData before insert: ', fileData)
  const [createdFileData] = (await createFiles([fileData], session)) || []
  console.log('Checking createdFileData: ', createdFileData)
  const { _id: fileId = '' } = createdFileData || {}
  if (fileId) {
    await addFileEvent(
      {
        fileId,
        status: 'created',
        note: 'Created new file for pdf generate'
      },
      session
    )

    const { isAppInvoice, invoiceId, type } = params
    if (indexOf(fileHelper.getInvoiceMainPdfTypes(), type) !== -1) {
      await invoiceService.addFileIdInInvoicePdf({
        invoiceId,
        pdfType: type,
        isAppInvoice,
        fileId
      })
    }

    const key = fileHelper.getFileKey(createdFileData)
    if (!key) {
      await addFileEvent(
        {
          fileId,
          status: 'save_to_path_not_found',
          note: 'File save to path not found'
        },
        session
      )
      throw new CustomError(404, 'File save to path not found!')
    }

    createdFileData.fileKey = key
    createdFileData.content = content
    return createdFileData
  }

  return false
}

const sendEvictionDueReminderToTenant = async (fileInfo, session) => {
  const { partnerId, invoiceId } = fileInfo
  let isSent = false
  if (!partnerId || !invoiceId) return isSent

  const partnerSettingsInfo = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const enabledCreateEvictionPackage =
    partnerSettingsInfo?.evictionDueReminderNotice?.isCreateEvictionPackage ||
    false
  if (!enabledCreateEvictionPackage) return true

  const isEvictionDueReminderNoticeEnabled =
    partnerSettingsInfo?.evictionDueReminderNotice?.enabled || false

  const invoiceInfo = await invoiceHelper.getAnInvoiceWithSort({
    _id: invoiceId,
    partnerId
  })

  if (!isEvictionDueReminderNoticeEnabled) return true

  const appQueueData = {
    destination: 'invoice',
    action: 'handle_eviction_due_reminder_notice',
    event: 'initialize_process_for_eviction_notice_and_reminder',
    params: {
      invoiceId,
      administrationEvictionFeeAmount:
        partnerSettingsInfo.administrationEvictionFee?.amount || 0,
      administrationEvictionFeeTax:
        partnerSettingsInfo.administrationEvictionFee?.tax || 0,
      contractId: invoiceInfo?.contractId,
      evictionFeeAmount: partnerSettingsInfo?.evictionFee?.amount || 0,
      evictionReminderDueDays:
        partnerSettingsInfo?.evictionDueReminderNotice?.days || 0,
      feesMeta: invoiceInfo?.feesMeta || [],
      haveToCreateEvictionProcessQueue: true,
      isAdministrationEvictionFeeEnabled:
        partnerSettingsInfo?.administrationEvictionFee?.enabled || false,
      isCreateEvictionPackage: false, // Making it false to send eviction due reminder notice with fee
      isEvictionFeeEnabled: partnerSettingsInfo?.evictionFee?.enabled || false,
      partnerId
    },
    priority: 'regular'
  }
  const appQueueInfo = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  if (appQueueInfo) isSent = true
  return isSent
}

const createEvictionPackageProducedLog = async (fileInfo, session) => {
  let isCreatedLog = false
  if (!fileInfo) return isCreatedLog
  const { partnerId, invoiceId } = fileInfo

  const logData = {
    partnerId,
    context: 'property',
    action: 'produced_eviction_document',
    fileId: fileInfo._id,
    invoiceId,
    visibility: ['property', 'invoice']
  }
  const logDataInfo = await logService.createLog(logData, session)
  if (logDataInfo) isCreatedLog = true
  return isCreatedLog
}

export const createProducedLogAndSendEvictionDueReminder = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['fileId'], body)
  const { fileId } = body
  const fileInfo = await fileHelper.getAFile({ _id: fileId })

  //  sendEvictionDueReminderToTenant
  const isSendDueReminder = await sendEvictionDueReminderToTenant(
    fileInfo,
    session
  )
  const isCreateProducedLog = await createEvictionPackageProducedLog(
    fileInfo,
    session
  )
  if (!isSendDueReminder || !isCreateProducedLog)
    throw new CustomError(
      404,
      'Error occurred during sendEvictionDueReminder or produce log'
    )
  return {
    result: isSendDueReminder && isCreateProducedLog
  }
}

export const createFileAndAppQueueForPdfGeneration = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(
    ['context', 'content', 'partnerId', 'type'],
    body
  )
  const fileInfo =
    (await prepareAndCreateFileForPdfGeneration(body, session)) || {}
  if (!size(fileInfo))
    throw new CustomError(400, 'Could not create file collection data!')

  console.log(`=== Created file for PDF generate. fileId: ${fileInfo._id} ===`)

  const appQueueData = appQueueHelper.getAppQueueDataForPdfGeneration(
    body,
    fileInfo
  )
  const [appQueueInfo] =
    (await appQueueService.createAnAppQueue(appQueueData, session)) || {}
  if (!size(appQueueInfo))
    throw new CustomError(400, 'Could not create app queue collection data!')

  const { _id: queueId = '' } = appQueueInfo
  const { _id: fileId = '' } = fileInfo

  if (queueId)
    await addFileEvent(
      {
        fileId,
        status: 'queued',
        note: 'App queue created for pdf generate'
      },
      session
    )
  else
    await addFileEvent(
      {
        fileId,
        status: 'upload_failed_to_s3',
        note: 'File upload failed to S3 New directory for pdf generate'
      },
      session
    )

  return { queueId }
}

export const createLogForUploadedFile = async (options, session) => {
  const logData = fileHelper.prepareLogDataForUploadedFile(options)
  await logService.createLog(logData, session)
}

export const addFileFromUI = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  if (partnerId) body.partnerId = partnerId
  const { createLog, fileData, options } =
    await fileHelper.prepareFileDataAndLogDataForAddFileFromUI(
      { ...body },
      session
    )
  fileData.createdBy = user.userId
  const [file] = await createAFile(fileData, session)
  //To create a log
  if (size(file) && createLog) {
    options.fileId = file._id
    options.createdBy = user.userId
    options.action = 'uploaded_file'
    await createLogForUploadedFile(options, session)
  }
  const { context, subContext, contractId, propertyFileType, propertyId } = body
  if (size(file)) {
    const { createdBy = '', name = '' } = file
    const userInfo = await userHelper.getAnUser({ _id: createdBy }, session)
    if (size(userInfo)) {
      file.createdByInfo = {
        name: userInfo.profile ? userInfo.profile.name : '',
        avatarKey: userHelper.getAvatar(userInfo)
      }
    }
    const extensionArr = name.split('.')
    const extension = extensionArr[extensionArr.length - 1]
    if (['png', 'jpeg', 'jpg', 'gif'].includes(extension)) {
      file.imageUrl =
        appHelper.getCDNDomain() +
        '/files' +
        '/' +
        partnerId +
        '/' +
        context +
        '/' +
        name
    }
  }
  //To update contract
  if (context === 'contract' && subContext && size(file)) {
    appHelper.checkRequiredFields(['contractId', 'propertyId'], body)
    await contractService.updateContract(
      {
        _id: contractId,
        propertyId,
        partnerId
      },
      { $addToSet: { files: { fileId: file._id, context: subContext } } },
      session
    )
  }
  // TODO:: Later need to write test cases for this method.
  if (context === 'moving_in_out' && size(propertyFileType)) {
    const { query, updateData } =
      await fileHelper.preparePropertyFilesDataForMovingInOut(body, file)
    const isPropertyItem = ['inventory', 'keys', 'meterReading'].includes(
      propertyFileType
    )
    if (isPropertyItem) {
      await propertyItemService.updateAPropertyItem(query, updateData, session)
      await propertyItemService.updatePropertyItemWithMovingProtocol(
        body,
        {
          query: {},
          updateData
        },
        session
      )
    }
    if (propertyFileType === 'rooms') {
      const result = await propertyRoomService.updateAPropertyRoom(
        query,
        updateData,
        session
      )
      if (!size(result)) {
        throw new CustomError(400, 'Failed to add file')
      }
      await propertyRoomService.updateRoomBasedOnContractIdAndMovingId(
        result.toObject(),
        session
      )
    }
  }
  return file
}

export const uploadFiles = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId = '', roles = [] } = user
  if (partnerId) body.partnerId = partnerId
  const { files, requestFrom } = body
  if (!size(files)) throw new CustomError(400, 'files data should not be empty')
  let uploadedBy
  if (requestFrom === 'partner_public') {
    if (roles.includes('partner_landlord')) {
      uploadedBy = 'landlord'
    } else if (roles.includes('partner_tenant')) {
      uploadedBy = 'tenant'
    }
  }
  const filesInfo = []
  for (const fileInfo of files) {
    fileInfo.partnerId = partnerId
    const { createLog, fileData, options } =
      await fileHelper.prepareFileDataAndLogDataForAddFileFromUI(
        { ...fileInfo },
        session
      )
    fileData.createdBy = user.userId
    fileData.uploadedBy = uploadedBy
    const [file] = await createAFile(fileData, session)
    //To create a log
    if (size(file) && createLog) {
      options.fileId = file._id
      options.createdBy = user.userId
      options.action = 'uploaded_file'
      await createLogForUploadedFile(options, session)
    }
    const { context, subContext, contractId, propertyFileType, propertyId } =
      fileInfo
    if (size(file)) {
      const { createdBy = '', name = '' } = file
      const userInfo = await userHelper.getAnUser({ _id: createdBy }, session)
      if (size(userInfo)) {
        file.createdByInfo = {
          name: userInfo.profile ? userInfo.profile.name : '',
          avatarKey: userHelper.getAvatar(userInfo)
        }
      }
      const extensionArr = name.split('.')
      const extension = extensionArr[extensionArr.length - 1]
      if (['png', 'jpeg', 'jpg', 'gif'].includes(extension)) {
        file.imageUrl =
          appHelper.getCDNDomain() +
          '/files' +
          '/' +
          partnerId +
          '/' +
          context +
          '/' +
          name
      }
    }
    //To update contract
    if (context === 'contract' && subContext && size(file)) {
      appHelper.checkRequiredFields(['contractId', 'propertyId'], fileInfo)
      await contractService.updateContract(
        {
          _id: contractId,
          propertyId,
          partnerId
        },
        { $addToSet: { files: { fileId: file._id, context: subContext } } },
        session
      )
    }
    if (context === 'moving_in_out' && size(propertyFileType)) {
      const { query, updateData } =
        await fileHelper.preparePropertyFilesDataForMovingInOut(fileInfo, file)
      const isPropertyItem = ['inventory', 'keys', 'meterReading'].includes(
        propertyFileType
      )
      if (isPropertyItem) {
        await propertyItemService.updateAPropertyItem(
          query,
          updateData,
          session
        )
        await propertyItemService.updatePropertyItemWithMovingProtocol(
          fileInfo,
          {
            query: {},
            updateData
          },
          session
        )
      }
      if (propertyFileType === 'rooms') {
        const result = await propertyRoomService.updateAPropertyRoom(
          query,
          updateData,
          session
        )
        if (!size(result)) {
          throw new CustomError(400, 'Failed to add file')
        }
        await propertyRoomService.updateRoomBasedOnContractIdAndMovingId(
          result.toObject(),
          session
        )
      }
    }
    filesInfo.push(file)
  }
  return filesInfo
}

export const uploadDirectRemittanceApprovalSignedFileToS3 = async (
  params,
  session
) => {
  const { documentId, fileData, idfyResData, partnerId, partnerPayoutId } =
    params
  const {
    fileKey = '',
    fileName = '',
    type: fileType = '',
    title: existingFileTitle = ''
  } = fileData || {}
  const directive = 'Files'
  let type = ''

  if (fileType === 'payouts_approval_esigning_pdf') {
    type = 'e_signing/payouts'
  } else if (fileType === 'send_payments_approval_esigning') {
    type = 'e_signing/payments'
  }

  if (!(size(idfyResData) && fileKey)) {
    const paramsAndOptions = fileHelper.getFileUploadParamsAndOptions({
      directive,
      existingFileTitle,
      partnerId,
      type
    })

    const queueData = {
      action: 'handle_e_signing',
      destination: 'esigner',
      event: 'fetch_or_upload_document',
      params: {
        partnerId,
        processType: 'fetch_or_upload_document',
        subProcessType: 'uploadDirectRemittanceApprovalSignedFileToS3',
        documentId,
        partnerPayoutId,
        paramsAndOptions,
        actions: ['status', 'file_upload']
      },
      priority: 'immediate',
      status: 'new'
    }
    const [createdQueue] = await appQueueService.createAnAppQueue(
      queueData,
      session
    )

    console.log(
      `### Created new app Queue for uploadDirectRemittanceApprovalSignedFileToS3 with
      actions: ${queueData.params.actions}, createdQueueId: ${createdQueue._id},
      partnerPayoutId: ${partnerPayoutId}, partnerId: ${partnerId}`
    )
  } else if (size(idfyResData) && fileName && size(fileKey)) {
    console.log(
      'Signed file successfully saved. filename: ',
      fileName,
      ', partnerPayoutId:',
      partnerPayoutId,
      ', partnerId:',
      partnerId
    )
  }
}

export const deleteFiles = async (query, session) => {
  const deletedFiles = await FileCollection.deleteMany(query, { session })
  if (!size(deletedFiles)) {
    throw new CustomError(404, 'Files not found')
  }
  return deletedFiles
}

export const removeFileFromUI = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['context'])
  const { body, session } = req
  const deletableFiles = await fileHelper.prepareDataForRemoveFilesFromS3(body)
  const queue = await appQueueService.createAppQueueForRemoveFilesFromS3(
    deletableFiles,
    session
  )
  if (!queue) throw new CustomError(400, 'Failed to create queue')
  await removeFileByContext(body, session)
  return {
    result: true
  }
}

const removeFileByContext = async (params, session) => {
  const { context, fileId, subContext } = params
  const { data, query } = await fileHelper.prepareDataForRemoveFileByContext(
    params
  )
  let deletedFile = {}
  if (fileId) {
    deletedFile = await deleteAFile(fileId, session)
  }
  if (context === 'property') {
    appHelper.checkRequiredFields(['propertyId', 'subContext'], params)
    if (['inventory', 'keys', 'meterReading'].includes(subContext)) {
      appHelper.checkRequiredFields(['from'], params)
      const removedItem = await propertyItemService.updateAPropertyItem(
        query,
        data,
        session
      )
      if (!removedItem) throw new CustomError(400, 'Failed to remove file')
      await propertyItemService.updatePropertyItemWithMovingProtocol(
        params,
        { query: {}, updateData: data },
        session
      )
    }
  }
  if (context === 'accounts' || context === 'contract') {
    appHelper.checkRequiredFields(['contractId', 'propertyId'], params)
    if (size(query) && size(data)) {
      const updatedContract = await contractService.updateContract(
        query,
        data,
        session
      )
      if (!size(updatedContract))
        throw new CustomError(400, 'Failed to remove file')
    }
  }
  if (context === 'propertyRoom') {
    const result = await propertyRoomService.updateAPropertyRoom(
      query,
      data,
      session
    )
    if (!size(result)) {
      throw new CustomError(400, 'Failed to remove file')
    }
    await propertyRoomService.updateRoomBasedOnContractIdAndMovingId(
      result.toObject(),
      session
    )
  }
  await createLogForFileRemove(deletedFile, params, session)
}

export const createLogForFileRemove = async (file, params, session) => {
  const { context, type } = file
  const allowedContextForRemoveFile = [
    'account',
    'property',
    'contract',
    'task',
    'tenant',
    'lease',
    'invoice',
    'correction',
    'assignment',
    'moving_in',
    'moving_out',
    'interest_form',
    'deposit_insurance',
    'attachments',
    'eviction_document'
  ]
  if (type || allowedContextForRemoveFile.includes(context)) {
    if (
      context !== 'interest_form' &&
      context !== 'deposit_accounts' &&
      context !== 'deposit_insurance' &&
      context !== 'attachments'
    ) {
      const logData = fileHelper.prepareLogDataForRemoveFile(file, params)
      await logService.createLog(logData, session)
    }
  }
}

export const createAppQueueForRemoveFilesFromS3 = async (
  files = [],
  session
) => {
  if (!size(files)) return false

  const appQueue = {
    action: 'remove_multiple_object_from_s3',
    event: 'remove_multiple_object_from_s3',
    destination: 'lease',
    params: { files },
    priority: 'regular',
    status: 'new'
  }
  return await appQueueService.createAnAppQueue(appQueue, session)
}

export const removeFilesAndCreateLogs = async (
  fileRemoveQuery,
  params = {},
  session
) => {
  const files = await fileHelper.getFiles(fileRemoveQuery)
  console.log('=== # of deletableFiles ===', size(files))

  if (!size(files)) return files

  for (const file of files) {
    await createLogForFileRemove(file, params, session)
  }

  const deletableFiles = await fileHelper.getFilesWithSelectedFields(
    fileRemoveQuery,
    ['type', 'partnerId', 'context', 'directive', 'name']
  )
  console.log('===  deletableFiles ===', deletableFiles)

  await createAppQueueForRemoveFilesFromS3(deletableFiles, session)
  await fileService.deleteFiles(fileRemoveQuery, session)
}

export const updateMultipleFiles = async (query, data, session) => {
  const result = await FileCollection.updateMany(query, data, {
    runValidators: true,
    session
  })
  if (result.nModified > 0) {
    return result
  }
}

export const updateFileFromPartnerApp = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['fileId'])
  const { body } = req
  const {
    fileId,
    isVisibleToLandlord,
    isVisibleToTenant,
    partnerId,
    isRequestFromCorrection
  } = body

  const partnerSetting =
    (await partnerSettingHelper.getSettingByPartnerId(partnerId)) || {}
  const isShowFileLandlord =
    partnerSetting.assignmentSettings?.enabledShowAssignmentFilesToLandlord
  const isShowFileToTenant =
    partnerSetting.leaseSetting?.enabledShowLeaseFilesToTenant

  const query = { _id: fileId, partnerId }
  const data = {}

  if (body.hasOwnProperty('isVisibleToLandlord')) {
    if (!isShowFileLandlord && !isRequestFromCorrection) {
      throw new CustomError(
        400,
        'Show assignment files to landlord is not enabled'
      )
    }
    data.isVisibleToLandlord = isVisibleToLandlord
  }
  if (body.hasOwnProperty('isVisibleToTenant')) {
    if (!isShowFileToTenant && !isRequestFromCorrection) {
      throw new CustomError(400, 'Show lease files to tenant is not enabled')
    }
    data.isVisibleToTenant = isVisibleToTenant
  }
  if (!size(data)) {
    throw new CustomError(400, 'Nothing to update')
  }
  await updateAFile(query, { $set: data })
  return {
    result: true
  }
}

export const addORRemoveFileInUseTag = async (query, data, session) => {
  if (!(size(query) && size(data)))
    throw new CustomError(400, 'Missing file query or updating data')
  console.log('=== File Query', query)
  const files = await FileCollection.find(query)
  if (size(files)) {
    const fileIds = map(files, '_id')
    console.log(
      `=== Updating File is use tag for fileIds: ${fileIds}, data: ${JSON.stringify(
        data
      )}`
    )
    const updatedFiles = await FileCollection.updateMany(
      { _id: { $in: fileIds } },
      data,
      {
        new: true,
        runValidators: true,
        session
      }
    )

    return updatedFiles
  } else {
    console.log(
      `=== File not found to update file is use tag for query: ${JSON.stringify(
        query
      )}`
    )
    return null
  }
}
