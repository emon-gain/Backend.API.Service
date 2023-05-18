import { size } from 'lodash'
import { CustomError } from '../common'
import { CorrectionCollection } from '../models'
import {
  appHelper,
  contractHelper,
  correctionHelper,
  fileHelper,
  finalSettlementHelper,
  invoiceHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  counterService,
  fileService,
  invoiceService,
  logService,
  transactionService
} from '../services'

export const updateACorrection = async (query, data, session) => {
  if (!size(data))
    throw new CustomError(404, 'No data found to update correction')
  const updatedCorrection = await CorrectionCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      runValidators: true,
      new: true
    }
  )
  if (!size(updatedCorrection))
    throw new CustomError(404, 'Correction not found')
  return updatedCorrection
}

export const createCorrectionLog = async (correction = {}, session) => {
  const {
    _id,
    accountId,
    amount,
    contractId,
    invoiceId,
    partnerId,
    propertyId,
    tenantId,
    createdBy
  } = correction
  const logData = {
    createdBy,
    context: 'correction',
    action: 'added_new_correction',
    partnerId,
    correctionId: _id,
    meta: [{ field: 'amount', value: amount }]
  }
  const visibility = ['correction']
  if (accountId) {
    visibility.push('account')
    logData.accountId = accountId
  }
  if (invoiceId) visibility.push('invoice')
  if (propertyId) {
    visibility.push('property')
    logData.propertyId = propertyId
  }
  if (tenantId) {
    visibility.push('tenant')
    logData.tenantId = tenantId
  }
  if (contractId) logData.contractId = contractId
  logData.visibility = visibility
  await logService.createLog(logData, session)
}
export const createACorrection = async (correctionData, session) => {
  const [correction] = await CorrectionCollection.create([correctionData], {
    session
  })
  return correction
}

export const createCorrection = async (req) => {
  const { user = {} } = req
  const { roles = [] } = user
  if (roles.includes('lambda_manager')) {
    req.user.partnerId = req.body.partnerId
    req.user.userId = req.body?.userId
  }
  appHelper.validatePartnerAppRequestData(req, [
    'addTo',
    'addons',
    'contractId',
    'propertyId'
  ])
  const { body, session } = req
  const { contractId, partnerId } = body
  const contract = await contractHelper.getAContract({
    _id: contractId,
    partnerId
  })

  if (!contract) {
    throw new CustomError(404, 'Could not find contract for this correction!')
  }

  await correctionHelper.validateCreateCorrectionData(contract, body, session)
  const correctionData = await correctionHelper.prepareAddCorrectionData(
    contract,
    body
  )

  if (size(body.files)) {
    body.createdBy = body.userId
    const filesData = await correctionHelper.prepareFilesForCorrection(
      body.files,
      body
    )
    const files = await fileService.createFiles(filesData, session)
    if (!size(files)) throw new CustomError(404, 'Could not upload files')
    correctionData.files = files.map((file) => file._id)
  }

  correctionData.correctionSerialId = await counterService.incrementCounter(
    `correction-${partnerId}`,
    session
  )

  const correction = await createACorrection(correctionData, session)
  if (!size(correction)) {
    throw new CustomError(400, 'Could not create correction')
  }

  // After create process
  await createCorrectionLog(correction, session)
  if (body.addTo === 'payout') {
    const landlordInvoiceParams = {
      contractId: correction.contractId,
      correctionId: correction._id,
      enabledNotification: false,
      isLandlordCorrectionInvoice: true,
      landlordInvoiceFor: 'payoutCorrections',
      partnerId: correction.partnerId,
      propertyId: correction.propertyId
    }
    await invoiceService.createLandlordInvoices(landlordInvoiceParams, session)
  }

  const correctionInfo = await correctionHelper.getNewlyCreatedCorrection(
    correction._id,
    session
  )
  // Adding invoice preview info
  if (body.addTo === 'rent_invoice' && body.createInvoice) {
    const params = {
      contract,
      correctionId: correction._id,
      isDemo: false,
      returnPreview: true
    }
    const [invoicesInfo] = await invoiceHelper.getCorrectionInvoicePreview(
      params,
      session
    )
    correctionInfo.invoicePreviewInfo = {
      invoiceStartOn: invoicesInfo.invoiceStartOn,
      dueDate: invoicesInfo.dueDate,
      invoiceTotal: correction.amount
    }
  }
  return correctionInfo
}

export const addAndRemoveFilesForUpdateCorrections = async (
  body,
  correction,
  session
) => {
  const { files, removeFileIds, isVisible } = body
  let allFileIds = size(correction.files) ? correction.files : []
  if (size(allFileIds)) {
    if (
      correction.addTo === 'rent_invoice' &&
      correction.isVisibleToTenant !== isVisible
    ) {
      await fileService.updateMultipleFiles(
        {
          _id: { $in: allFileIds }
        },
        {
          isVisibleToTenant: isVisible
        }
      )
    } else if (correction.isVisibleToLandlord !== isVisible) {
      await fileService.updateMultipleFiles(
        {
          _id: { $in: allFileIds }
        },
        {
          isVisibleToLandlord: isVisible
        }
      )
    }
  }
  if (size(files)) {
    correction.isVisible = isVisible
    const filesData = await correctionHelper.prepareFilesForCorrection(
      files,
      correction
    )
    const newFiles = await fileService.createFiles(filesData, session)
    if (!size(newFiles)) throw new CustomError(404, 'Could not upload files')
    const newFileIds = newFiles.map((file) => file._id)
    allFileIds = [...allFileIds, ...newFileIds]
  }

  if (size(removeFileIds)) {
    const fileRemoveQuery = {
      _id: { $in: removeFileIds },
      partnerId: correction.partnerId,
      createdBy: correction.createdBy,
      contractId: correction.contractId
    }
    const deletableFiles = await fileHelper.getFilesWithSelectedFields(
      fileRemoveQuery,
      ['type', 'partnerId', 'context', 'directive', 'name']
    )
    await appQueueService.createAppQueueForRemoveFilesFromS3(
      deletableFiles,
      session
    )
    await fileService.deleteFiles(fileRemoveQuery, session)
    allFileIds = allFileIds.filter((fileId) => !removeFileIds.includes(fileId))
  }
  return allFileIds
}

export const updateCorrection = async (req) => {
  const { user = {} } = req
  const { roles = [] } = user
  if (roles.includes('lambda_manager')) {
    req.user.partnerId = req.body.partnerId
    req.user.userId = req.body.userId
  }
  appHelper.validatePartnerAppRequestData(req, ['correctionId', 'addons'])
  const { body, session } = req
  const { correctionId, partnerId } = body
  const correction = await correctionHelper.getCorrectionById(
    correctionId,
    session
  )
  if (!correction) {
    throw new CustomError(
      404,
      'Could not find any correction by this correctionId!'
    )
  }

  await correctionHelper.validateUpdateCorrectionData(body, correction, session)
  const updateData = await correctionHelper.prepareUpdateCorrectionData(
    body,
    correction
  )
  updateData.files = await addAndRemoveFilesForUpdateCorrections(
    body,
    correction,
    session
  )

  if (!size(updateData)) {
    throw new CustomError(404, 'Could not find any updatable correction data')
  }

  const updatedData = await updateACorrection(
    { _id: correctionId },
    updateData,
    session
  )

  const correctionInfo = await correctionHelper.getNewlyCreatedCorrection(
    updatedData._id,
    session
  )

  // Adding invoice preview info
  if (updatedData.addTo === 'rent_invoice' && body.createInvoice) {
    const contract = await contractHelper.getAContract({
      _id: updatedData.contractId,
      partnerId
    })
    const params = {
      contract,
      correctionId: correction._id,
      isDemo: false,
      returnPreview: true
    }
    const [invoicesInfo] = await invoiceHelper.getCorrectionInvoicePreview(
      params,
      session
    )
    correctionInfo.invoicePreviewInfo = {
      invoiceStartOn: invoicesInfo.invoiceStartOn,
      dueDate: invoicesInfo.dueDate,
      invoiceTotal: updatedData.amount
    }
  }
  return correctionInfo
}

export const removeCorrectionFiles = async (req) => {
  const { body, session } = req
  const { correctionId, correctionData = {} } = body
  let correction = await correctionHelper.getCorrectionById(
    correctionId,
    session
  )
  if (!correction) {
    throw new CustomError(
      404,
      'Could not find any correction by this correctionId!'
    )
  }
  correction.files = correctionData.files || []
  correction = await correction.save()
  return correction
}

export const cancelCorrection = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['correctionId'])
  const { body, session } = req
  const { correctionId, partnerId } = body
  const correction = await correctionHelper.getCorrection({
    _id: correctionId,
    partnerId
  })
  if (!size(correction)) {
    throw new CustomError(
      404,
      'Could not find any correction by this correctionId!'
    )
  }
  if (correction.correctionStatus === 'cancelled')
    throw new CustomError(400, 'Correction already cancelled')
  const { contractId } = correction
  const isFinalSettlementDone =
    await finalSettlementHelper.isDoneFinalSettlement(contractId, partnerId)
  if (isFinalSettlementDone) {
    throw new CustomError(405, 'Final settlement is done for this contract!')
  }
  const updatedCorrection = await updateACorrection(
    {
      _id: correctionId,
      partnerId
    },
    {
      $set: {
        correctionStatus: 'cancelled',
        amount: 0,
        cancelledAt: new Date()
      },
      $unset: {
        payoutId: 1
      }
    },
    session
  )
  await createCorrectionCancelLog(correction, session)
  if (updatedCorrection.addTo === 'payout') {
    await createQueueForCreateLandlordInvoiceOrCreditNote(
      {
        correctionId,
        partnerId
      },
      session
    )
  }
  return updatedCorrection
}

export const createQueueForCreateLandlordInvoiceOrCreditNote = async (
  params = {},
  session
) => {
  const { correctionId, partnerId } = params
  const landlordInvoices = await invoiceHelper.getInvoices({
    partnerId,
    'addonsMeta.correctionId': correctionId,
    $or: [
      {
        invoiceType: 'landlord_invoice',
        status: { $ne: 'credited' },
        forCorrection: { $ne: true }
      },
      {
        invoiceType: 'landlord_credit_note',
        forCorrection: true,
        isCreditedForCancelledCorrection: { $ne: true }
      }
    ]
  })
  if (size(landlordInvoices)) {
    const appQueueData = []
    for (const invoice of landlordInvoices) {
      appQueueData.push({
        action: 'create_landlord_invoice_or_landlord_credit_note',
        event: 'create_landlord_invoice_or_landlord_credit_note',
        params: {
          partnerId,
          invoiceId: invoice._id,
          correctionId
        },
        destination: 'invoice',
        priority: 'immediate'
      })
    }
    await appQueueService.insertAppQueueItems(appQueueData, session)
  }
}

export const createCorrectionCancelLog = async (correction = {}, session) => {
  const {
    _id,
    accountId,
    amount,
    contractId,
    invoiceId,
    partnerId,
    propertyId,
    tenantId
  } = correction
  const logData = {
    context: 'correction',
    action: 'cancelled_a_correction',
    partnerId,
    correctionId: _id,
    meta: [{ field: 'amount', value: amount }]
  }
  const visibility = ['correction']
  if (accountId) {
    visibility.push('account')
    logData.accountId = accountId
  }
  if (invoiceId) visibility.push('invoice')
  if (propertyId) {
    visibility.push('property')
    logData.propertyId = propertyId
  }
  if (tenantId) {
    visibility.push('tenant')
    logData.tenantId = tenantId
  }
  if (contractId) logData.contractId = contractId
  logData.visibility = visibility
  await logService.createLog(logData, session)
}

export const updateCorrections = async (query, data, session) => {
  const response = await CorrectionCollection.updateMany(query, data, {
    session
  })
  return response
}

export const removeOldDataTagFromCorrections = async (partnerId, session) => {
  const query = { oldData: { $exists: true }, partnerId }
  const updateData = { $unset: { oldData: '' } }
  await updateCorrections(query, updateData, session)
}

export const addTransactionByAddonMeta = async (params, session) => {
  const { addonMeta, correction, transactionData } = params
  const { total: addonAmount } = addonMeta
  const createdAt = correction.cancelledAt
    ? correction.cancelledAt
    : correction.createdAt
  if (createdAt) {
    transactionData.createdAt = createdAt
  }
  const existingParams = { correction, addonMeta, addonAmount }
  const isTransactionExists =
    await correctionHelper.isExistsCorrectionTransaction(
      existingParams,
      session
    )
  if (isTransactionExists) {
    return false
  }
  const transactionParams = {
    correction,
    addonMeta,
    transactionData,
    addonAmount
  }
  const updatedTransactionData = await correctionHelper.updateTransactionData(
    transactionParams,
    session
  )
  const transaction = await transactionService.createTransaction(
    updatedTransactionData,
    session
  )
  return transaction
}

export const addCorrectionTransaction = async (
  correction,
  transactionEvent,
  addon,
  session
) => {
  const { partnerId = '', _id } = correction
  if (!(size(addon) && partnerId && _id)) {
    return false
  }
  const transactionData =
    await correctionHelper.prepareCorrectionTransactionData(
      correction,
      transactionEvent,
      session
    )
  const completedTransctions = []
  const params = { addonMeta: addon, correction, action: '', transactionData }
  const transaction = await addTransactionByAddonMeta(params, session)
  if (transaction) {
    completedTransctions.push(correction._id)
  }
  return completedTransctions
}

export const downloadCorrection = async (req) => {
  const { body, session, user } = req
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId

  const correctionQuery =
    await correctionHelper.prepareQueryDataForQueryCorrections(body)

  await appHelper.isMoreOrLessThanTargetRows(
    CorrectionCollection,
    correctionQuery,
    {
      moduleName: 'Corrections'
    }
  )

  const {
    accountId,
    agentId,
    branchId,
    contractId,
    correctionStatus,
    createdAtDateRange,
    createdBy,
    addTo,
    leaseSerial,
    propertyId,
    searchKeyword = '',
    sort = { createdAt: -1 },
    tenantId
  } = body

  appHelper.validateSortForQuery(sort)
  const params = {}

  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    params.createdAtDateRange = {
      startDate: new Date(startDate),
      endDate: new Date(endDate)
    }
  }
  if (tenantId) {
    appHelper.validateId({ tenantId })
    params.tenantId = tenantId
  }
  if (accountId) {
    appHelper.validateId({ accountId })
    params.accountId = accountId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    params.agentId = agentId
  }
  if (branchId) {
    appHelper.validateId({ branchId })
    params.branchId = branchId
  }
  if (contractId) params.contractId = contractId
  if (leaseSerial) params.leaseSerial = leaseSerial
  if (propertyId) {
    appHelper.validateId({ propertyId })
    params.propertyId = propertyId
  }
  if (createdBy) {
    appHelper.validateId({ createdBy })
    params.createdBy = createdBy
  }
  if (correctionStatus) {
    params.correctionStatus = correctionStatus
  }
  if (addTo) params.addTo = addTo
  if (searchKeyword) params.searchKeyword = searchKeyword

  params.partnerId = partnerId
  params.userId = userId
  params.sort = sort
  params.downloadProcessType = 'download_correction'

  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'

  const queueData = {
    action: 'download_email',
    event: 'download_email',
    priority: 'immediate',
    destination: 'excel-manager',
    status: 'new',
    params
  }

  const payoutQueue = await appQueueService.createAnAppQueue(queueData, session)
  if (size(payoutQueue)) {
    return {
      status: 200,
      message:
        'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
    }
  } else {
    throw new CustomError(404, `Unable to download payout`)
  }
}
