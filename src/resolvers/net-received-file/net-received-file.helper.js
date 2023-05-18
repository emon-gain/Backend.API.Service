import { has, indexOf, omit, size } from 'lodash'

import { getS3SignedUrl } from '../../lib/s3'
import settingJson from '../../../settings'

import { NetReceivedFileCollection } from '../models'
import { appHelper, invoicePaymentHelper } from '../helpers'

export const getANetReceivedFile = async (query, session) => {
  const netReceivedFile = await NetReceivedFileCollection.findOne(
    query
  ).session(session)
  return netReceivedFile
}

export const prepareNetReceivedFileQueryBasedOnFilters = (query) => {
  const {
    createdDateRange,
    partnerId,
    paymentStatuses,
    receivedFileName,
    statuses,
    receivedFileNameLambda
  } = query
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query['payments.partnerId'] = partnerId
  }
  query['payments.transactionType'] = { $eq: 'CRDT' }
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: createdDateRange.startDate,
      $lte: createdDateRange.endDate
    }
  }
  if (size(statuses)) {
    query.status = { $in: statuses }
  }
  if (size(paymentStatuses) && indexOf(statuses, 'processed') !== -1) {
    query['payments.status'] = { $in: paymentStatuses }
  }
  if (size(receivedFileName)) {
    query.receivedFileName = {
      $regex: new RegExp('.*' + receivedFileName + '.*', 'i')
    }
  }
  //For payments lambda #10482
  if (size(receivedFileNameLambda)) {
    query.receivedFileName = receivedFileNameLambda
    delete query['payments.transactionType']
  }
  console.log('Prepared query before omit: ', query)
  const netReceivedFilesQuery = omit(query, [
    'createdDateRange',
    'receivedFileNameLambda',
    'partnerId',
    'paymentStatuses',
    'statuses'
  ])
  return netReceivedFilesQuery
}

export const getNetReceiveFileForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const netReceivedFiles = await NetReceivedFileCollection.find(query)
    .populate(['payments.paymentInfo', 'payments.partnerInfo'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return netReceivedFiles
}

export const countNetReceivedFiles = async (query) => {
  const numberOfnetReceivedFiles = await NetReceivedFileCollection.find(
    query
  ).countDocuments()
  return numberOfnetReceivedFiles
}

export const queryNetReceivedFiles = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = prepareNetReceivedFileQueryBasedOnFilters(query)
  const netReceivedFilesData = await getNetReceiveFileForQuery(body)
  const filteredDocuments = await countNetReceivedFiles(body.query)
  const totalDocuments = await countNetReceivedFiles({})
  const directive = settingJson.S3.Directives['NETS']
  const bucket = process.env.S3_BUCKET || 'uninite-com-local'
  const netReceivedFiles = []
  for (const netReceivedFile of netReceivedFilesData) {
    let subFolder = '/Received/'
    if (netReceivedFile.status === 'processed') {
      subFolder = '/PROCESSED/Received/'
    }
    netReceivedFile.S3SentFileUrl = await getS3SignedUrl(
      directive.folder + subFolder + netReceivedFile.receivedFileName,
      bucket
    )
    netReceivedFiles.push(netReceivedFile)
  }
  return {
    data: netReceivedFiles,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const prepareQueryToUpdateNetReceivedFile = (body) => {
  const { netReceivedFileId } = body
  const query = {}
  if (size(netReceivedFileId)) query._id = netReceivedFileId
  return query
}

export const prepareDataToUpdateNetReceivedFile = async (body) => {
  const {
    fileType,
    haveToUpdatePaymentStatus = false,
    isCreditTransaction,
    isDebitTransaction,
    invalidFile,
    moveFailed,
    netReceivedFileId,
    payments,
    status
  } = body
  const updatingAddToSetData = {}
  if (size(payments)) updatingAddToSetData.payments = { $each: payments }
  const updatingSetData = {}
  if (size(status)) updatingSetData.status = status
  if (size(fileType)) updatingSetData.fileType = fileType
  if (has(body, 'isCreditTransaction'))
    updatingSetData.isCreditTransaction = isCreditTransaction
  if (has(body, 'isDebitTransaction'))
    updatingSetData.isDebitTransaction = isDebitTransaction
  if (has(body, 'invalidFile')) updatingSetData.invalidFile = invalidFile
  if (has(body, 'moveFailed')) updatingSetData.moveFailed = moveFailed
  if (haveToUpdatePaymentStatus) {
    const paymentStatusArray =
      (await invoicePaymentHelper.getPaymentStatusArrayForNETSReceivedFile(
        netReceivedFileId
      )) || []

    if (size(paymentStatusArray)) updatingSetData.payments = paymentStatusArray
  }

  const updatingData = {}
  if (size(updatingAddToSetData))
    updatingData['$addToSet'] = updatingAddToSetData
  if (size(updatingSetData)) updatingData['$set'] = updatingSetData

  return updatingData
}

export const queryNetReceivedFile = async (req) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['netReceivedFileId'], query)
  const { netReceivedFileId } = query
  const netReceivedFile = await getANetReceivedFile({ _id: netReceivedFileId })
  return netReceivedFile
}
