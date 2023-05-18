import nid from 'nid'
import mime from 'mime-types'
import { includes, indexOf, pick, size, union } from 'lodash'
import { CustomError } from '../common'
import settingJson from '../../../settings'
import { getS3SignedUrl, getWritePolicy } from '../../lib/s3'
import {
  appHelper,
  contractHelper,
  conversationHelper,
  listingHelper,
  logHelper,
  partnerSettingHelper,
  propertyItemHelper
} from '../helpers'
import { propertyItemService } from '../services'
import { FileCollection, PropertyItemCollection } from '../models'

export const bucket = process.env.S3_BUCKET || 'uninite-com-local'

export const getAFile = async (query, session, sort = {}) => {
  const fileInfo = await FileCollection.findOne(query)
    .sort(sort)
    .session(session)
  return fileInfo
}

export const getAFileWithSort = async (query = {}, sort = {}, session) => {
  const fileInfo = await FileCollection.findOne(query)
    .sort(sort)
    .session(session)
  return fileInfo
}

export const getFiles = async (query, session) => {
  const files = await FileCollection.find(query).session(session)
  return files
}

export const prepareFileData = async (data, userId) => {
  const { _id: fileId, size: fileSize } = data

  data._id = fileId || nid(17)
  if (userId && userId !== 'Lambda') {
    data.createdBy = userId
  }
  data.fileUrlHash = nid(30)
  if (!fileSize) {
    data.size = 0
  }
  data.createdAt = new Date()

  return data
}

export const validatePutAndDeleteRequest = (body) => {
  const { _id } = body
  if (!_id) {
    throw new CustomError(400, 'Bad request!_id is missing')
  }
}

export const checkRequiredDataForPolicyInfo = (body, user) => {
  const { query } = body
  let { directive } = query
  const { filename } = query
  const { userId } = user
  if (!userId && directive !== 'Listings')
    throw new CustomError(401, 'unauthorised') //Throw error if user is not logged in. Non logged in user can upload image only in Listings
  if (!directive) directive = 'ProfileImage'
  directive = settingJson.S3.Directives[directive]
  if (!directive) {
    throw new CustomError(401, 'S3 Directive not found!')
  }
  const fileType = mime.lookup(filename)
  if (!includes(directive.allowedFileTypes, fileType)) {
    throw new CustomError(500, 'Filetype is not supported!')
  }
  return true
}

export const preparePolicyData = (body) => {
  const { query } = body
  let { directive } = query
  const { filename, subFolderName } = query
  if (!directive) directive = 'ProfileImage'
  directive = settingJson.S3.Directives[directive]
  const { folder, maxSize = 10, timeOut = 60 } = directive
  let activeKey = folder + '/' + filename
  if (subFolderName) activeKey = folder + '/' + subFolderName + '/' + filename
  return {
    filesize: maxSize,
    duration: timeOut,
    key: activeKey,
    directive
  }
}

export const getUploadPolicy = async (req) => {
  const { body, user } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  checkRequiredDataForPolicyInfo(body, user)
  const policyData = preparePolicyData(body)
  const policy = await getWritePolicy(policyData)
  return policy
}

export const getInvoiceMainPdfTypes = () => [
  'invoice_pdf',
  'credit_note_pdf',
  'pre_reminder_pdf',
  'first_reminder_pdf',
  'second_reminder_pdf',
  'collection_notice_pdf',
  'landlord_invoice_pdf',
  'landlord_pre_reminder_pdf',
  'landlord_first_reminder_pdf',
  'landlord_second_reminder_pdf',
  'landlord_collection_notice_pdf',
  'landlord_credit_note_pdf',
  'correction_invoice_pdf',
  'app_invoice_pdf'
]

export const getAttachmentPdfTypes = () => [
  'invoice_attachment_pdf',
  'credit_note_attachment_pdf',
  'pre_reminder_attachment_pdf',
  'first_reminder_attachment_pdf',
  'second_reminder_attachment_pdf',
  'collection_notice_attachment_pdf',
  'eviction_notice_attachment_pdf',
  'eviction_due_reminder_notice_attachment_pdf',
  'email_attachment_pdf',
  'landlord_invoice_attachment_pdf',
  'landlord_pre_reminder_attachment_pdf',
  'landlord_first_reminder_attachment_pdf',
  'landlord_second_reminder_attachment_pdf',
  'landlord_collection_notice_attachment_pdf',
  'landlord_credit_note_attachment_pdf'
]

export const getPdfTypes = function () {
  let types = getInvoiceMainPdfTypes()

  types = union(types, [
    'assignment_pdf',
    'lease_pdf',
    'esigning_assignment_pdf',
    'esigning_contract_insurance_pdf',
    'esigning_lease_pdf',
    'esigning_moving_in_pdf',
    'esigning_moving_out_pdf',
    'lease_statement_pdf',
    'eviction_document_pdf',
    'payouts_approval_esigning_pdf',
    'payments_approval_esigning_pdf',
    'esigning_deposit_insurance_pdf'
  ])

  types = union(types, getAttachmentPdfTypes)

  return types
}

export const getFileKey = (fileData) => {
  if (!size(fileData)) return false
  const directive = appHelper.getFileDirective(fileData.directive)
  if (!size(directive)) return false
  if (!(fileData._id || fileData.name)) return false

  let key = directive.folder + '/'
  const invoiceMainPdfTypes = getInvoiceMainPdfTypes()
  const attachmentPdfTypes = getAttachmentPdfTypes()

  if (fileData.context === 'export_to_email') {
    if (fileData.partnerId) key += fileData.partnerId + '/'
    key += fileData.context + '/' + fileData._id + '/'
  } else if (fileData.context === 'interest_form') {
    if (fileData.partnerId) key += fileData.partnerId + '/'
    key += fileData.context + '/'
  } else if (
    fileData.type &&
    indexOf(invoiceMainPdfTypes, fileData.type) !== -1
  ) {
    if (fileData.partnerId) key += fileData.partnerId + '/'

    if (fileData.type === 'correction_invoice_pdf') key += 'expense/'
    else key += 'invoice/'

    if (fileData.type === 'invoice_pdf' || fileData.type === 'credit_note_pdf')
      key += 'rent_invoice/'
    if (fileData.type === 'app_invoice_pdf') key += 'deposit_insurance/'
    if (
      fileData.type === 'landlord_invoice_pdf' ||
      fileData.type === 'landlord_credit_note_pdf'
    )
      key += 'landlord_invoice/'
    if (
      fileData.type === 'pre_reminder_pdf' ||
      fileData.type === 'landlord_pre_reminder_pdf'
    )
      key += 'pre_reminder/'
    if (
      fileData.type === 'first_reminder_pdf' ||
      fileData.type === 'landlord_first_reminder_pdf'
    )
      key += 'first_reminder/'
    if (
      fileData.type === 'second_reminder_pdf' ||
      fileData.type === 'landlord_second_reminder_pdf'
    )
      key += 'second_reminder/'
    if (
      fileData.type === 'collection_notice_pdf' ||
      fileData.type === 'landlord_collection_notice_pdf'
    )
      key += 'collection_notice/'
  } else if (
    fileData.type === 'esigning_assignment_pdf' ||
    fileData.type === 'esigning_lease_pdf' ||
    fileData.type === 'lease_pdf'
  ) {
    key += fileData.partnerId + '/'

    if (fileData.type === 'esigning_assignment_pdf')
      key += 'e_signing' + '/' + 'assignment' + '/'
    if (fileData.type === 'esigning_lease_pdf')
      key += 'e_signing' + '/' + 'lease' + '/'
    if (fileData.type === 'lease_pdf') key += 'lease' + '/'
  } else if (
    fileData.type === 'esigning_moving_in_pdf' ||
    fileData.type === 'esigning_moving_out_pdf'
  ) {
    key += fileData.partnerId + '/' + 'e_signing' + '/' + 'lease' + '/'
  } else if (
    fileData.type &&
    indexOf(attachmentPdfTypes, fileData.type) !== -1
  ) {
    if (fileData.partnerId) key += fileData.partnerId + '/'

    key += 'email_attachments/'
  } else if (
    fileData.type === 'moving_in_ics' ||
    fileData.type === 'moving_out_ics'
  ) {
    key += 'ics/'

    if (fileData.partnerId) key += fileData.partnerId + '/'

    key += 'attach_to_email/'
  } else if (fileData.type === 'lease_statement_pdf') {
    //Files/statement/partnerId/attach_to_email/fileId.pdf
    key += 'statement/'

    if (fileData.partnerId) key += fileData.partnerId + '/'
    key += 'attach_to_email/'
  } else if (fileData.type === 'deposit_account_contract_pdf') {
    //Files/partnerId/deposit_account_contracts/fileId.pdf
    if (fileData.partnerId) key += fileData.partnerId + '/'

    key += 'deposit_account_contracts/'
  } else if (fileData.type === 'esigning_deposit_insurance_pdf') {
    if (fileData.partnerId) key += fileData.partnerId + '/'

    key += 'esigning_deposit_insurance/'
  } else if (
    fileData.type === 'esigning_lease_xml' ||
    fileData.type === 'esigning_assignment_xml'
  ) {
    key +=
      fileData.partnerId + '/' + 'e_signing' + '/' + fileData.context + '/xml/'
  } else if (fileData.type === 'eviction_document_pdf') {
    key += fileData.partnerId + '/' + 'eviction_documents' + '/'
  } else if (
    indexOf(
      ['task', 'tenant', 'property', 'contract', 'account'],
      fileData.context
    ) !== -1
  ) {
    if (fileData.partnerId) key += fileData.partnerId + '/'

    key += fileData.context + '/'
  } else if (fileData.type === 'payouts_approval_esigning_pdf') {
    key += fileData.partnerId + '/' + 'e_signing' + '/' + 'payouts' + '/'
  }

  key += fileData.name

  return key
}

export const getFileImages = async (fileIds = [], fileSize = {}) => {
  const filesInfo = size(fileIds)
    ? await getFiles({ _id: { $in: fileIds } })
    : []

  const imagesPath = []

  if (size(filesInfo)) {
    for (const file of filesInfo) {
      const imgObj = {}
      imgObj.image_path = file.getFileImage(fileSize)
      imgObj.clickable_image_path = file.getFileImage()
      imagesPath.push(imgObj)
    }
  }

  return imagesPath
}

const getContractPipelineForFileQuery = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'contractInfo'
    }
  },
  {
    $unwind: {
      path: '$contractInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getAccountPipelineForFileQuery = () => [
  {
    $lookup: {
      from: 'accounts',
      localField: 'contractInfo.accountId',
      foreignField: '_id',
      as: 'accountInfo'
    }
  },
  {
    $unwind: {
      path: '$accountInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getMainTenantPipelineForFileQuery = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'contractInfo.rentalMeta.tenantId',
      foreignField: '_id',
      as: 'mainTenant'
    }
  },
  {
    $unwind: {
      path: '$mainTenant',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      tenantName: {
        $ifNull: ['$mainTenant.name', '']
      }
    }
  }
]

const getCoTenantPipelineForFileQuery = () => [
  {
    $addFields: {
      coTenants: {
        $cond: [
          { $eq: ['$contractInfo.rentalMeta.enabledJointlyLiable', true] },
          {
            $filter: {
              input: { $ifNull: ['$contractInfo.rentalMeta.tenants', []] },
              as: 'tenant',
              cond: {
                $not: {
                  $eq: [
                    '$$tenant.tenantId',
                    '$contractInfo.rentalMeta.tenantId'
                  ]
                }
              }
            }
          },
          []
        ]
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'coTenants.tenantId',
      foreignField: '_id',
      as: 'coTenantsInfo'
    }
  },
  {
    $addFields: {
      coTenantsName: {
        $reduce: {
          input: { $ifNull: ['$coTenantsInfo', []] },
          initialValue: '',
          in: {
            $concat: ['$$value', ' - ', '$$this.name']
          }
        }
      }
    }
  },
  {
    $addFields: {
      coTenantsName: {
        $cond: [
          { $not: { $eq: ['$coTenantsName', ''] } },
          { $substr: ['$coTenantsName', 3, -1] },
          null
        ]
      }
    }
  }
]

const getFinalProjectPipelineForFileQuery = (dateFormat, timeZone) => [
  {
    $addFields: {
      allTenantName: {
        $concat: [
          '$tenantName',
          {
            $cond: [
              { $ifNull: ['$coTenantsName', false] },
              { $concat: [' - ', '$coTenantsName'] },
              ''
            ]
          }
        ]
      },
      fileTitle: {
        $switch: {
          branches: [
            {
              case: { $eq: ['$type', 'esigning_assignment_pdf'] },
              then: {
                $concat: ['Assignment', ' - ', '$accountInfo.name']
              }
            },
            {
              case: { $eq: ['$type', 'esigning_lease_pdf'] },
              then: 'Lease'
            },
            {
              case: { $eq: ['$type', 'esigning_moving_in_pdf'] },
              then: 'Moving in'
            },
            {
              case: { $eq: ['$type', 'esigning_moving_out_pdf'] },
              then: 'Moving out'
            },
            {
              case: { $eq: ['$type', 'deposit_account_contract_pdf'] },
              then: 'Deposit contract'
            }
          ],
          default: null
        }
      }
    }
  },
  {
    $project: {
      _id: 1,
      title: {
        $cond: [
          {
            $and: [
              { $not: { $eq: ['$type', 'esigning_assignment_pdf'] } },
              { $ifNull: ['$fileTitle', false] }
            ]
          },
          {
            $concat: ['$fileTitle', ' - ', '$allTenantName']
          },
          {
            $cond: [{ $ifNull: ['$fileTitle', false] }, '$fileTitle', '$title']
          }
        ]
      },
      uploadedAt: {
        $dateToString: {
          date: '$createdAt',
          format: dateFormat,
          timezone: timeZone
        }
      }
    }
  }
]

export const getFileForQueryForPublicApp = async (params) => {
  const { body, dateFormat, timeZone } = params
  const { options, preparedQuery } = body
  const { limit, skip, sort } = options
  const files = await FileCollection.aggregate([
    {
      $match: preparedQuery
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
    ...getContractPipelineForFileQuery(),
    ...getAccountPipelineForFileQuery(),
    ...getMainTenantPipelineForFileQuery(),
    ...getCoTenantPipelineForFileQuery(),
    ...getFinalProjectPipelineForFileQuery(dateFormat, timeZone)
  ])
  return files
}

export const prepareLeaseProtocolFileQuery = async (
  query,
  user,
  partnerSettings = {}
) => {
  const { roles = [], userId } = user
  query.userId = userId

  const isTenantDashboard = roles.includes('partner_tenant')
  const isLandlordDashboard = roles.includes('partner_landlord')
  const { leaseSetting = {}, assignmentSettings = {} } = partnerSettings
  const isShowFilesToTenant =
    leaseSetting?.enabledShowLeaseFilesToTenant || false
  const isShowFilesToLandlord =
    assignmentSettings?.enabledShowAssignmentFilesToLandlord || false

  if (isTenantDashboard && isShowFilesToTenant) {
    return await prepareTenantDashboardFileQuery(query)
  } else if (isTenantDashboard && !isShowFilesToTenant) {
    return { _id: 'nothing' }
  } else if (isLandlordDashboard && isShowFilesToLandlord) {
    return await prepareLandlordDashboardFileQuery(isShowFilesToTenant, query)
  } else if (isLandlordDashboard && !isShowFilesToLandlord) {
    return { _id: 'nothing' }
  }
}

export const prepareTenantDashboardFileQuery = async (params) => {
  const { contractId, partnerId } = params
  const movingIds = await propertyItemHelper.getPropertyItemIdsByQuery({
    contractId
  })
  const query = {
    partnerId,
    $and: [
      {
        $or: [
          { assignmentSerial: { $exists: false }, isVisibleToTenant: true },
          {
            assignmentSerial: { $exists: false },
            context: { $in: ['deposit_accounts', 'moving_in', 'moving_out'] }
          }
        ]
      },
      {
        $or: [
          {
            context: 'contract',
            isVisibleToTenant: true,
            assignmentSerial: { $exists: false }
          },
          {
            $and: [
              {
                context: {
                  $in: ['lease', 'moving_in', 'moving_out', 'deposit_accounts']
                }
              },
              {
                type: {
                  $nin: [
                    'assignment_pdf',
                    'esigning_assignment_xml',
                    'esigning_lease_xml'
                  ]
                }
              }
            ]
          }
        ]
      },
      {
        $or: [{ movingId: { $in: movingIds } }, { contractId }]
      }
    ]
  }
  return query
}

export const prepareLandlordDashboardFileQuery = async (
  isShowFilesToTenant,
  params
) => {
  const { contractId, partnerId } = params
  const query = { partnerId }
  const fileAndQuery = []
  const andQuery = []
  const landlordFileAndQuery = []
  const landlordAndQuery = []
  let allowedContext = ['assignment', 'lease']
  const movingIds = await propertyItemHelper.getPropertyItemIdsByQuery({
    contractId
  })

  landlordFileAndQuery.push({
    context: { $in: ['contract', 'lease'] },
    isVisibleToLandlord: true
  })
  landlordAndQuery.push({
    isVisibleToLandlord: { $ne: false },
    type: {
      $nin: ['lease_pdf', 'esigning_assignment_xml', 'esigning_lease_xml']
    }
  })

  // If tenant can see files then landlord will see too
  if (isShowFilesToTenant) {
    allowedContext = [
      'assignment',
      'lease',
      'moving_in',
      'moving_out',
      'deposit_accounts'
    ]

    landlordFileAndQuery.push({
      context: 'contract',
      isVisibleToTenant: true,
      assignmentSerial: { $exists: false }
    })

    landlordAndQuery.push({
      $or: [
        { assignmentSerial: { $exists: false }, isVisibleToTenant: true },
        {
          assignmentSerial: { $exists: false },
          context: { $in: ['moving_in', 'moving_out'] }
        }
      ]
    })
  }

  fileAndQuery.push({ $or: landlordFileAndQuery })
  andQuery.push({ $or: landlordAndQuery })

  // preparing the context to publish the files with proper view access
  fileAndQuery.push({
    $and: [
      { context: { $in: allowedContext } },
      {
        type: {
          $nin: ['esigning_assignment_xml', 'esigning_lease_xml']
        }
      }
    ]
  })

  //set the and query to show the which has the context and matches the property/contract/account/tenant/moving-in ids.
  andQuery.push({ $or: fileAndQuery })
  andQuery.push({
    $or: [{ movingId: { $in: movingIds } }, { contractId }]
  })

  if (size(andQuery)) {
    query['$and'] = andQuery
  }
  return query
}

export const queryFilesForPublicApp = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body
  appHelper.checkRequiredFields(['contractId'], query)
  appHelper.validateSortForQuery(options.sort)
  const { partnerId } = user
  const contractInfo = await contractHelper.getAContract({
    _id: query.contractId,
    partnerId
  })
  if (!contractInfo) throw new CustomError(404, 'Contract not found')
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  query.partnerId = partnerId
  // TODO:: Need to write test cases for prepare query
  body.preparedQuery = await prepareLeaseProtocolFileQuery(
    query,
    user,
    partnerSetting
  )
  const files = await getFileForQueryForPublicApp({
    body,
    dateFormat,
    timeZone
  })
  const filteredDocuments = await countFiles(body.preparedQuery)
  const totalDocuments = await countFiles({
    partnerId
  })
  return {
    data: files,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const countFiles = async (query, session) => {
  const noOfFiles = await FileCollection.countDocuments(query).session(session)
  return noOfFiles
}

export const getS3PdfFileUrl = async (file, isDownloadAble) => {
  if (!size(file)) {
    throw new CustomError(404, 'File not found')
  }
  const key = getFileKey(file)
  const url = await getS3SignedUrl(
    key,
    process.env.S3_BUCKET,
    null,
    !!isDownloadAble
  )
  return url
}

export const queryFileDownloadUrl = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['fileId'], body)
  const { fileId, isDownloadAble } = body
  appHelper.validateId({ fileId })
  const file = await getAFile({ _id: fileId })
  return await getS3PdfFileUrl(file, isDownloadAble)
}

export const queryConversationFileDownloadUrl = async (req) => {
  const { body, user } = req
  const { userId, partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['conversationId', 'fileName'], body)
  const { conversationId, fileName } = body
  const query = {
    conversationId,
    data: { userId }
  }
  if (partnerId) query.data.partnerId = partnerId
  const conversation = await conversationHelper.getAConversation({
    _id: conversationId
  })

  if (!size(conversation)) throw new CustomError(400, 'Conversation not found')
  const hasConversationAccess = await conversationHelper.getConversationInfo(
    query
  )

  if (!size(hasConversationAccess))
    throw new CustomError(
      400,
      "You don't have the right permission to access the file"
    )

  const { folder = '' } = settingJson.S3.Directives['Conversations']
  const bucket = process.env.S3_BUCKET || 'uninite-com-local'
  const url = await getS3SignedUrl(
    folder + '/' + conversationId + '/' + fileName,
    bucket
  )
  return url
}

export const validateTokenAndGetFileDownloadUrl = async (req) => {
  const { body = {} } = req
  const { token = '' } = body
  if (!size(token)) {
    throw new CustomError(400, 'Token is required!')
  }

  const fileInfo = await getAFile({
    fileUrlHash: token
  })
  if (!size(fileInfo)) {
    throw new CustomError(404, 'Could not find file info with fileId!')
  }

  const downloadURL = (await getFileUrl(fileInfo)) || null
  return downloadURL
}

export const getFileUrl = async (file, expires = 0) => {
  try {
    if (size(file)) {
      const key = getFileKey(file)

      if (!key) {
        return false
      }

      let download = false
      if (
        file.type === 'assignment_pdf' ||
        file.type === 'lease_pdf' ||
        file.type === 'esigning_assignment_xml' ||
        file.type === 'esigning_lease_xml'
      ) {
        download = true
      }

      const s3SignedURL = await getS3SignedUrl(key, bucket, expires, download)
      return s3SignedURL
    }
  } catch (err) {
    throw new CustomError(
      500,
      `Internal server error when generating file URL, error: ${err?.message}`
    )
  }
}

export const queryImportErrorExcelFileUrl = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['importRefId', 'importCollectionName'], body)

  const { importRefId, importCollectionName } = body
  appHelper.validateId({ importRefId })
  const file = await getAFile({ importRefId, importCollectionName }, session, {
    createdAt: -1
  })
  return await getS3PdfFileUrl(file, true)
}

export const getRemovedMaxWidthContent = (content) => {
  content = content.replace(/604px;/gi, ' ')
  content = content.replace(/804px;/gi, ' ')
  return content
}

export const getPdfFileDirectiveName = function (type) {
  const types = getPdfTypes()

  if (type && indexOf(types, type) !== -1) return 'Files'

  return ''
}

export const prepareFileDataForPdfGeneration = async (params = {}) => {
  const { context, partnerId, type } = params
  const directiveName = getPdfFileDirectiveName(type)
  if (!directiveName) throw new CustomError(404, 'Invalid directive name!')

  const directive = appHelper.getFileDirective(directiveName)
  if (!directive) throw new CustomError(404, 'Invalid directive!')

  const invoiceMainPdfTypes = getInvoiceMainPdfTypes()
  const types = [...invoiceMainPdfTypes, ...getAttachmentPdfTypes()]
  const fileId = nid(17)

  let fileName = `${fileId}.pdf`
  if (indexOf(invoiceMainPdfTypes, type) !== -1 && params?.invoiceSerialId)
    fileName = params.invoiceSerialId + '_' + fileId + '.pdf'

  let title = fileName
  if (type === 'lease_pdf') title = 'lease'
  else if (type === 'assignment_pdf') title = 'assignment'
  else if (type === 'lease_statement_pdf') {
    title = 'statement'
    types.push('lease_statement_pdf')
  }

  let isVisibleToLandlord
  let isVisibleToTenant
  if (context === 'assignment' || context === 'lease') {
    const partnerSettingsInfo =
      (await partnerSettingHelper.getSettingByPartnerId(partnerId)) || {}
    const { assignmentSettings, leaseSetting } = partnerSettingsInfo
    const {
      enableEsignAssignment: enabledAssignmentESigning = false,
      enabledShowAssignmentFilesToLandlord = false
    } = assignmentSettings || {}
    const {
      enableEsignLease: enabledLeaseESigning = false,
      enabledShowLeaseFilesToTenant = false
    } = leaseSetting || {}
    isVisibleToLandlord =
      context === 'assignment' &&
      enabledAssignmentESigning &&
      enabledShowAssignmentFilesToLandlord
        ? true
        : undefined
    isVisibleToTenant =
      context === '' && enabledLeaseESigning && enabledShowLeaseFilesToTenant
        ? true
        : undefined
  }
  console.log('Checking params before insert: ', params)
  return {
    _id: fileId,
    accountId: params?.accountId || undefined,
    agentId: params?.agentId || undefined,
    annualStatementId: params?.annualStatementId || undefined,
    assignmentSerial: params?.assignmentSerial || undefined,
    attachmentId: params?.attachmentId || undefined,
    context,
    contractId: params?.contractId || undefined,
    createdAt: new Date(),
    createdBy: params?.userId || undefined,
    directRemittanceApprovalUserIds:
      params?.directRemittanceApprovalUserIds || undefined,
    directive: directiveName,
    eventStatus: 'created',
    fileUrlHash: nid(30),
    jsonFileName: `${fileId}.json`,
    isLeasePdf: params?.isLeasePdf || undefined,
    isAssignmentPdf: params?.isAssignmentPdf || undefined,
    isEvictionDocumentPdf: params?.isEvictionDocumentPdf || undefined,
    isVisibleToLandlord,
    isVisibleToTenant,
    leaseSerial: params?.leaseSerial || undefined,
    name: fileName,
    invoiceId: params?.invoiceId || undefined,
    movingId: params?.movingId || undefined,
    notificationLogId: params?.notificationLogId || undefined,
    partnerId,
    partnerPayoutId: params?.partnerPayoutId || undefined,
    propertyId: params?.propertyId || undefined,
    size: 0,
    tenantId: params?.tenantId || undefined,
    title,
    type
  }
}

export const prepareFileDataAndLogDataForAddFileFromUI = async (
  fileData,
  session
) => {
  delete fileData.subContext
  const leaseSerial = fileData.leaseSerial || ''
  const assignmentSerial = fileData.assignmentSerial || ''
  let partnerId
  const landlordPartnerId = fileData.landlordPartnerId
  const tenantPartnerId = fileData.tenantPartnerId
  if (landlordPartnerId || tenantPartnerId) {
    delete fileData.partnerId
  } else {
    partnerId = fileData.partnerId
  }
  const allowedTaskContext = [
    'task',
    'property',
    'account',
    'tenant',
    'landlordDashboard',
    'tenantDashboard'
  ]
  let options = {}
  //Set accountId in uploaded data, when uploaded file from properties
  if (
    size(fileData) &&
    fileData.propertyId &&
    !size(fileData.propertyFileType)
  ) {
    const property = await listingHelper.getListingById(fileData.propertyId)
    if (size(property) && property.accountId)
      fileData['accountId'] = property.accountId
  }
  let createLog = false
  if (indexOf(allowedTaskContext, fileData.context) !== -1) {
    options = {
      context: fileData.context,
      fileTitle: fileData.title
    }

    if (landlordPartnerId) options.landlordPartnerId = landlordPartnerId
    if (tenantPartnerId) options.tenantPartnerId = tenantPartnerId
    if (partnerId) options.partnerId = partnerId
    if (fileData.propertyId) options.propertyId = fileData.propertyId
    if (fileData.taskId) options.taskId = fileData.taskId
    if (fileData.accountId) options.accountId = fileData.accountId
    if (fileData.tenantId) options.tenantId = fileData.tenantId
    if (fileData.contractId) options.contractId = fileData.contractId
    createLog = true
  } else if (
    fileData.context === 'contract' &&
    (leaseSerial || assignmentSerial)
  ) {
    options = {
      partnerId: fileData.partnerId,
      contractId: fileData.contractId,
      accountId: fileData.accountId,
      propertyId: fileData.propertyId,
      fileTitle: fileData.title,
      context: 'contract'
    }

    if (assignmentSerial) options.assignmentSerial = assignmentSerial
    if (leaseSerial) options.leaseSerial = leaseSerial
    createLog = true
  } else if (
    fileData.context === 'moving_in_out' &&
    size(fileData.propertyFileType)
  ) {
    const requiredFields = [
      'context',
      'directive',
      'name',
      'partnerId',
      'size',
      'title'
    ]
    await validationForMovingInOutAddFiles(
      [...requiredFields, 'from', 'propertyFileType', 'propertyId'],
      fileData,
      session
    )
    fileData = pick(fileData, requiredFields)
  }
  return { createLog, fileData, options }
}

const validationForMovingInOutAddFiles = async (
  requiredFields,
  fileData,
  session
) => {
  const { from, partnerId, propertyId, propertyItemId, propertyFileType } =
    fileData
  if (['inventory', 'keys', 'meterReading'].includes(propertyFileType)) {
    appHelper.checkRequiredFields(requiredFields, fileData)
    let query = {}
    if (from === 'moving_in' || from === 'moving_out') {
      appHelper.checkRequiredFields(['propertyItemId'], fileData)
      query = {
        _id: propertyItemId,
        partnerId
      }
    }
    if (from === 'property') {
      query = {
        partnerId,
        propertyId,
        contractId: { $exists: false },
        type: { $exists: false }
      }
    }
    const propertyItemInfo = await propertyItemHelper.getAPropertyItem(query)
    if (from === 'property' && !propertyItemInfo) {
      await propertyItemService.createAPropertyItem(
        {
          partnerId,
          propertyId
        },
        session
      )
    } else if (!propertyItemInfo)
      throw new CustomError(404, 'Property item not found')
  }
}

export const preparePropertyFilesDataForMovingInOut = async (params, file) => {
  const {
    from,
    partnerId,
    propertyId,
    propertyItemId,
    propertyFileType,
    roomId
  } = params
  let query = {}
  const updateData = {}

  if (propertyFileType === 'inventory') {
    updateData['$push'] = {
      'inventory.files': file._id
    }
  } else if (propertyFileType === 'keys') {
    updateData['$push'] = {
      'keys.files': file._id
    }
  } else if (propertyFileType === 'meterReading') {
    updateData['$push'] = {
      'meterReading.files': file._id
    }
  }
  const isPropertyItem = ['inventory', 'keys', 'meterReading'].includes(
    propertyFileType
  )
  if ((from === 'moving_in' || from === 'moving_out') && isPropertyItem) {
    query = {
      _id: propertyItemId,
      partnerId
    }
  }
  if (from === 'property' && isPropertyItem) {
    query = {
      partnerId,
      propertyId,
      contractId: { $exists: false },
      type: { $exists: false }
    }
  }
  if (propertyFileType === 'rooms') {
    appHelper.checkRequiredFields(['roomId'], params)
    query = {
      _id: roomId,
      partnerId
    }
    updateData['$push'] = {
      files: file._id
    }
  }
  return { query, updateData }
}

export const prepareLogDataForUploadedFile = (options) => {
  const metaData = []
  const logData = pick(options, [
    'accountId',
    'context',
    'contractId',
    'createdBy',
    'fileId',
    'landlordPartnerId',
    'partnerId',
    'propertyId',
    'taskId',
    'tenantId',
    'tenantPartnerId'
  ])

  if (options.leaseSerial)
    metaData.push({ field: 'leaseSerial', value: options.leaseSerial })
  if (options.assignmentSerial)
    metaData.push({
      field: 'assignmentSerial',
      value: options.assignmentSerial
    })

  logData.visibility = logHelper.getLogVisibility(options, logData)
  if (options.fileTitle)
    metaData.push({ field: 'fileName', value: options.fileTitle })
  if (options.action) logData.action = options.action

  if (size(metaData)) logData.meta = metaData
  return logData
}

export const getFileUploadParamsAndOptions = (params) => {
  const {
    directive,
    existingFileName,
    fileDirectory,
    partnerId,
    subFolder,
    type
  } = params
  const fileExtension = fileDirectory ? fileDirectory.split('.').pop() : 'pdf'
  const fileName = existingFileName || nid(17) + '.' + fileExtension

  const fileDirective = appHelper.getFileDirective(directive)

  if (!size(fileDirective)) return false

  let key = fileDirective.folder

  if (subFolder) key = key + '/' + subFolder
  if (partnerId) key = key + '/' + partnerId
  if (type) key = key + '/' + type

  key = key + '/' + fileName

  const fileParams = {
    ACL: fileDirective.acl,
    Bucket: process.env.S3_BUCKET || 'uninite-com-local',
    Key: key,
    Body: '',
    ContentType: 'application/pdf'
  }
  const fileOptions = {
    partSize: 10 * 1024 * 1024,
    queueSize: 1
  }

  return {
    params: fileParams,
    options: fileOptions,
    fileName
  }
}

const prepareQueryForGetFiles = async (params) => {
  const {
    accountId,
    context,
    contractId,
    partnerId,
    propertyId,
    taskId,
    tenantId,
    tenantUserId
  } = params
  let queryData = {}
  if (!context) throw new CustomError(400, 'Context must not be empty')
  if (accountId) queryData.accountId = accountId
  if (context) queryData.context = context
  if (contractId) queryData.contractId = contractId
  if (partnerId) queryData.partnerId = partnerId
  if (propertyId) queryData.propertyId = propertyId
  if (tenantId) queryData.tenantId = tenantId

  // Only for assignment
  let movingInInfo
  let movingOutInfo

  if (context === 'lease') {
    appHelper.checkRequiredFields(['contractId'], params)
    queryData.type = {
      $nin: [
        'esigning_assignment_xml',
        'esigning_assignment_pdf',
        'assignment_pdf'
      ]
    }
    queryData.$or = [
      { leaseSerial: { $exists: true } },
      { context: { $in: ['lease', 'deposit_accounts'] } }
    ]
    delete queryData.context
  } else if (context === 'assignment') {
    appHelper.checkRequiredFields(['contractId'], params)
    queryData.type = {
      $nin: [
        'esigning_lease_xml',
        'esigning_lease_pdf',
        'lease_pdf',
        'lease_statement_pdf'
      ]
    }
    queryData.leaseSerial = { $exists: false }
    queryData.context = {
      $in: ['assignment', 'contract']
    }
    const movingInfo = await propertyItemHelper.getPropertyItems({
      contractId,
      type: {
        $in: ['in', 'out']
      }
    })
    const movingIds = []
    for (const moving of movingInfo) {
      if (moving.type === 'in') {
        movingInInfo = moving
      } else if (moving.type === 'out') {
        movingOutInfo = moving
      }
      movingIds.push(moving._id)
    }
    if (size(movingIds)) {
      queryData.$or = [
        {
          contractId
        },
        { movingId: { $in: movingIds } }
      ]
      delete queryData.contractId
    }
  } else if (context === 'task') {
    appHelper.checkRequiredFields(['taskId'], params)
    queryData.taskId = taskId
  } else if (context === 'account') {
    appHelper.checkRequiredFields(['accountId'], params)
    delete queryData.context
  } else if (context === 'property') {
    appHelper.checkRequiredFields(['propertyId'], params)
    queryData.context = { $in: ['property', 'tenant'] }
    const orQuery = {
      context: {
        $in: [
          'assignment',
          'deposit_accounts',
          'deposit_insurance',
          'lease',
          'moving_in',
          'moving_out'
        ]
      }
    }
    const partnerUserFilesQuery = {
      context: 'contract',
      uploadedBy: {
        $exists: false
      }
    }
    if (queryData.contractId) {
      orQuery.contractId = queryData.contractId
      partnerUserFilesQuery.contractId = queryData.contractId
    }
    delete queryData.propertyId
    queryData = {
      propertyId,
      $or: [queryData, orQuery, partnerUserFilesQuery]
    }
  } else if (context === 'tenant') {
    appHelper.checkRequiredFields(['tenantId'], params)
  } else if (context === 'interest_form') {
    appHelper.checkRequiredFields(['propertyId', 'tenantUserId'], params)
    queryData.createdBy = tenantUserId
  }

  return { queryData, movingInInfo, movingOutInfo }
}

const userInfoPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'createdBy',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo')
]

const fileTitlePipeline = () => [
  {
    $addFields: {
      allTenantName: {
        $concat: [
          '$tenantName',
          {
            $cond: [
              { $ifNull: ['$coTenantsName', false] },
              { $concat: [' - ', '$coTenantsName'] },
              ''
            ]
          }
        ]
      },
      fileTitle: {
        $switch: {
          branches: [
            {
              case: { $eq: ['$type', 'esigning_assignment_pdf'] },
              then: {
                $concat: ['Assignment', ' - ', '$accountInfo.name']
              }
            },
            {
              case: { $eq: ['$type', 'esigning_lease_pdf'] },
              then: 'Lease'
            },
            {
              case: { $eq: ['$type', 'esigning_moving_in_pdf'] },
              then: 'Moving in'
            },
            {
              case: { $eq: ['$type', 'esigning_moving_out_pdf'] },
              then: 'Moving out'
            },
            {
              case: { $eq: ['$type', 'deposit_account_contract_pdf'] },
              then: 'Deposit contract'
            }
          ],
          default: null
        }
      }
    }
  }
]

const getAssignmentFileFilterPipeline = (movingInInfo, movingOutInfo) => [
  {
    $addFields: {
      movingInInfo,
      movingOutInfo
    }
  },
  {
    $match: {
      $or: [
        {
          type: {
            $nin: [
              'esigning_assignment_pdf',
              'esigning_moving_in_pdf',
              'esigning_moving_out_pdf'
            ]
          }
        },
        {
          type: 'esigning_assignment_pdf',
          'contractInfo.agentAssignmentSigningStatus.signed': true,
          'contractInfo.landlordAssignmentSigningStatus.signed': true,
          'contractInfo.draftAssignmentDoc': {
            $ne: true
          }
        },
        {
          type: 'esigning_moving_in_pdf',
          'movingInInfo.movingInSigningComplete': true,
          'movingInInfo.draftMovingInDoc': {
            $ne: true
          }
        },
        {
          type: 'esigning_moving_out_pdf',
          'movingOutInfo.movingOutSigningComplete': true,
          'movingOutInfo.draftMovingOutDoc': {
            $ne: true
          }
        }
      ]
    }
  }
]

const filesForDetailsPage = async (body, context) => {
  const { options, query, movingInInfo, movingOutInfo } = body
  const { limit, skip, sort } = options
  let pipelineHelper = []
  const assignmentFileFilterPipeline = []
  if (['assignment', 'lease', 'property'].includes(context)) {
    if (context === 'assignment') {
      assignmentFileFilterPipeline.push(...getContractPipelineForFileQuery())
      assignmentFileFilterPipeline.push(
        ...getAssignmentFileFilterPipeline(movingInInfo, movingOutInfo)
      )
    } else {
      pipelineHelper.push(...getContractPipelineForFileQuery())
    }
    pipelineHelper.push(...getAccountPipelineForFileQuery())
    pipelineHelper.push(...getMainTenantPipelineForFileQuery())
    pipelineHelper.push(...getCoTenantPipelineForFileQuery())
    pipelineHelper.push(...fileTitlePipeline())
    pipelineHelper.push({
      $addFields: {
        tenantsInfo: {
          $concatArrays: [['$mainTenant'], { $ifNull: ['$coTenantsInfo', []] }]
        }
      }
    })
  }
  pipelineHelper = [...pipelineHelper, ...userInfoPipeline()]
  const pipeline = [
    {
      $match: query
    },
    ...assignmentFileFilterPipeline,
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...pipelineHelper,
    {
      $addFields: {
        extension: {
          $last: {
            $split: ['$name', '.']
          }
        }
      }
    },
    {
      $addFields: {
        imageUrl: {
          $cond: [
            { $in: ['$extension', ['png', 'jpeg', 'jpg', 'gif']] },
            {
              $concat: [
                appHelper.getCDNDomain(),
                '/files',
                '/',
                '$partnerId',
                '/',
                '$context',
                '/',
                '$name'
              ]
            },
            null
          ]
        }
      }
    },
    {
      $project: {
        _id: 1,
        title: {
          $cond: [
            {
              $and: [
                { $not: { $eq: ['$type', 'esigning_assignment_pdf'] } },
                { $ifNull: ['$fileTitle', false] }
              ]
            },
            {
              $concat: ['$fileTitle', ' - ', '$allTenantName']
            },
            {
              $cond: [
                { $ifNull: ['$fileTitle', false] },
                '$fileTitle',
                '$title'
              ]
            }
          ]
        },
        name: 1,
        type: 1,
        accountInfo: {
          _id: 1,
          name: 1
        },
        tenantsInfo: {
          _id: 1,
          name: 1
        },
        isVisibleToTenant: 1,
        isVisibleToLandlord: 1,
        createdAt: 1,
        userInfo: 1,
        imageUrl: 1
      }
    }
  ]
  const files = await FileCollection.aggregate(pipeline)
  return files || []
}

export const getFilesForDetailPage = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { options, query } = body
  appHelper.checkRequiredFields(['context'], query)
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  const context = query.context
  const { queryData, movingInInfo, movingOutInfo } =
    await prepareQueryForGetFiles(query)
  body.query = queryData
  body.movingInInfo = movingInInfo
  body.movingOutInfo = movingOutInfo
  const files = await filesForDetailsPage(body, context)
  const filteredDocuments = await countFiles(body.query)
  const totalDocuments = filteredDocuments
  return { data: files, metaData: { filteredDocuments, totalDocuments } }
}

const projectByContext = (context) => {
  if (context === 'inventories')
    return {
      $project: {
        fileIds: '$inventory.files'
      }
    }

  if (context === 'keys')
    return {
      $project: {
        fileIds: '$keys.files'
      }
    }

  if (context === 'meterReading')
    return {
      $project: {
        fileIds: '$meterReading.files'
      }
    }
}

const getImagesForPropertyUtilityQuery = async (options, preparedQuery) => {
  const { limit, sort, skip } = options
  const { context } = preparedQuery
  const projectStage = projectByContext(context)
  delete preparedQuery.context
  delete preparedQuery.isFurnished

  const filesPipeline = [
    {
      $match: preparedQuery
    },
    projectStage,
    appHelper.getUnwindPipeline('fileIds', false),
    {
      $lookup: {
        from: 'files',
        localField: 'fileIds',
        foreignField: '_id',
        pipeline: [...appHelper.getFilesPathUrl()],
        as: 'fileInfo'
      }
    },
    appHelper.getUnwindPipeline('fileInfo'),
    {
      $sort: sort
    },
    {
      $skip: skip
    },

    {
      $limit: limit
    },
    {
      $project: {
        _id: 0,
        fileId: '$fileInfo._id',
        path: '$fileInfo.path'
      }
    }
  ]

  const utilities =
    (await PropertyItemCollection.aggregate(filesPipeline)) || []
  return utilities
}

const countFilesForInventory = async (query) => {
  const { context } = query

  const projectStage = projectByContext(context)
  delete query.context
  delete query.isFurnished

  const pipeline = [
    {
      $match: query
    },
    projectStage
  ]

  const [items = {}] = (await PropertyItemCollection.aggregate(pipeline)) || []
  let totalCount = 0
  if (items) totalCount = size(items.fileIds)

  return totalCount
}

const prepareQueryForImages = (query) => {
  const { propertyItemId } = query
  if (propertyItemId) {
    query._id = propertyItemId
    delete query.propertyItemId
  } else {
    query.contractId = { $exists: false }
    query.type = { $exists: false }
  }
  return query
}

export const queryImagesForPropertyDetailsUtility = async (req) => {
  const { body = {}, user = {} } = req
  const { query, options } = body
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['context', 'propertyId'], query)
  appHelper.validateSortForQuery(options.sort)
  const { partnerId } = user
  query.partnerId = partnerId
  const preparedQuery = await prepareQueryForImages(query)
  body.preparedQuery = preparedQuery
  const files = await getImagesForPropertyUtilityQuery(
    body.options,
    JSON.parse(JSON.stringify(preparedQuery))
  )
  const filteredDocuments = await countFilesForInventory(preparedQuery)
  const totalDocuments = filteredDocuments

  return {
    data: files,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getFilesWithSelectedFields = async (query, select = []) => {
  const files = await FileCollection.find(query).select(select)
  return files
}

export const getAFileForLambda = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  if (!size(body)) throw new CustomError(400, "QueryData can't be empty")
  const { fileId, statusWithNotProcessed } = body
  if (statusWithNotProcessed) {
    delete body.statusWithNotProcessed
    body.status = { $ne: 'processed' }
  }
  if (fileId) {
    delete body.fileId
    body._id = fileId
  }

  return await getAFile(body)
}

export const prepareDataForRemoveFilesFromS3 = async (params) => {
  const { fileId } = params
  const fileQuery = { _id: fileId }
  const selectors = [
    'type',
    'partnerId',
    'context',
    'directive',
    'name',
    'status',
    'isFileInUse'
  ]
  const deletableFiles = await getFilesWithSelectedFields(fileQuery, selectors)
  if (!size(deletableFiles)) throw new CustomError(404, 'Files not found')

  const [fileInfo = {}] = deletableFiles
  if (fileInfo.isFileInUse) {
    throw new CustomError(
      405,
      'This file is currently in use, please try again after some time.'
    )
  }
  return deletableFiles
}

export const prepareDataForRemoveFileByContext = async (params) => {
  const {
    context,
    contractId,
    fileId,
    partnerId,
    propertyId,
    propertyItemId,
    roomId,
    subContext
  } = params
  const data = {}
  let query = {}
  if (context === 'property') {
    const isPropertyItem = ['inventory', 'keys', 'meterReading'].includes(
      subContext
    )
    if (isPropertyItem) {
      // propertyItemId should be pass if file remove from moving_in_or_out
      if (propertyItemId) {
        query = {
          _id: propertyItemId
        }
      } else {
        query = {
          partnerId,
          propertyId,
          contractId: { $exists: false },
          type: { $exists: false }
        }
      }
      if (subContext === 'inventory')
        data['$pull'] = {
          'inventory.files': fileId
        }
      if (subContext === 'keys')
        data['$pull'] = {
          'keys.files': fileId
        }
      if (subContext === 'meterReading')
        data['$pull'] = {
          'meterReading.files': fileId
        }
    }
  }
  if (context === 'accounts' || context === 'contract') {
    query = {
      _id: contractId,
      propertyId
    }
    data['$pull'] = {
      files: {
        fileId
      }
    }
  }
  if (context === 'propertyRoom') {
    appHelper.checkRequiredFields(['roomId'], params)
    query = {
      _id: roomId,
      partnerId
    }
    data['$pull'] = {
      files: fileId
    }
  }
  return { data, query }
}

export const prepareLogDataForRemoveFile = (file, params) => {
  const { userId } = params
  const {
    accountId,
    context,
    contractId,
    directive,
    landlordPartnerId,
    tenantPartnerId,
    partnerId,
    propertyId,
    taskId,
    tenantId,
    title
  } = file
  const logData = {
    action: 'removed_file',
    context,
    createdBy: userId,
    partnerId
  }
  let metaData = []

  if (accountId) logData.accountId = accountId
  if (contractId) logData.contractId = contractId
  if (landlordPartnerId) logData.landlordPartnerId = landlordPartnerId
  if (tenantPartnerId) logData.tenantPartnerId = landlordPartnerId
  if (propertyId) logData.propertyId = propertyId
  if (taskId) logData.taskId = taskId
  if (tenantId) logData.tenantId = tenantId
  if (directive === 'Files' && title) {
    metaData = [
      {
        field: 'fileName',
        value: title
      }
    ]
  }
  if (size(metaData)) logData.meta = metaData
  const options = { context }
  logData.visibility = logHelper.getLogVisibility(options, file)
  return logData
}
