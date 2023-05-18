import { size } from 'lodash'
import { AppInvoiceCollection } from '../models'
import { appHelper, settingHelper } from '../helpers'
import { counterService } from '../services'

export const getAppInvoice = async (query, session) => {
  const appInvoice = await AppInvoiceCollection.findOne(query).session(session)
  return appInvoice
}

export const getAppInvoices = async (query, session) => {
  const appInvoices = await AppInvoiceCollection.find(query).session(session)
  return appInvoices
}

export const getUniqueFieldValue = async (field, query) =>
  (await AppInvoiceCollection.distinct(field, query)) || []

export const countAppInvoices = async (query, session) =>
  await AppInvoiceCollection.countDocuments(query).session(session)

const getPropertyPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            apartmentId: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              postalCode: 1
            }
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  {
    $unwind: {
      path: '$propertyInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getDepositInsurancePipeline = () => [
  {
    $lookup: {
      from: 'deposit_insurance',
      localField: 'depositInsuranceId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            referenceNo: '$creationResult.insuranceNo'
          }
        }
      ],
      as: 'depositInsuranceInfo'
    }
  },
  {
    $unwind: {
      path: '$depositInsuranceInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getInvoiceContentPipeline = () => [
  {
    $addFields: {
      invoiceContent: {
        $filter: {
          input: { $ifNull: ['$invoiceContent', []] },
          as: 'content',
          cond: {
            $eq: ['$$content.type', 'deposit_insurance']
          }
        }
      }
    }
  }
]

const getFileIdsPipeline = () => [
  {
    $addFields: {
      appInvoicePdf: {
        $first: {
          $filter: {
            input: { $ifNull: ['$pdf', []] },
            as: 'pdfContent',
            cond: {
              $eq: ['$$pdfContent.type', 'app_invoice_pdf']
            }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'files',
      localField: 'contractId',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$type', 'esigning_deposit_insurance_pdf']
            }
          }
        },
        {
          $limit: 1
        }
      ],
      as: 'signedDocumentInfo'
    }
  },
  {
    $unwind: {
      path: '$signedDocumentInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      signedDocumentFileId: '$signedDocumentInfo._id',
      appInvoicePdfFileId: '$appInvoicePdf.fileId'
    }
  }
]

const getPartnerPipeline = () => [
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $addFields: {
      partner: { $first: '$partner' }
    }
  }
]

const getAppInvoiceDetails = async (query) => {
  const pipeline = [
    {
      $match: query
    },
    ...getPropertyPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    ...getDepositInsurancePipeline(),
    ...getInvoiceContentPipeline(),
    ...getFileIdsPipeline(),
    ...getPartnerPipeline(),
    {
      $project: {
        _id: 1,
        serialId: 1,
        status: 1,
        partner: 1,
        propertyInfo: 1,
        tenantInfo: 1,
        createdAt: 1,
        dueDate: 1,
        invoiceAccountNumber: 1,
        isDefaulted: 1,
        isPartiallyPaid: 1,
        invoiceStartOn: 1,
        invoiceEndOn: 1,
        kidNumber: 1,
        referenceNo: '$depositInsuranceInfo.referenceNo',
        invoiceContent: {
          type: 1,
          total: 1
        },
        invoiceTotal: 1,
        totalPaid: 1,
        totalDue: {
          $subtract: [
            {
              $add: [
                { $ifNull: ['$invoiceTotal', 0] },
                { $ifNull: ['$creditedAmount', 0] }
              ]
            },
            {
              $add: [
                { $ifNull: ['$totalPaid', 0] },
                { $ifNull: ['$lostMeta.amount', 0] }
              ]
            }
          ]
        },
        appInvoicePdfFileId: 1,
        signedDocumentFileId: 1
      }
    }
  ]
  const [appInvoiceDetails = {}] =
    (await AppInvoiceCollection.aggregate(pipeline)) || []
  return appInvoiceDetails
}

export const appInvoiceDetails = async (req) => {
  const { body, user } = req
  const { roles = [], partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['appInvoiceId'], body)
  const { appInvoiceId } = body
  const appInvoiceQuery = {
    _id: appInvoiceId
  }
  if (!roles.includes('app_admin')) {
    appHelper.checkRequiredFields(['partnerId'], user)
    appInvoiceQuery.partnerId = partnerId
  }
  return await getAppInvoiceDetails(appInvoiceQuery)
}

const getAppInvoiceByAggregate = async (appInvoiceId) => {
  const pipeLine = [
    {
      $match: {
        _id: appInvoiceId
      }
    },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
        foreignField: '_id',
        pipeline: [
          {
            $lookup: {
              from: 'users',
              localField: 'userId',
              foreignField: '_id',
              pipeline: [
                {
                  $project: {
                    profile: 1
                  }
                }
              ],
              as: 'userInfo'
            }
          },
          {
            $unwind: '$userInfo'
          }
        ],
        as: 'tenantInfo'
      }
    },
    {
      $unwind: '$tenantInfo'
    }
  ]
  console.log('checking pipeLine: ', pipeLine)
  const [appInvoice = {}] =
    (await AppInvoiceCollection.aggregate(pipeLine)) || []
  console.log('checking appInvoice: ', appInvoice)
  return appInvoice
}

export const getAppInvoiceDropdownDataForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const appInvoiceData = await AppInvoiceCollection.aggregate([
    {
      $match: query
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
    {
      $project: {
        _id: 1,
        serialId: 1,
        invoiceMonth: 1,
        invoiceTotal: 1,
        totalDue: {
          $add: [
            {
              $subtract: ['$invoiceTotal', '$totalPaid']
            },
            {
              $subtract: [
                { $ifNull: ['$creditedAmount', 0] },
                { $ifNull: ['$lostMeta.amount', 0] }
              ]
            }
          ]
        }
      }
    }
  ])
  return appInvoiceData
}

export const prepareQueryForAppInvoiceDropdown = async (query) => {
  const { searchKeyword } = query
  const preparedQuery = {}
  if (size(searchKeyword)) {
    const parsedKeyword = parseInt(searchKeyword)
    if (isNaN(parsedKeyword)) {
      preparedQuery.serialId = 0 // It will not match any data which will return []
    } else {
      preparedQuery.serialId = parsedKeyword
    }
  }
  return preparedQuery
}

export const appInvoiceDropdownQuery = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = await prepareQueryForAppInvoiceDropdown(query)
  const appInvoiceDropdownData = await getAppInvoiceDropdownDataForQuery(body)
  const filteredDocuments = await countAppInvoices(body.query)
  const totalDocuments = await countAppInvoices()
  return {
    data: appInvoiceDropdownData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const appInvoiceForLambda = async (req) => {
  console.log('checking appInvoiceForLambda: ')
  const { body } = req
  appHelper.checkRequiredFields(['appInvoiceId'], body)
  console.log('checking body: ', body)
  const { appInvoiceId = '' } = body
  // const appInvoice = getAppInvoice({ _id: appInvoiceId })
  const appInvoice = await getAppInvoiceByAggregate(appInvoiceId)
  console.log('checking app invoice before response: ', appInvoice)
  return appInvoice
}

const prepareInvoiceContent = ({ depositInsuranceAmount }) => [
  {
    type: 'deposit_insurance',
    price: depositInsuranceAmount,
    total: depositInsuranceAmount,
    taxPercentage: 0 // use existing method
  }
]

const getCompanyInfoFromSettings = (settings) => {
  const company = {
    companyName: 'Unite Living AS',
    orgId: '916 861 923'
  }
  let name = '',
    orgId = ''

  const { appInfo } = settings || {}
  const { companyName = '', organizationId = '' } = appInfo || {}
  if (appInfo && appInfo.companyName && appInfo.organizationId) {
    name = companyName
    orgId = organizationId
  }

  if (name) company.companyName = name
  if (orgId) company.orgId = orgId.replace(/ /g, '')

  return company
}

export const prepareAppInvoiceData = async (depositInsurance, contract) => {
  const { tenant, partnerSettings } = contract
  const {
    propertyId,
    tenantId,
    contractId,
    partnerId,
    accountId,
    _id,
    depositInsuranceAmount,
    kidNumber
  } = depositInsurance
  const invoiceContent = prepareInvoiceContent(depositInsurance)
  const appInvoice = {
    propertyId,
    tenantId,
    contractId,
    partnerId,
    accountId,
    depositInsuranceId: _id,
    status: 'created',
    depositInsuranceAmount,
    kidNumber,
    serialId: await counterService.incrementCounter('app-invoice'),
    totalPaid: 0,
    invoiceContent,
    invoiceTotal: depositInsuranceAmount,
    totalTAX: 0,
    isFirstInvoice: true,
    invoiceType: 'app_invoice'
  }
  // Due date will be 14 days before moving in date
  appInvoice.dueDate = (
    await appHelper.getActualDate(
      partnerSettings,
      true,
      contract?.rentalMeta?.movingInDate
    )
  )
    .startOf('day')
    .subtract(1, 'days')
    .toDate()
  const today = (
    await appHelper.getActualDate(partnerSettings, true, new Date())
  )
    .endOf('day')
    .toDate()
  const invoiceStartOn = (
    await appHelper.getActualDate(partnerSettings, true, today)
  )
    .startOf('month')
    .toDate()
  appInvoice.invoiceMonth = invoiceStartOn
  appInvoice.invoiceStartOn = invoiceStartOn
  appInvoice.invoiceEndOn = (
    await appHelper.getActualDate(partnerSettings, true, today)
  )
    .endOf('month')
    .toDate()
  const settings = await settingHelper.getSettingInfo()
  const { bankAccountNumber = '' } = settings || {}
  appInvoice.invoiceAccountNumber = bankAccountNumber
  appInvoice.receiver = {
    tenantName: tenant && tenant.name ? tenant.name : ''
  }
  appInvoice.sender = getCompanyInfoFromSettings(settings)
  return appInvoice
}
