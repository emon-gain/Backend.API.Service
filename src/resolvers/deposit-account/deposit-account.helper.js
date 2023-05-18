import { assign, cloneDeep, find, isString, isNumber, size } from 'lodash'
import nid from 'nid'

import {
  appHelper,
  contractHelper,
  depositAccountHelper,
  partnerHelper,
  tenantHelper
} from '../helpers'
import { ContractCollection, DepositAccountCollection } from '../models'
import { CustomError } from '../common'

export const fetchDataForDepositAccount = async (tenantId, contractId) => {
  const pipeline = fetchDepositAccountDataPipeline(contractId, tenantId)
  const contractData =
    (await contractHelper.getContractByAggregate(pipeline)) || {}
  console.log('Fetched contract data', JSON.stringify(contractData))
  return contractData
}

export const getUniqueDepositAccountField = async (field, query) =>
  (await DepositAccountCollection.distinct(field, query)) || []

const fetchDepositAccountDataPipeline = (contractId, tenantId) => {
  const unwindPath = (path) => ({
    path,
    preserveNullAndEmptyArrays: true
  })
  return [
    {
      $match: {
        _id: contractId
      }
    },
    {
      $project: {
        rentalMeta: 1,
        partnerId: 1,
        accountId: 1
      }
    },
    {
      $addFields: {
        tenantId
      }
    },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
        foreignField: '_id',
        as: 'tenants',
        pipeline: [
          {
            $lookup: {
              from: 'users',
              localField: 'userId',
              foreignField: '_id',
              as: 'user'
            }
          },
          {
            $unwind: unwindPath('$user')
          },
          {
            $addFields: {
              firstEmail: { $arrayElemAt: ['$user.emails', 0] },
              isFacebookUser: {
                $ifNull: ['$user.services.facebook', false]
              },
              verifiedEmail: {
                $first: {
                  $filter: {
                    input: '$user.emails',
                    as: 'item',
                    cond: { $eq: ['$$item.verified', true] }
                  }
                }
              }
            }
          },
          {
            $addFields: {
              'user.verifiedEmail': {
                $switch: {
                  branches: [
                    {
                      case: {
                        $eq: ['$verifiedEmail.verified', true]
                      },
                      then: '$verifiedEmail.address'
                    },
                    {
                      case: {
                        $ne: ['$isFacebookUser', false]
                      },
                      then: '$isFacebookUser.email'
                    }
                  ],
                  default: '$firstEmail.address'
                }
              }
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'files',
        localField: '_id',
        foreignField: 'contractId',
        as: 'files',
        let: {
          partnerId: '$partnerId'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$$partnerId', '$partnerId'] },
                  { $eq: ['$context', 'lease'] },
                  { $eq: ['$context', 'lease'] },
                  { $eq: ['$type', 'esigning_lease_pdf'] }
                ]
              }
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partner'
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        as: 'account',
        pipeline: [
          {
            $lookup: {
              from: 'organizations',
              localField: 'organizationId',
              foreignField: '_id',
              as: 'organization'
            }
          },
          {
            $addFields: {
              organization: {
                $first: '$organization'
              }
            }
          },
          {
            $lookup: {
              from: 'users',
              localField: 'personId',
              foreignField: '_id',
              as: 'user'
            }
          },
          {
            $unwind: unwindPath('$user')
          },
          {
            $addFields: {
              firstEmail: { $arrayElemAt: ['$user.emails', 0] },
              isFacebookUser: {
                $ifNull: ['$user.services.facebook', false]
              },
              verifiedEmail: {
                $first: {
                  $filter: {
                    input: '$user.emails',
                    as: 'item',
                    cond: { $eq: ['$$item.verified', true] }
                  }
                }
              },
              hometown: {
                $cond: {
                  if: { $eq: ['$type', 'person'] },
                  then: '$user.hometown',
                  else: '$address'
                }
              }
            }
          },
          {
            $addFields: {
              address: {
                $concat: [
                  '$hometown',
                  ', ',
                  {
                    $ifNull: ['$zipcode', '']
                  },
                  ', ',
                  {
                    $ifNull: ['$city', '']
                  },
                  ', ',
                  {
                    $ifNull: ['$country', '']
                  }
                ]
              },
              'user.verifiedEmail': {
                $switch: {
                  branches: [
                    {
                      case: {
                        $eq: ['$verifiedEmail.verified', true]
                      },
                      then: '$verifiedEmail.address'
                    },
                    {
                      case: {
                        $ne: ['$isFacebookUser', false]
                      },
                      then: '$isFacebookUser.email'
                    }
                  ],
                  default: '$firstEmail.address'
                }
              }
            }
          },
          {
            $addFields: {
              address: {
                $replaceAll: {
                  input: '$address',
                  find: ', , ',
                  replacement: ', '
                }
              }
            }
          }
        ]
      }
    },
    {
      $unwind: '$tenants'
    },
    {
      $addFields: {
        kycForm: {
          $first: {
            $filter: {
              input: '$tenants.depositAccountMeta.kycForms',
              as: 'item',
              cond: {
                $and: [
                  { $eq: ['$$item.contractId', '$_id'] },
                  { $eq: ['$$item.status', 'new'] }
                ]
              }
            }
          }
        }
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'tenants.userId',
        foreignField: '_id',
        as: 'user'
      }
    },
    {
      $unwind: '$partner'
    },
    {
      $unwind: '$account'
    },
    {
      $unwind: '$files'
    },
    {
      $unwind: '$user'
    },
    {
      $addFields: {
        depositAmount: '$rentalMeta.depositAmount',
        tenantLeaseSigningStatus: {
          $first: {
            $filter: {
              input: '$rentalMeta.tenantLeaseSigningStatus',
              as: 'item',
              cond: { $eq: ['$$item.tenantId', '$tenantId'] }
            }
          }
        },
        signerInfo: {
          $first: {
            $filter: {
              input: '$rentalMeta.leaseSigningMeta.signers',
              as: 'item',
              cond: { $eq: ['$$item.externalSignerId', '$tenantId'] }
            }
          }
        }
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        localField: 'partnerId',
        foreignField: 'partnerId',
        as: 'partnerSettings',
        pipeline: [
          {
            $project: {
              companyInfo: 1
            }
          }
        ]
      }
    },
    {
      $addFields: {
        rentalMeta: '$$REMOVE',
        attachmentFileId: '$tenantLeaseSigningStatus.attachmentFileId',
        partnerSettings: {
          $first: '$partnerSettings'
        }
      }
    },
    {
      $lookup: {
        from: 'files',
        localField: '_id',
        foreignField: 'contractId',
        as: 'xmlFile',
        let: {
          partnerId: '$partnerId',
          signerInfo: '$signerInfo'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$partnerId', '$$partnerId'] },
                  { $eq: ['$signerId', '$$signerInfo.id'] }
                ]
              }
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'files',
        localField: 'attachmentFileId',
        foreignField: '_id',
        as: 'attachmentFile'
      }
    },
    {
      $unwind: unwindPath('$xmlFile')
    },
    {
      $unwind: unwindPath('$attachmentFile')
    },
    {
      $lookup: {
        from: 'deposit_accounts',
        localField: '_id',
        foreignField: 'contractId',
        as: 'depositeAccount',
        let: {
          tenantId: '$tenantId'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$tenantId', '$$tenantId']
              }
            }
          }
        ]
      }
    },
    {
      $addFields: {
        companyInfo: '$partnerSettings.companyInfo',
        isDepositAccountExists: {
          $cond: {
            if: {
              $gt: [
                {
                  $size: '$depositeAccount'
                },
                0
              ]
            },
            then: true,
            else: false
          }
        },
        depositeAccount: '$$REMOVE'
      }
    }
  ]
}

export const getDepositAccount = async (query, session) => {
  const depositAccount = await DepositAccountCollection.findOne(query).session(
    session
  )
  return depositAccount
}

export const getDepositAccounts = async (query, session) => {
  const depositAccounts = await DepositAccountCollection.find(query).session(
    session
  )
  return depositAccounts
}

export const getTenantLeaseSigningUrl = async (contractId, tenantId) => {
  if (!(contractId && tenantId)) return false

  const contractData =
    (await contractHelper.getAContract({ _id: contractId })) || {}
  const tenantLeaseSigningStatus =
    contractData?.rentalMeta?.tenantLeaseSigningStatus
  const tenantLeaseSigningInfo =
    find(tenantLeaseSigningStatus, { tenantId }) || {}

  const { signingUrl = '' } = tenantLeaseSigningInfo

  return signingUrl
}

export const getTenantDepositKycData = async (params) => {
  const {
    contractId = '',
    options = {},
    partnerId = '',
    referenceNumber = 0,
    tenantId = ''
  } = params
  const tenantQuery = {}

  if (tenantId) tenantQuery._id = tenantId
  if (partnerId) tenantQuery.partnerId = partnerId
  if (contractId)
    tenantQuery['depositAccountMeta.kycForms'] = { $elemMatch: { contractId } }
  if (referenceNumber)
    tenantQuery['depositAccountMeta.kycForms'] = {
      $elemMatch: { referenceNumber }
    }

  const tenantInfo = await tenantHelper.getATenant(tenantQuery)

  if (!size(tenantInfo)) return false
  else {
    const { depositAccountMeta = {} } = tenantInfo
    const { kycForms = [] } = depositAccountMeta
    const kycFormData = size(kycForms)
      ? find(
          kycForms,
          (kycForm) =>
            kycForm.referenceNumber === referenceNumber ||
            (kycForm.contractId === contractId && kycForm.status === 'new')
        )
      : {}
    // Fetching and adding tenant lease signing URL
    const tenantLeaseSigningUrl =
      (await getTenantLeaseSigningUrl(
        kycFormData?.contractId,
        tenantInfo._id
      )) || ''
    let returnArrayData = assign(kycFormData, {
      partnerId: tenantInfo.partnerId,
      tenantId: tenantInfo._id,
      tenantLeaseSigningUrl
    })

    if (options && options.isKycForms)
      returnArrayData = { ...returnArrayData, kycForms }

    return returnArrayData
  }
}

const getPipelineForProperty = () => [
  {
    $lookup: {
      from: 'listings',
      let: { propertyId: '$propertyId' },
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            'location.name': 1,
            'location.city': 1,
            'location.country': 1,
            'location.postalCode': 1,
            apartmentId: 1,
            propertyTypeId: 1,
            listingTypeId: 1,
            serial: 1,
            imageUrl: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo')
]

export const getPipelineForSendToBank = () => [
  {
    $addFields: {
      trueTenantLeaseSigningStatuses: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'signingStatus',
          cond: {
            $eq: ['$$signingStatus.isSentDepositDataToBank', true]
          }
        }
      }
    }
  },
  {
    $addFields: {
      sendToBank: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$rentalMeta.enabledJointDepositAccount', true] },
                  { $gt: [{ $size: '$trueTenantLeaseSigningStatuses' }, 0] }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$rentalMeta.enabledJointDepositAccount', false] },
                  { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', false] },
                  {
                    $eq: [
                      { $size: '$trueTenantLeaseSigningStatuses' },
                      { $size: '$rentalMeta.tenantLeaseSigningStatus' }
                    ]
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      }
    }
  }
]

export const getStatusProject = () => ({
  $switch: {
    branches: [
      {
        case: {
          $and: [
            { $eq: ['$existanceOfDepositAccount', false] },
            {
              $eq: ['$sendToBank', false]
            }
          ]
        },
        then: 'waiting_for_creation'
      },
      {
        case: {
          $and: [
            { $eq: ['$existanceOfDepositAccount', false] },
            {
              $eq: ['$sendToBank', true]
            }
          ]
        },
        then: 'sent_to_bank'
      },
      {
        case: {
          $and: [
            { $gt: [{ $ifNull: ['$depositAccount.depositAmount', 0] }, 0] },
            {
              $eq: [
                {
                  $size: {
                    $ifNull: ['$depositAccount.payments', []]
                  }
                },
                0
              ]
            }
          ]
        },
        then: 'waiting_for_payment'
      },
      {
        case: {
          $and: [
            { $ne: ['$existanceOfDepositAccount', false] },
            {
              $eq: [
                '$depositAccount.depositAmount',
                '$depositAccount.totalPaymentAmount'
              ]
            }
          ]
        },
        then: 'paid'
      },
      {
        case: {
          $and: [
            { $ne: ['$existanceOfDepositAccount', false] },
            {
              $lt: [
                '$depositAccount.depositAmount',
                '$depositAccount.totalPaymentAmount'
              ]
            }
          ]
        },
        then: 'over_paid'
      },
      {
        case: {
          $and: [
            { $ne: ['$existanceOfDepositAccount', false] },
            {
              $gt: [
                '$depositAccount.depositAmount',
                '$depositAccount.totalPaymentAmount'
              ]
            }
          ]
        },
        then: 'partially_paid'
      }
    ],
    default: ''
  }
})

export const getTenantPipeline = () => [
  {
    $addFields: {
      depositTenantId: {
        $cond: [
          { $not: { $eq: ['$existanceOfDepositAccount', false] } },
          '$depositAccount.tenantId',
          '$rentalMeta.tenantId'
        ]
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'depositTenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            pipeline: [
              ...appHelper.getUserEmailPipeline(),
              {
                $project: {
                  _id: 1,
                  profile: 1,
                  email: 1
                }
              }
            ],
            as: 'user'
          }
        },
        {
          $unwind: {
            path: '$user',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$user.profile.avatarKey'
            ),
            email: '$user.email',
            phoneNumber: '$user.profile.phoneNumber',
            serial: 1,
            type: 1
          }
        }
      ],
      as: 'tenantInfo'
    }
  },
  {
    $unwind: {
      path: '$tenantInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const prepareDepositAccountFilter = (query) => {
  const { bankAccountNumber } = query
  const preparedQuery = {}
  if (bankAccountNumber) {
    preparedQuery['depositAccount.bankAccountNumber'] = bankAccountNumber
  }
  return preparedQuery
}

export const getPipelineForDepositAccountFile = () => [
  {
    $lookup: {
      from: 'files',
      localField: '_id',
      foreignField: 'contractId',
      as: 'fileInfo'
    }
  },
  {
    $addFields: {
      fileInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$fileInfo', []] },
            as: 'item',
            cond: {
              $eq: ['$$item.type', 'deposit_account_contract_pdf']
            }
          }
        }
      }
    }
  }
]

export const getDepositAccountsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options

  const contractQueryForDepositAccounts =
    await prepareContractQueryForDepositAccounts(query)
  const propertyPipeline = getPipelineForProperty()
  const sendToBankPipeline = getPipelineForSendToBank()
  const statusProject = getStatusProject()

  const filterByStatus = prepareFilterByStatus(query)
  const depositAccountFilter = prepareDepositAccountFilter(query)
  const pipeline = [
    {
      $match: contractQueryForDepositAccounts
    },
    {
      $lookup: {
        from: 'deposit_accounts',
        localField: '_id',
        foreignField: 'contractId',
        as: 'depositAccount'
      }
    },
    appHelper.getUnwindPipeline('depositAccount'),
    {
      $match: depositAccountFilter
    },
    {
      $addFields: {
        existanceOfDepositAccount: {
          $ifNull: ['$depositAccount', false]
        }
      }
    },
    {
      $addFields: {
        depositAmount: {
          $cond: [
            { $not: { $eq: ['$existanceOfDepositAccount', false] } },
            '$depositAccount.depositAmount',
            '$rentalMeta.depositAmount'
          ]
        },
        totalPaymentAmount: {
          $cond: [
            { $not: { $eq: ['$existanceOfDepositAccount', false] } },
            '$depositAccount.totalPaymentAmount',
            0
          ]
        }
      }
    },
    ...sendToBankPipeline,
    {
      $addFields: {
        status: statusProject,
        createdAt: {
          $cond: {
            if: { $ifNull: ['$depositAccount.createdAt', false] },
            then: '$depositAccount.createdAt',
            else: '$createdAt'
          }
        }
      }
    },
    {
      $match: filterByStatus
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
    ...propertyPipeline,
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...getTenantPipeline(),
    ...getPipelineForDepositAccountFile(),
    {
      $project: {
        _id: 1,
        propertyInfo: 1,
        accountInfo: 1,
        branchInfo: 1,
        agentInfo: 1,
        depositAmount: 1,
        tenantInfo: 1,
        totalPaymentAmount: 1,
        payments: '$depositAccount.payments',
        bankAccountNumber: '$depositAccount.bankAccountNumber',
        referenceNumber: '$depositAccount.referenceNumber',
        createdAt: 1,
        status: 1,
        fileInfo: {
          _id: 1,
          name: 1,
          title: 1
        }
      }
    }
  ]

  const depositAccounts = await ContractCollection.aggregate(pipeline)
  return depositAccounts
}

export const countDepositAccounts = async (query) => {
  const contractQueryForDepositAccounts =
    await prepareContractQueryForDepositAccounts(query)
  const filterByStatus = prepareFilterByStatus(query)
  const depositAccountFilter = prepareDepositAccountFilter(query)
  const sendToBankPipeline = getPipelineForSendToBank()
  const statusProject = getStatusProject()
  const depositAccounts = await ContractCollection.aggregate([
    {
      $match: contractQueryForDepositAccounts
    },
    {
      $lookup: {
        from: 'deposit_accounts',
        localField: '_id',
        foreignField: 'contractId',
        as: 'depositAccount'
      }
    },
    {
      $unwind: {
        path: '$depositAccount',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: depositAccountFilter
    },
    {
      $addFields: {
        existanceOfDepositAccount: {
          $ifNull: ['$depositAccount', false]
        }
      }
    },
    ...sendToBankPipeline,
    {
      $project: {
        createdAt: {
          $cond: {
            if: { $ifNull: ['$depositAccount.createdAt', false] },
            then: '$depositAccount.createdAt',
            else: '$createdAt'
          }
        },
        status: statusProject
      }
    },
    {
      $match: filterByStatus
    }
  ])
  return size(depositAccounts)
}

export const prepareFilterByStatus = (query) => {
  const { createdAt, status } = query

  const filterByStatus = {}
  if (status) {
    filterByStatus.status = { $in: status }
  }
  if (size(createdAt)) {
    const { startDate, endDate } = createdAt
    filterByStatus.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }

  return filterByStatus
}

const prepareContractQueryForDepositAccounts = async (query) => {
  const {
    accountId,
    agentId,
    bankAccountNumber,
    branchId,
    createdAt,
    contractId,
    partnerId,
    propertyId,
    searchKeyword,
    tenantId
  } = query
  const preparedQuery = {}

  preparedQuery.partnerId = partnerId
  if (accountId) preparedQuery.accountId = accountId
  if (agentId) preparedQuery.agentId = agentId
  if (branchId) preparedQuery.branchId = branchId
  if (contractId) preparedQuery._id = contractId
  if (propertyId) preparedQuery.propertyId = propertyId
  if (tenantId) preparedQuery['rentalMeta.tenantId'] = tenantId
  if (size(createdAt)) {
    const { startDate, endDate } = createdAt
    preparedQuery.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }

  if (bankAccountNumber) {
    const contractIds = await getUniqueDepositAccountField('contractId', {
      bankAccountNumber
    })
    preparedQuery._id = { $in: contractIds }
  }

  if (searchKeyword) {
    if (parseInt(searchKeyword)) {
      const contractIds = await getUniqueDepositAccountField('contractId', {
        bankAccountNumber: parseInt(searchKeyword)
      })
      preparedQuery._id = { $in: contractIds }
    } else {
      preparedQuery._id = 'nothing'
    }
  }

  preparedQuery['rentalMeta.enabledLeaseEsigning'] = true
  preparedQuery['rentalMeta.depositType'] = 'deposit_account'
  preparedQuery['rentalMeta.enabledDepositAccount'] = true
  preparedQuery['rentalMeta.hasSignersAttachmentPadesFile'] = true
  return preparedQuery
}

const prepareSortForDepositAccounts = (sort) => {
  const { createdAt, amount, payment } = sort
  if (size(createdAt)) {
    sort['createdAt'] = createdAt
    delete sort.createdAt
  }
  if (size(amount)) {
    sort['depositAmount'] = amount
    delete sort.amount
  }
  if (size(payment)) {
    sort['totalPaymentAmount'] = payment
    delete sort.payment
  }
  return sort
}

export const queryDepositAccounts = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { partnerId } = user
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = {
    partnerId
  }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  body.query.partnerId = partnerId
  body.options.sort = prepareSortForDepositAccounts(options.sort)
  const depositAccounts = await getDepositAccountsForQuery(cloneDeep(body))
  const filteredDocuments = await countDepositAccounts(body.query)
  const totalDocuments = await countDepositAccounts(totalDocumentsQuery)
  return {
    data: depositAccounts,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const getDepositAccountsSummary = async (query) => {
  const contractsQuery = await prepareContractQueryForDepositAccounts(query)
  const filterByStatus = prepareFilterByStatus(query)
  const depositAccountFilter = prepareDepositAccountFilter(query)
  const sendToBankPipeline = getPipelineForSendToBank()
  const statusProject = getStatusProject()
  const [summary] = await ContractCollection.aggregate([
    {
      $match: contractsQuery
    },
    {
      $lookup: {
        from: 'deposit_accounts',
        localField: '_id',
        foreignField: 'contractId',
        as: 'depositAccount'
      }
    },
    {
      $unwind: {
        path: '$depositAccount',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: depositAccountFilter
    },
    {
      $addFields: {
        existanceOfDepositAccount: {
          $ifNull: ['$depositAccount', false]
        }
      }
    },
    ...sendToBankPipeline,
    {
      $project: {
        depositAmount: {
          $cond: {
            if: { $eq: ['$existanceOfDepositAccount', false] },
            then: '$rentalMeta.depositAmount',
            else: { $ifNull: ['$depositAccount.depositAmount', 0] }
          }
        },
        createdAt: {
          $cond: {
            if: { $ifNull: ['$depositAccount.createdAt', false] },
            then: '$depositAccount.createdAt',
            else: '$createdAt'
          }
        },
        status: statusProject
      }
    },
    {
      $match: filterByStatus
    },
    {
      $facet: {
        total: [
          {
            $group: {
              _id: null,
              totalDepositAmount: {
                $sum: '$depositAmount'
              }
            }
          }
        ],
        statusSummary: [
          {
            $group: {
              _id: '$status',
              depositAmount: {
                $sum: '$depositAmount'
              }
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$total'
      }
    },
    {
      $project: {
        totalDepositAmount: '$total.totalDepositAmount',
        statusSummary: 1
      }
    }
  ])
  return summary
}

export const depositAccountsSummary = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const summary = await getDepositAccountsSummary(body)
  return summary
}

export const getTenantLeaseSigningUrlForLambda = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['contractId', 'tenantId'], body)

  const { contractId, tenantId } = body

  if (!(contractId && tenantId))
    throw new CustomError(400, 'Missing required fields in the body')

  return (await getTenantLeaseSigningUrl(contractId, tenantId)) || null
}

export const isEnabledDepositAccountProcess = async (params) => {
  const {
    actionType = '',
    contractInfoOrId,
    isDepositAmountPaid = false,
    hasRentalContract = false,
    partnerInfoOrId
  } = params

  let partnerInfo = partnerInfoOrId
  if (isString(partnerInfoOrId)) {
    partnerInfo = (await partnerHelper.getPartnerById(partnerInfoOrId)) || {}
  }

  let contractInfo = contractInfoOrId
  if (isString(contractInfoOrId)) {
    contractInfo =
      (await contractHelper.getContractById(contractInfoOrId)) || {}
  }

  const { enableDepositAccount = false } = partnerInfo
  const { rentalMeta = {} } = contractInfo
  const {
    depositType = '',
    enabledDepositAccount = false,
    landlordLeaseSigningStatus,
    leaseSignatureMechanism = ''
  } = rentalMeta
  const enableDepositAccountOfContract = !!(
    depositType === 'deposit_account' && enabledDepositAccount
  )
  const isLandlordSigned = !!landlordLeaseSigningStatus?.signed
  const isEnabledDepositProcess = !!(
    enableDepositAccount &&
    enableDepositAccountOfContract &&
    leaseSignatureMechanism === 'bank_id'
  )

  if (actionType === 'esigning_lease_pdf' && isEnabledDepositProcess) {
    return true
  } else if (
    actionType === 'active' &&
    isLandlordSigned &&
    hasRentalContract &&
    isEnabledDepositProcess &&
    !isDepositAmountPaid
  ) {
    return true
  } else if (!actionType && isEnabledDepositProcess) {
    return true
  }
  return false
}

export const getDepositAmountAwaitStatus = async (query) => {
  const result = await ContractCollection.aggregate([
    {
      $match: {
        ...query,
        'rentalMeta.enabledDepositAccount': true,
        'rentalMeta.hasSignersAttachmentPadesFile': true
      }
    },
    {
      $group: {
        _id: null,
        contractIds: {
          $push: '$_id'
        }
      }
    },
    {
      $lookup: {
        from: 'deposit_accounts',
        localField: 'contractIds',
        foreignField: 'contractId',
        as: 'depositAccounts'
      }
    },
    {
      $addFields: {
        depositAccounts: {
          $filter: {
            input: '$depositAccounts',
            as: 'account',
            cond: {
              $eq: [
                {
                  $size: {
                    $ifNull: ['$$account.payments', []]
                  }
                },
                0
              ]
            }
          }
        },
        partiallyPaidDepositAccounts: {
          $filter: {
            input: '$depositAccounts',
            as: 'account',
            cond: {
              $and: [
                {
                  $gt: [
                    {
                      $size: { $ifNull: ['$$account.payments', []] }
                    },
                    0
                  ]
                },
                {
                  $gt: [
                    '$$account.depositAmount',
                    '$$account.totalPaymentAmount'
                  ]
                }
              ]
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 0,
        awaitingDepositAccountCount: {
          $size: '$depositAccounts'
        },
        partiallyPaidDepositAccountCount: {
          $size: '$partiallyPaidDepositAccounts'
        }
      }
    }
  ])
  const [depositAmountStatus = {}] = result || []
  const {
    awaitingDepositAccountCount = 0,
    partiallyPaidDepositAccountCount = 0
  } = depositAmountStatus
  return {
    awaitingDepositAccountCount,
    partiallyPaidDepositAccountCount
  }
}

const prepareBusinessLandlordForDirectPartner = (account = {}) => {
  if (!size(account)) return false

  const { organization: accountOrg, person: accountPerson } = account || {}

  return {
    address: {
      streetName:
        typeof account?.getFullAddress === 'function'
          ? account.getFullAddress()
          : ''
    },
    email:
      typeof accountPerson?.email === 'function'
        ? accountPerson?.email()
        : undefined,
    name: account?.name || '',
    organizationNumber: accountOrg?.orgId || '',
    phone:
      typeof accountPerson?.getPhone === 'function'
        ? accountPerson.getPhone()
        : undefined
  }
}

const prepareBusinessLandlordForBrokerPartner = (partner = {}) => {
  const { companyInfo } = partner?.partnerSetting || {}

  if (!size(companyInfo)) return false

  const businessLandlord = {
    email: companyInfo?.email ? companyInfo.email : undefined,
    name: companyInfo?.companyName
      ? companyInfo?.companyName
      : partner?.name || '',
    organizationNumber: companyInfo?.organizationId || '',
    phone: companyInfo?.phoneNumber ? companyInfo.phoneNumber : undefined
  }

  const partnerCompanyAddress = {
    addressLetter: companyInfo?.officeCity ? companyInfo.officeCity : undefined,
    postalCode: companyInfo?.officeZipCode
      ? companyInfo.officeZipCode
      : undefined,
    streetName: companyInfo?.officeAddress
      ? companyInfo.officeAddress
      : undefined
  }

  if (size(partnerCompanyAddress)) {
    businessLandlord.address = partnerCompanyAddress
  }

  return businessLandlord
}

// Need tenant collection data with populated user
// Need contract collection data with populated account (With populated organization and person), partner
export const prepareTenantBankContractObject = (
  tenant = {},
  contract = {},
  depositAmount = 0
) => {
  if (!(size(tenant) && size(contract) && depositAmount)) return false

  const { user: tenantUser } = tenant || {}

  const bankContractObj = {
    referenceNumber: nid(17),
    privateTenant: {
      address: {
        streetName: tenant?.billingAddress || '',
        addressLetter: tenant?.zipCode || '',
        postalCode: tenant?.city || ''
      },
      email:
        typeof tenantUser?.getEmail === 'function'
          ? tenantUser.getEmail()
          : undefined,
      name: tenant?.name || '',
      nationalIdentityNumber:
        typeof tenantUser?.getNorwegianNationalIdentification === 'function'
          ? tenantUser.getNorwegianNationalIdentification()
          : undefined,
      phone:
        typeof tenantUser?.getPhone === 'function'
          ? tenantUser.getPhone()
          : undefined
    },
    rentalAgreement: {
      depositAmount,
      signatureDate: contract?.rentalMeta?.signedAt || new Date()
    }
  }

  const isDirectPartner =
    typeof contract?.partner?.isDirect === 'function'
      ? contract.partner.isDirect()
      : false
  if (isDirectPartner) {
    bankContractObj.businessLandlord = prepareBusinessLandlordForDirectPartner(
      contract?.account
    )
  } else {
    bankContractObj.businessLandlord = prepareBusinessLandlordForBrokerPartner(
      contract?.partner
    )
  }

  return bankContractObj
}

export const prepareKycFormData = async (params) => {
  const {
    irregularIncome,
    isPoliticallyExposedPerson = false,
    isTaxResidentOrResidentOfUsa = false,
    partnerId,
    referenceNumber,
    taxableAbroad
  } = params

  const tenantInfo = await tenantHelper.getATenant(
    {
      partnerId,
      'depositAccountMeta.kycForms': { $elemMatch: { referenceNumber } }
    },
    null,
    ['user']
  )
  if (!size(tenantInfo)) throw new CustomError(401, 'Invalid reference number')

  const { depositAccountMeta = {} } = tenantInfo
  const { kycForms = [] } = depositAccountMeta
  const kycFormData = size(kycForms)
    ? kycForms.find((form) => form.referenceNumber === referenceNumber)
    : {}

  if (
    !size(kycFormData) ||
    kycFormData?.isSubmitted ||
    kycFormData?.isFormSubmitted
  ) {
    throw new CustomError(
      403,
      'Kyc form already submitted please wait sometime'
    )
  }
  const { user: tenantUser } = tenantInfo
  if (!size(tenantUser)) {
    throw new CustomError(404, 'Tenant user not found')
  }

  const nationalIdentityNumber = tenantUser.getNorwegianNationalIdentification()
  if (!nationalIdentityNumber) {
    throw new CustomError(404, 'SSN not found')
  }

  const formData = {
    politicallyExposedPerson: { isPoliticallyExposedPerson },
    taxResidentOrResidentOfUsa: { isTaxResidentOrResidentOfUsa },
    nationalIdentityNumber,
    referenceNumber
  }

  if (size(taxableAbroad)) {
    formData['taxableAbroad'] = {
      isTaxableAbroad: true,
      ...taxableAbroad
    }
  } else {
    formData['taxableAbroad'] = {
      isTaxableAbroad: false
    }
  }

  if (size(irregularIncome)) {
    formData['irregularIncome'] = {
      hasIrregularIncome: true,
      incomes: [irregularIncome]
    }
  } else {
    formData['irregularIncome'] = {
      hasIrregularIncome: false
    }
  }

  const signingUrl = await getTenantLeaseSigningUrl(
    kycFormData?.contractId,
    tenantInfo._id
  )

  return {
    contractId: kycFormData?.contractId,
    formData,
    kycForms,
    partnerId,
    referenceNumber,
    signingUrl,
    tenantId: tenantInfo._id
  }
}

const prepareDepositAccountQuery = (params) => {
  const {
    bankAccountNumber,
    branchId,
    contractId,
    depositAmount,
    depositAccountId,
    partnerId,
    propertyId,
    referenceNumber,
    tenantId
  } = params
  const queryData = {}

  if (bankAccountNumber) queryData.bankAccountNumber = bankAccountNumber
  if (branchId) queryData.branchId = branchId
  if (contractId) queryData.contractId = contractId
  if (isNumber(depositAmount)) queryData.depositAmount = depositAmount
  if (depositAccountId) queryData.depositAccountId = depositAccountId
  if (partnerId) queryData.partnerId = partnerId
  if (propertyId) queryData.propertyId = propertyId
  if (referenceNumber) queryData.referenceNumber = referenceNumber
  if (tenantId) queryData.tenantId = tenantId

  return queryData
}

export const getADepositAccountForLambda = async (req) => {
  const { body, user } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  if (!size(body))
    throw new CustomError(400, 'Missing queryData in request body')

  const queryData = prepareDepositAccountQuery(body)

  if (!size(queryData)) throw new CustomError(404, 'Missing queryData')
  console.log(`=== queryData: ${JSON.stringify(queryData)} ===`)
  return await getDepositAccount(queryData)
}

export const getTenantsTestNotificationCreate = async (contractId, type) => {
  const contractInfo = await contractHelper.getAContract({ _id: contractId })
  if (!size(contractInfo)) throw new CustomError(404, 'Contract not found')

  const isTestNotification =
    contractInfo?.rentalMeta?.isDepositAccountCreationTestProcessing || false
  const isIncomingTestNotification =
    contractInfo?.rentalMeta?.isDepositAccountPaymentTestProcessing || false
  if (isTestNotification && type === 'createTestNotification') {
    throw new CustomError(405, 'Test notification already in process.')
  } else if (
    isIncomingTestNotification &&
    type === 'incomingPaymentTestNotification'
  ) {
    throw new CustomError(405, 'Incoming test notification already in process.')
  } else {
    console.log(
      'contractInfo?.rentalMeta',
      JSON.stringify(contractInfo?.rentalMeta)
    )
    const isJointlyLiable =
      contractInfo?.rentalMeta?.enabledJointlyLiable || false
    console.log('isJointlyLiable', isJointlyLiable)

    let tenants = []
    if (
      isJointlyLiable &&
      size(contractInfo) &&
      size(contractInfo.rentalMeta?.tenants)
    ) {
      tenants = contractInfo.rentalMeta.tenants
    } else if (contractInfo.rentalMeta?.tenantId) {
      tenants = [{ tenantId: contractInfo.rentalMeta.tenantId }]
    }
    return tenants
  }
}

export const getIsShowTestNotification = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId'])
  if (process.env.STAGE === 'production')
    throw new CustomError(400, 'This options can not be shown on production')
  const { body, session } = req
  const { partnerId, contractId } = body
  const contractInfo = await contractHelper.getAContract(
    { _id: contractId, partnerId },
    session
  )
  const { rentalMeta } = contractInfo
  console.log(
    'rentalMeta.isDepositAccountCreationTestProcessing',
    rentalMeta.isDepositAccountCreationTestProcessing
  )
  console.log(
    'rentalMeta.isDepositAccountPaymentTestProcessing',
    rentalMeta.isDepositAccountPaymentTestProcessing
  )
  if (
    rentalMeta.isDepositAccountCreationTestProcessing ||
    rentalMeta.isDepositAccountPaymentTestProcessing
  ) {
    return {
      accountCreated: false,
      incomingPayment: false
    }
  }
  const leaseSigningComplete =
    contractInfo?.rentalMeta?.leaseSigningComplete || false
  console.log('leaseSigningComplete', leaseSigningComplete)
  const isEnableDepositAccount = await isEnabledDepositAccountProcess({
    actionType: 'esigning_lease_pdf',
    contractInfoOrId: contractInfo,
    partnerInfoOrId: contractInfo.partnerId
  })
  console.log('isEnableDepositAccount', isEnableDepositAccount)
  const result = {
    accountCreated: false,
    incomingPayment: false
  }
  if (isEnableDepositAccount && leaseSigningComplete) {
    const hasKycData = await depositAccountHelper.getTenantDepositKycData(
      partnerId,
      contractId
    )
    console.log('hasKycData', hasKycData)
    if (hasKycData) {
      const hasTenantAccounts = await hasAllTenantsDepositAccounts(
        contractInfo,
        session
      )
      console.log('hasTenantAccounts', hasTenantAccounts)
      hasTenantAccounts
        ? (result.incomingPayment = true)
        : (result.accountCreated = true)
    }
  }
  console.log('result', result)
  return result
}

const hasAllTenantsDepositAccounts = async (contractInfo, session) => {
  console.log('contractInfo._id ', contractInfo._id)
  console.log('contractInfo.partnerId', contractInfo.partnerId)
  const DepositAccounts = await getDepositAccounts(
    { contractId: contractInfo._id, partnerId: contractInfo.partnerId },
    session
  )
  console.log('DepositAccounts', DepositAccounts)
  const numberOfDepositAccount = DepositAccounts.length
  console.log('numberOfDepositAccount', numberOfDepositAccount)
  const numberOfTotalTenants =
    contractInfo &&
    contractInfo.rentalMeta.enabledJointlyLiable &&
    !contractInfo.rentalMeta.enabledJointDepositAccount &&
    size(contractInfo.rentalMeta.tenants)
      ? size(contractInfo.rentalMeta.tenants)
      : 1
  console.log('numberOfTotalTenants', numberOfTotalTenants)
  return !!(numberOfDepositAccount >= numberOfTotalTenants)
}
