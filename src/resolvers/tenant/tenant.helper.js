import {
  assign,
  clone,
  compact,
  each,
  find,
  filter,
  flattenDeep,
  has,
  head,
  isEmpty,
  indexOf,
  intersection,
  includes,
  isString,
  map,
  size,
  uniq,
  union
} from 'lodash'
import { validateNorwegianIdNumber } from 'norwegian-national-id-validator'
import { CustomError } from '../common'
import { InvoiceCollection, TenantCollection } from '../models'
import {
  accountHelper,
  addonHelper,
  appHelper,
  appQueueHelper,
  counterHelper,
  contractHelper,
  depositAccountHelper,
  depositInsuranceHelper,
  partnerHelper,
  propertyItemHelper,
  userHelper,
  listingHelper,
  partnerSettingHelper
} from '../helpers'

export const getTenantIdsByQuery = async (query = {}) => {
  const tenantIds = await TenantCollection.distinct('_id', query)
  return tenantIds
}

export const getATenant = async (query, session, populate = []) => {
  const tenant = await TenantCollection.findOne(query)
    .populate(populate)
    .session(session)
  return tenant
}

export const getTenants = async (query, session, populate = []) => {
  const tenants = await TenantCollection.find(query)
    .populate(populate)
    .session(session)
  return tenants
}

export const getTenantsDropdownForQuery = async (params) => {
  const { query, options } = params
  const { limit = 10, skip = 0 } = options

  const tenantsData = await TenantCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: {
        name: 1
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        as: 'tenantUser'
      }
    },
    {
      $addFields: {
        paymentRemarks: {
          $cond: [
            { $isArray: '$creditRatingInfo.BETALINGSANMERKNINGERP.DET' },
            '$creditRatingInfo.BETALINGSANMERKNINGERP.DET',
            ['$creditRatingInfo.BETALINGSANMERKNINGERP']
          ]
        }
      }
    },
    {
      $addFields: {
        hasNegativePaymentRemarks: {
          $first: {
            $filter: {
              input: '$paymentRemarks',
              as: 'item',
              cond: { $eq: ['$$item.ART', 'T'] }
            }
          }
        }
      }
    },
    {
      $unwind: {
        path: '$tenantUser',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        _id: 1,
        name: '$name',
        avatar: appHelper.getUserAvatarKeyPipeline(
          '$tenantUser.profile.avatarKey',
          undefined
        ),
        hasNegativePaymentRemarks: {
          $cond: [
            {
              $ifNull: ['$hasNegativePaymentRemarks', false]
            },
            true,
            false
          ]
        },
        creditScore: '$creditRatingInfo.CDG2_GENERAL_SCORE.SCORE',
        norwegianNationalIdentification:
          '$tenantUser.profile.norwegianNationalIdentification'
      }
    }
  ])
  return tenantsData
}

export const createTenantFieldNameForApi = (tenant, directPartnerAccountId) => {
  const tenantData = {
    _id: tenant._id,
    id: tenant.serial,
    name: tenant.name,
    address: tenant.billingAddress,
    zipCode: tenant.zipCode,
    city: tenant.city,
    country: tenant.country,
    type: tenant.type
  }

  //send only the direct partner account powerOffice info instead of the whole array
  if (size(directPartnerAccountId)) {
    const powerOffice = find(
      tenant.powerOffice,
      (powerOfficeArray) =>
        powerOfficeArray.accountId === directPartnerAccountId
    )

    tenantData.powerOffice = [powerOffice]
  } else tenantData.powerOffice = tenant.powerOffice

  const userInfo = size(tenant) && size(tenant.user) ? tenant.user : {}
  if (size(userInfo))
    tenantData.userInfo = {
      _id: userInfo._id,
      name: userInfo.getName() || '',
      email: userInfo.getEmail() || '',
      phoneNumber: userInfo.getPhone() || ''
    }
  return tenantData
}

const prepareDuePipelineForTenant = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'tenantId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$invoiceType', 'invoice']
            }
          }
        }
      ],
      as: 'mainTenantInvoices'
    }
  },
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'tenants.tenantId',
      let: { tenantId: '$_id' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$invoiceType', 'invoice'] },
                // Since we lookup with tenantId previously
                { $not: { $eq: ['$tenantId', '$$tenantId'] } }
              ]
            }
          }
        }
      ],
      as: 'otherTenantInvoices'
    }
  },
  {
    $addFields: {
      invoices: {
        $concatArrays: [
          { $ifNull: ['$mainTenantInvoices', []] },
          { $ifNull: ['$otherTenantInvoices', []] }
        ]
      }
    }
  },
  appHelper.getUnwindPipeline('invoices'),
  {
    $group: {
      _id: '$_id',
      overDueTotalAmount: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            '$invoices.invoiceTotal',
            0
          ]
        }
      },
      overDueTotalPaid: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            '$invoices.totalPaid',
            0
          ]
        }
      },
      overDueCreditedAmount: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            '$invoices.creditedAmount',
            0
          ]
        }
      },
      dueTotalAmount: {
        $sum: {
          $cond: [
            { $not: { $eq: ['$invoices.status', 'paid'] } },
            '$invoices.invoiceTotal',
            0
          ]
        }
      },
      dueTotalPaid: {
        $sum: {
          $cond: [
            { $not: { $eq: ['$invoices.status', 'paid'] } },
            '$invoices.totalPaid',
            0
          ]
        }
      },
      dueCreditedAmount: {
        $sum: {
          $cond: [
            { $not: { $eq: ['$invoices.status', 'paid'] } },
            '$invoices.creditedAmount',
            0
          ]
        }
      },
      totalLostAmount: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'lost'] },
            '$invoices.lostMeta.amount',
            0
          ]
        }
      },
      name: { $first: '$name' },
      serial: { $first: '$serial' },
      type: { $first: '$type' },
      userInfo: { $first: '$userInfo' },
      properties: { $first: '$properties' },
      creditRatingInfo: { $first: '$creditRatingInfo' },
      createdAt: { $first: '$createdAt' },
      isAskForCreditRating: { $first: '$isAskForCreditRating' },
      creditRatingTermsAcceptedOn: { $first: '$creditRatingTermsAcceptedOn' }
    }
  },
  {
    $addFields: {
      totalOverDue: {
        $subtract: [
          {
            $add: ['$overDueTotalAmount', '$overDueCreditedAmount']
          },
          '$overDueTotalPaid'
        ]
      },
      totalDue: {
        $subtract: [
          {
            $add: ['$dueTotalAmount', '$dueCreditedAmount']
          },
          {
            $add: ['$dueTotalPaid', '$totalLostAmount']
          }
        ]
      }
    }
  }
]

const getUserPipelineForTenant = () => {
  const userPipeline = [
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
              phoneNumber: '$profile.phoneNumber',
              avatarKey: appHelper.getUserAvatarKeyPipeline(
                '$profile.avatarKey',
                'assets/default-image/user-primary.png'
              ),
              nin: '$profile.norwegianNationalIdentification',
              email: 1
            }
          }
        ],
        as: 'userInfo'
      }
    },
    {
      $unwind: {
        path: '$userInfo',
        preserveNullAndEmptyArrays: true
      }
    }
  ]
  return userPipeline
}

const getPropertiesPipelineForTenant = () => {
  const propertiesPipeline = [
    {
      $unwind: {
        path: '$properties',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'listings',
        let: { propertyId: '$properties.propertyId' },
        localField: 'properties.propertyId',
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
              listingTypeId: 1,
              propertyTypeId: 1,
              apartmentId: 1,
              serial: 1,
              leaseStartDate: 1,
              leaseEndDate: 1,
              imageUrl: 1
            }
          }
        ],
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        'properties.property': '$property'
      }
    },
    {
      $group: {
        _id: '$_id',
        name: { $first: '$name' },
        serial: { $first: '$serial' },
        type: { $first: '$type' },
        userInfo: { $first: '$userInfo' },
        properties: { $push: '$properties' },
        creditRatingInfo: { $first: '$creditRatingInfo' },
        createdAt: { $first: '$createdAt' },
        branchInfo: { $first: '$branchInfo' },
        isAskForCreditRating: { $first: '$isAskForCreditRating' },
        creditRatingTermsAcceptedOn: { $first: '$creditRatingTermsAcceptedOn' }
      }
    }
  ]
  return propertiesPipeline
}

const getUserPipelineForTenantForPogo = () => {
  const userPipeline = [
    {
      $lookup: {
        from: 'users',
        let: { userId: '$userId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$_id', '$$userId']
              }
            }
          },
          {
            $addFields: {
              emails: {
                $ifNull: ['$emails', []]
              }
            }
          },
          {
            $addFields: {
              fbMail: { $ifNull: ['$services.facebook.email', null] },
              verifiedMails: {
                $filter: {
                  input: '$emails',
                  as: 'email',
                  cond: {
                    $eq: ['$$email.verified', true]
                  }
                }
              },
              unverifiedMail: {
                $cond: {
                  if: { $gt: [{ $size: '$emails' }, 0] },
                  then: { $first: '$emails' },
                  else: null
                }
              }
            }
          },
          {
            $addFields: {
              verifiedMail: {
                $cond: {
                  if: { $gt: [{ $size: '$verifiedMails' }, 0] },
                  then: { $last: '$verifiedMails' },
                  else: null
                }
              }
            }
          },
          {
            $project: {
              name: '$profile.name',
              email: {
                $switch: {
                  branches: [
                    {
                      case: {
                        $and: [
                          { $eq: ['$verifiedMail', null] },
                          { $ne: ['$fbMail', null] }
                        ]
                      },
                      then: '$fbMail'
                    },
                    {
                      case: {
                        $and: [
                          { $eq: ['$verifiedMail', null] },
                          { $ne: ['$unverifiedMail', null] }
                        ]
                      },
                      then: '$unverifiedMail.address'
                    }
                  ],
                  default: '$verifiedMail.address'
                }
              },
              phoneNumber: '$profile.phoneNumber'
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
    }
  ]
  return userPipeline
}

export const getTenantsForQuery = async (params = {}) => {
  const { query, options } = params || {}
  const { limit, sort } = options || {}
  let tenants = []
  //For pogo
  if (size(query.dataType)) {
    delete query.dataType
    if (size(query)) {
      console.log(
        '====> Checking tenant query for pogo integration, query:',
        JSON.stringify(query),
        '<==='
      )
    }
    const userPipeline = getUserPipelineForTenantForPogo()
    tenants = await TenantCollection.aggregate([
      {
        $match: query
      },
      {
        $sort: sort
      },
      {
        $limit: limit
      },
      ...userPipeline,
      {
        $project: {
          id: '$serial',
          name: '$name',
          address: '$billingAddress',
          zipCode: '$zipCode',
          city: '$city',
          country: '$country',
          type: '$type',
          powerOffice: '$powerOffice',
          userInfo: '$user'
        }
      }
    ])
  }
  return tenants
}

export const getMeterNumbersFromPropertyItems = async (
  partnerId,
  contractId,
  propertyId
) => {
  let meterNumbers = ''
  if (partnerId && contractId && propertyId) {
    const propertyItem = await propertyItemHelper.getAPropertyItem({
      partnerId,
      contractId,
      propertyId
    })
    if (
      propertyItem &&
      propertyItem.meterReading &&
      size(propertyItem.meterReading.meters)
    ) {
      const { meters } = propertyItem.meterReading
      each(meters, (meter, index) => {
        const isLastNumber = index === meters.length - 1
        if (meter.numberOfMeter && !isLastNumber) {
          meterNumbers += meter.numberOfMeter + ','
        } else if (meter.numberOfMeter && isLastNumber) {
          meterNumbers += meter.numberOfMeter
        }
      })
    }
  }
  return meterNumbers
}

export const getDepositStatusOfTenant = async (params) => {
  const { partnerId, tenantId, contractId, propertyId } = params
  let status = ''
  if (partnerId && tenantId && contractId && propertyId) {
    const depositAccount = await depositAccountHelper.getDepositAccount({
      partnerId,
      tenantId,
      contractId,
      propertyId
    })
    if (size(depositAccount)) {
      status = 'created'
      const { totalPaymentAmount = 0, depositAmount = 0 } = depositAccount
      if (totalPaymentAmount && depositAmount) {
        if (totalPaymentAmount === depositAmount) {
          status = 'paid'
        } else if (totalPaymentAmount > depositAmount) {
          status = 'overpaid'
        } else {
          status = 'due'
        }
      }
    }
  }
  return status
}

export const countTenants = async (query, session) => {
  const numberOfTenants = await TenantCollection.countDocuments(query).session(
    session
  )
  return numberOfTenants
}

const getUpdatedTenantPowerOfficeInfo = async (
  partnerId,
  directPartnerAccountId
) => {
  const matchQuery = { partnerId, lastUpdate: { $exists: true } }
  let conditionObj = {}

  if (size(directPartnerAccountId)) {
    matchQuery.powerOffice = {
      $exists: true,
      $elemMatch: { accountId: directPartnerAccountId }
    }
    conditionObj = {
      $and: [
        { $ifNull: ['$$powerOfficeArray.syncedAt', false] },
        { $lte: ['$$powerOfficeArray.syncedAt', '$lastUpdate'] },
        { $eq: ['$$powerOfficeArray.accountId', directPartnerAccountId] }
      ]
    }
  } else {
    matchQuery.powerOffice = { $exists: true }
    matchQuery['powerOffice.syncedAt'] = { $exists: true }
    conditionObj = { $lte: ['$$powerOfficeArray.syncedAt', '$lastUpdate'] }
  }

  const updatedTenants =
    (await TenantCollection.aggregate([
      { $match: matchQuery },
      { $sort: { createdAt: -1 } },
      {
        $project: {
          powerOffice: {
            $filter: {
              input: '$powerOffice',
              as: 'powerOfficeArray',
              cond: conditionObj
            }
          }
        }
      },
      {
        $match: { powerOffice: { $gt: { $size: 0 } } }
      },
      { $limit: 1 }
    ])) || []

  return head(updatedTenants)
}

export const prepareTenantsQuery = async (query) => {
  const preparedQuery = {}
  if (size(query._id)) preparedQuery._id = query._id
  if (size(query.partnerId)) preparedQuery.partnerId = query.partnerId
  if (size(query.dataType)) preparedQuery.dataType = query.dataType
  if (size(query.dataType)) {
    appHelper.checkRequiredFields(['partnerId'], query)
    if (query.dataType === 'get_tenant_for_pogo') {
      preparedQuery.serial = { $exists: true }
      //set the direct partner not integrated tenant find query
      if (size(query.directPartnerAccountId)) {
        preparedQuery.$or = [
          { powerOffice: { $exists: false } },
          {
            powerOffice: {
              $not: {
                $elemMatch: {
                  id: { $exists: true },
                  code: { $exists: true },
                  accountId: query.directPartnerAccountId
                }
              }
            }
          }
        ]
        preparedQuery.properties = {
          $elemMatch: { accountId: query.directPartnerAccountId }
        }
      } else preparedQuery.powerOffice = { $exists: false }
    } else if (query.dataType === 'get_updated_tenant') {
      preparedQuery.serial = { $exists: true }

      if (size(query.directPartnerAccountId)) {
        preparedQuery.powerOffice = {
          $exists: true,
          $elemMatch: { accountId: query.directPartnerAccountId }
        }
      } else {
        preparedQuery.powerOffice = { $exists: true }
      }

      const updatedTenants = await getUpdatedTenantPowerOfficeInfo(
        query.partnerId,
        query.directPartnerAccountId
      )

      if (size(updatedTenants) && size(updatedTenants._id))
        preparedQuery._id = updatedTenants._id
      else preparedQuery._id = 'nothing'
    }
  }

  return preparedQuery
}
const prepareTenantsOptions = (body) => {
  const { query, options } = body
  if (size(query.dataType)) {
    options.limit =
      indexOf(['get_tenant_for_pogo', 'get_updated_tenant'], query.dataType) !==
      -1
        ? 1
        : 0
    options.sort = { serial: 1 }
  }
  return options
}

const prepareTenantsDropdownQuery = (query = {}) => {
  const {
    accountId,
    contractId,
    partnerId,
    propertyId,
    status,
    searchString,
    tenantId
  } = query
  const preparedQuery = { partnerId }
  if (searchString)
    preparedQuery.name = new RegExp('.*' + searchString + '.*', 'i')
  if (propertyId) preparedQuery['properties.propertyId'] = propertyId
  if (contractId) preparedQuery['properties.contractId'] = contractId
  if (accountId) preparedQuery['properties.accountId'] = accountId
  if (tenantId) preparedQuery._id = tenantId
  if (status) preparedQuery['properties.status'] = status
  return preparedQuery
}

export const queryTenantsDropdown = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId } = user

  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const { query, options } = body
  query.partnerId = partnerId
  const preparedQuery = prepareTenantsDropdownQuery(query)
  const tenantsDropdownData = await getTenantsDropdownForQuery({
    query: preparedQuery,
    options
  })

  // To count filter dropdown documents
  const filteredDocuments = await countTenants(preparedQuery)
  const totalDocuments = await countTenants({ partnerId })

  return {
    data: tenantsDropdownData,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const queryTenants = async (req) => {
  const { body, user } = req
  const { userId } = user

  appHelper.checkUserId(userId)

  const query = await prepareTenantsQuery(body.query)
  const options = prepareTenantsOptions(body)

  appHelper.validateSortForQuery(options.sort)

  const tenantsData = await getTenantsForQuery({ query: { ...query }, options })

  if (size(query.dataType)) {
    delete query.dataType
  }
  if (size(query)) {
    console.log(
      '====> Checking tenant query for filter and total documents, query:',
      JSON.stringify(query),
      '<==='
    )
  }
  const filteredDocuments = await countTenants(query)
  const totalDocuments = await countTenants({})
  return { data: tenantsData, metaData: { filteredDocuments, totalDocuments } }
}

export const queryTenantForXledger = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  appHelper.validateId({ partnerId })
  const tenant = await getUnsyncTenantForXledger(body)
  return tenant
}

const getUnsyncTenantForXledger = async (body) => {
  const { dataType } = body
  const query = await prepareTenantQueryForXledger(body)
  const finalPipeline = [
    {
      $match: query
    }
  ]
  if (dataType === 'get_update_tenant') {
    const pipeline = await getPipelineForUpdateTenantForXledger(body)
    finalPipeline.push(...pipeline)
  }
  finalPipeline.push({
    $limit: 1
  })
  finalPipeline.push(...getUserPipelineForXledgerTenantInfo())
  finalPipeline.push({
    $project: {
      _id: 1,
      country: 1,
      code: '$serial',
      description: '$name',
      email: '$userInfo.email',
      phone: '$userInfo.phone',
      place: '$city',
      streetAddress: '$billingAddress',
      zipCode: 1,
      dbId: '$backdatedXledger.id'
    }
  })
  const [tenant] = (await TenantCollection.aggregate(finalPipeline)) || []
  return tenant
}

const getUserPipelineForXledgerTenantInfo = () => [
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
            email: 1,
            phone: '$profile.phoneNumber'
          }
        }
      ],
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo')
]

const prepareTenantQueryForXledger = async (body = {}) => {
  const { partnerId, accountId, dataType } = body
  const query = {
    partnerId,
    serial: {
      $exists: true
    }
  }
  if (dataType === 'get_update_tenant') {
    query.xledger = {
      $exists: true
    }
    query.lastUpdate = {
      $exists: true
    }
    if (accountId) {
      query.xledger = {
        $elemMatch: {
          accountId,
          hasUpdateError: {
            $exists: false
          }
        }
      }
    } else {
      query['xledger.hasUpdateError'] = {
        $exists: false
      }
    }
  } else {
    if (accountId) {
      query.$or = [
        { xledger: { $exists: false } },
        {
          'xledger.accountId': {
            $ne: accountId
          }
        }
      ]
    } else {
      query.xledger = { $exists: false }
    }
  }
  return query
}

const getPipelineForUpdateTenantForXledger = async (body = {}) => {
  const { accountId } = body
  const filterQuery = [
    {
      $gte: ['$lastUpdate', '$$ledger.syncedAt']
    }
  ]
  if (accountId) {
    filterQuery.push({ $eq: ['$$ledger.accountId', accountId] })
  }
  const pipeline = [
    {
      $addFields: {
        backdatedXledger: {
          $first: {
            $filter: {
              input: '$xledger',
              as: 'ledger',
              cond: {
                $and: filterQuery
              }
            }
          }
        }
      }
    },
    {
      $match: {
        backdatedXledger: {
          $exists: true
        }
      }
    }
  ]
  return pipeline
}

const getTenantsForPartnerApp = async (
  params = {},
  options = {},
  user = {}
) => {
  const { query, partnerId } = params
  const { limit, skip, sort } = options
  const userPipeline = getUserPipelineForTenant()
  const propertiesPipeline = getPropertiesPipelineForTenant()
  const partner = await partnerHelper.getAPartner({
    _id: partnerId,
    isActive: true
  })
  const enableCreditRating = partner?.enableCreditRating
  let pipeline = [
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
    ...userPipeline,
    ...propertiesPipeline
  ]
  if (!user.roles.includes('partner_janitor') || user.roles.length !== 1) {
    const duePipeline = prepareDuePipelineForTenant()
    pipeline = [...pipeline, ...duePipeline]
  }
  pipeline.push({
    $sort: sort
  })
  pipeline.push({
    $addFields: {
      riskClass: '$creditRatingInfo.CDG2_GENERAL.RISKCLASS'
    }
  })
  const lastProject = {
    $project: {
      _id: 1,
      name: 1,
      serial: 1,
      type: 1,
      userInfo: 1,
      'properties.property': 1,
      'properties.status': 1,
      totalOverDue: 1,
      totalDue: 1,
      creditRatingScore: {
        $cond: [
          {
            $and: [
              { $eq: [enableCreditRating, true] },
              {
                $or: [
                  { $eq: ['$isAskForCreditRating', true] },
                  {
                    $and: [
                      { $ifNull: ['$userInfo.nin', false] },
                      { $ifNull: ['$creditRatingTermsAcceptedOn', false] }
                    ]
                  }
                ]
              }
            ]
          },
          '$creditRatingInfo.CDG2_GENERAL_SCORE.SCORE',
          null
        ]
      },
      createdAt: 1,
      creditRatingRiskClass: {
        $cond: [
          {
            $and: [
              { $eq: [enableCreditRating, true] },
              {
                $or: [
                  { $eq: ['$isAskForCreditRating', true] },
                  {
                    $and: [
                      { $ifNull: ['$userInfo.nin', false] },
                      { $ifNull: ['$creditRatingTermsAcceptedOn', false] }
                    ]
                  }
                ]
              }
            ]
          },
          {
            $switch: {
              branches: [
                {
                  case: { $eq: ['$riskClass', '1'] },
                  then: 'very_high_risk'
                },
                { case: { $eq: ['$riskClass', '2'] }, then: 'high_risk' },
                { case: { $eq: ['$riskClass', '3'] }, then: 'medium_risk' },
                { case: { $eq: ['$riskClass', '4'] }, then: 'low_risk' },
                { case: { $eq: ['$riskClass', '5'] }, then: 'very_low_risk' }
              ],
              default: 'score_not_calculated'
            }
          },
          null
        ]
      },
      creditRatingInfo: {
        $cond: [
          {
            $and: [
              { $eq: [enableCreditRating, true] },
              {
                $or: [
                  { $eq: ['$isAskForCreditRating', true] },
                  {
                    $and: [
                      { $ifNull: ['$userInfo.nin', false] },
                      { $ifNull: ['$creditRatingTermsAcceptedOn', false] }
                    ]
                  }
                ]
              }
            ]
          },
          '$creditRatingInfo',
          null
        ]
      },
      isAskForCreditRating: 1,
      creditRatingTermsAcceptedOn: 1
    }
  }
  pipeline.push(lastProject)
  const tenants = await TenantCollection.aggregate(pipeline)
  return tenants
}

export const queryTenantsForPartnerApp = async (req) => {
  const { body, user } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  const preparedQuery = await prepareTenantsQueryForExcelCreator(query)
  const tenantsData = await getTenantsForPartnerApp(
    {
      query: preparedQuery,
      partnerId
    },
    options,
    user
  )
  const filteredDocuments = await countTenants(preparedQuery)
  const totalDocuments = await countTenants({ partnerId })
  return { data: tenantsData, metaData: { filteredDocuments, totalDocuments } }
}

const getUserPipeline = () => [
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
            email: 1,
            phoneNumber: '$profile.phoneNumber',
            nin: '$profile.norwegianNationalIdentification',
            birthDate: '$profile.birthday',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey'),
            organizationNumber: '$profile.organizationNumber'
          }
        }
      ],
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo')
]

const getTaskPipeline = () => [
  {
    $lookup: {
      from: 'tasks',
      localField: '_id',
      foreignField: 'tenantId',
      as: 'tasksInfo'
    }
  },
  {
    $addFields: {
      tasksInfo: {
        $filter: {
          input: { $ifNull: ['$tasksInfo', []] },
          as: 'task',
          cond: {
            $and: [
              { $ifNull: ['$$task.dueDate', false] },
              { $lt: ['$$task.dueDate', new Date()] }
            ]
          }
        }
      }
    }
  }
]

const getTotalDuePipeline = (monthStartDate, monthEndDate) => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'tenantId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$invoiceType', 'invoice']
            }
          }
        }
      ],
      as: 'mainTenantInvoices'
    }
  },
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'tenants.tenantId',
      let: { tenantId: '$_id' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$invoiceType', 'invoice'] },
                // Since we lookup with tenantId previously
                { $not: { $eq: ['$tenantId', '$$tenantId'] } }
              ]
            }
          }
        }
      ],
      as: 'otherTenantInvoices'
    }
  },
  {
    $addFields: {
      invoices: {
        $concatArrays: [
          { $ifNull: ['$mainTenantInvoices', []] },
          { $ifNull: ['$otherTenantInvoices', []] }
        ]
      }
    }
  },
  appHelper.getUnwindPipeline('invoices'),
  {
    $group: {
      _id: '$_id',
      overDueTotalAmount: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            '$invoices.invoiceTotal',
            0
          ]
        }
      },
      overDueTotalPaid: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            '$invoices.totalPaid',
            0
          ]
        }
      },
      overDueCreditedAmount: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            '$invoices.creditedAmount',
            0
          ]
        }
      },
      dueTotalAmount: {
        $sum: '$invoices.invoiceTotal'
      },
      dueTotalPaid: {
        $sum: '$invoices.totalPaid'
      },
      dueCreditedAmount: {
        $sum: '$invoices.creditedAmount'
      },
      totalLostAmount: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'lost'] },
            '$invoices.lostMeta.amount',
            0
          ]
        }
      },
      thisMonthInvoiceTotal: {
        $sum: {
          $cond: [
            {
              $and: [
                { $gte: ['$invoices.dueDate', monthStartDate] },
                { $lte: ['$invoices.dueDate', monthEndDate] }
              ]
            },
            '$invoices.invoiceTotal',
            0
          ]
        }
      },
      thisMonthCreditAmount: {
        $sum: {
          $cond: [
            {
              $and: [
                { $gte: ['$invoices.dueDate', monthStartDate] },
                { $lte: ['$invoices.dueDate', monthEndDate] }
              ]
            },
            '$invoices.creditedAmount',
            0
          ]
        }
      },
      name: { $first: '$name' },
      serial: { $first: '$serial' },
      type: { $first: '$type' },
      aboutText: { $first: '$aboutText' },
      userId: { $first: '$userId' },
      creditRatingInfo: { $first: '$creditRatingInfo' },
      billingAddress: { $first: '$billingAddress' },
      zipCode: { $first: '$zipCode' },
      city: { $first: '$city' },
      country: { $first: '$country' },
      isAskForCreditRating: { $first: '$isAskForCreditRating' },
      creditRatingTermsAcceptedOn: { $first: '$creditRatingTermsAcceptedOn' },
      properties: { $first: '$properties' }
    }
  },
  {
    $addFields: {
      totalOverDue: {
        $subtract: [
          {
            $add: ['$overDueTotalAmount', '$overDueCreditedAmount']
          },
          '$overDueTotalPaid'
        ]
      },
      totalDue: {
        $subtract: [
          {
            $add: ['$dueTotalAmount', '$dueCreditedAmount']
          },
          {
            $add: ['$dueTotalPaid', '$totalLostAmount']
          }
        ]
      },
      invoiceThisMonth: {
        $add: ['$thisMonthInvoiceTotal', '$thisMonthCreditAmount']
      }
    }
  }
]

const getTenantDetails = async (query) => {
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    query.partnerId
  )
  const monthStartDate = (await appHelper.getActualDate(partnerSettings, true))
    .startOf('month')
    .toDate()
  const monthEndDate = (await appHelper.getActualDate(partnerSettings, true))
    .endOf('month')
    .toDate()

  const partner = await partnerHelper.getAPartner({
    isActive: true,
    _id: query.partnerId
  })
  const enableCreditRating = partner?.enableCreditRating
  const pipeline = [
    {
      $match: query
    },
    ...getTotalDuePipeline(monthStartDate, monthEndDate),
    ...getUserPipeline(),
    ...getTaskPipeline(),
    {
      $addFields: {
        riskClass: '$creditRatingInfo.CDG2_GENERAL.RISKCLASS'
      }
    },
    {
      $project: {
        _id: 1,
        name: 1,
        serial: 1,
        type: 1,
        city: 1,
        zipCode: 1,
        country: 1,
        billingAddress: 1,
        aboutText: 1,
        userInfo: 1,
        taskDue: {
          $size: { $ifNull: ['$tasksInfo', []] }
        },
        totalOverDue: 1,
        totalDue: 1,
        invoiceThisMonth: 1,
        creditRatingScore: {
          $cond: [
            {
              $and: [
                { $eq: [enableCreditRating, true] },
                {
                  $or: [
                    { $eq: ['$isAskForCreditRating', true] },
                    {
                      $and: [
                        { $ifNull: ['$userInfo.nin', false] },
                        { $ifNull: ['$creditRatingTermsAcceptedOn', false] }
                      ]
                    }
                  ]
                }
              ]
            },
            '$creditRatingInfo.CDG2_GENERAL_SCORE.SCORE',
            null
          ]
        },
        creditRatingRiskClass: {
          $cond: [
            {
              $and: [
                { $eq: [enableCreditRating, true] },
                {
                  $or: [
                    { $eq: ['$isAskForCreditRating', true] },
                    {
                      $and: [
                        { $ifNull: ['$userInfo.nin', false] },
                        { $ifNull: ['$creditRatingTermsAcceptedOn', false] }
                      ]
                    }
                  ]
                }
              ]
            },
            {
              $switch: {
                branches: [
                  {
                    case: { $eq: ['$riskClass', '1'] },
                    then: 'very_high_risk'
                  },
                  { case: { $eq: ['$riskClass', '2'] }, then: 'high_risk' },
                  { case: { $eq: ['$riskClass', '3'] }, then: 'medium_risk' },
                  { case: { $eq: ['$riskClass', '4'] }, then: 'low_risk' },
                  { case: { $eq: ['$riskClass', '5'] }, then: 'very_low_risk' }
                ],
                default: 'score_not_calculated'
              }
            },
            null
          ]
        },
        creditRatingInfo: {
          $cond: [
            {
              $and: [
                { $eq: [enableCreditRating, true] },
                {
                  $or: [
                    { $eq: ['$isAskForCreditRating', true] },
                    {
                      $and: [
                        { $ifNull: ['$userInfo.nin', false] },
                        { $ifNull: ['$creditRatingTermsAcceptedOn', false] }
                      ]
                    }
                  ]
                }
              ]
            },
            '$creditRatingInfo',
            null
          ]
        },
        isAskForCreditRating: 1,
        creditRatingTermsAcceptedOn: 1,
        properties: 1
      }
    }
  ]
  const [tenant = {}] = (await TenantCollection.aggregate(pipeline)) || []
  return tenant
}

export const queryTenantDetails = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['tenantId'], body)
  const { partnerId } = user
  const { tenantId } = body
  return await getTenantDetails({
    _id: tenantId,
    partnerId
  })
}

export const getTenantById = async (tenantId, session, populate = []) => {
  const tenant = await TenantCollection.findById(tenantId)
    .session(session)
    .populate(populate)
  return tenant
}

export const validationCheckForTenantUpdate = (body) => {
  const requiredFields = ['partnerId', 'tenantId', 'data']
  appHelper.checkRequiredFields(requiredFields, body)
}

export const preparePropertiesArrayDataForTenant = (params) => {
  const { propertyInfo = {}, contractId, status, createdBy } = params
  const { _id, accountId, branchId, agentId } = propertyInfo
  const propertiesArrayData = {
    propertyId: _id,
    accountId,
    branchId,
    agentId,
    status: status || 'invited',
    createdAt: new Date(),
    createdBy
  }
  contractId ? (propertiesArrayData.contractId = contractId) : ''
  return propertiesArrayData
}

export const prepareActivePropertiesArrayForTenant = (params) => {
  const { tenantInfo, tenantData, contractId } = params
  let activeProperties = clone(tenantInfo.properties) || []
  const findPartnerProperty = find(activeProperties, (activeProperty) => {
    if (
      activeProperty &&
      activeProperty.propertyId === tenantData.propertyId &&
      activeProperty.accountId === tenantData.accountId &&
      activeProperty.branchId === tenantData.branchId &&
      activeProperty.agentId === tenantData.agentId &&
      ((contractId && contractId === activeProperty.contractId) || !contractId)
    ) {
      return activeProperty
    }
  })
  //for closed contract
  if (size(findPartnerProperty) && findPartnerProperty.status === 'closed') {
    activeProperties = filter(activeProperties, (tenantProperty) => {
      if (
        tenantProperty &&
        tenantProperty.propertyId === findPartnerProperty.propertyId &&
        tenantProperty.accountId === findPartnerProperty.accountId &&
        tenantProperty.branchId === findPartnerProperty.branchId &&
        tenantProperty.agentId === findPartnerProperty.agentId &&
        ((contractId && contractId === tenantProperty.contractId) ||
          !contractId) &&
        tenantProperty.status !== 'active'
      ) {
        tenantProperty.status = 'invited'
      }
      return tenantProperty
    })
  } else if (
    size(findPartnerProperty) &&
    findPartnerProperty.status !== 'closed'
  ) {
    throw new CustomError(405, 'You have already added')
  }
  if (isEmpty(findPartnerProperty)) {
    activeProperties.push(tenantData)
  }
  return activeProperties
}

export const nidValidationCheck = (oldNID, newNID) => {
  if (!newNID || (oldNID && oldNID === newNID)) {
    return false
  }
  const isValidNewSSN = validateNorwegianIdNumber(newNID)
  if (!isValidNewSSN) {
    throw new CustomError(405, 'Invalid NID Number')
  }
  return isValidNewSSN
}

export const hasNorwegianNationalIdentification = async (
  tenantData,
  session
) => {
  if (size(tenantData) && tenantData.userId) {
    const user = await userHelper.getAnUser({ _id: tenantData.userId }, session)
    if (size(user) && user.profile.norwegianNationalIdentification) {
      return user.profile.norwegianNationalIdentification || ''
    }
  }
}

export const checkNIDDuplication = async (params, session) => {
  const { norwegianNationalIdentification, currentNorwegianNationalId } = params
  if (norwegianNationalIdentification !== currentNorwegianNationalId) {
    await appHelper.checkNIDDuplication(currentNorwegianNationalId, session)
  }
}

export const getTenantSerial = async (params, session) => {
  const { partnerId, serial, tenantId } = params

  let isExistSerialIdInAccounts
  const isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  const tenantQuery = {
    _id: { $nin: [tenantId] },
    partnerId,
    serial
  }
  const isExistSerialIdInTenants = await getATenant(tenantQuery, session)
  if (!size(isExistSerialIdInTenants) && isBrokerPartner) {
    const accountQuery = { partnerId, serial }
    isExistSerialIdInAccounts = await accountHelper.getAnAccount(
      accountQuery,
      session
    )
  }
  if (size(isExistSerialIdInTenants) || size(isExistSerialIdInAccounts)) {
    throw new CustomError(404, 'serial already exists')
  }
  const counterQuery = {
    _id: `tenant-${partnerId}`,
    next_val: { $exists: true }
  }
  const counter = await counterHelper.getACounter(counterQuery, session)
  const nextValue = counter && counter.next_val ? counter.next_val + 1 : 1
  if (
    !isExistSerialIdInTenants &&
    !isExistSerialIdInAccounts &&
    serial >= nextValue
  ) {
    throw new CustomError(404, `serial id lower than ${nextValue}`)
  }
  if (!isExistSerialIdInTenants && !isExistSerialIdInAccounts) {
    return serial
  }
  return false
}

export const prepareDataForTenantOrUserUpdate = async (params, session) => {
  const tenantUpdatingData = {}
  const userUpdatingData = {}
  let isSerial
  const {
    billingAddress,
    city,
    country,
    email,
    name,
    norwegianNationalIdentification,
    organizationNumber,
    phoneNumber,
    referenceNumber,
    serial,
    tenantInfo,
    zipCode
  } = params
  if (norwegianNationalIdentification && organizationNumber) {
    throw new CustomError(400, 'Please use SSN or organization number')
  }
  if (referenceNumber) {
    const { depositAccountMeta = {} } = tenantInfo
    const { kycForms = [] } = depositAccountMeta
    const kycFormData = size(kycForms)
      ? kycForms.find((form) => form.referenceNumber === referenceNumber)
      : {}

    if (
      !size(kycFormData) ||
      kycFormData?.isSubmitted ||
      !kycFormData?.isFormSubmitted
    ) {
      throw new CustomError(403, 'Kyc form already submitted')
    }

    tenantUpdatingData['depositAccountMeta.kycForms.$.isSubmitted'] = true
  }
  if (norwegianNationalIdentification) {
    await checkNIDDuplication(
      {
        norwegianNationalIdentification:
          tenantInfo?.user?.profile?.norwegianNationalIdentification,
        currentNorwegianNationalId: norwegianNationalIdentification
      },
      session
    )
  }
  serial ? (isSerial = await getTenantSerial(params, session)) : ''
  isSerial ? (tenantUpdatingData.serial = isSerial) : ''
  if (params.hasOwnProperty('billingAddress')) {
    tenantUpdatingData.billingAddress = billingAddress
  }
  if (params.hasOwnProperty('city')) {
    tenantUpdatingData.city = city
  }
  if (params.hasOwnProperty('country')) {
    tenantUpdatingData.country = country
  }
  if (params.hasOwnProperty('zipCode')) {
    tenantUpdatingData.zipCode = zipCode
  }
  if (email) {
    const userInfo = await userHelper.getAnUser({
      _id: {
        $ne: tenantInfo.userId
      },
      'emails.address': email
    })
    if (size(userInfo)) throw new CustomError(400, 'Email already used')
    userUpdatingData['emails.0.address'] = email
  }
  if (name) {
    userUpdatingData['profile.name'] = name
    tenantUpdatingData.name = name
  }
  if (params.hasOwnProperty('phoneNumber')) {
    userUpdatingData['profile.phoneNumber'] = phoneNumber
  }
  if (params.hasOwnProperty('norwegianNationalIdentification')) {
    userUpdatingData['profile.norwegianNationalIdentification'] =
      norwegianNationalIdentification
  }
  if (params.hasOwnProperty('organizationNumber')) {
    if (organizationNumber && organizationNumber.length !== 9) {
      throw new CustomError(400, 'Organization number must be 9 digits')
    }
    userUpdatingData['profile.organizationNumber'] = organizationNumber
  }
  return { tenantUpdatingData, userUpdatingData }
}

export const prepareChangesFieldsArrayOfUser = (previousUserInfo, data) => {
  const changesFields = []
  const { name, email, phoneNumber, norwegianNationalIdentification } = data
  const personId = previousUserInfo ? previousUserInfo._id : ''
  if (name && previousUserInfo.getName() !== name) {
    changesFields.push({
      fieldName: 'name',
      previousName: previousUserInfo.getName(),
      personId
    })
  }
  if (email && previousUserInfo.getEmail() !== email) {
    changesFields.push({
      fieldName: 'email',
      previousEmail: previousUserInfo.getEmail(),
      personId
    })
  }
  if (
    (previousUserInfo.getPhone() || phoneNumber) &&
    previousUserInfo.getPhone() !== phoneNumber
  ) {
    changesFields.push({
      fieldName: 'phoneNumber',
      previousPhone: previousUserInfo.getPhone(),
      personId
    })
  }
  if (
    (previousUserInfo.getNorwegianNationalIdentification() ||
      norwegianNationalIdentification) &&
    previousUserInfo.getNorwegianNationalIdentification() !==
      norwegianNationalIdentification
  ) {
    changesFields.push({
      fieldName: 'norwegianNationalIdentification',
      previousNID: previousUserInfo.getNorwegianNationalIdentification(),
      personId
    })
  }
  return changesFields
}

export const prepareChangesFieldsArrayOfTenant = (params) => {
  const changesFields = []

  const { updatedTenant, previousTenantInfo } = params

  if (
    size(updatedTenant) &&
    size(previousTenantInfo) &&
    updatedTenant.billingAddress &&
    previousTenantInfo.billingAddress !== updatedTenant.billingAddress
  ) {
    changesFields.push({
      fieldName: 'billingAddress',
      previousBillingAddress: previousTenantInfo.billingAddress
    })
  }
  if (
    size(updatedTenant) &&
    size(previousTenantInfo) &&
    updatedTenant.serial &&
    previousTenantInfo.serial !== updatedTenant.serial
  ) {
    changesFields.push({
      fieldName: 'serial',
      previousSerial: previousTenantInfo.serial
    })
  }
  if (
    size(updatedTenant) &&
    size(previousTenantInfo) &&
    updatedTenant.city &&
    previousTenantInfo.city !== updatedTenant.city
  ) {
    changesFields.push({
      fieldName: 'city',
      previousCity: previousTenantInfo.city
    })
  }
  if (
    size(updatedTenant) &&
    size(previousTenantInfo) &&
    updatedTenant.zipCode &&
    previousTenantInfo.zipCode !== updatedTenant.zipCode
  ) {
    changesFields.push({
      fieldName: 'zipCode',
      previousZipCode: previousTenantInfo.zipCode
    })
  }
  if (
    size(updatedTenant) &&
    size(previousTenantInfo) &&
    updatedTenant.country &&
    previousTenantInfo.country !== updatedTenant.country
  ) {
    changesFields.push({
      fieldName: 'country',
      previousCountry: previousTenantInfo.country
    })
  }
  return changesFields
}

export const getMaxTenantSerial = async (partnerId) => {
  const maxTenantSerial = await TenantCollection.aggregate([
    { $match: { partnerId } },
    { $group: { _id: null, maxSerial: { $max: '$serial' } } }
  ])
  return maxTenantSerial
}

export const prepareDataForTenantUpdateForPogo = (data) => {
  const setData = {}
  if (size(data) && size(data.name)) setData.name = data.name
  if (size(data) && size(data.type)) setData.type = data.type
  if (size(data) && size(data.billingAddress))
    setData.billingAddress = data.billingAddress

  let pushData = {}
  let pullData = {}
  //Update powerOffice array
  if (size(data) && size(data.powerOffice)) {
    const updatePowerOfficeData = data.powerOffice
    const tenantPowerOfficeId = size(updatePowerOfficeData.id)
      ? updatePowerOfficeData.id.toString()
      : ''

    pullData = {
      powerOffice: {
        id: tenantPowerOfficeId
      }
    }
    pushData = { powerOffice: updatePowerOfficeData }
  }
  return { pullData, pushData, setData }
}

export const getContractsForExcelCreator = async (params) => {
  const {
    partnerId,
    isEnabledDepositAccount,
    hasUpcomingLease,
    hasActiveLease,
    hasInProgressLease
  } = params
  const query = { partnerId }
  const statusArray = []

  if (hasUpcomingLease) statusArray.push('upcoming')
  if (hasActiveLease) statusArray.push('active')
  if (hasInProgressLease) statusArray.push('in_progress')

  query['rentalMeta.status'] = { $in: statusArray }

  if (isEnabledDepositAccount) query['rentalMeta.enabledDepositAccount'] = true
  else query['rentalMeta.enabledDepositAccount'] = { $exists: false }
  const contracts = await contractHelper.getContracts(query)
  return contracts
}

const getTenantIdsForExcelCreator = async (contractInfo, signingStatus) => {
  const rentalMeta = contractInfo?.rentalMeta || null
  const isJointlyLiableEnabled = rentalMeta?.enabledJointlyLiable || false
  const leaseSigningMeta = rentalMeta?.leaseSigningMeta || null
  const signers =
    leaseSigningMeta && size(leaseSigningMeta.signers)
      ? leaseSigningMeta.signers
      : []
  let tenantIds = []
  const signedTenantIds = []
  const notSignedTenantIds = []

  if (isJointlyLiableEnabled) tenantIds = map(rentalMeta.tenants, 'tenantId')
  else tenantIds.push(rentalMeta.tenantId)

  if (size(tenantIds)) {
    for (const tenantId of tenantIds) {
      if (size(signers)) {
        const signedObj = find(signers, function (signer) {
          return signer.externalSignerId === tenantId
        })

        if (signedObj) signedTenantIds.push(tenantId)
        else notSignedTenantIds.push(tenantId)
      } else notSignedTenantIds.push(tenantId)
    }
  }

  if (
    size(signingStatus) &&
    includes(signingStatus, 'sentToTenant') &&
    !includes(signingStatus, 'signedByTenant')
  )
    return notSignedTenantIds
  else if (
    size(signingStatus) &&
    includes(signingStatus, 'signedByTenant') &&
    !includes(signingStatus, 'sentToTenant')
  )
    return signedTenantIds
  else return tenantIds
}

export const getContractTenantIds = (contract, tenantIds) => {
  let depositTenantIds = []

  if (contract.rentalMeta) {
    if (
      size(contract.rentalMeta.tenants) &&
      contract.rentalMeta.enabledJointlyLiable &&
      !contract.rentalMeta.enabledJointDepositAccount
    ) {
      const tenants = map(contract.rentalMeta.tenants, 'tenantId')

      depositTenantIds = uniq(tenantIds.concat(tenants))
    } else depositTenantIds = [contract.rentalMeta.tenantId]
  }

  return depositTenantIds
}

export const isDepositDataSentToBank = (contract) => {
  const tenantLeaseSigningStatus =
    contract?.rentalMeta?.tenantLeaseSigningStatus
  let tenantsCount = 0
  let noOfTenantsDepositDataSentTo = 0
  const isJointDepositAccount = contract?.rentalMeta?.enabledJointDepositAccount

  if (tenantLeaseSigningStatus && !isJointDepositAccount) {
    tenantsCount = tenantLeaseSigningStatus.length

    tenantLeaseSigningStatus.forEach((tenant) => {
      if (tenant.isSentDepositDataToBank) noOfTenantsDepositDataSentTo += 1
    })

    return tenantsCount === noOfTenantsDepositDataSentTo
  } else if (tenantLeaseSigningStatus && isJointDepositAccount) {
    return !!tenantLeaseSigningStatus.find(
      (tenant) => tenant.isSentDepositDataToBank
    ) // No need to check if all the tenants have signed if jointDepositAccount is true
  }

  return false
}

export const getPropertyOrTenantIds = async (params) => {
  const {
    partnerId,
    context,
    depositAccountStatus,
    hasUpcomingLease,
    hasActiveLease,
    hasInProgressLease
  } = params
  const isWaitingForCreation =
    indexOf(depositAccountStatus, 'waitingForCreation') !== -1 || false
  const isSentToBank =
    indexOf(depositAccountStatus, 'sentToBank') !== -1 || false
  const isWaitingForPayment =
    indexOf(depositAccountStatus, 'waitingForPayment') !== -1 || false
  const isPaid = indexOf(depositAccountStatus, 'paid') !== -1 || false
  const isPartiallyPaid =
    indexOf(depositAccountStatus, 'partiallyPaid') !== -1 || false
  const isOverPaid = indexOf(depositAccountStatus, 'overPaid') !== -1 || false
  let tenantIds = []
  let propertyIds = []

  if (
    isWaitingForCreation ||
    isSentToBank ||
    isWaitingForPayment ||
    isPaid ||
    isPartiallyPaid ||
    isOverPaid
  ) {
    const depositContracts = await getContractsForExcelCreator({
      partnerId,
      isEnabledDepositAccount: true,
      hasUpcomingLease,
      hasActiveLease,
      hasInProgressLease
    })

    if (size(depositContracts)) {
      for (const contract of depositContracts) {
        const depositTenantIds = getContractTenantIds(contract, tenantIds)

        const depositAccounts = await depositAccountHelper.getDepositAccounts({
          contractId: contract._id,
          tenantId: { $in: depositTenantIds },
          partnerId
        })

        const isSentDepositDataToBank = isDepositDataSentToBank(contract)

        if (
          !size(depositAccounts) &&
          isWaitingForCreation &&
          !isSentDepositDataToBank
        ) {
          if (context === 'property') propertyIds.push(contract.propertyId)
          else tenantIds.push(depositTenantIds)
        }
        if (!size(depositAccounts) && isSentToBank && isSentDepositDataToBank) {
          if (context === 'property') propertyIds.push(contract.propertyId)
          else tenantIds.push(depositTenantIds)
        }

        if (size(depositAccounts)) {
          for (const depositAccount of depositAccounts) {
            const depositAmount = depositAccount.depositAmount
            const totalPaymentAmount = depositAccount.totalPaymentAmount

            if (!size(depositAccount.payments) && isWaitingForPayment) {
              if (context === 'property')
                propertyIds.push(depositAccount.propertyId)
              else tenantIds.push(depositAccount.tenantId)
            }

            if (
              isPaid &&
              depositAmount &&
              totalPaymentAmount &&
              depositAmount === totalPaymentAmount
            ) {
              if (context === 'property')
                propertyIds.push(depositAccount.propertyId)
              else tenantIds.push(depositAccount.tenantId)
            }

            if (
              isPartiallyPaid &&
              depositAmount &&
              totalPaymentAmount &&
              totalPaymentAmount < depositAmount
            ) {
              if (context === 'property')
                propertyIds.push(depositAccount.propertyId)
              else tenantIds.push(depositAccount.tenantId)
            }

            if (
              isOverPaid &&
              depositAmount &&
              totalPaymentAmount &&
              depositAmount < totalPaymentAmount
            ) {
              if (context === 'property')
                propertyIds.push(depositAccount.propertyId)
              else tenantIds.push(depositAccount.tenantId)
            }
          }
        }
      }

      tenantIds = uniq(flattenDeep(tenantIds))
      propertyIds = uniq(propertyIds)
    }
  }

  if (indexOf(depositAccountStatus, 'noDeposit') !== -1) {
    const noDepositContracts = await getContractsForExcelCreator({
      partnerId,
      isEnabledDepositAccount: false,
      hasUpcomingLease,
      hasActiveLease,
      hasInProgressLease
    })

    if (size(noDepositContracts)) {
      for (const contract of noDepositContracts) {
        const depositTenantIds = getContractTenantIds(contract, tenantIds)

        tenantIds.push(depositTenantIds)
        propertyIds.push(contract.propertyId)
      }

      if (context === 'property') propertyIds = uniq(propertyIds)
      else tenantIds = uniq(flattenDeep(tenantIds))
    }
  }

  return context === 'property' ? propertyIds : tenantIds
}

export const tenantIdsFromRentalContracts = async (rentalContractQuery) => {
  const rentalContracts = size(rentalContractQuery)
    ? await contractHelper.getContracts(rentalContractQuery)
    : {}
  const rentalTenants = flattenDeep(map(rentalContracts, 'rentalMeta.tenants'))
  let tenantIds = uniq(map(rentalContracts, 'rentalMeta.tenantId')) || []

  if (size(rentalTenants)) {
    tenantIds = union(tenantIds, uniq(map(rentalTenants, 'tenantId')))
  }

  return tenantIds
}

export const prepareTenantPaymentStatusForQuery = async (
  params,
  paymentStatus
) => {
  const invoiceQuery = []
  if (indexOf(paymentStatus, 'partially_paid') !== -1) {
    invoiceQuery.push({
      isPartiallyPaid: true,
      partnerId: params.partnerId
    })
  }

  if (indexOf(paymentStatus, 'overpaid') !== -1) {
    invoiceQuery.push({ isOverPaid: true, partnerId: params.partnerId })
  }

  if (indexOf(paymentStatus, 'defaulted') !== -1) {
    invoiceQuery.push({ isDefaulted: true, partnerId: params.partnerId })
  }

  if (
    indexOf(paymentStatus, 'unpaid') === -1 &&
    indexOf(paymentStatus, 'overdue') !== -1
  ) {
    invoiceQuery.push({ status: 'overdue', partnerId: params.partnerId })
  } else if (indexOf(paymentStatus, 'unpaid') !== -1) {
    invoiceQuery.push({
      status: { $ne: 'paid' },
      partnerId: params.partnerId
    })
  }
  return invoiceQuery
}

export const prepareTenantLeaseStartAndEndQuery = async (params) => {
  const contractQuery = {}
  let startDateQuery = {}
  let endDateQuery = {}

  if (
    size(params.leaseStartDateRange) &&
    params.leaseStartDateRange.startDate &&
    params.leaseStartDateRange.endDate
  ) {
    startDateQuery = {
      $gte: params.leaseStartDateRange.startDate,
      $lte: params.leaseStartDateRange.endDate
    }
  } else if (
    size(params.leaseStartDateRange) &&
    params.leaseStartDateRange.startDate &&
    !params.leaseStartDateRange.endDate
  ) {
    startDateQuery = { $gte: params.leaseStartDateRange.startDate }
  } else if (
    size(params.leaseStartDateRange) &&
    !params.leaseStartDateRange.startDate &&
    params.leaseStartDateRange.endDate
  ) {
    startDateQuery = { $lte: params.leaseStartDateRange.endDate }
  }

  if (size(startDateQuery))
    contractQuery['rentalMeta.contractStartDate'] = startDateQuery

  if (
    size(params.leaseEndDateRange) &&
    params.leaseEndDateRange.startDate &&
    params.leaseEndDateRange.endDate
  ) {
    endDateQuery = {
      $gte: params.leaseEndDateRange.startDate,
      $lte: params.leaseEndDateRange.endDate
    }
  } else if (
    size(params.leaseEndDateRange) &&
    params.leaseEndDateRange.startDate &&
    !params.leaseEndDateRange.endDate
  ) {
    endDateQuery = { $gte: params.leaseEndDateRange.startDate }
  } else if (
    size(params.leaseEndDateRange) &&
    !params.leaseEndDateRange.startDate &&
    params.leaseEndDateRange.endDate
  ) {
    endDateQuery = { $lte: params.leaseEndDateRange.endDate }
  }

  if (size(endDateQuery))
    contractQuery['rentalMeta.contractEndDate'] = endDateQuery
  return contractQuery
}

export const prepareTenantSearchingKeyWord = async (searchKeyword) => {
  const keyword = new RegExp(searchKeyword, 'i')
  const queryData = {
    $or: [
      { 'emails.address': keyword },
      { 'profile.phoneNumber': keyword },
      { 'profile.norwegianNationalIdentification': keyword }
    ]
  }
  const userIds = await userHelper.getDistinctUserIds(queryData)
  const searchQuery = [{ userId: { $in: userIds } }, { name: keyword }]

  if (!isNaN(searchKeyword))
    searchQuery.push({ serial: parseInt(searchKeyword) })

  return searchQuery
}

export const prepareTenantsQueryForExcelCreator = async (params) => {
  const query = {}
  const propertyElementMatchQuery = {}
  let tenantIds = []
  let isNotTenant = false
  let excludedTenantIds = []
  const queryArray = []
  let contractIds = []
  const queryOrArray = []

  if (size(params)) {
    queryArray.push({ partnerId: params.partnerId })

    //Set tenant status filters in query
    const tenantType = compact(params.type)
    //Set tenant status filters in query
    let tenantStatus = compact(params.status)
    if (indexOf(tenantStatus, 'active') !== -1) {
      const paymentStatus = compact(params.paymentStatus)
      const invoiceQuery = await prepareTenantPaymentStatusForQuery(
        params,
        paymentStatus
      )
      if (size(invoiceQuery)) {
        const invoiceTenantIds = await InvoiceCollection.distinct('tenantId', {
          $or: invoiceQuery
        })

        if (size(invoiceTenantIds))
          tenantIds = uniq(union(tenantIds, invoiceTenantIds))
        else {
          queryArray.push({ _id: 'nothing' })

          isNotTenant = true
        }
      }
    }

    const prospect = compact(params.hasProspect)
    if (indexOf(prospect, 'yes') !== -1) {
      const prospectStatus = compact(params.prospectStatus)
      tenantStatus = uniq(union(tenantStatus, prospectStatus))

      if (indexOf(tenantType, 'archived')) {
        queryArray.push({ type: { $ne: 'archived' } })
      }
    }

    if (size(tenantStatus)) {
      propertyElementMatchQuery.status = { $in: tenantStatus }
    }

    if (
      indexOf(tenantStatus, 'active') !== -1 ||
      indexOf(tenantStatus, 'upcoming') !== -1
    ) {
      const contractQuery = await prepareTenantLeaseStartAndEndQuery(params)

      if (size(contractQuery)) {
        contractQuery.partnerId = params.partnerId
        contractQuery['rentalMeta.status'] = { $ne: 'closed' }
        const rentalContractList = await contractHelper.getContracts(
          contractQuery
        )
        let leaseTenantIds = map(rentalContractList, 'rentalMeta.tenantId')

        leaseTenantIds = uniq(leaseTenantIds)
        if (size(leaseTenantIds)) {
          if (size(tenantIds))
            tenantIds = intersection(tenantIds, leaseTenantIds)
          else tenantIds = leaseTenantIds
        } else {
          queryArray.push({ _id: 'nothing' })

          isNotTenant = true
        }
      }
    }

    //Set branch filters in query
    if (params.branchId) {
      queryArray.push({ 'properties.branchId': params.branchId })
    }
    //Set agent filters in query
    if (params.agentId) {
      queryArray.push({ 'properties.agentId': params.agentId }) //will be in $and query
    }

    //Set account filters in query
    if (params.accountId) {
      queryArray.push({ 'properties.accountId': params.accountId }) //will be in $and query
    }

    //Set property filters in query
    if (params.propertyId) {
      queryArray.push({ 'properties.propertyId': params.propertyId }) //will be in $and query
    }

    if (
      (params.dropdownType === 'addPayment' ||
        params.dropdownType === 'propertyList') &&
      params.propertyId
    ) {
      queryArray.push({ 'properties.propertyId': params.propertyId })
    }

    //Set tenant filters in query
    if (params.tenantId) {
      propertyElementMatchQuery.tenantId = params.tenantId
    }

    if (params.tenantPropertyStatus) {
      propertyElementMatchQuery.status = params.tenantPropertyStatus
    }

    if (
      (params.contractStatus || params.queryType === 'contracts') &&
      params.propertyId
    ) {
      const contractQuery = {
        partnerId: params.partnerId,
        propertyId: params.propertyId
      }
      if (params.contractStatus) contractQuery.status = params.contractStatus
      else
        contractQuery.$and = [
          { 'rentalMeta.status': 'closed' },
          { status: 'closed' }
        ]
      contractIds = map(await contractHelper.getContracts(contractQuery), '_id')
      if (size(contractIds)) {
        propertyElementMatchQuery.contractId = { $in: contractIds }
      } else {
        queryArray.push({ _id: 'nothing' })
      }
    }

    if (size(propertyElementMatchQuery)) {
      if (size(tenantType)) {
        queryOrArray.push(
          { properties: { $elemMatch: propertyElementMatchQuery } },
          { type: { $in: tenantType } }
        )
      } else {
        if (
          size(params.hasInProgressLease) ||
          indexOf(tenantStatus, 'upcoming') !== -1
        ) {
          queryOrArray.push({
            properties: { $elemMatch: propertyElementMatchQuery }
          })
        } else
          queryArray.push({
            properties: { $elemMatch: propertyElementMatchQuery }
          })
      }
    } else {
      if (size(params.hasInProgressLease) && size(tenantType)) {
        queryOrArray.push({ type: { $in: tenantType } })
      } else if (size(tenantType)) {
        queryArray.push({ type: { $in: tenantType } })
      }
    }

    if (
      size(params.hasInProgressLease) &&
      indexOf(params.hasInProgressLease, 'hasInProgressLease') !== -1 &&
      !size(params.depositAccountStatus)
    ) {
      const contractQuery = {}

      contractQuery['rentalMeta.status'] = 'in_progress'
      const rentalContractList = await contractHelper.getContracts(
        contractQuery
      )
      let leaseTenantIds = []

      if (size(rentalContractList)) {
        for (const contractInfo of rentalContractList) {
          const tenantIds = await getTenantIdsForExcelCreator(
            contractInfo,
            params.eSignStatus
          )
          leaseTenantIds.push(tenantIds)
        }
      }

      leaseTenantIds = uniq(flattenDeep(leaseTenantIds))

      queryOrArray.push({ _id: { $in: leaseTenantIds } })
    }

    const hasUpcomingLease = indexOf(params.status, 'upcoming') !== -1 || false
    const hasActiveLease = indexOf(params.status, 'active') !== -1 || false
    const hasInProgressLease =
      indexOf(params.hasInProgressLease, 'hasInProgressLease') !== -1 || false

    if (
      (indexOf(params.status, 'upcoming') !== -1 ||
        indexOf(params.status, 'active') !== -1 ||
        indexOf(params.hasInProgressLease, 'hasInProgressLease') !== -1) &&
      size(params.depositAccountStatus)
    ) {
      const depositAccountTenantIds = await getPropertyOrTenantIds({
        partnerId: params.partnerId,
        context: 'tenant',
        depositAccountStatus: params.depositAccountStatus,
        hasUpcomingLease,
        hasActiveLease,
        hasInProgressLease
      })

      queryArray.push({ _id: { $in: depositAccountTenantIds } })
    }

    if (size(queryOrArray)) queryArray.push({ $or: queryOrArray })

    // set Terminated contract in query
    if (params.hasTerminatedContract === 'yes') {
      const hasTerminatedTenantIds = await tenantIdsFromRentalContracts({
        partnerId: params.partnerId,
        'rentalMeta.status': { $in: ['active'] },
        terminatedByUserId: { $exists: true }
      })

      if (size(tenantIds)) tenantIds = union(tenantIds, hasTerminatedTenantIds)
      else tenantIds = hasTerminatedTenantIds

      if (!size(tenantIds)) {
        queryArray.push({ _id: 'nothing' })
        isNotTenant = true
      }
    } else if (params.hasTerminatedContract === 'no') {
      const hasTerminatedTenantIds = await tenantIdsFromRentalContracts({
        partnerId: params.partnerId,
        'rentalMeta.status': { $in: ['active'] },
        terminatedByUserId: { $exists: true }
      })

      if (size(excludedTenantIds))
        excludedTenantIds = union(excludedTenantIds, hasTerminatedTenantIds)
      else excludedTenantIds = hasTerminatedTenantIds
    }

    let searchKeyword = params.keyword || params.searchKeyword

    //set tenant name, emails, phone, ssn filters.
    if (searchKeyword) {
      searchKeyword = searchKeyword.trim()
      const searchQuery = await prepareTenantSearchingKeyWord(searchKeyword)
      queryArray.push({ $or: searchQuery })
    }
    if (size(params.createdAtDateRange)) {
      const { startDate, endDate } = params.createdAtDateRange
      queryArray.push({
        createdAt: {
          $gte: new Date(startDate),
          $lte: new Date(endDate)
        }
      })
    }
    if (size(params.depositInsuranceStatus)) {
      const tenantIds = await depositInsuranceHelper.getUniqueValueOfAField(
        'tenantId',
        {
          partnerId: params.partnerId,
          status: { $in: params.depositInsuranceStatus }
        }
      )
      queryArray.push({
        _id: { $in: tenantIds }
      })
    }
    if (params.name) {
      queryArray.push({
        name: new RegExp(params.name, 'i')
      })
    }
    const userQuery = {}
    if (params.email)
      userQuery['emails.address'] = new RegExp(params.email, 'i')
    if (params.phoneNumber)
      userQuery['profile.phoneNumber'] = new RegExp(params.phoneNumber, 'i')
    if (params.ssn)
      userQuery['profile.norwegianNationalIdentification'] = new RegExp(
        params.ssn,
        'i'
      )
    if (size(userQuery)) {
      const userIds = await userHelper.getDistinctUserIds(userQuery)
      queryArray.push({
        userId: { $in: userIds }
      })
    }

    if (!isNotTenant && size(tenantIds)) {
      queryArray.push({ _id: { $in: tenantIds } })

      if (size(excludedTenantIds))
        queryArray.push({
          _id: { _id: { $in: tenantIds, $nin: excludedTenantIds } }
        })
    } else if (size(excludedTenantIds))
      queryArray.push({ _id: { $nin: excludedTenantIds } })
  }

  if (size(queryArray)) query['$and'] = queryArray

  return query
}

export const getRentAddonsForExcel = async (contractData) => {
  // e.g: [ Electricity: 100 - Water: 250 - Heating: 100]
  let rentAddons = ''
  if (size(contractData) && size(contractData.addons)) {
    const { addons } = contractData
    const addonLength = addons.length
    for (let index = 0; index < addonLength; index++) {
      const isLastAddon = index === addonLength - 1
      const doc = await addonHelper.getAddon({ _id: addons[index].addonId })
      if (doc.name) {
        rentAddons += doc.name
      }
      if (addons[index].price) {
        rentAddons += ': ' + addons[index].price
      }
      if (!isLastAddon) {
        rentAddons += ' - '
      }
    }
  }
  return rentAddons
}

const getUserPipelineForTenantReportOfExcelManager = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'userId',
      foreignField: '_id',
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
    $addFields: {
      emails: {
        $ifNull: ['$user.emails', []]
      }
    }
  },
  {
    $addFields: {
      fbMail: { $ifNull: ['$user.services.facebook.email', null] },
      verifiedMails: {
        $filter: {
          input: '$emails',
          as: 'email',
          cond: {
            $eq: ['$$email.verified', true]
          }
        }
      },
      unverifiedMail: {
        $cond: {
          if: { $gt: [{ $size: '$emails' }, 0] },
          then: { $first: '$emails' },
          else: null
        }
      }
    }
  },
  {
    $addFields: {
      verifiedMail: {
        $cond: {
          if: { $gt: [{ $size: '$verifiedMails' }, 0] },
          then: { $last: '$verifiedMails' },
          else: null
        }
      }
    }
  },
  {
    $addFields: {
      email: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$verifiedMail', null] },
                  { $ne: ['$fbMail', null] }
                ]
              },
              then: '$fbMail'
            },
            {
              case: {
                $and: [
                  { $eq: ['$verifiedMail', null] },
                  { $ne: ['$unverifiedMail', null] }
                ]
              },
              then: '$unverifiedMail.address'
            }
          ],
          default: '$verifiedMail.address'
        }
      },
      phone: '$user.profile.phoneNumber',
      birthDate: {
        $cond: [
          {
            $ifNull: ['$user.profile.norwegianNationalIdentification', false]
          },
          {
            $substr: ['$user.profile.norwegianNationalIdentification', 0, 6]
          },
          ''
        ]
      }
    }
  }
]

const getInvoiceOverduePipelineForTenantOfExcelManager = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'tenants.tenantId',
      as: 'invoices'
    }
  },
  {
    $addFields: {
      overdueInvoices: {
        $filter: {
          input: '$invoices',
          as: 'invoice',
          cond: {
            $and: [
              { $eq: ['$$invoice.status', 'overdue'] },
              { $eq: ['$$invoice.invoiceType', 'invoice'] },
              { $eq: ['$$invoice.propertyId', '$properties.propertyId'] },
              { $eq: ['$$invoice.contractId', '$properties.contractId'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$overdueInvoices',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: {
        mainId: '$_id',
        propertyId: '$properties.propertyId',
        contractId: '$properties.contractId',
        status: '$properties.status'
      },
      invoiceTotal: { $sum: '$overdueInvoices.invoiceTotal' },
      paidTotal: { $sum: '$overdueInvoices.totalPaid' },
      creditedTotal: { $sum: '$overdueInvoices.creditedAmount' },
      userId: { $first: '$userId' },
      partnerId: { $first: '$partnerId' },
      properties: { $first: '$properties' },
      name: { $first: '$name' },
      serial: { $first: '$serial' },
      email: { $first: '$email' },
      phone: { $first: '$phone' },
      birthDate: { $first: '$birthDate' },
      createdAt: { $first: '$createdAt' }
    }
  },
  {
    $addFields: {
      _id: '$_id.mainId',
      totalOverDue: {
        $subtract: [
          {
            $add: [
              { $ifNull: ['$invoiceTotal', 0] },
              { $ifNull: ['$creditedTotal', 0] }
            ]
          },
          { $ifNull: ['$paidTotal', 0] }
        ]
      }
    }
  }
]

const getPropertyPipelineForTenantOfExcelManager = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'properties.propertyId',
      foreignField: '_id',
      as: 'property'
    }
  },
  {
    $unwind: {
      path: '$property',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getContractPipelineForTenantOfExcelManager = () => [
  {
    $lookup: {
      from: 'contracts',
      let: { propertyId: '$property._id', partnerId: '$property.partnerId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$propertyId', '$$propertyId'] },
                { $eq: ['$partnerId', '$$partnerId'] },
                { $in: ['$status', ['active', 'upcoming']] },
                { $eq: ['$hasRentalContract', true] }
              ]
            }
          }
        },
        {
          $limit: 1
        }
      ],
      as: 'propertyContract'
    }
  },
  {
    $unwind: {
      path: '$propertyContract',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'contracts',
      localField: 'properties.contractId',
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

const getAddonsPipelineForTenantOfExcelManager = () => [
  {
    $lookup: {
      from: 'products_services',
      localField: 'propertyContract.addons.addonId',
      foreignField: '_id',
      as: 'addonInfo'
    }
  },
  {
    $unwind: {
      path: '$addonInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: {
        _id: '$_id',
        propertyId: '$properties.propertyId',
        contractId: '$properties.contractId',
        status: '$properties.status'
      },
      mainId: { $first: '$_id' },
      properties: { $first: '$properties' },
      rentAddons: {
        $push: {
          $concat: [
            { $ifNull: ['$addonInfo.name', ''] },
            {
              $cond: [
                { $ifNull: ['$propertyContract.addons.price', false] },
                {
                  $concat: [
                    ': ',
                    { $toString: '$propertyContract.addons.price' }
                  ]
                },
                ''
              ]
            }
          ]
        }
      },
      propertyContract: {
        $first: '$propertyContract'
      },
      contractInfo: {
        $first: '$contractInfo'
      },
      name: {
        $first: '$name'
      },
      serial: {
        $first: '$serial'
      },
      email: {
        $first: '$email'
      },
      phone: {
        $first: '$phone'
      },
      birthDate: {
        $first: '$birthDate'
      },
      totalOverDue: {
        $first: '$totalOverDue'
      },
      partnerId: {
        $first: '$partnerId'
      },
      property: {
        $first: '$property'
      },
      createdAt: { $first: '$createdAt' }
    }
  }
]

const getMeeterReadingPipelineForTenantOfExcelManager = () => [
  {
    $lookup: {
      from: 'property_items',
      let: {
        partnerId: '$partnerId',
        contractId: '$properties.contractId',
        propertyId: '$properties.propertyId'
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$partnerId', '$$partnerId'] },
                { $eq: ['$contractId', '$$contractId'] },
                { $eq: ['$propertyId', '$$propertyId'] }
              ]
            }
          }
        },
        {
          $limit: 1
        }
      ],
      as: 'propertyItem'
    }
  },
  {
    $unwind: {
      path: '$propertyItem',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $unwind: {
      path: '$propertyItem.meterReading.meters',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: {
        mainId: '$mainId',
        propertyId: '$properties.propertyId',
        contractId: '$properties.contractId',
        status: '$properties.status'
      },
      mainId: { $first: '$mainId' },
      partnerId: { $first: '$partnerId' },
      meeterNumber: {
        $push: '$propertyItem.meterReading.meters.numberOfMeter'
      },
      properties: { $first: '$properties' },
      rentAddons: {
        $first: '$rentAddons'
      },
      propertyContract: {
        $first: '$propertyContract'
      },
      contractInfo: {
        $first: '$contractInfo'
      },
      name: {
        $first: '$name'
      },
      serial: {
        $first: '$serial'
      },
      email: {
        $first: '$email'
      },
      phone: {
        $first: '$phone'
      },
      birthDate: {
        $first: '$birthDate'
      },
      totalOverDue: {
        $first: '$totalOverDue'
      },
      property: {
        $first: '$property'
      },
      createdAt: { $first: '$createdAt' }
    }
  }
]

const getFinalProjectForTenantOfexcelManager = (dateFormat, timeZone) => [
  {
    $project: {
      _id: '$_id.mainId',
      name: 1,
      tenantId: '$serial',
      email: 1,
      phone: 1,
      birthDate: 1,
      totalOverDue: 1,
      rentAddons: {
        $substr: ['$rentAddons', 3, -1]
      },
      meeterNumber: {
        $substr: ['$meeterNumber', 1, -1]
      },
      deposit: '$depositStatus',
      objectId: '$property.serial',
      address: {
        $concat: [
          {
            $cond: {
              if: { $ifNull: ['$property.location.name', false] },
              then: { $concat: ['$property.location.name', ', '] },
              else: ''
            }
          },
          {
            $cond: {
              if: { $ifNull: ['$property.location.postalCode', false] },
              then: { $concat: ['$property.location.postalCode', ', '] },
              else: ''
            }
          },
          {
            $cond: {
              if: { $ifNull: ['$property.location.city', false] },
              then: { $concat: ['$property.location.city', ', '] },
              else: ''
            }
          },
          {
            $cond: {
              if: { $ifNull: ['$property.location.country', false] },
              then: '$property.location.country',
              else: ''
            }
          }
        ]
      },
      apartmentId: '$property.apartmentId',
      periodFrom: {
        $cond: [
          { $ifNull: ['$contractInfo.rentalMeta.contractStartDate', false] },
          {
            $dateToString: {
              format: dateFormat,
              date: '$contractInfo.rentalMeta.contractStartDate',
              timezone: timeZone
            }
          },
          ''
        ]
      },
      periodTo: {
        $cond: [
          { $ifNull: ['$contractInfo.rentalMeta.contractEndDate', false] },
          {
            $dateToString: {
              format: dateFormat,
              date: '$contractInfo.rentalMeta.contractEndDate',
              timezone: timeZone
            }
          },
          {
            $cond: [
              {
                $ifNull: ['$contractInfo.rentalMeta.contractStartDate', false]
              },
              'Undetermined',
              ''
            ]
          }
        ]
      },
      rentAmount: {
        $cond: [
          { $ifNull: ['$propertyContract', false] },
          {
            $ifNull: ['$propertyContract.rentalMeta.monthlyRentAmount', 0]
          },
          ''
        ]
      },
      isVatEnable: {
        $cond: [
          { $ifNull: ['$propertyContract', false] },
          {
            $cond: [
              {
                $ifNull: ['$propertyContract.rentalMeta.isVatEnable', false]
              },
              'YES',
              'NO'
            ]
          },
          ''
        ]
      },
      contractLastCpiDate: {
        $cond: [
          { $ifNull: ['$propertyContract', false] },
          {
            $cond: [
              {
                $ifNull: ['$propertyContract.rentalMeta.lastCpiDate', false]
              },
              {
                $dateToString: {
                  format: dateFormat,
                  date: '$propertyContract.rentalMeta.lastCpiDate',
                  timezone: timeZone
                }
              },
              ''
            ]
          },
          ''
        ]
      },
      contractNextCpiDate: {
        $cond: [
          { $ifNull: ['$propertyContract', false] },
          {
            $cond: [
              {
                $ifNull: ['$propertyContract.rentalMeta.nextCpiDate', false]
              },
              {
                $dateToString: {
                  format: dateFormat,
                  date: '$propertyContract.rentalMeta.nextCpiDate',
                  timezone: timeZone
                }
              },
              ''
            ]
          },
          ''
        ]
      },
      contractRemainingDays: {
        $cond: [
          {
            $ifNull: ['$propertyContract.rentalMeta.contractEndDate', false]
          },
          {
            $round: [
              {
                $divide: [
                  {
                    $subtract: [
                      '$propertyContract.rentalMeta.contractEndDate',
                      new Date()
                    ]
                  },
                  86400000
                ]
              },
              0
            ]
          },
          ''
        ]
      },
      status: '$properties.status',
      createdAt: 1
    }
  }
]

const getDepositAccountPipelineForTenantOfExcelManager = () => [
  {
    $lookup: {
      from: 'deposit_accounts',
      let: {
        partnerId: '$partnerId',
        tenantId: '$mainId',
        contractId: '$properties.contractId',
        propertyId: '$properties.propertyId'
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$partnerId', '$$partnerId'] },
                { $eq: ['$tenantId', '$$tenantId'] },
                { $eq: ['$contractId', '$$contractId'] },
                { $eq: ['$propertyId', '$$propertyId'] }
              ]
            }
          }
        },
        {
          $limit: 1
        }
      ],
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
    $addFields: {
      depositStatus: {
        $switch: {
          branches: [
            {
              case: { $not: { $ifNull: ['$depositAccount', false] } },
              then: ''
            },
            {
              case: {
                $not: {
                  $and: [
                    {
                      $ifNull: ['$depositAccount.totalPaymentAmount', false]
                    },
                    { $ifNull: ['$depositAccount.depositAmount', false] }
                  ]
                }
              },
              then: 'created'
            },
            {
              case: {
                $eq: [
                  '$depositAccount.totalPaymentAmount',
                  '$depositAccount.depositAmount'
                ]
              },
              then: 'paid'
            },
            {
              case: {
                $gt: [
                  '$depositAccount.totalPaymentAmount',
                  '$depositAccount.depositAmount'
                ]
              },
              then: 'overpaid'
            },
            {
              case: {
                $lt: [
                  '$depositAccount.totalPaymentAmount',
                  '$depositAccount.depositAmount'
                ]
              },
              then: 'due'
            }
          ],
          default: ''
        }
      }
    }
  }
]

const getTenantReportForExcelManager = async (params) => {
  const { query, options, dateFormat, timeZone } = params
  const { skip, limit, sort } = options

  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    { $skip: skip },
    { $limit: limit },
    ...getUserPipelineForTenantReportOfExcelManager(),
    {
      $unwind: {
        path: '$properties',
        preserveNullAndEmptyArrays: true
      }
    },
    ...getInvoiceOverduePipelineForTenantOfExcelManager(),
    ...getPropertyPipelineForTenantOfExcelManager(),
    ...getContractPipelineForTenantOfExcelManager(),
    {
      $unwind: {
        path: '$propertyContract.addons',
        preserveNullAndEmptyArrays: true
      }
    },
    ...getAddonsPipelineForTenantOfExcelManager(),
    ...getMeeterReadingPipelineForTenantOfExcelManager(),
    ...getDepositAccountPipelineForTenantOfExcelManager(),
    {
      $addFields: {
        rentAddons: {
          $reduce: {
            input: '$rentAddons',
            initialValue: '',
            in: {
              $concat: ['$$value', ' - ', '$$this']
            }
          }
        },
        meeterNumber: {
          $reduce: {
            input: '$meeterNumber',
            initialValue: '',
            in: {
              $concat: ['$$value', ',', '$$this']
            }
          }
        }
      }
    },
    ...getFinalProjectForTenantOfexcelManager(dateFormat, timeZone),
    {
      $sort: sort
    }
  ]

  const tenantReport = await TenantCollection.aggregate(pipeline)
  console.log('# Tenant Report', size(tenantReport))
  return tenantReport || []
}

export const tenantsDataForExcelCreator = async (params, options) => {
  const { partnerId, userId } = params
  await appHelper.validateId({ userId })
  await appHelper.validateId({ partnerId })
  const userInfo = await userHelper.getAnUser({ _id: params.userId })
  const userLanguage = userInfo?.getLanguage()
  const tenantsQuery = await prepareTenantsQueryForExcelCreator(params)
  const dataCount = await countTenants(tenantsQuery)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'

  const queryData = {
    query: tenantsQuery,
    options,
    dateFormat,
    timeZone,
    partnerId
  }

  const tenants = await getTenantReportForExcelManager(queryData)

  if (size(tenants)) {
    for (const tenant of tenants) {
      tenant.status = appHelper.translateToUserLng(
        'common.' + tenant.status,
        userLanguage
      )
    }
  }
  return { data: tenants, total: dataCount }
}

export const queryForTenantExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.checkRequiredFields(['queueId'], query)
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_tenants') {
    const transactionData = await tenantsDataForExcelCreator(queueInfo.params, {
      skip,
      limit,
      sort
    })
    return transactionData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const getTenantsWithProjection = async (params = {}, session) => {
  const { query, options = {}, projection = '' } = params
  const { sort = {} } = options
  const tenants = await TenantCollection.find(query, projection)
    .populate('user', '_id profile')
    .sort(sort)
    .session(session)
  return tenants
}

export const prepareUserUpdateDataForInterestForm = async (params = {}) => {
  const { userData, userInfo, aboutYou } = params
  const updateData = {}
  if (size(userData)) {
    const { interestFormMeta = {} } = userInfo
    const { employerMeta = {}, previousEmployerMeta = {} } = interestFormMeta
    if (userData.phoneNumber && !userInfo.profile?.phoneNumber)
      updateData['profile.phoneNumber'] = userData.phoneNumber
    if (
      userData.norwegianNationalIdentification &&
      userInfo.profile?.norwegianNationalIdentification !==
        userData.norwegianNationalIdentification
    ) {
      const nidExist = await userHelper.getAnUser({
        'profile.norwegianNationalIdentification':
          userData.norwegianNationalIdentification
      })
      if (size(nidExist)) throw new CustomError(400, 'Nid already used')
      updateData['profile.norwegianNationalIdentification'] =
        userData.norwegianNationalIdentification
    }

    if (userData.birthday && userInfo.profile?.birthday !== userData.birthday)
      updateData['profile.birthday'] = userData.birthday

    if (userData.termsAcceptedOn)
      updateData['profile.termsAcceptedOn'] = userData.termsAcceptedOn

    if (aboutYou && userInfo.profile?.aboutMe !== aboutYou)
      updateData['profile.aboutMe'] = aboutYou
    if (userData.hasOwnProperty('isSmoker')) {
      updateData['profile.isSmoker'] = userData.isSmoker
    }
    if (userData.hasOwnProperty('hasPets')) {
      updateData['profile.hasPets'] = userData.hasPets
    }

    if (
      userData.employerName &&
      userData.employerName !== employerMeta.employerName
    )
      employerMeta.employerName = userData.employerName

    if (
      userData.employerPhoneNumber &&
      userData.employerPhoneNumber !== employerMeta.employerPhoneNumber
    )
      employerMeta.employerPhoneNumber = userData.employerPhoneNumber

    if (
      userData.workingPeriod &&
      userData.workingPeriod !== employerMeta.workingPeriod
    )
      employerMeta.workingPeriod = userData.workingPeriod

    if (
      userData.reference &&
      userData.reference !== previousEmployerMeta.reference
    )
      previousEmployerMeta.reference = userData.reference

    if (
      userData.previousLandlordName &&
      userData.previousLandlordName !==
        previousEmployerMeta.previousLandlordName
    )
      previousEmployerMeta.previousLandlordName = userData.previousLandlordName

    if (
      userData.previousLandlordPhoneNumber &&
      userData.previousLandlordPhoneNumber !==
        previousEmployerMeta.previousLandlordPhoneNumber
    )
      previousEmployerMeta.previousLandlordPhoneNumber =
        userData.previousLandlordPhoneNumber

    if (
      userData.previousLandlordEmail &&
      userData.previousLandlordEmail !==
        previousEmployerMeta.previousLandlordEmail
    )
      previousEmployerMeta.previousLandlordEmail =
        userData.previousLandlordEmail

    if (
      userData.movingFrom &&
      userData.movingFrom !== userInfo.profile?.hometown
    )
      updateData['profile.hometown'] = userData.movingFrom

    if (size(employerMeta)) interestFormMeta.employerMeta = employerMeta
    if (size(previousEmployerMeta))
      interestFormMeta.previousEmployerMeta = previousEmployerMeta

    if (size(interestFormMeta)) updateData.interestFormMeta = interestFormMeta
  }
  return updateData
}

export const prepareTenantCreateDataForInterestForm = (params = {}) => {
  const {
    propertyInfo = {},
    tenantInterestFormData = {},
    tenantPropertiesData = {},
    userId,
    userData,
    userInfo
  } = params
  const { aboutYou, creditRatingTermsAcceptedOn } = tenantInterestFormData
  const { wantsRentFrom, numberOfTenant, preferredLengthOfLease } =
    tenantPropertiesData

  const employerMeta = {}
  const interestFormMeta = {}
  const previousEmployerMeta = {}

  const tenantData = {
    propertyId: propertyInfo._id,
    status: 'interested',
    createdAt: new Date(),
    createdBy: userId
  }

  if (propertyInfo.accountId) tenantData.accountId = propertyInfo.accountId
  if (propertyInfo.branchId) tenantData.branchId = propertyInfo.branchId
  if (propertyInfo.agentId) tenantData.agentId = propertyInfo.agentId

  if (wantsRentFrom) tenantData.wantsRentFrom = wantsRentFrom
  if (preferredLengthOfLease)
    tenantData.preferredLengthOfLease = preferredLengthOfLease
  if (has(tenantPropertiesData, 'numberOfTenant'))
    tenantData.numberOfTenant = numberOfTenant

  if (userData.employerName) employerMeta.employerName = userData.employerName

  if (userData.employerPhoneNumber)
    employerMeta.employerPhoneNumber = userData.employerPhoneNumber

  if (userData.workingPeriod)
    employerMeta.workingPeriod = userData.workingPeriod

  if (userData.reference) previousEmployerMeta.reference = userData.reference

  if (userData.previousLandlordName)
    previousEmployerMeta.previousLandlordName = userData.previousLandlordName

  if (userData.previousLandlordPhoneNumber)
    previousEmployerMeta.previousLandlordPhoneNumber =
      userData.previousLandlordPhoneNumber

  if (userData.previousLanlordEmail)
    previousEmployerMeta.previousLanlordEmail = userData.previousLanlordEmail

  if (size(employerMeta)) interestFormMeta.employerMeta = employerMeta
  if (size(previousEmployerMeta))
    interestFormMeta.previousEmployerMeta = previousEmployerMeta

  if (size(userInfo.interestFormMeta?.fileIds))
    tenantData.fileIds = userInfo.interestFormMeta.fileIds

  if (size(interestFormMeta)) tenantData.interestFormMeta = interestFormMeta

  userData.properties = [tenantData]
  userData.type = 'active'
  userData.userId = userId
  userData.partnerId = propertyInfo.partnerId
  userData.name = userInfo.profile.name
  userData.createdBy = userId

  if (aboutYou) userData.aboutText = aboutYou

  if (creditRatingTermsAcceptedOn)
    userData.creditRatingTermsAcceptedOn = creditRatingTermsAcceptedOn

  if (userData.birthday) delete userData.birthday

  if (userData.email) delete userData.email

  return userData
}

export const prepareTenantUpdateDataForInterestForm = (params = {}) => {
  const {
    creditRatingTermsAcceptedOn,
    listingInfo = {},
    tenantInfo = {},
    tenantPropertiesData = {},
    userId,
    userData
  } = params
  const { properties = [] } = tenantInfo
  const updateData = {}
  let propertyInfo =
    properties.find((item) => item.propertyId === listingInfo._id) || {}
  const interestFormMeta = size(propertyInfo?.interestFormMeta)
    ? propertyInfo.interestFormMeta
    : {}
  const employerMeta = size(interestFormMeta.employerMeta)
    ? interestFormMeta.employerMeta
    : {}
  const previousEmployerMeta = size(interestFormMeta.previousEmployerMeta)
    ? interestFormMeta.previousEmployerMeta
    : {}

  if (!size(propertyInfo)) {
    propertyInfo = {
      propertyId: listingInfo._id,
      status: 'interested',
      createdAt: new Date(),
      createdBy: userId
    }

    if (listingInfo.accountId) propertyInfo.accountId = listingInfo.accountId
    if (listingInfo.branchId) propertyInfo.branchId = listingInfo.branchId
    if (listingInfo.agentId) propertyInfo.agentId = listingInfo.agentId
  }

  if (
    userData.employerName &&
    userData.employerName !== employerMeta.employerName
  )
    employerMeta.employerName = userData.employerName

  if (
    userData.employerPhoneNumber &&
    userData.employerPhoneNumber !== employerMeta.employerPhoneNumber
  )
    employerMeta.employerPhoneNumber = userData.employerPhoneNumber

  if (
    userData.workingPeriod &&
    userData.workingPeriod !== employerMeta.workingPeriod
  )
    employerMeta.workingPeriod = userData.workingPeriod

  if (
    userData.reference &&
    userData.reference !== previousEmployerMeta.reference
  )
    previousEmployerMeta.reference = userData.reference

  if (userData.reference === 'yes') {
    if (
      userData.previousLandlordName &&
      userData.previousLandlordName !==
        previousEmployerMeta.previousLandlordName
    )
      previousEmployerMeta.previousLandlordName = userData.previousLandlordName

    if (
      userData.previousLandlordPhoneNumber &&
      userData.previousLandlordPhoneNumber !==
        previousEmployerMeta.previousLandlordPhoneNumber
    )
      previousEmployerMeta.previousLandlordPhoneNumber =
        userData.previousLandlordPhoneNumber

    if (
      userData.previousLanlordEmail &&
      userData.previousLanlordEmail !==
        previousEmployerMeta.previousLanlordEmail
    )
      previousEmployerMeta.previousLanlordEmail = userData.previousLanlordEmail
  } else {
    delete previousEmployerMeta.previousLandlordName
    delete previousEmployerMeta.previousLandlordPhoneNumber
    delete previousEmployerMeta.previousLanlordEmail
  }

  if (
    tenantPropertiesData.wantsRentFrom &&
    tenantPropertiesData.wantsRentFrom !== propertyInfo.wantsRentFrom
  )
    propertyInfo.wantsRentFrom = tenantPropertiesData.wantsRentFrom

  if (
    tenantPropertiesData.numberOfTenant &&
    tenantPropertiesData.numberOfTenant !== propertyInfo.numberOfTenant
  )
    propertyInfo.numberOfTenant = tenantPropertiesData.numberOfTenant
  if (tenantPropertiesData.preferredLengthOfLease)
    propertyInfo.preferredLengthOfLease =
      tenantPropertiesData.preferredLengthOfLease

  if (size(employerMeta)) interestFormMeta.employerMeta = employerMeta
  if (size(previousEmployerMeta))
    interestFormMeta.previousEmployerMeta = previousEmployerMeta

  if (size(interestFormMeta)) propertyInfo.interestFormMeta = interestFormMeta

  const updatedProperties = properties.filter(
    (item) => item.propertyId !== propertyInfo.propertyId
  )

  updatedProperties.push(propertyInfo)

  if (size(updatedProperties)) updateData.properties = updatedProperties

  if (creditRatingTermsAcceptedOn)
    updateData.creditRatingTermsAcceptedOn = creditRatingTermsAcceptedOn

  return updateData
}

export const hasCreditRating = async (tenantInfoOrId) => {
  if (!size(tenantInfoOrId)) return false

  let tenantInfo = {}
  if (isString(tenantInfoOrId)) {
    tenantInfo = (await getTenantById(tenantInfoOrId)) || {}
  } else {
    tenantInfo = tenantInfoOrId
  }

  return !!tenantInfo?.creditRatingInfo?.CDG2_GENERAL_SCORE?.SCORE
}

const prepareQueryForInterestFormsQuery = (query) => {
  const { partnerId, propertyId, tenantId, accountId, agentId, branchId } =
    query
  const preparedQuery = { partnerId }
  const elemMatchQuery = {
    status: { $in: ['interested', 'not_interested'] },
    numberOfTenant: { $gt: 0 }
  }
  if (tenantId) preparedQuery._id = tenantId
  if (accountId) elemMatchQuery.accountId = accountId
  if (agentId) elemMatchQuery.agentId = agentId
  if (branchId) elemMatchQuery.branchId = branchId
  if (propertyId) elemMatchQuery.propertyId = propertyId
  preparedQuery.properties = { $elemMatch: elemMatchQuery }
  return preparedQuery
}

const getPropertiesQueryForInterestForm = (propertyId) => {
  const propertiesQuery = {
    'properties.status': { $in: ['interested', 'not_interested'] },
    'properties.numberOfTenant': { $gt: 0 }
  }
  if (propertyId) propertiesQuery['properties.propertyId'] = propertyId
  return propertiesQuery
}

const getTenantInfoPipeline = () => [
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
            email: 1,
            phoneNumber: '$profile.phoneNumber',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo'),
  {
    $project: {
      _id: 1,
      serial: 1,
      name: 1,
      userInfo: 1,
      status: '$properties.status',
      submittedAt: '$properties.createdAt',
      fileIds: '$properties.fileIds',
      propertyId: '$properties.propertyId'
    }
  }
]

const getPropertyInfoPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'properties.propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            imageUrl: 1,
            location: {
              name: 1,
              postalCode: 1,
              city: 1,
              country: 1
            },
            propertyTypeId: 1,
            listingTypeId: 1,
            apartmentId: 1,
            serial: 1,
            listed: 1,
            propertyStatus: 1,
            hasActiveLease: 1,
            hasUpcomingLease: 1,
            hasInProgressLease: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo'),
  {
    $project: {
      _id: 1,
      propertyInfo: 1,
      status: '$properties.status',
      submittedAt: '$properties.createdAt',
      fileIds: '$properties.fileIds',
      propertyId: '$properties.propertyId'
    }
  }
]

const getPipelineForInterestFormInfo = (context) => {
  let pipeline = []
  if (context === 'property') pipeline = getTenantInfoPipeline()
  else pipeline = getPropertyInfoPipeline()
  return pipeline
}

const getFileInfoPipeline = () => [
  {
    $lookup: {
      from: 'files',
      localField: 'fileIds',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: 1,
            title: 1
          }
        }
      ],
      as: 'filesInfo'
    }
  }
]

const getInterestForms = async (params = {}) => {
  const { query = {}, options = {}, propertyId, context } = params
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    appHelper.getUnwindPipeline('properties', false),
    {
      $match: getPropertiesQueryForInterestForm(propertyId)
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
    ...getPipelineForInterestFormInfo(context),
    ...getFileInfoPipeline(),
    {
      $project: {
        _id: 1,
        serial: 1,
        name: 1,
        userInfo: 1,
        status: 1,
        submittedAt: 1,
        propertyId: 1,
        propertyInfo: 1,
        filesInfo: 1
      }
    }
  ]
  const forms = (await TenantCollection.aggregate(pipeline)) || []
  return forms
}

const countInterestForms = async ({ query, propertyId }) => {
  const propertiesQuery = getPropertiesQueryForInterestForm(propertyId)
  const pipeline = [
    {
      $match: query
    },
    appHelper.getUnwindPipeline('properties', false),
    {
      $match: propertiesQuery
    }
  ]
  const forms = (await TenantCollection.aggregate(pipeline)) || []
  return forms.length
}

export const queryInterestForms = async (req) => {
  const { body = {}, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query = {}, options } = body
  appHelper.checkRequiredFields(['context'], query)
  appHelper.validateSortForQuery(options.sort)
  const { context, propertyId } = query
  const b2cUserInfo =
    await appHelper.validateSelfServicePartnerRequestAndUpdateBody(
      user,
      session
    )
  if (size(b2cUserInfo)) {
    query.accountId = b2cUserInfo.accountId
    query.agentId = b2cUserInfo.agentId
    query.branchId = b2cUserInfo.branchId
  }
  if (context === 'b2c') appHelper.checkRequiredFields(['accountId'], query)
  else if (context === 'tenant')
    appHelper.checkRequiredFields(['tenantId'], query)
  else appHelper.checkRequiredFields(['propertyId'], query)

  const { partnerId } = user
  query.partnerId = partnerId
  body.query = prepareQueryForInterestFormsQuery(query)
  const data = await getInterestForms({
    ...body,
    propertyId,
    context
  })
  const totalDocuments = await countInterestForms({
    query: body.query,
    propertyId
  })
  return {
    data,
    metaData: {
      filteredDocuments: totalDocuments,
      totalDocuments
    }
  }
}

const isShowNewLease = ({
  isContractStatusRight,
  propertyStatus,
  leaseContract,
  isDirectPartner,
  hasUpcomingContract
}) => [
  {
    $addFields: {
      isShowNewLease: {
        $cond: {
          if: {
            $eq: [isDirectPartner, true]
          },
          then: {
            $cond: {
              if: {
                $and: [
                  {
                    $not: {
                      $in: [
                        '$properties.status',
                        ['not_interested', 'rejected']
                      ]
                    }
                  },
                  { $eq: [isContractStatusRight, true] },
                  { $not: { $eq: [propertyStatus, 'archived'] } }
                ]
              },
              then: true,
              else: false
            }
          },
          else: {
            $cond: {
              if: {
                $and: [
                  { $not: { $eq: ['$properties.status', 'not_interested'] } },
                  {
                    $or: [
                      {
                        $and: [
                          { $eq: [leaseContract?.status, 'active'] },
                          { $eq: [!!size(hasUpcomingContract), true] },
                          {
                            $not: { $eq: [propertyStatus, 'archived'] }
                          },
                          {
                            $ifNull: [
                              leaseContract?.rentalMeta?.contractEndDate,
                              false
                            ]
                          },
                          {
                            $not: {
                              $ifNull: [
                                hasUpcomingContract?.rentalMeta?.tenantId,
                                false
                              ]
                            }
                          }
                        ]
                      },
                      {
                        $and: [
                          {
                            $not: { $ifNull: [leaseContract?.status, false] }
                          },
                          {
                            $not: {
                              $ifNull: [
                                hasUpcomingContract?.rentalMeta?.tenantId,
                                false
                              ]
                            }
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              then: true,
              else: false
            }
          }
        }
      }
    }
  }
]

const getTenantInfoForProspectedContract = async ({
  query,
  options,
  propertyId,
  status
}) => {
  const partner = await partnerHelper.getPartnerById(query.partnerId)
  const enableCreditRating = partner?.enableCreditRating
  const propertyQuery = [{ $eq: ['$$property.propertyId', propertyId] }]
  if (status === 'all') {
    propertyQuery.push({ $not: { $eq: ['$$property.status', 'active'] } })
  } else propertyQuery.push({ $eq: ['$$property.status', status] })
  const { limit, skip, sort } = options
  const contracts =
    (await contractHelper.getContracts({
      propertyId,
      partnerId: partner._id
    })) || []
  const property = await listingHelper.getListingById(propertyId)
  const isDirectPartner = partner?.accountType === 'direct'
  const hasUpcomingContract = contracts.find(
    (contract) => contract.status === 'upcoming'
  )
  const leaseContract = contracts.find(
    (contract) => contract.hasRentalContract === true
  )
  const hasInProgressContract = contracts.find(
    (contract) =>
      ['in_progress', 'upcoming'].includes(contract.status) &&
      contract.rentalMeta?.status === 'in_progress'
  )
  const isContractStatusRight = !!(
    (!leaseContract?.status &&
      hasUpcomingContract &&
      !hasUpcomingContract.rentalMeta?.tenantId) ||
    (leaseContract?.status === 'active' &&
      leaseContract?.rentalMeta?.contractEndDate &&
      !hasInProgressContract)
  )

  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        properties: {
          $first: {
            $filter: {
              input: '$properties',
              as: 'property',
              cond: {
                $and: propertyQuery
              }
            }
          }
        }
      }
    },
    appHelper.getUnwindPipeline('properties'),
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
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        pipeline: [
          {
            $project: {
              _id: 1,
              avatarKey:
                appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
            }
          }
        ],
        as: 'userInfo'
      }
    },
    appHelper.getUnwindPipeline('userInfo'),
    ...isShowNewLease({
      isContractStatusRight,
      propertyStatus: property?.propertyStatus,
      leaseContract,
      isDirectPartner,
      hasUpcomingContract
    }),
    {
      $project: {
        _id: 1,
        name: 1,
        createdAt: '$properties.createdAt',
        status: '$properties.status',
        userInfo: 1,
        isShowNewLease: 1,
        isShowInterestForm: {
          $cond: [
            {
              $or: [
                {
                  $ifNull: ['$properties.numberOfTenant', false]
                },
                {
                  $ifNull: ['$properties.interestFormMeta', false]
                }
              ]
            },
            true,
            false
          ]
        },
        creditRatingScore: {
          $cond: [
            {
              $and: [
                { $eq: [enableCreditRating, true] },
                { $eq: ['$properties.status', 'interested'] }
              ]
            },
            '$creditRatingInfo.CDG2_GENERAL_SCORE.SCORE',
            null
          ]
        },
        riskClass: '$creditRatingInfo.CDG2_GENERAL.RISKCLASS'
      }
    },
    {
      $addFields: {
        creditRatingRiskClass: {
          $cond: [
            {
              $and: [
                { $eq: [enableCreditRating, true] },
                { $eq: ['$status', 'interested'] }
              ]
            },
            {
              $switch: {
                branches: [
                  {
                    case: { $eq: ['$riskClass', '1'] },
                    then: 'very_high_risk'
                  },
                  { case: { $eq: ['$riskClass', '2'] }, then: 'high_risk' },
                  { case: { $eq: ['$riskClass', '3'] }, then: 'medium_risk' },
                  { case: { $eq: ['$riskClass', '4'] }, then: 'low_risk' },
                  { case: { $eq: ['$riskClass', '5'] }, then: 'very_low_risk' }
                ],
                default: 'score_not_calculated'
              }
            },
            null
          ]
        }
      }
    }
  ]

  const prospects = await TenantCollection.aggregate(pipeline)
  return prospects || []
}

const prepareProspectQuery = (query) => {
  const { contractId, partnerId, propertyId, status } = query
  const queryData = { partnerId }
  const elemMatchQuery = { propertyId }
  if (status === 'all') {
    elemMatchQuery.status = {
      $ne: 'active'
    }
  } else elemMatchQuery.status = status
  if (contractId) elemMatchQuery.contractId = contractId

  queryData['properties'] = { $elemMatch: elemMatchQuery }

  return queryData
}

export const getProspects = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { options, query } = body
  query.partnerId = partnerId
  appHelper.checkRequiredFields(['propertyId', 'status'], query)
  const { propertyId, status } = query
  const queryData = prepareProspectQuery(query)
  const prospects = await getTenantInfoForProspectedContract({
    query: queryData,
    options,
    propertyId,
    status
  })
  const filteredDocuments = await countTenants(queryData)
  const totalDocuments = await countTenants({
    partnerId,
    'properties.propertyId': propertyId
  })
  return {
    data: prospects,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const validateDataForUpdateTenantPropertyStatus = async (body = {}) => {
  appHelper.checkRequiredFields(['tenantId', 'propertyId', 'status'], body)
  const { partnerId, propertyId, status, tenantId } = body
  appHelper.validateId({ tenantId })
  appHelper.validateId({ propertyId })
  const tenantInfo = await getATenant({
    _id: tenantId,
    partnerId
  })
  if (!size(tenantInfo)) throw new CustomError(404, 'Tenant not found')
  const propertyInfo = tenantInfo.properties.find(
    (item) => item.propertyId === propertyId
  )
  if (!size(propertyInfo)) throw new CustomError(404, 'Property not found')
  const allowedStatus = {
    interested: ['invited', 'not_interested', 'rejected'],
    not_interested: ['invited', 'interested'],
    rejected: ['interested']
  }
  if (!allowedStatus[status].includes(propertyInfo.status))
    throw new CustomError(
      400,
      `Not changed status ${status} from ${propertyInfo.status}`
    )
}

const validateRequestForAddTenantCreditRatingInfo = async (body) => {
  const { partnerId, propertyId, tenantId } = body
  const { enableCreditRating } =
    (await partnerHelper.getPartnerById(partnerId)) || {}
  if (!enableCreditRating)
    throw new CustomError(400, 'Partner credit rating is not enabled')
  const tenantInfo = await getATenant({ _id: tenantId, partnerId })
  if (!size(tenantInfo)) throw new CustomError(404, 'Tenant not found')
  if (propertyId) {
    const propertyInfo = await listingHelper.getAListing({ _id: propertyId })
    if (!size(propertyInfo)) throw new CustomError(404, 'Property not found')
  }
  const userInfo = await userHelper.getAnUser({ _id: tenantInfo.userId })
  const SSN = userInfo?.profile?.norwegianNationalIdentification
  if (!SSN) throw new CustomError(404, 'SSN not found')
  return { SSN }
}

export const validateAndPrepareDataForAddTenantCreditRatingInfo = async (
  body
) => {
  const { creditRatingTermsAcceptedOn, partnerId, propertyId, tenantId } = body
  const { SSN } = await validateRequestForAddTenantCreditRatingInfo(body)
  let creditRatingTermsAcceptedOnDate
  const tenantData = {}
  if (creditRatingTermsAcceptedOn) {
    creditRatingTermsAcceptedOnDate = new Date()
    tenantData.query = { _id: tenantId }
    tenantData.data = {
      $set: { creditRatingTermsAcceptedOn: creditRatingTermsAcceptedOnDate }
    }
  }

  const appQueueData = {
    action: 'handle_credit_rating',
    event: 'credit_rating',
    delaySeconds: 0,
    destination: 'credit-rating',
    params: {
      partnerId,
      tenantId,
      creditRatingTermsAcceptedOn: creditRatingTermsAcceptedOnDate,
      ssn: SSN,
      processType: 'add_credit_rating',
      createdBy: body.userId
    },
    priority: 'immediate',
    status: 'new'
  }
  if (propertyId) appQueueData.params.propertyId = propertyId
  return {
    appQueueData,
    tenantData
  }
}

export const getTenantsByAggregate = async (preparedQuery) => {
  if (!size(preparedQuery)) {
    throw new CustomError(404, 'Query not found to get tenants')
  }
  console.log('=== preparedQuery', JSON.stringify(preparedQuery))
  const tenants =
    (await TenantCollection.aggregate([
      {
        $match: preparedQuery
      },
      {
        $lookup: {
          from: 'users',
          as: 'user',
          localField: 'userId',
          foreignField: '_id'
        }
      },
      {
        $unwind: '$user'
      },
      {
        $group: {
          _id: null,
          tenantIdsWithPhoneNumbers: {
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.phoneNumber', ''] },
                    { $ifNull: ['$user.profile.phoneNumber', false] }
                  ]
                },
                then: '$_id',
                else: '$$REMOVE'
              }
            }
          },
          tenantNamesWithoutPhoneNumbers: {
            // if tenant user has NO phone number, add user's name in array
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.phoneNumber', ''] },
                    { $ifNull: ['$user.profile.phoneNumber', false] }
                  ]
                },
                then: '$$REMOVE',
                else: '$name'
              }
            }
          },
          tenantNamesWithPhoneNumbers: {
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.phoneNumber', ''] },
                    { $ifNull: ['$user.profile.phoneNumber', false] }
                  ]
                },
                then: '$name',
                else: '$$REMOVE'
              }
            }
          }
        }
      }
    ])) || []
  return tenants
}

const prepareTenantQuery = (params) => {
  const { referenceNumber } = params
  const queryData = {}

  if (referenceNumber)
    queryData['depositAccountMeta.kycForms'] = {
      $elemMatch: { referenceNumber }
    }
  return queryData
}

export const queryTenant = async (req) => {
  const { body, user } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query } = body
  if (!size(query))
    throw new CustomError(400, 'Missing queryData in request body')

  const queryData = prepareTenantQuery(query)

  if (!size(queryData)) throw new CustomError(404, 'Missing queryData')
  console.log(`queryData: ${JSON.stringify(queryData)}`)
  return await getATenant(queryData)
}

export const getTenantKycFormData = async (req) => {
  const { body } = req
  const { referenceNumber } = body?.query || {}
  if (!referenceNumber) throw new CustomError(400, 'Missing referenceNumber')

  const tenantInfo = await getATenant(
    {
      'depositAccountMeta.kycForms': { $elemMatch: { referenceNumber } }
    },
    undefined,
    ['user']
  )
  if (!size(tenantInfo)) throw new CustomError(400, 'Wrong reference number')

  const norwegianNationalIdentification =
    tenantInfo.user?.profile?.norwegianNationalIdentification

  if (!norwegianNationalIdentification) {
    throw new CustomError(400, 'Not found Norwegian National id')
  }
  const { kycForms = [] } = tenantInfo?.depositAccountMeta || {}
  const kycFormData =
    kycForms.find((kycData) => kycData.referenceNumber === referenceNumber) ||
    {}

  if (!size(kycFormData)) throw new CustomError(400, 'Kyc form not found')
  const {
    contractId,
    isFormSubmitted = false,
    isSubmitted = false
  } = kycFormData
  const signingUrl = await depositAccountHelper.getTenantLeaseSigningUrl(
    contractId,
    tenantInfo._id
  )

  return {
    isFormSubmitted: isSubmitted || isFormSubmitted,
    norwegianNationalIdentification,
    signingUrl
  }
}

const getPipelineForInterestPreviewPropertyInfo = (propertyId) => [
  {
    $addFields: {
      propertyInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$properties', []] },
            as: 'property',
            cond: {
              $eq: [propertyId, '$$property.propertyId']
            }
          }
        }
      }
    }
  }
]

export const getTenantSSN = async (req) => {
  const { body } = req
  appHelper.checkRequiredFields(['tenantId'], body)
  const { tenantId } = body
  const tenantQuery = { _id: tenantId }
  const tenantInfo = await getATenant(tenantQuery, undefined, ['user'])
  if (!tenantInfo) {
    throw new CustomError(404, 'Tenant not found')
  }
  return {
    norwegianNationalIdentification:
      tenantInfo?.user?.profile?.norwegianNationalIdentification
  }
}

export const getInterestFormPreview = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['propertyId', 'tenantId'])
  const { body = {}, session, user } = req
  const { partnerId, propertyId, tenantId } = body
  const dataNeedTobeMerge =
    await appHelper.validateSelfServicePartnerRequestAndUpdateBody(
      user,
      session
    )
  assign(body, dataNeedTobeMerge)
  const query = {
    _id: tenantId,
    partnerId
  }
  const [result = {}] =
    (await TenantCollection.aggregate([
      {
        $match: query
      },
      {
        $lookup: {
          from: 'users',
          localField: 'userId',
          foreignField: '_id',
          pipeline: [
            ...appHelper.getUserEmailPipeline(),
            {
              $project: {
                aboutMe: '$profile.aboutMe',
                avatarKey:
                  appHelper.getUserAvatarKeyPipeline('$profile.avatarKey'),
                birthday: '$profile.birthday',
                email: 1,
                phoneNumber: '$profile.phoneNumber',
                hasPets: '$profile.hasPets',
                isSmoker: '$profile.isSmoker'
              }
            }
          ],
          as: 'userInfo'
        }
      },
      appHelper.getUnwindPipeline('userInfo'),
      ...getPipelineForInterestPreviewPropertyInfo(propertyId),
      {
        $project: {
          _id: 1,
          name: '$name',
          avatarKey: '$userInfo.avatarKey',
          email: '$userInfo.email',
          birthday: '$userInfo.birthday',
          phoneNumber: '$userInfo.phoneNumber',
          aboutMe: '$userInfo.aboutMe',
          hasPets: '$userInfo.hasPets',
          isSmoker: '$userInfo.isSmoker',
          userId: '$userId',
          wantsRentFrom: '$propertyInfo.wantsRentFrom',
          numberOfTenant: '$propertyInfo.numberOfTenant',
          employerName:
            '$propertyInfo.interestFormMeta.employerMeta.employerName',
          employerPhoneNumber:
            '$propertyInfo.interestFormMeta.employerMeta.employerPhoneNumber',
          workingPeriod:
            '$propertyInfo.interestFormMeta.employerMeta.workingPeriod',
          preferredLengthOfLease: '$propertyInfo.preferredLengthOfLease'
        }
      }
    ])) || []
  return result
}

export const hasTenantCreditInfo = async (tenantId) => {
  const tenant = await getATenant({
    _id: tenantId,
    'creditRatingInfo.CDG2_GENERAL_SCORE.SCORE': { $exists: true }
  })
  return size(tenant) ? true : false
}

export const getTenantIdsBasedOnLeaseStatus = async (tenantQuery) => {
  console.log('=== tenantQuery', JSON.stringify(tenantQuery))
  const response = await TenantCollection.aggregate([
    { $match: tenantQuery },
    {
      $lookup: {
        from: 'contracts',
        localField: '_id',
        foreignField: 'rentalMeta.tenants.tenantId',
        pipeline: [
          {
            $addFields: {
              isLeaseActive: {
                $cond: {
                  if: { $eq: ['$rentalMeta.status', 'closed'] },
                  then: false,
                  else: true
                }
              }
            }
          },
          { $project: { _id: 1, isLeaseActive: 1 } }
        ],
        as: 'contract'
      }
    },
    {
      $unwind: {
        path: '$contract',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: '$contract.isLeaseActive',
        tenantIds: { $addToSet: '$_id' },
        names: { $addToSet: '$name' }
      }
    }
  ])

  console.log('===> Response from TenantQuery', JSON.stringify(response))

  let tenantIdsWithActiveLease = []
  let tenantNamesWithActiveLease = []
  let tenantIdsWithClosedLease = []
  let tenantNamesWithClosedLease = []
  for (const tenant of response) {
    const { _id, tenantIds, names } = tenant
    if (_id === true) {
      tenantIdsWithActiveLease = tenantIds
      tenantNamesWithActiveLease = names
    }
    if (_id === false) {
      tenantIdsWithClosedLease = tenantIds
      tenantNamesWithClosedLease = names
    }
  }

  return {
    tenantIdsWithActiveLease,
    tenantIdsWithClosedLease,
    tenantNamesWithClosedLease,
    tenantNamesWithActiveLease
  }
}
export const getTenantIdsBasedOnActiveLeaseAndPhoneNumbers = async (
  tenantQuery
) => {
  console.log('=== tenantQuery', JSON.stringify(tenantQuery))
  const response = await TenantCollection.aggregate([
    { $match: tenantQuery },
    {
      $lookup: {
        from: 'contracts',
        localField: '_id',
        foreignField: 'rentalMeta.tenants.tenantId',
        pipeline: [
          {
            $addFields: {
              isLeaseActive: {
                $cond: {
                  if: { $eq: ['$rentalMeta.status', 'closed'] },
                  then: false,
                  else: true
                }
              }
            }
          },
          { $project: { _id: 1, isLeaseActive: 1 } }
        ],
        as: 'contract'
      }
    },
    {
      $unwind: {
        path: '$contract',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        as: 'user',
        localField: 'userId',
        foreignField: '_id'
      }
    },
    {
      $unwind: '$user'
    },
    { $match: { 'contract.isLeaseActive': true } },
    {
      $group: {
        _id: null,
        tenantIdsWithPhoneNumbers: {
          $push: {
            $cond: {
              if: {
                $and: [
                  { $ne: ['$user.profile.phoneNumber', ''] },
                  { $ifNull: ['$user.profile.phoneNumber', false] }
                ]
              },
              then: '$_id',
              else: '$$REMOVE'
            }
          }
        },
        tenantNamesWithoutPhoneNumbers: {
          // if tenant user has NO phone number, add user's name in array
          $push: {
            $cond: {
              if: {
                $and: [
                  { $ne: ['$user.profile.phoneNumber', ''] },
                  { $ifNull: ['$user.profile.phoneNumber', false] }
                ]
              },
              then: '$$REMOVE',
              else: '$name'
            }
          }
        }
      }
    }
  ])

  console.log('===> Response from TenantQuery', JSON.stringify(response))

  return response
}

export const getTenantNamesByAggregate = async (query) => {
  if (!size(query)) throw new CustomError(404, 'Query not found to get tenants')

  console.log('=== prepared tenant Query', JSON.stringify(query))
  const tenants =
    (await TenantCollection.aggregate([
      { $match: query },
      {
        $lookup: {
          from: 'users',
          as: 'user',
          localField: 'userId',
          foreignField: '_id'
        }
      },
      {
        $unwind: '$user'
      },
      {
        $group: {
          _id: null,
          tenantNames: {
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.name', ''] },
                    { $ifNull: ['$user.profile.name', false] }
                  ]
                },
                then: '$name',
                else: '$$REMOVE'
              }
            }
          }
        }
      }
    ])) || []
  return tenants
}
