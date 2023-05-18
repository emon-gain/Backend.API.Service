import {
  compact,
  concat,
  difference,
  each,
  filter,
  get,
  includes,
  indexOf,
  intersection,
  map,
  omit,
  size,
  union,
  uniq
} from 'lodash'
import moment from 'moment-timezone'
import {
  accountingHelper,
  appHelper,
  appQueueHelper,
  appRoleHelper,
  branchHelper,
  contractHelper,
  counterHelper,
  dashboardHelper,
  listingHelper,
  partnerHelper,
  partnerSettingHelper,
  propertyItemHelper,
  propertyRoomHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import { counterService } from '../services'
import { CustomError } from '../common'
import {
  AppRoleCollection,
  ContractCollection,
  ConversationCollection,
  ListingCollection,
  PropertyItemCollection,
  PropertyRoomCollection,
  SettingCollection,
  TenantCollection
} from '../models'

export const getPropertyAndAccountIds = async (query = {}) => {
  const result = await ListingCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: null,
        accountIds: { $addToSet: '$accountId' },
        propertyIds: { $addToSet: '$_id' }
      }
    }
  ])
  return result
}

const getTotalDuePipeLine = (params) => {
  const { tenantId, requestFrom } = params
  const matchStage = {
    $match: {
      $expr: {
        $eq: ['$invoiceType', 'invoice']
      }
    }
  }
  if (tenantId && requestFrom === 'tenant') {
    matchStage['$match']['$or'] = [
      { 'tenants.tenantId': tenantId },
      { tenantId }
    ]
  }
  // matchStage['$match']['tenants.tenantId'] = tenantId
  return [
    {
      $lookup: {
        from: 'invoices',
        localField: '_id',
        foreignField: 'propertyId',
        pipeline: [matchStage],
        as: 'invoices'
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
        imageUrl: { $first: '$imageUrl' },
        location: { $first: '$location' },
        serial: { $first: '$serial' },
        propertyTypeId: { $first: '$propertyTypeId' },
        listingTypeId: { $first: '$listingTypeId' },
        apartmentId: { $first: '$apartmentId' },
        listed: { $first: '$listed' },
        floor: { $first: '$floor' },
        propertyStatus: { $first: '$propertyStatus' },
        hasActiveLease: { $first: '$hasActiveLease' },
        hasUpcomingLease: { $first: '$hasUpcomingLease' },
        hasInProgressLease: { $first: '$hasInProgressLease' },
        isSoonEnding: { $first: '$isSoonEnding' },
        isTerminated: { $first: '$isTerminated' },
        placeSize: { $first: '$placeSize' },
        noOfAvailableBedrooms: { $first: '$noOfAvailableBedrooms' },
        noOfBedrooms: { $first: '$noOfBedrooms' },
        agentInfo: { $first: '$agentInfo' },
        accountInfo: { $first: '$accountInfo' },
        branchInfo: { $first: '$branchInfo' },
        createdAt: { $first: '$createdAt' },
        contractsInfo: { $first: '$contractsInfo' },
        activeContract: { $first: '$activeContract' },
        upcomingContract: { $first: '$upcomingContract' },
        monthlyRentAmount: { $first: '$monthlyRentAmount' },
        depositAmount: { $first: '$depositAmount' },
        availabilityStartDate: { $first: '$availabilityStartDate' },
        availabilityEndDate: { $first: '$availabilityEndDate' },
        minimumStay: { $first: '$minimumStay' },
        tenantInfo: { $first: '$tenantInfo' },
        hasAssignment: { $first: '$hasAssignment' }
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
}

const getPipelineForTenantPropertyListDetails = (tenantId) => [
  {
    $addFields: {
      activeContract: {
        $cond: [
          {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: {
                      $ifNull: ['$activeContract.rentalMeta.tenants', []]
                    },
                    as: 'tenant',
                    cond: {
                      $eq: [tenantId, '$$tenant.tenantId']
                    }
                  }
                }
              },
              0
            ]
          },
          '$activeContract',
          null
        ]
      },
      upcomingContract: {
        $cond: [
          {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: {
                      $ifNull: ['$upcomingContract.rentalMeta.tenants', []]
                    },
                    as: 'tenant',
                    cond: {
                      $eq: [tenantId, '$$tenant.tenantId']
                    }
                  }
                }
              },
              0
            ]
          },
          '$upcomingContract',
          null
        ]
      }
    }
  },
  {
    $addFields: {
      isSoonEnding: {
        $cond: [
          {
            $ifNull: ['$activeContract', false]
          },
          '$isSoonEnding',
          null
        ]
      },
      isTerminated: {
        $cond: [
          {
            $ifNull: ['$activeContract', false]
          },
          '$isTerminated',
          null
        ]
      }
    }
  },
  {
    $addFields: {
      inProgressContract: {
        $first: {
          $filter: {
            input: { $ifNull: ['$contractsInfo', []] },
            as: 'contract',
            cond: {
              $and: [
                {
                  $in: [
                    tenantId,
                    { $ifNull: ['$$contract.rentalMeta.tenants.tenantId', []] }
                  ]
                },
                {
                  $eq: ['$$contract.status', 'in_progress']
                }
              ]
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      closedContracts: {
        $filter: {
          input: { $ifNull: ['$contractsInfo', []] },
          as: 'contract',
          cond: {
            $and: [
              {
                $eq: ['$$contract.status', 'closed']
              }
            ]
          }
        }
      }
    }
  },
  {
    $addFields: {
      closedContracts: {
        $map: {
          input: { $ifNull: ['$closedContracts', []] },
          as: 'contract',
          in: {
            $concatArrays: [
              {
                $cond: [
                  { $ifNull: ['$$contract.rentalMeta', false] },
                  ['$$contract.rentalMeta'],
                  []
                ]
              },
              { $ifNull: ['$$contract.rentalMetaHistory', []] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$closedContracts',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      closedContract: {
        $filter: {
          input: '$closedContracts',
          as: 'rentalMeta',
          cond: {
            $in: [tenantId, { $ifNull: ['$$rentalMeta.tenants.tenantId', []] }]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$closedContract',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $sort: {
      'closedContracts.contractEndDate': -1
    }
  },
  {
    $group: {
      _id: '$_id',
      totalDue: { $first: '$totalDue' },
      totalOverDue: { $first: '$totalOverDue' },
      imageUrl: { $first: '$imageUrl' },
      location: { $first: '$location' },
      serial: { $first: '$serial' },
      propertyTypeId: { $first: '$propertyTypeId' },
      listingTypeId: { $first: '$listingTypeId' },
      apartmentId: { $first: '$apartmentId' },
      listed: { $first: '$listed' },
      floor: { $first: '$floor' },
      propertyStatus: { $first: '$propertyStatus' },
      hasActiveLease: { $first: '$hasActiveLease' },
      hasUpcomingLease: { $first: '$hasUpcomingLease' },
      hasInProgressLease: { $first: '$hasInProgressLease' },
      isSoonEnding: { $first: '$isSoonEnding' },
      isTerminated: { $first: '$isTerminated' },
      placeSize: { $first: '$placeSize' },
      noOfAvailableBedrooms: { $first: '$noOfAvailableBedrooms' },
      noOfBedrooms: { $first: '$noOfBedrooms' },
      agentInfo: { $first: '$agentInfo' },
      accountInfo: { $first: '$accountInfo' },
      branchInfo: { $first: '$branchInfo' },
      createdAt: { $first: '$createdAt' },
      activeContract: { $first: '$activeContract' },
      upcomingContract: { $first: '$upcomingContract' },
      closedContract: { $first: '$closedContract' },
      inProgressContract: { $first: '$inProgressContract' },
      monthlyRentAmount: { $first: '$monthlyRentAmount' },
      depositAmount: { $first: '$depositAmount' },
      availabilityStartDate: { $first: '$availabilityStartDate' },
      availabilityEndDate: { $first: '$availabilityEndDate' },
      minimumStay: { $first: '$minimumStay' },
      tenantInfo: { $first: '$tenantInfo' }
    }
  }
]

const getTenantInfoPipeline = () => [
  {
    $addFields: {
      tenantId: {
        $switch: {
          branches: [
            {
              case: {
                $ifNull: ['$activeContract', false]
              },
              then: '$activeContract.rentalMeta.tenantId'
            },
            {
              case: {
                $ifNull: ['$upcomingContract', false]
              },
              then: '$upcomingContract.rentalMeta.tenantId'
            }
          ],
          default: ''
        }
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: 1,
            userId: 1
          }
        },
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            pipeline: [
              {
                $project: {
                  avatarKey:
                    appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
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
            avatarKey: '$user.avatarKey'
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

export const getPropertiesForQuery = async (params = {}, user = {}) => {
  const { query, options = {}, requestFrom, tenantId } = params
  const { limit, skip, sort } = options
  const { partnerId } = user
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const soonEndingMonths = partnerSetting?.propertySettings?.soonEndingMonths
  const soonEndingMonthsDate = (
    await appHelper.getActualDate(partnerSetting, true)
  )
    .add(soonEndingMonths || 4, 'months')
    .toDate()
  let dataPipeline = [
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
    ...appHelper.getListingFirstImageUrl('$images'),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appHelper.getSoonEndingTerminatedActiveUpcomingContractPipeline(
      soonEndingMonthsDate,
      true,
      true
    ),
    ...getTenantInfoPipeline()
  ]
  if (!appHelper.isPartnerJanitor(user.roles) || size(user.roles) > 1) {
    const duePipeLine = getTotalDuePipeLine(params)
    dataPipeline = [...dataPipeline, ...duePipeLine]
  }
  if (tenantId && requestFrom === 'tenant') {
    const tenantPropertyDetailsPipeline =
      getPipelineForTenantPropertyListDetails(tenantId)
    dataPipeline = [...dataPipeline, ...tenantPropertyDetailsPipeline]
  }
  const lastProject = {
    $project: {
      _id: 1,
      imageUrl: 1,
      location: {
        name: 1,
        city: 1,
        country: 1,
        postalCode: 1
      },
      serial: 1,
      propertyTypeId: 1,
      listingTypeId: 1,
      apartmentId: 1,
      listed: 1,
      floor: 1,
      propertyStatus: 1,
      hasActiveLease: 1,
      hasUpcomingLease: 1,
      hasInProgressLease: 1,
      isSoonEnding: 1,
      isTerminated: 1,
      monthlyRentAmount: {
        $cond: [
          {
            $eq: ['$propertyStatus', 'active']
          },
          {
            $switch: {
              branches: [
                {
                  case: { $ifNull: ['$activeContract', false] },
                  then: '$activeContract.rentalMeta.monthlyRentAmount'
                },
                {
                  case: { $ifNull: ['$upcomingContract', false] },
                  then: '$upcomingContract.rentalMeta.monthlyRentAmount'
                },
                {
                  case: { $ifNull: ['$inProgressContract', false] },
                  then: '$inProgressContract.rentalMeta.monthlyRentAmount'
                },
                {
                  case: { $ifNull: ['$closedContract', false] },
                  then: '$closedContract.monthlyRentAmount'
                },
                {
                  case: { $eq: [requestFrom, 'tenant'] },
                  then: null
                }
              ],
              default: '$monthlyRentAmount'
            }
          },
          '$monthlyRentAmount'
        ]
      },
      depositAmount: {
        $cond: [
          {
            $eq: ['$propertyStatus', 'active']
          },
          {
            $switch: {
              branches: [
                {
                  case: { $ifNull: ['$activeContract', false] },
                  then: '$activeContract.rentalMeta.depositAmount'
                },
                {
                  case: { $ifNull: ['$upcomingContract', false] },
                  then: '$upcomingContract.rentalMeta.depositAmount'
                },
                {
                  case: { $ifNull: ['$inProgressContract', false] },
                  then: '$inProgressContract.rentalMeta.depositAmount'
                },
                {
                  case: { $ifNull: ['$closedContract', false] },
                  then: '$closedContract.depositAmount'
                },
                {
                  case: { $eq: [requestFrom, 'tenant'] },
                  then: null
                }
              ],
              default: '$depositAmount'
            }
          },
          '$depositAmount'
        ]
      },
      availabilityStartDate: {
        $cond: [
          { $eq: ['$propertyStatus', 'active'] },
          {
            $switch: {
              branches: [
                {
                  case: { $ifNull: ['$activeContract', false] },
                  then: '$activeContract.rentalMeta.contractStartDate'
                },
                {
                  case: { $ifNull: ['$upcomingContract', false] },
                  then: '$upcomingContract.rentalMeta.contractStartDate'
                },
                {
                  case: { $ifNull: ['$inProgressContract', false] },
                  then: '$inProgressContract.rentalMeta.contractStartDate'
                },
                {
                  case: { $ifNull: ['$closedContract', false] },
                  then: '$closedContract.contractStartDate'
                },
                {
                  case: { $eq: [requestFrom, 'tenant'] },
                  then: null
                }
              ],
              default: '$availabilityStartDate'
            }
          },
          '$availabilityStartDate'
        ]
      },
      availabilityEndDate: {
        $cond: [
          { $eq: ['$propertyStatus', 'active'] },
          {
            $switch: {
              branches: [
                {
                  case: { $ifNull: ['$activeContract', false] },
                  then: '$activeContract.rentalMeta.contractEndDate'
                },
                {
                  case: { $ifNull: ['$upcomingContract', false] },
                  then: '$upcomingContract.rentalMeta.contractEndDate'
                },
                {
                  case: { $ifNull: ['$inProgressContract', false] },
                  then: '$inProgressContract.rentalMeta.contractEndDate'
                },
                {
                  case: { $ifNull: ['$closedContract', false] },
                  then: '$closedContract.contractEndDate'
                },
                {
                  case: { $eq: [requestFrom, 'tenant'] },
                  then: null
                }
              ],
              default: '$availabilityEndDate'
            }
          },
          '$availabilityEndDate'
        ]
      },
      minimumStay: {
        $cond: [
          { $eq: ['$propertyStatus', 'active'] },
          {
            $switch: {
              branches: [
                {
                  case: { $ifNull: ['$activeContract', false] },
                  then: '$activeContract.rentalMeta.minimumStay'
                },
                {
                  case: { $ifNull: ['$upcomingContract', false] },
                  then: '$upcomingContract.rentalMeta.minimumStay'
                }
              ],
              default: '$minimumStay'
            }
          },
          '$minimumStay'
        ]
      },
      placeSize: 1,
      noOfAvailableBedrooms: 1,
      noOfBedrooms: 1,
      totalOverDue: 1,
      totalDue: 1,
      tenantInfo: {
        _id: 1,
        name: 1,
        avatarKey: 1
      },
      agentInfo: 1,
      accountInfo: 1,
      branchInfo: 1,
      createdAt: 1,
      hasAssignment: 1
    }
  }
  dataPipeline.push(lastProject)
  dataPipeline.push({
    $sort: sort
  })
  const listings = (await ListingCollection.aggregate(dataPipeline)) || []
  return listings
}

const countTotalProperties = async (totalListingsQuery) => {
  const { partnerId = '', tenantId = '' } = totalListingsQuery
  let totalProperties = 0
  if (tenantId) {
    const tenantInfo = await tenantHelper.getATenant({
      _id: tenantId,
      partnerId
    })
    totalProperties = size(tenantInfo?.properties || [])
  } else {
    totalProperties = await listingHelper.countListings(totalListingsQuery)
  }
  return totalProperties
}

export const queryProperties = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  const { query, options } = body
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)
  const { accountId = '', requestFrom = '', tenantId = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'b2c') {
    const b2cUserData =
      await appHelper.validateSelfServicePartnerRequestAndUpdateBody(user)
    query.ownerId = userId
    query.accountId = b2cUserData.accountId
    query.branchId = b2cUserData.branchId
    totalDocumentsQuery.ownerId = userId
    totalDocumentsQuery.accountId = b2cUserData.accountId
    totalDocumentsQuery.branchId = b2cUserData.branchId
  } else if (requestFrom === 'account') {
    appHelper.checkRequiredFields(['accountId'], query)
    totalDocumentsQuery.accountId = accountId
  } else if (requestFrom === 'tenant') {
    appHelper.checkRequiredFields(['tenantId'], query)
    totalDocumentsQuery.tenantId = tenantId
    body.tenantId = tenantId
    body.requestFrom = requestFrom
  }
  const { preparedQuery, tenantStatus } =
    await preparePropertiesQueryFromFilterData(query)
  body.query = preparedQuery

  let properties = await getPropertiesForQuery(body, user)
  const filteredDocuments = await listingHelper.countListings(body.query)
  const totalDocuments = await countTotalProperties(totalDocumentsQuery)
  if (size(tenantStatus)) {
    properties = JSON.parse(JSON.stringify(properties))
    for (const property of properties) {
      property.tenantStatus = tenantStatus[property._id]
    }
  }
  return {
    data: properties,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const preparePropertyQueryForPartnerDashboard = (params) => {
  const { accountId, agentId, branchId, partnerId, propertyId } = params
  const query = {
    partnerId
  }
  if (accountId) query.accountId = accountId
  if (agentId) query.agentId = agentId
  if (branchId) query.branchId = branchId
  if (propertyId) query._id = propertyId
  return query
}

const getAccountInfoPipeline = () => [
  {
    $lookup: {
      from: 'accounts',
      localField: 'accountId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'personId',
            foreignField: '_id',
            pipeline: [...appHelper.getUserEmailPipeline()],
            as: 'person'
          }
        },
        { $unwind: { path: '$person', preserveNullAndEmptyArrays: true } },
        {
          $lookup: {
            from: 'organizations',
            localField: 'organizationId',
            foreignField: '_id',
            as: 'organization'
          }
        },
        {
          $unwind: { path: '$organization', preserveNullAndEmptyArrays: true }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            type: 1,
            city: 1,
            country: 1,
            zipCode: 1,
            address: 1,
            invoiceAccountNumber: 1,
            contactPerson: {
              _id: 1,
              email: '$person.email',
              name: '$person.profile.name',
              phoneNumber: '$person.profile.phoneNumber',
              norwegianNationalIdentification:
                '$person.profile.norwegianNationalIdentification',
              city: '$person.profile.city',
              country: '$person.profile.country',
              hometown: '$person.profile.hometown',
              zipCode: '$person.profile.zipCode',
              avatarKey: appHelper.getUserAvatarKeyPipeline(
                '$person.profile.avatarKey'
              )
            },
            avatarKey: {
              $cond: [
                { $eq: ['$type', 'person'] },
                appHelper.getUserAvatarKeyPipeline('$person.profile.avatarKey'),
                appHelper.getOrganizationLogoPipeline('$organization.image')
              ]
            }
          }
        }
      ],
      as: 'accountInfo'
    }
  },
  appHelper.getUnwindPipeline('accountInfo')
]

const getJanitorInformationPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'janitorId',
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
      as: 'janitorInfo'
    }
  },
  appHelper.getUnwindPipeline('janitorInfo')
]
const getOwnerInformationPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'ownerId',
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
      as: 'ownerInfo'
    }
  },
  appHelper.getUnwindPipeline('ownerInfo')
]

export const getTotalAssignmentPipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: '_id',
      foreignField: 'propertyId',
      as: 'assignmentInfo'
    }
  }
]

export const getUpcomingLeaseInfoPipeline = () => [
  {
    $addFields: {
      upcomingLeaseInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$assignmentInfo', []] },
            as: 'leaseInfo',
            cond: {
              $eq: ['$$leaseInfo.status', 'upcoming']
            }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'users',
      localField: 'upcomingLeaseInfo.representativeId',
      foreignField: '_id',
      as: 'representativeInfo'
    }
  },
  {
    $unwind: {
      path: '$representativeInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      upcomingLeaseInfo: {
        _id: '$upcomingLeaseInfo._id',
        contractStartDate:
          '$upcomingLeaseInfo.listingInfo.availabilityStartDate',
        depositType: '$upcomingLeaseInfo.rentalMeta.depositType',
        invoiceAccountNumber:
          '$upcomingLeaseInfo.rentalMeta.invoiceAccountNumber',
        minimumStay: {
          $ifNull: ['$upcomingLeaseInfo.listingInfo.minimumStay', 3]
        },
        hasRentalContract: '$upcomingLeaseInfo.hasRentalContract',
        representativeInfo: {
          _id: '$upcomingLeaseInfo.representativeId',
          avatarKey: appHelper.getUserAvatarKeyPipeline(
            '$representativeInfo.profile.avatarKey',
            undefined,
            'representativeInfo'
          ),
          name: '$representativeInfo.profile.name'
        }
      }
    }
  }
]

export const getTotalLeasePipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: '_id',
      foreignField: 'propertyId',
      pipeline: [
        {
          $match: {
            $expr: {
              $or: [
                { $eq: ['$hasRentalContract', true] },
                { $ifNull: ['$rentalMetaHistory', false] }
              ]
            }
          }
        },
        {
          $group: {
            _id: null,
            activeLeaseNumber: {
              $sum: {
                $cond: [{ $ifNull: ['$hasRentalContract', false] }, 1, 0]
              }
            },
            leaseHistoryNumber: {
              $sum: {
                $cond: [
                  { $ifNull: ['$rentalMetaHistory', false] },
                  { $size: '$rentalMetaHistory' },
                  0
                ]
              }
            },
            activeLeaseInfo: {
              $push: {
                $cond: [
                  { $eq: ['$status', 'active'] },
                  {
                    _id: '$_id',
                    leaseSerial: '$leaseSerial',
                    tenantId: '$rentalMeta.tenantId'
                  },
                  '$$REMOVE'
                ]
              }
            }
          }
        }
      ],
      as: 'leaseInfo'
    }
  },
  appHelper.getUnwindPipeline('leaseInfo')
]

export const getActiveLeaseInfo = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'leaseInfo.activeLeaseInfo.tenantId',
      foreignField: '_id',
      as: 'mainTenantInfo'
    }
  },
  appHelper.getUnwindPipeline('mainTenantInfo'),
  {
    $addFields: {
      activeLeaseInfo: {
        _id: { $first: '$leaseInfo.activeLeaseInfo._id' },
        leaseSerial: {
          $cond: {
            if: { $ifNull: ['$mainTenantInfo', false] },
            then: {
              $concat: [
                'Lease ',
                {
                  $toString: {
                    $first: '$leaseInfo.activeLeaseInfo.leaseSerial'
                  }
                },
                ' - ',
                '$mainTenantInfo.name'
              ]
            },
            else: {
              $concat: [
                'Lease ',
                {
                  $toString: {
                    $first: '$leaseInfo.activeLeaseInfo.leaseSerial'
                  }
                }
              ]
            }
          }
        }
      }
    }
  }
]

export const getTotalOverDuePipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'propertyId',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$status', 'overdue'] },
                { $eq: ['$invoiceType', 'invoice'] }
              ]
            }
          }
        },
        {
          $group: {
            _id: null,
            invoiceTotal: { $sum: '$invoiceTotal' },
            totalPaidAmount: { $sum: '$totalPaid' },
            totalLostAmount: { $sum: '$lostMeta.amount' },
            totalCreditedAmount: { $sum: '$creditedAmount' },
            totalBalancedAmount: { $sum: '$totalBalanced' }
          }
        },
        {
          $project: {
            totalDue: {
              $subtract: [
                { $add: ['$invoiceTotal', '$totalCreditedAmount'] },
                {
                  $add: [
                    '$totalPaidAmount',
                    '$totalLostAmount',
                    '$totalBalancedAmount'
                  ]
                }
              ]
            }
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo')
]

export const dueTaskPipeline = () => [
  {
    $lookup: {
      from: 'tasks',
      localField: '_id',
      foreignField: 'propertyId',
      pipeline: [
        {
          $match: {
            $expr: {
              $lt: ['dueDate', new Date()]
            }
          }
        }
      ],
      as: 'taskInfo'
    }
  }
]

export const getPropertyDetails = async (query) => {
  const { partnerId } = query
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const soonEndingMonths = partnerSetting?.propertySettings?.soonEndingMonths
  const soonEndingMonthsDate = (
    await appHelper.getActualDate(partnerSetting, true)
  )
    .add(soonEndingMonths || 4, 'months')
    .toDate()
  const pipeline = [
    {
      $match: {
        ...query
      }
    },
    ...appHelper.getListingFirstImageUrl('$images'),
    ...getAccountInfoPipeline(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appHelper.getSoonEndingTerminatedActiveUpcomingContractPipeline(
      soonEndingMonthsDate
    ),
    ...getJanitorInformationPipeline(),
    ...getOwnerInformationPipeline(),
    ...getTotalAssignmentPipeline(),
    ...getUpcomingLeaseInfoPipeline(),
    ...getTotalLeasePipeline(),
    ...getActiveLeaseInfo(),
    ...getTotalOverDuePipeline(),
    ...dueTaskPipeline(),
    {
      $project: {
        _id: 1,
        imageUrl: 1,
        aboutText: 1,
        serial: 1,
        propertyStatus: 1,
        propertyTypeId: 1,
        listingTypeId: 1,
        apartmentId: 1,
        location: {
          name: '$location.name',
          city: '$location.city',
          country: '$location.country',
          streetNumber: '$location.streetNumber',
          postalCode: '$location.postalCode'
        },
        floor: 1,
        placeSize: 1,
        noOfBedrooms: 1,
        hasActiveLease: 1,
        hasUpcomingLease: 1,
        hasAssignment: 1,
        hasProspects: 1,
        hasInProgressLease: 1,
        accountInfo: 1,
        branchInfo: 1,
        janitorInfo: 1,
        ownerInfo: 1,
        summary: {
          assignmentTotal: { $size: { $ifNull: ['$assignmentInfo', []] } },
          leaseTotal: {
            $add: [
              { $ifNull: ['$leaseInfo.activeLeaseNumber', 0] },
              { $ifNull: ['$leaseInfo.leaseHistoryNumber', 0] }
            ]
          },
          totalDue: { $ifNull: ['$invoiceInfo.totalDue', 0] },
          taskDue: { $size: { $ifNull: ['$taskInfo', []] } }
        },
        activeLeaseInfo: 1,
        monthlyRentAmount: 1,
        depositAmount: 1,
        groupId: 1,
        gnr: 1,
        bnr: 1,
        snr: 1,
        upcomingLeaseInfo: 1,
        listed: 1,
        listedAt: 1,
        finn: {
          isPublishing: 1,
          isRePublishing: 1,
          isArchiving: 1,
          requestedAt: 1,
          statisticsURL: 1,
          finnShareAt: 1,
          finnErrorRequest: 1,
          disableFromFinn: 1,
          isShareAtFinn: 1,
          finnArchivedAt: 1
        },
        isSoonEnding: 1,
        isTerminated: 1
      }
    }
  ]

  const [propertyDetails = {}] =
    (await ListingCollection.aggregate(pipeline)) || []
  return propertyDetails
}

export const queryPropertyDetails = async (req) => {
  const { body, user } = req
  const { partnerId, userId } = user
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query } = body
  query.partnerId = partnerId
  appHelper.checkRequiredFields(['partnerId', 'propertyId'], query)
  const { propertyId } = query
  appHelper.validateId({ propertyId })
  query._id = propertyId
  delete query.propertyId

  const propertyDetail = await getPropertyDetails(query)
  return propertyDetail
}

export const validatePropertyAddData = (data, setting) => {
  const requiredFields = [
    'accountId',
    'branchId',
    'agentId',
    'listingTypeId',
    'location',
    'partnerId'
  ]
  appHelper.checkRequiredFields(requiredFields, data)
  appHelper.checkPositiveNumbers(data)
  const {
    accountId,
    agentId,
    branchId,
    listingTypeId,
    location,
    partnerId,
    propertyTypeId
  } = data
  appHelper.validateId({ accountId })
  appHelper.validateId({ branchId })
  appHelper.validateId({ partnerId })
  appHelper.validateId({ agentId })
  listingHelper.validateLocation(location)
  listingHelper.validateListingTypeId(listingTypeId, setting)
  if (propertyTypeId)
    listingHelper.validatePropertyTypeId(propertyTypeId, setting)
}

export const getPropertySerial = async (partnerId, session) => {
  const serial = await counterService.incrementCounter(
    `property-${partnerId}`,
    session
  )
  return serial
}

export const preparePropertyAddData = async (params, session) => {
  const { user, setting } = params
  let { data } = params
  const {
    agentId,
    depositAmount,
    groupId,
    listingTypeId,
    location,
    monthlyRentAmount,
    partnerId,
    propertyTypeId = 'e4v6fpNxTjGwJM243' // default property type id for meteor views
  } = data
  if (groupId) {
    const { propertySettings } =
      await partnerSettingHelper.getSettingByPartnerId(partnerId, session)
    const { enabledGroupId } = propertySettings
    if (!enabledGroupId) {
      data = omit(data, ['groupId'])
    }
  }
  const { countryShortName } = location
  data.availabilityStartDate = new Date()
  data.listed = false
  if (user.defaultRole === 'landlord') {
    // From B2C APP
    data.agentId = agentId
    data.ownerId = user.userId
  } else {
    //From Partner Admin
    if (agentId) {
      data.ownerId = agentId
    } else {
      data.agentId = user.userId
      data.ownerId = user.userId
    }
  }
  if (!depositAmount) data.depositAmount = 0
  if (!monthlyRentAmount) data.monthlyRentAmount = 0
  if (countryShortName) {
    data.currency = await appHelper.getCurrencyOfCountry(
      countryShortName,
      session
    )
  }
  data.serial = await getPropertySerial(partnerId, session)
  data.baseMonthlyRentAmount = 0
  data.propertyStatus = 'active'
  data.hasActiveLease = false
  if (listingHelper.isListingTypeParking(listingTypeId, setting)) {
    data = omit(data, ['bnr', 'gnr', 'noOfBedrooms', 'propertyTypeId', 'snr'])
  }
  data.propertyTypeId = propertyTypeId
  return data
}

export const validatePropertySerial = async (params = {}) => {
  const { _id, partnerId, serial } = params
  const query = {
    _id: { $ne: _id },
    partnerId,
    serial
  }
  const isSerialIdAlreadyExists = !!(await listingHelper.getAListing(query))
  if (isSerialIdAlreadyExists) {
    throw new CustomError(400, 'The serial id already exists')
  }
  const counterQuery = {
    _id: `property-${partnerId}`,
    next_val: { $exists: true }
  }
  const counter = await counterHelper.getACounter(counterQuery)
  const nextValue = get(counter, 'next_val', 0) + 1
  if (serial >= nextValue) {
    throw new CustomError(400, `The serial id must be lower than ${nextValue}`)
  }
}

export const validatePropertyUpdateData = async (body, setting) => {
  const {
    depositAmount,
    groupId,
    listingTypeId,
    noOfBedrooms,
    placeSize,
    partnerId,
    propertyId,
    propertyTypeId,
    serial
  } = body
  appHelper.checkPositiveNumbers({
    depositAmount,
    noOfBedrooms,
    placeSize,
    serial
  })
  if (listingTypeId || listingTypeId === '')
    listingHelper.validateListingTypeId(listingTypeId, setting)
  if (propertyTypeId || propertyTypeId === '')
    listingHelper.validatePropertyTypeId(propertyTypeId, setting)
  if (serial) {
    const params = { _id: propertyId, partnerId, serial }
    await validatePropertySerial(params)
  } else delete body.serial
  if (groupId) {
    const { propertySettings = {} } =
      await partnerSettingHelper.getSettingByPartnerId(partnerId)
    const { enabledGroupId } = propertySettings

    if (!enabledGroupId)
      throw new CustomError(400, 'Property groupId is not enabled')
  }
}

export const preparePropertyUpdateData = async (
  body = {},
  setting,
  prevProperty
) => {
  const { listingTypeId, location = {}, noOfBedrooms, propertyTypeId } = body
  const updateData = {}
  const propertyType = listingHelper.getPropertyTypeNameById(
    propertyTypeId,
    setting
  )
  if (propertyType === 'house') {
    updateData.$unset = { apartmentId: 1, floor: 1 }
    body = omit(body, ['apartmentId', 'floor'])
  }
  if (listingHelper.isListingTypeParking(listingTypeId, setting)) {
    updateData.$unset = {
      bnr: 1,
      gnr: 1,
      noOfBedrooms: 1,
      propertyTypeId: 1,
      snr: 1
    }
    body = omit(body, ['bnr', 'gnr', 'noOfBedrooms', 'propertyTypeId', 'snr'])
  }
  if (
    listingTypeId &&
    listingTypeId !== prevProperty.listingTypeId &&
    listingHelper.getListingTypeNameById(listingTypeId, setting) ===
      'entire_place'
  ) {
    body.noOfAvailableBedrooms = noOfBedrooms || prevProperty.noOfBedrooms || 0
  }
  if (
    location.countryShortName &&
    listingHelper.isUpdateCurrency(location, prevProperty)
  ) {
    body.currency = await appHelper.getCurrencyOfCountry(
      location.countryShortName
    )
  }
  body = omit(body, ['partnerId', 'propertyId'])
  if (size(body)) {
    updateData.$set = body
  }
  return updateData
}

export const isAddContract = async (property) => {
  const { _id, agentId, branchId, partnerId, accountId } = property
  if (!(_id && agentId && branchId && partnerId && accountId)) return false
  const isDirectPartner = !!(await partnerHelper.getDirectPartnerById(
    partnerId
  ))
  return isDirectPartner
}

export const propertyIdsFromRentalContracts = async (rentalContractQuery) => {
  const rentalContractsList = size(rentalContractQuery)
    ? await contractHelper.getContracts(rentalContractQuery)
    : {}
  let rentalContractPropertyIds = size(rentalContractsList)
    ? map(rentalContractsList, 'propertyId')
    : []
  rentalContractPropertyIds = compact(rentalContractPropertyIds)

  return rentalContractPropertyIds
}

export const getMovingInOutPropertyIds = async (params = {}) => {
  const { partnerId, movingInStatus, type, isDirectPartner } = params
  if (!partnerId) return false

  let propertyIds = []
  const query = { partnerId, isEsigningInitiate: true, type }
  const orQuery = []
  if (size(movingInStatus)) {
    for (const status of movingInStatus) {
      if (
        status === 'movingInSentToTenant' ||
        status === 'movingOutSentToTenant'
      ) {
        orQuery.push({
          tenantSigningStatus: {
            $exists: true,
            $elemMatch: { signed: false }
          }
        })
      } else if (
        status === 'movingInSignedByTenant' ||
        status === 'movingOutSignedByTenant'
      ) {
        orQuery.push({
          tenantSigningStatus: {
            $exists: true,
            $not: { $elemMatch: { signed: false } }
          }
        })
      } else if (
        status === 'movingInSentToAgent' ||
        status === 'movingOutSentToAgent'
      ) {
        if (isDirectPartner) {
          orQuery.push({
            'landlordSigningStatus.signed': false
          })
        } else {
          orQuery.push({
            'agentSigningStatus.signed': false
          })
        }
      } else if (
        status === 'movingInSignedByAgent' ||
        status === 'movingOutSignedByAgent'
      ) {
        if (isDirectPartner) {
          orQuery.push({
            'landlordSigningStatus.signed': true
          })
        } else {
          orQuery.push({
            'agentSigningStatus.signed': true
          })
        }
      }
    }
  }
  if (size(orQuery)) {
    query.$or = orQuery
    propertyIds = await propertyItemHelper.getUniqueIds('propertyId', query)
  }
  return propertyIds
}

const getPipelineForMovingPropertyItems = (type) => [
  {
    $lookup: {
      from: 'property_items',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            type,
            isEsigningInitiate: true
          }
        }
      ],
      as: 'propertyItems'
    }
  }
]

export const getMovingInOutCreatedPropertyIds = async (
  partnerId,
  type,
  hasCreated
) => {
  const contractQuery = {
    partnerId,
    'rentalMeta.status': { $in: ['active', 'upcoming', 'in_progress'] }
  }
  if (type === 'in' && !hasCreated) {
    contractQuery['rentalMeta.status'] = 'active'
  }
  if (type === 'out' && hasCreated) {
    contractQuery['rentalMeta.contractEndDate'] = { $exists: true }
    contractQuery['rentalMeta.status'] = {
      $in: ['active', 'upcoming', 'in_progress', 'closed']
    }
  } else if (type === 'out' && !hasCreated) {
    contractQuery['rentalMeta.contractEndDate'] = { $exists: true }
    contractQuery['rentalMeta.status'] = 'closed'
  }
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  let soonEndingMonths = 4
  soonEndingMonths =
    partnerSettings?.propertySettings?.soonEndingMonths || soonEndingMonths

  const selectedMonth = (
    await appHelper.getActualDate(partnerSettings, true)
  ).add(soonEndingMonths, 'months')

  const [result = {}] = await ContractCollection.aggregate([
    {
      $match: contractQuery
    },
    {
      $addFields: {
        type,
        hasCreated,
        isSoonEnding: {
          $cond: [
            {
              $and: [
                { $ifNull: ['$rentalMeta.contractEndDate', false] },
                { $lte: ['$rentalMeta.contractEndDate', selectedMonth._d] },
                { $eq: ['$status', 'active'] },
                { $eq: ['$hasRentalContract', true] }
              ]
            },
            true,
            false
          ]
        }
      }
    },
    ...getPipelineForMovingPropertyItems(type),
    {
      $group: {
        _id: null,
        propertyIds: {
          $addToSet: {
            $cond: [
              {
                $and: [
                  {
                    $or: [
                      { $eq: ['$type', 'out'] },
                      {
                        $and: [
                          {
                            $eq: ['$type', 'in']
                          },
                          { $not: '$isSoonEnding' }
                        ]
                      }
                    ]
                  },
                  {
                    $or: [
                      {
                        $and: [
                          '$hasCreated',
                          { $gt: [{ $size: '$propertyItems' }, 0] },
                          '$isSoonEnding'
                        ]
                      },
                      {
                        $not: {
                          $or: [
                            '$hasCreated',
                            { $gt: [{ $size: '$propertyItems' }, 0] }
                          ]
                        }
                      }
                    ]
                  }
                ]
              },
              '$propertyId',
              '$$REMOVE'
            ]
          }
        }
      }
    }
  ])
  const { propertyIds = [] } = result
  return propertyIds
}

export const getIssuesPropertyIds = async (
  partnerId,
  status,
  responsibleForFixing
) => {
  if (!partnerId) return false

  let propertyIds = []
  const propertyItemsQuery = { partnerId }
  const propertyRoomQuery = { partnerId }

  if (!status) {
    propertyItemsQuery['inventory.furniture'] = {
      $exists: true,
      $not: { $elemMatch: { status: 'issues' } }
    }
    propertyRoomQuery['items'] = { $not: { $elemMatch: { status: 'issues' } } }
    propertyRoomQuery['items.0'] = { $exists: true }
  } else {
    const elemMatchQuery = { status: 'issues' }

    if (size(responsibleForFixing)) {
      elemMatchQuery.responsibleForFixing = { $in: responsibleForFixing }
    }

    propertyItemsQuery['inventory.furniture'] = {
      $exists: true,
      $elemMatch: elemMatchQuery
    }
    propertyRoomQuery['items'] = { $exists: true, $elemMatch: elemMatchQuery }
  }

  const propertyItems = await propertyItemHelper.getPropertyItems(
    propertyItemsQuery
  )
  const propertyRooms = await propertyRoomHelper.getPropertyRoom(
    propertyRoomQuery
  )
  const itemsPropertyIds = size(propertyItems)
    ? uniq(map(propertyItems, 'propertyId'))
    : []
  const roomPropertyIds = size(propertyRooms)
    ? uniq(map(propertyRooms, 'propertyId'))
    : []

  propertyIds = status
    ? compact(union(itemsPropertyIds, roomPropertyIds))
    : appHelper.getUnion(itemsPropertyIds, roomPropertyIds)

  return propertyIds
}

export const preparePropertyStatusForQuery = async (propertyData) => {
  const {
    params,
    propertyStatus,
    isWaitingForPayment,
    isPaid,
    isOverPaid,
    noDeposit,
    isPartiallyPaid,
    depositAccountStatus
  } = propertyData
  let statusQueryArray = [{ propertyStatus: { $in: propertyStatus } }]
  if (size(propertyStatus) && indexOf(propertyStatus, 'active') !== -1) {
    if (params.hasActiveLease && !params.hasUpcomingLease) {
      statusQueryArray = [
        {
          $and: [
            { hasUpcomingLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    }

    if (params.hasUpcomingLease && !params.hasActiveLease) {
      statusQueryArray = [
        {
          $and: [
            { hasActiveLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    }

    if (!params.hasUpcomingLease && !params.hasActiveLease) {
      statusQueryArray = [
        {
          $and: [
            { hasActiveLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { hasUpcomingLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    }
    if (
      !params.hasUpcomingLease &&
      !params.hasActiveLease &&
      params.isActiveVacant
    ) {
      statusQueryArray = [
        {
          $and: [
            { hasActiveLease: { $ne: true } },
            { hasUpcomingLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    }
  } else {
    if (
      params.hasActiveLease &&
      !isWaitingForPayment &&
      !isPaid &&
      !noDeposit &&
      !isOverPaid &&
      !isPartiallyPaid
    )
      statusQueryArray.push({ hasActiveLease: true })

    if (params.hasUpcomingLease && !size(depositAccountStatus)) {
      statusQueryArray.push({ hasUpcomingLease: true })
    }
  }

  return statusQueryArray
}

export const preparePropertiesEsignStatusQuery = async (esignStatusData) => {
  const {
    params,
    partnerId,
    leaseESignStatus,
    assignmentESignStatus,
    hasUpcomingLease,
    hasActiveLease,
    hasInProgressLease,
    depositAccountStatus
  } = esignStatusData
  const esignQueryStatus = []
  if (
    size(params.hasInProgressLease) &&
    indexOf(params.hasInProgressLease, 'hasInProgressLease') !== -1
  ) {
    let leaseContractQuery = {
      partnerId,
      'rentalMeta.enabledLeaseEsigning': true,
      'rentalMeta.status': 'in_progress'
    }
    let assignmentContractQuery = {
      partnerId,
      enabledEsigning: true,
      status: 'in_progress'
    }
    const leaseStatusQuery = []
    const assignmentStatusQuery = []
    let propertiesIds = ''

    //Lease E-Sign Status query
    if (size(leaseESignStatus)) {
      if (includes(leaseESignStatus, 'leaseSentToTenant')) {
        leaseStatusQuery.push({
          'rentalMeta.tenantLeaseSigningStatus': {
            $elemMatch: { signed: false }
          }
        })
      }
      if (includes(leaseESignStatus, 'leaseSignedByTenant')) {
        leaseStatusQuery.push({
          $and: [
            {
              'rentalMeta.tenantLeaseSigningStatus': {
                $not: { $elemMatch: { signed: false } }
              }
            },
            { 'rentalMeta.landlordLeaseSigningStatus.signed': false }
          ]
        })
      }

      if (size(leaseStatusQuery)) leaseContractQuery['$or'] = leaseStatusQuery
    } else if (size(assignmentESignStatus)) {
      leaseContractQuery = null
    }

    //Assignment e-sign-status query builder
    if (size(assignmentESignStatus)) {
      if (includes(assignmentESignStatus, 'assignmentSentToAgent')) {
        assignmentStatusQuery.push({
          ['agentAssignmentSigningStatus.signed']: false
        })
      }
      if (includes(assignmentESignStatus, 'assignmentSignedByAgent')) {
        assignmentStatusQuery.push({
          ['agentAssignmentSigningStatus.signed']: true
        })
      }
      if (includes(assignmentESignStatus, 'assignmentSentToLandlord')) {
        assignmentStatusQuery.push({
          ['landlordAssignmentSigningStatus.signed']: false
        })
      }
      if (includes(assignmentESignStatus, 'assignmentSignedByLandlord')) {
        assignmentStatusQuery.push({
          ['landlordAssignmentSigningStatus.signed']: true
        })
      }
      if (size(assignmentStatusQuery))
        assignmentContractQuery['$or'] = assignmentStatusQuery
    } else if (size(leaseESignStatus)) {
      assignmentContractQuery = null
    }

    const leasePropertiesIds = leaseContractQuery
      ? await propertyIdsFromRentalContracts(leaseContractQuery)
      : []
    const assignmentPropertiesIds = assignmentContractQuery
      ? await propertyIdsFromRentalContracts(assignmentContractQuery)
      : []
    let contractPropertyIds = union(leasePropertiesIds, assignmentPropertiesIds)

    contractPropertyIds = uniq(contractPropertyIds)

    if (
      (hasUpcomingLease || hasActiveLease || hasInProgressLease) &&
      size(depositAccountStatus)
    ) {
      const depositPropertyIds = await tenantHelper.getPropertyOrTenantIds({
        partnerId,
        context: 'property',
        depositAccountStatus,
        hasUpcomingLease,
        hasActiveLease,
        hasInProgressLease
      })

      if (params.eSignStatus && params.eSignStatus.length > 0)
        contractPropertyIds = uniq(
          intersection(contractPropertyIds, depositPropertyIds)
        )
      else
        contractPropertyIds = uniq(
          union(contractPropertyIds, depositPropertyIds)
        )
    }
    propertiesIds = contractPropertyIds
    esignQueryStatus.push({ _id: { $in: propertiesIds } })
  }

  return esignQueryStatus
}

export const preparePropertiesDepositAmountQuery = async (params) => {
  const queryArray = []
  let minDeposit = 0
  let maxDeposit = 0
  if (params.minDeposit) minDeposit = Math.floor(params.minDeposit)
  if (params.maxDeposit) maxDeposit = Math.floor(params.maxDeposit)

  const depositAmountQuery = {}

  if (minDeposit) depositAmountQuery['$gte'] = minDeposit
  if (maxDeposit) depositAmountQuery['$lte'] = maxDeposit

  if (!minDeposit && size(depositAmountQuery))
    queryArray.push({
      $or: [
        { depositAmount: { $exists: false } },
        { depositAmount: depositAmountQuery }
      ]
    })
  else if (size(depositAmountQuery))
    queryArray.push({ depositAmount: depositAmountQuery })

  return queryArray
}

export const setTenantIdForProperties = async (params, partnerId) => {
  //Set tenantId
  let query = {}
  if (params.tenantId) {
    let propertyStatusArray = []

    if (params.context === 'tenant')
      propertyStatusArray = [
        'invited',
        'interested',
        'offer',
        'signed',
        'upcoming',
        'active',
        'rejected',
        'closed',
        'not_interested',
        'in_progress'
      ]
    else propertyStatusArray = ['active', 'upcoming']
    const tenantInfo = await tenantHelper.getATenant({
      _id: params.tenantId,
      partnerId,
      'properties.status': { $in: propertyStatusArray }
    })

    if (tenantInfo && tenantInfo.properties) {
      const propertyIds = []

      each(tenantInfo.properties, function (activeAndUpcomingProperties) {
        if (
          indexOf(propertyStatusArray, activeAndUpcomingProperties.status) !==
          -1
        )
          propertyIds.push(activeAndUpcomingProperties.propertyId)
      })
      if (size(propertyIds)) query = { $in: propertyIds }
    }
  }
  return query
}

export const prepareMovingOutProperties = async (movingOutData) => {
  const {
    hasProtocol,
    movingInStatus,
    movingOutStatus,
    partnerId,
    hasMovingInProtocol,
    hasMovingOutProtocol
  } = movingOutData
  const queryArray = []
  let movingInOutPropertyIds = []
  let movingInOutCreatedPropertyIds = []
  const partnerSettingsInfo = await partnerSettingHelper.getAPartnerSetting(
    {
      partnerId
    },
    null,
    ['partner']
  )
  const isDirectPartner = partnerSettingsInfo?.partner?.isDirect() || false
  const isEnableMovingInOut =
    partnerSettingsInfo?.propertySettings?.movingInOutProtocol || false
  let isFilterSet = false
  if (isEnableMovingInOut) {
    if (size(movingInStatus) && hasMovingInProtocol) {
      isFilterSet = true
      const movingInPropertyIds = await getMovingInOutPropertyIds({
        partnerId,
        movingInStatus,
        type: 'in',
        isDirectPartner
      })

      movingInOutPropertyIds = concat(
        movingInOutPropertyIds,
        movingInPropertyIds
      )
    }
    if (size(movingOutStatus) && hasMovingOutProtocol) {
      isFilterSet = true
      const movingOutPropertyIds = await getMovingInOutPropertyIds({
        partnerId,
        movingInStatus: movingOutStatus,
        type: 'out',
        isDirectPartner
      })

      movingInOutPropertyIds = concat(
        movingInOutPropertyIds,
        movingOutPropertyIds
      )
    }
    if (size(movingInOutPropertyIds))
      queryArray.push({ _id: { $in: movingInOutPropertyIds } })
  }

  if (hasProtocol === 'yes') {
    isFilterSet = true
    const movingInCreatedPropertyIds = hasMovingInProtocol
      ? await getMovingInOutCreatedPropertyIds(partnerId, 'in', true)
      : []
    const movingOutCreatedPropertyIds = hasMovingOutProtocol
      ? await getMovingInOutCreatedPropertyIds(partnerId, 'out', true)
      : []

    movingInOutCreatedPropertyIds = union(
      movingInCreatedPropertyIds,
      movingOutCreatedPropertyIds
    )
  } else if (hasProtocol === 'no') {
    isFilterSet = true
    const movingInNotCreatedPropertyIds = hasMovingInProtocol
      ? await getMovingInOutCreatedPropertyIds(partnerId, 'in')
      : []
    const movingOutNotCreatedPropertyIds = hasMovingOutProtocol
      ? await getMovingInOutCreatedPropertyIds(partnerId, 'out')
      : []

    movingInOutCreatedPropertyIds = union(
      movingInNotCreatedPropertyIds,
      movingOutNotCreatedPropertyIds
    )
  }

  if (size(movingInOutCreatedPropertyIds))
    queryArray.push({
      _id: { $in: movingInOutCreatedPropertyIds }
    })
  else if (isFilterSet && !size(queryArray))
    queryArray.push({
      _id: 'noting'
    })
  return queryArray
}

export const prepareAndCheckActiveLease = async (params) => {
  const queryArray = []
  if (params.hasActiveUpcomingLease === 'yes') {
    queryArray.push({ hasUpcomingLease: true })
  } else if (params.hasActiveUpcomingLease === 'no') {
    queryArray.push({ hasUpcomingLease: { $ne: true } })
  }

  if (params.hasActiveInProgressLease === 'yes') {
    queryArray.push({ hasInProgressLease: true })
  } else if (params.hasActiveInProgressLease === 'no') {
    queryArray.push({ hasInProgressLease: { $ne: true } })
  }
  return queryArray
}

export const preparePropertiesQuery = async (params) => {
  const query = {}
  const queryArray = []
  let minRent = 0
  let maxRent = 0

  if (size(params) && params.partnerId) {
    //set property status filters in query
    const partnerId = params.partnerId
    const propertyStatus = compact(params.propertyStatus)
    const listingTypeIds = compact(params.listingTypeId)
    let statusQueryArray = []
    const placeSize = {}
    let propertyIds = []
    let excludedPropertyIds = []
    let isNotProperty = false

    queryArray.push({ partnerId })

    //set property types
    if (size(listingTypeIds))
      queryArray.push({
        listingTypeId: { $in: compact(params.listingTypeId) }
      })
    //Set branch filters in query
    if (params.branchId) queryArray.push({ branchId: params.branchId })
    //Set agent filters in query
    if (params.agentId) queryArray.push({ agentId: params.agentId })
    //Set account filters in query
    if (params.accountId) queryArray.push({ accountId: params.accountId })
    //Set account filters in query
    if (params.accountIds)
      queryArray.push({ accountId: { $in: params.accountIds } })
    //Set janitor filters in query
    if (params.janitorId) queryArray.push({ janitorId: params.janitorId })
    //Set property filters in query
    if (params.propertyId) queryArray.push({ _id: params.propertyId })

    const hasUpcomingLease = params.hasUpcomingLease || false
    const hasActiveLease = params.hasActiveLease || false
    const hasInProgressLease =
      (params.hasInProgressLease &&
        indexOf(params.hasInProgressLease, 'hasInProgressLease') !== -1) ||
      false
    let depositAccountStatus = params.depositAccountStatus || []
    const isWaitingForPayment =
      indexOf(depositAccountStatus, 'waitingForPayment') !== -1 || false
    const isPaid = indexOf(depositAccountStatus, 'paid') !== -1 || false
    const isOverPaid = indexOf(depositAccountStatus, 'overPaid') !== -1 || false
    const isPartiallyPaid =
      indexOf(depositAccountStatus, 'partiallyPaid') !== -1 || false
    const noDeposit = indexOf(depositAccountStatus, 'noDeposit') !== -1 || false
    const movingInStatus = params.movingInStatus || []
    const movingOutStatus = params.movingOutStatus || []
    const protocolType = params.protocolType || []
    const hasMovingInProtocol = includes(protocolType, 'movingIn') || false
    const hasMovingOutProtocol = includes(protocolType, 'movingOut') || false
    const leaseESignStatus = params.leaseESignStatus || []
    const assignmentESignStatus = params.assignmentESignStatus || []
    const hasProtocol = params.hasProtocol

    //Prepare property status query
    statusQueryArray = await preparePropertyStatusForQuery({
      params,
      propertyStatus,
      isWaitingForPayment,
      isPaid,
      isOverPaid,
      noDeposit,
      isPartiallyPaid,
      depositAccountStatus
    })

    // e sign status query
    const esignQueryStatus = await preparePropertiesEsignStatusQuery({
      params,
      partnerId,
      leaseESignStatus,
      assignmentESignStatus,
      hasUpcomingLease,
      hasActiveLease,
      hasInProgressLease,
      depositAccountStatus
    })

    if (size(esignQueryStatus)) {
      statusQueryArray.push(...esignQueryStatus)
    }

    //assignmentStatusContractQuery

    if (size(statusQueryArray)) queryArray.push({ $or: statusQueryArray })

    // working here .....
    const propertyMovingInAndOut = await prepareMovingOutProperties({
      hasProtocol,
      movingInStatus,
      movingOutStatus,
      partnerId,
      hasMovingInProtocol,
      hasMovingOutProtocol
    })
    if (size(propertyMovingInAndOut)) {
      queryArray.push(...propertyMovingInAndOut)
    }
    //check if the active lease has another upcoming/in progress lease for property
    if (hasActiveLease) {
      const activeLeaseQuery = await prepareAndCheckActiveLease(params)
      if (size(activeLeaseQuery)) {
        queryArray.push(...activeLeaseQuery)
      }
    }

    //Set has assignment filters in query
    if (params.hasAssignment === 'yes') queryArray.push({ hasAssignment: true })
    else if (params.hasAssignment === 'no')
      queryArray.push({ hasAssignment: { $ne: true } })

    //Set listed filters in query
    if (params.listed === 'yes') queryArray.push({ listed: true })
    else if (params.listed === 'no') queryArray.push({ listed: { $ne: true } })

    //Set hasProspect filters in query
    if (params.hasProspects === 'yes') queryArray.push({ hasProspects: true })
    else if (params.hasProspects === 'no')
      queryArray.push({ hasProspects: { $ne: true } })

    //Set place size filters in query
    if (params.sizeFrom) placeSize['$gte'] = params.sizeFrom
    if (params.sizeTo) placeSize['$lte'] = params.sizeTo
    if (size(placeSize)) queryArray.push({ placeSize })

    //Set bedrooms
    if (params.bedrooms)
      queryArray.push({ noOfAvailableBedrooms: { $gte: params.bedrooms } })

    //Set rent amount value
    if (params.minRent) minRent = Math.floor(params.minRent)
    if (params.maxRent) maxRent = Math.floor(params.maxRent)

    const monthlyRentAmountQuery = {}

    if (minRent) monthlyRentAmountQuery['$gte'] = minRent
    if (maxRent) monthlyRentAmountQuery['$lte'] = maxRent
    if (size(monthlyRentAmountQuery))
      queryArray.push({ monthlyRentAmount: monthlyRentAmountQuery })

    //Set deposit amount value
    const depositAmountQuery = await preparePropertiesDepositAmountQuery(params)
    if (size(depositAmountQuery)) {
      queryArray.push(...depositAmountQuery)
    }
    //set availability dates from user's selection
    if (
      size(params.availabilityDateRange) &&
      params.availabilityDateRange.startDate &&
      params.availabilityDateRange.endDate
    ) {
      queryArray.push({
        availabilityStartDate: {
          $gte: params.availabilityDateRange.startDate,
          $lte: params.availabilityDateRange.endDate
        }
      })
    }

    //set leaseStartDateRange filters in query
    if (
      size(params.leaseStartDateRange) &&
      params.leaseStartDateRange.startDate &&
      params.leaseStartDateRange.endDate
    ) {
      const leasePropertiesIds = await propertyIdsFromRentalContracts({
        partnerId,
        'rentalMeta.status': { $ne: 'closed' },
        'rentalMeta.contractStartDate': {
          $gte: params.leaseStartDateRange.startDate,
          $lte: params.leaseStartDateRange.endDate
        }
      })

      if (size(propertyIds))
        propertyIds = intersection(propertyIds, leasePropertiesIds)
      else propertyIds = leasePropertiesIds

      if (!size(propertyIds)) {
        queryArray.push({ _id: 'nothing' })
        isNotProperty = true
      }
    }

    //Set leaseEndDateRange filters in query
    if (
      size(params.leaseEndDateRange) &&
      params.leaseEndDateRange.startDate &&
      params.leaseEndDateRange.endDate
    ) {
      const startDate = (
        await appHelper.getActualDate(
          partnerId,
          true,
          params.leaseEndDateRange.startDate
        )
      )
        .startOf('day')
        .toDate()
      const endDate = (
        await appHelper.getActualDate(
          partnerId,
          true,
          params.leaseEndDateRange.endDate
        )
      )
        .endOf('day')
        .toDate()
      const getPropertiesIds = await propertyIdsFromRentalContracts({
        partnerId,
        'rentalMeta.contractEndDate': {
          $gte: startDate,
          $lte: endDate
        }
      })

      if (size(propertyIds))
        propertyIds = intersection(propertyIds, getPropertiesIds)
      else propertyIds = getPropertiesIds

      //If rental contract not found by lease start date range then don`t show any properties
      if (!size(propertyIds)) {
        queryArray.push({ _id: 'nothing' })
        isNotProperty = true
      }
    }

    //set cpiEnabled filter in query
    if (params.cpiEnabled === 'yes') {
      const activePropertyIds = await propertyIdsFromRentalContracts({
        partnerId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] },
        'rentalMeta.cpiEnabled': true
      })

      if (size(propertyIds))
        propertyIds = intersection(propertyIds, activePropertyIds)
      else propertyIds = activePropertyIds

      if (!size(propertyIds)) {
        queryArray.push({ _id: 'nothing' })
        isNotProperty = true
      }
    } else if (params.cpiEnabled === 'no') {
      excludedPropertyIds = await propertyIdsFromRentalContracts({
        partnerId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] },
        'rentalMeta.cpiEnabled': true
      })

      propertyIds = uniq(difference(propertyIds, excludedPropertyIds))
    }

    // Set Terminated contract in query
    if (params.hasTerminatedContract === 'yes') {
      const hasTerminatedPropertyIds = await propertyIdsFromRentalContracts({
        partnerId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] },
        terminatedByUserId: { $exists: true }
      })

      if (size(propertyIds))
        propertyIds = intersection(propertyIds, hasTerminatedPropertyIds)
      else propertyIds = hasTerminatedPropertyIds

      if (!size(propertyIds)) {
        queryArray.push({ _id: 'nothing' })
        isNotProperty = true
      }
    } else if (params.hasTerminatedContract === 'no') {
      const nonTerminatedContract = await propertyIdsFromRentalContracts({
        partnerId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] },
        terminatedByUserId: { $exists: true }
      })

      if (size(excludedPropertyIds))
        excludedPropertyIds = union(excludedPropertyIds, nonTerminatedContract)
      else excludedPropertyIds = nonTerminatedContract
    }

    if (!isNotProperty && size(propertyIds)) {
      let _idQuery = { _id: { $in: propertyIds } }
      if (size(excludedPropertyIds))
        _idQuery = { _id: { $in: propertyIds, $nin: excludedPropertyIds } }
      queryArray.push(_idQuery)
    } else if (size(excludedPropertyIds)) {
      queryArray.push({ _id: { $nin: excludedPropertyIds } })
    }

    if (params.hasIssues === 'yes') {
      const responsibleForFixing = params.responsibleForFixing || []
      const hasIssuesPropertyIds = await getIssuesPropertyIds(
        partnerId,
        true,
        responsibleForFixing
      )

      if (size(hasIssuesPropertyIds))
        queryArray.push({ _id: { $in: hasIssuesPropertyIds } })
      else queryArray.push({ _id: 'nothing' })
    } else if (params.hasIssues === 'no') {
      const hasIssuesPropertyIds = await getIssuesPropertyIds(partnerId, null)

      if (size(hasIssuesPropertyIds))
        queryArray.push({ _id: { $in: hasIssuesPropertyIds } })
      else queryArray.push({ _id: 'nothing' })
    }

    //set listing address filters in query
    if (params.searchKeyword) {
      if (!isNaN(parseInt(params.searchKeyword))) {
        queryArray.push({
          $or: [
            { 'location.streetNumber': params.searchKeyword },
            { serial: parseInt(params.searchKeyword) }
          ]
        })
      } else
        queryArray.push({
          'location.name': new RegExp(params.searchKeyword, 'i')
        })
    }

    //Set listing according apartment Id
    if (params.searchApartmentId) {
      queryArray.push({
        apartmentId: new RegExp(params.searchApartmentId, 'i')
      })
    }

    const setTenantIds = await setTenantIdForProperties(params, partnerId)
    if (size(setTenantIds)) {
      query['_id'] = setTenantIds
    }
    if (
      params.createdDateRange &&
      params.createdDateRange.startDate &&
      params.createdDateRange.endDate
    ) {
      query.createdAt = {
        $gte: params.createdDateRange.startDate,
        $lte: params.createdDateRange.endDate
      }
    }

    if (!hasUpcomingLease && !hasActiveLease) {
      depositAccountStatus = filter(
        depositAccountStatus,
        function (depositAcc) {
          return depositAcc === 'waitingForCreation'
        }
      )
    }
    if (!hasInProgressLease) {
      depositAccountStatus = filter(
        depositAccountStatus,
        function (depositAcc) {
          return depositAcc !== 'waitingForCreation'
        }
      )
    }

    depositAccountStatus = compact(depositAccountStatus)

    if (
      (hasUpcomingLease || hasActiveLease || hasInProgressLease) &&
      size(depositAccountStatus)
    ) {
      const depositPropertyIds = await tenantHelper.getPropertyOrTenantIds({
        partnerId,
        context: 'property',
        depositAccountStatus,
        hasUpcomingLease,
        hasActiveLease,
        hasInProgressLease
      })

      queryArray.push({ _id: { $in: depositPropertyIds } })
    }
  } else queryArray.push({ _id: 'nothing' })
  if (size(queryArray)) query['$and'] = queryArray
  console.log('queryArray ==> ', queryArray)
  return query
}

export const getContractPipelineForExcelManager = () => {
  const pipeline = [
    {
      $lookup: {
        from: 'contracts',
        let: {
          propertyId: '$_id',
          partnerId: '$partnerId'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$propertyId', '$$propertyId'] },
                  { $eq: ['$partnerId', '$$partnerId'] },
                  { $eq: ['$hasRentalContract', true] },
                  {
                    $or: [
                      { $eq: ['$status', 'active'] },
                      { $eq: ['$status', 'upcoming'] }
                    ]
                  }
                ]
              }
            }
          },
          {
            $project: {
              _id: 1,
              rentalMeta: 1,
              status: 1
            }
          },
          {
            $facet: {
              activeContract: [
                {
                  $match: {
                    status: 'active'
                  }
                },
                {
                  $limit: 1
                }
              ],
              upcomingContract: [
                {
                  $match: {
                    status: 'upcoming'
                  }
                },
                {
                  $limit: 1
                }
              ]
            }
          },
          {
            $unwind: {
              path: '$activeContract',
              preserveNullAndEmptyArrays: true
            }
          },
          {
            $unwind: {
              path: '$upcomingContract',
              preserveNullAndEmptyArrays: true
            }
          }
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
        from: 'contracts',
        let: {
          propertyId: '$_id',
          partnerId: '$partnerId'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$propertyId', '$$propertyId'] },
                  { $eq: ['$partnerId', '$$partnerId'] },
                  { $eq: ['$hasRentalContract', true] },
                  {
                    $or: [
                      { $eq: ['$status', 'active'] },
                      { $eq: ['$status', 'upcoming'] }
                    ]
                  }
                ]
              }
            }
          },
          {
            $limit: 1
          },
          {
            $unwind: {
              path: '$addons',
              preserveNullAndEmptyArrays: true
            }
          },
          {
            $lookup: {
              from: 'products_services',
              foreignField: '_id',
              localField: 'addons.addonId',
              as: 'addon'
            }
          },
          {
            $unwind: {
              path: '$addon',
              preserveNullAndEmptyArrays: true
            }
          },
          {
            $addFields: {
              rentAddons: {
                $concat: [
                  '$addon.name',
                  {
                    $cond: {
                      if: { $ifNull: ['$addons.price', false] },
                      then: { $concat: [': ', { $toString: '$addons.price' }] },
                      else: ''
                    }
                  }
                ]
              }
            }
          },
          {
            $group: {
              _id: '$id',
              rentalMeta: { $first: '$rentalMeta' },
              rentAddons: { $push: '$rentAddons' },
              status: { $first: '$status' }
            }
          },
          {
            $lookup: {
              from: 'tenants',
              localField: 'rentalMeta.tenantId',
              foreignField: '_id',
              as: 'tenant'
            }
          },
          {
            $unwind: {
              path: '$tenant',
              preserveNullAndEmptyArrays: true
            }
          },
          {
            $project: {
              rentalMeta: 1,
              rentAddons: 1,
              tenantId: '$tenant.serial',
              tenantName: '$tenant.name'
            }
          }
        ],
        as: 'contractData'
      }
    },
    {
      $unwind: {
        path: '$contractData',
        preserveNullAndEmptyArrays: true
      }
    }
  ]
  return pipeline
}

export const getProjectForExcelManager = (params) => {
  const { dateFormat, language, timeZone } = params
  const pipeline = [
    {
      $project: {
        listingTypeId: '$listingTypeId',
        propertyTypeId: '$propertyTypeId',
        location: '$location.name',
        floor: '$floor',
        propertyStatus: '$propertyStatus',
        hasActiveLease: '$hasActiveLease',
        hasUpcomingLease: '$hasUpcomingLease',
        hasInProgressLease: '$hasInProgressLease',
        apartmentId: '$apartmentId',
        id: '$serial',
        listing: {
          $cond: {
            if: { $ifNull: ['$listed', false] },
            then: appHelper.translateToUserLng('common.listed', language),
            else: appHelper.translateToUserLng('common.unlisted', language)
          }
        },
        tenantId: '$contractData.tenantId',
        tenant: '$contractData.tenantName',
        bedrooms: {
          $cond: [{ $eq: ['$noOfBedrooms', 0] }, null, '$noOfBedrooms']
        },
        accountName: '$account.name',
        contractEndDate: {
          $cond: {
            if: {
              $ifNull: ['$contractData.rentalMeta.contractEndDate', false]
            },
            then: '$contractData.rentalMeta.contractEndDate',
            else: ''
          }
        },
        contractStatus: {
          $cond: {
            if: { $ifNull: ['$contractData.status', false] },
            then: '$contractData.status',
            else: ''
          }
        },
        branchName: {
          $cond: {
            if: { $ifNull: ['$branch', false] },
            then: '$branch.name',
            else: ''
          }
        },
        agentName: {
          $cond: {
            if: { $ifNull: ['$agent', false] },
            then: '$agent.profile.name',
            else: ''
          }
        },
        minimumStay: {
          $cond: {
            if: {
              $or: [
                { $ifNull: ['$contract.activeContract', false] },
                { $ifNull: ['$contract.upcomingContract', false] }
              ]
            },
            then: {
              $cond: [
                { $ifNull: ['$contract.activeContract', false] },
                {
                  $ifNull: [
                    '$contract.activeContract.rentalMeta.minimumStay',
                    0
                  ]
                },
                {
                  $ifNull: [
                    '$contract.upcomingContract.rentalMeta.minimumStay',
                    0
                  ]
                }
              ]
            },
            else: { $ifNull: ['$minimumStay', 0] }
          }
        },
        placeSize: { $cond: [{ $eq: ['$placeSize', 0] }, null, '$placeSize'] },
        availabilityStartDate: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$propertyStatus', 'active'] },
                {
                  $or: [
                    { $ifNull: ['$contract.activeContract', false] },
                    { $ifNull: ['$contract.upcomingContract', false] }
                  ]
                }
              ]
            },
            then: {
              $dateToString: {
                format: dateFormat,
                date: {
                  $cond: [
                    { $ifNull: ['$contract.activeContract', false] },
                    '$contract.activeContract.rentalMeta.contractStartDate',
                    '$contract.upcomingContract.rentalMeta.contractStartDate'
                  ]
                },
                timezone: timeZone
              }
            },
            else: {
              $dateToString: {
                format: dateFormat,
                date: '$availabilityStartDate',
                timezone: timeZone
              }
            }
          }
        },
        availabilityEndDate: {
          $cond: {
            if: { $eq: ['$propertyStatus', 'active'] },
            then: {
              $cond: {
                if: {
                  $or: [
                    { $ifNull: ['$contract.activeContract', false] },
                    { $ifNull: ['$contract.upcomingContract', false] }
                  ]
                },
                then: {
                  $switch: {
                    branches: [
                      {
                        case: { $ifNull: ['$contract.activeContract', false] },
                        then: {
                          $cond: [
                            {
                              $ifNull: [
                                '$contract.activeContract.rentalMeta.contractEndDate',
                                false
                              ]
                            },
                            {
                              $dateToString: {
                                format: dateFormat,
                                date: '$contract.activeContract.rentalMeta.contractEndDate',
                                timezone: timeZone
                              }
                            },
                            'Undetermined'
                          ]
                        }
                      },
                      {
                        case: {
                          $ifNull: ['$contract.upcomingContract', false]
                        },
                        then: {
                          $cond: [
                            {
                              $ifNull: [
                                '$contract.upcomingContract.rentalMeta.contractEndDate',
                                false
                              ]
                            },
                            {
                              $dateToString: {
                                format: dateFormat,
                                date: '$contract.upcomingContract.rentalMeta.contractEndDate',
                                timezone: timeZone
                              }
                            },
                            'Undetermined'
                          ]
                        }
                      }
                    ],
                    default: 'Undetermined'
                  }
                },
                else: 'Undetermined'
              }
            },
            else: {
              $cond: {
                if: { $ifNull: ['$availabilityEndDate', false] },
                then: {
                  $dateToString: {
                    format: dateFormat,
                    date: '$availabilityEndDate',
                    timezone: timeZone
                  }
                },
                else: appHelper.translateToUserLng('labels.unlimited', language)
              }
            }
          }
        },
        contractLastCpiDate: {
          $cond: {
            if: {
              $or: [
                {
                  $eq: ['$contract.activeContract.rentalMeta.cpiEnabled', true]
                },
                {
                  $eq: [
                    '$contract.upcomingContract.rentalMeta.cpiEnabled',
                    true
                  ]
                }
              ]
            },
            then: {
              $dateToString: {
                format: dateFormat,
                date: '$contractData.rentalMeta.lastCpiDate',
                timezone: timeZone
              }
            },
            else: ''
          }
        },
        contractNextCpiDate: {
          $cond: {
            if: {
              $or: [
                {
                  $eq: ['$contract.activeContract.rentalMeta.cpiEnabled', true]
                },
                {
                  $eq: [
                    '$contract.upcomingContract.rentalMeta.cpiEnabled',
                    true
                  ]
                }
              ]
            },
            then: {
              $dateToString: {
                format: dateFormat,
                date: '$contractData.rentalMeta.nextCpiDate',
                timezone: timeZone
              }
            },
            else: ''
          }
        },
        monthlyRentAmount: '$totalMonthlyRent',
        depositAmount: '$totalDepositAmount',
        totalOverDue: {
          $cond: {
            if: { $ifNull: ['$invoice', false] },
            then: {
              $subtract: [
                {
                  $add: ['$invoice.invoiceTotal', '$invoice.creditedTotal']
                },
                '$invoice.paidTotal'
              ]
            },
            else: 0
          }
        },
        isVatEnable: {
          $cond: {
            if: { $ifNull: ['$contractData', false] },
            then: {
              $cond: {
                if: { $eq: ['$contractData.rentalMeta.isVatEnable', true] },
                then: 'YES',
                else: 'NO'
              }
            },
            else: ''
          }
        },
        rentAddons: {
          $cond: [
            { $ifNull: ['$contractData', false] },
            '$contractData.rentAddons',
            ''
          ]
        },
        activeContract: '$contract.activeContract'
      }
    }
  ]
  return pipeline
}

export const addFieldsForExcelManager = () => {
  const pipeline = [
    {
      $addFields: {
        totalMonthlyRent: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$propertyStatus', 'active'] },
                {
                  $ifNull: ['$contract.activeContract', false]
                }
              ]
            },
            then: {
              $ifNull: [
                '$contract.activeContract.rentalMeta.monthlyRentAmount',
                0
              ]
            },
            else: { $ifNull: ['$monthlyRentAmount', 0] }
          }
        },
        totalDepositAmount: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$propertyStatus', 'active'] },
                {
                  $ifNull: ['$contract.activeContract', false]
                }
              ]
            },
            then: {
              $ifNull: ['$contract.activeContract.rentalMeta.depositAmount', 0]
            },
            else: { $ifNull: ['$depositAmount', 0] }
          }
        }
      }
    }
  ]
  return pipeline
}

export const getPropertyForExcelManager = async (queryData) => {
  const { query, options } = queryData
  const { sort, skip, limit } = options
  const contract = getContractPipelineForExcelManager()
  const addFields = addFieldsForExcelManager()
  const project = getProjectForExcelManager(queryData)
  const pipeline = [
    {
      $match: query
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $lookup: {
        from: 'invoices',
        let: { propertyId: '$_id' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$propertyId', '$$propertyId'] },
                  { $eq: ['$status', 'overdue'] },
                  { $eq: ['$invoiceType', 'invoice'] }
                ]
              }
            }
          },
          {
            $group: {
              _id: null,
              invoiceTotal: { $sum: { $ifNull: ['$invoiceTotal', 0] } },
              paidTotal: { $sum: { $ifNull: ['$totalPaid', 0] } },
              creditedTotal: { $sum: { $ifNull: ['$creditedAmount', 0] } }
            }
          }
        ],
        as: 'invoice'
      }
    },
    {
      $unwind: {
        path: '$invoice',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        as: 'account'
      }
    },
    {
      $unwind: {
        path: '$account',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'branches',
        localField: 'branchId',
        foreignField: '_id',
        as: 'branch'
      }
    },
    {
      $unwind: {
        path: '$branch',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'agentId',
        foreignField: '_id',
        as: 'agent'
      }
    },
    {
      $unwind: {
        path: '$agent',
        preserveNullAndEmptyArrays: true
      }
    },
    ...contract,
    ...addFields,
    ...project
  ]

  const property = await ListingCollection.aggregate(pipeline)
  return property || []
}

export const propertyDataForExcelCreator = async (params, options) => {
  const { partnerId = '', userId = '', isQueueCreatedFromV2 = false } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const userInfo = await userHelper.getAnUser({ _id: userId })
  const userLanguage = userInfo.getLanguage()

  let propertiesQuery = {}
  if (isQueueCreatedFromV2) {
    const { preparedQuery } = await preparePropertiesQueryFromFilterData(params)

    propertiesQuery = preparedQuery
  } else propertiesQuery = await preparePropertiesQuery(params)

  const dataCount = await listingHelper.countListings(propertiesQuery)
  console.log('dataCount  ==> ', dataCount)
  const setting = await SettingCollection.findOne()
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'

  const queryData = {
    query: propertiesQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage
  }
  const propertyLists = await getPropertyForExcelManager(queryData)

  if (size(propertyLists)) {
    for (const property of propertyLists) {
      const listingType =
        listingHelper.getListingTypeNameById(property.listingTypeId, setting) ||
        ''
      const propertyType =
        listingHelper.getPropertyTypeNameById(
          property.propertyTypeId,
          setting
        ) || ''

      let type = '',
        status = '',
        vacantStatus = ''

      if (listingType === 'parking') {
        type = appHelper.translateToUserLng(
          'listing_and_property_types.' + listingType,
          userLanguage
        )
        if (property.apartmentId) {
          type =
            type +
            ' - ' +
            appHelper.translateToUserLng('common.id', userLanguage) +
            ' : ' +
            property.apartmentId
        }
      } else {
        if (propertyType) {
          type =
            appHelper.translateToUserLng(
              'listing_and_property_types.' + propertyType,
              userLanguage
            ) + ' : '
        }
        if (listingType) {
          type =
            type +
            ' ' +
            appHelper.translateToUserLng(
              'listing_and_property_types.' + listingType,
              userLanguage
            )
        }
      }

      if (property.propertyStatus === 'archived') {
        status = appHelper.translateToUserLng(
          'common.' + property.propertyStatus,
          userLanguage
        )
      } else {
        if (property.hasActiveLease) {
          status = appHelper.translateToUserLng('common.occupied', userLanguage)

          let soonEndingMonths = 4
          soonEndingMonths =
            partnerSetting?.propertySettings?.soonEndingMonths ||
            soonEndingMonths
          const selectedMonth = (
            await appHelper.getActualDate(partnerSetting, true)
          ).add(soonEndingMonths, 'months')
          if (
            size(property.activeContract) &&
            property.activeContract.rentalMeta.contractEndDate <=
              selectedMonth._d
          ) {
            status =
              status +
              ', ' +
              appHelper.translateToUserLng(
                'properties.soon_ending',
                userLanguage
              )
          }
        }
        if (property.hasUpcomingLease) {
          status = appHelper.translateToUserLng('common.upcoming', userLanguage)
        }
        if (property.hasInProgressLease) {
          status = appHelper.translateToUserLng(
            'common.in_progress',
            userLanguage
          )
        }
        if (
          !(
            property.hasActiveLease ||
            property.hasUpcomingLease ||
            property.hasInProgressLease
          )
        ) {
          status = appHelper.translateToUserLng('common.vacant', userLanguage)
          vacantStatus = status + ', '
        }
        if (property.propertyStatus !== 'active') {
          status =
            vacantStatus +
            appHelper.translateToUserLng(
              'common.' + property.propertyStatus,
              userLanguage
            )
        }
      }
      property.type = type || ''
      property.floor =
        listingType !== 'parking' && propertyType !== 'house' && property.floor
          ? property.floor
          : ''
      property.apartmentId =
        listingType !== 'parking' &&
        propertyType !== 'house' &&
        property.apartmentId
          ? property.apartmentId
          : ''
      property.rentAddons = property.rentAddons
        ? property.rentAddons.join(' - ')
        : ''
      property.propertyStatus = status
    }
  }

  return {
    data: propertyLists,
    total: dataCount
  }
}

const prepareSortForPropertyData = (sort) => {
  const { location_name } = sort
  if (location_name) sort['location.name'] = location_name
  const preparedSort = omit(sort, ['location_name'])
  return preparedSort
}

export const queryForPropertyExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.validateId({ queueId })
  const preparedSort = prepareSortForPropertyData(sort)
  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_properties') {
    console.log(
      ' Started preparing data for excel for queueId: ',
      queueInfo._id
    )
    const payoutData = await propertyDataForExcelCreator(queueInfo.params, {
      skip,
      limit,
      sort: preparedSort
    })
    return payoutData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const validateDataForSharingAtFinn = (partner, property) => {
  const { finnId } = partner || {}
  const {
    availabilityEndDate,
    availabilityStartDate,
    location,
    propertyTypeId,
    title
  } = property || {}

  if (!finnId) {
    throw new CustomError(400, 'properties.finn_errors.finn_id_required')
  } else if (!size(property)) {
    throw new CustomError(400, 'errors.you_have_not_permission')
  } else if (!location?.postalCode) {
    throw new CustomError(400, 'properties.finn_errors.postal_code_required')
  } else if (location?.countryShortName !== 'NO') {
    throw new CustomError(400, 'properties.finn_errors.country_code_required')
  } else if (
    availabilityEndDate &&
    availabilityStartDate &&
    moment(availabilityEndDate).format('YYYY.MM.DD') ===
      moment(availabilityStartDate).format('YYYY.MM.DD')
  ) {
    throw new CustomError(
      400,
      'properties.finn_errors.end_and_start_date_not_same'
    )
  } else if (!title) {
    throw new CustomError(400, 'properties.finn_errors.title_required')
  } else if (!propertyTypeId) {
    throw new CustomError(400, 'properties.finn_errors.property_type_required')
  }
}

export const validatePendingFinnRequest = async (data, session) => {
  const { checkProcessFlow, partnerId, propertyId } = data
  const query = {
    action: 'handle_finn_listing',
    status: { $ne: 'completed' },
    'params.partnerId': partnerId,
    'params.propertyId': propertyId
  }
  if (checkProcessFlow) {
    query['params.processFlow'] = { $ne: 'archive_and_republish' }
  }
  const appQueue = await appQueueHelper.getAppQueues(query, session)
  if (size(appQueue)) {
    throw new CustomError(405, 'Finn request is already in progress')
  }
  return true
}

export const preparePropertiesQueryFromFilterData = async (body) => {
  const queryData = []
  const query = {}
  let statusQueryData = []
  let excludedPropertyIds = []
  let propertyIds = []
  let isNotProperty = false
  let depositPropertyIds = []
  const tenantStatus = {}
  if (size(body)) {
    const {
      accountId = '',
      agentId = '',
      apartmentId = '',
      assignmentESignStatus = [],
      availabilityDateRange = {},
      branchId = '',
      createdAtDateRange = {},
      depositAmount = {},
      hasActiveLease = false,
      hasActiveInProgressLease = '',
      hasActiveUpcomingLease = '',
      hasAssignment = '',
      hasCpiEnabled = '',
      hasInProgressLease = false,
      hasIssues,
      hasListed = '',
      hasProspects = '',
      hasProtocol,
      hasTerminatedContract = '',
      hasUpcomingLease = false,
      leaseEndDateRange = {},
      leaseESignStatus = [],
      leaseStartDateRange = {},
      listingTypeIds = [],
      movingInStatus = [],
      movingOutStatus = [],
      name,
      noOfBedrooms = [],
      partnerId,
      placeSize = {},
      propertyId = '',
      propertyStatus = [],
      protocolType = [],
      rentAmount = {},
      responsibleForFixing = [],
      searchKeyword = '',
      serial,
      tenantDetailsView,
      tenantId = ''
    } = body
    let { depositAccountStatus = [] } = body

    const isWaitingForPayment =
      indexOf(depositAccountStatus, 'waitingForPayment') !== -1 || false
    const isPaid = indexOf(depositAccountStatus, 'paid') !== -1 || false
    const isOverPaid = indexOf(depositAccountStatus, 'overPaid') !== -1 || false
    const isPartiallyPaid =
      indexOf(depositAccountStatus, 'partiallyPaid') !== -1 || false
    const noDeposit = indexOf(depositAccountStatus, 'noDeposit') !== -1 || false

    // search property id
    if (size(listingTypeIds)) {
      const compactListingTypeIds = compact(listingTypeIds)
      queryData.push({ listingTypeId: { $in: compactListingTypeIds } })
    }
    if (partnerId) queryData.push({ partnerId })
    if (branchId) queryData.push({ branchId })
    if (agentId) queryData.push({ agentId })
    if (accountId) queryData.push({ accountId })
    if (propertyId) queryData.push({ _id: propertyId })
    if (name)
      queryData.push({
        'location.name': { $regex: name, $options: 'i' }
      })
    if (body.hasOwnProperty('serial')) queryData.push({ serial })

    // Property status should be update for extra field
    if (size(propertyStatus))
      statusQueryData.push({ propertyStatus: { $in: propertyStatus } })

    const statusParams = {
      depositAccountStatus,
      hasActiveLease,
      hasUpcomingLease,
      isWaitingForPayment,
      isPaid,
      isOverPaid,
      isPartiallyPaid,
      noDeposit,
      propertyStatus
    }
    // Prepare property status query
    statusQueryData = preparePropertyStatusQuery(statusParams, statusQueryData)
    const queryForPropertyIds = {
      context: 'property',
      partnerId,
      depositAccountStatus,
      hasUpcomingLease,
      hasActiveLease,
      hasInProgressLease
    }
    // Get property ids
    depositPropertyIds = await tenantHelper.getPropertyOrTenantIds(
      queryForPropertyIds
    )

    if (hasInProgressLease) {
      const leaseQueryParams = {
        hasUpcomingLease,
        hasActiveLease,
        hasInProgressLease,
        depositAccountStatus,
        leaseESignStatus,
        assignmentESignStatus,
        depositPropertyIds,
        partnerId
      }
      const leaseQueryData = await prepareLeaseStatusQuery(leaseQueryParams)
      if (size(leaseQueryData)) statusQueryData.push(...leaseQueryData)
    }
    if (size(statusQueryData)) {
      queryData.push({ $or: statusQueryData })
    }
    //Check if the active lease has another upcoming/in progress lease for property
    // Has active lease
    if (hasActiveLease) {
      if (hasActiveUpcomingLease === 'yes')
        queryData.push({ hasUpcomingLease: true })
      else if (hasActiveUpcomingLease === 'no')
        queryData.push({ hasUpcomingLease: { $ne: true } })
      if (hasActiveInProgressLease === 'yes')
        queryData.push({ hasInProgressLease: true })
      else if (hasActiveInProgressLease === 'no')
        queryData.push({ hasInProgressLease: { $ne: true } })
    }
    // Has assignment
    if (hasAssignment === 'yes') queryData.push({ hasAssignment: true })
    else if (hasAssignment === 'no')
      queryData.push({ hasAssignment: { $ne: true } })
    // Has listed
    if (hasListed === 'yes') queryData.push({ listed: true })
    else if (hasListed === 'no') queryData.push({ listed: { $ne: true } })
    // Has prospect
    if (hasProspects === 'yes') queryData.push({ hasProspects: true })
    else if (hasProspects === 'no')
      queryData.push({ hasProspects: { $ne: false } })

    let placeSizeQuery = {}
    if (size(placeSize)) {
      placeSizeQuery = {
        $gte: placeSize.minimum,
        $lte: placeSize.maximum
      }
    }
    if (size(placeSizeQuery)) queryData.push({ placeSize: placeSizeQuery })
    if (size(noOfBedrooms))
      queryData.push({ noOfAvailableBedrooms: { $in: noOfBedrooms } })
    let monthlyRentAmountQuery = {}
    if (size(rentAmount)) {
      monthlyRentAmountQuery = {
        $gte: Math.floor(rentAmount.minimum),
        $lte: Math.floor(rentAmount.maximum)
      }
    }
    if (size(monthlyRentAmountQuery))
      queryData.push({ monthlyRentAmount: monthlyRentAmountQuery })

    let depositAmountQuery = {}
    if (size(depositAmount)) {
      depositAmountQuery = {
        $gte: Math.floor(depositAmount.minimum),
        $lte: Math.floor(depositAmount.maximum)
      }
    }
    if (size(depositAmountQuery)) {
      if (depositAmount.minimum === 0) {
        queryData.push({
          $or: [
            { depositAmount: depositAmountQuery },
            {
              depositAmount: {
                $exists: false
              }
            },
            {
              depositAmount: null
            }
          ]
        })
      } else {
        queryData.push({ depositAmount: depositAmountQuery })
      }
    }

    //Set availability dates from user's selection
    if (
      size(availabilityDateRange) &&
      availabilityDateRange.startDate &&
      availabilityDateRange.endDate
    ) {
      queryData.push({
        availabilityStartDate: {
          $gte: new Date(availabilityDateRange.startDate),
          $lte: new Date(availabilityDateRange.endDate)
        }
      })
    }
    //Set leaseStartDateRange filters in query
    if (
      size(leaseStartDateRange) &&
      leaseStartDateRange.startDate &&
      leaseStartDateRange.endDate
    ) {
      const leaseQuery = {
        partnerId,
        'rentalMeta.status': { $ne: 'closed' },
        'rentalMeta.contractStartDate': {
          $gte: new Date(leaseStartDateRange.startDate),
          $lte: new Date(leaseStartDateRange.endDate)
        }
      }
      const leasePropertiesIds = await contractHelper.getContractPropertyIds(
        leaseQuery
      )
      if (size(propertyIds))
        propertyIds = intersection(propertyIds, leasePropertiesIds)
      else propertyIds = leasePropertiesIds
      if (!size(propertyIds)) {
        queryData.push({ _id: 'nothing' })
        isNotProperty = true
      }
    }

    //Set leaseEndDateRange filters in query
    if (
      size(leaseEndDateRange) &&
      leaseEndDateRange.startDate &&
      leaseEndDateRange.endDate
    ) {
      const leaseEndDateQuery = {
        partnerId,
        'rentalMeta.contractEndDate': {
          $gte: new Date(leaseEndDateRange.startDate),
          $lte: new Date(leaseEndDateRange.endDate)
        }
      }
      const getPropertiesIds = await contractHelper.getContractPropertyIds(
        leaseEndDateQuery
      )

      if (size(propertyIds))
        propertyIds = intersection(propertyIds, getPropertiesIds)
      else propertyIds = getPropertiesIds

      //if rental contract not found by lease start date range then don`t show any properties
      if (!size(propertyIds)) {
        queryData.push({ _id: 'nothing' })
        isNotProperty = true
      }
    }
    //Set cpiEnabled filter in query
    if (size(hasCpiEnabled)) {
      const cpiEnabledQuery = {
        partnerId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] },
        'rentalMeta.cpiEnabled': true
      }

      if (hasCpiEnabled === 'yes') {
        const activePropertyIds = await contractHelper.getContractPropertyIds(
          cpiEnabledQuery
        )
        if (size(propertyIds))
          propertyIds = intersection(propertyIds, activePropertyIds)
        else propertyIds = activePropertyIds
        if (!size(propertyIds)) {
          queryData.push({ _id: 'nothing' })
          isNotProperty = true
        }
      } else if (hasCpiEnabled === 'no') {
        excludedPropertyIds = await contractHelper.getContractPropertyIds(
          cpiEnabledQuery
        )
        propertyIds = uniq(difference(propertyIds, excludedPropertyIds))
      }
    }

    // Set Terminated contract in query
    if (hasTerminatedContract) {
      const terminatedContractQuery = {
        partnerId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] },
        terminatedByUserId: { $exists: true }
      }
      const terminatedPropertyIds = await contractHelper.getContractPropertyIds(
        terminatedContractQuery
      )
      if (hasTerminatedContract === 'yes') {
        if (size(propertyIds))
          propertyIds = intersection(propertyIds, terminatedPropertyIds)
        else propertyIds = terminatedPropertyIds

        if (!size(propertyIds)) {
          queryData.push({ _id: 'nothing' })
          isNotProperty = true
        }
      } else if (hasTerminatedContract === 'no') {
        if (size(excludedPropertyIds))
          excludedPropertyIds = union(
            excludedPropertyIds,
            terminatedPropertyIds
          )
        else excludedPropertyIds = terminatedPropertyIds
      }
    }

    if (!isNotProperty && size(propertyIds)) {
      let queryByPropertyId = { _id: { $in: propertyIds } }
      if (size(excludedPropertyIds))
        queryByPropertyId = {
          _id: { $in: propertyIds, $nin: excludedPropertyIds }
        }
      queryData.push(queryByPropertyId)
    } else if (size(excludedPropertyIds)) {
      queryData.push({ _id: { $nin: excludedPropertyIds } })
    }
    if (hasIssues === 'yes') {
      const hasIssuesPropertyIds = await getIssuesPropertyIds(
        partnerId,
        true,
        responsibleForFixing
      )

      if (size(hasIssuesPropertyIds))
        queryData.push({ _id: { $in: hasIssuesPropertyIds } })
      else queryData.push({ _id: 'nothing' })
    } else if (hasIssues === 'no') {
      const hasIssuesPropertyIds = await getIssuesPropertyIds(partnerId, null)

      if (size(hasIssuesPropertyIds))
        queryData.push({ _id: { $in: hasIssuesPropertyIds } })
      else queryData.push({ _id: 'nothing' })
    }
    if (size(searchKeyword)) {
      if (parseInt(searchKeyword)) {
        queryData.push({
          $or: [
            {
              'location.streetNumber': { $regex: searchKeyword, $options: 'i' }
            },
            { serial: parseInt(searchKeyword) },
            { apartmentId: { $regex: searchKeyword, $options: 'i' } }
          ]
        })
      } else
        queryData.push({
          $or: [
            { 'location.name': { $regex: searchKeyword, $options: 'i' } },
            { apartmentId: { $regex: searchKeyword, $options: 'i' } }
          ]
        })
    }

    if (size(apartmentId)) {
      queryData.push({
        apartmentId: { $regex: apartmentId, $options: 'i' }
      })
    }
    if (tenantId) {
      appHelper.validateId({ tenantId })
      let propertyStatusArray = []
      const statusQuery = {}
      if (!tenantDetailsView) {
        propertyStatusArray = ['active', 'upcoming']
        statusQuery['properties.status'] = { $in: propertyStatusArray }
      }
      const tenantInfo = await tenantHelper.getATenant({
        _id: tenantId,
        partnerId,
        ...statusQuery
      })

      if (size(tenantInfo) && tenantInfo?.properties) {
        const tenantPropertyIdQuery = []

        for (const property of tenantInfo.properties) {
          if (
            tenantDetailsView ||
            propertyStatusArray.includes(property.status)
          ) {
            tenantPropertyIdQuery.push(property.propertyId)
            if (tenantDetailsView)
              tenantStatus[property.propertyId] = property.status
          }
        }
        if (size(tenantPropertyIdQuery))
          query['_id'] = { $in: tenantPropertyIdQuery }
      }
    }
    if (
      size(createdAtDateRange) &&
      createdAtDateRange.startDate &&
      createdAtDateRange.endDate
    ) {
      query.createdAt = {
        $gte: new Date(createdAtDateRange.startDate),
        $lte: new Date(createdAtDateRange.endDate)
      }
    }

    if (!hasUpcomingLease && !hasActiveLease) {
      depositAccountStatus = filter(
        depositAccountStatus,
        function (depositAcc) {
          return depositAcc === 'waitingForCreation'
        }
      )
    }
    if (!hasInProgressLease) {
      depositAccountStatus = filter(
        depositAccountStatus,
        function (depositAcc) {
          return depositAcc !== 'waitingForCreation'
        }
      )
    }
    depositAccountStatus = compact(depositAccountStatus)
    if (
      (hasUpcomingLease || hasActiveLease || hasInProgressLease) &&
      size(depositAccountStatus)
    ) {
      if (size(depositPropertyIds))
        queryData.push({ _id: { $in: depositPropertyIds } })
    }
    // For moving in/out protocol
    const hasMovingInProtocol = protocolType.includes('movingIn')
    const hasMovingOutProtocol = protocolType.includes('movingOut')
    const propertyMovingInAndOut = await prepareMovingOutProperties({
      hasProtocol,
      movingInStatus,
      movingOutStatus,
      partnerId,
      hasMovingInProtocol,
      hasMovingOutProtocol
    })
    if (size(propertyMovingInAndOut)) {
      queryData.push(...propertyMovingInAndOut)
    }
  } else queryData.push({ _id: 'nothing' })

  if (size(queryData)) query['$and'] = queryData

  return {
    preparedQuery: query,
    tenantStatus
  }
}

const preparePropertyStatusQuery = (params, statusQueryData = []) => {
  const {
    depositAccountStatus,
    hasActiveLease,
    hasUpcomingLease,
    isWaitingForPayment,
    isPaid,
    isOverPaid,
    isPartiallyPaid,
    noDeposit,
    propertyStatus
  } = params
  let statusArray = statusQueryData
  if (size(propertyStatus) && indexOf(propertyStatus, 'active') !== -1) {
    if (hasActiveLease && !hasUpcomingLease) {
      statusArray = [
        {
          $and: [
            { hasUpcomingLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    } else if (hasUpcomingLease && !hasActiveLease) {
      statusArray = [
        {
          $and: [
            { hasActiveLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    } else if (!hasUpcomingLease && !hasActiveLease) {
      statusArray = [
        {
          $and: [
            { hasActiveLease: { $ne: true } },
            { hasInProgressLease: { $ne: true } },
            { hasUpcomingLease: { $ne: true } },
            { propertyStatus: { $in: propertyStatus } }
          ]
        }
      ]
    }
  } else {
    if (
      hasActiveLease &&
      !isWaitingForPayment &&
      !isPaid &&
      !noDeposit &&
      !isOverPaid &&
      !isPartiallyPaid
    )
      statusArray.push({ hasActiveLease: true })

    if (hasUpcomingLease && !size(depositAccountStatus)) {
      statusArray.push({ hasUpcomingLease: true })
    }
  }
  return statusArray
}

const prepareLeaseStatusQuery = async (params) => {
  const {
    assignmentESignStatus,
    depositPropertyIds,
    depositAccountStatus,
    hasUpcomingLease,
    hasActiveLease,
    hasInProgressLease,
    leaseESignStatus,
    partnerId
  } = params
  const statusQuery = []
  let leaseContractQuery = {
    partnerId,
    'rentalMeta.enabledLeaseEsigning': true,
    'rentalMeta.status': 'in_progress'
  }
  let assignmentContractQuery = {
    partnerId,
    enabledEsigning: true,
    status: 'in_progress'
  }
  const leaseStatusQuery = []
  const assignmentStatusQuery = []

  //Lease E-Sign Status query
  if (size(leaseESignStatus)) {
    if (includes(leaseESignStatus, 'leaseSentToTenant')) {
      leaseStatusQuery.push({
        'rentalMeta.tenantLeaseSigningStatus': {
          $elemMatch: { signed: false }
        }
      })
    }
    if (includes(leaseESignStatus, 'leaseSignedByTenant')) {
      leaseStatusQuery.push({
        $and: [
          {
            'rentalMeta.tenantLeaseSigningStatus': {
              $not: { $elemMatch: { signed: false } }
            }
          },
          { 'rentalMeta.landlordLeaseSigningStatus.signed': false }
        ]
      })
    }
    if (includes(leaseESignStatus, 'leaseSentToLandlord')) {
      leaseStatusQuery.push({
        'rentalMeta.landlordLeaseSigningStatus.signed': false
      })
    }
    if (includes(leaseESignStatus, 'leaseSignedByLandlord')) {
      leaseStatusQuery.push({
        'rentalMeta.landlordLeaseSigningStatus.signed': true
      })
    }
    if (size(leaseStatusQuery)) leaseContractQuery['$or'] = leaseStatusQuery
  } else if (size(assignmentESignStatus)) {
    leaseContractQuery = null
  }
  //assignment e-sign-status query builder
  if (size(assignmentESignStatus)) {
    if (includes(assignmentESignStatus, 'assignmentSentToAgent')) {
      assignmentStatusQuery.push({
        ['agentAssignmentSigningStatus.signed']: false
      })
    }
    if (includes(assignmentESignStatus, 'assignmentSignedByAgent')) {
      assignmentStatusQuery.push({
        ['agentAssignmentSigningStatus.signed']: true
      })
    }
    if (includes(assignmentESignStatus, 'assignmentSentToLandlord')) {
      assignmentStatusQuery.push({
        ['landlordAssignmentSigningStatus.signed']: false
      })
    }
    if (includes(assignmentESignStatus, 'assignmentSignedByLandlord')) {
      assignmentStatusQuery.push({
        ['landlordAssignmentSigningStatus.signed']: true
      })
    }
    if (size(assignmentStatusQuery))
      assignmentContractQuery['$or'] = assignmentStatusQuery
  } else if (size(leaseESignStatus)) {
    assignmentContractQuery = null
  }
  const leasePropertiesIds = size(leaseContractQuery)
    ? await contractHelper.getContractPropertyIds(leaseContractQuery)
    : []

  const assignmentPropertiesIds = size(assignmentContractQuery)
    ? await contractHelper.getContractPropertyIds(assignmentContractQuery)
    : []

  let contractPropertyIds = uniq(
    union(leasePropertiesIds, assignmentPropertiesIds)
  )

  if (
    (hasUpcomingLease || hasActiveLease || hasInProgressLease) &&
    size(depositAccountStatus)
  ) {
    if (size(depositPropertyIds)) {
      contractPropertyIds = uniq(union(contractPropertyIds, depositPropertyIds))
    }
  }

  statusQuery.push({ _id: { $in: contractPropertyIds } })

  return statusQuery
}

const getPropertyUtilitiesForQuery = async (params) => {
  const { partnerId, propertyId } = params
  const KeyInventoryMeterReadingPipeline = [
    {
      $match: {
        partnerId,
        propertyId,
        contractId: { $exists: false },
        type: { $exists: false }
      }
    },
    {
      $addFields: {
        meterReading: '$meterReading.meters',
        inventory: '$inventory.furniture',
        keys: '$keys.keysList'
      }
    },
    {
      $project: {
        inventory: {
          name: 1,
          quantity: 1,
          status: 1
        },
        meterReading: {
          measureOfMeter: 1,
          numberOfMeter: 1
        },
        keys: {
          id: 1,
          kindOfKey: 1,
          numberOfKey: 1
        }
      }
    }
  ]

  const [keyInventoryMeterReading] =
    (await PropertyItemCollection.aggregate(
      KeyInventoryMeterReadingPipeline
    )) || []

  const roomUtilityPipeline = [
    {
      $match: {
        partnerId,
        propertyId,
        contractId: { $exists: false }
      }
    },
    {
      $project: {
        name: 1,
        numberOfIssues: {
          $cond: {
            if: { $ifNull: ['$items', false] },
            then: {
              $size: {
                $filter: {
                  input: '$items',
                  as: 'item',
                  cond: {
                    $eq: ['$$item.status', 'issues']
                  }
                }
              }
            },
            else: 0
          }
        }
      }
    }
  ]

  const roomUtility =
    (await PropertyRoomCollection.aggregate(roomUtilityPipeline)) || []

  return {
    meterReadings: keyInventoryMeterReading?.meterReading || [],
    keys: keyInventoryMeterReading?.keys || [],
    inventories: keyInventoryMeterReading?.inventory || [],
    rooms: roomUtility
  }
}

export const queryGetPropertyUtilities = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  appHelper.checkRequiredFields(['propertyId'], body)
  body.partnerId = partnerId

  return await getPropertyUtilitiesForQuery(body)
}

const getPropertyIdsForTenantId = async ({ partnerId, tenantId }) => {
  const pipeline = [
    {
      $match: {
        _id: tenantId,
        partnerId
      }
    },
    {
      $project: {
        _id: 1,
        properties: 1
      }
    },
    appHelper.getUnwindPipeline('properties'),
    {
      $match: {
        'properties.status': { $in: ['active', 'upcoming'] }
      }
    },
    {
      $group: {
        _id: null,
        propertyIds: {
          $addToSet: '$properties.propertyId'
        }
      }
    }
  ]
  const [ids = {}] = (await TenantCollection.aggregate(pipeline)) || []
  return ids?.propertyIds || []
}

const preparePropertyIssuesQueryFromFilterData = async (params) => {
  const {
    accountId,
    agentId,
    branchId,
    janitorId,
    partnerId,
    propertyId,
    status = [],
    searchKeyword,
    tenantId
  } = params
  const preparedQuery = { partnerId }

  if (accountId) preparedQuery.accountId = accountId
  if (agentId) preparedQuery.agentId = agentId
  if (branchId) preparedQuery.branchId = branchId
  if (janitorId) preparedQuery.janitorId = janitorId
  if (propertyId) preparedQuery._id = propertyId
  if (tenantId) {
    const propertyIds = await getPropertyIdsForTenantId({ partnerId, tenantId })
    if (propertyIds.length > 0) preparedQuery._id = { $in: propertyIds }
  }

  // query for status
  const prepareStatus = []

  if (status.includes('occupied')) prepareStatus.push({ hasActiveLease: true })

  if (status.includes('up_coming'))
    prepareStatus.push({ hasUpcomingLease: true })

  if (status.includes('in_progress'))
    prepareStatus.push({ hasInProgressLease: true })

  if (status.includes('vacant'))
    prepareStatus.push({
      $and: [
        { hasActiveLease: false },
        { hasUpcomingLease: false },
        { hasInProgressLease: false }
      ]
    })

  const archivedAndMaintenanceArr = []
  if (status.includes('maintenance'))
    archivedAndMaintenanceArr.push('maintenance')
  if (status.includes('archived')) archivedAndMaintenanceArr.push('archived')

  if (archivedAndMaintenanceArr.length > 0)
    prepareStatus.push({
      propertyStatus: { $in: archivedAndMaintenanceArr }
    })

  if (size(status)) preparedQuery['$or'] = prepareStatus

  if (searchKeyword) {
    if (parseInt(searchKeyword)) {
      preparedQuery['$or'] = [
        {
          'location.streetNumber': { $regex: searchKeyword, $options: 'i' }
        },
        { serial: parseInt(searchKeyword) },
        { apartmentId: { $regex: searchKeyword, $options: 'i' } }
      ]
    } else
      preparedQuery['$or'] = [
        { 'location.name': { $regex: searchKeyword, $options: 'i' } },
        { apartmentId: { $regex: searchKeyword, $options: 'i' } }
      ]
  }

  return preparedQuery
}

const getPropertyIdsForFilter = async (filteredQuery) => {
  const pipeline = [
    {
      $match: filteredQuery
    },
    {
      $group: { _id: null, propertyIds: { $push: '$_id' } }
    }
  ]

  const [ids = {}] = (await ListingCollection.aggregate(pipeline)) || []
  return ids.propertyIds || []
}

const getPropertyIdsForItems = async (issuesQuery, responsibleFor) => {
  const elemMatchQuery = { 'inventory.furniture.status': 'issues' }

  if (size(responsibleFor)) {
    elemMatchQuery['$or'] = [
      { 'inventory.furniture.responsibleForFixing': { $in: responsibleFor } }
    ]
    if (responsibleFor.includes('noActionRequired'))
      elemMatchQuery['$or'].push({
        'inventory.furniture.responsibleForFixing': { $exists: false }
      })
  }

  const pipeline = [
    {
      $match: issuesQuery
    },
    {
      $unwind: {
        path: '$inventory.furniture',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: elemMatchQuery
    },
    {
      $group: {
        _id: null,
        propertyIds: {
          $addToSet: '$propertyId'
        }
      }
    }
  ]
  const [properties = {}] =
    (await PropertyItemCollection.aggregate(pipeline)) || []
  return properties?.propertyIds || []
}

const getPropertyIdsForRoomItems = async (issuesQuery, responsibleFor) => {
  const elemMatchQuery = { 'items.status': 'issues' }

  if (size(responsibleFor)) {
    elemMatchQuery['$or'] = [
      { 'items.responsibleForFixing': { $in: responsibleFor } }
    ]
    if (responsibleFor.includes('noActionRequired'))
      elemMatchQuery['$or'].push({
        'items.responsibleForFixing': { $exists: false }
      })
  }

  const pipeline = [
    {
      $match: issuesQuery
    },
    {
      $unwind: {
        path: '$items',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: elemMatchQuery
    },
    {
      $group: {
        _id: null,
        propertyIds: {
          $addToSet: '$propertyId'
        }
      }
    }
  ]
  const [properties = {}] =
    (await PropertyRoomCollection.aggregate(pipeline)) || []
  return properties?.propertyIds || []
}

const getPropertyIdsForQuery = async ({ propertyQuery, query }) => {
  const { janitorId, partnerId, responsibleFor = [] } = query
  if (!size(propertyQuery)) return []

  const initialPropertyIds = await getPropertyIdsForFilter(propertyQuery)

  const issuesQuery = {
    partnerId,
    contractId: { $exists: false }
  }
  if (size(initialPropertyIds))
    issuesQuery.propertyId = { $in: initialPropertyIds }

  if (janitorId && !size(initialPropertyIds)) return []

  const propertyIdsOfItems = await getPropertyIdsForItems(
    issuesQuery,
    responsibleFor
  )
  const propertyIdsOfRoomItems = await getPropertyIdsForRoomItems(
    issuesQuery,
    responsibleFor
  )
  const map = {}
  for (let i = 0; i < size(propertyIdsOfItems); i++) {
    if (map[propertyIdsOfItems[i]] === undefined) map[propertyIdsOfItems[i]] = i
  }
  for (let i = 0; i < size(propertyIdsOfRoomItems); i++) {
    if (map[propertyIdsOfRoomItems[i]] === undefined)
      map[propertyIdsOfRoomItems[i]] = i
  }
  return Object.keys(map) || []
}

const initialListingProject = () => ({
  $project: {
    _id: 1,
    partnerId: 1,
    propertyStatus: 1,
    location: 1,
    listingTypeId: 1,
    propertyTypeId: 1,
    apartmentId: 1,
    floor: 1,
    images: 1,
    listed: 1,
    createdAt: 1,
    serial: 1,
    hasActiveLease: 1,
    hasUpcomingLease: 1,
    hasInProgressLease: 1
  }
})

const lookupRoomsAndItems = () => [
  {
    $lookup: {
      from: 'property_rooms',
      localField: '_id',
      foreignField: 'propertyId',
      as: 'propertyRoomInfo'
    }
  },
  {
    $lookup: {
      from: 'property_items',
      localField: '_id',
      foreignField: 'propertyId',
      as: 'propertyItemInfo'
    }
  }
]

const filterOnlyIssuesItems = ({ responsibleFor = [] }) => {
  const elemMatchQuery = {}
  if (size(responsibleFor)) {
    elemMatchQuery['$or'] = [
      { $in: ['$$propertyRoom.responsibleForFixing', responsibleFor] }
    ]
    if (responsibleFor.includes('noActionRequired'))
      elemMatchQuery['$or'].push({
        $not: { $ifNull: ['$$propertyRoom.responsibleForFixing', false] }
      })
  }
  return [
    {
      $addFields: {
        propertyRoomInfo: {
          $filter: {
            input: '$propertyRoomInfo',
            as: 'room',
            cond: {
              $and: [
                { $eq: ['$$room.partnerId', '$partnerId'] },
                { $not: { $ifNull: ['$$room.contractId', false] } }
              ]
            }
          }
        },
        propertyItemInfo: {
          $filter: {
            input: '$propertyItemInfo',
            as: 'item',
            cond: {
              $and: [
                { $eq: ['$$item.partnerId', '$partnerId'] },
                { $not: { $ifNull: ['$$item.contractId', false] } }
              ]
            }
          }
        }
      }
    },
    {
      $addFields: {
        propertyRoomInfo: {
          $filter: {
            input: '$propertyRoomInfo',
            as: 'room',
            cond: {
              $anyElementTrue: {
                $filter: {
                  input: { $ifNull: ['$$room.items', []] },
                  as: 'roomItem',
                  cond: { $eq: ['$$roomItem.status', 'issues'] }
                }
              }
            }
          }
        },
        propertyItemInfo: {
          $filter: {
            input: '$propertyItemInfo',
            as: 'propertyItem',
            cond: {
              $anyElementTrue: {
                $filter: {
                  input: {
                    $ifNull: ['$$propertyItem.inventory.furniture', []]
                  },
                  as: 'item',
                  cond: { $eq: ['$$item.status', 'issues'] }
                }
              }
            }
          }
        }
      }
    },
    {
      $match: {
        $expr: {
          $or: [
            {
              $gt: [{ $size: '$propertyRoomInfo' }, 0]
            },
            {
              $gt: [{ $size: '$propertyItemInfo' }, 0]
            }
          ]
        }
      }
    },
    {
      $unwind: {
        path: '$propertyRoomInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        'propertyRoomInfo.items': {
          $filter: {
            input: '$propertyRoomInfo.items',
            as: 'propertyRoom',
            cond: {
              $and: [
                { $eq: ['$$propertyRoom.status', 'issues'] },
                elemMatchQuery
              ]
            }
          }
        }
      }
    },
    {
      $match: {
        $expr: {
          $or: [
            {
              $gt: [{ $size: { $ifNull: ['$propertyRoomInfo.items', []] } }, 0]
            },
            { $gt: [{ $size: { $ifNull: ['$propertyItemInfo', []] } }, 0] }
          ]
        }
      }
    }
  ]
}

const lookupTaskInfo = () => [
  {
    $unwind: {
      path: '$propertyRoomInfo.items',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'tasks',
      localField: 'propertyRoomInfo.items.taskId',
      foreignField: '_id',
      as: 'taskInfo'
    }
  },
  {
    $unwind: {
      path: '$taskInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      'propertyRoomInfo.items.taskInfo': '$taskInfo'
    }
  }
]

const groupRoomItems = () => [
  {
    $group: {
      _id: {
        _id: '$propertyRoomInfo._id',
        propertyId: '$_id'
      },
      mainId: {
        $first: '$_id'
      },
      propertyStatus: {
        $first: '$propertyStatus'
      },
      location: {
        $first: '$location'
      },
      listingTypeId: {
        $first: '$listingTypeId'
      },
      propertyTypeId: {
        $first: '$propertyTypeId'
      },
      apartmentId: {
        $first: '$apartmentId'
      },
      floor: {
        $first: '$floor'
      },
      listed: {
        $first: '$listed'
      },
      images: {
        $first: '$images'
      },
      createdAt: {
        $first: '$createdAt'
      },
      serial: {
        $first: '$serial'
      },
      hasActiveLease: {
        $first: '$hasActiveLease'
      },
      hasUpcomingLease: {
        $first: '$hasUpcomingLease'
      },
      hasInProgressLease: {
        $first: '$hasInProgressLease'
      },
      propertyRoomInfo_name: {
        $first: '$propertyRoomInfo.name'
      },
      propertyRoomInfo_type: {
        $first: '$propertyRoomInfo.type'
      },
      propertyRoomInfo_id: {
        $first: '$propertyRoomInfo._id'
      },
      propertyRoomInfo_createdAt: {
        $first: '$propertyRoomInfo.createdAt'
      },
      propertyRoomInfo_items: {
        $push: '$propertyRoomInfo.items'
      },
      propertyItemInfo: {
        $first: '$propertyItemInfo'
      }
    }
  },
  {
    $addFields: {
      firstRoomItem: {
        $first: '$propertyRoomInfo_items'
      }
    }
  },
  {
    $addFields: {
      propertyRoomInfo_items: {
        $cond: [
          { $ifNull: ['$firstRoomItem.id', false] },
          '$propertyRoomInfo_items',
          '$$REMOVE'
        ]
      }
    }
  },
  {
    $addFields: {
      roomIssues: { $size: { $ifNull: ['$propertyRoomInfo_items', []] } }
    }
  },
  {
    $group: {
      _id: '$mainId',
      propertyStatus: {
        $first: '$propertyStatus'
      },
      location: {
        $first: '$location'
      },
      listingTypeId: {
        $first: '$listingTypeId'
      },
      propertyTypeId: {
        $first: '$propertyTypeId'
      },
      apartmentId: {
        $first: '$apartmentId'
      },
      floor: {
        $first: '$floor'
      },
      images: {
        $first: '$images'
      },
      listed: {
        $first: '$listed'
      },
      createdAt: {
        $first: '$createdAt'
      },
      serial: {
        $first: '$serial'
      },
      hasActiveLease: {
        $first: '$hasActiveLease'
      },
      hasUpcomingLease: {
        $first: '$hasUpcomingLease'
      },
      hasInProgressLease: {
        $first: '$hasInProgressLease'
      },
      propertyRoomIssues: {
        $push: {
          _id: '$propertyRoomInfo_id',
          name: '$propertyRoomInfo_name',
          type: '$propertyRoomInfo_type',
          createdAt: '$propertyRoomInfo_createdAt',
          items: '$propertyRoomInfo_items'
        }
      },
      roomIssues: {
        $sum: '$roomIssues'
      },
      propertyItemInfo: {
        $first: '$propertyItemInfo'
      }
    }
  }
]

const checkRoomIssues = () => [
  {
    $addFields: {
      firstRoomItem: {
        $first: '$propertyRoomIssues'
      }
    }
  },
  {
    $addFields: {
      propertyRoomIssues: {
        $cond: [
          { $ifNull: ['$firstRoomItem._id', false] },
          '$propertyRoomIssues',
          '$$REMOVE'
        ]
      }
    }
  }
]

const aggregateRoomItemsIssues = ({ responsibleFor = [] }) => [
  ...lookupRoomsAndItems(),
  ...filterOnlyIssuesItems({ responsibleFor }),
  ...lookupTaskInfo(),
  ...groupRoomItems(),
  ...checkRoomIssues()
]

const filterInventoryIssues = ({ responsibleFor = [] }) => {
  const elemMatchQuery = {}
  if (size(responsibleFor)) {
    elemMatchQuery['$or'] = [
      { $in: ['$$propertyItem.responsibleForFixing', responsibleFor] }
    ]
    if (responsibleFor.includes('noActionRequired'))
      elemMatchQuery['$or'].push({
        $not: { $ifNull: ['$$propertyItem.responsibleForFixing', false] }
      })
  }
  return [
    {
      $unwind: {
        path: '$propertyItemInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        propertyItemIssues: {
          $filter: {
            input: { $ifNull: ['$propertyItemInfo.inventory.furniture', []] },
            as: 'propertyItem',
            cond: {
              $and: [
                { $eq: ['$$propertyItem.status', 'issues'] },
                elemMatchQuery
              ]
            }
          }
        }
      }
    }
  ]
}

const lookupTaskInfoForItems = () => [
  {
    $unwind: {
      path: '$propertyItemIssues',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'tasks',
      localField: 'propertyItemIssues.taskId',
      foreignField: '_id',
      as: 'propertyItemIssues.taskInfo'
    }
  }
]

const groupItemIssues = () => [
  {
    $group: {
      _id: '$_id',
      propertyStatus: {
        $first: '$propertyStatus'
      },
      location: {
        $first: '$location'
      },
      listingTypeId: {
        $first: '$listingTypeId'
      },
      propertyTypeId: {
        $first: '$propertyTypeId'
      },
      apartmentId: {
        $first: '$apartmentId'
      },
      floor: {
        $first: '$floor'
      },
      images: {
        $first: '$images'
      },
      listed: {
        $first: '$listed'
      },
      createdAt: {
        $first: '$createdAt'
      },
      serial: {
        $first: '$serial'
      },
      hasActiveLease: {
        $first: '$hasActiveLease'
      },
      hasUpcomingLease: {
        $first: '$hasUpcomingLease'
      },
      hasInProgressLease: {
        $first: '$hasInProgressLease'
      },
      propertyItemIssues: {
        $push: '$propertyItemIssues'
      },
      propertyRoomIssues: {
        $first: '$propertyRoomIssues'
      },
      totalRoomItems: {
        $first: '$roomIssues'
      }
    }
  },
  {
    $addFields: {
      firstElement: {
        $first: '$propertyItemIssues'
      }
    }
  },
  {
    $addFields: {
      propertyItemIssues: {
        $cond: [
          { $ifNull: ['$firstElement.id', false] },
          '$propertyItemIssues',
          '$$REMOVE'
        ]
      }
    }
  }
]

const countTotalIssues = () => [
  {
    $addFields: {
      totalInventoryItems: { $size: { $ifNull: ['$propertyItemIssues', []] } }
    }
  },
  {
    $addFields: {
      totalIssues: {
        $sum: ['$totalRoomItems', '$totalInventoryItems']
      }
    }
  }
]

const aggregateItemIssues = ({ responsibleFor = [] }) => [
  ...filterInventoryIssues({ responsibleFor }),
  ...lookupTaskInfoForItems(),
  ...groupItemIssues(),
  ...countTotalIssues()
]

const getPropertyIssuesForQuery = async (
  { partnerId = '', propertyIds = [], responsibleFor = [] },
  options = {}
) => {
  const { sort, skip, limit } = options

  const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const soonEndingMonths = partnerSetting?.propertySettings?.soonEndingMonths
  const soonEndingMonthsDate = (
    await appHelper.getActualDate(partnerSetting, true)
  )
    .add(soonEndingMonths || 4, 'months')
    .toDate()

  const pipeline = [
    {
      $match: {
        _id: { $in: propertyIds }
      }
    },
    initialListingProject(),
    ...aggregateRoomItemsIssues({ responsibleFor }),
    ...aggregateItemIssues({ responsibleFor }),
    ...appHelper.getListingFirstImageUrl('$images'),
    ...appHelper.getSoonEndingTerminatedActiveUpcomingContractPipeline(
      soonEndingMonthsDate
    ),
    {
      $project: {
        _id: 1,
        propertyStatus: 1,
        location: {
          name: 1,
          city: 1,
          country: 1,
          postalCode: 1,
          streetNumber: 1
        },
        listingTypeId: 1,
        propertyTypeId: 1,
        apartmentId: 1,
        floor: 1,
        isSoonEnding: 1,
        isTerminated: 1,
        listed: 1,
        imageUrl: 1,
        createdAt: 1,
        serial: 1,
        hasActiveLease: 1,
        hasUpcomingLease: 1,
        hasInProgressLease: 1,
        totalIssues: 1,
        propertyRoomIssues: {
          _id: 1,
          name: 1,
          type: 1,
          items: {
            id: 1,
            status: 1,
            title: 1,
            description: 1,
            responsibleForFixing: 1,
            taskInfo: {
              _id: 1,
              title: 1
            }
          },
          createdAt: 1
        },
        propertyItemIssues: {
          id: 1,
          name: 1,
          quantity: 1,
          status: 1,
          description: 1,
          taskInfo: {
            _id: 1,
            title: 1
          }
        }
      }
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ]
  const propertyIssuesInfo = (await ListingCollection.aggregate(pipeline)) || []
  return propertyIssuesInfo
}

const countIssues = async ({ query, partnerId, propertyIds }, context = '') => {
  const issuesQuery = { partnerId, contractId: { $exists: false } }
  let propertiesForJanitor = []
  const { janitorId, responsibleFor = [] } = query

  if (propertyIds) issuesQuery.propertyId = { $in: propertyIds }
  if (janitorId) {
    propertiesForJanitor = await listingHelper.getUniqueFieldValueOfListings(
      '_id',
      {
        partnerId,
        janitorId
      }
    )
  }

  if (size(propertiesForJanitor))
    issuesQuery.propertyId = { $in: propertiesForJanitor }

  const matchForTotalInventory = {
    'inventory.furniture.status': 'issues'
  }
  const matchForTotalRoom = {
    'items.status': 'issues'
  }

  const matchForFilterInventory = {
    'inventory.furniture.status': 'issues'
  }
  const matchForFilteredRoom = {
    'items.status': 'issues'
  }

  if (size(responsibleFor)) {
    matchForFilterInventory['$or'] = [
      { 'inventory.furniture.responsibleForFixing': { $in: responsibleFor } }
    ]
    matchForFilteredRoom['$or'] = [
      { 'items.responsibleForFixing': { $in: responsibleFor } }
    ]
    if (responsibleFor.includes('noActionRequired')) {
      matchForFilterInventory['$or'].push({
        'inventory.furniture.responsibleForFixing': { $exists: false }
      })
      matchForFilteredRoom['$or'].push({
        'items.responsibleForFixing': { $exists: false }
      })
    }
  }

  let elemMatchQueryForInventory = {}
  let elemMatchQueryForRooms = {}
  if (context === 'filter') {
    elemMatchQueryForInventory = matchForFilterInventory
    elemMatchQueryForRooms = matchForFilteredRoom
  }
  if (context === 'total') {
    elemMatchQueryForInventory = matchForTotalInventory
    elemMatchQueryForRooms = matchForTotalRoom
  }

  // find inventory issues
  const propertyItemPipeline = [
    {
      $match: issuesQuery
    },
    {
      $unwind: '$inventory.furniture'
    },
    {
      $match: elemMatchQueryForInventory
    },
    {
      $group: {
        _id: '$inventory.furniture'
      }
    }
  ]
  const inventoryIssues =
    (await PropertyItemCollection.aggregate(propertyItemPipeline)) || []

  //  find room issues
  const propertyRoomPipeline = [
    {
      $match: issuesQuery
    },
    {
      $unwind: '$items'
    },
    {
      $match: elemMatchQueryForRooms
    },
    {
      $group: {
        _id: '$items'
      }
    }
  ]
  const roomIssues =
    (await PropertyRoomCollection.aggregate(propertyRoomPipeline)) || []

  return inventoryIssues.length + roomIssues.length
}

export const queryPropertyIssues = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { query, options } = body
  const { partnerId } = user
  query.partnerId = partnerId

  // this condition is commented, because janitorId is set when janitorId is pass in queryData
  // if (size(roles) === 1 && appHelper.isPartnerJanitor(roles))
  //   query.janitorId = userId

  appHelper.validateSortForQuery(options.sort)
  const propertyQuery = await preparePropertyIssuesQueryFromFilterData(query)
  const propertyIds = await getPropertyIdsForQuery({ propertyQuery, query })

  const { responsibleFor } = query
  const propertyIssues = await getPropertyIssuesForQuery(
    {
      partnerId,
      propertyIds,
      responsibleFor
    },
    options
  )
  const filteredDocuments = await countIssues(
    { query, partnerId, propertyIds },
    'filter'
  )
  const totalDocuments = await countIssues({ query, partnerId }, 'total')
  return {
    data: propertyIssues,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

const getPartnerDashboardPropertyInfo = async (query) => {
  const result = await ListingCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: null,
        totalProperty: { $sum: 1 },
        totalOccupied: {
          $sum: { $cond: [{ $eq: ['$hasActiveLease', true] }, 1, 0] }
        },
        havingNewLease: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$hasActiveLease', true] },
                  { $eq: ['$hasUpcomingLease', true] }
                ]
              },
              1,
              0
            ]
          }
        },
        withoutNewLease: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$hasActiveLease', true] },
                  { $eq: ['$hasUpcomingLease', false] }
                ]
              },
              1,
              0
            ]
          }
        },
        totalVacant: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$hasActiveLease', true] },
                  { $ne: ['$hasUpcomingLease', true] },
                  { $ne: ['$hasInProgressLease', true] },
                  {
                    $not: {
                      $in: ['$propertyStatus', ['archived', 'maintenance']]
                    }
                  }
                ]
              },
              1,
              0
            ]
          }
        },
        alreadyListed: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$hasActiveLease', true] },
                  { $ne: ['$hasUpcomingLease', true] },
                  { $ne: ['$hasInProgressLease', true] },
                  {
                    $not: {
                      $in: ['$propertyStatus', ['archived', 'maintenance']]
                    }
                  },
                  { $eq: ['$listed', true] }
                ]
              },
              1,
              0
            ]
          }
        },
        withoutListing: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$hasActiveLease', true] },
                  { $ne: ['$hasUpcomingLease', true] },
                  { $ne: ['$hasInProgressLease', true] },
                  {
                    $not: {
                      $in: ['$propertyStatus', ['archived', 'maintenance']]
                    }
                  },
                  { $ne: ['$listed', true] }
                ]
              },
              1,
              0
            ]
          }
        },
        totalUpcoming: {
          $sum: {
            $cond: [{ $eq: ['$hasUpcomingLease', true] }, 1, 0]
          }
        },
        totalMaintenance: {
          $sum: {
            $cond: [{ $eq: ['$propertyStatus', 'maintenance'] }, 1, 0]
          }
        },
        totalOccupiedInProgress: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$propertyStatus', 'archived'] },
                  {
                    $or: [
                      { $eq: ['$hasActiveLease', true] },
                      { $eq: ['$hasInProgressLease', true] },
                      { $eq: ['$hasUpcomingLease', true] }
                    ]
                  }
                ]
              },
              1,
              0
            ]
          }
        }
      }
    },
    {
      $addFields: {
        totalRentCoveragePercentage: {
          $cond: {
            if: {
              $eq: [{ $add: ['$totalOccupiedInProgress', '$totalVacant'] }, 0]
            },
            then: 0,
            else: {
              $multiply: [
                {
                  $subtract: [
                    1,
                    {
                      $divide: [
                        '$totalVacant',
                        { $add: ['$totalOccupiedInProgress', '$totalVacant'] }
                      ]
                    }
                  ]
                },
                100
              ]
            }
          }
        },
        occupiedProperty: {
          totalOccupied: '$totalOccupied',
          havingNewLease: '$havingNewLease',
          withoutNewLease: '$withoutNewLease'
        },
        vacantProperty: {
          totalVacant: '$totalVacant',
          withoutListing: '$withoutListing',
          alreadyListed: '$alreadyListed'
        }
      }
    },
    {
      $project: {
        _id: 0,
        totalProperty: 1,
        occupiedProperty: 1,
        vacantProperty: 1,
        totalUpcoming: 1,
        totalMaintenance: 1,
        totalRentCoveragePercentage: {
          $cond: [
            {
              $gte: [
                {
                  $subtract: [
                    '$totalRentCoveragePercentage',
                    { $floor: '$totalRentCoveragePercentage' }
                  ]
                },
                0.5
              ]
            },
            { $ceil: '$totalRentCoveragePercentage' },
            { $floor: '$totalRentCoveragePercentage' }
          ]
        }
      }
    }
  ])
  const [propertyInfo = {}] = result || []
  return propertyInfo
}

const getAndCheckQueryForJanitor = (query, user) => {
  const preparedQuery = { ...query }
  const { roles, userId } = user
  if (roles.includes('partner_janitor') && roles.length <= 2) {
    preparedQuery.janitorId = userId
  }
  return preparedQuery
}

const getPropertiesHavingIssues = async (query, user, partnerId) => {
  const preparedQuery = getAndCheckQueryForJanitor(query, user)
  const propertyIds = await listingHelper.getListingIds(preparedQuery)
  const partnerInfo = (await partnerHelper.getPartnerById(partnerId)) || {}
  const accountType = partnerInfo?.accountType
    ? partnerInfo.accountType
    : 'broker'
  const responsibleForFixing =
    accountType === 'direct'
      ? ['tenant', 'agent']
      : ['tenant', 'landlord', 'agent']
  const queryForIssues = {
    partnerId,
    contractId: { $exists: false },
    propertyId: { $in: propertyIds }
  }
  const { inventoryIssues, inventoryIssuePropertyIds } =
    await getPropertyInventoryItemIssues(queryForIssues, responsibleForFixing)
  const { roomIssues, roomIssuePropertyIds } = await getPropertyRoomItemsIssues(
    queryForIssues,
    responsibleForFixing
  )
  const totalPropertyHavingIssue = uniq([
    ...roomIssuePropertyIds,
    ...inventoryIssuePropertyIds
  ]).length
  return {
    totalIssues: inventoryIssues + roomIssues,
    totalPropertyHavingIssue,
    janitorTotalProperty: propertyIds.length
  }
}

const getPropertyInventoryItemIssues = async (query, responsibleForFixing) => {
  const result = await PropertyItemCollection.aggregate([
    { $unwind: '$inventory.furniture' },
    {
      $match: {
        ...query,
        'inventory.furniture.status': 'issues',
        'inventory.furniture.responsibleForFixing': {
          $in: responsibleForFixing
        }
      }
    },
    {
      $group: {
        _id: null,
        issues: { $push: '$inventory.furniture.id' },
        inventoryIssuePropertyIds: { $push: '$propertyId' }
      }
    },
    {
      $project: {
        _id: 0,
        inventoryIssues: {
          $size: {
            $ifNull: ['$issues', []]
          }
        },
        inventoryIssuePropertyIds: 1
      }
    }
  ])
  const [property = {}] = result || []
  const { inventoryIssues = 0, inventoryIssuePropertyIds = [] } = property
  return { inventoryIssues, inventoryIssuePropertyIds }
}

const getPropertyRoomItemsIssues = async (query, responsibleForFixing) => {
  const result = await PropertyRoomCollection.aggregate([
    { $unwind: '$items' },
    {
      $match: {
        ...query,
        'items.status': 'issues',
        'items.responsibleForFixing': { $in: responsibleForFixing }
      }
    },
    {
      $group: {
        _id: null,
        issues: { $push: '$items.id' },
        roomIssuePropertyIds: { $push: '$propertyId' }
      }
    },
    {
      $project: {
        _id: 0,
        roomIssues: {
          $size: {
            $ifNull: ['$issues', []]
          }
        },
        roomIssuePropertyIds: 1
      }
    }
  ])

  const [property = {}] = result || []
  const { roomIssues = 0, roomIssuePropertyIds = [] } = property
  return { roomIssues, roomIssuePropertyIds }
}

export const queryPropertyInfoForPartnerDashboard = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body, user } = req
  const { partnerId } = body
  const preparedQuery = dashboardHelper.prepareQueryForPartnerDashboard(body)
  const propertyIssue = await getPropertiesHavingIssues(
    preparedQuery,
    user,
    partnerId
  )
  const dashboardPropertyInfo = await getPartnerDashboardPropertyInfo(
    preparedQuery
  )
  return {
    ...dashboardPropertyInfo,
    ...propertyIssue
  }
}

export const prepareDataToUpdatePropertyStatus = (body = {}) => {
  const { propertyStatus } = body
  const updateData = {
    propertyStatus
  }
  if (propertyStatus === 'archived') updateData.listed = false
  return updateData
}

export const prepareReturnDataForUpdatePropertyOwner = async (
  property = {}
) => {
  const { _id, agentId, branchId } = property
  const branch = (await branchHelper.getBranchById(branchId)) || {}
  const agent = (await userHelper.getUserById(agentId)) || {}
  if (size(agent)) agent.avatarKey = userHelper.getAvatar(agent)
  return {
    _id,
    agentInfo: {
      _id: agent._id,
      name: agent.profile?.name,
      avatarKey: agent.avatarKey
    },
    branchInfo: {
      _id: branch._id,
      name: branch.name
    }
  }
}

export const validateParamsForDownloadProperty = (body) => {
  const { accountId, agentId, branchId, propertyId, sort, tenantId } = body
  if (accountId) appHelper.validateId({ accountId })
  if (agentId) appHelper.validateId({ agentId })
  if (branchId) appHelper.validateId({ branchId })
  if (propertyId) appHelper.validateId({ propertyId })
  if (size(sort)) appHelper.validateSortForQuery(sort)
  if (tenantId) appHelper.validateId({ tenantId })
}

export const getConversationsForOwnerChange = async (params) => {
  const { agentId, conversationQuery } = params
  const conversations = await ConversationCollection.aggregate([
    {
      $match: conversationQuery
    },
    {
      $lookup: {
        from: 'contracts',
        localField: 'contractId',
        foreignField: '_id',
        as: 'contract'
      }
    },
    { $unwind: { path: '$contract', preserveNullAndEmptyArrays: true } },
    {
      $addFields: {
        oldAgentId: {
          $ifNull: ['$contract.agentId', agentId]
        }
      }
    },
    {
      $lookup: {
        from: 'conversation-messages',
        let: {
          oldAgentId: '$oldAgentId',
          conversationId: '$_id'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$$oldAgentId', '$createdBy'] },
                  { $eq: ['$$conversationId', '$conversationId'] }
                ]
              },
              status: { $ne: 'closed' }
            }
          },
          { $limit: 1 }
        ],
        as: 'conversationMessage'
      }
    },
    {
      $unwind: {
        path: '$conversationMessage',
        preserveNullAndEmptyArrays: true
      }
    }
  ])
  return conversations
}

const prepareJanitorDropdownQueryPipeline = (query) => {
  const { searchString } = query
  const prepareQuery = []
  if (searchString)
    prepareQuery.push({
      $match: {
        'janitorInfo.name': { $regex: searchString, $options: 'i' }
      }
    })
  return prepareQuery
}

export const queryJanitorDropdown = async (req) => {
  const { body = {} } = req
  appHelper.validatePartnerAppRequestData(req)
  const { partnerId, query, options } = body
  const { searchString } = query
  const { sort, skip, limit } = options

  const pipeline = [
    {
      $match: {
        partnerId,
        type: 'partner_janitor'
      }
    },
    {
      $unwind: '$users'
    },
    ...appHelper.getCommonUserInfoPipeline('users', 'janitorInfo'),
    ...prepareJanitorDropdownQueryPipeline(query),
    {
      $project: {
        _id: '$janitorInfo._id',
        name: '$janitorInfo.name',
        avatar: '$janitorInfo.avatarKey'
      }
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ]

  const janitors = (await AppRoleCollection.aggregate(pipeline)) || []

  const appRole = await appRoleHelper.getAppRole({
    partnerId,
    type: 'partner_janitor'
  })

  const totalDocuments = appRole?.users?.length || 0
  let filteredDocuments = totalDocuments

  if (searchString) {
    filteredDocuments = await userHelper.countUsers({
      _id: {
        $in: appRole?.users || []
      },
      'profile.name': { $regex: searchString, $options: 'i' }
    })
  }

  return {
    data: janitors,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const prepareQueryForRentRollReport = async (query) => {
  const {
    accountId,
    agentId,
    branchId,
    createdAtDateRange,
    groupId,
    hasCpiEnabled,
    leaseStartDateRange,
    leaseEndDateRange,
    noOfBedrooms,
    partnerId,
    placeSize,
    rentAmount,
    tenantId
  } = query
  const preparedQuery = [
    {
      partnerId
    }
  ]
  if (branchId) preparedQuery.push({ branchId })
  if (agentId) preparedQuery.push({ agentId })
  if (accountId) preparedQuery.push({ accountId })
  if (groupId) preparedQuery.push({ groupId })
  if (
    size(createdAtDateRange) &&
    createdAtDateRange.startDate &&
    createdAtDateRange.endDate
  ) {
    preparedQuery.push({
      createdAt: {
        $gte: new Date(createdAtDateRange.startDate),
        $lte: new Date(createdAtDateRange.endDate)
      }
    })
  }
  if (size(noOfBedrooms)) {
    if (noOfBedrooms.includes(11)) {
      const otherNumbers = noOfBedrooms.filter((item) => item !== 11)
      preparedQuery.push({
        $or: [
          { noOfAvailableBedrooms: { $in: otherNumbers } },
          { noOfAvailableBedrooms: { $gt: 10 } }
        ]
      })
    } else {
      preparedQuery.push({
        noOfAvailableBedrooms: { $in: noOfBedrooms }
      })
    }
  }
  let propertyIds = []
  //Set leaseStartDateRange filters in query
  if (
    size(leaseStartDateRange) &&
    leaseStartDateRange.startDate &&
    leaseStartDateRange.endDate
  ) {
    const leaseQuery = {
      partnerId,
      'rentalMeta.status': { $ne: 'closed' },
      'rentalMeta.contractStartDate': {
        $gte: new Date(leaseStartDateRange.startDate),
        $lte: new Date(leaseStartDateRange.endDate)
      }
    }
    const leasePropertiesIds = await contractHelper.getContractPropertyIds(
      leaseQuery
    )
    if (size(propertyIds))
      propertyIds = intersection(propertyIds, leasePropertiesIds)
    else propertyIds = leasePropertiesIds
    if (!size(leasePropertiesIds)) {
      preparedQuery.push({ _id: 'nothing' })
    }
  }

  //Set leaseEndDateRange filters in query
  if (
    size(leaseEndDateRange) &&
    leaseEndDateRange.startDate &&
    leaseEndDateRange.endDate
  ) {
    const leaseEndDateQuery = {
      partnerId,
      'rentalMeta.contractEndDate': {
        $gte: new Date(leaseEndDateRange.startDate),
        $lte: new Date(leaseEndDateRange.endDate)
      }
    }
    const getPropertiesIds = await contractHelper.getContractPropertyIds(
      leaseEndDateQuery
    )

    if (size(propertyIds))
      propertyIds = intersection(propertyIds, getPropertiesIds)
    else propertyIds = getPropertiesIds

    //if rental contract not found by lease start date range then don`t show any properties
    if (!size(getPropertiesIds)) {
      preparedQuery.push({ _id: 'nothing' })
    }
  }

  if (tenantId) {
    const propertyStatusArray = ['active', 'upcoming']
    const tenantInfo = await tenantHelper.getATenant({
      _id: tenantId,
      partnerId,
      'properties.status': { $in: propertyStatusArray }
    })
    if (size(tenantInfo)) {
      const tenantPropertyIds = []
      for (const property of tenantInfo.properties) {
        if (propertyStatusArray.includes(property.status)) {
          tenantPropertyIds.push(property.propertyId)
        }
      }
      if (size(propertyIds))
        propertyIds = intersection(propertyIds, tenantPropertyIds)
      else propertyIds = tenantPropertyIds
    } else {
      preparedQuery.push({ _id: 'nothing' })
    }
  }

  //Set cpiEnabled filter in query
  if (hasCpiEnabled) {
    const cpiEnabledQuery = {
      partnerId,
      'rentalMeta.status': { $in: ['active', 'upcoming'] },
      'rentalMeta.cpiEnabled': true
    }

    if (hasCpiEnabled === 'yes') {
      const activePropertyIds = await contractHelper.getContractPropertyIds(
        cpiEnabledQuery
      )
      if (size(propertyIds))
        propertyIds = intersection(propertyIds, activePropertyIds)
      else propertyIds = activePropertyIds
      if (!size(activePropertyIds)) {
        preparedQuery.push({ _id: 'nothing' })
      }
    } else if (hasCpiEnabled === 'no') {
      cpiEnabledQuery['rentalMeta.cpiEnabled'] = { $ne: true }
      const cpiDisabledPropertyIds =
        await contractHelper.getContractPropertyIds(cpiEnabledQuery)
      if (size(propertyIds))
        propertyIds = intersection(propertyIds, cpiDisabledPropertyIds)
      else propertyIds = cpiDisabledPropertyIds
      if (!size(cpiDisabledPropertyIds)) {
        preparedQuery.push({ _id: 'nothing' })
      }
    }
  }

  if (size(propertyIds)) {
    preparedQuery.push({
      _id: {
        $in: propertyIds
      }
    })
  }

  if (size(rentAmount)) {
    preparedQuery.push({
      monthlyRentAmount: {
        $gte: Math.floor(rentAmount.minimum),
        $lte: Math.floor(rentAmount.maximum)
      }
    })
  }

  if (size(placeSize)) {
    preparedQuery.push({
      placeSize: {
        $gte: placeSize.minimum,
        $lte: placeSize.maximum
      }
    })
  }
  return {
    $and: preparedQuery
  }
}

export const queryRentRollReportForExcelManager = async (req) => {
  const { body = {}, user } = req
  appHelper.checkUserId(user.userId)
  const { query, options } = body
  appHelper.checkRequiredFields(['partnerId'], query)
  const { partnerId } = query
  appHelper.validateId({ partnerId })
  appHelper.validateSortForQuery(options.sort)
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  options.sort = prepareSortForPropertyData(options.sort)
  const preparedQuery = await prepareQueryForRentRollReport(query)
  return {
    data: await getRentRollReportForExcelManager({
      query: preparedQuery,
      options,
      partnerSetting,
      userLanguage: query.userLanguage || 'en'
    })
  }
}

const getRentRollReportForExcelManager = async (params = {}) => {
  const { query, options, partnerSetting, userLanguage } = params
  const { limit, skip, sort } = options
  const pipeline = [
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
    // This pipeline has group statement
    ...getAddonInfoPipelineForRentRollReport(),
    ...getLeaseInfoPipelineForRentRollReport(),
    // This pipeline has group statement
    ...getCoTenantsPipelineForRentRollReport(),
    ...getAccountPipelineForRentRollReport(),
    ...getMainTenantPipelineForRentRollReport(),
    ...(await getWaultYearsPipelineForRentRollReport(partnerSetting)),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...getAgentPipelineForRentRollReport(),
    ...(await getAreaGroupAndAreaTypePipeline()),
    ...getContractTypePipelineForRentRollReport(userLanguage),
    ...getMonthRentAmountPipelineForRentRollReport(),
    ...getDepositAmountPipelineForRentRollReport(),
    {
      $sort: sort
    },
    ...getFinalProjectPipelineForRentRollReport(partnerSetting)
  ]
  return (await ListingCollection.aggregate(pipeline)) || []
}

const getFinalProjectPipelineForRentRollReport = (partnerSetting = {}) => {
  const { currencySettings = {}, dateTimeSettings = {} } = partnerSetting
  const dateFormat =
    dateTimeSettings.dateFormat === 'DD.MM.YYYY' ? '%d-%m-%Y' : '%Y-%m-%d'
  const timeZone = dateTimeSettings.timezone || 'Europe/Oslo'
  const numberOfDecimal = currencySettings.numberOfDecimal || 2
  return [
    {
      $project: {
        _id: 1,
        accountName: '$accountInfo.name',
        propertyName: 1,
        tenantName: '$mainTenant.name',
        tenantEmail: '$mainTenantUser.email',
        tenantTLF: '$mainTenantUser.profile.phoneNumber',
        areaType: 1,
        areaGroup: 1,
        startDate: {
          $dateToString: {
            date: '$leaseInfo.rentalMeta.contractStartDate',
            format: dateFormat,
            timezone: timeZone,
            onNull: null
          }
        },
        endDate: {
          $dateToString: {
            date: '$leaseInfo.rentalMeta.contractEndDate',
            format: dateFormat,
            timezone: timeZone,
            onNull: null
          }
        },
        yearlyRent: {
          $round: [
            {
              $multiply: ['$monthlyRentAmount', 12]
            },
            numberOfDecimal
          ]
        },
        contractExclusiveArea: '$placeSize',
        contractInclusiveArea: '$placeSize',
        yearlyRentPerSqm: {
          $round: [
            {
              $divide: [
                {
                  $multiply: ['$monthlyRentAmount', 12]
                },
                {
                  $cond: [
                    {
                      $or: [
                        { $ifNull: ['$placeSize', false] },
                        { $gt: ['$placeSize', 0] }
                      ]
                    },
                    '$placeSize',
                    1
                  ]
                }
              ]
            },
            numberOfDecimal
          ]
        },
        noticePeriod: '$minimumStay',
        waultYears: 1,
        regulationType: 'Kpi',
        percentOfRegulation: '100.00%',
        cpiToDate: {
          $dateToString: {
            date: '$leaseInfo.rentalMeta.lastCpiDate',
            format: dateFormat,
            timezone: timeZone,
            onNull: null
          }
        },
        nextCpiDate: {
          $dateToString: {
            date: '$leaseInfo.rentalMeta.nextCpiDate',
            format: dateFormat,
            timezone: timeZone,
            onNull: null
          }
        },
        areaName: {
          $concat: ['$propertyName', ' ', { $ifNull: ['$apartmentId', ''] }]
        },
        contractType: 1,
        apartmentId: 1,
        noOfAvailableBedrooms: 1,
        floor: 1,
        depositAmount: 1,
        branchName: '$branchInfo.name',
        groupId: 1,
        agentName: '$agentInfo.profile.name',
        addons: {
          $cond: [{ $eq: ['$addons', ''] }, '', { $substr: ['$addons', 2, -1] }]
        },
        coTenantsInfo: {
          _id: 1,
          name: 1,
          email: 1,
          tlf: 1
        }
      }
    }
  ]
}

const getMonthRentAmountPipelineForRentRollReport = () => [
  {
    $addFields: {
      monthlyRentAmount: {
        $cond: [
          { $ifNull: ['$leaseInfo', false] },
          '$leaseInfo.rentalMeta.monthlyRentAmount',
          '$monthlyRentAmount'
        ]
      }
    }
  }
]

const getDepositAmountPipelineForRentRollReport = () => [
  {
    $addFields: {
      depositAmount: {
        $cond: [
          { $ifNull: ['$leaseInfo', false] },
          '$leaseInfo.rentalMeta.depositAmount',
          '$depositAmount'
        ]
      }
    }
  }
]

const getContractTypePipelineForRentRollReport = (userLanguage) => [
  {
    $addFields: {
      contractType: {
        $switch: {
          branches: [
            {
              case: { $eq: ['$hasActiveLease', true] },
              then: appHelper.translateToUserLng('common.active', userLanguage)
            },
            {
              case: { $eq: ['$hasUpcomingLease', true] },
              then: appHelper.translateToUserLng(
                'common.upcoming',
                userLanguage
              )
            },
            {
              case: { $eq: ['$hasInProgressLease', true] },
              then: appHelper.translateToUserLng(
                'common.in_progress',
                userLanguage
              )
            }
          ],
          default: appHelper.translateToUserLng('common.vacant', userLanguage)
        }
      }
    }
  }
]

const getAreaGroupAndAreaTypePipeline = async (partnerId) => {
  const rentAccounts = await accountingHelper.getAccountings(
    {
      partnerId,
      type: { $in: ['rent', 'rent_with_vat'] }
    },
    null,
    [
      {
        path: 'creditAccount',
        populate: 'taxCodeInfo'
      }
    ]
  )
  let rentText = ''
  let rentWithVatText = ''
  for (let i = 0; i < rentAccounts.length; i++) {
    const { creditAccount, type } = rentAccounts[i]
    if (type === 'rent') {
      rentText =
        creditAccount?.accountName +
        ' (' +
        creditAccount?.taxCodeInfo?.taxPercentage +
        '%)'
    } else if (type === 'rent_with_vat') {
      rentWithVatText =
        creditAccount?.accountName +
        ' (' +
        creditAccount?.taxCodeInfo?.taxPercentage +
        '%)'
    }
  }
  return [
    {
      $addFields: {
        areaType: {
          $cond: [
            { $ifNull: ['$leaseInfo', false] },
            {
              $cond: [
                { $eq: ['$leaseInfo.rentalMeta.isVatEnable', true] },
                'commercial',
                'residential'
              ]
            },
            null
          ]
        }
      }
    },
    {
      $addFields: {
        areaGroup: {
          $switch: {
            branches: [
              {
                case: { $eq: ['$areaType', 'commercial'] },
                then: rentWithVatText
              },
              {
                case: { $eq: ['$areaType', 'residential'] },
                then: rentText
              }
            ],
            default: null
          }
        }
      }
    }
  ]
}

const getCoTenantsPipelineForRentRollReport = () => [
  {
    $addFields: {
      coTenants: {
        $filter: {
          input: { $ifNull: ['$leaseInfo.rentalMeta.tenants', []] },
          as: 'tenant',
          cond: {
            $not: {
              $eq: ['$$tenant.tenantId', '$leaseInfo.rentalMeta.tenantId']
            }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'coTenants.tenantId',
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
                  email: 1,
                  tlf: '$profile.phoneNumber'
                }
              }
            ],
            as: 'tenantUser'
          }
        },
        appHelper.getUnwindPipeline('tenantUser'),
        {
          $project: {
            _id: 1,
            name: 1,
            email: '$tenantUser.email',
            tlf: '$tenantUser.tlf'
          }
        }
      ],
      as: 'coTenantsInfo'
    }
  }
]

const getAgentPipelineForRentRollReport = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agentId',
      foreignField: '_id',
      as: 'agentInfo'
    }
  },
  appHelper.getUnwindPipeline('agentInfo')
]

const getWaultYearsPipelineForRentRollReport = async (partnerSetting) => {
  const timeZone = partnerSetting.dateTimeSettings?.timezone || 'Europe/Oslo'
  const currentDate = await appHelper.getActualDate(
    partnerSetting,
    false,
    new Date()
  )
  return [
    {
      $addFields: {
        dayDifference: {
          $dateDiff: {
            startDate: currentDate,
            endDate: '$leaseInfo.rentalMeta.contractEndDate',
            unit: 'day',
            timezone: timeZone
          }
        }
      }
    },
    {
      $addFields: {
        waultYears: {
          $cond: [
            { $lte: ['$dayDifference', 0] },
            0,
            {
              $round: [
                {
                  $divide: ['$dayDifference', 365]
                },
                2
              ]
            }
          ]
        }
      }
    }
  ]
}

const getMainTenantPipelineForRentRollReport = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'leaseInfo.rentalMeta.tenantId',
      foreignField: '_id',
      as: 'mainTenant'
    }
  },
  appHelper.getUnwindPipeline('mainTenant'),
  {
    $lookup: {
      from: 'users',
      localField: 'mainTenant.userId',
      foreignField: '_id',
      pipeline: [...appHelper.getUserEmailPipeline()],
      as: 'mainTenantUser'
    }
  },
  appHelper.getUnwindPipeline('mainTenantUser')
]

const getAddonInfoPipelineForRentRollReport = () => [
  appHelper.getUnwindPipeline('addons'),
  {
    $lookup: {
      from: 'products_services',
      localField: 'addons.addonId',
      foreignField: '_id',
      as: 'addonInfo'
    }
  },
  appHelper.getUnwindPipeline('addonInfo'),
  {
    $group: {
      _id: '$_id',
      addons: {
        $push: {
          $cond: [
            { $ifNull: ['$addonInfo', false] },
            {
              $concat: [
                ', ',
                '$addonInfo.name',
                ': ',
                { $toString: '$addons.price' }
              ]
            },
            '$$REMOVE'
          ]
        }
      },
      accountId: {
        $first: '$accountId'
      },
      propertyName: {
        $first: '$location.name'
      },
      location: {
        $first: '$location'
      },
      monthlyRentAmount: {
        $first: '$monthlyRentAmount'
      },
      placeSize: {
        $first: '$placeSize'
      },
      apartmentId: {
        $first: '$apartmentId'
      },
      noOfAvailableBedrooms: {
        $first: '$noOfAvailableBedrooms'
      },
      floor: {
        $first: '$floor'
      },
      depositAmount: {
        $first: '$depositAmount'
      },
      branchId: {
        $first: '$branchId'
      },
      groupId: {
        $first: '$groupId'
      },
      agentId: {
        $first: '$agentId'
      },
      hasActiveLease: {
        $first: '$hasActiveLease'
      },
      hasUpcomingLease: {
        $first: '$hasUpcomingLease'
      },
      hasInProgressLease: {
        $first: '$hasInProgressLease'
      }
    }
  },
  {
    $addFields: {
      addons: {
        $reduce: {
          input: '$addons',
          initialValue: '',
          in: { $concat: ['$$value', '$$this'] }
        }
      }
    }
  }
]

const getLeaseInfoPipelineForRentRollReport = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: '_id',
      foreignField: 'propertyId',
      pipeline: [
        {
          $match: {
            hasRentalContract: true
          }
        },
        {
          $sort: {
            'rentalMeta.contractStartDate': -1
          }
        },
        {
          $limit: 1
        }
      ],
      as: 'leaseInfo'
    }
  },
  appHelper.getUnwindPipeline('leaseInfo')
]

const getAccountPipelineForRentRollReport = () => [
  {
    $lookup: {
      from: 'accounts',
      localField: 'accountId',
      foreignField: '_id',
      as: 'accountInfo'
    }
  },
  appHelper.getUnwindPipeline('accountInfo')
]

const getPropertyDefaultInventoryIssues = async (query) => {
  const { issues = true, propertyId } = query
  const filterCond = {}
  if (issues) {
    filterCond.$eq = ['$$propertyItem.status', 'issues']
  } else {
    filterCond.$ne = ['$$propertyItem.status', 'issues']
  }
  const pipeline = [
    {
      $match: {
        propertyId,
        contractId: {
          $exists: false
        }
      }
    },
    {
      $addFields: {
        propertyItemIssues: {
          $filter: {
            input: { $ifNull: ['$inventory.furniture', []] },
            as: 'propertyItem',
            cond: filterCond
          }
        }
      }
    },
    {
      $match: {
        'propertyItemIssues.0': {
          $exists: true
        }
      }
    },
    {
      $project: {
        _id: 1,
        propertyItemIssues: 1,
        files: '$inventory.files'
      }
    }
  ]
  const [inventoryItemIssues = {}] =
    (await PropertyItemCollection.aggregate(pipeline)) || []
  return inventoryItemIssues
}

const getPropertyDefaultRoomIssues = async (query) => {
  const { issues = true, propertyId } = query
  const filterCond = {}
  const matchArr = []
  if (issues) {
    filterCond.$eq = ['$$roomItem.status', 'issues']
    matchArr.push({
      $match: {
        'issues.0': { $exists: true }
      }
    })
  } else {
    filterCond.$ne = ['$$roomItem.status', 'issues']
  }
  const pipeline = [
    {
      $match: {
        propertyId,
        contractId: {
          $exists: false
        }
      }
    },
    {
      $addFields: {
        issues: {
          $filter: {
            input: { $ifNull: ['$items', []] },
            as: 'roomItem',
            cond: filterCond
          }
        }
      }
    },
    ...matchArr,
    {
      $unwind: {
        path: '$issues',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'property_room_items',
        localField: 'issues.id',
        foreignField: '_id',
        as: 'propertyRoomItem'
      }
    },
    {
      $unwind: {
        path: '$propertyRoomItem',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        issues: {
          id: '$issues.id',
          status: '$issues.status',
          title: '$issues.title',
          description: '$issues.description',
          responsibleForFixing: '$issues.responsibleForFixing',
          taskId: '$issues.taskId',
          name: '$propertyRoomItem.name'
        }
      }
    },
    {
      $group: {
        _id: '$_id',
        name: {
          $first: '$name'
        },
        type: {
          $first: '$type'
        },
        files: {
          $first: '$files'
        },
        issues: {
          $push: {
            $cond: [
              {
                $ifNull: ['$issues.id', false]
              },
              '$issues',
              '$$REMOVE'
            ]
          }
        }
      }
    },
    {
      $project: {
        _id: 1,
        name: 1,
        type: 1,
        files: 1,
        issues: 1
      }
    }
  ]
  const propertyRoomIssues = await PropertyRoomCollection.aggregate(pipeline)
  return propertyRoomIssues
}

export const getAllIssuesForPartnerPublicSite = async (req) => {
  const { body = {}, user = {} } = req
  const { query } = body
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['propertyId'], query)

  const inventoryItems = await getPropertyDefaultInventoryIssues(query)
  const roomItems = await getPropertyDefaultRoomIssues(query)

  return {
    inventory: inventoryItems,
    rooms: roomItems
  }
}

const getPropertyRoomsIssuePipeline = () => [
  {
    $lookup: {
      from: 'property_rooms',
      localField: '_id',
      foreignField: 'propertyId',
      pipeline: [
        {
          $match: {
            contractId: {
              $exists: false
            }
          }
        },
        { $unwind: { path: '$items', preserveNullAndEmptyArrays: true } },
        { $match: { 'items.status': 'issues' } },
        {
          $lookup: {
            from: 'property_room_items',
            localField: 'items.id',
            foreignField: '_id',
            as: 'propertyRoomItem'
          }
        },
        {
          $unwind: {
            path: '$propertyRoomItem',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            roomId: '$_id',
            name: '$name',
            title: '$items.title',
            description: '$items.description',
            status: '$items.status',
            responsibleForFixing: '$items.responsibleForFixing',
            type: '$type',
            itemId: '$items.id',
            propertyId: '$propertyId',
            taskId: '$items.taskId',
            dueDate: '$items.dueDate',
            issueType: 'room',
            roomItemName: '$propertyRoomItem.name'
          }
        }
      ],
      as: 'propertyRoomIssueItems'
    }
  }
]

const getInventoryItemIssuePipeline = () => [
  {
    $lookup: {
      from: 'property_items',
      localField: '_id',
      foreignField: 'propertyId',
      pipeline: [
        {
          $match: {
            contractId: {
              $exists: false
            }
          }
        },
        {
          $unwind: {
            path: '$inventory.furniture',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            item: '$inventory.furniture'
          }
        },
        { $match: { 'item.status': 'issues' } },
        {
          $project: {
            propertyItemId: '$_id',
            name: '$item.name',
            title: '$item.title',
            status: '$item.status',
            description: '$item.description',
            quantity: '$item.quantity',
            responsibleForFixing: '$item.responsibleForFixing',
            propertyId: '$propertyId',
            itemId: '$item.id',
            taskId: '$item.taskId',
            dueDate: '$item.dueDate',
            issueType: 'inventory'
          }
        }
      ],
      as: 'inventoryIssueItems'
    }
  }
]

const getIssuesForPartnerPublic = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const result = await ListingCollection.aggregate([
    {
      $match: {
        _id: query.propertyId
      }
    },
    ...getPropertyRoomsIssuePipeline(),
    ...getInventoryItemIssuePipeline(),
    {
      $project: {
        item: {
          $concatArrays: ['$propertyRoomIssueItems', '$inventoryIssueItems']
        }
      }
    },
    { $unwind: '$item' },
    {
      $project: {
        _id: '$item._id',
        roomId: '$item.roomId',
        propertyItemId: '$item.propertyItemId',
        itemId: '$item.itemId',
        name: '$item.name',
        title: '$item.title',
        description: '$item.description',
        roomItemName: '$item.roomItemName',
        quantity: '$item.quantity',
        status: '$item.status',
        responsibleForFixing: '$item.responsibleForFixing',
        type: '$item.type',
        issueType: '$item.issueType',
        propertyId: '$item.propertyId',
        taskId: '$item.taskId',
        dueDate: '$item.dueDate'
      }
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ])
  return result
}

export const getPropertyIssuesForPartnerPublic = async (req) => {
  const { body = {}, user = {} } = req
  const { query, options } = body
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['propertyId'], query)
  appHelper.validateSortForQuery(options.sort)

  const issues = await getIssuesForPartnerPublic(body)
  return {
    data: issues
  }
}
