import { size } from 'lodash'
import {
  BranchCollection,
  ListingCollection,
  PartnerUsageCollection
} from '../models'
import {
  appHelper,
  appRoleHelper,
  branchHelper,
  dashboardHelper,
  listingHelper,
  userHelper
} from '../helpers'

export const getPartnerUsages = async (query, session) => {
  const partnerUsages = await PartnerUsageCollection.find(query).session(
    session
  )
  return partnerUsages
}

export const prepareInsertData = (body, user) => {
  const insertData = body
  const { userId } = user
  if (userId) {
    insertData.createdBy = userId
  }
  return insertData
}

const validateQueryData = (body) => {
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId, createdDateRange } = body
  appHelper.validateId({ partnerId })
  appHelper.validateCreatedAtForQuery(createdDateRange)
}

const prepareQueryData = (query) => {
  const { branchId, createdDateRange, partnerId, type } = query
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
  }
  const { startDate, endDate } = createdDateRange || {}
  const match = { partnerId }
  if (size(branchId)) match.branchId = { $in: branchId }
  if (startDate && endDate) {
    match.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  if (type) match.type = type
  return match
}

const getBranchFilterPipeline = (branches) => {
  if (size(branches) > 1) {
    return [
      {
        $group: {
          _id: {
            type: '$type',
            branchId: '$branchId'
          },
          countedTotal: { $max: '$total' },
          type: { $first: '$type' }
        }
      },
      {
        $group: {
          _id: '$_id.type',
          countedTotal: {
            $sum: '$countedTotal'
          },
          type: {
            $first: '$type'
          }
        }
      }
    ]
  } else {
    return [
      {
        $group: {
          _id: '$type',
          countedTotal: {
            $max: '$total'
          },
          type: {
            $first: '$type'
          }
        }
      }
    ]
  }
}

const getPartnerUsageCountForQuery = async (body) => {
  validateQueryData(body)
  const match = prepareQueryData(body)
  const active_group = [
    'active_properties',
    'active_users',
    'active_agents',
    'active_agents_with_active_properties',
    'parking_lots'
  ]
  const { branchId } = body

  const partnerUsages = await PartnerUsageCollection.aggregate([
    {
      $match: match
    },
    {
      $facet: {
        active_category: [
          {
            $match: { type: { $in: active_group } }
          },
          ...getBranchFilterPipeline(branchId)
        ],
        non_active_category: [
          {
            $match: { type: { $nin: active_group } }
          },
          {
            $group: {
              _id: '$type',
              countedTotal: {
                $sum: {
                  $cond: [
                    { $eq: ['$type', 'outgoing_sms'] },
                    '$totalMessages',
                    1
                  ]
                }
              },
              type: { $first: '$type' }
            }
          }
        ]
      }
    }
  ])
  return partnerUsages
}

export const queryPartnerUsages = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  const partnerUsagesdata = await getPartnerUsageCountForQuery(body)
  const [{ active_category, non_active_category }] = partnerUsagesdata
  const partnerUsages = [...active_category, ...non_active_category]
  return { data: partnerUsages, metaData: {} }
}

export const getActivePropertyInfoForDashboard = async (
  query = {},
  partnerType = '',
  dateRange = ''
) => {
  const pipeline = []
  const match = {
    $match: {
      ...query,
      type: 'active_properties',
      branchId: { $exists: false }
    }
  }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: {
      _id: {
        $dateToString: { date: '$createdAt', format: '%Y-%m-%d' }
      },
      total: { $sum: '$total' },
      createdAt: { $first: '$createdAt' }
    }
  }
  pipeline.push(group)
  pipeline.push({
    $sort: { createdAt: -1 }
  })
  const secondGroup = {
    $group: {
      _id: {
        $dateToString: {
          date: '$createdAt',
          format: appHelper.getDateFormatString(dateRange)
        }
      },
      countedTotal: { $first: '$total' }
    }
  }
  pipeline.push(secondGroup)
  const project = {
    $project: {
      _id: 0,
      date: '$_id',
      countedTotal: { $toInt: '$countedTotal' }
    }
  }
  pipeline.push(project)
  pipeline.push({ $sort: { date: 1 } })
  const finalGroup = {
    $group: {
      _id: null,
      countedProperties: { $avg: '$countedTotal' },
      activePropertyGraphData: {
        $push: {
          date: '$date',
          countedTotal: '$countedTotal'
        }
      }
    }
  }
  pipeline.push(finalGroup)
  const [activePropertyInfo] = await PartnerUsageCollection.aggregate(pipeline)
  return activePropertyInfo
}

// Calculate average for active_agents and active_properties
// Calculate summation for all other types of partner usage
export const getPartnerUsageInfoForDashboard = async (
  query = {},
  partnerType = ''
) => {
  const types = [
    'credit_rating',
    'deposit_account',
    'esign',
    'finn',
    'outgoing_sms',
    'vipps_invoice',
    'compello_invoice'
  ]
  const pipeline = []
  const match = {
    $match: {
      ...query,
      $or: [
        { type: { $in: types } },
        { type: 'active_agents', branchId: { $exists: false } }
      ]
    }
  }
  pipeline.push(match)
  const lookup = {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  }
  pipeline.push(lookup)
  const facetQuery = {}
  if (partnerType !== 'all') {
    facetQuery['partner.accountType'] = partnerType
  }
  const facet = {
    $facet: {
      agent: [
        {
          $match: {
            ...facetQuery,
            type: 'active_agents',
            branchId: { $exists: false }
          }
        },
        {
          $group: {
            _id: { $dateToString: { date: '$createdAt', format: '%Y-%m-%d' } },
            total: { $sum: '$total' }
          }
        },
        {
          $group: {
            _id: null,
            countedActiveAgents: { $avg: '$total' }
          }
        }
      ],
      other: [
        { $match: { ...facetQuery, type: { $in: types } } },
        {
          $group: {
            _id: null,
            countedEsigns: {
              $sum: { $cond: [{ $eq: ['$type', 'esign'] }, '$total', 0] }
            },
            countedSms: {
              $sum: {
                $cond: [{ $eq: ['$type', 'outgoing_sms'] }, '$totalMessages', 0]
              }
            },
            countedVipps: {
              $sum: {
                $cond: [{ $eq: ['$type', 'vipps_invoice'] }, '$total', 0]
              }
            },
            countedDeposits: {
              $sum: {
                $cond: [{ $eq: ['$type', 'deposit_account'] }, '$total', 0]
              }
            },
            countedFinns: {
              $sum: { $cond: [{ $eq: ['$type', 'finn'] }, '$total', 0] }
            },
            countedCreditRatings: {
              $sum: {
                $cond: [{ $eq: ['$type', 'credit_rating'] }, '$total', 0]
              }
            },
            countedCompello: {
              $sum: {
                $cond: [{ $eq: ['$type', 'compello_invoice'] }, '$total', 0]
              }
            }
          }
        }
      ]
    }
  }
  pipeline.push(facet)
  const [partnerUsageInfo] = await PartnerUsageCollection.aggregate(pipeline)
  return partnerUsageInfo
}

// Calculate average for active_agents and active_properties
// Calculate summation for all other types of partner usage
export const getPartnerUsageGraphDataForDashboard = async (
  query = {},
  dateRange = '',
  partnerType = ''
) => {
  const pipeline = []
  if (query.type === 'active_agents') {
    query.branchId = { $exists: false }
  }
  const match = { $match: { ...query } }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: {
      _id: {
        $dateToString: { date: '$createdAt', format: '%Y-%m-%d' }
      },
      total: { $sum: '$total' },
      createdAt: { $first: '$createdAt' }
    }
  }
  if (query.type === 'active_agents') pipeline.push(group)
  const finalGroup = {
    $group: {
      _id: {
        $dateToString: {
          date: '$createdAt',
          format: appHelper.getDateFormatString(dateRange)
        }
      },
      countedTotal:
        query.type === 'active_agents'
          ? { $avg: '$total' }
          : query.type === 'outgoing_sms'
          ? { $sum: '$totalMessages' }
          : { $sum: '$total' }
    }
  }
  pipeline.push(finalGroup)
  const project = {
    $project: {
      _id: 0,
      date: '$_id',
      countedTotal: { $toInt: '$countedTotal' }
    }
  }
  pipeline.push(project)
  pipeline.push({ $sort: { date: 1 } })
  const partnerUsageGraphData = await PartnerUsageCollection.aggregate(pipeline)
  return partnerUsageGraphData
}

export const getUPAGraphDataForDashboard = async (
  query = {},
  dateRange = '',
  partnerType = ''
) => {
  const pipeline = []
  const match = {
    $match: {
      ...query,
      branchId: { $exists: false },
      type: {
        $in: ['active_agents_with_active_properties', 'active_properties']
      }
    }
  }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: {
      _id: {
        $dateToString: { date: '$createdAt', format: '%Y-%m-%d' }
      },
      countedAgents: {
        $sum: {
          $cond: {
            if: { $eq: ['$type', 'active_agents_with_active_properties'] },
            then: '$total',
            else: 0
          }
        }
      },
      countedProperties: {
        $sum: {
          $cond: {
            if: { $eq: ['$type', 'active_properties'] },
            then: '$total',
            else: 0
          }
        }
      },
      createdAt: { $first: '$createdAt' }
    }
  }
  pipeline.push(group)
  const secondGroup = {
    $group: {
      _id: {
        $dateToString: {
          date: '$createdAt',
          format: appHelper.getDateFormatString(dateRange)
        }
      },
      countedAgents: { $avg: '$countedAgents' },
      countedProperties: { $avg: '$countedProperties' }
    }
  }
  pipeline.push(secondGroup)
  const project = {
    $project: {
      date: '$_id',
      upa: {
        $divide: [
          '$countedProperties',
          {
            $cond: {
              if: { $eq: ['$countedAgents', 0] },
              then: 1,
              else: '$countedAgents'
            }
          }
        ]
      }
    }
  }
  pipeline.push(project)
  pipeline.push({ $sort: { date: 1 } })
  const upaGraphData = await PartnerUsageCollection.aggregate(pipeline)
  return upaGraphData
}

export const getDashboardUPAForPartner = async (params = {}) => {
  const { query, limit, order, partnerType } = params
  const pipeline = []
  const match = {
    $match: {
      ...query,
      branchId: { $exists: false },
      type: {
        $in: ['active_agents_with_active_properties', 'active_properties']
      }
    }
  }
  pipeline.push(match)
  const lookup = {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  }
  pipeline.push(lookup)
  const unwind = { $unwind: { path: '$partner' } }
  pipeline.push(unwind)
  const finalMatch = {
    $match: {
      'partner.accountType': partnerType
    }
  }
  if (partnerType !== 'all') pipeline.push(finalMatch)
  const project = {
    $project: {
      activeAgents: {
        $cond: {
          if: { $eq: ['$type', 'active_agents_with_active_properties'] },
          then: '$total',
          else: 0
        }
      },
      activeProperties: {
        $cond: {
          if: { $eq: ['$type', 'active_properties'] },
          then: '$total',
          else: 0
        }
      },
      type: 1,
      agents: {
        $cond: {
          if: { $eq: ['$type', 'active_agents_with_active_properties'] },
          then: 1,
          else: 0
        }
      },
      properties: {
        $cond: { if: { $eq: ['$type', 'active_properties'] }, then: 1, else: 0 }
      },
      partnerId: 1,
      partnerName: '$partner.name',
      partnersSubDomain: '$partner.subDomain'
    }
  }
  pipeline.push(project)
  const group = {
    $group: {
      _id: '$partnerId',
      activeAgentsCount: { $sum: '$activeAgents' },
      activePropertiesCount: { $sum: '$activeProperties' },
      agentsCount: { $sum: '$agents' },
      propertiesCount: { $sum: '$properties' },
      partnerName: { $first: '$partnerName' },
      partnersSubDomain: { $first: '$partnersSubDomain' }
    }
  }
  pipeline.push(group)
  const projectForCalculation = [
    {
      $addFields: {
        agentsAvg: {
          $divide: [
            '$activeAgentsCount',
            { $cond: [{ $eq: ['$agentsCount', 0] }, 1, '$agentsCount'] }
          ]
        },
        propertiesAvg: {
          $divide: [
            '$activePropertiesCount',
            { $cond: [{ $eq: ['$propertiesCount', 0] }, 1, '$propertiesCount'] }
          ]
        }
      }
    },
    {
      $project: {
        agentsAvg: 1,
        propertiesAvg: 1,
        upa: {
          $divide: [
            '$propertiesAvg',
            {
              $cond: [{ $eq: ['$agentsAvg', 0] }, 1, '$agentsAvg']
            }
          ]
        },
        partnerName: 1,
        partnersSubDomain: 1
      }
    }
  ]
  pipeline.push(...projectForCalculation)
  const facet = {
    $facet: {
      upaForPartners: [
        {
          $project: {
            countedAgents: { $toInt: '$agentsAvg' },
            countedProperties: { $toInt: '$propertiesAvg' },
            upa: 1,
            partnerName: 1,
            partnersSubDomain: 1
          }
        },
        {
          $sort: { upa: order === 'lowToHigh' ? 1 : -1 }
        },
        { $limit: limit || 10 }
      ],
      finalSummary: [
        {
          $group: {
            _id: null,
            countedTotalAgents: {
              $sum: '$agentsAvg'
            },
            countedTotalProperties: {
              $sum: '$propertiesAvg'
            },
            countedTotalPartners: {
              $sum: 1
            },
            totalUpaOfPartners: {
              $sum: '$upa'
            }
          }
        },
        {
          $project: {
            countedTotalAgents: { $toInt: '$countedTotalAgents' },
            countedTotalProperties: { $toInt: '$countedTotalProperties' },
            countedTotalPartners: 1,
            totalUpaOfPartners: 1
          }
        },
        {
          $addFields: {
            totalUpaOfPartners: {
              $divide: [
                '$totalUpaOfPartners',
                {
                  $cond: [
                    { $eq: ['$countedTotalPartners', 0] },
                    1,
                    '$countedTotalPartners'
                  ]
                }
              ]
            }
          }
        }
      ]
    }
  }
  pipeline.push(facet)
  const [upaInfo] = await PartnerUsageCollection.aggregate(pipeline)
  return upaInfo
}

export const getUniquePartnerUsageTypes = async (query) => {
  const types = await PartnerUsageCollection.distinct('type', query)
  return types || []
}

export const queryPartnerUsageTypes = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  return {
    data: await getUniquePartnerUsageTypes({ partnerId })
  }
}

export const preparePartnerUsagesDataForTotalActiveParkingLots = async (
  partnerId,
  parkingId
) => {
  const properties = await ListingCollection.aggregate([
    {
      $match: {
        partnerId,
        listingTypeId: parkingId,
        hasActiveLease: true
      }
    },
    {
      $group: {
        _id: '$branchId',
        total: { $sum: 1 }
      }
    }
  ])
  const partnerUsageData = []
  if (size(properties)) {
    let totalParkingProperties = 0
    for (const property of properties) {
      partnerUsageData.push({
        branchId: property._id,
        type: 'parking_lots',
        partnerId,
        total: property.total
      })
      totalParkingProperties += property.total || 0
    }
    partnerUsageData.push({
      type: 'parking_lots',
      partnerId,
      total: totalParkingProperties
    })
  }
  return partnerUsageData
}

export const preparePartnerUsagesDataForTotalActiveProperties = async (
  partnerId,
  parkingId
) => {
  const activeProperties = await ListingCollection.aggregate([
    {
      $match: {
        partnerId,
        listingTypeId: { $ne: parkingId },
        hasActiveLease: true
      }
    },
    {
      $group: {
        _id: '$branchId',
        total: { $sum: 1 }
      }
    }
  ])

  const partnerUsageData = []
  if (size(activeProperties)) {
    let totalActiveProperties = 0

    for (const property of activeProperties) {
      partnerUsageData.push({
        branchId: property._id,
        type: 'active_properties',
        partnerId,
        total: property.total
      })
      totalActiveProperties += property.total || 0
    }

    partnerUsageData.push({
      type: 'active_properties',
      partnerId,
      total: totalActiveProperties
    })
  }
  return partnerUsageData
}

export const preparePartnerUsagesDataForTotalActiveUsers = async (
  partnerId
) => {
  const activeUsers = await userHelper.getUsers({
    partners: {
      $elemMatch: {
        partnerId,
        type: 'user',
        status: 'active'
      }
    }
  })

  return [
    {
      partnerId,
      type: 'active_users',
      total: activeUsers?.length || 0
    }
  ]
}

export const preparePartnerUsagesDataForTotalActiveAgents = async (
  partnerId,
  parkingId
) => {
  const agentRole = await appRoleHelper.getAppRole({
    partnerId,
    type: 'partner_agent'
  })
  const totalRoleAgents = agentRole?.users ? size(agentRole.users) : 0
  const partnerUsageData = []
  // Add total active agents in 'partner_usages' without branchId
  partnerUsageData.push({
    partnerId,
    type: 'active_agents',
    total: totalRoleAgents
  })
  const branches = await branchHelper.getBranches({
    partnerId
  })
  if (size(branches)) {
    for (const branch of branches) {
      partnerUsageData.push({
        partnerId,
        type: 'active_agents',
        branchId: branch._id,
        total: branch?.agents?.length || 0
      })
    }
  }
  // For active agents with active properties
  const activeAgentsWithActiveProperties =
    await listingHelper.getUniqueFieldValueOfListings('agentId', {
      partnerId,
      listingTypeId: { $ne: parkingId },
      hasActiveLease: true,
      agentId: {
        $in: agentRole?.users || []
      }
    })
  partnerUsageData.push({
    partnerId,
    type: 'active_agents_with_active_properties',
    total: activeAgentsWithActiveProperties.length
  })
  const branchWiseActiveAgentsWithActiveProperties =
    await BranchCollection.aggregate([
      {
        $match: {
          partnerId
        }
      },
      {
        $unwind: '$agents'
      },
      ...getPipelineForListingForPartnerUsage(partnerId, parkingId),
      {
        $group: {
          _id: '$_id',
          total: { $sum: 1 }
        }
      }
    ])
  if (size(branchWiseActiveAgentsWithActiveProperties)) {
    for (const branch of branchWiseActiveAgentsWithActiveProperties) {
      partnerUsageData.push({
        partnerId,
        type: 'active_agents_with_active_properties',
        branchId: branch._id,
        total: branch.total || 0
      })
    }
  }
  return partnerUsageData
}

const getPipelineForListingForPartnerUsage = (partnerId, parkingId) => [
  {
    $lookup: {
      from: 'listings',
      localField: 'agents',
      foreignField: 'agentId',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $not: { $eq: ['$listingTypeId', parkingId] } },
                { $eq: ['$hasActiveLease', true] },
                { $eq: ['$partnerId', partnerId] }
              ]
            }
          }
        },
        {
          $limit: 1
        }
      ],
      as: 'property'
    }
  },
  {
    $unwind: '$property'
  }
]
