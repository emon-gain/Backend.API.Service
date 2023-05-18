import _, { omit, size } from 'lodash'

import { AnnualStatementCollection, TransactionCollection } from '../models'
import {
  appHelper,
  partnerHelper,
  partnerSettingHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import { CustomError } from '../common'

export const annualStatementAggregate = async (pipeline = []) =>
  await AnnualStatementCollection.aggregate(pipeline)

export const getSpecificFiledDataForAnnualStatement = async (
  fieldName = '',
  query = {}
) => {
  if (!size(fieldName)) {
    throw new CustomError(400, 'Field name must not be empty')
  }
  const result = await AnnualStatementCollection.distinct(fieldName, query)
  return result
}

export const getAnnualStatement = async (query, session) => {
  const annualStatement = await AnnualStatementCollection.findOne(
    query
  ).session(session)
  return annualStatement
}

export const getAnnualStatementWithSort = async (
  query,
  sort = { createdAt: -1 },
  session
) => {
  const annualStatement = await AnnualStatementCollection.findOne(query)
    .sort(sort)
    .session(session)
  return annualStatement
}

export const getAnnualStatements = async (query, session) => {
  const annualStatements = await AnnualStatementCollection.find(query).session(
    session
  )
  return annualStatements
}

const getPropertyPipelineForAnnualStatement = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'property'
    }
  },
  appHelper.getUnwindPipeline('property')
]

const getContractPipelineForAnnualStatement = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'contract'
    }
  },
  appHelper.getUnwindPipeline('contract')
]

const getFilePipelineForAnnualStatement = () => [
  {
    $lookup: {
      from: 'files',
      localField: 'fileId',
      foreignField: '_id',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$status', 'processed']
            }
          }
        }
      ],
      as: 'file'
    }
  },
  appHelper.getUnwindPipeline('file')
]

const getAnnualStatementsForQuery = async (query, options) => {
  const { sort, skip, limit } = options
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
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...getPropertyPipelineForAnnualStatement(),
    ...getContractPipelineForAnnualStatement(),
    ...getFilePipelineForAnnualStatement(),
    ...appHelper.getListingFirstImageUrl('$property.images', 'property'),
    {
      $project: {
        _id: 1,
        account: '$accountInfo',
        tenant: '$tenantInfo',
        agent: '$agentInfo',
        property: {
          _id: 1,
          imageUrl: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          },
          listingTypeId: 1,
          propertyTypeId: 1,
          apartmentId: 1,
          serial: 1
        },
        statementYear: 1,
        contract: {
          _id: 1,
          leaseSerial: 1
        },
        file: {
          _id: 1,
          name: 1,
          title: 1
        },
        rentTotalExclTax: 1,
        rentTotal: 1,
        landlordTotalExclTax: 1,
        landlordTotal: 1,
        createdAt: 1
      }
    }
  ]
  const annualStatements =
    (await AnnualStatementCollection.aggregate(pipeline)) || []
  return annualStatements
}

const countAnnualStatements = async (query) => {
  const numOfAnnualStatements = await AnnualStatementCollection.countDocuments(
    query
  )
  return numOfAnnualStatements
}

const prepareQueryForAnnualStatementQuery = (query) => {
  const {
    accountId,
    branchId,
    contractId,
    createdAtDateRange,
    propertyId,
    statementYear
  } = query
  query.status = 'completed'
  if (size(createdAtDateRange)) {
    appHelper.validateCreatedAtForQuery(createdAtDateRange)
    const { startDate, endDate } = createdAtDateRange
    query.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  if (size(statementYear)) {
    query.statementYear = {
      $in: statementYear
    }
  }
  if (accountId) appHelper.validateId({ accountId })
  if (branchId) appHelper.validateId({ branchId })
  if (contractId) appHelper.validateId({ contractId })
  if (propertyId) appHelper.validateId({ propertyId })

  const preparedQuery = omit(query, ['createdAtDateRange', 'requestFrom'])
  return preparedQuery
}

export const queryAnnualStatements = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = {
    status: 'completed',
    partnerId
  }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  query.partnerId = partnerId
  const preparedQuery = prepareQueryForAnnualStatementQuery(query)
  const annualStatements = await getAnnualStatementsForQuery(
    preparedQuery,
    options
  )
  const totalDocuments = await countAnnualStatements(totalDocumentsQuery)
  const filteredDocuments = await countAnnualStatements(preparedQuery)
  return {
    data: annualStatements,
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

export const queryAnnualStatementForXmlCreator = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query, options } = body
  appHelper.compactObject(query)
  appHelper.checkRequiredFields(['partnerId', 'statementYear'], query)
  const { partnerId, statementYear } = query
  appHelper.validateId({ partnerId })
  const { limit, skip } = options
  const pipeline = [
    { $match: { partnerId, statementYear } },
    {
      $group: {
        _id: '$accountId',
        propertyIds: { $push: '$propertyId' },
        contractIds: { $push: '$contractId' },
        sumOfRentTotalExclTax: { $sum: '$rentTotalExclTax' },
        sumOfLandlordTotalExclTax: { $sum: '$landlordTotalExclTax' },
        annualStatements: { $push: '$$ROOT' }
      }
    },
    { $sort: { _id: 1 } },
    { $skip: skip },
    { $limit: limit },
    {
      $lookup: {
        from: 'accounts',
        let: {
          accountId: '$_id'
        },
        pipeline: [
          {
            $match: {
              $expr: { $eq: ['$_id', '$$accountId'] }
            }
          },
          {
            $lookup: {
              from: 'users',
              localField: 'personId',
              foreignField: '_id',
              as: 'person'
            }
          },
          {
            $unwind: {
              path: '$person',
              preserveNullAndEmptyArrays: true
            }
          },
          {
            $lookup: {
              from: 'organizations',
              localField: 'organizationId',
              foreignField: '_id',
              as: 'organization'
            }
          },
          {
            $unwind: {
              path: '$organization',
              preserveNullAndEmptyArrays: true
            }
          }
        ],
        as: 'account'
      }
    },
    { $unwind: { path: '$account' } },
    {
      $lookup: {
        from: 'listings',
        let: {
          contractIds: '$contractIds',
          propertyIds: '$propertyIds',
          accountId: '$account._id'
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$accountId', '$$accountId'] },
                  { $in: ['$_id', '$$propertyIds'] }
                ]
              }
            }
          },
          { $sort: { createdAt: 1 } },
          {
            $lookup: {
              from: 'contracts',
              let: {
                contractIds: '$$contractIds',
                accountId: '$$accountId',
                propertyId: '$_id'
              },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $eq: ['$propertyId', '$$propertyId'] },
                        { $eq: ['$accountId', '$$accountId'] },
                        { $in: ['$_id', '$$contractIds'] }
                      ]
                    }
                  }
                },
                { $sort: { createdAt: 1 } }
              ],
              as: 'contracts'
            }
          }
        ],
        as: 'properties'
      }
    }
  ]
  const annualStatements = await AnnualStatementCollection.aggregate(pipeline)
  return annualStatements
}

export const queryForAnnualStatementData = async (
  contractId,
  statementYear
) => {
  const pipeline = pipelineForQueryOfAnnualStatement(contractId, statementYear)
  const annualStatementData = await TransactionCollection.aggregate(pipeline)
  return annualStatementData[0]
}

const pipelineForQueryOfAnnualStatement = (contractId, statementYear) => {
  const nonSubType = [
    'administration_eviction_notice_fee',
    'administration_eviction_notice_fee_move_to',
    'collection_notice_fee',
    'collection_notice_fee_move_to',
    'eviction_notice_fee',
    'eviction_notice_fee_move_to',
    'invoice_fee',
    'invoice_reminder_fee',
    'loss_recognition',
    'reminder_fee_move_to',
    'rounded_amount',
    'unpaid_administration_eviction_notice',
    'unpaid_collection_notice',
    'unpaid_eviction_notice',
    'unpaid_reminder'
  ]
  const rentInvoiceCond = {
    $or: [
      {
        $and: [{ $in: ['$type', ['invoice', 'credit_note']] }]
      },
      {
        $and: [{ $eq: ['$type', 'correction'] }, { $eq: ['$subType', 'addon'] }]
      }
    ]
  }
  const landlordInvoice = {
    $or: [
      { $eq: ['$type', 'commission'] },
      {
        $and: [
          { $eq: ['$type', 'correction'] },
          { $eq: ['$subType', 'payout_addon'] }
        ]
      }
    ]
  }
  // statementYear = '/' + statementYear + '/'
  const pipeline = [
    {
      $match: {
        subType: {
          $nin: nonSubType
        },
        contractId,
        period: {
          $regex: statementYear,
          $options: 'i'
        }
      }
    },
    {
      $project: {
        type: 1,
        subType: 1,
        amountExclTax: 1,
        amountTotalTax: 1,
        creditTaxPercentage: 1,
        invoiceId: 1,
        amount: 1,
        partnerId: 1,
        contractId: 1,
        tenantId: 1,
        agentId: 1,
        branchId: 1,
        accountId: 1,
        propertyId: 1
      }
    },
    {
      $lookup: {
        from: 'contracts',
        foreignField: '_id',
        localField: 'contractId',
        as: 'contracts'
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        foreignField: 'partnerId',
        localField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $lookup: {
        from: 'accounts',
        foreignField: '_id',
        localField: 'accountId',
        as: 'accountInfo'
      }
    },
    {
      $unwind: {
        path: '$accountInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$partnerSettings',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'tenants',
        foreignField: '_id',
        localField: 'tenantId',
        as: 'tenantInfo'
      }
    },
    {
      $unwind: {
        path: '$tenantInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        foreignField: '_id',
        localField: 'accountInfo.personId',
        as: 'accountUserInfo'
      }
    },
    {
      $lookup: {
        from: 'users',
        foreignField: '_id',
        localField: 'tenantInfo.userId',
        as: 'tenantUserInfo'
      }
    },
    {
      $addFields: {
        creditTaxPercentage: {
          $ifNull: ['$creditTaxPercentage', 0]
        }
      }
    },
    {
      $addFields: {
        totalTax: {
          $divide: [
            { $multiply: ['$creditTaxPercentage', '$amount'] },
            { $add: [100, '$creditTaxPercentage'] }
          ]
        },
        excludedSubType: {
          $cond: {
            if: {
              $or: [
                { $ne: ['$subType', 'invoice_fee'] },
                { $ne: ['$subType', 'invoice_reminder_fee'] },
                { $ne: ['$subType', 'collection_notice_fee'] },
                { $ne: ['$subType', 'eviction_notice_fee'] },
                { $ne: ['$subType', 'administration_eviction_notice_fee'] },
                { $ne: ['$subType', 'reminder_fee_move_to'] },
                { $ne: ['$subType', 'collection_notice_fee_move_to'] },
                { $ne: ['$subType', 'eviction_notice_fee_move_to'] },
                {
                  $ne: [
                    '$subType',
                    'administration_eviction_notice_fee_move_to'
                  ]
                },
                { $ne: ['$subType', 'unpaid_reminder'] },
                { $ne: ['$subType', 'unpaid_collection_notice'] },
                { $ne: ['$subType', 'unpaid_eviction_notice'] },
                { $ne: ['$subType', 'unpaid_administration_eviction_notice'] },
                { $ne: ['$subType', 'loss_recognition'] },
                { $ne: ['$subType', 'rounded_amount'] }
              ]
            },
            then: true,
            else: false
          }
        }
      }
    },

    {
      $group: {
        _id: null,
        partnerId: { $first: '$partnerId' },
        tenantUserInfo: { $first: '$tenantUserInfo' },
        accountUserInfo: { $first: '$accountUserInfo' },
        contractId: { $first: '$contractId' },
        tenantId: { $first: '$tenantId' },
        agentId: { $first: '$agentId' },
        branchId: { $first: '$branchId' },
        accountId: { $first: '$accountId' },
        propertyId: { $first: '$propertyId' },
        partnerSettings: { $first: '$partnerSettings' },
        rentTotalExclTax: {
          $sum: {
            $cond: {
              if: rentInvoiceCond,
              then: {
                $ifNull: ['$amountExclTax', '$amount']
              },
              else: 0
            }
          }
        },
        rentTotalTax: {
          $sum: {
            $cond: {
              if: rentInvoiceCond,
              then: {
                $ifNull: ['$amountTotalTax', '$amount']
              },
              else: 0
            }
          }
        },
        rentTotal: {
          $sum: {
            $cond: {
              if: rentInvoiceCond,
              then: '$amount',
              else: 0
            }
          }
        },
        landlordTotal: {
          $sum: {
            $cond: {
              if: landlordInvoice,
              then: '$amount',
              else: 0
            }
          }
        },
        landlordTotalTax: {
          $sum: {
            $cond: {
              if: landlordInvoice,
              then: '$totalTax',
              else: 0
            }
          }
        },
        totalFinalSettlementPayments: {
          $sum: {
            $cond: {
              if: {
                $and: [
                  { $eq: ['$type', 'payment'] },
                  { $eq: ['$subType', 'final_settlement_payment'] }
                ]
              },
              then: '$amount',
              else: 0
            }
          }
        },
        totalPayouts: {
          $sum: {
            $cond: {
              if: {
                $eq: ['$type', 'payout']
              },
              then: '$amount',
              else: 0
            }
          }
        }
      }
    },
    {
      $addFields: {
        status: 'created',
        statementYear: parseInt(statementYear),
        landlordTotalExclTax: {
          $subtract: ['$landlordTotal', '$landlordTotalTax']
        },
        totalPayouts: {
          $subtract: [
            '$totalPayouts',
            { $ifNull: ['$totalFinalSettlementPayments', 0] }
          ]
        }
      }
    },
    {
      $project: {
        partnerId: 1,
        tenantUserInfo: 1,
        accountUserInfo: 1,
        contractId: 1,
        tenantId: 1,
        agentId: 1,
        branchId: 1,
        accountId: 1,
        propertyId: 1,
        partnerSettings: 1,
        rentTotalExclTax: {
          $round: [
            '$rentTotalExclTax',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        },
        rentTotalTax: {
          $round: [
            '$rentTotalTax',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        },
        rentTotal: {
          $round: [
            '$rentTotal',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        },
        landlordTotal: {
          $round: [
            '$landlordTotal',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        },
        landlordTotalTax: {
          $round: [
            '$landlordTotalTax',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        },
        totalPayouts: {
          $round: [
            '$totalPayouts',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        },
        status: 1,
        statementYear: 1,
        landlordTotalExclTax: {
          $round: [
            '$landlordTotalExclTax',
            {
              $ifNull: [
                '$partnerSettings.invoiceSettings.numberOfDecimalInInvoice',
                2
              ]
            }
          ]
        }
      }
    }
  ]

  return pipeline
}

export const queryPartnerAndUserForXmlCreator = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query } = body
  appHelper.compactObject(query)
  appHelper.checkRequiredFields(['partnerId', 'userId'], query)
  const { partnerId, userId } = query
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const partner = (await partnerHelper.getAPartner({ _id: partnerId })) || {}
  const partnerSetting =
    (await partnerSettingHelper.getAPartnerSetting({ partnerId })) || {}
  const userInfo = (await userHelper.getAnUser({ _id: userId })) || {}
  return { partner, partnerSetting, user: userInfo }
}

export const getAnnualStatementYear = async (req) => {
  const { user = {} } = req
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const statementYear = await getSpecificFiledDataForAnnualStatement(
    'statementYear',
    { partnerId }
  )
  return { statementYear }
}

export const getContractIdForAnnualStatements = async (
  statementYear,
  dataToSkip
) => {
  const type = ['invoice', 'credit_note', 'commission', 'payout', 'correction']
  const period = getPeriodBasedOnYear(statementYear)
  const partnerIds = await partnerHelper.findPartnerIdsForAnnualStatement()
  const nonSubType = [
    'administration_eviction_notice_fee',
    'administration_eviction_notice_fee_move_to',
    'collection_notice_fee',
    'collection_notice_fee_move_to',
    'eviction_notice_fee',
    'eviction_notice_fee_move_to',
    'invoice_fee',
    'invoice_reminder_fee',
    'loss_recognition',
    'reminder_fee_move_to',
    'rounded_amount',
    'unpaid_administration_eviction_notice',
    'unpaid_collection_notice',
    'unpaid_eviction_notice',
    'unpaid_reminder'
  ]

  const pipeline = [
    {
      $match: {
        type: { $in: type },
        subType: { $nin: nonSubType },
        period: { $in: period },
        partnerId: { $in: partnerIds }
      }
    },
    {
      $group: { _id: '$contractId' }
    },
    {
      $lookup: {
        from: 'annual_statements',
        localField: '_id',
        foreignField: 'contractId',
        pipeline: [
          {
            $match: { statementYear: Number(statementYear) }
          }
        ],
        as: 'annualStatements'
      }
    },
    {
      $unwind: {
        path: '$annualStatements',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        hasAnnualStatement: {
          $cond: [
            { $eq: ['$annualStatements.statementYear', Number(statementYear)] },
            true,
            false
          ]
        }
      }
    },
    {
      $match: { hasAnnualStatement: false }
    },
    {
      $sort: { _id: 1 }
    },
    { $skip: dataToSkip },
    { $limit: 100 },
    {
      $group: {
        _id: null,
        contractIds: {
          $push: '$_id'
        }
      }
    }
  ]

  const transactionData =
    (await transactionHelper.transactionAggregate(pipeline, {
      hint: 'type_1_period_1_partnerId_1'
    })) || []
  return transactionData[0]?.contractIds || []
}

const getPeriodBasedOnYear = (lastYear) => {
  if (!lastYear) return false

  const lastYearPeriod = []
  let period = 1

  for (period = 1; period <= 12; period++) {
    const periodText = period < 10 ? '0' + period : period

    lastYearPeriod.push(_.clone(lastYear) + '-' + periodText)
  }

  return lastYearPeriod
}
