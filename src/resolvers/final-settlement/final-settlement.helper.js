import { size } from 'lodash'

import { CustomError } from '../common'
import {
  ContractCollection,
  CorrectionCollection,
  InvoiceCollection,
  InvoicePaymentCollection,
  PayoutCollection
} from '../models'
import {
  appHelper,
  accountHelper,
  contractHelper,
  correctionHelper,
  invoiceHelper,
  invoicePaymentHelper,
  payoutHelper
} from '../helpers'

import { contractService, appQueueService } from '../services'

const getPipelineForFinalSettlementRent = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$invoiceType', 'invoice']
            }
          }
        }
      ],
      as: 'invoices'
    }
  },
  {
    $unwind: {
      path: '$invoices',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: '$_id',
      invoiceTotalAmount: { $sum: '$invoices.invoiceTotal' },
      invoiceTotalPaidAmount: { $sum: '$invoices.totalPaid' },
      invoiceTotalLostAmount: { $sum: '$invoices.lostMeta.amount' },
      invoiceTotalCreditedAmount: { $sum: '$invoices.creditedAmount' },
      propertyId: {
        $first: '$propertyId'
      },
      finalSettlementStatus: {
        $first: '$finalSettlementStatus'
      },
      isFinalSettlementDone: {
        $first: '$isFinalSettlementDone'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      rentalMetaHistory: {
        $first: '$rentalMetaHistory'
      },
      accountId: {
        $first: '$accountId'
      },
      agentId: {
        $first: '$agentId'
      },
      branchId: {
        $first: '$branchId'
      },
      leaseSerial: {
        $first: '$leaseSerial'
      },
      createdAt: {
        $first: '$createdAt'
      }
    }
  },
  {
    $addFields: {
      invoiceTotalAmount: {
        $add: ['$invoiceTotalAmount', '$invoiceTotalCreditedAmount']
      },
      invoiceTotalDue: {
        $subtract: [
          { $add: ['$invoiceTotalAmount', '$invoiceTotalCreditedAmount'] },
          { $add: ['$invoiceTotalPaidAmount', '$invoiceTotalLostAmount'] }
        ]
      }
    }
  }
]

const getPipelineForFinalSettlementPayment = () => [
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $ne: ['$isFinalSettlement', true]
            }
          }
        },
        {
          $addFields: {
            totalInvoicePaid: {
              $reduce: {
                input: { $ifNull: ['$invoices', []] },
                initialValue: 0,
                in: {
                  $add: ['$$value', '$$this.amount']
                }
              }
            }
          }
        }
      ],
      as: 'paymentsInfo'
    }
  },
  {
    $addFields: {
      totalPaymentPaid: {
        $reduce: {
          input: { $ifNull: ['$paymentsInfo', []] },
          initialValue: 0,
          in: {
            $add: ['$$value', '$$this.totalInvoicePaid']
          }
        }
      }
    }
  }
]

const getPipelineForFinalSettlementPayout = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: '_id',
      foreignField: 'contractId',
      as: 'payouts'
    }
  },
  {
    $unwind: {
      path: '$payouts',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: '$_id',
      invoiceTotalAmount: {
        $first: '$invoiceTotalAmount'
      },
      invoiceTotalDue: {
        $first: '$invoiceTotalDue'
      },
      invoiceTotalPaidAmount: {
        $first: '$invoiceTotalPaidAmount'
      },
      invoiceTotalLostAmount: {
        $first: '$invoiceTotalLostAmount'
      },
      totalPaymentPaid: {
        $first: '$totalPaymentPaid'
      },
      totalPayout: {
        $sum: '$payouts.amount'
      },
      totalPayoutDue: {
        $sum: {
          $cond: [
            { $eq: ['$payouts.status', 'completed'] },
            0,
            '$payouts.amount'
          ]
        }
      },
      propertyId: {
        $first: '$propertyId'
      },
      finalSettlementStatus: {
        $first: '$finalSettlementStatus'
      },
      isFinalSettlementDone: {
        $first: '$isFinalSettlementDone'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      rentalMetaHistory: {
        $first: '$rentalMetaHistory'
      },
      accountId: {
        $first: '$accountId'
      },
      agentId: {
        $first: '$agentId'
      },
      branchId: {
        $first: '$branchId'
      },
      leaseSerial: {
        $first: '$leaseSerial'
      },
      createdAt: {
        $first: '$createdAt'
      }
    }
  }
]

const getPipelineForFinalSettlementLandlord = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $in: [
                '$invoiceType',
                ['landlord_invoice', 'landlord_credit_note']
              ]
            }
          }
        }
      ],
      as: 'landlord_invoices'
    }
  },
  {
    $unwind: {
      path: '$landlord_invoices',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: '$_id',
      invoiceTotalAmount: {
        $first: '$invoiceTotalAmount'
      },
      invoiceTotalDue: {
        $first: '$invoiceTotalDue'
      },
      invoiceTotalPaidAmount: {
        $first: '$invoiceTotalPaidAmount'
      },
      invoiceTotalLostAmount: {
        $first: '$invoiceTotalLostAmount'
      },
      totalPaymentPaid: {
        $first: '$totalPaymentPaid'
      },
      totalPayout: {
        $first: '$totalPayout'
      },
      totalPayoutDue: {
        $first: '$totalPayoutDue'
      },
      landlordInvoiceTotalAmount: {
        $sum: {
          $cond: [
            { $eq: ['$landlord_invoices.isFinalSettlement', true] },
            0,
            '$landlord_invoices.invoiceTotal'
          ]
        }
      },
      landlordTotalPaidAmount: {
        $sum: {
          $cond: [
            { $eq: ['$landlord_invoices.isFinalSettlement', true] },
            0,
            '$landlord_invoices.totalPaid'
          ]
        }
      },
      landlordTotalBalancedAmount: {
        $sum: {
          $cond: [
            { $eq: ['$landlord_invoices.isFinalSettlement', true] },
            0,
            '$landlord_invoices.totalBalanced'
          ]
        }
      },
      landlordInvoiceTotalLostAmount: {
        $sum: {
          $cond: [
            { $eq: ['$landlord_invoices.isFinalSettlement', true] },
            0,
            '$landlord_invoices.lostMeta.amount'
          ]
        }
      },
      settlementInvoiceTotalAmount: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$landlord_invoices.isFinalSettlement', true] },
                {
                  $eq: ['$landlord_invoices.invoiceType', 'landlord_invoice']
                }
              ]
            },
            '$landlord_invoices.invoiceTotal',
            0
          ]
        }
      },
      settlementTotalPaidAmount: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$landlord_invoices.isFinalSettlement', true] },
                {
                  $eq: ['$landlord_invoices.invoiceType', 'landlord_invoice']
                }
              ]
            },
            '$landlord_invoices.totalPaid',
            0
          ]
        }
      },
      settlementTotalBalancedAmount: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$landlord_invoices.isFinalSettlement', true] },
                {
                  $eq: ['$landlord_invoices.invoiceType', 'landlord_invoice']
                }
              ]
            },
            '$landlord_invoices.totalBalanced',
            0
          ]
        }
      },
      settlementInvoiceTotalLostAmount: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$landlord_invoices.isFinalSettlement', true] },
                {
                  $eq: ['$landlord_invoices.invoiceType', 'landlord_invoice']
                }
              ]
            },
            '$landlord_invoices.lostMeta.amount',
            0
          ]
        }
      },
      propertyId: {
        $first: '$propertyId'
      },
      finalSettlementStatus: {
        $first: '$finalSettlementStatus'
      },
      isFinalSettlementDone: {
        $first: '$isFinalSettlementDone'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      rentalMetaHistory: {
        $first: '$rentalMetaHistory'
      },
      accountId: {
        $first: '$accountId'
      },
      agentId: {
        $first: '$agentId'
      },
      branchId: {
        $first: '$branchId'
      },
      leaseSerial: {
        $first: '$leaseSerial'
      },
      createdAt: {
        $first: '$createdAt'
      }
    }
  },
  {
    $addFields: {
      landlordInvoiceTotalDue: {
        $subtract: [
          '$landlordInvoiceTotalAmount',
          {
            $add: [
              '$landlordTotalPaidAmount',
              '$landlordInvoiceTotalLostAmount',
              '$landlordTotalBalancedAmount'
            ]
          }
        ]
      },
      settlementTotalDue: {
        $subtract: [
          '$settlementInvoiceTotalAmount',
          {
            $add: [
              '$settlementTotalPaidAmount',
              '$settlementInvoiceTotalLostAmount',
              '$settlementTotalBalancedAmount'
            ]
          }
        ]
      }
    }
  },
  {
    $addFields: {
      landlordInvoiceTotalDue: {
        $add: ['$landlordInvoiceTotalDue', '$settlementTotalDue']
      }
    }
  }
]

const getPropertyPipelineForFinalSettlement = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            listingTypeId: 1,
            propertyTypeId: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              postalCode: 1
            },
            apartmentId: 1,
            serial: 1,
            imageUrl: 1
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

const getTenantPipelineForFinalSettlement = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'rentalMeta.tenantId',
      foreignField: '_id',
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
          $unwind: {
            path: '$user',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            _id: 1,
            name: '$user.profile.name',
            serial: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$user.profile.avatarKey'
            )
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

const getAccountPipelineForFinalSettlement = () => [
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
        },
        {
          $project: {
            _id: 1,
            name: 1,
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
  {
    $unwind: {
      path: '$accountInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getBranchPipelineForFinalSettlement = () => [
  {
    $lookup: {
      from: 'branches',
      localField: 'branchId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: 1
          }
        }
      ],
      as: 'branchInfo'
    }
  },
  {
    $unwind: {
      path: '$branchInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getAgentPipelineForFinalSettlement = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agentId',
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
      as: 'agentInfo'
    }
  },
  {
    $unwind: {
      path: '$agentInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getFinalProjectPipelineForFinalSettlement = () => [
  {
    $project: {
      propertyInfo: 1,
      finalSettlementStatus: {
        $ifNull: ['$finalSettlementStatus', 'new']
      },
      invoiceTotalAmount: 1,
      invoiceTotalDue: 1,
      invoiceTotalPaidAmount: 1,
      invoiceTotalLostAmount: 1,
      isFinalSettlementDone: {
        $ifNull: ['$isFinalSettlementDone', false]
      },
      totalPaymentPaid: 1,
      totalPayout: 1,
      totalPayoutDue: 1,
      landlordInvoiceTotalAmount: 1,
      landlordInvoiceTotalDue: 1,
      tenantInfo: 1,
      accountInfo: 1,
      branchInfo: 1,
      agentInfo: 1,
      contractStartDate: '$rentalMeta.contractStartDate',
      contractEndDate: '$rentalMeta.contractEndDate',
      createdAt: 1,
      leaseSerial: {
        $switch: {
          branches: [
            {
              case: { $ifNull: ['$rentalMeta.leaseSerial', false] },
              then: '$rentalMeta.leaseSerial'
            },
            {
              case: { $ifNull: ['$lastHistory.leaseSerial', false] },
              then: '$lastHistory.leaseSerial'
            }
          ],
          default: '$leaseSerial'
        }
      }
    }
  }
]

const getLeaseSerialPipelineForFinalSettlement = () => [
  appHelper.getUnwindPipeline('rentalMetaHistory'),
  {
    $sort: {
      'rentalMetaHistory.cancelledAt': -1
    }
  },
  {
    $group: {
      _id: '$_id',
      lastHistory: {
        $first: '$rentalMetaHistory'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      finalSettlementStatus: {
        $first: '$finalSettlementStatus'
      },
      isFinalSettlementDone: {
        $first: '$isFinalSettlementDone'
      },
      invoiceTotalAmount: {
        $first: '$invoiceTotalAmount'
      },
      invoiceTotalDue: {
        $first: '$invoiceTotalDue'
      },
      invoiceTotalPaidAmount: {
        $first: '$invoiceTotalPaidAmount'
      },
      invoiceTotalLostAmount: {
        $first: '$invoiceTotalLostAmount'
      },
      totalPaymentPaid: {
        $first: '$totalPaymentPaid'
      },
      totalPayout: {
        $first: '$totalPayout'
      },
      totalPayoutDue: {
        $first: '$totalPayoutDue'
      },
      landlordInvoiceTotalAmount: {
        $first: '$landlordInvoiceTotalAmount'
      },
      landlordInvoiceTotalDue: {
        $first: '$landlordInvoiceTotalDue'
      },
      tenantInfo: {
        $first: '$tenantInfo'
      },
      accountInfo: {
        $first: '$accountInfo'
      },
      branchInfo: {
        $first: '$branchInfo'
      },
      agentInfo: {
        $first: '$agentInfo'
      },
      createdAt: {
        $first: '$createdAt'
      },
      leaseSerial: {
        $first: '$leaseSerial'
      }
    }
  }
]

export const getFinalSettlementsForQuery = async (params) => {
  const { query = {}, options } = params
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
    // Rent invoice start
    ...getPipelineForFinalSettlementRent(),
    // Rent invoice end
    // Payment start
    ...getPipelineForFinalSettlementPayment(),
    // Payment end
    // Payout start
    ...getPipelineForFinalSettlementPayout(),
    // Payout end
    //Landlord start
    ...getPipelineForFinalSettlementLandlord(),
    //Landlord end
    //Property start
    ...getPropertyPipelineForFinalSettlement(),
    // Tenant start
    ...getTenantPipelineForFinalSettlement(),
    //Tenant end
    //Account start
    ...getAccountPipelineForFinalSettlement(),
    //Account end
    // Branch start
    ...getBranchPipelineForFinalSettlement(),
    //Branch end
    // Agent start
    ...getAgentPipelineForFinalSettlement(),
    //Agent end
    ...getLeaseSerialPipelineForFinalSettlement(),
    //Final project
    {
      $sort: sort
    },
    ...getFinalProjectPipelineForFinalSettlement()
  ]

  const getAllFinalSettlement = await ContractCollection.aggregate(pipeline)
  return getAllFinalSettlement
}

export const countFinalSettlements = async (query = {}, session) => {
  if (!query.hasOwnProperty('status')) query.status = 'closed'
  if (!query.hasOwnProperty('rentalMeta.status'))
    query['rentalMeta.status'] = 'closed'
  const numberOfFinalSettlements = await ContractCollection.countDocuments(
    query
  ).session(session)
  return numberOfFinalSettlements
}

const prepareFinalSettlementQuery = async (query) => {
  const {
    agentId,
    accountId,
    branchId,
    context,
    contractId,
    createdDateRange,
    finalSettlementStatus,
    partnerId,
    propertyId,
    periodDateRange,
    tenantId,
    userId
  } = query
  const preparedQuery = {}
  if (partnerId) preparedQuery.partnerId = partnerId

  preparedQuery.status = 'closed'
  preparedQuery['rentalMeta.status'] = 'closed'

  if (context === 'landlordDashboard') {
    const accountIds = await accountHelper.getAccountIdsByQuery({
      personId: userId
    })
    preparedQuery.accountId = { $in: accountIds }
  }

  if (
    createdDateRange &&
    createdDateRange.startDate &&
    createdDateRange.endDate
  ) {
    preparedQuery.createdAt = {
      $gte: new Date(createdDateRange.startDate),
      $lte: new Date(createdDateRange.endDate)
    }
  }

  if (periodDateRange && periodDateRange.startDate && periodDateRange.endDate) {
    preparedQuery.createdAt = {
      $gte: new Date(periodDateRange.startDate),
      $lte: new Date(periodDateRange.endDate)
    }
  }

  if (branchId) preparedQuery.branchId = branchId
  if (agentId) preparedQuery.agentId = agentId
  if (accountId) preparedQuery.accountId = accountId
  if (propertyId) preparedQuery.propertyId = propertyId
  if (contractId) preparedQuery._id = contractId

  if (tenantId) {
    preparedQuery['$or'] = [
      { 'rentalMeta.tenantId': tenantId },
      { 'rentalMeta.tenants.tenantId': tenantId }
    ]
  }

  if (size(finalSettlementStatus)) {
    if (finalSettlementStatus.includes('new'))
      preparedQuery['$and'] = [
        {
          $or: [
            { finalSettlementStatus: { $exists: false } },
            { finalSettlementStatus: { $in: finalSettlementStatus } }
          ]
        }
      ]
    else preparedQuery.finalSettlementStatus = { $in: finalSettlementStatus }
  }

  return preparedQuery
}

export const totalDocsOfFinalSettlement = async () => {
  const numberOfFinalSettlements = await ContractCollection.countDocuments()
  return numberOfFinalSettlements
}

export const queryFinalSettlements = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { userId, partnerId } = user
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  query.partnerId = partnerId
  query.userId = userId
  const prepareQuery = await prepareFinalSettlementQuery(query)
  body.query = prepareQuery

  const finalSettlements = await getFinalSettlementsForQuery(body)

  const filteredDocument = await countFinalSettlements(prepareQuery)
  const totalDocument = await countFinalSettlements(totalDocumentsQuery)

  const metaDataInfo = {
    filteredDocuments: filteredDocument,
    totalDocuments: totalDocument
  }

  return {
    data: finalSettlements,
    metaData: metaDataInfo
  }
}

export const getFinalSettlementSummaryForQuery = async (query) => {
  const contractIds = await contractHelper.getUniqueFieldValue('_id', query)
  const pipeline = [
    {
      $match: {
        contractId: { $in: contractIds },
        invoiceType: {
          $in: ['invoice', 'landlord_invoice', 'landlord_credit_note']
        }
      }
    },
    {
      $group: {
        _id: null,
        invoiceTotalAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$invoiceTotal', 0]
          }
        },
        invoiceTotalPaidAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$totalPaid', 0]
          }
        },
        invoiceTotalLostAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$lostMeta.amount', 0]
          }
        },
        invoiceTotalCreditedAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$creditedAmount', 0]
          }
        },
        landlordInvoiceTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$invoiceType', 'invoice'] },
                  { $ne: ['$isFinalSettlement', true] }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        landlordInvoiceTotalPaidAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$invoiceType', 'invoice'] },
                  { $ne: ['$isFinalSettlement', true] }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        landlordInvoiceTotalLostAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$invoiceType', 'invoice'] },
                  { $ne: ['$isFinalSettlement', true] }
                ]
              },
              '$lostMeta.amount',
              0
            ]
          }
        },
        landlordInvoiceTotalBalancedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $ne: ['$invoiceType', 'invoice'] },
                  { $ne: ['$isFinalSettlement', true] }
                ]
              },
              '$totalBalanced',
              0
            ]
          }
        }
      }
    },
    {
      $addFields: {
        totalRentDue: {
          $subtract: [
            {
              $add: ['$invoiceTotalAmount', '$invoiceTotalCreditedAmount']
            },
            {
              $add: ['$invoiceTotalPaidAmount', '$invoiceTotalLostAmount']
            }
          ]
        },
        totalLandlordDue: {
          $subtract: [
            '$landlordInvoiceTotalAmount',
            {
              $add: [
                '$landlordInvoiceTotalPaidAmount',
                '$landlordInvoiceTotalLostAmount',
                '$landlordInvoiceTotalBalancedAmount'
              ]
            }
          ]
        }
      }
    }
  ]
  const [dues = {}] = (await InvoiceCollection.aggregate(pipeline)) || []
  const payoutPipeline = [
    {
      $match: {
        contractId: { $in: contractIds },
        status: { $ne: 'completed' }
      }
    },
    {
      $group: {
        _id: null,
        totalUnpaidPayout: { $sum: '$amount' }
      }
    }
  ]
  const [payoutDue = {}] =
    (await PayoutCollection.aggregate(payoutPipeline)) || []
  return {
    totalRentDue: dues.totalRentDue || 0,
    totalLandLordDue: dues.totalLandlordDue || 0,
    totalUnpaidPayout: payoutDue.totalUnpaidPayout || 0
  }
}

export const queryFinalSettlementSummary = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId

  const preparedQuery = await prepareFinalSettlementQuery(body)

  return await getFinalSettlementSummaryForQuery(preparedQuery)
}

export const isDoneFinalSettlement = async (contractId, partnerId) => {
  if (!(contractId || partnerId)) return false

  const query = {
    _id: contractId,
    partnerId,
    status: 'closed',
    isFinalSettlementDone: true
  }
  const isDoneFinalSettlement = !!(await ContractCollection.findOne(query))
  return isDoneFinalSettlement
}

export const getRentCorrections = async (contract) => {
  const query = {
    contractId: contract._id,
    propertyId: contract.propertyId,
    invoiceId: { $exists: false },
    addTo: 'rent_invoice',
    correctionStatus: 'active'
  }
  const corrections = await CorrectionCollection.find(query)
  return corrections
}

export const isPayableLandlordInvoice = async (invoiceId) => {
  if (!invoiceId) return false

  const isPayableLandlordInvoice = await invoiceHelper.getInvoice({
    _id: invoiceId,
    invoiceType: 'landlord_invoice',
    isPayable: true,
    status: { $nin: ['credited', 'paid', 'lost', 'cancelled'] }
  })

  return !!isPayableLandlordInvoice
}

export const isFinalSettlementInProgress = async (contractId, partnerId) => {
  if (!(contractId || partnerId)) return false

  const isFinalSettlementInProgress = await contractHelper.getAContract({
    _id: contractId,
    finalSettlementStatus: 'in_progress',
    isFinalSettlementDone: true,
    partnerId,
    status: 'closed'
  })

  return !!isFinalSettlementInProgress
}

const getPaymentTotalAmount = async (contractId, partnerId, session) => {
  const totalAmountInfo = await InvoicePaymentCollection.aggregate([
    {
      $match: {
        contractId,
        partnerId,
        isFinalSettlement: { $ne: true },
        refundPaymentStatus: { $ne: 'paid' }
      }
    },
    { $unwind: '$invoices' },
    {
      $lookup: {
        from: 'invoices',
        localField: 'invoices.invoiceId',
        foreignField: '_id',
        as: 'paymentInvoices'
      }
    },
    { $unwind: '$paymentInvoices' },
    {
      $project: {
        contractId: 1,
        invoices: {
          invoiceId: 1,
          amount: 1,
          isFinalSettlement: {
            $cond: {
              if: {
                $and: [{ $eq: ['$paymentInvoices.isFinalSettlement', true] }]
              },
              then: true,
              else: false
            }
          },
          _id: '$paymentInvoices._id'
        }
      }
    },
    { $match: { 'invoices.isFinalSettlement': false } },
    {
      $group: {
        _id: null,
        total: { $sum: '$invoices.amount' }
      }
    }
  ]).session(session)
  return size(totalAmountInfo) ? totalAmountInfo[0].total : 0
}

const getRefundPaymentDataForFinalSettlement = async (
  contractInfo,
  session
) => {
  const { _id: contractId, partnerId, propertyId } = contractInfo

  const tenantId = contractInfo?.rentalMeta?.tenantId
    ? contractInfo.rentalMeta.tenantId
    : ''
  let invoiceTotal = 0
  let invoiceTotalPaid = 0
  const paymentTotal = await getPaymentTotalAmount(
    contractId,
    partnerId,
    session
  )
  const lastPayment =
    propertyId && tenantId
      ? await InvoicePaymentCollection.findOne(
          {
            contractId,
            propertyId,
            tenantId
          },
          { sort: { createdAt: -1 } }
        ).session(session)
      : false
  const invoiceTotalAmountInfo = await InvoiceCollection.aggregate([
    {
      $match: {
        partnerId,
        contractId,
        invoiceType: 'invoice'
      }
    },
    {
      $group: {
        _id: null,
        invoiceTotalAmount: { $sum: '$invoiceTotal' },
        creditedTotalAmount: { $sum: '$creditedAmount' },
        lostTotalAmount: { $sum: '$lostMeta.amount' },
        totalPaidAmount: { $sum: '$totalPaid' }
      }
    }
  ]).session(session)

  if (size(invoiceTotalAmountInfo)) {
    const totalInvoice = invoiceTotalAmountInfo[0].invoiceTotalAmount || 0
    const totalLost = invoiceTotalAmountInfo[0].lostTotalAmount || 0
    const totalCredited = invoiceTotalAmountInfo[0].creditedTotalAmount || 0

    invoiceTotal = totalInvoice - totalLost + totalCredited

    invoiceTotalPaid = invoiceTotalAmountInfo[0].totalPaidAmount || 0
  }

  return {
    contractId,
    invoiceTotal,
    invoiceTotalPaid,
    lastPayment,
    paymentTotal,
    propertyId,
    tenantId
  }
}

export const checkNotInProgressFinalSettlementStatus = async (
  contractInfo,
  session
) => {
  if (!size(contractInfo))
    throw new CustomError(
      404,
      'Missing contract data for final settlement check'
    )

  const { _id: contractId, partnerId, propertyId } = contractInfo
  let isNeedFSUpdate = false

  // Find rent invoice corrections for create tenant invoice
  const corrections = await correctionHelper.getCorrections(
    {
      contractId,
      propertyId,
      invoiceId: { $exists: false },
      addTo: 'rent_invoice',
      correctionStatus: 'active'
    },
    session
  )
  isNeedFSUpdate = !!size(corrections)
  console.log(
    '====> Checking corrections status to update final settlement, isNeedFSUpdate:',
    isNeedFSUpdate,
    '<===='
  )

  if (!isNeedFSUpdate) {
    // Find remaining balance of landlord invoice or landlord credit note for adjust in payout
    const landlordInvoiceWithRemainingBalance = await invoiceHelper.getInvoices(
      {
        contractId,
        partnerId,
        propertyId,
        invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] },
        remainingBalance: { $ne: 0 },
        $or: [
          { isFinalSettlement: { $ne: true } },
          { isFinalSettlement: true, status: 'cancelled' }
        ]
      },
      session
    )

    isNeedFSUpdate = !!size(landlordInvoiceWithRemainingBalance)
    console.log(
      '====> Checking landlord invoice status to update final settlement, isNeedFSUpdate:',
      isNeedFSUpdate,
      '<===='
    )
  }

  if (!isNeedFSUpdate) {
    // Find payouts where payouts process not finished
    const nonFinishedPayouts = await payoutHelper.getPayouts(
      {
        contractId,
        partnerId,
        propertyId,
        $or: [
          { amount: { $lt: 0 } },
          {
            amount: { $ne: 0 },
            status: 'completed',
            paymentStatus: { $nin: ['paid', 'balanced'] }
          },
          {
            amount: { $ne: 0 },
            status: { $in: ['estimated', 'in_progress', 'failed'] }
          }
        ]
      },
      session
    )
    isNeedFSUpdate = !!size(nonFinishedPayouts)
    console.log(
      '====> Checking payouts status to update final settlement, isNeedFSUpdate:',
      isNeedFSUpdate,
      '<===='
    )
  }

  if (!isNeedFSUpdate) {
    // Check all payments of tenant for need to refund to tenant
    let refundableAmount = 0
    const refundPaymentInfo = await getRefundPaymentDataForFinalSettlement(
      contractInfo,
      session
    )

    if (
      size(refundPaymentInfo) &&
      refundPaymentInfo.paymentTotal > refundPaymentInfo.invoiceTotal &&
      refundPaymentInfo.lastPayment
    ) {
      refundableAmount = await appHelper.convertTo2Decimal(
        refundPaymentInfo.paymentTotal - refundPaymentInfo.invoiceTotal
      )
      if (refundableAmount) isNeedFSUpdate = true
      console.log(
        '====> Checking refund payment status to update final settlement, isNeedFSUpdate:',
        isNeedFSUpdate,
        '<===='
      )
    }

    // Check invoice due amount; if we found any due then don`t completed final settlement process
    if (!isNeedFSUpdate && size(refundPaymentInfo)) {
      const dueAmount = await appHelper.convertTo2Decimal(
        refundPaymentInfo.invoiceTotal - refundPaymentInfo.invoiceTotalPaid
      )

      if (
        dueAmount &&
        ((refundPaymentInfo.invoiceTotal > 0 && dueAmount > 0) ||
          (refundPaymentInfo.invoiceTotal < 0 && dueAmount < 0))
      ) {
        isNeedFSUpdate = true
      }
    }
    console.log(
      '====> Checking refund payment invoice status to update final settlement, isNeedFSUpdate:',
      isNeedFSUpdate,
      '<===='
    )
  }

  return isNeedFSUpdate
}

export const checkInProgressFinalSettlementStatus = async (params, session) => {
  const { contractId, partnerId, propertyId } = params
  let isNeedFSUpdate = false

  // Find remaining balance of landlord invoice or landlord credit note for adjust in payout
  const landlordInvoices = await invoiceHelper.getInvoices(
    {
      contractId,
      partnerId,
      propertyId,
      invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] },
      remainingBalance: { $ne: 0 },
      isFinalSettlement: true
    },
    session
  )
  isNeedFSUpdate = !!size(landlordInvoices)
  console.log(
    '====> Checking final settlement invoice status to update final settlement, isNeedFSUpdate:',
    isNeedFSUpdate,
    '<===='
  )

  if (!isNeedFSUpdate) {
    // Find inCompleted refundPayments
    const incompletedRefundPayments =
      await invoicePaymentHelper.getInvoicePayments(
        {
          contractId,
          partnerId,
          type: 'refund',
          amount: { $ne: 0 },
          isFinalSettlement: { $ne: true },
          $or: [
            {
              refundStatus: {
                $in: ['created', 'estimated', 'in_progress', 'failed']
              }
            },
            { refundStatus: 'completed', refundPaymentStatus: { $ne: 'paid' } }
          ]
        },
        session
      )
    isNeedFSUpdate = !!size(incompletedRefundPayments)
    console.log(
      '====> Checking incomplete refund payment status to update final settlement, isNeedFSUpdate:',
      isNeedFSUpdate,
      '<===='
    )
  }

  return isNeedFSUpdate
}

export const updateContractIfFinalSettlementIsNotNeeded = async (
  contractId
) => {
  try {
    const needFinalSettlement = await checkIfFinalSettlementNeeded(contractId)
    if (!needFinalSettlement) {
      const contract = await contractService.updateContract(
        {
          _id: contractId,
          status: 'closed',
          finalSettlementStatus: { $ne: 'completed' }
        },
        { finalSettlementStatus: 'completed', isFinalSettlementDone: true }
      )
      console.log('Updated info', contract)
      return true
    } else return false
  } catch (e) {
    console.log(
      `Error happened while updating contract ${contractId}, final settlement as completed`
    )
  }
}

export const checkIfFinalSettlementNeeded = async (contractId) => {
  const pipeline = pipelineForFinalSettlements(contractId)
  const finalSettlementData = await ContractCollection.aggregate(pipeline)
  if (!size(finalSettlementData))
    throw new CustomError(404, 'Contract not found')

  const {
    corrections,
    hasIncompletePayoutProcess,
    hasUnbalanceInvoices,
    incompleteRefundProcess,
    invoiceTotalAmountInfo,
    lastPayment,
    paymentTotal,
    unbalancedInProgressInvoice
  } = finalSettlementData[0]

  console.log('hasUnbalanceInvoices', hasUnbalanceInvoices)
  console.log('hasIncompletePayoutProcess', hasIncompletePayoutProcess)

  if (size(corrections) || hasIncompletePayoutProcess || hasUnbalanceInvoices)
    return true

  if (size(invoiceTotalAmountInfo)) {
    const {
      creditedTotalAmount = 0,
      invoiceTotalAmount = 0,
      invoiceTotalPaid = 0,
      lostTotalAmount = 0
    } = invoiceTotalAmountInfo

    const invoiceTotal =
      invoiceTotalAmount - lostTotalAmount + creditedTotalAmount

    console.log('invoiceTotal', invoiceTotal)
    console.log('paymentTotal', paymentTotal)

    if (size(lastPayment) && paymentTotal > invoiceTotal) {
      const refundableAmount =
        (await appHelper.convertTo2Decimal(paymentTotal - invoiceTotal)) * 1

      if (refundableAmount) return true
    }

    const dueAmount =
      (await appHelper.convertTo2Decimal(invoiceTotal - invoiceTotalPaid)) * 1

    if (
      dueAmount &&
      ((invoiceTotal > 0 && dueAmount > 0) ||
        (invoiceTotal < 0 && dueAmount < 0))
    ) {
      return true
    }
  }
  console.log('unbalancedInProgressInvoice', size(unbalancedInProgressInvoice))
  console.log('incompleteRefundProcess', size(incompleteRefundProcess))

  if (size(unbalancedInProgressInvoice) || size(incompleteRefundProcess))
    return true
  return false
}

const pipelineForFinalSettlements = (contractId) => [
  {
    $match: { _id: contractId }
  },
  {
    $lookup: {
      from: 'expenses',
      localField: '_id',
      foreignField: 'contractId',
      as: 'corrections',
      pipeline: [
        {
          $addFields: {
            invoiceId: {
              $ifNull: ['$invoiceId', false]
            }
          }
        },
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$invoiceId', false] },
                { $eq: ['$addTo', 'rent_invoice'] },
                { $eq: ['$correctionStatus', 'active'] }
              ]
            }
          }
        },
        { $limit: 1 }
      ]
    }
  },
  {
    $unwind: {
      path: '$corrections',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      as: 'unbalanceInvoicesCount',
      pipeline: [
        {
          $addFields: {
            invoiceId: {
              $ifNull: ['$invoiceId', false]
            }
          }
        },
        {
          $match: {
            $expr: {
              $and: [
                {
                  $in: [
                    '$invoiceType',
                    ['landlord_invoice', 'landlord_credit_note']
                  ]
                },
                { $ne: ['$remainingBalance', 0] },
                {
                  $or: [
                    { $ne: ['$isFinalSettlement', true] },
                    {
                      $and: [
                        { $eq: ['$isFinalSettlement', true] },
                        { $eq: ['$status', 'cancelled'] }
                      ]
                    }
                  ]
                }
              ]
            }
          }
        },
        { $limit: 1 }
      ]
    }
  },
  {
    $unwind: {
      path: '$unbalanceInvoicesCount',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'payouts',
      localField: '_id',
      foreignField: 'contractId',
      as: 'incompletePayoutProcessCount',
      pipeline: [
        {
          $match: {
            $expr: {
              $or: [
                { $lt: ['$amount', 0] },
                {
                  $and: [
                    { $ne: ['$amount', 0] },
                    { $eq: ['$status', 'completed'] },
                    { $ne: ['$paymentStatus', 'paid'] },
                    { $ne: ['$paymentStatus', 'balanced'] }
                  ]
                },
                {
                  $and: [
                    { $ne: ['$amount', 0] },
                    {
                      $in: ['$status', ['estimated', 'in_progress', 'failed']]
                    }
                  ]
                }
              ]
            }
          }
        },
        { $limit: 1 }
      ]
    }
  },
  {
    $unwind: {
      path: '$incompletePayoutProcessCount',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      as: 'lastPayment',
      let: { rentalMetaTenantId: '$rentalMeta.tenantId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$tenantId', '$$rentalMetaTenantId']
            }
          }
        },
        {
          $sort: {
            createdAt: -1
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
      path: '$lastPayment',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      as: 'invoiceTotalAmountInfo',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$invoiceType', 'invoice']
            }
          }
        },
        {
          $group: {
            _id: null,
            invoiceTotalAmount: { $sum: '$invoiceTotal' },
            creditedTotalAmount: { $sum: '$creditedAmount' },
            lostTotalAmount: { $sum: '$lostMeta.amount' },
            totalPaidAmount: { $sum: '$totalPaid' }
          }
        },
        {
          $project: {
            invoiceTotalAmount: 1,
            lostTotalAmount: 1,
            creditedTotalAmount: 1,
            invoiceTotalPaid: '$totalPaidAmount'
          }
        }
      ]
    }
  },
  {
    $unwind: {
      path: '$invoiceTotalAmountInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      as: 'paymentTotalInfo',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $ne: ['$isFinalSettlement', true] },
                { $ne: ['$refundPaymentStatus', 'paid'] }
              ]
            }
          }
        },
        { $unwind: '$invoices' },
        {
          $lookup: {
            from: 'invoices',
            localField: 'invoices.invoiceId',
            foreignField: '_id',
            as: 'paymentInvoices',
            pipeline: [
              {
                $addFields: {
                  isFinalSettlement: {
                    $cond: {
                      if: { $and: [{ $eq: ['isFinalSettlement', true] }] },
                      then: true,
                      else: false
                    }
                  }
                }
              }
            ]
          }
        },
        { $unwind: '$paymentInvoices' },
        {
          $match: {
            'paymentInvoices.isFinalSettlement': false
          }
        },
        {
          $group: {
            _id: null,
            total: {
              $sum: {
                $cond: {
                  if: { $eq: ['$paymentInvoices.isFinalSettlement', false] },
                  then: '$invoices.amount',
                  else: 0
                }
              }
            }
          }
        }
      ]
    }
  },
  {
    $unwind: {
      path: '$paymentTotalInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      as: 'unbalancedInProgressInvoice',
      pipeline: [
        {
          $addFields: {
            invoiceId: {
              $ifNull: ['$invoiceId', false]
            }
          }
        },
        {
          $match: {
            $expr: {
              $and: [
                {
                  $in: [
                    '$invoiceType',
                    ['landlord_invoice', 'landlord_credit_note']
                  ]
                },
                { $ne: ['$remainingBalance', 0] },
                { $eq: ['$isFinalSettlement', true] }
              ]
            }
          }
        },
        { $limit: 1 }
      ]
    }
  },
  {
    $unwind: {
      path: '$unbalancedInProgressInvoice',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      as: 'incompleteRefundProcess',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$type', 'refund'] },
                { $ne: ['$amount', 0] },
                { $eq: ['$type', 'refund'] },
                {
                  $or: [
                    {
                      $and: [
                        { $eq: ['$refundStatus', 'completed'] },
                        { $ne: ['$refundPaymentStatus', 'paid'] }
                      ]
                    },
                    {
                      $in: [
                        '$refundStatus',
                        ['created', 'estimated', 'in_progress', 'failed']
                      ]
                    }
                  ]
                }
              ]
            }
          }
        },
        { $limit: 1 }
      ]
    }
  },
  {
    $unwind: {
      path: '$incompleteRefundProcess',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $project: {
      corrections: 1,
      hasUnbalanceInvoices: {
        $cond: [
          {
            $ifNull: ['$unbalanceInvoicesCount', false]
          },
          true,
          false
        ]
      },
      hasIncompletePayoutProcess: {
        $cond: [
          {
            $ifNull: ['$incompletePayoutProcessCount', false]
          },
          true,
          false
        ]
      },
      lastPayment: 1,
      invoiceTotalAmountInfo: 1,
      paymentTotal: '$paymentTotalInfo.total',
      unbalancedInProgressInvoice: 1,
      incompleteRefundProcess: 1
    }
  }
]

export const initializeFinalSettlementProcessService = async (
  contractId,
  partnerId,
  session
) => {
  //Update final settlement status to in progress
  try {
    const isNeededFinalStatement = await checkIfFinalSettlementNeeded(
      contractId
    )
    if (isNeededFinalStatement) {
      await contractService.updateContract(
        {
          _id: contractId
        },
        { finalSettlementStatus: 'in_progress', isFinalSettlementDone: true },
        session
      )
      await appQueueService.createAnAppQueue({
        event: 'create_tenant_invoice_for_rent_invoice_correction',
        action: 'create_tenant_invoice_for_rent_invoice_correction',
        params: {
          contractId,
          partnerId
        },
        destination: 'lease',
        priority: 'immediate'
      })
      return true
    } else {
      return false
    }
  } catch (e) {
    console.log(
      'Error happened while updating the final settlement or creating a queue',
      e
    )
    throw new Error(e)
  }
}

export const getRemainingLandlordInvoiceTotal = async (
  params = {},
  session
) => {
  const { contractId, partnerId, propertyId } = params
  const pipeline = [
    {
      $match: {
        contractId,
        partnerId,
        propertyId,
        $or: [
          {
            invoiceType: 'landlord_invoice',
            remainingBalance: { $ne: 0 },
            isFinalSettlement: { $ne: true }
          },
          { invoiceType: 'landlord_credit_note', remainingBalance: { $ne: 0 } }
        ]
      }
    },
    {
      $group: {
        _id: null,
        remainingBalanceTotal: { $sum: '$remainingBalance' }
      }
    }
  ]
  const landlordInvoicesTotalInfo =
    await invoiceHelper.getInvoicesViaAggregation(pipeline, session)
  const [sumOfLandlordInvoice = {}] = landlordInvoicesTotalInfo || []
  return sumOfLandlordInvoice.remainingBalanceTotal || 0
}

export const getNotAdjustedAmountForFinalSettlementInvoices = async (
  params = {},
  session
) => {
  const { contractId, partnerId, propertyId } = params
  const pipeline = [
    {
      $match: {
        invoiceType: 'landlord_invoice',
        remainingBalance: { $ne: 0 },
        isFinalSettlement: true,
        contractId,
        partnerId,
        propertyId
      }
    },
    {
      $group: {
        _id: null,
        totalPayoutableAmount: { $sum: '$payoutableAmount' },
        finalSettlementInvoiceIds: { $push: '$_id' }
      }
    }
  ]
  const finalSettlementInvoicesTotalInfo =
    await invoiceHelper.getInvoicesViaAggregation(pipeline, session)
  const [finalSettlementInvoiceTotalInfo = {}] =
    finalSettlementInvoicesTotalInfo || []
  let notAdjustedAmount =
    finalSettlementInvoiceTotalInfo.totalPayoutableAmount || 0
  if (notAdjustedAmount) {
    const landlordInvoiceIds =
      finalSettlementInvoiceTotalInfo.finalSettlementInvoiceIds
    const payoutPipeline = [
      {
        $match: {
          contractId,
          partnerId,
          propertyId,
          meta: {
            $elemMatch: {
              type: {
                $in: [
                  'final_settlement_invoiced',
                  'final_settlement_invoiced_cancelled'
                ]
              },
              landlordInvoiceId: { $in: landlordInvoiceIds }
            }
          }
        }
      },
      {
        $addFields: {
          adjustedAmount: {
            $reduce: {
              input: { $ifNull: ['$meta', []] },
              initialValue: 0,
              in: {
                $add: [
                  '$$value',
                  {
                    $cond: [
                      {
                        $in: [
                          '$$this.type',
                          [
                            'final_settlement_invoiced',
                            'final_settlement_invoiced_cancelled'
                          ]
                        ]
                      },
                      '$$this.amount',
                      0
                    ]
                  }
                ]
              }
            }
          }
        }
      },
      {
        $group: {
          _id: null,
          adjustedTotal: {
            $sum: '$adjustedAmount'
          }
        }
      }
    ]
    const adjustedTotalInfo = await payoutHelper.getAggregatedPayouts(
      payoutPipeline,
      session
    )
    const [adjusted = {}] = adjustedTotalInfo || []
    const adjustedTotal = adjusted.adjustedTotal || 0
    notAdjustedAmount =
      (await appHelper.convertTo2Decimal(notAdjustedAmount)) -
      (await appHelper.convertTo2Decimal(adjustedTotal))
  }
  return notAdjustedAmount
}
