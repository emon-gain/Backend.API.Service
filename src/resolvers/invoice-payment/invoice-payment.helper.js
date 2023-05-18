import { compact, map, omit, size } from 'lodash'
import nid from 'nid'

import { CustomError } from '../common'

import {
  InvoicePaymentCollection,
  PartnerPayoutCollection,
  PayoutCollection
} from '../models'
import {
  appHelper,
  accountHelper,
  appInvoiceHelper,
  appQueueHelper,
  invoiceHelper,
  invoiceSummaryHelper,
  partnerSettingHelper,
  userHelper
} from '../helpers'

export const getInvoicePayment = async (query, session) => {
  const invoicePayment = await InvoicePaymentCollection.findOne(query).session(
    session
  )
  return invoicePayment
}

export const getInvoicePayments = async (query, session) => {
  const invoicePayments = await InvoicePaymentCollection.find(query).session(
    session
  )
  return invoicePayments
}

const getInvoicesPipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoices.invoiceId',
      foreignField: '_id',
      as: 'invoicesInfo'
    }
  }
]

const getInvoiceInfoPipeline = () => [
  {
    $addFields: {
      invoiceInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$invoices', []] },
            as: 'invoice',
            cond: {
              $eq: ['$$invoice.invoiceId', '$invoiceId']
            }
          }
        }
      }
    }
  }
]

export const getInvoicePaymentForAppHealth = async (partnerId) => {
  const invoicePaymentTotal = await InvoicePaymentCollection.aggregate([
    {
      $match: {
        partnerId,
        $or: [
          {
            status: {
              $in: ['registered', 'refunded']
            }
          },
          { refundPaymentStatus: 'paid' }
        ]
      }
    },
    {
      $project: {
        amount: 1
      }
    },
    {
      $lookup: {
        from: 'transactions',
        localField: '_id',
        foreignField: 'paymentId',
        as: 'transactions'
      }
    },
    {
      $addFields: {
        transactions: {
          $filter: {
            input: '$transactions',
            as: 'item',
            cond: { $in: ['$$item.type', ['payment', 'refund']] }
          }
        }
      }
    },
    {
      $addFields: {
        totalRoundedAmount: {
          $sum: {
            $ifNull: ['$transactions.rounded_amount', 0]
          }
        },
        transactions: '$transactions._id',
        transactionAmounts: {
          $sum: '$transactions.amount'
        }
      }
    },
    {
      $addFields: {
        transactionAmounts: {
          $subtract: ['$transactionAmounts', '$totalRoundedAmount']
        }
      }
    },
    {
      $addFields: {
        missMatchTransactionsAmount: {
          $abs: {
            $subtract: ['$transactionAmounts', '$amount']
          }
        }
      }
    },
    {
      $group: {
        _id: null,
        totalTransactions: {
          $sum: '$transactionAmounts'
        },
        totalPayment: {
          $sum: '$amount'
        },
        missingAmount: {
          $sum: '$missMatchTransactionsAmount'
        },
        missingTransactionsInPayment: {
          $push: {
            $cond: {
              if: {
                $gte: [
                  {
                    $abs: '$missMatchTransactionsAmount'
                  },
                  1
                ]
              },
              then: {
                paymentId: '$_id',
                paymentAmount: '$amount',
                transactions: '$transactions',
                transactionAmounts: '$transactionAmounts'
              },
              else: '$$REMOVE'
            }
          }
        }
      }
    }
  ])
  return invoicePaymentTotal[0]
}

const getAppInvoicePipelineForInsurancePayment = () => [
  {
    $lookup: {
      from: 'app_invoices',
      localField: 'appInvoiceId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            serialId: 1
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  {
    $unwind: {
      path: '$invoiceInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const paymentDateTextPipeline = () => [
  {
    $addFields: {
      paymentBookingDate: {
        $switch: {
          branches: [
            {
              case: {
                $not: { $eq: ['$type', 'refund'] }
              },
              then: '$paymentDate'
            },
            {
              case: {
                $and: [
                  { $eq: ['$type', 'refund'] },
                  { $ifNull: ['$bookingDate', false] }
                ]
              },
              then: '$bookingDate'
            },
            {
              case: {
                $and: [
                  { $eq: ['$type', 'refund'] },
                  { $eq: ['$isManualRefund', true] },
                  { $eq: ['$refundStatus', 'completed'] },
                  { $eq: ['$refundPaymentStatus', 'paid'] }
                ]
              },
              then: '$paymentDate'
            },
            {
              case: {
                $and: [
                  { $eq: ['$type', 'refund'] },
                  { $eq: ['$refundStatus', 'pending_for_approval'] }
                ]
              },
              then: '$paymentDate'
            }
          ],
          default: ''
        }
      }
    }
  }
]

const getPropertyPipelineForInsurancePayment = () => [
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
            imageUrl: 1,
            location: {
              apartmentId: '$apartmentId',
              city: 1,
              country: 1,
              name: 1,
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

const getPartnerPipeline = () => [
  {
    $lookup: {
      from: 'partners',
      localField: 'appPartnerId',
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

const getTenantPipeline = () => [
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

const getContractPipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'contractInfo'
    }
  },
  appHelper.getUnwindPipeline('contractInfo')
]

const getFinalProjectPipeline = () => [
  {
    $project: {
      _id: 1,
      paymentDateText: {
        $switch: {
          branches: [
            {
              case: { $not: { $eq: ['$type', 'refund'] } },
              then: '$paymentDate'
            },
            {
              case: {
                $and: [
                  { $eq: ['$type', 'refund'] },
                  { $ifNull: ['$bookingDate', false] }
                ]
              },
              then: '$bookingDate'
            },
            {
              case: {
                $and: [
                  { $eq: ['$type', 'refund'] },
                  { $eq: ['$isManualRefund', true] },
                  { $eq: ['$refundStatus', 'completed'] },
                  { $eq: ['$refundPaymentStatus', 'paid'] }
                ]
              },
              then: '$paymentDate'
            },
            {
              case: {
                $and: [
                  { $eq: ['$type', 'refund'] },
                  { $eq: ['$refundStatus', 'pending_for_approval'] }
                ]
              },
              then: '$paymentDate'
            }
          ],
          default: ''
        }
      },
      invoicesInfo: {
        _id: 1,
        invoiceSerialId: 1
      },
      status: 1,
      paymentType: 1,
      paymentReason: 1,
      amount: 1,
      bankRef: '$meta.bankRef',
      tenantInfo: 1,
      propertyInfo: {
        _id: 1,
        location: {
          name: 1,
          city: 1,
          country: 1,
          postalCode: 1
        },
        apartmentId: 1,
        listingTypeId: 1,
        propertyTypeId: 1,
        imageUrl: 1
      },
      createdAt: 1,
      type: 1,
      meta: 1,
      refunded: 1,
      partiallyRefunded: 1,
      refundStatus: 1,
      refundToAccountNumber: 1,
      numberOfFails: 1,
      refundPaymentStatus: 1,
      isFinalSettlementDone: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$invoiceInfo.invoiceType', 'landlord_invoice'] },
                  { $eq: ['$invoiceInfo.isFinalSettlement', true] },
                  { $eq: ['$invoiceInfo.isPayable', true] }
                ]
              },
              then: false
            },
            {
              case: { $eq: ['$contractInfo.isFinalSettlementDone', true] },
              then: true
            }
          ],
          default: false
        }
      },
      contractId: 1,
      leaseSerial: '$contractInfo.leaseSerial',
      invoiceInfo: {
        _id: 1,
        creditedAmount: 1,
        invoiceMonth: 1,
        invoiceTotal: 1,
        invoiceType: 1,
        isPayable: 1,
        lostMeta: 1,
        totalPaid: 1,
        totalBalanced: 1
      }
    }
  }
]

export const getInsurancePaymentSummary = async (params) => {
  const pipeline = [
    {
      $match: params
    },
    {
      $group: {
        _id: null,
        totalPayment: { $sum: '$amount' },
        totalRegistered: {
          $sum: {
            $cond: [{ $eq: ['$status', 'registered'] }, '$amount', 0]
          }
        },
        totalUnspecified: {
          $sum: {
            $cond: [{ $eq: ['$status', 'unspecified'] }, '$amount', 0]
          }
        }
      }
    }
  ]
  return (await InvoicePaymentCollection.aggregate(pipeline)) || []
}

export const getInsurancePaymentListQuery = async (params) => {
  const { query = {}, options = {} } = params
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
    ...getPartnerPipeline(),
    ...getTenantPipeline(),
    ...getPropertyPipelineForInsurancePayment(),
    ...getAppInvoicePipelineForInsurancePayment(),
    ...paymentDateTextPipeline(),
    {
      $project: {
        _id: 1,
        amount: 1,
        createdAt: 1,
        invoiceInfo: 1,
        partner: 1,
        paymentReason: 1,
        paymentType: 1,
        paymentBookingDate: 1,
        propertyInfo: 1,
        status: 1,
        tenantInfo: 1
      }
    }
  ]
  return (await InvoicePaymentCollection.aggregate(pipeline)) || []
}

export const getInvoicePaymentsForQuery = async (params) => {
  const { query = {}, options = {} } = params
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
    ...getInvoicesPipeline(),
    ...getTenantPipeline(),
    ...appHelper.getCommonPropertyInfoPipeline(),
    ...getContractPipeline(),
    {
      $addFields: {
        invoiceInfo: {
          $first: {
            $filter: {
              input: { $ifNull: ['$invoicesInfo', []] },
              as: 'invoice',
              cond: {
                $eq: ['$$invoice._id', '$invoiceId']
              }
            }
          }
        }
      }
    },
    ...getFinalProjectPipeline()
  ]

  const invoicePayments = await InvoicePaymentCollection.aggregate(pipeline)
  return invoicePayments
}

export const countInvoicePayments = async (query, session) => {
  const numberOfInvoicePayments = await InvoicePaymentCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfInvoicePayments
}

const prepareProjectPipeline = () => ({
  $project: {
    totalPaymentAmount: {
      $cond: {
        if: { $eq: ['$refundStatus', 'canceled'] },
        then: 0,
        else: '$amount'
      }
    },
    totalRegisteredAmount: {
      $cond: {
        if: { $eq: ['$status', 'registered'] },
        then: '$amount',
        else: 0
      }
    },
    totalUnspecifiedAmount: {
      $cond: {
        if: { $eq: ['$status', 'unspecified'] },
        then: '$amount',
        else: 0
      }
    },
    totalRefundAmount: {
      $cond: {
        if: {
          $and: [
            { $eq: ['$type', 'refund'] },
            {
              $not: {
                $in: ['$refundStatus', ['completed', 'canceled', 'failed']]
              }
            }
          ]
        },
        then: '$amount',
        else: 0
      }
    }
  }
})

const prepareGroupPipeline = () => ({
  $group: {
    _id: null,
    totalPaymentAmount: { $sum: '$totalPaymentAmount' },
    totalRegisteredAmount: { $sum: '$totalRegisteredAmount' },
    totalUnspecifiedAmount: { $sum: '$totalUnspecifiedAmount' },
    totalRefundAmount: { $sum: '$totalRefundAmount' }
  }
})

const getTotalCounts = async (query) => {
  const projectPipeline = prepareProjectPipeline()
  const groupPipeline = prepareGroupPipeline()
  const aggregatePipeline = [{ $match: query }, projectPipeline, groupPipeline]
  const totals =
    (await InvoicePaymentCollection.aggregate(aggregatePipeline)) || []
  return totals
}

const getAllPaymentsEsignInfo = async (partnerId) => {
  const pipeline = [
    {
      $match: {
        partnerId,
        'directRemittanceSigningStatus.signed': false,
        status: 'waiting_for_signature',
        type: 'refund_payment'
      }
    },
    {
      $lookup: {
        from: 'invoice-payments',
        localField: 'paymentIds',
        foreignField: '_id',
        as: 'payments'
      }
    },
    {
      $unwind: {
        path: '$payments',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$directRemittanceSigningStatus',
        preserveNullAndEmptyArrays: true
      }
    },
    { $match: { 'directRemittanceSigningStatus.signed': false } },
    {
      $group: {
        _id: '$directRemittanceSigningStatus.userId',
        amount: { $sum: '$payments.amount' },
        createdAt: { $first: '$createdAt' },
        paymentsApprovalEsigningUrl: {
          $first: '$directRemittanceSigningStatus.signingUrl'
        }
      }
    }
  ]

  const result = await PartnerPayoutCollection.aggregate(pipeline)
  return result || []
}

export const insurancePaymentsSummaryQuery = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  const preparedQuery = await preparedInsurancePaymentsQuery(body)
  const [insurancePaymentsSummary = {}] = await getInsurancePaymentSummary(
    preparedQuery
  )
  const {
    totalPayment = 0,
    totalRegistered = 0,
    totalUnspecified = 0
  } = insurancePaymentsSummary
  return {
    totalPayment,
    totalRegistered,
    totalUnspecified
  }
}

export const insurancePaymentsList = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { query = {}, options = {} } = body
  appHelper.validateSortForQuery(options.sort)
  const preparedQuery = await preparedInsurancePaymentsQuery(query)
  body.query = preparedQuery
  const insurancePaymentsListData = await getInsurancePaymentListQuery(body)
  const filteredDocuments = await countInvoicePayments(preparedQuery)
  const totalDocuments = await countInvoicePayments({
    isDepositInsurancePayment: true
  })
  return {
    data: insurancePaymentsListData,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const queryInvoicePayments = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.validateId({ partnerId })
  const { query = {}, options = {} } = body
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  query.partnerId = partnerId
  query.userId = userId
  appHelper.validateSortForQuery(options.sort)
  const preparedQuery = await prepareInvoicePaymentsQuery(query)
  body.query = preparedQuery
  const invoicePaymentsData = await getInvoicePaymentsForQuery(body)
  const filteredDocuments = await countInvoicePayments(preparedQuery)
  const totalDocuments = await countInvoicePayments(totalDocumentsQuery)

  const esignInfo = await getAllPaymentsEsignInfo(partnerId)
  return {
    data: invoicePaymentsData,
    metaData: {
      filteredDocuments,
      totalDocuments
    },
    actions: esignInfo
  }
}

const getPropertyInfo = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            listingTypeId: 1,
            propertyTypeId: 1,
            imageUrl: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              streetNumber: 1,
              sublocality: 1,
              postalCode: 1
            },
            apartmentId: 1,
            floor: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo')
]

const getInvoicesforPayments = () => [
  appHelper.getUnwindPipeline('invoices'),
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoices.invoiceId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            invoiceSerialId: 1,
            isFinalSettlement: 1,
            isPayable: 1,
            invoiceType: 1 // For isFinalSettlementDone
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo'),
  {
    $group: {
      _id: '$_id',
      manualRefundReason: {
        $first: '$manualRefundReason'
      },
      mainTenant: {
        $first: '$tenantInfo'
      },
      meta: {
        $first: '$meta'
      },
      numberOfFails: {
        $first: '$numberOfFails'
      },
      partiallyRefunded: {
        $first: '$partiallyRefunded'
      },
      paymentDate: {
        $first: '$paymentDate'
      },
      paymentId: {
        $first: '$paymentId'
      },
      paymentReason: {
        $first: '$paymentReason'
      },
      paymentType: {
        $first: '$paymentType'
      },
      propertyId: {
        $first: '$propertyId'
      },
      refundBankRef: {
        $first: '$refundBankRef'
      },
      refundStatus: {
        $first: '$refundStatus'
      },
      refundPaymentStatus: {
        $first: '$refundPaymentStatus'
      },
      refundedMeta: {
        $first: '$refundedMeta'
      },
      refundToAccountNumber: {
        $first: '$refundToAccountNumber'
      },
      refundToAccountName: {
        $first: '$refundToAccountName'
      },
      status: {
        $first: '$status'
      },
      amount: {
        $first: '$amount'
      },
      type: {
        $first: '$type'
      },
      totalPaymentAmount: {
        $first: '$totalPaymentAmount'
      },
      invoices: {
        $push: {
          $cond: [
            { $ifNull: ['$invoiceInfo', false] },
            {
              invoiceId: '$invoices.invoiceId',
              invoiceSerialId: '$invoiceInfo.invoiceSerialId',
              isPayable: '$invoiceInfo.isPayable',
              isFinalSettlement: '$invoiceInfo.isFinalSettlement',
              amount: '$invoices.amount',
              invoiceType: '$invoiceInfo.invoiceType'
            },
            '$$REMOVE'
          ]
        }
      },
      accountInfo: {
        $first: '$accountInfo'
      },
      agentInfo: {
        $first: '$agentInfo'
      },
      branchInfo: {
        $first: '$branchInfo'
      },
      partnerId: { $first: '$partnerId' },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      tenantId: {
        $first: '$tenantId'
      },
      tenants: {
        $first: '$tenants'
      },
      invoiceId: {
        $first: '$invoiceId'
      },
      refunded: {
        $first: '$refunded'
      },
      contractId: {
        $first: '$contractId'
      },
      additionalTaxInfo: {
        $first: '$additionalTaxInfo'
      }
    }
  }
]

const getTenantsForPayment = () => [
  {
    $addFields: {
      tenants: {
        $filter: {
          input: '$tenants',
          as: 'tenant',
          cond: {
            $ne: ['$$tenant.tenantId', '$tenantId']
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenants.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'tenantUserInfo'
          }
        },
        appHelper.getUnwindPipeline('tenantUserInfo'),
        {
          $addFields: {
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$tenantUserInfo.profile.avatarKey'
            )
          }
        }
      ],
      as: 'otherTenants'
    }
  }
]

const getRefundableAmountForInvoicePipeline = (invoiceId) => [
  {
    $addFields: {
      paymentInvoiceId: invoiceId
    }
  },
  {
    $lookup: {
      from: 'invoices',
      foreignField: '_id',
      localField: 'paymentInvoiceId',
      as: 'paymentInvoice'
    }
  },
  appHelper.getUnwindPipeline('paymentInvoice'),
  {
    $lookup: {
      from: 'invoice-payments',
      localField: 'paymentInvoiceId',
      foreignField: 'invoices.invoiceId',
      let: { paymentInvoiceId: '$paymentInvoiceId' },
      as: 'invoicePayments',
      pipeline: [
        {
          $match: {
            type: 'payment'
          }
        },
        {
          $addFields: {
            matchedInvoice: {
              $first: {
                $filter: {
                  input: { $ifNull: ['$invoices', []] },
                  as: 'invoice',
                  cond: {
                    $and: [
                      { $eq: ['$$invoice.invoiceId', '$$paymentInvoiceId'] },
                      { $gt: ['$$invoice.amount', 0] }
                    ]
                  }
                }
              }
            }
          }
        },
        {
          $match: {
            matchedInvoice: {
              $exists: true
            }
          }
        },
        {
          $unwind: '$refundPaymentIds'
        },
        {
          $group: {
            _id: null,
            refundPaymentIds: {
              $push: '$refundPaymentIds'
            }
          }
        }
      ]
    }
  },
  appHelper.getUnwindPipeline('invoicePayments'),
  {
    $lookup: {
      from: 'invoice-payments',
      localField: 'invoicePayments.refundPaymentIds',
      foreignField: '_id',
      as: 'refundPayments',
      pipeline: [
        {
          $match: {
            type: 'refund',
            refundPaymentStatus: { $ne: 'paid' },
            refundStatus: {
              $ne: 'completed'
            }
          }
        },
        {
          $group: {
            _id: null,
            totalRefundPayment: {
              $sum: '$amount'
            }
          }
        }
      ]
    }
  },
  appHelper.getUnwindPipeline('refundPayments'),
  {
    $addFields: {
      refundableAmount: {
        $add: [
          { $ifNull: ['$paymentInvoice.totalPaid', 0] },
          { $ifNull: ['$refundPayments.totalRefundPayment', 0] }
        ]
      }
    }
  }
]

const getRefundableAmountForPaymentPipeline = () => [
  {
    $addFields: {
      refundedAmountFromMeta: {
        $reduce: {
          input: { $ifNull: ['$refundedMeta', []] },
          initialValue: 0,
          in: {
            $add: ['$$value', '$$this.amount']
          }
        }
      }
    }
  },
  {
    $addFields: {
      refundableAmount: {
        $add: ['$amount', '$refundedAmountFromMeta']
      }
    }
  }
]

const getInvoicePaymentDetailsForQuery = async (query) => {
  let refundableAmountPipeline = []
  const { invoiceId, partnerId, paymentId } = query
  query._id = paymentId
  query.$or = [{ partnerId }, { appPartnerId: partnerId }]
  if (invoiceId) {
    query['invoices.invoiceId'] = invoiceId
    refundableAmountPipeline = getRefundableAmountForInvoicePipeline(invoiceId)
  } else {
    refundableAmountPipeline = getRefundableAmountForPaymentPipeline()
  }
  const preparedQuery = omit(query, ['paymentId', 'invoiceId', 'partnerId'])

  const pipeline = [
    {
      $match: preparedQuery
    },
    { $addFields: { partnerId: { $ifNull: ['$partnerId', '$appPartnerId'] } } },
    ...appHelper.getCommonTenantInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...getPropertyInfo(),
    ...getInvoicesforPayments(),
    ...getInvoiceInfoPipeline(),
    ...getContractPipeline(),
    ...getTenantsForPayment(),
    ...refundableAmountPipeline,
    {
      $lookup: {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        pipeline: [
          {
            $lookup: {
              from: 'partner_settings',
              localField: '_id',
              foreignField: 'partnerId',
              as: 'partnerSetting'
            }
          },
          {
            $addFields: {
              partnerSetting: '$$REMOVE',
              address: { $first: '$partnerSetting.companyInfo.officeAddress' }
            }
          }
        ],
        as: 'partner'
      }
    },
    { $addFields: { partner: { $first: '$partner' } } },
    {
      $addFields: {
        cdTrAccount: {
          address: '$partner.address',
          avatarKey: {
            $cond: [
              { $ifNull: ['$partner.logo', false] },
              {
                $concat: [
                  appHelper.getCDNDomain(),
                  '/partner_logo/',
                  '$_id',
                  '/',
                  '$partner.logo'
                ]
              },
              appHelper.getDefaultLogoURL('organization')
            ]
          },
          name: '$partner.name'
        }
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'meta.cdTrAccountNumber',
        foreignField: 'invoiceAccountNumber',
        let: { partnerId: '$partnerId' },
        pipeline: [
          { $match: { $expr: { $eq: ['$$partnerId', '$partnerId'] } } },
          { $sort: { createdAt: 1 } },
          { $limit: 1 },
          {
            $lookup: {
              from: 'users',
              localField: 'personId',
              foreignField: '_id',
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
              address: {
                $cond: [
                  { $eq: ['$type', 'person'] },
                  '$person.profile.hometown',
                  '$address'
                ]
              },
              avatarKey: {
                $cond: [
                  { $eq: ['$type', 'person'] },
                  appHelper.getUserAvatarKeyPipeline(
                    '$person.profile.avatarKey'
                  ),
                  appHelper.getOrganizationLogoPipeline('$organization.image')
                ]
              }
            }
          }
        ],
        as: 'cdTrAccounts'
      }
    },
    {
      $project: {
        _id: 1,
        accountInfo: 1,
        agentInfo: 1,
        branchInfo: 1,
        cdTrInfos: {
          $cond: [
            { $eq: ['$partner.accountType', 'broker'] },
            ['$cdTrAccount'],
            '$cdTrAccounts'
          ]
        },
        invoices: {
          amount: 1,
          invoiceId: 1,
          invoiceSerialId: 1,
          isFinalSettlement: 1,
          isPayable: 1
        },
        mainTenant: {
          _id: 1,
          name: 1,
          avatarKey: 1
        },
        manualRefundReason: 1,
        meta: {
          bankRef: 1,
          cdTrName: 1,
          cdTrAccountNumber: 1,
          dbTrName: 1,
          dbTrAccountNumber: 1,
          kidNumber: 1
        },
        numberOfFails: 1,
        otherTenants: {
          _id: 1,
          name: 1,
          avatarKey: 1
        },
        partner: 1,
        partiallyRefunded: 1,
        paymentDate: 1,
        paymentId: 1,
        paymentReason: 1,
        paymentType: 1,
        propertyId: 1,
        propertyInfo: 1,
        refundBankRef: 1,
        refundStatus: 1,
        refundPaymentStatus: 1,
        refundedMeta: {
          refundPaymentId: 1,
          amount: 1
        },
        refundToAccountNumber: 1,
        refundToAccountName: 1,
        status: 1,
        type: 1,
        amount: 1,
        refunded: 1,
        isFinalSettlementDone: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    { $eq: ['$invoiceInfo.invoiceType', 'landlord_invoice'] },
                    { $eq: ['$invoiceInfo.isFinalSettlement', true] },
                    { $eq: ['$invoiceInfo.isPayable', true] }
                  ]
                },
                then: false
              },
              {
                case: { $eq: ['$contractInfo.isFinalSettlementDone', true] },
                then: true
              }
            ],
            default: false
          }
        },
        invoiceInfo: 1,
        refundableAmount: 1,
        additionalTaxInfo: 1
      }
    }
  ]
  const [invoicePaymentDetails = {}] = await InvoicePaymentCollection.aggregate(
    pipeline
  )
  if (!size(invoicePaymentDetails)) {
    throw new CustomError(404, 'Payment details data not found!')
  }
  return invoicePaymentDetails
}

export const queryInvoicePaymentDetails = async (req) => {
  const { query = {}, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['paymentId'], query)
  const { partnerId } = user
  query.partnerId = partnerId
  const invoicePaymentsData = await getInvoicePaymentDetailsForQuery(query)
  return invoicePaymentsData
}

export const queryInvoicePaymentsSummary = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const preparedQuery = await prepareInvoicePaymentsQuery(body)
  const [totalCounts = {}] = (await getTotalCounts(preparedQuery)) || []
  const {
    totalPaymentAmount = 0,
    totalRegisteredAmount = 0,
    totalUnspecifiedAmount = 0,
    totalRefundAmount = 0
  } = totalCounts

  return {
    totalPaymentAmount,
    totalRegisteredAmount,
    totalUnspecifiedAmount,
    totalRefundAmount
  }
}

export const prepareInvoicePaymentsQuery = async (params) => {
  const query = {}
  const tenantIdsObj = {}
  const statusAndTypeObj = {}
  console.log('=== Qparams', JSON.stringify(params))
  if (size(params)) {
    query.partnerId = params.partnerId

    //Set branch filters in query
    if (params.branchId) query.branchId = params.branchId
    //Set agent filters in query
    if (params.agentId) query.agentId = params.agentId
    //Set account filters in query
    if (params.accountId) query.accountId = params.accountId
    //Set property filters in query
    if (params.propertyId) query.propertyId = params.propertyId
    //Set tenant filters in query
    if (params.tenantId) {
      tenantIdsObj.$or = [
        { tenantId: params.tenantId },
        { tenants: { $elemMatch: { tenantId: params.tenantId } } }
      ]
    }

    if (params.invoiceSummaryId) {
      let invoiceId = params.invoiceId
      if (!invoiceId) {
        const summaryInfo = await invoiceSummaryHelper.getInvoiceSummary(
          {
            _id: params.invoiceSummaryId
          },
          null
        )
        invoiceId = summaryInfo?.invoiceId || ''
      }

      query.invoices = { $elemMatch: { invoiceId } }
    }

    //For landlord dashboard
    if (params.context && params.context === 'landlordDashboard') {
      if (params.userId) {
        const accountIds = await accountHelper.getAccountIdsByQuery({
          personId: params.userId
        })

        query.accountId = { $in: accountIds }
      }
    }

    //Set dateRange filters in query
    if (
      size(params.dateRange) &&
      params.dateRange.startDate &&
      params.dateRange.endDate
    ) {
      query.paymentDate = {
        $gte: new Date(params.dateRange.startDate),
        $lte: new Date(params.dateRange.endDate)
      }
    }

    //Set dateRange in query for export data
    if (
      params.download &&
      size(params.dateRange) &&
      params.dateRange.startDate_string &&
      params.dateRange.endDate_string
    ) {
      const startDate = (
        await appHelper.getActualDate(
          params.partnerId,
          true,
          params.dateRange.startDate_string
        )
      )
        .startOf('day')
        .toDate()
      const endDate = (
        await appHelper.getActualDate(
          params.partnerId,
          true,
          params.dateRange.endDate_string
        )
      )
        .endOf('day')
        .toDate()

      if (startDate && endDate) {
        query.paymentDate = {
          $gte: startDate,
          $lte: endDate
        }
      }
    }

    // Set status and type filters in query
    const orQueryArray = []

    if (params.status && !params.isInvoicesTab) {
      const status = compact(params.status)
      if (status.length > 0) orQueryArray.push({ status: { $in: status } })
    }
    if (params.type) {
      const type = compact(params.type)
      if (type.length > 0) orQueryArray.push({ type: { $in: type } })
    }

    if (size(orQueryArray)) statusAndTypeObj.$or = orQueryArray
    // For top search bar
    if (params.hasOwnProperty('invoiceSerialId')) {
      const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
        partnerId: params.partnerId,
        invoiceSerialId: params.invoiceSerialId
      })
      query['invoices.invoiceId'] = {
        $in: invoiceIds
      }
    }
    if (params.hasOwnProperty('amount')) query.amount = params.amount
    //Set payments amount filters in query.
    let searchOrQuery = {}
    if (params.searchKeyword) {
      const parsedSearchKeyword = parseInt(params.searchKeyword)
      if (!isNaN(parsedSearchKeyword)) {
        const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
          partnerId: params.partnerId,
          invoiceSerialId: parsedSearchKeyword
        })
        searchOrQuery = {
          $or: [
            { amount: parsedSearchKeyword },
            { 'invoices.invoiceId': { $in: invoiceIds } }
          ]
        }
      } else {
        query._id = 'nothing'
      }
    }

    query.$and = [tenantIdsObj, statusAndTypeObj, searchOrQuery]

    if (params.bankReferenceId) query['meta.bankRef'] = params.bankReferenceId

    // Set type status  in query
    if (params.paymentType) {
      const paymentType = compact(params.paymentType)

      if (paymentType.length > 0) query.paymentType = { $in: paymentType }
    }
    if (!size(params.type) && params.refundStatus) {
      const typeInPayment = compact(params.refundStatus)

      if (size(typeInPayment)) query.refundStatus = { $in: typeInPayment }
    }
    if (size(params.type) && params.refundStatus) {
      const typeInPayment = compact(params.refundStatus)

      if (typeInPayment.length > 0) query.refundStatus = { $in: typeInPayment }
    }
    if (params.contractId && params.leaseSerial) {
      const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
        params.contractId,
        params.leaseSerial
      )
      query.invoices = { $elemMatch: { invoiceId: { $in: invoiceIds } } }
    }

    if (params.contractId) query.contractId = params.contractId

    if (size(params.createdDateRange)) {
      appHelper.validateCreatedAtForQuery(params.createdDateRange)
      const { startDate, endDate } = params.createdDateRange
      query.createdAt = {
        $gte: new Date(startDate),
        $lte: new Date(endDate)
      }
      console.log('=== Final createdAt is', JSON.stringify(query.createdAt))
    }
  }
  return query
}

export const getPaymentsForExcelManager = async (queryData) => {
  const { query, options, dateFormat, timeZone } = queryData
  const { sort, skip, limit } = options

  const pipeline = [
    {
      $match: query
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
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
      $lookup: {
        from: 'invoices',
        localField: 'invoiceId',
        foreignField: '_id',
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
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
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
      $project: {
        invoiceId: '$invoice.invoiceSerialId',
        date: {
          $dateToString: {
            format: dateFormat,
            date: '$paymentDate',
            timezone: timeZone
          }
        },
        tenantId: '$tenant.serial',
        tenant: '$tenant.name',
        objectId: '$property.serial',
        property: {
          $concat: [
            { $ifNull: ['$property.location.name', ''] },
            {
              $cond: [
                { $ifNull: ['$property.location.postalCode', false] },
                { $concat: [', ', '$property.location.postalCode'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.city', false] },
                { $concat: [', ', '$property.location.city'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.country', false] },
                { $concat: [', ', '$property.location.country'] },
                ''
              ]
            }
          ]
        },
        apartmentId: '$property.apartmentId',
        status: 1,
        amount: {
          $cond: {
            if: { $eq: ['$refundStatus', 'canceled'] },
            then: 0,
            else: {
              $cond: {
                if: { $ifNull: ['$amount', false] },
                then: '$amount',
                else: 0
              }
            }
          }
        },
        refundStatus: 1,
        partiallyRefunded: 1
      }
    }
  ]

  const payments = await InvoicePaymentCollection.aggregate(pipeline)
  return payments || []
}

export const paymentDataForExcelCreator = async (params, options) => {
  const { partnerId = '', userId = '' } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const userInfo = await userHelper.getAnUser({ _id: userId })
  const userLang = userInfo ? userInfo.getLanguage() : ''
  const invoicesPaymentsQuery = await prepareInvoicePaymentsQuery(params)
  console.log(
    '=== invoicesPaymentsQuery',
    JSON.stringify(invoicesPaymentsQuery)
  )
  const dataCount = await countInvoicePayments(invoicesPaymentsQuery)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const queryData = {
    query: invoicesPaymentsQuery,
    options,
    dateFormat,
    timeZone,
    language: userLang
  }

  const invoicesPayments = await getPaymentsForExcelManager(queryData)

  if (size(invoicesPayments)) {
    for (const invoicesPayment of invoicesPayments) {
      let text = ''
      if (invoicesPayment.status) {
        text = appHelper.translateToUserLng(
          'common.filters.' + invoicesPayment.status,
          userLang
        )
      }
      if (invoicesPayment.refundStatus) {
        text = appHelper.translateToUserLng(
          'payments.refundStatus.' + invoicesPayment.refundStatus,
          userLang
        )
      }
      if (invoicesPayment.partiallyRefunded) {
        text =
          text +
          ', ' +
          appHelper.translateToUserLng('payments.partially_refunded', userLang)
      }

      invoicesPayment.status = text
    }
  }
  return {
    data: invoicesPayments,
    total: dataCount
  }
}

export const queryForPaymentExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_payments') {
    const paymentData = await paymentDataForExcelCreator(queueInfo.params, {
      skip,
      limit,
      sort
    })
    return paymentData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const queryForPaymentXml = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query } = body
  const { paymentId } = query
  const paymentInfo = await getInvoicePayment({ _id: paymentId })
  return paymentInfo
}

export const queryForPaymentLambda = async (req) => {
  const { body, user = {} } = req
  console.log('body ', body)
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query } = body
  const { receivedFileName, nodeIndex } = query
  const paymentInfo = await getInvoicePayment({ receivedFileName, nodeIndex })
  return paymentInfo
}

const preparePipelineForAggregation = (paymentId) => {
  const pipeline = [
    {
      $match: {
        _id: paymentId
      }
    },
    {
      $lookup: {
        from: 'invoices',
        as: 'invoiceInfo',
        let: { invoiceId: '$invoiceId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [{ $eq: ['$_id', '$$invoiceId'] }]
              }
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'partners',
        as: 'partner',
        let: { partnerId: '$partnerId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [{ $eq: ['$_id', '$$partnerId'] }]
              }
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        as: 'partnerSettings',
        let: { partnerId: '$partnerId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [{ $eq: ['$partnerId', '$$partnerId'] }]
              }
            }
          }
        ]
      }
    },
    { $unwind: { path: '$invoiceInfo', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$partner', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true } }
  ]
  return pipeline
}

export const getRelationalDataForAddPayment = async (req) => {
  const { body, user = {} } = req
  console.log('body getRelationalDataForAddPayment', body)
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query } = body
  const { paymentId } = query
  const pipeline = preparePipelineForAggregation(paymentId)
  const [payment = {}] =
    (await InvoicePaymentCollection.aggregate(pipeline)) || []
  return payment
}

export const getInvoicePaymentByAggregation = async (pipeline) => {
  const payment = (await InvoicePaymentCollection.aggregate(pipeline)) || []
  return payment
}

export const getPaymentStatusArrayForNETSReceivedFile = async (
  netsReceivedFileId
) => {
  if (!netsReceivedFileId)
    throw new CustomError(400, 'Missing netsReceivedFileId')

  const pipelines = [
    { $match: { netsReceivedFileId } },
    {
      $group: {
        _id: null,
        payments: {
          $push: {
            partnerId: {
              $cond: [
                { $ifNull: ['$appPartnerId', false] },
                '$appPartnerId',
                '$partnerId'
              ]
            },
            paymentId: '$_id',
            status: '$status',
            transactionType: 'CRDT'
          }
        }
      }
    }
  ]
  const [paymentsWithNETSReceivedFileId = {}] =
    (await getInvoicePaymentByAggregation(pipelines)) || []
  const { payments } = paymentsWithNETSReceivedFileId || {}

  return payments
}

export const getCreditTransferInfoForRefundPayment = async (
  refundPaymentIds = {}
) => {
  if (!size(refundPaymentIds)) return false

  const pipeline = [
    { $match: { _id: { $in: refundPaymentIds } } },
    {
      $lookup: {
        as: 'invoice',
        from: 'invoices',
        localField: 'invoiceId',
        foreignField: '_id'
      }
    },
    { $unwind: { path: '$invoice', preserveNullAndEmptyArrays: true } },
    {
      $addFields: {
        newDebtorAccountId: {
          $cond: [
            { $ifNull: ['$invoice.invoiceAccountNumber', false] },
            '$invoice.invoiceAccountNumber',
            null
          ]
        }
      }
    },
    {
      $project: {
        _id: 0,
        accountId: 1,
        amount: { $multiply: ['$amount', -1] },
        contractId: 1,
        creditorAccountId: '$refundToAccountNumber',
        debtorAccountId: '$newDebtorAccountId',
        paymentId: '$_id',
        paymentReferenceId: '$bankReferenceId',
        status: 'new'
      }
    }
  ]
  const creditTransferData = await InvoicePaymentCollection.aggregate(pipeline)

  return map(creditTransferData, (creditTransfer) => {
    creditTransfer.paymentInstrId = nid(17)
    creditTransfer.paymentEndToEndId = nid(17)
    return creditTransfer
  })
}

export const getCollectionIdsForApproval = async (req) => {
  await appHelper.validatePartnerAppRequestData(req, ['type'])
  const { body = {} } = req
  const { partnerId, type } = body
  let collectionIds
  if (type === 'payment') {
    collectionIds =
      (await InvoicePaymentCollection.distinct('_id', {
        partnerId,
        refundStatus: 'pending_for_approval'
      })) || []
  }
  if (type === 'payout') {
    collectionIds =
      (await PayoutCollection.distinct('_id', {
        partnerId,
        status: 'pending_for_approval'
      })) || []
  }
  return collectionIds
}

export const pendingPaymentsList = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body = {}, user } = req
  const { query = {}, options = {}, partnerId } = body
  const { propertyId = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (propertyId) totalDocumentsQuery.propertyId = propertyId
  appHelper.validateSortForQuery(options.sort)
  const preparedQuery = {
    partnerId,
    refundStatus: { $in: ['pending_for_approval'] }
  }
  body.query = preparedQuery

  const isAllowed = await getPermissionForApprovingPayoutsOrPayments(user)
  if (!isAllowed) {
    throw new CustomError(403, 'User is not permitted!')
  }

  const invoicePaymentsData = await getInvoicePaymentsForQuery(body)
  const filteredDocuments = await countInvoicePayments(preparedQuery)
  const totalDocuments = await countInvoicePayments(totalDocumentsQuery)
  const esignInfo = await getAllPaymentsEsignInfo(partnerId)
  return {
    data: invoicePaymentsData,
    metaData: {
      filteredDocuments,
      totalDocuments
    },
    actions: esignInfo
  }
}

export const getPermissionForApprovingPayoutsOrPayments = async (user) => {
  const { partnerId, roles, userId } = user
  const isPartnerAccountingUser = roles.includes('partner_accounting')
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const remittanceApprovePersonsIds =
    partnerSettings?.directRemittanceApproval?.persons || []
  const isUserHasDRPermission = remittanceApprovePersonsIds.includes(userId)

  return !!(isUserHasDRPermission && isPartnerAccountingUser)
}

export const preparedInsurancePaymentsQuery = async (params) => {
  const query = {}
  query.isDepositInsurancePayment = true
  if (params.status) {
    query.status = { $in: params.status }
  }
  if (params.type) {
    query.paymentType = params.type
  }
  if (
    params.dateRange &&
    params.dateRange.startDate &&
    params.dateRange.endDate
  ) {
    query.paymentDate = {
      $gte: new Date(params.dateRange.startDate),
      $lte: new Date(params.dateRange.endDate)
    }
  }
  if (params.hasOwnProperty('amount')) {
    const parsedAmount = Number(params.amount)
    if (!!parsedAmount) {
      query.amount = parsedAmount
    } else {
      query._id = 'nothing'
    }
  }
  if (params.hasOwnProperty('serialId')) {
    const parsedSerialId = parseInt(params.serialId)
    if (!!parsedSerialId) {
      const invoiceIds = await appInvoiceHelper.getUniqueFieldValue('_id', {
        serialId: params.serialId
      })
      query.appInvoiceId = { $in: invoiceIds }
    } else {
      query._id = 'nothing'
    }
  }
  if (params.searchKeyword) {
    const parsedSearchKeyword = parseInt(params.searchKeyword)
    if (!isNaN(parsedSearchKeyword)) {
      const invoiceIds =
        (await appInvoiceHelper.getUniqueFieldValue('_id', {
          serialId: parsedSearchKeyword
        })) || []
      query['$or'] = [
        { amount: parsedSearchKeyword },
        { appInvoiceId: { $in: invoiceIds } }
      ]
    } else {
      query._id = 'nothing'
    }
  }
  return query
}

export const canApproveDirectRemittances = async (user) => {
  const { partnerId, userId } = user
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const remittanceApprovePersonsIds =
    partnerSettings?.directRemittanceApproval?.persons || []
  const isUserHasDRPermission = remittanceApprovePersonsIds.includes(userId)
  if (!isUserHasDRPermission) {
    throw new CustomError(403, 'You do not have permission to approve')
  }
}
