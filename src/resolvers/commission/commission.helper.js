import { clone, extend, find, map, pick, size, uniq } from 'lodash'

import { CommissionCollection } from '../models'
import {
  accountHelper,
  addonHelper,
  appHelper,
  appQueueHelper,
  correctionHelper,
  invoiceHelper,
  logHelper,
  partnerSettingHelper,
  payoutHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import { CustomError } from '../common'

export const prepareLogData = (commission) => {
  const { partnerId, _id, type, amount } = commission
  const logData = pick(commission, [
    'accountId',
    'propertyId',
    'agentId',
    'tenantId',
    'branchId',
    'invoiceId'
  ])
  logData.partnerId = partnerId
  logData.context = 'commission'
  logData.action = 'added_new_commission'
  logData.commissionId = _id
  if (type && amount) {
    logData.meta = [
      {
        field: type,
        value: amount
      }
    ]
  }
  logData.visibility = logHelper.getLogVisibility(
    { context: 'commission' },
    commission
  )
  logData.createdBy = 'SYSTEM'
  return logData
}

export const prepareAssignmentIncomeData = async (
  invoiceData,
  contractAddonsMeta
) => {
  const insertDataList = []
  for (const addonMetaInfo of contractAddonsMeta) {
    if (addonMetaInfo.type === 'assignment') {
      const addonIncomeData = cleanByCommissionSchema({
        ...invoiceData,
        invoiceId: invoiceData._id
      })
      addonIncomeData.type = 'assignment_addon_income'
      addonIncomeData.amount = await appHelper.convertTo2Decimal(
        addonMetaInfo.total || 0
      )
      addonIncomeData.addonId = addonMetaInfo.addonId
      if (addonIncomeData.amount !== 0) {
        insertDataList.push(addonIncomeData)
      }
    }
  }
  return insertDataList
}

export const prepareManagementAddData = async (params = {}) => {
  const { invoiceData, invoiceCommissionableTotal, propertyContractInfo } =
    params
  const rentalManagementCommissionData = cleanByCommissionSchema({
    ...invoiceData,
    invoiceId: invoiceData._id
  })
  // Calculate commission by rental management contract
  let rentalManagementCommission = 0
  const { rentalManagementCommissionType, rentalManagementCommissionAmount } =
    propertyContractInfo
  if (rentalManagementCommissionType === 'fixed') {
    rentalManagementCommission = rentalManagementCommissionAmount || 0
  } else if (rentalManagementCommissionType === 'percent') {
    rentalManagementCommission = rentalManagementCommissionAmount
      ? invoiceCommissionableTotal * (rentalManagementCommissionAmount / 100)
      : 0
  }
  // Set rental management type
  rentalManagementCommissionData.type = 'rental_management_contract'
  rentalManagementCommissionData.amount = await appHelper.convertTo2Decimal(
    rentalManagementCommission || 0
  )
  return rentalManagementCommissionData
}

export const getAddonCommissionPercent = async (invoiceAddonInfo) => {
  const addonInfo = await addonHelper.getAddonById(invoiceAddonInfo.addonId)
  const isEnableAddonCommission = !!(addonInfo && addonInfo.enableCommission)
  const addonCommissionPercent =
    isEnableAddonCommission &&
    addonInfo.commissionPercentage &&
    addonInfo.commissionPercentage > 0
      ? addonInfo.commissionPercentage
      : ''
  return { addonCommissionPercent, isEnableAddonCommission }
}

export const isPrepareDataByAddon = (
  invoiceAddonInfo,
  propertyContractInfo,
  addonCommissionPercent
) =>
  size(invoiceAddonInfo) &&
  invoiceAddonInfo.addonId &&
  (propertyContractInfo.rentalManagementCommissionType === 'percent' ||
    addonCommissionPercent)

export const getAddonMetaByCorrectionAddon = async (invoiceAddonInfo) => {
  const correctionInfo = await correctionHelper.getCorrection({
    _id: invoiceAddonInfo.correctionId
  })
  const addonsMetaData =
    correctionInfo && size(correctionInfo.addons) ? correctionInfo.addons : []
  return addonsMetaData
}

export const prepareAddonCommissionData = async (params) => {
  const commissionsData = []
  const {
    addonsMeta,
    invoiceData,
    propertyContractInfo,
    invoiceCommissionableTotal
  } = params
  const contractAddons =
    propertyContractInfo && size(propertyContractInfo.addons)
      ? propertyContractInfo.addons
      : []
  let commissionTotal = invoiceCommissionableTotal
  for (const invoiceAddonInfo of addonsMeta) {
    const { addonCommissionPercent, isEnableAddonCommission } =
      await getAddonCommissionPercent(invoiceAddonInfo)
    if (
      !(
        isEnableAddonCommission &&
        isPrepareDataByAddon(
          invoiceAddonInfo,
          propertyContractInfo,
          addonCommissionPercent
        )
      )
    ) {
      continue
    }
    const addonCommissionData = cleanByCommissionSchema({
      ...invoiceData,
      invoiceId: invoiceData._id
    })
    let addonsMetaData = clone(contractAddons)
    if (invoiceAddonInfo.correctionId) {
      addonsMetaData = await getAddonMetaByCorrectionAddon(invoiceAddonInfo)
    }
    if (!size(addonsMetaData)) {
      continue
    }
    const findCommissionableAddon = find(
      addonsMetaData,
      (addonInfo) => addonInfo && addonInfo.addonId === invoiceAddonInfo.addonId
    )
    if (!size(findCommissionableAddon)) {
      continue
    }
    const commissionAmount =
      addonCommissionPercent ||
      propertyContractInfo.rentalManagementCommissionAmount
    commissionTotal = commissionTotal - (invoiceAddonInfo.total || 0)
    const calculatedCommissionAmount =
      (invoiceAddonInfo.total || 0) * (commissionAmount / 100)
    addonCommissionData.type = 'addon_commission'
    addonCommissionData.amount = await appHelper.convertTo2Decimal(
      calculatedCommissionAmount || 0
    )
    addonCommissionData.addonId = invoiceAddonInfo.addonId
    if (addonCommissionData.amount !== 0) {
      commissionsData.push(addonCommissionData)
    }
  }
  return { commissionsData, commissionTotal }
}

export const cleanByCommissionSchema = (invoiceData) => {
  const commissionFields = [
    'agentId',
    'branchId',
    'partnerId',
    'accountId',
    'propertyId',
    'tenantId',
    'invoiceId',
    'amount',
    'type',
    'note',
    'payoutId',
    'refundCommissionId',
    'refundCommissionAmount',
    'commissionId',
    'addonId',
    'tenants',
    'serialId',
    'landlordInvoiceId',
    'createdAt',
    'createdBy'
  ]
  return pick(invoiceData, commissionFields)
}

export const prepareBrokeringCommissionData = async (params) => {
  const { propertyContractInfo, monthlyRentAmount } = params
  const { invoiceData } = params
  const brokeringCommissionData = cleanByCommissionSchema({
    ...invoiceData,
    invoiceId: invoiceData._id
  })
  const { brokeringCommissionAmount, brokeringCommissionType } =
    propertyContractInfo || {}
  let updatedBrokeringCommissionAmount = 0
  if (brokeringCommissionType === 'fixed') {
    updatedBrokeringCommissionAmount = brokeringCommissionAmount
  } else if (brokeringCommissionType === 'percent') {
    updatedBrokeringCommissionAmount =
      monthlyRentAmount * (brokeringCommissionAmount / 100)
  }
  brokeringCommissionData.type = 'brokering_contract'
  brokeringCommissionData.amount = await appHelper.convertTo2Decimal(
    updatedBrokeringCommissionAmount || 0
  )
  return brokeringCommissionData
}

export const getMonthlyRentTotal = (invoiceContent) => {
  if (!size(invoiceContent)) {
    return 0
  }
  const monthlyRentContent = invoiceContent.find(
    (content) => content.type === 'monthly_rent'
  )
  return monthlyRentContent.total || 0
}

export const getCommission = async (query, session, populate = []) => {
  const commission = await CommissionCollection.findOne(query)
    .populate(populate)
    .session(session)
  return commission
}

export const getCommissions = async (query, session) => {
  const commissions = await CommissionCollection.find(query).session(session)
  return commissions
}

const getInvoicePipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            invoiceSerialId: 1,
            invoiceTotal: 1
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

const getPayoutPipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'landlordInvoiceId',
      foreignField: '_id',
      as: 'landlordInvoiceInfo'
    }
  },
  {
    $unwind: {
      path: '$landlordInvoiceInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      commissionMeta: {
        $first: {
          $filter: {
            input: { $ifNull: ['$landlordInvoiceInfo.commissionsMeta', []] },
            as: 'commission',
            cond: {
              $eq: ['$$commission.commissionId', '$_id']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      payoutIds: { $ifNull: ['$commissionMeta.payoutsIds', []] }
    }
  },
  {
    $lookup: {
      from: 'payouts',
      localField: 'payoutIds',
      foreignField: '_id',
      as: 'payoutsInfo'
    }
  }
]

const getAgentPipeline = () => [
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

const getAccountPipeline = () => [
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

// Needed for commission details
export const getTenantPipeline = () => [
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
            as: 'userInfo'
          }
        },
        {
          $unwind: {
            path: '$userInfo',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$userInfo.profile.avatarKey'
            )
          }
        }
      ],
      as: 'mainTenantInfo'
    }
  },
  {
    $addFields: {
      otherTenants: {
        $filter: {
          input: { $ifNull: ['$tenants', []] },
          as: 'tenant',
          cond: {
            $not: { $eq: ['$$tenant.tenantId', '$tenantId'] }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'otherTenants.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'userInfo'
          }
        },
        {
          $unwind: {
            path: '$userInfo',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$userInfo.profile.avatarKey'
            )
          }
        }
      ],
      as: 'otherTenantsInfo'
    }
  },
  {
    $addFields: {
      tenantsInfo: {
        $concatArrays: ['$mainTenantInfo', '$otherTenantsInfo']
      }
    }
  }
]

export const getBranchPipeline = () => [
  {
    $lookup: {
      from: 'branches',
      localField: 'branchId',
      foreignField: '_id',
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

const getAddonNamePipeline = () => [
  {
    $lookup: {
      from: 'products_services',
      localField: 'addonId',
      foreignField: '_id',
      as: 'mainAddon'
    }
  },
  appHelper.getUnwindPipeline('mainAddon'),
  {
    $lookup: {
      from: 'commissions',
      localField: 'commissionId',
      foreignField: '_id',
      as: 'creditNoteCommission'
    }
  },
  appHelper.getUnwindPipeline('creditNoteCommission'),
  {
    $lookup: {
      from: 'products_services',
      localField: 'creditNoteCommission.addonId',
      foreignField: '_id',
      as: 'creditNoteAddon'
    }
  },
  appHelper.getUnwindPipeline('creditNoteAddon'),
  {
    $addFields: {
      addonName: {
        $cond: [
          { $ifNull: ['$mainAddon', false] },
          '$mainAddon.name',
          '$creditNoteAddon.name'
        ]
      }
    }
  }
]

export const getCommissionsForQuery = async (params = {}) => {
  const { query, options } = params
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
    ...getInvoicePipeline(),
    ...getPayoutPipeline(),
    ...getAgentPipeline(),
    ...getAccountPipeline(),
    ...appHelper.getCommonPropertyInfoPipeline(),
    ...getTenantPipeline(),
    ...getBranchPipeline(),
    ...getAddonNamePipeline(),
    {
      $project: {
        _id: 1,
        serialId: 1,
        type: 1,
        addonName: 1,
        amount: 1,
        createdAt: 1,
        invoiceInfo: 1,
        payoutsInfo: {
          _id: 1,
          serialId: 1,
          amount: 1
        },
        agentInfo: 1,
        accountInfo: 1,
        propertyInfo: {
          _id: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          },
          listingTypeId: 1,
          propertyTypeId: 1,
          apartmentId: 1,
          imageUrl: 1
        },
        tenantsInfo: 1,
        branchInfo: {
          _id: 1,
          name: 1
        }
      }
    }
  ]
  const commissions = await CommissionCollection.aggregate(pipeline)
  return commissions
}

export const queryCommissions = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.query.partnerId = partnerId
  const { query } = body
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  body.query = await prepareCommissionsQuery(body.query)
  const commissions = await getCommissionsForQuery(body)
  const filteredDocuments = await countCommissions(body.query)
  const totalDocuments = await countCommissions(totalDocumentsQuery)
  return {
    data: commissions,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const countCommissions = async (query) => {
  const numberOfCommissions = await CommissionCollection.find(
    query
  ).countDocuments()
  return numberOfCommissions
}

export const getAccountingType = (commissionType) => {
  const accountingType = {
    rental_management_contract: 'management_commission',
    brokering_contract: 'brokering_commission',
    assignment_addon_income: 'addon',
    addon_commission: 'addon_commission'
  }
  if (accountingType[commissionType]) {
    return accountingType[commissionType]
  }
  return commissionType
}

export const isCommissionTransactionExists = async (
  commission,
  accountingType,
  session
) => {
  const {
    partnerId = '',
    invoiceId = '',
    _id = '',
    amount,
    addonId = ''
  } = commission
  const query = {
    partnerId,
    invoiceId,
    commissionId: _id,
    amount,
    type: 'commission'
  }
  query.subType = accountingType
  if (accountingType === 'addon') {
    query.addonId = addonId
  }
  const isExists = !!(await transactionHelper.getTransaction(query, session))
  return isExists
}

export const prepareTransactionData = async (
  commission,
  accountingType,
  transactionEvent,
  session
) => {
  const { invoiceId = '', partnerId = '', _id = '' } = commission
  const invoice = await invoiceHelper.getInvoice({ _id: invoiceId }, session)
  const { contractId = '', invoiceStartOn } = invoice || {}
  let transactionData = pick(commission, [
    'partnerId',
    'tenantId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'invoiceId',
    'payoutId',
    'amount',
    'landlordInvoiceId',
    'createdBy'
  ])
  const transactionPeriod =
    await transactionHelper.getFormattedTransactionPeriod(
      invoiceStartOn,
      partnerId
    )
  if (transactionPeriod) {
    transactionData.period = transactionPeriod
  }

  if (transactionEvent === 'legacy') {
    transactionData.createdAt = commission.createdAt
    transactionData.transactionEvent = transactionEvent
  }

  transactionData.commissionId = _id
  transactionData.contractId = contractId
  transactionData.type = 'commission'

  //Assignment addon commission should be processed as addon
  let options = {}

  if (accountingType === 'addon' || accountingType === 'addon_commission') {
    options = { addonId: commission.addonId }
    transactionData.addonId = commission.addonId
  }

  const accountingParams = {
    partnerId,
    accountingType,
    options
  }
  const addonTransactionData =
    await transactionHelper.getAccountingDataForTransaction(
      accountingParams,
      session
    )
  transactionData = extend(transactionData, addonTransactionData)
  return transactionData
}

export const prepareCommissionsQuery = async (params) => {
  const query = {}
  const partnerId = params.partnerId

  if (size(params)) {
    query.partnerId = partnerId

    //Set branch filters in query
    if (params.branchId) query.branchId = params.branchId
    //Set agent filters in query
    if (params.agentId) query.agentId = params.agentId
    //Set account filters in query
    if (params.accountId) query.accountId = params.accountId
    //Set property filters in query
    if (params.propertyId) query.propertyId = params.propertyId
    if (params.commissionId) query._id = params.commissionId
    if (params.type) query.type = params.type
    if (params.context && params.context === 'landlordDashboard') {
      const accountIds =
        uniq(
          map(
            await accountHelper.getAccounts({ personId: params.userId }),
            '_id'
          )
        ) || []

      if (size(accountIds)) query.accountId = { $in: accountIds }
    }
    //Set tenant filters in query
    if (params.tenantId) {
      query.$or = [
        { tenantId: params.tenantId },
        { tenants: { $elemMatch: { tenantId: params.tenantId } } }
      ]
    }
    //Set invoice filters in query
    if (params.invoiceId) query.invoiceId = params.invoiceId
    //Set lease filter for sub commission
    if (params.contractId) {
      const invoiceIds = map(
        await invoiceHelper.getInvoices({
          contractId: params.contractId,
          partnerId
        }),
        '_id'
      )

      query.invoiceId = { $in: invoiceIds }
    }
    if (size(params.payoutStatus)) {
      const payoutIds = await payoutHelper.getUniqueFieldValues('_id', {
        partnerId,
        status: { $in: params.payoutStatus }
      })
      query.payoutId = {
        $in: payoutIds
      }
    }

    //Set expenses amount filters in query.
    if (params.searchKeyword) {
      if (!isNaN(params.searchKeyword)) {
        query.amount = params.searchKeyword * 1
      } else {
        query._id = 'nothing'
      }
    }
    //Set dateRange filters in query
    if (
      params.dateRange &&
      params.dateRange.startDate &&
      params.dateRange.endDate
    ) {
      query.createdAt = {
        $gte: new Date(params.dateRange.startDate),
        $lte: new Date(params.dateRange.endDate)
      }
    }

    //Set payout sentToNETSOn range in query for export data
    if (
      params.download &&
      params.dateRange &&
      params.dateRange.startDate_string &&
      params.dateRange.endDate_string
    ) {
      const startDate = (
        await appHelper.getActualDate(
          partnerId,
          true,
          params.dateRange.startDate_string
        )
      )
        .startOf('day')
        .toDate()

      const endDate = (
        await appHelper.getActualDate(
          partnerId,
          true,
          params.dateRange.endDate_string
        )
      )
        .endOf('day')
        .toDate()

      if (startDate && endDate) {
        query.createdAt = {
          $gte: startDate,
          $lte: endDate
        }
      }
    }

    if (params.contractId && params.leaseSerial) {
      const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
        params.contractId,
        params.leaseSerial
      )
      query.invoiceId = { $in: invoiceIds }
    }
    if (params.type) {
      if (params.type === 'other_commissions') {
        query.type = {
          $nin: ['brokering_contract', 'rental_management_contract']
        }
      } else {
        query.type = params.type
      }
    }
  }
  return query
}

export const getCommissionForExcelManager = async (queryData) => {
  const {
    query,
    options,
    dateFormat,
    timeZone,
    language = 'no',
    context
  } = queryData
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: {
        ...query
      }
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $lookup: {
        from: 'products_services',
        localField: 'addonId',
        foreignField: '_id',
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
      $addFields: {
        statusText: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    { $eq: ['$invoice.status', 'new'] },
                    { $ne: ['$invoice.invoiceSent', true] }
                  ]
                },
                then: appHelper.translateToUserLng(
                  'common.filters.new',
                  language
                )
              },
              {
                case: {
                  $and: [
                    { $eq: ['$invoice.status', 'created'] },
                    { $ne: ['$invoice.invoiceSent', true] }
                  ]
                },
                then: appHelper.translateToUserLng(
                  'common.filters.created',
                  language
                )
              },
              {
                case: { $eq: ['$invoice.status', 'credited'] },
                then: appHelper.translateToUserLng(
                  'common.filters.credited',
                  language
                )
              },
              {
                case: { $eq: ['$invoice.status', 'lost'] },
                then: appHelper.translateToUserLng(
                  'common.filters.lost',
                  language
                )
              },
              {
                case: {
                  $and: [
                    { $ne: ['$invoice.isPartiallyPaid', true] },
                    { $ne: ['$invoice.isDefaulted', true] },
                    { $eq: ['$invoice.invoiceSent', true] },
                    {
                      $or: [
                        { $eq: ['$invoice.status', 'new'] },
                        { $eq: ['$invoice.status', 'created'] }
                      ]
                    }
                  ]
                },
                then: appHelper.translateToUserLng(
                  'common.filters.sent',
                  language
                )
              },
              {
                case: {
                  $eq: ['$invoice.status', 'paid']
                },
                then: appHelper.translateToUserLng(
                  'common.filters.paid',
                  language
                )
              },
              {
                case: {
                  $and: [
                    { $eq: ['$invoice.status', 'overdue'] },
                    { $ne: ['$invoice.isDefaulted', true] }
                  ]
                },
                then: appHelper.translateToUserLng(
                  'common.filters.unpaid',
                  language
                )
              },
              {
                case: {
                  $eq: ['$invoice.isDefaulted', true]
                },
                then: appHelper.translateToUserLng(
                  'common.filters.defaulted',
                  language
                )
              },
              {
                case: {
                  $eq: ['$invoice.status', 'balanced']
                },
                then: appHelper.translateToUserLng(
                  'common.filters.balanced',
                  language
                )
              }
            ],
            default: null
          }
        },
        tagText: {
          $switch: {
            branches: [
              {
                case: { $eq: ['$invoice.isPartiallyPaid', true] },
                then: appHelper.translateToUserLng(
                  'common.filters.partially_paid',
                  language
                )
              },
              {
                case: { $eq: ['$invoice.isOverPaid', true] },
                then: appHelper.translateToUserLng(
                  'common.filters.overpaid',
                  language
                )
              },
              {
                case: { $eq: ['$invoice.isPartiallyCredited', true] },
                then: appHelper.translateToUserLng(
                  'common.filters.partially_credited',
                  language
                )
              },
              {
                case: { $eq: ['$invoice.isPartiallyBalanced', true] },
                then: appHelper.translateToUserLng(
                  'common.filters.partially_balanced',
                  language
                )
              }
            ],
            default: null
          }
        }
      }
    },
    {
      $lookup: {
        from: 'payouts',
        localField: 'payoutId',
        foreignField: '_id',
        as: 'payout'
      }
    },
    {
      $unwind: {
        path: '$payout',
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
    {
      $project: {
        invoiceId: '$invoice.invoiceSerialId',
        invoiceStatus: {
          $cond: {
            if: {
              $and: [
                { $ne: ['$statusText', null] },
                { $ne: ['$tagText', null] }
              ]
            },
            then: { $concat: ['$statusText', ', ', '$tagText'] },
            else: {
              $cond: {
                if: '$statusText',
                then: '$statusText',
                else: '$tagText'
              }
            }
          }
        },
        serialId: 1,
        date: {
          $dateToString: {
            format: dateFormat,
            date: '$createdAt',
            timezone: timeZone
          }
        },
        type: '$type',
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
        amount: 1,
        payoutId: '$payout.serialId',
        accountId: '$account.serial',
        invoicePaidDate: {
          $cond: {
            if: { $ne: [context, 'landlordDashboard'] },
            then: {
              $dateToString: {
                format: dateFormat,
                date: '$invoice.lastPaymentDate',
                timezone: timeZone
              }
            },
            else: ''
          }
        },
        agent: {
          $cond: {
            if: { $ne: [context, 'landlordDashboard'] },
            then: '$agent.profile.name',
            else: ''
          }
        },
        account: {
          $cond: {
            if: { $ne: [context, 'landlordDashboard'] },
            then: '$account.name',
            else: ''
          }
        },
        addonName: '$addon.name'
      }
    }
  ]
  const commission = await CommissionCollection.aggregate(pipeline)
  return commission || []
}

export const commissionsDataForExcelCreator = async (params, options) => {
  const { partnerId = {}, userId = {} } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const context = params?.context || ''
  const userInfo = await userHelper.getAnUser({ _id: userId })
  const userLanguage = userInfo.getLanguage()
  const commissionsQuery = await prepareCommissionsQuery(params)
  const dataCount = await countCommissions(commissionsQuery)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const queryData = {
    query: commissionsQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage,
    context
  }
  const commissions = await getCommissionForExcelManager(queryData)
  if (size(commissions)) {
    for (const commission of commissions) {
      let typeData = ''
      if (commission.type === 'assignment_addon_income') {
        typeData =
          appHelper.translateToUserLng(
            'transactions.sub_type.addon',
            userLanguage
          ) +
          ': ' +
          commission.addonName
      } else {
        typeData = appHelper.translateToUserLng(
          'contract.' + commission.type,
          userLanguage
        )
      }
      commission.type = typeData
    }
  }
  return {
    data: commissions,
    total: dataCount
  }
}

export const queryCommissionForExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_commissions') {
    const commissionData = await commissionsDataForExcelCreator(
      queueInfo.params,
      {
        skip,
        limit,
        sort
      }
    )
    return commissionData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const getCommissionIdsForLegacyTransaction = async (partnerId) => {
  const commissions = await CommissionCollection.aggregate([
    {
      $match: {
        partnerId
      }
    },
    {
      $group: {
        _id: null,
        commissionIds: { $addToSet: '$_id' }
      }
    }
  ])
  const [commissionInfo = {}] = commissions || []
  const { commissionIds = [] } = commissionInfo
  return commissionIds
}

const getCommissionSummary = async (query = {}) => {
  const pipeline = [
    {
      $match: query
    },
    {
      $project: {
        amount: 1,
        brokeringCommission: {
          $cond: [{ $eq: ['$type', 'brokering_contract'] }, '$amount', 0]
        },
        managementCommission: {
          $cond: [
            { $eq: ['$type', 'rental_management_contract'] },
            '$amount',
            0
          ]
        },
        otherCommission: {
          $cond: [
            {
              $not: {
                $in: [
                  '$type',
                  ['brokering_contract', 'rental_management_contract']
                ]
              }
            },
            '$amount',
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        brokeringCommissionAmount: {
          $sum: '$brokeringCommission'
        },
        managementCommissionAmount: {
          $sum: '$managementCommission'
        },
        otherCommissionAmount: {
          $sum: '$otherCommission'
        },
        totalCommissionAmount: {
          $sum: '$amount'
        }
      }
    }
  ]
  const [summary = {}] = (await CommissionCollection.aggregate(pipeline)) || []
  return summary
}

export const queryCommissionSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  const preparedQuery = await prepareCommissionsQuery(body)
  return await getCommissionSummary(preparedQuery)
}

export const getCommissionForAppHealthCommission = async (partnerId) => {
  const commissionAmount = await CommissionCollection.aggregate(
    preparePipelineForAppHealthTransactionCommission(partnerId)
  )
  return commissionAmount[0] || {}
}

const preparePipelineForAppHealthTransactionCommission = (partnerId) => [
  {
    $match: {
      partnerId
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
      foreignField: 'commissionId',
      as: 'transactions',
      pipeline: [
        {
          $project: {
            amount: 1,
            type: 1,
            commissionId: 1,
            subType: 1,
            totalRounded: {
              $cond: {
                if: { $eq: ['$subType', 'rounded_amount'] },
                then: '$amount',
                else: 0
              }
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      totalRoundedAmount: {
        $sum: '$transactions.totalRounded'
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
      totalCommission: {
        $sum: '$amount'
      },
      missingAmount: {
        $sum: '$missMatchTransactionsAmount'
      },
      missingTransactionsInCommission: {
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
              commissionId: '$_id',
              commissionAmount: '$amount',
              transactions: '$transactions',
              transactionAmounts: '$transactionAmounts'
            },
            else: '$$REMOVE'
          }
        }
      }
    }
  }
]
