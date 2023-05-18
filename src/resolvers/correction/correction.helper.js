import nid from 'nid'
import {
  differenceWith,
  each,
  every,
  extend,
  isEqual,
  map,
  omit,
  pick,
  size
} from 'lodash'
import { CorrectionCollection } from '../models'
import {
  accountHelper,
  annualStatementHelper,
  appHelper,
  appQueueHelper,
  contractHelper,
  correctionHelper,
  finalSettlementHelper,
  invoiceHelper,
  invoiceSummaryHelper,
  listingHelper,
  partnerSettingHelper,
  payoutHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import { appPermission, CustomError } from '../common'
import settingJson from '../../../settings.json'

export const getCorrection = async (query, session, populate = []) => {
  const correction = await CorrectionCollection.findOne(query)
    .session(session)
    .populate(populate)
  return correction
}

export const getCorrections = async (query, session) => {
  const corrections = await CorrectionCollection.find(query).session(session)
  return corrections
}

export const getCorrectionById = async (id, session) => {
  const correction = await CorrectionCollection.findById(id).session(session)
  return correction
}

export const getAggregatedCorrection = async (pipeline = []) => {
  const corrections = (await CorrectionCollection.aggregate(pipeline)) || []
  return corrections
}

export const calculateAddonsTotal = async (addons) => {
  let addonsTotal = 0
  if (size(addons)) {
    each(addons, (addon) => {
      addonsTotal += addon.total || 0
    })
  }
  addonsTotal = await appHelper.convertTo2Decimal((addonsTotal || 0) * 1)
  return addonsTotal
}

export const prepareAddCorrectionData = async (contract, body) => {
  const { addons, addTo, partnerId, propertyId, userId, isNonRent, isVisible } =
    body
  const { _id, accountId, agentId, branchId, rentalMeta } = contract
  const correctionData = {
    accountId,
    addTo,
    addons,
    agentId,
    branchId,
    contractId: _id,
    correctionStatus: 'active',
    createdBy: userId,
    status: 'unpaid',
    propertyId,
    partnerId
  }

  if (isVisible) {
    if (addTo === 'payout') correctionData.isVisibleToLandlord = true
    else correctionData.isVisibleToTenant = true
  }

  if (addTo === 'rent_invoice' && isNonRent) correctionData.isNonRent = true

  if (rentalMeta?.tenantId) correctionData.tenantId = rentalMeta.tenantId
  correctionData.amount = await calculateAddonsTotal(addons)
  if (size(rentalMeta?.tenants)) correctionData.tenants = rentalMeta.tenants

  return correctionData
}
export const getCorrectionInvoicePeriod = async (
  contract = {},
  partnerSettings,
  session
) => {
  let invoiceStartOn = (await appHelper.getActualDate(partnerSettings, true))
    .startOf('month')
    .toDate()
  let invoiceEndOn = (await appHelper.getActualDate(partnerSettings, true))
    .endOf('month')
    .toDate()

  const contractStartDate = await appHelper.getActualDate(
    partnerSettings,
    false,
    contract.rentalMeta?.contractStartDate
  )
  const contractEndDate = contract.rentalMeta?.contractEndDate
    ? await appHelper.getActualDate(
        partnerSettings,
        false,
        contract.rentalMeta?.contractEndDate
      )
    : ''

  const [lastInvoice] = await invoiceHelper.getInvoices(
    { contractId: contract._id, invoiceType: 'invoice' },
    session,
    { limit: 1, sort: { invoiceSerialId: -1 } }
  )

  if (lastInvoice) {
    invoiceStartOn = await appHelper.getActualDate(
      partnerSettings,
      false,
      lastInvoice.invoiceStartOn
    )
    invoiceEndOn = await appHelper.getActualDate(
      partnerSettings,
      false,
      lastInvoice.invoiceEndOn
    )
  }

  if (invoiceStartOn < contractStartDate) invoiceStartOn = contractStartDate
  if (contractEndDate && invoiceEndOn > contractEndDate)
    invoiceEndOn = contractEndDate

  const invoiceStartOnPeriod = Number(
    (
      await appHelper.getActualDate(partnerSettings, true, invoiceStartOn)
    ).format('YYYY') * 1
  )
  const invoiceEndOnPeriod = Number(
    (await appHelper.getActualDate(partnerSettings, true, invoiceEndOn)).format(
      'YYYY'
    ) * 1
  )

  return {
    invoiceEndOnPeriod,
    invoiceStartOnPeriod
  }
}
export const validateCreateCorrectionData = async (
  contract,
  body = {},
  session
) => {
  if (!size(body.addons)) {
    throw new CustomError(404, 'Required addons data')
  }
  if (body.addTo !== 'rent_invoice' && body.isNonRent) {
    throw new CustomError(400, 'Non-rent correction addons are invalid!')
  }
  if (body.addTo !== 'rent_invoice' && body.createInvoice) {
    throw new CustomError(400, 'Create invoice correction addTo are invalid!')
  }
  // For non rent correction  we have to check if all the addon items are non-rent and the total amount is positive in correction
  if (body.isNonRent) {
    const validAddons = every(body.addons, ['isNonRent', true])
    const totalAmount = await correctionHelper.calculateAddonsTotal(body.addons)
    if (!validAddons || totalAmount < 0) {
      throw new CustomError(400, 'Non-rent correction addons are invalid!')
    }
  }

  if (contract.status === 'closed' && contract.isFinalSettlementDone) {
    throw new CustomError(405, 'Final settlement is done for this contract!')
  }

  if (body.addTo === 'rent_invoice' && !body.isNonRent) {
    const correctionInfo = await getAggregatedCorrection([
      {
        $match: {
          contractId: contract._id,
          invoiceId: { $exists: false },
          addTo: 'rent_invoice',
          correctionStatus: 'active',
          isNonRent: { $ne: true }
        }
      },
      {
        $group: {
          _id: null,
          totalAmount: {
            $sum: '$amount'
          }
        }
      }
    ])
    const addonTotal = await calculateAddonsTotal(body.addons)
    const remains =
      (contract?.rentalMeta?.monthlyRentAmount || 0) +
      (correctionInfo[0]?.totalAmount || 0) +
      addonTotal
    if (remains < 0) {
      throw new CustomError(
        400,
        'Total active correction amount must not be less than monthly rent amount'
      )
    }
  }

  // If app admin is creating invoice then don't have to check annual statement period
  const isAppAdmin = await appPermission.isAppAdmin(body.userId)
  if (!isAppAdmin && (body.addTo === 'payout' || body.createInvoice)) {
    const isValidPeriod = await isValidateAnnualStatementPeriodForCorrection(
      body.partnerId,
      body.contractId,
      session
    )
    if (!isValidPeriod) {
      throw new CustomError(
        400,
        'You can not create this correction, as the annual report has already been created for this period'
      )
    }
  }
  return contract
}

export const getNewlyCreatedCorrection = async (correctionId, session) => {
  const [correction] = await CorrectionCollection.aggregate([
    {
      $match: { _id: correctionId }
    },
    {
      $limit: 1
    },
    ...getInvoicePipeline(),
    ...getPropertyPipeline(),
    ...getCreatedByPipeline(),
    ...getFilesInfoPipeline(),
    ...getFinalProjectPipeline()
  ]).session(session)
  return correction
}

export const isCorrectionWillBeUpdated = async (correction, session) => {
  let isCorrectionUpdated = false
  if (correction) {
    const { payoutId, invoiceId, oldData } = correction
    const payout = await payoutHelper.getPayoutById(payoutId, session)
    if (
      !invoiceId &&
      (oldData || !payoutId || (payout && payout.status === 'estimated'))
    ) {
      isCorrectionUpdated = true
    }
  }
  return isCorrectionUpdated
}
const getPayoutInfo = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: 'payoutId',
      foreignField: '_id',
      as: 'payoutInfo'
    }
  },
  {
    $unwind: {
      path: '$payoutInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getContactInfo = () => [
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

const getInvoicePipeline = () => [
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
      from: 'invoices',
      localField: 'landlordInvoiceId',
      foreignField: '_id',
      as: 'landlordInvoice'
    }
  },
  {
    $unwind: {
      path: '$landlordInvoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getPropertyPipeline = () => [
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
              name: 1,
              postalCode: 1,
              city: 1,
              country: 1
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

const getPropertyPipelineForDetails = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            location: {
              apartmentId: '$apartmentId',
              name: 1,
              postalCode: 1,
              city: 1,
              country: 1
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

const getCreatedByPipeline = () => [
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
      as: 'createdByInfo'
    }
  },
  {
    $unwind: {
      path: '$createdByInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getFileUrlPipeline = () => {
  const directive = settingJson.S3.Directives['Files']
  const { folder } = directive
  return {
    $concat: [
      appHelper.getCDNDomain(),
      '/',
      folder,
      '/',
      '$partnerId',
      '/expense/',
      '$name'
    ]
  }
}

const getFilesInfoPipeline = () => [
  {
    $lookup: {
      from: 'files',
      localField: 'files',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: 1,
            title: 1,
            fileUrl: getFileUrlPipeline()
          }
        }
      ],
      as: 'filesInfo'
    }
  }
]

const getFinalProjectPipeline = () => [
  {
    $project: {
      _id: 1,
      correctionSerialId: 1,
      invoiceSerialId: {
        $cond: [
          { $eq: ['$addTo', 'rent_invoice'] },
          '$invoice.invoiceSerialId',
          '$landlordInvoice.invoiceSerialId'
        ]
      },
      invoiceId: {
        $cond: [
          { $eq: ['$addTo', 'rent_invoice'] },
          '$invoice._id',
          '$landlordInvoice._id'
        ]
      },
      addTo: 1,
      correctionStatus: 1,
      amount: 1,
      propertyInfo: 1,
      filesInfo: 1,
      createdByInfo: 1,
      createdAt: 1,
      isNonRent: 1,
      payoutId: 1,
      isFinalSettlementDone: '$contractInfo.isFinalSettlementDone',
      payoutStatus: '$payoutInfo.status'
    }
  }
]

export const getCorrectionsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const corrections = await CorrectionCollection.aggregate([
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
    ...getPayoutInfo(),
    ...getContactInfo(),
    ...getPropertyPipeline(),
    ...getCreatedByPipeline(),
    ...getFilesInfoPipeline(),
    ...getFinalProjectPipeline()
  ])
  return corrections
}

export const countCorrections = async (query, session) => {
  const numberOfCorrections = await CorrectionCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfCorrections
}

export const prepareQueryDataForQueryCorrections = async (query) => {
  const {
    addTo,
    contractId,
    createdAtDateRange,
    invoiceSerialId,
    leaseSerial,
    partnerId,
    periodDateRange,
    searchKeyword = '',
    tenantId
  } = query
  const fieldNameOfInvoiceId =
    addTo === 'rent_invoice' ? 'invoiceId' : 'landlordInvoiceId'
  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    query.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  if (size(periodDateRange)) {
    const { startDate, endDate } = periodDateRange
    query.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  if (tenantId) {
    query['$and'] = [
      {
        $or: [{ tenantId }, { 'tenants.tenantId': tenantId }]
      }
    ]
  }

  if (invoiceSerialId) {
    const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
      invoiceSerialId,
      partnerId
    })
    query[fieldNameOfInvoiceId] = { $in: invoiceIds }
  }

  if (query.hasOwnProperty('searchKeyword')) {
    const intSearchKeyword = parseInt(searchKeyword)
    if (isNaN(intSearchKeyword)) {
      query._id = 'nothing'
    } else {
      const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
        invoiceSerialId: intSearchKeyword,
        partnerId
      })
      query['$or'] = [
        { amount: parseFloat(searchKeyword) },
        { correctionSerialId: intSearchKeyword },
        { [fieldNameOfInvoiceId]: { $in: invoiceIds } }
      ]
    }
  }

  const preparedQuery = omit(query, [
    'createdAtDateRange',
    'invoiceSerialId',
    'periodDateRange',
    'requestFrom',
    'searchKeyword',
    'tenantId'
  ])
  if (contractId && leaseSerial) {
    const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
      contractId,
      leaseSerial
    )
    preparedQuery[fieldNameOfInvoiceId] = { $in: invoiceIds }
    delete preparedQuery.leaseSerial
  }
  return preparedQuery
}

export const queryCorrections = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  const { query, options } = body
  appHelper.checkRequiredFields(['addTo'], query)
  query.partnerId = partnerId
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = {
    partnerId: query.partnerId,
    addTo: query.addTo
  }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  appHelper.validateSortForQuery(options.sort)
  body.query = await prepareQueryDataForQueryCorrections(query)
  const correctionsData = await getCorrectionsForQuery(body)
  const filteredDocuments = await countCorrections(body.query)
  const totalDocuments = await countCorrections(totalDocumentsQuery)
  return {
    data: correctionsData,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const getCorrectionSummary = async (query = {}) => {
  const pipeline = [
    {
      $match: query
    },
    {
      $unwind: { path: '$addons', preserveNullAndEmptyArrays: true }
    },
    {
      $group: {
        _id: '$addons.addonId',
        activeAmount: {
          $sum: {
            $cond: [
              { $ne: ['$correctionStatus', 'cancelled'] },
              '$addons.total',
              0
            ]
          }
        },
        cancelledAmount: {
          $sum: {
            $cond: [
              { $eq: ['$correctionStatus', 'cancelled'] },
              '$addons.total',
              0
            ]
          }
        }
      }
    },
    {
      $lookup: {
        from: 'products_services',
        localField: '_id',
        foreignField: '_id',
        as: 'addon'
      }
    },
    { $addFields: { addonName: { $first: '$addon.name' }, addon: '$$REMOVE' } },
    {
      $group: {
        _id: null,
        activeAmount: { $sum: '$activeAmount' },
        addonsSummary: {
          $push: {
            _id: '$addonName',
            amount: '$activeAmount'
          }
        },
        cancelledAmount: { $sum: '$cancelledAmount' }
      }
    },
    {
      $project: {
        addonsSummary: 1,
        statusSummary: [
          {
            _id: 'active',
            amount: '$activeAmount'
          },
          {
            _id: 'cancelled',
            amount: '$cancelledAmount'
          }
        ],
        totalAmount: {
          $add: ['$activeAmount', '$cancelledAmount']
        }
      }
    }
  ]
  const [summary = {}] = (await CorrectionCollection.aggregate(pipeline)) || []
  return summary
}

export const correctionsSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['addTo'], body)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  const preparedQuery = await prepareQueryDataForQueryCorrections(body)
  return await getCorrectionSummary(preparedQuery)
}

export const getAddonAmountAndCreatedAt = (addonMeta, correction) => {
  const { total: addonAmount } = addonMeta
  console.log('check correction id  ====> ', correction._id)
  console.log('check addonMeta ====> ', addonMeta)
  //Add transaction with correction cancel date if correction cancel date exist.
  const createdAt = correction.cancelledAt
    ? correction.cancelledAt
    : correction.createdAt
  return { createdAt, addonAmount }
}

export const pickBasicDataFromCorrection = (correction) =>
  pick(correction, [
    'partnerId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'contractId',
    'tenantId',
    'payoutId',
    'landlordInvoiceId',
    'createdBy'
  ])

export const prepareCorrectionTransactionData = async (
  correction,
  transactionEvent,
  session
) => {
  const transactionData = pickBasicDataFromCorrection(correction)
  const { _id } = correction
  transactionData.type = 'correction'
  transactionData.correctionId = _id

  if (!transactionData.payoutId && _id) {
    const correctionInfo = await getCorrection({ _id }, session)
    const { payoutId = '' } = correctionInfo

    if (payoutId) {
      transactionData.payoutId = payoutId
    }
  }
  if (transactionEvent === 'legacy') {
    transactionData.createdAt = correction.createdAt
    transactionData.transactionEvent = transactionEvent
  }
  return transactionData
}

export const isExistsCorrectionTransaction = async (params, session) => {
  const { correction, addonMeta, addonAmount } = params
  const { partnerId, _id, addTo } = correction
  const { addonId } = addonMeta
  const query = {
    partnerId,
    correctionId: _id,
    amount: addonAmount,
    addonId,
    type: 'correction'
  }
  let existsTransaction = !!(await transactionHelper.getTransaction(
    query,
    session
  ))

  //Cancelled correction type transaction should be added for 0 amount addon even though transaction exist
  if (addonAmount === 0 && addTo === 'payout' && existsTransaction)
    existsTransaction = false
  return existsTransaction
}

export const updateTransactionData = async (params, session) => {
  const { correction, addonMeta, transactionData, addonAmount } = params
  const { partnerId, addTo, createdAt } = correction
  const { addonId } = addonMeta
  console.log('addTo ===  ', addTo)
  if (addTo === 'payout') {
    addonMeta.addTo = 'payout'

    transactionData.period =
      await transactionHelper.getFormattedTransactionPeriod(
        createdAt,
        partnerId
      )
  }
  const accountingParams = {
    partnerId,
    accountingType: 'addon',
    options: addonMeta
  }

  let addonTransactionData =
    await transactionHelper.getAccountingDataForTransaction(
      accountingParams,
      session
    )

  addonTransactionData.amount = addonAmount
  addonTransactionData.addonId = addonId
  console.log('addonTransactionData ====> ', addonTransactionData)
  addonTransactionData = extend(addonTransactionData, transactionData)
  return addonTransactionData
}

export const prepareCorrectionsQuery = async (params) => {
  const query = {}
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
      query.$or = [
        { tenantId: params.tenantId },
        { tenants: { $elemMatch: { tenantId: params.tenantId } } }
      ]
    }
    if (params.correctionStatus) {
      query.correctionStatus = params.correctionStatus
    }
    if (params.createdBy) query.createdBy = params.createdBy
    // For partner admin and app manager
    if (params.addTo) query.addTo = params.addTo
    //Set accountId for landlord dashboard
    if (params.context && params.context === 'landlordDashboard') {
      const accountIds =
        map(
          await accountHelper.getAccountsWithSelect(
            { personId: params.userId },
            { _id: 1 }
          ),
          '_id'
        ) || []
      query.accountId = { $in: accountIds }
    }
    //Set correction type for tenant dashboard
    if (params.context && params.context === 'tenantDashboard') {
      query.addTo = 'rent_invoice'
    }
    //Set expenses amount filters in query.
    if (params.searchKeyword) {
      query.amount = params.searchKeyword * 1
    }
    if (
      size(params.createdAtDateRange) &&
      params.createdAtDateRange.startDate &&
      params.createdAtDateRange.endDate
    ) {
      query.createdAt = {
        $gte: params.createdAtDateRange.startDate,
        $lte: params.createdAtDateRange.endDate
      }
    }
    //Set dateRange filters in query
    if (
      size(params.dateRange) &&
      params.dateRange.startDate &&
      params.dateRange.endDate
    ) {
      query.createdAt = {
        $gte: params.dateRange.startDate,
        $lte: params.dateRange.endDate
      }
    }
    //Set dateRange in query for export data
    if (
      params.download &&
      size(params.dateRange) &&
      params.dateRange.startDate_string &&
      params.dateRange.endDate_string
    ) {
      const newDateRange = await appHelper.getDateRangeFromStringDate(
        params.partnerId,
        params.dateRange
      )

      if (
        size(newDateRange) &&
        newDateRange.startDate &&
        newDateRange.endDate
      ) {
        query.createdAt = {
          $gte: newDateRange.startDate,
          $lte: newDateRange.endDate
        }
      }
    }

    const invoiceSummaryId = params.invoiceSummaryId
    if (invoiceSummaryId) {
      const invoiceSummary = await invoiceSummaryHelper.getInvoiceSummary({
        _id: invoiceSummaryId
      })
      const correctionIds = invoiceSummary?.correctionsIds || []

      if (params.isOnlyPayoutCorrection) {
        query.addTo = 'payout'
        query._id = { $in: correctionIds }
      } else query._id = { $in: correctionIds }
    }

    if (params.contractId && params.leaseSerial) {
      const invoiceIds = await invoiceHelper.getInvoiceIdsForLeaseFilter(
        params.contractId,
        params.leaseSerial
      )
      query[
        query.addTo === 'rent_invoice' ? 'invoiceId' : 'landlordInvoiceId'
      ] = { $in: invoiceIds }
    }

    if (params.contractId) query.contractId = params.contractId
  }

  return query
}

export const getCorrectionExcelManager = async (queryData) => {
  const { query, options, dateFormat, timeZone, language } = queryData
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $unwind: {
        path: '$addons'
      }
    },
    {
      $lookup: {
        from: 'products_services',
        localField: 'addons.addonId',
        foreignField: '_id',
        as: 'addon'
      }
    },
    {
      $unwind: {
        path: '$addon'
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
        localField: 'createdBy',
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
      $project: {
        correctionId: '$correctionSerialId',
        date: {
          $dateToString: {
            format: dateFormat,
            date: '$createdAt',
            timezone: timeZone
          }
        },
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
        createdBy: '$user.profile.name',
        amount: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$addTo', 'rent_invoice'] },
                { $eq: ['$invoice.status', 'credited'] }
              ]
            },
            then: 0,
            else: '$amount'
          }
        },
        addTo: {
          $cond: {
            if: { $eq: ['$addTo', 'payout'] },
            then: {
              $concat: [
                appHelper.translateToUserLng('common.payout', language),
                {
                  $cond: [
                    { $ifNull: ['$payout.serialId', false] },
                    { $concat: [' #', { $toString: '$payout.serialId' }] },
                    ''
                  ]
                }
              ]
            },
            else: appHelper.translateToUserLng('common.rent_invoice', language)
          }
        },
        subType: '$addon.name',
        description: { $ifNull: ['$addon.description', 'No description'] },
        createdAt: 1
      }
    },
    {
      $group: {
        _id: '$_id',
        subType: { $push: '$subType' },
        description: { $push: '$description' },
        correctionId: { $first: '$correctionId' },
        date: { $first: '$date' },
        objectId: { $first: '$objectId' },
        property: { $first: '$property' },
        apartmentId: { $first: '$apartmentId' },
        createdBy: { $first: '$createdBy' },
        addTo: { $first: '$addTo' },
        amount: { $first: '$amount' },
        createdAt: { $first: '$createdAt' }
      }
    },
    {
      $sort: sort
    }
  ]
  const correctionData = await CorrectionCollection.aggregate(pipeline)
  return correctionData || []
}

export const correctionDataForExcelCreator = async (params, options) => {
  const { partnerId = {}, userId = {} } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })

  const userInfo = await userHelper.getAnUser({ _id: userId })
  const userLanguage = userInfo.getLanguage()
  const correctionsQuery = await prepareCorrectionsQuery(params)
  const dataCount = await countCorrections(correctionsQuery)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'

  const queryData = {
    query: correctionsQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage
  }

  let corrections = await getCorrectionExcelManager(queryData)
  corrections = JSON.parse(JSON.stringify(corrections))
  for (const correction of corrections) {
    if (size(correction.subType)) {
      correction.subType = correction.subType.join(', ')
    } else {
      correction.subType = ''
    }
    if (size(correction.description)) {
      correction.description = correction.description.join(', ')
    } else {
      correction.description = ''
    }
  }

  return {
    data: corrections,
    total: dataCount
  }
}

export const queryCorrectionsForExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_correction') {
    const payoutData = await correctionDataForExcelCreator(queueInfo.params, {
      skip,
      limit,
      sort
    })
    return payoutData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const getCorrectionIdsForLegacyTransaction = async (partnerId) => {
  const corrections = await CorrectionCollection.aggregate([
    {
      $match: {
        partnerId,
        addTo: 'payout'
      }
    },
    {
      $group: {
        _id: null,
        correctionIds: { $addToSet: '$_id' }
      }
    }
  ])
  const [correctionInfo = {}] = corrections || []
  const { correctionIds = [] } = correctionInfo
  return correctionIds
}

const getTenantPipelineForDetails = () => [
  {
    $addFields: {
      tenants: {
        $concatArrays: [
          { $ifNull: ['$tenants', []] },
          [{ tenantId: '$tenantId' }]
        ]
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
      as: 'tenantsInfo'
    }
  }
]

const getInvoicePipelineForDetails = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'correctionsIds',
      pipeline: [
        {
          $project: {
            _id: 1,
            invoiceSerialId: 1,
            invoiceTotal: 1
          }
        }
      ],
      as: 'invoicesInfo'
    }
  }
]

const getFilesPipelineForDetails = () => [
  {
    $lookup: {
      from: 'files',
      localField: 'files',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'createdBy',
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
            title: 1,
            createdAt: 1,
            isVisibleToLandlord: 1,
            isVisibleToTenant: 1,
            fileUrl: getFileUrlPipeline(),
            createdUserName: '$userInfo.profile.name',
            createdUserAvatarKey: appHelper.getUserAvatarKeyPipeline(
              '$userInfo.profile.avatarKey'
            )
          }
        }
      ],
      as: 'filesInfo'
    }
  }
]

const getAddonsPipelineForDetails = () => [
  {
    $unwind: {
      path: '$addons',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'products_services',
      localField: 'addons.addonId',
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
      _id: '$_id',
      addonsInfo: {
        $push: {
          addonId: '$addons.addonId',
          name: '$addonInfo.name',
          description: '$addons.description',
          taxPercentage: '$addons.taxPercentage',
          total: '$addons.total'
        }
      },
      amount: { $first: '$amount' },
      addTo: { $first: '$addTo' },
      isNonRent: { $first: '$isNonRent' },
      correctionSerialId: { $first: '$correctionSerialId' },
      propertyId: { $first: '$propertyId' },
      contractId: { $first: '$contractId' },
      payoutId: { $first: '$payoutId' },
      tenantId: { $first: '$tenantId' },
      tenants: { $first: '$tenants' },
      branchId: { $first: '$branchId' },
      agentId: { $first: '$agentId' },
      files: { $first: '$files' },
      correctionStatus: { $first: '$correctionStatus' },
      status: { $first: '$status' },
      createdAt: { $first: '$createdAt' }
    }
  }
]

const getBranchPipelineForDetails = () => [
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

const getAgentPipelineForDetails = () => [
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

export const getCorrectionDetails = async (body) => {
  const { correctionId, partnerId } = body
  console.log('Checking for correctionId, partnerId: ', correctionId, partnerId)
  const pipeline = [
    {
      $match: {
        _id: correctionId,
        partnerId
      }
    },
    ...getAddonsPipelineForDetails(),
    ...getPropertyPipelineForDetails(),
    ...getTenantPipelineForDetails(),
    ...getInvoicePipelineForDetails(),
    ...getPayoutInfo(),
    ...getContactInfo(),
    ...getBranchPipelineForDetails(),
    ...getAgentPipelineForDetails(),
    ...getFilesPipelineForDetails(),
    {
      $project: {
        _id: 1,
        amount: 1,
        addTo: 1,
        isNonRent: 1,
        correctionSerialId: 1,
        correctionStatus: 1,
        status: 1,
        createdAt: 1,
        addonsInfo: 1,
        propertyInfo: 1,
        tenantsInfo: 1,
        invoicesInfo: 1,
        branchInfo: 1,
        agentInfo: 1,
        filesInfo: 1,
        isFinalSettlementDone: '$contractInfo.isFinalSettlementDone',
        payoutStatus: '$payoutInfo.status'
      }
    }
  ]
  console.log('Checking for pipeline: ', pipeline)
  const [correctionDetails] =
    (await CorrectionCollection.aggregate(pipeline)) || []
  console.log('Checking for correctionDetails: ', correctionDetails)
  return correctionDetails
}

export const correctionDetails = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['correctionId'], body)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  return await getCorrectionDetails(body)
}
export const isValidateAnnualStatementPeriodForCorrection = async (
  partnerId,
  contractId,
  session
) => {
  const statementYear =
    await annualStatementHelper.getSpecificFiledDataForAnnualStatement(
      'statementYear',
      { partnerId, contractId }
    )

  const annualStatementsPeriod = Math.max(...statementYear)
  if (!annualStatementsPeriod) return true

  const contractInfo = await contractHelper.getAContract({
    _id: contractId,
    partnerId
  })

  if (!size(contractInfo)) {
    throw new CustomError(404, 'Contract not found')
  }
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const { invoiceStartOnPeriod, invoiceEndOnPeriod } =
    await getCorrectionInvoicePeriod(contractInfo, partnerSetting, session)
  if (
    invoiceStartOnPeriod <= annualStatementsPeriod ||
    invoiceEndOnPeriod <= annualStatementsPeriod
  ) {
    return false
  }
  return true
}
export const validateUpdateCorrectionData = async (
  body,
  correction,
  session
) => {
  if (!size(body.addons)) {
    throw new CustomError(404, 'Required addons data')
  }
  const { contractId, partnerId } = correction
  const isFinalSettlementDone =
    await finalSettlementHelper.isDoneFinalSettlement(contractId, partnerId)

  // For non rent correction  we have to check if all the addon items are non-rent and the total amount is positive in correction
  if (correction.isNonRent) {
    const { addons = [] } = body
    const validAddons = every(addons, ['isNonRent', true])
    const totalAmount = await correctionHelper.calculateAddonsTotal(addons)
    if (!validAddons || totalAmount < 0) {
      throw new CustomError(400, 'Non-rent correction addons are invalid!')
    }
  }

  if (isFinalSettlementDone) {
    throw new CustomError(405, 'Final settlement is done for this contract!')
  }

  const isCorrectionUpdate = await correctionHelper.isCorrectionWillBeUpdated(
    correction,
    session
  )

  if (!isCorrectionUpdate) {
    throw new CustomError(405, 'Update is not allowed for this correction!')
  }

  if (correction.addTo === 'payout' || body.createInvoice) {
    const isValidPeriod = await isValidateAnnualStatementPeriodForCorrection(
      partnerId,
      contractId,
      session
    )
    if (!isValidPeriod) {
      throw new CustomError(
        400,
        'You can not update this correction, as the annual report has already been created for this period'
      )
    }
  }
  return correction
}

export const prepareUpdateCorrectionData = async (body, correction) => {
  const { addons } = body
  const updateData = { status: 'unpaid' }
  const isChangedAddons = differenceWith(addons, correction.addons, isEqual)
  if (size(isChangedAddons)) {
    updateData.addons = addons
    updateData.amount = await correctionHelper.calculateAddonsTotal(addons)
  }
  return updateData
}

export const prepareFilesForCorrection = async (files, params) => {
  const {
    addTo,
    contractId,
    partnerId,
    propertyId,
    createdBy,
    isVisible = false
  } = params
  let { accountId } = params

  if (!accountId) {
    const property = await listingHelper.getAListing({ _id: propertyId })
    accountId = property?.accountId
  }
  let visibility = {}
  if (addTo == 'payout') visibility = { isVisibleToLandlord: isVisible }
  else visibility = { isVisibleToTenant: isVisible }

  const filesData = files.map((file) => ({
    _id: nid(17),
    accountId,
    contractId,
    context: 'correction',
    createdBy,
    createdAt: new Date(),
    directive: 'Files',
    eventStatus: 'processed',
    ...visibility,
    partnerId,
    propertyId,
    name: file.name,
    size: file.size,
    title: file.title,
    type: 'correction_invoice_pdf'
  }))
  return filesData
}

const getActiveCorrectionList = async (params) => {
  const { query, options } = params
  const { limit, skip, sort = {} } = options
  appHelper.validateSortForQuery(sort)
  const correctionsListPipeline = [
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
      $lookup: {
        from: 'payouts',
        localField: 'payoutId',
        foreignField: '_id',
        as: 'payoutInfo'
      }
    },
    appHelper.getUnwindPipeline('payoutInfo'),
    ...appHelper.getCommonTenantInfoPipeline(),
    {
      $addFields: {
        payoutSerialId: {
          $cond: [
            { $ifNull: ['$payoutInfo.serialId', false] },
            {
              $concat: [
                '#',
                { $toString: { $ifNull: ['$payoutInfo.serialId', ''] } }
              ]
            },
            null
          ]
        }
      }
    },
    {
      $project: {
        amount: 1,
        correctionId: '$_id',
        correctionSerialId: 1,
        correctionStatus: 1,
        createdAt: 1,
        payoutId: 1,
        payoutSerialId: '$payoutSerialId',
        tenantId: 1,
        tenantName: '$tenantInfo.name',
        tenantUserAvatar: '$tenantInfo.avatarKey'
      }
    }
  ]

  const correctionList =
    (await CorrectionCollection.aggregate(correctionsListPipeline)) || []

  return correctionList
}

export const queryActiveCorrection = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], user)
  const { partnerId } = user
  const { query } = body
  appHelper.checkRequiredFields(['contractId'], query)
  const { contractId } = query
  body.query = {
    contractId,
    invoiceId: { $exists: false },
    addTo: 'rent_invoice',
    correctionStatus: 'active',
    partnerId
  }
  const corrections = await getActiveCorrectionList(body)
  const filteredDocuments = await correctionHelper.countCorrections(body.query)
  return {
    data: corrections,
    metaData: {
      filteredDocuments,
      totalDocuments: filteredDocuments
    }
  }
}
