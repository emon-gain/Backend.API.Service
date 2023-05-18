import { has, intersection, isString, omit, pick, size } from 'lodash'
import { CustomError } from '../common'
import {
  IntegrationCollection,
  PartnerCollection,
  PartnerSettingCollection,
  TransactionCollection
} from '../models'
import {
  appHelper,
  branchHelper,
  partnerSettingHelper,
  userHelper
} from '../helpers'
import settingJSON from '../../../settings.json'

const partnerSettingsPickedArray = [
  'administrationEvictionFee',
  'assignmentSettings',
  'bankPayment',
  'collectionNoticeFee',
  'country',
  'CPISettlement',
  'currencySettings',
  'customPayoutDays',
  'dateTimeSettings',
  'defaultFindHomeLocation',
  'duePreReminder',
  'evictionFee',
  'evictionNotice',
  'evictionDueReminderNotice',
  'invoiceCalculation',
  'invoiceCollectionNotice',
  'invoiceDueDays',
  'invoiceFee',
  'invoiceFirstReminder',
  'invoiceSecondReminder',
  'invoiceSettings',
  'lastBankReference',
  'leaseSetting',
  'listingSetting',
  'notifications',
  'payout',
  'postalFee',
  'propertySettings',
  'reminderFee',
  'retryFailedPayouts',
  'sameKIDNumber',
  'standardPayoutDate',
  'tenantSetting',
  'stopCPIRegulation',
  'landlordBankPayment',
  'landlordInvoiceFee',
  'landlordReminderFee',
  'landlordCollectionNoticeFee',
  'landlordPostalFee',
  'landlordInvoiceDueDays',
  'landlordDuePreReminder',
  'landlordInvoiceFirstReminder',
  'landlordInvoiceSecondReminder',
  'landlordInvoiceCollectionNotice',
  'tenantPaysAllDueDuringEviction'
]

export const isBrokerPartner = async (id, session) => {
  const partner = await PartnerCollection.findOne({
    _id: id,
    accountType: 'broker'
  }).session(session)
  return !!partner
}

export const isDirectPartner = async (partnerId) => {
  const _isDirectPartner = !!(await PartnerCollection.findOne({
    _id: partnerId,
    accountType: 'direct'
  }))
  return _isDirectPartner
}

export const getPartners = async (query, session) => {
  const partners = await PartnerCollection.find(query).session(session)
  return partners
}

export const getDirectPartnerById = async (id, session) => {
  const query = {
    _id: id,
    accountType: 'direct'
  }
  const directPartner = await PartnerCollection.findOne(query).session(session)
  return directPartner
}

export const getPartnerById = async (id, session) => {
  const partner = await PartnerCollection.findById(id).session(session)
  return partner
}

export const getAPartner = async (query, session, populate = []) => {
  const partner = await PartnerCollection.findOne(query)
    .session(session)
    .populate(populate)
  return partner
}
export const preparePartnersQueryBasedOnFilters = async (query, session) => {
  const {
    _id,
    country,
    createdDateRange,
    defaultSearch,
    isSelfService,
    partnerType,
    status,
    statusNotEnabled,
    name,
    dataType,
    filter
  } = query
  let partnersQuery = {}
  if (dataType === 'pogo_integrated_partners') {
    const integrationQuery = { status: 'integrated', type: 'power_office_go' }
    if (size(_id)) {
      partnersQuery._id = _id
      integrationQuery.partnerId = query._id
    }
    const integratedPartnerIds = await IntegrationCollection.distinct(
      'partnerId',
      integrationQuery
    ).session(session)

    partnersQuery._id = { $in: integratedPartnerIds }
    partnersQuery.enableTransactions = true
    partnersQuery.isActive = true
  } else if (filter === 'external_id_invalid') {
    // First fetch partner ids from transactions, where external Id is invalid
    const transactionMatchQuery = {
      $or: [
        {
          externalEntityId: { $exists: false },
          'powerOffice.id': { $exists: true }
        },
        {
          externalEntityId: { $exists: true },
          $expr: {
            $gt: [{ $strLenCP: { $ifNull: ['$externalEntityId', ''] } }, 7]
          }
        }
      ]
    }
    const transactions = await TransactionCollection.distinct(
      'partnerId',
      transactionMatchQuery
    )
    const integrations = await IntegrationCollection.distinct('partnerId', {
      status: 'integrated',
      type: 'power_office_go'
    })
    const partnerIds = intersection(transactions, integrations)
    //now partner query
    partnersQuery._id = { $in: partnerIds }
    partnersQuery.enableTransactions = true
    partnersQuery.isActive = true
  } else {
    if (size(status)) {
      if (status.includes('finnActivated')) query.enableFinn = true
      if (status.includes('transactionsApiEnabled'))
        query.enableTransactionsApi = true
      if (status.includes('annualStatementEnabled'))
        query.enableAnnualStatement = true
      if (status.includes('depositAccountEnabled'))
        query.enableDepositAccount = true
      if (status.includes('brokerJournalsEnabled'))
        query.enableBrokerJournals = true
      if (status.includes('creditRatingsEnabled'))
        query.enableCreditRating = true
      if (status.includes('transactionsEnabled'))
        query.enableTransactions = true
      if (status.includes('individualInvoicesSeriesEnabled'))
        query.enableInvoiceStartNumber = true
      if (status.includes('enableSkatteetaten')) query.enableSkatteetaten = true
    }
    if (size(statusNotEnabled)) {
      if (statusNotEnabled.includes('finnDeactivated')) {
        if (query.hasOwnProperty('enableFinn')) {
          delete query.enableFinn
        } else {
          query.enableFinn = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('transactionsApiDisabled')) {
        if (query.hasOwnProperty('enableTransactionsApi')) {
          delete query.enableTransactionsApi
        } else {
          query.enableTransactionsApi = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('annualStatementDisabled')) {
        if (query.hasOwnProperty('enableAnnualStatement')) {
          delete query.enableAnnualStatement
        } else {
          query.enableAnnualStatement = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('depositAccountDisabled')) {
        if (query.hasOwnProperty('enableDepositAccount')) {
          delete query.enableDepositAccount
        } else {
          query.enableDepositAccount = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('brokerJournalsDisabled')) {
        if (query.hasOwnProperty('enableBrokerJournals')) {
          delete query.enableBrokerJournals
        } else {
          query.enableBrokerJournals = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('creditRatingsDisabled')) {
        if (query.hasOwnProperty('enableCreditRating')) {
          delete query.enableCreditRating
        } else {
          query.enableCreditRating = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('transactionsDisabled')) {
        if (query.hasOwnProperty('enableTransactions')) {
          delete query.enableTransactions
        } else {
          query.enableTransactions = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('individualInvoicesSeriesDisabled')) {
        if (query.hasOwnProperty('enableInvoiceStartNumber')) {
          delete query.enableInvoiceStartNumber
        } else {
          query.enableInvoiceStartNumber = { $ne: true }
        }
      }
      if (statusNotEnabled.includes('disableSkatteetaten')) {
        if (query.hasOwnProperty('enableSkatteetaten')) {
          delete query.enableSkatteetaten
        } else {
          query.enableSkatteetaten = { $ne: true }
        }
      }
    }
    if (partnerType === 'directPartner') query.accountType = 'direct'
    if (partnerType === 'brokerPartner') query.accountType = 'broker'
    if (country) query.country = country
    if (isSelfService) query.isSelfService = isSelfService
    if (size(createdDateRange)) {
      appHelper.validateCreatedAtForQuery(createdDateRange)
      query.createdAt = {
        $gte: createdDateRange.startDate,
        $lte: createdDateRange.endDate
      }
    }
    if (name) {
      query.name = { $regex: new RegExp('.*' + name + '.*', 'i') }
    }
    if (defaultSearch) {
      const defaultSearchArr = [
        { name: { $regex: new RegExp('.*' + defaultSearch + '.*', 'i') } }
      ]
      if (!isNaN(defaultSearch))
        defaultSearchArr.push({ serial: defaultSearch })
      query.$or = defaultSearchArr
    }
    partnersQuery = omit(query, [
      'createdDateRange',
      'defaultSearch',
      'partnerType',
      'status',
      'statusNotEnabled'
    ])
  }
  return partnersQuery
}

export const getPartnersForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  let partners = []
  if (
    query.dataType === 'pogo_integrated_partners' ||
    query.filter === 'external_id_invalid'
  ) {
    delete query.dataType
    delete query.filter
    const partnersInfo = await PartnerCollection.find(query)
    for (const partner of partnersInfo) {
      partners.push(createPartnerFieldNameForApi(partner))
    }
  } else {
    partners = await PartnerCollection.find(query)
      .populate('owner')
      .limit(limit)
      .skip(skip)
      .sort(sort)
      .collation({ locale: 'en', strength: 2 })
    partners = JSON.parse(JSON.stringify(partners)) // Should not delete
    partners = partners.map((partner) => {
      if (size(partner.owner)) {
        partner.owner.profile = {
          ...partner.owner.profile,
          avatar: userHelper.getAvatar(partner.owner) || ''
        }
      }
      partner.logo = getPartnerLogo(partner) || ''
      return partner
    })
  }
  return partners
}

export const countPartners = async (query) => {
  const numberOfPartners = await PartnerCollection.find(query).countDocuments()
  return numberOfPartners
}

const createPartnerFieldNameForApi = (partner) => ({
  id: partner.serial,
  _id: partner._id,
  name: partner.name,
  accountType: partner.accountType
})

export const queryPartners = async (req) => {
  const { body, user = {} } = req
  const { query, options } = body
  appHelper.checkUserId(user.userId)
  appHelper.validateSortForQuery(options.sort)
  body.query = await preparePartnersQueryBasedOnFilters(query)
  const partners = await getPartnersForQuery(body)
  delete body.query.dataType
  delete body.query.filter
  const filteredDocuments = await countPartners(body.query)
  const totalDocuments = await countPartners({})
  return {
    data: partners,
    metaData: { filteredDocuments, totalDocuments }
  }
}
export const findPartnerIdsForAnnualStatement = async () => {
  const partnerPipeline = [
    {
      $match: {
        enableAnnualStatement: true,
        isActive: true
      }
    },
    {
      $group: {
        _id: null,
        partnerIds: {
          $push: '$_id'
        }
      }
    }
  ]
  const partners = await partnerAggregate(partnerPipeline)
  return partners[0]?.partnerIds || []
}

export const partnerAggregate = async (pipeline) =>
  PartnerCollection.aggregate(pipeline)

export const queryPartnersForLambda = async (req) => {
  const { body, user = {} } = req
  const { query } = body
  appHelper.checkUserId(user.userId)

  const { _id, isTransaction } = query
  if (_id) appHelper.validateId({ _id })
  if (isTransaction) query.enableTransactions = true
  query.isActive = true
  const partners = await PartnerCollection.find(query)
  return partners
}

export const getPartnerIds = async (limit, skip) => {
  const partners = await PartnerSettingCollection.aggregate([
    {
      $match: { 'listingSetting.disabledListing.enabled': true }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $group: {
        _id: 'null',
        partnerIds: {
          $addToSet: '$partnerId'
        }
      }
    }
  ])
  return partners[0].partnerIds
}

export const commonFieldsValidationCheck = (params) => {
  appHelper.checkRequiredFields(['partnerId', 'isEnable'], params)
  const { partnerId } = params
  appHelper.validateId({ partnerId })
}

export const partnerIdValidationCheck = (params) => {
  appHelper.checkRequiredFields(['partnerId'], params)
  const { partnerId } = params
  appHelper.validateId({ partnerId })
}

export const isTransactionEnabledOfAPartner = async (id, session) => {
  const partner = await getPartnerById(id, session)
  return partner && partner.enableTransactions
}

export const validateSubDomain = async (subDomain, partnerId, session) => {
  const query = { subDomain }
  if (partnerId) {
    query._id = { $ne: partnerId }
  }
  if (!subDomain) {
    throw new CustomError(400, 'Invalid sub-domain')
  }
  const isExists = await PartnerCollection.find(query).session(session)
  if (size(isExists)) {
    throw new CustomError(405, 'Sub-domain already exists')
  }
}

export const preparePartnerSettingsData = async (params, session) => {
  let partnerSettings = {}
  const { _id, name, createdBy } = params
  const query = { partnerId: { $exists: false } }
  const adminSettingInfo = await partnerSettingHelper.getAPartnerSetting(
    query,
    session
  )
  if (size(adminSettingInfo)) {
    partnerSettings = pick(adminSettingInfo, partnerSettingsPickedArray)
  }
  partnerSettings.partnerId = _id
  partnerSettings.companyInfo = { companyName: name }
  if (!size(partnerSettings.invoiceSettings))
    partnerSettings.invoiceSettings = { numberOfDecimalInInvoice: 0 }

  size(partnerSettings) && partnerSettings.bankPayment
    ? (partnerSettings.bankPayment.enabled = true)
    : (partnerSettings.bankPayment.enabled = false)
  if (!size(partnerSettings.tenantSetting)) {
    partnerSettings.tenantSetting = {
      removeProspects: { enabled: false, months: 1 },
      deleteInterestForm: { enabled: false, months: 1 },
      disableCreditRatingForm: { enabled: false, months: 1 }
    }
  }
  partnerSettings.createdBy = createdBy
  if (!size(partnerSettings.assignmentSettings)) {
    partnerSettings.assignmentSettings = {
      internalAssignmentId: false,
      enableEsignAssignment: false,
      enabledAssignmentEsignReminder: false,
      esignReminderNoticeDays: 1,
      enabledShowAssignmentFilesToLandlord: false
    }
  }
  if (!size(partnerSettings.leaseSetting)) {
    partnerSettings.leaseSetting = {
      internalLeaseId: false,
      enableEsignLease: false,
      enabledLeaseESigningReminder: false,
      esignReminderNoticeDays: 2,
      naturalLeaseTermination: {
        enabled: false,
        days: 5
      },
      enabledShowLeaseFilesToTenant: false
    }
  }
  if (!size(partnerSettings.evictionFee))
    partnerSettings.evictionFee = { enabled: false }
  if (!size(partnerSettings.administrationEvictionFee))
    partnerSettings.administrationEvictionFee = { enabled: false }
  if (!size(partnerSettings.CPISettlement))
    partnerSettings.CPISettlement = { enabled: false }
  if (!size(partnerSettings.evictionNotice))
    partnerSettings.evictionNotice = { enabled: false }
  if (!size(partnerSettings.evictionDueReminderNotice))
    partnerSettings.evictionDueReminderNotice = {
      enabled: false,
      isCreateEvictionPackage: false
    }
  if (!size(partnerSettings.defaultFindHomeLocation))
    partnerSettings.defaultFindHomeLocation = {
      defaultMapLocation: 'Oslo, Norway',
      defaultMapZoom: 12
    }

  partnerSettings.stopCPIRegulation = partnerSettings.stopCPIRegulation || false

  return partnerSettings
}

export const checkPartnerUsersData = (data) => {
  const requiredFields = ['name', 'email', 'roles', 'partnerId']
  appHelper.checkRequiredFields(requiredFields, data)
  const { branchId, roles, partnerId } = data
  appHelper.validateId({ partnerId })
  if (branchId) {
    appHelper.validateId({ branchId })
  }
  if (!size(roles)) {
    throw new CustomError(400, 'Roles can not be empty')
  }
}

export const checkPartnerUpdatingData = async (body, session) => {
  const requiredFields = ['partnerId', 'partnerData']
  appHelper.checkRequiredFields(requiredFields, body)
  const { partnerId, partnerData } = body
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['name', 'subDomain'], partnerData)
  const { name, subDomain, sms, phoneNumber } = partnerData
  await validateSubDomain(subDomain.toLowerCase(), partnerId, session)
  if (!name) {
    throw new CustomError(400, 'Invalid name')
  }
  if (sms && !phoneNumber) {
    throw new CustomError(400, 'Phone number is required')
  }
}

export const validationCheckForPartnerEmployeeId = async (params, session) => {
  const requiredFields = ['partnerId', 'partnerUserId', 'partnerEmployeeId']
  appHelper.checkRequiredFields(requiredFields, params)
  const { partnerId, partnerUserId } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ partnerUserId })
  const partner = await getAPartner({ _id: partnerId }, session)
  if (!partner) {
    throw new CustomError(404, "Partner doesn't exists")
  }
  const isExistingEmployeeId = await userHelper.existingEmployeeId(params)
  if (isExistingEmployeeId) {
    throw new CustomError(404, 'EmployeeId already exists')
  }
  return { partner }
}

export const isAdminBranch = async (params, session) => {
  const { partnerId, partnerUserId, status } = params
  const adminBranch = await branchHelper.getBranches(
    { adminId: partnerUserId, partnerId },
    session
  )
  if (size(adminBranch) && status === 'inactive') {
    throw new CustomError(405, 'This is an admin branch')
  }
}

export const validationCheckForPartnerUserStatus = async (params, session) => {
  const requiredFields = ['partnerUserId', 'status']
  appHelper.checkRequiredFields(requiredFields, params)
  const { partnerId, partnerUserId } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ partnerUserId })
  const partner = await getAPartner({ _id: partnerId }, session)
  if (!size(partner)) {
    throw new CustomError(404, "Partner doesn't exists")
  }
  await isAdminBranch(params, session)

  return partner
}

export const validatePartnerAddData = (params) => {
  const requiredFields = ['name', 'subDomain', 'accountType']
  appHelper.checkRequiredFields(requiredFields, params)
}

export const prepareQueueDataForLegacyTransaction = (partnerId) => ({
  params: {
    partnerId
  },
  event: 'transaction_enabled',
  action: 'run_legacy_transaction',
  priority: 'regular',
  destination: 'invoice'
})

export const getPartnersWithOptions = async (params) => {
  const { query = {}, options = {} } = params
  const { select = {}, sort = {} } = options
  const partners = await PartnerCollection.find(query).sort(sort).select(select)
  return partners
}

export const getPartnerLogo = (partner = {}) => {
  const { _id, logo = '' } = partner
  const { folder } = settingJSON.S3.Directives['PartnerLogo'] // Get image directory from settings
  const domain = appHelper.getCDNDomain()
  const logoUrl = logo
    ? `${domain}/${folder}/${_id}/${logo}`
    : `${domain}/assets/default-image/ul-full-logo-primary.png`
  return logoUrl
}

export const queryPartnersSubDomain = async (req) => {
  appHelper.checkRequiredFields(['partnerSiteType', 'subDomain'], req.body)

  const { partnerSiteType, subDomain } = req.body
  if (!(partnerSiteType && subDomain)) {
    throw new CustomError(400, 'Missing partnerSiteType or subDomain!')
  }

  const partnerQuery = { isActive: true, subDomain }
  if (partnerSiteType === 'public') {
    partnerQuery.isSelfService = { $ne: true }
  }
  const partner = await getAPartner(partnerQuery)
  let subDomainInfo = { isSubDomainExists: false }
  if (size(partner)) {
    const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
      partnerId: partner._id
    })
    subDomainInfo = {
      name: partner.name,
      partnerId: partner._id,
      logo: partner.siteLogo
        ? appHelper.getCDNDomain() +
          '/partner_logo/' +
          partner._id +
          '/' +
          partner.siteLogo
        : null,
      // : appHelper.getDefaultLogoURL('uniteliving_logo'),
      isSubDomainExists: true,
      defaultFindHomeLocation: partnerSetting?.defaultFindHomeLocation,
      currencySettings: partnerSetting?.currencySettings,
      partnerSiteURL: partnerSetting?.companyInfo?.isLogoLinkedToWebsite
        ? partnerSetting?.companyInfo?.website
        : null
    }
  }
  return subDomainInfo
}

export const getActivePartnerInfoForDashboard = async (
  query = {},
  dateRange = ''
) => {
  const pipeline = []
  const match = { $match: { ...query, isActive: true } }
  pipeline.push(match)
  const group = {
    $group: {
      _id: {
        $dateToString: {
          date: '$createdAt',
          format: appHelper.getDateFormatString(dateRange)
        }
      },
      countedBroker: {
        $sum: {
          $cond: { if: { $eq: ['$accountType', 'broker'] }, then: 1, else: 0 }
        }
      },
      countedDirect: {
        $sum: {
          $cond: { if: { $eq: ['$accountType', 'direct'] }, then: 1, else: 0 }
        }
      },
      countedTotal: { $sum: 1 }
    }
  }
  pipeline.push(group)
  pipeline.push({ $sort: { _id: 1 } })
  const finalGroup = {
    $group: {
      _id: null,
      countedBrokerPartners: { $sum: '$countedBroker' },
      countedDirectPartners: { $sum: '$countedDirect' },
      countedTotalPartners: { $sum: '$countedTotal' },
      activePartnerGraphData: {
        $push: {
          date: '$_id',
          countedBroker: '$countedBroker',
          countedDirect: '$countedDirect',
          countedTotal: '$countedTotal'
        }
      }
    }
  }
  pipeline.push(finalGroup)
  const [activePartnerInfo] = await PartnerCollection.aggregate(pipeline)
  return activePartnerInfo
}

export const countIntegratedPartners = async (query, session) => {
  const partnerIds = await IntegrationCollection.distinct(
    'partnerId',
    query
  ).session(session)
  return partnerIds.length
}

export const isEnabledPartnerCreditRating = async (partnerInfoOrId) => {
  if (!size(partnerInfoOrId)) return false

  if (isString(partnerInfoOrId)) {
    const partner = (await getPartnerById(partnerInfoOrId)) || {}
    return !!partner?.enableCreditRating
  }

  return !!partnerInfoOrId?.enableCreditRating
}

export const preparePartnerFunctionalityUpdateData = (
  body = {},
  partner = {}
) => {
  const {
    enableFinn,
    enableTransactionsApi,
    enableTransactionsPeriod,
    enableInvoiceStartNumber,
    enableAnnualStatement,
    enableSkatteetaten,
    enableBrokerJournals,
    enableDepositAccount,
    enableCreditRating,
    enableRecurringDueDate
  } = body
  const updateData = {}
  if (has(body, 'enableFinn')) updateData.enableFinn = enableFinn
  if (has(body, 'enableTransactionsApi')) {
    updateData.enableTransactionsApi = enableTransactionsApi
    updateData.enableTransactionsPeriod = enableTransactionsApi
  }
  if (has(body, 'enableTransactionsPeriod')) {
    if (!partner.enableTransactionsApi) {
      throw new CustomError(400, 'Enable transactions api first')
    }
    updateData.enableTransactionsPeriod = enableTransactionsPeriod
  }
  if (has(body, 'enableInvoiceStartNumber')) {
    if (enableInvoiceStartNumber === false) {
      throw new CustomError(
        400,
        `You can't disable the individual invoice number series`
      )
    }
    if (partner.accountType !== 'direct') {
      throw new CustomError(
        400,
        `You can't enable the individual invoice number series for broker partner`
      )
    }
    updateData.enableInvoiceStartNumber = true
  }
  if (has(body, 'enableAnnualStatement')) {
    if (enableAnnualStatement === false) {
      throw new CustomError(400, `You can't disable the annual statement`)
    }
    updateData.enableAnnualStatement = true
  }
  if (has(body, 'enableSkatteetaten')) {
    if (enableSkatteetaten === false) {
      throw new CustomError(400, `You can't disable the report to skatteetaten`)
    }
    updateData.enableSkatteetaten = true
  }
  if (has(body, 'enableBrokerJournals')) {
    if (enableBrokerJournals === false) {
      throw new CustomError(400, `You can't disable the broker journals`)
    }
    updateData.enableBrokerJournals = true
  }
  if (has(body, 'enableDepositAccount')) {
    if (enableDepositAccount === false) {
      throw new CustomError(400, `You can't disable the deposit account`)
    }
    updateData.enableDepositAccount = true
  }
  if (has(body, 'enableCreditRating')) {
    if (enableCreditRating === false) {
      throw new CustomError(400, `You can't disable the credit rating`)
    }
    updateData.enableCreditRating = true
  }
  if (has(body, 'enableRecurringDueDate')) {
    updateData.enableRecurringDueDate = enableRecurringDueDate
  }
  return updateData
}

export const getSelfServicePartner = async () => {
  const partner = await getAPartner(
    { isActive: true, isSelfService: true },
    null
  )
  if (!partner?._id) {
    throw new CustomError(404, 'No self-service partner found!')
  }
  return partner
}
