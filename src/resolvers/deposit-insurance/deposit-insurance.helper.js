import { compact, isArray, round, size } from 'lodash'
import moment from 'moment-timezone'

import { DepositInsuranceCollection } from '../models'
import { CustomError } from '../common'
import { counterService } from '../services'
import {
  accountHelper,
  appHelper,
  appInvoiceHelper,
  appQueueHelper,
  contractHelper,
  depositInsuranceHelper,
  partnerHelper,
  partnerSettingHelper,
  settingHelper,
  tenantHelper
} from '../helpers'
import checkdigit from 'checkdigit'

export const getADepositInsurance = async (query, session, populate = []) => {
  const depositInsurance = await DepositInsuranceCollection.findOne(query)
    .session(session)
    .populate(populate)
  return depositInsurance
}

export const getUniqueValueOfAField = async (field, query, session) => {
  const values =
    (await DepositInsuranceCollection.distinct(field, query).session(
      session
    )) || []
  return values
}

export const prepareDepositInsuranceQueryAndUpdatingData = async (data) => {
  const { insuranceData, queueId } = data
  const appQueueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (!size(appQueueInfo))
    throw new CustomError(404, "App queue doesn't exists")

  const { params = {} } = appQueueInfo
  const { depositInsuranceId = '' } = params
  const { Messages, ResultCode: resultCode } = insuranceData || {}
  const status = resultCode !== 'Success' ? 'failed' : 'registered'
  const creationResult = {
    createdAt: new Date(),
    resultCode,
    amount: insuranceData.Amount,
    entityId: insuranceData.EntityId,
    insuranceNo: insuranceData.GuaranteeNo,
    policyUrl: insuranceData.PolicyUrl
  }

  const { string: reasons } = Messages || {}
  if (isArray(reasons)) creationResult.reasons = reasons
  else creationResult.reasons = compact([reasons])
  const query = { _id: depositInsuranceId }
  const updatingData = { $set: { status, creationResult } }

  return { query, updatingData }
}

export const getBrokerObj = async () => {
  const settings = await settingHelper.getSettingInfo({})
  const brokerObj = {
    organizationId: settings?.appInfo?.organizationId,
    emailAddress: settings?.appInfo?.email,
    phoneNumber: settings?.appInfo?.phoneNumber
  }

  return brokerObj
}

export const getDividedNameString = (stringValue) => {
  const splitStr = stringValue.split(' ')
  const splitArraySize = size(splitStr)
  let firstStr = ''
  let lastStr = ''

  splitStr.forEach((str, index) => {
    if (index === 0) firstStr = str
    else {
      if (index !== 1 && index !== splitArraySize) {
        lastStr += ' '
      }
      lastStr += str
    }
  })

  return { firstStr, lastStr }
}

export const getAccountObj = async (accountInfo) => {
  if (size(accountInfo)) {
    const { name = '', organization = {}, person = {} } = accountInfo
    const address = accountInfo.getAddress()
    const city = accountInfo.getCity()
    const zipCode = accountInfo.getZipCode()
    const { firstStr = '', lastStr = '' } = getDividedNameString(name)

    accountInfo.firstName = firstStr
    accountInfo.lastName = lastStr

    if (address) {
      accountInfo.addressName = appHelper.getAddressName(address)
    }
    if (size(organization)) {
      accountInfo.organizationName = organization.name ? organization.name : ''
    }
    if (city) accountInfo.city = city
    if (zipCode) accountInfo.zipCode = zipCode

    if (size(person)) {
      const personEmail = person.getEmail() || null
      const personPhoneNumber = person.getPhone() || null
      if (personEmail) accountInfo.emailAddress = personEmail
      if (personPhoneNumber) accountInfo.phoneNumber = personPhoneNumber
    }
  }

  return accountInfo
}

export const getTenantObj = async (tenantInfo) => {
  if (size(tenantInfo)) {
    const { name = '', user = {} } = tenantInfo
    const address = await tenantInfo.getAddress()
    const city = await tenantInfo.getCity()
    const zipCode = await tenantInfo.getZipCode()
    const { firstStr = '', lastStr = '' } = getDividedNameString(name)

    tenantInfo.firstName = firstStr
    tenantInfo.lastName = lastStr
    if (address) {
      tenantInfo.addressName = appHelper.getAddressName(address)
    }
    if (city) tenantInfo.city = city
    if (zipCode) tenantInfo.zipCode = zipCode

    if (size(user)) {
      const personEmail = user.getEmail() || null
      const personPhoneNumber = user.getPhone() || null

      if (personEmail) tenantInfo.emailAddress = personEmail
      if (personPhoneNumber) tenantInfo.phoneNumber = personPhoneNumber
    }
  }

  return tenantInfo
}

export const getPropertyObj = (propertyInfo) => {
  if (!size(propertyInfo)) return {}

  const locationName = propertyInfo.getLocationName()
  const propertyAddress = appHelper.getAddressName(locationName)
  const propertyCity = propertyInfo.getCity()
  const propertyZipCode = propertyInfo.getPostalCode()
  const propertyApartmentId = propertyInfo.getApartmentId()
  const propertySnr = propertyInfo.snr || ''

  const propertyObj = {
    propertyAddress,
    propertyApartmentId,
    propertyCity,
    propertyZipCode,
    propertySnr
  }
  return propertyObj
}

export const getContractRefNumber = async (contractInfo) => {
  const { partner, partnerId } = contractInfo

  const partnerSerial = appHelper.getFixedDigits(partner?.serial, 4)

  const nextVal = await counterService.incrementCounter(
    `deposit-insurance-${partnerId}`
  )
  const depositInsuranceCounter = appHelper.getFixedDigits(nextVal, 5)

  const refNumber = `${partnerSerial}${depositInsuranceCounter}`

  return refNumber
}

export const prepareXmlGeneratingDataForDI = async (queueId) => {
  const appQueueInfo = await appQueueHelper.getQueueItemById(queueId)
  if (!size(appQueueInfo))
    throw new CustomError(404, "App queue doesn't exists")

  const { params = {} } = appQueueInfo
  const { contractId = '', depositInsuranceId = '' } = params
  if (!(contractId && depositInsuranceId))
    throw new CustomError(404, 'Required params are missing')

  const contractInfo = await contractHelper.getAContract(
    { _id: contractId },
    null,
    ['partner', 'property']
  )
  const depositInsuranceInfo =
    await depositInsuranceHelper.getADepositInsurance({
      _id: depositInsuranceId
    })
  if (!(size(contractInfo) && size(depositInsuranceInfo))) return {}
  const {
    accountId = null,
    partnerId = null,
    property,
    rentalMeta = {}
  } = contractInfo
  const { tenantId = null } = rentalMeta
  const { depositAmount = 0 } = depositInsuranceInfo
  const accountInfo = accountId
    ? await accountHelper.getAnAccount({ _id: accountId }, null, [
        'person',
        'organization'
      ])
    : null
  const tenantInfo = tenantId
    ? await tenantHelper.getATenant({ _id: tenantId }, null, ['user'])
    : null

  const contractRefNumber = await getContractRefNumber(contractInfo)
  const brokerObj = await getBrokerObj()
  const accountObj = await getAccountObj(accountInfo)
  const tenantObj = await getTenantObj(tenantInfo)
  const propertyObj = getPropertyObj(property)

  const dateFormat = 'YYYY-MM-DDTHH:mm:ss'
  const contractStartDate = (
    await appHelper.getActualDate(
      partnerId,
      true,
      rentalMeta?.contractStartDate
    )
  ).format(dateFormat)
  const contractEndDate = rentalMeta?.contractEndDate
    ? (
        await appHelper.getActualDate(
          partnerId,
          true,
          rentalMeta?.contractEndDate
        )
      ).format(dateFormat)
    : (
        await appHelper.getActualDate(
          partnerId,
          true,
          moment(rentalMeta?.signedAt).add(3, 'years').toDate()
        )
      ).format(dateFormat)
  const leaseSignedAt = (
    await appHelper.getActualDate(partnerId, true, rentalMeta?.signedAt)
  ).format(dateFormat)

  const dataForXml = {
    contractRefNumber,
    contractStartDate,
    contractEndDate,
    leaseSignedAt,
    depositAmount
  }
  if (size(brokerObj)) dataForXml.brokerObj = brokerObj
  if (size(accountObj)) dataForXml.accountObj = accountObj
  if (size(tenantObj)) dataForXml.tenantObj = tenantObj
  if (size(propertyObj)) dataForXml.propertyObj = propertyObj

  return dataForXml
}

export const getDepositInsuranceXmlGeneratingData = async (req) => {
  const { body, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)

  const { query } = body
  appHelper.checkRequiredFields(['queueId'], query)

  const { queueId = '' } = query
  appHelper.validateId({ queueId })

  const xmlGeneratingData = await prepareXmlGeneratingDataForDI(queueId)
  return xmlGeneratingData
}

export const getParamsForQueueCreationOfDINotification = async (data) => {
  const { _id, partnerId = '', contractId = '' } = data
  const partnerSetting = partnerId
    ? await partnerSettingHelper.getAPartnerSetting({ partnerId })
    : null
  const { notifications = {} } = partnerSetting || {}

  if (!(size(notifications) && notifications.depositInsurance)) return {}

  const params = {
    collectionId: contractId,
    collectionNameStr: 'contracts',
    depositInsuranceId: _id,
    partnerId
  }

  return { params }
}

const getPipelineForTenant = () => [
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
            pipeline: [...appHelper.getUserEmailPipeline()],
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
            phoneNumber: '$user.profile.phoneNumber',
            email: '$user.email',
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$user.profile.avatarKey'
            ),
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

const getPipelineForProperty = () => [
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
  {
    $unwind: {
      path: '$propertyInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getAppInvoicePipeline = () => [
  {
    $lookup: {
      from: 'app_invoices',
      localField: '_id',
      foreignField: 'depositInsuranceId',
      pipeline: [
        {
          $project: {
            _id: 1,
            serialId: 1
          }
        }
      ],
      as: 'appInvoiceInfo'
    }
  },
  {
    $unwind: {
      path: '$appInvoiceInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getDepositInsurancesForQuery = async (body) => {
  const { query, options } = body
  const { sort, skip, limit } = options
  const tenantPipeline = getPipelineForTenant()
  const propertyPipeline = getPipelineForProperty()
  const appInvoicePipeline = getAppInvoicePipeline()
  const setting = await settingHelper.getSettingInfo()
  const depositInsurances = await DepositInsuranceCollection.aggregate([
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
    ...propertyPipeline,
    ...tenantPipeline,
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appInvoicePipeline,
    {
      $project: {
        propertyInfo: 1,
        status: 1,
        depositInsuranceAmount: 1,
        totalPaymentAmount: 1,
        payments: 1,
        tenantInfo: 1,
        accountInfo: 1,
        branchInfo: 1,
        kidNumber: 1,
        referenceNo: '$creationResult.insuranceNo',
        createdAt: 1,
        appInvoiceInfo: 1,
        errorReason: {
          $reduce: {
            input: { $ifNull: ['$creationResult.reasons', []] },
            initialValue: '',
            in: {
              $concat: [
                '$$value',
                {
                  $cond: [
                    { $eq: ['$$value', ''] },
                    '$$this',
                    { $concat: [', ', '$$this'] }
                  ]
                }
              ]
            }
          }
        },
        bankAccountNumber: setting?.bankAccountNumber
      }
    }
  ])
  return depositInsurances
}

const countDepositInsurances = async (query) => {
  const numOfDepositInsurances =
    await DepositInsuranceCollection.countDocuments(query)
  return numOfDepositInsurances
}

const prepareDepositInsuranceQueryBasedOnFilters = async (query) => {
  const {
    accountId,
    agentId,
    bankAccountNumber,
    branchId,
    contractId,
    createdAtDateRange,
    depositInsuranceAmount,
    invoiceKIDNumber,
    invoiceSerialId,
    partnerId,
    propertyId,
    searchKeyword,
    status,
    tenantId
  } = query
  const preparedQuery = {}
  if (agentId) preparedQuery.agentId = agentId
  if (accountId) preparedQuery.accountId = accountId
  if (bankAccountNumber) preparedQuery.bankAccountNumber = bankAccountNumber
  if (branchId) preparedQuery.branchId = branchId
  if (contractId) preparedQuery.contractId = contractId
  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    preparedQuery.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  if (query.hasOwnProperty('depositInsuranceAmount')) {
    const parsedAmount = Number(depositInsuranceAmount)
    if (!!parsedAmount) {
      preparedQuery.depositInsuranceAmount = parsedAmount
    } else {
      preparedQuery._id = 'nothing'
    }
  }
  if (invoiceKIDNumber) {
    const depositInsuranceIds = await appInvoiceHelper.getUniqueFieldValue(
      'depositInsuranceId',
      {
        kidNumber: { $regex: invoiceKIDNumber, $options: 'i' }
      }
    )
    if (size(depositInsuranceIds)) {
      preparedQuery._id = { $in: depositInsuranceIds }
    } else preparedQuery._id = 'nothing'
  }
  if (query.hasOwnProperty('invoiceSerialId')) {
    const depositInsuranceIds = !!parseInt(invoiceSerialId)
      ? (await appInvoiceHelper.getUniqueFieldValue('depositInsuranceId', {
          serialId: invoiceSerialId
        })) || []
      : []
    if (size(depositInsuranceIds)) {
      preparedQuery._id = { $in: depositInsuranceIds }
    } else preparedQuery._id = 'nothing'
  }
  if (partnerId) preparedQuery.partnerId = partnerId
  if (propertyId) preparedQuery.propertyId = propertyId
  if (searchKeyword) {
    const searchOrQuery = []

    const depositInsuranceIds = []
    if (!!Number(searchKeyword)) {
      searchOrQuery.push({ depositInsuranceAmount: Number(searchKeyword) })

      const depositInsuranceIdsForSerialId = !!parseInt(searchKeyword)
        ? (await appInvoiceHelper.getUniqueFieldValue('depositInsuranceId', {
            serialId: parseInt(searchKeyword)
          })) || []
        : []

      depositInsuranceIds.push(...depositInsuranceIdsForSerialId)
    }

    const depositInsuranceIdsForKidNumber =
      (await appInvoiceHelper.getUniqueFieldValue('depositInsuranceId', {
        kidNumber: { $regex: searchKeyword, $options: 'i' }
      })) || []

    if (size(depositInsuranceIdsForKidNumber)) {
      depositInsuranceIds.push(...depositInsuranceIdsForKidNumber)
    }

    if (size(depositInsuranceIds)) {
      searchOrQuery.push({ _id: { $in: depositInsuranceIds } })
    } else {
      searchOrQuery.push({ _id: 'nothing' })
    }

    if (size(searchOrQuery)) preparedQuery['$or'] = searchOrQuery
  }
  if (size(status)) {
    preparedQuery.status = { $in: status }
  }
  if (tenantId) preparedQuery.tenantId = tenantId
  return preparedQuery
}

export const queryDepositInsurances = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = { partnerId }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }

  query.partnerId = partnerId
  body.query = await prepareDepositInsuranceQueryBasedOnFilters(query)
  const depositInsurances = await getDepositInsurancesForQuery(body)
  const totalDocuments = await countDepositInsurances(totalDocumentsQuery)
  const filteredDocuments = await countDepositInsurances(body.query)
  return {
    data: depositInsurances,
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

const getDepositInsuranceSummary = async (query) => {
  const [summary] = await DepositInsuranceCollection.aggregate([
    {
      $match: query
    },
    {
      $facet: {
        total: [
          {
            $group: {
              _id: null,
              totalInsuranceAmount: { $sum: '$depositInsuranceAmount' }
            }
          }
        ],
        statusSummary: [
          {
            $group: {
              _id: '$status',
              insuranceAmount: { $sum: '$depositInsuranceAmount' }
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$total',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        totalDepositInsuranceAmount: '$total.totalInsuranceAmount',
        statusSummary: 1
      }
    }
  ])
  return summary
}

export const depositInsuranceSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })

  body.partnerId = partnerId
  const preparedQuery = await prepareDepositInsuranceQueryBasedOnFilters(body)
  const summary = await getDepositInsuranceSummary(preparedQuery)
  return summary
}

export const isEnabledDepositInsuranceForContract = async (
  partnerInfo,
  tenantInfo
) => {
  if (!(size(partnerInfo) && size(tenantInfo))) return false

  const isEnabledPartnerCreditRating =
    await partnerHelper.isEnabledPartnerCreditRating(partnerInfo)

  const hasTenantCreditRating = await tenantHelper.hasCreditRating(tenantInfo)

  return isEnabledPartnerCreditRating && hasTenantCreditRating
}

const isDepositInsuranceProcessEnabled = async (partnerId) => {
  const partner = (await partnerHelper.getAPartner({ _id: partnerId })) || {}
  const { enableCreditRating = false } = partner
  return enableCreditRating
}

export const checkRequirementsForAddingDI = async (params) => {
  const { partnerId, contractId } = params
  const isDIProcessEnabled = await isDepositInsuranceProcessEnabled(partnerId)
  const contract =
    (await contractHelper.getAContract({ _id: contractId })) || {}
  const { rentalMeta = {} } = contract
  const { tenantId = '' } = rentalMeta
  const hasTenantCreditInfo = await tenantHelper.hasTenantCreditInfo(tenantId)
  if (!isDIProcessEnabled)
    throw new CustomError(400, 'Deposit insurance not enabled for partner')
  if (!hasTenantCreditInfo)
    throw new CustomError(400, 'Tenant has no credit rating info')
  return true
}

const prepareDepositInsuranceKidNumber = (contract) => {
  const { property = {}, partner = {}, leaseSerial = '' } = contract

  const partnerSerial = appHelper.getFixedDigits(partner.serial, 4)
  const propertySerial = appHelper.getFixedDigits(property.serial, 5)
  const contractLeaseSerial = appHelper.getFixedDigits(leaseSerial, 3)
  const kidNumber = `${partnerSerial}${propertySerial}${contractLeaseSerial}`
  if (kidNumber) {
    checkdigit.mod11.create(kidNumber)
    return checkdigit.mod11.apply(kidNumber)
  }
  return kidNumber
}

export const preparePipelineForDepositInsurance = (contractId) => [
  {
    $match: {
      _id: contractId
    }
  },
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            serial: 1
          }
        }
      ],
      as: 'property'
    }
  },
  {
    $unwind: '$property'
  },
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            serial: 1
          }
        }
      ],
      as: 'partner'
    }
  },
  {
    $unwind: '$partner'
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
    $unwind: '$tenant'
  },
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: '$partnerSettings'
  }
]
export const prepareDataForDepositInsurance = (body, contract) => {
  const { partnerId = '', contractId = '' } = body || {}
  const {
    rentalMeta = {},
    branchId,
    agentId,
    accountId,
    propertyId
  } = contract || {}
  const { tenantId = '', depositAmount } = rentalMeta
  const kidNumber = prepareDepositInsuranceKidNumber(contract) || ''
  const depositInsuranceAmount = round(depositAmount * 0.16)
  return {
    partnerId,
    branchId,
    agentId,
    accountId,
    propertyId,
    tenantId,
    contractId,
    depositAmount,
    depositInsuranceAmount,
    kidNumber,
    status: 'created',
    isActive: true
  }
}

export const isEnabledDepositInsuranceProcess = async (contractInfo) => {
  if (!size(contractInfo))
    throw new CustomError(
      404,
      'Contract is required to check isEnabledDepositInsurance or not'
    )

  const partnerInfo = contractInfo.partnerId
    ? await partnerHelper.getAPartner({ _id: contractInfo.partnerId })
    : {}

  if (!size(partnerInfo))
    throw new CustomError(
      404,
      'Partner is required to check isEnabledDepositInsurance or not'
    )

  const tenantId = contractInfo.rentalMeta?.tenantId || ''

  const tenantInfo = tenantId
    ? await tenantHelper.getATenant({ _id: tenantId })
    : {}

  if (!size(tenantInfo))
    throw new CustomError(
      404,
      'TenantInfo is required to check isEnabledDepositInsurance or not'
    )

  return (
    !!partnerInfo?.enableCreditRating &&
    !!tenantInfo?.creditRatingInfo?.CDG2_GENERAL_SCORE?.SCORE
  )
}

export const getDepositInsuranceStatusForPartnerDashboard = async (query) => {
  const result = await DepositInsuranceCollection.aggregate([
    {
      $match: query
    },
    {
      $addFields: {
        awaitingDepositInsurance: {
          $cond: [
            {
              $in: ['$status', ['created', 'partially_paid', 'due']]
            },
            1,
            0
          ]
        },
        partiallyPaidDepositInsurance: {
          $cond: [
            {
              $eq: ['$status', 'partially_paid']
            },
            1,
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        awaitingDepositInsuranceCount: {
          $sum: '$awaitingDepositInsurance'
        },
        partiallyPaidDepositInsuranceCount: {
          $sum: '$partiallyPaidDepositInsurance'
        }
      }
    }
  ])
  const [depositInsuranceStatus = {}] = result || []
  const {
    awaitingDepositInsuranceCount = 0,
    partiallyPaidDepositInsuranceCount = 0
  } = depositInsuranceStatus
  return { awaitingDepositInsuranceCount, partiallyPaidDepositInsuranceCount }
}

export const getDepositInsuranceForPaymentReminder = async (
  query = {},
  options = {}
) => {
  const matchPipeline = {
    $match: {
      ...query,
      status: { $in: ['created', 'due', 'partially_paid'] }
    }
  }
  const contractLookupPipeline = {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      let: { tenantId: '$tenantId' },
      pipeline: [
        { $project: { rentalMeta: { status: 1, tenants: 1 }, status: 1 } },
        { $unwind: '$rentalMeta.tenants' },
        {
          $addFields: {
            isMatched: { $eq: ['$$tenantId', '$rentalMeta.tenants.tenantId'] }
          }
        },
        {
          $match: {
            'rentalMeta.status': { $ne: 'closed' },
            isMatched: true
          }
        }
      ],
      as: 'contract'
    }
  }
  const contractAddFieldsPipeline = {
    $addFields: { contract: { $first: '$contract' } }
  }
  const contractMatchPipeline = {
    $match: { 'contract._id': { $exists: true } }
  }
  const partnerSettingLookupPipeline = {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSetting'
    }
  }
  const partnerSettingAddFieldsPipeline = {
    $addFields: { partnerSetting: { $first: '$partnerSetting' } }
  }
  const depositInsuranceSettingAddFieldsPipeline = {
    $addFields: {
      depositInsurancePaymentReminder:
        '$partnerSetting.depositInsuranceSetting.paymentReminder'
    }
  }
  const depositInsuranceSettingMatchPipeline = {
    $match: {
      'depositInsurancePaymentReminder.enabled': true,
      'depositInsurancePaymentReminder.days': { $gt: 0 }
    }
  }
  const datesAddFieldsPipeline = {
    $addFields: {
      today: {
        $dateAdd: {
          startDate: new Date(),
          unit: 'day',
          amount: 0,
          timezone: '$partnerSetting.dateTimeSettings.timezone'
        }
      },
      paymentReminderSentAt: {
        $dateAdd: {
          startDate: { $ifNull: ['$paymentReminderSentAt', '$createdAt'] },
          unit: 'day',
          amount: 0,
          timezone: '$partnerSetting.dateTimeSettings.timezone'
        }
      }
    }
  }
  const endOfDaysDateAddFieldsPipeline = {
    $addFields: {
      today: {
        $dateFromParts: {
          year: { $year: { $toDate: '$today' } },
          month: { $month: { $toDate: '$today' } },
          day: { $dayOfMonth: { $toDate: '$today' } },
          hour: 23,
          minute: 59,
          second: 59,
          millisecond: 999,
          timezone: '$partnerSetting.dateTimeSettings.timezone'
        }
      },
      paymentReminderSentAt: {
        $dateFromParts: {
          year: { $year: { $toDate: '$paymentReminderSentAt' } },
          month: { $month: { $toDate: '$paymentReminderSentAt' } },
          day: { $dayOfMonth: { $toDate: '$paymentReminderSentAt' } },
          hour: 23,
          minute: 59,
          second: 59,
          millisecond: 999,
          timezone: '$partnerSetting.dateTimeSettings.timezone'
        }
      }
    }
  }
  const dateDifferenceAddFieldsPipeline = {
    $addFields: {
      dateDifferenceInDays: {
        $dateDiff: {
          startDate: '$paymentReminderSentAt',
          endDate: '$today',
          unit: 'day',
          timezone: '$partnerSetting.dateTimeSettings.timezone'
        }
      },
      createdAtDifferenceInDays: {
        $dateDiff: {
          startDate: '$createdAt',
          endDate: '$today',
          unit: 'day',
          timezone: '$partnerSetting.dateTimeSettings.timezone'
        }
      }
    }
  }
  const isDatePastOrTodayAddFieldsPipeline = {
    $addFields: {
      isDatePastOrToday: {
        $and: [
          {
            $gte: [
              '$dateDifferenceInDays',
              '$depositInsurancePaymentReminder.days'
            ]
          },
          { $lte: ['$createdAtDifferenceInDays', 120] } // https://github.com/Uninite/uninite.com/issues/13595#issuecomment-1430171352
        ]
      },
      partnerSetting: '$$REMOVE'
    }
  }
  const isDatePastOrTodayMatchPipeline = { $match: { isDatePastOrToday: true } }
  const appInvoiceLookupPipeline = {
    $lookup: {
      from: 'app_invoices',
      localField: '_id',
      foreignField: 'depositInsuranceId',
      as: 'appInvoice'
    }
  }
  const appInvoiceAddFieldsPipeline = {
    $addFields: {
      appInvoiceId: { $first: '$appInvoice._id' },
      appInvoice: '$$REMOVE'
    }
  }
  const projectPipeline = {
    $project: {
      appInvoiceId: 1,
      notificationSendingDate: '$today',
      partnerId: 1
    }
  }
  const sortPipeline = { $sort: options?.sort || { createdAt: 1 } }
  const skipPipeline = { $skip: options?.skip || 0 }
  const limitPipeline = { $limit: options?.limit || 50 }

  return DepositInsuranceCollection.aggregate([
    matchPipeline,
    sortPipeline,
    contractLookupPipeline,
    contractAddFieldsPipeline,
    contractMatchPipeline,
    partnerSettingLookupPipeline,
    partnerSettingAddFieldsPipeline,
    depositInsuranceSettingAddFieldsPipeline,
    depositInsuranceSettingMatchPipeline,
    datesAddFieldsPipeline,
    endOfDaysDateAddFieldsPipeline,
    dateDifferenceAddFieldsPipeline,
    isDatePastOrTodayAddFieldsPipeline,
    isDatePastOrTodayMatchPipeline,
    appInvoiceLookupPipeline,
    appInvoiceAddFieldsPipeline,
    projectPipeline,
    skipPipeline,
    limitPipeline
  ])
}
