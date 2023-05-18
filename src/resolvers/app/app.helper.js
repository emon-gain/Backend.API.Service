import accounting from 'accounting-js'
import { countries } from 'country-data'
const i18n = require('i18n')
import moment from 'moment-timezone'
import Handlebars from 'handlebars'
import {
  cloneDeep,
  difference,
  each,
  find,
  head,
  indexOf,
  isString,
  map,
  round,
  size
} from 'lodash'
import { validateNorwegianIdNumber } from 'norwegian-national-id-validator'

import settingsJSON from '../../../settings'
import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  branchHelper,
  partnerHelper,
  partnerSettingHelper,
  settingHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import {
  AccountCollection,
  AppHealthCollection,
  AppInvoiceCollection,
  CommentCollection,
  ContractCollection,
  ConversationMessageCollection,
  FileCollection,
  InvoiceCollection,
  InvoicePaymentCollection,
  ListingCollection,
  PayoutCollection,
  PropertyItemCollection,
  TaskCollection,
  TenantCollection
} from '../models'

// Configure i18n for language translation
i18n.configure({
  staticCatalog: {
    no: require('../../../i18n/no.json'),
    en: require('../../../i18n/en.json')
  },
  defaultLocale: 'en',
  objectNotation: true
})

/**
 * @param {string | object} partnerSettingOrId - partner setting collection object or partner id
 * @param {boolean} returnMoment - return moment object if true
 * @param {Date|null} date - Date object
 * @returns {Promise} - return actual date based on partner setting
 */
export const getActualDate = async (partnerSettingOrId, returnMoment, date) => {
  const paramDate = date ? date : new Date()
  let newDate
  let partnerSetting
  if (size(partnerSettingOrId)) {
    partnerSetting = partnerSettingOrId
    // If string, partnerSettingOrId param is an Id.
    if (isString(partnerSettingOrId)) {
      partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
        partnerSettingOrId
      )
    }

    const timezone = partnerSetting?.dateTimeSettings?.timezone || ''
    console.log(
      `=== Current TimeZone for partnerId: ${
        isString(partnerSettingOrId)
          ? partnerSettingOrId
          : partnerSettingOrId.partnerId
      }, timezone: ${timezone}`
    )
    if (timezone) {
      newDate = moment.tz(paramDate, timezone)
    } else {
      newDate = moment(paramDate, 'YYYY-MM-DD HH:mm:ss')
    }
  } else {
    newDate = moment(paramDate, 'YYYY-MM-DD HH:mm:ss')
  }
  // If return moment then return moment object, else native date object
  return returnMoment ? newDate : newDate.toDate()
}

export const convertTo2Decimal = async (amount, partnerSettingsOrId, type) => {
  if (type === 'round' && partnerSettingsOrId) {
    // If object, then we'll assume the partnerSettings otherwise partnerId
    let partnerSetting = partnerSettingsOrId
    if (isString(partnerSettingsOrId)) {
      partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
        partnerSettingsOrId
      )
    }
    const { invoiceSettings } = partnerSetting || {}
    const { numberOfDecimalInInvoice = 0 } = invoiceSettings || {}

    if (partnerSetting && numberOfDecimalInInvoice === 0) {
      const roundAmount = round(amount)
      return roundAmount
    }
    const decimalAmount = parseFloat(parseFloat(amount).toFixed(2))
    return decimalAmount
  }
  const decimalAmount = parseFloat(parseFloat(amount).toFixed(2))
  return decimalAmount
}

export const getDateFormat = async (partnerSettingsOrId) => {
  let dateFormats = 'DD.MM.YYYY'
  if (partnerSettingsOrId) {
    // If object, then we'll assume the partnerSettings otherwise partnerId
    let partnerSetting = partnerSettingsOrId
    if (isString(partnerSettingsOrId)) {
      partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
        partnerSettingsOrId
      )
    }
    if (
      partnerSetting &&
      partnerSetting.dateTimeSettings &&
      partnerSetting.dateTimeSettings.dateFormat
    ) {
      dateFormats = partnerSetting.dateTimeSettings.dateFormat
    }
  }
  return dateFormats
}

export const getRoundedAmount = async (total, partnerSetting) => {
  const type = 'round'
  const roundAmount = await convertTo2Decimal(total, partnerSetting, type)
  return roundAmount
}

export const getFixedDigits = (value, digits) => {
  const string = ''
  let newValue = value + string
  if (newValue.length < digits) {
    let zeros = ''
    const newDigits = digits - newValue.length
    for (let i = 1; i <= newDigits; i++) {
      zeros += '0'
    }
    newValue = zeros + newValue
  }
  return newValue
}

export const getUnwindPipeline = (field = '', preserve = true) => ({
  $unwind: {
    path: `$${field}`,
    preserveNullAndEmptyArrays: preserve
  }
})

export const getCurrencyOptions = async (params) => {
  const {
    number = 0,
    options = {},
    partnerSettingsOrId = '',
    showSymbol = false
  } = params

  let partnerSettings = partnerSettingsOrId,
    symbolFormat = '',
    precision = 0,
    symbolShow = showSymbol

  if (isString(partnerSettingsOrId) && partnerSettingsOrId) {
    partnerSettings = await partnerSettingHelper.getAPartnerSetting({
      partnerId: partnerSettingsOrId
    })
  }

  const { currencySettings = {} } = partnerSettings || {}

  if (!size(currencySettings)) return null

  const {
    currencyPosition = '',
    currencySymbol = '',
    decimalSeparator,
    numberOfDecimal,
    thousandSeparator = ''
  } = currencySettings

  const {
    inputMode = false,
    inputValue = false,
    isInvoice = false,
    isNotThousand = false
  } = options

  if (currencyPosition === 'left') symbolFormat = '%s%v'
  else if (currencyPosition === 'right') symbolFormat = '%v%s'
  else if (currencyPosition === 'left_with_space') symbolFormat = '%s %v'
  else if (currencyPosition === 'right_with_space') symbolFormat = '%v %s'
  else symbolFormat = '%s%v'

  let thousandOptions = thousandSeparator === 'space' ? ' ' : thousandSeparator

  // If is not thousand separator needed
  if (isNotThousand) thousandOptions = ''

  // For input related values, we'll show the decimals as inputted by user
  if (number && (inputMode || inputValue)) {
    precision = (number.toString().split('.')[1] || []).length

    // If there are no decimal inputed by user, then follow the settings
    if (inputValue && !precision) {
      precision = numberOfDecimal
    }
  } else precision = numberOfDecimal

  if (isInvoice) {
    precision = 2 // Show 2 decimal for invoice
    symbolShow = false
  }

  return {
    symbol: symbolShow ? currencySymbol : '',
    precision,
    thousand: thousandOptions,
    decimal: decimalSeparator,
    format: symbolFormat
  }
}

export const convertToCurrency = async (params, currencyOptions = {}) => {
  const { number = 0 } = params
  if (!size(currencyOptions)) currencyOptions = await getCurrencyOptions(params)

  if (number && size(currencyOptions)) {
    if (currencyOptions.precision) {
      return accounting.formatMoney(number, currencyOptions)
    } else {
      // Don't show '-0', when settings precision is 0
      if (number < 1 && number > -1) currencyOptions.precision = 2

      return accounting.formatMoney(number, currencyOptions)
    }
  } else if (number) {
    return Math.round(number)
  } else return 0
}

export const subtractDays = async (date, days, partnerSetting) => {
  const calculatedDays = (await getActualDate(partnerSetting, true, date))
    .subtract(days || 0, 'days')
    .toDate()
  return calculatedDays
}

export const getLowerCase = (text) => {
  if (text) {
    return text.toLowerCase()
  }
}

export const getDefaultLogoURL = (logoName = '', folderName = 'default') => {
  const CDN = process.env.CDN_DOMAIN
  if (!CDN) throw new CustomError(404, 'Missing CDN URL')

  if (!logoName) throw new CustomError(400, 'Logo name is required')

  return `${CDN}/logo/${folderName}/${logoName}.png`
}

export const checkEmailDuplication = async (email, session) => {
  const query = {
    emails: {
      $elemMatch: {
        address: email
      }
    }
  }
  const isUserExists = await userHelper.countUsers(query, session)
  if (isUserExists) {
    throw new CustomError(405, 'Email already exists')
  }
}

export const checkNIDDuplication = async (nid, session) => {
  const query = {
    'profile.norwegianNationalIdentification': nid
  }
  const isUserExists = await userHelper.getAnUser(query, session)
  if (isUserExists) {
    throw new CustomError(405, 'NID already exists')
  }
}

export const validateSortForQuery = (sort) => {
  Object.keys(sort).forEach((key) => {
    const value = sort[key]
    if (!(value === 1 || value === -1))
      throw new CustomError(400, 'Sorting order value should be only 1 or -1')
  })
}

export const validateCreatedAtForQuery = (createdDateRange) => {
  const formats = [
    'YYYY-MM-DD LT',
    'YYYY-MM-DD h:mm:ss A',
    'YYYY-MM-DD HH:mm:ss',
    'YYYY-MM-DD HH:mm'
  ]
  if (size(createdDateRange)) {
    const { startDate, endDate } = createdDateRange
    if (!moment(moment(startDate), formats, true).isValid())
      throw new CustomError(400, 'startDate must be a Date')
    if (!moment(moment(endDate), formats, true).isValid())
      throw new CustomError(400, 'endDate must be a Date')
  }
}

export const validateId = (params) => {
  const [name] = Object.keys(params)
  const [value] = Object.values(params)
  if (!(isString(value) && value.length === 17))
    throw new CustomError(400, `Invalid ${name}`)
}

export const isValidDate = (date) => date instanceof Date && !isNaN(date)

export const validateArrayOfId = (params) => {
  const [name] = Object.keys(params)
  const [value] = Object.values(params)
  for (let i = 0; i < value.length; i++) {
    if (!(isString(value[i]) && value[i].length === 17))
      throw new CustomError(400, `Invalid ${name} of ${value[i]}`)
  }
}

export const checkUserId = (userId) => {
  if (userId) {
    if (userId === 'SYSTEM' || userId === 'Lambda') return true
    else validateId({ userId })
  } else throw new CustomError(401, 'Unauthorized')
}

export const autoDateGenerator = async (params) => {
  const { partnerIdOrSettings, eventName } = params || {}
  const partnerTime = size(partnerIdOrSettings)
    ? await appHelper.getActualDate(partnerIdOrSettings, true, new Date())
    : moment.tz(new Date(), 'Europe/Oslo')

  const momentObjForStartDate = cloneDeep(partnerTime)
  const momentObjForEndDate = cloneDeep(partnerTime)

  let startDate = null
  let endDate = null

  if (eventName === 'today') {
    startDate = momentObjForStartDate.startOf('day')
    endDate = momentObjForEndDate.endOf('day')
  } else if (eventName === 'thisWeek') {
    startDate = momentObjForStartDate.startOf('week')
    endDate = momentObjForEndDate.endOf('week')
  } else if (eventName === 'lastWeek') {
    startDate = momentObjForStartDate.subtract(7, 'days').startOf('week')
    endDate = momentObjForEndDate.subtract(7, 'days').endOf('week')
  } else if (eventName === 'thisMonth') {
    startDate = momentObjForStartDate.startOf('month')
    endDate = momentObjForEndDate.endOf('month')
  } else if (eventName === 'lastMonth') {
    startDate = momentObjForStartDate.subtract(1, 'month').startOf('month')
    endDate = momentObjForEndDate.subtract(1, 'month').endOf('month')
  } else if (eventName === 'thisYear') {
    startDate = momentObjForStartDate.startOf('year')
    endDate = momentObjForEndDate.endOf('year')
  } else if (eventName === 'lastYear') {
    startDate = momentObjForStartDate.subtract(1, 'year').startOf('year')
    endDate = momentObjForEndDate.subtract(1, 'year').endOf('year')
  } else if (eventName === 'yesterday') {
    startDate = momentObjForStartDate.subtract(1, 'day').startOf('day')
    endDate = momentObjForEndDate.subtract(1, 'day').endOf('day')
  }

  if (startDate && endDate) {
    return { startDate: startDate._d, endDate: endDate._d }
  }
  return {}
}

export const getCDNDomain = () => process.env.CDN_DOMAIN

export const prepareQueryBasedOnPartnerId = (partnerId) => {
  const query = {}
  if (partnerId) {
    validateId({ partnerId })
    query.partnerId = partnerId
  } else {
    query['partnerId'] = {
      $exists: false
    }
  }
  return { query }
}

export const isAppAdmin = (roles = []) => {
  const isAppAdmin = indexOf(roles, 'app_admin') > -1
  return isAppAdmin
}

export const isPartnerAdmin = (roles = []) => {
  const isPartnerAdmin = indexOf(roles, 'partner_admin') > -1
  return isPartnerAdmin
}

export const isPartnerJanitor = (roles = []) => {
  const isPartnerJanitor = indexOf(roles, 'partner_janitor') > -1
  return isPartnerJanitor
}

export const checkPartnerId = (user = {}, body = {}) => {
  const { roles = [] } = user
  const { partnerId = '' } = body
  if (partnerId) {
    validateId({ partnerId })
  }
  if (isPartnerAdmin(roles) && !isAppAdmin(roles)) {
    if (partnerId) {
      if (partnerId !== user.partnerId) {
        throw new CustomError(401, 'Unauthorized')
      }
    } else {
      throw new CustomError(400, `Missing partnerId`)
    }
  }
}

export const convertToInt = (val) => {
  if (typeof val === 'number') {
    return parseInt(val.toFixed(0))
  } else {
    return parseInt(val)
  }
}

// Deletes all null and undefined fields from the object
export const compactObject = (data = {}, requireString = true) => {
  each(data, (value, key) => {
    if (
      value === null ||
      value === undefined ||
      (requireString && value === '')
    ) {
      delete data[key]
    }
  })
}

export const checkRequiredFields = (requiredFields = [], data = {}) => {
  const missingFields = difference(requiredFields, Object.keys(data))
  if (size(missingFields)) {
    throw new CustomError(400, `Missing ${missingFields}`)
  }
}

export const getAddressHouseNumber = (address) => {
  if (size(address)) {
    const stringsNumberArray = address.match(/\d+/g)
    return size(stringsNumberArray) ? head(stringsNumberArray) : ''
  } else {
    return ''
  }
}

export const getAddressHouseBlockNumber = (address) => {
  if (size(address)) {
    const addressHouseNumber = getAddressHouseNumber(address)
    const stringsDividedArray = address
      .slice(address.indexOf(addressHouseNumber) + size(addressHouseNumber))
      .trim()
      .split(' ')
    const addressHouseBlockNumber = head(stringsDividedArray)

    return addressHouseBlockNumber
  } else {
    return ''
  }
}

export const getAddressName = (address) => {
  if (size(address)) {
    const addressHouseBlockNumber = getAddressHouseBlockNumber(address)
    const addressHouseNumber = getAddressHouseNumber(address)
    let addressName = ''

    if (address.indexOf(addressHouseNumber) === 0) {
      const addressNameWithoutHouseBlockNumber = address.replace(
        addressHouseBlockNumber,
        ''
      )
      addressName = addressNameWithoutHouseBlockNumber
        .replace(addressHouseNumber, '')
        .trim()
    } else {
      addressName = address.slice(0, address.indexOf(addressHouseNumber)).trim()
    }
    return addressName
  } else {
    return ''
  }
}

export const translateToUserLng = (langKey, language = 'no', options = {}) => {
  i18n.setLocale(language)
  return i18n.__(langKey, options)
}

export const getCurrencyOfCountry = async (countryCode = '', session) => {
  const { currencies = [] } = countries[countryCode] || {}
  const [currency] = currencies
  if (currency) {
    // Check is this currency exists in currency settings, otherwise we'll not be able to convert this listing price
    const setting = await settingHelper.getSettingInfo(
      { 'currencySettings.currency': currency },
      session
    )
    if (size(setting)) {
      return currency // Setting exists. so, it's a valid currency
    }
  }
  return 'NOK' // Else return NOK
}

export const checkPositiveNumbers = (data = {}) => {
  const negativeFields = []
  each(data, (value, key) => {
    if (typeof value === 'number' && value < 0) {
      negativeFields.push(key)
    }
  })
  if (size(negativeFields)) {
    throw new CustomError(
      400,
      `Negative value not allowed for: ${negativeFields}`
    )
  }
}

export const SSR = (content, variables) => {
  const template = Handlebars.compile(content)
  return template(variables)
}

export const getCollectionNameAndFieldNameByString = (collectionNameStr) => {
  const collectionInfo = {}

  if (collectionNameStr) {
    if (collectionNameStr === 'accounts') {
      collectionInfo.collectionName = AccountCollection
      collectionInfo.fieldName = 'accountId'
    } else if (collectionNameStr === 'app-health') {
      collectionInfo.collectionName = AppHealthCollection
    } else if (
      collectionNameStr === 'app_invoices' ||
      collectionNameStr === 'invoices'
    ) {
      collectionInfo.collectionName =
        collectionNameStr === 'invoices'
          ? InvoiceCollection
          : AppInvoiceCollection
      collectionInfo.fieldName = 'invoiceId'
    } else if (collectionNameStr === 'comments') {
      collectionInfo.collectionName = CommentCollection
      collectionInfo.fieldName = 'commentId'
    } else if (collectionNameStr === 'contracts') {
      collectionInfo.collectionName = ContractCollection
      collectionInfo.fieldName = 'contractId'
    } else if (collectionNameStr === 'conversation-messages') {
      collectionInfo.collectionName = ConversationMessageCollection
    } else if (collectionNameStr === 'files') {
      collectionInfo.collectionName = FileCollection
      collectionInfo.fieldName = 'fileId'
    } else if (collectionNameStr === 'payouts') {
      collectionInfo.collectionName = PayoutCollection
      collectionInfo.fieldName = 'payoutId'
    } else if (collectionNameStr === 'payments') {
      collectionInfo.collectionName = InvoicePaymentCollection
      collectionInfo.fieldName = 'paymentId'
    } else if (collectionNameStr === 'listings') {
      collectionInfo.collectionName = ListingCollection
      collectionInfo.fieldName = 'propertyId'
    } else if (collectionNameStr === 'property-items') {
      collectionInfo.collectionName = PropertyItemCollection
      collectionInfo.fieldName = 'movingId'
    } else if (collectionNameStr === 'tenants') {
      collectionInfo.collectionName = TenantCollection
      collectionInfo.fieldName = 'tenantId'
    } else if (collectionNameStr === 'tasks') {
      collectionInfo.collectionName = TaskCollection
      collectionInfo.fieldName = 'taskId'
    }
  }

  return collectionInfo
}

export const getFileDirective = (fileDirective) => {
  const directive = settingsJSON.S3.Directives[fileDirective]
  return directive
}

export const getPartnerURL = async (partnerId, isV1Link = false, session) => {
  let partnerUrl = process.env.PARTNER_SITE_URL || ''
  if (!partnerUrl) throw new CustomError(404, 'Unable to find partnerSite')

  if (partnerId) {
    const { subDomain = '' } =
      (await partnerHelper.getAPartner({ _id: partnerId }, session)) || {}
    if (subDomain) {
      const regex = /SUBDOMAIN/gi

      partnerUrl = partnerUrl.replace(regex, subDomain)

      if (isV1Link) {
        const stage = process.env.STAGE

        if (process.env.EXCLUDE_V2_FROM_URL === '1') {
          partnerUrl =
            stage === 'production'
              ? partnerUrl.replace(`.app`, '.v1')
              : partnerUrl.replace(`app.${stage}`, `${stage}.v1`)
        } else {
          partnerUrl =
            stage === 'production'
              ? partnerUrl.replace(`.app.v2`, '')
              : partnerUrl.replace(`app.${stage}.v2`, `${stage}`)
        }
      }

      return partnerUrl
    }
    return ''
  }
  return ''
}

export const getPartnerPublicURL = async (partnerId, session) => {
  let partnerPublicUrl = process.env.PARTNER_PUBLIC_SITE_URL
  if (!partnerPublicUrl)
    throw new CustomError(404, 'Unable to find partnerSite')

  if (partnerId) {
    const { subDomain = '' } =
      (await partnerHelper.getAPartner({ _id: partnerId }, session)) || {}
    if (subDomain) {
      const regex = /SUBDOMAIN/gi

      partnerPublicUrl = partnerPublicUrl.replace(regex, subDomain)

      return partnerPublicUrl
    }
    return ''
  }
  return ''
}

export const getAdminURL = () => process.env.ADMIN_SITE_URL

export const getPublicURL = () => process.env.Public_SITE_URL

export const getLinkServiceURL = () => process.env.LINK_SERVICE_URL

export const getAuthServiceURL = () => process.env.AUTH_SERVICE_URL

export const getSettingsInfoByFieldName = async (fieldName) => {
  const settingsInfo = await settingHelper.getSettingInfo({})

  return size(settingsInfo) && settingsInfo[fieldName]
    ? settingsInfo[fieldName]
    : fieldName === 'openExchangeInfo'
    ? {}
    : []
}

// For MongoDB aggregate
export const getDateFormatString = (dateRange) => {
  let formatString = '%Y'
  if (dateRange === 'thisMonth' || dateRange === 'lastMonth') {
    formatString = '%Y-%m-%d'
  } else if (dateRange === 'thisYear' || dateRange === 'lastYear') {
    formatString = '%Y-%m'
  }
  return formatString
}

export const getFormattedExportDate = async (partnerSettingsOrId, date) =>
  partnerSettingsOrId && date
    ? moment(await getActualDate(partnerSettingsOrId, true, date)).format(
        await getDateFormat(partnerSettingsOrId)
      )
    : ''

export const getDateRangeFromStringDate = async (partnerId, dateRange) => {
  if (
    partnerId &&
    dateRange &&
    dateRange.startDate_string &&
    dateRange.endDate_string
  ) {
    const startDate = (
      await getActualDate(partnerId, true, dateRange.startDate_string)
    )
      .startOf('day')
      .toDate()
    const endDate = (
      await getActualDate(partnerId, true, dateRange.endDate_string)
    )
      .endOf('day')
      .toDate()

    return { startDate, endDate }
  }
}

export const getFixedDigitsSerialNumber = (dataArr = []) => {
  if (size(dataArr)) {
    let serialNumber = ''
    for (const dataObj of dataArr) {
      const { digits, value } = dataObj || {}
      serialNumber += getFixedDigits(value, digits)
    }
    return serialNumber
  }
  return ''
}

export const getUserAvatarKeyPipeline = (
  path,
  defaultPath = 'assets/default-image/user-primary.png',
  prefix = ''
) => {
  const branches = [
    {
      case: { $ifNull: [path, false] },
      then: {
        $concat: [getCDNDomain(), '/', path]
      }
    }
  ]
  if (prefix) {
    // Return field when prefix object not exist
    branches.push({
      case: {
        $not: {
          $ifNull: ['$' + prefix + '._id', false]
        }
      },
      then: '$$REMOVE'
    })
  }
  return {
    $switch: {
      branches,
      default: {
        $concat: [getCDNDomain(), '/', defaultPath]
      }
    }
  }
}

export const getOrganizationLogoPipeline = (
  image = '',
  defaultPath = 'assets/default-image/organization-primary.png'
) => ({
  $cond: {
    if: { $ifNull: [image, false] },
    then: {
      $concat: [
        getCDNDomain(),
        '/partner_logo/',
        '$partnerId',
        '/accounts/',
        image
      ]
    },
    else: {
      $concat: [getCDNDomain(), '/', defaultPath]
    }
  }
})

export const getUserEmailPipeline = () => [
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
      }
    }
  }
]
export const getUserLanguageByPartnerId = async (partnerId, session) => {
  const { owner: partnerOwnerInfo } =
    (await partnerHelper.getAPartner({ _id: partnerId }), session, ['owner']) ||
    {}
  return partnerOwnerInfo?.getLanguage() || 'no'
}

export const getListingFirstImageUrl = (images = '', prefix = '') => {
  const directive = settingsJSON.S3.Directives['Listings']
  const attrPrefix = prefix ? prefix + '.' : ''
  const pipeline = [
    {
      $addFields: {
        firstImage: { $first: images }
      }
    },
    {
      $addFields: {
        [attrPrefix + 'imageUrl']: {
          $cond: [
            { $ifNull: ['$firstImage', false] },
            {
              $concat: [
                getCDNDomain(),
                '/',
                directive.folder,
                '/',
                '$' + attrPrefix + '_id',
                '/',
                '$firstImage.imageName'
              ]
            },
            {
              $concat: [
                getCDNDomain(),
                '/',
                'assets/default-image/property-primary.png'
              ]
            }
          ]
        }
      }
    }
  ]
  // To make whole object null if prefix object doesn't have property _id that means property not joined
  if (prefix) {
    pipeline.push({
      $addFields: {
        [prefix]: {
          $cond: [
            { $ifNull: ['$' + attrPrefix + '_id', false] },
            '$' + prefix,
            null
          ]
        }
      }
    })
  }
  return pipeline
}

export const getCommonAccountInfoPipeline = () => [
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
            avatarKey: {
              $cond: [
                { $eq: ['$type', 'person'] },
                getUserAvatarKeyPipeline('$person.profile.avatarKey'),
                getOrganizationLogoPipeline('$organization.image')
              ]
            }
          }
        }
      ],
      as: 'accountInfo'
    }
  },
  {
    $unwind: { path: '$accountInfo', preserveNullAndEmptyArrays: true }
  }
]

export const getCommonAgentInfoPipeline = () => [
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
            avatarKey: getUserAvatarKeyPipeline('$profile.avatarKey')
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

export const getCommonUserInfoPipeline = (
  localFieldName = '',
  asFieldName = ''
) => [
  {
    $lookup: {
      from: 'users',
      localField: localFieldName,
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: asFieldName
    }
  },
  getUnwindPipeline(asFieldName)
]

export const getCommonBranchInfoPipeline = () => [
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

export const getCommonContractInfoPipeline = (localFieldId = 'contractId') => [
  {
    $lookup: {
      from: 'contracts',
      localField: localFieldId,
      foreignField: '_id',
      as: 'contractInfo'
    }
  },
  getUnwindPipeline('contractInfo')
]

export const getCommonTenantInfoPipeline = (localField = 'tenantId') => [
  {
    $lookup: {
      from: 'tenants',
      localField,
      foreignField: '_id',
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
      localField: 'tenantInfo.userId',
      foreignField: '_id',
      as: 'tenantUser'
    }
  },
  {
    $unwind: {
      path: '$tenantUser',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      'tenantInfo.avatarKey': getUserAvatarKeyPipeline(
        '$tenantUser.profile.avatarKey',
        undefined,
        'tenantInfo'
      ),
      tenantUser: '$$REMOVE'
    }
  }
]

export const getCommonPropertyInfoPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'propertyInfo'
    }
  },
  getUnwindPipeline('propertyInfo'),
  ...getListingFirstImageUrl('$propertyInfo.images', 'propertyInfo')
]

export const isMoreOrLessThanTargetRows = async (
  collectionName,
  query,
  options = {}
) => {
  let { moreThan } = options
  const { moduleName, downloadDataList, rejectEmptyList } = options

  //Accept default data rows
  moreThan = moreThan ? moreThan : 50000
  let totalDataItems = collectionName
    ? await collectionName.find(query).countDocuments()
    : 0

  if (downloadDataList) totalDataItems = size(downloadDataList)

  if (rejectEmptyList && totalDataItems === 0)
    throw new CustomError(
      404,
      `${moduleName} not found, Please change the filter.`
    )

  const isNotExecutableDataSize = totalDataItems > moreThan
  if (isNotExecutableDataSize)
    throw new CustomError(
      400,
      `Too many ${moduleName}. Please change the filter.`
    )

  return totalDataItems
}

export const getSoonEndingTerminatedActiveUpcomingContractPipeline = (
  soonEndingMonth,
  needActiveContract = false,
  needUpcomingContract = false
) => {
  const addFields = []
  if (needActiveContract) {
    addFields.push({
      $addFields: {
        activeContract: {
          $first: {
            $filter: {
              input: { $ifNull: ['$contractsInfo', []] },
              as: 'contract',
              cond: {
                $and: [
                  {
                    $eq: ['$$contract.hasRentalContract', true]
                  },
                  {
                    $eq: ['$$contract.status', 'active']
                  }
                ]
              }
            }
          }
        }
      }
    })
  }
  if (needUpcomingContract) {
    addFields.push({
      $addFields: {
        upcomingContract: {
          $first: {
            $filter: {
              input: { $ifNull: ['$contractsInfo', []] },
              as: 'contract',
              cond: {
                $and: [
                  {
                    $eq: ['$$contract.hasRentalContract', true]
                  },
                  {
                    $eq: ['$$contract.status', 'upcoming']
                  }
                ]
              }
            }
          }
        }
      }
    })
  }
  return [
    {
      $lookup: {
        from: 'contracts',
        localField: '_id',
        foreignField: 'propertyId',
        as: 'contractsInfo'
      }
    },
    {
      $addFields: {
        soonEndingContracts: {
          $filter: {
            input: { $ifNull: ['$contractsInfo', []] },
            as: 'contract',
            cond: {
              $and: [
                {
                  $eq: ['$$contract.hasRentalContract', true]
                },
                {
                  $eq: ['$$contract.status', 'active']
                },
                {
                  $ifNull: ['$$contract.rentalMeta.contractEndDate', false]
                },
                {
                  $lte: [
                    '$$contract.rentalMeta.contractEndDate',
                    soonEndingMonth
                  ]
                }
              ]
            }
          }
        },
        terminatedContracts: {
          $filter: {
            input: { $ifNull: ['$contractsInfo', []] },
            as: 'contract',
            cond: {
              $and: [
                {
                  $ifNull: ['$$contract.terminatedByUserId', false]
                },
                {
                  $eq: ['$$contract.rentalMeta.status', 'active']
                }
              ]
            }
          }
        }
      }
    },
    {
      $addFields: {
        isSoonEnding: {
          $cond: [
            {
              $gt: [{ $size: '$soonEndingContracts' }, 0]
            },
            true,
            false
          ]
        },
        isTerminated: {
          $cond: [
            {
              $gt: [{ $size: '$terminatedContracts' }, 0]
            },
            true,
            false
          ]
        }
      }
    },
    ...addFields
  ]
}

export const createDownloadUrl = (endPoint) =>
  `${appHelper.getLinkServiceURL()}/download/${endPoint}` || ''

export const isAvailableBranchOfAgent = async (userId, partnerId) =>
  !!(await branchHelper.getABranch({ partnerId, agents: userId }))

export const getFilesPathUrl = () => {
  const directive = settingsJSON.S3.Directives['Files']
  return [
    {
      $addFields: {
        path: {
          $concat: [
            getCDNDomain(),
            '/',
            directive.folder,
            '/',
            {
              $switch: {
                branches: [
                  {
                    case: { $ifNull: ['$partnerId', false] },
                    then: '$partnerId'
                  },
                  {
                    case: { $ifNull: ['$landlordPartnerId', false] },
                    then: '$landlordPartnerId'
                  },
                  {
                    case: { $ifNull: ['$tenantPartnerId', false] },
                    then: '$tenantPartnerId'
                  }
                ],
                default: ''
              }
            },
            '/',
            '$context',
            '/',
            '$name'
          ]
        }
      }
    }
  ]
}

export const getUnion = (firstArray = [], secondArray = []) => [
  ...new Set([...firstArray, ...secondArray])
]

export const getDifference = (firstArray = [], secondArray = []) => {
  const map = {}
  for (let i = 0; i < firstArray.length; i++) {
    if (map[firstArray[i]] === undefined) map[firstArray[i]] = firstArray[i]
  }
  for (let i = 0; i < secondArray.length; i++) {
    if (map[secondArray[i]] !== undefined) delete map[secondArray[i]]
  }
  return Object.values(map) || []
}

export const getIntersection = (arr1, arr2) => {
  const setA = new Set(arr1)
  const intersectionResult = []

  for (const item of arr2) {
    if (setA.has(item)) {
      intersectionResult.push(item)
    }
  }
  return intersectionResult
}

export const getXor = (arr1, arr2) => {
  const map1 = {}
  const map2 = {}
  const xor = []

  for (let i = 0; i < arr1.length; i++) {
    if (map1[arr1[i]] === undefined) map1[arr1[i]] = arr1[i]
  }
  for (let i = 0; i < arr2.length; i++) {
    if (map2[arr2[i]] === undefined) map2[arr2[i]] = arr2[i]
    if (map1[arr2[i]] === undefined) xor.push(arr2[i])
  }
  for (let i = 0; i < arr1.length; i++) {
    if (map2[arr1[i]] === undefined) xor.push(arr1[i])
  }

  return xor
}

export const validatePartnerAppRequestData = (
  req = {},
  bodyRequiredFields = []
) => {
  const { body = {}, user = {} } = req
  const { roles = [] } = user
  if (!roles?.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }
  checkRequiredFields(['userId', 'partnerId'], body)
  const { partnerId, userId } = body
  checkUserId(userId)
  validateId({ partnerId })
  if (size(bodyRequiredFields)) checkRequiredFields(bodyRequiredFields, body)
}

export const isDateUpdated = (partnerSettings, previousDate, presentDate) => {
  previousDate = this.getActualDate(partnerSettings, false, previousDate)
  presentDate = this.getActualDate(partnerSettings, false, presentDate)

  return previousDate.getTime() !== presentDate.getTime()
}

export const isAnyTenantSigned = (
  preTenantSigningStatus = [],
  currentTenantSigningStatus = []
) => {
  console.log('=== Checking if any tenant signed ===')
  const signedTenantsArray = map(
    currentTenantSigningStatus,
    function (signingInfo) {
      return signingInfo.signed ? signingInfo : {}
    }
  )
  console.log('=== signedTenantsArray ===', signedTenantsArray)
  let isTenantSigned = false

  if (size(signedTenantsArray)) {
    each(signedTenantsArray, (signedTenantObj) => {
      const prevTenantLeaseInfo = signedTenantObj.idfySignerId
        ? find(preTenantSigningStatus, {
            idfySignerId: signedTenantObj.idfySignerId
          })
        : null // Get only tenant signing stauses if prev and doc idfySignerId is same
      console.log('=== prevTenantLeaseInfo ===', prevTenantLeaseInfo)
      if (size(prevTenantLeaseInfo) && !prevTenantLeaseInfo.signed)
        isTenantSigned = true
    })
  }
  console.log('=== isTenantSigned ===', isTenantSigned)
  return isTenantSigned
}

export const getPartnerLogo = (partnerInfo, size) => {
  const { _id, logo } = partnerInfo
  if (logo) {
    const { height = 215, width = 215 } = size || {}

    return (
      getCDNDomain() +
      '/partner_logo/' +
      _id +
      '/' +
      logo +
      '?w=' +
      width +
      '&h=' +
      height
    )
  } else return getDefaultLogoURL('organization')
}

export const getOrgLogo = (organizationInfo, size) => {
  const { image, partnerId } = organizationInfo
  if (image) {
    const { height = 215, width = 215 } = size || {}

    return (
      getCDNDomain() +
      '/partner_logo/' +
      partnerId +
      '/accounts/' +
      image +
      '?w=' +
      width +
      '&h=' +
      height
    )
  } else return getDefaultLogoURL('organization')
}

export const validateUserSSN = async (userId, session) => {
  checkUserId(userId)
  const { profile } =
    (await userHelper.getAnUser({ _id: userId }, session)) || {}
  return validateNorwegianIdNumber(profile?.norwegianNationalIdentification)
}

export const validateSelfServicePartnerRequestAndUpdateBody = async (
  params,
  session
) => {
  const {
    defaultRole,
    hasAccessForTenantRole = false,
    partnerId,
    userId
  } = params || {}

  const b2cUserData = {}
  if (defaultRole === 'landlord' || defaultRole === 'tenant') {
    // Find and validate b2c partner
    const partner = await partnerHelper.getAPartner({ _id: partnerId }, session)
    if (!partner?.isSelfService) {
      throw new CustomError(400, 'Invalid B2C Partner!')
    }

    // Adding partner information after validating b2c partner
    b2cUserData.partner = partner
    b2cUserData.partnerId = partnerId

    if (defaultRole === 'landlord') {
      const account = await accountHelper.getAnAccount(
        { partnerId, personId: userId },
        session
      )
      if (!account?._id) {
        throw new CustomError(400, 'Landlord not found!')
      }

      b2cUserData.accountId = account._id
      b2cUserData.agentId = account.agentId
      b2cUserData.branchId = account.branchId
    } else if (defaultRole === 'tenant') {
      if (hasAccessForTenantRole) {
        const tenant = await tenantHelper.getATenant(
          { partnerId, userId },
          session
        )
        if (!tenant?._id) {
          throw new CustomError(400, 'Tenant not found!')
        }

        b2cUserData.tenantId = tenant._id
      } else throw new CustomError(403, 'Access denied for tenant!')
    }
  }

  return b2cUserData
}
