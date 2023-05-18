import validator from 'validator'
import { indexOf, isBoolean, map, omit, pull, size, uniq, uniqBy } from 'lodash'

import { PartnerSettingCollection } from '../models'
import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  contractHelper,
  invoiceHelper,
  partnerHelper,
  partnerSettingHelper,
  userHelper
} from '../helpers'
import { fileService } from '../services'

export const getSettingByPartnerId = async (partnerId, session) => {
  const setting = await PartnerSettingCollection.findOne({ partnerId }).session(
    session
  )
  return setting
}

export const getAPartnerSetting = async (query, session, populate = []) => {
  const setting = await PartnerSettingCollection.findOne(query)
    .session(session)
    .populate(populate)
  return setting
}

export const getAllPartnerSettings = async (query, session) => {
  const partnerSettings = await PartnerSettingCollection.find(query).session(
    session
  )
  return partnerSettings
}

export const prepareNotificationsSettingUpdatingData = (params) => {
  const { name, value } = params
  let notificationsSettingUpdatingData = {}
  if (!name) throw new CustomError(400, 'Required name')
  const dataObj = {
    // AppHealth (Settings)
    appHealthNotification: { 'notifications.appHealthNotification': value },
    // Financial (Partner Settings)
    creditNote: { 'notifications.creditNote': value },
    finalSettlementInvoice: { 'notifications.finalSettlementInvoice': value },
    annualStatement: { 'notifications.annualStatement': value },
    depositAccount: { 'notifications.depositAccount': value },
    depositIncomingPayment: { 'notifications.depositIncomingPayment': value },
    depositInsurance: { 'notifications.depositInsurance': value },
    wrongSSNNotification: { 'notifications.wrongSSNNotification': value },
    // Lease Start (Partner Settings)
    sentWelcomeLease: { 'notifications.sentWelcomeLease': value },
    sentAssignment: { 'notifications.sentAssignment': value },
    nextScheduledPayouts: { 'notifications.nextScheduledPayouts': value },
    interestForm: { 'notifications.interestForm': value },
    // Lease End (Partner Settings)
    leaseTerminatedByLandlord: {
      'notifications.leaseTerminatedByLandlord': value
    },
    leaseTerminatedByTenant: { 'notifications.leaseTerminatedByTenant': value },
    leaseScheduleTerminatedByTenant: {
      'notifications.leaseScheduleTerminatedByTenant': value
    },
    leaseScheduleTerminatedByLandlord: {
      'notifications.leaseScheduleTerminatedByLandlord': value
    },
    soonEndingLease: { 'notifications.soonEndingLease': value },
    // Others (Partner Settings)
    taskNotification: { 'notifications.taskNotification': value }
  }
  if (dataObj[name]) notificationsSettingUpdatingData = dataObj[name]
  if (!size(notificationsSettingUpdatingData))
    throw new CustomError(400, 'Invalid name for notification change')
  return notificationsSettingUpdatingData
}

export const prepareQueryForDomainSetting = async (body) => {
  const { data } = body
  const { name, value } = data
  const query = { partnerId: { $exists: false } }
  let updatedData = {}
  if (name === 'allowedDomains') updatedData = { allowedDomains: value }
  else throw new CustomError(400, 'Invalid name')
  return { query, updatedData }
}

export const getDecimalSeparatorUpdatingData = (value, currencySettings) => {
  if (!value)
    throw new CustomError(400, 'value is required to update decimalSeparator')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  let decimalSeparatorUpdatingData = {}
  if (currencySettings.thousandSeparator === 'space')
    decimalSeparatorUpdatingData = {
      'currencySettings.decimalSeparator': value,
      'currencySettings.thousandSeparator': 'space'
    }
  else if (value === '.')
    decimalSeparatorUpdatingData = {
      'currencySettings.decimalSeparator': value,
      'currencySettings.thousandSeparator': ','
    }
  else if (value === ',')
    decimalSeparatorUpdatingData = {
      'currencySettings.decimalSeparator': value,
      'currencySettings.thousandSeparator': '.'
    }
  if (!size(decimalSeparatorUpdatingData))
    throw new CustomError(400, 'Invalid value to update decimalSeparator')
  return decimalSeparatorUpdatingData
}

export const getThousandSeparatorUpdatingData = (value, currencySettings) => {
  if (!value)
    throw new CustomError(400, 'value is required to update thousandSeparator')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  let thousandSeparatorUpdatingData = {}
  if (value === '.')
    thousandSeparatorUpdatingData = {
      'currencySettings.thousandSeparator': value,
      'currencySettings.decimalSeparator': ','
    }
  else if (value === ',')
    thousandSeparatorUpdatingData = {
      'currencySettings.thousandSeparator': value,
      'currencySettings.decimalSeparator': '.'
    }
  else if (value === 'space')
    thousandSeparatorUpdatingData = {
      'currencySettings.thousandSeparator': value,
      'currencySettings.decimalSeparator': currencySettings.decimalSeparator
    }
  if (!size(thousandSeparatorUpdatingData))
    throw new CustomError(400, 'Invalid value to update thousandSeparator')
  return thousandSeparatorUpdatingData
}

export const getNumberOfDecimalUpdatingData = (value) => {
  if (validator.isInt(`${value}`)) {
    if (value === 0 || value === 2) {
      return { 'currencySettings.numberOfDecimal': value }
    } else throw new CustomError(400, 'Value must be 0 or 2')
  }
  throw new CustomError(400, 'Value must be an integer')
}

export const getCurrencySymbolUpdatingData = (value) => {
  if (!value)
    throw new CustomError(400, 'value is required to update currencySymbol')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  return { 'currencySettings.currencySymbol': value }
}

export const getCurrencyPositionUpdatingData = (value) => {
  if (!value)
    throw new CustomError(400, 'value is required to update currencyPosition')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  if (
    value === 'left' ||
    value === 'right' ||
    value === 'left_with_space' ||
    value === 'right_with_space'
  ) {
    return { 'currencySettings.currencyPosition': value }
  }
  throw new CustomError(400, 'Invalid value to update currencyPosition')
}

export const getDateFormatUpdatingData = (value) => {
  if (!value)
    throw new CustomError(400, 'value is required to update dateFormat')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  if (value === 'YYYY.MM.DD' || value === 'DD.MM.YYYY') {
    return { 'dateTimeSettings.dateFormat': value }
  }
  throw new CustomError(400, 'Value must be YYYY.MM.DD or DD.MM.YYYY')
}

export const getTimeFormatUpdatingData = (value) => {
  if (!value)
    throw new CustomError(400, 'value is required to update timeFormat')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  if (value === '12_hours' || value === '24_hours') {
    return { 'dateTimeSettings.timeFormat': value }
  }
  throw new CustomError(400, 'Value must be 12_hours or 24_hours')
}

export const getTimezoneUpdatingData = (value) => {
  if (!value) throw new CustomError(400, 'value is required to update timezone')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  return { 'dateTimeSettings.timezone': value }
}

export const getEnabledGroupIdUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'propertySettings.enabledGroupId': value }
}

export const getSoonEndingMonthsUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value >= 1 && value <= 12) {
    return { 'propertySettings.soonEndingMonths': value }
  }
  throw new CustomError(400, 'Value must be in between 1 to 12')
}

export const getMovingInOutProtocolUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'propertySettings.movingInOutProtocol': value }
}

export const getEnabledMoveInEsignReminderUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'propertySettings.enabledMoveInEsignReminder': value }
}

export const getMoveInESigningReminderNoticeDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value >= 1 && value <= 45) {
    return { 'propertySettings.esignReminderNoticeDaysForMoveIn': value }
  }
  throw new CustomError(400, 'Value must be in between 1 to 45')
}

export const getEnabledMoveOutEsignReminderUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'propertySettings.enabledMoveOutEsignReminder': value }
}

export const getMoveOutESigningReminderNoticeDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value >= 1 && value <= 45) {
    return { 'propertySettings.esignReminderNoticeDaysForMoveOut': value }
  }
  throw new CustomError(400, 'Value must be in between 1 to 45')
}

export const getCommonTenantSettingUpdatingData = async (params) => {
  const { partnerSetting, partnerId, name, value } = params
  const { tenantSetting = {} } = partnerSetting
  const tenantSettingNames = [
    'enabledRemoveProspects',
    'enabledDeleteInterestForm',
    'enabledRemoveCreditRating',
    'removeProspectsMonths',
    'deleteInterestFormMonths',
    'removeCreditRatingMonths',
    'enabledAutomaticCreditRating'
  ]

  if (indexOf(tenantSettingNames, name) !== -1) {
    const isDirectPartner = partnerId
      ? !!(await partnerHelper.getAPartner({
          _id: partnerId,
          accountType: 'direct'
        }))
      : false
    let prospectsMonths =
      size(tenantSetting) &&
      tenantSetting.removeProspects &&
      tenantSetting.removeProspects.months
        ? tenantSetting.removeProspects.months
        : 1
    let interestFormMonths =
      size(tenantSetting) &&
      tenantSetting.deleteInterestForm &&
      tenantSetting.deleteInterestForm.months
        ? tenantSetting.deleteInterestForm.months
        : 1
    let creditRatingMonths =
      size(tenantSetting) &&
      tenantSetting.removeCreditRating &&
      tenantSetting.removeCreditRating.months
        ? tenantSetting.removeCreditRating.months
        : isDirectPartner
        ? 1
        : 120
    let enabledProspects =
      size(tenantSetting) &&
      tenantSetting.removeProspects &&
      tenantSetting.removeProspects.enabled
        ? tenantSetting.removeProspects.enabled
        : false
    let enabledInterestForm =
      size(tenantSetting) &&
      tenantSetting.deleteInterestForm &&
      tenantSetting.deleteInterestForm.enabled
        ? tenantSetting.deleteInterestForm.enabled
        : false
    let enabledCreditRating =
      size(tenantSetting) &&
      tenantSetting.removeCreditRating &&
      tenantSetting.removeCreditRating.enabled
        ? tenantSetting.removeCreditRating.enabled
        : false
    let enabledAutomaticCreditRating =
      size(tenantSetting) &&
      tenantSetting.automaticCreditRating &&
      tenantSetting.automaticCreditRating.enabled
        ? tenantSetting.automaticCreditRating.enabled
        : false

    if (name === 'deleteInterestFormMonths') {
      if (!validator.isInt(`${value}`))
        throw new CustomError(400, 'Value must be a number')
      if (value <= 0 || value > prospectsMonths)
        throw new CustomError(
          400,
          `Value must be in between 1 to ${prospectsMonths}`
        )
      interestFormMonths = value > 0 ? value : 1
      prospectsMonths =
        prospectsMonths >= interestFormMonths
          ? prospectsMonths
          : interestFormMonths
    }

    if (name === 'removeProspectsMonths') {
      if (!validator.isInt(`${value}`))
        throw new CustomError(400, 'Value must be a number')
      if (value <= 0) throw new CustomError(400, 'Value must be greater than 0')
      prospectsMonths = value > 0 ? value : 1
      prospectsMonths =
        prospectsMonths >= interestFormMonths
          ? prospectsMonths
          : interestFormMonths
    }

    if (name === 'removeCreditRatingMonths') {
      if (!validator.isInt(`${value}`))
        throw new CustomError(400, 'Value must be a number')
      creditRatingMonths = value > 0 ? value : 1
    }

    if (name === 'enabledRemoveProspects') {
      if (!validator.isBoolean(`${value}`))
        throw new CustomError(400, 'Value must be a boolean')
      enabledProspects = value
    }
    if (name === 'enabledDeleteInterestForm') {
      if (!validator.isBoolean(`${value}`))
        throw new CustomError(400, 'Value must be a boolean')
      enabledInterestForm = value
    }
    if (name === 'enabledRemoveCreditRating') {
      if (!validator.isBoolean(`${value}`))
        throw new CustomError(400, 'Value must be a boolean')
      enabledCreditRating = value
    }
    if (name === 'enabledAutomaticCreditRating') {
      if (!validator.isBoolean(`${value}`))
        throw new CustomError(400, 'Value must be a boolean')
      enabledAutomaticCreditRating = value
    }

    const enabledRemoveProspectsUpdatingData = {
      'tenantSetting.removeProspects': {
        enabled: enabledProspects,
        months: prospectsMonths
      },
      'tenantSetting.deleteInterestForm': {
        enabled: enabledInterestForm,
        months: interestFormMonths
      },
      'tenantSetting.removeCreditRating': {
        enabled: enabledCreditRating,
        months: creditRatingMonths
      },
      'tenantSetting.automaticCreditRating': {
        enabled: enabledAutomaticCreditRating
      }
    }
    return enabledRemoveProspectsUpdatingData
  }
  return {}
}

export const getNumberOfDecimalInInvoiceUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value === 0 || value === 2) {
    return { 'invoiceSettings.numberOfDecimalInInvoice': value }
  }
  throw new CustomError(400, 'Value must be 0 or 2')
}

export const getEnabledVippsRegningerUpdatingData = (partnerSetting, value) => {
  const { companyInfo = {} } = partnerSetting
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (!(size(companyInfo) && companyInfo.organizationId))
    throw new CustomError(405, 'Please add organizationId first')
  return { 'invoiceSettings.enabledVippsRegninger': value }
}

export const getInternalAssignmentIdUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'assignmentSettings.internalAssignmentId': value }
}

export const getEnableEsignAssignmentUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'assignmentSettings.enableEsignAssignment': value }
}

export const getEnabledAssignmentEsignReminderUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'assignmentSettings.enabledAssignmentEsignReminder': value }
}

export const getAssignmentEsignReminderNoticeDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value >= 1 && value <= 45) {
    return { 'assignmentSettings.esignReminderNoticeDays': value }
  }
  throw new CustomError(400, 'Value must be in between 1 to 45')
}

export const getEnabledShowAssignmentFilesToLandlordUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'assignmentSettings.enabledShowAssignmentFilesToLandlord': value }
}

export const getInternalLeaseIdUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'leaseSetting.internalLeaseId': value }
}

export const getEnableEsignLeaseUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'leaseSetting.enableEsignLease': value }
}

export const getEnabledLeaseESigningReminderUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'leaseSetting.enabledLeaseESigningReminder': value }
}

export const getLeaseESigningReminderNoticeDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value >= 1 && value <= 45) {
    return { 'leaseSetting.esignReminderNoticeDays': value }
  }
  throw new CustomError(400, 'Value must be in between 1 to 45')
}

export const getNaturalLeaseTerminationUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'leaseSetting.naturalLeaseTermination.enabled': value }
}

export const getNaturalLeaseTerminationSentDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'leaseSetting.naturalLeaseTermination.days': value }
}

export const getEnabledShowLeaseFilesToTenantUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'leaseSetting.enabledShowLeaseFilesToTenant': value }
}

export const getEnabledDeactivateListingUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'listingSetting.disabledListing.enabled': value }
}

export const getDeactivateListingDayUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'listingSetting.disabledListing.days': value }
}

export const getDefaultMapLocationUpdatingData = (value) => {
  if (!value)
    throw new CustomError(400, 'value is required to update defaultMapLocation')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  return { 'defaultFindHomeLocation.defaultMapLocation': value }
}

export const getDefaultMapZoomUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'defaultFindHomeLocation.defaultMapZoom': value }
}

export const getDepositTypeUpdatingData = (value) => {
  if (!['no_deposit', 'deposit_insurance', 'deposit_account'].includes(value))
    throw new CustomError(400, 'Please provide valid deposit type')
  return { 'leaseSetting.depositType': value }
}

export const getDIPaymentReminderUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'depositInsuranceSetting.paymentReminder.enabled': value }
}

export const getDIPaymentReminderDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'depositInsuranceSetting.paymentReminder.days': value }
}

export const getEnabledCompelloRegningerUpdatingData = (
  partnerSetting = {},
  value
) => {
  const { companyInfo = {} } = partnerSetting
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (!(size(companyInfo) && companyInfo.organizationId))
    throw new CustomError(405, 'Please add organizationId first')
  return { 'invoiceSettings.enabledCompelloRegninger': value }
}

export const prepareGeneralSettingUpdatingData = async (params) => {
  const { partnerSetting, partnerId, data } = params
  const { name, valueString, valueBoolean, valueInt } = data
  let updatingData = {}
  if (!name) throw new CustomError(400, 'Required name')
  const { currencySettings = {} } = partnerSetting
  const tenantSettingUpdatingData = {
    partnerSetting,
    partnerId,
    name
  }
  // Currency
  if (name === 'decimalSeparator')
    updatingData = getDecimalSeparatorUpdatingData(
      valueString,
      currencySettings
    )
  else if (name === 'thousandSeparator')
    updatingData = getThousandSeparatorUpdatingData(
      valueString,
      currencySettings
    )
  else if (name === 'numberOfDecimal')
    updatingData = getNumberOfDecimalUpdatingData(valueInt)
  else if (name === 'currencySymbol')
    updatingData = getCurrencySymbolUpdatingData(valueString)
  else if (name === 'currencyPosition')
    updatingData = getCurrencyPositionUpdatingData(valueString)
  // Time & Date
  else if (name === 'dateFormat')
    updatingData = getDateFormatUpdatingData(valueString)
  else if (name === 'timeFormat')
    updatingData = getTimeFormatUpdatingData(valueString)
  else if (name === 'timezone')
    updatingData = getTimezoneUpdatingData(valueString)
  // Properties
  else if (name === 'enabledGroupId')
    updatingData = getEnabledGroupIdUpdatingData(valueBoolean)
  else if (name === 'soonEndingMonths')
    updatingData = getSoonEndingMonthsUpdatingData(valueInt)
  else if (name === 'movingInOutProtocol')
    updatingData = getMovingInOutProtocolUpdatingData(valueBoolean)
  else if (name === 'enabledMoveInEsignReminder')
    updatingData = getEnabledMoveInEsignReminderUpdatingData(valueBoolean)
  else if (name === 'moveInESigningReminderNoticeDays')
    updatingData = getMoveInESigningReminderNoticeDaysUpdatingData(valueInt)
  else if (name === 'enabledMoveOutEsignReminder')
    updatingData = getEnabledMoveOutEsignReminderUpdatingData(valueBoolean)
  else if (name === 'moveOutESigningReminderNoticeDays')
    updatingData = getMoveOutESigningReminderNoticeDaysUpdatingData(valueInt)
  // Tenant
  else if (
    name === 'removeProspectsMonths' ||
    name === 'deleteInterestFormMonths' ||
    name === 'removeCreditRatingMonths'
  ) {
    tenantSettingUpdatingData.value = valueInt
    updatingData = await getCommonTenantSettingUpdatingData(
      tenantSettingUpdatingData
    )
  } else if (
    name === 'enabledRemoveProspects' ||
    name === 'enabledDeleteInterestForm' ||
    name === 'enabledAutomaticCreditRating' ||
    name === 'enabledRemoveCreditRating'
  ) {
    tenantSettingUpdatingData.value = valueBoolean
    updatingData = await getCommonTenantSettingUpdatingData(
      tenantSettingUpdatingData
    )
  }
  //Invoice
  else if (name === 'enabledVippsRegninger')
    updatingData = getEnabledVippsRegningerUpdatingData(
      partnerSetting,
      valueBoolean
    )
  else if (name === 'enabledCompelloRegninger')
    updatingData = getEnabledCompelloRegningerUpdatingData(
      partnerSetting,
      valueBoolean
    )
  else if (name === 'numberOfDecimalInInvoice')
    updatingData = getNumberOfDecimalInInvoiceUpdatingData(valueInt)
  // Assignment
  else if (name === 'internalAssignmentId')
    updatingData = getInternalAssignmentIdUpdatingData(valueBoolean)
  else if (name === 'enableEsignAssignment')
    updatingData = getEnableEsignAssignmentUpdatingData(valueBoolean)
  else if (name === 'enabledAssignmentEsignReminder')
    updatingData = getEnabledAssignmentEsignReminderUpdatingData(valueBoolean)
  else if (name === 'assignmentEsignReminderNoticeDays')
    updatingData = getAssignmentEsignReminderNoticeDaysUpdatingData(valueInt)
  else if (name === 'enabledShowAssignmentFilesToLandlord') {
    updatingData =
      getEnabledShowAssignmentFilesToLandlordUpdatingData(valueBoolean)
    await fileService.updateMultipleFiles(
      {
        partnerId,
        $or: [
          { context: 'contract', assignmentSerial: { $exists: true } },
          { context: 'correction', type: 'correction_invoice_pdf' }
        ]
      },
      { $set: { isVisibleToLandlord: valueBoolean } },
      null
    )
  }
  // Lease
  else if (name === 'internalLeaseId')
    updatingData = getInternalLeaseIdUpdatingData(valueBoolean)
  else if (name === 'enableEsignLease')
    updatingData = getEnableEsignLeaseUpdatingData(valueBoolean)
  else if (name === 'enabledLeaseESigningReminder')
    updatingData = getEnabledLeaseESigningReminderUpdatingData(valueBoolean)
  else if (name === 'leaseESigningReminderNoticeDays')
    updatingData = getLeaseESigningReminderNoticeDaysUpdatingData(valueInt)
  else if (name === 'naturalLeaseTermination')
    updatingData = getNaturalLeaseTerminationUpdatingData(valueBoolean)
  else if (name === 'naturalLeaseTerminationSentDays')
    updatingData = getNaturalLeaseTerminationSentDaysUpdatingData(valueInt)
  else if (name === 'enabledShowLeaseFilesToTenant') {
    updatingData = getEnabledShowLeaseFilesToTenantUpdatingData(valueBoolean)
    await fileService.updateMultipleFiles(
      {
        partnerId,
        $or: [
          { context: 'contract', leaseSerial: { $exists: true } },
          { context: 'correction', type: 'correction_invoice_pdf' },
          { context: 'lease', type: 'lease_pdf' }
        ]
      },
      { $set: { isVisibleToTenant: valueBoolean } },
      null
    )
  }
  // Listings
  else if (name === 'enabledDeactivateListing')
    updatingData = getEnabledDeactivateListingUpdatingData(valueBoolean)
  else if (name === 'deactivateListingDay')
    updatingData = getDeactivateListingDayUpdatingData(valueInt)
  else if (name === 'defaultMapLocation')
    updatingData = getDefaultMapLocationUpdatingData(valueString)
  else if (name === 'defaultMapZoom')
    updatingData = getDefaultMapZoomUpdatingData(valueInt)
  else if (name === 'depositType') {
    updatingData = getDepositTypeUpdatingData(valueString)
  } else if (name === 'depositInsurancePaymentReminder') {
    updatingData = getDIPaymentReminderUpdatingData(valueBoolean)
  } else if (name === 'depositInsurancePaymentReminderDays') {
    updatingData = getDIPaymentReminderDaysUpdatingData(valueInt)
  }
  return { updatingData }
}

export const preparePartnerSettingsQueryBasedOnFilters = (query) => {
  const { appAdmin } = query
  if (appAdmin) query.partnerId = { $exists: false }
  const partnerSettingsQuery = omit(query, ['appAdmin'])
  return partnerSettingsQuery
}

export const getPartnerSettingForPartnerApp = async (partnerId) => {
  const partnerSetting = await PartnerSettingCollection.aggregate([
    {
      $match: { partnerId }
    },
    {
      $lookup: {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partner',
        pipeline: [
          {
            $addFields: {
              logoUrl: {
                $cond: [
                  { $ifNull: ['$logo', false] },
                  {
                    $concat: [
                      appHelper.getCDNDomain(),
                      '/partner_logo/',
                      '$_id',
                      '/',
                      '$logo'
                    ]
                  },
                  appHelper.getDefaultLogoURL('organization')
                ]
              },
              siteLogoUrl: {
                $cond: [
                  { $ifNull: ['$siteLogo', false] },
                  {
                    $concat: [
                      appHelper.getCDNDomain(),
                      '/partner_logo/',
                      '$_id',
                      '/',
                      '$siteLogo'
                    ]
                  },
                  null
                  // appHelper.getDefaultLogoURL('organization')
                ]
              }
            }
          }
        ]
      }
    },
    appHelper.getUnwindPipeline('partner')
  ])
  return partnerSetting
}

export const getPartnerSettingsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const partnerSettings = await PartnerSettingCollection.aggregate([
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
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partner',
        pipeline: [
          {
            $addFields: {
              logoUrl: {
                $cond: [
                  { $ifNull: ['$logo', false] },
                  {
                    $concat: [
                      appHelper.getCDNDomain(),
                      '/partner_logo/',
                      '$_id',
                      '/',
                      '$logo'
                    ]
                  },
                  appHelper.getDefaultLogoURL('organization')
                ]
              },
              siteLogoUrl: {
                $cond: [
                  { $ifNull: ['$siteLogo', false] },
                  {
                    $concat: [
                      appHelper.getCDNDomain(),
                      '/partner_logo/',
                      '$_id',
                      '/',
                      '$siteLogo'
                    ]
                  },
                  null
                  // appHelper.getDefaultLogoURL('organization')
                ]
              }
            }
          }
        ]
      }
    },
    appHelper.getUnwindPipeline('partner'),
    {
      $lookup: {
        from: 'users',
        localField: 'directRemittanceApproval.persons',
        foreignField: '_id',
        as: 'directRemittanceApprovalUsers'
      }
    }
  ])
  return partnerSettings
}

export const countPartnerSettings = async (query, session) => {
  const numberOfPartnerSettings = await PartnerSettingCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfPartnerSettings
}

export const queryPartnerSettings = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { partnerId } = user
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  if (partnerId) {
    query.partnerId = partnerId
    delete query.appAdmin
  }
  body.query = preparePartnerSettingsQueryBasedOnFilters(query)
  const partnerSettingsData = await getPartnerSettingsForQuery(body)
  const filteredDocuments = await countPartnerSettings(body.query)
  const totalDocuments = await countPartnerSettings({})
  return {
    data: partnerSettingsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryPartnerSetting = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body = {} } = req
  const partnerSettingData = await getPartnerSettingForPartnerApp(
    body.partnerId
  )
  return partnerSettingData
}

export const getPostalFeeEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'postalFee.enabled': value }
}

export const getPostalFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'postalFee.amount': value }
}

export const getPostalTaxUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'postalFee.tax': value }
}

export const getInvoiceFeeEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'invoiceFee.enabled': value }
}

export const getInvoiceFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'invoiceFee.amount': value }
}

export const getReminderFeeEnabledUpdatingData = (
  value,
  invoiceFirstReminder,
  invoiceSecondReminder
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (value) {
    const firstReminderDays =
      size(invoiceFirstReminder) &&
      invoiceFirstReminder.enabled &&
      invoiceFirstReminder.days
        ? invoiceFirstReminder.days
        : 0

    const secondReminderDays =
      size(invoiceSecondReminder) &&
      invoiceSecondReminder.enabled &&
      invoiceSecondReminder.days
        ? invoiceSecondReminder.days
        : 0

    const totalReminderDays = firstReminderDays + secondReminderDays

    if (totalReminderDays) {
      if (firstReminderDays >= 14 || totalReminderDays >= 14) {
        return { 'reminderFee.enabled': value }
      }
      throw new CustomError(
        405,
        'InvoiceFirstReminder days  or total reminder days must be greater than 13'
      )
    }
    throw new CustomError(405, 'InvoiceFirstReminder must be enabled')
  } else return { 'reminderFee.enabled': false }
}

export const getReminderFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'reminderFee.amount': value }
}

export const getCollectionNoticeFeeEnabledUpdatingData = (
  value,
  invoiceCollectionNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (value) {
    if (size(invoiceCollectionNotice) && invoiceCollectionNotice.enabled) {
      if (invoiceCollectionNotice.days >= 14) {
        return { 'collectionNoticeFee.enabled': value }
      }
      throw new CustomError(
        405,
        'InvoiceCollectionNotice days must be greater than 13'
      )
    }
    throw new CustomError(405, 'InvoiceCollectionNotice must be enabled')
  } else return { 'collectionNoticeFee.enabled': false }
}

export const getCollectionNoticeFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'collectionNoticeFee.amount': value }
}

export const getEvictionFeeEnabledUpdatingData = (
  value,
  evictionDueReminderNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (value) {
    if (size(evictionDueReminderNotice) && evictionDueReminderNotice.enabled) {
      if (evictionDueReminderNotice.days >= 14) {
        return { 'evictionFee.enabled': value }
      }
      throw new CustomError(
        405,
        'EvictionDueReminderNotice days must be greater than 13'
      )
    }
    throw new CustomError(405, 'EvictionDueReminderNotice must be enabled')
  } else return { 'evictionFee.enabled': false }
}

export const getEvictionFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'evictionFee.amount': value }
}

export const getAdministrationEvictionFeeEnabledUpdatingData = (
  value,
  evictionDueReminderNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (value) {
    if (size(evictionDueReminderNotice) && evictionDueReminderNotice.enabled) {
      if (evictionDueReminderNotice.days >= 14) {
        return { 'administrationEvictionFee.enabled': value }
      }
      throw new CustomError(
        405,
        'EvictionDueReminderNotice days must be greater than 13'
      )
    }
    throw new CustomError(405, 'EvictionDueReminderNotice must be enabled')
  }
  return { 'administrationEvictionFee.enabled': value }
}

export const getAdministrationEvictionFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'administrationEvictionFee.amount': value }
}

export const getAdministrationEvictionTaxUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'administrationEvictionFee.tax': value }
}

export const getSameKIDNumberUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { sameKIDNumber: value }
}

export const getStopCPIRegulationUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { stopCPIRegulation: value }
}

export const getInvoiceCalculationUpdatingData = (value) => {
  if (!value)
    throw new CustomError(400, 'value is required to update invoiceCalculation')
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  if (value === 'prorated_first_month' || value === 'prorated_second_month')
    return { invoiceCalculation: value }
  throw new CustomError(
    400,
    'Value must be prorated_first_month or prorated_second_month'
  )
}

export const getInvoiceDueDayUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { invoiceDueDays: value }
}

export const getCPISettlementUpdatingData = async (value, partnerId) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const contractsQuery = { partnerId, 'rentalMeta.cpiEnabled': true }
  const contracts = partnerId
    ? await contractHelper.countContracts(contractsQuery)
    : 0
  if (!value && contracts)
    throw new CustomError(405, "CPISettlement setting can't be disabled")
  else return { 'CPISettlement.enabled': value }
}

export const getCPISettlementMonthsUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value >= 0 && value <= 12) return { 'CPISettlement.months': value }
  throw new CustomError(400, 'Value must be in between 0 to 12')
}

export const getRentDueUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'duePreReminder.enabled': value }
}

export const getRentDueDayUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'duePreReminder.days': value }
}

export const getFirstReminderEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const updatingData = { 'invoiceFirstReminder.enabled': value }
  if (!value) {
    updatingData['invoiceSecondReminder.enabled'] = false
    updatingData['reminderFee.enabled'] = false
  }
  return updatingData
}

export const getInvoiceFirstReminderUpdatingData = (
  value,
  invoiceSecondReminder
) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const secondReminderDays =
    size(invoiceSecondReminder) &&
    invoiceSecondReminder.enabled &&
    invoiceSecondReminder.days
      ? invoiceSecondReminder.days
      : 0
  const updatingData = { 'invoiceFirstReminder.days': value }
  if (value + secondReminderDays < 14) {
    updatingData['reminderFee.enabled'] = false
  }
  return updatingData
}

export const getSecondReminderEnabledUpdatingData = (
  value,
  invoiceFirstReminder
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const isFirstReminderEnabled =
    size(invoiceFirstReminder) && invoiceFirstReminder.enabled
      ? invoiceFirstReminder.enabled
      : false
  const firstReminderDays =
    isFirstReminderEnabled && invoiceFirstReminder.days
      ? invoiceFirstReminder.days
      : 0
  if (value) {
    if (isFirstReminderEnabled)
      return { 'invoiceSecondReminder.enabled': value }
    else throw new CustomError(405, 'RentInvoiceFirstReminder must be enabled')
  } else {
    const updatingData = { 'invoiceSecondReminder.enabled': false }
    if (firstReminderDays < 14) {
      updatingData['reminderFee.enabled'] = false
    }
    return updatingData
  }
}

export const getInvoiceSecondReminderUpdatingData = (
  value,
  invoiceFirstReminder
) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const firstReminderDays =
    size(invoiceFirstReminder) &&
    invoiceFirstReminder.enabled &&
    invoiceFirstReminder.days
      ? invoiceFirstReminder.days
      : 0
  const updatingData = { 'invoiceSecondReminder.days': value }
  if (value + firstReminderDays < 14) {
    updatingData['reminderFee.enabled'] = false
  }
  return updatingData
}

export const getInvoiceCollectionNoticeEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const updatingData = { 'invoiceCollectionNotice.enabled': value }
  if (!value) updatingData['collectionNoticeFee.enabled'] = false
  return updatingData
}

export const getInvoiceCollectionNoticeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const updatingData = { 'invoiceCollectionNotice.days': value }
  if (value < 14) updatingData['collectionNoticeFee.enabled'] = false
  return updatingData
}

export const getInvoiceCollectionNoticeNewDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'invoiceCollectionNotice.newDueDays': value }
}

export const getEvictionNoticeEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const updatingData = { 'evictionNotice.enabled': value }
  if (!value) {
    updatingData['evictionDueReminderNotice.enabled'] = false
    updatingData['evictionDueReminderNotice.isCreateEvictionPackage'] = false
    updatingData['evictionFee.enabled'] = false
    updatingData['administrationEvictionFee.enabled'] = false
    updatingData['tenantPaysAllDueDuringEviction.enabled'] = false
  }
  return updatingData
}

export const getEvictionNoticeDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'evictionNotice.days': value }
}

export const getEvictionNoticeRequiredTotalOverDue = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'evictionNotice.requiredTotalOverDue': value }
}

export const getEvictionDueReminderNoticeEnabledUpdatingData = (
  value,
  evictionNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const isEvictionNoticeEnabled =
    size(evictionNotice) && evictionNotice.enabled
      ? evictionNotice.enabled
      : false
  if (value) {
    if (isEvictionNoticeEnabled)
      return { 'evictionDueReminderNotice.enabled': value }
    else throw new CustomError(405, 'EvictionNotice must be enabled')
  } else {
    const updatingData = {
      'evictionDueReminderNotice.enabled': false,
      'evictionDueReminderNotice.isCreateEvictionPackage': false,
      'evictionFee.enabled': false,
      'administrationEvictionFee.enabled': false,
      'tenantPaysAllDueDuringEviction.enabled': false
    }
    return updatingData
  }
}

export const getEvictionDueReminderNoticeDaysUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const updatingData = { 'evictionDueReminderNotice.days': value }
  if (value < 14) {
    updatingData['evictionFee.enabled'] = false
    updatingData['administrationEvictionFee.enabled'] = false
  }
  return updatingData
}

export const getEvictionPackageUpdatingData = (
  value,
  evictionDueReminderNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const isEvictionDueReminderNoticeEnabled =
    size(evictionDueReminderNotice) && evictionDueReminderNotice.enabled
      ? evictionDueReminderNotice.enabled
      : false
  if (value) {
    if (isEvictionDueReminderNoticeEnabled)
      return { 'evictionDueReminderNotice.isCreateEvictionPackage': value }
    else throw new CustomError(405, 'EvictionDueReminderNotice must be enabled')
  } else return { 'evictionDueReminderNotice.isCreateEvictionPackage': false }
}

export const getEnabledTenantPaysAllDueDuringEvictionUpdatingData = (
  value,
  evictionDueReminderNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const isEvictionDueReminderNoticeEnabled =
    size(evictionDueReminderNotice) && evictionDueReminderNotice.enabled
      ? evictionDueReminderNotice.enabled
      : false
  if (value) {
    if (isEvictionDueReminderNoticeEnabled)
      return { 'tenantPaysAllDueDuringEviction.enabled': value }
    else throw new CustomError(405, 'EvictionDueReminderNotice must be enabled')
  } else return { 'tenantPaysAllDueDuringEviction.enabled': value }
}

export const prepareRentInvoiceUpdatingData = async (params) => {
  const { partnerSetting, partnerId, data } = params
  const { name, valueString, valueBoolean, valueInt } = data
  let updatingData = {}
  if (!name) throw new CustomError(400, 'Required name')
  const {
    invoiceFirstReminder = {},
    invoiceSecondReminder = {},
    invoiceCollectionNotice = {},
    evictionDueReminderNotice = {},
    evictionNotice = {}
  } = partnerSetting
  // Fees
  if (name === 'postalFeeEnabled')
    updatingData = getPostalFeeEnabledUpdatingData(valueBoolean)
  else if (name === 'postalFee')
    updatingData = getPostalFeeUpdatingData(valueInt)
  else if (name === 'postalTax')
    updatingData = getPostalTaxUpdatingData(valueInt)
  else if (name === 'invoiceFeeEnabled')
    updatingData = getInvoiceFeeEnabledUpdatingData(valueBoolean)
  else if (name === 'invoiceFee')
    updatingData = getInvoiceFeeUpdatingData(valueInt)
  else if (name === 'reminderFeeEnabled')
    updatingData = getReminderFeeEnabledUpdatingData(
      valueBoolean,
      invoiceFirstReminder,
      invoiceSecondReminder
    )
  else if (name === 'reminderFee')
    updatingData = getReminderFeeUpdatingData(valueInt)
  else if (name === 'collectionNoticeFeeEnabled')
    updatingData = getCollectionNoticeFeeEnabledUpdatingData(
      valueBoolean,
      invoiceCollectionNotice
    )
  else if (name === 'collectionNoticeFee')
    updatingData = getCollectionNoticeFeeUpdatingData(valueInt)
  else if (name === 'evictionFeeEnabled')
    updatingData = getEvictionFeeEnabledUpdatingData(
      valueBoolean,
      evictionDueReminderNotice
    )
  else if (name === 'evictionFee')
    updatingData = getEvictionFeeUpdatingData(valueInt)
  else if (name === 'administrationEvictionFeeEnabled')
    updatingData = getAdministrationEvictionFeeEnabledUpdatingData(
      valueBoolean,
      evictionDueReminderNotice
    )
  else if (name === 'administrationEvictionFee')
    updatingData = getAdministrationEvictionFeeUpdatingData(valueInt)
  else if (name === 'administrationEvictionTax')
    updatingData = getAdministrationEvictionTaxUpdatingData(valueInt)
  // Invoicing
  else if (name === 'sameKIDNumber')
    updatingData = getSameKIDNumberUpdatingData(valueBoolean)
  else if (name === 'stopCPIRegulation')
    updatingData = getStopCPIRegulationUpdatingData(valueBoolean)
  else if (name === 'invoiceCalculation')
    updatingData = getInvoiceCalculationUpdatingData(valueString)
  else if (name === 'invoiceDueDay')
    updatingData = getInvoiceDueDayUpdatingData(valueInt)
  else if (name === 'cpiSettlement')
    updatingData = await getCPISettlementUpdatingData(valueBoolean, partnerId)
  else if (name === 'cpiSettlementMonths')
    updatingData = getCPISettlementMonthsUpdatingData(valueInt)
  else if (name === 'rentDue')
    updatingData = getRentDueUpdatingData(valueBoolean)
  else if (name === 'rentDueDay')
    updatingData = getRentDueDayUpdatingData(valueInt)
  // Reminders
  else if (name === 'firstReminderEnabled')
    updatingData = getFirstReminderEnabledUpdatingData(valueBoolean)
  else if (name === 'invoiceFirstReminder')
    updatingData = getInvoiceFirstReminderUpdatingData(
      valueInt,
      invoiceSecondReminder
    )
  else if (name === 'secondReminderEnabled')
    updatingData = getSecondReminderEnabledUpdatingData(
      valueBoolean,
      invoiceFirstReminder
    )
  else if (name === 'invoiceSecondReminder')
    updatingData = getInvoiceSecondReminderUpdatingData(
      valueInt,
      invoiceFirstReminder
    )
  // Collection notice
  else if (name === 'invoiceCollectionNoticeEnabled')
    updatingData = getInvoiceCollectionNoticeEnabledUpdatingData(valueBoolean)
  else if (name === 'invoiceCollectionNotice')
    updatingData = getInvoiceCollectionNoticeUpdatingData(valueInt)
  else if (name === 'invoiceCollectionNoticeNewDays')
    updatingData = getInvoiceCollectionNoticeNewDaysUpdatingData(valueInt)
  else if (name === 'evictionNoticeEnabled')
    updatingData = getEvictionNoticeEnabledUpdatingData(valueBoolean)
  else if (name === 'evictionNoticeDays')
    updatingData = getEvictionNoticeDaysUpdatingData(valueInt)
  else if (name === 'evictionDueReminderNoticeEnabled')
    updatingData = getEvictionDueReminderNoticeEnabledUpdatingData(
      valueBoolean,
      evictionNotice
    )
  else if (name === 'evictionNoticeRequiredTotalOverDue')
    updatingData = getEvictionNoticeRequiredTotalOverDue(valueInt)
  else if (name === 'evictionDueReminderNoticeDays')
    updatingData = getEvictionDueReminderNoticeDaysUpdatingData(valueInt)
  else if (name === 'isCreateEvictionPackage')
    updatingData = getEvictionPackageUpdatingData(
      valueBoolean,
      evictionDueReminderNotice
    )
  else if (name === 'enabledTenantPaysAllDueDuringEviction')
    updatingData = getEnabledTenantPaysAllDueDuringEvictionUpdatingData(
      valueBoolean,
      evictionDueReminderNotice
    )
  // Payment types
  else if (name === 'firstMonthACNo' || name === 'afterFirstMonthACNo') {
    updatingData = await getPaymentTypeUpdatingData(
      name,
      valueString,
      partnerId
    )
  }
  return { updatingData }
}

const getPaymentTypeUpdatingData = async (name, value, partnerId) => {
  if (!value)
    throw new CustomError(400, 'Value is required to update payment types info')
  if (!partnerId) {
    throw new CustomError(
      400,
      'PartnerId is required to update payment types info'
    )
  }
  const query = {
    partnerId,
    'bankAccounts.accountNumber': value
  }
  const partnerSetting = await getAPartnerSetting(query)
  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Account number not found')
  }
  return {
    ['bankPayment.' + name]: value
  }
}

export const getLandlordPostalFeeEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'landlordPostalFee.enabled': value }
}

export const getLandlordPostalFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'landlordPostalFee.amount': value }
}

export const getLandlordPostalTaxUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'landlordPostalFee.tax': value }
}

export const getLandlordReminderFeeEnabledUpdatingData = (
  value,
  landlordInvoiceFirstReminder,
  landlordInvoiceSecondReminder
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (value) {
    const firstReminderDays =
      size(landlordInvoiceFirstReminder) &&
      landlordInvoiceFirstReminder.enabled &&
      landlordInvoiceFirstReminder.days
        ? landlordInvoiceFirstReminder.days
        : 0

    const secondReminderDays =
      size(landlordInvoiceSecondReminder) &&
      landlordInvoiceSecondReminder.enabled &&
      landlordInvoiceSecondReminder.days
        ? landlordInvoiceSecondReminder.days
        : 0

    const totalReminderDays = firstReminderDays + secondReminderDays

    if (totalReminderDays) {
      if (firstReminderDays >= 14 || totalReminderDays >= 14) {
        return { 'landlordReminderFee.enabled': value }
      }
      throw new CustomError(
        405,
        'LandlordInvoiceFirstReminder days or sum of LandlordInvoiceFirstReminder and LandlordInvoiceSecondReminder days must be greater than 13'
      )
    }
    throw new CustomError(405, 'LandlordInvoiceFirstReminder must be enabled')
  } else return { 'landlordReminderFee.enabled': false }
}

export const getLandlordReminderFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'landlordReminderFee.amount': value }
}

export const getLandlordCollectionNoticeFeeEnabledUpdatingData = (
  value,
  landlordInvoiceCollectionNotice
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  if (value) {
    if (
      size(landlordInvoiceCollectionNotice) &&
      landlordInvoiceCollectionNotice.enabled
    ) {
      if (landlordInvoiceCollectionNotice.days >= 14) {
        return { 'landlordCollectionNoticeFee.enabled': value }
      }
      throw new CustomError(
        405,
        'LandlordInvoiceCollectionNotice days must be greater than 13'
      )
    }
    throw new CustomError(
      405,
      'LandlordInvoiceCollectionNotice must be enabled'
    )
  } else return { 'landlordCollectionNoticeFee.enabled': false }
}

export const getLandlordCollectionNoticeFeeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'landlordCollectionNoticeFee.amount': value }
}

export const getLandlordInvoiceDueDayUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { landlordInvoiceDueDays: value }
}

export const getLandlordRentDueUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'landlordDuePreReminder.enabled': value }
}

export const getLandlordRentDueDayUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'landlordDuePreReminder.days': value }
}

export const getLandlordFirstReminderEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const updatingData = { 'landlordInvoiceFirstReminder.enabled': value }
  if (!value) {
    updatingData['landlordInvoiceSecondReminder.enabled'] = false
    updatingData['landlordReminderFee.enabled'] = false
  }
  return updatingData
}

export const getLandlordInvoiceFirstReminderUpdatingData = (
  value,
  landlordInvoiceSecondReminder
) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const landlordSecondReminderDays =
    size(landlordInvoiceSecondReminder) &&
    landlordInvoiceSecondReminder.enabled &&
    landlordInvoiceSecondReminder.days
      ? landlordInvoiceSecondReminder.days
      : 0
  const updatingData = { 'landlordInvoiceFirstReminder.days': value }
  if (value + landlordSecondReminderDays < 14) {
    updatingData['landlordReminderFee.enabled'] = false
  }
  return updatingData
}

export const getLandlordSecondReminderEnabledUpdatingData = (
  value,
  landlordInvoiceFirstReminder
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const isLandlordInvoiceFirstReminderEnabled =
    size(landlordInvoiceFirstReminder) && landlordInvoiceFirstReminder.enabled
      ? landlordInvoiceFirstReminder.enabled
      : false
  const landlordFirstReminderDays =
    isLandlordInvoiceFirstReminderEnabled && landlordInvoiceFirstReminder.days
      ? landlordInvoiceFirstReminder.days
      : 0
  if (value) {
    if (isLandlordInvoiceFirstReminderEnabled)
      return { 'landlordInvoiceSecondReminder.enabled': value }
    else
      throw new CustomError(405, 'LandlordInvoiceFirstReminder must be enabled')
  } else {
    const updatingData = { 'landlordInvoiceSecondReminder.enabled': false }
    if (landlordFirstReminderDays < 14)
      updatingData['landlordReminderFee.enabled'] = false
    return updatingData
  }
}

export const getLandlordInvoiceSecondReminderUpdatingData = (
  value,
  landlordInvoiceFirstReminder
) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const landlordFirstReminderDays =
    size(landlordInvoiceFirstReminder) &&
    landlordInvoiceFirstReminder.enabled &&
    landlordInvoiceFirstReminder.days
      ? landlordInvoiceFirstReminder.days
      : 0
  const updatingData = { 'landlordInvoiceSecondReminder.days': value }
  if (value + landlordFirstReminderDays < 14) {
    updatingData['landlordReminderFee.enabled'] = false
  }
  return updatingData
}

export const getLandlordInvoiceCollectionNoticeEnabledUpdatingData = (
  value
) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  const updatingData = { 'landlordInvoiceCollectionNotice.enabled': value }
  if (!value) updatingData['landlordCollectionNoticeFee.enabled'] = false
  return updatingData
}

export const getLandlordInvoiceCollectionNoticeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  const updatingData = { 'landlordInvoiceCollectionNotice.days': value }
  if (value < 14) updatingData['landlordCollectionNoticeFee.enabled'] = false
  return updatingData
}

export const getLandlordInvoiceCollectionNoticeNewDaysUpdatingData = (
  value
) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'landlordInvoiceCollectionNotice.newDueDays': value }
}

export const getLandlordPaymentAccountNumberUpdatingData = (value) => {
  if (!(value.length === 11 && validator.isInt(value)))
    throw new CustomError(400, 'Value must be 11 digit number')
  return { 'landlordBankPayment.firstMonthACNo': value }
}

export const prepareLandlordInvoiceUpdatingData = (params) => {
  const { partnerSetting, data } = params
  const { name, valueBoolean, valueInt, valueString } = data
  let updatingData = {}
  if (!name) throw new CustomError(400, 'Required name')
  const {
    landlordInvoiceFirstReminder = {},
    landlordInvoiceSecondReminder = {},
    landlordInvoiceCollectionNotice = {}
  } = partnerSetting
  // Fees
  if (name === 'landlordPostalFeeEnabled')
    updatingData = getLandlordPostalFeeEnabledUpdatingData(valueBoolean)
  else if (name === 'landlordPostalFee')
    updatingData = getLandlordPostalFeeUpdatingData(valueInt)
  else if (name === 'landlordPostalTax')
    updatingData = getLandlordPostalTaxUpdatingData(valueInt)
  else if (name === 'landlordReminderFeeEnabled')
    updatingData = getLandlordReminderFeeEnabledUpdatingData(
      valueBoolean,
      landlordInvoiceFirstReminder,
      landlordInvoiceSecondReminder
    )
  else if (name === 'landlordReminderFee')
    updatingData = getLandlordReminderFeeUpdatingData(valueInt)
  else if (name === 'landlordCollectionNoticeFeeEnabled')
    updatingData = getLandlordCollectionNoticeFeeEnabledUpdatingData(
      valueBoolean,
      landlordInvoiceCollectionNotice
    )
  else if (name === 'landlordCollectionNoticeFee')
    updatingData = getLandlordCollectionNoticeFeeUpdatingData(valueInt)
  // Invoicing
  else if (name === 'landlordInvoiceDueDay')
    updatingData = getLandlordInvoiceDueDayUpdatingData(valueInt)
  else if (name === 'landlordRentDue')
    updatingData = getLandlordRentDueUpdatingData(valueBoolean)
  else if (name === 'landlordRentDueDay')
    updatingData = getLandlordRentDueDayUpdatingData(valueInt)
  // Reminders
  else if (name === 'landlordFirstReminderEnabled')
    updatingData = getLandlordFirstReminderEnabledUpdatingData(valueBoolean)
  else if (name === 'landlordInvoiceFirstReminder')
    updatingData = getLandlordInvoiceFirstReminderUpdatingData(
      valueInt,
      landlordInvoiceSecondReminder
    )
  else if (name === 'landlordSecondReminderEnabled')
    updatingData = getLandlordSecondReminderEnabledUpdatingData(
      valueBoolean,
      landlordInvoiceFirstReminder
    )
  else if (name === 'landlordInvoiceSecondReminder')
    updatingData = getLandlordInvoiceSecondReminderUpdatingData(
      valueInt,
      landlordInvoiceFirstReminder
    )
  // Collection notice
  else if (name === 'landlordInvoiceCollectionNoticeEnabled')
    updatingData =
      getLandlordInvoiceCollectionNoticeEnabledUpdatingData(valueBoolean)
  else if (name === 'landlordInvoiceCollectionNotice')
    updatingData = getLandlordInvoiceCollectionNoticeUpdatingData(valueInt)
  else if (name === 'landlordInvoiceCollectionNoticeNewDays')
    updatingData =
      getLandlordInvoiceCollectionNoticeNewDaysUpdatingData(valueInt)
  else if (name === 'landlordAccountNumber')
    updatingData = getLandlordPaymentAccountNumberUpdatingData(valueString)

  return { updatingData }
}

export const getStandardPayoutDateUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  if (value > 0 && value <= 31) return { standardPayoutDate: value }
  else throw new CustomError(400, 'Value must be in between 1 to 31')
}

export const getCustomPayoutUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'customPayoutDays.enabled': value }
}

export const getDirectRemittanceApprovalEnabledData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'directRemittanceApproval.enabled': value }
}

export const getCustomPayoutDayUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'customPayoutDays.days': value }
}

export const getPayoutEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'payout.enabled': value }
}

export const getPayoutMonthUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'payout.payBeforeMonth': value }
}

export const getRetryFailedPayoutsEnabledUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'retryFailedPayouts.enabled': value }
}

export const getRetryFailedPayoutsTimeUpdatingData = (value) => {
  if (!validator.isInt(`${value}`))
    throw new CustomError(400, 'Value must be an integer')
  return { 'retryFailedPayouts.days': value }
}

export const getMultipleSigningUpdatingData = (value) => {
  if (!validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a boolean')
  return { 'directRemittanceApproval.isEnableMultipleSigning': value }
}

export const getCategoryPurposeCodeUpdatingData = (value) => {
  if (!value)
    throw new CustomError(
      400,
      'Value is required to update categoryPurposeCode'
    )
  if (validator.isInt(`${value}`) || validator.isBoolean(`${value}`))
    throw new CustomError(400, 'Value must be a string')
  if (value === 'OTHR' || value === 'SALA')
    return { 'directRemittanceApproval.categoryPurposeCode': value }
  throw new CustomError(400, 'Value must be SALA or OTHR')
}

export const getDirectRemittanceApprovalPersonAddingData = async (
  personId,
  directRemittanceApproval
) => {
  const { persons = [] } = directRemittanceApproval
  if (!personId)
    throw new CustomError(
      400,
      'Value is required to add directRemittanceApproval'
    )
  appHelper.validateId({ value: personId })
  const user = await userHelper.getAnUser({ _id: personId })
  if (!size(user)) throw new CustomError(404, "User doesn't exists")
  const ssn = user.getNorwegianNationalIdentification() || ''
  if (ssn) {
    persons.push(personId)
    return { 'directRemittanceApproval.persons': uniq(persons) }
  } else throw new CustomError(405, "User doesn't have NID")
}

export const getDirectRemittanceApprovalPersonRemovingData = async (
  personId,
  directRemittanceApproval
) => {
  const { persons = [] } = directRemittanceApproval
  if (!personId)
    throw new CustomError(
      400,
      'Value is required to remove directRemittanceApproval'
    )
  appHelper.validateId({ value: personId })
  if (indexOf(persons, personId) !== -1) {
    const newPersonsArray = pull(persons, personId)
    return { 'directRemittanceApproval.persons': uniq(newPersonsArray) }
  } else
    throw new CustomError(
      400,
      "Selected person doesn't exists in directRemittanceApproval"
    )
}

export const preparePayoutUpdatingData = async (params) => {
  const { partnerSetting, data } = params
  const { name, valueString, valueBoolean, valueInt } = data
  let updatingData = {}
  if (!name) throw new CustomError(400, 'Required name')
  const { directRemittanceApproval = {} } = partnerSetting
  // Payout
  if (name === 'standardPayoutDate')
    updatingData = getStandardPayoutDateUpdatingData(valueInt)
  else if (name === 'customPayout')
    updatingData = getCustomPayoutUpdatingData(valueBoolean)
  else if (name === 'customPayoutDay')
    updatingData = getCustomPayoutDayUpdatingData(valueInt)
  else if (name === 'payoutEnabled')
    updatingData = getPayoutEnabledUpdatingData(valueBoolean)
  else if (name === 'payoutMonth')
    updatingData = getPayoutMonthUpdatingData(valueInt)
  else if (name === 'retryFailedPayoutsEnabled')
    updatingData = getRetryFailedPayoutsEnabledUpdatingData(valueBoolean)
  else if (name === 'retryFailedPayoutsTime')
    updatingData = getRetryFailedPayoutsTimeUpdatingData(valueInt)
  else if (name === 'isEnableMultipleSigning')
    updatingData = getMultipleSigningUpdatingData(valueBoolean)
  else if (name === 'categoryPurposeCode')
    updatingData = getCategoryPurposeCodeUpdatingData(valueString)
  else if (name === 'addDirectRemittanceApproval')
    updatingData = await getDirectRemittanceApprovalPersonAddingData(
      valueString,
      directRemittanceApproval
    )
  // valueString must be an UserId
  else if (name === 'removeDirectRemittanceApproval')
    updatingData = await getDirectRemittanceApprovalPersonRemovingData(
      valueString,
      directRemittanceApproval
    )
  else if (name === 'enabledDirectRemittanceApprovalForDirectPartner')
    updatingData = getDirectRemittanceApprovalEnabledData(valueBoolean)
  return { updatingData }
}

const validateBankAccountData = (bankAccountData = {}) => {
  const { accountNumber, ledgerAccountId } = bankAccountData
  if (
    accountNumber &&
    (size(accountNumber) !== 11 || !validator.isNumeric(accountNumber))
  ) {
    throw new CustomError(400, 'Account number must be a string of 11 digits')
  }
  if (ledgerAccountId) {
    appHelper.validateId({ ledgerAccountId })
  }
}

export const validateBankAccountAddData = (data = {}) => {
  let requiredFields = ['bankAccountData', 'partnerId']
  appHelper.checkRequiredFields(requiredFields, data)
  const { bankAccountData } = data
  requiredFields = ['accountNumber', 'bic', 'ledgerAccountId']
  appHelper.checkRequiredFields(requiredFields, bankAccountData)
}

export const validateBankAccountUpdateData = (data = {}) => {
  const requiredFields = ['bankAccountData', 'bankAccountId', 'partnerId']
  appHelper.checkRequiredFields(requiredFields, data)
  const { bankAccountData, bankAccountId } = data
  appHelper.validateId({ bankAccountId })
  if (!size(bankAccountData)) {
    throw new CustomError(400, 'Bank account data can not be empty')
  }
  validateBankAccountData(bankAccountData)
}

export const validateBankAccountDeleteData = (data = {}) => {
  const requiredFields = ['accountNumber', 'bankAccountId', 'partnerId']
  appHelper.checkRequiredFields(requiredFields, data)
  validateBankAccountData(data)
  const { bankAccountId, partnerId } = data
  appHelper.validateId({ bankAccountId })
  appHelper.validateId({ partnerId })
}

export const validateDataForCreatingBankAccount = (
  partnerId,
  bankAccountData = {}
) => {
  const { accountNumber } = bankAccountData
  if (!partnerId) {
    throw new CustomError(400, 'Missing partner id')
  }
  appHelper.validateId({ partnerId })
  if (!accountNumber) {
    throw new CustomError(400, 'Missing account number')
  }
  validateBankAccountData(bankAccountData)
}

export const findAccountNumber = (partnerSetting = {}, bankAccountId) => {
  const { bankAccounts = [] } = partnerSetting
  const bankAccount =
    bankAccounts.find((bankAccount) => bankAccount.id === bankAccountId) || {}
  return bankAccount.accountNumber || ''
}

export const findBankAccount = (partnerSetting = {}, bankAccountId) => {
  const { bankAccounts = [] } = partnerSetting
  const bankAccount =
    bankAccounts.find((bankAccount) => bankAccount.id === bankAccountId) || {}
  return bankAccount
}

export const isAccountNumberExists = async (accountNumber, session) => {
  if (accountNumber) {
    const query = {
      bankAccounts: { $exists: true, $elemMatch: { accountNumber } }
    }
    const partnerSetting = await getAPartnerSetting(query, session)
    return !!partnerSetting
  }
}

export const isAccountNumberBeingUsed = async (
  accountNumber,
  partnerId,
  session
) => {
  let query = {
    partnerId,
    invoiceAccountNumber: accountNumber
  }
  const isInInvoice = !!size(await invoiceHelper.getInvoice(query, session))
  query = {
    partnerId,
    'rentalMeta.invoiceAccountNumber': accountNumber
  }
  const isInContract = !!size(await contractHelper.getAContract(query, session))
  return isInInvoice || isInContract
}

export const prepareBankAccountUpdateData = (bankAccountData = {}) => {
  const {
    accountNumber,
    bic,
    orgName,
    orgId,
    orgAddress,
    orgZipCode,
    orgCity,
    orgCountry,
    vatRegistered,
    ledgerAccountId,
    backupOrgAddress
  } = bankAccountData

  const updateData = {}
  if (accountNumber) updateData['bankAccounts.$.accountNumber'] = accountNumber
  if (bic) updateData['bankAccounts.$.bic'] = bic
  if (orgName) updateData['bankAccounts.$.orgName'] = orgName
  if (orgId) updateData['bankAccounts.$.orgId'] = orgId
  if (orgAddress) updateData['bankAccounts.$.orgAddress'] = orgAddress
  if (orgZipCode) updateData['bankAccounts.$.orgZipCode'] = orgZipCode
  if (orgCity) updateData['bankAccounts.$.orgCity'] = orgCity
  if (orgCountry) updateData['bankAccounts.$.orgCountry'] = orgCountry
  if (isBoolean(vatRegistered))
    updateData['bankAccounts.$.vatRegistered'] = vatRegistered
  if (ledgerAccountId)
    updateData['bankAccounts.$.ledgerAccountId'] = ledgerAccountId
  if (backupOrgAddress)
    updateData['bankAccounts.$.backupOrgAddress'] = backupOrgAddress
  return updateData
}

export const prepareCompanyInfoSettingUpdateData = (data) => {
  const { name, value, valueBoolean } = data
  if (!(name && (value || isBoolean(valueBoolean)))) {
    throw new CustomError(400, 'Missing name, value or valueBoolean!')
  }
  let updatingData = {}
  const dataObj = {
    companyName: { 'companyInfo.companyName': value },
    organizationId: { 'companyInfo.organizationId': value },
    officeAddress: { 'companyInfo.officeAddress': value },
    officeZipCode: { 'companyInfo.officeZipCode': value },
    officeCity: { 'companyInfo.officeCity': value },
    officeCountry: { 'companyInfo.officeCountry': value },
    postalAddress: { 'companyInfo.postalAddress': value },
    postalZipCode: { 'companyInfo.postalZipCode': value },
    postalCity: { 'companyInfo.postalCity': value },
    postalCountry: { 'companyInfo.postalCountry': value },
    phoneNumber: { 'companyInfo.phoneNumber': value },
    email: { 'companyInfo.email': value },
    website: { 'companyInfo.website': value },
    isLogoLinkedToWebsite: { 'companyInfo.isLogoLinkedToWebsite': valueBoolean }
  }
  if (size(dataObj[name])) updatingData = dataObj[name]
  if (!size(updatingData)) {
    throw new CustomError(400, 'Invalid name for company info update')
  }
  updatingData['companyInfo.lastUpdate'] = new Date()
  return updatingData
}

export const getAPartnerSettingInfo = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)

  const { query } = body
  appHelper.checkRequiredFields(['partnerId'], query)

  const partnerSetting = await getAPartnerSetting(query, session)
  if (!size(partnerSetting))
    throw new CustomError(404, 'PartnerSetting is not found by query!')

  return partnerSetting
}

const prepareDropDownByInvoice = ({ invoiceAccountNumber = '' }) => {
  if (!invoiceAccountNumber) return {}
  return { text: invoiceAccountNumber, id: invoiceAccountNumber }
}

const prepareDropDownByContract = async (contractId) => {
  const contract =
    (await contractHelper.getAContract({ _id: contractId })) || {}
  const { rentalMeta = {} } = contract
  const { invoiceAccountNumber = '' } = rentalMeta
  if (!invoiceAccountNumber) return
  return { text: invoiceAccountNumber, id: invoiceAccountNumber }
}

const prepareDropDownByAccounts = async (accountId) => {
  const account = (await accountHelper.getAccountById(accountId)) || {}
  const { bankAccountNumbers = [] } = account
  if (!size(bankAccountNumbers)) return []
  return map(bankAccountNumbers, (accNumber) => ({
    text: accNumber,
    id: accNumber
  }))
}

const prepareDropDownByPartnerSettings = async (partnerId) => {
  const partnerSettings =
    (await partnerSettingHelper.getSettingByPartnerId(partnerId)) || {}
  const { bankAccounts } = partnerSettings || {}
  if (!size(bankAccounts)) return []
  return map(bankAccounts, ({ accountNumber }) => ({
    text: accountNumber,
    id: accountNumber
  }))
}

const prepareDropDownData = async (body, user) => {
  const { query } = body
  const { invoiceId, contractId, context } = query
  const { partnerId } = user
  const invoiceInfo = (await invoiceHelper.getInvoice({ _id: invoiceId })) || {}
  const dropDown = []
  if (size(invoiceInfo)) {
    if (context === 'onlyInvoiceAccNumber') {
      return [
        {
          text: invoiceInfo?.invoiceAccountNumber,
          id: invoiceInfo?.invoiceAccountNumber
        }
      ]
    }
    const dropDownDataByInvoice = prepareDropDownByInvoice(invoiceInfo)
    if (size(dropDownDataByInvoice)) dropDown.push(dropDownDataByInvoice)
  } else {
    const dropDownDataByContract = await prepareDropDownByContract(contractId)
    if (size(dropDownDataByContract)) dropDown.push(dropDownDataByContract)
  }
  const isDirect = await partnerHelper.isDirectPartner(partnerId)
  if (isDirect) {
    const { accountId } = invoiceInfo
    const dropDownDataByAccount = await prepareDropDownByAccounts(accountId)
    if (size(dropDownDataByAccount)) {
      dropDown.push(...dropDownDataByAccount)
    }
  } else {
    const dropDownDataByPartnerSetting = await prepareDropDownByPartnerSettings(
      partnerId
    )
    if (size(dropDownDataByPartnerSetting))
      dropDown.push(...dropDownDataByPartnerSetting)
  }
  return uniqBy(dropDown, (item) => item.id)
}

export const getBankAccountNumbers = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const dropDownData = await prepareDropDownData(body, user)
  return dropDownData
}

export const getPartnerSettingForPublicApp = async (req) => {
  const { user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId = '' } = user
  console.log('partnerId ==> ', partnerId)
  const partnerSetting = await getAPartnerSetting({ partnerId })
  console.log('Found partnerSetting ==> ', partnerSetting)
  return partnerSetting
}
