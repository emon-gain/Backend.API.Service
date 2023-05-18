import Handlebars from 'handlebars'
import nid from 'nid'
import { omit, pick, map, size } from 'lodash'

import {
  AccountCollection,
  ContractCollection,
  ListingCollection,
  NotificationTemplateCollection,
  PartnerCollection,
  TenantCollection,
  UserCollection
} from '../models'
import {
  accountHelper,
  addonHelper,
  appHelper,
  branchHelper,
  contractHelper,
  fileHelper,
  invoiceHelper,
  listingHelper,
  notificationLogHelper,
  partnerHelper,
  propertyItemHelper,
  ruleHelper,
  userHelper
} from '../helpers'
import { CustomError } from '../common'

export const getNotificationTemplate = async (query, session) => {
  const notificationTemplate = await NotificationTemplateCollection.findOne(
    query
  ).session(session)
  return notificationTemplate
}

export const getNotificationTemplates = async (query, session) => {
  const notificationTemplates = await NotificationTemplateCollection.find(
    query
  ).session(session)
  return notificationTemplates
}

export const getDistinctNotificationTemplates = async (
  field,
  query,
  session
) => {
  const distinctNotificationTemplate =
    await NotificationTemplateCollection.distinct(field, query).session(session)
  return distinctNotificationTemplate || []
}

const prepareQueryData = (query) => {
  const { category, defaultSearch, partnerId, subject, title } = query
  delete query.title
  delete query.subject
  delete query.defaultSearch
  delete query.partnerId
  let partnerQuery = []
  let searchQuery = []
  if (category === 'send_wrong_ssn_notification') {
    query.category = {
      $in: ['account_wrong_ssn_notification', 'tenant_wrong_ssn_notification']
    }
  }
  if (!partnerId) {
    query.partnerId = { $exists: false }
  } else {
    partnerQuery = [
      { partnerId },
      {
        partnerId: { $exists: false },
        copiedBy: { $ne: partnerId },
        templateType: { $ne: 'app' }
      }
    ]
  }
  if (title) {
    searchQuery = [
      { 'title.en': new RegExp('.*' + title + '.*', 'i') },
      { 'title.no': new RegExp('.*' + title + '.*', 'i') }
    ]
  }
  if (subject) {
    searchQuery = [
      {
        'subject.en': new RegExp('.*' + subject + '.*', 'i')
      },
      {
        'subject.no': new RegExp('.*' + subject + '.*', 'i')
      }
    ]
  }
  if (defaultSearch) {
    const defaultSearchExp = new RegExp('.*' + defaultSearch + '.*', 'i')
    searchQuery = [
      {
        $or: [
          { 'title.en': defaultSearchExp },
          { 'title.no': defaultSearchExp }
        ]
      },
      {
        $or: [
          { 'subject.en': defaultSearchExp },
          { 'subject.no': defaultSearchExp }
        ]
      }
    ]
  }
  if (size(partnerQuery) && size(searchQuery)) {
    query['$and'] = [{ $or: partnerQuery }, { $or: searchQuery }]
  } else if (size(partnerQuery) || size(searchQuery)) {
    query['$or'] = [...partnerQuery, ...searchQuery]
  }
  return query
}

export const getNotificationTemplatesForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const preparedQuery = prepareQueryData(query)
  const notificationTemplates = await NotificationTemplateCollection.find(
    preparedQuery
  )
    .populate(['partner', 'createdUser'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return notificationTemplates
}

export const validateNotificationTemplatesQuery = (query) => {
  const { type, templateType, partnerId } = query
  if (
    type &&
    !(
      type === 'email' ||
      type === 'sms' ||
      type === 'attachment' ||
      type === 'pdf' ||
      type === 'assignment_contract' ||
      type === 'lease_contract' ||
      type === 'moving_in_esigning' ||
      type === 'moving_out_esigning'
    )
  )
    throw new CustomError(400, 'type should be valid')

  if (templateType && !(templateType === 'dtms' || templateType === 'app'))
    throw new CustomError(400, 'templateType should be valid')

  if (partnerId) {
    appHelper.validateId({ partnerId })
  }
}

export const countNotificationTemplates = async (query, session) => {
  const numberOfNotificationTemplates =
    await NotificationTemplateCollection.find(query)
      .session(session)
      .countDocuments()
  return numberOfNotificationTemplates
}

export const countNumberOfTemplates = async (uniqueId, partnerId, session) => {
  const attachmentObj = { uniqueId, type: 'attachment' }
  const emailObj = {
    type: 'email',
    attachments: { $in: [uniqueId] }
  }

  if (partnerId) {
    attachmentObj['partnerId'] = partnerId
    emailObj['partnerId'] = partnerId
  }
  const query = {
    $or: [attachmentObj, emailObj]
  }
  const countedTemplates = await countNotificationTemplates(query, session) // Count template
  return countedTemplates
}

export const getNumberOfRule = async (uniqueId) => {
  if (!uniqueId) {
    return false
  }
  const countedRules = await ruleHelper.getNumberOfRuleByUniqueId(uniqueId)
  return countedRules
}

export const checkIsDeletable = async (notificationTemplate, partnerId) => {
  const { type, copiedBy, uniqueId = '' } = notificationTemplate
  if (type === 'attachment') {
    // If the attachment template exist in any email template
    const countedTemplates = await countNumberOfTemplates(uniqueId, partnerId)
    if (countedTemplates && countedTemplates > 1) {
      return false //If count more then one then can not delete
    }
  }
  if (size(copiedBy)) {
    return false
  } else if (await getNumberOfRule(uniqueId)) {
    return false
  }
  return true
}

const getAttachmentTemplates = async (uniqueIds = [], partnerId) => {
  const templates = []
  for (const uniqueId of uniqueIds) {
    const query = {
      uniqueId,
      partnerId: partnerId || { $exists: false }
    }
    let template = await getNotificationTemplate(query)
    if (partnerId && !size(template)) {
      query.partnerId = { $exists: false }
      template = await getNotificationTemplate(query)
    }
    if (size(template)) {
      templates.push(template)
    }
  }
  return templates
}

export const getNotificationTemplateInfoByUniqueId = async (
  uniqueId,
  partnerId
) => {
  let notificationTemplateInfo = null
  const query = { uniqueId }

  if (uniqueId) {
    if (partnerId) {
      query.partnerId = partnerId
      notificationTemplateInfo = await getNotificationTemplate(query)
      if (size(notificationTemplateInfo)) return notificationTemplateInfo
    }
    query.partnerId = { $exists: false }
    notificationTemplateInfo = await getNotificationTemplate(query)
  }

  return notificationTemplateInfo
}

export const getTemplatesWithAttachmentForLambda = async (options) => {
  const notificationTemplates = []
  const attachmentTemplates = []

  const { event = null, partnerId = null, uniqueIds = [] } = options

  if (!event) throw new CustomError(400, "Event doesn't exists")

  for (const uniqueId of uniqueIds) {
    let notificationTemplateInfo = await getNotificationTemplateInfoByUniqueId(
      uniqueId,
      partnerId
    )

    const { attachments = [] } = notificationTemplateInfo || {}

    if (size(attachments)) {
      for (const attachmentUniqueId of attachments) {
        let attachmentTemplateInfo =
          await getNotificationTemplateInfoByUniqueId(
            attachmentUniqueId,
            partnerId
          )

        if (size(attachmentTemplateInfo)) {
          attachmentTemplateInfo = JSON.parse(
            JSON.stringify(attachmentTemplateInfo)
          )
          attachmentTemplates.push({
            ...attachmentTemplateInfo,
            event,
            partnerId
          })
        }
      }
    }

    if (size(notificationTemplateInfo)) {
      notificationTemplateInfo = JSON.parse(
        JSON.stringify(notificationTemplateInfo)
      )
      notificationTemplates.push({
        ...notificationTemplateInfo,
        event,
        partnerId
      })
    }
  }

  return { notificationTemplates, attachmentTemplates }
}

export const prepareNotificationTemplatesQueryDataForLambda = (query) => {
  const { templateFor = '' } = query
  query.partnerId = {
    $exists: true
  }
  if (templateFor === 'otpEmail') query.isOtpEmail = true
  else if (templateFor === 'downloadTemplate') query.isDownloadTemplate = true
  else if (templateFor === 'changeEmail') query.isChangeEmail = true
  else if (templateFor === 'chatNotification') query.isChatNotification = true
  else if (templateFor === 'contactUs') query.isContactUs = true
  else if (templateFor === 'emailFooter') query.isEmailFooter = true
  else if (templateFor === 'partnerUserInvitation')
    query.isPartnerUserInvitation = true
  else if (templateFor === 'isInvoice') query.isInvoice = true
  else if (templateFor === 'isAppInvoice') {
    query.isAppInvoice = true
    delete query.partnerId
  } else if (templateFor === 'isCreditNote') query.isCreditNote = true
  else if (templateFor === 'isAnnualStatement') {
    query.isAnnualStatement = true
    query.partnerId = {
      $exists: false
    }
  } else if (templateFor === 'isFinalSettlementInvoice')
    query.isFinalSettlementInvoice = true
  else if (templateFor === 'isLandlordInvoice') query.isLandlordInvoice = true
  else if (templateFor === 'isLandlordCreditNote')
    query.isLandlordCreditNote = true
  else if (templateFor === 'isPdfFooter') query.isPdfFooter = true
  return omit(query, ['templateFor'])
}

export const getNotificationTemplatesForLambdaQuery = async (query) => {
  const templateQuery = prepareNotificationTemplatesQueryDataForLambda(query)
  console.log('prepared templateQuery : ', templateQuery)
  let notificationTemplates = await NotificationTemplateCollection.find(
    templateQuery
  )
  console.log('prepared notificationTemplates : ', notificationTemplates)
  if (!size(notificationTemplates) && templateQuery.partnerId) {
    templateQuery.partnerId = { $exists: false }
    notificationTemplates = await NotificationTemplateCollection.find(
      templateQuery
    )
  }

  return notificationTemplates
}

export const queryNotificationTemplatesForLambda = async (req) => {
  const { body, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { query } = body
  console.log('checking query for notification template : ', query)
  // For Lambda notifier service
  if (size(query.options)) {
    const notificationTemplates = await getTemplatesWithAttachmentForLambda(
      query.options
    )
    return { data: [notificationTemplates] }
  }
  // Others
  validateNotificationTemplatesQuery(query)

  const notificationTemplates = await getNotificationTemplatesForLambdaQuery(
    query
  )

  return { data: notificationTemplates }
}

export const queryNotificationTemplates = async (req) => {
  const { body, user = {} } = req
  const { partnerId = '' } = user

  appHelper.checkRequiredFields(['userId'], user)
  const { options } = body
  appHelper.validateSortForQuery(options.sort)

  if (partnerId) {
    body.query['partnerId'] = partnerId
    body.query['isLandlordCreditNote'] = { $ne: true }
    body.query['isLandlordDueReminder'] = { $ne: true }
    body.query['isLandlordFirstReminder'] = { $ne: true }
    body.query['isLandlordSecondReminder'] = { $ne: true }
  }
  validateNotificationTemplatesQuery(body.query)
  if (body.query.type === 'pdf') {
    body.query.type = {
      $in: [
        'pdf',
        'assignment_contract',
        'lease_contract',
        'moving_in_esigning',
        'moving_out_esigning'
      ]
    }
  }
  const { type = '' } = body.query

  const notificationTemplatesData = await getNotificationTemplatesForQuery(body)
  const filteredDocuments = await countNotificationTemplates(body.query)
  const totalDocumentsQuery = type
    ? !partnerId
      ? { type, partnerId: { $exists: false } }
      : { type }
    : !partnerId
    ? { partnerId: { $exists: false } }
    : {}
  const totalDocuments = await countNotificationTemplates(totalDocumentsQuery)
  const notificationTemplates = await Promise.all(
    notificationTemplatesData.map(async (template) => {
      const notificationTemplate = JSON.parse(JSON.stringify(template))
      notificationTemplate.isDeletable = await checkIsDeletable(
        notificationTemplate,
        partnerId
      )
      notificationTemplate.attachments = await getAttachmentTemplates(
        notificationTemplate.attachments,
        partnerId
      )
      const { createdUser } = notificationTemplate
      notificationTemplate.createdUser = {
        name: createdUser?.profile?.name,
        avatarKey: createdUser ? userHelper.getAvatar(createdUser) : ''
      }
      notificationTemplate.templateLabel = notificationTemplate.isCustom
        ? 'custom'
        : notificationTemplate.partnerId
        ? 'edited'
        : 'standard'
      return notificationTemplate
    })
  )
  return {
    data: notificationTemplates,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const getTemplateVariables = () => [
  {
    name: 'annual_statement.title',
    context: 'landlord_annual_statement',
    variables: [
      { name: 'report_year', type: 'year' },
      {
        name: 'annual_statement_items',
        type: 'annual_statement_items',
        subItems: [
          'report_year',
          'rent_total_excl_tax',
          'rent_total_tax',
          'rent_total',
          'commission_total_amount',
          'commission_total_vat',
          'commission_total',
          'total_payouts'
        ]
      }
    ]
  },
  {
    name: 'contract.title',
    context: 'contract',
    variables: [
      { name: 'lease_start_date', type: 'date' },
      {
        name: 'lease_start_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      { name: 'lease_end_date', type: 'date' },
      { name: 'lease_id', type: 'id' },
      { name: 'monthly_rent_amount', type: 'amount' },
      { name: 'deposit_amount', type: 'amount' },
      { name: 'deposit_insurance_amount', type: 'amount' },
      { name: 'invoice_amount', type: 'amount' },
      { name: 'total_deposit_amount', type: 'amount' },
      { name: 'deposit_insurance_reference_number', type: 'number' },
      { name: 'internal_deposit_insurance_reference_number', type: 'number' },
      { name: 'minimum_stay', type: 'number' },
      { name: 'future_monthly_rent_amount', type: 'amount' },
      { name: 'internal_lease_id', type: 'id' },
      { name: 'last_CPI_date', type: 'date' },
      { name: 'next_CPI_date', type: 'date' },
      { name: 'has_future_monthly_rent_amount', type: 'amount' },
      { name: 'CPI_from_month', type: 'date' },
      { name: 'CPI_to_month', type: 'date' },
      { name: 'invoice_due_date', type: 'date' },
      {
        name: 'VAT_status',
        type: 'boolean',
        availability: ['isLeaseEsigningPdf']
      },
      {
        name: 'bank_account_number',
        type: 'bank_account',
        availability: ['isLeaseEsigningPdf']
      },
      { name: 'notice_in_effect', type: 'notice_effect' },
      { name: 'notice_period', type: 'notice_period' },
      { name: 'representative_name', type: 'name' },
      { name: 'representative_occupation', type: 'occupation' },
      { name: 'representative_phone', type: 'phone_number' },
      { name: 'representative_email', type: 'email' },
      { name: 'termination_reason', type: 'string' },
      {
        name: 'lease_addons',
        type: 'addons',
        subItems: ['addon_name', 'addon_price'],
        availability: ['isLeaseEsigningPdf']
      },
      {
        name: 'monthly_due_date',
        type: 'number',
        availability: ['isLeaseEsigningPdf']
      },
      {
        name: 'invoice_items',
        type: 'items',
        subItems: [
          'item_name',
          'item_quantity',
          'item_price',
          'item_tax',
          'item_total'
        ],
        availability: ['isEvictionDocument'] //This variable visible only for invoice, reminder and collection notice attachment templates
      },
      {
        name: 'invoice_total',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'total_due',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'eviction_notice_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      {
        name: 'total_due_rent_invoices',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'total_overdue_rent_invoices',
        type: 'amount'
      },
      {
        name: 'lease_signed_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      { name: 'hiscox_logo_url', type: 'url' },
      {
        name: 'administration_of_eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'invoice_eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'administration_eviction_notice_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      }
    ]
  },
  {
    name: 'common.agent_esigning',
    context: 'agent_esigning',
    variables: [{ name: 'agent_esigning_url', type: 'url' }]
  },
  {
    name: 'common.agent_moving_in_esigning',
    context: 'agent_moving_in_esigning',
    variables: [{ name: 'agent_moving_in_esigning_url', type: 'url' }]
  },
  {
    name: 'common.agent_moving_out_esigning',
    context: 'agent_moving_out_esigning',
    variables: [{ name: 'agent_moving_out_esigning_url', type: 'url' }]
  },
  {
    name: 'common.all',
    context: 'all',
    variables: [
      { name: 'todays_date', type: 'date' },
      { name: 'app_logo_url', type: 'url' }
    ]
  },
  {
    name: 'common.app_health',
    context: 'appHealth',
    variables: [
      { name: 'app_url', type: 'app_url' },
      {
        name: 'error_issues',
        type: 'items',
        subItems: ['errors_type', 'errors_value']
      },
      {
        name: 'new_issues',
        type: 'items',
        subItems: ['new_issue_type', 'new_issue_value']
      },
      { name: 'total_issues', type: 'number' },
      { name: 'all_issues_are_same', type: 'boolean' },
      { name: 'all_issues_are_new', type: 'boolean' }
    ]
  },
  {
    name: 'common.assignment',
    context: 'assignment',
    variables: [
      { name: 'assignment_id', type: 'id' },
      { name: 'brokering_commission', type: 'amount' },
      { name: 'management_commission', type: 'amount' },
      { name: 'monthly_rent', type: 'amount' },
      { name: 'deposit_amount', type: 'amount' },
      { name: 'assignment_from', type: 'date' },
      { name: 'assignment_to', type: 'date' },
      { name: 'payout_to_bank_account', type: 'bank_account' },
      { name: 'brokering_commission_amount', type: 'amount' },
      { name: 'brokering_commission_percentage', type: 'percentage' },
      { name: 'management_commission_amount', type: 'amount' },
      { name: 'management_commission_percentage', type: 'percentage' },
      { name: 'representative_name', type: 'name' },
      { name: 'representative_occupation', type: 'occupation' },
      { name: 'representative_phone', type: 'phone_number' },
      { name: 'representative_email', type: 'email' },
      {
        name: 'assignment_addons',
        type: 'addons',
        subItems: ['addon_name', 'addon_price'],
        availability: ['isAssignmentEsigningPdf']
      }
    ]
  },
  {
    name: 'common.credit_note',
    context: 'credit_note',
    variables: [
      { name: 'invoice_id', type: 'id' },
      { name: 'credit_invoice_id', type: 'id' },
      { name: 'credit_note_date', type: 'date' },
      { name: 'credit_note_start_date', type: 'date' },
      { name: 'credit_note_end_date', type: 'date' },
      { name: 'credit_note_due_date', type: 'date' },
      { name: 'credit_note_total', type: 'amount' },
      { name: 'credit_note_account_number', type: 'number' },
      {
        name: 'credit_note_items',
        type: 'items',
        subItems: [
          'item_name',
          'item_quantity',
          'item_price',
          'item_tax',
          'item_total'
        ],
        availability: ['isCreditNote'] // This variable visible only for invoice attachment template
      },
      {
        name: 'credit_reason',
        availability: ['isCreditNote']
      }
    ]
  },
  {
    name: 'common.credit_rating',
    context: 'credit_rating',
    variables: [{ name: 'credit_rating_ask_url', type: 'url' }]
  },
  {
    name: 'common.deposit',
    context: 'deposit',
    variables: [
      { name: 'deposit_bank_account_number', type: 'bank_account' },
      { name: 'payment_amount', type: 'amount' },
      { name: 'total_payment_amount', type: 'amount' },
      { name: 'payment_reference', type: 'number' },
      { name: 'tenant_deposit_amount', type: 'number' },
      { name: 'current_balance', type: 'amount' },
      { name: 'payment_date', type: 'date' }
    ]
  },
  {
    name: 'common.download',
    context: 'download',
    variables: [
      { name: 'download_context', type: 'context' },
      { name: 'download_url', type: 'url' },
      { name: 'has_download_url', type: 'boolean' }
    ]
  },
  {
    name: 'common.filters.account',
    context: 'account',
    variables: [
      { name: 'account_name', type: 'name' },
      { name: 'account_address', type: 'address' },
      { name: 'account_zip_code', type: 'zip_code' },
      { name: 'account_city', type: 'city' },
      { name: 'account_country', type: 'country' },
      { name: 'account_email', type: 'email' },
      { name: 'account_id', type: 'id' },
      { name: 'account_phonenumber', type: 'phone_number' },
      { name: 'account_person_id', type: 'NID' },
      { name: 'account_org_id', type: 'id' },
      {
        name: 'account_bank_account',
        type: 'bank_account',
        availability: ['isEvictionDocument']
      }
    ]
  },
  {
    name: 'common.filters.property',
    context: 'property',
    variables: [
      { name: 'property_location', type: 'location_name' },
      { name: 'property_zip_code', type: 'zip_code' },
      { name: 'property_city', type: 'city' },
      { name: 'property_country', type: 'country' },
      { name: 'property_id', type: 'id' },
      { name: 'property_gnr', type: 'string' },
      { name: 'property_bnr', type: 'string' },
      { name: 'property_snr', type: 'string' },
      { name: 'property_number_of_bedrooms', type: 'number' },
      { name: 'property_livingroom_yes_or_no', type: 'boolean' },
      { name: 'property_kitchen_yes_or_no', type: 'boolean' },
      { name: 'property_furnished_yes_or_no', type: 'boolean' },
      { name: 'property_municipality', type: 'address' },
      { name: 'apartment_id', type: 'id' },
      {
        name: 'interestform_url',
        type: 'url'
      },
      {
        name: 'property_addons',
        type: 'addons',
        subItems: ['addon_name', 'addon_price']
      }
    ]
  },
  {
    name: 'common.landlord_esigning',
    context: 'landlord_esigning',
    variables: [{ name: 'landlord_esigning_url', type: 'url' }]
  },
  {
    name: 'common.landlord_moving_in_esigning',
    context: 'landlord_moving_in_esigning',
    variables: [{ name: 'landlord_moving_in_esigning_url', type: 'url' }]
  },
  {
    name: 'common.landlord_moving_out_esigning',
    context: 'landlord_moving_out_esigning',
    variables: [{ name: 'landlord_moving_out_esigning_url', type: 'url' }]
  },
  {
    name: 'common.landlord_lease_esigning',
    context: 'landlord_lease_esigning',
    variables: [{ name: 'landlord_lease_esigning_url', type: 'url' }]
  },
  {
    name: 'common.listing',
    context: 'listing',
    variables: [
      { name: 'has_listing' },
      { name: 'listing_location', type: 'address' },
      { name: 'listing_type_and_property_name', type: 'name' },
      { name: 'listing_image_url', type: 'url' },
      { name: 'listing_availability', type: 'date' },
      { name: 'listing_minimum_stay', type: 'number' }
    ]
  },
  {
    name: 'common.partner',
    context: 'partner',
    variables: [
      { name: 'partner_name', type: 'name' },
      { name: 'partner_logo_url', type: 'url' },
      { name: 'partner_id', type: 'id' },
      { name: 'partner_address', type: 'address' },
      { name: 'partner_zip_code', type: 'zip_code' },
      { name: 'partner_city', type: 'city' },
      { name: 'partner_country', type: 'country' },
      { name: 'partner_org_id', type: 'id' },
      { name: 'branch_name', type: 'branch_name' },
      { name: 'agent_name', type: 'name' },
      { name: 'agent_email', type: 'email' },
      { name: 'agent_phonenumber', type: 'phone_number' },
      { name: 'agent_occupation', type: 'job_title' },
      { name: 'manager_name', type: 'name' },
      { name: 'partner_url', type: 'partner_url' },
      {
        name: 'partner_bank_account',
        type: 'bank_account',
        availability: ['isEvictionDocument']
      }
    ]
  },
  {
    name: 'common.payout',
    context: 'payout',
    variables: [
      { name: 'payout_total', type: 'amount' },
      { name: 'brokering_commission_amount', type: 'amount' },
      { name: 'management_commission_amount', type: 'amount' },
      { name: 'payout_addons', type: 'amount' },
      { name: 'payout_corrections', type: 'amount' },
      { name: 'payout_paid_by_bank_date', type: 'date' },
      { name: 'payout_to_bank_account', type: 'number' },
      { name: 'payout_from_bank_account', type: 'number' },
      { name: 'last_unpaid_payouts', type: 'amount' },
      { name: 'addon_total', type: 'amount' },
      {
        name: 'addons',
        type: 'addons',
        subItems: [
          'addon_name',
          'addon_price',
          'addon_tax_percentage',
          'addon_description'
        ]
      }
    ]
  },
  {
    name: 'common.tasks',
    context: 'task',
    variables: [
      { name: 'assignee_name', type: 'name' },
      { name: 'assigned_by', type: 'name' },
      { name: 'task_title', type: 'task_title' },
      { name: 'comment', type: 'comment' },
      { name: 'task_url', type: 'url' }
    ]
  },
  {
    name: 'common.tenant',
    context: 'tenant',
    variables: [
      { name: 'tenant_name', type: 'name' },
      { name: 'tenant_id', type: 'id' },
      { name: 'tenant_serial_id', type: 'id' },
      { name: 'tenant_address', type: 'address' },
      { name: 'tenant_zip_code', type: 'zip_code' },
      { name: 'tenant_city', type: 'city' },
      { name: 'tenant_country', type: 'country' },
      { name: 'tenant_email', type: 'email' },
      { name: 'tenant_phonenumber', type: 'phone_number' },
      { name: 'tenant_person_id', type: 'NID' },
      {
        name: 'tenants',
        type: 'tenants',
        subItems: [
          'tenant_name',
          'tenant_id',
          'tenant_address',
          'tenant_zip_code',
          'tenant_city',
          'tenant_country',
          'tenant_email',
          'tenant_phonenumber',
          'tenant_person_id'
        ],
        availability: ['isLeaseEsigningPdf', 'isDepositInsurancePaymentPending']
      },
      {
        name: 'new_password_url',
        type: 'url',
        availability: ['isWelcomeLease']
      },
      { name: 'jointly_liable_tenant_name', type: 'name' },
      { name: 'jointly_liable_tenant_names', type: 'name' },
      { name: 'jointly_liable_tenant_email', type: 'email' },
      { name: 'jointly_liable_tenant_emails', type: 'email' },
      { name: 'jointly_liable_tenant_person_IDs', type: 'NID' }
    ]
  },
  {
    name: 'common.tenant_esigning',
    context: 'tenant_lease_esigning',
    variables: [{ name: 'tenant_lease_esigning_url', type: 'url' }]
  },
  {
    name: 'common.tenant_moving_in_esigning',
    context: 'tenant_moving_in_esigning',
    variables: [{ name: 'tenant_moving_in_esigning_url', type: 'url' }]
  },
  {
    name: 'common.tenant_moving_out_esigning',
    context: 'tenant_moving_out_esigning',
    variables: [{ name: 'tenant_moving_out_esigning_url', type: 'url' }]
  },
  {
    name: 'common.user',
    context: 'user',
    variables: [
      { name: 'has_password', type: 'boolean' },
      { name: 'user_invitation_url', type: 'url' }
    ]
  },
  {
    name: 'payments.labels.invoice',
    context: 'invoice' || 'app_invoice',
    variables: [
      { name: 'invoice_id', type: 'id' },
      { name: 'final_settlement_claim_id', type: 'id' },
      { name: 'invoice_date', type: 'date' },
      { name: 'invoice_start_date', type: 'date' },
      { name: 'invoice_end_date', type: 'date' },
      { name: 'invoice_due_date', type: 'date' },
      { name: 'invoice_total', type: 'amount' },
      { name: 'invoice_total_cent', type: 'amount' },
      { name: 'invoice_total_round', type: 'amount' },
      { name: 'invoice_kid_number', type: 'number' },
      { name: 'invoice_account_number', type: 'number' },
      {
        name: 'invoice_reminder_fee',
        type: 'amount',
        availability: ['isReminderInvoice']
      },
      {
        name: 'reminder_date',
        type: 'date',
        availability: ['isReminderInvoice']
      },
      {
        name: 'collection_notice_date',
        type: 'date',
        availability: ['isCollectionInvoice']
      },
      {
        name: 'invoice_items',
        type: 'items',
        subItems: [
          'item_name',
          'item_quantity',
          'item_price',
          'item_tax',
          'item_total'
        ],
        availability: [
          'isInvoice',
          'isReminderInvoice',
          'isCollectionInvoice',
          'isEviction',
          'isEvictionDocument'
        ] // This variable visible only for invoice, reminder and collection notice attachment templates
      },
      {
        name: 'collection_notice_due_date',
        type: 'date',
        availability: ['collection_notice']
      },
      { name: 'invoice_eviction_fee', type: 'amount' },
      { name: 'internal_lease_id', type: 'id' },
      { name: 'administration_eviction_notice_fee', type: 'amount' },
      { name: 'total_paid', type: 'amount' },
      { name: 'total_due', type: 'amount' },
      { name: 'is_exceed_attached_file_size', type: 'boolean' },
      {
        name: 'eviction_notice_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      {
        name: 'eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'administration_of_eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'deposit_insurance_amount',
        type: 'amount',
        availability: ['isDepositInsurancePaymentPending']
      },
      {
        name: 'deposit_amount',
        type: 'amount',
        availability: ['isDepositInsurancePaymentPending']
      },
      // App info variables for app invoice
      { name: 'app_org_name', type: 'name' },
      { name: 'app_org_id', type: 'id' },
      { name: 'app_org_address', type: 'address' },
      {
        name: 'total_overdue_rent_invoices',
        type: 'amount'
      }
    ]
  },
  {
    name: 'pending_payments.header',
    context: 'pending_payments',
    variables: [
      { name: 'user_name', type: 'name' },
      { name: 'pending_payments_url', type: 'url' },
      { name: 'payments_approval_esigning_url', type: 'url' }
    ]
  },
  {
    name: 'pending_payouts.header',
    context: 'pending_payouts',
    variables: [
      { name: 'user_name', type: 'name' },
      { name: 'pending_payouts_url', type: 'url' },
      { name: 'payouts_approval_esigning_url', type: 'url' }
    ]
  },
  {
    name: 'properties.moving_in.moving_in_esigning',
    context: 'moving_in_esigning',
    variables: [
      { name: 'rooms', type: 'rooms', subItems: ['room_name'] },
      {
        name: 'items',
        type: 'items',
        subItems: [
          'item_name',
          'has_issue',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'furniture',
        type: 'furniture',
        subItems: [
          'furniture_name',
          'furniture_quantity',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'keys',
        type: 'keys',
        subItems: ['kind_of_key', 'number_of_key']
      },
      {
        name: 'meterReading',
        type: 'meter_reading',
        subItems: [
          'number_of_meter',
          'type_of_meter',
          'measure_of_meter',
          'meter_date'
        ]
      },
      {
        name: 'inventoryImages',
        type: 'inventoryImages',
        subItems: ['image_path']
      },
      { name: 'keyImages', type: 'keyImages', subItems: ['image_path'] },
      {
        name: 'meterReadingImages',
        type: 'meterReadingImages',
        subItems: ['image_path']
      },
      { name: 'tenantPdfDescription', type: 'string' }
    ]
  },
  {
    name: 'properties.moving_out.moving_out_esigning',
    context: 'moving_out_esigning',
    variables: [
      { name: 'rooms', type: 'rooms', subItems: ['room_name'] },
      {
        name: 'items',
        type: 'items',
        subItems: [
          'item_name',
          'has_issue',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'furniture',
        type: 'furniture',
        subItems: [
          'furniture_name',
          'furniture_quantity',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'keys',
        type: 'keys',
        subItems: ['kind_of_key', 'number_of_key', 'number_of_keys_returned']
      },
      {
        name: 'meterReading',
        type: 'meter_reading',
        subItems: [
          'number_of_meter',
          'type_of_meter',
          'measure_of_meter',
          'meter_date'
        ]
      },
      {
        name: 'inventoryImages',
        type: 'inventoryImages',
        subItems: ['image_path']
      },
      { name: 'keyImages', type: 'keyImages', subItems: ['image_path'] },
      {
        name: 'meterReadingImages',
        type: 'meterReadingImages',
        subItems: ['image_path']
      },
      { name: 'isRentPaid', type: 'boolean' },
      { name: 'isRentNotPaid', type: 'boolean' },
      { name: 'rentPaidMsg', type: 'string' },
      { name: 'tenantPdfDescription', type: 'string' }
    ]
  },
  {
    name: 'rent_invoices.estimated_payouts',
    context: 'estimated_payouts',
    variables: [
      { name: 'first_estimated_payout', type: 'amount' },
      { name: 'second_estimated_payout', type: 'amount' },
      { name: 'third_estimated_payout', type: 'amount' },
      {
        name: 'first_estimated_payout_management_commission',
        type: 'amount'
      },
      {
        name: 'second_estimated_payout_management_commission',
        type: 'amount'
      },
      {
        name: 'third_estimated_payout_management_commission',
        type: 'amount'
      },
      { name: 'first_estimated_payout_addons', type: 'amount' },
      { name: 'second_estimated_payout_addons', type: 'amount' },
      { name: 'third_estimated_payout_addons', type: 'amount' },
      { name: 'estimated_brokering_commission', type: 'amount' },
      { name: 'estimated_brokering_commission_percentage', type: 'amount' },
      { name: 'estimated_management_commission_percentage', type: 'amount' },
      { name: 'first_estimated_addons_commission', type: 'amount' },
      { name: 'second_estimated_addons_commission', type: 'amount' },
      { name: 'third_estimated_addons_commission', type: 'amount' },
      { name: 'second_amount_moved_from_last_payout', type: 'amount' },
      { name: 'third_amount_moved_from_last_payout', type: 'amount' },
      { name: 'first_rent_invoice', type: 'amount' },
      { name: 'second_rent_invoice', type: 'amount' },
      { name: 'third_rent_invoice', type: 'amount' },
      { name: 'payout_to_bank_account', type: 'number' },
      { name: 'first_rent_invoice_dates', type: 'date' },
      { name: 'second_rent_invoice_dates', type: 'date' },
      { name: 'third_rent_invoice_dates', type: 'date' }
    ]
  },
  {
    name: 'templates.chat',
    context: 'chat',
    variables: [
      { name: 'sent_from_user_name', type: 'name' },
      { name: 'messages', type: 'string' },
      { name: 'reply_link', type: 'url' }
    ]
  }
]

export const queryNotificationTemplatesVariables = async () =>
  getTemplateVariables()

export const validateTemplateAddData = (data = {}) => {
  const { title } = data
  const requiredFields = ['category', 'templateType', 'title', 'type']
  appHelper.checkRequiredFields(requiredFields, data)
  if (!(size(title) && (title.no || title.en))) {
    throw new CustomError(400, 'Title can not be empty')
  }
}

export const prepareTemplateAddData = (user = {}, data = {}) => {
  const { attachments, category, content, partnerId, subject, title, type } =
    data
  const templateData = pick(data, [
    'category',
    'partnerId',
    'templateType',
    'type'
  ])
  if (user.partnerId) templateData.partnerId = user.partnerId
  templateData.createdBy = user.userId
  templateData.uniqueId = nid(17)

  if (type === 'email' && size(attachments)) {
    templateData.attachments = attachments
  }

  if (partnerId && (type === 'attachment' || type === 'pdf')) {
    templateData.isCustom = true
  }

  if (size(title)) {
    if (!title.no) title.no = title.en
    if (!title.en) title.en = title.no
    templateData.title = title
  }

  if (size(content)) {
    if (!content.no) content.no = content.en
    if (!content.en) content.en = content.no
    templateData.content = content
  }

  if (size(subject)) {
    if (!subject.no) subject.no = subject.en
    if (!subject.en) subject.en = subject.no
    templateData.subject = subject
  }

  if (type === 'pdf') {
    if (category === 'assignment') {
      templateData.isAssignmentEsigningPdf = true
    } else if (category === 'lease_contract') {
      templateData.isLeaseEsigningPdf = true
    }
  }
  return templateData
}

export const validateTemplateCloneData = (data = {}) => {
  const requiredFields = ['_id', 'partnerId']
  appHelper.checkRequiredFields(requiredFields, data)
  const { _id, partnerId } = data
  appHelper.validateId({ _id })
  appHelper.validateId({ partnerId })
}

export const prepareTemplateCloneData = async (data = {}, session) => {
  const { _id, partnerId } = data
  let query = { _id, partnerId: { $exists: false } } // Only default template can be cloned
  const template = await getNotificationTemplate({ _id }, session)
  if (!size(template)) {
    throw new CustomError(
      404,
      `Could not find any default template with _id: ${_id}`
    )
  }

  query = {
    uniqueId: template.uniqueId,
    partnerId
  }
  const partnerTemplate = await getNotificationTemplate(query, session)
  if (size(partnerTemplate)) {
    throw new CustomError(
      405,
      `Template with uniqueId: ${template.uniqueId} already exists for this partner`
    )
  }

  // Delete some fields for proper insert
  const templateData = template.toObject()
  delete templateData._id
  delete templateData.copiedBy
  delete templateData.createdAt
  delete templateData.createdBy
  templateData.partnerId = partnerId

  return templateData
}

export const validateTemplateUpdateData = (body = {}) => {
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { _id, partnerId, data = {} } = body
  appHelper.validateId({ _id })
  if (partnerId) {
    appHelper.validateId({ partnerId })
  }
  if (!size(data)) {
    throw new CustomError(400, 'Field: data can not be empty')
  }
  const { content = {} } = data
  try {
    if (content.no) {
      const template = Handlebars.compile(content.no)
      template({})
    }
    if (content.en) {
      const template = Handlebars.compile(content.en)
      template({})
    }
  } catch (err) {
    throw new CustomError(400, err?.message)
  }
}

export const prepareTemplateUpdateData = async (body = {}, session) => {
  let { _id } = body
  const { partnerId, data = {} } = body
  const { attachments, content, subject, title } = data

  const query = { _id }
  const templateInfo = await getNotificationTemplate(query, session)
  if (!size(templateInfo)) {
    throw new CustomError(404, `Could not find template with _id: ${_id}`)
  }

  let updateData = {}
  let isCopyForPartner = false

  //check partner template exists using admin template uniqueId
  if (
    partnerId &&
    (!templateInfo.partnerId || templateInfo.partnerId !== partnerId)
  ) {
    const partnerTemplateInfo = await getNotificationTemplate({
      uniqueId: templateInfo.uniqueId,
      partnerId
    })

    //if it is partner template then update template
    if (partnerTemplateInfo) {
      _id = partnerTemplateInfo._id
    } else {
      //copy admin template data and insert for partner;
      //but uniqueId will be same for rules and template relation
      isCopyForPartner = true

      //set admin template info
      // data delete data proper insert
      delete templateInfo.copiedBy
      delete templateInfo.createdAt
      delete templateInfo.createdBy

      updateData = JSON.parse(JSON.stringify(templateInfo))
      updateData.partnerId = partnerId

      if (size(data)) {
        //Partner can update template title only for custom attachment template
        if (
          templateInfo.type === 'attachment' &&
          templateInfo.isCustom &&
          title &&
          title.no
        )
          updateData.title.no = title.no
        if (
          templateInfo.type === 'attachment' &&
          templateInfo.isCustom &&
          title &&
          title.en
        )
          updateData.title.en = title.en

        if (size(content) && content.no) updateData.content.no = content.no
        if (size(content) && content.en) updateData.content.en = content.en

        if (size(subject) && subject.no) updateData.subject.no = subject.no
        if (size(subject) && subject.en) updateData.subject.en = subject.en
      }
    }
  }

  // Set attachments data
  updateData.attachments = attachments ? attachments : []
  if (!isCopyForPartner && data) {
    if (title && title.no) updateData['title.no'] = title.no
    if (title && title.en) updateData['title.en'] = title.en

    if (size(content) && content.no) updateData['content.no'] = content.no
    if (size(content) && content.en) updateData['content.en'] = content.en

    if (size(subject) && subject.no) updateData['subject.no'] = subject.no
    if (size(subject) && subject.en) updateData['subject.en'] = subject.en
  }

  return { updateData, _id, isCopyForPartner }
}

export const getTemplateVariablesForAccount = () => [
  {
    name: 'common.filters.account',
    context: 'account',
    variables: [
      { name: 'account_name', type: 'name' },
      { name: 'account_address', type: 'address' },
      { name: 'account_zip_code', type: 'zip_code' },
      { name: 'account_city', type: 'city' },
      { name: 'account_country', type: 'country' },
      { name: 'account_email', type: 'email' },
      { name: 'account_id', type: 'id' },
      { name: 'account_phonenumber', type: 'phone_number' },
      { name: 'account_person_id', type: 'NID' },
      { name: 'account_org_id', type: 'id' },
      {
        name: 'account_bank_account',
        type: 'bank_account',
        availability: ['isEvictionDocument']
      }
    ]
  }
]

export const getTemplateVariablesForAgentEsigning = () => [
  {
    name: 'common.agent_esinging',
    context: 'agent_esigning',
    variables: [{ name: 'agent_esigning_url', type: 'url' }]
  }
]
// Not Using For Notifier VariablesData
export const getTemplateVariablesForAgentMovingInEsigning = () => [
  {
    name: 'common.agent_esinging',
    context: 'agent_moving_in_esigning',
    variables: [{ name: 'agent_moving_in_esigning_url', type: 'url' }]
  }
]
// Not Using For Notifier VariablesData
export const getTemplateVariablesForAgentMovingOutEsigning = () => [
  {
    name: 'common.agent_esinging',
    context: 'agent_moving_out_esigning',
    variables: [{ name: 'agent_moving_out_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForAll = () => [
  {
    name: 'common.all',
    context: 'all',
    variables: [
      { name: 'todays_date', type: 'date' },
      { name: 'app_logo_url', type: 'url' }
    ]
  }
]

export const getTemplateVariablesForAppHealth = () => [
  {
    name: 'common.app_health',
    context: 'appHealth',
    variables: [
      { name: 'app_url', type: 'app_url' },
      {
        name: 'error_issues',
        type: 'items',
        subItems: ['errors_type', 'errors_value']
      },
      {
        name: 'new_issues',
        type: 'items',
        subItems: ['new_issue_type', 'new_issue_value']
      },
      { name: 'total_issues', type: 'number' },
      { name: 'all_issues_are_same', type: 'boolean' },
      { name: 'all_issues_are_new', type: 'boolean' }
    ]
  }
]

export const getTemplateVariablesForAssignment = () => [
  {
    name: 'common.assignment',
    context: 'assignment',
    variables: [
      { name: 'assignment_id', type: 'id' },
      { name: 'brokering_commission', type: 'amount' },
      { name: 'management_commission', type: 'amount' },
      { name: 'monthly_rent', type: 'amount' },
      { name: 'deposit_amount', type: 'amount' },
      { name: 'assignment_from', type: 'date' },
      { name: 'assignment_to', type: 'date' },
      { name: 'payout_to_bank_account', type: 'bank_account' },
      { name: 'brokering_commission_amount', type: 'amount' },
      { name: 'brokering_commission_percentage', type: 'percentage' },
      { name: 'management_commission_amount', type: 'amount' },
      { name: 'management_commission_percentage', type: 'percentage' },
      { name: 'representative_name', type: 'name' },
      { name: 'representative_occupation', type: 'occupation' },
      { name: 'representative_phone', type: 'phone_number' },
      { name: 'representative_email', type: 'email' },
      {
        name: 'assignment_addons',
        type: 'addons',
        subItems: ['addon_name', 'addon_price'],
        availability: ['isAssignmentEsigningPdf']
      }
    ]
  }
]

export const getTemplateVariablesForChat = () => [
  {
    name: 'templates.chat',
    context: 'chat',
    variables: [
      { name: 'sent_from_user_name', type: 'name' },
      { name: 'messages', type: 'string' },
      { name: 'reply_link', type: 'url' }
    ]
  }
]

export const getTemplateVariablesForContract = () => [
  {
    name: 'contract.title',
    context: 'contract',
    variables: [
      {
        name: 'lease_start_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      { name: 'lease_end_date', type: 'date' },
      { name: 'lease_id', type: 'id' },
      { name: 'monthly_rent_amount', type: 'amount' },
      { name: 'deposit_amount', type: 'amount' },
      { name: 'deposit_insurance_amount', type: 'amount' },
      { name: 'invoice_amount', type: 'amount' },
      { name: 'total_deposit_amount', type: 'amount' },
      { name: 'deposit_insurance_reference_number', type: 'number' },
      { name: 'internal_deposit_insurance_reference_number', type: 'number' },
      { name: 'minimum_stay', type: 'number' },
      { name: 'future_monthly_rent_amount', type: 'amount' },
      { name: 'internal_lease_id', type: 'id' },
      { name: 'last_CPI_date', type: 'date' },
      { name: 'next_CPI_date', type: 'date' },
      { name: 'has_future_monthly_rent_amount', type: 'amount' },
      { name: 'CPI_from_month', type: 'date' },
      { name: 'CPI_to_month', type: 'date' },
      { name: 'invoice_due_date', type: 'date' },
      {
        name: 'VAT_status',
        type: 'boolean',
        availability: ['isLeaseEsigningPdf']
      },
      {
        name: 'bank_account_number',
        type: 'bank_account',
        availability: ['isLeaseEsigningPdf']
      },
      { name: 'notice_in_effect', type: 'notice_effect' },
      { name: 'notice_period', type: 'notice_period' },
      { name: 'representative_name', type: 'name' },
      { name: 'representative_occupation', type: 'occupation' },
      { name: 'representative_phone', type: 'phone_number' },
      { name: 'representative_email', type: 'email' },
      { name: 'termination_reason', type: 'string' },
      {
        name: 'lease_addons',
        type: 'addons',
        subItems: ['addon_name', 'addon_price'],
        availability: ['isLeaseEsigningPdf']
      },
      {
        name: 'monthly_due_date',
        type: 'number',
        availability: ['isLeaseEsigningPdf']
      },
      {
        name: 'invoice_items',
        type: 'items',
        subItems: [
          'item_name',
          'item_quantity',
          'item_price',
          'item_tax',
          'item_total'
        ],
        availability: ['isEvictionDocument'] //This variable visible only for invoice, reminder and collection notice attachment templates
      },
      {
        name: 'invoice_total',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'total_due',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'eviction_notice_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      {
        name: 'total_due_rent_invoices',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'total_overdue_rent_invoices',
        type: 'amount'
      },
      {
        name: 'lease_signed_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      { name: 'hiscox_logo_url', type: 'url' },
      {
        name: 'administration_of_eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'invoice_eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'administration_eviction_notice_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      }
    ]
  }
]

export const getTemplateVariablesForCreditNote = () => [
  {
    name: 'common.credit_note',
    context: 'credit_note',
    variables: [
      { name: 'invoice_id', type: 'id' },
      { name: 'credit_invoice_id', type: 'id' },
      { name: 'credit_note_date', type: 'date' },
      { name: 'credit_note_start_date', type: 'date' },
      { name: 'credit_note_end_date', type: 'date' },
      { name: 'credit_note_due_date', type: 'date' },
      { name: 'credit_note_total', type: 'amount' },
      { name: 'credit_note_account_number', type: 'number' },
      {
        name: 'credit_note_items',
        type: 'items',
        subItems: [
          'item_name',
          'item_quantity',
          'item_price',
          'item_tax',
          'item_total'
        ],
        availability: ['isCreditNote'] //This variable visible only for invoice attachment template
      },
      {
        name: 'credit_reason',
        availability: ['isCreditNote']
      }
    ]
  }
]

export const getTemplateVariablesForCreditRating = () => [
  {
    name: 'common.credit_rating',
    context: 'credit_rating',
    variables: [{ name: 'credit_rating_ask_url', type: 'url' }]
  }
]

export const getTemplateVariablesForDeposit = () => [
  {
    name: 'common.deposit',
    context: 'deposit',
    variables: [
      { name: 'deposit_bank_account_number', type: 'bank_account' },
      { name: 'payment_amount', type: 'amount' },
      { name: 'total_payment_amount', type: 'amount' },
      { name: 'payment_reference', type: 'number' },
      { name: 'tenant_deposit_amount', type: 'number' },
      { name: 'current_balance', type: 'amount' },
      { name: 'payment_date', type: 'date' }
    ]
  }
]

export const getTemplateVariablesForDownload = () => [
  {
    name: 'common.download',
    context: 'download',
    variables: [
      { name: 'download_context', type: 'context' },
      { name: 'download_url', type: 'url' },
      { name: 'has_download_url', type: 'boolean' }
    ]
  }
]

export const getTemplateVariablesForEstimatedPayouts = () => [
  {
    name: 'rent_invoices.estimated_payouts',
    context: 'estimated_payouts',
    variables: [
      { name: 'first_estimated_payout', type: 'amount' },
      { name: 'second_estimated_payout', type: 'amount' },
      { name: 'third_estimated_payout', type: 'amount' },
      {
        name: 'first_estimated_payout_management_commission',
        type: 'amount'
      },
      {
        name: 'second_estimated_payout_management_commission',
        type: 'amount'
      },
      {
        name: 'third_estimated_payout_management_commission',
        type: 'amount'
      },
      { name: 'first_estimated_payout_addons', type: 'amount' },
      { name: 'second_estimated_payout_addons', type: 'amount' },
      { name: 'third_estimated_payout_addons', type: 'amount' },
      { name: 'estimated_brokering_commission', type: 'amount' },
      { name: 'estimated_brokering_commission_percentage', type: 'amount' },
      { name: 'estimated_management_commission_percentage', type: 'amount' },
      { name: 'first_estimated_addons_commission', type: 'amount' },
      { name: 'second_estimated_addons_commission', type: 'amount' },
      { name: 'third_estimated_addons_commission', type: 'amount' },
      { name: 'second_amount_moved_from_last_payout', type: 'amount' },
      { name: 'third_amount_moved_from_last_payout', type: 'amount' },
      { name: 'first_rent_invoice', type: 'amount' },
      { name: 'second_rent_invoice', type: 'amount' },
      { name: 'third_rent_invoice', type: 'amount' },
      { name: 'payout_to_bank_account', type: 'number' },
      { name: 'first_rent_invoice_dates', type: 'date' },
      { name: 'second_rent_invoice_dates', type: 'date' },
      { name: 'third_rent_invoice_dates', type: 'date' }
    ]
  }
]

export const getTemplateVariablesForInvoiceOrAppInvoice = () => [
  {
    name: 'payments.labels.invoice',
    context: 'invoice' || 'app_invoice',
    variables: [
      { name: 'invoice_id', type: 'id' },
      { name: 'final_settlement_claim_id', type: 'id' },
      { name: 'invoice_date', type: 'date' },
      { name: 'invoice_start_date', type: 'date' },
      { name: 'invoice_end_date', type: 'date' },
      { name: 'invoice_due_date', type: 'date' },
      { name: 'invoice_total', type: 'amount' },
      { name: 'invoice_total_cent', type: 'amount' },
      { name: 'invoice_total_round', type: 'amount' },
      { name: 'invoice_kid_number', type: 'number' },
      { name: 'invoice_account_number', type: 'number' },
      {
        name: 'invoice_reminder_fee',
        type: 'amount',
        availability: ['isReminderInvoice']
      },
      {
        name: 'reminder_date',
        type: 'date',
        availability: ['isReminderInvoice']
      },
      {
        name: 'collection_notice_date',
        type: 'date',
        availability: ['isCollectionInvoice']
      },
      {
        name: 'invoice_items',
        type: 'items',
        subItems: [
          'item_name',
          'item_quantity',
          'item_price',
          'item_tax',
          'item_total'
        ],
        availability: [
          'isInvoice',
          'isReminderInvoice',
          'isCollectionInvoice',
          'isEviction',
          'isEvictionDocument'
        ] //This variable visible only for invoice, reminder and collection notice attachment templates
      },
      {
        name: 'collection_notice_due_date',
        type: 'date',
        availability: ['collection_notice']
      },
      { name: 'invoice_eviction_fee', type: 'amount' },
      { name: 'internal_lease_id', type: 'id' },
      { name: 'administration_eviction_notice_fee', type: 'amount' },
      { name: 'total_paid', type: 'amount' },
      { name: 'total_due', type: 'amount' },
      { name: 'is_exceed_attached_file_size', type: 'boolean' },
      {
        name: 'eviction_notice_date',
        type: 'date',
        availability: ['isEvictionDocument']
      },
      {
        name: 'eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'administration_of_eviction_fee',
        type: 'amount',
        availability: ['isEvictionDocument']
      },
      {
        name: 'deposit_insurance_amount',
        type: 'amount',
        availability: ['isDepositInsurancePaymentPending']
      },
      {
        name: 'deposit_amount',
        type: 'amount',
        availability: ['isDepositInsurancePaymentPending']
      },
      // App info variables for app invoice
      { name: 'app_org_name', type: 'name' },
      { name: 'app_org_id', type: 'id' },
      { name: 'app_org_address', type: 'address' },
      {
        name: 'total_overdue_rent_invoices',
        type: 'amount'
      }
    ]
  }
]

export const getTemplateVariablesForLandlordAnnualStatement = () => [
  {
    name: 'annual_statement.title',
    context: 'landlord_annual_statement',
    variables: [
      { name: 'report_year', type: 'year' },
      {
        name: 'annual_statement_items',
        type: 'annual_statement_items',
        subItems: [
          'report_year',
          'rent_total_excl_tax',
          'rent_total_tax',
          'rent_total',
          'commission_total_amount',
          'commission_total_vat',
          'commission_total',
          'total_payouts'
        ]
      }
    ]
  }
]

export const getTemplateVariablesForLandlordEsigning = () => [
  {
    name: 'common.landlord_esigning',
    context: 'landlord_esigning',
    variables: [{ name: 'landlord_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForLandlordLeaseEsigning = () => [
  {
    name: 'common.landlord_lease_esigning',
    context: 'landlord_lease_esigning',
    variables: [{ name: 'landlord_lease_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForLandlordMovingInEsigning = () => [
  {
    name: 'common.landlord_esigning',
    context: 'landlord_moving_in_esigning',
    variables: [{ name: 'landlord_moving_in_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForLandlordMovingOutEsigning = () => [
  {
    name: 'common.landlord_esigning',
    context: 'landlord_moving_out_esigning',
    variables: [{ name: 'landlord_moving_out_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForListing = () => [
  {
    name: 'common.listing',
    context: 'listing',
    variables: [
      { name: 'has_listing' },
      { name: 'listing_location', type: 'address' },
      { name: 'listing_type_and_property_name', type: 'name' },
      { name: 'listing_image_url', type: 'url' },
      { name: 'listing_availability', type: 'date' },
      { name: 'listing_minimum_stay', type: 'number' }
    ]
  }
]

export const getTemplateVariablesForMovingInEsigning = () => [
  {
    name: 'properties.moving_in.moving_in_esigning',
    context: 'moving_in_esigning',
    variables: [
      { name: 'rooms', type: 'rooms', subItems: ['room_name'] },
      {
        name: 'items',
        type: 'items',
        subItems: [
          'item_name',
          'has_issue',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'furniture',
        type: 'furniture',
        subItems: [
          'furniture_name',
          'furniture_quantity',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'keys',
        type: 'keys',
        subItems: ['kind_of_key', 'number_of_key']
      },
      {
        name: 'meterReading',
        type: 'meter_reading',
        subItems: [
          'number_of_meter',
          'type_of_meter',
          'measure_of_meter',
          'meter_date'
        ]
      },
      {
        name: 'inventoryImages',
        type: 'inventoryImages',
        subItems: ['image_path']
      },
      { name: 'keyImages', type: 'keyImages', subItems: ['image_path'] },
      {
        name: 'meterReadingImages',
        type: 'meterReadingImages',
        subItems: ['image_path']
      },
      { name: 'tenantPdfDescription', type: 'string' }
    ]
  }
]

export const getTemplateVariablesForMovingOutEsigning = () => [
  {
    name: 'properties.moving_out.moving_out_esigning',
    context: 'moving_out_esigning',
    variables: [
      { name: 'rooms', type: 'rooms', subItems: ['room_name'] },
      {
        name: 'items',
        type: 'items',
        subItems: [
          'item_name',
          'has_issue',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'furniture',
        type: 'furniture',
        subItems: [
          'furniture_name',
          'furniture_quantity',
          'responsible_for_fixing',
          'issue_description'
        ]
      },
      {
        name: 'keys',
        type: 'keys',
        subItems: ['kind_of_key', 'number_of_key', 'number_of_keys_returned']
      },
      {
        name: 'meterReading',
        type: 'meter_reading',
        subItems: [
          'number_of_meter',
          'type_of_meter',
          'measure_of_meter',
          'meter_date'
        ]
      },
      {
        name: 'inventoryImages',
        type: 'inventoryImages',
        subItems: ['image_path']
      },
      { name: 'keyImages', type: 'keyImages', subItems: ['image_path'] },
      {
        name: 'meterReadingImages',
        type: 'meterReadingImages',
        subItems: ['image_path']
      },
      { name: 'tenantPdfDescription', type: 'string' },
      { name: 'isRentPaid', type: 'boolean' },
      { name: 'isRentNotPaid', type: 'boolean' },
      { name: 'rentPaidMsg', type: 'string' }
    ]
  }
]

export const getTemplateVariablesForPartner = () => [
  {
    name: 'common.partner',
    context: 'partner',
    variables: [
      { name: 'partner_name', type: 'name' },
      { name: 'partner_logo_url', type: 'url' },
      { name: 'partner_id', type: 'id' },
      { name: 'partner_address', type: 'address' },
      { name: 'partner_zip_code', type: 'zip_code' },
      { name: 'partner_city', type: 'city' },
      { name: 'partner_country', type: 'country' },
      { name: 'partner_org_id', type: 'id' },
      { name: 'branch_name', type: 'branch_name' },
      { name: 'agent_name', type: 'name' },
      { name: 'agent_email', type: 'email' },
      { name: 'agent_phonenumber', type: 'phone_number' },
      { name: 'agent_occupation', type: 'job_title' },
      { name: 'manager_name', type: 'name' },
      { name: 'partner_url', type: 'partner_url' },
      {
        name: 'partner_bank_account',
        type: 'bank_account',
        availability: ['isEvictionDocument']
      }
    ]
  }
]

export const getTemplateVariablesForPayout = () => [
  {
    name: 'common.payout',
    context: 'payout',
    variables: [
      { name: 'payout_total', type: 'amount' },
      { name: 'brokering_commission_amount', type: 'amount' },
      { name: 'management_commission_amount', type: 'amount' },
      { name: 'payout_addons', type: 'amount' },
      { name: 'payout_corrections', type: 'amount' },
      { name: 'payout_paid_by_bank_date', type: 'date' },
      { name: 'payout_to_bank_account', type: 'number' },
      { name: 'payout_from_bank_account', type: 'number' },
      { name: 'last_unpaid_payouts', type: 'amount' },
      { name: 'addon_total', type: 'amount' },
      {
        name: 'addons',
        type: 'addons',
        subItems: [
          'addon_name',
          'addon_price',
          'addon_tax_percentage',
          'addon_description'
        ]
      }
    ]
  }
]

export const getTemplateVariablesForPendingPayments = () => [
  {
    name: 'pending_payments.header',
    context: 'pending_payments',
    variables: [
      { name: 'user_name', type: 'name' },
      { name: 'pending_payments_url', type: 'url' },
      { name: 'payments_approval_esigning_url', type: 'url' }
    ]
  }
]

export const getTemplateVariablesForPendingPayouts = () => [
  {
    name: 'pending_payouts.header',
    context: 'pending_payouts',
    variables: [
      { name: 'user_name', type: 'name' },
      { name: 'pending_payouts_url', type: 'url' },
      { name: 'payouts_approval_esigning_url', type: 'url' }
    ]
  }
]

export const getTemplateVariablesForProperty = () => [
  {
    name: 'common.filters.property',
    context: 'property',
    variables: [
      { name: 'property_location', type: 'location_name' },
      { name: 'property_zip_code', type: 'zip_code' },
      { name: 'property_city', type: 'city' },
      { name: 'property_country', type: 'country' },
      { name: 'property_id', type: 'id' },
      { name: 'property_gnr', type: 'string' },
      { name: 'property_bnr', type: 'string' },
      { name: 'property_snr', type: 'string' },
      { name: 'property_number_of_bedrooms', type: 'number' },
      { name: 'property_livingroom_yes_or_no', type: 'boolean' },
      { name: 'property_kitchen_yes_or_no', type: 'boolean' },
      { name: 'property_furnished_yes_or_no', type: 'boolean' },
      { name: 'property_municipality', type: 'address' },
      { name: 'apartment_id', type: 'id' },
      {
        name: 'interestform_url',
        type: 'url'
        //availability: ["isInterestForm"]
      }
    ]
  }
]

export const getTemplateVariablesForTask = () => [
  {
    name: 'common.tasks',
    context: 'task',
    variables: [
      { name: 'assignee_name', type: 'name' },
      { name: 'assigned_by', type: 'name' },
      { name: 'task_title', type: 'task_title' },
      { name: 'comment', type: 'comment' },
      { name: 'task_url', type: 'url' }
    ]
  }
]

export const getTemplateVariablesForTenant = () => [
  {
    name: 'common.tenant',
    context: 'tenant',
    variables: [
      { name: 'tenant_name', type: 'name' },
      { name: 'tenant_id', type: 'id' },
      { name: 'tenant_serial_id', type: 'id' },
      { name: 'tenant_address', type: 'address' },
      { name: 'tenant_zip_code', type: 'zip_code' },
      { name: 'tenant_city', type: 'city' },
      { name: 'tenant_country', type: 'country' },
      { name: 'tenant_email', type: 'email' },
      { name: 'tenant_phonenumber', type: 'phone_number' },
      { name: 'tenant_person_id', type: 'NID' },
      {
        name: 'tenants',
        type: 'tenants',
        subItems: [
          'tenant_name',
          'tenant_id',
          'tenant_address',
          'tenant_zip_code',
          'tenant_city',
          'tenant_country',
          'tenant_email',
          'tenant_phonenumber',
          'tenant_person_id'
        ],
        availability: ['isLeaseEsigningPdf', 'isDepositInsurancePaymentPending']
      },
      {
        name: 'new_password_url',
        type: 'url',
        availability: ['isWelcomeLease']
      },
      { name: 'jointly_liable_tenant_name', type: 'name' },
      { name: 'jointly_liable_tenant_names', type: 'name' },
      { name: 'jointly_liable_tenant_email', type: 'email' },
      { name: 'jointly_liable_tenant_emails', type: 'email' },
      { name: 'jointly_liable_tenant_person_IDs', type: 'NID' }
    ]
  }
]

export const getTemplateVariablesForTenantLeaseEsigning = () => [
  {
    name: 'common.tenant_esigning',
    context: 'tenant_lease_esigning',
    variables: [{ name: 'tenant_lease_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForTenantMovingInEsigning = () => [
  {
    name: 'common.tenant_esigning',
    context: 'tenant_moving_in_esigning',
    variables: [{ name: 'tenant_moving_in_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForTenantMovingOutEsigning = () => [
  {
    name: 'common.tenant_esigning',
    context: 'tenant_moving_out_esigning',
    variables: [{ name: 'tenant_moving_out_esigning_url', type: 'url' }]
  }
]

export const getTemplateVariablesForUser = () => [
  {
    name: 'common.user',
    context: 'user',
    variables: [
      { name: 'has_password', type: 'boolean' },
      { name: 'user_invitation_url', type: 'url' }
    ]
  }
]

export const getESigningTemplateContent = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)

  const { query } = body
  appHelper.checkRequiredFields(['context', 'partnerId'], query)

  const { accountId, context, partnerId } = query

  if (
    !(
      context === 'assignment' ||
      context === 'lease' ||
      context === 'deposit_insurance'
    )
  )
    throw new CustomError(400, 'Wrong context!')

  let userLang = 'no'
  if (accountId) {
    const accountInfo =
      (await accountHelper.getAnAccount({ _id: accountId }, session, [
        'person'
      ])) || {}
    const { person } = accountInfo
    const { profile } = person || {}
    const { language } = profile || {}
    if (language) userLang = language
  }

  const templateQuery = {}

  if (context === 'assignment') templateQuery.isAssignmentEsigningPdf = true
  else if (context === 'lease') templateQuery.isLeaseEsigningPdf = true
  else if (context === 'deposit_insurance')
    templateQuery.isDepositInsurancePdf = true

  let notificationTemplateInfo = {}
  templateQuery.partnerId = partnerId
  notificationTemplateInfo = await getNotificationTemplate(
    templateQuery,
    session
  )

  if (!size(notificationTemplateInfo)) {
    templateQuery.partnerId = { $exists: false }
    notificationTemplateInfo = await getNotificationTemplate(
      templateQuery,
      session
    )
  }

  if (size(notificationTemplateInfo)) {
    const { content = {} } = notificationTemplateInfo
    return content[userLang]
  } else throw new CustomError(404, 'Could not find e-signing template!')
}

const prepareLeaseTemplateDropdownQuery = (params) => {
  const { query, partnerId } = params
  const { category, searchPattern } = query

  let prepareQuery = {}

  if (category) prepareQuery.category = category

  if (searchPattern) {
    prepareQuery['$or'] = [
      {
        partnerId,
        $or: [
          {
            'title.en': { $regex: searchPattern, $options: 'i' }
          },
          { 'title.no': { $regex: searchPattern, $options: 'i' } }
        ]
      },
      {
        partnerId: { $exists: false },
        copiedBy: { $nin: [partnerId] },
        $or: [
          { 'title.en': { $regex: searchPattern, $options: 'i' } },
          { 'title.no': { $regex: searchPattern, $options: 'i' } }
        ],
        templateType: { $ne: 'app' }
      }
    ]
  } else {
    prepareQuery['$or'] = [
      {
        partnerId
      },
      {
        partnerId: { $exists: false },
        copiedBy: { $nin: [partnerId] },
        templateType: { $ne: 'app' }
      }
    ]
  }

  if (category === 'assignment') {
    prepareQuery.type = 'pdf'
    prepareQuery = {
      $or: [prepareQuery, { isAssignmentEsigningPdf: true, partnerId }]
    }
  }

  return prepareQuery
}

const getLeaseTemplates = async (query, userId, options) => {
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
    {
      $project: {
        _id: 1,
        title: 1,
        type: 1,
        templateType: 1,
        uniqueId: 1,
        category: 1
      }
    }
  ]
  const notificationTemplates = await NotificationTemplateCollection.aggregate(
    pipeline
  )
  const dropdownData = []
  if (size(notificationTemplates)) {
    const userInfo = await userHelper.getAnUser({ _id: userId })
    const lang = userInfo?.profile?.language || 'no'
    for (const template of notificationTemplates) {
      dropdownData.push({
        _id: template._id,
        text: template.title?.[lang]
      })
    }
  }

  return dropdownData
}

export const getLeaseTemplateDropdown = async (req) => {
  const { body } = req
  appHelper.validatePartnerAppRequestData(req, [])
  const { userId, options } = body
  const preparedQuery = prepareLeaseTemplateDropdownQuery(body)
  const notificationTemplates = await getLeaseTemplates(
    preparedQuery,
    userId,
    options
  )
  return { dropdownData: notificationTemplates }
}

const getAccountData = (accountInfo, field) => {
  const { type, person } = accountInfo
  let value = ''

  if (type === 'person') {
    if (field === 'address') value = person?.profile?.hometown || ''
    else value = person?.profile?.[field] || ''
  } else value = accountInfo[field]

  return value
}

const getContractAddons = async (addons = [], type = '') => {
  const addonsInfo = []

  for (const addon of addons) {
    if (addon.type === type) {
      const addonInfo = await addonHelper.getAddon({ _id: addon._id })
      if (size(addonInfo) && addonInfo.name) {
        addonsInfo.push({
          addon_name: addonInfo.name,
          addon_price: addonInfo.total
        })
      }
    }
  }

  return addonsInfo
}

const getLeaseId = async (contractData) => {
  const { contractId, partnerId } = contractData
  const contractPipeline = [
    {
      $match: {
        _id: contractId,
        partnerId
      }
    },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
        foreignField: '_id',
        as: 'tenantInfo'
      }
    },
    appHelper.getUnwindPipeline('tenantInfo'),
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'propertyInfo'
      }
    },
    appHelper.getUnwindPipeline('propertyInfo')
  ]
  const [contractInfo = {}] =
    (await ContractCollection.aggregate(contractPipeline)) || []
  const { propertyInfo, tenantInfo } = contractInfo

  const leaseSerial = contractInfo?.leaseSerial
    ? contractInfo.leaseSerial + 1
    : 1
  const tenantSerial = tenantInfo?.serial || ''
  const propertySerial = propertyInfo?.serial || ''

  let leaseNumber = ''

  if (tenantSerial && propertySerial && leaseSerial) {
    leaseNumber =
      appHelper.getFixedDigits(propertySerial, 5) +
      appHelper.getFixedDigits(tenantSerial, 4) +
      appHelper.getFixedDigits(leaseSerial, 3)
    if (leaseNumber) return `# ${leaseNumber}`
  }

  return `# ${leaseSerial}`
}

const getFirstMonthAccountNumber = async (
  accountInfo = {},
  partnerInfo = {}
) => {
  const { accountType, partnerSettingsInfo } = partnerInfo
  if (accountType === 'broker')
    return partnerSettingsInfo?.bankPayment?.firstMonthACNo || ''
  else return accountInfo?.invoiceAccountNumber || ''
}

const groupTenantsVariableData = (propertyInfo) => [
  {
    $group: {
      _id: null,
      tenants: {
        $push: {
          tenant_name: '$name',
          tenant_id: '$serial',
          tenant_address: {
            $cond: [
              { $ifNull: ['$billingAddress', false] },
              '$billingAddress',
              propertyInfo?.location?.name
            ]
          },
          tenant_zip_code: {
            $cond: {
              if: {
                $and: [
                  { $ifNull: ['$billingAddress', false] },
                  { $ifNull: ['$zipCode', false] }
                ]
              },
              then: '$zipCode',
              else: propertyInfo?.location?.postalCode
            }
          },
          tenant_city: {
            $cond: {
              if: {
                $and: [
                  { $ifNull: ['$billingAddress', false] },
                  { $ifNull: ['$city', false] }
                ]
              },
              then: '$city',
              else: propertyInfo?.location?.city
            }
          },
          tenant_country: {
            $cond: {
              if: {
                $and: [
                  { $ifNull: ['$billingAddress', false] },
                  { $ifNull: ['$country', false] }
                ]
              },
              then: '$country',
              else: propertyInfo?.location?.country
            }
          },
          tenant_email: {
            $cond: [
              { $ifNull: ['$userDetails.email', false] },
              '$userDetails.email',
              ''
            ]
          },
          tenant_phonenumber: {
            $cond: [
              { $ifNull: ['$userDetails.profile.phoneNumber', false] },
              '$userDetails.profile.phoneNumber',
              ''
            ]
          },
          tenant_person_id: {
            $cond: [
              {
                $ifNull: [
                  '$userDetails.profile.norwegianNationalIdentification',
                  false
                ]
              },
              '$userDetails.profile.norwegianNationalIdentification',
              ''
            ]
          }
        }
      }
    }
  }
]

const getLeaseTenantInfo = async (propertyInfo, tenantIds) => {
  if (size(tenantIds)) {
    const tenantsPipeline = [
      {
        $match: {
          _id: { $in: tenantIds }
        }
      },
      {
        $lookup: {
          from: 'users',
          localField: 'userId',
          foreignField: '_id',
          pipeline: [...appHelper.getUserEmailPipeline()],
          as: 'userDetails'
        }
      },
      appHelper.getUnwindPipeline('userDetails'),
      ...groupTenantsVariableData(propertyInfo)
    ]

    const [tenantsData = {}] =
      (await TenantCollection.aggregate(tenantsPipeline)) || []

    return tenantsData?.tenants || []
  }

  return []
}

const getAssignmentId = async ({ partnerId, propertyId }) => {
  if (!partnerId || !propertyId) return ''

  const propertyContracts = await contractHelper.countContracts({
    partnerId,
    propertyId
  })
  const assignmentSerial = propertyContracts + 1

  const partnerInfo = await partnerHelper.getAPartner({ _id: partnerId })
  const partnerSerial = partnerInfo?.serial || ''
  const propertyInfo = await listingHelper.getAListing({ _id: propertyId })
  const propertySerial = propertyInfo?.serial || ''

  if (!assignmentSerial || !partnerSerial || !propertySerial) return ''
  const assignmentNumber =
    appHelper.getFixedDigits(partnerSerial, 4) +
    appHelper.getFixedDigits(propertySerial, 5) +
    appHelper.getFixedDigits(assignmentSerial, 3)
  return `#${assignmentNumber}`
}

const getVariablesByContractFrom = async (
  contractData,
  contractType,
  params
) => {
  let variables = {}
  const { contractId, invoiceId, userLang } = params
  const {
    accountId,
    addons,
    agentId,
    branchId,
    depositAmount,
    dueDate,
    evictionCases,
    firstInvoiceDueDate,
    internalLeaseId,
    isVatEnable,
    language,
    lastCpiDate,
    leaseStartDate,
    leaseEndDate,
    minimumStay,
    monthlyRentAmount,
    nextCpiDate,
    newTenantName,
    noticeInEffect,
    noticePeriod,
    partnerId,
    propertyId,
    tenantId,
    representativeId,
    tenants,
    enabledJointlyLiable
  } = contractData

  if (size(contractData)) {
    variables.brokering_commission = contractData.brokeringCommissionAmount || 0
    variables.management_commission =
      contractData.rentalManagementCommissionAmount || 0
    variables.monthly_rent = contractData?.listingInfo?.monthlyRentAmount || 0
    variables.deposit_amount = contractData?.listingInfo?.depositAmount || 0
    variables.notice_in_effect = size(contractData?.rentalMeta?.noticeInEffect)
      ? appHelper.translateToUserLng(
          'properties.' + contractData.rentalMeta.noticeInEffect,
          'no'
        )
      : ''
    variables.notice_period = size(contractData?.rentalMeta?.noticePeriod)
      ? contractData.rentalMeta.noticePeriod +
        ' ' +
        appHelper.translateToUserLng('properties.months', 'no')
      : ''
    variables.brokering_commission_amount =
      notificationLogHelper.getBrokeringCommissionAmount(contractData)
    variables.brokering_commission_percentage =
      contractData.brokeringCommissionType === 'percent'
        ? contractData.brokeringCommissionAmount
        : 0
    variables.management_commission_amount =
      notificationLogHelper.getManagementCommissionAmount(contractData)
    variables.management_commission_percentage =
      contractData.rentalManagementCommissionType === 'percent'
        ? contractData.rentalManagementCommissionAmount
        : 0
    variables.assignment_addons = await getContractAddons(
      contractData.addons,
      'assignment'
    )
    variables.assignment_id = await getAssignmentId({ propertyId, partnerId })
    variables.assignment_from = contractData.assignmentFrom
      ? (
          await appHelper.getActualDate(
            contractData.partnerId,
            true,
            contractData.assignmentFrom
          )
        ).format(await appHelper.getDateFormat(contractData.partnerId))
      : ''
    variables.assignment_to = contractData.assignmentTo
      ? (
          await appHelper.getActualDate(
            contractData.partnerId,
            true,
            contractData.assignmentTo
          )
        ).format(await appHelper.getDateFormat(contractData.partnerId))
      : ''
    variables.payout_to_bank_account = contractData.payoutTo || ''
    variables.todays_date = (
      await appHelper.getActualDate(contractData.partnerId, true, new Date())
    ).format(await appHelper.getDateFormat(contractData.partnerId))
  }

  let accountInfo = {}
  let partnerInfo = {}
  let propertyInfo = {}
  let tenantInfo = {}

  if (accountId) {
    const accountPipeline = [
      {
        $match: {
          _id: accountId
        }
      },
      {
        $lookup: {
          from: 'users',
          localField: 'personId',
          foreignField: '_id',
          pipeline: [...appHelper.getUserEmailPipeline()],
          as: 'person'
        }
      },
      appHelper.getUnwindPipeline('person'),
      {
        $lookup: {
          from: 'organizations',
          localField: 'organizationId',
          foreignField: '_id',
          as: 'organization'
        }
      },
      appHelper.getUnwindPipeline('organization')
    ]
    const [accountData = {}] =
      (await AccountCollection.aggregate(accountPipeline)) || []
    accountInfo = accountData

    const { name, person } = accountInfo
    variables.account_name = name
    variables.account_address = getAccountData(accountInfo, 'address')
    variables.account_zip_code = getAccountData(accountInfo, 'zipCode')
    variables.account_city = getAccountData(accountInfo, 'city')
    variables.account_country = getAccountData(accountInfo, 'country')
    variables.account_person_id =
      accountInfo?.person?.profile.norwegianNationalIdentification || ''
    variables.account_email = person?.email || ''
    variables.account_phonenumber =
      accountInfo?.person?.profile.phoneNumber || ''
    variables.account_id = accountInfo?.serial ? `#${accountInfo.serial}` : ''
    variables.account_org_id = accountInfo?.organization?.orgId || ''
  }

  if (tenantId) {
    const tenantPipeline = [
      {
        $match: {
          _id: tenantId
        }
      },
      {
        $lookup: {
          from: 'users',
          localField: 'userId',
          foreignField: '_id',
          pipeline: [...appHelper.getUserEmailPipeline()],
          as: 'userInfo'
        }
      },
      appHelper.getUnwindPipeline('userInfo'),
      {
        $lookup: {
          from: 'listings',
          localField: 'propertyId',
          foreignField: '_id',
          as: 'propertyInfo'
        }
      },
      appHelper.getUnwindPipeline('propertyInfo')
    ]
    const [tenantData = {}] =
      (await TenantCollection.aggregate(tenantPipeline)) || []

    tenantInfo = tenantData

    const { userInfo } = tenantInfo
    variables.tenant_person_id =
      userInfo?.profile?.norwegianNationalIdentification || ''
    variables.tenant_email = userInfo?.email || ''
    variables.tenant_phonenumber = userInfo?.profile?.phoneNumber || ''
    variables.jointly_liable_tenant_name = tenantInfo.name
    variables.jointly_liable_tenant_email = userInfo?.email || ''
  }
  if (size(tenants) && enabledJointlyLiable) {
    const tenantIds = map(tenants, 'tenantId') || []
    console.log('Found tenantIds: ', tenantIds)
    variables.jointly_liable_tenant_names =
      await notificationLogHelper.getTenantsNameOrEmailOrPersonId(
        tenantIds,
        'jointly_liable_tenant_names'
      )
    variables.jointly_liable_tenant_emails =
      await notificationLogHelper.getTenantsNameOrEmailOrPersonId(
        tenantIds,
        'jointly_liable_tenant_emails'
      )
    variables.jointly_liable_tenant_person_IDs =
      await notificationLogHelper.getTenantsNameOrEmailOrPersonId(
        tenantIds,
        'jointly_liable_tenant_person_IDs'
      )
  }

  if (propertyId) {
    const propertyPipeline = [
      {
        $match: {
          _id: propertyId
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
      appHelper.getUnwindPipeline('addonInfo'),
      {
        $group: {
          _id: null,
          propertyId: {
            $first: '$_id'
          },
          addonsInfo: {
            $push: {
              addon_name: { $ifNull: ['$addonInfo.name', ''] },
              addon_price: { $ifNull: ['$addonInfo.price', 0] }
            }
          },
          location: {
            $first: '$location'
          },
          apartmentId: {
            $first: '$apartmentId'
          },
          gnr: {
            $first: '$gnr'
          },
          bnr: {
            $first: '$bnr'
          },
          snr: {
            $first: '$snr'
          },
          noOfBedrooms: {
            $first: '$noOfBedrooms'
          },
          livingRoom: {
            $first: '$livingRoom'
          },
          kitchen: {
            $first: '$kitchen'
          },
          livingRoomFurnished: {
            $first: '$livingRoomFurnished'
          }
        }
      }
    ]

    const [propertyData = {}] =
      (await ListingCollection.aggregate(propertyPipeline)) || []
    propertyInfo = propertyData

    variables.property_id = propertyInfo?.propertyId || ''
    variables.property_location = propertyInfo?.location?.name || ''
    variables.property_zip_code = propertyInfo?.location?.postalCode || ''
    variables.property_city = propertyInfo?.location?.city || ''
    variables.property_Country = propertyInfo?.location?.country || ''
    variables.property_municipality = propertyInfo?.location?.city
      ? propertyInfo.location.sublocality
        ? `${propertyInfo.location.city} ${propertyInfo.location.sublocality}`
        : propertyInfo.location.city
      : ''
    variables.property_addons = propertyInfo?.addonsInfo || []
    variables.apartment_id = propertyInfo?.apartmentId || ''
    variables.property_gnr = propertyInfo?.gnr || ''
    variables.property_bnr = propertyInfo?.bnr || ''
    variables.property_snr = propertyInfo?.snr || ''
    variables.property_number_of_bedrooms = propertyInfo?.noOfBedrooms || ''
    variables.property_livingroom_yes_or_no = propertyInfo?.livingRoom
      ? 'Yes'
      : 'No'
    variables.property_kitchen_yes_or_no = propertyInfo?.kitchen ? 'Yes' : 'No'
    variables.property_furnished_yes_or_no = propertyInfo?.livingRoomFurnished
      ? 'Yes'
      : 'No'
  }

  if (representativeId) {
    const representativePipeline = [
      {
        $match: {
          _id: representativeId
        }
      },
      ...appHelper.getUserEmailPipeline()
    ]
    const [representativeInfo = {}] =
      (await UserCollection.aggregate(representativePipeline)) || []

    variables.representative_name = representativeInfo?.profile?.name || ''

    // work not exists in schema
    variables.representative_occupation = representativeInfo?.profile
      ?.occupation
      ? representativeInfo.profile.occupation
      : representativeInfo?.profile?.work?.length > 0
      ? representativeInfo.profile.work[0].position?.name
      : ''

    variables.representative_phone =
      representativeInfo?.profile?.phoneNumber || ''
    variables.representative_email = representativeInfo?.email || ''
  }

  if (agentId) {
    const agentPipeline = [
      {
        $match: {
          _id: agentId
        }
      },
      ...appHelper.getUserEmailPipeline()
    ]
    const [agentInfo = {}] =
      (await UserCollection.aggregate(agentPipeline)) || []

    variables.agent_name = agentInfo?.profile?.name
    variables.manager_name = agentInfo?.profile?.name
    variables.agent_email = agentInfo?.email || ''
    variables.agent_phonenumber = agentInfo?.profile?.phoneNumber
    variables.agent_occupation = agentInfo?.profile?.occupation
      ? agentInfo.profile.occupation
      : agentInfo?.profile?.work?.length > 0
      ? agentInfo.profile.work[0].position?.name
      : ''
  }

  if (branchId) {
    const branchInfo = (await branchHelper.getABranch({ _id: branchId })) || {}

    variables.branch_name = branchInfo?.name || ''
  }

  if (partnerId) {
    const partnerPipeline = [
      {
        $match: {
          _id: partnerId
        }
      },
      {
        $lookup: {
          from: 'partner_settings',
          localField: '_id',
          foreignField: 'partnerId',
          as: 'partnerSettingsInfo'
        }
      },
      appHelper.getUnwindPipeline('partnerSettingsInfo')
    ]

    const [partnerData = {}] =
      (await PartnerCollection.aggregate(partnerPipeline)) || []
    partnerInfo = partnerData

    variables.partner_id = `#${partnerInfo?.serial}` || ''
    variables.partner_name = partnerInfo?.name
    variables.partner_address =
      partnerInfo?.partnerSettingsInfo?.companyInfo?.postalAddress || ''
    variables.partner_zip_code =
      partnerInfo?.partnerSettingsInfo?.companyInfo?.postalZipCode || ''
    variables.partner_city =
      partnerInfo?.partnerSettingsInfo?.companyInfo?.postalCity || ''
    variables.partner_country =
      partnerInfo?.partnerSettingsInfo?.companyInfo?.postalCountry || ''
    variables.partner_org_id =
      partnerInfo?.partnerSettingsInfo?.companyInfo?.organizationId || ''

    if (partnerInfo?.accountType === 'direct') {
      variables.partner_logo_url = accountInfo?.organization
        ? appHelper.getOrgLogo(accountInfo.organization)
        : ''
    } else {
      variables.partner_logo_url = appHelper.getPartnerLogo(partnerInfo) || ''
    }
    variables.hiscox_logo_url = appHelper.getDefaultLogoURL('hiscox-logo')
  }

  if (contractType === 'lease') {
    const { partnerSettingsInfo } = partnerInfo
    if (leaseStartDate)
      variables.lease_start_date = (
        await appHelper.getActualDate(partnerSettingsInfo, true, leaseStartDate)
      ).format(await appHelper.getDateFormat(partnerSettingsInfo))

    if (leaseEndDate)
      variables.lease_end_date = (
        await appHelper.getActualDate(partnerSettingsInfo, true, leaseEndDate)
      ).format(await appHelper.getDateFormat(partnerSettingsInfo))

    if (monthlyRentAmount) variables.monthly_rent_amount = monthlyRentAmount
    if (size(tenantInfo)) {
      variables.tenant_name = tenantInfo.name || ''
      variables.tenant_address = tenantInfo.billingAddress || ''
      variables.tenant_zip_code = tenantInfo.zipCode || ''
      variables.tenant_city = tenantInfo.city || ''
      variables.tenant_country = tenantInfo.country || ''
    }
    if (newTenantName) variables.tenant_name = newTenantName
    if (size(addons))
      variables.lease_addons = await getContractAddons(addons, 'lease')

    variables.lease_id = getLeaseId(contractData)
    variables.minimum_stay = minimumStay || ''
    variables.next_CPI_date = await appHelper.getFormattedExportDate(
      partnerSettingsInfo,
      nextCpiDate
    )
    variables.last_CPI_date = await appHelper.getFormattedExportDate(
      partnerSettingsInfo,
      lastCpiDate
    )
    variables.notice_in_effect = noticeInEffect
      ? appHelper.translateToUserLng(`properties.${noticeInEffect}`, language)
      : ''
    variables.notice_period = noticePeriod || ''
    variables.bank_account_number = await getFirstMonthAccountNumber(
      accountInfo,
      partnerInfo
    )
    variables.invoice_due_date = firstInvoiceDueDate
      ? (
          await appHelper.getActualDate(
            partnerSettingsInfo,
            true,
            firstInvoiceDueDate
          )
        ).format(await appHelper.getDateFormat(partnerSettingsInfo))
      : ''
    variables.VAT_status = isVatEnable ? 'Yes' : 'No'
    variables.tenants = await getLeaseTenantInfo(
      propertyInfo,
      contractData.tenantIds
    )
    variables.deposit_amount = depositAmount || ''
    variables.internal_lease_id = internalLeaseId || ''
    variables.monthly_due_date = dueDate || ''

    // Use only for lease e-sign pdf regenerate
    if (!size(variables.tenants) && contractData.tenants) {
      const tenantIds = map(contractData.tenants, 'tenantId')
      variables.tenants = await getLeaseTenantInfo(propertyInfo, tenantIds)
    }
  }

  if (contractType === 'eviction_document') {
    let collectionData = {}
    const options = {
      sendToUserLang: userLang,
      partnerId,
      contractId,
      collectionName: 'invoices',
      event: 'eviction_document'
    }
    let evictionInvoiceIds = []
    const { partnerSettingsInfo } = partnerInfo
    if (invoiceId && size(evictionCases)) {
      const evictionInfo = evictionCases.find(
        (eviction) => eviction.invoiceId === invoiceId
      )
      evictionInvoiceIds = evictionInfo?.evictionInvoiceIds
    }
    if (invoiceId && size(evictionInvoiceIds) && partnerId) {
      collectionData = await invoiceHelper.getAnInvoiceWithSort({
        _id: invoiceId,
        partnerId
      })
      options.invoiceId = invoiceId
      options.evictionInvoiceIds = evictionInvoiceIds
    }
    const variablesList = await notificationLogHelper.getVariablesData(
      contractType,
      collectionData,
      options
    )
    if (variablesList.total_due_rent_invoices)
      variablesList.total_due_rent_invoices = await appHelper.convertToCurrency(
        {
          number: variablesList.total_due_rent_invoices,
          partnerSettingsOrId: partnerSettingsInfo,
          showSymbol: false,
          options: {
            isInvoice: true
          }
        }
      )

    if (variablesList.total_overdue_rent_invoices)
      variablesList.total_overdue_rent_invoices =
        await appHelper.convertToCurrency({
          number: variablesList.total_overdue_rent_invoices,
          partnerSettingsOrId: partnerSettingsInfo,
          showSymbol: false,
          options: {
            isInvoice: true
          }
        })

    variables = { ...variables, ...variablesList }
  }

  return variables
}

export const getESigningTemplateForPartner = async (req) => {
  const { body = {} } = req
  const { contractData } = body
  if (!size(contractData))
    appHelper.validatePartnerAppRequestData(req, ['contractId', 'contractType'])
  else appHelper.validatePartnerAppRequestData(req, ['contractType'])
  const { contractId, partnerId, contractType, invoiceId, templateId } = body
  let contractInfo
  if (!size(contractData) && contractId) {
    contractInfo = await contractHelper.getAContract(
      { _id: contractId },
      undefined,
      ['partner', 'property']
    )
    if (!size(contractInfo)) {
      throw new CustomError(404, 'Contract not found')
    }
    contractInfo.tenantId = contractInfo.rentalMeta?.tenantId
  } else {
    contractInfo = contractData
  }
  contractInfo.partnerId = partnerId
  const templateQuery = {}
  if (templateId) {
    templateQuery._id = templateId
  } else {
    if (contractType === 'lease') templateQuery.isLeaseEsigningPdf = true
    else if (contractType === 'eviction_document')
      templateQuery.isEvictionDocument = true
    else templateQuery.isAssignmentEsigningPdf = true

    //Template query for partner template
    templateQuery.partnerId = { $exists: false }
  }
  const adminTemplate = await getNotificationTemplate(templateQuery)
  if (!size(adminTemplate)) {
    throw new CustomError(404, 'No template found')
  }
  const partnerTemplate = await getNotificationTemplate({
    uniqueId: adminTemplate.uniqueId,
    partnerId
  })
  const notificationTemplateInfo = size(partnerTemplate)
    ? partnerTemplate
    : adminTemplate

  const { content = {} } = notificationTemplateInfo
  let userLang = 'no'
  if (contractInfo.accountId) {
    const accountInfo = await accountHelper.getAnAccount(
      {
        _id: contractInfo.accountId
      },
      undefined,
      ['person']
    )
    if (accountInfo?.person?.profile?.language) {
      userLang = accountInfo.person.profile.language
    }
  }
  const templateContent = content[userLang]
  let variables
  try {
    variables = await getVariablesByContractFrom(contractInfo, contractType, {
      userLang,
      invoiceId,
      contractId
    })
  } catch (err) {
    console.log('Error Found while getting variables for template', err)
  }
  console.log('Template variables ===> ', JSON.parse(JSON.stringify(variables)))
  const template = appHelper.SSR(templateContent, variables)
  return template
}

export const previewMovingEsignPdf = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['movingId', 'accountId'])
  const { body } = req
  const { accountId, movingId, partnerId } = body
  const movingInfo = await propertyItemHelper.getAPropertyItem({
    _id: movingId,
    partnerId,
    type: { $in: ['in', 'out'] }
  })
  if (!size(movingInfo)) {
    throw new CustomError(404, 'Moving info not found')
  }
  movingInfo.accountId = accountId
  movingInfo.movingId = movingId
  const templateInfo = await getMovingTemplateInfo(movingInfo.type, partnerId)
  const content = await getMovingPreviewContent(movingInfo, templateInfo)
  const movingFile = await fileHelper.getAFile({ movingId })
  return { content, movingFileId: movingFile?._id }
}

const getMovingPreviewContent = async (movingInfo, templateInfo) => {
  const accountInfo = await accountHelper.getAnAccount(
    { _id: movingInfo.accountId },
    undefined,
    ['person']
  )
  const userLang = accountInfo?.person?.profile?.language || 'no'
  const notifyEventName =
    movingInfo.type === 'in' ? 'preview_moving_in' : 'preview_moving_out'
  const variables = await notificationLogHelper.getVariablesData(
    notifyEventName,
    movingInfo,
    {
      collectionName: 'property-items',
      collectionId: movingInfo._id
    }
  )
  if (movingInfo.isRentPaid) variables.isRentPaid = movingInfo.isRentPaid
  if (movingInfo.isRentNotPaid)
    variables.isRentNotPaid = movingInfo.isRentNotPaid
  if (movingInfo.rentPaidMsg) variables.rentPaidMsg = movingInfo.rentPaidMsg
  if (movingInfo.tenantAgree) variables.tenantAgree = movingInfo.tenantAgree
  if (movingInfo.tenantNotAgree)
    variables.tenantNotAgree = movingInfo.tenantNotAgree
  if (movingInfo.tenantPdfDescription)
    variables.tenantPdfDescription = movingInfo.tenantPdfDescription

  const content = appHelper.SSR(templateInfo.content[userLang], variables)
  return content
}

const getMovingTemplateInfo = async (movingType, partnerId) => {
  const templateQuery = {
    partnerId: { $exists: false }
  }
  if (movingType === 'in') {
    templateQuery.isMovingInEsigningPdf = true
  } else {
    templateQuery.isMovingOutEsigningPdf = true
  }
  const adminTemplate = await getNotificationTemplate(templateQuery)
  let partnerTemplate = null
  if (size(adminTemplate)) {
    partnerTemplate = await getNotificationTemplate({
      uniqueId: adminTemplate.uniqueId,
      partnerId
    })
  }
  const templateInfo = size(partnerTemplate) ? partnerTemplate : adminTemplate
  if (!size(templateInfo)) throw new CustomError(404, 'Template not found')
  return templateInfo
}
