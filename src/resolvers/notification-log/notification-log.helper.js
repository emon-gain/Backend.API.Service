import {
  assign,
  concat,
  each,
  filter,
  find,
  includes,
  indexOf,
  intersection,
  isString,
  map,
  omit,
  pick,
  size,
  union,
  uniq
} from 'lodash'
import nid from 'nid'
import moment from 'moment-timezone'

import {
  ContractCollection,
  InvoiceCollection,
  NotificationLogCollection,
  UserCollection
} from '../models'
import {
  accountHelper,
  addonHelper,
  annualStatementHelper,
  appHelper,
  appRoleHelper,
  contractHelper,
  conversationHelper,
  correctionHelper,
  depositAccountHelper,
  depositInsuranceHelper,
  fileHelper,
  invoiceHelper,
  logHelper,
  notificationLogHelper,
  notificationTemplateHelper,
  partnerHelper,
  partnerSettingHelper,
  propertyHelper,
  propertyItemHelper,
  taskHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import { CustomError } from '../common'
import { createVerificationToken, getPartnerURLForV1 } from '../v1/v1.helper'

export const getNotificationLog = async (query, session) => {
  const notificationLog = await NotificationLogCollection.findOne(
    query
  ).session(session)
  return notificationLog
}

export const getNotificationLogs = async (query = {}, session, sort = {}) => {
  const notificationLogs = await NotificationLogCollection.find(query)
    .session(session)
    .sort(sort)
  return notificationLogs
}

export const getNotificationLogsWithPopulate = async (
  params = {},
  populate = []
) => {
  const { query = {}, options = {} } = params
  const { sort, skip, limit } = options
  const notificationLogs = await NotificationLogCollection.find(query)
    .sort(sort)
    .skip(skip)
    .limit(limit)
    .populate(populate)
  return notificationLogs
}

export const prepareQueryForLambdaService = (notificationLogIds, type) => {
  const query = {}
  if (size(notificationLogIds)) {
    query._id = { $in: notificationLogIds }
  }

  if (type) {
    query.type =
      type === 'group-mail' ? 'email' : type === 'group-sms' ? 'sms' : ''
    query.status = 'ready'
  }
  return query
}

export const prepareNotificationLogsQueryBasedOnFilters = async (query) => {
  const {
    defaultSearchText,
    email,
    partnerId,
    partnerIds,
    phoneNumber,
    sendDateRange,
    status,
    subject,
    templateType,
    toUser,
    type
  } = query
  if (size(partnerId)) {
    appHelper.validateId({ partnerId })
    delete query.partnerIds
  }
  // Set sendAt filter in query
  if (size(sendDateRange)) {
    appHelper.validateCreatedAtForQuery(sendDateRange)
    let { startDate, endDate } = sendDateRange
    if (partnerId) {
      const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
        partnerId
      )
      startDate = (
        await appHelper.getActualDate(partnerSettings, true, startDate)
      )
        .startOf('day')
        .toDate()
      endDate = (await appHelper.getActualDate(partnerSettings, true, endDate))
        .endOf('day')
        .toDate()
    }
    query.createdAt = {
      $gte: startDate,
      $lte: endDate
    }
  }
  // Set type filter in query
  if (type && !(type === 'email' || type === 'sms'))
    throw new CustomError(400, `invalid type`)
  // Validate partnerId from query
  if (Array.isArray(partnerIds)) {
    appHelper.validateArrayOfId({ partnerIds })
    query.partnerId = { $in: partnerIds }
    delete query.partnerIds
  }
  // Validate toUserId from query and Set toUser filter in query
  if (toUser) {
    appHelper.validateId({ toUser })
    query.toUserId = toUser
  }
  // Set TemplateType filter in query
  if (templateType) query.event = templateType
  if (status) query.status = { $in: status }
  if (subject) {
    query.subject = { $regex: subject.trim() }
  }

  if (defaultSearchText) {
    const phoneNumberRegex = /^\+(?:[0-9] ?){6,14}[0-9]$/
    const emailRegex = new RegExp(
      `([!#-'*+/-9=?A-Z^-~-]+(.[!#-'*+/-9=?A-Z^-~-]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@([!#-'*+/-9=?A-Z^-~-]+(.[!#-'*+/-9=?A-Z^-~-]+)*|[[\t -Z^-~]*])`
    )
    if (phoneNumberRegex.test(defaultSearchText)) {
      query.toPhoneNumber = defaultSearchText.trim()
    } else if (emailRegex.test(defaultSearchText.toLowerCase())) {
      query.toEmail = defaultSearchText.toLowerCase()
    } else {
      query.subject = { $regex: defaultSearchText.trim() }
    }
  } else if (email) {
    query.toEmail = email.trim()
  } else if (phoneNumber) {
    query.toPhoneNumber = phoneNumber.trim()
  }
  const notificationLogsQuery = omit(query, [
    'attachmentFileIds',
    'defaultSearchText',
    'email',
    'isNotifierSms',
    'phoneNumber',
    'sendDateRange',
    'toUser',
    'templateType'
  ])
  return notificationLogsQuery
}
export const getNotificationLogsForQuery = async (params) => {
  const { query, options, populate = [] } = params
  const { limit, skip, sort } = options
  const notificationLogs = await NotificationLogCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return notificationLogs
}

export const getNotifyTenantIdsForSendLeaseEsignReminder = (contractInfo) => {
  if (!size(contractInfo)) return []

  const { rentalMeta = {} } = contractInfo
  const { leaseSigningMeta = {}, tenants = [] } = rentalMeta
  const leaseSigners =
    size(leaseSigningMeta) && size(leaseSigningMeta.signers)
      ? leaseSigningMeta.signers
      : []

  let tenantIds = map(tenants, 'tenantId')

  if (size(leaseSigningMeta) && size(leaseSigners)) {
    const newTenantIds = []

    each(tenants, (tenant) => {
      const { tenantId } = tenant

      const findSiner = find(
        leaseSigners,
        (signer) => signer.externalSignerId === tenantId
      )

      if (!findSiner) newTenantIds.push(tenantId)
    })

    tenantIds = newTenantIds
  }

  return tenantIds
}

export const prepareSendToInfo = (params, type, nameOrPhoneNumber) => {
  const sendToInfo = {}

  if (size(params)) {
    const {
      email = '',
      lang = '',
      phoneNumber = '',
      send_to_user_id = ''
    } = params

    sendToInfo.toUserId = send_to_user_id
    sendToInfo.lang = lang

    if (type === 'email') sendToInfo.toEmail = email
    else if (type === 'sms') {
      if (nameOrPhoneNumber) sendToInfo.fromPhoneNumber = nameOrPhoneNumber
      sendToInfo.toPhoneNumber = phoneNumber
    }
  }

  return sendToInfo
}

export const getTenantInfo = async (tenantId, notificationType) => {
  let tenantSendToInfo = {}
  let tenantEmail = ''

  const tenantInfo = await tenantHelper.getATenant({ _id: tenantId }, null, [
    'user'
  ])
  const { user = {} } = tenantInfo || {}

  if (size(user)) {
    tenantEmail = user.getEmail()
    tenantSendToInfo = getNotificationSendToUserInfo(user, notificationType)
  }

  return { tenantEmail, tenantSendToInfo }
}

export const getTenantSendToInfo = async (params) => {
  const { nameOrPhoneNumber, options, tenantId, type } = params

  if (!tenantId) return {}

  const { tenantEmail, tenantSendToInfo } = await getTenantInfo(tenantId, type)

  if (!(tenantEmail || size(tenantSendToInfo))) return {}

  const sendToInfo = prepareSendToInfo(
    tenantSendToInfo,
    type,
    nameOrPhoneNumber
  )

  sendToInfo.sendTo = 'tenant'
  sendToInfo.tenantId = tenantId
  sendToInfo.variables = {}

  const tenantName = tenantSendToInfo?.send_to_user_name || ''

  if (size(options) && (options.movingId || options.contract)) {
    const { contract = '', event = '', movingId = '' } = options

    sendToInfo.variables.tenant_lease_esigning_url =
      (await contract?.getTenantLeaseEsigningUrl(tenantId)) || ''
    // Todo :: will be Update later
    sendToInfo.variables.new_password_url = await getNewPasswordUrl(tenantId)

    if (
      movingId &&
      (event === 'send_tenant_moving_in_esigning' ||
        event === 'send_move_in_esigning_reminder_notice_to_tenant')
    )
      sendToInfo.variables.tenant_moving_in_esigning_url =
        await getMovingInOutEsigningUrl({
          event,
          sendTo: 'tenant',
          movingId,
          tenantId
        })
    if (
      movingId &&
      (event === 'send_tenant_moving_out_esigning' ||
        event === 'send_move_out_esigning_reminder_notice_to_tenant')
    )
      sendToInfo.variables.tenant_moving_out_esigning_url =
        await getMovingInOutEsigningUrl({
          event,
          sendTo: 'tenant',
          movingId,
          tenantId
        })
  }

  sendToInfo.variables.tenant_name = tenantName
  sendToInfo.variables.jointly_liable_tenant_name = tenantName
  sendToInfo.variables.jointly_liable_tenant_email = tenantEmail

  return sendToInfo
}

export const getAgentOrUserInfo = async (userId, notificationType) => {
  const sendToUserInfo = {}

  if (userId) {
    const userInfo = await userHelper.getAnUser({ _id: userId })

    if (size(userInfo)) {
      const { _id } = userInfo

      sendToUserInfo.send_to_user_name = userInfo.getName()
      sendToUserInfo.lang = userInfo.getLanguage()
      sendToUserInfo.send_to_user_id = _id

      if (notificationType === 'email')
        sendToUserInfo.email = userInfo.getEmail()
      else if (notificationType === 'sms')
        sendToUserInfo.phoneNumber = userInfo.getPhone()
    }
  }

  return sendToUserInfo
}

export const getUserSendToInfo = async (params) => {
  const { nameOrPhoneNumber, type, userId } = params

  if (!userId) return {}

  const userSendToInfo = await getAgentOrUserInfo(userId, type)
  if (!size(userSendToInfo)) return {}

  const sendToInfo = prepareSendToInfo(userSendToInfo, type, nameOrPhoneNumber)

  sendToInfo.sendTo = 'user'
  sendToInfo.variables = {}

  return sendToInfo
}

export const getAgentSendToInfo = async (params) => {
  const { agentId, nameOrPhoneNumber, options, type } = params

  if (!agentId) return {}

  const agentSendToInfo = await getAgentOrUserInfo(agentId, type)
  if (!size(agentSendToInfo)) return {}

  const sendToInfo = prepareSendToInfo(agentSendToInfo, type, nameOrPhoneNumber)

  sendToInfo.sendTo = 'agent'
  sendToInfo.variables = {}

  const { event = '', movingId = '' } = options

  if (size(agentSendToInfo)) {
    if (movingId) {
      if (
        event === 'send_agent_moving_in_esigning' ||
        event === 'send_move_in_esigning_reminder_notice_to_agent'
      )
        sendToInfo.variables.agent_moving_in_esigning_url =
          await getMovingInOutEsigningUrl({ event, sendTo: 'agent', movingId })
      if (
        event === 'send_agent_moving_out_esigning' ||
        event === 'send_move_out_esigning_reminder_notice_to_agent'
      )
        sendToInfo.variables.agent_moving_out_esigning_url =
          await getMovingInOutEsigningUrl({ event, sendTo: 'agent', movingId })
    }
  }

  return sendToInfo
}

export const getNotificationSendToUserInfo = (userInfo, notificationType) => {
  let sendToUserInfo = {}

  if (size(userInfo)) {
    const { _id } = userInfo

    sendToUserInfo.send_to_user_name = userInfo.getName()
    sendToUserInfo.lang = userInfo.getLanguage()
    sendToUserInfo.send_to_user_id = _id

    if (notificationType === 'email') {
      const emailAddress = userInfo.getEmail()

      if (emailAddress) sendToUserInfo.email = emailAddress
      else sendToUserInfo = {}
    } else if (notificationType === 'sms') {
      const phoneNumber = userInfo.getPhone()

      if (phoneNumber) sendToUserInfo.phoneNumber = phoneNumber
      else sendToUserInfo = {}
    }
  }

  return sendToUserInfo
}

export const getAccountContactPersonInfo = async (
  accountId,
  notificationType
) => {
  let accountContactPersonInfo = {}

  if (accountId) {
    const accountInfo = await accountHelper.getAnAccount(
      { _id: accountId },
      null,
      ['agent', 'person']
    )
    const { agent = {}, person = {}, type = '' } = accountInfo || {}

    // If there is a person connected to account then we will get person info and will send notification to the person email or phone
    // Else if there is no person, account type is organization and there is an agent connected to account then will send notification to the agent email or phone
    if (size(person)) {
      accountContactPersonInfo = getNotificationSendToUserInfo(
        person,
        notificationType
      )
    } else if (type === 'organization' && size(agent)) {
      accountContactPersonInfo = getNotificationSendToUserInfo(
        agent,
        notificationType
      )
    }
  }

  return accountContactPersonInfo
}

export const getAccountSendToInfo = async (params) => {
  const { accountId, nameOrPhoneNumber, type } = params

  if (!accountId) return {}

  const accountSendToInfo = await getAccountContactPersonInfo(accountId, type)
  if (!size(accountSendToInfo)) return {}

  const sendToInfo = prepareSendToInfo(
    accountSendToInfo,
    type,
    nameOrPhoneNumber
  )

  sendToInfo.sendTo = 'account'

  return sendToInfo
}

export const validationCheckForNotificationSendToInfo = (body) => {
  appHelper.compactObject(body)

  const requiredFields = [
    'collectionId',
    'collectionNameStr',
    'event',
    'notifyToData',
    'partnerId'
  ]
  appHelper.checkRequiredFields(requiredFields, body)

  const { partnerId } = body
  appHelper.validateId({ partnerId })
}

export const getNotificationSendToInfo = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)

  validationCheckForNotificationSendToInfo(body)

  const {
    collectionId,
    collectionNameStr,
    event,
    notifyToData,
    options = {},
    partnerId
  } = body

  const { collectionName = '' } =
    appHelper.getCollectionNameAndFieldNameByString(collectionNameStr)

  if (!collectionName) return []

  const collectionData = await collectionName.findOne({ _id: collectionId })

  const { phoneNumber = '', sms = false } =
    (await partnerHelper.getAPartner({ _id: partnerId })) || {}
  const { smsSettings = {} } =
    (await partnerSettingHelper.getAPartnerSetting({ partnerId })) || {}

  const isEnabledSMS = sms
  const nameOrPhoneNumber =
    isEnabledSMS && !!smsSettings.smsSenderName
      ? smsSettings.smsSenderName
      : isEnabledSMS && !smsSettings.smsSenderName
      ? phoneNumber
      : ''
  // Setting event and collectionData in options for getting tenant and agent variables
  options.event = event
  if (collectionNameStr === 'contracts' && size(collectionData))
    options.contract = collectionData

  const notificationSendToInfos = []

  for (const notifyToInfo of notifyToData) {
    const { id = '', templateUniqueId = '', type = 'email' } = notifyToInfo

    if (id === 'accountId') {
      const { accountId = '' } = collectionData

      const accountSendToInfoParams = {
        accountId,
        nameOrPhoneNumber,
        type
      }
      const accountSendToInfo =
        (await getAccountSendToInfo(accountSendToInfoParams)) || {}

      if (size(accountSendToInfo)) {
        const notificationSendToInfo = {
          ...accountSendToInfo,
          templateUniqueId,
          type
        }
        notificationSendToInfos.push(notificationSendToInfo)
      }
    } else if (id === 'agentId') {
      const { agentId = '' } = collectionData

      const agentSendToInfoParams = {
        agentId,
        nameOrPhoneNumber,
        options,
        type
      }
      const agentSendToInfo =
        (await getAgentSendToInfo(agentSendToInfoParams)) || {}

      if (size(agentSendToInfo)) {
        const notificationSendToInfo = {
          ...agentSendToInfo,
          templateUniqueId,
          type
        }
        notificationSendToInfos.push(notificationSendToInfo)
      }
    } else if (id === 'userId') {
      const { userId = '', assignTo = [] } = options
      const userIds = [userId]

      if (size(assignTo) && event === 'send_task_notification')
        userIds.push(...assignTo)

      if (size(userIds)) {
        const usersSendToInfos = await getUsersSendToInfos(userIds, {
          nameOrPhoneNumber,
          templateUniqueId,
          type
        })
        notificationSendToInfos.push(...usersSendToInfos)
      }
    } else if (id === 'tenantId') {
      const { contractId = '' } = collectionData
      let tenantId = collectionData.tenantId || ''
      let contractInfo = {}
      let isJointlyLiable = false
      let tenantIds = []

      if (collectionNameStr !== 'contracts' && contractId)
        contractInfo = await contractHelper.getAContract({ _id: contractId })

      if (collectionNameStr === 'contracts' && size(collectionData)) {
        if (!tenantId && collectionData.rentalMeta?.tenantId) {
          tenantId = collectionData.rentalMeta.tenantId
        }

        isJointlyLiable = collectionData.isJointlyLiable()
        contractInfo = collectionData
      } else if (collectionNameStr !== 'contracts' && size(contractInfo)) {
        isJointlyLiable = contractInfo.isJointlyLiable()
      }

      // Get tenantIds from contract
      if (
        size(contractInfo) &&
        (isJointlyLiable ||
          contractInfo.rentalMeta.depositType === 'deposit_insurance')
      ) {
        const { rentalMeta = {} } = contractInfo
        const { tenants = [] } = rentalMeta
        if (size(tenants)) {
          if (event === 'send_lease_esigning_reminder_notice_to_tenant')
            tenantIds =
              getNotifyTenantIdsForSendLeaseEsignReminder(contractInfo)
          else tenantIds = map(tenants, 'tenantId')
        }
      }
      if (collectionNameStr === 'listings' && size(options) && options.tenantId)
        tenantId = options.tenantId

      if (collectionNameStr === 'tenants') tenantId = collectionId

      // If options.tenantId, we will only send notification to options.tenantId
      if (size(options) && options.tenantId) tenantIds = [options.tenantId]

      if (!size(tenantIds) && tenantId) tenantIds = [tenantId]

      if (size(tenantIds)) {
        for (const tenantId of tenantIds) {
          const tenantSendToInfoParams = {
            nameOrPhoneNumber,
            options,
            tenantId,
            type
          }
          const tenantSendToInfo =
            (await getTenantSendToInfo(tenantSendToInfoParams)) || {}

          if (size(tenantSendToInfo)) {
            const notificationSendToInfo = {
              ...tenantSendToInfo,
              templateUniqueId,
              type
            }
            notificationSendToInfos.push(notificationSendToInfo)
          }
        }
      }
    } else if (id === 'appAdminId') {
      // Todo :E: test script is not implemented
      const { users: userIds = [] } =
        (await appRoleHelper.getAppRole({
          type: 'app_admin'
        })) || {}

      if (size(userIds)) {
        const usersSendToInfos = await getUsersSendToInfos(userIds, {
          nameOrPhoneNumber,
          templateUniqueId,
          type
        })
        notificationSendToInfos.push(...usersSendToInfos)
      }
    } else if (checkIfStringIsAnEmailAddress(id)) {
      const { _id: userId } =
        (await userHelper.getAnUser({
          $or: [{ 'emails.address': id }, { 'services.facebook.email': id }]
        })) || {}

      if (userId) {
        const userSendToInfoParams = {
          nameOrPhoneNumber,
          type,
          userId
        }
        const userSendToInfo =
          (await getUserSendToInfo(userSendToInfoParams)) || {}

        if (size(userSendToInfo)) {
          const notificationSendToInfo = {
            ...userSendToInfo,
            templateUniqueId,
            type
          }
          notificationSendToInfos.push(notificationSendToInfo)
        }
      } else {
        const notificationSendToInfo = {
          lang: 'no',
          sendTo: 'email',
          templateUniqueId,
          toEmail: id,
          toUserId: id,
          type,
          variables: {}
        }
        notificationSendToInfos.push(notificationSendToInfo)
      }
    }
  }

  return { data: notificationSendToInfos }
}

const getInvoiceRelatedEvents = () => [
  'send_invoice',
  'send_due_reminder',
  'send_first_reminder',
  'send_second_reminder',
  'send_collection_notice',
  'send_credit_note',
  'send_eviction_notice',
  'send_eviction_due_reminder_notice',
  'send_eviction_due_reminder_notice_without_eviction_fee',
  'send_landlord_invoice',
  'send_landlord_due_reminder',
  'send_landlord_first_reminder',
  'send_landlord_second_reminder',
  'send_landlord_collection_notice',
  'send_landlord_credit_note',
  'send_final_settlement',
  'send_deposit_insurance_payment_reminder'
]

export const validationCheckForNotificationVariablesData = (body) => {
  appHelper.compactObject(body)
  appHelper.checkRequiredFields(['event', 'options'], body)
  const { options = {} } = body

  if (!size(options) || !(options.collectionNameStr && options.collectionId))
    throw new CustomError(400, "Didn't find required data")
}

export const allowedEventForContractContext = [
  'send_termination_notice_by_tenant',
  'send_termination_notice_by_landlord',
  'send_schedule_termination_notice_by_tenant',
  'send_schedule_termination_notice_by_landlord',
  'send_natural_termination_notice',
  'send_notification_tenant_pays_all_due_during_eviction',
  'send_CPI_settlement_notice',
  'send_soon_ending_notice',
  'send_welcome_lease',
  'send_wrong_ssn_notification'
]

export const allowedEventForInvoiceContext = [
  'send_invoice',
  'send_due_reminder',
  'send_first_reminder',
  'send_second_reminder',
  'send_collection_notice',
  'send_eviction_notice',
  'send_eviction_due_reminder_notice',
  'send_eviction_due_reminder_notice_without_eviction_fee',
  'send_landlord_invoice',
  'send_landlord_due_reminder',
  'send_landlord_first_reminder',
  'send_landlord_second_reminder',
  'send_landlord_collection_notice',
  'send_final_settlement'
]

export const getAllowedVariableContext = (notifyEvent) => {
  let allowedVariableContext = [
    'all',
    'account',
    'partner',
    'property',
    'tenant'
  ]

  if (indexOf(allowedEventForInvoiceContext, notifyEvent) !== -1) {
    allowedVariableContext = union(allowedVariableContext, ['invoice'])
  } else if (indexOf(allowedEventForContractContext, notifyEvent) !== -1) {
    allowedVariableContext = union(allowedVariableContext, ['contract'])
  } else if (
    notifyEvent === 'send_pending_payout_for_approval' ||
    notifyEvent === 'send_payouts_approval_esigning'
  ) {
    allowedVariableContext = ['all', 'partner', 'pending_payouts']
  } else if (
    notifyEvent === 'send_pending_payment_for_approval' ||
    notifyEvent === 'send_payments_approval_esigning'
  ) {
    allowedVariableContext = ['all', 'partner', 'pending_payments']
  } else if (
    notifyEvent === 'send_credit_note' ||
    notifyEvent === 'send_landlord_credit_note'
  ) {
    allowedVariableContext = union(allowedVariableContext, ['credit_note'])
  } else if (
    notifyEvent === 'send_assignment_email' ||
    notifyEvent === 'create_assignment_contract'
  ) {
    allowedVariableContext = union(allowedVariableContext, ['assignment'])
  } else if (
    notifyEvent === 'send_assignment_esigning' ||
    notifyEvent === 'send_assignment_esigning_reminder_notice_to_landlord'
  ) {
    allowedVariableContext = [
      'all',
      'account',
      'agent_esigning',
      'landlord_esigning',
      'partner',
      'property'
    ]
  } else if (
    notifyEvent === 'send_landlord_lease_esigning' ||
    notifyEvent === 'send_lease_esigning_reminder_notice_to_landlord'
  ) {
    allowedVariableContext = [
      'all',
      'account',
      'landlord_lease_esigning',
      'partner',
      'property'
    ]
  } else if (
    notifyEvent === 'send_tenant_lease_esigning' ||
    notifyEvent === 'send_lease_esigning_reminder_notice_to_tenant'
  ) {
    allowedVariableContext = union(allowedVariableContext, [
      'tenant_lease_esigning'
    ])
  } else if (
    notifyEvent === 'send_tenant_moving_in_esigning' ||
    notifyEvent === 'send_move_in_esigning_reminder_notice_to_tenant' ||
    notifyEvent === 'send_tenant_moving_out_esigning' ||
    notifyEvent === 'send_agent_moving_in_esigning' ||
    notifyEvent === 'send_move_in_esigning_reminder_notice_to_agent' ||
    notifyEvent === 'send_agent_moving_out_esigning'
  ) {
    // agent_moving_in_esigning & agent_moving_out_esigning &
    // tenant_moving_in_esigning & tenant_moving_out_esigning is not required here,
    // Though we are getting those from userSendToInfo of Agent & Tenant
    return allowedVariableContext
  } else if (
    notifyEvent === 'send_deposit_incoming_payment' ||
    notifyEvent === 'send_deposit_account_created' ||
    notifyEvent === 'send_deposit_insurance_created'
  ) {
    allowedVariableContext = union(allowedVariableContext, [
      'contract',
      'deposit'
    ])
  } else if (notifyEvent === 'send_payout') {
    allowedVariableContext = ['all', 'account', 'partner', 'payout', 'property']
  } else if (notifyEvent === 'send_chat_notification') {
    allowedVariableContext = ['all', 'chat', 'listing', 'user']
  } else if (notifyEvent === 'send_interest_form') {
    allowedVariableContext = ['all', 'partner', 'property', 'tenant']
  } else if (notifyEvent === 'send_task_notification') {
    allowedVariableContext = ['all', 'partner', 'task']
  } else if (notifyEvent === 'create_lease_contract') {
    allowedVariableContext = ['all', 'contract']
  } else if (notifyEvent === 'send_download_notification') {
    allowedVariableContext = ['all', 'download', 'partner']
  } else if (notifyEvent === 'send_next_schedule_payout') {
    allowedVariableContext = union(allowedVariableContext, [
      'estimated_payouts'
    ])
  } else if (
    notifyEvent === 'send_landlord_moving_in_esigning' ||
    notifyEvent === 'send_move_in_esigning_reminder_notice_to_landlord'
  ) {
    allowedVariableContext = union(allowedVariableContext, [
      'landlord_moving_in_esigning'
    ])
  } else if (
    notifyEvent === 'send_landlord_moving_out_esigning' ||
    notifyEvent === 'send_move_out_esigning_reminder_notice_to_landlord'
  ) {
    allowedVariableContext = union(allowedVariableContext, [
      'landlord_moving_out_esigning'
    ])
  } else if (notifyEvent === 'send_landlord_annual_statement') {
    allowedVariableContext = union(allowedVariableContext, [
      'contract',
      'landlord_annual_statement'
    ])
  } else if (notifyEvent === 'preview_moving_in') {
    allowedVariableContext = union(allowedVariableContext, [
      'moving_in_esigning'
    ])
  } else if (notifyEvent === 'preview_moving_out') {
    allowedVariableContext = union(allowedVariableContext, [
      'moving_out_esigning'
    ])
  } else if (notifyEvent === 'send_app_health_status') {
    allowedVariableContext = ['all', 'appHealth']
  } else if (notifyEvent === 'send_interest_form_invitation') {
    allowedVariableContext = ['all', 'partner', 'property', 'tenant', 'user']
  } else if (notifyEvent === 'eviction_document') {
    allowedVariableContext = [
      'all',
      'contract',
      'partner',
      'property',
      'tenant',
      'invoice'
    ]
  } else if (notifyEvent === 'send_notification_ask_for_credit_rating') {
    allowedVariableContext = ['all', 'credit_rating', 'partner', 'tenant']
  } else if (notifyEvent === 'send_deposit_insurance_payment_reminder') {
    allowedVariableContext = [
      'all',
      'app_invoice',
      'contract',
      'partner',
      'tenant'
    ]
  } else allowedVariableContext = []

  return allowedVariableContext
}

const getTenantInfoForVariablesData = async (
  collectionData,
  collectionName,
  tenantId
) => {
  let tenantInfo = {}

  if (collectionName === 'listings') {
    if (tenantId) tenantInfo = await tenantHelper.getATenant({ _id: tenantId })
  } else if (collectionName === 'tenants') {
    tenantInfo = collectionData
  } else tenantInfo = await collectionData.getTenant() // Where tenantId exists in collection

  return tenantInfo
}

export const getTenantsNameOrEmailOrPersonId = async (
  tenantIds,
  variableName
) => {
  let jointlyTenantEmails = ''
  let jointlyTenantNames = ''
  let jointlyTenantPersonIds = ''

  if (size(tenantIds)) {
    for (const [i, tenantId] of tenantIds.entries()) {
      const tenantInfo = await tenantHelper.getATenant(
        { _id: tenantId },
        null,
        ['user']
      )

      if (variableName === 'jointly_liable_tenant_emails') {
        const { user = {} } = tenantInfo || {}
        const tenantEmail = size(user) ? user.getEmail() : ''
        if (tenantEmail) {
          jointlyTenantEmails += tenantEmail
          if (i + 1 !== tenantIds.length) jointlyTenantEmails += ', '
        }
      } else if (variableName === 'jointly_liable_tenant_person_IDs') {
        const { user = {} } = tenantInfo || {}
        const tenantPersonId = size(user)
          ? user.getNorwegianNationalIdentification()
          : ''
        if (tenantPersonId) {
          jointlyTenantPersonIds += tenantPersonId
          if (i + 1 !== tenantIds.length) jointlyTenantPersonIds += ', '
        }
      } else {
        const { name = '' } = tenantInfo || {}

        if (name) {
          jointlyTenantNames += name
          if (i + 1 !== tenantIds.length) jointlyTenantNames += ', '
        }
      }
    } // Ends of For loop
  }

  if (variableName === 'jointly_liable_tenant_emails')
    return jointlyTenantEmails
  else if (variableName === 'jointly_liable_tenant_person_IDs')
    return jointlyTenantPersonIds
  else return jointlyTenantNames
}

const getTenantIdsFromCollectionData = async (
  collectionData,
  collectionName
) => {
  let contractInfo = {}

  if (collectionName === 'contracts' && size(collectionData))
    contractInfo = collectionData
  else {
    const { contractId = '' } = collectionData || {}

    if (!contractId) return []

    contractInfo =
      (await contractHelper.getAContract({ _id: contractId })) || {}

    if (!size(contractInfo)) return []
  }

  const { rentalMeta = {} } = contractInfo || {}
  const { tenants = [] } = rentalMeta

  if (!size(tenants)) return []

  const tenantIds = map(tenants, 'tenantId') || []

  return tenantIds
}

const getAccountInfoForVariablesData = async (collectionData) => {
  const { accountId = '' } = collectionData

  if (!accountId) return {}

  const accountInfo = await accountHelper.getAnAccount(
    { _id: accountId },
    null,
    ['organization', 'person']
  )

  return accountInfo
}

const getAccountBankAccountInfoForVariablesData = async (
  partnerId,
  contractInfo
) => {
  let accountBankAccount = ''

  if (partnerId && size(contractInfo)) {
    const { accountId = '', payoutTo = '' } = contractInfo

    const partnerInfo = await partnerHelper.getAPartner({ _id: partnerId })
    const { accountType = '' } = partnerInfo || {}

    if (accountId && accountType === 'direct') {
      const accountInfo = await accountHelper.getAnAccount({
        _id: accountId,
        partnerId
      })
      const { invoiceAccountNumber = '' } = accountInfo || {}

      accountBankAccount = invoiceAccountNumber
    } else accountBankAccount = payoutTo
  }

  return accountBankAccount
}

const getPartnerBankAccountInfoForVariablesData = (partnerSetting) => {
  const { bankPayment = {}, partner } = partnerSetting || {}
  const { accountType = '' } = partner || {}

  if (size(bankPayment) && accountType === 'broker') {
    const { afterFirstMonthACNo = '', firstMonthACNo = '' } = bankPayment
    return afterFirstMonthACNo || firstMonthACNo
  } else return ''
}

const getPropertyLocationInfoForVariablesData = async (params) => {
  const { collectionData = {}, collectionName = '', locationType = '' } = params

  let propertyInfo = {}

  if (collectionName === 'listings') {
    propertyInfo = collectionData
  } else propertyInfo = await collectionData.getProperty()

  const { location = {}, title = '' } = propertyInfo || {}

  if (size(location)) {
    if (locationType === 'name') return location[locationType] || title
    else return location[locationType] || ''
  }

  return ''
}

const getPartnerAddressInfoForVariablesData = async (params) => {
  const { collectionData = {}, event = '', addressType = '' } = params
  const {
    accountId = '',
    invoiceAccountNumber = '',
    partnerId = ''
  } = collectionData

  const partnerSettings = partnerId
    ? await partnerSettingHelper.getAPartnerSetting({
        partnerId
      })
    : {}

  let partnerAddress = ''

  if (indexOf(getInvoiceRelatedEvents(), event) !== -1) {
    const senderInfo = await invoiceHelper.getSenderInfo(
      partnerSettings,
      accountId,
      invoiceAccountNumber
    )
    const {
      address = '',
      city = '',
      country = '',
      zipCode = ''
    } = senderInfo || {}

    if (addressType === 'Address') return address
    else if (addressType === 'ZipCode') return zipCode
    else if (addressType === 'City') return city
    else if (addressType === 'Country') return country
  } else {
    const { companyInfo = {} } = partnerSettings || {}

    partnerAddress =
      addressType && size(companyInfo) && companyInfo[`postal${addressType}`]
        ? companyInfo[`postal${addressType}`]
        : ''
  }

  return partnerAddress
}

const getPropertyInfoForVariablesData = async (
  collectionData,
  collectionName
) => {
  let propertyInfo = {}

  if (collectionName === 'listings') {
    propertyInfo = collectionData
  } else propertyInfo = await collectionData.getProperty() // Where tenantId exists in collection

  return propertyInfo
}

const getPartnerNameForVariablesData = async (event, collectionData) => {
  const {
    accountId = '',
    invoiceAccountNumber = '',
    partnerId = ''
  } = collectionData

  let partnerName = ''

  const partnerInfo = partnerId
    ? await partnerHelper.getAPartner({ _id: partnerId })
    : {}

  const { name = '' } = partnerInfo || {}

  if (indexOf(getInvoiceRelatedEvents(), event) !== -1) {
    const partnerSettings = partnerId
      ? await partnerSettingHelper.getAPartnerSetting({
          partnerId
        })
      : {}

    const senderInfo = await invoiceHelper.getSenderInfo(
      partnerSettings,
      accountId,
      invoiceAccountNumber
    )
    const { name = '' } = senderInfo || {}

    partnerName = name
  } else {
    partnerName = name
  }

  return partnerName
}

const getPartnerOrdIdForVariablesData = async (event, collectionData) => {
  const {
    accountId = '',
    invoiceAccountNumber = '',
    partnerId = ''
  } = collectionData

  let partnerOrgId = ''

  const partnerSettings = partnerId
    ? await partnerSettingHelper.getAPartnerSetting({
        partnerId
      })
    : {}

  const { companyInfo = {} } = partnerSettings || {}
  const { organizationId = '' } = companyInfo

  if (indexOf(getInvoiceRelatedEvents(), event) !== -1) {
    const senderInfo = await invoiceHelper.getSenderInfo(
      partnerSettings,
      accountId,
      invoiceAccountNumber
    )
    const { orgId = '' } = senderInfo || {}

    partnerOrgId = orgId
  } else {
    partnerOrgId = organizationId
  }

  // Add 'MVA' suffix in org id
  if (partnerId && partnerOrgId && invoiceAccountNumber) {
    const partnerInfo = await partnerHelper.getAPartner(
      { _id: partnerId },
      null,
      ['account']
    )
    const { account, accountType = '' } = partnerInfo || {}
    const { vatRegistered = false } = account || {}

    if (accountType === 'direct' && vatRegistered) {
      partnerOrgId = partnerOrgId + ' MVA'
    } else {
      const { bankAccounts = [] } = partnerSettings || {}
      if (size(bankAccounts)) {
        const findAccount = find(
          bankAccounts,
          (bankAccount) =>
            bankAccount.accountNumber === invoiceAccountNumber &&
            bankAccount.vatRegistered === true &&
            bankAccount.orgId === partnerOrgId
        )

        if (size(findAccount)) partnerOrgId = partnerOrgId + ' MVA'
      }
    }
  }

  return partnerOrgId
}

export const getAnnualStatementItemsForVariablesData = async (
  annualStatementId
) => {
  const annualStatementItems = []

  if (annualStatementId) {
    const annualStatementData = await annualStatementHelper.getAnnualStatement({
      _id: annualStatementId
    })

    if (size(annualStatementData)) {
      annualStatementItems.push({
        report_year: annualStatementData.statementYear || '',
        rent_total_excl_tax: annualStatementData.rentTotalExclTax,
        rent_total_tax: annualStatementData.rentTotalTax,
        rent_total: annualStatementData.rentTotal,
        commission_total_amount: annualStatementData.landlordTotalExclTax,
        commission_total_vat: annualStatementData.landlordTotalTax,
        commission_total: annualStatementData.landlordTotal,
        total_payouts: annualStatementData.totalPayouts
      })
    }
  }

  return annualStatementItems
}

export const getBrokeringCommissionAmount = (contractData) => {
  if (contractData.brokeringCommissionType === 'fixed') {
    return contractData.brokeringCommissionAmount
  } else if (contractData.brokeringCommissionType === 'percent') {
    const monthlyRentAmount = contractData.listingInfo?.monthlyRentAmount || 0
    const percentageAmount =
      monthlyRentAmount * (contractData.brokeringCommissionAmount / 100)

    return percentageAmount && percentageAmount !== Infinity
      ? percentageAmount
      : 0
  } else return 0
}

export const getManagementCommissionAmount = (contractData) => {
  if (contractData.rentalManagementCommissionType === 'fixed') {
    return contractData.rentalManagementCommissionAmount
  } else if (contractData.rentalManagementCommissionType === 'percent') {
    const monthlyRentAmount = contractData.listingInfo?.monthlyRentAmount || 0
    const amount =
      monthlyRentAmount * (contractData.rentalManagementCommissionAmount / 100)

    return amount && amount !== Infinity ? amount : 0
  } else return 0
}

export const getCommissionAmountByCommissionType = async (
  type,
  metaInfo,
  payoutId
) => {
  let adjustedTotalAmount = 0

  if (size(metaInfo) && payoutId) {
    for (const payoutMeta of metaInfo) {
      const { landlordInvoiceId = '' } = payoutMeta
      const invoiceInfo = landlordInvoiceId
        ? await invoiceHelper.getInvoice({ _id: landlordInvoiceId })
        : {}
      let metaData = []

      if (size(invoiceInfo)) {
        const { addonsMeta = [], commissionsMeta = [] } = invoiceInfo
        if (type === 'addon') metaData = addonsMeta
        else metaData = commissionsMeta

        if (size(metaData)) {
          for (const meta of metaData) {
            if (meta.type === type && size(meta.payouts)) {
              const adjustedPayoutInfo = find(meta.payouts, [
                'payoutId',
                payoutId
              ])

              if (size(adjustedPayoutInfo) && adjustedPayoutInfo.amount)
                adjustedTotalAmount += adjustedPayoutInfo.amount
            }
          }
        }
      }
    }

    adjustedTotalAmount = adjustedTotalAmount
      ? adjustedTotalAmount * -1
      : adjustedTotalAmount
  }

  return adjustedTotalAmount
}

export const getPayoutAddons = async (type, metaInfo, payoutId) => {
  const payoutAddons = []
  let addonTotal = 0

  if (size(metaInfo) && payoutId) {
    for (const payoutMeta of metaInfo) {
      const { landlordInvoiceId = '' } = payoutMeta
      const invoiceInfo = landlordInvoiceId
        ? await invoiceHelper.getInvoice({ _id: landlordInvoiceId })
        : {}
      let metaData = []

      if (size(invoiceInfo)) {
        const { addonsMeta = [] } = invoiceInfo
        if (type === 'addon') {
          metaData = addonsMeta
        }
        if (size(metaData)) {
          for (const invoiceAddon of metaData) {
            const addon = {}
            const addonInfo = await addonHelper.getAddon({
              _id: invoiceAddon.addonId
            })

            addon.addon_name = addonInfo.name
            addon.addon_description = invoiceAddon.description
            addon.addon_tax_percentage = invoiceAddon.taxPercentage
            addon.addon_price = invoiceAddon.total

            addonTotal += invoiceAddon.total
            if (addon) payoutAddons.push(addon)
          }
        }
      }
    }
  }

  return { addons: payoutAddons, addonsTotal: addonTotal }
}

export const getLeaseEndDate = async (
  partnerId,
  collectionData,
  sendToUserLang
) => {
  if (
    !!collectionData &&
    !!collectionData.rentalMeta &&
    !collectionData.rentalMeta['contractEndDate']
  ) {
    return appHelper.translateToUserLng('common.undetermined', sendToUserLang)
  } else {
    const leaseEndDate = await appHelper.getActualDate(
      partnerId,
      true,
      collectionData.rentalMeta['contractEndDate']
    )
    const dateFormats = await appHelper.getDateFormat(partnerId)
    return leaseEndDate.format(dateFormats)
  }
}

export const getCorrectionInvoiceAttachmentInfo = async (
  invoiceInfoOrId,
  sendToLang
) => {
  if (!invoiceInfoOrId) return false

  const attachmentVars = []
  let totalFileSize = 0
  let isExceedAttachedFileSize = false
  let invoiceInfo

  // If string, invoiceInfoOrId param is an Id.
  if (isString(invoiceInfoOrId)) {
    invoiceInfo =
      (await invoiceHelper.getInvoice({ _id: invoiceInfoOrId })) || {}
  } else {
    invoiceInfo = invoiceInfoOrId
  }
  const correctionsQuery = {
    _id: { $in: invoiceInfo.correctionsIds },
    isVisibleToTenant: true,
    files: { $exists: true }
  }
  const corrections =
    (await correctionHelper.getCorrections(correctionsQuery)) || {}

  if (size(corrections)) {
    for (const correction of corrections) {
      const { files = [] } = correction
      for (const fileId of files) {
        const fileQuery = {
          _id: fileId,
          type: 'correction_invoice_pdf',
          context: 'correction'
        }
        const fileInfo = (await fileHelper.getAFile(fileQuery)) || {}

        if (size(fileInfo)) {
          const fileKey = fileHelper.getFileKey(fileInfo)
          const { _id, size, title } = fileInfo

          totalFileSize += size / (1024 * 1024)

          if (totalFileSize < 10) {
            attachmentVars.push({
              id: nid(17),
              name: title,
              type: 'email_attachment_pdf',
              lang: sendToLang,
              status: 'done',
              fileId: _id,
              fileKey
            })
          } else {
            isExceedAttachedFileSize = true
          }
        }
      }
    }
  }

  return { isExceedAttachedFileSize, attachmentVars }
}

const getEstimatedBrokeringCommissionAmount = async (collectionData) => {
  const { contractId, partnerId } = collectionData
  const contract =
    contractId && partnerId
      ? await contractHelper.getAContract({ _id: contractId, partnerId })
      : {}
  const {
    brokeringCommissionAmount = 0,
    brokeringCommissionType = '',
    rentalMeta = {}
  } = contract || {}
  const { monthlyRentAmount = 0 } = rentalMeta

  if (brokeringCommissionType === 'fixed') {
    return brokeringCommissionAmount
  } else if (brokeringCommissionType === 'percent') {
    const percentageAmount =
      monthlyRentAmount * (brokeringCommissionAmount / 100)
    return percentageAmount && percentageAmount !== Infinity
      ? percentageAmount
      : 0
  } else return 0
}

const getEstimatedBrokeringCommissionPercentage = async (collectionData) => {
  const { contractId, partnerId } = collectionData
  const contract =
    contractId && partnerId
      ? await contractHelper.getAContract({ _id: contractId, partnerId })
      : {}

  const { brokeringCommissionType = '', brokeringCommissionAmount = 0 } =
    contract || {}
  if (brokeringCommissionType === 'percent') return brokeringCommissionAmount
  else return 0
}

const getEstimatedManagementCommissionPercentage = async (collectionData) => {
  const { contractId, partnerId } = collectionData
  const contract =
    contractId && partnerId
      ? await contractHelper.getAContract({ _id: contractId, partnerId })
      : {}

  const {
    rentalManagementCommissionType = '',
    rentalManagementCommissionAmount = 0
  } = contract || {}
  if (rentalManagementCommissionType === 'percent')
    return rentalManagementCommissionAmount
  else return 0
}

export const getContractAddons = async (contractData, type) => {
  const addonsInfo = []

  if (size(contractData.addons)) {
    for (const addon of contractData.addons) {
      if (type === addon.type) {
        const addonInfo = await addonHelper.getAddon({ _id: addon.addonId })
        const { name = '' } = addonInfo || {}
        if (name) {
          addonsInfo.push({
            addon_name: name,
            addon_price: addon.total
          })
        }
      }
    }
  }

  return addonsInfo
}

export const getMovingInOutEsigningUrl = async (params) => {
  const { event, movingId, sendTo, tenantId = '' } = params
  let signingUrl = ''
  const movingInOutInfo = movingId
    ? await propertyItemHelper.getAPropertyItem(
        {
          _id: movingId
        },
        null
      )
    : {}

  if (sendTo === 'tenant') {
    const { tenantSigningStatus = [] } = movingInOutInfo || {}
    const tenantSigningObj =
      size(tenantSigningStatus) && tenantId
        ? find(tenantSigningStatus, { tenantId })
        : {}
    const { internalUrl = '' } = tenantSigningObj || {}

    const tenantMovingInfo =
      event === 'send_tenant_moving_in_esigning'
        ? 'tenant_moving_in'
        : 'tenant_moving_out'

    signingUrl =
      appHelper.getLinkServiceURL() +
      '/e-signing/' +
      tenantMovingInfo +
      '/' +
      movingId +
      '/' +
      internalUrl
  } else if (sendTo === 'agent') {
    const { agentSigningStatus = {} } = movingInOutInfo || {}
    const { internalUrl = '' } = agentSigningStatus
    const agentMovingInfo =
      event === 'send_agent_moving_in_esigning'
        ? 'agent_moving_in'
        : 'agent_moving_out'

    signingUrl =
      appHelper.getLinkServiceURL() +
      '/e-signing/' +
      agentMovingInfo +
      '/' +
      movingId +
      '/' +
      internalUrl
  } else {
    const { landlordSigningStatus = {} } = movingInOutInfo || {}
    const { internalUrl = '' } = landlordSigningStatus || {}
    const landlordMovingInfo = event.includes('in_esigning')
      ? 'landlord_moving_in'
      : 'landlord_moving_out'

    signingUrl =
      appHelper.getLinkServiceURL() +
      '/e-signing/' +
      landlordMovingInfo +
      '/' +
      movingId +
      '/' +
      internalUrl
  }
  return signingUrl
}

const getFormattedDateRange = async (partnerId, invoiceInfo) => {
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  if (!size(partnerSettings)) return ''
  const dateFormat = await appHelper.getDateFormat(partnerSettings)

  const invoiceStartOn = (
    await appHelper.getActualDate(
      partnerSettings,
      true,
      invoiceInfo?.invoiceStartOn
    )
  ).format(dateFormat)

  const invoiceEndOn = (
    await appHelper.getActualDate(
      partnerSettings,
      true,
      invoiceInfo?.invoiceEndOn
    )
  ).format(dateFormat)

  return invoiceStartOn && invoiceEndOn
    ? `(${invoiceStartOn} - ${invoiceEndOn})`
    : ''
}

const getDepositAmountVariableInfo = async (
  collectionData,
  collectionName,
  contractInfo
) => {
  const { contractId = '', listingInfo = {} } = collectionData
  let depositAmount = 0

  if (collectionName === 'app_invoices') {
    const depositInsurance = contractId
      ? await depositInsuranceHelper.getADepositInsurance({ contractId })
      : {}

    depositAmount = depositInsurance?.depositAmount
  } else if (size(contractInfo) && !size(listingInfo)) {
    if (
      size(contractInfo.listingInfo) &&
      contractInfo.listingInfo.depositAmount
    )
      depositAmount = contractInfo.listingInfo.depositAmount
  } else {
    if (size(listingInfo) && listingInfo.depositAmount)
      depositAmount = listingInfo.depositAmount
  }

  return { depositAmount }
}

const getDepositInsuranceAmountVariableInfo = async (
  collectionData,
  collectionName
) => {
  const { contractId = '', rentalMeta = {} } = collectionData
  let depositType = ''
  let contractDIAmount = ''
  if (collectionName === 'contracts' && size(rentalMeta)) {
    depositType = rentalMeta.depositType || ''
    contractDIAmount = rentalMeta.depositInsuranceAmount || ''
  }
  const depositInsurance = contractId
    ? await depositInsuranceHelper.getADepositInsurance({ contractId })
    : {}
  let depositInsuranceAmount = ''
  if (
    depositType === 'deposit_insurance' &&
    !size(depositInsurance) &&
    contractDIAmount
  ) {
    depositInsuranceAmount = contractDIAmount
  } else if (size(depositInsurance))
    depositInsuranceAmount = depositInsurance.depositInsuranceAmount
  return { depositInsuranceAmount }
}

const getPayoutToBankAccountVariableInfo = async (
  collectionData,
  collectionName,
  payoutTo
) => {
  if (collectionName === 'contracts') {
    return payoutTo
  } else if (collectionName === 'invoices') {
    const { payoutTo = '' } = collectionData.contractId
      ? await contractHelper.getAContract({
          _id: collectionData.contractId
        })
      : {}
    return payoutTo
  } else if (collectionName === 'payouts') {
    const accountId = await collectionData.getCreditorAccountId()
    return accountId
  }
}

const getBrokeringCommissionAmountVariableInfo = async (
  collectionData,
  collectionName,
  partnerId
) => {
  if (collectionName === 'contracts') {
    const brokeringCommissionAmount =
      getBrokeringCommissionAmount(collectionData) || ''
    return brokeringCommissionAmount
  } else {
    const payoutMeta = collectionData.meta || []
    const landlordInvoiceInfo =
      filter(payoutMeta, ['type', 'landlord_invoice']) || {}
    const brokeringCommissionAmount = await getCommissionAmountByCommissionType(
      'brokering_contract',
      landlordInvoiceInfo,
      collectionData._id
    )

    const convertToCurrencyParams = {
      number: brokeringCommissionAmount,
      options: { isInvoice: true },
      partnerSettingsOrId: partnerId,
      showSymbol: false
    }

    const convertedBrokeringCommissionAmount =
      await appHelper.convertToCurrency(convertToCurrencyParams)

    return convertedBrokeringCommissionAmount
  }
}

const getManagementCommissionAmountVariableInfo = async (
  collectionData,
  collectionName,
  partnerId
) => {
  if (collectionName === 'contracts') {
    const managementCommissionAmount =
      getManagementCommissionAmount(collectionData) || ''
    return managementCommissionAmount
  } else {
    const payoutMeta = collectionData.meta || []
    const landlordInvoiceInfo =
      filter(payoutMeta, ['type', 'landlord_invoice']) || {}
    const managementCommissionAmount =
      await getCommissionAmountByCommissionType(
        'rental_management_contract',
        landlordInvoiceInfo,
        collectionData._id
      )

    const convertToCurrencyParams = {
      number: managementCommissionAmount,
      options: { isInvoice: true },
      partnerSettingsOrId: partnerId,
      showSymbol: false
    }

    const convertedManagementCommissionAmount =
      await appHelper.convertToCurrency(convertToCurrencyParams)

    return convertedManagementCommissionAmount
  }
}

const getRepresentativeNameVariableInfo = async (
  collectionData,
  collectionName,
  contractInfo
) => {
  let contractData = {}

  if (size(contractInfo) && !collectionData?.representativeId)
    contractData = contractInfo
  else if (collectionName === 'contracts') contractData = collectionData

  if (size(contractData)) {
    const representativeUserInfo = (await contractData.representative()) || {}
    const representativeName = size(representativeUserInfo)
      ? representativeUserInfo.getName()
      : ''

    return representativeName
  }

  return ''
}

const getRepresentativeOccupationVariableInfo = async (
  collectionData,
  collectionName,
  contractInfo
) => {
  let contractData = {}

  if (size(contractInfo) && !collectionData?.representativeId)
    contractData = contractInfo
  else if (collectionName === 'contracts') contractData = collectionData

  if (size(contractData)) {
    const representativeUserInfo = (await contractData.representative()) || {}
    const representativeOccupation = size(representativeUserInfo)
      ? representativeUserInfo.getOccupation()
      : ''

    return representativeOccupation
  }

  return ''
}

const getRepresentativePhoneVariableInfo = async (
  collectionData,
  collectionName,
  contractInfo
) => {
  let contractData = {}

  if (size(contractInfo) && !collectionData?.representativeId)
    contractData = contractInfo
  else if (collectionName === 'contracts') contractData = collectionData

  if (size(contractData)) {
    const representativeUserInfo = (await contractData.representative()) || {}
    const representativePhone = size(representativeUserInfo)
      ? representativeUserInfo.getPhone()
      : ''

    return representativePhone
  }

  return ''
}

const getRepresentativeEmailVariableInfo = async (
  collectionData,
  collectionName,
  contractInfo
) => {
  let contractData = {}

  if (size(contractInfo) && !collectionData?.representativeId)
    contractData = contractInfo
  else if (collectionName === 'contracts') contractData = collectionData

  if (size(contractData)) {
    const representativeUserInfo = (await contractData.representative()) || {}
    const representativeEmail = size(representativeUserInfo)
      ? representativeUserInfo.getEmail()
      : ''

    return representativeEmail
  }

  return ''
}

const getInternalLeaseIdVariableInfo = async (
  collectionData,
  collectionName,
  contractInfo
) => {
  let contractData = {}

  if (size(contractInfo) && !size(collectionData.rentalMeta))
    contractData = contractInfo
  else if (collectionName === 'contracts') contractData = collectionData

  if (size(contractData)) {
    const internalLeaseId = contractData.getInternalLeaseId() || ''
    return internalLeaseId
  } else if (
    collectionName === 'invoices' ||
    collectionName === 'app_invoices'
  ) {
    const internalLeaseId = (await collectionData.getInternalLeaseId()) || ''
    return internalLeaseId
  }

  return ''
}

const getInvoiceDueDateVariableInfo = async (
  collectionData,
  contractInfo,
  partnerId
) => {
  const dateFormat = await appHelper.getDateFormat(partnerId)
  let invoiceDueDate = ''
  console.log('== collectionData dueDate ==', collectionData?.dueDate)
  console.log('== contractInfo dueDate ==', contractInfo.dueDate)
  if (size(contractInfo) && !collectionData.dueDate) {
    const dueDate = await appHelper.getActualDate(
      partnerId,
      true,
      contractInfo.dueDate
    )
    invoiceDueDate = dueDate.format(dateFormat)
  } else {
    const dueDate = await appHelper.getActualDate(
      partnerId,
      true,
      collectionData.dueDate
    )
    console.log('== else dueDate ==', dueDate.format())
    invoiceDueDate = dueDate.format(dateFormat)
  }

  return invoiceDueDate || ''
}

const getInvoiceItemsVariableInfo = async (
  collectionData,
  collectionName,
  params
) => {
  const { event, evictionInvoiceIds, sendToUserLang } = params

  if (event === 'eviction_document' && size(evictionInvoiceIds)) {
    let invoiceItems = []

    for (const invoiceId of evictionInvoiceIds) {
      const invoiceInfo = await invoiceHelper.getInvoice({ _id: invoiceId })

      const items = size(invoiceInfo)
        ? await invoiceInfo.getInvoiceItems(sendToUserLang, event)
        : []

      if (size(items)) invoiceItems = concat(invoiceItems, items)
    }

    return invoiceItems
  } else {
    let invoiceItems = []

    if (collectionName === 'invoices') {
      invoiceItems = size(collectionData)
        ? await collectionData.getInvoiceItems(sendToUserLang, event)
        : []
    }

    return invoiceItems
  }
}

const getInvoiceTotalVariableInfo = async (
  collectionData,
  collectionName,
  params
) => {
  const { event, evictionInvoiceIds, partnerId } = params
  let total

  if (event === 'eviction_document' && size(evictionInvoiceIds)) {
    let sumOfInvoiceTotal = 0
    for (const invoiceId of evictionInvoiceIds) {
      const invoiceInfo = await invoiceHelper.getInvoice({ _id: invoiceId })
      const { invoiceTotal = 0 } = invoiceInfo || {}
      sumOfInvoiceTotal += invoiceTotal
    }

    total = sumOfInvoiceTotal
  } else {
    // Where invoiceTotal exists in collection
    total = 0

    if (collectionName === 'invoices') {
      const { rentTotal = 0 } = collectionData || {}
      total = rentTotal
    }

    //Get invoice total with all fees included
    if (
      event === 'send_first_reminder' ||
      event === 'send_second_reminder' ||
      event === 'send_collection_notice' ||
      event === 'send_eviction_notice' ||
      event === 'send_eviction_due_reminder_notice' ||
      event === 'send_eviction_due_reminder_notice_without_eviction_fee' ||
      collectionName === 'app_invoices'
    ) {
      const { invoiceTotal = 0 } = collectionData || {}
      total = invoiceTotal
    }
  }

  const convertToCurrencyParams = {
    number: total,
    options: { isInvoice: true },
    partnerSettingsOrId: partnerId,
    showSymbol: false
  }

  const convertedInvoiceTotal = await appHelper.convertToCurrency(
    convertToCurrencyParams
  )

  return convertedInvoiceTotal
}

const getInvoiceTotalDueVariableInfo = async (
  collectionData,
  collectionName,
  params
) => {
  const { event, evictionInvoiceIds, partnerId } = params

  let totalDue

  if (event === 'eviction_document' && size(evictionInvoiceIds)) {
    let due = 0

    for (const invoiceId of evictionInvoiceIds) {
      const invoiceInfo = await invoiceHelper.getInvoice({ _id: invoiceId })
      if (size(invoiceInfo)) due += (await invoiceInfo.getTotalDueAmount()) || 0
    }
    totalDue = due
  } else {
    if (collectionName === 'invoices' || collectionName === 'app_invoices') {
      totalDue = (await collectionData.getTotalDueAmount()) || 0
    } else {
      const { invoiceId = '' } = collectionData

      const invoiceInfo = invoiceId
        ? await invoiceHelper.getInvoice({ _id: invoiceId })
        : {}

      totalDue = size(invoiceInfo) ? await invoiceInfo.getTotalDueAmount() : 0
    }
  }

  const convertToCurrencyParams = {
    number: totalDue || 0,
    options: { isInvoice: true },
    partnerSettingsOrId: partnerId,
    showSymbol: false
  }

  const convertedTotalDue = await appHelper.convertToCurrency(
    convertToCurrencyParams
  )

  return convertedTotalDue
}

const getEvictionDueDateVariableInfo = async (collectionData, partnerId) => {
  const { evictionNoticeSentOn = null } = collectionData

  const evictionNoticeDate = evictionNoticeSentOn
    ? (
        await appHelper.getActualDate(partnerId, true, evictionNoticeSentOn)
      ).format(await appHelper.getDateFormat(partnerId))
    : ''

  return evictionNoticeDate
}

const getAdministrationOfEvictionFeeVariableInfo = async (
  contractInfo,
  partnerId
) => {
  const administrationEvictionFee = size(contractInfo)
    ? await contractInfo.getAdministrationEvictionFee()
    : 0

  const convertToCurrencyParams = {
    number: administrationEvictionFee,
    options: { isInvoice: true },
    partnerSettingsOrId: partnerId,
    showSymbol: false
  }

  const convertedAdministrationEvictionFee = administrationEvictionFee
    ? await appHelper.convertToCurrency(convertToCurrencyParams)
    : 0

  return convertedAdministrationEvictionFee
}

const getInvoiceEvictionFeeVariableInfo = async (collectionData, partnerId) => {
  const evictionFee = (await collectionData.getEvictionFee()) || 0
  const convertToCurrencyParams = {
    number: evictionFee,
    options: { isInvoice: true },
    partnerSettingsOrId: partnerId,
    showSymbol: false
  }

  const convertedInvoiceEvictionFee = await appHelper.convertToCurrency(
    convertToCurrencyParams
  )

  return convertedInvoiceEvictionFee
}

const getAdministrationEvictionNoticeFeeVariableInfo = async (
  collectionData,
  partnerId
) => {
  const administrationEvictionFee =
    (await collectionData.getAdministrationEvictionFee()) || 0

  const convertToCurrencyParams = {
    number: administrationEvictionFee,
    options: { isInvoice: true },
    partnerSettingsOrId: partnerId,
    showSymbol: false
  }

  const convertedAdministrationEvictionFee = await appHelper.convertToCurrency(
    convertToCurrencyParams
  )

  return convertedAdministrationEvictionFee
}

const getInvoiceIdVariableInfo = async (collectionData, collectionName) => {
  let invoiceId = ''
  // Set credit note invoiceId
  if (
    collectionData.invoiceType === 'credit_note' ||
    collectionData.invoiceType === 'landlord_credit_note'
  ) {
    const oldInvoiceData = collectionData.invoiceId
      ? await invoiceHelper.getInvoice({
          _id: collectionData.invoiceId
        })
      : null

    const { invoiceSerialId = '' } = oldInvoiceData || {}
    invoiceId = invoiceSerialId
  } else if (
    collectionName === 'invoices' ||
    collectionName === 'app_invoices'
  ) {
    invoiceId = collectionData.getInvoiceId()
  }

  return invoiceId || ''
}
const getRentInvoiceDatesVariableInfo = async (
  collectionData,
  variableName,
  partnerId
) => {
  const { contractId } = collectionData
  const invoiceQuery = {
    contractId,
    invoiceType: 'invoice',
    isFirstInvoice: true
  }
  let invoiceQuerySkip = 0
  let invoiceRangeSkip = 0

  if (variableName === 'second_rent_invoice_dates') {
    invoiceQuery.isFirstInvoice = false
    invoiceQuerySkip = 0
    invoiceRangeSkip = 1
  } else if (variableName === 'third_rent_invoice_dates') {
    invoiceQuery.isFirstInvoice = false
    invoiceQuerySkip = 1
    invoiceRangeSkip = 2
  }

  const invoiceInfo = await InvoiceCollection.find(invoiceQuery)
    .skip(invoiceQuerySkip)
    .sort({ createdAt: 1 })

  if (size(invoiceInfo))
    return await getFormattedDateRange(partnerId, invoiceInfo[0])

  const contractInfo = await contractHelper.getAContract({ _id: contractId })
  const invoicesRangeList = await invoiceHelper.getFirstThreeInvoicesRangeList(
    partnerId,
    contractInfo
  )

  const invoiceDateRange =
    size(invoicesRangeList) && size(invoicesRangeList[invoiceRangeSkip])
      ? await getFormattedDateRange(
          partnerId,
          invoicesRangeList[invoiceRangeSkip]
        )
      : ''
  return invoiceDateRange
}

export const getVariablesDataOfFooterText = (event, sendToUserLang) => {
  let footerTextKey = ''

  if (
    event === 'send_invoice' ||
    event === 'send_deposit_insurance_payment_reminder'
  )
    footerTextKey = 'invoice_sent_from'
  else if (event === 'send_first_reminder' || event === 'send_second_reminder')
    footerTextKey = 'reminder_sent_from'
  else if (
    event === 'send_eviction_due_reminder_notice' ||
    event === 'send_eviction_due_reminder_notice_without_eviction_fee'
  )
    footerTextKey = 'eviction_due_reminder_notice_sent_from'
  else if (event === 'send_collection_notice')
    footerTextKey = 'collection_notice_sent_from'
  else if (event === 'send_credit_note') footerTextKey = 'credit_note_sent_from'
  else if (event === 'send_eviction_notice')
    footerTextKey = 'eviction_notice_sent_from'
  else if (event === 'send_landlord_invoice')
    footerTextKey = 'landlord_invoice_sent_from'
  else if (event === 'send_landlord_credit_note')
    footerTextKey = 'landlord_credit_note_invoice_sent_from'
  else if (event === 'send_final_settlement')
    footerTextKey = 'final_settlement_sent_from'
  else if (event === 'send_landlord_annual_statement')
    footerTextKey = 'annual_statement_sent_from'

  const footerText = footerTextKey
    ? appHelper.translateToUserLng(
        'pdf_footer.' + footerTextKey,
        sendToUserLang
      )
    : ''

  return { footer_text: footerText || '' }
}

const getVariablesDataOfAccount = async (variableNames, params) => {
  const { collectionData, collectionName, contractInfo, partnerId } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'account_name') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const { name = '' } = accountInfo || {}

      variablesData[variableName] = name
    } else if (variableName === 'account_address') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const accountAddress = size(accountInfo) ? accountInfo.getAddress() : ''

      variablesData[variableName] = accountAddress || ''
    } else if (variableName === 'account_zip_code') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const accountZipCode = size(accountInfo) ? accountInfo.getZipCode() : ''

      variablesData[variableName] = accountZipCode || ''
    } else if (variableName === 'account_city') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const accountCity = size(accountInfo) ? accountInfo.getCity() : ''

      variablesData[variableName] = accountCity || ''
    } else if (variableName === 'account_country') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const accountCountry = size(accountInfo) ? accountInfo.getCountry() : ''

      variablesData[variableName] = accountCountry || ''
    } else if (variableName === 'account_email') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const { person = {} } = accountInfo || {}
      const accountEmail = size(person) ? person.getEmail() : ''

      variablesData[variableName] = accountEmail || ''
    } else if (variableName === 'account_id') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const { serial = '' } = accountInfo || {}
      const accountId = serial ? '#' + serial : ''

      variablesData[variableName] = accountId
    } else if (variableName === 'account_phonenumber') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const { person = {} } = accountInfo || {}
      const accountPhoneNumber = size(person) ? person.getPhone() : ''

      variablesData[variableName] = accountPhoneNumber
    } else if (variableName === 'account_person_id') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const { person = {} } = accountInfo || {}
      const accountPersonNID = size(person)
        ? person.getNorwegianNationalIdentification()
        : ''

      variablesData[variableName] = accountPersonNID
    } else if (variableName === 'account_org_id') {
      const accountInfo = await getAccountInfoForVariablesData(collectionData)
      const { organization = {} } = accountInfo || {}
      const { orgId = '' } = organization || {}

      variablesData[variableName] = orgId
    } else if (variableName === 'account_bank_account') {
      const contract =
        collectionName === 'contracts' ? collectionData : contractInfo
      const accountBankAccount =
        await getAccountBankAccountInfoForVariablesData(partnerId, contract)

      variablesData[variableName] = accountBankAccount || ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfAgentEsigning = async (
  variableNames,
  collectionData
) => {
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'agent_esigning_url') {
      const { agentAssignmentSigningStatus = {} } = collectionData
      if (
        size(agentAssignmentSigningStatus) &&
        agentAssignmentSigningStatus.internalUrl
      ) {
        variablesData[variableName] =
          appHelper.getLinkServiceURL() +
          '/e-signing/agent_assignment/' +
          collectionData._id +
          '/' +
          agentAssignmentSigningStatus.internalUrl
      } else variablesData[variableName] = ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfAll = async (variableNames, partnerId) => {
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'todays_date') {
      const toDate = await appHelper.getActualDate(partnerId, true, new Date())
      const dateFormats = await appHelper.getDateFormat(partnerId)

      variablesData[variableName] = toDate ? toDate.format(dateFormats) : ''
    } else if (variableName === 'app_logo_url') {
      variablesData[variableName] = appHelper.getDefaultLogoURL(
        'uniteliving_logo_new'
      )
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfAppHealth = async (variableNames, params) => {
  const { appHealthErrors, collectionData, collectionName, event } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'app_url') {
      let adminURL = appHelper.getAdminURL()

      if (event === 'send_app_health_status') {
        adminURL = adminURL + '/admin/app-health'
      }

      variablesData[variableName] = adminURL
    } else if (collectionName === 'app-health') {
      if (variableName === 'error_issues') {
        variablesData[variableName] =
          collectionData.getAppHealthTodayIssues(appHealthErrors)
      } else if (variableName === 'new_issues') {
        variablesData[variableName] =
          await collectionData.getAppHealthNewIssues(appHealthErrors)
      } else if (variableName === 'total_issues') {
        variablesData[variableName] = size(appHealthErrors) || 0
      } else if (variableName === 'all_issues_are_same') {
        variablesData[variableName] = await collectionData.isAllIssuesSame(
          appHealthErrors
        )
      } else if (variableName === 'all_issues_are_new') {
        variablesData[variableName] =
          await collectionData.isAppHealthAllIssuesNew(appHealthErrors)
      }
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfAssignment = async (variableNames, params) => {
  const { collectionData, collectionName, contractInfo, partnerId } = params
  const {
    assignmentFrom = null,
    assignmentTo = null,
    brokeringCommissionAmount = 0,
    brokeringCommissionType = '',
    listingInfo = {},
    payoutTo = '',
    rentalManagementCommissionAmount = 0,
    rentalManagementCommissionType = ''
  } = collectionData
  const { monthlyRentAmount = 0 } = listingInfo

  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'assignment_id') {
      if (collectionName === 'contracts') {
        variablesData[variableName] =
          (await collectionData.getAssignmentSerialNumber()) || ''
      }
    } else if (variableName === 'brokering_commission') {
      variablesData[variableName] = brokeringCommissionAmount
    } else if (variableName === 'management_commission') {
      variablesData[variableName] = rentalManagementCommissionAmount
    } else if (variableName === 'monthly_rent') {
      variablesData[variableName] = monthlyRentAmount
    } else if (variableName === 'deposit_amount') {
      const { depositAmount } = await getDepositAmountVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )

      variablesData.deposit_amount = depositAmount
      variablesData.total_deposit_amount = depositAmount
    } else if (variableName === 'assignment_from') {
      const formattedAssignmentFromDate = assignmentFrom
        ? (
            await appHelper.getActualDate(partnerId, true, assignmentFrom)
          ).format(await appHelper.getDateFormat(partnerId))
        : ''
      variablesData[variableName] = formattedAssignmentFromDate
    } else if (variableName === 'assignment_to') {
      const formattedAssignmentToDate = assignmentTo
        ? (await appHelper.getActualDate(partnerId, true, assignmentTo)).format(
            await appHelper.getDateFormat(partnerId)
          )
        : ''
      variablesData[variableName] = formattedAssignmentToDate
    } else if (variableName === 'payout_to_bank_account') {
      variablesData[variableName] = await getPayoutToBankAccountVariableInfo(
        collectionData,
        collectionName,
        payoutTo
      )
    } else if (variableName === 'brokering_commission_amount') {
      variablesData[variableName] =
        await getBrokeringCommissionAmountVariableInfo(
          collectionData,
          collectionName,
          partnerId
        )
    } else if (variableName === 'brokering_commission_percentage') {
      variablesData[variableName] =
        brokeringCommissionType === 'percent' ? brokeringCommissionAmount : 0
    } else if (variableName === 'management_commission_amount') {
      variablesData[variableName] =
        await getManagementCommissionAmountVariableInfo(
          collectionData,
          collectionName,
          partnerId
        )
    } else if (variableName === 'management_commission_percentage') {
      variablesData[variableName] =
        rentalManagementCommissionType === 'percent'
          ? rentalManagementCommissionAmount
          : 0
    } else if (variableName === 'representative_name') {
      variablesData[variableName] = await getRepresentativeNameVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'representative_occupation') {
      variablesData[variableName] =
        await getRepresentativeOccupationVariableInfo(
          collectionData,
          collectionName,
          contractInfo
        )
    } else if (variableName === 'representative_phone') {
      variablesData[variableName] = await getRepresentativePhoneVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'representative_email') {
      variablesData[variableName] = await getRepresentativeEmailVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'assignment_addons') {
      variablesData[variableName] = await getContractAddons(
        collectionData,
        'assignment'
      )
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfChat = async (variableNames, params) => {
  const { collectionData, collectionName, messageContent } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (
      variableName === 'sent_from_user_name' &&
      collectionName === 'conversation-messages'
    ) {
      const { createdBy = '' } = collectionData
      const userInfo = createdBy
        ? await userHelper.getAnUser({ _id: createdBy })
        : {}
      const { profile = {} } = userInfo || {}
      const { name = '' } = profile || {}

      variablesData[variableName] = name
    } else if (variableName === 'messages') {
      variablesData[variableName] = messageContent
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfContract = async (variableNames, params) => {
  const {
    collectionData,
    collectionName,
    contractInfo,
    event,
    evictionInvoiceIds,
    partnerId,
    sendToUserLang
  } = params
  const { rentalMeta = {} } = collectionData
  const variablesData = {}
  for (const variableName of variableNames) {
    if (variableName === 'lease_start_date') {
      let leaseStartDate = ''

      if (size(contractInfo)) {
        const contractMeta =
          size(contractInfo) && size(contractInfo.rentalMeta)
            ? contractInfo.rentalMeta
            : null
        leaseStartDate = await appHelper.getActualDate(
          partnerId,
          true,
          contractMeta['contractStartDate']
        )
      } else {
        leaseStartDate = await appHelper.getActualDate(
          partnerId,
          true,
          rentalMeta['contractStartDate']
        )
      }
      const dateFormat = await appHelper.getDateFormat(partnerId)

      variablesData[variableName] = !!leaseStartDate
        ? leaseStartDate.format(dateFormat)
        : ''
    } else if (variableName === 'lease_end_date') {
      if (size(contractInfo)) {
        variablesData[variableName] = await getLeaseEndDate(
          partnerId,
          contractInfo,
          sendToUserLang
        )
      } else {
        variablesData[variableName] = await getLeaseEndDate(
          partnerId,
          collectionData,
          sendToUserLang
        )
      }
    } else if (variableName === 'lease_id') {
      if (size(contractInfo) && !collectionData.leaseSerial) {
        variablesData[variableName] =
          (await contractInfo.getLeaseSerialNumber()) || ''
      } else if (collectionName === 'contracts') {
        variablesData[variableName] =
          (await collectionData.getLeaseSerialNumber()) || ''
      }
    } else if (variableName === 'monthly_rent_amount') {
      const convertToCurrencyParams = {
        options: { isInvoice: true },
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }

      if (size(contractInfo)) {
        const monthlyRentAmount = contractInfo.getMonthlyRentAmount() || 0
        convertToCurrencyParams.number = monthlyRentAmount
      } else if (collectionName === 'contracts') {
        const monthlyRentAmount = collectionData.getMonthlyRentAmount() || 0
        convertToCurrencyParams.number = monthlyRentAmount
      }
      const convertedMonthlyRentAmount = await appHelper.convertToCurrency(
        convertToCurrencyParams
      )

      variablesData[variableName] = convertedMonthlyRentAmount
    } else if (variableName === 'deposit_amount') {
      const { depositAmount } = await getDepositAmountVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )

      variablesData.deposit_amount = depositAmount
      variablesData.total_deposit_amount = depositAmount
    } else if (variableName === 'deposit_insurance_amount') {
      const { depositInsuranceAmount = '' } =
        await getDepositInsuranceAmountVariableInfo(
          collectionData,
          collectionName
        ) // contractId
      variablesData.deposit_insurance_amount = depositInsuranceAmount
      variablesData.invoice_amount = depositInsuranceAmount
    } else if (variableName === 'deposit_insurance_reference_number') {
      const { contractId = '' } = collectionData

      const depositInsurance =
        (await depositInsuranceHelper.getADepositInsurance({ contractId })) ||
        {}

      const referenceNumber =
        depositInsurance?.creationResult?.insuranceNo || ''

      variablesData[variableName] = referenceNumber
    } else if (variableName === 'internal_deposit_insurance_reference_number') {
      let partnerSerial = (await collectionData.getPartner())?.serial || ''
      partnerSerial = appHelper.getFixedDigits(partnerSerial, 4)
      let propertySerial = (await collectionData.getProperty())?.serial || ''
      propertySerial = appHelper.getFixedDigits(propertySerial, 5)

      const refNumber = `${partnerSerial}${propertySerial}`

      variablesData[variableName] = refNumber
    } else if (variableName === 'minimum_stay') {
      let contractData = {}
      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      const { rentalMeta = {} } = contractData || {}
      const { minimumStay = 0 } = rentalMeta || {}

      variablesData[variableName] = minimumStay
    } else if (
      variableName === 'future_monthly_rent_amount' ||
      variableName === 'has_future_monthly_rent_amount'
    ) {
      let futureMonthlyRentAmount = 0
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      if (size(contractData)) {
        const { rentalMeta = {} } = contractData
        const { futureRentAmount = 0 } = rentalMeta
        futureMonthlyRentAmount = futureRentAmount
      }

      if (variableName === 'has_future_monthly_rent_amount') {
        variablesData[variableName] = !!futureMonthlyRentAmount
      } else {
        const convertToCurrencyParams = {
          number: futureMonthlyRentAmount,
          options: { isInvoice: true },
          partnerSettingsOrId: partnerId,
          showSymbol: false
        }

        const convertedFutureMonthlyRentAmount =
          await appHelper.convertToCurrency(convertToCurrencyParams)

        variablesData[variableName] = convertedFutureMonthlyRentAmount
      }
    } else if (variableName === 'internal_lease_id') {
      variablesData[variableName] = await getInternalLeaseIdVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'last_CPI_date') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      if (size(contractData))
        variablesData[variableName] =
          (await contractData.getContractLastCpiDate()) || ''
      else variablesData[variableName] = ''
    } else if (variableName === 'next_CPI_date') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      if (size(contractData))
        variablesData[variableName] =
          (await contractData.getContractNextCpiDate()) || ''
      else variablesData[variableName] = ''
    } else if (variableName === 'CPI_from_month') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      if (size(contractData)) {
        const { partnerId = '', rentalMeta = {} } = contractData
        const { cpiFromMonth } = rentalMeta || {}
        const actualCPIFromMonth = await appHelper.getActualDate(
          partnerId,
          true,
          cpiFromMonth
        )

        let formattedDate = ''
        if (sendToUserLang === 'no') {
          const jsDate = new Date(actualCPIFromMonth.format('YYYY-MM-DD'))
          formattedDate = jsDate.toLocaleDateString('nn-No', {
            year: '2-digit',
            month: 'long'
          })
        } else {
          formattedDate = actualCPIFromMonth.format('MMMM YY')
        }

        variablesData[variableName] = formattedDate
      } else variablesData[variableName] = ''
    } else if (variableName === 'CPI_to_month') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      if (size(contractData)) {
        const { partnerId = '', rentalMeta = {} } = contractData
        const { cpiInMonth } = rentalMeta || {}
        const actualCPIInMonth = await appHelper.getActualDate(
          partnerId,
          true,
          cpiInMonth
        )

        let formattedDate = ''
        if (sendToUserLang === 'no') {
          const jsDate = new Date(actualCPIInMonth.format('YYYY-MM-DD'))
          formattedDate = jsDate.toLocaleDateString('nn-No', {
            year: '2-digit',
            month: 'long'
          })
        } else {
          formattedDate = actualCPIInMonth.format('MMMM YY')
        }

        variablesData[variableName] = formattedDate
      } else variablesData[variableName] = ''
    } else if (variableName === 'invoice_due_date') {
      variablesData[variableName] = await getInvoiceDueDateVariableInfo(
        collectionData,
        contractInfo,
        partnerId
      )
    } else if (variableName === 'VAT_status') {
      let vatStatus = 'No'
      const partnerInfo = partnerId
        ? await partnerHelper.getAPartner({ _id: partnerId })
        : {}
      const { accountType = '' } = partnerInfo || {}
      if (accountType === 'broker') {
        const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
          partnerId
        })
        const { bankAccounts = [], bankPayment = {} } = partnerSettings || {}
        const { firstMonthACNo = '' } = bankPayment || {}
        let firstMonthACNoInfo = {}

        if (firstMonthACNo && size(bankAccounts)) {
          firstMonthACNoInfo = find(
            bankAccounts,
            (bankAccountInfo) =>
              bankAccountInfo.accountNumber === firstMonthACNo
          )

          if (size(firstMonthACNoInfo) && firstMonthACNoInfo.vatRegistered)
            vatStatus = 'Yes'
        }
      } else {
        const { accountId = '' } = collectionData
        const account = accountId
          ? await accountHelper.getAnAccount({ _id: accountId })
          : {}
        const { vatRegistered = false } = account || {}
        vatStatus = vatRegistered ? 'Yes' : 'No'
      }

      variablesData[variableName] = vatStatus
    } else if (variableName === 'bank_account_number') {
      const partnerInfo = partnerId
        ? await partnerHelper.getAPartner({ _id: partnerId })
        : {}

      const { accountType = '' } = partnerInfo || {}
      if (accountType === 'broker') {
        variablesData[variableName] =
          (await collectionData.getFirstMonthBankAccountNumber()) || ''
      } else {
        const { accountId = '' } = collectionData || {}
        const account = accountId
          ? await accountHelper.getAnAccount({ _id: accountId })
          : {}
        const { invoiceAccountNumber = '' } = account || {}

        variablesData[variableName] = invoiceAccountNumber
      }
    } else if (variableName === 'notice_in_effect') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      const { rentalMeta = {} } = contractData || {}
      const { noticeInEffect = '' } = rentalMeta || {}
      variablesData[variableName] = noticeInEffect
        ? appHelper.translateToUserLng(
            'properties.' + noticeInEffect,
            sendToUserLang
          )
        : ''
    } else if (variableName === 'notice_period') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      const { rentalMeta = {} } = contractData || {}
      const { noticePeriod = '' } = rentalMeta || {}

      variablesData[variableName] = noticePeriod
    } else if (variableName === 'representative_name') {
      variablesData[variableName] = await getRepresentativeNameVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'representative_occupation') {
      variablesData[variableName] =
        await getRepresentativeOccupationVariableInfo(
          collectionData,
          collectionName,
          contractInfo
        )
    } else if (variableName === 'representative_phone') {
      variablesData[variableName] = await getRepresentativePhoneVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'representative_email') {
      variablesData[variableName] = await getRepresentativeEmailVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'termination_reason') {
      let contractData = {}

      if (size(contractInfo)) contractData = contractInfo
      else if (collectionName === 'contracts') contractData = collectionData

      const { rentalMeta = {} } = contractData || {}
      const { terminateReasons = '' } = rentalMeta || {}

      variablesData[variableName] = terminateReasons
    } else if (variableName === 'lease_addons') {
      variablesData[variableName] = await getContractAddons(
        collectionData,
        'lease'
      )
    } else if (variableName === 'monthly_due_date') {
      const { dueDate = null } = rentalMeta
      let monthlyDueDate = 1

      if (dueDate) monthlyDueDate = dueDate

      variablesData[variableName] = monthlyDueDate
    } else if (variableName === 'invoice_items') {
      variablesData[variableName] = await getInvoiceItemsVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'invoice_total') {
      variablesData[variableName] = await getInvoiceTotalVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'total_due') {
      variablesData[variableName] = await getInvoiceTotalDueVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'eviction_notice_date') {
      variablesData[variableName] = await getEvictionDueDateVariableInfo(
        collectionData,
        partnerId
      )
    } else if (variableName === 'total_due_rent_invoices') {
      let contractInvoiceInfo = null
      //calculate total due for the eviction invoices
      if (event === 'eviction_document' && size(evictionInvoiceIds)) {
        contractInvoiceInfo = size(collectionData)
          ? await contractHelper.getContractInvoiceInfo(collectionData)
          : null
      } else if (size(contractInfo)) {
        contractInvoiceInfo =
          (await contractHelper.getContractInvoiceInfo(contractInfo)) || null
      }
      const { totalDue = 0 } = contractInvoiceInfo || {}

      variablesData[variableName] = totalDue
    } else if (variableName === 'total_overdue_rent_invoices') {
      // Calculate total overdue for the eviction invoices
      const contractInvoiceInfo =
        (await contractHelper.getContractInvoiceInfo(contractInfo, [], true)) ||
        null
      const { totalDue = 0 } = contractInvoiceInfo || {}

      variablesData[variableName] = totalDue
    } else if (variableName === 'lease_signed_date') {
      const { rentalMeta = {} } = contractInfo || {}
      const { signedAt = null } = rentalMeta
      const dateFormats = await appHelper.getDateFormat(partnerId)

      const leaseSignDate = signedAt
        ? (await appHelper.getActualDate(partnerId, true, signedAt)).format(
            dateFormats
          )
        : ''

      variablesData[variableName] = leaseSignDate
    } else if (variableName === 'hiscox_logo_url') {
      variablesData[variableName] = appHelper.getDefaultLogoURL('hiscox-logo')
    } else if (variableName === 'administration_of_eviction_fee') {
      variablesData[variableName] =
        await getAdministrationOfEvictionFeeVariableInfo(
          contractInfo,
          partnerId
        )
    } else if (variableName === 'invoice_eviction_fee') {
      if (collectionName === 'contracts' || collectionName === 'invoices') {
        variablesData[variableName] = await getInvoiceEvictionFeeVariableInfo(
          collectionData,
          partnerId
        )
      } else variablesData[variableName] = ''
    } else if (variableName === 'administration_eviction_notice_fee') {
      if (collectionName === 'contracts' || collectionName === 'invoices') {
        variablesData[variableName] =
          await getAdministrationEvictionNoticeFeeVariableInfo(
            collectionData,
            partnerId
          )
      } else variablesData[variableName] = ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfCreditNote = async (variableNames, params) => {
  const { collectionData, collectionName, contractInfo, event, partnerId } =
    params
  const landlordReminderAndCollectionNoticeEvent = [
    'send_landlord_due_reminder',
    'send_landlord_first_reminder',
    'send_landlord_second_reminder',
    'send_landlord_collection_notice'
  ]
  const variablesData = {}

  for (const variableName of variableNames) {
    if (
      variableName === 'invoice_id' ||
      variableName === 'final_settlement_claim_id'
    ) {
      variablesData[variableName] = await getInvoiceIdVariableInfo(
        collectionData,
        collectionName
      )

      if (
        collectionData.isFinalSettlement &&
        indexOf(landlordReminderAndCollectionNoticeEvent, event) !== -1
      ) {
        variablesData.invoice_id = ''
      }
    } else if (variableName === 'credit_invoice_id') {
      let invoiceId = ''

      if (collectionName === 'invoices' || collectionName === 'app_invoices') {
        invoiceId = collectionData.getInvoiceId()
      }

      variablesData[variableName] = invoiceId || ''
    } else if (variableName === 'credit_note_date') {
      let invoiceCreatedDate = ''

      if (
        collectionName === 'invoices' ||
        collectionName === 'app_invoices' ||
        collectionName === 'payouts'
      ) {
        invoiceCreatedDate = (await collectionData.createdDateText()) || ''
      }

      variablesData[variableName] = invoiceCreatedDate || ''
    } else if (variableName === 'credit_note_start_date') {
      const dateFormat = await appHelper.getDateFormat(partnerId)
      const invoiceStartDate = await appHelper.getActualDate(
        partnerId,
        true,
        collectionData.invoiceStartOn
      )

      variablesData[variableName] = invoiceStartDate.format(dateFormat)
    } else if (variableName === 'credit_note_end_date') {
      const dateFormat = await appHelper.getDateFormat(partnerId)
      const invoiceEndDate = await appHelper.getActualDate(
        partnerId,
        true,
        collectionData.invoiceEndOn
      )

      variablesData[variableName] = invoiceEndDate.format(dateFormat)
    } else if (variableName === 'credit_note_due_date') {
      variablesData[variableName] = await getInvoiceDueDateVariableInfo(
        collectionData,
        contractInfo,
        partnerId
      )
    } else if (variableName === 'credit_note_total') {
      variablesData[variableName] = await getInvoiceTotalVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'credit_note_account_number') {
      const { invoiceAccountNumber = '' } = collectionData

      variablesData[variableName] = invoiceAccountNumber
    } else if (variableName === 'credit_note_items') {
      variablesData[variableName] = await getInvoiceItemsVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'credit_reason') {
      variablesData[variableName] = collectionData.creditReason || ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfCreditRating = async (variableNames, params) => {
  const { partnerId, tenantId, token } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    console.log('++> Checking for variableName: ', variableName)
    if (variableName === 'credit_rating_ask_url') {
      // const url =
      //     (await appHelper.getPartnerURL(partnerId)) +
      //     '/credit_rating/ask/' +
      //     tenantId +
      //     '?token=' +
      //     token
      const v1PartnerUrl = await appHelper.getPartnerURL(partnerId, true)
      console.log('++> Checking for v1PartnerUrl: ', v1PartnerUrl)
      const v2PartnerUrl = await appHelper.getPartnerURL(partnerId, false)
      console.log('++> Checking for v1PartnerUrl: ', v2PartnerUrl)
      const v1_url =
        v1PartnerUrl + '/credit_rating/ask/' + tenantId + '?token=' + token
      console.log('++> Checking for v1_url: ', v1_url)
      const v2_url =
        v2PartnerUrl + '/credit_rating/ask/' + tenantId + '?token=' + token
      console.log('++> Checking for v2_url: ', v2_url)
      const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${v1_url}`
      console.log('++> Checking for linkForV1AndV2: ', linkForV1AndV2)
      const creditRatingAskUrl =
        appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`
      console.log('++> Checking for creditRatingAskUrl: ', creditRatingAskUrl)
      variablesData[variableName] = creditRatingAskUrl || ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfDeposit = async (variableNames, params) => {
  const {
    collectionData,
    currentBalance,
    depositAccountId,
    partnerId,
    paymentAmount,
    paymentDate,
    paymentReference,
    tenantId
  } = params
  const variablesData = {}
  for (const variableName of variableNames) {
    if (variableName === 'deposit_bank_account_number') {
      const depositAccountInfo = depositAccountId
        ? await depositAccountHelper.getDepositAccount({
            _id: depositAccountId
          })
        : {}
      const { bankAccountNumber = '' } = depositAccountInfo || {}

      variablesData[variableName] = bankAccountNumber
    } else if (variableName === 'payment_amount') {
      let convertedPaymentAmount = 0
      if (paymentAmount) {
        const convertToCurrencyParams = {
          number: paymentAmount,
          partnerSettingsOrId: partnerId,
          showSymbol: false
        }
        convertedPaymentAmount = await appHelper.convertToCurrency(
          convertToCurrencyParams
        )
      }
      variablesData[variableName] = convertedPaymentAmount
    } else if (variableName === 'total_payment_amount') {
      const depositAccountInfo = depositAccountId
        ? await depositAccountHelper.getDepositAccount({
            _id: depositAccountId
          })
        : {}
      const { totalPaymentAmount = 0 } = depositAccountInfo || {}
      if (totalPaymentAmount) {
        const convertToCurrencyParams = {
          number: totalPaymentAmount,
          partnerSettingsOrId: partnerId,
          showSymbol: false
        }
        const convertedTotalPaymentAmount = await appHelper.convertToCurrency(
          convertToCurrencyParams
        )
        variablesData[variableName] = convertedTotalPaymentAmount
      } else variablesData[variableName] = totalPaymentAmount
    } else if (variableName === 'payment_reference') {
      variablesData[variableName] = paymentReference
    } else if (variableName === 'tenant_deposit_amount') {
      const kycFormData = await depositAccountHelper.getTenantDepositKycData({
        partnerId,
        tenantId,
        contractId: collectionData._id
      })
      const { depositAmount = 0 } = kycFormData || {}
      const convertToCurrencyParams = {
        number: depositAmount,
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }
      const convertedTenantDepositAmount = await appHelper.convertToCurrency(
        convertToCurrencyParams
      )

      variablesData[variableName] = convertedTenantDepositAmount
    } else if (variableName === 'current_balance') {
      let convertedCurrentBalance = 0
      if (currentBalance) {
        const convertToCurrencyParams = {
          number: currentBalance,
          partnerSettingsOrId: partnerId,
          showSymbol: false
        }
        convertedCurrentBalance = await appHelper.convertToCurrency(
          convertToCurrencyParams
        )
      }
      variablesData[variableName] = convertedCurrentBalance
    } else if (variableName === 'payment_date') {
      const formattedPaymentDate = paymentDate
        ? (await appHelper.getActualDate(partnerId, true, paymentDate)).format(
            await appHelper.getDateFormat(partnerId)
          )
        : ''

      variablesData[variableName] = formattedPaymentDate
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfDownload = async (variableNames, params) => {
  const { collectionData, fileId, partnerId } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'download_context') {
      variablesData[variableName] = appHelper.translateToUserLng(
        'templates.download_context.' + collectionData.title
      )
    } else if (variableName === 'download_url') {
      // Todo :: Will be update URL later
      // Get fileUrlHash from file collection and create url for download link on email template
      if (fileId) {
        const fileQuery = {
          _id: fileId,
          partnerId,
          fileUrlHash: { $exists: true }
        }
        const fileInfo = (await fileHelper.getAFile(fileQuery)) || {}
        const { fileUrlHash = '', type = '' } = fileInfo || {}

        const partnerInfo = await partnerHelper.getAPartner(
          { _id: partnerId },
          null,
          ['owner']
        )
        const { owner = {} } = partnerInfo || {}
        const loginVersion = size(owner) ? owner.getLoginVersion() : ''

        if (type === 'xml_attachment') {
          if (loginVersion === 'v2') {
            variablesData.download_url = fileUrlHash
              ? appHelper.getLinkServiceURL() +
                '/' +
                partnerId +
                '/api/download-xml/' +
                fileUrlHash
              : ''
          } else {
            variablesData.download_url = fileUrlHash
              ? (await getPartnerURLForV1(partnerId)) +
                '/api/download-xml/' +
                fileUrlHash
              : ''
          }
        } else {
          if (loginVersion === 'v2') {
            variablesData.download_url = fileUrlHash
              ? appHelper.getLinkServiceURL() +
                '/' +
                partnerId +
                '/api/download-excel/' +
                fileUrlHash
              : ''
          } else {
            variablesData.download_url = fileUrlHash
              ? (await getPartnerURLForV1(partnerId)) +
                '/api/download-excel/' +
                fileUrlHash
              : ''
          }
        }
        variablesData.has_download_url = !!fileUrlHash
      }
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfEstimatedPayouts = async (variableNames, params) => {
  const { collectionData, collectionName, partnerId } = params

  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'first_estimated_payout') {
      let firstEstimatedPayout = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.firstMonth) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.firstMonth,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          firstEstimatedPayout = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = firstEstimatedPayout
    } else if (variableName === 'second_estimated_payout') {
      let secondEstimatedPayout = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.secondMonth) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.secondMonth,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          secondEstimatedPayout = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = secondEstimatedPayout
    } else if (variableName === 'third_estimated_payout') {
      let thirdEstimatedPayout = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.thirdMonth) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.thirdMonth,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          thirdEstimatedPayout = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = thirdEstimatedPayout
    } else if (
      variableName === 'first_estimated_payout_management_commission'
    ) {
      let firstEstimatedPayoutManagementCommission = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.firstMonthManagementCommission
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.firstMonthManagementCommission,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          firstEstimatedPayoutManagementCommission =
            await appHelper.convertToCurrency(convertToCurrencyParams)
        }
      }

      variablesData[variableName] = firstEstimatedPayoutManagementCommission
    } else if (
      variableName === 'second_estimated_payout_management_commission'
    ) {
      let secondEstimatedPayoutManagementCommission = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.secondMonthManagementCommission
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.secondMonthManagementCommission,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          secondEstimatedPayoutManagementCommission =
            await appHelper.convertToCurrency(convertToCurrencyParams)
        }
      }

      variablesData[variableName] = secondEstimatedPayoutManagementCommission
    } else if (
      variableName === 'third_estimated_payout_management_commission'
    ) {
      let thirdEstimatedPayoutManagementCommission = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.thirdMonthManagementCommission
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.thirdMonthManagementCommission,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          thirdEstimatedPayoutManagementCommission =
            await appHelper.convertToCurrency(convertToCurrencyParams)
        }
      }

      variablesData[variableName] = thirdEstimatedPayoutManagementCommission
    } else if (variableName === 'first_estimated_payout_addons') {
      let firstEstimatedPayoutAddons = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.firstMonthPayoutAddons) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.firstMonthPayoutAddons,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          firstEstimatedPayoutAddons = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = firstEstimatedPayoutAddons
    } else if (variableName === 'second_estimated_payout_addons') {
      let secondEstimatedPayoutAddons = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.secondMonthPayoutAddons
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.secondMonthPayoutAddons,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          secondEstimatedPayoutAddons = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = secondEstimatedPayoutAddons
    } else if (variableName === 'third_estimated_payout_addons') {
      let thirdEstimatedPayoutAddons = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.thirdMonthPayoutAddons) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.thirdMonthPayoutAddons,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          thirdEstimatedPayoutAddons = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = thirdEstimatedPayoutAddons
    } else if (variableName === 'estimated_brokering_commission') {
      let estimatedBrokeringCommissionAmount = 0

      const amount = await getEstimatedBrokeringCommissionAmount(collectionData)

      if (amount) {
        const convertToCurrencyParams = {
          number: amount,
          options: { isInvoice: true },
          partnerSettingsOrId: partnerId,
          showSymbol: false
        }

        estimatedBrokeringCommissionAmount = await appHelper.convertToCurrency(
          convertToCurrencyParams
        )
      }

      variablesData[variableName] = estimatedBrokeringCommissionAmount
    } else if (variableName === 'estimated_brokering_commission_percentage') {
      variablesData[variableName] =
        (await getEstimatedBrokeringCommissionPercentage(collectionData)) || 0
    } else if (variableName === 'estimated_management_commission_percentage') {
      variablesData[variableName] =
        (await getEstimatedManagementCommissionPercentage(collectionData)) || 0
    } else if (variableName === 'first_estimated_addons_commission') {
      let firstEstimatedAddonsCommission = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.firstMonthAddonsCommission
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.firstMonthAddonsCommission,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          firstEstimatedAddonsCommission = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = firstEstimatedAddonsCommission
    } else if (variableName === 'second_estimated_addons_commission') {
      let secondEstimatedAddonsCommission = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.secondMonthAddonsCommission
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.secondMonthAddonsCommission,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          secondEstimatedAddonsCommission = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = secondEstimatedAddonsCommission
    } else if (variableName === 'third_estimated_addons_commission') {
      let thirdEstimatedAddonsCommission = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.thirdMonthAddonsCommission
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.thirdMonthAddonsCommission,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          thirdEstimatedAddonsCommission = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = thirdEstimatedAddonsCommission
    } else if (variableName === 'second_amount_moved_from_last_payout') {
      let secondAmountMovedFromLastPayout = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.secondAmountMovedFromLastPayout
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.secondAmountMovedFromLastPayout,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          secondAmountMovedFromLastPayout = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = secondAmountMovedFromLastPayout
    } else if (variableName === 'third_amount_moved_from_last_payout') {
      let thirdAmountMovedFromLastPayout = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (
          size(estimatedPayouts) &&
          estimatedPayouts.thirdAmountMovedFromLastPayout
        ) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.thirdAmountMovedFromLastPayout,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          thirdAmountMovedFromLastPayout = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = thirdAmountMovedFromLastPayout
    } else if (variableName === 'first_rent_invoice') {
      let firstRentInvoice = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.firstRentInvoice) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.firstRentInvoice,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          firstRentInvoice = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = firstRentInvoice
    } else if (variableName === 'second_rent_invoice') {
      let secondRentInvoice = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.secondRentInvoice) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.secondRentInvoice,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          secondRentInvoice = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = secondRentInvoice
    } else if (variableName === 'third_rent_invoice') {
      let thirdRentInvoice = 0

      if (collectionName === 'invoices') {
        const estimatedPayouts =
          (await collectionData.getEstimatedPayouts()) || {}
        if (size(estimatedPayouts) && estimatedPayouts.thirdRentInvoice) {
          const convertToCurrencyParams = {
            number: estimatedPayouts.thirdRentInvoice,
            options: { isInvoice: true },
            partnerSettingsOrId: partnerId,
            showSymbol: false
          }

          thirdRentInvoice = await appHelper.convertToCurrency(
            convertToCurrencyParams
          )
        }
      }

      variablesData[variableName] = thirdRentInvoice
    } else if (variableName === 'payout_to_bank_account') {
      variablesData[variableName] = await getPayoutToBankAccountVariableInfo(
        collectionData,
        collectionName
      )
    } else if (
      variableName === 'first_rent_invoice_dates' ||
      variableName === 'second_rent_invoice_dates' ||
      variableName === 'third_rent_invoice_dates'
    ) {
      variablesData[variableName] = await getRentInvoiceDatesVariableInfo(
        collectionData,
        variableName,
        partnerId
      )
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfInvoiceOrAppInvoice = async (variableNames, params) => {
  const {
    collectionName,
    collectionData,
    contractInfo,
    doc,
    event,
    partnerId,
    sendToUserLang
  } = params
  const { invoiceAccountNumber = '', kidNumber = '' } = collectionData

  const landlordReminderAndCollectionNoticeEvent = [
    'send_landlord_due_reminder',
    'send_landlord_first_reminder',
    'send_landlord_second_reminder',
    'send_landlord_collection_notice'
  ]
  const variablesData = {}

  for (const variableName of variableNames) {
    if (
      variableName === 'invoice_id' ||
      variableName === 'final_settlement_claim_id'
    ) {
      variablesData[variableName] = await getInvoiceIdVariableInfo(
        collectionData,
        collectionName
      )

      if (
        collectionData.isFinalSettlement &&
        indexOf(landlordReminderAndCollectionNoticeEvent, event) !== -1
      ) {
        variablesData.invoice_id = ''
      }
    } else if (variableName === 'invoice_date') {
      let invoiceCreatedDate = ''

      if (
        collectionName === 'invoices' ||
        collectionName === 'app_invoices' ||
        collectionName === 'payouts'
      ) {
        invoiceCreatedDate = (await collectionData.createdDateText()) || ''
      }

      variablesData[variableName] = invoiceCreatedDate || ''
    } else if (variableName === 'invoice_start_date') {
      const dateFormat = await appHelper.getDateFormat(partnerId)
      const invoiceStartDate = await appHelper.getActualDate(
        partnerId,
        true,
        collectionData.invoiceStartOn
      )

      variablesData[variableName] = invoiceStartDate.format(dateFormat)
    } else if (variableName === 'invoice_end_date') {
      const dateFormat = await appHelper.getDateFormat(partnerId)
      const invoiceEndDate = await appHelper.getActualDate(
        partnerId,
        true,
        collectionData.invoiceEndOn
      )

      variablesData[variableName] = invoiceEndDate.format(dateFormat)
    } else if (variableName === 'invoice_due_date') {
      variablesData[variableName] = await getInvoiceDueDateVariableInfo(
        collectionData,
        contractInfo,
        partnerId
      )
    } else if (variableName === 'invoice_total') {
      variablesData[variableName] = await getInvoiceTotalVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (
      variableName === 'invoice_total_round' ||
      variableName === 'invoice_total_cent'
    ) {
      // Where invoiceTotal exists in collection
      const { invoiceTotal = 0 } = collectionData
      const total = invoiceTotal.toFixed(2)

      if (variableName === 'invoice_total_round')
        variablesData[variableName] = total.split('.')[0]
      if (variableName === 'invoice_total_cent')
        variablesData[variableName] = total.split('.')[1]
    } else if (variableName === 'invoice_kid_number') {
      variablesData[variableName] = kidNumber
    } else if (variableName === 'invoice_account_number') {
      variablesData[variableName] = invoiceAccountNumber
    } else if (variableName === 'invoice_reminder_fee') {
      if (collectionName === 'invoices' || collectionName === 'app_invoices')
        variablesData[variableName] = collectionData.getInvoiceReminderFee()
    } else if (variableName === 'reminder_date') {
      if (collectionName === 'invoices' || collectionName === 'app_invoices') {
        let reminderDate = await collectionData.getFirstReminderDate()

        if (event === 'send_second_reminder')
          reminderDate = await collectionData.getSecondReminderDate()

        variablesData[variableName] = reminderDate
      }
    } else if (variableName === 'collection_notice_date') {
      if (collectionName === 'invoices')
        variablesData[variableName] =
          await collectionData.getCollectionNoticeDate()
    } else if (variableName === 'invoice_items') {
      variablesData[variableName] = await getInvoiceItemsVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'collection_notice_due_date') {
      const { collectionNoticeDueDate = '', partnerId = '' } = collectionData
      let formattedCollectionNoticeDueDate = ''
      if (collectionNoticeDueDate) {
        const dateFormat = await appHelper.getDateFormat(partnerId)
        formattedCollectionNoticeDueDate = moment(
          collectionNoticeDueDate
        ).format(dateFormat)
      }

      variablesData[variableName] = formattedCollectionNoticeDueDate || ''
    } else if (variableName === 'invoice_eviction_fee') {
      if (collectionName === 'contracts' || collectionName === 'invoices') {
        variablesData[variableName] = await getInvoiceEvictionFeeVariableInfo(
          collectionData,
          partnerId
        )
      } else variablesData[variableName] = ''
    } else if (variableName === 'internal_lease_id') {
      variablesData[variableName] = await getInternalLeaseIdVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )
    } else if (variableName === 'administration_eviction_notice_fee') {
      if (collectionName === 'contracts' || collectionName === 'invoices') {
        variablesData[variableName] =
          await getAdministrationEvictionNoticeFeeVariableInfo(
            collectionData,
            partnerId
          )
      } else variablesData[variableName] = ''
    } else if (variableName === 'total_paid') {
      const { totalBalanced = 0, totalPaid = 0 } = collectionData
      const paidTotal = totalPaid + totalBalanced

      const convertToCurrencyParams = {
        number: paidTotal,
        options: { isInvoice: true },
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }

      const convertedTotalPaid = await appHelper.convertToCurrency(
        convertToCurrencyParams
      )

      variablesData[variableName] = convertedTotalPaid
    } else if (variableName === 'total_due') {
      variablesData[variableName] = await getInvoiceTotalDueVariableInfo(
        collectionData,
        collectionName,
        params
      )
    } else if (variableName === 'is_exceed_attached_file_size') {
      let exceedAttachedFileSize = false

      if (collectionName === 'invoices' && size(doc)) {
        const { isExceedAttachedFileSize } =
          await getCorrectionInvoiceAttachmentInfo(doc._id, sendToUserLang)

        exceedAttachedFileSize = isExceedAttachedFileSize
      }

      variablesData[variableName] = exceedAttachedFileSize
    } else if (variableName === 'eviction_notice_date') {
      variablesData[variableName] = await getEvictionDueDateVariableInfo(
        collectionData,
        partnerId
      )
    } else if (variableName === 'eviction_fee') {
      const evictionFee = size(contractInfo)
        ? await contractInfo.getEvictionFee()
        : 0

      const convertToCurrencyParams = {
        number: evictionFee,
        options: { isInvoice: true },
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }
      const convertedEvictionFee = evictionFee
        ? await appHelper.convertToCurrency(convertToCurrencyParams)
        : 0

      variablesData[variableName] = convertedEvictionFee
    } else if (variableName === 'administration_of_eviction_fee') {
      variablesData[variableName] =
        await getAdministrationOfEvictionFeeVariableInfo(
          contractInfo,
          partnerId
        )
    } else if (variableName === 'deposit_insurance_amount') {
      const { depositInsuranceAmount } =
        await getDepositInsuranceAmountVariableInfo(
          collectionData,
          collectionName
        ) // contractId

      variablesData.deposit_insurance_amount = depositInsuranceAmount
      variablesData.invoice_amount = depositInsuranceAmount
    } else if (variableName === 'deposit_amount') {
      const { depositAmount } = await getDepositAmountVariableInfo(
        collectionData,
        collectionName,
        contractInfo
      )

      variablesData.deposit_amount = depositAmount
      variablesData.total_deposit_amount = depositAmount
    } else if (
      variableName === 'app_org_name' ||
      variableName === 'app_org_id' ||
      variableName === 'app_org_address'
    ) {
      const appInfo = await appHelper.getSettingsInfoByFieldName('appInfo')

      variablesData.app_org_name = appInfo?.companyName || ''
      variablesData.app_org_id = appInfo?.organizationId || ''
      variablesData.app_org_address = appInfo?.address || ''
    } else if (variableName === 'total_overdue_rent_invoices') {
      // Calculate total overdue for the eviction invoices
      const contractInvoiceInfo =
        (await contractHelper.getContractInvoiceInfo(contractInfo, [], true)) ||
        null
      const { totalDue = 0 } = contractInvoiceInfo || {}

      variablesData[variableName] = totalDue
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfLandlordAnnualStatement = async (
  variableNames,
  collectionData
) => {
  const { annualStatementId = '' } = collectionData
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'report_year') {
      const annualStatementInfo = annualStatementId
        ? await annualStatementHelper.getAnnualStatement({
            _id: annualStatementId
          })
        : {}

      const { statementYear = '' } = annualStatementInfo || {}

      variablesData[variableName] = statementYear
    } else if (variableName === 'annual_statement_items') {
      const annualStatementItems =
        await getAnnualStatementItemsForVariablesData(annualStatementId)

      variablesData[variableName] = annualStatementItems
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfLandlordEsigning = async (
  variableNames,
  collectionData
) => {
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'landlord_esigning_url') {
      const { landlordAssignmentSigningStatus = {} } = collectionData
      if (
        size(landlordAssignmentSigningStatus) &&
        landlordAssignmentSigningStatus.internalUrl
      ) {
        variablesData[variableName] =
          appHelper.getLinkServiceURL() +
          '/e-signing/landlord_assignment/' +
          collectionData._id +
          '/' +
          landlordAssignmentSigningStatus.internalUrl
      } else variablesData[variableName] = ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfLandlordLeaseEsigning = async (
  variableNames,
  collectionData
) => {
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'landlord_lease_esigning_url') {
      const { rentalMeta = {} } = collectionData
      const { landlordLeaseSigningStatus = {} } = rentalMeta

      if (
        size(landlordLeaseSigningStatus) &&
        landlordLeaseSigningStatus.internalUrl
      ) {
        variablesData[variableName] =
          appHelper.getLinkServiceURL() +
          '/e-signing/landlord_lease/' +
          collectionData._id +
          '/' +
          landlordLeaseSigningStatus.internalUrl
      } else variablesData[variableName] = ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfLandlordMovingInOutEsigning = async (
  variableNames,
  event,
  movingId
) => {
  const variablesData = {}

  for (const variableName of variableNames) {
    if (
      variableName === 'landlord_moving_in_esigning_url' ||
      variableName === 'landlord_moving_out_esigning_url'
    ) {
      if (movingId) {
        const params = { event, movingId, sendTo: 'landlord' }

        variablesData[variableName] = await getMovingInOutEsigningUrl(params)
      }
    }
  } // Ends of For Loops
  return variablesData
}

const getVariablesDataOfListing = async (variableNames, params) => {
  const { collectionData, collectionName, emailInfo } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'has_listing') {
      if (collectionName === 'conversation-messages') {
        const { conversationId = '' } = collectionData
        const conversation = conversationId
          ? await conversationHelper.getAConversation({ _id: conversationId })
          : {}

        const { listingId = '', propertyId = '' } = conversation || {}

        if (listingId || propertyId) {
          const listingInfo = await conversation.getListingInfo()
          const listingImgUrl = listingInfo?.getListingFirstImage()

          const listingMinimumStay =
            appHelper.translateToUserLng(
              'listing_preview.min_stay',
              emailInfo?.language
            ) +
            ' : ' +
            appHelper.translateToUserLng(
              'listings.minimum_stay',
              emailInfo?.language,
              { count: listingInfo.getMinimumStay() }
            )
          const listingTypeInfo = (await listingInfo.listingTypeInfo()) || {}
          const propertyTypeInfo = (await listingInfo.propertyTypeInfo()) || {}

          const listingTypeAndPropertyName =
            appHelper.translateToUserLng(
              'listing_and_property_types.' + listingTypeInfo?.name,
              emailInfo?.language
            ) +
            ' ' +
            appHelper.translateToUserLng('listing_preview.in') +
            ' ' +
            appHelper.translateToUserLng(
              'listing_and_property_types.' + propertyTypeInfo?.name,
              emailInfo?.language
            )

          const availabilityStartDateText =
            await listingInfo.availabilityStartDateText()
          const availabilityEndDateText =
            await listingInfo.availabilityEndDateText()

          const listingAvailability =
            availabilityStartDateText + ' - ' + availabilityEndDateText
          const listingLocation = listingInfo?.location?.name

          if (size(listingInfo)) variablesData.has_listing = true
          if (size(listingImgUrl))
            variablesData.listing_image_url = listingImgUrl.fullUrl || ''
          if (listingLocation) variablesData.listing_location = listingLocation
          if (listingTypeAndPropertyName)
            variablesData.listing_type_and_property_name =
              listingTypeAndPropertyName
          if (listingAvailability)
            variablesData.listing_availability = listingAvailability
          if (listingMinimumStay)
            variablesData.listing_minimum_stay = listingMinimumStay
        }
      }
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfMovingInOutEsigning = async (variableNames, params) => {
  const { collectionData, collectionName } = params
  const variablesData = {}

  if (collectionName !== 'property-items') return variablesData

  for (const variableName of variableNames) {
    if (variableName === 'rooms') {
      const rooms = (await collectionData.getRooms()) || []
      if (size(rooms)) variablesData[variableName] = rooms
    } else if (variableName === 'furniture') {
      const inventory = (await collectionData.getInventory()) || []
      if (size(inventory)) variablesData[variableName] = inventory
    } else if (variableName === 'keys') {
      const keys = collectionData.getKeys() || []
      if (size(keys)) variablesData[variableName] = keys
    } else if (variableName === 'meterReading') {
      const meterReading = (await collectionData.getMeterReading()) || []
      if (meterReading) variablesData[variableName] = meterReading
    } else if (variableName === 'inventoryImages') {
      const inventoryImages = (await collectionData.getInventoryImages()) || []
      if (size(inventoryImages)) variablesData[variableName] = inventoryImages
    } else if (variableName === 'keyImages') {
      const keysImages = (await collectionData.getKeysImages()) || []
      if (size(keysImages)) variablesData[variableName] = keysImages
    } else if (variableName === 'meterReadingImages') {
      const meterReadingImages =
        (await collectionData.getMeterReadingImages()) || []
      if (size(meterReadingImages))
        variablesData[variableName] = meterReadingImages
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfPartner = async (variableNames, params) => {
  const { collectionData, collectionName, event, partnerId } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'partner_name') {
      const partnerName = await getPartnerNameForVariablesData(
        event,
        collectionData
      )

      variablesData[variableName] = partnerName || ''
    } else if (variableName === 'partner_logo_url') {
      const partnerInfo = await collectionData.getPartner() // Where partnerId exists in collection
      const { accountType = '' } = partnerInfo || {}

      if (accountType === 'direct') {
        const accountInfo = await getAccountInfoForVariablesData(collectionData)
        const { organization = {} } = accountInfo || {}
        const logo = size(organization) ? organization.getLogo() : ''

        variablesData[variableName] = logo || ''
      } else {
        const logo = size(partnerInfo) ? partnerInfo.getLogo() : ''

        variablesData[variableName] = logo || ''
      }
    } else if (variableName === 'partner_id') {
      const partnerInfo = await collectionData.getPartner()
      const { serial = 0 } = partnerInfo || {}

      variablesData[variableName] = serial ? '#' + serial : ''
    } else if (
      variableName === 'partner_address' ||
      variableName === 'partner_zip_code' ||
      variableName === 'partner_city' ||
      variableName === 'partner_country'
    ) {
      const partnerAddressParams = { collectionData, event }

      if (variableName === 'partner_address')
        partnerAddressParams.addressType = 'Address'
      else if (variableName === 'partner_zip_code')
        partnerAddressParams.addressType = 'ZipCode'
      else if (variableName === 'partner_city')
        partnerAddressParams.addressType = 'City'
      else if (variableName === 'partner_country')
        partnerAddressParams.addressType = 'Country'

      const partnerAddressInfo = await getPartnerAddressInfoForVariablesData(
        partnerAddressParams
      )

      variablesData[variableName] = partnerAddressInfo
    } else if (variableName === 'partner_org_id') {
      const partnerOrgId = await getPartnerOrdIdForVariablesData(
        event,
        collectionData
      )

      variablesData[variableName] = partnerOrgId || ''
    } else if (variableName === 'branch_name') {
      const branchInfo = collectionData.branchId
        ? await collectionData.getBranch()
        : {} // Where branchId exists in collection
      const { name = '' } = branchInfo || {}

      variablesData[variableName] = name
    } else if (
      variableName === 'agent_name' ||
      variableName === 'manager_name' ||
      variableName === 'agent_email' ||
      variableName === 'agent_phonenumber' ||
      variableName === 'agent_occupation'
    ) {
      const isFieldExists =
        collectionName === 'property-items'
          ? collectionData.contractId
          : collectionData.agentId
      const agentInfo = isFieldExists ? await collectionData.getAgent() : {} // Where agentId exists in collection

      let variableValue = ''

      if (size(agentInfo)) {
        if (variableName === 'agent_email')
          variableValue = agentInfo.getEmail() || ''
        else if (variableName === 'agent_phonenumber')
          variableValue = agentInfo.getPhone() || ''
        else if (variableName === 'agent_occupation')
          variableValue = agentInfo.getOccupation() || ''
        else variableValue = agentInfo.getName() || ''
      }

      variablesData[variableName] = variableValue || ''
    } else if (variableName === 'partner_url') {
      variablesData[variableName] =
        (await appHelper.getPartnerPublicURL(partnerId)) || ''
    } else if (variableName === 'partner_bank_account') {
      const partnerSettings = await partnerSettingHelper.getAPartnerSetting(
        { partnerId },
        null,
        ['partner']
      )
      const partnerBankAccount =
        getPartnerBankAccountInfoForVariablesData(partnerSettings)

      variablesData[variableName] = partnerBankAccount || ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfPayout = async (variableNames, params) => {
  const { collectionData, collectionName, partnerId } = params
  const { _id, bookingDate = null, meta = [], payoutTo = '' } = collectionData
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'payout_total') {
      const { amount = 0 } = collectionData

      const convertToCurrencyParams = {
        number: amount,
        options: { isInvoice: true },
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }

      const convertedPayoutTotal = await appHelper.convertToCurrency(
        convertToCurrencyParams
      )
      variablesData[variableName] = convertedPayoutTotal
    } else if (variableName === 'brokering_commission_amount') {
      variablesData[variableName] =
        await getBrokeringCommissionAmountVariableInfo(
          collectionData,
          collectionName,
          partnerId
        )
    } else if (variableName === 'management_commission_amount') {
      variablesData[variableName] =
        await getManagementCommissionAmountVariableInfo(
          collectionData,
          collectionName,
          partnerId
        )
    } else if (variableName === 'payout_addons') {
      const payoutMeta = meta || []
      const landlordInvoiceInfo =
        filter(payoutMeta, ['type', 'landlord_invoice']) || {}
      const addonsCommissionAmount = await getCommissionAmountByCommissionType(
        'addon_commission',
        landlordInvoiceInfo,
        _id
      )
      const convertToCurrencyParams = {
        number: addonsCommissionAmount,
        options: { isInvoice: true },
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }

      const convertedAddonsCorrections = await appHelper.convertToCurrency(
        convertToCurrencyParams
      )

      variablesData[variableName] = convertedAddonsCorrections
    } else if (variableName === 'payout_corrections') {
      const landlordInvoiceInfo =
        filter(meta, ['type', 'landlord_invoice']) || {}
      const addonsCorrectionAmount = await getCommissionAmountByCommissionType(
        'addon',
        landlordInvoiceInfo,
        _id
      )

      const convertToCurrencyParams = {
        number: addonsCorrectionAmount,
        options: { isInvoice: true },
        partnerSettingsOrId: partnerId,
        showSymbol: false
      }

      const convertedPayoutCorrections = await appHelper.convertToCurrency(
        convertToCurrencyParams
      )

      variablesData[variableName] = convertedPayoutCorrections
    } else if (variableName === 'payout_paid_by_bank_date') {
      if (bookingDate) {
        const dateFormat = await appHelper.getDateFormat(partnerId)
        const paidByBankDate = await appHelper.getActualDate(
          partnerId,
          true,
          bookingDate
        )
        variablesData[variableName] = paidByBankDate.format(dateFormat)
      }
    } else if (variableName === 'payout_to_bank_account') {
      variablesData[variableName] = await getPayoutToBankAccountVariableInfo(
        collectionData,
        collectionName,
        payoutTo
      )
    } else if (variableName === 'payout_from_bank_account') {
      if (collectionName === 'payouts') {
        variablesData[variableName] = await collectionData.getDebtorAccountId()
      }
    } else if (variableName === 'last_unpaid_payouts') {
      let lastUnpaidPayouts = 0

      if (collectionName === 'payouts')
        lastUnpaidPayouts = (await collectionData.getLastUnpaidPayouts()) || 0

      variablesData[variableName] = lastUnpaidPayouts
    } else if (variableName === 'addons') {
      const payoutMeta = meta || []
      const landlordInvoiceInfo =
        filter(payoutMeta, ['type', 'landlord_invoice']) || {}
      const payoutAddons = await getPayoutAddons(
        'addon',
        landlordInvoiceInfo,
        _id
      )
      const { addons = [], addonsTotal = 0 } = payoutAddons || {}

      variablesData.addons = addons
      variablesData.addon_total = addonsTotal
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfPendingPayments = async (variableNames, params) => {
  const { partnerId, paymentsApprovalESigningURL, userId } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'user_name') {
      const userInfo = userId ? await userHelper.getAnUser({ _id: userId }) : {}
      const name = size(userInfo) ? userInfo.getName() : ''

      variablesData[variableName] = name
    } else if (variableName === 'pending_payments_url') {
      // variablesData[variableName] =
      //   (await appHelper.getPartnerURL(partnerId, true)) +
      //   '/dtms/payments?show-pending-payments=true'
      if (process.env.NODE_ENV === 'test')
        // TODO: Remove this when V2 is using permanently
        variablesData[variableName] =
          (await appHelper.getPartnerURL(partnerId, true)) +
          '/dtms/payments?show-pending-payments=true'
      else {
        const v1Url =
          (await appHelper.getPartnerURL(partnerId, true)) +
          '/dtms/payments?show-pending-payments=true'
        const v2Url =
          (await appHelper.getPartnerURL(partnerId, false)) +
          '/invoices/payment?show-pending-payments=true'
        const linkForV1AndV2 = `redirect?v2_url=${v2Url}&v1_url=${v1Url}`
        variablesData[variableName] =
          appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`
      }
    } else if (variableName === 'payments_approval_esigning_url') {
      variablesData[variableName] = paymentsApprovalESigningURL
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfPendingPayouts = async (variableNames, params) => {
  const { partnerId, payoutsApprovalESigningURL, userId } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'user_name') {
      const userInfo = userId ? await userHelper.getAnUser({ _id: userId }) : {}
      const name = size(userInfo) ? userInfo.getName() : ''

      variablesData[variableName] = name
    } else if (variableName === 'pending_payouts_url') {
      // variablesData[variableName] =
      //   (await appHelper.getPartnerURL(partnerId, true)) +
      //   '/dtms/payouts?show-pending-payouts=true'
      if (process.env.NODE_ENV === 'test')
        // TODO: Remove this when V2 is using permanently
        variablesData[variableName] =
          (await appHelper.getPartnerURL(partnerId, true)) +
          '/dtms/payouts?show-pending-payouts=true'
      else {
        const v1Url =
          (await appHelper.getPartnerURL(partnerId, true)) +
          '/dtms/payouts?show-pending-payouts=true'
        const v2Url =
          (await appHelper.getPartnerURL(partnerId, false)) +
          '/invoices/payouts?show-pending-payouts=true'
        const linkForV1AndV2 = `redirect?v2_url=${v2Url}&v1_url=${v1Url}`
        variablesData[variableName] =
          appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`
      }
    } else if (variableName === 'payouts_approval_esigning_url') {
      variablesData[variableName] = payoutsApprovalESigningURL
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfProperty = async (variableNames, params) => {
  const { collectionData, collectionName, partnerId, tenantId } = params
  const { _id } = collectionData
  const variablesData = {}

  for (const variableName of variableNames) {
    if (
      variableName === 'property_location' ||
      variableName === 'property_zip_code' ||
      variableName === 'property_city' ||
      variableName === 'property_country'
    ) {
      const propertyLocationParams = { collectionData, collectionName }

      if (variableName === 'property_location')
        propertyLocationParams.locationType = 'name'
      else if (variableName === 'property_zip_code')
        propertyLocationParams.locationType = 'postalCode'
      else if (variableName === 'property_city')
        propertyLocationParams.locationType = 'city'
      else if (variableName === 'property_country')
        propertyLocationParams.locationType = 'country'

      const propertyLocationInfo =
        await getPropertyLocationInfoForVariablesData(propertyLocationParams)

      variablesData[variableName] = propertyLocationInfo
    } else if (variableName === 'property_id') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { serial = '' } = propertyInfo || {}

      variablesData[variableName] = serial ? '#' + serial : ''
    } else if (variableName === 'property_gnr') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { gnr = '' } = propertyInfo || {}

      variablesData[variableName] = gnr
    } else if (variableName === 'property_bnr') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { bnr = '' } = propertyInfo || {}

      variablesData[variableName] = bnr
    } else if (variableName === 'property_snr') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { snr = '' } = propertyInfo || {}

      variablesData[variableName] = snr
    } else if (variableName === 'property_number_of_bedrooms') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { noOfBedrooms = '' } = propertyInfo || {}

      variablesData[variableName] = noOfBedrooms
    } else if (variableName === 'property_livingroom_yes_or_no') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { livingRoom = false } = propertyInfo || {}

      variablesData[variableName] = livingRoom ? 'Yes' : 'No'
    } else if (variableName === 'property_kitchen_yes_or_no') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { kitchen = false } = propertyInfo || {}

      variablesData[variableName] = kitchen ? 'Yes' : 'No'
    } else if (variableName === 'property_furnished_yes_or_no') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { livingRoomFurnished = false } = propertyInfo || {}

      variablesData[variableName] = livingRoomFurnished ? 'Yes' : 'No'
    } else if (variableName === 'property_municipality') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )

      const { location = {} } = propertyInfo || {}
      const { city = '', sublocality = '' } = location

      variablesData[variableName] = city
        ? sublocality
          ? city + ' ' + sublocality
          : city
        : ''
    } else if (variableName === 'apartment_id') {
      const propertyInfo = await getPropertyInfoForVariablesData(
        collectionData,
        collectionName
      )
      const { apartmentId = '' } = propertyInfo || {}

      variablesData[variableName] = apartmentId
    } else if (variableName === 'interestform_url') {
      const urlParams = {
        urlParamsV1:
          '/dtms/properties/' + _id + '?interestTenantId=' + tenantId,
        urlParamsV2:
          '/property/properties/' + _id + '?interestTenantId=' + tenantId
      }
      variablesData[variableName] =
        (await prepareRedirectUrlForLinkService(partnerId, urlParams, false)) ||
        ''
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfTask = async (variableNames, params) => {
  const {
    assignTo,
    collectionData,
    collectionName,
    collectionId,
    partnerId,
    taskId
  } = params
  const { content = '' } = collectionData
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'assignee_name') {
      const userId = assignTo[0]
      const userInfo = userId ? await userHelper.getAnUser({ _id: userId }) : {}
      const name = size(userInfo) ? userInfo.getName() : ''

      variablesData[variableName] = name
    } else if (variableName === 'assigned_by') {
      const taskQueryId = taskId || collectionId
      const taskInfo = taskQueryId
        ? await taskHelper.getATask({ _id: taskQueryId }, null, ['user'])
        : {}
      const { user = {} } = taskInfo || {}
      const name = size(user) ? user.getName() : ''

      variablesData[variableName] = name
    } else if (variableName === 'task_title') {
      let taskQueryId = collectionId

      if (collectionName === 'comments' && taskId) taskQueryId = taskId

      const taskInfo = taskQueryId
        ? await taskHelper.getATask({ _id: taskQueryId })
        : {}
      const { title = '' } = taskInfo || {}

      variablesData[variableName] = title
    } else if (variableName === 'task_url') {
      let taskQueryId = collectionId

      if (collectionName === 'comments' && taskId) taskQueryId = taskId

      variablesData[variableName] =
        (await appHelper.getPartnerURL(partnerId, false)) +
        '/dashboard?taskId=' +
        taskQueryId
    } else if (variableName === 'comment') {
      variablesData[variableName] = content
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfTenant = async (variableNames, params) => {
  const { collectionData, collectionName, partnerId, tenantId } = params
  const variablesData = {}

  for (const variableName of variableNames) {
    if (
      variableName === 'tenant_name' ||
      variableName === 'jointly_liable_tenant_name'
    ) {
      const tenantInfo = await getTenantInfoForVariablesData(
        collectionData,
        collectionName,
        tenantId
      )
      const { name = '' } = tenantInfo || {}

      variablesData[variableName] = name
    } else if (variableName === 'tenant_id') {
      const tenantInfo = await getTenantInfoForVariablesData(
        collectionData,
        collectionName,
        tenantId
      )
      const { serial = '' } = tenantInfo || {}

      variablesData[variableName] = serial
    } else if (variableName === 'tenant_serial_id') {
      const partnerInfo = await collectionData.getPartner()
      const tenantInfo = await getTenantInfoForVariablesData(
        collectionData,
        collectionName,
        tenantId
      )

      const partnerSerial = appHelper.getFixedDigits(partnerInfo?.serial, 4)
      const tenantSerial = appHelper.getFixedDigits(tenantInfo?.serial, 5)

      variablesData[variableName] = `${partnerSerial}${tenantSerial}`
    } else if (
      variableName === 'tenant_address' ||
      variableName === 'tenant_zip_code' ||
      variableName === 'tenant_city' ||
      variableName === 'tenant_country'
    ) {
      let propertyInfo = {}
      let tenantInfo = {}

      if (collectionName === 'listings') {
        propertyInfo = collectionData
        if (tenantId)
          tenantInfo = (await tenantHelper.getATenant({ _id: tenantId })) || {}
      } else {
        propertyInfo = await collectionData.getProperty()
        tenantInfo =
          collectionName !== 'tenants'
            ? await collectionData.getTenant()
            : collectionData.getTenant()
      }
      const { location: propertyAddress = {} } = propertyInfo || {}
      const {
        city = '',
        country = '',
        name = '',
        postalCode = ''
      } = propertyAddress || {}
      const { billingAddress: tenantAddress = '' } = tenantInfo || {}

      if (variableName === 'tenant_address') {
        variablesData[variableName] = tenantAddress ? tenantAddress : name
      } else if (variableName === 'tenant_zip_code') {
        variablesData[variableName] =
          tenantAddress && tenantInfo && tenantInfo.getZipCode()
            ? tenantInfo.getZipCode()
            : postalCode
      } else if (variableName === 'tenant_city') {
        variablesData[variableName] =
          tenantAddress && tenantInfo && tenantInfo.getCity()
            ? tenantInfo.getCity()
            : city
      } else if (variableName === 'tenant_country') {
        variablesData[variableName] =
          tenantAddress && tenantInfo && tenantInfo.getCountry()
            ? tenantInfo.getCountry()
            : country
      }
    } else if (
      variableName === 'tenant_email' ||
      variableName === 'jointly_liable_tenant_email'
    ) {
      const tenantInfo =
        (await getTenantInfoForVariablesData(
          collectionData,
          collectionName,
          tenantId
        )) || {}

      let userInfo = {}
      if (size(tenantInfo)) userInfo = await tenantInfo.getUser()
      const email = size(userInfo) ? userInfo.getEmail() : ''

      variablesData[variableName] = email
    } else if (variableName === 'tenant_phonenumber') {
      const tenantInfo =
        (await getTenantInfoForVariablesData(
          collectionData,
          collectionName,
          tenantId
        )) || {}

      let userInfo = {}
      if (size(tenantInfo)) userInfo = await tenantInfo.getUser()
      const phoneNumber = size(userInfo) ? userInfo.getPhone() : ''

      variablesData[variableName] = phoneNumber
    } else if (variableName === 'tenant_person_id') {
      const tenantInfo =
        (await getTenantInfoForVariablesData(
          collectionData,
          collectionName,
          tenantId
        )) || {}

      let userInfo = {}
      if (size(tenantInfo)) userInfo = await tenantInfo.getUser()
      const norwegianNationalIdentification = size(userInfo)
        ? userInfo.getNorwegianNationalIdentification()
        : ''

      variablesData[variableName] = norwegianNationalIdentification
    } else if (variableName === 'tenants') {
      if (collectionName === 'invoices' || collectionName === 'app_invoices') {
        const invoiceContractInfo =
          (await collectionData.getInvoiceContractInfo()) || {}
        const tenantsItems = (await invoiceContractInfo.getTenantsItems()) || []

        variablesData[variableName] = tenantsItems
      } else if (collectionName === 'contracts') {
        variablesData[variableName] =
          (await collectionData.getTenantsItems()) || []
      } else {
        variablesData[variableName] = []
      }
    } else if (variableName === 'new_password_url') {
      if (collectionName === 'contracts') {
        // Todo :: Will be update URL later
        const { rentalMeta = {} } = collectionData
        const { tenantId = '' } = rentalMeta
        const tenantInfo = tenantId
          ? await tenantHelper.getATenant({ _id: tenantId }, null, ['user'])
          : {}
        const { user = {} } = tenantInfo || {}
        const loginVersion = size(user) ? user.getLoginVersion() : ''
        const email = size(user) ? user.getEmail() : ''
        const hasPassword = size(user) ? user.hasPassword() : ''

        if (!hasPassword && user._id)
          if (loginVersion === 'v2') {
            variablesData[variableName] =
              appHelper.getLinkServiceURL() +
              '/' +
              'create-new-password/?u=' +
              user._id +
              '&e=' +
              email +
              '&pid=' +
              partnerId
          } else {
            // For V1
            const token = await createVerificationToken(
              user._id,
              partnerId,
              email
            )
            // variablesData[variableName] =
            //   (await appHelper.getPartnerURL(partnerId)) +
            //   '/create-new-password/?u=' +
            //   user._id +
            //   '&t=' +
            //   token +
            //   '&pid=' +
            //   partnerId
            const urlParams = {
              urlParamsV1:
                '/create-new-password/?u=' +
                user._id +
                '&t=' +
                token +
                '&pid=' +
                partnerId,
              urlParamsV2:
                '/create-new-password/?u=' +
                user._id +
                '&t=' +
                token +
                '&pid=' +
                partnerId
            }
            console.log(' checking urlParams', urlParams)

            const newPasswordUrl =
              (await prepareRedirectUrlForLinkService(
                partnerId,
                urlParams,
                true
              )) || '' // redirects to link service
            console.log(' checking newPasswordUrl', newPasswordUrl)
            variablesData[variableName] = newPasswordUrl
          }
      }
    } else if (
      variableName === 'jointly_liable_tenant_names' ||
      variableName === 'jointly_liable_tenant_emails' ||
      variableName === 'jointly_liable_tenant_person_IDs'
    ) {
      const multiTenantIds = await getTenantIdsFromCollectionData(
        collectionData,
        collectionName
      )

      const multiTenantNameOrEmailOrPersonId =
        await getTenantsNameOrEmailOrPersonId(multiTenantIds, variableName)

      variablesData[variableName] = multiTenantNameOrEmailOrPersonId
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfTenantLeaseEsigning = async (
  variableNames,
  collectionData,
  collectionName
) => {
  const variablesData = {}

  for (const variableName of variableNames) {
    if (variableName === 'tenant_lease_esigning_url') {
      let tenantLeaseEsigningUrl = ''

      if (collectionName === 'contracts') {
        tenantLeaseEsigningUrl =
          (await collectionData.getTenantLeaseEsigningUrl()) || ''
      }

      variablesData[variableName] = tenantLeaseEsigningUrl
    }
  } // Ends of For Loops

  return variablesData
}

const getVariablesDataOfUser = async (variableNames, params) => {
  const {
    collectionData,
    collectionName,
    emailInfo,
    partnerId,
    tenantId,
    conversation
  } = params
  // const { conversationId } = collectionData
  const variablesData = {}
  for (const variableName of variableNames) {
    if (variableName === 'has_password') {
      // Todo:: Will be update url later
      if (collectionName === 'conversation-messages') {
        if (emailInfo.id) {
          let sendToUserInfo = {}
          if (size(conversation)) {
            sendToUserInfo = await getUserInfoByConversation(
              conversation,
              emailInfo.id
            )
          } else {
            sendToUserInfo = await collectionData?.sendToUserInfo(emailInfo.id)
          }
          const hasPassword =
            size(sendToUserInfo) && sendToUserInfo.hasPassword()
              ? sendToUserInfo.hasPassword()
              : false
          if (hasPassword) {
            // const stage = process.env.STAGE || 'local'
            variablesData.has_password = true
            variablesData.reply_link = await prepareReplyLink(
              collectionData,
              sendToUserInfo
            )
            // variablesData.reply_link =
            //   stage === 'production'
            //     ? `https://uniteliving.com/chat/${conversationId}`
            //     : `https://${stage}.uniteliving.com/chat/${conversationId}`
          }
        }
      }
    } else if (variableName === 'user_invitation_url') {
      // Only for V1
      if (tenantId) {
        const tenantInfo = await tenantHelper.getATenant(
          { _id: tenantId },
          null,
          ['user']
        )
        const { user = {} } = tenantInfo || {}
        const token = await createVerificationToken(user._id, partnerId, '')
        let invitationVerificationUrl =
          '/acceptInvitation/?u=' + user._id + '&t=' + token

        if (partnerId && user.hasPassword()) {
          invitationVerificationUrl += '&pid=' + partnerId
        }
        variablesData[variableName] = await prepareRedirectUrlForLinkService(
          partnerId,
          {
            urlParamsV1: invitationVerificationUrl,
            urlParamsV2: invitationVerificationUrl
          },
          true
        )
      }
    }
  } // Ends of For Loops

  return variablesData
}

export const getVariablesData = async (event, collectionData, params) => {
  if (!size(collectionData) || !event) return {}
  try {
    const {
      appHealthErrors = 0, // => appHealth
      assignTo = [], // => task
      collectionName = '', // *
      collectionId = '', // => task
      contractId = '', // Needed
      currentBalance = '', // => deposit
      depositAccountId = '', // => deposit
      doc = {}, // Needed => invoice || app_invoice
      emailInfo = {}, // => listings, user
      evictionInvoiceIds = [], // => contract, credit_note, invoice || app_invoice
      fileId = '', // => download
      messageContent = '', // => chat
      movingId = '', // => landlord_moving_in_esigning, landlord_moving_out_esigning
      partnerId = '', // *
      paymentAmount = '', // deposit
      paymentsApprovalESigningURL = '', // pending_payments
      paymentDate = '', // deposit
      paymentReference = '', // deposit
      payoutsApprovalESigningURL = '', // pending_payouts
      sendToUserLang = 'no', // => footer_text, contract, credit_note, invoice || app_invoice
      taskId = '', // => task
      tenantId = '', // => credit_rating, deposit, property, tenant, user
      token = '', // => credit_rating
      userId = '', // pending_payments, pending_payouts,
      conversation = {}
    } = params

    const variablesDataArray = []

    // For footer_text variable
    const allowedEventForFooterText = [
      'send_invoice',
      'send_first_reminder',
      'send_second_reminder',
      'send_collection_notice',
      'send_credit_note',
      'send_eviction_notice',
      'send_eviction_due_reminder_notice',
      'send_landlord_invoice',
      'send_landlord_credit_note',
      'send_final_settlement',
      'send_landlord_annual_statement',
      'send_deposit_incoming_payment',
      'send_deposit_account_created',
      'send_deposit_insurance_payment_reminder',
      'send_deposit_insurance_created'
    ]
    // Getting variablesData for footer_text
    if (indexOf(allowedEventForFooterText, event) !== -1) {
      const variablesData = getVariablesDataOfFooterText(event, sendToUserLang)
      if (size(variablesData)) variablesDataArray.push(variablesData)
    }

    // Getting AllowedVariableContext
    const allowedVariableContext = getAllowedVariableContext(event) || []
    if (!size(allowedVariableContext))
      return size(variablesDataArray) ? variablesDataArray[0] : {} // !AllowedVariableContext then return only footer_text

    const contractInfo = contractId
      ? await contractHelper.getAContract({ _id: contractId })
      : collectionName !== 'contracts' && collectionData.contractId
      ? await contractHelper.getAContract({ _id: collectionData.contractId })
      : {}
    // contractInfo => account, assignment, contract, credit_note, invoice || app_invoice
    // Update collectionData tenantId by newTenantId for updated main tenant variables data
    const { newTenantId = '' } = doc
    if (newTenantId) collectionData.tenantId = newTenantId
    // Set contractId  => assignment
    if (
      !collectionData.contractId &&
      collectionName === 'contracts' &&
      collectionData._id
    )
      collectionData.contractId = collectionData._id

    // Getting variablesData based on AllowedVariableContext
    // FYI
    // agent_moving_in_esigning & agent_moving_out_esigning &
    // tenant_moving_in_esigning & tenant_moving_out_esigning is not required here,
    // Though we are getting those from userSendToInfo of Agent & Tenant

    // Account
    if (includes(allowedVariableContext, 'account')) {
      const accountVariables =
        notificationTemplateHelper.getTemplateVariablesForAccount() // Getting Default Template VariablesInfo
      const { variables = {} } = accountVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          contractInfo,
          partnerId
        }
        const variablesData = await getVariablesDataOfAccount(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Agent Esigning
    if (includes(allowedVariableContext, 'agent_esigning')) {
      const agentEsigningVariables =
        notificationTemplateHelper.getTemplateVariablesForAgentEsigning() // Getting Default Template VariablesInfo
      const { variables = {} } = agentEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData = await getVariablesDataOfAgentEsigning(
          variableNames,
          collectionData
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // All
    if (includes(allowedVariableContext, 'all')) {
      const allVariables =
        notificationTemplateHelper.getTemplateVariablesForAll() // Getting Default Template VariablesInfo
      const { variables = {} } = allVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData = await getVariablesDataOfAll(
          variableNames,
          partnerId
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // AppHealth
    if (includes(allowedVariableContext, 'appHealth')) {
      const appHealthVariables =
        notificationTemplateHelper.getTemplateVariablesForAppHealth() // Getting Default Template VariablesInfo
      const { variables = {} } = appHealthVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          appHealthErrors,
          collectionData,
          collectionName,
          event
        }
        const variablesData = await getVariablesDataOfAppHealth(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Assignment
    if (includes(allowedVariableContext, 'assignment')) {
      const assignmentVariables =
        notificationTemplateHelper.getTemplateVariablesForAssignment() // Getting Default Template VariablesInfo
      const { variables = {} } = assignmentVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          contractInfo,
          partnerId
        }
        const variablesData = await getVariablesDataOfAssignment(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Chat
    if (includes(allowedVariableContext, 'chat')) {
      const chatVariables =
        notificationTemplateHelper.getTemplateVariablesForChat() // Getting Default Template VariablesInfo
      const { variables = {} } = chatVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { collectionData, collectionName, messageContent }
        const variablesData = await getVariablesDataOfChat(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Contract
    if (includes(allowedVariableContext, 'contract')) {
      const contractVariables =
        notificationTemplateHelper.getTemplateVariablesForContract() // Getting Default Template VariablesInfo
      const { variables = {} } = contractVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          contractInfo,
          event,
          evictionInvoiceIds,
          partnerId,
          sendToUserLang
        }
        const variablesData = await getVariablesDataOfContract(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Credit_Note
    if (includes(allowedVariableContext, 'credit_note')) {
      const creditNoteVariables =
        notificationTemplateHelper.getTemplateVariablesForCreditNote() // Getting Default Template VariablesInfo
      const { variables = {} } = creditNoteVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          contractInfo,
          partnerId,
          event,
          evictionInvoiceIds,
          sendToUserLang
        }
        const variablesData = await getVariablesDataOfCreditNote(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Credit_Rating
    if (includes(allowedVariableContext, 'credit_rating')) {
      const creditRatingVariables =
        notificationTemplateHelper.getTemplateVariablesForCreditRating() // Getting Default Template VariablesInfo
      const { variables = {} } = creditRatingVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          partnerId,
          tenantId,
          token
        }
        const variablesData = await getVariablesDataOfCreditRating(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Deposit
    if (includes(allowedVariableContext, 'deposit')) {
      const depositVariables =
        notificationTemplateHelper.getTemplateVariablesForDeposit() // Getting Default Template VariablesInfo
      const { variables = {} } = depositVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          currentBalance,
          depositAccountId,
          partnerId,
          paymentAmount,
          paymentDate,
          paymentReference,
          tenantId
        }
        const variablesData = await getVariablesDataOfDeposit(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Download
    if (includes(allowedVariableContext, 'download')) {
      const downloadVariables =
        notificationTemplateHelper.getTemplateVariablesForDownload() // Getting Default Template VariablesInfo
      const { variables = {} } = downloadVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          fileId,
          partnerId
        }
        const variablesData = await getVariablesDataOfDownload(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Estimated_Payouts
    if (includes(allowedVariableContext, 'estimated_payouts')) {
      const estimatedPayoutsVariables =
        notificationTemplateHelper.getTemplateVariablesForEstimatedPayouts() // Getting Default Template VariablesInfo
      const { variables = {} } = estimatedPayoutsVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          partnerId
        }
        const variablesData = await getVariablesDataOfEstimatedPayouts(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Invoice || AppInvoice
    if (
      includes(allowedVariableContext, 'invoice') ||
      includes(allowedVariableContext, 'app_invoice')
    ) {
      const invoiceOrAppInvoiceVariables =
        notificationTemplateHelper.getTemplateVariablesForInvoiceOrAppInvoice() // Getting Default Template VariablesInfo
      const { variables = {} } = invoiceOrAppInvoiceVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionName,
          collectionData,
          contractInfo,
          doc,
          evictionInvoiceIds,
          event,
          partnerId,
          sendToUserLang
        }
        const variablesData = await getVariablesDataOfInvoiceOrAppInvoice(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Landlord_Annual_Statement
    if (includes(allowedVariableContext, 'landlord_annual_statement')) {
      const landlordAnnualStatementVariables =
        notificationTemplateHelper.getTemplateVariablesForLandlordAnnualStatement() // Getting Default Template VariablesInfo
      const { variables = {} } = landlordAnnualStatementVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData = await getVariablesDataOfLandlordAnnualStatement(
          variableNames,
          collectionData
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Landlord_Esigning
    if (includes(allowedVariableContext, 'landlord_esigning')) {
      const landlordEsigningVariables =
        notificationTemplateHelper.getTemplateVariablesForLandlordEsigning() // Getting Default Template VariablesInfo
      const { variables = {} } = landlordEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData = await getVariablesDataOfLandlordEsigning(
          variableNames,
          collectionData
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Landlord_Lease_Esigning
    if (includes(allowedVariableContext, 'landlord_lease_esigning')) {
      const landlordLeaseEsigningVariables =
        notificationTemplateHelper.getTemplateVariablesForLandlordLeaseEsigning() // Getting Default Template VariablesInfo
      const { variables = {} } = landlordLeaseEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData = await getVariablesDataOfLandlordLeaseEsigning(
          variableNames,
          collectionData
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Landlord_Moving_In_Esigning
    if (includes(allowedVariableContext, 'landlord_moving_in_esigning')) {
      const landlordMovingInEsigningVariables =
        notificationTemplateHelper.getTemplateVariablesForLandlordMovingInEsigning() // Getting Default Template VariablesInfo
      const { variables = {} } = landlordMovingInEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData =
          await getVariablesDataOfLandlordMovingInOutEsigning(
            variableNames,
            event,
            movingId
          )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Landlord_Moving_Out_Esigning
    if (includes(allowedVariableContext, 'landlord_moving_out_esigning')) {
      const landlordMovingOutEsigningVariables =
        notificationTemplateHelper.getTemplateVariablesForLandlordMovingOutEsigning() // Getting Default Template VariablesInfo
      const { variables = {} } = landlordMovingOutEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData =
          await getVariablesDataOfLandlordMovingInOutEsigning(
            variableNames,
            event,
            movingId
          )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Listing
    if (includes(allowedVariableContext, 'listing')) {
      const listingVariables =
        notificationTemplateHelper.getTemplateVariablesForListing() // Getting Default Template VariablesInfo
      const { variables = {} } = listingVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { collectionData, collectionName, emailInfo }
        const variablesData = await getVariablesDataOfListing(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Moving_In_Esigning || Moving_Out_Esigning
    if (
      includes(allowedVariableContext, 'moving_in_esigning') ||
      includes(allowedVariableContext, 'moving_out_esigning')
    ) {
      let movingInOutEsigningVariables = []

      if (includes(allowedVariableContext, 'moving_in_esigning')) {
        movingInOutEsigningVariables =
          notificationTemplateHelper.getTemplateVariablesForMovingInEsigning() // Getting Default Template VariablesInfo
      } else {
        movingInOutEsigningVariables =
          notificationTemplateHelper.getTemplateVariablesForMovingOutEsigning() // Getting Default Template VariablesInfo
      }
      const { variables = {} } = movingInOutEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { collectionData, collectionName }
        const variablesData = await getVariablesDataOfMovingInOutEsigning(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Partner
    if (includes(allowedVariableContext, 'partner')) {
      const partnerVariables =
        notificationTemplateHelper.getTemplateVariablesForPartner() // Getting Default Template VariablesInfo
      const { variables = {} } = partnerVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { collectionData, collectionName, event, partnerId }
        const variablesData = await getVariablesDataOfPartner(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Payout
    if (includes(allowedVariableContext, 'payout')) {
      const payoutVariables =
        notificationTemplateHelper.getTemplateVariablesForPayout() // Getting Default Template VariablesInfo
      const { variables = {} } = payoutVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { collectionData, collectionName, partnerId }
        const variablesData = await getVariablesDataOfPayout(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Pending_Payments
    if (includes(allowedVariableContext, 'pending_payments')) {
      const pendingPaymentsVariables =
        notificationTemplateHelper.getTemplateVariablesForPendingPayments() // Getting Default Template VariablesInfo
      const { variables = {} } = pendingPaymentsVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { partnerId, paymentsApprovalESigningURL, userId }
        const variablesData = await getVariablesDataOfPendingPayments(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Pending_Payouts
    if (includes(allowedVariableContext, 'pending_payouts')) {
      const pendingPayoutsVariables =
        notificationTemplateHelper.getTemplateVariablesForPendingPayouts() // Getting Default Template VariablesInfo
      const { variables = {} } = pendingPayoutsVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { partnerId, payoutsApprovalESigningURL, userId }
        const variablesData = await getVariablesDataOfPendingPayouts(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Property
    if (includes(allowedVariableContext, 'property')) {
      const propertyVariables =
        notificationTemplateHelper.getTemplateVariablesForProperty() // Getting Default Template VariablesInfo
      const { variables = {} } = propertyVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = { collectionData, collectionName, partnerId, tenantId }
        const variablesData = await getVariablesDataOfProperty(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Task
    if (includes(allowedVariableContext, 'task')) {
      const taskVariables =
        notificationTemplateHelper.getTemplateVariablesForTask() // Getting Default Template VariablesInfo
      const { variables = {} } = taskVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          assignTo,
          collectionData,
          collectionName,
          collectionId,
          partnerId,
          taskId
        }
        const variablesData = await getVariablesDataOfTask(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Tenant
    if (includes(allowedVariableContext, 'tenant')) {
      const tenantVariables =
        notificationTemplateHelper.getTemplateVariablesForTenant() // Getting Default Template VariablesInfo
      const { variables = {} } = tenantVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          partnerId,
          tenantId
        }
        const variablesData = await getVariablesDataOfTenant(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // Tenant_Lease_Esigning
    if (includes(allowedVariableContext, 'tenant_lease_esigning')) {
      const tenantLeaseEsigningVariables =
        notificationTemplateHelper.getTemplateVariablesForTenantLeaseEsigning() // Getting Default Template VariablesInfo
      const { variables = {} } = tenantLeaseEsigningVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const variablesData = await getVariablesDataOfTenantLeaseEsigning(
          variableNames,
          collectionData,
          collectionName
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }
    // User
    if (includes(allowedVariableContext, 'user')) {
      const userVariables =
        notificationTemplateHelper.getTemplateVariablesForUser() // Getting Default Template VariablesInfo
      const { variables = {} } = userVariables[0]
      if (size(variables)) {
        const variableNames = map(variables, 'name')
        const params = {
          collectionData,
          collectionName,
          emailInfo,
          partnerId,
          tenantId,
          conversation
        }
        const variablesData = await getVariablesDataOfUser(
          variableNames,
          params
        )
        if (size(variablesData)) variablesDataArray.push(variablesData)
      }
    }

    const variablesData = Object.assign({}, ...variablesDataArray) // Converting multiple [OBJ] into one OBJ

    return variablesData
  } catch (err) {
    console.log('Something went wrong in variablesData:', err.message, err)
    throw new CustomError(
      404,
      `Something went wrong in variablesData: ${err.message}`
    )
  }
}

export const getVariablesDataForLambda = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  validationCheckForNotificationVariablesData(body)

  const { event, options } = body
  const {
    annualStatementId = '',
    assignTo = [],
    collectionNameStr = '',
    collectionId = '',
    depositAccountId = '',
    incomingPaymentData = null,
    movingId = '',
    paymentsApprovalESigningURL = '',
    payoutsApprovalESigningURL = '',
    sendToUserLang = 'no',
    taskId = '',
    tenantId = '',
    token = '',
    userId = ''
  } = options

  const { collectionName = '' } =
    appHelper.getCollectionNameAndFieldNameByString(collectionNameStr)

  if (!collectionName) return { variablesData: {} }

  const collectionData = await collectionName.findOne({ _id: collectionId })

  if (!collectionData) return { variablesData: {} }

  const params = {
    collectionName: collectionNameStr,
    doc: collectionData,
    partnerId: options.partnerId || collectionData.partnerId,
    sendToUserLang
  }

  // Adding annualStatementId for getting annual statement variables
  if (annualStatementId) collectionData.annualStatementId = annualStatementId
  // Adding assignTo for getting task variables
  if (assignTo) params.assignTo = assignTo
  // Adding depositAccountId for getting deposit account variables
  if (depositAccountId) params.depositAccountId = depositAccountId
  // Adding incomingPaymentData for getting deposit payment variables
  if (size(incomingPaymentData)) assign(params, incomingPaymentData)
  // Adding movingId for getting moving in-out variables
  if (movingId) params.movingId = movingId
  // Adding taskId for getting task variables
  if (taskId) params.taskId = taskId
  // Adding tenantId for getting user variables
  if (tenantId) params.tenantId = tenantId
  // Adding token for getting variables
  if (token) params.token = token
  // Adding userId for getting user variables
  if (userId) params.userId = userId
  // Adding paymentsApprovalESigningURL for setting payouts or payments e-signing URL
  if (paymentsApprovalESigningURL)
    params.paymentsApprovalESigningURL = paymentsApprovalESigningURL
  // Adding payoutsApprovalESigningURL for setting payouts or payments e-signing URL
  if (payoutsApprovalESigningURL)
    params.payoutsApprovalESigningURL = payoutsApprovalESigningURL

  console.log(
    'Checking collection data for preparing variables value: ',
    collectionData
  )
  const variablesData = await getVariablesData(event, collectionData, params)
  console.log(
    '++++ Checking for variablesData before returning 5625: ',
    variablesData
  )
  return { variablesData }
}

const getNotificationLogsList = async (query, options) => {
  const { skip, limit } = options
  if (
    query.hasOwnProperty('$or') ||
    query.hasOwnProperty('subject') ||
    query.hasOwnProperty('toEmail') ||
    query.hasOwnProperty('toPhoneNumber')
  ) {
    options.sort = {}
  }
  const notificationLogs = await NotificationLogCollection.find(query, {
    _id: 1,
    type: 1,
    status: 1,
    subject: 1,
    event: 1,
    toUserId: 1,
    events: 1,
    partnerId: 1,
    sentAt: 1,
    createdAt: 1,
    toEmail: 1,
    toPhoneNumber: 1
  })
    .sort(options.sort)
    .skip(skip)
    .limit(limit)
    .populate('user', '_id profile.name profile.avatarKey profile.picture')
    .populate('partner', 'name subDomain')
  return notificationLogs
}

export const countNotificationLogs = async (query = {}) => {
  const numberOfNotificationLogs = await notificationLogCount(query)
  return numberOfNotificationLogs
}

export const notificationLogCount = async (query = {}) => {
  const countData = await NotificationLogCollection.find(query).count()
  return countData
}

export const queryNotificationLogs = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query, options } = body
  // For lambda service
  const { attachmentFileIds = [] } = query
  // if query is from lambda service
  if (size(attachmentFileIds)) {
    const query = {
      attachmentsMeta: {
        $elemMatch: { id: { $in: attachmentFileIds } }
      }
    }
    const notificationLogsData = await getNotificationLogs(query)
    return { data: notificationLogsData }
  }
  // Others
  appHelper.validateSortForQuery(options.sort)
  body.query = await prepareNotificationLogsQueryBasedOnFilters(query)
  body.populate = ['user', 'partner']
  const notificationLogsData = await getNotificationLogsForQuery(body)
  const filteredDocuments = await notificationLogCount(body.query)
  const totalDocuments = await notificationLogCount({})
  return {
    data: notificationLogsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryNotificationLogsForLambda = async (req) => {
  const { body } = req
  const { query = {}, options = {} } = body
  const { notificationLogIds } = query
  appHelper.validateSortForQuery(options.sort)
  if (size(notificationLogIds)) {
    query._id = { $in: notificationLogIds }
  }
  delete query.notificationLogIds
  const notificationLogs = await getNotificationLogsForQuery(body)
  return notificationLogs
}

export const queryNotificationLogsForAdminApp = async (req) => {
  const { body = {}, user = {} } = req
  const { userId = '', partnerId = '' } = user
  appHelper.checkUserId(userId)
  const { query = {}, options } = body
  appHelper.validateSortForQuery(options.sort)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  }
  const totalDocuments = partnerId
    ? await notificationLogCount({ partnerId })
    : await NotificationLogCollection.estimatedDocumentCount()
  body.query = await prepareNotificationLogsQueryBasedOnFilters(query)
  let notificationLogsData = await getNotificationLogsList(body.query, options)
  let filteredDocuments = 0
  if (size(body.query)) {
    filteredDocuments = await notificationLogCount(body.query)
  } else {
    filteredDocuments = totalDocuments
  }
  const notificationLogs = []
  notificationLogsData = JSON.parse(JSON.stringify(notificationLogsData))
  for (const notificationLogData of notificationLogsData) {
    if (size(notificationLogData.user)) {
      notificationLogData.user.profile.avatarKey = userHelper.getAvatar(
        notificationLogData.user
      )
    }
    notificationLogs.push(notificationLogData)
  }
  return {
    data: notificationLogs,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getAllowedEvents = () => [
  'send_agent_moving_in_esigning',
  'send_agent_moving_out_esigning',
  'send_assignment_email',
  'send_assignment_esigning',
  'send_assignment_esigning_reminder_notice_to_landlord',
  'send_collection_notice',
  'send_CPI_settlement_notice',
  'send_credit_note',
  'send_custom_notification',
  'send_deposit_account_created',
  'send_deposit_incoming_payment',
  'send_deposit_insurance_created',
  'send_deposit_insurance_payment_reminder',
  'send_due_reminder',
  'send_eviction_due_reminder_notice',
  'send_eviction_due_reminder_notice_without_eviction_fee',
  'send_eviction_notice',
  'send_final_settlement',
  'send_first_reminder',
  'send_interest_form',
  'send_invoice',
  'send_landlord_annual_statement',
  'send_landlord_credit_note',
  'send_landlord_invoice',
  'send_landlord_lease_esigning',
  'send_landlord_moving_in_esigning',
  'send_landlord_moving_out_esigning',
  'send_lease_esigning_reminder_notice_to_landlord',
  'send_lease_esigning_reminder_notice_to_tenant',
  'send_move_in_esigning_reminder_notice_to_agent',
  'send_move_in_esigning_reminder_notice_to_landlord',
  'send_move_in_esigning_reminder_notice_to_tenant',
  'send_move_out_esigning_reminder_notice_to_agent',
  'send_move_out_esigning_reminder_notice_to_landlord',
  'send_move_out_esigning_reminder_notice_to_tenant',
  'send_natural_termination_notice',
  'send_next_schedule_payout',
  'send_notification_ask_for_credit_rating',
  'send_notification_tenant_pays_all_due_during_eviction',
  'send_payout',
  'send_schedule_termination_notice_by_landlord',
  'send_schedule_termination_notice_by_tenant',
  'send_second_reminder',
  'send_soon_ending_notice',
  'send_task_notification',
  'send_tenant_lease_esigning',
  'send_tenant_moving_in_esigning',
  'send_tenant_moving_out_esigning',
  'send_termination_notice_by_landlord',
  'send_termination_notice_by_tenant',
  'send_welcome_lease',
  'send_wrong_ssn_notification',
  'sent_schedule_termination_email'
]

export const prepareLogActionName = (event, type) => {
  const actionNames = {
    send_agent_moving_in_esigning: 'sent_agent_moving_in_esigning_',
    send_agent_moving_out_esigning: 'sent_agent_moving_out_esigning_',
    send_assignment_email: 'sent_assignment_',
    send_assignment_esigning: 'sent_assignment_esigning_',
    send_assignment_esigning_reminder_notice_to_landlord:
      'sent_assignment_esigning_reminder_notice_',
    send_collection_notice: 'sent_collection_notice_',
    send_CPI_settlement_notice: 'sent_CPI_settlement_notice_',
    send_credit_note: 'sent_invoice_',
    send_custom_notification: 'sent_custom_notification_',
    send_deposit_account_created: 'sent_deposit_account_created_',
    send_deposit_incoming_payment: 'sent_deposit_incoming_payment_',
    send_deposit_insurance_created: 'sent_deposit_insurance_created_',
    send_deposit_insurance_payment_reminder: 'sent_app_invoice_',
    send_due_reminder: 'sent_due_reminder_',
    send_eviction_due_reminder_notice: 'sent_eviction_due_reminder_notice_',
    send_eviction_due_reminder_notice_without_eviction_fee:
      'sent_eviction_due_reminder_notice_without_eviction_fee_',
    send_eviction_notice: 'sent_eviction_notice_',
    send_final_settlement: 'sent_final_settlement_',
    send_first_reminder: 'sent_first_reminder_',
    send_interest_form: 'sent_interest_form_',
    send_invoice: 'sent_invoice_',
    send_landlord_annual_statement: 'send_landlord_annual_statement_',
    send_landlord_credit_note: 'sent_landlord_invoice_',
    send_landlord_invoice: 'sent_landlord_invoice_',
    send_landlord_lease_esigning: 'sent_landlord_lease_esigning_',
    send_landlord_moving_in_esigning: 'sent_landlord_moving_in_esigning_',
    send_landlord_moving_out_esigning: 'sent_landlord_moving_out_esigning_',
    send_lease_esigning_reminder_notice_to_landlord:
      'sent_landlord_lease_esigning_reminder_notice_',
    send_lease_esigning_reminder_notice_to_tenant:
      'sent_tenant_lease_esigning_reminder_notice_',
    send_move_in_esigning_reminder_notice_to_agent:
      'sent_agent_moving_in_esigning_reminder_notice_',
    send_move_in_esigning_reminder_notice_to_landlord:
      'sent_landlord_moving_in_esigning_reminder_notice_',
    send_move_in_esigning_reminder_notice_to_tenant:
      'sent_tenant_moving_in_esigning_reminder_notice_',
    send_move_out_esigning_reminder_notice_to_agent:
      'sent_agent_moving_out_esigning_reminder_notice_',
    send_move_out_esigning_reminder_notice_to_landlord:
      'sent_landlord_moving_out_esigning_reminder_notice_',
    send_move_out_esigning_reminder_notice_to_tenant:
      'sent_tenant_moving_out_esigning_reminder_notice_',
    send_natural_termination_notice: 'sent_natural_termination_',
    send_next_schedule_payout: 'sent_next_schedule_payout_',
    send_notification_ask_for_credit_rating:
      'send_notification_ask_for_credit_rating_',
    send_notification_tenant_pays_all_due_during_eviction:
      'sent_notification_tenant_pays_all_due_during_eviction_',
    send_payout: 'sent_payout_',
    send_payouts_approval_esigning: 'send_payouts_approval_esigning_',
    send_schedule_termination_notice_by_landlord: 'sent_schedule_termination_',
    send_schedule_termination_notice_by_tenant: 'sent_schedule_termination_',
    send_second_reminder: 'sent_second_reminder_',
    send_soon_ending_notice: 'sent_soon_ending_',
    send_task_notification: 'sent_task_notification_',
    send_tenant_lease_esigning: 'sent_tenant_lease_esigning_',
    send_tenant_moving_in_esigning: 'sent_tenant_moving_in_esigning_',
    send_tenant_moving_out_esigning: 'sent_tenant_moving_out_esigning_',
    send_termination_notice_by_landlord: 'sent_termination_',
    send_termination_notice_by_tenant: 'sent_termination_',
    send_welcome_lease: 'send_welcome_lease_',
    send_wrong_ssn_notification: 'sent_wrong_ssn_notification_'
  }
  let actionName = actionNames[event] || ''
  if (actionName) {
    return `${actionNames[event]}${type}` // Ex: sent_app_invoice_email
  }
  actionName = event.replace('send', 'sent') // Ex: send_app_invoice => sent_app_invoice
  return `${actionName}_${type}`
}

export const getAllowedInvoiceContextArray = () => [
  'send_collection_notice',
  'send_due_reminder',
  'send_eviction_due_reminder_notice',
  'send_eviction_due_reminder_notice_without_eviction_fee',
  'send_eviction_notice',
  'send_first_reminder',
  'send_invoice',
  'send_landlord_invoice',
  'send_next_schedule_payout',
  'send_second_reminder'
]

export const prepareContext = (event, sendTo) => {
  const allowedInvoiceContext = getAllowedInvoiceContextArray()
  if (includes(allowedInvoiceContext, event)) {
    return 'invoice'
  }
  const contexts = {
    send_custom_notification: sendTo,
    send_deposit_insurance_payment_reminder: 'app_invoice',
    send_notification_ask_for_credit_rating: 'tenant',
    send_payout: 'payout',
    send_task_notification: 'task',
    send_wrong_ssn_notification: sendTo
  }
  if (contexts[event]) {
    return contexts[event]
  }
  return 'property'
}

export const prepareNotificationLogMetaData = async (
  notificationLog,
  action,
  context,
  session
) => {
  const {
    partnerId,
    contractId,
    agentId,
    accountId,
    depositPaymentId,
    sendTo,
    toEmail,
    bankAccountNumber
  } = notificationLog
  let { tenantId } = notificationLog
  const notAllowedContext = ['invoice', 'payout']
  const metaData = []
  if (contractId) {
    const contractInfo =
      (await contractHelper.getAContract(
        { _id: contractId, partnerId },
        session
      )) || {}
    const { rentalMeta, assignmentSerial, leaseSerial } = contractInfo
    if (!tenantId && rentalMeta?.tenantId) {
      tenantId = rentalMeta.tenantId
    }

    if (
      leaseSerial &&
      indexOf(notAllowedContext, context) === -1 &&
      action !== 'sent_assignment_email'
    )
      metaData.push({
        field: 'leaseSerial',
        value: leaseSerial
      })
    if (assignmentSerial && indexOf(notAllowedContext, context) === -1)
      metaData.push({
        field: 'assignmentSerial',
        value: assignmentSerial
      })
  }

  //Deposit payment amount meta
  if (depositPaymentId) {
    const depositAccountInfo =
      (await depositAccountHelper.getDepositAccount(
        {
          tenantId,
          contractId,
          partnerId
        },
        session
      )) || {}
    const { payments = [] } = depositAccountInfo
    if (size(depositAccountInfo.payments)) {
      const paymentInfoObj = find(
        payments,
        (paymentInfo) => paymentInfo.id === depositPaymentId
      )
      const { paymentAmount, paymentReference } = paymentInfoObj
      metaData.push({
        field: 'paymentAmount',
        value: paymentAmount
      })
      metaData.push({
        field: 'paymentReference',
        value: paymentReference
      })
      metaData.push({
        field: 'accountNumber',
        value: bankAccountNumber
      })
    }
  }
  if (sendTo === 'account' && accountId)
    metaData.push({ field: 'sendTo', value: 'account' })
  if (sendTo === 'tenant' && tenantId)
    metaData.push({ field: 'sendTo', value: 'tenant' })
  if (sendTo === 'email')
    metaData.push({
      field: 'sendTo',
      value: 'email',
      toEmail
    })
  if (sendTo === 'agent' && agentId)
    metaData.push({ field: 'sendTo', value: 'agent' })

  return metaData
}

export const prepareCreateLogData = async (notificationLog, session) => {
  const { _id: notificationLogId, event, type, sendTo } = notificationLog
  const allowedEvents = getAllowedEvents()
  if (!includes(allowedEvents, event)) {
    return false
  }
  const action = prepareLogActionName(event, type)
  const context = prepareContext(event, sendTo)
  const visibility = logHelper.getLogVisibility({ context }, notificationLog)
  const newLogData = pick(notificationLog, [
    'accountId',
    'agentId',
    'branchId',
    'commentId',
    'contractId',
    'invoiceId',
    'partnerId',
    'payoutId',
    'propertyId',
    'taskId',
    'tenantId'
  ])
  newLogData.notificationLogId = notificationLogId
  newLogData.meta = await prepareNotificationLogMetaData(
    notificationLog,
    action,
    context,
    session
  )
  newLogData.action = action
  newLogData.context = context
  newLogData.visibility = visibility
  return newLogData
}

export const preparePartnerUsageData = (notificationLog) => {
  const { partnerId, branchId, _id, totalMessages } = notificationLog
  return {
    partnerId,
    branchId,
    notificationLogId: _id,
    totalMessages,
    type: 'outgoing_sms'
  }
}

export const getInvoiceAttachmentFileIdAndKey = async (
  invoiceInfo = {},
  session
) => {
  if (!size(invoiceInfo)) return {}
  const { invoiceType = '', pdf = [] } = invoiceInfo
  const pdfList = pdf
  let pdfType = ''

  if (
    invoiceType &&
    indexOf(
      [
        'app_invoice',
        'invoice',
        'credit_note',
        'landlord_invoice',
        'landlord_credit_note',
        'send_final_settlement'
      ],
      invoiceType
    ) !== -1
  ) {
    pdfType = invoiceType + '_pdf'
  }

  const pdfInfo = find(
    pdfList,
    (pdfData) => pdfType && size(pdfData) && pdfData.type === pdfType
  )

  const fileInfo =
    size(pdfInfo) && pdfInfo.fileId
      ? (await fileHelper.getAFile({ _id: pdfInfo.fileId }, session)) || {}
      : {}
  const fileId = size(fileInfo) && fileInfo._id ? fileInfo._id : ''
  const fileKey = fileHelper.getFileKey(fileInfo)

  return { fileKey, fileId }
}

export const getAnnualStatementAttachmentFileIdAndKey = async (
  annualStatementId = '',
  session
) => {
  if (!annualStatementId) return {}

  const annualStatementInfo = await annualStatementHelper.getAnnualStatement(
    {
      _id: annualStatementId
    },
    session
  )
  const { fileId = '' } = annualStatementInfo || {}
  const fileInfo = fileId
    ? (await fileHelper.getAFile({ _id: fileId }, session)) || {}
    : {}
  const fileKey = fileHelper.getFileKey(fileInfo)

  return { fileId, fileKey }
}

export const getAttachmentsMetaObj = (fileInfo, sendToLang) => {
  const fileKey = fileHelper.getFileKey(fileInfo)

  return {
    name: fileInfo.title,
    lang: sendToLang,
    status: 'done',
    fileId: fileInfo._id,
    fileKey,
    id: nid(17),
    type: 'email_attachment_pdf'
  }
}

export const getAttachmentsMetas = async (params, session) => {
  const { collectionData, collectionNameStr, event, notificationLogData } =
    params

  if (
    !(
      size(collectionData) &&
      collectionNameStr &&
      event &&
      size(notificationLogData)
    )
  ) {
    return []
  }
  const {
    annualStatementId = '',
    contractId = '',
    partnerId = '',
    sendTo = '',
    tenantId = '',
    type = '',
    toUserId = ''
  } = notificationLogData

  const attachmentsMeta = notificationLogData.attachmentsMeta || []

  const sendToUser = toUserId
    ? (await userHelper.getUserById(toUserId, session)) || {}
    : {}
  const sendToLang = (size(sendToUser) && sendToUser.getLanguage()) || 'no'
  let attachmentFileInfo = {}
  // Adding attachments of e_signing contract PDF files
  if (
    contractId &&
    sendTo === 'tenant' &&
    (event === 'send_welcome_lease' || event === 'send_tenant_lease_esigning')
  ) {
    const contractInfo = await contractHelper.getAContract(
      { _id: contractId, partnerId },
      session
    )
    const { rentalMeta = {} } = contractInfo || {}
    const { depositType = '', enabledLeaseEsigning = false } = rentalMeta

    if (enabledLeaseEsigning) {
      const type = 'esigning_lease_pdf'
      const query = { contractId, partnerId }

      if (event === 'send_tenant_lease_esigning') {
        if (depositType === 'deposit_account') {
          query['$or'] = [
            { type },
            {
              type: 'deposit_account_contract_pdf',
              tenantId,
              isExistingFile: { $exists: false }
            }
          ]
        } else if (depositType === 'deposit_insurance') {
          query['$or'] = [
            { type },
            {
              type: 'esigning_deposit_insurance_pdf',
              isExistingFile: { $exists: false }
            }
          ]
        } else {
          query.type = type
        }
      } else if (event === 'send_welcome_lease') {
        query.type = type
      }
      const files = await fileHelper.getFiles(query, session)

      if (size(files)) {
        for (const fileInfo of files) {
          const attachmentsMetaObj = getAttachmentsMetaObj(fileInfo, sendToLang)
          attachmentsMeta.push(attachmentsMetaObj)
        }
      }
    }
  }

  if (event === 'send_deposit_insurance_created') {
    const type = 'esigning_deposit_insurance_pdf'
    const query = {
      contractId,
      partnerId,
      type
    }
    const files = await fileHelper.getFiles(query, session)

    if (size(files)) {
      for (const fileInfo of files) {
        const attachmentsMetaObj = getAttachmentsMetaObj(fileInfo, sendToLang)
        attachmentsMeta.push(attachmentsMetaObj)
      }
    }
  }

  if (contractId && event === 'send_assignment_email') {
    const query = {
      contractId,
      partnerId,
      type: 'assignment_pdf'
    }
    const files = await fileHelper.getFiles(query, session)

    if (size(files)) {
      for (const fileInfo of files) {
        const attachmentsMetaObj = getAttachmentsMetaObj(fileInfo, sendToLang)
        attachmentsMeta.push(attachmentsMetaObj)
      }
    }
  }

  // Adding attachment fileId and fileKey for invoice PDF attachments of event: send_invoice & send_credit_note
  if (
    collectionNameStr === 'invoices' &&
    includes(
      [
        'send_invoice',
        'send_credit_note',
        'send_landlord_invoice',
        'send_landlord_credit_note',
        'send_final_settlement'
      ],
      event
    ) &&
    type === 'email'
  ) {
    // Collecting attachment fileId and fileKey
    attachmentFileInfo = await getInvoiceAttachmentFileIdAndKey(collectionData)
    const { fileId = '', fileKey = '' } = attachmentFileInfo || {}

    for (const attachmentMeta of notificationLogData.attachmentsMeta) {
      const { content = '', isInvoice = false, type = '' } = attachmentMeta

      if (
        (isInvoice ||
          includes(
            [
              'credit_note_pdf',
              'landlord_invoice_attachment_pdf',
              'landlord_credit_note_pdf'
            ],
            type
          )) &&
        !content
      ) {
        attachmentMeta.fileId = fileId
        attachmentMeta.fileKey = fileKey
      }
    }
  }

  // Adding attachment fileId and fileKey for annual statement PDF attachments of event: send_landlord_annual_statement
  if (
    collectionNameStr === 'contracts' &&
    event === 'send_landlord_annual_statement'
  ) {
    // Collecting attachment fileId and fileKey
    attachmentFileInfo = await getAnnualStatementAttachmentFileIdAndKey(
      annualStatementId
    )
    const { fileId = '', fileKey = '' } = attachmentFileInfo || {}

    for (const attachmentMeta of notificationLogData.attachmentsMeta) {
      const { content = '', type = '' } = attachmentMeta

      if (type === 'lease_statement_pdf' && !content) {
        attachmentMeta.fileId = fileId
        attachmentMeta.fileKey = fileKey
      }
    }
  }

  if (
    collectionNameStr === 'app_invoices' &&
    event === 'send_deposit_insurance_payment_reminder' &&
    type === 'email'
  ) {
    // Collecting attachment fileId and fileKey
    attachmentFileInfo = await getInvoiceAttachmentFileIdAndKey(collectionData)
    const { fileId = '', fileKey = '' } = attachmentFileInfo || {}

    for (const attachmentMeta of notificationLogData.attachmentsMeta) {
      const { content = '', type = '' } = attachmentMeta

      if (type === 'app_invoice_pdf' && !content) {
        attachmentMeta.fileId = fileId
        attachmentMeta.fileKey = fileKey
      }
    }
  }

  return attachmentsMeta
}

export const prepareQueryAndDataForSNSResponse = (events, SESMsgIds, type) => {
  const query = { SESMsgId: { $in: SESMsgIds } }

  const updateData = {}
  let eventName = ''
  if (
    type === 'sentStatus' ||
    type === 'bounceStatus' ||
    type === 'rejectStatus'
  ) {
    const { status = '' } = events
    console.log('=== Events status', status)
    if (status === 'sent') {
      updateData['$set'] = { status, sentAt: new Date() }
    } else {
      updateData['$set'] = { status }
    }

    if (status) {
      eventName = status
    }
  } else if (
    type === 'openEvent' ||
    type === 'clickEvent' ||
    type === 'complainEvent'
  ) {
    const {
      openCount: msgOpenCount = 0,
      clickCount: msgClickCount = 0,
      complaint = false
    } = events

    if (msgOpenCount) {
      updateData['$inc'] = { msgOpenCount }
      eventName = 'opened'
    } else if (msgClickCount) {
      updateData['$inc'] = { msgClickCount }
      eventName = 'clicked'
    } else if (complaint) {
      updateData['$set'] = {
        status: 'failed',
        complaint,
        errorReason: 'Got complaint response from this email',
        isResend: false
      }
      eventName = 'complaint'
    }
  }
  updateData['$push'] = {
    events: {
      status: eventName,
      createdAt: new Date()
    }
  }
  console.log('=== updateData', updateData)
  return { query, updateData }
}
export const countNotificationLogForLambda = async () => {
  const notifierSmsCount = await notificationLogCount({
    status: 'ready',
    type: 'sms'
  })
  const notifierEmailCount = await notificationLogCount({
    status: 'ready',
    type: 'email'
  })

  return {
    notifierSmsCount,
    notifierEmailCount
  }
}

export const getNotificationLogDetails = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { _id, invoiceId } = body
  const query = {}
  if (_id) query._id = _id
  if (invoiceId) {
    query.invoiceId = invoiceId
    query.event = 'send_invoice'
  }
  if (!size(query))
    throw new CustomError(
      400,
      'Please provide notification log id or invoiceId'
    )
  const [notificationLog = {}] = await getNotificationLogsWithPopulate(
    { query },
    'user'
  )
  const userInfo = notificationLog.user
  if (size(userInfo)) {
    notificationLog.email = notificationLog.user.getEmail()
    notificationLog.phoneNumber = userInfo.profile.phoneNumber
  }
  return notificationLog
}

export const prepareQueueParamsForSendMailToAll = (body) => {
  const {
    agentId = '',
    branchId = '',
    content = '',
    context = '',
    partnerId,
    sendTo = '',
    subject = '',
    notificationType = ''
  } = body

  if (notificationType === 'email' && !subject)
    throw new CustomError(400, 'Subject is required')
  if (!content) throw new CustomError(400, 'Content is required')
  if (!sendTo) throw new CustomError(400, 'SendTo is required')
  if (!context) throw new CustomError(400, 'Context is required')

  const queueParams = {
    context,
    content,
    partnerId,
    subject,
    sendTo
  }

  if (branchId) {
    appHelper.validateId({ branchId })
    queueParams.branchId = branchId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    queueParams.agentId = agentId
  }

  return queueParams
}

const prepareQueryByContractStartAndEndDate = async (params) => {
  const { leaseStartDateRange, leaseEndDateRange, partnerId } = params
  const contractQuery = {}
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (
    size(leaseStartDateRange) &&
    leaseStartDateRange.startDate &&
    leaseStartDateRange.endDate
  ) {
    contractQuery['rentalMeta.contractStartDate'] = {
      $gte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseStartDateRange.startDate
      ),
      $lte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseStartDateRange.endDate
      )
    }
  } else if (
    size(leaseStartDateRange) &&
    leaseStartDateRange.startDate &&
    !leaseStartDateRange.endDate
  ) {
    contractQuery['rentalMeta.contractStartDate'] = {
      $gte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseStartDateRange.startDate
      )
    }
  } else if (
    size(leaseStartDateRange) &&
    !leaseStartDateRange.startDate &&
    leaseStartDateRange.endDate
  ) {
    contractQuery['rentalMeta.contractStartDate'] = {
      $lte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseStartDateRange.endDate
      )
    }
  }
  if (
    size(leaseEndDateRange) &&
    leaseEndDateRange.startDate &&
    leaseEndDateRange.endDate
  ) {
    contractQuery['rentalMeta.contractEndDate'] = {
      $gte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseEndDateRange.startDate
      ),
      $lte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseEndDateRange.endDate
      )
    }
  } else if (
    size(leaseEndDateRange) &&
    leaseEndDateRange.startDate &&
    !leaseEndDateRange.endDate
  ) {
    contractQuery['rentalMeta.contractEndDate'] = {
      $gte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseEndDateRange.startDate
      )
    }
  } else if (
    size(leaseEndDateRange) &&
    !leaseEndDateRange.startDate &&
    leaseEndDateRange.endDate
  ) {
    contractQuery['rentalMeta.contractEndDate'] = {
      $lte: await appHelper.getActualDate(
        partnerSetting,
        false,
        leaseEndDateRange.endDate
      )
    }
  }

  return contractQuery
}

export const prepareTenantQueryForEmailSendToAll = async (body) => {
  const query = {}
  const queryData = []
  const invoiceQuery = []
  const propertyQuery = {}
  const setPropertyQuery = []
  let tenantIds = []
  let isNotTenant = false

  if (size(body)) {
    const {
      accountId = '',
      agentId = '',
      branchId = '',
      hasInProgressLease = false,
      isArchived = false, // tenantType
      isProspect = false,
      leaseStartDateRange = {},
      leaseEndDateRange = {},
      partnerId,
      paymentStatus = [],
      prospectStatus = [],
      propertyId = '',
      searchKeyword = ''
    } = body
    let { tenantStatus = [] } = body
    queryData.push({ partnerId })

    if (indexOf(tenantStatus, 'active') !== -1) {
      if (indexOf(paymentStatus, 'partially_paid') !== -1)
        invoiceQuery.push({ isPartiallyPaid: true, partnerId })
      if (indexOf(paymentStatus, 'overpaid') !== -1)
        invoiceQuery.push({ isOverPaid: true, partnerId })
      if (indexOf(paymentStatus, 'defaulted') !== -1)
        invoiceQuery.push({ isDefaulted: true, partnerId })
      if (
        indexOf(paymentStatus, 'unpaid') !== -1 &&
        indexOf(paymentStatus, 'overdue') !== -1
      ) {
        invoiceQuery.push({ status: 'overdue', partnerId })
      } else if (indexOf(paymentStatus, 'unpaid') !== -1)
        invoiceQuery.push({ status: { $ne: 'paid' }, partnerId })

      if (size(invoiceQuery)) {
        const invoiceTenantIds = await InvoiceCollection.distinct('tenantId', {
          $or: invoiceQuery
        })
        if (size(invoiceTenantIds)) tenantIds = invoiceTenantIds
        else {
          queryData.push({ _id: 'nothing' })
          isNotTenant = true
        }
      }
    }
    if (isProspect === true) {
      tenantStatus = union(tenantStatus, prospectStatus)
      if (!isArchived) {
        queryData.push({ type: { $ne: 'archived' } })
      }
    }

    if (size(tenantStatus)) propertyQuery.status = { $in: tenantStatus }

    if (
      indexOf(tenantStatus, 'active') !== -1 ||
      indexOf(tenantStatus, 'upcoming') !== -1
    ) {
      const contractQuery = await prepareQueryByContractStartAndEndDate({
        leaseStartDateRange,
        leaseEndDateRange,
        partnerId
      })
      if (size(contractQuery)) {
        contractQuery.partnerId = partnerId
        contractQuery['rentalMeta.status'] = { $ne: 'closed' }
        const contractTenantIds = await ContractCollection.distinct(
          'rentalMeta.tenantId',
          contractQuery
        )
        if (size(contractTenantIds)) {
          if (size(tenantIds))
            tenantIds = intersection(tenantIds, contractTenantIds)
          else tenantIds = uniq(contractTenantIds)
        } else {
          queryData.push({ _id: 'nothing' })
          isNotTenant = true
        }
      }
    }

    if (branchId) {
      appHelper.validateId({ branchId })
      queryData.push({ 'properties.branchId': branchId })
    }
    if (agentId) {
      appHelper.validateId({ agentId })
      queryData.push({ 'properties.agentId': agentId })
    }
    if (accountId) {
      appHelper.validateId({ accountId })
      queryData.push({ 'properties.accountId': accountId })
    }
    if (propertyId) {
      appHelper.validateId({ propertyId })
      queryData.push({ 'properties.propertyId': propertyId })
    }

    if (size(propertyQuery)) {
      if (isArchived)
        setPropertyQuery.push(
          { properties: { $elemMatch: propertyQuery } },
          { type: { $in: ['archived'] } }
        )
      else {
        if (hasInProgressLease || indexOf(tenantStatus, 'upcoming') !== -1)
          setPropertyQuery.push({ properties: { $elemMatch: propertyQuery } })
        else queryData.push({ properties: { $elemMatch: propertyQuery } })
      }
    } else {
      if (hasInProgressLease && isArchived)
        setPropertyQuery.push({ type: { $in: ['archived'] } })
      else if (isArchived) queryData.push({ type: { $in: ['archived'] } })
    }
    // TODO:: if hasInProgressLease and depositStatus, prepare some query
    // TODO:: if hasInProgressLease or tenantStatus `active` or `upcoming` && depositStatus

    if (size(setPropertyQuery)) queryData.push({ $or: setPropertyQuery })
    if (size(searchKeyword)) {
      const searchData = searchKeyword.replace('+', '').trim()
      console.log('=== searchData', searchData)
      const userIds = await UserCollection.distinct('_id', {
        $or: [
          { 'emails.address': new RegExp(searchData, 'i') },
          {
            'profile.phoneNumber': new RegExp(searchData, 'i')
          },
          {
            'profile.norwegianNationalIdentification': new RegExp(
              searchData,
              'i'
            )
          }
        ]
      })
      console.log('=== userIds', userIds)

      const searchQuery = []
      if (size(userIds)) searchQuery.push({ userId: { $in: userIds } })

      if (searchData) {
        searchQuery.push({ name: new RegExp(searchData, 'i') })
        if (parseInt(searchData))
          searchQuery.push({ serial: parseInt(searchData) })
      }

      console.log('=== searchQuery', searchQuery)
      if (size(searchQuery)) queryData.push({ $or: searchQuery })
      console.log('=== QueryData', { ...queryData })
    }
    if (!isNotTenant && size(tenantIds))
      queryData.push({ _id: { $in: tenantIds } })
  }
  if (size(queryData)) query['$and'] = queryData
  console.log('=== query', { ...query })

  return query
}

export const prepareAccountQueryForEmailSendToAll = async (
  body,
  isBrokerPartner = 0
) => {
  const query = {}

  if (size(body)) {
    const {
      accountIds = [], // For multi select from ui
      accountStatus = [],
      accountType = '',
      agentId = '',
      branchId = '',
      partnerId,
      searchKeyword = ''
    } = body

    query.partnerId = partnerId
    if (size(accountStatus)) query.status = { $in: accountStatus }
    else query.status = { $ne: 'archived' }

    if (size(accountIds)) query._id = { $in: accountIds }

    if (isBrokerPartner) {
      if (size(accountType)) query['type'] = accountType
    }
    if (size(branchId)) {
      appHelper.validateId({ branchId })
      query['branchId'] = branchId
    }
    if (size(agentId)) {
      query['agentId'] = agentId
    }
    if (size(searchKeyword)) {
      const searchData = searchKeyword.replace('+', '').trim()
      console.log('=== searchData: ', searchData)
      const userIds = await UserCollection.distinct('_id', {
        $or: [
          { 'emails.address': new RegExp(searchData, 'i') },
          {
            'profile.phoneNumber': new RegExp(searchData, 'i')
          },
          {
            'profile.norwegianNationalIdentification': new RegExp(
              searchData,
              'i'
            )
          }
        ]
      })
      console.log('=== userIds: ', userIds)

      const searchQuery = []

      if (size(userIds)) searchQuery.push({ personId: { $in: userIds } })
      if (searchData) {
        searchQuery.push({ name: new RegExp(searchData, 'i') })
        if (parseInt(searchData))
          searchQuery.push({ serial: parseInt(searchData) })
      }
      console.log('=== searchQuery', searchQuery)
      if (size(searchQuery)) query['$or'] = searchQuery
    }
  }
  console.log('=== Query', { ...query })
  return query
}

export const getESigningVariablesData = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)

  const { query } = body
  appHelper.checkRequiredFields(['context', 'contractId'], query)

  const { context, contractId } = query
  const contractInfo = await contractHelper.getAContractForVariablesData(
    { _id: contractId },
    session
  )
  if (!size(contractInfo))
    throw new CustomError(404, 'Could not find contract info!')

  console.log(
    '==== Preparing variables data for contractId: ',
    contractId,
    ' for context: ',
    context
  )
  const variablesData = await getVariablesDataForESigningForm({
    context,
    contractInfo
  })
  console.log('==== Prepared variables data: ', variablesData)
  if (!size(variablesData))
    throw new CustomError(404, 'Could not find variables data!')

  return { variablesData }
}

const getVariablesDataForESigningForm = async (params = {}) => {
  const { context, contractInfo } = params
  console.log('==== Checking contractInfo from params: ', contractInfo)
  const {
    account: accountInfo,
    agent: agentInfo,
    branch: branchInfo,
    partner: partnerInfo,
    property: propertyInfo,
    propertyRepresentative: representativeInfo,
    rentalMeta,
    tenant: tenantInfo
  } = contractInfo
  const { depositType, newTenantName } = rentalMeta || {}

  const variables = {}
  if (size(contractInfo)) {
    const contractVariables = await getContractVariablesForESigningForm(
      context,
      contractInfo
    )
    assign(variables, contractVariables)
  }
  console.log('==== Checking depositType: ', depositType)
  if (depositType === 'deposit_insurance') {
    const depositInsuranceVariables =
      getDepositInsuranceVariablesForESigningForm(contractInfo)
    console.log(
      '==== Prepared depositInsuranceVariables: ',
      depositInsuranceVariables
    )
    assign(variables, depositInsuranceVariables)
  }

  if (size(accountInfo)) {
    const accountVariables = getAccountVariablesForESigningForm(accountInfo)
    assign(variables, accountVariables)
  }

  if (size(agentInfo)) {
    const agentName = agentInfo.getName() || ''
    variables.agent_name = agentName
    variables.agent_email = agentInfo.getEmail() || ''
    variables.agent_phonenumber = agentInfo.getPhone() || ''
    variables.agent_occupation = agentInfo.getOccupation() || ''
    variables.manager_name = agentName
  }

  if (size(branchInfo)) variables.branch_name = branchInfo.name || ''

  if (size(partnerInfo)) {
    const partnerVariables = getPartnerVariablesForESigningForm(
      partnerInfo,
      accountInfo
    )
    assign(variables, partnerVariables)
  }

  if (size(propertyInfo)) {
    const propertyVariables = await getPropertyVariablesForESigningForm(
      propertyInfo
    )
    assign(variables, propertyVariables)
  }

  if (size(representativeInfo)) {
    variables.representative_name = representativeInfo.getName() || ''
    variables.representative_occupation =
      representativeInfo.getOccupation() || ''
    variables.representative_phone = representativeInfo.getPhone() || ''
    variables.representative_email = representativeInfo.getEmail() || ''
  }

  if (size(tenantInfo)) {
    const tenantVariables = getTenantVariablesForESigningForm(
      tenantInfo,
      propertyInfo,
      newTenantName
    )
    assign(variables, tenantVariables)
  }

  return variables
}

const getContractVariablesForESigningForm = async (
  context = '',
  contractInfo = {}
) => {
  const variables = {}
  const {
    account: accountInfo,
    assignmentFrom,
    assignmentTo,
    assignmentSerial: assignmentRawSerialNo,
    brokeringCommissionAmount,
    brokeringCommissionType,
    listingInfo,
    leaseSerial: leaseRawSerialNo,
    payoutTo,
    partner: partnerInfo,
    property: propertyInfo,
    rentalManagementCommissionAmount,
    rentalManagementCommissionType,
    rentalMeta,
    tenant: tenantInfo
  } = contractInfo
  const { depositAmount, monthlyRentAmount } = listingInfo || {}
  const { noticeInEffect, noticePeriod } = rentalMeta || {}

  // Partner info
  const { accountType: partnerType, serial: partnerRawSerialNo } =
    partnerInfo || {}

  // Partner setting info
  const { partnerSetting: partnerSettingInfo } = partnerInfo || {}

  // Property info
  const { serial: propertyRawSerialNo } = propertyInfo || {}

  const { serial: tenantRawSerialNo } = tenantInfo || {}

  variables.brokering_commission = brokeringCommissionAmount || 0
  variables.management_commission = rentalManagementCommissionAmount || 0
  variables.monthly_rent = monthlyRentAmount || 0
  variables.deposit_amount = depositAmount || 0
  variables.notice_in_effect = appHelper.translateToUserLng(
    `properties.${noticeInEffect}`,
    'no'
  )
  variables.notice_period =
    noticePeriod + ' ' + appHelper.translateToUserLng('properties.months', 'no')
  variables.brokering_commission_amount =
    getBrokeringCommissionAmount(contractInfo)
  variables.brokering_commission_percentage =
    brokeringCommissionType === 'percent' ? brokeringCommissionAmount : 0
  variables.management_commission_amount =
    getManagementCommissionAmount(contractInfo)
  variables.management_commission_percentage =
    rentalManagementCommissionType === 'percent'
      ? rentalManagementCommissionAmount
      : 0
  variables.assignment_addons = await getContractAddons(
    contractInfo,
    'assignment'
  )
  variables.assignment_id =
    '#' +
    appHelper.getFixedDigitsSerialNumber([
      { digits: 4, value: partnerRawSerialNo },
      { digits: 5, value: propertyRawSerialNo },
      { digits: 3, value: assignmentRawSerialNo }
    ])
  variables.assignment_from = assignmentFrom
    ? await appHelper.getFormattedExportDate(partnerSettingInfo, assignmentFrom)
    : ''
  variables.assignment_to = assignmentTo
    ? await appHelper.getFormattedExportDate(partnerSettingInfo, assignmentTo)
    : ''
  variables.payout_to_bank_account = payoutTo
  variables.todays_date = await appHelper.getFormattedExportDate(
    partnerSettingInfo,
    new Date()
  )

  if (context === 'lease' || context === 'deposit_insurance') {
    const {
      contractStartDate,
      contractEndDate,
      monthlyRentAmount,
      minimumStay,
      nextCpiDate,
      lastCpiDate,
      firstInvoiceDueDate,
      isVatEnable,
      internalLeaseId,
      dueDate
    } = rentalMeta || {}

    variables.lease_start_date = contractStartDate
      ? await appHelper.getFormattedExportDate(
          partnerSettingInfo,
          contractStartDate
        )
      : undefined
    variables.lease_end_date = contractEndDate
      ? await appHelper.getFormattedExportDate(
          partnerSettingInfo,
          contractEndDate
        )
      : undefined
    const convertToCurrencyParams = {
      number: monthlyRentAmount,
      options: { isInvoice: true },
      partnerSettingsOrId: partnerSettingInfo,
      showSymbol: false
    }
    const convertedMonthlyRentAmount = await appHelper.convertToCurrency(
      convertToCurrencyParams
    )
    variables.monthly_rent_amount = convertedMonthlyRentAmount
      ? convertedMonthlyRentAmount
      : undefined
    variables.lease_addons = size(contractInfo.addons)
      ? await getContractAddons(contractInfo, 'lease')
      : []
    variables.lease_id =
      '#' +
      appHelper.getFixedDigitsSerialNumber([
        { digits: 5, value: propertyRawSerialNo },
        { digits: 4, value: tenantRawSerialNo },
        { digits: 3, value: leaseRawSerialNo }
      ])
    variables.minimum_stay = minimumStay ? minimumStay : ''
    variables.next_CPI_date = nextCpiDate
      ? await appHelper.getFormattedExportDate(partnerSettingInfo, nextCpiDate)
      : ''
    variables.last_CPI_date = lastCpiDate
      ? await appHelper.getFormattedExportDate(partnerSettingInfo, lastCpiDate)
      : ''

    let bankAccountNumber
    if (partnerType === 'broker') {
      const { bankPayment = {} } = partnerSettingInfo || {}
      const { firstMonthACNo = '' } = bankPayment
      bankAccountNumber = firstMonthACNo
    } else {
      const { invoiceAccountNumber = '' } = accountInfo || {}
      bankAccountNumber = invoiceAccountNumber
    }
    variables.bank_account_number = bankAccountNumber
    variables.invoice_due_date = firstInvoiceDueDate
      ? await appHelper.getFormattedExportDate(
          partnerSettingInfo,
          firstInvoiceDueDate
        )
      : ''
    variables.VAT_status = isVatEnable ? 'Yes' : 'No'
    variables.tenants = (await contractInfo.getTenantsItems()) || []
    variables.internal_lease_id = internalLeaseId || ''
    variables.monthly_due_date = dueDate || ''
  }
  return variables
}

const getAccountVariablesForESigningForm = (accountInfo = {}) => {
  const variables = {}
  const {
    name: accountName,
    organization: accountOrgInfo = {},
    person: accountPersonInfo,
    serial: accountSerialNo,
    type: accountType
  } = accountInfo || {}

  let {
    accountEmail = '',
    accountPersonId = '',
    accountPhoneNumber = '',
    address: accountAddress,
    city: accountCity,
    country: accountCountry,
    zipCode: accountZipCode
  } = accountInfo || {}

  if (accountType === 'person') {
    const { profile: accountPersonProfile } = accountPersonInfo || {}
    const {
      city: accountPersonCity,
      country: accountPersonCountry,
      hometown: accountPersonHomeTown,
      norwegianNationalIdentification: accountPersonSSN,
      zipCode: accountPersonZipCode
    } = accountPersonProfile || {}

    if (accountPersonHomeTown) accountAddress = accountPersonHomeTown
    if (accountPersonZipCode) accountZipCode = accountPersonZipCode
    if (accountPersonCity) accountCity = accountPersonCity
    if (accountPersonCountry) accountCountry = accountPersonCountry
    if (accountPersonSSN) accountPersonId = accountPersonSSN

    if (size(accountPersonInfo)) {
      accountEmail = accountPersonInfo.getEmail()
      accountPhoneNumber = accountPersonInfo.getPhone()
    }
  }

  variables.account_id = accountSerialNo ? `#${accountSerialNo}` : ''
  variables.account_org_id =
    size(accountOrgInfo) && accountOrgInfo.orgId ? accountOrgInfo.orgId : ''
  variables.account_name = accountName
  variables.account_address = accountAddress
  variables.account_zip_code = accountZipCode
  variables.account_city = accountCity
  variables.account_country = accountCountry
  variables.account_person_id = accountPersonId
  variables.account_email = accountEmail || ''
  variables.account_phonenumber = accountPhoneNumber || ''

  return variables
}

const getDepositInsuranceVariablesForESigningForm = (contractInfo = {}) => {
  const variables = {}
  const {
    depositInsurance,
    partner: partnerInfo,
    property: propertyInfo,
    rentalMeta
  } = contractInfo || {}
  console.log('==== Checking rentalMeta: ', rentalMeta)
  const { depositAmount, depositInsuranceAmount } = rentalMeta || {}
  console.log('==== Checking depositAmount from rental meta: ', depositAmount)
  console.log(
    '==== Checking depositInsuranceAmount from rental meta: ',
    depositInsuranceAmount
  )
  const { serial: partnerRawSerialNo } = partnerInfo || {}
  const { serial: propertyRawSerialNo } = propertyInfo || {}
  const { creationResult } = depositInsurance || {}
  const { insuranceNo } = creationResult || {}

  variables.total_deposit_amount = depositAmount || 0
  variables.invoice_amount = depositInsuranceAmount || 0
  variables.deposit_insurance_amount = depositInsuranceAmount || 0
  variables.deposit_insurance_reference_number = insuranceNo || ''
  variables.internal_deposit_insurance_reference_number =
    appHelper.getFixedDigitsSerialNumber([
      { digits: 4, value: partnerRawSerialNo },
      { digits: 5, value: propertyRawSerialNo }
    ])
  variables.hiscox_logo_url = appHelper.getDefaultLogoURL('hiscox-logo')
  variables.app_logo_url = appHelper.getDefaultLogoURL('uniteliving_logo_new')
  console.log('==== Checking deposit variables: ', variables)
  return variables
}

const getPartnerVariablesForESigningForm = (
  partnerInfo = {},
  accountInfo = {}
) => {
  const variables = {}
  const {
    accountType: partnerType,
    name: partnerName,
    partnerSetting: partnerSettingInfo,
    serial: partnerRawSerialNo
  } = partnerInfo
  const { companyInfo } = partnerSettingInfo || {}
  const {
    organizationId,
    postalAddress,
    postalCity,
    postalCountry,
    postalZipCode
  } = companyInfo || {}

  variables.partner_id = partnerRawSerialNo ? `#${partnerRawSerialNo}` : ''
  variables.partner_name = partnerName
  variables.partner_address = postalAddress
  variables.partner_zip_code = postalZipCode
  variables.partner_city = postalCity
  variables.partner_country = postalCountry
  variables.partner_org_id = organizationId

  if (partnerType === 'direct') {
    const { organization: organizationInfo } = accountInfo || {}
    variables.partner_logo_url = organizationInfo.getLogo() || ''
  } else {
    variables.partner_logo_url = partnerInfo.getLogo() || ''
  }

  return variables
}

const getPropertyVariablesForESigningForm = async (propertyInfo) => {
  const variables = {}
  const {
    apartmentId,
    gnr,
    bnr,
    snr,
    kitchen,
    livingRoom,
    livingRoomFurnished,
    location,
    noOfBedrooms,
    serial: propertySerialNo
  } = propertyInfo
  const {
    city: propertyCity,
    country: propertyCountry,
    name: propertyName,
    sublocality: propertySubLocality,
    postalCode: propertyPostalCode
  } = location || {}

  variables.property_id = propertySerialNo ? `#${propertySerialNo}` : ''
  variables.property_location = propertyName
  variables.property_zip_code = propertyPostalCode
  variables.property_city = propertyCity
  variables.property_country = propertyCountry
  variables.property_municipality = propertyCity
    ? propertySubLocality
      ? `${propertyCity} ${propertySubLocality}`
      : propertyCity
    : ''
  variables.property_addons = (await propertyInfo.getPropertyAddons()) || []
  variables.apartment_id = apartmentId || ''
  variables.property_gnr = gnr || ''
  variables.property_bnr = bnr || ''
  variables.property_snr = snr || ''
  variables.property_number_of_bedrooms = noOfBedrooms || ''
  variables.property_livingroom_yes_or_no = livingRoom ? 'Yes' : 'No'
  variables.property_kitchen_yes_or_no = kitchen ? 'Yes' : 'No'
  variables.property_furnished_yes_or_no = livingRoomFurnished ? 'Yes' : 'No'

  return variables
}

const getTenantVariablesForESigningForm = (
  tenantInfo = {},
  propertyInfo = {},
  newTenantName = ''
) => {
  const variables = {}
  const {
    name: tenantName,
    billingAddress: tenantAddress,
    city: tenantCity,
    country: tenantCountry,
    zipCode: tenantZipCode,
    serial,
    user: tenantUserInfo
  } = tenantInfo
  const propertyLocation =
    !size(tenantAddress) && propertyInfo && propertyInfo.location
      ? propertyInfo.location
      : {}
  const { profile: tenantUserProfileInfo } = tenantUserInfo || {}
  const { norwegianNationalIdentification: tenantUserId } =
    tenantUserProfileInfo || {}

  let tenantUserEmail
  let tenantUserPhoneNumber
  if (size(tenantUserInfo)) {
    tenantUserEmail = tenantUserInfo.getEmail()
    tenantUserPhoneNumber = tenantUserInfo.getPhone()
  }

  variables.tenant_id = serial
  variables.tenant_name = newTenantName ? newTenantName : tenantName
  variables.tenant_email = tenantUserEmail || ''
  variables.tenant_phonenumber = tenantUserPhoneNumber || ''
  variables.tenant_address = tenantAddress
    ? tenantAddress
    : propertyLocation.name
  variables.tenant_city =
    tenantAddress && tenantCity ? tenantCity : propertyLocation.city
  variables.tenant_country =
    tenantAddress && tenantCountry ? tenantCountry : propertyLocation.country
  variables.tenant_zip_code =
    tenantAddress && tenantZipCode ? tenantZipCode : propertyLocation.postalCode
  variables.tenant_person_id = tenantUserId

  return variables
}

export const getSendToInfoForAccountsOrTenants = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)

  const { query } = body
  appHelper.checkRequiredFields(
    ['collectionNameStr', 'collectionIds', 'partnerId', 'type'],
    query
  )

  const { collectionNameStr, collectionIds, partnerId, type } = query
  appHelper.validateId({ partnerId })

  if (
    !(
      includes(['accounts', 'tenants'], collectionNameStr) &&
      size(collectionIds) &&
      includes(['email', 'sms'], type)
    )
  )
    throw new CustomError(400, 'Passed invalid data for getting send to info!')

  let fromPhoneNumber
  if (type === 'sms') {
    const {
      partnerSetting,
      phoneNumber,
      sms: isSmsEnabled = false
    } = (await partnerHelper.getAPartner({ _id: partnerId }, session, [
      'partnerSetting'
    ])) || {}

    fromPhoneNumber =
      partnerSetting?.smsSettings?.smsSenderName || phoneNumber || undefined

    if (!(fromPhoneNumber && isSmsEnabled))
      throw new CustomError(400, 'Partner setting is invalid for sending sms!')
  }

  const userDataKey = collectionNameStr === 'accounts' ? 'person' : 'user'
  const sendTo = collectionNameStr === 'accounts' ? 'account' : 'tenant'

  const { collectionName, fieldName } =
    appHelper.getCollectionNameAndFieldNameByString(collectionNameStr) || {}
  const collectionsData = await collectionName
    .find({ _id: { $in: collectionIds } })
    .populate([userDataKey])
    .session(session)

  if (size(collectionsData)) {
    const sendToInfos = []
    for (const collectionData of collectionsData) {
      const userInfo = collectionData[userDataKey] || {}
      sendToInfos.push({
        [fieldName]: collectionData._id || undefined,
        sendTo,
        branchId:
          sendTo === 'account' && collectionData.branchId
            ? collectionData.branchId
            : undefined,
        fromPhoneNumber,
        toEmail: type === 'email' ? userInfo.getEmail() : undefined,
        toPhoneNumber: type === 'sms' ? userInfo.getPhone() : undefined,
        toUserId: userInfo._id || undefined
      })
    }
    return sendToInfos
  } else throw new CustomError(404, 'Accounts or tenants not found!')
}

const getAccountsDataBasedOnPhoneNumber = async (accountsQuery) => {
  console.log('=== accountsQuery', accountsQuery)
  const preparedQuery = await accountHelper.prepareQueryForAccounts(
    accountsQuery
  )
  console.log('=== preparedQuery', JSON.stringify(preparedQuery))
  const accounts = await accountHelper.getAccountsByAggregate(preparedQuery)
  const [account = {}] = accounts || []
  const {
    accountNamesWithoutPhoneNumbers = [],
    accountIdsWithPhoneNumbers = [],
    accountNamesWithPhoneNumbers = []
  } = account
  return {
    idsWithPhoneNumbers: accountIdsWithPhoneNumbers,
    accountNamesWithoutPhoneNumbers,
    accountNamesWithPhoneNumbers
  }
}

const getTenantsDataBasedOnPhoneNumber = async (query) => {
  const preparedQuery = await tenantHelper.prepareTenantsQueryForExcelCreator(
    query
  )
  const tenants = await tenantHelper.getTenantsByAggregate(preparedQuery)
  const [tenant = {}] = tenants || []
  const {
    tenantIdsWithPhoneNumbers = [],
    tenantNamesWithoutPhoneNumbers = [],
    tenantNamesWithPhoneNumbers = []
  } = tenant
  return {
    idsWithPhoneNumbers: tenantIdsWithPhoneNumbers,
    tenantNamesWithoutPhoneNumbers,
    tenantNamesWithPhoneNumbers
  }
}

const prepareAccountQueryAndGetData = async (preparedQuery, partnerId) => {
  const [property = {}] =
    (await propertyHelper.getPropertyAndAccountIds(preparedQuery)) || []

  console.log('Account property response =>', JSON.stringify(property))
  const { accountIds = [] } = property || {}
  const accountsQuery = {
    _id: { $in: accountIds },
    partnerId
  }
  const [account = {}] = await accountHelper.getAccountsByAggregate(
    accountsQuery
  )
  console.log('Account response =>', JSON.stringify(account))

  const {
    accountNamesWithoutPhoneNumbers = [],
    accountIdsWithPhoneNumbers = [],
    accountNamesWithPhoneNumbers = []
  } = account
  return {
    idsWithPhoneNumbers: accountIdsWithPhoneNumbers,
    accountNamesWithoutPhoneNumbers,
    accountNamesWithPhoneNumbers
  }
}

const prepareTenantQueryAndGetData = async (preparedQuery, partnerId) => {
  const [property = {}] =
    (await propertyHelper.getPropertyAndAccountIds(preparedQuery)) || []
  const { propertyIds = [] } = property || {}
  console.log('Tenant property response =>', JSON.stringify(property))

  const tenantsQuery = {
    partnerId,
    properties: {
      $elemMatch: { propertyId: { $in: propertyIds }, status: 'active' }
    }
  }
  const [tenant = {}] = await tenantHelper.getTenantsByAggregate(tenantsQuery)
  console.log('Tenant response =>', JSON.stringify(tenant))

  const {
    tenantIdsWithPhoneNumbers = [],
    tenantNamesWithoutPhoneNumbers = [],
    tenantNamesWithPhoneNumbers = []
  } = tenant
  return {
    idsWithPhoneNumbers: tenantIdsWithPhoneNumbers,
    tenantNamesWithoutPhoneNumbers,
    tenantNamesWithPhoneNumbers
  }
}

const prepareAccountTenantQueryAndGetData = async (
  preparedQuery,
  partnerId
) => {
  const accountsInfo = await prepareAccountQueryAndGetData(
    preparedQuery,
    partnerId
  )
  const tenantsInfo = await prepareTenantQueryAndGetData(
    preparedQuery,
    partnerId
  )
  return {
    idsWithPhoneNumbers: concat(
      accountsInfo.idsWithPhoneNumbers,
      tenantsInfo.idsWithPhoneNumbers
    ),
    accountNamesWithoutPhoneNumbers:
      accountsInfo?.accountNamesWithoutPhoneNumbers,
    accountNamesWithPhoneNumbers: accountsInfo?.accountNamesWithPhoneNumbers,
    tenantNamesWithoutPhoneNumbers: tenantsInfo?.tenantNamesWithoutPhoneNumbers,
    tenantNamesWithPhoneNumbers: tenantsInfo?.tenantNamesWithPhoneNumbers
  }
}

const getAccountsAndTenantsData = async (propertiesQuery) => {
  const { sendTo = '', partnerId = '' } = propertiesQuery
  const { preparedQuery } =
    await propertyHelper.preparePropertiesQueryFromFilterData(propertiesQuery)
  console.log('=== sendTo', sendTo)
  console.log('=== Prepare properties query', JSON.stringify(preparedQuery))
  if (sendTo === 'accounts')
    return await prepareAccountQueryAndGetData(preparedQuery, partnerId)
  else if (sendTo === 'tenants')
    return await prepareTenantQueryAndGetData(preparedQuery, partnerId)
  else if (sendTo === 'all')
    return await prepareAccountTenantQueryAndGetData(preparedQuery, partnerId)
}

const getPhoneNumbersInfoByContext = async (body, user) => {
  const { query = {} } = body || {}
  console.log('=== query ===', query)
  const {
    accountsQuery = {},
    tenantsQuery = {},
    propertiesQuery = {}
  } = query || {}
  const { partnerId = '' } = user || {}
  const { context = '' } = query
  console.log('=== accountsQuery ===', accountsQuery)
  console.log('===  context ===', context)
  if (context === 'accounts') {
    accountsQuery.partnerId = partnerId
    return await getAccountsDataBasedOnPhoneNumber(accountsQuery)
  } else if (context === 'tenants') {
    tenantsQuery.partnerId = partnerId
    return getTenantsDataBasedOnPhoneNumber(tenantsQuery)
  } else {
    propertiesQuery.partnerId = partnerId
    return getAccountsAndTenantsData(propertiesQuery)
  }
}

export const getEmptyPhoneNumbersInfo = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  const phoneNumbersInfo = await getPhoneNumbersInfoByContext(body, user)
  return phoneNumbersInfo
}

export const prepareNewNotificationLogDataToResendEmailOrSms = async (
  notificationLogData,
  metaData
) => {
  const notificationInfo = omit(notificationLogData, [
    '_id',
    'complaint',
    'createdAt',
    'createdBy',
    'doNotSend',
    'errorReason',
    'history',
    'messageCost',
    'msgClickCount',
    'msgOpenCount',
    'processStartedAt',
    'rejectReason',
    'repairing',
    'retryCount',
    'SESMsgId',
    'totalMessages'
  ])

  notificationInfo.notificationLogId = notificationLogData._id
  notificationInfo.status = 'ready'
  if (
    metaData.toPhoneNumber &&
    notificationLogData.toPhoneNumber !== metaData.toPhoneNumber
  ) {
    notificationInfo.history = [
      {
        oldToPhoneNumber: notificationInfo.toPhoneNumber,
        toPhoneNumber: metaData.toPhoneNumber,
        changedAt: new Date()
      }
    ]
    notificationInfo.toPhoneNumber = metaData.toPhoneNumber
  }
  if (
    notificationLogData.type === 'email' &&
    metaData.toEmail &&
    metaData.toEmail !== notificationLogData.toEmail
  ) {
    notificationInfo.history = [
      {
        oldToEmail: notificationInfo.toEmail,
        toEmail: metaData.toEmail,
        changedAt: new Date()
      }
    ]
    notificationInfo.toEmail = metaData.toEmail
  }
  //Implementation of before insert
  notificationInfo.events = [
    {
      status: 'ready',
      createdAt: new Date()
    }
  ]
  const partnerId = notificationLogData?.partnerId
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
    partnerId: partnerId ? partnerId : { $exists: false }
  })
  const fromPhoneNumber = partnerSettings?.smsSettings?.smsSenderName || null
  if (notificationLogData.type === 'sms' && fromPhoneNumber) {
    notificationInfo.fromPhoneNumber = fromPhoneNumber
  }
  notificationInfo.isResend = true
  return notificationInfo
}

const checkIfStringIsAnEmailAddress = (emailAddress = '') => {
  const regexEmail =
    /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
  return regexEmail.test(emailAddress)
}

const getUsersSendToInfos = async (userIds = [], options = {}) => {
  const notificationSendToInfos = []

  const {
    nameOrPhoneNumber = '',
    templateUniqueId = '',
    type = ''
  } = options || {}
  if (size(userIds)) {
    for (const userId of userIds) {
      const userSendToInfoParams = {
        nameOrPhoneNumber,
        type,
        userId
      }
      const userSendToInfo =
        (await getUserSendToInfo(userSendToInfoParams)) || {}

      if (size(userSendToInfo)) {
        const notificationSendToInfo = {
          ...userSendToInfo,
          templateUniqueId,
          type
        }
        notificationSendToInfos.push(notificationSendToInfo)
      }
    }
  }

  return notificationSendToInfos
}

const getNewPasswordUrl = async (tenantId) => {
  const tenantInfo = tenantId
    ? await tenantHelper.getATenant({ _id: tenantId }, null, ['user'])
    : {}
  const { partnerId, user = {} } = tenantInfo || {}

  if (!size(user)) return ''

  const email = user.getEmail() || ''
  const hasPassword = email ? user.hasPassword() : ''

  if (!hasPassword) {
    const token = await createVerificationToken(user._id, partnerId, email)
    // const newPasswordURL =
    //   (await appHelper.getPartnerURL(partnerId)) +
    //   '/create-new-password/?u=' +
    //   user._id +
    //   '&t=' +
    //   token +
    //   '&pid=' +
    //   partnerId
    // return newPasswordURL
    const urlParams = {
      urlParamsV1:
        '/create-new-password/?u=' +
        user._id +
        '&t=' +
        token +
        '&pid=' +
        partnerId,
      urlParamsV2:
        '/create-new-password/?u=' +
        user._id +
        '&t=' +
        token +
        '&pid=' +
        partnerId
    }

    const newPasswordUrl =
      (await prepareRedirectUrlForLinkService(partnerId, urlParams, true)) || '' // redirects to link service
    return newPasswordUrl
  }

  return ''
}

const prepareRedirectUrlForLinkService = async (
  partnerId = '',
  urlParams = {},
  isPartnerPublicUrl
) => {
  const { urlParamsV1 = '', urlParamsV2 = '' } = urlParams
  const v1Url = (await appHelper.getPartnerURL(partnerId, true)) + urlParamsV1
  let v2Url = ''
  if (isPartnerPublicUrl) {
    v2Url = (await appHelper.getPartnerPublicURL(partnerId)) + urlParamsV2
  } else {
    v2Url = (await appHelper.getPartnerURL(partnerId, false)) + urlParamsV2
  }
  const linkForV1AndV2 = `redirect?v2_url=${v2Url}&v1_url=${v1Url}`
  const preparedUrl = appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`
  return preparedUrl
}

const prepareV2LinkForChat = async (collectionData, sendToUserInfo) => {
  const { conversationId } = collectionData
  const conversation = await conversationHelper.getAConversation({
    _id: conversationId
  })
  const {
    partnerId = '',
    propertyId = '',
    tenantId = '',
    accountId = ''
  } = conversation
  if (partnerId) {
    const { partners = [] } = sendToUserInfo

    const partnerInfo = partners.find(
      (partner) => partner.partnerId === partnerId
    )
    const { type = '' } = partnerInfo || {}
    let url = ''
    if (type === 'tenant' || type === 'account')
      url =
        (await appHelper.getPartnerPublicURL(partnerId)) +
        '/inbox/' +
        conversationId
    else if (propertyId)
      url =
        (await appHelper.getPartnerURL(partnerId)) +
        '/property/properties/' +
        propertyId
    else if (accountId)
      url =
        (await appHelper.getPartnerURL(partnerId)) + '/accounts/' + accountId
    else if (tenantId)
      url = (await appHelper.getPartnerURL(partnerId)) + '/tenants/' + tenantId
    return url
  } else {
    return appHelper.getPublicURL() + '/inbox/' + conversationId
  }
}

const prepareReplyLink = async (collectionData, sendToUserInfo) => {
  const stage = process.env.STAGE || 'local'
  const { conversationId } = collectionData
  const v1Link =
    stage === 'production'
      ? `https://uniteliving.com/chat/${conversationId}`
      : `https://${stage}.uniteliving.com/chat/${conversationId}`
  const v2Link = await prepareV2LinkForChat(collectionData, sendToUserInfo)
  const linkForV1AndV2 = `redirect?v2_url=${v2Link}&v1_url=${v1Link}`
  return appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`
}

const getUserInfoByConversation = async (conversation, identityId) => {
  const { identity } = conversation
  if (size(identity)) {
    const sendToIdentity = find(
      identity,
      (identityObj) => identityObj.id === identityId
    )
    const { userId = '' } = sendToIdentity || {}
    if (userId) {
      const userInfo = (await UserCollection.findOne({ _id: userId })) || {}
      return userInfo
    }
  }
}

export const getSendMailToInfo = async (req) => {
  const { body, user = {} } = req
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const { query } = body

  if (!size(query)) throw new CustomError(400, 'Query data can not be empty')

  const { context, sendTo, notificationType } = query
  query.partnerId = partnerId

  if (!sendTo) throw new CustomError(400, 'SendTo is required')
  if (!context) throw new CustomError(400, 'Context is required')
  if (notificationType !== 'email')
    throw new CustomError(400, 'Notification type must be email')

  const isBrokerPartner = await partnerHelper.getAPartner({
    _id: partnerId,
    accountType: 'broker'
  })

  console.log('=== isBrokerPartner', !!isBrokerPartner)
  console.log('=== notificationType', notificationType)
  console.log('=== context', context)

  if (context === 'tenant') {
    const tenantQuery =
      await notificationLogHelper.prepareTenantQueryForEmailSendToAll(query)
    console.log('===> tenantQuery', { ...tenantQuery })
    const tenantsInfo = await tenantHelper.getTenantNamesByAggregate(
      tenantQuery
    )
    console.log('=== tenants after aggregate ==> ', tenantsInfo)
    const [tenants = {}] = tenantsInfo || []
    const { tenantNames = [] } = tenants
    return { tenantNames }
  } else if (context === 'account') {
    const accountQuery =
      await notificationLogHelper.prepareAccountQueryForEmailSendToAll(
        query,
        size(isBrokerPartner)
      )
    console.log('===> accountQuery', { ...accountQuery })
    const accountsInfo = await accountHelper.getAccountNamesByAggregate(
      accountQuery
    )
    console.log('=== accounts after aggregate ==> ', accountsInfo)
    const [accounts = {}] = accountsInfo || []
    const { accountNames = [] } = accounts
    return { accountNames }
  } else if (context === 'property') {
    const { preparedQuery: queryData } =
      await propertyHelper.preparePropertiesQueryFromFilterData(query)

    const property = await propertyHelper.getPropertyAndAccountIds(queryData)
    console.log('property ', property)
    const tenantQuery = {
      partnerId,
      $and: [
        {
          properties: {
            $elemMatch: {
              status: 'active',
              propertyId: { $in: property[0]?.propertyIds }
            }
          }
        }
      ]
    }
    console.log('=== sendTo', sendTo)
    if (sendTo === 'tenants') {
      const { tenantNamesWithActiveLease, tenantNamesWithClosedLease } =
        await tenantHelper.getTenantIdsBasedOnLeaseStatus(tenantQuery)
      return {
        tenantNames: tenantNamesWithActiveLease,
        tenantNamesWithClosedLease
      }
    } else if (size(isBrokerPartner) && sendTo === 'accounts') {
      const accountQuery = { _id: { $in: property[0]?.accountIds } }
      const accountsInfo = await accountHelper.getAccountNamesByAggregate(
        accountQuery
      )

      console.log('=== accounts after aggregate ==> ', accountsInfo)
      const [accounts = {}] = accountsInfo || []
      const { accountNames = [] } = accounts
      return { accountNames }
    } else if (sendTo === 'all') {
      const { tenantNamesWithActiveLease, tenantNamesWithClosedLease } =
        await tenantHelper.getTenantIdsBasedOnLeaseStatus(tenantQuery)

      const response = {
        tenantNames: tenantNamesWithActiveLease,
        tenantNamesWithClosedLease
      }

      if (size(isBrokerPartner)) {
        const accountQuery = { _id: { $in: property[0]?.accountIds } }
        const accountsInfo = await accountHelper.getAccountNamesByAggregate(
          accountQuery
        )

        console.log('=== accounts after aggregate ==> ', accountsInfo)
        const [accounts = {}] = accountsInfo || []
        response.accountNames = accounts.accountNames
      }
      return response
    }
  } else throw new CustomError(400, 'Invalid context found')
}
