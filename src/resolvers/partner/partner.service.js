import { has, size, isEmpty, omit, indexOf, compact } from 'lodash'
import nid from 'nid'
import moment from 'moment-timezone'

import { CustomError } from '../common'
import { PartnerCollection } from '../models'
import {
  accountHelper,
  accountingHelper,
  addonHelper,
  apiKeyHelper,
  appHelper,
  branchHelper,
  counterHelper,
  ledgerAccountHelper,
  partnerHelper,
  phoneNumberHelper,
  propertyRoomItemHelper,
  taxCodeHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import {
  accountingService,
  addonService,
  appQueueService,
  apiKeyService,
  appRoleService,
  branchService,
  counterService,
  ledgerAccountService,
  partnerSettingService,
  phoneNumberService,
  propertyRoomItemService,
  taxCodeService,
  userService
} from '../services'

export const createAPartner = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for partner creation')
  }
  const createdPartner = await PartnerCollection.create([data], { session })
  if (isEmpty(createdPartner)) {
    throw new CustomError(404, 'Unable to create partner')
  }
  return createdPartner
}

export const updateAPartner = async (query, data, session, populate = []) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for partner update')
  }
  const updatedPartner = await PartnerCollection.findOneAndUpdate(query, data, {
    session,
    new: true,
    runValidators: true
  }).populate(populate)
  if (!size(updatedPartner)) {
    throw new CustomError(404, `Unable to update Partner`)
  }
  return updatedPartner
}

export const addRolesForPartner = async (params, session) => {
  // Create 3 roles for new partner
  const { _id, ownerId, createdBy } = params
  const partnerId = _id
  const createdAt = new Date()
  const rolesIds = []
  const partnerAdmin = {
    // For Partner Admin
    name: 'Admin',
    users: [ownerId],
    type: 'partner_admin',
    partnerId,
    createdAt,
    createdBy
  }
  const partnerAgent = {
    // For Partner Agent
    name: 'Agent',
    users: [ownerId],
    type: 'partner_agent',
    partnerId,
    createdAt,
    createdBy
  }
  const partnerAccounting = {
    // For Partner Accounting
    name: 'Accounting',
    users: [],
    type: 'partner_accounting',
    partnerId,
    createdAt,
    createdBy
  }
  const partnerJanitor = {
    // For Partner Janitor
    name: 'Janitor',
    users: [],
    type: 'partner_janitor',
    partnerId,
    createdAt,
    createdBy
  }
  const rolesDataArray = [
    partnerAdmin,
    partnerAgent,
    partnerAccounting,
    partnerJanitor
  ]
  const createdRoles = await appRoleService.createRoles(rolesDataArray, session)
  for (const roles of createdRoles) {
    rolesIds.push(roles._id)
  }
}

export const addBranchForPartner = async (params, session) => {
  // Create default Branch for new partner
  const { _id, ownerId, createdBy } = params
  const defaultBranchData = {
    name: 'Main Branch',
    agents: [ownerId],
    adminId: ownerId,
    partnerId: _id,
    createdBy
  }
  const createdBranch = await branchService.createABranch(
    defaultBranchData,
    session
  )
  if (isEmpty(createdBranch)) {
    throw new CustomError(404, 'Unable to create branch')
  }
}

export const addDefaultPartnerSetting = async (params, session) => {
  // Create default Partner Settings for new partner
  const partnerSettingsData = await partnerHelper.preparePartnerSettingsData(
    params,
    session
  )
  await partnerSettingService.createAPartnerSetting(
    partnerSettingsData,
    session
  )
}

export const addTaxCodeForPartner = async (params, session) => {
  // Create TaxCode for new partner.
  const taxCodeIds = {}
  const { _id, createdBy } = params
  const query = { partnerId: { $exists: false } }
  const taxCodes = await taxCodeHelper.getTaxCodes(query, session)
  for (const taxCode of taxCodes) {
    const oldTaxCodeId = taxCode._id
    const taxCodeData = omit(taxCode, ['_id', 'createdAt', 'createdBy'])
    taxCodeData.partnerId = _id
    taxCodeData.createdBy = createdBy
    const [createdTaxCode] = await taxCodeService.createATaxCode(
      taxCodeData,
      session
    )
    taxCodeIds[oldTaxCodeId] = createdTaxCode._id
  }
  return taxCodeIds
}

export const addLedgerAccountForPartner = async (
  params,
  taxCodeIds,
  session
) => {
  // Create LedgerAccount for new partner.
  const ledgerAccountIds = {}
  const { _id, createdBy } = params
  const query = { partnerId: { $exists: false } }
  const ledgerAccounts = await ledgerAccountHelper.getLedgerAccounts(
    query,
    session
  )
  for (const ledgerAccount of ledgerAccounts) {
    const oldLedgerAccountId = ledgerAccount._id
    const ledgerAccountData = omit(ledgerAccount, [
      '_id',
      'createdAt',
      'createdBy'
    ])
    ledgerAccountData.partnerId = _id
    ledgerAccountData.taxCodeId = taxCodeIds[ledgerAccount.taxCodeId] // Find new taxCode id
    ledgerAccountData.createdBy = createdBy
    const [createdLedgerAccount] =
      await ledgerAccountService.createALedgerAccount(
        ledgerAccountData,
        session
      )
    ledgerAccountIds[oldLedgerAccountId] = createdLedgerAccount._id
  }
  return ledgerAccountIds
}

export const addAccountingForPartner = async (
  params,
  ledgerAccountIds,
  session
) => {
  // Create Accounting for new partner.
  const accountingIds = []
  const { _id, accountType, createdBy } = params
  const query = { partnerId: { $exists: false } }
  const accountings = await accountingHelper.getAccountings(query, session)
  for (const accounting of accountings) {
    const accountingData = omit(accounting, ['_id', 'createdAt', 'createdBy'])
    accountingData.createdBy = createdBy
    accountingData.partnerId = _id
    accountingData.debitAccountId = ledgerAccountIds[accounting.debitAccountId] // Find account id
    accountingData.creditAccountId =
      ledgerAccountIds[accounting.creditAccountId] // Find account id
    if (accountType === 'direct') {
      // Remove some accounting for direct partners
      const accountingTypeArray = [
        'payout_to_landlords',
        'brokering_commission',
        'management_commission',
        'addon_commission',
        'final_settlement_payment'
      ]
      if (indexOf(accountingTypeArray, accounting.type) === -1) {
        const [createdAccounting] = await accountingService.createAnAccounting(
          accountingData,
          session
        )
        accountingIds.push(createdAccounting._id)
      }
    } else {
      const [createdAccounting] = await accountingService.createAnAccounting(
        accountingData,
        session
      )
      accountingIds.push(createdAccounting._id)
    }
  }
  return accountingIds
}

export const addAddonsForPartner = async (
  params,
  ledgerAccountIds,
  session
) => {
  // Create Addons for new partner.
  const addonIds = []
  const { _id, accountType, createdBy } = params
  const query = { partnerId: { $exists: false } }
  const addons = await addonHelper.getAddons(query, session)
  for (const addon of addons) {
    const addonData = omit(addon, ['_id', 'createdAt', 'createdBy'])
    addonData.partnerId = _id
    addonData.createdBy = createdBy
    addonData.debitAccountId = ledgerAccountIds[addon.debitAccountId] // Find account id
    addonData.creditAccountId = ledgerAccountIds[addon.creditAccountId] // Find account id
    if (accountType === 'direct' && addon.type === 'lease') {
      addonData.enableCommission = false
      const [createdAddon] = await addonService.createAnAddon(
        addonData,
        session
      )
      addonIds.push(createdAddon._id)
    } else if (accountType === 'broker') {
      const [createdAddon] = await addonService.createAnAddon(
        addonData,
        session
      )
      addonIds.push(createdAddon._id)
    }
  }
  return addonIds
}

export const addSerialInPartner = async (params, session) => {
  // Adding serial in new partner
  const { _id } = params
  const serialId = await counterService.incrementCounter('partner', session)
  const updatedPartner = await updateAPartner(
    { _id },
    { serial: serialId },
    session
  )
  return updatedPartner
}

export const addRoomItemForPartner = async (params, session) => {
  // Create Property room items for partner settings
  const roomItemsIds = []
  const { _id, createdBy } = params
  const query = { partnerId: { $exists: false } }
  const roomItems = await propertyRoomItemHelper.getPropertyRoomItems(
    query,
    session
  )
  for (const roomItem of roomItems) {
    const roomItemData = omit(roomItem, ['_id', 'createdAt', 'createdBy'])
    roomItemData.partnerId = _id
    roomItemData.createdBy = createdBy
    const [createdRoomItem] = await propertyRoomItemService.createARoomItem(
      roomItemData,
      session
    )
    roomItemsIds.push(createdRoomItem._id)
  }
}

export const addApiKeyForPartner = async (params, session) => {
  // Create/Add API KEY for new partner verification while transaction
  const { _id, createdBy } = params
  let isApiKeyExists = true
  let randomKey = ''
  while (isApiKeyExists) {
    randomKey = nid(30)
    isApiKeyExists = await apiKeyHelper.getAnApiKey(
      { apiKey: randomKey },
      session
    )
  }
  if (randomKey && !isApiKeyExists) {
    const data = { partnerId: _id, apiKey: randomKey, createdBy }
    await apiKeyService.createAnApiKey(data, session)
  }
}

export const addPartnerInPhoneNumber = async (partner, session) => {
  const { _id, phoneNumber } = partner
  await phoneNumberHelper.updateAPhoneNumber(
    { phoneNumber },
    { $set: { partnerId: _id } },
    session
  )
}

export const createPartner = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  partnerHelper.validatePartnerAddData(body)
  const {
    name,
    subDomain,
    ownerName,
    ownerEmail,
    accountType,
    sms,
    phoneNumber
  } = body
  await partnerHelper.validateSubDomain(subDomain.toLowerCase(), '', session)
  let owner = {}
  if (ownerName && ownerEmail) {
    const params = { name: ownerName, email: ownerEmail }
    const [createdUser] = await userService.createAnUserWithNameAndEmail(
      params,
      session
    )
    body.ownerId = createdUser._id
    owner = createdUser
  } else {
    owner = await userHelper.getUserById(body.ownerId)
    if (!size(owner)) throw new CustomError(404, 'Owner not found')
  }
  const partnerCreationData = {
    // Set partner creation data/info
    name,
    subDomain: subDomain.toLowerCase(),
    ownerId: body.ownerId,
    isActive: true,
    accountType,
    sms,
    createdBy: user.userId
  }
  if (sms) {
    appHelper.checkRequiredFields(['phoneNumber'], body)
    partnerCreationData.phoneNumber = phoneNumber
    const existingPhoneNumber = await phoneNumberHelper.getPhoneNumber({
      phoneNumber,
      partnerId: { $exists: false }
    })
    if (!size(existingPhoneNumber)) {
      throw new CustomError(400, 'Phone number not available')
    }
  } else {
    partnerCreationData.phoneNumber = ''
  }
  const [partner] = await createAPartner(partnerCreationData, session)
  if (sms) {
    await addPartnerInPhoneNumber(partner.toObject(), session)
  }
  await addRolesForPartner(partner.toObject(), session)
  await addBranchForPartner(partner.toObject(), session)
  await addDefaultPartnerSetting(partner.toObject(), session)
  const taxCodeIds = await addTaxCodeForPartner(partner.toObject(), session)
  const ledgerAccountIds = await addLedgerAccountForPartner(
    partner.toObject(),
    taxCodeIds,
    session
  )
  await addAccountingForPartner(partner.toObject(), ledgerAccountIds, session)
  await addAddonsForPartner(partner.toObject(), ledgerAccountIds, session)
  const updatedPartner = await addSerialInPartner(partner.toObject(), session)
  await addRoomItemForPartner(partner.toObject(), session)
  await addApiKeyForPartner(partner.toObject(), session)
  await sendUserInvitation(
    {
      senderUserId: user.userId,
      invitedUser: owner,
      partnerId: partner._id
    },
    session
  )

  return [updatedPartner]
}

export const addPartnerUser = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  let invitedUserId
  const { partnerId, userId } = user
  body.partnerId = partnerId
  partnerHelper.checkPartnerUsersData(body) // Checking required Data for partner user creation
  const { name, email, roles, branchId, partnerEmployeeId } = body
  const partner = await partnerHelper.getAPartner({ _id: partnerId }, session)
  if (isEmpty(partner)) {
    throw new CustomError(404, 'Partner not found')
  }
  const existingUser = await userHelper.getUserByEmail(email, session)
  let invitedUser = {}
  if (size(existingUser)) {
    invitedUserId = existingUser._id
    invitedUser = existingUser
  } else {
    const params = { name, email }
    const [createdUser] = await userService.createAnUserWithNameAndEmail(
      params,
      session
    )
    invitedUserId = createdUser._id
    invitedUser = createdUser
  }
  if (invitedUserId) {
    const partnersData = {
      partnerId,
      type: 'user',
      status: 'invited'
    }
    if (partnerEmployeeId) {
      const params = { partnerId, partnerEmployeeId }
      const isExistingEmployeeId = await userHelper.existingEmployeeId(
        params,
        session
      )
      if (isExistingEmployeeId) {
        throw new CustomError(409, 'EmployeeId already exists')
      }
      partnersData.employeeId = partnerEmployeeId
    }
    const existingInvitation =
      invitedUser.partners &&
      invitedUser.partners.find(
        (item) => item.partnerId === partnerId && item.type === 'user'
      )
    if (existingInvitation) {
      throw new CustomError(400, 'User already invited')
    }
    const userData = { $push: { partners: partnersData } }
    const userInfo = await userService.updateAnUser(
      { _id: invitedUserId },
      userData,
      session
    ) // Update user for partner user
    if (size(roles)) {
      // Add user in app-roles collection
      const roleData = { $addToSet: { users: invitedUserId } }
      await appRoleService.updateRoles(
        { type: { $in: roles }, partnerId },
        roleData,
        session
      )
    }
    if (branchId) {
      // Add agent in branches collection, If branchId is exists
      const branchData = { $addToSet: { agents: invitedUserId } }
      await branchService.updateABranch(
        { _id: branchId, partnerId },
        branchData,
        session
      )
    }
    await sendUserInvitation(
      {
        senderUserId: userId,
        invitedUser: userInfo,
        partnerId
      },
      session
    )
    return userInfo
  }
}

export const sendUserInvitation = async (params = {}, session) => {
  const { senderUserId, invitedUser, partnerId, type = 'user' } = params
  const senderUser = await userHelper.getAnUser({ _id: senderUserId }, session)

  if (!size(senderUser)) throw new CustomError(404, 'Sender user not found')

  let invitationVerificationUrl = ''
  let token = nid(30)
  //create token for new partner
  //don't create token for exiting validated user or facebook user
  const expires = moment().add(7, 'days').toDate()

  // Updating user partners array
  const { partners = [] } = invitedUser
  // For re-invitation from roles
  const updatingData = {}
  const query = {
    _id: invitedUser._id
  }
  for (const partner of partners) {
    if (partner.partnerId === partnerId && partner.type === type) {
      // Don't replace token if already exist, just extend the expires date
      if (partner.token) token = partner.token
      query.partners = {
        $elemMatch: {
          partnerId,
          type
        }
      }
      updatingData.$set = {
        'partners.$.token': token,
        'partners.$.expires': expires,
        'partners.$.status': 'invited'
      }
    }
  }
  if (!size(updatingData)) {
    updatingData.$push = {
      partners: {
        expires,
        partnerId,
        status: 'invited',
        token,
        type: 'user'
      }
    }
  }
  await userService.updateAnUser(query, updatingData, session)
  if (invitedUser?.services?.facebook || invitedUser?.services?.password) {
    invitationVerificationUrl =
      appHelper.getAuthServiceURL() +
      `/verify-user-invitation-token/${token}/${invitedUser._id}/${partnerId}`
  } else {
    //create verification password
    // invitationVerificationUrl =
    //   (await appHelper.getPartnerURL(partnerId)) +
    //   `/create-new-password/${token}/${invitedUser._id}/${partnerId}`

    const v2SubDomain = await appHelper.getPartnerURL(partnerId, false)
    console.log('Checking v2SubDomain: ', v2SubDomain)
    const v2_url = `${v2SubDomain}/create-new-password/${token}/${invitedUser._id}/${partnerId}`
    console.log('Checking v2_url: ', v2_url)
    const v1RedirectUrl =
      (await appHelper.getPartnerURL(partnerId, true)) +
      `/create-new-password/${token}/${invitedUser._id}/${partnerId}`
    const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${v1RedirectUrl}`
    console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
    const invitationRedirectUrl =
      appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`
    invitationVerificationUrl = invitationRedirectUrl
  }
  //Create an app queue to send email notification to invited user

  const queueData = {
    event: 'partner_user_invitation',
    action: 'send_email',
    destination: 'notifier',
    priority: 'immediate',
    params: {
      toEmail: invitedUser.getEmail() || '',
      userId: invitedUser._id,
      userLang: invitedUser?.profile?.language || 'no',
      variablesData: {
        user_name: invitedUser?.profile?.name,
        partner_user_name: senderUser?.profile?.name,
        invitation_verification_url: invitationVerificationUrl
      }
    }
  }

  await appQueueService.createAnAppQueue(queueData, session)
}

const updatePhoneNumberWhileUpdatePartner = async (
  partnerId,
  partnerData,
  session
) => {
  const { sms, phoneNumber } = partnerData
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  console.log('found partner info ', partnerInfo)
  if (!size(partnerInfo)) {
    throw new CustomError(404, 'No partner found')
  }
  console.log('found sms ', sms)
  if (!sms) {
    console.log('partnerInfo.phoneNumber ', partnerInfo.phoneNumber)
    if (partnerInfo.phoneNumber && partnerInfo.phoneNumber !== '') {
      console.log('preparing for update data', partnerId)
      await phoneNumberService.updateAPhoneNumber(
        {
          partnerId
        },
        { $unset: { partnerId: 1 } },
        session
      )
    }
    partnerData.phoneNumber = ''
    console.log('partnerData', partnerData)
  } else {
    const existenceOfPhoneNumber = await phoneNumberHelper.getPhoneNumber({
      phoneNumber,
      $or: [{ partnerId: { $exists: false } }, { partnerId }]
    })
    if (!size(existenceOfPhoneNumber)) {
      throw new CustomError(400, 'Phone number not available')
    }
    await phoneNumberService.updateAPhoneNumber(
      {
        phoneNumber
      },
      { $set: { partnerId } },
      session
    )
    if (partnerInfo.phoneNumber && partnerInfo.phoneNumber !== phoneNumber) {
      await phoneNumberService.updateAPhoneNumber(
        {
          phoneNumber: partnerInfo.phoneNumber
        },
        { $unset: { partnerId: 1 } },
        session
      )
    }
  }
}

export const updatePartner = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  await partnerHelper.checkPartnerUpdatingData(body)
  const { partnerId, partnerData } = body
  const { subDomain = '' } = partnerData
  if (size(subDomain)) partnerData.subDomain = subDomain.toLowerCase()
  if (has(partnerData, 'sms')) {
    console.log('partnerData has sms field ', partnerData)
    await updatePhoneNumberWhileUpdatePartner(partnerId, partnerData, session)
  }
  console.log('Checking for partnerData ', partnerData)
  const updatedPartner = await updateAPartner(
    { _id: partnerId },
    partnerData,
    session,
    'owner'
  )
  console.log('Checking for updatedPartner ', updatedPartner)
  return updatedPartner
}

export const activatePartner = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  partnerHelper.partnerIdValidationCheck(body)
  const { partnerId } = body
  const query = { _id: partnerId }
  const data = { isActive: true }
  const updatedPartner = await updateAPartner(query, data, session)
  return updatedPartner
}

export const deactivatePartner = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  partnerHelper.partnerIdValidationCheck(body)
  const { partnerId } = body
  const query = { _id: partnerId }
  const data = { isActive: false }
  const updatedPartner = await updateAPartner(query, data, session)
  return updatedPartner
}

export const updatePartnerLogo = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body, user = {} } = req
  const { partnerId } = user
  const { logo, siteLogo } = body
  const query = { _id: partnerId }
  const data = {}
  if (logo) data.logo = logo
  if (siteLogo) data.siteLogo = siteLogo
  if (!size(data)) {
    throw new CustomError(400, 'Nothing to update')
  }
  const updatedPartner = await updateAPartner(query, { $set: data })
  updatedPartner.logoUrl = updatedPartner.logo
    ? appHelper.getCDNDomain() +
      '/partner_logo/' +
      partnerId +
      '/' +
      updatedPartner.logo
    : ''
  updatedPartner.siteLogoUrl = updatedPartner.siteLogo
    ? appHelper.getCDNDomain() +
      '/partner_logo/' +
      partnerId +
      '/' +
      updatedPartner.siteLogo
    : ''
  return updatedPartner
}

export const updatePartnerFinnId = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  appHelper.checkRequiredFields(['partnerId', 'finnId'], body)
  const { partnerId, finnId } = body
  appHelper.validateId({ partnerId })
  appHelper.validateId({ finnId })
  const query = { _id: partnerId }
  const data = { finnId }
  const updatedPartner = await updateAPartner(query, data, session)
  return updatedPartner
}

export const updatePartnerUserEmployeeId = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  const { partner } = await partnerHelper.validationCheckForPartnerEmployeeId(
    body,
    session
  )
  const { partnerId, partnerUserId, partnerEmployeeId } = body
  const query = {
    _id: partnerUserId,
    partners: { $elemMatch: { partnerId } }
  }
  const data = { 'partners.$.employeeId': partnerEmployeeId }
  await userService.updateAnUser(query, data, session)
  return partner
}

export const updatePartnerUserStatus = async (req) => {
  const { body, session, user = {} } = req
  const { userId, partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  body.partnerId = partnerId
  await partnerHelper.validationCheckForPartnerUserStatus(body, session)
  const { partnerUserId, status } = body
  const query = {
    _id: partnerUserId,
    partners: { $elemMatch: { partnerId, type: 'user' } }
  }
  const data = { $set: { 'partners.$.status': status } }
  const updatePartnerStatus = await userService.updateAnUser(
    query,
    data,
    session
  )
  if (status === 'invited') {
    const owner = await userHelper.getAnUser({ _id: partnerUserId })
    const params = {
      senderUserId: userId,
      invitedUser: owner,
      partnerId
    }
    await sendUserInvitation(params, session)
  }
  // Remove partner agent From Role & Branch Collection
  if (status === 'inactive') {
    const filteredBranch = await branchHelper.getABranch({
      partnerId,
      agents: partnerUserId
    })
    const roleData = { $pull: { users: partnerUserId } }
    const branchData = { $pull: { agents: partnerUserId } }
    await appRoleService.updateRoles({ partnerId }, roleData, session)
    if (size(filteredBranch))
      await branchService.updateBranches(
        { _id: filteredBranch._id, partnerId },
        branchData,
        session
      )
  }
  return updatePartnerStatus
}

const addAppQueuesForLegacyTransactions = async (partnerId, session) => {
  const invoiceAppQueue =
    (await transactionHelper.getAppQueueDataForRentInvoiceTransactions(
      partnerId
    )) || {}
  const invoiceLostAppQueue =
    (await transactionHelper.getAppQueueDataForInvoicesLostTransactions(
      partnerId
    )) || {}
  const paymentAppQueue =
    (await transactionHelper.getAppQueueDataForPaymentsTransactions(
      partnerId
    )) || {}
  const payoutsAppQueue =
    (await transactionHelper.getAppQueueDataForPayoutsTransactions(
      partnerId
    )) || {}
  const landlordInvoiceAppQueue =
    await transactionHelper.getAppQueueDataForLandlordInvoiceTransactions(
      partnerId
    )
  const serialIdAppQueue = await transactionHelper.prepareSerialIdAppQueue(
    partnerId,
    session
  )
  let appQueues = []
  if (size(invoiceAppQueue)) {
    appQueues.push(invoiceAppQueue)
  }
  if (size(invoiceLostAppQueue)) {
    appQueues.push(invoiceLostAppQueue)
  }
  if (size(paymentAppQueue)) {
    appQueues.push(paymentAppQueue)
  }
  if (size(payoutsAppQueue)) {
    appQueues.push(payoutsAppQueue)
  }
  if (size(landlordInvoiceAppQueue)) {
    appQueues.push(landlordInvoiceAppQueue)
  }
  if (size(serialIdAppQueue)) {
    appQueues.push(serialIdAppQueue)
  }
  appQueues = compact(appQueues)
  if (!size(appQueues)) {
    return true
  }
  // console.log('updateCounterCollection')
  // await updateCounterCollection(appQueues, partnerId, session)
  // console.log('updateCounterCollection done')
  // appQueues[0].status = 'new' // set first queue status 'new', others are 'hold'
  const addedAppQueues = await appQueueService.createMultipleAppQueues(
    appQueues,
    session
  )
  console.log('addedAppQueues ', addedAppQueues)
  return addedAppQueues
}

export const updatePartnerTransaction = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId } = body
  if (!partnerId) {
    throw new CustomError(400, 'Missing partnerId')
  }
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  await addAppQueuesForLegacyTransactions(partnerId, session)
  const updateData = { $set: { enableTransactions: true } }
  const updatedPartner = await updateAPartner(
    { _id: partnerId },
    updateData,
    session
  )
  return updatedPartner
}

export const updatePartnerFunctionality = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  appHelper.validateId({ partnerId })
  const partner = await partnerHelper.getPartnerById(partnerId)
  if (!size(partner)) throw new CustomError(404, 'Partner not found')
  const updateData = partnerHelper.preparePartnerFunctionalityUpdateData(
    body,
    partner
  )
  const updatedPartner = await updateAPartner(
    { _id: partnerId },
    { $set: updateData },
    session
  )
  //To implement after update hook partner
  if (
    updateData.enableInvoiceStartNumber &&
    !partner.enableInvoiceStartNumber
  ) {
    const accounts = await accountHelper.getAccounts({ partnerId })
    if (size(accounts)) {
      const counterInfo = await counterHelper.getACounter({ _id: partnerId })
      const next_val = counterInfo?.next_val
      if (next_val) {
        for (const account of accounts) {
          await counterService.createACounter(
            {
              _id: `invoice-start-number-${account._id}`,
              next_val
            },
            session
          )
        }
      }
    }
  }
  if (updateData.enableCreditRating && !partner.enableCreditRating) {
    //Enable automatically credit rating for partner
    await partnerSettingService.updateAPartnerSetting(
      { partnerId },
      {
        'tenantSetting.automaticCreditRating.enabled': true
      },
      session
    )
  }
  return updatedPartner
}
