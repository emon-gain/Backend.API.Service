import nid from 'nid'
import { cloneDeep, includes, size, uniq } from 'lodash'

import { AccountCollection } from '../models'
import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  branchHelper,
  contractHelper,
  counterHelper,
  logHelper,
  organizationHelper,
  partnerHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  contractService,
  counterService,
  logService,
  organizationService,
  partnerSettingService,
  tenantService,
  userService
} from '../services'

export const createAnAccount = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for account creation')
  }
  const createdAccount = await AccountCollection.create([data], { session })
  if (!size(createdAccount)) {
    throw new CustomError(404, `Unable to create an account`)
  }
  return createdAccount
}

export const updateAnAccount = async (query, data, session, populate = []) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedAccountData = await AccountCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  ).populate(populate)
  if (!size(updatedAccountData)) {
    throw new CustomError(404, `Unable to update Account`)
  }
  return updatedAccountData
}

export const updateAccounts = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedAccountData = await AccountCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  if (updatedAccountData.nModified > 0) {
    return updatedAccountData
  }
}

export const createOrUpdateUser = async (params, user, session) => {
  const {
    name,
    email,
    phoneNumber,
    norwegianNationalIdentification,
    zipCode,
    address,
    city,
    country,
    isDemo
  } = params

  // TODO:: Later need to write test case for nid check.
  if (norwegianNationalIdentification) {
    const params = {
      norwegianNationalIdentification:
        user?.profile?.norwegianNationalIdentification,
      currentNorwegianNationalId: norwegianNationalIdentification
    }
    user
      ? await tenantHelper.checkNIDDuplication(params)
      : await appHelper.checkNIDDuplication(norwegianNationalIdentification)
  }
  if (size(user)) {
    // User exists with the email, update existing user.
    const userData = {}
    if (norwegianNationalIdentification)
      userData['profile.norwegianNationalIdentification'] =
        norwegianNationalIdentification
    if (!user?.profile?.phoneNumber)
      userData['profile.phoneNumber'] = phoneNumber || ''
    if (address) userData['profile.hometown'] = address
    if (zipCode) userData['profile.zipCode'] = zipCode
    if (city) userData['profile.city'] = city
    if (country) userData['profile.country'] = country
    if (size(userData)) {
      const updatedUser = await userService.updateAnUser(
        { _id: user._id },
        { $set: userData },
        session
      )
      return updatedUser
    }
    return user
  } else {
    // User doesn't exists with the email, create new user.
    const profileData = {
      name,
      phoneNumber: phoneNumber || '',
      hometown: address || '',
      norwegianNationalIdentification: norwegianNationalIdentification || '',
      zipCode,
      city,
      country,
      isDemoUser: isDemo
    }
    const [createdUser] = await userService.createAnUserWithNameAndEmail(
      {
        name,
        email,
        profile: profileData
      },
      session
    )
    return createdUser
  }
}

export const createABrokerAccountForPersonType = async (params, session) => {
  const { name, partnerId, branchId, agentId, email, createdBy } = params
  if (!email) {
    throw new CustomError(400, 'Email is required for person type account')
  }
  const user = await userHelper.getUserByEmail(email, session)
  const accountName = user ? user.profile.name : name
  // If we found personId, then we'll use that otherwise we'll add new user and set user._id as a personId.
  // And create A BrokerAccount for person type
  const createdOrUpdatedUserData = await createOrUpdateUser(
    params,
    user,
    session
  )
  // So, we have the user, now create a BrokerAccount for person type.
  const personAccountData = {
    type: 'person',
    personId: createdOrUpdatedUserData._id,
    name: accountName,
    partnerId,
    branchId,
    agentId,
    status: 'in_progress',
    totalActiveProperties: 0,
    createdBy
  }
  const [createdAccount] = await createAnAccount(personAccountData, session)
  return createdAccount
}

export const prepareDataAndCreateOrUpdateAnUser = async (
  params,
  contactPersonUser,
  session
) => {
  const userCreatingAndUpdatingData = {
    name: params.contactPersonName,
    email: params.contactPersonEmail,
    address: params.contactPersonAddress,
    phoneNumber: params.contactPersonPhoneNumber,
    isDemo: params.isDemo
  }
  const createdOrUpdatedUser = await createOrUpdateUser(
    userCreatingAndUpdatingData,
    contactPersonUser,
    session
  )
  return createdOrUpdatedUser
}

export const createABrokerAccountForOrganizationType = async (
  params,
  session
) => {
  const {
    name,
    personId,
    partnerId,
    branchId,
    agentId,
    orgId,
    zipCode,
    address,
    city,
    country,
    contactPersonEmail,
    createdBy
  } = params
  appHelper.compactObject(params)
  let contactPersonUser = null
  // Find or create new user/contact person
  if (personId) {
    contactPersonUser = await userHelper.getUserById({ _id: personId })
    if (!size(contactPersonUser)) {
      throw new CustomError(404, 'Invalid contact person')
    }
  } else if (contactPersonEmail)
    contactPersonUser = await userHelper.getUserByEmail(contactPersonEmail)
  if (!size(contactPersonUser)) {
    appHelper.checkRequiredFields(
      ['contactPersonEmail', 'contactPersonName'],
      params
    )
  }
  const createdOrUpdatedUserData = await prepareDataAndCreateOrUpdateAnUser(
    params,
    contactPersonUser,
    session
  )
  const [createdOrganization] = await organizationService.createAnOrganization(
    { name, orgId },
    session
  )
  const organizationAccountData = {
    type: 'organization',
    organizationId: createdOrganization._id,
    personId: createdOrUpdatedUserData._id,
    name,
    address,
    partnerId,
    branchId,
    agentId,
    status: 'in_progress',
    totalActiveProperties: 0,
    zipCode,
    city,
    country,
    createdBy
  }
  const [createdAccount] = await createAnAccount(
    organizationAccountData,
    session
  )
  return createdAccount
}

export const createBrokerAccount = async (params, session) => {
  const { type } = params
  if (!type) {
    throw new CustomError(400, 'Type is required for broker partner account')
  }
  let createdBrokerAccount
  if (type === 'person') {
    // Creating person type BrokerAccount
    createdBrokerAccount = await createABrokerAccountForPersonType(
      params,
      session
    )
  } else if (type === 'organization') {
    // Creating organization type BrokerAccount
    createdBrokerAccount = await createABrokerAccountForOrganizationType(
      params,
      session
    )
  } else {
    throw CustomError(404, 'Account type is wrong!')
  }
  return createdBrokerAccount
}

export const createDirectAccount = async (params, session) => {
  const {
    name,
    partnerId,
    branchId,
    agentId,
    invoiceAccountNumber,
    vatRegistered,
    orgId,
    address,
    createdBy,
    zipCode,
    city,
    country
  } = params
  const bankAccountNumbers = invoiceAccountNumber ? [invoiceAccountNumber] : []
  const organizationData = { name, createdBy }
  if (orgId) {
    organizationData.orgId = orgId
  }
  const [organization] = await organizationService.createAnOrganization(
    organizationData,
    session
  )
  if (invoiceAccountNumber) {
    // If invoice account number isn't exist in partner settings, then create new bank account
    const bankData = {
      accountNumber: invoiceAccountNumber,
      vatRegistered,
      canUsePartnerAccountNumber: true // It's user for partnerAccountNumber validation.
    }
    const body = {}
    body.partnerId = partnerId
    const createdBankAccount = await partnerSettingService.createABankAccount(
      body,
      bankData,
      session
    )
    const directAccountData = {
      type: 'organization',
      name,
      partnerId,
      branchId,
      agentId,
      status: 'in_progress',
      organizationId: organization._id,
      totalActiveProperties: 0,
      address,
      city,
      country,
      zipCode,
      invoiceAccountNumber,
      bankAccountNumbers,
      vatRegistered,
      createdBy
    }
    if (size(createdBankAccount)) {
      directAccountData.serial = await counterService.incrementCounter(
        `account-${partnerId}`,
        session
      ) // Setting serial
      const [createdAccount] = await createAnAccount(directAccountData, session) // Creating Account
      if (!size(createdAccount)) {
        throw new CustomError(404, `Unable to create an Account`)
      }
      return createdAccount
    }
  }
}

export const addRelationBetweenContactPersonAndPartner = async (
  params,
  session
) => {
  const { personId, partnerId } = params
  if (personId) {
    const accountData = {
      userId: personId,
      type: 'account',
      partnerId
    }
    await userService.addRelationBetweenUserAndPartner(accountData, session) // Adding partners info for user
  }
}

export const updateOrganizationAndAccount = async (params, session) => {
  const { _id, partnerId, type, organizationId } = params
  if (type === 'organization' && organizationId) {
    const query = {
      _id: organizationId
    }
    const data = {
      partnerId,
      accountId: _id
    }
    const organizationInfo = await organizationHelper.getAnOrganization(
      query,
      session
    )
    const updatedOrganizationData =
      await organizationService.updateAnOrganization(query, data, session)
    if (organizationInfo !== updatedOrganizationData) {
      const updatingData = {
        lastUpdate: new Date()
      }
      await updateAnAccount({ organizationId }, updatingData, session)
    }
  }
}

export const createAccount = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, roles = [], userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  // For authenticate partner_agent which includes at least one branch
  if (roles.includes('partner_agent') && roles.length === 1) {
    if (!(await appHelper.isAvailableBranchOfAgent(userId, partnerId))) {
      throw new CustomError(401, 'Unauthorized')
    }
  }
  // Only partner_admin has access to create accounts for another agents
  if (!(roles.includes('partner_admin') || roles.includes('app_manager')))
    body.agentId = userId
  body.partnerId = partnerId
  body.createdBy = userId
  // Checking required fields and duplicate account
  await accountHelper.checkRequiredAndDuplicatedData(body)
  // Checking if partner exists or not
  const partner = await partnerHelper.getPartnerById(partnerId, session)
  if (!size(partner)) {
    throw new CustomError(404, `Can't find any valid partner for the account.`)
  }
  // Based on the partner type, we'll create different types of accounts.
  // Direct partner shouldn't have person type contacts
  let createdAccount = {}
  if (partner.accountType === 'direct') {
    appHelper.checkRequiredFields(['invoiceAccountNumber'], body)
    // Create Direct Account
    const { invoiceStartNumber } = body
    createdAccount = await createDirectAccount(body, session)
    if (invoiceStartNumber && partner.enableInvoiceStartNumber) {
      // Creating counters for invoice-start-number
      let nextValue = invoiceStartNumber - 1
      nextValue = nextValue < 0 ? 0 : nextValue
      const counterData = {
        _id: `invoice-start-number-${createdAccount._id}`,
        next_val: nextValue
      }
      await counterService.createACounter(counterData, session)
    }
  } else {
    // Create Broker Account
    createdAccount = await createBrokerAccount(body, session)
  }
  if (!size(createdAccount)) {
    throw new CustomError(405, `Couldn't create account`)
  }
  await addRelationBetweenContactPersonAndPartner(createdAccount, session)
  await updateOrganizationAndAccount(createdAccount, session)
  // Preparing return data
  const { contactPersonEmail, email } = body
  const returnData = await accountHelper.getAccountCreationReturnData(
    {
      contactPersonEmail,
      createdAccount,
      email,
      partnerType: partner.accountType
    },
    session
  )
  return returnData
}

export const updateAccountAbout = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'aboutText'], body)
  const { partnerId } = user
  const { aboutText, accountId } = body
  const query = {
    _id: accountId,
    partnerId
  }
  const updatingData = {
    aboutText
  }
  const updatedAccount = await updateAnAccount(query, {
    $set: updatingData
  })
  return {
    _id: updatedAccount._id,
    aboutText: updatedAccount.aboutText
  }
}

export const updateAccountLogo = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'logo'], body)
  const { partnerId } = user
  const { accountId, logo } = body
  const query = {
    _id: accountId,
    partnerId
  }
  const accountInfo = await accountHelper.getAnAccount(query)
  if (!size(accountInfo)) throw new CustomError(404, "Account doesn't exists")
  let avatarKey = ''
  const cdn = appHelper.getCDNDomain()
  if (accountInfo.organizationId) {
    // Updating Organization Image
    const organizationQuery = {
      _id: accountInfo.organizationId
    }
    await organizationService.updateAnOrganization(
      organizationQuery,
      { image: logo },
      session
    )
    avatarKey = cdn + '/partner_logo/' + partnerId + '/accounts/' + logo
  } else {
    const userUpdatingData = {
      'profile.avatarKey': `profile_images/${logo}`
    }
    await userService.updateAnUser(
      { _id: accountInfo.personId },
      userUpdatingData,
      session
    ) // Updating User Image
    avatarKey = cdn + '/profile_images/' + logo
  }
  const updatedAccountInfo = await updateAnAccount(
    { _id: accountId },
    { lastUpdate: new Date() },
    session
  )
  return {
    _id: updatedAccountInfo._id,
    avatarKey
  }
}

export const updateAccountStatus = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'changeStatus'], body)
  const { partnerId } = user
  const { accountId, changeStatus } = body
  const query = {
    _id: accountId,
    partnerId
  }
  const updatingData = {
    $set: {
      status: changeStatus
    }
  }
  const updatedAccount = await updateAnAccount(query, updatingData)
  return {
    _id: updatedAccount._id,
    status: updatedAccount.status
  }
}

export const createAccountUpdatedLog = async (action, options, session) => {
  const { createdBy } = options
  const accountLogData = await logHelper.prepareAccountUpdatedLogData(
    action,
    options,
    session
  )
  if (!size(accountLogData)) {
    throw new CustomError(404, 'Could not create log for account update')
  }
  if (createdBy) accountLogData.createdBy = createdBy
  await logService.createLog(accountLogData, session)
}

export const updateAccountBranchInfo = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'branchId', 'agentId'], body)
  const { partnerId, userId } = user
  const { accountId, agentId, branchId } = body
  const updatingData = { agentId, branchId }
  // Check exists agent in branch
  const branchQuery = {
    _id: branchId,
    partnerId,
    agents: agentId
  }
  const isExistsBranchAgent = await branchHelper.getABranch(branchQuery)
  if (!size(isExistsBranchAgent)) {
    throw new CustomError(404, 'Owner not available in this branch')
  }
  const query = {
    _id: accountId,
    partnerId
  }
  const previousAccount = await accountHelper.getAnAccount(query)
  const updatedAccount = await updateAnAccount(
    query,
    {
      $set: updatingData
    },
    session
  )
  if (
    size(updatedAccount) &&
    size(previousAccount) &&
    previousAccount.agentId !== agentId
  ) {
    const options = {
      // Creating a log for account's agentId updated / accounts updated
      partnerId,
      collectionId: accountId,
      context: 'account',
      fieldName: 'agentId',
      value: agentId,
      previousDoc: previousAccount,
      createdBy: userId
    }
    await createAccountUpdatedLog('updated_account', options, session)
  }
  return await prepareUpdateAccountBranchInfoReturnData(updatedAccount)
}

const prepareUpdateAccountBranchInfoReturnData = async (account = {}) => {
  const { _id, agentId, branchId } = account
  const agent = (await userHelper.getUserById(agentId)) || {}
  if (size(agent)) {
    agent.avatarKey = userHelper.getAvatar(agent)
  }
  const branch = (await branchHelper.getBranchById(branchId)) || {}
  return {
    _id,
    agentInfo: {
      _id: agent._id,
      avatarKey: agent.avatarKey,
      name: agent.profile?.name
    },
    branchInfo: {
      _id: branch._id,
      name: branch.name
    }
  }
}

export const addBankAccount = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'bankAccountNumber'], body)
  accountHelper.validateDataForAddBankAccount(body)
  const { partnerId, userId } = user
  const { accountId, bankAccountNumber } = body
  const query = {
    _id: accountId,
    partnerId
  }
  const accountInfo = await accountHelper.getAnAccount(query)
  if (!size(accountInfo)) {
    throw new CustomError(404, "Account doesn't exists")
  }
  // If bank account number exists then return an error
  if (includes(accountInfo.bankAccountNumbers, bankAccountNumber))
    throw new CustomError(405, 'Account number already exists')
  const allBankAccounts = accountInfo.bankAccountNumbers || []
  allBankAccounts.push(bankAccountNumber)
  const updatedAccount = await updateAnAccount(
    query,
    { bankAccountNumbers: allBankAccounts },
    session
  )
  if (size(updatedAccount)) {
    // Creating a log for account's bank account number added / accounts updated
    const options = {
      partnerId,
      collectionId: accountId,
      context: 'account',
      fieldName: 'bankAccountNumbers',
      newText: bankAccountNumber,
      createdBy: userId
    }
    await createAccountUpdatedLog('updated_account', options, session)
  }
  return {
    _id: updatedAccount._id,
    bankAccountNumbers: updatedAccount.bankAccountNumbers
  }
}

export const removeBankAccount = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'bankAccountNumber'], body)
  const { accountId, bankAccountNumber } = body
  const { partnerId, userId } = user
  const query = {
    _id: accountId,
    partnerId
  }
  const accountInfo = await accountHelper.getAnAccount(query)
  if (!size(accountInfo)) {
    throw new CustomError(404, "Account doesn't exists")
  }
  const bankAccountNumbers = accountInfo.bankAccountNumbers || []
  if (!bankAccountNumbers.includes(bankAccountNumber))
    throw new CustomError(404, "Account number doesn't exists")
  if (accountInfo.invoiceAccountNumber === bankAccountNumber)
    throw new CustomError(405, 'Account number is using')
  const duplicateAccountQuery = {
    _id: { $ne: accountId },
    partnerId,
    invoiceAccountNumber: bankAccountNumber
  }
  const isUsedInAnotherAccount = await accountHelper.getAnAccount(
    duplicateAccountQuery
  )
  if (!size(isUsedInAnotherAccount)) {
    const data = {
      partnerId,
      oldBankAccountNumber: bankAccountNumber
    }
    await partnerSettingService.updateOrDeleteBankAccount(data, session)
  }
  const updatedAccount = await updateAnAccount(
    query,
    { $pull: { bankAccountNumbers: bankAccountNumber } },
    session
  )
  const options = {
    // Creating a log for account's bank account number removed / accounts updated
    partnerId,
    collectionId: accountId,
    context: 'account',
    fieldName: 'bankAccountNumbers',
    oldText: bankAccountNumber,
    createdBy: userId
  }
  await createAccountUpdatedLog('updated_account', options, session)
  return {
    _id: updatedAccount._id,
    bankAccountNumbers: updatedAccount.bankAccountNumbers
  }
}

export const updateContractsAndCreateLog = async (
  body,
  previousDoc,
  session
) => {
  let contractIds = []
  let isContractUpdated = {}
  const { accountId, newValue, oldValue, partnerId, userId } = body
  if (oldValue !== newValue) {
    // Update all contract's payouts id with newBankId
    const contractQuery = {
      accountId,
      payoutTo: oldValue,
      status: { $ne: 'closed' }
    }
    contractIds = await contractHelper.getUniqueFieldValue('_id', contractQuery)
    if (size(contractIds)) {
      const contractUpdatingQuery = { _id: { $in: contractIds } }
      const contractData = { payoutTo: newValue }
      isContractUpdated = await contractService.updateContracts(
        contractUpdatingQuery,
        contractData,
        session
      )
    }
  }
  const options = {
    // Creating a log for account's bank account number updated / accounts updated
    partnerId,
    collectionId: accountId,
    context: 'account',
    fieldName: 'bankAccountNumbers',
    previousDoc,
    oldText: oldValue,
    newText: newValue,
    isVisibleInProperty: size(isContractUpdated) ? !!isContractUpdated : false,
    contractIds,
    createdBy: userId
  }
  await createAccountUpdatedLog('updated_account', options, session)
}

export const updateBankAccount = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'newValue', 'oldValue'], body)
  const { partnerId, userId } = user
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  if (!size(partnerInfo)) throw new CustomError(404, 'Partner not found')
  body.userId = userId
  body.partnerId = partnerId
  const { accountId, newValue, oldValue } = body
  accountHelper.validateDataForAddBankAccount({
    accountId,
    bankAccountNumber: newValue
  })
  const query = { _id: accountId, partnerId }
  const accountInfo = await accountHelper.getAnAccount(query, session)
  if (!size(accountInfo)) {
    throw new CustomError(404, "Account doesn't exists")
  }
  const bankAccountNumbers = accountInfo.bankAccountNumbers || []
  if (!bankAccountNumbers.includes(oldValue))
    throw new CustomError(404, "Account number doesn't exists")
  if (bankAccountNumbers.includes(newValue))
    throw new CustomError(405, 'Account number already exists')
  const updatingData = {}
  const duplicateAccountQuery = {
    _id: { $ne: accountId },
    partnerId,
    invoiceAccountNumber: oldValue
  }
  const isUsedInAnotherAccount = !!(await accountHelper.getAnAccount(
    duplicateAccountQuery
  ))
  const data = {
    partnerId,
    oldBankAccountNumber: oldValue,
    newBankAccountNumber: newValue,
    shouldAddNewBankAccount: isUsedInAnotherAccount
  }
  await partnerSettingService.updateOrDeleteBankAccount(data, session) // Manage Bank Account Number in partner Settings
  const oldValueIndex = bankAccountNumbers.findIndex(
    (item) => item === oldValue
  )
  bankAccountNumbers[oldValueIndex] = newValue
  updatingData.bankAccountNumbers = bankAccountNumbers
  accountInfo.invoiceAccountNumber === oldValue
    ? (updatingData.invoiceAccountNumber = newValue)
    : ''
  if (size(updatingData)) {
    const updatedAccount = await updateAnAccount(
      query,
      {
        $set: updatingData
      },
      session
    ) // Update account's bank number
    await updateContractsAndCreateLog(body, accountInfo, session) // Update contracts payouts and creating account updating log
    // Implementation of after update hook
    if (
      partnerInfo.accountType === 'direct' &&
      updatedAccount.type === 'organization' &&
      updatingData.invoiceAccountNumber
    ) {
      await contractService.updateContracts(
        {
          accountId,
          partnerId,
          status: { $in: ['active', 'upcoming'] }
        },
        {
          $set: {
            'rentalMeta.invoiceAccountNumber': updatingData.invoiceAccountNumber
          }
        },
        session
      )
    }
    return {
      _id: updatedAccount._id,
      bankAccountNumbers: updatedAccount.bankAccountNumbers
    }
  }
}

export const createAccountActivitiesUpdatingLog = async (params, session) => {
  const {
    accountId = '',
    organizationId = '',
    partnerId = '',
    previousAccount = {},
    previousOrganization = {},
    previousPersonInfo = {},
    userId
  } = params
  if (size(previousAccount)) {
    // If previousAccount is null, Do nothing
    const newAccount = accountId
      ? await accountHelper.getAnAccount({ _id: accountId }, session)
      : {}
    const newOrganization =
      accountId && organizationId
        ? await organizationHelper.getAnOrganization(
            { _id: organizationId, accountId },
            session
          )
        : {}
    const { personId } = newAccount
    const presentPersonInfo = personId
      ? await userHelper.getAnUser({ _id: personId }, session)
      : {}
    const options = {
      partnerId,
      collectionId: accountId,
      context: 'account',
      previousDoc: previousAccount,
      createdBy: userId
    }
    const data = {
      newAccount,
      previousAccount,
      newOrganization,
      previousOrganization,
      presentPersonInfo,
      previousPersonInfo,
      organizationId
    }
    const { changeFieldOptions = [] } =
      logHelper.prepareChangeFieldOptionsData(data)
    if (size(changeFieldOptions)) {
      options.changesFields = changeFieldOptions
      if (!organizationId) {
        options.accountType = 'organization'
      }
      await createAccountUpdatedLog('updated_account', options, session)
    }
  }
}

const updateAccountAndTenantForUpdateContactPersonName = async (
  params = {},
  session
) => {
  const { name, personId, updatedAccountId, userId } = params
  const changeLogsData = []
  // If user's profile name changed, then update All personType Accounts profile name too
  const previousAccounts = await accountHelper.getAccounts({
    _id: {
      $ne: updatedAccountId
    },
    personId,
    type: 'person',
    name: {
      $ne: name
    }
  })
  if (size(previousAccounts)) {
    await updateAccounts(
      {
        _id: {
          $ne: updatedAccountId
        },
        personId,
        type: 'person'
      },
      { $set: { name } },
      session
    )
    for (const previousAccount of previousAccounts) {
      const logData = {
        _id: nid(17),
        partnerId: previousAccount.partnerId,
        context: 'account',
        action: 'updated_account',
        isChangeLog: true,
        accountId: previousAccount._id,
        visibility: ['account'],
        changes: [
          {
            field: 'name',
            type: 'text',
            oldText: previousAccount.name,
            newText: name
          }
        ],
        createdBy: userId
      }
      changeLogsData.push(logData)
    }
  }
  // If user's profile name changed, then update Tenants profile name too
  const previousTenants = await tenantHelper.getTenants({
    userId: personId,
    name: {
      $ne: name
    }
  })
  if (size(previousTenants)) {
    await tenantService.updateTenants(
      {
        userId: personId
      },
      { $set: { name } },
      session
    )
  }
  for (const previousTenant of previousTenants) {
    const logData = {
      _id: nid(17),
      partnerId: previousTenant.partnerId,
      context: 'tenant',
      action: 'updated_tenant',
      isChangeLog: true,
      tenantId: previousTenant._id,
      visibility: ['tenant'],
      changes: [
        {
          field: 'name',
          type: 'text',
          oldText: previousTenant.name,
          newText: name
        }
      ],
      createdBy: userId
    }
    changeLogsData.push(logData)
  }
  if (size(changeLogsData)) {
    await logService.createLogs(changeLogsData, session)
  }
}

export const updateContactPerson = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['accountId', 'email', 'name'], body)
  const { partnerId, userId } = user
  const { accountId, address, email, name, phoneNumber } = body
  let { personId } = body
  const previousPersonInfo = personId
    ? await userHelper.getUserById(personId)
    : {}
  if (personId) {
    // Update contact person info
    if (!size(previousPersonInfo)) {
      throw new CustomError(404, "user doesn't exists")
    }
    const profileData = {}
    if (body.hasOwnProperty('phoneNumber')) {
      profileData['profile.phoneNumber'] = phoneNumber
    }
    if (body.hasOwnProperty('address')) {
      profileData['profile.hometown'] = address
    }
    if (name) profileData['profile.name'] = name
    if (size(profileData)) {
      const updatedUser = await userService.updateAnUser(
        { _id: personId },
        {
          $set: profileData
        },
        session
      )
      if (
        previousPersonInfo.profile.name !== updatedUser.profile.name &&
        name
      ) {
        await updateAccountAndTenantForUpdateContactPersonName(
          {
            name,
            personId,
            userId
          },
          session
        )
      }
    }
  } else if (name && email) {
    // Check for existing email
    const existingUser = await userHelper.getUserByEmail(email)
    if (size(existingUser)) throw new CustomError(409, 'Email already in use')
    // Create new contact person user
    const data = {
      address,
      email,
      name,
      phoneNumber
    }
    const { _id } = await createOrUpdateUser(data, null, session)
    personId = _id
  } else {
    throw new CustomError(400, 'Bad Request, personId or name & email required')
  }
  if (personId) {
    const query = { _id: accountId, partnerId }
    const updatingData = { personId, lastUpdate: new Date() }
    const previousAccount = await accountHelper.getAnAccount(query)
    if (!size(previousAccount)) throw new CustomError(404, 'Account not found')
    const updatedAccount = await updateAnAccount(
      query,
      {
        $set: updatingData
      },
      session
    ) // Updating contract person
    const newUser = await userHelper.getUserById(
      updatedAccount.personId,
      session
    )
    if (previousAccount.personId !== updatedAccount.personId) {
      await addRelationBetweenContactPersonAndPartner(updatedAccount, session)
      if (updatedAccount.type === 'organization') {
        const oldUser = await userHelper.getUserById(previousAccount.personId)
        const logData = {
          _id: nid(17),
          partnerId: updatedAccount.partnerId,
          context: 'account',
          action: 'updated_contact_person',
          agentId: updatedAccount.agentId,
          branchId: updatedAccount.branchId,
          isChangeLog: true,
          accountId: updatedAccount._id,
          visibility: ['account'],
          changes: [
            {
              field: 'personId',
              type: 'foreignKey',
              oldText: oldUser.profile.name,
              newText: newUser.profile.name
            }
          ],
          createdBy: userId
        }
        await logService.createLog(logData, session)
      }
    }
    const accountActivitiesData = {
      accountId,
      partnerId,
      previousAccount,
      previousPersonInfo,
      userId
    }
    await createAccountActivitiesUpdatingLog(accountActivitiesData, session) // Create Account Activities updating log
    return {
      _id: updatedAccount._id,
      userInfo: {
        _id: newUser._id,
        email: newUser.getEmail(),
        name: newUser.profile.name,
        phoneNumber: newUser.profile.phoneNumber,
        norwegianNationalIdentification:
          newUser.profile.norwegianNationalIdentification,
        hometown: newUser.profile.hometown,
        zipCode: newUser.profile.zipCode,
        city: newUser.profile.city,
        country: newUser.profile.country,
        avatarKey: userHelper.getAvatar(newUser)
      }
    }
  }
}

export const createAndGetBankAccountNumbers = async (
  params,
  oldBankAccountNumbers = [],
  session
) => {
  const { invoiceAccountNumber, name, partnerId, vatRegistered } = params
  const bankAccountNumbers = oldBankAccountNumbers
  const bankData = {
    accountName: name,
    accountNumber: invoiceAccountNumber,
    vatRegistered,
    canUsePartnerAccountNumber: true // It's user for partnerAccountNumber validation.
  }

  const body = {}
  body.partnerId = partnerId
  const createdBankAccount = await partnerSettingService.createABankAccount(
    body,
    bankData,
    session
  )
  if (size(createdBankAccount)) {
    bankAccountNumbers.push(invoiceAccountNumber)
    return uniq(bankAccountNumbers)
  }
}

export const prepareAccountInfoUpdatingData = async (
  accountId,
  params,
  session
) => {
  const {
    address,
    city,
    country,
    email,
    invoiceAccountNumber,
    name,
    norwegianNationalIdentification,
    partnerId,
    serial,
    vatRegistered,
    zipCode
  } = params
  const updateData = {}
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  if (!size(partnerInfo)) throw new CustomError(404, 'Partner not found')
  const isDirectPartner = partnerInfo.accountType === 'direct'
  if (isDirectPartner)
    appHelper.checkRequiredFields(['invoiceAccountNumber'], params)
  if (name) updateData.name = name
  if (address) updateData.address = address
  const previousAccount = await accountHelper.getAnAccount({
    _id: accountId,
    partnerId
  })
  if (!size(previousAccount)) throw new CustomError(404, 'Account not found')
  if (previousAccount.type === 'organization') {
    updateData.city = city
    updateData.country = country
    updateData.zipCode = zipCode
  }
  if (
    invoiceAccountNumber &&
    invoiceAccountNumber !== previousAccount.invoiceAccountNumber
  ) {
    // If invoice account number isn't exist in partner settings, then create new bank account
    const oldBankAccountNumbers = previousAccount.bankAccountNumbers || []
    const accountsArray = await createAndGetBankAccountNumbers(
      params,
      [...oldBankAccountNumbers],
      session
    )
    if (oldBankAccountNumbers.length !== accountsArray.length)
      updateData.bankAccountNumbers = accountsArray
    updateData.invoiceAccountNumber = invoiceAccountNumber
    updateData.vatRegistered = vatRegistered
  }
  if (serial && serial !== previousAccount.serial) {
    // Check account serial validation
    updateData.serial = await accountHelper.checkingSerialNumberForAccount(
      accountId,
      {
        ...params,
        isDirectPartner
      }
    )
  }
  let previousPersonInfo = {}
  if (previousAccount.personId) {
    previousPersonInfo = await userHelper.getAnUser({
      _id: previousAccount.personId
    })
    if (!size(previousPersonInfo))
      throw new CustomError(404, 'Person not found')
    email && previousPersonInfo.getEmail() !== email
      ? await appHelper.checkEmailDuplication(email)
      : ''
    norwegianNationalIdentification &&
    previousPersonInfo.getNorwegianNationalIdentification() !==
      norwegianNationalIdentification
      ? await appHelper.checkNIDDuplication(norwegianNationalIdentification)
      : ''
  }
  return {
    directPartnerInfo: isDirectPartner ? partnerInfo : {},
    previousAccount,
    previousPersonInfo,
    updateData
  }
}

export const setProfileNames = async (accountData, session, userId) => {
  const { name, personId } = accountData
  const userData = { 'profile.name': name }
  const previousPerson = await userHelper.getUserById(personId)
  if (!size(previousPerson))
    throw new CustomError(404, 'Account person not found')
  if (name !== previousPerson.profile.name) {
    await userService.updateAnUser({ _id: personId }, userData, session) // Update user name with account name
    const params = {
      name,
      personId,
      updatedAccountId: accountData._id,
      userId
    }
    await updateAccountAndTenantForUpdateContactPersonName(params, session)
  }
}
const addOrUpdateAccountInvoiceStartNumber = async (
  accountId,
  invoiceStartNumber,
  session
) => {
  const counterInfo = await counterHelper.getACounter({
    _id: `invoice-start-number-${accountId}`
  })
  const counterId = counterInfo?._id
  let nextInvoiceNumber = invoiceStartNumber - 1

  if (counterId) {
    const allowedInvoiceStartNumber = counterInfo.next_val
      ? counterInfo.next_val + 1
      : 1
    const isInvalidInvoiceStartNumber =
      invoiceStartNumber < allowedInvoiceStartNumber

    if (isInvalidInvoiceStartNumber)
      throw new CustomError(
        405,
        `Invoice start number cannot be less than ${allowedInvoiceStartNumber}`
      )
    const updatedCounter = await counterService.updateACounter(
      { _id: counterId },
      { $set: { next_val: nextInvoiceNumber } },
      session
    )
    return updatedCounter
  }

  if (nextInvoiceNumber < 0) nextInvoiceNumber = 0
  const createdCounter = await counterService.createACounter(
    {
      _id: `invoice-start-number-${accountId}`,
      next_val: nextInvoiceNumber
    },
    session
  )
  return createdCounter
}
export const updateAccountInfo = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.compactObject(body, false)
  appHelper.checkRequiredFields(['accountId', 'name'], body)
  const { userId, partnerId } = user
  body.partnerId = partnerId
  const { accountId = '', invoiceStartNumber } = body
  appHelper.validateId({ accountId })
  const {
    updateData,
    previousAccount = {},
    previousPersonInfo = {},
    directPartnerInfo = {}
  } = await prepareAccountInfoUpdatingData(accountId, body, session)
  if (size(updateData) || invoiceStartNumber) {
    // Updating Accounts
    updateData.lastUpdate = new Date()
    const updatedAccount = await updateAnAccount(
      { _id: accountId, partnerId: user.partnerId },
      { $set: updateData },
      session
    )
    // Implementation of after update hook of account when account type person
    if (
      updatedAccount.type === 'person' &&
      size(previousAccount) &&
      previousAccount.name !== updatedAccount.name
    ) {
      const options = {
        partnerId: updatedAccount.partnerId,
        collectionId: updatedAccount._id,
        context: 'account',
        previousDoc: previousAccount,
        fieldName: 'name',
        createdBy: userId
      }
      await createAccountUpdatedLog('updated_account', options, session)
      await setProfileNames(updatedAccount, session, userId)
    }
    const {
      city,
      contactPersonAddress,
      country,
      email,
      norwegianNationalIdentification,
      orgId,
      partnerId,
      phone,
      zipCode
    } = body
    let previousOrganization = {}
    let updatedOrganization = {}
    if (updatedAccount.type === 'organization') {
      const organizationQuery = {
        _id: updatedAccount.organizationId,
        accountId
      }
      previousOrganization = await organizationHelper.getAnOrganization(
        organizationQuery
      )
      const updatingData = {}
      if (orgId) updatingData.orgId = orgId
      // Should update organization name when account name updated
      if (size(previousAccount) && previousAccount.name !== updatedAccount.name)
        updatingData.name = updatedAccount.name
      // Update OrganizationsCollection [orgId]
      updatedOrganization = await organizationService.updateAnOrganization(
        organizationQuery,
        { $set: updatingData },
        session
      )
      // Update active or upcoming leases when invoiceAccountNumber changed
      if (
        previousAccount.invoiceAccountNumber !==
        updatedAccount.invoiceAccountNumber
      ) {
        const contractQuery = {
          partnerId,
          accountId,
          status: { $in: ['active', 'upcoming'] }
        }
        const contractUpdatingData = {
          $set: {
            'rentalMeta.invoiceAccountNumber':
              updatedAccount.invoiceAccountNumber
          }
        }
        await contractService.updateContracts(
          contractQuery,
          contractUpdatingData,
          session
        )
      }
    }
    if (updatedAccount.type === 'person' && updatedAccount.personId) {
      // Updating User phone || email || NID
      const userData = {}
      if (email)
        userData.emails = [{ address: email.toLowerCase(), verified: true }]
      if (body.hasOwnProperty('phone')) {
        userData['profile.phoneNumber'] = phone
      }

      if (body.hasOwnProperty('norwegianNationalIdentification')) {
        userData['profile.norwegianNationalIdentification'] =
          norwegianNationalIdentification
      }

      if (contactPersonAddress) {
        userData['profile.hometown'] = contactPersonAddress
        userData['profile.city'] = city
        userData['profile.country'] = country
        userData['profile.zipCode'] = zipCode
      }
      if (size(userData))
        await userService.updateAnUser(
          { _id: updatedAccount.personId },
          userData,
          session
        )
    }
    const accountActivitiesData = {
      // Create Account Updating Log
      accountId,
      organizationId: updatedAccount.organizationId,
      partnerId,
      previousAccount,
      previousPersonInfo,
      previousOrganization,
      userId
    }
    await createAccountActivitiesUpdatingLog(accountActivitiesData, session) // Create Account Activities updating log
    const updatedUser = await userHelper.getUserById(
      updatedAccount.personId,
      session
    )
    if (invoiceStartNumber) {
      if (!size(directPartnerInfo))
        throw new CustomError(
          405,
          'Unable to update Invoice start number for broker partner'
        )
      if (!directPartnerInfo.enableInvoiceStartNumber)
        throw new CustomError(
          405,
          'Please enable the invoice start number from settings'
        )
      await addOrUpdateAccountInvoiceStartNumber(
        accountId,
        invoiceStartNumber,
        session
      )
    }
    return {
      _id: updatedAccount._id,
      serial: updatedAccount.serial,
      name: updatedAccount.name,
      orgId: updatedOrganization.orgId,
      address: updatedAccount.address,
      zipCode: updatedAccount.zipCode,
      city: updatedAccount.city,
      country: updatedAccount.country,
      accountNumber: updatedAccount.invoiceAccountNumber,
      bankAccountNumbers: updatedAccount.bankAccountNumbers,
      userInfo: size(updatedUser)
        ? {
            _id: updatedUser._id,
            email: updatedUser.getEmail(),
            name: updatedUser.profile.name,
            phoneNumber: updatedUser.profile.phoneNumber,
            norwegianNationalIdentification:
              updatedUser.profile.norwegianNationalIdentification,
            hometown: updatedUser.profile.hometown,
            zipCode: updatedUser.profile.zipCode,
            city: updatedUser.profile.city,
            country: updatedUser.profile.country,
            avatarKey: userHelper.getAvatar(updatedUser)
          }
        : null
    }
  } else throw new CustomError(400, 'Nothing to update')
}

export const updateAccountsTotalActiveProperties = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  const requiredFields = ['accountId', 'partnerId']
  appHelper.checkRequiredFields(requiredFields, body)
  const { accountId, partnerId } = body
  appHelper.validateId({ accountId })
  appHelper.validateId({ partnerId })
  const updateData =
    await accountHelper.prepareUpdateDataForTotalActiveProperties(body, session)
  const query = {
    _id: accountId,
    partnerId
  }
  const updatedAccount = await updateAnAccount(query, updateData, session)
  return updatedAccount
}

export const updateAccountForPogo = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const query = accountHelper.prepareAccountsQueryForUpdateAccountForPogo(body)
  const updatedData = accountHelper.getAccountUpdateData(body)
  const updatedAccount = await updateAnAccount(
    query,
    updatedData,
    session,
    'person'
  )
  const account = accountHelper.createAccountFieldNameForApi(updatedAccount)
  return account
}

export const downloadAccounts = async (req) => {
  const { body = {}, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  body.partnerId = partnerId
  const { agentId, branchId, createdAt, sort = { createdAt: -1 } } = body
  if (agentId) {
    appHelper.validateId({ agentId })
  }
  if (branchId) {
    appHelper.validateId({ branchId })
  }
  appHelper.validateSortForQuery(sort)
  const accountQuery = cloneDeep(body)
  const accountsQuery = await accountHelper.prepareQueryForAccounts(
    accountQuery
  )
  await appHelper.isMoreOrLessThanTargetRows(AccountCollection, accountsQuery, {
    moduleName: 'Accounts',
    rejectEmptyList: true
  })

  const params = {
    downloadProcessType: 'download_accounts',
    userId,
    ...body,
    sort
  }
  if (size(createdAt)) {
    const { startDate, endDate } = createdAt
    params.createdAt = {
      startDate: new Date(startDate),
      endDate: new Date(endDate)
    }
  }
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'
  if (body.status) params.accountStatus = body.status
  if (body.type) params.accountType = body.type

  const queueData = {
    action: 'download_email',
    event: 'download_email',
    destination: 'excel-manager',
    params,
    priority: 'immediate',
    status: 'new'
  }

  await appQueueService.createAnAppQueue(queueData, session)
  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

export const removeAnAccount = async (query, session) => {
  if (!size(query))
    throw new CustomError(400, 'Query must be required while removing account')

  const response = await AccountCollection.findOneAndDelete(query, { session })
  console.log('=== Account Removed ===', response)
  return response
}
