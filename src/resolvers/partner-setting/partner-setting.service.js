import { isEmpty, size } from 'lodash'
import validator from 'validator'
import nid from 'nid'

import { appPermission, CustomError } from '../common'
import { PartnerSettingCollection } from '../models'
import { appHelper, partnerSettingHelper } from '../helpers'
import { counterService, partnerService } from '../services'

export const addNewBankAccount = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  partnerSettingHelper.validateBankAccountAddData(body)
  const { bankAccountData } = body
  const createdBankAccount = await createABankAccount(
    body,
    bankAccountData,
    session
  )
  return createdBankAccount
}

export const createABankAccount = async (body, bankAccountData, session) => {
  const { partnerId } = body
  appHelper.compactObject(bankAccountData)
  const { accountNumber, vatRegistered, canUsePartnerAccountNumber } =
    bankAccountData
  partnerSettingHelper.validateDataForCreatingBankAccount(
    partnerId,
    bankAccountData
  )
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )

  if (!size(partnerSetting)) {
    throw new CustomError(404, `Could not find partner setting`)
  }
  const isAccountNumberExists =
    await partnerSettingHelper.isAccountNumberExists(accountNumber)
  console.log(
    '====> Check isAccountNumberExists: ',
    isAccountNumberExists,
    accountNumber
  )
  if (isAccountNumberExists) {
    if (canUsePartnerAccountNumber) {
      const bankAccounts = partnerSetting.bankAccounts || []
      const bankAccount = bankAccounts.find(
        (item) => item.accountNumber === accountNumber
      )
      console.log(
        '===> Check partner setting available bank account: ',
        bankAccount
      )
      if (bankAccount) return bankAccount
    }
    throw new CustomError(405, 'Account number already exists')
  }
  bankAccountData.id = nid(17)
  bankAccountData.vatRegistered = !!vatRegistered
  delete bankAccountData.canUsePartnerAccountNumber
  let updatedBankAccounts = []
  if (size(partnerSetting.bankAccounts)) {
    updatedBankAccounts = [...partnerSetting.bankAccounts, bankAccountData]
  } else {
    updatedBankAccounts = [bankAccountData]
  }
  await updateAPartnerSetting(
    {
      partnerId
    },
    {
      bankAccounts: updatedBankAccounts
    },
    session
  )
  return bankAccountData
}

export const createAPartnerSetting = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for partnerSetting creation')
  }
  const createdPartner = await PartnerSettingCollection.create([data], {
    session
  })
  if (isEmpty(createdPartner)) {
    throw new CustomError(404, 'Unable to create partnerSetting')
  }
  return createdPartner
}

export const deleteBankAccount = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId } = user
  const { accountNumber, bankAccountId } = body
  appHelper.checkRequiredFields(['userId'], user)
  if (partnerId) {
    body.partnerId = partnerId
  }
  partnerSettingHelper.validateBankAccountDeleteData(body)

  const query = {
    partnerId: body.partnerId,
    bankAccounts: {
      $elemMatch: { id: bankAccountId, accountNumber }
    }
  }

  let partnerSetting = await partnerSettingHelper.getAPartnerSetting(
    query,
    session
  )
  if (!size(partnerSetting)) {
    throw new CustomError(404, `Could not find partner setting`)
  }
  const { bankAccounts = [], bankPayment = {} } = partnerSetting
  const { firstMonthACNo, afterFirstMonthACNo } = bankPayment
  if (
    firstMonthACNo === accountNumber ||
    afterFirstMonthACNo === accountNumber
  ) {
    throw new CustomError(
      405,
      'Account number is used in partner bank payment setting'
    )
  }

  const isAccountNumberBeingUsed =
    await partnerSettingHelper.isAccountNumberBeingUsed(
      accountNumber,
      partnerId,
      session
    )
  if (isAccountNumberBeingUsed) {
    throw new CustomError(
      405,
      'Could not delete bank account, it is being used'
    )
  }

  const updateData = {
    bankAccounts: bankAccounts.filter(
      (bankAccount) => bankAccount.id !== bankAccountId
    )
  }
  partnerSetting = await updateAPartnerSetting(
    { partnerId },
    updateData,
    session
  )
  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Could not delete bank account')
  }
  return { bankAccountId, partnerId }
}

export const updateAPartnerSetting = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedPartnerSetting = await PartnerSettingCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return updatedPartnerSetting
}

export const updatePartnerSetting = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['data'], body)
  const { partnerId, data } = body
  const query = { partnerId }
  const updatedPartnerSetting = await updateAPartnerSetting(
    query,
    data,
    session
  )
  if (!size(updatedPartnerSetting)) {
    throw new CustomError(404, `Could not update partner setting`)
  }
  // Todo :pending: Reload Browser Policy => updateBrowserPolicyForIFrame
  return updatedPartnerSetting
}

const updateABankAccount = async (params, session) => {
  const {
    bankAccountData = {},
    bankAccountId,
    partnerId,
    partnerSetting,
    previousAccountNumber
  } = params
  const { accountNumber } = bankAccountData
  const isSameAccountNumber = accountNumber === previousAccountNumber
  if (accountNumber) {
    const isAccountNumberExists =
      await partnerSettingHelper.isAccountNumberExists(accountNumber, session)
    if (!isSameAccountNumber && isAccountNumberExists) {
      throw new CustomError(405, 'Account number already exists')
    }
  }
  const updateData =
    partnerSettingHelper.prepareBankAccountUpdateData(bankAccountData)

  // Update bankPayment settings for 'firstMonthACNo' or 'afterFirstMonthACNo'
  // If updated bankAccountNumber in bank accounts info, which used in bankPayment settings
  if (accountNumber && previousAccountNumber && !isSameAccountNumber) {
    const { bankPayment } = partnerSetting
    if (size(bankPayment)) {
      if (bankPayment.firstMonthACNo === previousAccountNumber) {
        updateData['bankPayment.firstMonthACNo'] = accountNumber
      }
      if (bankPayment.afterFirstMonthACNo === previousAccountNumber) {
        updateData['bankPayment.afterFirstMonthACNo'] = accountNumber
      }
    }
  }
  const query = {
    partnerId,
    bankAccounts: { $elemMatch: { id: bankAccountId } }
  }
  const updatedPartnerSetting = await updateAPartnerSetting(
    query,
    updateData,
    session
  )
  const updatedBankAccount = partnerSettingHelper.findBankAccount(
    updatedPartnerSetting,
    bankAccountId
  )
  return updatedBankAccount
}

export const updateBankAccountForPartnerSetting = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['data'], body)
  const { partnerId, bankId } = body
  const query = {
    partnerId,
    bankAccounts: { $elemMatch: { id: bankId } }
  }
  const { data } = body
  const updatedPartnerSetting = await updateAPartnerSetting(
    query,
    data,
    session
  )
  if (!size(updatedPartnerSetting)) {
    throw new CustomError(
      404,
      `Could not update bank account for partner setting`
    )
  }
  return updatedPartnerSetting
}

export const updateAppSetting = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['data'], body)
  const query = {
    partnerId: { $exists: false }
  }
  const { data } = body
  const updatedPartnerSetting = await updateAPartnerSetting(
    query,
    data,
    session
  )
  if (!size(updatedPartnerSetting)) {
    throw new CustomError(404, `Could not update app setting`)
  }
  return updatedPartnerSetting
}

export const updateBankAccount = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId } = user
  const { bankAccountId, bankAccountData } = body
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.compactObject(bankAccountData)
  if (partnerId) {
    body.partnerId = partnerId
  }
  partnerSettingHelper.validateBankAccountUpdateData(body)

  const query = {
    partnerId: body.partnerId,
    bankAccounts: { $elemMatch: { id: bankAccountId } }
  }
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(
    query,
    session
  )
  if (!size(partnerSetting)) {
    throw new CustomError(404, `Could not find partner setting`)
  }
  const previousAccountNumber = partnerSettingHelper.findAccountNumber(
    partnerSetting,
    bankAccountId
  )

  const isAccountNumberBeingUsed =
    await partnerSettingHelper.isAccountNumberBeingUsed(
      previousAccountNumber,
      body.partnerId,
      session
    )

  if (isAccountNumberBeingUsed) {
    throw new CustomError(
      405,
      'Could not update bank account, it is being used'
    )
  }
  const params = {
    bankAccountData,
    bankAccountId,
    partnerId: body.partnerId,
    partnerSetting,
    previousAccountNumber
  }
  const updatedBankAccount = await updateABankAccount(params, session)
  return updatedBankAccount
}

export const updateOrDeleteBankAccount = async (params, session) => {
  const {
    partnerId,
    oldBankAccountNumber,
    newBankAccountNumber,
    shouldAddNewBankAccount
  } = params
  const query = { partnerId }
  let setQuery = {
    $pull: { bankAccounts: { accountNumber: oldBankAccountNumber } }
  }
  if (oldBankAccountNumber && newBankAccountNumber) {
    const updateData = {
      id: nid(17),
      accountNumber: newBankAccountNumber
    }
    const isAccountNumberExists =
      await partnerSettingHelper.isAccountNumberExists(newBankAccountNumber)
    if (isAccountNumberExists) {
      throw new CustomError(405, 'Account number already exists')
    }
    if (shouldAddNewBankAccount) {
      setQuery = { $push: { bankAccounts: updateData } }
    } else {
      query.bankAccounts = {
        $elemMatch: { accountNumber: oldBankAccountNumber }
      }
      setQuery = {
        $set: { 'bankAccounts.$.accountNumber': newBankAccountNumber }
      }
    }
  }
  const updatedPartnerSetting = await PartnerSettingCollection.findOneAndUpdate(
    query,
    setQuery,
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return updatedPartnerSetting
}

export const updateNotificationsSetting = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { partnerId } = user
  const { data } = body
  const { query } = appHelper.prepareQueryBasedOnPartnerId(partnerId)
  const notificationsSettingUpdatingData =
    partnerSettingHelper.prepareNotificationsSettingUpdatingData(data)
  const updatedPartnerSetting = await updateAPartnerSetting(
    query,
    notificationsSettingUpdatingData,
    session
  )
  return updatedPartnerSetting
}

export const updateStartNumbersOrFinnId = async (partnerId, data, session) => {
  const { name, valueInt, valueString } = data
  if (name !== 'finnId' && !validator.isInt(`${valueInt}`))
    throw new CustomError(400, 'Value must be an integer')
  const commonParams = { partnerId, value: valueInt, session }
  let isUpdated = false
  if (name === 'propertyStartNumber') {
    // Properties
    await counterService.updatePropertyStartNumber(commonParams)
    isUpdated = true
  } else if (name === 'tenantStartNumber') {
    // Tenant
    await counterService.updateTenantStartNumber(commonParams)
    isUpdated = true
  } else if (name === 'invoiceStartNumber') {
    //Invoice
    await counterService.updateInvoiceStartNumber(commonParams)
    isUpdated = true
  } else if (name === 'finalSettlementInvoiceStartNumber') {
    //Invoice
    await counterService.updateFinalSettlementInvoiceStartNumber(commonParams)
    isUpdated = true
  } else if (name === 'finnId') {
    // Listings
    if (
      validator.isInt(`${valueString}`) ||
      validator.isBoolean(`${valueString}`)
    )
      throw new CustomError(400, 'Value must be a string')
    await partnerService.updateAPartner(
      { _id: partnerId },
      { finnId: valueString },
      session
    )
    isUpdated = true
  }
  return isUpdated
}

export const updateGeneralSetting = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { partnerId } = user
  const { data } = body
  const { query } = appHelper.prepareQueryBasedOnPartnerId(partnerId)
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(query)
  if (!partnerSetting)
    throw new CustomError(404, "partnerSetting doesn't exists")
  const params = { partnerSetting, partnerId, data }
  const { updatingData } =
    await partnerSettingHelper.prepareGeneralSettingUpdatingData(params)
  if (size(updatingData)) {
    const updatedPartnerSetting = await updateAPartnerSetting(
      query,
      updatingData,
      session
    )
    return updatedPartnerSetting
  }
  const allowedNames = [
    'propertyStartNumber',
    'tenantStartNumber',
    'invoiceStartNumber',
    'finalSettlementInvoiceStartNumber',
    'finnId'
  ]
  if (allowedNames.includes(data.name)) {
    if (!partnerId)
      throw new CustomError(400, `PartnerId is required to update ${data.name}`)
    const isUpdated = await updateStartNumbersOrFinnId(partnerId, data, session)
    if (isUpdated) return partnerSetting
  }
  throw new CustomError(400, 'Invalid name for general settings change')
}

export const updateDomainSetting = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkPartnerId(user, body)
  appHelper.checkRequiredFields(['data'], body)
  const { query, updatedData } =
    await partnerSettingHelper.prepareQueryForDomainSetting(body)
  if (userId && (await appPermission.isAppAdmin(userId))) {
    const updatedPartner = await updateAPartnerSetting(
      query,
      updatedData,
      session
    )
    return updatedPartner
  } else throw new CustomError(401, 'Unauthorized')
}

export const updateRentInvoiceSetting = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { partnerId } = user
  const { data } = body
  const { query } = appHelper.prepareQueryBasedOnPartnerId(partnerId)
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(query)
  if (!partnerSetting)
    throw new CustomError(404, "partnerSetting doesn't exists")
  const params = { partnerSetting, partnerId, data }
  const { updatingData } =
    await partnerSettingHelper.prepareRentInvoiceUpdatingData(params)
  if (size(updatingData)) {
    const updatedPartnerSetting = await updateAPartnerSetting(
      query,
      updatingData,
      session
    )
    return updatedPartnerSetting
  } else
    throw new CustomError(400, 'Invalid name for rent invoice settings change')
}

export const updateLandlordInvoiceSetting = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { partnerId } = user
  const { data } = body
  const { query } = appHelper.prepareQueryBasedOnPartnerId(partnerId)
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(
    query,
    session
  )
  if (!partnerSetting)
    throw new CustomError(404, "partnerSetting doesn't exists")
  const params = { partnerSetting, data }
  const { updatingData } =
    partnerSettingHelper.prepareLandlordInvoiceUpdatingData(params)
  if (size(updatingData)) {
    const updatedPartnerSetting = await updateAPartnerSetting(
      query,
      updatingData,
      session
    )
    return updatedPartnerSetting
  } else
    throw new CustomError(
      400,
      'Invalid name for landlord invoice settings change'
    )
}

export const updatePayoutSetting = async (req) => {
  const { body, user = {}, session } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { data } = body
  const { query } = await appHelper.prepareQueryBasedOnPartnerId(partnerId)
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(query)
  if (!partnerSetting)
    throw new CustomError(404, "partnerSetting doesn't exists")
  const params = { partnerSetting, data }
  const { updatingData } = await partnerSettingHelper.preparePayoutUpdatingData(
    params
  )
  if (size(updatingData)) {
    const updatedPartnerSetting = await updateAPartnerSetting(
      query,
      updatingData,
      session
    )
    return updatedPartnerSetting
  } else throw new CustomError(400, 'Invalid name for payout settings change')
}

export const updateCompanyInfo = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { data } = body
  const { partnerId = '' } = user
  appHelper.validateId({ partnerId })
  const settingInfo = await partnerSettingHelper.getAPartnerSetting(
    {
      partnerId
    },
    session
  )
  if (!size(settingInfo)) {
    throw new CustomError(404, 'No partner setting found')
  }
  const updatingData =
    partnerSettingHelper.prepareCompanyInfoSettingUpdateData(data)
  const updatedPartnerSetting = await updateAPartnerSetting(
    { _id: settingInfo._id, partnerId },
    updatingData,
    session
  )
  return updatedPartnerSetting
}
