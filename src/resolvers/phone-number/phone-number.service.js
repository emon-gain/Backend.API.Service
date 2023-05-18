import { size } from 'lodash'
import { CustomError } from '../common'
import { PhoneNumberCollection } from '../models'
import { appHelper, phoneNumberHelper } from '../helpers'
import { partnerService } from '../services'

export const createAPhoneNumber = async (data, session) => {
  const response = await PhoneNumberCollection.create([data], { session })
  return response
}

export const createPhoneNumber = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkPartnerId(user, body)
  body.createdBy = userId
  phoneNumberHelper.checkRequiredFieldsForPhoneNumberCreation(body)
  const isAlreadyExists = !!(await phoneNumberHelper.getPhoneNumber(
    {
      phoneNumber: body.phoneNumber
    },
    session
  ))

  if (isAlreadyExists) throw new CustomError(400, 'PhoneNumber already exists')

  const createdPhoneNumber = await createAPhoneNumber(body, session)
  return createdPhoneNumber
}

export const updateAPhoneNumber = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await PhoneNumberCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  if (!response) {
    throw new CustomError(404, `Could not update Phone Number`)
  }
  return response
}

export const updatePhoneNumberOfAPartner = async (partnerId, data, session) => {
  const query = { _id: partnerId }
  await partnerService.updateAPartner(query, data, session)
}

export const updatePhoneNumber = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkPartnerId(user, body)
  phoneNumberHelper.checkRequiredFieldsForPhoneNumberUpdate(body)
  const { _id, data } = body
  const existPhoneNumber = await phoneNumberHelper.getPhoneNumber({
    phoneNumber: data.phoneNumber,
    _id: { $ne: _id }
  })
  if (size(existPhoneNumber)) {
    throw new CustomError(405, 'Phone number already exist')
  }
  const updatedPhoneNumber = await updateAPhoneNumber({ _id }, data, session)
  const { partnerId = '' } = updatedPhoneNumber
  if (partnerId) {
    // If has partnerId, then update that partner phoneNumber too (Relation)
    await updatePhoneNumberOfAPartner(partnerId, data, session)
  }
  return updatedPhoneNumber
}

export const removeAPhoneNumber = async (query, session) => {
  const response = await PhoneNumberCollection.findOneAndDelete(query, {
    session
  })
  if (!size(response)) {
    throw new CustomError(404, `Could not delete Phone Number`)
  }

  return response
}

export const removePhoneNumber = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkPartnerId(user, body)
  phoneNumberHelper.checkRequiredFieldsForPhoneNumberDelete(body)
  const { _id } = body
  const isPhoneNumberBeingUsed = await phoneNumberHelper.isPhoneNumberBeingUsed(
    _id
  )
  if (isPhoneNumberBeingUsed) {
    throw new CustomError(405, "Could not delete, it's being used")
  }
  const deletedPhoneNumber = await removeAPhoneNumber({ _id }, session)
  return deletedPhoneNumber
}

export const updateRemainingBalance = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { options } = body
  const requiredFields = ['partnerId', 'fromPhoneNumber', 'remainingBalance']
  let remainingBalanceUpdatedCount = 0
  for (let i = 0; i < options.length; i++) {
    const option = options[i]
    appHelper.checkRequiredFields(requiredFields, option)
    const { remainingBalance, partnerId, fromPhoneNumber } = option
    const query = {
      $or: [{ partnerId, phoneNumber: fromPhoneNumber }, { partnerId }]
    }
    const updateData = { remainingBalance }
    await updateAPhoneNumber(query, updateData, session)
    remainingBalanceUpdatedCount += 1
  }
  return remainingBalanceUpdatedCount
}
