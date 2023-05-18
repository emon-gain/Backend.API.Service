import { size, omit } from 'lodash'

import { PhoneNumberCollection } from '../models'
import { userHelper, appHelper, phoneNumberHelper } from '../helpers'
import { CustomError } from '../common'

export const getPhoneNumber = async (query) => {
  const phoneNumberInfo = await PhoneNumberCollection.findOne(query)
  return phoneNumberInfo
}

export const updateAPhoneNumber = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for phone-number update')
  }
  const updatedPhoneNumber = await PhoneNumberCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!size(updatedPhoneNumber)) {
    throw new CustomError(404, `Unable to update phone-number`)
  }
  return updatedPhoneNumber
}

export const preparePhoneNumbersQueryBasedOnFilters = (query) => {
  const { partnerId, phoneNumber, createdDateRange, createdBy } = query
  // Set partner filters in query
  if (partnerId) appHelper.validateId({ partnerId })
  // Set createdDateRange filters in query
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: createdDateRange.startDate,
      $lte: createdDateRange.endDate
    }
  }
  // Set createdBy filters in query
  if (createdBy) appHelper.validateId({ createdBy })
  if (phoneNumber)
    query.phoneNumber = {
      $regex: new RegExp('.*' + phoneNumber.replace('+', '') + '.*', 'i')
    }
  const phoneNumbersQuery = omit(query, ['createdDateRange'])
  return phoneNumbersQuery
}

export const getPhoneNumbersForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const phoneNumbers = await PhoneNumberCollection.find(query)
    .populate(['partner', 'user'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return phoneNumbers
}

export const countPhoneNumbers = async (query) => {
  const numberOfPhoneNumbers = await PhoneNumberCollection.find(
    query
  ).countDocuments()
  return numberOfPhoneNumbers
}

export const queryPhoneNumbers = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = preparePhoneNumbersQueryBasedOnFilters(query)
  const phoneNumbersData = await getPhoneNumbersForQuery(body)
  const filteredDocuments = await countPhoneNumbers(body.query)
  const totalDocuments = await countPhoneNumbers({})
  const phoneNumbers = phoneNumbersData.map(async (phoneNumber) => {
    if (phoneNumber.createdBy) {
      const { user } = phoneNumber
      if (size(user)) {
        user.imgUrl = user ? userHelper.getAvatar(user) : ''
        phoneNumber.user = user
      }
    }
    phoneNumber.isDeletable = !phoneNumber.partnerId

    return phoneNumber
  })
  return {
    data: phoneNumbers,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const checkRequiredFieldsForPhoneNumberCreation = (body) => {
  appHelper.checkRequiredFields(['phoneNumber'], body)
  const { phoneNumber } = body
  if (!phoneNumber) throw new CustomError(400, 'Required phoneNumber')
}

export const checkRequiredFieldsForPhoneNumberUpdate = (body) => {
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { _id = '', data } = body
  appHelper.validateId({ _id })
  const { phoneNumber } = data
  if (!phoneNumber) throw new CustomError(400, 'Required phoneNumber')
}

export const checkRequiredFieldsForPhoneNumberDelete = (body) => {
  appHelper.checkRequiredFields(['_id'], body)
  const { _id = '' } = body
  appHelper.validateId({ _id })
}

export const isPhoneNumberBeingUsed = async (phoneNumberId) => {
  const query = {
    _id: phoneNumberId,
    partnerId: { $exists: true }
  }
  const isPhoneNumberExists = !!(await phoneNumberHelper.getPhoneNumber(query))
  return isPhoneNumberExists
}

const prepareQueryForDropdown = (query) => {
  query.partnerId = { $exists: false }
  const { keyword } = query
  if (keyword) {
    const updatedKeyword = keyword.replace('+', '') // remove '+' sign as it is special char in regex
    query.phoneNumber = { $regex: `.*${updatedKeyword}.*`, $options: 'i' }
    delete query.keyword
  }
  return query
}

export const getPhoneNumbersForDropdown = async (req) => {
  const { body = {} } = req
  const { query = {}, options = {} } = body
  const preparedQuery = prepareQueryForDropdown(query)
  const { limit } = options
  const phoneNumbers = await PhoneNumberCollection.find(preparedQuery).limit(
    limit
  )
  const filteredDocuments = await countPhoneNumbers(preparedQuery)
  const totalDocuments = await countPhoneNumbers({})
  return {
    data: phoneNumbers,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}
