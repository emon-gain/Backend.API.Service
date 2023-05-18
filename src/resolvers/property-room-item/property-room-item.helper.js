import { difference, size } from 'lodash'
import validator from 'validator'
import { PropertyRoomItemCollection } from '../models'
import { appHelper, propertyRoomHelper } from '../helpers'
import { CustomError } from '../common'

export const getPropertyRoomItem = async (query, session) => {
  const propertyRoomItem = await PropertyRoomItemCollection.findOne(
    query
  ).session(session)
  return propertyRoomItem
}

export const getPropertyRoomItems = async (query, session) => {
  const roomItems = await PropertyRoomItemCollection.find(query).session(
    session
  )
  return roomItems
}

export const preparePropertyRoomItemsQueryBasedOnFilters = (query) => {
  const { appAdmin, partnerId } = query
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.isCustomRoomItem = { $exists: false }
    query.partnerId = partnerId
  } else if (appAdmin) {
    query.partnerId = { $exists: false }
    delete query.appAdmin
  } else {
    throw new CustomError('400', 'Bad request')
  }
  return query
}

export const getPropertyRoomItemsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const PropertyRoomItems = await PropertyRoomItemCollection.find(query)
    .populate(['partner', 'property'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return PropertyRoomItems
}

export const countPropertyRoomItems = async (query, session) => {
  const numberOfPropertyRoomItems = await PropertyRoomItemCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfPropertyRoomItems
}

export const queryPropertyRoomItems = async (req) => {
  const { body = {}, user = {} } = req
  const { query, options } = body
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)

  appHelper.validateSortForQuery(options.sort)

  if (partnerId) query.partnerId = partnerId

  body.query = preparePropertyRoomItemsQueryBasedOnFilters(query)
  const propertyRoomItemsData = await getPropertyRoomItemsForQuery(body)
  const filteredDocuments = await countPropertyRoomItems(body.query)
  const totalDocuments = await countPropertyRoomItems({
    partnerId: partnerId ? partnerId : { $exists: false }
  })
  const propertyRoomItems = await Promise.all(
    propertyRoomItemsData.map(async (roomItem) => {
      roomItem.isDeletable = !(await isRoomItemBeingUsed(roomItem._id))
      return roomItem
    })
  )
  return {
    data: propertyRoomItems,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const checkRequiredFieldsForRoomItemCreation = (body) => {
  const requiredFields = ['name', 'roomTypes']
  appHelper.checkRequiredFields(requiredFields, body)
  const {
    name = '',
    movingId = '',
    partnerId = '',
    propertyId = '',
    roomId = '',
    roomTypes = []
  } = body
  if (!name) throw new CustomError(400, 'Required name')
  if (!size(roomTypes)) throw new CustomError(400, 'Required roomTypes')
  if (movingId) appHelper.validateId({ movingId })
  if (partnerId) appHelper.validateId({ partnerId })
  if (propertyId) appHelper.validateId({ propertyId })
  if (roomId) appHelper.validateId({ roomId })
}

export const checkRequiredFieldsForRoomItemUpdate = (body) => {
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { data = {} } = body
  if (!size(data)) throw new CustomError(400, 'Required data')
}

export const prepareRoomItemUpdatingData = (params) => {
  let updatingData = {}
  const { name, valueString, valueArray, valueBoolean } = params
  if (!name) throw new CustomError(400, 'Required name')
  if (name === 'name') {
    if (!valueString) throw new CustomError(400, 'Required valueString')
    updatingData = { name: valueString }
  } else if (name === 'roomTypes') {
    if (valueArray === null) throw new CustomError(400, 'Required valueArray')
    updatingData = { roomTypes: validateRoomTypeItem(valueArray) }
  } else if (name === 'isEnable') {
    if (!validator.isBoolean(`${valueBoolean}`))
      throw new CustomError(400, 'Required valueBoolean')
    updatingData = { isEnable: valueBoolean }
  }
  if (!size(updatingData))
    throw new CustomError(400, 'Invalid name to update room items')

  return { updatingData }
}

export const prepareQueryForRoomItemBasedOnPartnerId = (body) => {
  const { _id, partnerId } = body
  appHelper.validateId({ _id })
  const query = { _id }
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  } else {
    query.partnerId = {
      $exists: false
    }
  }
  return query
}

export const isRoomItemBeingUsed = async (roomItemId, session) => {
  const propertyRoomQuery = { items: { $elemMatch: { id: roomItemId } } }
  const isRoomExists = !!(await propertyRoomHelper.getAPropertyRoom(
    propertyRoomQuery,
    session
  ))
  return isRoomExists
}

export const validateRoomTypeItem = (roomTypes = []) => {
  const defaultRoomType = [
    'living_room',
    'bedroom',
    'kitchen',
    'bath_toilet',
    'balcony_outdoor_area',
    'facade',
    'other',
    'laundry_room',
    'entrance_hallway',
    'storage_unit',
    'garage'
  ]

  const invalidRoomType = difference(roomTypes, defaultRoomType)
  if (size(invalidRoomType)) {
    throw new CustomError(
      400,
      `Invalid room type ${invalidRoomType.toString()}`
    )
  }

  return roomTypes
}
