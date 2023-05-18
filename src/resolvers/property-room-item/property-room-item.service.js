import { size } from 'lodash'
import { CustomError } from '../common'
import { PropertyRoomItemCollection } from '../models'
import { propertyRoomService } from '../services'
import { appHelper, propertyRoomItemHelper } from '../helpers'

export const createARoomItem = async (data, session) => {
  const response = await PropertyRoomItemCollection.create([data], { session })
  return response
}

export const updateARoomItem = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await PropertyRoomItemCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  if (!response)
    throw new CustomError(404, 'Unable to update property room Item')

  return response
}

export const removeARoomItem = async (query, session) => {
  const response = await PropertyRoomItemCollection.findOneAndDelete(query, {
    session
  })
  if (!response)
    throw new CustomError(404, 'Unable to remove property room Item')

  return response
}

export const createRoomItem = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId = '', userId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)

  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  propertyRoomItemHelper.checkRequiredFieldsForRoomItemCreation(body)
  const { isEnable, roomId = '' } = body
  body.createdBy = userId
  if (!isEnable) body.isEnable = false
  const createdRoomItem = await createARoomItem(body, session)
  if (size(createdRoomItem) && roomId) {
    const params = {
      roomId,
      body,
      newRoomItemId: createdRoomItem[0]._id,
      session
    }
    await propertyRoomService.processDataAndUpdatePropertyRooms(params)
  }
  return createdRoomItem
}

export const updateRoomItem = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  propertyRoomItemHelper.checkRequiredFieldsForRoomItemUpdate(body)
  body.partnerId = partnerId
  const query =
    propertyRoomItemHelper.prepareQueryForRoomItemBasedOnPartnerId(body)
  const { data } = body
  const { updatingData } =
    propertyRoomItemHelper.prepareRoomItemUpdatingData(data)
  const updatedRoomItem = await updateARoomItem(query, updatingData, session)

  return updatedRoomItem
}

export const removeRoomItem = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['_id'], body)
  body.partnerId = partnerId

  const query =
    propertyRoomItemHelper.prepareQueryForRoomItemBasedOnPartnerId(body)
  const isRoomItemBeingUsed = await propertyRoomItemHelper.isRoomItemBeingUsed(
    body._id,
    session
  )
  if (isRoomItemBeingUsed)
    throw new CustomError(405, `Could not delete, item-in-used`)

  const removedRoomItem = await removeARoomItem(query, session)
  return removedRoomItem
}
