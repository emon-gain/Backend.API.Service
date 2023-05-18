import { size, omit, union } from 'lodash'
import { CustomError } from '../common'
import { PropertyRoomCollection } from '../models'
import {
  appHelper,
  contractHelper,
  propertyItemHelper,
  propertyRoomHelper,
  propertyRoomItemHelper
} from '../helpers'
import { fileService, propertyItemService, taskService } from '../services'

export const createARoom = async (data, session) => {
  const response = await PropertyRoomCollection.create([data], { session })
  return response
}

export const createMultipleRooms = async (data, session) => {
  const response = await PropertyRoomCollection.insertMany(data, {
    session,
    runValidators: true
  })
  return response
}

export const setPropertyItemIdIntoRoomData = async (body, session) => {
  const { roomData, roomFor, partnerId, userId } = body
  const { propertyId } = roomData
  const propertyItemQuery = {
    partnerId,
    propertyId,
    contractId: { $exists: false },
    type: { $exists: false }
  }
  // Find a propertyItem based on partnerId and propertyId
  // Has propertyItem ? set propertyItemId into roomData : create a propertyItem and set propertyItemId into roomData
  const propertyItem = await propertyItemHelper.getAPropertyItem(
    propertyItemQuery,
    session
  )
  roomData.partnerId = partnerId
  roomData.createdBy = userId
  if (roomFor !== 'property' && size(propertyItem)) {
    roomData.propertyItemId = propertyItem._id
  } else if (!size(propertyItem)) {
    const ItemQuery = {
      partnerId,
      propertyId
    }
    const insertedPropertyItem = await propertyItemService.createAPropertyItem(
      ItemQuery,
      session
    )
    if (size(insertedPropertyItem)) {
      console.log(
        `--- Created Property Item for Id: ${insertedPropertyItem._id} ---`
      )
      roomData.propertyItemId = insertedPropertyItem._id
      // TODO :: Type in => update propertyItem => Update Hooks
      // TODO :: Type out => Insert propertyRoomItem
    }
  }
  return roomData
}

export const checkIsMovedInOrNot = async (roomData, roomId, session) => {
  const { partnerId, propertyId } = roomData
  const leaseInfoQuery = {
    partnerId,
    propertyId,
    status: 'active'
  }
  const activeLeaseInfo = await contractHelper.getAContract(
    leaseInfoQuery,
    session
  )
  if (activeLeaseInfo && activeLeaseInfo._id) {
    const propertyItemQuery = {
      partnerId,
      propertyId,
      type: { $in: ['in', 'out'] },
      contractId: activeLeaseInfo._id,
      isEsigningInitiate: { $exists: false },
      $or: [
        { moveInCompleted: { $exists: false } },
        { moveOutCompleted: { $exists: false } }
      ]
    }
    const lastPropertyItem = await propertyItemHelper.getLastPropertyItem(
      propertyItemQuery,
      session
    )
    if (size(lastPropertyItem)) {
      roomData.propertyRoomId = roomId
      roomData.contractId = activeLeaseInfo._id
      roomData.movingId = lastPropertyItem._id
      return roomData
    }
  }
  return false
}

export const createARoomForMovingInProtocol = async (
  roomData,
  roomId,
  session
) => {
  const data = await checkIsMovedInOrNot(roomData, roomId, session)
  if (size(data)) {
    const response = await createARoom(data, session)
    if (!size(response)) {
      throw new CustomError(404, `Could not create Property Room`)
    }
    console.log(
      `--- Created Room for MovingInProtocol Id: ${response[0]._id} ---`
    )
  }
}

export const createARoomWithOutMovingIdAndContractId = async (
  roomData,
  session
) => {
  const dataForRoom = omit(roomData, ['contractId', 'movingId'])
  const result = await createARoom(dataForRoom, session)
  if (!size(result)) {
    throw new CustomError(404, `Could not create Property Room`)
  }
  console.log(`--- Created Room for Id: ${result[0]._id} ---`)
  return result
}

export const processDataAndCreateRooms = async (roomData, session) => {
  const { contractId, movingId, partnerId, type } = roomData
  let result = {}
  const roomItemsQuery = {
    partnerId,
    isEnable: true,
    roomTypes: {
      $in: [type, 'common']
    }
  }
  const roomItems = (
    (await propertyRoomItemHelper.getPropertyRoomItems(
      roomItemsQuery,
      session
    )) || []
  ).map((item) => ({ id: item._id }))
  if (size(roomItems)) roomData.items = roomItems
  if (!contractId && !movingId) {
    result = await createARoom(roomData, session)
    if (!size(result)) {
      throw new CustomError(404, `Could not create Property Room`)
    }
    console.log(`--- Created Room for Id: ${result[0]._id} ---`)
    // Create A room with contractId, movingId & propertyRoomId
    await createARoomForMovingInProtocol(roomData, result[0]._id, session)
  } else if (contractId && movingId) {
    // Create A room with out contractId & movingId
    const createdRoom = await createARoomWithOutMovingIdAndContractId(
      roomData,
      session
    )
    roomData.propertyRoomId = createdRoom[0]._id // Set propertyRoomId and create A Room For MovingInProtocol
    result = await createARoom(roomData, session)
    if (!size(result)) {
      throw new CustomError(404, `Could not create Property Room`)
    }
    console.log(
      `--- Created Room for MovingInProtocol Id: ${result[0]._id} ---`
    )
  }
  return result
}

export const createRooms = async (req) => {
  const { body, session, user } = req
  appHelper.validatePartnerAppRequestData(req, ['roomData', 'roomFor'])
  const { partnerId, userId } = user
  body.partnerId = partnerId
  body.userId = userId
  const roomDataWithPropertyItemId = await setPropertyItemIdIntoRoomData(
    body,
    session
  )
  const result = await processDataAndCreateRooms(
    roomDataWithPropertyItemId,
    session
  )
  if (!size(result)) {
    throw new CustomError(404, `Could not create Property Room`)
  }
  return result
}

export const updateAPropertyRoom = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await PropertyRoomCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  return response
}

export const updatePropertyRooms = async (query, data, session) => {
  const response = await PropertyRoomCollection.updateMany(query, data, {
    runValidators: true,
    session
  })
  if (response.nModified > 0) {
    return response
  }
}

export const updateRoomBasedOnContractIdAndMovingId = async (
  params,
  session
) => {
  const {
    _id,
    partnerId,
    propertyId,
    contractId,
    movingId,
    propertyRoomId,
    newFiles
  } = params
  const updateRoomQuery = {
    partnerId,
    propertyId
  }
  let updateRoomData = {}
  if (contractId && movingId) {
    updateRoomQuery._id = propertyRoomId
    updateRoomData = omit(params, [
      '_id',
      'createdAt',
      'createdBy',
      'contractId',
      'movingId',
      'propertyRoomId'
    ])
    if (size(newFiles)) {
      updateRoomData.files = union(updateRoomData.files, newFiles)
    }
    if (size(updateRoomData)) {
      const result = await updateAPropertyRoom(
        updateRoomQuery,
        updateRoomData,
        session
      )
      if (!size(result)) {
        throw new CustomError(404, `Could not update Property Room`)
      }
      console.log(`--- Updated Property Room for Id: ${result._id} ---`)
    }
  } else if (!contractId && !movingId) {
    const data = await checkIsMovedInOrNot(params, _id, session)
    if (size(data)) {
      updateRoomQuery.movingId = data.movingId
      updateRoomQuery.propertyRoomId = data.propertyRoomId
      updateRoomData = omit(params, ['_id', 'createdAt', 'createdBy'])
      if (size(updateRoomData)) {
        const result = await updateAPropertyRoom(
          updateRoomQuery,
          updateRoomData,
          session
        )
        if (!size(result)) {
          throw new CustomError(404, `Could not update Property Room`)
        }
        console.log(
          `--- Updated Property for MovingInProtocol Id: ${result._id} ---`
        )
      }
    }
  }
}

export const updateBothRooms = async (params, updateData, session) => {
  const { _id, propertyRoomId } = params
  if (!(_id && propertyRoomId)) {
    throw new CustomError(404, `missing _id and propertyId`)
  }
  const updatedRoomForMoveInProtocol = await updateAPropertyRoom(
    { _id },
    updateData,
    session
  )
  if (!size(updatedRoomForMoveInProtocol)) {
    throw new CustomError(404, `Could not update Property Room`)
  }
  console.log(
    `--- Updated Property Room for moveInProtocol: ${updatedRoomForMoveInProtocol._id} ---`
  )
  const updatedRoomForNonMoveInProtocol = await updateAPropertyRoom(
    { _id: propertyRoomId },
    updateData,
    session
  )
  if (!size(updatedRoomForNonMoveInProtocol)) {
    throw new CustomError(404, `Could not update Property Room`)
  }
  console.log(
    `--- Updated Property Room for non moveInProtocol: ${updatedRoomForNonMoveInProtocol._id} ---`
  )
}

export const processDataAndUpdatePropertyRooms = async (params) => {
  const { roomId, body, newRoomItemId, session } = params
  const query = {
    _id: roomId
  }
  if (body.partnerId) {
    query.partnerId = body.partnerId
  }
  if (body.propertyId) {
    query.propertyId = body.propertyId
  }
  const propertyRoom = await propertyRoomHelper.getPropertyRoom(query, session)
  if (!size(propertyRoom)) {
    throw new CustomError(404, `Could not find Property Room`)
  }
  const {
    _id,
    contractId,
    movingId,
    propertyRoomId,
    items,
    partnerId,
    propertyId
  } = propertyRoom
  const itemsArray = size(propertyRoom) && size(items) ? items : []
  itemsArray.push({ id: newRoomItemId })
  const updateData = {
    items: itemsArray
  }
  if (contractId && movingId && propertyRoomId) {
    // Update property rooms for moveInProtocol and non moveInProtocol
    const params = { _id, propertyRoomId }
    await updateBothRooms(params, updateData, session)
  } else if (!contractId && !movingId) {
    const params = { partnerId, propertyId }
    const response = await checkIsMovedInOrNot(params, _id, session)
    if (!response) {
      // Update property room for only non moveInProtocol
      const updatedRoomForNonMoveInProtocol = await updateAPropertyRoom(
        { _id },
        updateData,
        session
      )
      if (!updatedRoomForNonMoveInProtocol) {
        throw new CustomError(404, `Could not update Property Room`)
      }
      console.log(
        `--- Updated Property Room for non moveInProtocol: ${updatedRoomForNonMoveInProtocol._id} ---`
      )
    } else {
      // Update property rooms for moveInProtocol and non moveInProtocol
      const roomOfMoveInProtocol = await propertyRoomHelper.getPropertyRoom(
        {
          propertyRoomId: _id,
          partnerId,
          propertyId
        },
        session
      )
      const updateParams = { _id, propertyRoomId: roomOfMoveInProtocol._id }
      await updateBothRooms(updateParams, updateData, session)
    }
  }
}

export const processQueryAndUpdatingDataForPropertyRoomUpdate = (
  roomId,
  data
) => {
  const { roomData, partnerId, type } = data
  const {
    name,
    propertyRoomId,
    roomItemId,
    status,
    responsibleForFixing,
    title,
    dueDate,
    description
  } = roomData
  let updateData = {}
  const query = {
    _id: roomId
  }
  if (partnerId) {
    query.partnerId = partnerId
  }
  if (name) {
    updateData.name = name
  }
  if (propertyRoomId) {
    updateData.propertyRoomId = propertyRoomId
  }
  if (roomItemId) {
    query.items = { $elemMatch: { id: roomItemId } }
    if (type === 'status') {
      updateData = {
        'items.$.status': status || 'notApplicable'
      }
      if (status !== 'issues') {
        updateData = {
          'items.$.status': status || 'notApplicable',
          'items.$.responsibleForFixing': '',
          'items.$.description': ''
        }
      }
    }
    if (type === 'responsibleForFixing') {
      updateData = {
        'items.$.responsibleForFixing':
          responsibleForFixing || 'noActionRequired'
      }
    }
    if (type === 'title') updateData = { 'items.$.title': title ? title : '' }
    if (type === 'dueDate')
      updateData = {
        'items.$.dueDate': dueDate ? dueDate : ''
      }
    if (type === 'description') {
      updateData = {
        'items.$.description': description || ''
      }
    }
  }
  return { query, updateData }
}

export const addOrRemoveImagesFromPropertyRooms = async (
  query,
  roomData,
  session
) => {
  const updatedRoom = await updateAPropertyRoom(query, roomData, session)
  if (size(updatedRoom)) {
    console.log(
      `--- Updated Property Room for MovingInProtocol/NonMovingInProtocol Id: ${updatedRoom._id} ---`
    )
    await updateRoomBasedOnContractIdAndMovingId(
      updatedRoom.toObject(),
      session
    )
    return updatedRoom
  }
  throw new CustomError(404, `Could not update Property Room`)
}

export const updateRooms = async (req) => {
  const { body, session } = req
  appHelper.validatePartnerAppRequestData(req, ['roomId', 'roomData', 'type'])

  const { roomId, roomData, partnerId, userId, type } = body
  const { roomItemId } = roomData
  if (type === 'propertyRoomImage') {
    // Add or Remove Property Room Image { roomData => files: []}
    const query = {
      _id: roomId,
      partnerId
    }
    const response = await addOrRemoveImagesFromPropertyRooms(
      query,
      roomData,
      session
    )
    return response
  }
  const { query, updateData } =
    processQueryAndUpdatingDataForPropertyRoomUpdate(roomId, body)
  const result = await updateAPropertyRoom(query, updateData, session)
  if (!size(result)) {
    throw new CustomError(404, `Could not update Property Room`)
  }
  console.log(
    `--- Updated Property Room for MovingInProtocol/NonMovingInProtocol Id: ${result._id} ---`
  )
  const params = {
    roomItemId,
    partnerId,
    userId
  }
  await updateRoomBasedOnContractIdAndMovingId(result.toObject(), session)

  // update or create task, log and comment after update property room
  const taskId = await taskService.addOrUpdateTaskForRoomItems(
    params,
    result,
    session
  )
  if (taskId) result.taskId = taskId

  return result
}

export const removeARoom = async (query, session) => {
  const response = await PropertyRoomCollection.findOneAndDelete(query, {
    session
  })
  return response
}

export const removeRoomBasedOnContractIdAndMovingId = async (
  params,
  session
) => {
  const { _id, partnerId, propertyId, contractId, movingId, propertyRoomId } =
    params
  if (contractId && movingId) {
    const removeQuery = {
      _id: propertyRoomId,
      partnerId,
      propertyId
    }
    const result = await removeARoom(removeQuery, session)
    if (size(result)) {
      console.log(
        `--- Deleted Room based on propertyRoomId: ${propertyRoomId} ---`
      )
    }
  } else if (!contractId && !movingId) {
    const leaseInfoQuery = {
      partnerId,
      propertyId,
      status: 'active'
    }
    const activeLeaseInfo = await contractHelper.getAContract(
      leaseInfoQuery,
      session
    )
    if (activeLeaseInfo && activeLeaseInfo._id) {
      const propertyItemQuery = {
        partnerId,
        propertyId,
        type: { $in: ['in', 'out'] },
        contractId: activeLeaseInfo._id,
        isEsigningInitiate: { $exists: false },
        $or: [
          { moveInCompleted: { $exists: false } },
          { moveOutCompleted: { $exists: false } }
        ]
      }
      const lastPropertyItem = await propertyItemHelper.getLastPropertyItem(
        propertyItemQuery,
        session
      )
      if (lastPropertyItem) {
        const removeQuery = {
          propertyRoomId: _id,
          movingId: lastPropertyItem._id,
          partnerId,
          propertyId
        }
        const result = await removeARoom(removeQuery, session)
        if (size(result)) {
          console.log(`--- Deleted Room based on propertyRoomId: ${_id} ---`)
        }
      }
    }
  }
}

export const removeRoom = async (req) => {
  const { session, body, user } = req
  appHelper.validatePartnerAppRequestData(req, ['roomId'])
  const { partnerId } = user
  const { roomId } = body
  const query = { _id: roomId, partnerId }
  const propertyRoom = await propertyRoomHelper.getPropertyRoom(query)

  const { contractId, movingId, propertyRoomId } = propertyRoom
  if (contractId && movingId && propertyRoomId) {
    const removedRoom = await removeARoom(query, session)
    if (size(removedRoom)) {
      console.log(
        `--- Deleted Room for MovingInProtocol Id: ${removedRoom._id} ---`
      )
      const response = await removeARoom({ _id: propertyRoomId }, session)
      if (size(response)) {
        console.log(`--- Deleted Room for Id: ${response._id} ---`)
        return removedRoom
      }
    }
  } else if (!contractId && !movingId && !propertyRoomId) {
    const removedRoom = await removeARoom(query, session)
    if (size(removedRoom)) {
      console.log(`--- Deleted Room for Id: ${removedRoom._id} ---`)
      const data = await checkIsMovedInOrNot(
        removedRoom,
        removedRoom._id,
        session
      )
      if (size(data) && data.movingId && data.propertyRoomId) {
        const deleteQuery = {
          partnerId: removedRoom.partnerId,
          propertyId: removedRoom.propertyId,
          propertyRoomId: removedRoom.propertyRoomId,
          movingId: data.movingId
        }
        const response = await removeARoom(deleteQuery, session)
        if (size(response)) {
          console.log(
            `--- Deleted Room for MovingInProtocol Id: ${response._id} ---`
          )
        }
      }
      return removedRoom
    }
  }
  throw new CustomError(404, `Could not delete Room`)
}

export const createPropertyIssue = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'description',
    'issueType',
    'itemId',
    'title',
    'propertyId'
  ])

  const { body, session } = req
  const fileIds = await prepareAndCreateFileForIssue(body, session)
  if (size(fileIds)) body.fileIds = fileIds

  const {
    roomItemUpdateData,
    roomItemQuery,
    inventoryItemQuery,
    inventoryItemUpdateData
  } = await propertyRoomHelper.prepareAndValidatePropertyIssueData(
    body,
    session
  )

  if (size(roomItemUpdateData)) {
    const updatedProperty = await updateAPropertyRoom(
      roomItemQuery,
      roomItemUpdateData,
      session
    )

    await updateRoomBasedOnContractIdAndMovingId(
      updatedProperty.toObject(),
      session
    )

    if (!size(updatedProperty)) {
      throw new CustomError(404, 'Could not update Property room')
    }
  }

  if (size(inventoryItemUpdateData)) {
    const updatedPropertyItem = await propertyItemService.updateAPropertyItem(
      inventoryItemQuery,
      { $set: inventoryItemUpdateData },
      session
    )
    if (!size(updatedPropertyItem)) {
      throw new CustomError(404, 'Could not update Property item')
    }

    body.from = 'property'
    await propertyItemService.updatePropertyItemWithMovingProtocol(
      body,
      {
        query: inventoryItemQuery,
        updateData: { $set: inventoryItemUpdateData }
      },
      session
    )
  }

  return {
    msg: 'Successfully created property issue',
    code: '201'
  }
}

export const prepareAndCreateFileForIssue = async (body = {}, session) => {
  const fileIds = []
  if (!size(body.files)) return false

  for (const fileInfo of body.files) {
    const fileData = {
      ...fileInfo,
      createdBy: body.userId,
      context: 'moving_in_out',
      directive: 'Files',
      partnerId: body.partnerId
    }

    if (fileInfo?.issueType === 'rooms') {
      fileData.roomId = body.roomId
    } else {
      fileData.propertyItemId = body.propertyItemId
    }

    const [file] = await fileService.createAFile(fileData, session)
    fileIds.push(file._id)
  }
  return fileIds
}
