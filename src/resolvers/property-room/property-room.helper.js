import { find, size, union } from 'lodash'
import nid from 'nid'

import { PropertyRoomCollection } from '../models'
import {
  appHelper,
  fileHelper,
  importHelper,
  partnerHelper,
  propertyItemHelper,
  propertyRoomItemHelper
} from '../helpers'
import { CustomError } from '../common'

export const getPropertyRoomsByAggregation = async (pipeline = []) =>
  await PropertyRoomCollection.aggregate(pipeline)

export const getPropertyRoom = async (query, session) => {
  const roomInfo = await PropertyRoomCollection.findOne(query).session(session)
  return roomInfo
}

export const getAPropertyRoom = async (query, session) => {
  const propertyRoom = await PropertyRoomCollection.findOne(query).session(
    session
  )
  return propertyRoom
}

export const getPropertyRooms = async (query, session) => {
  const propertyRooms = await PropertyRoomCollection.find(query).session(
    session
  )
  return propertyRooms
}
export const getPropertyRoomsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options

  const propertyRoomPipeline = [
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
      $addFields: {
        hasMovingInOutProtocol: {
          $cond: [{ $ifNull: ['$propertyRoomId', false] }, true, false]
        }
      }
    },
    {
      $lookup: {
        from: 'property_rooms',
        foreignField: 'propertyRoomId',
        localField: 'propertyRoomId',
        let: { mainId: '$_id' },
        as: 'movingInPropertyRoom',
        pipeline: [
          {
            $match: {
              $expr: {
                $not: { $eq: ['$_id', '$$mainId'] }
              }
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'property_rooms',
        foreignField: 'propertyRoomId',
        localField: '_id',
        as: 'hasMovingInOutRooms'
      }
    },
    {
      $addFields: {
        hasMovingInOutProtocol: {
          $cond: [
            {
              $or: [
                '$hasMovingInOutProtocol',
                { $gt: [{ $size: '$hasMovingInOutRooms' }, 0] }
              ]
            },
            true,
            false
          ]
        },
        movingInPropertyRoom: {
          $first: '$movingInPropertyRoom'
        }
      }
    },
    appHelper.getUnwindPipeline('items'),
    {
      $addFields: {
        movingInItem: {
          $first: {
            $filter: {
              input: { $ifNull: ['$movingInPropertyRoom.items', []] },
              as: 'item',
              cond: {
                $eq: ['$$item.id', '$items.id']
              }
            }
          }
        }
      }
    },
    {
      $lookup: {
        from: 'property_room_items',
        localField: 'items.id',
        foreignField: '_id',
        as: 'room_item'
      }
    },
    appHelper.getUnwindPipeline('room_item'),
    {
      $addFields: {
        'items.name': '$room_item.name',
        'items.hasChange': {
          $cond: [
            {
              $and: [
                { $ifNull: ['$movingInPropertyRoom', false] },
                {
                  $or: [
                    { $not: { $ifNull: ['$movingInItem', false] } },
                    {
                      $not: { $eq: ['$items.status', '$movingInItem.status'] }
                    },
                    {
                      $not: {
                        $eq: ['$items.description', '$movingInItem.description']
                      }
                    },
                    {
                      $not: {
                        $eq: [
                          '$items.responsibleForFixing',
                          '$movingInItem.responsibleForFixing'
                        ]
                      }
                    }
                  ]
                }
              ]
            },
            true,
            false
          ]
        },
        'items.movingInItem': '$movingInItem'
      }
    },
    {
      $group: {
        _id: '$_id',
        name: {
          $first: '$name'
        },
        type: {
          $first: '$type'
        },
        items: {
          $push: {
            $cond: [
              {
                $ifNull: ['$items.id', false]
              },
              '$items',
              '$$REMOVE'
            ]
          }
        },
        createdAt: {
          $first: '$createdAt'
        },
        files: {
          $first: '$files'
        },
        hasMovingInOutProtocol: {
          $first: '$hasMovingInOutProtocol'
        },
        propertyRoomId: {
          $first: '$propertyRoomId'
        }
      }
    },
    appHelper.getUnwindPipeline('files'),
    {
      $lookup: {
        from: 'files',
        localField: 'files',
        foreignField: '_id',
        pipeline: [...appHelper.getFilesPathUrl()],
        as: 'fileInfo'
      }
    },
    appHelper.getUnwindPipeline('fileInfo'),
    {
      $addFields: {
        fileInfo: {
          $cond: [
            { $not: { $ifNull: ['$fileInfo', false] } },
            '$$REMOVE',
            {
              fileId: '$fileInfo._id',
              path: '$fileInfo.path'
            }
          ]
        }
      }
    },
    {
      $group: {
        _id: '$_id',
        name: {
          $first: '$name'
        },
        type: {
          $first: '$type'
        },
        items: {
          $first: '$items'
        },
        createdAt: {
          $first: '$createdAt'
        },
        files: {
          $push: '$fileInfo'
        },
        hasMovingInOutProtocol: {
          $first: '$hasMovingInOutProtocol'
        },
        propertyRoomId: {
          $first: '$propertyRoomId'
        }
      }
    },
    {
      $sort: sort
    }
  ]
  const propertyRooms =
    (await PropertyRoomCollection.aggregate(propertyRoomPipeline)) || []
  return propertyRooms
}

export const countPropertyRooms = async (query, session) => {
  const numberOfPropertyRooms = await PropertyRoomCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfPropertyRooms
}

export const queryPropertyRooms = async (req) => {
  const { body, user } = req
  const { query, options } = body
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['propertyId'], query)
  const { partnerId } = user
  query.partnerId = partnerId
  if (!size(query.movingId)) {
    query.contractId = { $exists: false }
    query.movingId = { $exists: false }
  }
  appHelper.validateSortForQuery(options.sort)

  const propertyRoomsData = await getPropertyRoomsForQuery(body)
  const filteredDocuments = await countPropertyRooms(query)
  const totalDocuments = filteredDocuments
  return {
    data: propertyRoomsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getItemsInfo = async (propertyRoom) => {
  if (size(propertyRoom.items)) {
    const items = []

    for (const item of propertyRoom.items) {
      const { id = '', description = '', responsibleForFixing, status } = item
      // Not include the room items if status is notApplicable/false while creating pdf
      if (status && status !== 'notApplicable') {
        const roomItemObj = {}

        if (id) {
          const roomItem = await propertyRoomItemHelper.getPropertyRoomItem({
            _id: id
          })
          const { name = '' } = roomItem || {}
          roomItemObj.item_name = name
        }

        roomItemObj.has_issue = status === 'issues'

        if (responsibleForFixing) {
          const partnerId = propertyRoom.partnerId || ''
          const partnerInfo = await partnerHelper.getAPartner(
            { _id: partnerId },
            null,
            ['owner']
          )
          const { owner = {} } = partnerInfo || {}

          const userLang =
            size(owner) && owner.getLanguage() ? owner.getLanguage() : 'no'

          if (responsibleForFixing === 'noActionRequired')
            roomItemObj.responsible_for_fixing = appHelper.translateToUserLng(
              'properties.moving_in.no_action_required',
              userLang
            )
          else
            roomItemObj.responsible_for_fixing = appHelper.translateToUserLng(
              'common.' + responsibleForFixing,
              userLang
            )
        }
        if (description) roomItemObj.issue_description = description

        items.push(roomItemObj)
      }
    }

    return items
  }
}

export const getRoomsImages = async (propertyRoom) => {
  const movingInfo =
    (await propertyItemHelper.getAPropertyItem({
      _id: propertyRoom.movingId
    })) || {}
  let files = []

  if (size(movingInfo)) {
    if (movingInfo.type === 'in') files = propertyRoom.files

    if (movingInfo.type === 'out') {
      if (size(propertyRoom.files) || size(propertyRoom.newFiles)) {
        files = union(propertyRoom.files, propertyRoom.newFiles)
      }
    }
    const defaultSize = { width: 215, height: 180, fit: 'min' } // Default size
    const fileImages = await fileHelper.getFileImages(files, defaultSize)
    return fileImages
  }
}

const getTypeFromName = (name = '') => {
  let type
  if (name) {
    if (name.includes('stue') || name.includes('livingroom')) {
      type = 'living_room'
    } else if (name.includes('soverom') || name.includes('bedroom')) {
      type = 'bedroom'
    } else if (name.includes('kjøkken') || name.includes('kitchen')) {
      type = 'kitchen'
    } else if (
      name.includes('bad') ||
      name.includes('toalet') ||
      name.includes('bath') ||
      name.includes('toilet')
    ) {
      type = 'bath_toilet'
    } else if (
      name.includes('balkong') ||
      name.includes('uteområde') ||
      name.includes('balcony') ||
      name.includes('outdoor')
    ) {
      type = 'balcony_outdoor_area'
    } else if (name.includes('fasade') || name.includes('facade')) {
      type = 'facade'
    } else if (name.includes('andre') || name.includes('other')) {
      type = 'other'
    } else if (name.includes('vaskerom') || name.includes('laundry')) {
      type = 'laundry_room'
    } else if (
      name.includes('gang') ||
      name.includes('entré') ||
      name.includes('entrance') ||
      name.includes('hallway')
    ) {
      type = 'entrance_hallway'
    } else if (
      name.includes('bod') ||
      name.includes('storage') ||
      name.includes('unit')
    ) {
      type = 'storage_unit'
    } else if (name.includes('garasje') || name.includes('Ggrage')) {
      type = 'garage'
    }
  }
  return type
}

export const prepareRoomsList = async (importData, preparedData, session) => {
  const roomsList = []
  const { importRefId, partnerId, propertyId } = importData
  const { rooms } = preparedData

  if (size(rooms)) {
    const roomImportsData = await importHelper.getMultipleImports(
      { importRefId, partnerId, collectionName: 'room' },
      session
    )

    for (const roomNo of rooms) {
      const roomImportData = find(
        roomImportsData,
        (data) => data?.jsonData['Room No'] === roomNo
      )

      if (size(roomImportData)) {
        const name = roomImportData?.jsonData?.Name
        const type = getTypeFromName(name?.toLowerCase())

        roomsList.push({
          _id: nid(17),
          name,
          files: [],
          items: [],
          partnerId,
          propertyId,
          type
        })
      }
    }
  }

  return roomsList
}

export const prepareAndValidatePropertyIssueData = async (
  params = {},
  session
) => {
  if (params.issueType === 'rooms') {
    const roomItemUpdateData = {}
    const roomItemQuery = {
      _id: params.roomId,
      items: { $elemMatch: { id: params.itemId } }
    }

    const propertyRoom = await getAPropertyRoom(
      {
        _id: params.roomId,
        partnerId: params.partnerId
      },
      session
    )

    if (!propertyRoom?._id) {
      throw new CustomError(404, 'Property room not found')
    }

    if (!size(propertyRoom?.items)) {
      throw new CustomError(
        404,
        'Property room item is not available to create issue'
      )
    }

    if (!propertyRoom.items.find((item) => item.id === params.itemId)) {
      throw new CustomError(404, 'Property room item not found')
    }

    roomItemUpdateData['items.$.title'] = params.title
    roomItemUpdateData['items.$.description'] = params.description
    roomItemUpdateData['items.$.status'] = 'issues'
    roomItemUpdateData['items.$.responsibleForFixing'] = 'noActionRequired'
    if (size(params.fileIds)) {
      roomItemUpdateData.files = union(propertyRoom.files, params.fileIds)
    }
    return {
      roomItemUpdateData,
      roomItemQuery
    }
  } else if (params.issueType === 'inventory') {
    const inventoryItemUpdateData = {}
    let taskUpdateData = {}
    const inventoryItemQuery = {
      _id: params.propertyItemId,
      propertyId: params.propertyId,
      'inventory.furniture': { $elemMatch: { id: params.itemId } }
    }

    const propertyItem = await propertyItemHelper.getAPropertyItem(
      {
        _id: params.propertyItemId
      },
      session
    )

    if (!propertyItem?._id) {
      throw new CustomError(404, 'Property item not found')
    }

    if (!size(propertyItem?.inventory?.furniture)) {
      throw new CustomError(
        404,
        'Property furniture is not available to create issue'
      )
    }

    const existFurniture = propertyItem.inventory.furniture.find(
      (item) => item.id === params.itemId
    )

    if (!existFurniture) {
      throw new CustomError(404, 'Furniture not found')
    }

    inventoryItemUpdateData['inventory.furniture.$.description'] =
      params.description
    inventoryItemUpdateData['inventory.furniture.$.title'] = params.title
    inventoryItemUpdateData['inventory.furniture.$.status'] = 'issues'
    if (params.quantity) {
      inventoryItemUpdateData['inventory.furniture.$.quantity'] =
        params.quantity
    }
    if (size(params.fileIds)) {
      inventoryItemUpdateData['inventory.files'] = union(
        propertyItem.inventory.files,
        params.fileIds
      )
    }
    taskUpdateData = {
      contractId: params?.contractId,
      description: params.description,
      existFurnitureInfo: existFurniture,
      furnitureId: existFurniture.id,
      partnerId: params.partnerId,
      previousAssignType: existFurniture.responsibleForFixing,
      propertyId: params.propertyId,
      status: 'open',
      title: params.title,
      userId: params.userId
    }

    if (existFurniture.taskId) {
      taskUpdateData.createdTaskId = existFurniture.taskId
    }
    return {
      inventoryItemQuery,
      inventoryItemUpdateData,
      taskUpdateData
    }
  }
  return {}
}
