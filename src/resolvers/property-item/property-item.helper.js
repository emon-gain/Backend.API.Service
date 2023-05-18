import { concat, clone, each, find, isBoolean, size } from 'lodash'
import nid from 'nid'
import moment from 'moment-timezone'
import { PropertyItemCollection } from '../models'
import {
  appHelper,
  contractHelper,
  fileHelper,
  importHelper,
  listingHelper,
  propertyRoomHelper
} from '../helpers'
import { propertyItemService } from '../services'
import { CustomError } from '../common'

export const getLastPropertyItem = async (query, session) => {
  const lastItemInfo = await PropertyItemCollection.find(query)
    .sort({ createdAt: -1 })
    .session(session)
  return size(lastItemInfo) ? lastItemInfo[0] : false
}

export const getUniqueIds = async (fieldName, query, session) =>
  (await PropertyItemCollection.distinct(fieldName, query).session(session)) ||
  []

export const propertyItemAggregateHelper = async (pipeline) =>
  PropertyItemCollection.aggregate(pipeline)

export const getAPropertyItem = async (query, session, populate = []) => {
  const item = await PropertyItemCollection.findOne(query)
    .populate(populate)
    .session(session)
  return item
}
export const getPropertyItems = async (query, session, options = {}) => {
  const { sort = {} } = options || {}
  const item = await PropertyItemCollection.find(query)
    .sort(sort || {})
    .session(session)
  return item
}

export const getPropertyItemsForQuery = async (params, populate = []) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const propertyItems = await PropertyItemCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return propertyItems
}

export const countPropertyItems = async (query, session) => {
  const numberOfPropertyItems = await PropertyItemCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfPropertyItems
}

export const queryPropertyItems = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const propertyItemsData = await getPropertyItemsForQuery(body, [
    'partner',
    'agent',
    'property',
    'tenant',
    'contract'
  ])
  const filteredDocuments = await countPropertyItems(query)
  const totalDocuments = await countPropertyItems({})
  return {
    data: propertyItemsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const inventoryPipeline = () => [
  {
    $project: {
      _id: 1,
      propertyId: 1,
      contractId: 1,
      inventory: {
        furniture: 1,
        isFurnished: 1
      }
    }
  },
  appHelper.getUnwindPipeline('inventory.furniture'),
  {
    $project: {
      _id: 1,
      furnitureId: '$inventory.furniture.id',
      name: '$inventory.furniture.name',
      quantity: '$inventory.furniture.quantity',
      status: '$inventory.furniture.status',
      title: '$inventory.furniture.title',
      description: '$inventory.furniture.description',
      responsibleForFixing: '$inventory.furniture.responsibleForFixing',
      dueDate: '$inventory.furniture.dueDate',
      propertyId: 1,
      contractId: 1
    }
  },
  {
    $lookup: {
      from: 'property_items',
      localField: 'propertyId',
      foreignField: 'propertyId',
      as: 'propertyItems',
      let: { propertyItemId: '$_id', furnitureId: '$furnitureId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                {
                  $not: {
                    $eq: ['$_id', '$$propertyItemId']
                  }
                },
                { $ifNull: ['$contractId', false] }
              ]
            }
          }
        },
        {
          $addFields: {
            matchedFurniture: {
              $first: {
                $filter: {
                  input: { $ifNull: ['$inventory.furniture', []] },
                  as: 'furniture',
                  cond: {
                    $eq: ['$$furniture.id', '$$furnitureId']
                  }
                }
              }
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      movingInOutPropertyItem: {
        $first: {
          $filter: {
            input: { $ifNull: ['$propertyItems', []] },
            as: 'item',
            cond: {
              $ifNull: ['$$item.matchedFurniture', false]
            }
          }
        }
      },
      movingInItem: {
        $first: {
          $filter: {
            input: { $ifNull: ['$propertyItems', []] },
            as: 'item',
            cond: {
              $ifNull: ['$$item.contractId', '$contractId']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      hasMovingInOutProtocolInfo: {
        $cond: [
          {
            $or: [
              { $ifNull: ['$contractId', false] },
              { $ifNull: ['$movingInOutPropertyItem', false] }
            ]
          },
          true,
          false
        ]
      },
      movingInFurniture: '$movingInItem.matchedFurniture'
    }
  },
  {
    $addFields: {
      hasChange: {
        $cond: [
          {
            $and: [
              { $ifNull: ['$movingInItem', false] },
              {
                $or: [
                  { $not: { $eq: ['$status', '$movingInFurniture.status'] } },
                  {
                    $not: {
                      $eq: ['$description', '$movingInFurniture.description']
                    }
                  },
                  {
                    $not: {
                      $eq: [
                        '$responsibleForFixing',
                        '$movingInFurniture.responsibleForFixing'
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
      }
    }
  }
]

const getPropertyItemPipelineForMovingOut = () => [
  {
    $lookup: {
      from: 'property_items',
      localField: 'contractId',
      foreignField: 'contractId',
      let: { mainId: '$_id' },
      pipeline: [
        {
          $match: {
            $expr: {
              $not: { $eq: ['$$mainId', '$_id'] }
            }
          }
        },
        {
          $limit: 1
        }
      ],
      as: 'movingItem'
    }
  },
  {
    $unwind: {
      path: '$movingItem',
      preserveNullAndEmptyArrays: true
    }
  }
]

const keysPipeline = () => [
  {
    $project: {
      _id: 1,
      contractId: 1,
      keys: {
        keysList: 1
      }
    }
  },
  ...getPropertyItemPipelineForMovingOut(),
  appHelper.getUnwindPipeline('keys.keysList'),
  {
    $addFields: {
      movingInKey: {
        $first: {
          $filter: {
            input: { $ifNull: ['$movingItem.keys.keysList', []] },
            as: 'key',
            cond: {
              $eq: ['$keys.keysList.id', '$$key.id']
            }
          }
        }
      }
    }
  },
  {
    $project: {
      _id: 1,
      keysId: '$keys.keysList.id',
      kindOfKey: '$keys.keysList.kindOfKey',
      numberOfKey: '$keys.keysList.numberOfKey',
      numberOfKeysReturned: '$keys.keysList.numberOfKeysReturned',
      movingInKey: 1
    }
  }
]
const meterReadingPipeline = () => [
  {
    $project: {
      _id: 1,
      contractId: 1,
      meterReading: {
        meters: 1
      }
    }
  },
  ...getPropertyItemPipelineForMovingOut(),
  appHelper.getUnwindPipeline('meterReading.meters'),
  {
    $addFields: {
      movingInMeterReading: {
        $first: {
          $filter: {
            input: { $ifNull: ['$movingItem.meterReading.meters', []] },
            as: 'meter',
            cond: {
              $eq: ['$meterReading.meters.id', '$$meter.id']
            }
          }
        }
      }
    }
  },
  {
    $project: {
      _id: 1,
      meterId: '$meterReading.meters.id',
      numberOfMeter: '$meterReading.meters.numberOfMeter',
      measureOfMeter: '$meterReading.meters.measureOfMeter',
      typeOfMeter: '$meterReading.meters.typeOfMeter',
      date: '$meterReading.meters.date',
      movingInMeterReading: 1
    }
  }
]

const getPropertyUtilityDetailsDataForQuery = async (params) => {
  const { query, options } = params
  const { sort, skip, limit } = options
  const { context } = query

  delete query.context
  const pipeline = [
    {
      $match: query
    }
  ]
  if (context === 'inventories') pipeline.push(...inventoryPipeline())
  if (context === 'keys') pipeline.push(...keysPipeline())
  if (context === 'meter_reading') pipeline.push(...meterReadingPipeline())
  pipeline.push(
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  )

  const propertyUtilityDetailItems =
    (await PropertyItemCollection.aggregate(pipeline)) || []
  return propertyUtilityDetailItems
}

const prepareQueryForInventory = (params) => {
  const {
    context,
    isFurnished = false,
    partnerId,
    propertyId,
    propertyItemId
  } = params

  let query = {
    partnerId,
    propertyId,
    contractId: { $exists: false },
    type: { $exists: false },
    context
  }
  if (context === 'inventories') query['inventory.isFurnished'] = isFurnished
  if (size(propertyItemId)) {
    query = {
      _id: propertyItemId,
      partnerId,
      context
    }
  }

  return query
}

const countPropertyItemForInventory = async (query, context) => {
  delete query.context
  const pipeline = [
    {
      $match: query
    }
  ]
  if (context === 'inventories')
    pipeline.push(appHelper.getUnwindPipeline('inventory.furniture'))
  if (context === 'keys')
    pipeline.push(appHelper.getUnwindPipeline('keys.keysList'))
  if (context === 'meter_reading')
    pipeline.push(appHelper.getUnwindPipeline('meterReading.meters'))
  const inventoryDetailItems =
    (await PropertyItemCollection.aggregate(pipeline)) || []
  return inventoryDetailItems.length
}

export const queryPropertyUtilityDetails = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { query, options } = body
  appHelper.checkRequiredFields(['context', 'propertyId'], query)
  appHelper.validateSortForQuery(options.sort)
  const { partnerId } = user
  query.partnerId = partnerId
  const { propertyId, context } = query
  body.query = prepareQueryForInventory(query)
  const inventoryPropertyItemData = await getPropertyUtilityDetailsDataForQuery(
    body
  )
  const propertyItem = (await getAPropertyItem(body.query)) || {}
  const isFurnished = propertyItem?.inventory?.isFurnished || false
  const filteredDocuments = await countPropertyItemForInventory(
    body.query,
    context
  )
  delete body.query['inventory.isFurnished']
  const totalDocuments = await countPropertyItemForInventory(
    {
      partnerId,
      propertyId,
      contractId: { $exists: false },
      type: { $exists: false }
    },
    context
  )
  return {
    data: inventoryPropertyItemData,
    metaData: {
      filteredDocuments,
      isFurnished,
      totalDocuments,
      type: propertyItem?.type
    }
  }
}

export const prepareFurnitureList = async (
  importData,
  preparedData,
  session
) => {
  const furniture = []
  const { importRefId, partnerId } = importData
  const { inventories, quantityOfInventories } = preparedData
  if (size(inventories)) {
    const inventoryImportsData = await importHelper.getMultipleImports(
      { importRefId, partnerId, collectionName: 'inventory' },
      session
    )

    for (const [index, inventoryNo] of inventories.entries()) {
      const inventoryImportData = find(inventoryImportsData, (data) => {
        const { jsonData } = data
        const { 'Inventory No': dataInventoryNo } = jsonData
        return dataInventoryNo === inventoryNo
      })

      if (size(inventoryImportData)) {
        furniture.push({
          id: nid(17),
          name: inventoryImportData?.jsonData?.Name,
          quantity: quantityOfInventories[index] || 1
        })
      }
    }
  }
  return furniture
}

export const prepareKeysList = (preparedData) => {
  const keysList = []
  const { quantityOfKeys, typeOfKey } = preparedData

  if (size(typeOfKey)) {
    for (const [index, type] of typeOfKey.entries()) {
      keysList.push({
        id: nid(17),
        kindOfKey: type,
        numberOfKey: quantityOfKeys[index] || 1
      })
    }
  }

  return keysList
}

export const prepareMetersList = (preparedData) => {
  const meters = []
  const { meterNumber, typeOfMeter, measures, date } = preparedData

  for (const [index, number] of meterNumber.entries()) {
    meters.push({
      id: nid(17),
      measureOfMeter: measures[index],
      typeOfMeter: typeOfMeter[index],
      numberOfMeter: number,
      date: size(date) ? new Date(date) : new Date()
    })
  }

  return meters
}

export const preparePropertyItemAddData = (body, previousItem = {}) => {
  const {
    from,
    inventory = {},
    isFurnished,
    key = {},
    meterReading = {},
    propertyItemElement
  } = body
  const furniture = previousItem.inventory?.furniture || []
  const keysList = previousItem.keys?.keysList || []
  const meters = previousItem.meterReading?.meters || []
  const updateData = {}
  const setData = {}
  const pushData = {}
  const returnData = {}
  const elementId = nid(17)
  if (from !== 'property')
    appHelper.checkRequiredFields(['propertyItemId'], body)
  if (propertyItemElement === 'inventory') {
    if (body.hasOwnProperty('isFurnished')) {
      setData['inventory.isFurnished'] = isFurnished
      returnData.isFurnished = isFurnished
    } else {
      appHelper.compactObject(inventory, true)
      appHelper.checkRequiredFields(['name', 'quantity'], inventory)
      const existFurniture = furniture.find(
        (item) => item.name === inventory.name
      )
      if (existFurniture)
        throw new CustomError(400, 'You have already added the inventory item')
      const furnitureData = {
        name: inventory.name,
        id: elementId,
        quantity: inventory.quantity
      }
      returnData.name = inventory.name
      returnData.quantity = inventory.quantity
      returnData.furnitureId = elementId
      const {
        status = '',
        title = '',
        responsibleForFixing = '',
        taskId = '',
        dueDate = '',
        description = ''
      } = inventory
      if (status) furnitureData.status = status
      if (title) furnitureData.title = title
      if (responsibleForFixing)
        furnitureData.responsibleForFixing = responsibleForFixing
      if (taskId) furnitureData.taskId = taskId
      if (dueDate) furnitureData.dueDate = dueDate
      if (description) furnitureData.description = description
      pushData['inventory.furniture'] = furnitureData
    }
  } else if (propertyItemElement === 'keys') {
    appHelper.compactObject(key, true)
    appHelper.checkRequiredFields(['kindOfKey', 'numberOfKey'], key)
    const existKey = keysList.find((item) => item.kindOfKey === key.kindOfKey)
    if (existKey) throw new CustomError(400, 'You have already added the key')
    pushData['keys.keysList'] = {
      id: elementId,
      kindOfKey: key.kindOfKey,
      numberOfKey: key.numberOfKey
    }
    returnData.kindOfKey = key.kindOfKey
    returnData.keysId = elementId
    returnData.numberOfKey = key.numberOfKey
  } else if (propertyItemElement === 'meterReading') {
    appHelper.compactObject(meterReading, true)
    appHelper.checkRequiredFields(
      ['numberOfMeter', 'typeOfMeter', 'measureOfMeter', 'date'],
      meterReading
    )
    const existMeter = meters.find(
      (item) => item.numberOfMeter === meterReading.numberOfMeter
    )
    if (existMeter)
      throw new CustomError(400, 'You have already added the meter number')
    pushData['meterReading.meters'] = {
      id: elementId,
      numberOfMeter: meterReading.numberOfMeter,
      typeOfMeter: meterReading.typeOfMeter,
      measureOfMeter: meterReading.measureOfMeter,
      date: meterReading.date
    }
    returnData.numberOfMeter = meterReading.numberOfMeter
    returnData.typeOfMeter = meterReading.typeOfMeter
    returnData.measureOfMeter = meterReading.measureOfMeter
    returnData.date = meterReading.date
    returnData.meterId = elementId
  }
  if (size(setData)) updateData.$set = setData
  if (size(pushData)) updateData.$push = pushData
  return { updateData, returnData }
}

export const preparePropertyItemUpdateData = (body, previousItem) => {
  const { from, inventory, key, meterReading, propertyItemElement } = body
  const query = {
    _id: previousItem._id
  }
  const updateData = {}
  let setData = {}
  const taskUpdateData = {}
  if (propertyItemElement === 'inventory') {
    appHelper.compactObject(inventory)
    appHelper.checkRequiredFields(['furnitureId'], inventory)
    const previousFurniture = previousItem.inventory?.furniture || []
    const {
      description,
      dueDate,
      furnitureId,
      name,
      quantity,
      responsibleForFixing,
      status,
      title
    } = inventory
    const existFurniture = previousFurniture.find(
      (item) => item.id === furnitureId
    )
    if (!existFurniture) throw new CustomError(404, 'Furniture not found')
    query['inventory.furniture'] = {
      $elemMatch: {
        id: furnitureId
      }
    }
    if (name && name !== existFurniture.name) {
      const duplicateFurniture = previousFurniture.find(
        (item) => item.id !== furnitureId && item.name === name
      )
      if (duplicateFurniture)
        throw new CustomError(400, 'Furniture already exist')
      setData['inventory.furniture.$.name'] = name
    } else if (quantity && quantity !== existFurniture.quantity)
      setData['inventory.furniture.$.quantity'] = quantity
    else if (status && status !== existFurniture.status) {
      setData['inventory.furniture.$.status'] = status
      taskUpdateData.status = status
    } else if (
      dueDate &&
      existFurniture.status === 'issues' &&
      existFurniture.responsibleForFixing !== 'noActionRequired' &&
      new Date(dueDate).getTime() !== new Date(existFurniture.dueDate).getTime()
    ) {
      setData['inventory.furniture.$.dueDate'] = dueDate
      taskUpdateData.dueDate = dueDate
    } else if (
      title &&
      existFurniture.status === 'issues' &&
      title !== existFurniture.title
    ) {
      setData['inventory.furniture.$.title'] = title
      taskUpdateData.title = title
    } else if (
      description &&
      existFurniture.status === 'issues' &&
      description !== existFurniture.description
    ) {
      setData['inventory.furniture.$.description'] = description
      taskUpdateData.description = description
    } else if (
      responsibleForFixing &&
      existFurniture.status === 'issues' &&
      responsibleForFixing !== existFurniture.responsibleForFixing
    ) {
      setData['inventory.furniture.$.responsibleForFixing'] =
        responsibleForFixing
      taskUpdateData.responsibleForFixing = responsibleForFixing
      if (responsibleForFixing === 'noActionRequired')
        taskUpdateData.status = 'notApplicable'
      else taskUpdateData.status = 'issues'
    }
    if (from === 'moving_out') {
      if (size(setData)) {
        const furnitureData = JSON.parse(JSON.stringify(existFurniture))
        const previousData = {}
        for (const item in furnitureData) {
          previousData['inventory.furniture.$.' + item] = furnitureData[item]
        }
        setData = {
          ...previousData,
          ...setData
        }
      }
    }
  } else if (propertyItemElement === 'keys') {
    appHelper.compactObject(key)
    appHelper.checkRequiredFields(['keysId'], key)
    const { keysId, kindOfKey, numberOfKey, numberOfKeysReturned } = key
    const previousKeys = previousItem.keys?.keysList || []
    const existKey = previousKeys.find((item) => item.id === keysId)
    if (!existKey) throw new CustomError(404, 'Key not found')
    query['keys.keysList'] = {
      $elemMatch: {
        id: keysId
      }
    }
    if (kindOfKey && kindOfKey !== existKey.kindOfKey) {
      const duplicateKey = previousKeys.find(
        (item) => item.id !== keysId && item.kindOfKey === kindOfKey
      )
      if (duplicateKey) throw new CustomError(400, 'Key already exist')
      setData['keys.keysList.$.kindOfKey'] = kindOfKey
    } else if (
      key.hasOwnProperty('numberOfKey') &&
      numberOfKey !== existKey.numberOfKey
    )
      setData['keys.keysList.$.numberOfKey'] = numberOfKey
    else if (key.hasOwnProperty('numberOfKeysReturned'))
      setData['keys.keysList.$.numberOfKeysReturned'] = numberOfKeysReturned
  } else if (propertyItemElement === 'meterReading') {
    appHelper.compactObject(meterReading)
    appHelper.checkRequiredFields(['meterId'], meterReading)
    const { date, measureOfMeter, meterId, numberOfMeter, typeOfMeter } =
      meterReading
    const previousMeters = previousItem.meterReading?.meters || []
    const existMeter = previousMeters.find((item) => item.id === meterId)
    if (!existMeter) throw new CustomError(404, 'Key not found')
    query['meterReading.meters'] = {
      $elemMatch: {
        id: meterId
      }
    }
    if (numberOfMeter && numberOfMeter !== existMeter.numberOfMeter) {
      const duplicateKey = previousMeters.find(
        (item) => item.id !== meterId && item.numberOfMeter === numberOfMeter
      )
      if (duplicateKey) throw new CustomError(400, 'Meter already exist')
      setData['meterReading.meters.$.numberOfMeter'] = numberOfMeter
    } else if (
      date &&
      new Date(date).getTime() !== new Date(existMeter.date).getTime()
    )
      setData['meterReading.meters.$.date'] = date
    else if (measureOfMeter && measureOfMeter !== existMeter.measureOfMeter)
      setData['meterReading.meters.$.measureOfMeter'] = measureOfMeter
    else if (typeOfMeter && typeOfMeter !== existMeter.typeOfMeter)
      setData['meterReading.meters.$.typeOfMeter'] = typeOfMeter
  }
  if (size(setData)) updateData.$set = setData
  return {
    query,
    taskUpdateData,
    updateData
  }
}

export const preparePropertyItemRemoveData = async (body) => {
  const { from, itemId, propertyItemElement, propertyItemId } = body
  const itemQuery = { _id: propertyItemId }
  if (from === 'moving_in') itemQuery.type = 'in'
  const propertyItemInfo = await getAPropertyItem(itemQuery)
  if (!propertyItemInfo) throw new CustomError(404, 'Property item not found')
  const query = {
    _id: propertyItemInfo._id
  }
  const data = {}
  if (propertyItemElement === 'inventory') {
    const propertyItem = propertyItemInfo.inventory.furniture.find(
      (item) => item.id === itemId
    )
    if (!size(propertyItem)) throw new CustomError(404, 'Furniture not found')
    data['$pull'] = {
      'inventory.furniture': {
        id: itemId
      }
    }
  }
  if (propertyItemElement === 'keys') {
    const propertyItem = propertyItemInfo.keys.keysList.find(
      (item) => item.id === itemId
    )
    if (!size(propertyItem)) throw new CustomError(404, 'Key not found')
    data['$pull'] = {
      'keys.keysList': {
        id: itemId
      }
    }
  }
  if (propertyItemElement === 'meterReading') {
    const propertyItem = propertyItemInfo.meterReading.meters.find(
      (item) => item.id === itemId
    )
    if (!size(propertyItem)) throw new CustomError(404, 'Meter not found')
    data['$pull'] = {
      'meterReading.meters': {
        id: itemId
      }
    }
  }
  return {
    data,
    query
  }
}

export const prepareQueryAndDataToUpdatePropertyItemForLambda = (params) => {
  const { contractId, data, partnerId, propertyItemId } = params

  const query = { _id: propertyItemId }
  if (contractId) query.contractId = contractId
  if (partnerId) query.partnerId = partnerId

  const updatingData = {}
  const { movingInPdfGenerated, movingOutPdfGenerated } = data

  if (isBoolean(movingInPdfGenerated))
    updatingData.movingInPdfGenerated = movingInPdfGenerated
  if (isBoolean(movingOutPdfGenerated))
    updatingData.movingOutPdfGenerated = movingOutPdfGenerated

  return { query, updatingData }
}

const getMovingSignersInfo = async (propertyItem) => {
  console.log('== Preparing moving in out signers data ==>')
  const {
    _id: propertyItemId,
    contract,
    partner,
    signatureMechanism
  } = propertyItem

  const {
    _id: contractId,
    accountId,
    agentId,
    rentalMeta,
    propertyId
  } = contract
  const { _id: partnerId, accountType } = partner

  if (!signatureMechanism) {
    throw new CustomError(
      `SignatureMechanism is not found for this propertyItem. propertyItemId: ${propertyItemId}, contractId: ${contractId}`
    )
  }
  if (!size(rentalMeta)) throw new CustomError('RentalMeta missing in contract')
  const isJointlyLiable = !!rentalMeta?.enabledJointlyLiable
  const partnerUrl = await appHelper.getPartnerURL(partnerId, true)

  const v1RedirectUrl = partnerUrl + '/esigning-success'

  console.log('Checking partnerId: ', partnerId, ' contractId', contractId)
  const v2SubDomain = await appHelper.getPartnerPublicURL(partnerId)
  console.log('Checking v2SubDomain: ', v2SubDomain)
  const v2_url = `${v2SubDomain}/lease/${contractId}?redirectFrom=idfy`
  console.log('Checking v2_url: ', v2_url)
  const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${v1RedirectUrl}`
  console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
  const redirectUrl = appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`

  const redirectSettings = {
    redirectMode: 'redirect',
    success: redirectUrl,
    cancel: redirectUrl + '&signingStatus=cancel',
    error: redirectUrl + '&signingStatus=error'
  }

  let signatureType = {}

  const signersMeta = {
    ui: {
      language: 'en'
    }
  }

  if (signatureMechanism === 'handWritten')
    signatureType = { signatureMethods: [], mechanism: 'handwritten' }
  else
    signatureType = {
      signatureMethods: ['NO_BANKID'],
      mechanism: 'pkisignature'
    }

  signersMeta.signatureType = signatureType

  const signers = []

  if (accountType === 'broker') {
    const signersMetaForAgent = clone(signersMeta)

    console.log('Checking partnerId: ', partnerId, ' contractId', contractId)
    const v2SubDomain = await appHelper.getPartnerURL(partnerId)
    console.log('Checking v2SubDomain: ', v2SubDomain)
    const v2_url = `${v2SubDomain}/property/properties/${propertyId}?redirectFrom=idfy`
    console.log('Checking v2_url: ', v2_url)
    const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${partnerUrl}/dtms/properties/${propertyId}`
    console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
    const agentRedirectUrl =
      appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`

    const agentRedirectSettings = {
      redirectMode: 'redirect',
      success: agentRedirectUrl,
      cancel: agentRedirectUrl + '&signingStatus=cancel',
      error: agentRedirectUrl + '&signingStatus=error'
    }

    signersMetaForAgent['redirectSettings'] = agentRedirectSettings

    signersMetaForAgent['externalSignerId'] = agentId
    signersMetaForAgent['tags'] = ['agent']

    signers.push(signersMetaForAgent)
  } else if (accountType === 'direct') {
    const signersMetaForAccount = clone(signersMeta)

    signersMetaForAccount['redirectSettings'] = redirectSettings

    signersMetaForAccount['externalSignerId'] = accountId
    signersMetaForAccount['tags'] = ['account']

    signers.push(signersMetaForAccount)
  }

  if (isJointlyLiable) {
    const multiTenantIds = rentalMeta?.tenants || []

    if (size(multiTenantIds)) {
      each(multiTenantIds, (tenantObj) => {
        const signersMetaForTenant = clone(signersMeta)

        signersMetaForTenant['redirectSettings'] = redirectSettings

        signersMetaForTenant['externalSignerId'] = tenantObj.tenantId || ''
        signersMetaForTenant['tags'] = ['tenant']

        signers.push(signersMetaForTenant)
      })
    }
  } else {
    const signersMetaForTenant = clone(signersMeta)

    signersMetaForTenant['redirectSettings'] = redirectSettings

    signersMetaForTenant['externalSignerId'] = rentalMeta.tenantId || ''
    signersMetaForTenant['tags'] = ['tenant']

    signers.push(signersMetaForTenant)
  }
  return signers
}

export const prepareMovingESignerCreationData = async (
  propertyItem,
  callBackParams
) => {
  const { _id: propertyItemId, contract, partner, type } = propertyItem

  if (!(type === 'in' || type === 'out'))
    throw new CustomError(404, 'Invalid propertyItem type found')

  if (!size(contract)) {
    throw new CustomError(
      404,
      `Contract not found for this propertyItem. propertyItemId: ${propertyItemId}`
    )
  }

  if (!size(partner)) {
    throw new CustomError(
      404,
      `Partner not found for this propertyItem. propertyItemId: ${propertyItemId}`
    )
  }
  const { _id: partnerId, owner } = partner

  if (!size(owner)) {
    throw new CustomError(
      404,
      `Owner not found in partner. partnerId: ${partnerId} for this propertyItem. propertyItemId: ${propertyItemId}`
    )
  }

  const userLang = owner?.profile?.language || 'no'
  const fileType =
    type === 'in' ? 'esigning_moving_in_pdf' : 'esigning_moving_out_pdf'
  const movingTitle = type === 'in' ? 'moving_in_signing' : 'moving_out_signing'
  const movingDes =
    type === 'in' ? 'moving_in_to_be_signed' : 'moving_out_to_be_signed'
  const externalId =
    type === 'in' ? `movingIn-${propertyItemId}` : `movingOut-${propertyItemId}`
  const fileNameForIdfy = type === 'in' ? 'movingIn.pdf' : 'movingOut.pdf'
  const contactEmail =
    process.env.STAGE === 'production'
      ? 'contact-us@uniteliving.com'
      : `contact-us.${process.env.STAGE}@uniteliving.com`

  const eSigningFileInfo =
    (await fileHelper.getAFileWithSort(
      { movingId: propertyItemId, type: fileType },
      { createdAt: -1 },
      null
    )) || {}
  if (!size(eSigningFileInfo))
    throw new CustomError(404, 'E-signing PDF file not found!')

  const fileKey = (await fileHelper.getFileKey(eSigningFileInfo)) || ''
  if (!size(fileKey))
    throw new CustomError(404, 'E-signing PDF file key not found!')

  const signers = await getMovingSignersInfo(propertyItem)

  if (!size(signers))
    throw new CustomError(
      404,
      'Signers info not found while preparing moving idfy data'
    )

  const dataForIdfy = {
    title: appHelper.translateToUserLng(
      'properties.moving_in.' + movingTitle,
      userLang
    ),
    description: appHelper.translateToUserLng(
      'properties.moving_in.' + movingDes,
      userLang
    ),
    contactDetails: { email: contactEmail },
    dataToSign: { fileName: fileNameForIdfy },
    signers,
    externalId,
    advanced: { tags: ['movingInOut'] },
    signatureType: { mechanism: 'handwritten' }
  }

  const queueCreationData = {
    action: callBackParams.callBackAction,
    destination: callBackParams.callBackDestination,
    event: callBackParams.callBackEvent,
    params: {
      callBackParams: {
        callBackAction: 'moving_e_signing_initialisation_process',
        callBackDestination: 'lease',
        callBackEvent: 'moving_e_signing_initialisation_process',
        callBackPriority: 'immediate'
      },
      dataForIdfy,
      docId: propertyItemId,
      eSignType: type === 'in' ? 'moving_in' : 'moving_out',
      fileType,
      fileKey,
      partnerId
    },
    priority: callBackParams.callBackPriority
  }

  return queueCreationData
}

export const queryPropertyItemForLambda = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { query } = body
  appHelper.checkRequiredFields(['propertyItemId'], query)
  const { propertyItemId, type } = query

  const queryData = { _id: propertyItemId }
  if (type) queryData.type = type

  return await getAPropertyItem(queryData)
}

export const preparingUpdatingDataOfPropertyItemForLambda = (params) => {
  if (!size(params)) return {}

  const {
    agentSigningStatus = {},
    draftMovingInDoc,
    draftMovingOutDoc,
    idfyMovingInDocId,
    landlordSigningStatus = {},
    movingSigningMeta = {},
    moveInCompleted,
    movingInSigningComplete,
    moveOutCompleted,
    movingOutSigningComplete,
    tenantSigningStatus = []
  } = params

  const updatingData = {}

  if (idfyMovingInDocId) updatingData.idfyMovingInDocId = idfyMovingInDocId

  if (size(agentSigningStatus))
    updatingData.agentSigningStatus = agentSigningStatus
  if (size(landlordSigningStatus))
    updatingData.landlordSigningStatus = landlordSigningStatus
  if (size(movingSigningMeta))
    updatingData.movingSigningMeta = movingSigningMeta
  if (size(tenantSigningStatus))
    updatingData.tenantSigningStatus = tenantSigningStatus

  if (isBoolean(draftMovingInDoc))
    updatingData.draftMovingInDoc = draftMovingInDoc
  if (isBoolean(draftMovingOutDoc))
    updatingData.draftMovingOutDoc = draftMovingOutDoc
  if (isBoolean(moveInCompleted)) updatingData.moveInCompleted = moveInCompleted
  if (isBoolean(moveOutCompleted))
    updatingData.moveOutCompleted = moveOutCompleted
  if (isBoolean(movingInSigningComplete))
    updatingData.movingInSigningComplete = movingInSigningComplete
  if (isBoolean(movingOutSigningComplete))
    updatingData.movingOutSigningComplete = movingOutSigningComplete

  return updatingData
}

export const getUpdatablePropertyItem = async (params) => {
  const { from = '', partnerId, propertyId } = params
  let propertyItemQuery = {}
  if (!from) return false
  if (from === 'property') {
    const leaseInfo =
      (await contractHelper.getAContract({
        partnerId,
        propertyId,
        status: 'active'
      })) || {}
    propertyItemQuery = {
      partnerId,
      propertyId,
      contractId: leaseInfo._id,
      type: { $exists: true },
      moveInCompleted: { $exists: false },
      moveOutCompleted: { $exists: false },
      isEsigningInitiate: { $exists: false }
    }
  }
  if (from === 'moving_in' || from === 'moving_out') {
    propertyItemQuery = {
      partnerId,
      propertyId,
      contractId: { $exists: false },
      type: { $exists: false }
    }
  }
  return await getLastPropertyItem(propertyItemQuery)
}

export const prepareDataForMovingProtocol = async (params, session) => {
  const { contractId, partnerId, propertyId, type, userId } = params

  const propertyInfo = await listingHelper.getListingById(propertyId)
  if (!propertyInfo) throw new CustomError(404, 'Property not found')
  const contractInfo = await contractHelper.getAContract({
    _id: contractId,
    partnerId,
    propertyId
  })
  if (!contractInfo) throw new CustomError(404, 'Contract not found')

  const propertyItemQuery = {
    partnerId,
    propertyId,
    contractId,
    type,
    isEsigningInitiate: { $exists: false },
    moveInCompleted: { $exists: false }
  }
  const movingProtocolInfo = await getLastPropertyItem(propertyItemQuery)

  if (size(movingProtocolInfo)) {
    return { movingProtocolInfo }
  } else {
    const propertyItemInfo =
      await propertyItemService.createOrGetPreviousPropertyItem(params, session)
    const newPropertyItemData = {
      ...JSON.parse(JSON.stringify(propertyItemInfo)),
      contractId,
      createdBy: userId,
      partnerId,
      propertyId,
      type,
      _id: undefined,
      createdAt: undefined,
      updatedAt: undefined
    }
    return { newPropertyItemData }
  }
}
export const prepareDataForAddPropertyRooms = async (
  params,
  propertyItemInfo = {}
) => {
  const { partnerId, propertyId, userId } = params
  const roomsData = []
  const roomsQuery = {
    partnerId,
    propertyId
  }
  if (propertyItemInfo.type === 'in') roomsQuery.contractId = { $exists: false }
  if (propertyItemInfo.type === 'out') roomsQuery.movingId = { $exists: false }

  const propertyRooms =
    (await propertyRoomHelper.getPropertyRooms(roomsQuery)) || []

  const lastMoveInPropertyItem = await getLastPropertyItem({
    propertyId: propertyItemInfo.propertyId,
    partnerId: propertyItemInfo.partnerId,
    contractId: propertyItemInfo.contractId,
    type: 'in',
    isEsigningInitiate: true,
    moveInCompleted: true
  })

  for (const roomInfo of propertyRooms) {
    const newRoomInfo = {
      _id: nid(17),
      contractId: propertyItemInfo.contractId,
      createdBy: userId,
      files: roomInfo.files || [],
      items: roomInfo.items || [],
      name: roomInfo.name || '',
      partnerId: roomInfo.partnerId,
      propertyId: roomInfo.propertyId,
      propertyItemId: roomInfo.propertyItemId,
      type: roomInfo.type
    }
    if (propertyItemInfo.type === 'in') {
      newRoomInfo.movingId = propertyItemInfo._id
      newRoomInfo.propertyRoomId = roomInfo._id
      roomsData.push(newRoomInfo)
    }
    if (propertyItemInfo.type === 'out') {
      newRoomInfo.movingId = propertyItemInfo._id

      //It's for direct move out room
      if (roomInfo && !roomInfo.propertyRoomId)
        newRoomInfo.propertyRoomId = roomInfo._id
      else newRoomInfo.moveInRoomId = roomInfo._id

      if (lastMoveInPropertyItem) {
        const moveInRoomInfo =
          (await propertyRoomHelper.getAPropertyRoom({
            movingId: lastMoveInPropertyItem._id,
            propertyRoomId: roomInfo._id
          })) || {}
        if (size(moveInRoomInfo)) newRoomInfo.moveInRoomId = moveInRoomInfo._id
      }
      roomsData.push(newRoomInfo)
    }
  }
  return roomsData
}

export const prepareDataForResetMovingProtocol = async (params) => {
  const { partnerId, propertyId } = params

  let taskIds = []
  let fileIds = []
  let propertyItemQuery = {}
  let propertyRoomQuery = {}
  const propertyItemIds = []
  const propertyRoomIds = []

  const propertyItemsUpdateData = [
    {
      $addFields: {
        'inventory.furniture': {
          $ifNull: ['$inventory.furniture', []]
        }
      }
    },
    {
      $set: {
        'inventory.isFurnished': false,
        'inventory.files': [],
        'inventory.furniture.status': 'notApplicable'
      }
    },
    {
      $unset: ['keys', 'meterReading']
    }
  ]
  const propertyRoomsUpdateData = {}

  const query = { partnerId, propertyId }
  const propertyItems = (await getPropertyItems(query)) || []
  const propertyRooms = (await propertyRoomHelper.getPropertyRooms(query)) || []

  if (propertyItems) {
    each(propertyItems, (propertyItem) => {
      const { inventory = {}, keys = {}, meterReading = {} } = propertyItem
      const { files = [], furniture = [] } = inventory

      const keyFiles = keys.files || []
      const meterFiles = meterReading.files || []

      if (size(furniture)) {
        const furnitureTaskIds = []
        each(furniture, (item) => {
          if (size(item.taskId)) furnitureTaskIds.push(item.taskId)
        })
        taskIds = concat(taskIds, furnitureTaskIds)
      }
      if (size(files)) fileIds = concat(fileIds, files)
      if (size(keyFiles)) fileIds = concat(fileIds, keyFiles)
      if (size(meterFiles)) fileIds = concat(fileIds, meterFiles)

      propertyItemIds.push(propertyItem._id)
    })
  }

  if (size(propertyRooms)) {
    each(propertyRooms, (propertyRoom) => {
      const { items = [], files = [] } = propertyRoom

      const roomItemsTaskIds = []
      each(items, (item) => {
        if (size(item.taskId)) roomItemsTaskIds.push(item.taskId)
      })
      if (size(files)) fileIds = concat(fileIds, files)
      taskIds = concat(taskIds, roomItemsTaskIds)

      if (size(items)) {
        propertyRoomsUpdateData['$set'] = {
          'items.$[].status': 'notApplicable'
        }
        propertyRoomIds.push(propertyRoom._id)
      }
    })
  }

  if (size(propertyItemIds)) {
    propertyItemQuery = { _id: { $in: propertyItemIds } }
  }
  if (size(propertyRoomIds)) {
    propertyRoomQuery = { _id: { $in: propertyRoomIds } }
  }

  return {
    fileIds,
    propertyItemQuery,
    propertyItemsUpdateData,
    propertyRoomQuery,
    propertyRoomsUpdateData,
    taskIds
  }
}

export const validateInitiateMovingInOutProtocolData = async (
  params = {},
  session
) => {
  const { contractId, movingId, movingType, partnerId, propertyId } = params

  const propertyItemQuery = {
    _id: movingId,
    contractId,
    partnerId,
    propertyId
  }

  const propertyItem = await getAPropertyItem(propertyItemQuery, session, [
    { path: 'partner', populate: ['partnerSetting'] }
  ])

  if (!size(propertyItem)) throw new CustomError(404, 'Property item not found')

  if (propertyItem.type !== movingType) {
    throw new CustomError(404, 'Wrong moving type')
  }

  const { partner = {} } = propertyItem
  const { partnerSetting } = partner

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner settings not found')
  }

  if (!partnerSetting?.propertySettings?.movingInOutProtocol) {
    throw new CustomError(
      400,
      "Moving in out protocol isn't enabled for this partner"
    )
  }

  const isMoveInOutCompleted =
    movingType === 'out'
      ? propertyItem.moveOutCompleted
      : propertyItem.moveInCompleted

  if (propertyItem.isEsigningInitiate && !isMoveInOutCompleted) {
    throw new CustomError(
      400,
      'We are processing the document. It could take couple of minutes.'
    )
  } else if (isMoveInOutCompleted) {
    throw new CustomError(
      400,
      `Moving ${movingType} protocol already completed`
    )
  }

  if (movingType === 'out') {
    const contract = await contractHelper.getAContract(
      { _id: contractId },
      session,
      [
        'property',
        {
          path: 'propertyItems',
          match: { isEsigningInitiate: { $ne: true } },
          options: { sort: { createdAt: -1 } }
        }
      ]
    )
    const { property, propertyItems = [] } = contract || {}
    if (!size(property)) {
      throw new CustomError(404, 'Property not found')
    }

    const isSoonEnding = (await property?.isSoonEnding()) || false
    const movingOutItem =
      find(propertyItems, (item) => item?.type === 'out') || {}
    const processedItem =
      find(
        propertyItems,
        (item) => item?.moveInCompleted && item?.moveOutCompleted
      ) || {}
    const isMoveOutForCaseOne =
      isSoonEnding && !movingOutItem?.isEsigningInitiate
    const isMoveOutForCaseTwo =
      !isSoonEnding &&
      (!size(processedItem) ||
        (processedItem?.type === 'in' && !movingOutItem?.isEsigningInitiate) ||
        processedItem?.type === 'out')
    const isMoveOutForClosedLease =
      contract?.status === 'closed' &&
      (!size(movingOutItem) ||
        (size(movingOutItem) &&
          !(
            movingOutItem.moveOutCompleted || movingOutItem.isEsigningInitiate
          )))
    const isMoveOut =
      contract?.rentalMeta?.status === 'closed'
        ? isMoveOutForClosedLease
        : isMoveOutForCaseOne || isMoveOutForCaseTwo
    console.log('Checking moving out validation data:', {
      isMoveOutForClosedLease,
      isMoveOutForCaseOne,
      isMoveOutForCaseTwo
    })
    if (!isMoveOut) {
      throw new CustomError(
        400,
        `Moving out protocol is not available for update`
      )
    }
  }
  return params
}

export const prepareAppQueueDataForMovingInOutProtocol = (params = {}) => {
  const { contractId, eSigningPdfContent, movingId, movingType, partnerId } =
    params

  const appQueueData = {
    action: 'handle_moving_in_out_pdf_generation',
    destination: 'lease',
    event: 'handle_pdf_generation',
    status: 'new',
    params: {
      contractId,
      eSigningPdfContent,
      movingId,
      movingType,
      partnerId
    },
    priority: 'immediate'
  }
  return appQueueData
}

export const getPropertyItemIdsByQuery = async (query = {}) => {
  const propertyItemIds = await PropertyItemCollection.distinct('_id', query)
  return propertyItemIds || []
}

export const prepareMovingInOutCancelData = (type) => {
  const unsetData = {
    tenantSigningStatus: 1,
    agentSigningStatus: 1,
    landlordSigningStatus: 1,
    movingSigningMeta: 1,
    isEsigningInitiate: 1,
    esigningInitiatedAt: 1,
    signatureMechanism: 1,
    idfyMovingInDocId: 1,
    idfyErrorEvents: 1
  }
  if (type === 'in') {
    unsetData.movingInPdfGenerated = 1
    unsetData.draftMovingInDoc = 1
  } else {
    unsetData.movingOutPdfGenerated = 1
    unsetData.draftMovingOutDoc = 1
  }
  return unsetData
}

export const getMovingInOutDataForESigningCleaner = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkUserId(user.userId)

  const pipeline = [
    {
      $match: {
        contractId: { $exists: true },
        ...body.query,
        esigningInitiatedAt: {
          $gte: moment().subtract(3, 'months').toDate(),
          $lte: moment().subtract(1, 'hour').toDate()
        },
        idfyMovingInDocId: { $exists: true },
        isEsigningInitiate: true,
        $or: [
          { movingInSigningComplete: { $ne: true } },
          { movingOutSigningComplete: { $ne: true } }
        ]
      }
    },
    { $sort: { createdAt: 1 } },
    {
      $project: {
        _id: 0,
        contractId: 1,
        movingId: '$_id',
        partnerId: 1
      }
    }
  ]
  return PropertyItemCollection.aggregate(pipeline)
}
