import { differenceBy, map, pick, size } from 'lodash'

import { CustomError } from '../common'
import { FileCollection, PropertyItemCollection } from '../models'
import {
  appHelper,
  contractHelper,
  fileHelper,
  partnerHelper,
  partnerSettingHelper,
  propertyItemHelper
} from '../helpers'
import {
  appQueueService,
  fileService,
  logService,
  partnerUsageService,
  propertyRoomService,
  taskService
} from '../services'
import { checkRequiredFields } from '../app/app.helper'

export const createAPropertyItem = async (data, session) => {
  const [response] = await PropertyItemCollection.create([data], { session })
  return response
}

export const updateAPropertyItem = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await PropertyItemCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  return response
}

export const removeAPropertyItem = async (query, session) => {
  const response = await PropertyItemCollection.findOneAndDelete(query, {
    session
  })
  return response
}

export const processQueryAndUpdatingDataForPropertyItemUpdate = (body) => {
  const { _id, protocolData } = body
  const {
    agentStatus,
    inventory,
    keys,
    landlordSigningStatus,
    meterReading,
    movingInSigningComplete,
    moveInCompleted,
    moveOutCompleted,
    movingOutSigningComplete,
    partnerId,
    roomsId,
    tenantSigningStatus
  } = protocolData
  let updateData = {}
  const query = {
    _id
  }
  if (partnerId) {
    query.partnerId = partnerId
  }
  // Set updating data
  if (size(roomsId)) {
    updateData = { roomsId }
  }
  if (size(inventory)) {
    updateData = { inventory }
  }
  if (size(keys)) {
    updateData = { keys }
  }
  if (size(meterReading)) {
    updateData = { meterReading }
  }
  if (size(agentStatus)) {
    updateData = {
      ...updateData,
      agentStatus
    }
  }
  if (size(landlordSigningStatus)) {
    updateData = {
      ...updateData,
      landlordSigningStatus
    }
  }
  if (size(tenantSigningStatus)) {
    updateData = {
      ...updateData,
      tenantSigningStatus
    }
  }
  if (size(tenantSigningStatus)) {
    updateData = {
      ...updateData,
      movingInSigningComplete
    }
  }
  if (size(movingInSigningComplete)) {
    updateData = {
      ...updateData,
      movingInSigningComplete
    }
  }
  if (size(moveInCompleted)) {
    updateData = {
      ...updateData,
      moveInCompleted
    }
  }
  if (size(movingOutSigningComplete)) {
    updateData = {
      ...updateData,
      movingOutSigningComplete
    }
  }
  if (size(moveOutCompleted)) {
    updateData = {
      ...updateData,
      moveOutCompleted
    }
  }
  return { query, updateData }
}

export const removePdfFileForMovingInOrOut = async (params, session) => {
  const { _id, type } = params
  if (!(_id && type)) {
    return false
  }
  const query = {
    movingId: _id
  }
  query.type =
    type === 'in' ? 'esigning_moving_in_pdf' : 'esigning_moving_out_pdf'
  const response = await FileCollection.findOneAndDelete(query, { session })
  if (size(response)) {
    console.log(`--- Removed a Pdf File For MovingInOrOut: ${response._id} ---`)
    // TODO :: removeFileFromS3 [fileCollection.js]
    // TODO :: removeFileLog [fileCollection.js]
  }
}

export const updateItemBasedOnContractIdAndType = async (params, session) => {
  const {
    partnerId,
    propertyId,
    contractId,
    type,
    inventory,
    keys,
    meterReading
  } = params
  let query = {}
  let updateData = {}
  if (contractId && type) {
    query = {
      partnerId,
      propertyId,
      $and: [{ contractId: { $exists: false } }, { type: { $exists: false } }]
    }
    updateData = {
      partnerId,
      propertyId,
      inventory: inventory || {},
      keys: keys || {},
      meterReading: meterReading || {}
    }
  } else if (!(contractId && type)) {
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
        contractId: activeLeaseInfo._id,
        type: { $exists: true },
        isEsigningInitiate: { $exists: false },
        moveInCompleted: { $exists: false },
        moveOutCompleted: { $exists: false }
      }
      const lastPropertyItem = await propertyItemHelper.getLastPropertyItem(
        propertyItemQuery,
        session
      )
      if (size(lastPropertyItem)) {
        query = {
          _id: lastPropertyItem._id,
          partnerId,
          propertyId
        }
        updateData = {
          partnerId,
          propertyId,
          inventory: inventory || {},
          keys: keys || {},
          meterReading: meterReading || {}
        }
      }
    }
  }
  if (size(query) && size(updateData)) {
    const updatedPropertyItem = await updateAPropertyItem(
      query,
      updateData,
      session
    )
    if (!size(updatedPropertyItem)) {
      throw new CustomError(404, `Could not update property item`)
    }
    console.log(
      `--- Updated Property Item for Id: ${updatedPropertyItem._id} ---`
    )
  }
}

export const updatePropertyItem = async (req) => {
  const { body, session } = req
  const requiredFields = ['_id', 'protocolData']
  appHelper.checkRequiredFields(requiredFields, body)
  const { query, updateData } =
    processQueryAndUpdatingDataForPropertyItemUpdate(body)
  const updatedPropertyItem = await updateAPropertyItem(
    query,
    updateData,
    session
  )
  if (!size(updatedPropertyItem)) {
    throw new CustomError(404, `Could not update property item`)
  }
  console.log(
    `--- Updated Property Item for Id: ${updatedPropertyItem._id} ---`
  )
  // TODO:: uploadFileToIdfy
  // TODO:: removePdfFileForMovingInOrOut
  await removePdfFileForMovingInOrOut(updatedPropertyItem.toObject(), session)
  // TODO:: sendMovingInEsigningNotice tenant
  // TODO:: sendMovingInEsigningNotice agent
  // TODO:: sendMovingInEsigningNotice landlord
  // TODO:: addPartnerUsagesForEsignCount & createMovingInOutSignedLog tenant
  // TODO:: addPartnerUsagesForEsignCount & createMovingInOutSignedLog landlord
  // TODO:: update propertyItems based on contractId and Type
  await updateItemBasedOnContractIdAndType(
    updatedPropertyItem.toObject(),
    session
  )
  return updatedPropertyItem
}

const updateReturnData = (propertyItem = {}, propertyItemElement = '') => {
  const {
    inventory = {},
    keys = {},
    meterReading = {},
    _id = ''
  } = propertyItem || {}
  const { furniture = [], isFurnished = false } = inventory
  const { keysList = [] } = keys
  const { meters = [] } = meterReading
  const furnitureData = map(
    furniture,
    ({
      name,
      quantity,
      id,
      status,
      title,
      description,
      dueDate,
      responsibleForFixing,
      taskId
    }) => ({
      _id,
      isFurnished,
      name,
      quantity,
      furnitureId: id,
      status,
      title,
      description,
      dueDate,
      responsibleForFixing,
      taskId
    })
  )
  const keysData = map(keysList, ({ numberOfKey, kindOfKey, id }) => ({
    _id,
    isFurnished,
    numberOfKey,
    kindOfKey,
    keysId: id
  }))
  const metersData = map(
    meters,
    ({ numberOfMeter, typeOfMeter, measureOfMeter, id, date }) => ({
      _id,
      isFurnished,
      numberOfMeter,
      typeOfMeter,
      measureOfMeter,
      date,
      meterId: id
    })
  )
  let returnData = []
  if (propertyItemElement === 'inventory') {
    returnData = furnitureData
    if (!size(returnData)) returnData.push({ _id, isFurnished }) // if there is no furniture, send isFurnished only
  } else if (propertyItemElement === 'keys') returnData = keysData
  else returnData = metersData
  return returnData
}

export const addPropertyItem = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'from',
    'propertyId',
    'propertyItemElement'
  ])
  const { body, session } = req
  appHelper.compactObject(body, true)
  const { partnerId, propertyId, propertyItemElement, propertyItemId, userId } =
    body
  let query = {
    partnerId,
    propertyId,
    contractId: { $exists: false },
    type: { $exists: false }
  }
  if (propertyItemId) query = { _id: propertyItemId, partnerId }
  const previousItem = await propertyItemHelper.getAPropertyItem(query)
  if (propertyItemId && !size(previousItem)) {
    throw new CustomError(400, 'Property item not found')
  }
  const { updateData } = propertyItemHelper.preparePropertyItemAddData(
    body,
    previousItem || {}
  )
  let propertyItem = {}
  let updatedItem = []
  if (size(previousItem)) {
    propertyItem = await updateAPropertyItem(
      {
        _id: previousItem._id
      },
      updateData,
      session
    )
    updatedItem = updateReturnData(propertyItem, propertyItemElement)
  } else {
    const createData = {
      ...updateData.$set,
      ...updateData.$push,
      partnerId,
      propertyId,
      createdBy: userId
    }
    propertyItem = await createAPropertyItem(createData, session)
    updatedItem = updateReturnData(propertyItem, propertyItemElement)
  }
  // After update of property item
  await updatePropertyItemWithMovingProtocol(
    body,
    {
      query: {},
      updateData
    },
    session
  )
  return updatedItem
}

export const updateNonMovingActivePropertyItem = async (
  params = {},
  session
) => {
  const { partnerId, propertyId, updateData } = params
  const activeLease = await contractHelper.getAContract({
    partnerId,
    propertyId,
    status: 'active'
  })
  if (size(activeLease)) {
    const updateAblePropertyItem = await propertyItemHelper.getLastPropertyItem(
      {
        partnerId,
        propertyId,
        contractId: activeLease._id,
        type: { $exists: true },
        moveInCompleted: { $exists: false },
        moveOutCompleted: { $exists: false },
        isEsigningInitiate: { $exists: false }
      }
    )
    if (size(updateAblePropertyItem)) {
      await updateAPropertyItem(
        {
          _id: updateAblePropertyItem._id
        },
        updateData,
        session
      )
    }
  }
}

export const updatePropertyItemFromPartnerApp = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'from',
    'propertyId',
    'propertyItemElement',
    'propertyItemId'
  ])
  const { body, session } = req
  const { from, inventory, partnerId, propertyId, propertyItemId, userId } =
    body
  const itemQuery = { _id: propertyItemId }
  if (from === 'moving_in') itemQuery.type = 'in'
  if (from === 'moving_out') itemQuery.type = 'out'
  const previousItem = await propertyItemHelper.getAPropertyItem(itemQuery)
  if (!previousItem) throw new CustomError(404, 'Property item not found')
  const { query, taskUpdateData, updateData } =
    propertyItemHelper.preparePropertyItemUpdateData(body, previousItem)
  if (!size(updateData)) throw new CustomError(400, 'Nothing to update')
  let newTaskId = ''
  if (size(taskUpdateData)) {
    const existFurniture = previousItem.inventory.furniture.find(
      (item) => item.id === inventory.furnitureId
    )
    if (!existFurniture) throw new CustomError(404, 'Furniture not found')
    if (existFurniture.taskId)
      taskUpdateData.createdTaskId = existFurniture.taskId
    const taskStatus = {
      ok: 'closed',
      issues: 'open',
      notApplicable: 'closed'
    }
    taskUpdateData.status = taskStatus[taskUpdateData.status]
    newTaskId = await taskService.addOrUpdateTaskForFurniture(
      {
        ...taskUpdateData,
        contractId: inventory?.contractId,
        existFurnitureInfo: existFurniture,
        furnitureId: existFurniture.id,
        partnerId,
        previousAssignType: existFurniture.responsibleForFixing,
        propertyId,
        userId
      },
      session
    )
  }
  if (newTaskId) {
    updateData['$set'] = {
      ...updateData.$set,
      'inventory.furniture.$.taskId': newTaskId
    }
  }
  const updatedPropertyItem = await updateAPropertyItem(
    query,
    updateData,
    session
  )
  await updatePropertyItemWithMovingProtocol(
    body,
    {
      query,
      updateData
    },
    session
  )
  return updatedPropertyItem
}

export const removePropertyItem = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'from',
    'itemId',
    'propertyId',
    'propertyItemElement',
    'propertyItemId'
  ])
  const { body, session } = req
  const { data, query } =
    await propertyItemHelper.preparePropertyItemRemoveData(body)
  await updateAPropertyItem(query, data, session)
  await updatePropertyItemWithMovingProtocol(
    body,
    { query: {}, updateData: data },
    session
  )
  return {
    result: true
  }
}

export const updatePropertyItemAndProcessESigning = async (req) => {
  const { body, session } = req
  const requiredFields = ['propertyItemId', 'data', 'callBackParams']
  appHelper.checkRequiredFields(requiredFields, body)

  const { propertyItemId, callBackParams = {} } = body

  if (!(propertyItemId && size(callBackParams)))
    throw new CustomError(400, 'Missing required field data in body')

  checkRequiredFields(
    [
      'callBackAction',
      'callBackDestination',
      'callBackEvent',
      'callBackPriority'
    ],
    callBackParams
  )

  const propertyItem = await propertyItemHelper.getAPropertyItem(
    {
      _id: propertyItemId
    },
    null,
    [{ path: 'partner', populate: ['owner'] }, 'contract']
  )

  if (!size(propertyItem)) {
    throw new CustomError(
      404,
      'PropertyItem not found. propertyItemId: ' + propertyItemId
    )
  }

  const { query, updatingData } =
    propertyItemHelper.prepareQueryAndDataToUpdatePropertyItemForLambda(body)

  if (!(size(query) && size(updatingData))) {
    throw new CustomError(
      404,
      'Query or updating data not found to update a property item'
    )
  }

  const updatedPropertyItem = await updateAPropertyItem(
    query,
    updatingData,
    session
  )
  if (!size(updatedPropertyItem))
    throw new CustomError(404, `Could not update property item`)

  console.log(
    `--- Updated Property Item for Id: ${updatedPropertyItem._id} ---`
  )
  const queueData = await propertyItemHelper.prepareMovingESignerCreationData(
    propertyItem,
    callBackParams
  )

  const [appQueueInfo] = await appQueueService.createAnAppQueue(
    queueData,
    session
  )

  if (!size(appQueueInfo))
    throw new CustomError(400, 'Could not create app queue collection data!')

  const { _id: queueId } = appQueueInfo || {}
  return { queueId }
}

const getSignersId = (oldPropertyItem, updatedPropertyItem) => {
  const currentSigningMeta = updatedPropertyItem?.movingSigningMeta
    ? updatedPropertyItem.movingSigningMeta
    : null
  const prevSigningMeta = oldPropertyItem?.movingSigningMeta
    ? oldPropertyItem?.movingSigningMeta
    : null

  const currentSigners =
    size(currentSigningMeta) && size(currentSigningMeta.signers)
      ? currentSigningMeta.signers
      : []

  const prevSigners =
    size(prevSigningMeta) && size(prevSigningMeta.signers)
      ? prevSigningMeta.signers
      : []

  let newSigners = []

  if (
    (!size(prevSigners) && !size(currentSigners)) ||
    size(prevSigners) === size(currentSigners)
  ) {
    return null
  } else
    newSigners = differenceBy(currentSigners, prevSigners, 'externalSignerId')

  return size(newSigners) && newSigners[0].externalSignerId
    ? newSigners[0].externalSignerId
    : ''
}

const createMovingInOutSignedLog = async (params, session) => {
  const { oldPropertyItem, updatedPropertyItem, signer } = params

  if (!(size(oldPropertyItem) && size(updatedPropertyItem) && signer))
    return false

  const { _id, contractId, partnerId, propertyId, type } = updatedPropertyItem

  const logData = {
    context: 'property',
    contractId,
    movingId: _id,
    partnerId,
    propertyId,
    visibility: ['property'],
    isChangeLog: false
  }

  let action = ''

  if (signer === 'tenant') {
    if (type === 'out') action = 'tenant_signed_moving_out'
    else action = 'tenant_signed_moving_in'

    const signersId = getSignersId(oldPropertyItem, updatedPropertyItem)
    logData.tenantId = signersId || undefined
  } else if (signer === 'landlord') {
    if (type === 'out') action = 'landlord_signed_moving_out'
    else action = 'landlord_signed_moving_in'

    const contractInfo = await contractHelper.getAContract({ _id: contractId })
    logData.accountId = contractInfo?.accountId || undefined
  } else if (signer === 'agent') {
    if (type === 'out') action = 'agent_signed_moving_out'
    else action = 'agent_signed_moving_in'

    const contractInfo = await contractHelper.getAContract({ _id: contractId })
    logData.agentId = contractInfo?.agentId || undefined
  }
  logData.action = action

  const log = await logService.createLog(logData, session)
  size(log)
    ? console.log(`=== Created log for ${log.action}. logId: ${log._id} ===`)
    : console.log(`=== Unable to create log for ${log.action} ===`)

  return true
}
export const createPartnerUsagesAndLogForNewlySignedUsers = async (
  oldPropertyItem,
  updatedPropertyItem,
  session
) => {
  console.log(' === creating PartnerUsages And Log For Newly Signed Users ===')
  if (!(size(oldPropertyItem) && size(updatedPropertyItem))) return false

  const { contractId, partnerId } = updatedPropertyItem

  if (!partnerId)
    throw new CustomError(404, 'PartnerId does not exist in propertyItem')

  const partner = await partnerHelper.getAPartner({ _id: partnerId })

  if (!size(partner))
    throw new CustomError(404, 'Partner not found for propertyItem')

  const { accountType } = partner
  const isBrokerPartner = accountType === 'broker'

  const contract = contractId
    ? await contractHelper.getAContract({ _id: contractId })
    : {}

  const { branchId } = contract || {}
  const partnerUsagesCreationData = { partnerId, type: 'esign', total: 1 }
  if (branchId) partnerUsagesCreationData.branchId = branchId

  if (
    appHelper.isAnyTenantSigned(
      oldPropertyItem.tenantSigningStatus,
      updatedPropertyItem.tenantSigningStatus
    )
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for tenantSigned. partnerUsageId: ${partnerUsageId} ===`
    )
    // Create tenantSigned Log
    await createMovingInOutSignedLog(
      {
        oldPropertyItem,
        updatedPropertyItem,
        signer: 'tenant'
      },
      session
    )
  }

  if (
    size(oldPropertyItem.landlordSigningStatus) &&
    oldPropertyItem.landlordSigningStatus.signed !== true &&
    size(updatedPropertyItem.landlordSigningStatus) &&
    updatedPropertyItem.landlordSigningStatus.signed === true &&
    !isBrokerPartner
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for landlordSigned. partnerUsageId: ${partnerUsageId} ===`
    )
    // Create landlordSigned Log
    await createMovingInOutSignedLog(
      {
        oldPropertyItem,
        updatedPropertyItem,
        signer: 'landlord'
      },
      session
    )
  }

  if (
    size(oldPropertyItem.agentSigningStatus) &&
    oldPropertyItem.agentSigningStatus.signed !== true &&
    size(updatedPropertyItem.agentSigningStatus) &&
    updatedPropertyItem.agentSigningStatus.signed === true &&
    isBrokerPartner
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for agentSigned. partnerUsageId: ${partnerUsageId} ===`
    )
    // Create agentSigned Log
    await createMovingInOutSignedLog(
      {
        oldPropertyItem,
        updatedPropertyItem,
        signer: 'agent'
      },
      session
    )
  }
}

export const updateAPropertyItemForLambda = async (req) => {
  const { body, session } = req
  const requiredFields = ['propertyItemId', 'data']
  appHelper.checkRequiredFields(requiredFields, body)

  const { propertyItemId, data } = body

  if (!(propertyItemId && size(data)))
    throw new CustomError(400, 'Missing required fields data in body')

  const propertyItem = await propertyItemHelper.getAPropertyItem({
    _id: propertyItemId
  })

  if (!size(propertyItem)) {
    throw new CustomError(
      404,
      'PropertyItem not found. propertyItemId: ' + propertyItemId
    )
  }

  const updatingData =
    propertyItemHelper.preparingUpdatingDataOfPropertyItemForLambda(data)

  if (!size(updatingData)) throw new CustomError(404, 'Updating data not found')

  const updatedPropertyItem = await updateAPropertyItem(
    { _id: propertyItemId },
    updatingData,
    session
  )
  console.log(' == Updated propertyItem ===')
  if (size(updatedPropertyItem)) {
    await createPartnerUsagesAndLogForNewlySignedUsers(
      propertyItem.toObject(),
      updatedPropertyItem.toObject(),
      session
    )
  }

  return updatedPropertyItem
}

export const updatePropertyItemWithMovingProtocol = async (
  params,
  data = {},
  session
) => {
  const { query = {}, updateData = {} } = data
  const propertyItem = await propertyItemHelper.getUpdatablePropertyItem(params)
  if (size(propertyItem)) {
    delete query._id
    const updateQuery = {
      _id: propertyItem._id,
      ...query
    }
    await updateAPropertyItem(updateQuery, updateData, session)
  }
}

export const goToMovingProtocol = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'contractId',
    'propertyId',
    'type'
  ])
  const { body, session } = req
  const partnerSettingsInfo =
    (await partnerSettingHelper.getSettingByPartnerId(body.partnerId)) || {}
  const movingInOutProtocol =
    partnerSettingsInfo?.propertySettings?.movingInOutProtocol || false
  if (!movingInOutProtocol)
    throw new CustomError(400, 'Moving in out protocol is not enabled')

  const { movingProtocolInfo, newPropertyItemData } =
    await propertyItemHelper.prepareDataForMovingProtocol(body, session)
  let movingProtocolId = ''
  if (size(movingProtocolInfo)) {
    movingProtocolId = movingProtocolInfo._id
  } else {
    const propertyItemWithMovingProtocol = await createAPropertyItem(
      newPropertyItemData,
      session
    )
    await addPropertyItemRoom(body, propertyItemWithMovingProtocol, session)
    movingProtocolId = propertyItemWithMovingProtocol._id
  }
  const completedMovingInInfo = await propertyItemHelper.getLastPropertyItem({
    partnerId: body.partnerId,
    propertyId: body.propertyId,
    contractId: body.contractId,
    type: 'in',
    isEsigningInitiate: true,
    moveInCompleted: true
  })
  return {
    movingProtocolId,
    completedMovingInId: completedMovingInInfo._id
  }
}

export const createOrGetPreviousPropertyItem = async (params, session) => {
  const { partnerId, propertyId } = params

  const query = {
    partnerId,
    propertyId,
    contractId: { $exists: false },
    type: { $exists: false },
    isEsigningInitiate: { $exists: false },
    moveInCompleted: { $exists: false },
    moveOutCompleted: { $exists: false }
  }
  let existPropertyItem = await propertyItemHelper.getAPropertyItem(query)
  if (!existPropertyItem) {
    existPropertyItem = await createAPropertyItem(
      {
        partnerId,
        propertyId
      },
      session
    )
  }
  return existPropertyItem
}

export const addPropertyItemRoom = async (params, propertyItem, session) => {
  const roomsData = await propertyItemHelper.prepareDataForAddPropertyRooms(
    params,
    propertyItem,
    session
  )
  if (size(roomsData)) {
    await propertyRoomService.createMultipleRooms(roomsData, session)
  }
}

export const resetMovingProtocol = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['propertyId'])
  const { body, session } = req
  const {
    fileIds,
    propertyItemQuery,
    propertyItemsUpdateData,
    propertyRoomQuery,
    propertyRoomsUpdateData,
    taskIds
  } = await propertyItemHelper.prepareDataForResetMovingProtocol(body)
  if (size(propertyItemQuery)) {
    await updatePropertyItems(
      propertyItemQuery,
      propertyItemsUpdateData,
      session
    )
  }
  if (size(propertyRoomQuery) && size(propertyRoomsUpdateData)) {
    await propertyRoomService.updatePropertyRooms(
      propertyRoomQuery,
      propertyRoomsUpdateData,
      session
    )
  }
  if (size(fileIds)) {
    const selectors = ['type', 'partnerId', 'context', 'directive', 'name']
    const deletableFiles = await fileHelper.getFilesWithSelectedFields(
      { _id: { $in: fileIds } },
      selectors
    )
    await appQueueService.createAppQueueForRemoveFilesFromS3(
      deletableFiles,
      session
    )
    await fileService.deleteMultipleFile(fileIds, session)
  }
  if (size(taskIds)) {
    await taskService.updateMultipleTasks(
      { _id: { $in: taskIds } },
      { $set: { status: 'closed' } },
      session
    )
  }
  return {
    result: true
  }
}

export const updatePropertyItems = async (query, data, session) => {
  const propertyItems = await PropertyItemCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  if (propertyItems.nModified > 0) {
    return propertyItems
  }
}

export const initiateMovingInOutProtocol = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'contractId',
    'eSigningPdfContent',
    'movingId',
    'movingType',
    'propertyId',
    'signatureMechanism'
  ])

  const { body = {}, session } = req
  await propertyItemHelper.validateInitiateMovingInOutProtocolData(
    body,
    session
  )
  const { movingId, signatureMechanism } = body

  await updateAPropertyItem(
    { _id: movingId },
    {
      $set: {
        isEsigningInitiate: true,
        esigningInitiatedAt: new Date(),
        signatureMechanism
      }
    },
    session
  )

  const appQueuesData =
    propertyItemHelper.prepareAppQueueDataForMovingInOutProtocol(body)
  await appQueueService.createAnAppQueue(appQueuesData, session)
  return { result: true }
}

export const cancelMovingInOutProtocol = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'type'])
  const { body = {}, session } = req
  const { contractId, partnerId, type, userId } = body
  const query = {
    partnerId,
    contractId,
    type,
    isEsigningInitiate: true
  }
  if (type === 'in') {
    query.moveInCompleted = { $exists: false }
  } else {
    query.moveOutCompleted = { $exists: false }
  }
  const movingInfo = await propertyItemHelper.getAPropertyItem(query, session, [
    'contract'
  ])
  if (!size(movingInfo))
    throw new CustomError(
      400,
      `Cancel moving ${type} protocol for this property is not possible`
    )
  const updateData = propertyItemHelper.prepareMovingInOutCancelData(type)
  const updatedMovingInfo = await updateAPropertyItem(
    { _id: movingInfo._id },
    { $unset: updateData },
    session
  )
  await removeMovingInOrOutPdfFile(updatedMovingInfo, userId, session)
  await createLogForCancelMovingInOut(movingInfo, userId, session)
  return {
    result: true
  }
}

const createLogForCancelMovingInOut = async (
  movingInfo = {},
  userId,
  session
) => {
  const contract = movingInfo.contract
  const logData = {
    partnerId: movingInfo.partnerId,
    context: 'property',
    action: 'cancel_move_' + movingInfo.type,
    accountId: contract?.accountId,
    propertyId: contract?.propertyId,
    collectionId: contract?._id,
    tenantId: contract?.rentalMeta?.tenantId,
    contractId: contract?._id,
    visibility: ['property', 'account', 'tenant'],
    meta: [{ field: 'leaseSerial', value: contract?.leaseSerial }],
    createdBy: userId
  }
  await logService.createLog(logData, session)
}

export const removeMovingInOrOutPdfFile = async (
  movingInfo,
  userId,
  session
) => {
  const query = {
    movingId: movingInfo._id
  }
  if (movingInfo.type === 'in') query.type = 'esigning_moving_in_pdf'
  else query.type = 'esigning_moving_out_pdf'
  const file = await fileHelper.getAFile(query)
  if (size(file)) {
    const deletableFiles = []
    const selectedField = pick(file, [
      '_id',
      'type',
      'partnerId',
      'context',
      'directive',
      'name'
    ])
    deletableFiles.push(selectedField)
    const logData = fileHelper.prepareLogDataForRemoveFile(file, { userId })
    await appQueueService.createAppQueueForRemoveFilesFromS3(
      deletableFiles,
      session
    )
    await fileService.deleteAFile(file._id, session)
    await logService.createLog(logData, session)
  }
}
