import { size } from 'lodash'
import { CustomError } from '../common'
import { ImportCollection } from '../models'
import {
  appHelper,
  branchHelper,
  importHelper,
  listingHelper,
  partnerHelper,
  propertyItemHelper,
  propertyRoomHelper,
  settingHelper,
  userHelper
} from '../helpers'
import {
  accountService,
  addonService,
  appQueueService,
  appRoleService,
  branchService,
  propertyItemService,
  propertyRoomService,
  propertyService,
  tenantService,
  userService
} from '../services'

export const addAnImport = async (data, session) => {
  const importInfo = await ImportCollection.create([data], {
    session
  })
  return importInfo
}

export const updateAnImport = async (queryData, updatedData, session) => {
  const importInfo = await ImportCollection.findOneAndUpdate(
    queryData,
    updatedData,
    {
      new: true,
      runValidators: true,
      session
    }
  )
  return importInfo
}

const createABranch = async (data, session) => {
  try {
    const { adminId, branchSerialId, name, partnerId, userId } = data
    const params = {
      body: {
        adminId,
        branchSerialId,
        name,
        partnerId
      },
      session,
      user: {
        userId
      }
    }
    const [branchInfo] = await branchService.createBranch(params)
    const { _id: collectionId } = branchInfo || {}
    return { success: true, collectionId }
  } catch (err) {
    console.log(
      '====> Checking error when creating branch for excel import, error:',
      err
    )
    return { success: false, errorMessage: err.message }
  }
}

const createAPartnerUser = async (data, session) => {
  try {
    const { name, email, roles, partnerId, branchId, partnerEmployeeId } = data
    const existingUser = await userHelper.getUserByEmail(email, session)

    let invitedUserId
    if (size(existingUser)) {
      invitedUserId = existingUser._id
    } else {
      const userObject = {
        emails: [{ address: email.toLowerCase(), verified: false }],
        profile: { name }
      }
      const [createdUser] = await userService.createAnUser(userObject, session)
      invitedUserId = createdUser._id
    }

    if (invitedUserId) {
      const partnersData = {
        partnerId,
        type: 'user',
        status: 'invited'
      }

      if (partnerEmployeeId) {
        const params = { partnerId, partnerEmployeeId }
        const isExistingEmployeeId = await userHelper.existingEmployeeId(
          params,
          session
        )

        if (!isExistingEmployeeId) {
          partnersData.employeeId = partnerEmployeeId
        }
      }

      const userData = { $addToSet: { partners: partnersData } }
      await userService.updateAnUser({ _id: invitedUserId }, userData, session) // Update user for partner user

      if (size(roles)) {
        // Add user in app-roles collection
        const roleData = { $addToSet: { users: invitedUserId } }
        for (const role of roles) {
          await appRoleService.updateARole(
            { type: role, partnerId },
            roleData,
            session
          )
        }
      }

      if (branchId) {
        // Add agent in branches collection, If branchId is exists
        const branchData = { $addToSet: { agents: invitedUserId } }
        await branchService.updateABranch(
          { _id: branchId },
          branchData,
          session
        )
      }

      return { success: true, collectionId: invitedUserId }
    }
  } catch (err) {
    console.log(
      '====> Checking error when creating partner user for excel import, error:',
      err
    )
    return { success: false, errorMessage: err.message }
  }
}

const createAnAccount = async (data, partnerInfo, session) => {
  try {
    const { userId, partnerId, type } = data
    if (partnerInfo.accountType === 'direct' && type !== 'organization') {
      return {
        success: false,
        errorMessage: 'Type should be Organization for direct partner'
      }
    }
    const params = {
      body: data,
      session,
      user: { userId, partnerId }
    }
    const accountInfo = (await accountService.createAccount(params)) || {}
    const { _id: collectionId } = accountInfo

    if (collectionId) {
      return { success: true, collectionId }
    }

    return { success: false, errorMessage: 'Something error happened!' }
  } catch (err) {
    console.log(
      '====> Checking error when creating account for excel import, error:',
      err
    )
    return { success: false, errorMessage: err.message }
  }
}

const createAPropertyAndRelatedData = async (
  importData,
  preparedData,
  session
) => {
  try {
    const { importRefId, partnerId } = importData

    // Getting accountId from import collection data
    const { collectionId: accountId } =
      (await importHelper.getAnImport(
        {
          importRefId,
          partnerId,
          collectionName: 'account',
          'jsonData.Account No': preparedData?.accountNo
        },
        session
      )) || {}

    // Setting accountId or return false as property can not be created without account
    if (accountId) {
      preparedData.accountId = accountId
    } else {
      return { success: false, errorMessage: 'Account is not created!' }
    }

    // Getting listingTypeId and propertyTypeId
    const { propertyType, type } = preparedData
    const setting = await settingHelper.getSettingInfo()
    const listingTypeName = type?.toLowerCase()?.replace(' ', '_')
    const listingTypeId = listingHelper.getListingTypeIdByName(
      listingTypeName,
      setting
    )
    const propertyTypeName = propertyType?.toLowerCase()?.replace(' ', '_')
    const propertyTypeId = listingHelper.getPropertyTypeIdByName(
      propertyTypeName,
      setting
    )

    // Setting listingTypeId and propertyTypeId
    if (listingTypeId) {
      preparedData.listingTypeId = listingTypeId
    } else {
      return { success: false, errorMessage: 'Listing type is not found!' }
    }
    if (propertyTypeId) {
      preparedData.propertyTypeId = propertyTypeId
    } else {
      return { success: false, errorMessage: 'Property type is not found!' }
    }

    // Creating property
    const params = {
      body: preparedData,
      user: { userId: preparedData.userId, partnerId: preparedData.partnerId },
      session
    }
    const { _id: propertyId } =
      (await propertyService.addProperty(params)) || {}

    // Setting propertyId
    importData.propertyId = propertyId

    // Preparing and creating property rooms
    const propertyRoomsData = await propertyRoomHelper.prepareRoomsList(
      importData,
      preparedData,
      session
    )
    const createdRoomsData = await propertyRoomService.createMultipleRooms(
      propertyRoomsData,
      session
    )
    const arePropertyRoomsCreated =
      size(propertyRoomsData) === size(createdRoomsData)

    // Preparing and creating property item data
    const furniture = await propertyItemHelper.prepareFurnitureList(
      importData,
      preparedData,
      session
    )
    const keysList = propertyItemHelper.prepareKeysList(preparedData)
    const meters = propertyItemHelper.prepareMetersList(preparedData)
    const propertyItemData = {
      partnerId,
      propertyId,
      createdBy: preparedData?.userId,
      keys: {
        keysList,
        files: []
      },
      inventory: {
        isFurnished: true,
        furniture,
        files: []
      },
      meterReading: {
        meters,
        files: []
      }
    }
    const isPropertyItemCreated = await propertyItemService.createAPropertyItem(
      propertyItemData,
      session
    )

    if (propertyId && arePropertyRoomsCreated && isPropertyItemCreated) {
      return { success: true, collectionId: propertyId }
    }

    return { success: false, errorMessage: 'Something error happened!' }
  } catch (err) {
    console.log(
      '====> Checking error when creating property for excel import, error:',
      err
    )
    return { success: false, errorMessage: err.message }
  }
}

const createATenant = async (importData, preparedData, session) => {
  try {
    const { importRefId, partnerId } = importData

    // Getting propertyId from import collection data
    const { collectionId: propertyId } =
      (await importHelper.getAnImport(
        {
          importRefId,
          partnerId,
          collectionName: 'property',
          'jsonData.Property No': preparedData?.propertyNo
        },
        session
      )) || {}

    // Setting propertyId or return false as tenant can not be created without property
    if (propertyId) {
      preparedData.propertyId = propertyId
    } else {
      return { success: false, errorMessage: 'Property is not created!' }
    }

    const params = {
      body: {
        data: preparedData
      },
      session,
      user: { userId: preparedData.userId, partnerId: preparedData.partnerId }
    }
    const { _id: collectionId } =
      (await tenantService.createTenant(params)) || {}

    if (collectionId) {
      return { success: true, collectionId }
    }

    return { success: false, errorMessage: 'Something error happened!' }
  } catch (err) {
    console.log(
      '====> Checking error when creating tenant for excel import, error:',
      err
    )
    return { success: false, errorMessage: err.message }
  }
}

const createAnAddon = async (data, session) => {
  try {
    const { name, type, partnerId } = data
    const [{ _id: collectionId }] = (await addonService.createAnAddon(
      { name, type, partnerId },
      session
    )) || [{}]

    if (collectionId) {
      return { success: true, collectionId }
    }

    return { success: false, errorMessage: 'Something error happened!' }
  } catch (err) {
    console.log(
      '====> Checking error when creating addon for excel import, error:',
      err
    )
    return { success: false, errorMessage: err.message }
  }
}

export const addAnExcelImport = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user || {}
  appHelper.checkUserId(userId)

  appHelper.checkRequiredFields(['fileKey', 'fileBucket', 'partnerId'], body)
  const {
    collectionName = '',
    fileKey = '',
    fileBucket = '',
    importRefId = '',
    isImportingFromError = false,
    partnerId = ''
  } = body || {}

  if (!(fileKey && fileBucket && partnerId))
    throw new CustomError(400, 'Invalid input data')

  const mainImportData = importRefId
    ? await importHelper.getAnImport({ _id: importRefId }, session)
    : {}
  if (!size(mainImportData) && importRefId && collectionName)
    throw new CustomError(404, 'Could not found root import data')

  const importData = {
    createdBy: userId,
    fileKey,
    fileBucket,
    partnerId
  }
  if (collectionName) importData.collectionName = collectionName
  if (importRefId) importData.importRefId = importRefId
  if (isImportingFromError)
    importData.isImportingFromError = isImportingFromError
  const [createdImportData] = (await addAnImport(importData, session)) || []

  if (size(createdImportData)) {
    const appQueueData =
      getAppQueueDataForExcelImport(
        size(mainImportData) ? mainImportData : createdImportData,
        size(mainImportData) ? createdImportData : {}
      ) || {}
    const [createdAppQueue] = size(appQueueData)
      ? await appQueueService.createAnAppQueue(appQueueData, session)
      : {}

    if (!size(createdAppQueue))
      throw new CustomError(400, 'Could not create app queue')
  }
  const populatedImportData = await importHelper.getAnImport(
    { _id: createdImportData._id },
    session
  )
  return populatedImportData
}

export const addImportAndCollectionData = async (req) => {
  const { body, session } = req
  const { userId, partnerId, importRefId, collectionName, preparedDataArr } =
    body
  const requiredFields = [
    'userId',
    'partnerId',
    'importRefId',
    'collectionName',
    'preparedDataArr'
  ]
  appHelper.checkRequiredFields(requiredFields, body)
  if (!size(preparedDataArr)) {
    throw new CustomError(400, `No data found in preparedDataArr`)
  }
  const partnerInfo = await partnerHelper.getPartnerById(partnerId, session)

  const results = []
  for (const preparedData of preparedDataArr) {
    preparedData.userId = userId
    preparedData.createdBy = userId
    // If branch info found by branch serial Id then adding branchId to preparedData
    const { branchSerialId, partnerEmployeeId } = preparedData
    if (branchSerialId) {
      const { _id: branchId } =
        (await branchHelper.getABranch(
          {
            partnerId,
            branchSerialId
          },
          session
        )) || {}

      if (branchId) {
        preparedData.branchId = branchId
      }
    }

    if (partnerEmployeeId) {
      const { _id: agentId } =
        (await userHelper.getAnUser(
          { 'partners.employeeId': partnerEmployeeId },
          session
        )) || {}

      if (agentId) {
        preparedData.agentId = agentId
      }
    }

    let response = {}
    if (collectionName === 'branch') {
      preparedData.adminId = partnerInfo?.ownerId
      response = await createABranch(preparedData, session)
    } else if (collectionName === 'user') {
      response = await createAPartnerUser(preparedData, session)
    } else if (collectionName === 'account') {
      response = await createAnAccount(preparedData, partnerInfo, session)
    } else if (collectionName === 'property') {
      response = await createAPropertyAndRelatedData(
        body,
        preparedData,
        session
      )
    } else if (collectionName === 'tenant') {
      response = await createATenant(body, preparedData, session)
    } else if (collectionName === 'addon') {
      response = await createAnAddon(preparedData, session)
    }

    const { jsonData } = preparedData
    const { importId } = jsonData
    const importData = { jsonData }
    const { success, errorMessage, collectionId } = response

    if (!success && errorMessage) {
      importData.hasError = true
      importData.errorMessage = response.errorMessage
    } else if (success && collectionId) {
      importData.hasError = false
      importData.collectionId = response.collectionId
    }
    importData.createdBy = userId

    if (importId) {
      const isImportUpdated = await updateAnImport(
        { _id: importId },
        { $set: importData },
        session
      )

      results.push(isImportUpdated)
    } else {
      const importCreationData = {
        partnerId,
        importRefId,
        collectionName,
        ...importData
      }
      const previousImportData = await importHelper.getAnImport(
        { importRefId, jsonData: importCreationData.jsonData },
        session
      )
      if (size(previousImportData)) {
        results.push(previousImportData)
      } else {
        const [isImportCreated] = await addAnImport(importCreationData, session)
        results.push(isImportCreated)
      }
    }
  }

  return results
}

const getAppQueueDataForExcelImport = (
  importData = {},
  errorImportData = {}
) => {
  const {
    _id: importRefId,
    createdBy,
    fileKey = '',
    fileBucket = '',
    partnerId = ''
  } = importData || {}
  const {
    collectionName: errorCollectionName = '',
    fileKey: errorFileKey = '',
    fileBucket: errorFileBucket = ''
  } = errorImportData || {}

  const params = {
    collectionName: 'branch',
    dataToSkip: 0,
    fileKey,
    fileBucket,
    importRefId,
    partnerId,
    userId: createdBy
  }
  if (errorCollectionName && errorFileKey && errorFileBucket) {
    params.collectionName = errorCollectionName
    params.errorFileKey = errorFileKey
    params.errorFileBucket = errorFileBucket
    params.isImportingFromError = true
  } else params.isFirstAppQueue = true

  return {
    event: 'import_collection_data_from_excel',
    action: 'convert_excel_to_json',
    destination: 'excel-manager',
    params,
    priority: 'regular'
  }
}

export const updateImport = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['importId'], body)
  const { importId } = body
  const updateData = importHelper.prepareImportUpdateData(body)
  const updatedImport = await updateAnImport(
    { _id: importId },
    {
      $set: updateData
    }
  )
  return updatedImport
}
