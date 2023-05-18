import { assign, size } from 'lodash'

import {
  accountHelper,
  appHelper,
  appRoleHelper,
  branchHelper,
  contractHelper,
  listingHelper,
  organizationHelper,
  partnerHelper,
  propertyHelper,
  settingHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  contractService,
  conversationService,
  listingService,
  logService
} from '../services'
import { CustomError } from '../common/error'
import { ListingCollection } from '../models'

export const addProperty = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.compactObject(body)
  body.partnerId = partnerId
  const dataNeedTobeMerge =
    await appHelper.validateSelfServicePartnerRequestAndUpdateBody(
      user,
      session
    )
  assign(body, dataNeedTobeMerge)
  const setting = await settingHelper.getSettingInfo()
  propertyHelper.validatePropertyAddData(body, setting)
  const params = { data: body, user, setting }
  const data = await propertyHelper.preparePropertyAddData(params, session)
  const [property] = await listingService.createListing(data, session)
  if (size(property)) {
    await listingService.initAfterInsertProcesses(property, session)
    if (await propertyHelper.isAddContract(property)) {
      await contractService.addContractForDirectPartner(property, session)
    }
  }
  return await prepareAddPropertyReturnData(property)
}

const prepareAddPropertyReturnData = async (property) => {
  const agent = (await userHelper.getUserById(property.agentId)) || {}
  if (size(agent)) agent.avatarKey = userHelper.getAvatar(agent)
  const branch =
    (await branchHelper.getABranch({
      _id: property.branchId
    })) || {}
  const account =
    (await accountHelper.getAnAccount({
      _id: property.accountId
    })) || {}
  const cdn = appHelper.getCDNDomain()
  if (size(account)) {
    if (account.type === 'person') {
      const personInfo = (await userHelper.getUserById(account.personId)) || {}
      account.avatarKey = cdn + '/' + personInfo.profile?.avatarKey
    } else {
      const organizationInfo =
        (await organizationHelper.getAnOrganization({
          _id: account.organizationId
        })) || {}
      account.avatarKey =
        cdn +
        '/partner_logo/' +
        account.partnerId +
        '/accounts/' +
        organizationInfo.image
    }
  }
  const propertyObj = {
    _id: property._id,
    imageUrl: cdn + '/assets/default-image/property-primary.png',
    location: property.location,
    serial: property.serial,
    propertyTypeId: property.propertyTypeId,
    listingTypeId: property.listingTypeId,
    apartmentId: property.apartmentId,
    listed: property.listed,
    floor: property.floor,
    propertyStatus: property.propertyStatus,
    hasActiveLease: property.hasActiveLease,
    hasUpcomingLease: property.hasUpcomingLease,
    hasInProgressLease: property.hasInProgressLease,
    isSoonEnding: false,
    isTerminated: false,
    monthlyRentAmount: property.monthlyRentAmount,
    depositAmount: property.depositAmount,
    availabilityStartDate: property.availabilityStartDate,
    availabilityEndDate: property.availabilityEndDate,
    minimumStay: property.minimumStay,
    placeSize: property.placeSize,
    noOfAvailableBedrooms: property.noOfAvailableBedrooms,
    noOfBedrooms: property.noOfBedrooms,
    totalOverDue: 0,
    totalDue: 0,
    agentInfo: {
      _id: agent._id,
      name: agent.profile?.name,
      avatarKey: agent.avatarKey
    },
    accountInfo: {
      _id: account._id,
      name: account.name,
      avatarKey: account.avatarKey
    },
    branchInfo: {
      _id: branch._id,
      name: branch.name
    },
    createdAt: property.createdAt
  }
  return propertyObj
}

export const updateProperty = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['propertyId'], body)
  const { partnerId, userId } = user
  body.partnerId = partnerId
  const { propertyId } = body
  appHelper.validateId({ propertyId })
  await appHelper.validateSelfServicePartnerRequestAndUpdateBody(user, session)
  const query = { _id: propertyId, partnerId }
  const prevProperty = await listingHelper.getAListing(query)
  if (!size(prevProperty)) throw new CustomError(404, 'Property not found')
  const setting = await settingHelper.getSettingInfo()
  await propertyHelper.validatePropertyUpdateData(body, setting)
  const data = await propertyHelper.preparePropertyUpdateData(
    body,
    setting,
    prevProperty
  )
  if (!size(data))
    throw new CustomError(404, 'No data found to update property')
  const property = await listingService.updateAListing(query, data, session)
  if (size(property)) {
    await listingService.initAfterUpdateProcesses({
      previousListing: prevProperty,
      session,
      updatedListing: property,
      userId
    })
  }
  return await prepareUpdatePropertyReturnData(property)
}

export const updateMultipleProperty = async (queryData, inputData) => {
  await ListingCollection.updateMany(queryData, inputData)
}

const prepareUpdatePropertyReturnData = async (property) => {
  const { propertyStatus } = property
  const activeContract = await contractHelper.getAContract({
    propertyId: property._id,
    hasRentalContract: true,
    status: 'active'
  })
  return {
    _id: property._id,
    apartmentId: property.apartmentId,
    bnr: property.bnr,
    depositAmount:
      propertyStatus === 'active' && size(activeContract)
        ? activeContract.rentalMeta?.depositAmount
        : property.depositAmount,
    floor: property.floor,
    gnr: property.gnr,
    groupId: property.groupId,
    listingTypeId: property.listingTypeId,
    location: property.location,
    monthlyRentAmount:
      propertyStatus === 'active' && size(activeContract)
        ? activeContract.rentalMeta?.monthlyRentAmount
        : property.monthlyRentAmount,
    noOfAvailableBedrooms: property.noOfAvailableBedrooms,
    noOfBedrooms: property.noOfBedrooms,
    placeSize: property.placeSize,
    propertyTypeId: property.propertyTypeId,
    serial: property.serial,
    snr: property.snr
  }
}

export const shareAtFinn = async (req) => {
  const { body, session, user = {} } = req
  const { roles, userId } = user
  let { partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['propertyId', 'type'], body)
  if (!partnerId && appHelper.isAppAdmin(roles)) {
    appHelper.checkRequiredFields(['partnerId'], body)
    partnerId = body.partnerId
  }
  appHelper.validateId({ partnerId })
  const { propertyId, shareWithWarning } = body
  let { type } = body
  appHelper.validateId({ propertyId })

  const query = { _id: propertyId, partnerId }
  const partner = await partnerHelper.getAPartner({ _id: partnerId }, session)
  let property = await listingHelper.getAListing(query, session)
  propertyHelper.validateDataForSharingAtFinn(partner, property)
  await propertyHelper.validatePendingFinnRequest(
    { ...body, checkProcessFlow: true },
    session
  )
  const { availabilityEndDate, finn, images = [], noOfBedrooms } = property
  let isShareWithWarning = false

  if (size(property?.description) >= 50000) {
    throw new CustomError(
      400,
      'Property description cannot be longer than 50000 characters'
    )
  }

  if (shareWithWarning) {
    isShareWithWarning = true
  } else if (noOfBedrooms && availabilityEndDate && images.length <= 50) {
    isShareWithWarning = true
  } else {
    isShareWithWarning = false
  }
  if (!isShareWithWarning) {
    const missingData = {}
    if (!availabilityEndDate) missingData.availabilityEndDate = true
    if (!noOfBedrooms) missingData.noOfBedrooms = true
    if (size(images) > 50) missingData.moreThan50Images = true
    if (size(missingData)) {
      missingData.type = type
      return { _id: propertyId, partnerId, missingData }
    }
  }

  const updateSet = {
    'finn.finnShareAt': new Date(),
    'finn.requestedAt': new Date()
  }
  const updateUnset = {
    'finn.finnArchivedAt': 1,
    'finn.isArchiving': 1,
    'finn.finnErrorRequest': 1,
    'finn.disableFromFinn': 1
  }
  const updateData = {}

  if (!finn) type = 'firstAd'
  if (type === 'firstAd' || type === 'update') {
    updateSet['finn.isPublishing'] = true
  } else if (type === 'republish') {
    updateSet['finn.isRePublishing'] = true
  }
  updateData['$set'] = updateSet
  updateData['$unset'] = updateUnset

  property = await listingService.updateAListing(query, updateData, session)

  if (type !== 'republish') {
    const queueData = {
      event: 'share_or_archive_finn_listing',
      action: 'handle_finn_listing',
      params: {
        propertyId,
        partnerId,
        userId,
        type,
        processType: 'share' // ["share", "remove"]
      },
      destination: 'listing',
      priority: 'immediate'
    }
    await appQueueService.insertInQueue(queueData, session)
  }

  return property
}

export const removeFromFinn = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['propertyId'], body)
  const { propertyId, type } = body
  const { partnerId, userId } = user
  await propertyHelper.validatePendingFinnRequest(
    { ...body, partnerId },
    session
  )
  appHelper.validateId({ propertyId })

  if (type === 'firstAd' || type === 'update') {
    return { success: true }
  }
  const listingInfo = await listingHelper.getAListing({
    _id: propertyId
  })
  const isArchived =
    listingInfo && listingInfo.finn && listingInfo.finn.isShareAtFinn
      ? false
      : true
  let isUpdateListing = false

  let updateData = {}
  if (!isArchived) {
    updateData = {
      $set: {
        'finn.finnArchivedAt': new Date(),
        'finn.updateType': type,
        'finn.requestedAt': new Date(),
        'finn.isArchiving': true
      },
      $unset: {
        'finn.isPublishing': 1,
        'finn.isRePublishing': 1
      }
    }
  } else {
    updateData = { $unset: { 'finn.finnArchivedAt': 1 } }
  }
  isUpdateListing = await listingService.updateAListing(
    {
      _id: propertyId
    },
    updateData,
    session
  )
  if (size(isUpdateListing)) {
    const appQueueData = {
      event: 'share_or_archive_finn_listing',
      action: 'handle_finn_listing',
      destination: 'listing',
      params: {
        propertyId,
        partnerId,
        userId,
        type: !isArchived ? type : 'republish',
        processType: !isArchived ? 'remove' : 'share',
        processFlow: type === 'republish' ? 'archive_and_republish' : undefined
      },
      priority: 'immediate'
    }
    await appQueueService.insertInQueue(appQueueData, session)
  }
  return {
    success: true
  }
}

export const cancelListingFinn = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['propertyId'], body)
  const { propertyId } = body
  const { partnerId } = user
  appHelper.validateId({ propertyId })
  const listingInfo = await listingHelper.getAListing(
    {
      _id: propertyId
    },
    undefined,
    ['partnerSetting']
  )
  if (size(listingInfo)) {
    const {
      isPublishing = false,
      isRePublishing = false,
      isArchiving = false,
      requestedAt
    } = listingInfo.finn || {}
    const partnerSetting = listingInfo?.partnerSetting
    if (!(isPublishing || isRePublishing || isArchiving)) {
      throw new CustomError(
        400,
        'No finn process running at this moment for this property'
      )
    }
    const requestDiff = (
      await appHelper.getActualDate(partnerSetting, true, new Date())
    ).diff(
      await appHelper.getActualDate(partnerSetting, true, requestedAt),
      'minute'
    )
    if (requestDiff <= 10) {
      throw new CustomError(
        400,
        `Finn has been processing for ${requestDiff} minutes. Please try again after ${
          11 - requestDiff
        } minute`
      )
    }
    const findQuery = {
      action: 'handle_finn_listing',
      status: { $ne: 'completed' },
      'params.partnerId': partnerId,
      'params.propertyId': propertyId
    }
    const isFinn =
      size(listingInfo.finn) &&
      listingInfo.finn.finnShareAt &&
      !listingInfo.finn.statisticsURL
    const updateData = {
      'finn.isArchiving': 1,
      'finn.isPublishing': 1,
      'finn.isRePublishing': 1
    }
    if (isFinn) {
      updateData['finn.finnShareAt'] = 1
    }
    await appQueueService.updateAppQueueItems(
      findQuery,
      {
        $set: {
          completedAt: new Date(),
          isManuallyCompleted: true,
          status: 'completed'
        }
      },
      session
    )
    if (size(updateData)) {
      const success = !!(await listingService.updateAListing(
        { _id: propertyId },
        { $unset: updateData },
        session
      ))
      return { success }
    }
  } else {
    throw new CustomError(404, 'Property not found')
  }
  return {
    success: true
  }
}

export const updatePropertyStatus = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['propertyId', 'propertyStatus'], body)
  await appHelper.validateSelfServicePartnerRequestAndUpdateBody(user, session)
  const { propertyId, propertyStatus } = body
  const previousProperty = await listingHelper.getAListing({
    _id: propertyId,
    partnerId
  })
  if (!size(previousProperty)) throw new CustomError(404, 'Property not found')
  if (previousProperty.propertyStatus === propertyStatus)
    throw new CustomError(400, 'Nothing to update')
  const updateData = propertyHelper.prepareDataToUpdatePropertyStatus(body)
  const updatedProperty = await listingService.updateAListing(
    {
      _id: propertyId,
      partnerId
    },
    {
      $set: updateData
    },
    session
  )
  await createLogForChangingPropertyStatus({
    previousProperty,
    session,
    updatedProperty,
    userId
  })
  return {
    _id: updatedProperty._id,
    listed: updatedProperty.listed,
    propertyStatus: updatedProperty.propertyStatus
  }
}

const createLogForChangingPropertyStatus = async ({
  previousProperty,
  session,
  updatedProperty,
  userId
}) => {
  const { _id, agentId, branchId, partnerId } = updatedProperty
  const logData = {
    action: 'updated_property',
    agentId,
    branchId,
    changes: [
      {
        field: 'propertyStatus',
        newText: updatedProperty.propertyStatus,
        oldText: previousProperty.propertyStatus,
        type: 'text'
      }
    ],
    context: 'property',
    createdBy: userId,
    isChangeLog: true,
    partnerId,
    propertyId: _id,
    visibility: ['property']
  }
  await logService.createLog(logData, session)
}

export const updateConversationParticipantForOwnerChange = async (
  params,
  session
) => {
  const { alsoUpdateContract, newAgentId, oldAgentId, partnerId, propertyId } =
    params

  const conversationQuery = { partnerId, propertyId }
  if (!alsoUpdateContract) conversationQuery.contractId = { $exists: false }
  const conversations = await propertyHelper.getConversationsForOwnerChange({
    agentId: oldAgentId,
    conversationQuery
  })

  if (!size(conversations)) return true

  const pushableConversationIds = []
  const updateConversationPromises = []

  for (let i = 0; i < conversations.length; i++) {
    const conversation = conversations[i]
    if (
      !size(conversation) ||
      conversation?.contract?.status === 'closed' ||
      conversation.oldAgentId === newAgentId
    )
      continue

    if (size(conversation.conversationMessage)) {
      pushableConversationIds.push(conversation._id)
    } else {
      const updateData = { $set: { 'participants.$.userId': newAgentId } }
      updateConversationPromises.push(
        conversationService.updateAConversation(
          {
            _id: conversation._id,
            participants: { $elemMatch: { userId: oldAgentId } }
          },
          updateData,
          session
        )
      )
    }
  }
  if (size(updateConversationPromises)) {
    await Promise.all(updateConversationPromises)
  }

  if (size(pushableConversationIds)) {
    await conversationService.updateConversations(
      { _id: { $in: pushableConversationIds } },
      { $addToSet: { participants: { userId: newAgentId } } },
      session
    )
  }
  return true
}

export const updatePropertyOwner = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['propertyId', 'branchId', 'agentId'], body)
  const { agentId, alsoUpdateContract, branchId, propertyId } = body
  const previousProperty = await listingHelper.getAListing({
    _id: propertyId,
    partnerId
  })
  if (!size(previousProperty)) throw new CustomError(404, 'Property not found')
  const branchInfo = await branchHelper.getABranch({
    _id: branchId,
    agents: agentId,
    partnerId
  })
  if (!size(branchInfo))
    throw new CustomError(404, 'Owner not available in this branch')
  if (
    previousProperty.agentId === agentId &&
    previousProperty.branchId === branchId
  )
    throw new CustomError(404, 'Nothing to update')
  const updatedProperty = await listingService.updateAListing(
    {
      _id: propertyId,
      partnerId
    },
    {
      $set: {
        agentId,
        branchId,
        ownerId: agentId
      }
    },
    session
  )
  await updateConversationParticipantForOwnerChange(
    {
      alsoUpdateContract,
      newAgentId: agentId,
      oldAgentId: previousProperty?.agentId || previousProperty?.ownerId,
      partnerId,
      propertyId
    },
    session
  )
  if (alsoUpdateContract)
    await contractService.updateContracts(
      {
        partnerId,
        propertyId,
        status: { $in: ['active', 'upcoming'] }
      },
      {
        $set: {
          agentId,
          branchId
        }
      },
      session
    )
  await createLogForUpdatePropertyOwner({
    previousProperty,
    session,
    updatedProperty,
    userId
  })
  return await propertyHelper.prepareReturnDataForUpdatePropertyOwner(
    updatedProperty
  )
}

const createLogForUpdatePropertyOwner = async ({
  previousProperty,
  session,
  updatedProperty,
  userId
}) => {
  const changes = []
  if (previousProperty.agentId !== updatedProperty.agentId)
    changes.push({
      field: 'agentId',
      newText: updatedProperty.agentId,
      oldText: previousProperty.agentId,
      type: 'foreignKey'
    })
  if (previousProperty.branchId !== updatedProperty.branchId)
    changes.push({
      field: 'branchId',
      newText: updatedProperty.branchId,
      oldText: previousProperty.branchId,
      type: 'foreignKey'
    })
  const logData = {
    action: 'updated_property',
    agentId: updatedProperty.agentId,
    branchId: updatedProperty.branchId,
    changes,
    context: 'property',
    createdBy: userId,
    isChangeLog: true,
    partnerId: updatedProperty.partnerId,
    propertyId: updatedProperty._id,
    visibility: ['property']
  }
  await logService.createLog(logData, session)
}

export const updatePropertyAbout = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['propertyId', 'aboutText'], body)
  const { aboutText, propertyId } = body
  const updatedProperty = await listingService.updateAListing(
    {
      _id: propertyId,
      partnerId
    },
    {
      $set: {
        aboutText
      }
    }
  )
  return {
    _id: updatedProperty._id,
    aboutText: updatedProperty.aboutText
  }
}

export const downloadProperty = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  propertyHelper.validateParamsForDownloadProperty(body)
  const { preparedQuery } =
    await propertyHelper.preparePropertiesQueryFromFilterData(body)
  await appHelper.isMoreOrLessThanTargetRows(ListingCollection, preparedQuery, {
    moduleName: 'Properties',
    rejectEmptyList: true
  })
  const {
    sort = {
      createdAt: -1
    }
  } = body
  body.userId = userId
  if (sort['location.name']) {
    sort.location_name = sort['location.name']
    delete sort['location.name']
  }
  body.sort = sort
  body.download = true
  body.downloadProcessType = 'download_properties'
  body.isQueueCreatedFromV2 = true
  const userInfo = await userHelper.getAnUser({ _id: userId })
  body.userLanguage = userInfo?.profile?.language || 'en'
  const {
    availabilityDateRange,
    createdAtDateRange,
    leaseStartDateRange,
    leaseEndDateRange
  } = body

  if (size(availabilityDateRange)) {
    body.availabilityDateRange = {
      startDate: new Date(availabilityDateRange.startDate),
      endDate: new Date(availabilityDateRange.endDate)
    }
  }
  if (size(createdAtDateRange)) {
    body.createdAtDateRange = {
      startDate: new Date(createdAtDateRange.startDate),
      endDate: new Date(createdAtDateRange.endDate)
    }
  }
  if (size(leaseStartDateRange)) {
    body.leaseStartDateRange = {
      startDate: new Date(leaseStartDateRange.startDate),
      endDate: new Date(leaseStartDateRange.endDate)
    }
  }
  if (size(leaseEndDateRange)) {
    body.leaseEndDateRange = {
      startDate: new Date(leaseEndDateRange.startDate),
      endDate: new Date(leaseEndDateRange.endDate)
    }
  }
  const queueData = {
    action: 'download_email',
    destination: 'excel-manager',
    event: 'download_email',
    params: body,
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(queueData)
  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

export const updatePropertyJanitor = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['janitorId', 'propertyId'])
  const { body, session } = req
  const { janitorId, partnerId, propertyId, userId } = body
  const query = { _id: propertyId, partnerId }

  const propertyInfo = await listingHelper.getAListing(query)
  if (!propertyInfo) throw new CustomError(400, 'Property not found')
  const userInfo = await userHelper.getUserById(janitorId)
  if (!userInfo) throw new CustomError(400, 'User not found')
  const isJanitor = await appRoleHelper.getAppRole({
    partnerId,
    type: 'partner_janitor',
    users: janitorId
  })
  if (!isJanitor) throw new CustomError(400, 'Janitor role not found')
  if (propertyInfo.janitorId === janitorId)
    throw new CustomError(400, 'Nothing to update')

  const updatedProperty = await listingService.updateAListing(
    query,
    {
      $set: { janitorId }
    },
    session
  )
  await createLogAfterUpdatePropertyJanitor(
    {
      propertyInfo,
      updatedProperty,
      userId
    },
    session
  )
  const avatarKey = userInfo?.profile?.avatarKey
    ? appHelper.getCDNDomain() + '/' + userInfo.profile.avatarKey
    : appHelper.getCDNDomain() + '/' + 'assets/default-image/user-primary.png'
  return {
    _id: userInfo._id,
    avatarKey,
    name: userInfo.profile?.name
  }
}

const createLogAfterUpdatePropertyJanitor = async (params, session) => {
  const { propertyInfo, updatedProperty, userId } = params
  const { _id, agentId, branchId, partnerId } = updatedProperty
  const logData = {
    action: 'updated_property',
    agentId,
    branchId,
    context: 'property',
    createdBy: userId,
    isChangeLog: true,
    partnerId,
    propertyId: _id,
    visibility: ['property']
  }
  const changesValue = {
    field: 'janitorId',
    type: 'foreignKey',
    newText: updatedProperty.janitorId
  }
  if (propertyInfo.janitorId) changesValue.oldText = propertyInfo.janitorId
  logData.changes = [changesValue]
  await logService.createLog(logData, session)
}

export const downloadRentRollReport = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const preparedQuery = await propertyHelper.prepareQueryForRentRollReport(body)
  await appHelper.isMoreOrLessThanTargetRows(ListingCollection, preparedQuery, {
    moduleName: 'Rent roll reports',
    rejectEmptyList: true
  })
  const {
    createdAtDateRange,
    leaseEndDateRange,
    leaseStartDateRange,
    sort = {
      location_name: 1
    },
    userId
  } = body
  if (sort['location.name']) {
    sort.location_name = sort['location.name']
    delete sort['location.name']
  }
  body.sort = sort
  body.download = true
  body.downloadProcessType = 'download_rent_roll_report'
  const userInfo = await userHelper.getAnUser({ _id: userId })
  body.userLanguage = userInfo?.profile?.language || 'en'
  if (size(createdAtDateRange)) {
    body.createdAtDateRange = {
      startDate: new Date(createdAtDateRange.startDate),
      endDate: new Date(createdAtDateRange.endDate)
    }
  }
  if (size(leaseEndDateRange)) {
    body.leaseEndDateRange = {
      startDate: new Date(leaseEndDateRange.startDate),
      endDate: new Date(leaseEndDateRange.endDate)
    }
  }
  if (size(leaseStartDateRange)) {
    body.leaseStartDateRange = {
      startDate: new Date(leaseStartDateRange.startDate),
      endDate: new Date(leaseStartDateRange.endDate)
    }
  }
  const queueData = {
    action: 'download_email',
    destination: 'excel-manager',
    event: 'download_email',
    params: body,
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(queueData)
  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}
