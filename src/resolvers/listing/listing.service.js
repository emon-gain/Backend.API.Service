import { assign, get, size } from 'lodash'
import { CustomError } from '../common'
import { ListingCollection } from '../models'
import {
  accountHelper,
  appHelper,
  listingHelper,
  partnerSettingHelper,
  settingHelper
} from '../helpers'
import {
  appQueueService,
  contractService,
  logService,
  partnerUsageService,
  userService
} from '../services'

export const removeAListing = async (query, session) => {
  const response = await ListingCollection.findOneAndDelete(query, { session })
  return response
}

export const updateAListing = async (query, updateData, session) => {
  const updatedListing = await ListingCollection.findOneAndUpdate(
    query,
    updateData,
    {
      session,
      runValidators: true,
      new: true
    }
  )
  if (!updatedListing) {
    throw new CustomError(404, 'Could not update listing')
  }
  return updatedListing
}

export const updateListingBasePrice = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { listingId, onlyActiveListings } = body
  let query = {}
  if (onlyActiveListings) query = { listed: true }
  if (listingId) query = { _id: listingId }

  const listings = await listingHelper.getListings(query, session)
  if (!size(listings)) {
    throw new CustomError(404, 'Could not find any listings')
  }

  const setting = await settingHelper.getSettingInfo({}, session)
  const rates = get(setting, 'openExchangeInfo.rates', {})
  if (!size(rates)) {
    throw new CustomError(404, 'Could not find open exchange rates in setting')
  }

  for (let i = 0; i < listings.length; i++) {
    const { _id, currency, monthlyRentAmount } = listings[i]
    const rate = rates[currency] // Convert monthlyRentAmount to base currency. Get from the settings for this listing currency
    if (rate && monthlyRentAmount) {
      const baseMonthlyRentAmount = await appHelper.convertTo2Decimal(
        monthlyRentAmount / rate
      )
      const updateData = { $set: { baseMonthlyRentAmount } }
      listings[i] = await updateAListing({ _id }, updateData, session)
    }
  }
  return listings
}

export const updateListingPlaceIds = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['listingId'], body)
  const { listingId, placeIdInfo } = body
  const updateData = await listingHelper.prepareListingPlaceUpdateData(
    placeIdInfo
  )
  const updatedListing = await updateAListing(
    { _id: listingId },
    updateData,
    session
  )
  return updatedListing
}

export const createListing = async (data, session) => {
  const createdListing = await ListingCollection.create([data], {
    session
  })
  return createdListing || []
}

export const addListing = async (req) => {
  const { body, session, user = {} } = req
  console.log('Checking user id for add listing', user.userId)
  appHelper.checkUserId(user.userId)
  if (user.defaultRole && user.defaultRole === 'landlord') {
    const account = await accountHelper.getAnAccount(
      {
        personId: user.userId,
        partnerId: user.partnerId
      },
      session
    )
    if (!account?._id) throw new CustomError(404, 'Landlord not found')
    body.partnerId = user.partnerId
    body.branchId = account.branchId
    body.agentId = account.agentId
  }
  body.ownerId = user.userId
  console.log('Checking body.ownerId', body.ownerId)
  appHelper.compactObject(body)
  const setting = await settingHelper.getSettingInfo()
  listingHelper.validateListingAddData(body, setting)
  console.log('Checking body', body)
  const data = await listingHelper.prepareListingAddData(body, setting, session)
  console.log('Checking data', data)
  const [listing] = await createListing(data, session)
  console.log('Checking listing', listing)
  if (size(listing)) {
    await initAfterInsertProcesses(listing, session)
  }
  return listing
}

export const initAfterInsertProcesses = async (listing, session) => {
  const { currency, location } = listing
  await userService.updateUsersListingStatus(listing.ownerId, session)
  if (location.city) await insertInQueueForListingPlaceIds(listing._id, session)
  if (currency) await insertInQueueForListingBasePrice(listing._id, session)
}

export const updateListedAt = async (listing, session) => {
  const query = { _id: listing._id }
  const updateData = { $set: { listedAt: new Date() } }
  const updatedListing = await updateAListing(query, updateData, session)
  return updatedListing
}

export const updateDisabledAt = async (listing, session) => {
  const query = { _id: listing._id }
  const updateData = { $set: { disabledAt: new Date() } }
  const updatedListing = await updateAListing(query, updateData, session)
  return updatedListing
}

export const createPropertyChangeLog = async (params, session) => {
  const { changedFields, previousListing, updatedListing, userId } = params
  const logData = listingHelper.prepareChangeLogData(
    updatedListing,
    previousListing,
    changedFields
  )
  if (userId) logData.createdBy = userId
  const insertedLog = await logService.createLog(logData, session)
  return insertedLog
}

export const updateListings = async (query, data, session) => {
  await ListingCollection.updateMany(query, data, {
    session
  })
}

export const addPropertyStatusChangeLog = async (
  updatedListing,
  previousListing,
  session
) => {
  const logData = listingHelper.prepareBasicChangeLogData(
    updatedListing,
    previousListing
  )
  const insertedLog = await logService.createLog(logData, session)
  return insertedLog
}

export const initAfterUpdateProcesses = async ({
  previousListing,
  session,
  updatedListing,
  userId
}) => {
  const { _id, partnerId } = updatedListing
  if (listingHelper.isUpdateListingPlaceIds(updatedListing, previousListing)) {
    await insertInQueueForListingPlaceIds(_id, session, true)
  }
  if (listingHelper.isUpdateListingBasePrice(updatedListing, previousListing)) {
    await insertInQueueForListingBasePrice(_id, session, true)
  }
  // For property
  if (partnerId) {
    if (listingHelper.isUpdateAccountsData(updatedListing, previousListing)) {
      await insertInQueueForAccountUpdate(updatedListing, session)
    }
    const changedFields = listingHelper.getPropertyChangedFields(
      updatedListing,
      previousListing
    )
    if (size(changedFields)) {
      const params = { changedFields, previousListing, updatedListing, userId }
      await createPropertyChangeLog(params, session)
    }
    if (listingHelper.isAddHistoryInContract(updatedListing, previousListing)) {
      await contractService.addChangeLogHistoryToContract(
        updatedListing,
        previousListing,
        session
      )
    }
    if (
      await listingHelper.isUpdateContractOwner(updatedListing, previousListing)
    ) {
      await contractService.updateContractOwner(updatedListing, session)
    }
  }
}

export const updateListing = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  listingHelper.validateListingUpdateData(body)
  const { partnerId, roles, userId } = user
  const dataNeedTobeMerge =
    await appHelper.validateSelfServicePartnerRequestAndUpdateBody(
      user,
      session
    )
  console.log('dataNeedTobeMerge: ', dataNeedTobeMerge)
  assign(body, dataNeedTobeMerge)
  if (partnerId) {
    //For partner app
    body.partnerId = partnerId
  }
  body.propertyId = body._id
  const { propertyId, finnPublishInfo = {}, listed } = body
  const { isShareAtFinn, finnUpdateType } = finnPublishInfo || {}
  const query = await listingHelper.prepareQueryForUpdateListings(body, user)
  console.log('Checking query to find listing: ', query)
  const listing = await listingHelper.getAListing(query) // No need to use session
  console.log('Checking listing to update: ', listing)
  if (!size(listing)) {
    throw new CustomError(404, 'Could not find listing')
  }
  if (!listed && isShareAtFinn) {
    throw new CustomError(400, 'Publish to UL first')
  }
  if (listed && isShareAtFinn) {
    const finnMissingData = await listingHelper.getFinnMissingData({
      body,
      listing
    })
    if (size(finnMissingData)) return finnMissingData
  }

  listingHelper.validateListingOwner({
    listing,
    partnerId,
    roles,
    userId
  })
  const params = { body, listing, user }
  const updateData = await listingHelper.prepareUpdateData(params, session)
  console.log('update_data', updateData)
  const updatedListing = await updateAListing(query, updateData, session)
  if (size(updatedListing)) {
    console.log('UPDATEDlIST', updatedListing)
    await initAfterUpdateProcesses({
      previousListing: listing,
      session,
      updatedListing
    })
  }
  if (finnUpdateType !== 'republish') {
    const queueData = {
      event: 'share_or_archive_finn_listing',
      action: 'handle_finn_listing',
      params: {
        propertyId,
        partnerId,
        userId,
        finnUpdateType,
        processType: 'share' // ["share", "remove"]
      },
      destination: 'listing',
      priority: 'immediate'
    }
    const appQueue = await appQueueService.insertInQueue(queueData, session)
    console.log('appQueue: ', appQueue)
  }
  return updatedListing
}

// for lambda listingsFinnData
export const updateFinnDataForListing = async (req) => {
  const { user = {}, session, body } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['listingId', 'finnData'], body)
  const { listingId, finnData, zipFileResin } = body
  appHelper.validateId({ listingId })
  const listingInfo = await listingHelper.getListingById(listingId, session)
  if (!size(listingInfo)) {
    throw new CustomError(404, 'Listing info not found')
  }
  if (!size(finnData)) {
    throw new CustomError(400, 'Incomplete request body')
  }
  body.listingInfo = listingInfo
  const finnUpdateData = listingHelper.prepareFinnDataForUpdateListing(body)
  const updatedListing = await updateAListing(
    { _id: listingId },
    finnUpdateData,
    session
  )
  body.listingInfo = updatedListing
  const logUpdateData =
    listingHelper.prepareDataForCreateLogOfUpdateOrFailFinn(body)
  await logService.createLog(logUpdateData, session)
  if (
    listingInfo &&
    listingInfo.partnerId &&
    logUpdateData.action === 'publish_to_finn'
  ) {
    if (zipFileResin === 'firstAd' || zipFileResin === 'republish') {
      await partnerUsageService.createAPartnerUsage(
        {
          type: 'finn',
          partnerId: listingInfo.partnerId,
          total: 1
        },
        session
      )
    }
  }
  return {}
}

export const disableListingForPartner = async (partnerId) => {
  //Get app settings
  const appSettings = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const isEnableListing = !!(
    appSettings &&
    appSettings.listingSetting &&
    appSettings.listingSetting.disabledListing &&
    appSettings.listingSetting.disabledListing.enabled &&
    appSettings.listingSetting.disabledListing.days > 0
  )
  const date = new Date()
  if (isEnableListing) {
    const disableTargetDays = appSettings.listingSetting.disabledListing.days
    let targetDate = await appHelper.getActualDate(partnerId, true, date)
    targetDate = targetDate
      .subtract(disableTargetDays, 'days')
      .endOf('day')
      .toDate()
    const query = {
      partnerId,
      listed: true,
      listedAt: { $lte: targetDate }
    }

    //Update listings
    await updateListings(query, { $set: { listed: false } })
    return {
      msg: 'Listing Disabled',
      code: 201
    }
  } else {
    return {
      msg: 'Partner listing not enabled',
      code: 404
    }
  }
}

export const dailyListingAvailabilityService = async () => {
  try {
    const firstOfThisMonth = new Date()
    firstOfThisMonth.setDate(1) //1st day of this month

    //update listing availability for past month
    await updateListings(
      {
        listed: true,
        availabilityStartDate: { $lt: firstOfThisMonth },
        $or: [
          { availabilityEndDate: { $exists: false } },
          { availabilityEndDate: { $gt: firstOfThisMonth } }
        ]
      },
      { $set: { availabilityStartDate: firstOfThisMonth } }
    )

    //disable the listings where availabilityEndDate has been expired
    await updateListings(
      {
        listed: true,
        availabilityEndDate: { $exists: true, $lt: firstOfThisMonth }
      },
      { $set: { listed: false } }
    )
    return {
      msg: 'Listing availability updated',
      code: 201
    }
  } catch (error) {
    throw new CustomError(500, 'Could not update listing')
  }
}

// Creating app-queue for after insert or update a listing
const insertInQueueForListingPlaceIds = async (
  listingId,
  session,
  isUpdated
) => {
  const queueData = {
    event: isUpdated ? 'updated_listing' : 'created_new_listing',
    action: 'update_listing_place_ids',
    priority: 'regular',
    destination: 'listing',
    params: { listingId }
  }
  await appQueueService.insertInQueue(queueData, session)
}

export const insertInQueueForListingBasePrice = async (
  listingId,
  session,
  isUpdated
) => {
  const queueData = {
    event: isUpdated ? 'updated_listing' : 'created_new_listing',
    action: 'update_listing_base_price',
    priority: 'regular',
    destination: 'listing',
    params: { listingId }
  }
  await appQueueService.insertInQueue(queueData, session)
}

const insertInQueueForAccountUpdate = async (data = {}, session) => {
  const { accountId, partnerId } = data
  const queueData = {
    event: 'updated_listing',
    action: 'update_accounts_total_active_properties',
    priority: 'regular',
    destination: 'listing',
    params: { accountId, partnerId }
  }
  await appQueueService.insertInQueue(queueData, session)
}

export const addOrRemoveListingFromFavourite = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { listingId } = body
  appHelper.checkRequiredFields(['listingId', 'favourite'], body)
  appHelper.validateId({ listingId })
  const updateData = listingHelper.prepareDataForAddOrRemoveFromFavourite(req)
  const query = {
    _id: listingId
  }
  await updateAListing(query, updateData)
  return {
    result: true
  }
}

export const removeListings = async (query, session) => {
  if (!size(query))
    throw new CustomError(400, 'Query must be required while removing listings')
  const response = await ListingCollection.deleteMany(query, {
    session
  })
  console.log('=== Listings Removed ===', response)
  return response
}
