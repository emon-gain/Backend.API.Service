import {
  assign,
  clone,
  each,
  filter,
  get,
  includes,
  isArray,
  omit,
  pick,
  size
} from 'lodash'
import momemt from 'moment-timezone'
import { appPermission, CustomError } from '../common'
import { ListingCollection } from '../models'
import {
  accountHelper,
  appHelper,
  appQueueHelper,
  branchHelper,
  contractHelper,
  dashboardHelper,
  logHelper,
  partnerHelper,
  partnerSettingHelper,
  propertyHelper,
  settingHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import { listingService } from '../services'
import settingJson from '../../../settings'

export const getListingById = async (id, session, populate) => {
  const listing = await ListingCollection.findById(id)
    .session(session)
    .populate(populate)
  return listing
}

export const getAListing = async (query, session, populate = []) => {
  const listing = await ListingCollection.findOne(query)
    .populate(populate)
    .session(session)
  return listing
}

export const getFacilityNameById = (id = '', setting) => {
  const { facilities = [] } = setting || {}
  const { name } = facilities.find((type) => id === type.id) || {}
  return name
}

export const getIncludedInRentNameById = (id = '', setting) => {
  const { includedInRent = [] } = setting || {}
  const { name } = includedInRent.find((type) => id === type.id) || {}
  return name
}

export const getListingTypeIdByName = (name = '', setting) => {
  const { listingTypes = [] } = setting || {}
  const { id } = listingTypes.find((type) => name === type.name) || {}
  return id
}

export const getListingTypeNameById = (id = '', setting) => {
  const { listingTypes = [] } = setting || {}
  const { name } = listingTypes.find((type) => id === type.id) || {}
  return name
}

export const getPropertyTypeIdByName = (name = '', setting) => {
  const { propertyTypes = [] } = setting || {}
  const { id } = propertyTypes.find((type) => name === type.name) || {}
  return id
}

export const getPropertyTypeNameById = (id = '', setting) => {
  const { propertyTypes = [] } = setting || {}
  const { name } = propertyTypes.find((type) => id === type.id) || {}
  return name
}

export const isListingTypeParking = (listingTypeId = '', setting) => {
  const listingType = getListingTypeNameById(listingTypeId, setting)
  return !!(listingType === 'parking')
}

export const validateAvailabilityDates = (startDate, endDate) => {
  startDate = new Date(startDate)
  endDate = new Date(endDate)
  if (startDate > endDate) {
    throw new CustomError(400, "Start date can't be greater than end date")
  }
}

export const getListingIds = async (query) => {
  const listings = await ListingCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: 'null',
        propertyIds: {
          $addToSet: '$_id'
        }
      }
    }
  ])
  return listings[0]?.propertyIds || []
}

// Only listed ids in "setting => facilities" are valid
export const validateFacilityId = (id = '', setting) => {
  const name = getFacilityNameById(id, setting)
  if (!name) {
    throw new CustomError(400, 'Invalid id for facilities')
  }
}

// Only listed ids in "setting => includedInRent" are valid
export const validateIncludedInRentId = (id = '', setting) => {
  const name = getIncludedInRentNameById(id, setting)
  if (!name) {
    throw new CustomError(400, 'Invalid id for includedInRent')
  }
}

// Only listed ids in "setting => listingTypes" are valid
export const validateListingTypeId = (id = '', setting) => {
  const name = getListingTypeNameById(id, setting)
  if (!name) {
    throw new CustomError(400, 'Invalid listingTypeId')
  }
}

// Only listed ids in "setting => propertyTypes" are valid
export const validatePropertyTypeId = (id = '', setting) => {
  const name = getPropertyTypeNameById(id, setting)
  if (!name) {
    throw new CustomError(400, 'Invalid propertyTypeId')
  }
}

export const validateFurnished = (furnished = '') => {
  const allowedValues = ['furnished', 'partially_furnished', 'unfurnished']
  if (!allowedValues.includes(furnished)) {
    throw new CustomError(400, `Allowed values: ${allowedValues}`)
  }
}

export const validateLocation = (location = {}) => {
  if (!location.name) {
    throw new CustomError(400, 'Missing location name')
  }
}

export const validateTitle = (title = '') => {
  if (!title) {
    throw new CustomError(400, 'Title is required')
  } else if (title.length > 80) {
    throw new CustomError(400, "Title can't contain more than 80 characters")
  }
}

export const validateListingImages = (images) => {
  if (!size(images)) {
    throw new CustomError(400, "Images can't be empty")
  }
  for (const image of images) {
    const { imageName, rotate, title } = image
    if (!imageName) {
      throw new CustomError(400, 'Missing imageName')
    }
    if (rotate && (rotate < 0 || rotate > 360)) {
      throw new CustomError(400, 'Rotation range is 0 to 359')
    }
    if (title && title.length > 80) {
      throw new CustomError(
        400,
        "Image title can't contain more than 80 characters"
      )
    }
  }
}

export const validateUrlDomain = async (url, session) => {
  const isAllowed = await isUrlDomainWhiteListed(url, session)
  if (!isAllowed) {
    throw new CustomError(400, `The url ${url} is not allowed`)
  }
}

export const validateListingAddData = (data = {}, setting) => {
  const requiredFields = [
    'title',
    'location',
    'availabilityStartDate',
    'monthlyRentAmount',
    'listingTypeId'
  ]
  appHelper.checkRequiredFields(requiredFields, data)
  appHelper.checkPositiveNumbers(data)
  const {
    availabilityStartDate,
    availabilityEndDate,
    location = {},
    listingTypeId,
    propertyTypeId,
    title
  } = data
  validateTitle(title)
  validateListingTypeId(listingTypeId, setting)
  if (propertyTypeId) validatePropertyTypeId(propertyTypeId, setting)
  appHelper.compactObject(location, false)
  validateLocation(location)
  data.availabilityStartDate = new Date(availabilityStartDate)
  if (availabilityEndDate) {
    data.availabilityEndDate = new Date(availabilityEndDate)
    validateAvailabilityDates(availabilityStartDate, availabilityEndDate)
  }
}

export const getBaseMonthlyRentAmount = async (data = {}, session) => {
  const { currency, monthlyRentAmount } = data
  if (currency && monthlyRentAmount) {
    const setting = (await settingHelper.getSettingInfo({}, session)) || {}
    const rate = get(setting, `openExchangeInfo.rates.${currency}`)
    if (rate) {
      // Convert monthlyRentAmount to base currency and add to data
      const baseMonthlyRentAmount = await appHelper.convertTo2Decimal(
        monthlyRentAmount / rate
      )
      return baseMonthlyRentAmount
    }
  }
}

export const prepareListingAddData = async (data = {}, setting, session) => {
  const {
    listingTypeId,
    location = {},
    noOfBedrooms,
    noOfBathroom,
    noOfKitchen,
    noOfLivingRoom
  } = data
  const { countryShortName } = location

  data.listed = false
  if (countryShortName) {
    data.currency = await appHelper.getCurrencyOfCountry(
      countryShortName,
      session
    )
  }
  if (noOfBedrooms) {
    data.availableBedrooms = []
    for (let id = 1; id <= noOfBedrooms; id++) {
      data.availableBedrooms.push({ id })
    }
  }
  if (isListingTypeParking(listingTypeId, setting)) {
    delete data.noOfBedrooms
    delete data.propertyTypeId
  }

  if (noOfBathroom) data.bathroom = true
  else if (noOfBathroom === 0) data.bathroom = false
  if (noOfKitchen) data.kitchen = true
  else if (noOfKitchen === 0) data.kitchen = false
  if (noOfLivingRoom) data.livingRoom = true
  else if (noOfLivingRoom === 0) data.livingRoom = false
  return data
}

export const isAddContract = async (listing, session) => {
  const { _id, agentId, branchId, partnerId, accountId } = listing
  const isDirectPartner = !!(await partnerHelper.getDirectPartnerById(
    partnerId,
    session
  ))
  return _id && agentId && branchId && partnerId && accountId && isDirectPartner
}

export const getListing = async (query, session) => {
  const listing = await ListingCollection.findOne(query).session(session)
  return listing
}

export const getListings = async (query, session) => {
  const listings = await ListingCollection.find(query).session(session)
  return listings
}

export const prepareListingPlaceUpdateData = async (placeIdInfo) => {
  const { cityPlaceId, placeIds } = placeIdInfo
  const updateData = {}
  if (cityPlaceId) updateData['location.cityPlaceId'] = cityPlaceId
  if (size(placeIds)) updateData.placeIds = placeIds
  return updateData
}

export const prepareQueryForUpdateListings = async (body, user) => {
  const { _id, bedroomId, partnerId, updateType } = body
  const { userId, roles } = user
  console.log('Checking userId: ', userId)
  console.log('Checking roles: ', roles)
  const queries = {
    addAddon: { _id, partnerId },
    deleteAddon: { _id, partnerId },
    deleteListing: { _id },
    increasePageView: { _id },
    listingInfo: { _id },
    removeFromFinn: { _id },
    publish: { _id },
    removeImage: { _id },
    updateAvailableBedrooms: { _id, 'availableBedrooms.id': bedroomId },
    updateAvailableEndDate: { _id },
    updateFavorite: { _id },
    updatePropertyInfo: { _id, partnerId },
    updatePropertyStatus: { _id, partnerId },
    updatePropertyOwner: { _id, partnerId },
    updatePropertyOwnerAndBranch: { _id, partnerId }
  }
  const preparedQuery = queries[updateType]
  if (preparedQuery) {
    preparedQuery['ownerId'] = userId
    let canEditProperty = false
    if (partnerId) {
      preparedQuery['partnerId'] = partnerId
      canEditProperty = await appPermission.canEditPartnerProperties(
        userId,
        partnerId
      )
    }
    if (appHelper.isAppAdmin(roles) || canEditProperty) {
      delete preparedQuery.ownerId
    }
  }
  console.log('Checking query to find listing: ', preparedQuery)
  return preparedQuery
}

export const prepareAddAddonData = async ({ body }, session) => {
  const { _id, addonId, isRecurring, partnerId, price, total, type } = body
  const propertyData = {
    isRecurring,
    addonId,
    price,
    total,
    type
  }
  let hasAddon = false
  const propertyInfo = await getAListing({ _id, partnerId }, session)
  const { addons = [] } = propertyInfo || {}
  const addonsList = filter(addons, (addonInfo) => {
    if (addonInfo.addonId === addonId) {
      addonInfo.isRecurring = isRecurring
      addonInfo.price = price
      addonInfo.total = total
      addonInfo.type = type
      hasAddon = true
    }
    return addonInfo
  })
  if (!hasAddon) {
    addonsList.push(propertyData)
  }
  return { $set: { addons: addonsList } }
}

export const validateListingInfo = async (body, setting, session) => {
  const { _id, listingInfo = {} } = body
  const {
    availabilityStartDate,
    availabilityEndDate,
    images,
    facilities,
    furnished,
    includedInRent,
    listingTypeId,
    location,
    propertyTypeId,
    title,
    videoUrl,
    view360Url
  } = listingInfo

  appHelper.checkPositiveNumbers(listingInfo)
  if (title || title === '') validateTitle(title)
  if (listingTypeId || listingTypeId === '')
    validateListingTypeId(listingTypeId, setting)
  if (propertyTypeId) validatePropertyTypeId(propertyTypeId, setting)

  if (facilities) {
    for (const id of facilities) {
      validateFacilityId(id, setting)
    }
  }
  if (includedInRent) {
    for (const id of includedInRent) {
      validateIncludedInRentId(id, setting)
    }
  }
  if (furnished || furnished === '') validateFurnished(furnished)
  if (location) validateLocation(location)

  if (availabilityStartDate && availabilityEndDate)
    validateAvailabilityDates(availabilityStartDate, availabilityEndDate)
  else if (availabilityStartDate && !availabilityEndDate) {
    const listing = await getAListing({ _id }, session)
    const endDate = get(listing, 'availabilityEndDate')
    if (endDate) validateAvailabilityDates(availabilityStartDate, endDate)
  } else if (!availabilityStartDate && availabilityEndDate) {
    const listing = await getAListing({ _id }, session)
    const startDate = get(listing, 'availabilityStartDate')
    if (startDate) validateAvailabilityDates(startDate, availabilityEndDate)
  }

  if (images) validateListingImages(images)
  if (videoUrl) await validateUrlDomain(videoUrl, session)
  if (view360Url) await validateUrlDomain(view360Url, session)
}

const prepareListingUnsetData = (
  listing = {},
  body = {},
  prevUnsetData = {}
) => {
  console.log('Checking prevUnsetData: ', prevUnsetData)
  const unsetData = { ...prevUnsetData }
  console.log('Checking unsetData: ', unsetData)
  const { propertyTypeId: oldPropertyTypeId = '' } = listing
  const { listingInfo = {} } = body
  const { isUnsetPropertyType = false } = listingInfo
  console.log('Checking oldPropertyTypeId: ', oldPropertyTypeId)
  console.log('Checking isUnsetPropertyType: ', isUnsetPropertyType)
  if (
    oldPropertyTypeId &&
    isUnsetPropertyType &&
    !prevUnsetData?.propertyTypeId
  )
    unsetData.propertyTypeId = 1
  console.log('Checking unsetData: ', unsetData)
  return unsetData
}

export const prepareListingInfo = async ({ body, listing }, session) => {
  const { listingInfo = {} } = body
  console.log('Checking listingInfo in body: ', listingInfo)
  appHelper.compactObject(listingInfo, false)
  if (!size(listingInfo)) {
    throw new CustomError(400, 'Listing info can not be empty')
  }
  const setting = await settingHelper.getSettingInfo()
  await validateListingInfo(body, setting, session)
  const {
    availabilityStartDate,
    availabilityEndDate,
    description,
    images,
    listingTypeId,
    location,
    noOfBedrooms,
    noOfBathroom,
    noOfKitchen,
    noOfLivingRoom
  } = listingInfo

  if (availabilityStartDate)
    listingInfo.availabilityStartDate = new Date(availabilityStartDate)
  if (availabilityEndDate)
    listingInfo.availabilityEndDate = new Date(availabilityEndDate)

  if (description) listingInfo.description = description

  if (images) {
    listingInfo.images = images
    //TODO: #10972 Check wether the below function needed or full images found from fronend request
    // listingInfo.images = prepareListingImages(images, listing.images)
  }
  if (location) {
    appHelper.compactObject(location, false)
    if (isUpdateCurrency(location, listing)) {
      listingInfo.currency = await appHelper.getCurrencyOfCountry(
        location.countryShortName,
        session
      )
    }
  }

  if (noOfBedrooms) {
    listingInfo.availableBedrooms = []
    for (let id = 1; id <= noOfBedrooms; id++) {
      listingInfo.availableBedrooms.push({ id })
    }
  }

  if (noOfBathroom) listingInfo.bathroom = true
  else if (noOfBathroom === 0) listingInfo.bathroom = false
  if (noOfKitchen) listingInfo.kitchen = true
  else if (noOfKitchen === 0) listingInfo.kitchen = false
  if (noOfLivingRoom) listingInfo.livingRoom = true
  else if (noOfLivingRoom === 0) listingInfo.livingRoom = false

  const updateData = {}
  if (isListingTypeParking(listingTypeId, setting)) {
    delete listingInfo.noOfBedrooms
    delete listingInfo.propertyTypeId
    delete listingInfo.availableBedrooms
    updateData.$unset = {
      noOfBedrooms: 1,
      propertyTypeId: 1,
      availableBedrooms: 1
    }
  }
  updateData.$set = listingInfo
  if (listingInfo.isUnsetPropertyType) {
    delete updateData.$set.isUnsetPropertyType
  }
  const prevUnsetData = updateData?.$unset || {}
  const unsetData = prepareListingUnsetData(listing, body, prevUnsetData)
  if (size(unsetData)) updateData.$unset = unsetData
  console.log('Checking updateData: ', updateData)
  return updateData
}

export const prepareListingImages = (images = [], prevImages = []) => {
  const commonNames = []
  for (const prevImage of prevImages) {
    const image = images.find(
      ({ imageName }) => imageName === prevImage.imageName
    )
    if (size(image)) {
      if (image.rotate || image.rotate === 0) prevImage.rotate = image.rotate
      if (image.title || image.title === '') prevImage.title = image.title
      commonNames.push(image.imageName)
    }
  }
  const newImages = images.filter(
    ({ imageName }) => !commonNames.includes(imageName)
  )
  return [...prevImages, ...newImages]
}

export const preparePropertyStatusData = async ({ body }) => {
  const { propertyStatus } = body
  const allowedStatus = ['active', 'archived', 'maintenance']
  if (!allowedStatus.includes(propertyStatus)) {
    throw new CustomError(400, `Allowed property status: ${allowedStatus}`)
  }
  const updateData = { propertyStatus }
  if (propertyStatus === 'archived') {
    updateData.listed = false
  }
  return { $set: updateData }
}

export const prepareOwnerAndBranchUpdateData = async ({ body }, session) => {
  const { agentId, branchId, partnerId } = body
  const query = {
    _id: branchId,
    partnerId,
    agents: { $in: [agentId] }
  }
  const isExistsBranchAgent = !!(await branchHelper.getABranch(query, session))
  if (isExistsBranchAgent) {
    throw new CustomError(401, `Selected agent is not available in this branch`)
  }
  return { $set: { agentId, ownerId: agentId, branchId } } // Should update agentId along with ownerId
}

export const prepareRemoveFromFinn = async ({ body }, session) => {
  const { _id, finnUpdateType } = body
  const listing = await getAListing({ _id }, session)
  const isArchived = !(listing && listing.finn && listing.finn.isShareAtFinn)
  let updateData = {}
  if (!isArchived) {
    updateData = {
      $set: {
        'finn.finnArchivedAt': new Date(),
        'finn.updateType': finnUpdateType
      }
    }
  } else {
    updateData = { $unset: { 'finn.finnArchivedAt': 1 } }
  }
  return updateData
}

const prepareAvailableBedroomData = async ({ body }) => {
  const { bedroomSize, bedroomFurnished } = body
  const data = {}
  if (bedroomSize > 0) {
    data['availableBedrooms.$.bedroomSize'] = bedroomSize
  }
  if (bedroomFurnished === true || bedroomFurnished === false) {
    data['availableBedrooms.$.bedroomFurnished'] = bedroomFurnished
  }
  if (!size(data)) {
    throw new CustomError(400, 'Missing bedroom data')
  }
  return { $set: data }
}

export const getFinnMissingData = async ({ body, listing }) => {
  const { availabilityEndDate, images = [], noOfBedrooms } = listing
  const { propertyId, partnerId, finnPublishInfo = {} } = body
  const { shareWithWarning, finnUpdateType } = finnPublishInfo || {}
  let isShareWithWarning = false
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
      missingData.type = finnUpdateType
      return { _id: propertyId, partnerId, missingData }
    }
  }
}

const prepareListingDataForSharingToFinn = async (
  { body, listing },
  session
) => {
  const { finnPublishInfo = {} } = body
  let { finnUpdateType } = finnPublishInfo || {}

  propertyHelper.validateDataForSharingAtFinn(body.partner, listing)
  await propertyHelper.validatePendingFinnRequest(
    { ...body, checkProcessFlow: true },
    session
  )

  if (size(listing?.description) >= 50000) {
    throw new CustomError(
      400,
      'Property description cannot be longer than 50000 characters'
    )
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
  if (!listing.finn) finnUpdateType = 'firstAd'
  if (finnUpdateType === 'firstAd' || finnUpdateType === 'update') {
    updateSet['finn.isPublishing'] = true
  } else if (finnUpdateType === 'republish') {
    updateSet['finn.isRePublishing'] = true
  }
  updateData['updateSet'] = updateSet
  updateData['updateUnset'] = updateUnset
  return updateData
}

export const preparePublishData = async ({ body, listing, session }) => {
  const { listed, finnPublishInfo = {} } = body
  const todayDate = new Date()

  todayDate.setHours(0, 0, 0, 0)
  if (listed && size(listing) && listing.availabilityStartDate < todayDate) {
    throw new CustomError(400, 'Availability dates of this listing is expire')
  }
  const data = {}
  const setData = { listed }
  const unSetData = {}
  if (listed && finnPublishInfo?.isShareAtFinn) {
    const finnInfo = await prepareListingDataForSharingToFinn(
      {
        body,
        listing
      },
      session
    )
    if (size(finnInfo.updateSet)) assign(setData, finnInfo.updateSet)
    if (size(finnInfo.updateUnset)) assign(unSetData, finnInfo.updateUnset)
  }
  if (listed && !listing.listed) setData.listedAt = new Date()
  if (!listed && listing.listed) setData.disabledAt = new Date()
  if (size(setData)) data['$set'] = setData
  if (size(unSetData)) data['$unset'] = unSetData
  console.log('readFinndata: ', data)
  return data
}

export const prepareUpdateData = async (params = {}, session) => {
  const { body } = params
  console.log('Checking body to update: ', body)
  const { addonId, agentId, imageName, updateType } = body
  const updateDataObj = {
    addAddon: prepareAddAddonData,
    deleteAddon: { $pull: { addons: { addonId } } },
    deleteListing: { $set: { deleted: true, liveThere: false } }, // User can not live there, if the listing is deleted,
    listingInfo: prepareListingInfo,
    removeFromFinn: prepareRemoveFromFinn,
    publish: preparePublishData,
    removeImage: { $pull: { images: { imageName } } },
    updateAvailableBedrooms: prepareAvailableBedroomData,
    updateAvailableEndDate: { $unset: { availabilityEndDate: 1 } },
    updatePropertyStatus: preparePropertyStatusData,
    updatePropertyOwner: { $set: { agentId, ownerId: agentId } }, // Should update agentId along with ownerId
    updatePropertyOwnerAndBranch: prepareOwnerAndBranchUpdateData
  }
  let updateData = updateDataObj[updateType] || {}
  if (typeof updateData === 'function') {
    updateData = await updateData(params, session)
  }
  return updateData
}
export const isUpdateListingPlaceIds = (updatedListing, previousListing) =>
  previousListing &&
  previousListing.location &&
  updatedListing.location &&
  updatedListing.location.name !== previousListing.location.name

export const isUpdateListingBasePrice = (updatedListing, previousListing) =>
  updatedListing.monthlyRentAmount !== previousListing.monthlyRentAmount

export const isUpdateNoOfBedRooms = (updatedListing, previousListing) =>
  updatedListing.listingTypeId &&
  previousListing.listingTypeId &&
  updatedListing.listingTypeId !== previousListing.listingTypeId

export const isUpdateCurrency = (location, previousListing) =>
  location &&
  previousListing.location &&
  location.countryShortName !== previousListing.location.countryShortName

export const isUpdateContractOwner = async (
  updatedListing,
  previousListing,
  session
) => {
  const { agentId, partnerId } = updatedListing
  const isDirectPartner = !!(await partnerHelper.getDirectPartnerById(
    partnerId,
    session
  ))
  return agentId !== previousListing.agentId && isDirectPartner
}

export const isUpdateAccountsData = (updatedListing, previousListing) => {
  const { partnerId, accountId, hasActiveLease, hasAssignment } = updatedListing
  return (
    partnerId &&
    accountId &&
    (hasActiveLease !== previousListing.hasActiveLease ||
      hasAssignment !== previousListing.hasAssignment)
  )
}

export const isUpdateListedAt = (updatedListing, previousListing) =>
  updatedListing.listed === true &&
  previousListing.listed === false &&
  !updatedListing.listedAt

export const isAddPropertyStatusChangeLog = (updatedListing, previousListing) =>
  updatedListing.propertyStatus !== previousListing.propertyStatus

export const getPropertyChangedFields = (updatedListing, previousListing) => {
  const changedFields = []
  if (previousListing.location && updatedListing.location) {
    if (
      previousListing.location.name &&
      updatedListing.location.name &&
      previousListing.location.name !== updatedListing.location.name
    ) {
      changedFields.push({ fieldName: 'address' })
    }
    if (
      (previousListing.location.postalCode ||
        updatedListing.location.postalCode) &&
      previousListing.location.postalCode !== updatedListing.location.postalCode
    ) {
      changedFields.push({ fieldName: 'postalCode' })
    }
    if (
      (previousListing.location.city || updatedListing.location.city) &&
      previousListing.location.city !== updatedListing.location.city
    ) {
      changedFields.push({ fieldName: 'city' })
    }
    if (
      (previousListing.location.country || updatedListing.location.country) &&
      previousListing.location.country !== updatedListing.location.country
    ) {
      changedFields.push({ fieldName: 'country' })
    }
  }
  if (
    updatedListing.groupId &&
    previousListing.groupId !== updatedListing.groupId
  ) {
    changedFields.push({ fieldName: 'groupId' })
  }
  if (previousListing.agentId !== updatedListing.agentId) {
    changedFields.push({ fieldName: 'agentId' })
  }
  if (previousListing.branchId !== updatedListing.branchId) {
    changedFields.push({ fieldName: 'branchId' })
  }
  return changedFields
}

export const prepareBasicChangeLogData = (updatedListing) => {
  const { partnerId, _id, agentId, branchId } = updatedListing
  return {
    partnerId,
    agentId,
    branchId,
    propertyId: _id,
    context: 'property',
    action: 'updated_property',
    isChangeLog: true,
    visibility: ['property']
  }
}

export const prepareChangesArray = (
  changedFields,
  updatedListing,
  previousDoc
) => {
  const changesArray = []
  each(changedFields, (field) => {
    let type = 'text'
    const fieldName = field.fieldName || ''
    let oldText = ''
    let newText = ''

    if (fieldName === 'address') {
      oldText = previousDoc ? previousDoc.location.name : ''
      newText = updatedListing.location.name
    } else if (fieldName === 'postalCode') {
      oldText = previousDoc ? previousDoc.location.postalCode : ''
      newText = updatedListing.location.postalCode
    } else if (fieldName === 'city') {
      oldText = previousDoc ? previousDoc.location.city : ''
      newText = updatedListing.location.city
    } else if (fieldName === 'country') {
      oldText = previousDoc ? previousDoc.location.country : ''
      newText = updatedListing.location.country
    } else if (fieldName === 'agentId') {
      type = 'foreignKey'

      oldText = previousDoc.agentId || ''
      newText = updatedListing.agentId || ''
    } else if (fieldName === 'branchId') {
      type = 'foreignKey'

      oldText = previousDoc.branchId || ''
      newText = updatedListing.branchId || ''
    } else {
      oldText = previousDoc ? previousDoc[fieldName] : ''
      newText = updatedListing[fieldName]
    }

    changesArray.push({
      field: fieldName,
      type,
      oldText,
      newText
    })
  })
  return changesArray
}

export const prepareChangeLogData = (
  updatedListing,
  previousListing,
  changedFields
) => {
  const logData = prepareBasicChangeLogData(updatedListing, previousListing)
  if (isArray(changedFields)) {
    logData.changes = prepareChangesArray(
      changedFields,
      updatedListing,
      previousListing
    )
    return logData
  }
}

export const prepareAdminListingsQueryBasedOnFilters = (query) => {
  const listingsQuery = updateQueryByCommonFilter(query) // common filters for find home and admin listing
  listingsQuery.deleted = { $ne: true }
  return listingsQuery
}

export const updateQueryByCommonFilter = (query) => {
  const {
    address,
    baseMonthlyRent,
    bedroom,
    city,
    createdDateRange,
    facilityIds,
    furnished,
    has360Url = false,
    hasVideoUrl = false,
    includedInRentIds,
    leaseAvailabilityDateRange,
    listedToFinn,
    listingTypeIds,
    mapPosition,
    minimumStay,
    monthlyRent,
    propertyTypeIds,
    space,
    status
  } = query

  // Status filter is also required for public site
  if (status === 'listed') {
    query.listed = true
  } else if (status === 'unlisted') {
    query.listed = false
  }

  if (size(listingTypeIds)) query.listingTypeId = { $in: listingTypeIds }
  if (size(propertyTypeIds)) query.propertyTypeId = { $in: propertyTypeIds }
  // Set price budget filters in query
  if (size(monthlyRent)) {
    query.monthlyRentAmount = {
      $gte: monthlyRent.minimum,
      $lte: monthlyRent.maximum
    }
  }
  if (size(baseMonthlyRent)) {
    query.baseMonthlyRentAmount = {
      $gte: baseMonthlyRent.minimum,
      $lte: baseMonthlyRent.maximum
    }
  }
  // Set space filters in query
  if (size(space)) {
    query.placeSize = {
      $gte: space.minimum,
      $lte: space.maximum
    }
  }
  // Set bedroom filters in query
  if (size(bedroom)) {
    if (includes(bedroom, 11)) {
      query['$or'] = [
        { noOfBedrooms: { $gt: 10 } },
        { noOfBedrooms: { $in: bedroom } }
      ]
    } else {
      query.noOfBedrooms = { $in: bedroom }
    }
  }
  // Set listedToFinn filters in query
  if (listedToFinn) query.addedInFinn = true
  // Set availability dates filters in query
  if (size(leaseAvailabilityDateRange)) {
    query.availabilityStartDate = {
      $gte: leaseAvailabilityDateRange.startDate,
      $lte: leaseAvailabilityDateRange.endDate
    }
  }
  //Set city filters in query
  if (city)
    query['location.city'] = { $regex: new RegExp('.*' + city + '.*', 'i') }
  //Set createdDateRange filters in query
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: createdDateRange.startDate,
      $lte: createdDateRange.endDate
    }
  }
  if (address) {
    query['location.name'] = { $regex: new RegExp('.*' + address + '.*', 'i') }
  }
  if (size(furnished) && size(furnished) !== 3) {
    // furnished == furnished, unfurnished, partially_furnished, then no need furnished to add on query
    query.furnished = { $in: furnished }
  } else delete query.furnished
  if (size(facilityIds)) query.facilities = { $all: facilityIds }
  if (size(includedInRentIds))
    query.includedInRent = { $all: includedInRentIds }
  if (minimumStay) {
    query['minimumStay'] = {
      $gte: minimumStay.minimum,
      $lte: minimumStay.maximum
    }
  }
  if (hasVideoUrl) query.videoUrl = { $nin: [null, ''] }
  if (has360Url) query.view360Url = { $nin: [null, ''] }

  if (size(mapPosition)) {
    const { latL, lngL, latR, lngR } = mapPosition
    query['location.lat'] = { $gte: latL, $lte: latR }
    query['location.lng'] = { $gte: lngL, $lte: lngR }
  }

  const listingsQuery = omit(query, [
    'address',
    'baseMonthlyRent',
    'bedroom',
    'city',
    'createdDateRange',
    'facilityIds',
    'has360Url',
    'hasVideoUrl',
    'includedInRentIds',
    'leaseAvailabilityDateRange',
    'listedToFinn',
    'listingTypeIds',
    'mapPosition',
    'monthlyRent',
    'propertyTypeIds',
    'space',
    'status'
  ])
  return listingsQuery
}

export const prepareListingsQueryBasedOnFilters = (query) => {
  const listingsQuery = updateQueryByCommonFilter(query) // Common filters for find home and admin listing
  const { availableFrom, bedroomRange, deleted, partnerId = '' } = query
  if (partnerId) listingsQuery.partnerId = partnerId

  console.log('Checking listingsQuery ', listingsQuery)
  const andQuery = []
  if (deleted === false) listingsQuery.deleted = { $ne: true }
  if (size(availableFrom)) {
    const orQuery = []
    for (const date of availableFrom) {
      orQuery.push({
        availabilityStartDate: {
          $gte: date.startDate,
          $lte: date.endDate
        }
      })
    }
    //Since we used or query for bed room filter previously
    andQuery.push({ $or: orQuery })
    delete listingsQuery.availableFrom
  }
  if (size(bedroomRange)) {
    const { minimum, maximum } = bedroomRange
    listingsQuery.noOfBedrooms = {
      $gte: minimum,
      $lte: maximum
    }
    delete listingsQuery.bedroomRange
  }
  if (size(andQuery)) listingsQuery['$and'] = andQuery
  return listingsQuery
}

export const getListingsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const listings = await ListingCollection.find(query)
    .populate(['account', 'agent', 'branch', 'owner', 'partner'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return listings
}

export const countListings = async (query, session) => {
  const numberOfListings = await ListingCollection.countDocuments(
    query
  ).session(session)
  return numberOfListings
}

export const countFavouritesByUser = async (userId, session) => {
  const numberOfFavourites = await ListingCollection.countDocuments({
    favorite: userId,
    listed: true,
    deleted: {
      $ne: true
    }
  }).session(session)
  return numberOfFavourites
}

export const getListingImageUrls = (listing = {}) => {
  const { _id, images = [] } = listing
  const { folder } = settingJson.S3.Directives['Listings'] // Get image directory from settings
  const domain = appHelper.getCDNDomain(process.env.STAGE)
  if (!size(images)) images.push({ imageName: '' })
  return images.map((image) => {
    const { imageName } = image || {}
    image.url = imageName
      ? `${domain}/${folder}/${_id}/${imageName}`
      : `${domain}/assets/default-image/property-secondary.png`
    return image
  })
}

export const queryListings = async (req) => {
  const { body, user = {} } = req
  const { query = {}, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { userId } = user
  if (userId && query.myFavourite) {
    appHelper.validateId({ userId })
    query.favorite = userId
    delete query.myFavourite
  }
  body.query = prepareListingsQueryBasedOnFilters(query)
  body.userId = userId
  const result = await getDataAndMetaDataForListings(body, { listed: true }) // returns data and meta data, counts only listed
  return result
}

export const isUpdateDisabledAt = (updatedLisiting, previousListing) =>
  previousListing.listed && !updatedLisiting.listed

export const getMaxPropertySerial = async (partnerId) => {
  const maxPropertySerial = await ListingCollection.aggregate([
    { $match: { partnerId } },
    { $group: { _id: null, maxSerial: { $max: '$serial' } } }
  ])
  return maxPropertySerial
}
//for lambda listings-bridge-finn
const getPropertyFacilities = async (propertyInfo) => {
  const facilities = []
  if (size(propertyInfo)) {
    const facilitiesDetails = await propertyInfo.facilitiesDetails()

    if (size(facilitiesDetails)) {
      each(facilitiesDetails, function (facility) {
        if (facility.name) {
          facilities.push(
            appHelper.translateToUserLng(
              'listings.fields.facilities.' + facility.name,
              'no'
            )
          )
        }
      })
    }

    if (size(propertyInfo.furnished))
      facilities.push(
        appHelper.translateToUserLng(
          'listings.fields.facilities.' + propertyInfo.furnished,
          'no'
        )
      )
    else
      facilities.push(
        appHelper.translateToUserLng(
          'listings.fields.facilities.unfurnished',
          'no'
        )
      )
  }
  return facilities
}

const includedInRent = (includeds) => {
  if (size(includeds)) {
    const rentIncludes = []
    includeds.forEach((included) => {
      rentIncludes.push(
        appHelper.translateToUserLng('listings.includes.' + included.name, 'no')
      )
    })

    const includeString = rentIncludes.toString()
    const replace = new RegExp(',', 'g')

    return includeString.replace(replace, ', ')
  } else return ''
}

const getListingImages = (propertyInfo) => {
  const finnListingImages = propertyInfo.getListingImages()
  if (size(propertyInfo) && size(finnListingImages)) {
    if (size(finnListingImages) > 50) {
      const imageInfos = []
      each(finnListingImages, function (listingImage, index) {
        if (index < 50) imageInfos.push(listingImage)
      })
      return imageInfos
    } else return finnListingImages
  }
}

const getBrokerInfo = async (propertyInfo) => {
  if (size(propertyInfo) && size(propertyInfo.ownerId)) {
    const ownerInfo = await userHelper.getUserById(propertyInfo.ownerId)
    if (size(ownerInfo)) {
      return {
        Name: ownerInfo?.getName(),
        Phone: ownerInfo?.getPhone(),
        Email: ownerInfo?.getEmail()
      }
    }
  }
}

const getFinnDataForSharingAd = async (body) => {
  const { propertyId, userId, type } = body
  if (size(propertyId) && size(userId)) {
    const propertyInfo = await getListingById(propertyId, undefined, [
      'partner',
      'owner'
    ])
    const userInfo = await userHelper.getUserById(userId)
    const partnerId =
      size(propertyInfo) && size(propertyInfo.partnerId)
        ? propertyInfo.partnerId
        : ''
    const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
      partnerId
    )
    let serial =
      size(propertyInfo) && size(propertyInfo.finn) && propertyInfo.finn.serial
        ? propertyInfo.finn.serial
        : ''

    if (size(propertyInfo)) {
      if (type === 'republish') {
        if (!serial) serial = 1
        else serial = serial + 1
      }
      const setting = await settingHelper.getSettingInfo()
      const propertyType = getPropertyTypeNameById(
        propertyInfo.propertyTypeId,
        setting
      )
      return {
        ORDERNO: propertyInfo._id,
        USER_REFERENCE: size(userInfo) && userInfo.profile?.name,
        FROMDATE: (
          await appHelper.getActualDate(partnerSetting, true, new Date())
        ).format('DD.MM.YYYY'),
        TODATE: propertyInfo.availabilityEndDate
          ? (
              await appHelper.getActualDate(
                partnerSetting,
                true,
                propertyInfo.availabilityEndDate
              )
            ).format('DD.MM.YYYY')
          : '',
        STREETADDRESS:
          size(propertyInfo.location) && size(propertyInfo.location.name)
            ? propertyInfo.location.name
            : '',
        ZIPCODE:
          size(propertyInfo.location) && size(propertyInfo.location.postalCode)
            ? propertyInfo.location.postalCode
            : '',
        COUNTRYCODE: 'NO', //propertyInfo.location && propertyInfo.location.countryShortName ? propertyInfo.location.countryShortName : '',
        HEADING: size(propertyInfo.title) ? propertyInfo.title : '',
        ESTATE_PREFERENCE: await getPropertyFacilities(propertyInfo),
        PROPERTY_TYPE: propertyType
          ? appHelper.translateToUserLng(
              'listing_and_property_types.' + propertyType,
              'no'
            )
          : '',
        NO_OF_BEDROOMS: propertyInfo.noOfBedrooms
          ? propertyInfo.noOfBedrooms
          : '',
        PRIMARY_ROOM_AREA: propertyInfo.placeSize || '',
        PRIMARY_ROOM_DESCRIPTION: '',
        LIVING_AREA: '',
        GROSS_AREA: '',
        FLOOR_AREA: '',
        USEABLE_AREA: '',
        FLOOR: propertyInfo.floor ? propertyInfo.floor : '',
        AREA: '',
        CONDITION: '',
        GROUND_TAX: '',
        GROUND_YEAR: '',
        PER_YEAR: '',
        PER_MONTH: propertyInfo.monthlyRentAmount
          ? propertyInfo.monthlyRentAmount
          : 0,
        PER_WEEK: '',
        PER_DAY: '',
        DEPOSIT: propertyInfo.depositAmount ? propertyInfo.depositAmount : 0,
        CURRENCY: size(propertyInfo.currency) ? propertyInfo.currency : 'NOK',
        INCLUDES: includedInRent(await propertyInfo.includedInRentDetails()),
        GENERAL_HEADING: size(propertyInfo.description)
          ? appHelper.translateToUserLng(
              'listing_preview.about_listing_title',
              'no'
            )
          : '',
        GENERAL_TEXT: size(propertyInfo.description)
          ? propertyInfo.description
          : '',
        RENTFROM: propertyInfo.availabilityStartDate
          ? (
              await appHelper.getActualDate(
                partnerSetting,
                true,
                propertyInfo.availabilityStartDate
              )
            ).format('DD.MM.YYYY')
          : '',
        RENTTO: propertyInfo.availabilityEndDate
          ? (
              await appHelper.getActualDate(
                partnerSetting,
                true,
                propertyInfo.availabilityEndDate
              )
            ).format('DD.MM.YYYY')
          : '',

        listing_images: getListingImages(propertyInfo),
        keys: 'listings/'.concat(propertyInfo._id),
        finnId: size(propertyInfo.partner) && propertyInfo.partner.finnId,
        BrokerInfo: await getBrokerInfo(propertyInfo),
        type,
        serial: serial ? serial : '',
        propertyId
      }
    }
  }
}

const getFinnDataForRemovingAd = async (propertyId) => {
  const propertyInfo = await getListingById(propertyId, undefined, 'partner')
  if (size(propertyInfo)) {
    return {
      propertyId,
      type: 'inactive',
      serial:
        size(propertyInfo.finn) && propertyInfo.finn.serial
          ? propertyInfo.finn.serial
          : '',
      finnId: propertyInfo.partner?.finnId
    }
  }
}

const getFinnForQuery = async (sqsDoc) => {
  const { propertyId, userId, type = '', processType } = sqsDoc.params
  let content = {}
  if (processType === 'share') {
    content = await getFinnDataForSharingAd({ propertyId, userId, type })
  } else if (processType === 'remove') {
    content = await getFinnDataForRemovingAd(propertyId)
  } else {
    throw new CustomError(500, 'Server Error, processType not found')
  }
  return content
}

export const queryFinn = async (req) => {
  const { body, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['queueId'], body)
  const { queueId = '' } = body
  appHelper.validateId({ queueId })
  const queue = (await appQueueHelper.getQueueItemById(queueId)) || {}
  const { action = '' } = queue
  if (action === 'handle_finn_listing') {
    const content = await getFinnForQuery(queue)
    return content
  } else {
    throw new CustomError(400, 'Incorrect action type')
  }
}

export const prepareFinnMessages = (data) => {
  const fatalError = []
  const errors = []
  const warnings = []
  const info = []
  const finnMessages = {}
  if (data && data.OBJECT && data.OBJECT.length) {
    each(data.OBJECT[0].ERRORMESSAGE, function (errorType) {
      const elementName = errorType.ELEMENTNAME
      const message = errorType.MESSAGE
      const errorDoc = {
        elementName: elementName.toString(),
        message: message.toString()
      }

      if (errorType.$.ERRORLEVEL === 'fatal') fatalError.push(errorDoc)
      if (errorType.$.ERRORLEVEL === 'error') errors.push(errorDoc)
      if (errorType.$.ERRORLEVEL === 'warning') warnings.push(errorDoc)
      if (errorType.$.ERRORLEVEL === 'info') info.push(errorDoc)
    })
  }
  if (size(fatalError)) finnMessages.fatal = fatalError
  if (size(errors)) finnMessages.errorsMeta = errors
  if (size(warnings)) finnMessages.warnings = warnings
  if (size(info)) finnMessages.info = info
  return finnMessages
}

export const prepareFinnDataForUpdateListing = (body) => {
  const { listingInfo, finnData, zipFileResin } = body
  const data = finnData['IAD.IF.OBJECTRESPONSE']
  let finnUpdateData = {}
  const updateData = {}
  const finnMessages = prepareFinnMessages(data)
  if (
    size(data) &&
    size(data.OBJECT) &&
    size(data.OBJECT[0]) &&
    size(data.OBJECT[0].URL) &&
    size(data.OBJECT[0].URL[0])
  ) {
    const finnUpdateTime =
      size(listingInfo.finn) && size(listingInfo.finn.adUpdateTimes)
        ? listingInfo.finn.adUpdateTimes
        : []
    const finnData = {}

    finnUpdateTime.push(new Date())
    finnData['finn.messages'] = finnMessages

    if (zipFileResin === 'inactive') {
      console.log('zipFileResin propertyId: ', listingInfo._id)
      if (
        size(listingInfo.finn) &&
        listingInfo.finn.updateType === 'republish'
      ) {
        updateData['$unset'] = {
          'finn.finnArchivedAt': 1,
          'finn.updateType': 1,
          'finn.isArchiving': 1
        }
        finnData['finn.finnShareAt'] = new Date()
      } else {
        finnData['finn.disableFromFinn'] = true
        finnData['finn.isShareAtFinn'] = false
        updateData['$unset'] = {
          'finn.isArchiving': 1
        }
      }
    } else {
      finnData['finn.isShareAtFinn'] = true
      finnData['finn.adURL'] = data.OBJECT[0].URL[0]
      finnData['finn.statisticsURL'] =
        data.OBJECT[0].STATISTICS_URL && data.OBJECT[0].STATISTICS_URL[0]
          ? data.OBJECT[0].STATISTICS_URL[0]
          : ''
      finnData['finn.adSendTime'] = new Date()
      finnData['finn.adUpdateTimes'] = finnUpdateTime
      if (size(listingInfo.finn) && listingInfo.finn.disableFromFinn) {
        finnData['finn.disableFromFinn'] = false
      }
      updateData['$unset'] = {
        'finn.isPublishing': 1
      }
    }
    if (zipFileResin === 'republish') {
      const serial =
        size(listingInfo.finn) && listingInfo.finn.serial
          ? listingInfo.finn.serial + 1
          : 1
      if (serial) finnData['finn.serial'] = serial
      const serialHistory = {
        orderSerial: serial,
        statisticsURL:
          size(listingInfo.finn) && listingInfo.finn.statisticsURL
            ? listingInfo.finn.statisticsURL
            : '',
        createdAt: new Date()
      }
      updateData['$push'] = { 'finn.serialHistory': { $each: [serialHistory] } }
      updateData['$unset'] = { 'finn.isRePublishing': 1 }
    }
    if (size(finnMessages?.errorsMeta)) {
      finnData['finn.isShareAtFinn'] = false
    }
    updateData['$set'] = finnData
    finnUpdateData = updateData
  } else {
    if (typeof finnData === 'string') {
      console.log('finn error request data:', finnData)
      updateData['finn.finnErrorRequest'] = finnData
    } else {
      updateData['finn.messages'] = finnMessages
    }

    finnUpdateData['$set'] = updateData
    console.log(
      'Something went wrong when share at finn, propertyId:',
      listingInfo._id
    )
  }
  return finnUpdateData
}

export const prepareDataForCreateLogOfUpdateOrFailFinn = (body) => {
  const { listingInfo, finnData, zipFileResin } = body
  const data = finnData['IAD.IF.OBJECTRESPONSE']
  let logUpdateData = {}
  const finnMessages = prepareFinnMessages(data)
  if (
    size(data) &&
    size(data.OBJECT) &&
    size(data.OBJECT[0]) &&
    size(data.OBJECT[0].URL) &&
    size(data.OBJECT[0].URL[0]) &&
    !size(finnMessages?.errorsMeta)
  ) {
    logUpdateData = getLogDataForFinn({
      event: 'publish_to_finn',
      listingInfo,
      zipFileResin
    })
  } else {
    logUpdateData = getLogDataForFinn({
      event: 'failed_to_finn',
      listingInfo,
      error:
        size(finnMessages?.errorsMeta) && finnMessages.errorsMeta[0]
          ? finnMessages?.errorsMeta[0]
          : null
    })
  }
  return logUpdateData
}

const getLogDataForFinn = (params = {}) => {
  const { event, error, listingInfo, zipFileResin } = params

  const options = {
    partnerId: listingInfo.partnerId,
    collectionId: listingInfo._id,
    context: 'property',
    collectionName: 'listing',
    error
  }
  if (zipFileResin === 'inactive') options.isArchivedFinn = true
  if (zipFileResin === 'republish') options.isRepublished = true
  const logData = {
    partnerId: listingInfo.partnerId,
    context: options.context,
    action: event,
    propertyId: listingInfo._id
  }
  const preparedLogData = prepareUpdateOrFailLogOfListingFinn(
    listingInfo,
    logData,
    options
  )
  return preparedLogData
}

const prepareUpdateOrFailLogOfListingFinn = (
  collectionData,
  logData,
  options
) => {
  const newLogData = pick(collectionData, [
    'accountId',
    'agentId',
    'branchId',
    'contractId',
    'invoiceId',
    'payoutId',
    'propertyId',
    'taskId',
    'tenantId'
  ])
  logData = assign(logData, newLogData)
  logData.visibility = logHelper.getLogVisibility(options, collectionData)
  const metaData = getListingMetaData(collectionData, options, logData)
  logData.meta = metaData
  return logData
}

const getListingMetaData = (collectionData, options, logData) => {
  let metaData = []

  if (options.isArchivedFinn) {
    metaData = [{ field: 'archivedOnFinn', value: '1' }]
  } else if (
    logData.action === 'failed_to_finn' &&
    collectionData &&
    collectionData.finn &&
    collectionData.finn.finnErrorRequest
  ) {
    metaData = [
      { field: 'finnError', value: collectionData.finn.finnErrorRequest }
    ]
  } else if (options.isRepublished) {
    metaData = [{ field: 'republishedToFinn', value: '1' }]
  } else if (
    logData.action === 'publish_to_finn' &&
    collectionData &&
    collectionData.finn &&
    size(collectionData.finn.adUpdateTimes) > 1
  ) {
    metaData = [{ field: 'updateOnFinn', value: '1' }]
  } else if (
    logData.action === 'failed_to_finn' &&
    size(options?.error?.message)
  ) {
    metaData = [{ field: 'finnError', value: options.error.message }]
  }
  if (size(metaData)) return metaData
}

export const validateListingUpdateData = (data = {}) => {
  appHelper.compactObject(data)
  appHelper.checkRequiredFields(['_id', 'updateType'], data)
  const { _id, addonId, agentId, branchId, partnerId, updateType } = data
  appHelper.validateId({ _id })
  addonId && appHelper.validateId({ addonId })
  agentId && appHelper.validateId({ agentId })
  branchId && appHelper.validateId({ branchId })
  partnerId && appHelper.validateId({ partnerId })
  const updateTypeFields = {
    addAddon: ['partnerId', 'addonId', 'isRecurring', 'price', 'total', 'type'],
    deleteAddon: ['addonId', 'partnerId'],
    deleteListing: [],
    listingInfo: ['listingInfo'],
    increasePageView: [],
    removeFromFinn: ['finnUpdateType'],
    publish: ['listed'],
    removeImage: ['imageName'],
    updateAvailableBedrooms: ['bedroomId'],
    updateAvailableEndDate: [],
    updateFavorite: ['favorite'],
    updatePropertyInfo: [],
    updatePropertyStatus: ['partnerId', 'propertyStatus'],
    updatePropertyOwner: ['agentId', 'partnerId'],
    updatePropertyOwnerAndBranch: ['agentId', 'branchId', 'partnerId']
  }
  const requiredFields = updateTypeFields[updateType]
  if (!requiredFields) {
    throw new CustomError(400, 'Invalid updateType')
  }
  appHelper.checkRequiredFields(requiredFields, data)
}

// Returns only domain of url
export const getDomainName = (url = '', isIncludeSubDomain = false) => {
  let domainName = ''
  if (isIncludeSubDomain) {
    domainName = url
      .replace('http://', '')
      .replace('https://', '')
      .split(/[/?#]/)[0]
  } else {
    domainName = url.replace(/^[^.]+\./g, '').split(/[/?#]/)[0]
  }
  return domainName
}

export const isUrlDomainWhiteListed = async (url, session) => {
  const query = { partnerId: { $exists: false } }
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(
    query,
    session
  )
  let { allowedDomains = [] } = partnerSetting || {}
  const urlDomain = getDomainName(url, false)
  const urlWithSubDomain = getDomainName(url, true)
  allowedDomains = allowedDomains.map((domain) => getDomainName(domain, true))
  return (
    allowedDomains.includes(urlDomain) ||
    allowedDomains.includes(urlWithSubDomain)
  )
}

export const isAddHistoryInContract = (updatedListing, previousListing) => {
  const isLocationChanged = contractHelper.isLocationChanged(
    updatedListing,
    previousListing
  )
  const isGnrBnrChanged = contractHelper.isGnrBnrChanged(
    updatedListing,
    previousListing
  )
  const isListingTypeChanged = contractHelper.isListingTypeChanged(
    updatedListing,
    previousListing
  )
  const isPropertyTypeChanged = contractHelper.isPropertyTypeChanged(
    updatedListing,
    previousListing
  )
  return (
    isLocationChanged ||
    isGnrBnrChanged ||
    isListingTypeChanged ||
    isPropertyTypeChanged
  )
}

export const getListingPreview = async (query) => {
  const listingPreview = await getAListing(query, null, ['owner'])
  if (!size(listingPreview)) {
    throw new CustomError(404, 'Could not find listing for preview')
  }
  return listingPreview
}

export const queryListingPreview = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['_id'], body)
  appHelper.validateId(body)
  const { _id, incrementPageView = true } = body
  const query = { _id }
  let listingPreview = await getListingPreview(query)
  if (size(listingPreview)) {
    if (
      listingPreview.ownerId &&
      listingPreview.ownerId !== user.userId &&
      incrementPageView
    ) {
      listingPreview = await listingService.updateAListing(
        {
          _id: listingPreview._id
        },
        {
          $inc: {
            pageView: 1
          }
        }
      )
    }
  }
  listingPreview = JSON.parse(JSON.stringify(listingPreview))
  const { owner } = listingPreview
  listingPreview.images = getListingImageUrls(listingPreview)
  if (size(owner)) {
    owner.profile = {
      ...owner.profile,
      avatar: userHelper.getAvatar(owner) || ''
    }
  }
  return listingPreview
}

export const getListingInfoForDashboard = async (
  query = {},
  partnerType = ''
) => {
  const pipeline = []
  const match = { $match: query }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: {
      _id: null,
      countedFinnListings: {
        $sum: {
          $cond: {
            if: { $eq: ['$finn.isShareAtFinn', true] },
            then: 1,
            else: 0
          }
        }
      },
      countedUniteListings: {
        $sum: {
          $cond: { if: { $eq: ['$listed', true] }, then: 1, else: 0 }
        }
      },
      countedTotalListings: { $sum: 1 }
    }
  }
  pipeline.push(group)
  const [listingInfo] = await ListingCollection.aggregate(pipeline)
  return listingInfo
}

export const getDataAndMetaDataForListings = async (body, totalQuery = {}) => {
  let listings = await getListingsForQuery(body)
  const filteredDocuments = await countListings(body.query)
  const totalDocuments = await countListings(totalQuery) // Should not change this field
  const totalFavourites = await countFavouritesByUser(body.userId)

  listings = JSON.parse(JSON.stringify(listings)) // Deleting this line will raise an error or unexpected result
  listings.forEach((listing) => {
    const { agent, owner } = listing
    listing.images = getListingImageUrls(listing)
    if (size(agent)) {
      listing.agent.profile = {
        ...agent.profile,
        avatar: userHelper.getAvatar(agent) || ''
      }
    }
    if (size(owner)) {
      listing.owner.profile = {
        ...owner.profile,
        avatar: userHelper.getAvatar(owner) || ''
      }
    }
    return listing
  })
  return {
    data: listings,
    metaData: { filteredDocuments, totalDocuments, totalFavourites }
  }
}

export const queryListingsForAppAdmin = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = prepareAdminListingsQueryBasedOnFilters(query)
  const result = await getDataAndMetaDataForListings(body, {}) // returns data and meta data
  return result
}

export const getListingsUniqueCities = async () => {
  const cities = await ListingCollection.aggregate([
    {
      $group: {
        _id: {
          $toLower: '$location.city'
        },
        name: {
          $first: '$location.city'
        }
      }
    },
    {
      $sort: {
        name: 1
      }
    }
  ])
  const uniqueCities = []
  for (const city of cities) {
    uniqueCities.push(city.name)
  }
  return uniqueCities
}

export const getPublicListingsQueryFilters = async (req) => {
  const { query, options } = req
  const { sort, skip, limit } = options

  const pipeline = []

  const match = {
    $match: query
  }
  console.log('Checking query ', query)
  console.log('Checking match query ', match)

  const favourite = {
    $addFields: {
      noOfFavorite: {
        $cond: {
          if: { $ifNull: ['$favorite', false] },
          then: {
            $size: '$favorite'
          },
          else: 0
        }
      }
    }
  }

  const project = {
    $project: {
      _id: 1,
      title: 1,
      location: 1,
      noOfBedrooms: 1,
      monthlyRentAmount: 1,
      noOfFavorite: 1,
      pageView: 1,
      listedAt: 1,
      finn: 1,
      listed: 1,
      baseMonthlyRentAmount: 1,
      availabilityStartDate: 1,
      availabilityEndDate: 1,
      description: 1,
      propertyStatus: 1,
      propertyTypeId: 1,
      listingTypeId: 1,
      images: 1,
      createdAt: 1,
      updatedAt: 1,
      currency: 1,
      apartmentId: 1,
      placeSize: 1,
      floor: 1
    }
  }
  pipeline.push(match)
  pipeline.push(favourite)
  pipeline.push(project)
  console.log('favourite ', favourite)
  console.log('project ', project)

  pipeline.push({ $sort: sort }, { $skip: skip }, { $limit: limit })
  const listingData = await ListingCollection.aggregate([pipeline])
  console.log('listingData ', listingData)
  return listingData
}

const prepareQueryForListingsForPublicSite = (query) => {
  const { userId, isListed, searchAddress } = query
  if (userId) query.ownerId = userId
  if (query.hasOwnProperty('isListed')) {
    query.listed = isListed
  }
  query.deleted = {
    $ne: true
  }
  if (searchAddress) {
    const searchAddressExpr = new RegExp('.*' + searchAddress + '.*', 'i')
    query['$or'] = [
      { title: searchAddressExpr },
      { 'location.name': searchAddressExpr }
    ]
  }
  return omit(query, ['userId', 'isListed', 'searchAddress'])
}

export const queryListingsForPublicSite = async (req) => {
  const { body, user = {} } = req
  const { query = {}, options } = body
  const { userId = '', partnerId = '' } = user

  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkUserId(userId)
  if (user.defaultRole && user.defaultRole === 'landlord') {
    const account = await accountHelper.getAnAccount({
      personId: userId,
      partnerId
    })
    if (!account?._id) throw new CustomError(404, 'Landlord not found')
  }
  appHelper.validateSortForQuery(options.sort)
  query.userId = userId
  const preparedQuery = prepareQueryForListingsForPublicSite(query)
  console.log('Checking preparedQuery ', preparedQuery)
  if (partnerId) preparedQuery.partnerId = partnerId
  else preparedQuery.partnerId = { $exists: false }
  const result = await getPublicListingsQueryFilters({
    query: preparedQuery,
    options
  })

  result.map((item) => {
    getListingImageUrls(item)
  })
  const metaData = {
    filteredDocuments: await countListings(preparedQuery),
    totalDocuments: await countListings({
      ownerId: userId
    })
  }
  return {
    data: result,
    metaData
  }
}

const prepareQueryForListingsDropdown = async (query = {}) => {
  const {
    accountId,
    agentId,
    branchId,
    contractStatus,
    hasActiveLease,
    hasUpcomingLease,
    partnerId,
    searchString,
    tenantId
  } = query
  const preparedQuery = {
    partnerId,
    propertyStatus: { $nin: ['archived', 'maintenance'] }
  }
  if (accountId) preparedQuery.accountId = accountId
  if (agentId) preparedQuery.agentId = agentId
  if (branchId) preparedQuery.branchId = branchId
  if (searchString)
    preparedQuery['location.name'] = new RegExp(searchString, 'i')
  if (tenantId) {
    const tenantInfo =
      (await tenantHelper.getATenant({
        _id: tenantId,
        partnerId
      })) || {}
    let propertyIds = []
    if (size(tenantInfo.properties)) {
      propertyIds = tenantInfo.properties.map((item) => item.propertyId)
    }
    preparedQuery._id = { $in: propertyIds }
  }
  if (hasActiveLease) {
    if (hasUpcomingLease) {
      preparedQuery['$or'] = [
        { hasActiveLease: true },
        { hasUpcomingLease: true }
      ]
    } else {
      preparedQuery.hasActiveLease = true
    }
  }
  if (contractStatus) {
    const propertyIds = await contractHelper.getUniqueFieldValue('propertyId', {
      status: contractStatus,
      partnerId
    })
    preparedQuery._id = { $in: propertyIds }
  }
  return preparedQuery
}

const prepareQueryDropdownForAddTenant = async (query = {}) => {
  const { partnerId, searchString } = query
  const preparedQuery = {
    partnerId,
    propertyStatus: { $ne: 'archived' }
  }
  const isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  if (isBrokerPartner) preparedQuery.hasAssignment = true
  if (searchString)
    preparedQuery['location.name'] = new RegExp(searchString, 'i')
  return preparedQuery
}

export const queryListingsDropdown = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const { query, options } = body
  query.partnerId = partnerId
  const preparedQuery = await prepareQueryForListingsDropdown(query)
  const listingsDropdownData = await getListingsDropdownForQuery({
    query: preparedQuery,
    options
  })
  const filteredDocuments = await countListings(preparedQuery)
  const totalDocuments = await countListings({ partnerId })

  return {
    data: listingsDropdownData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const listingsDropdownForAddTenant = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const { query } = body
  query.partnerId = partnerId
  const preparedQuery = await prepareQueryDropdownForAddTenant(query)
  const listingsDropdownData = await getListings(preparedQuery)
  return {
    data: listingsDropdownData
  }
}

export const getListingsDropdownForQuery = async (params = {}) => {
  const { query, options = {} } = params
  const { limit, skip } = options
  const listingsDropdownData = await ListingCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: {
        'location.name': 1
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...appHelper.getListingFirstImageUrl('$images'),
    {
      $project: {
        _id: 1,
        imageUrl: 1,
        location: 1,
        apartmentId: 1
      }
    }
  ])

  return listingsDropdownData
}

export const listingDetails = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['_id'], body)
  const { userId = '', roles, partnerId } = user
  const { _id, incrementPageView = true } = body
  appHelper.validateId({ _id })
  let listing = await getListingById(_id, undefined, 'owner')
  if (size(listing)) {
    const partnerInfo =
      (await partnerHelper.getPartnerById(listing.partnerId)) || {}
    if (listing.ownerId && listing.ownerId !== userId && incrementPageView) {
      listing.pageView = (listing.pageView || 0) + 1
      await listing.save()
    }
    listing = JSON.parse(JSON.stringify(listing))
    if (size(listing.owner)) {
      listing.owner.profile.avatarKey = userHelper.getAvatar(listing.owner)
    }
    if (listing.ownerId && !listing.listed) {
      validateListingOwner({
        listing,
        partnerId,
        roles,
        userId
      })
    }
    listing.images = getListingImageUrls(listing)
    if (size(partnerInfo)) {
      listing.partner = {
        name: partnerInfo.name
      }
    }
  } else {
    throw new CustomError(404, 'Listing not found')
  }
  return listing
}

export const validateListingOwner = (params = {}) => {
  const { listing, partnerId, roles = [], userId } = params
  if (
    !(
      appHelper.isAppAdmin(roles) ||
      ((roles.includes('partner_admin') || roles.includes('partner_agent')) &&
        listing.partnerId === partnerId)
    ) &&
    listing.ownerId !== userId
  ) {
    throw new CustomError(404, 'Listing not found')
  }
}

export const prepareDataForAddOrRemoveFromFavourite = ({ body, user }) => {
  const { favourite } = body
  const { userId } = user
  const updateData = {}
  if (favourite) {
    updateData['$addToSet'] = { favorite: userId }
  } else {
    updateData['$pull'] = { favorite: userId }
  }
  return updateData
}

const prepareQueryForListingsFilterCharts = (body = {}) => {
  const { city, listed, mapPosition } = body
  const minMaxQuery = {
    deleted: { $ne: true }
  }
  if (body.hasOwnProperty('listed')) minMaxQuery.listed = listed
  if (size(mapPosition)) {
    const { latL, lngL, latR, lngR } = mapPosition
    minMaxQuery['location.lat'] = { $gte: latL, $lte: latR }
    minMaxQuery['location.lng'] = { $gte: lngL, $lte: lngR }
  }
  if (city) minMaxQuery['location.city'] = new RegExp('.*' + city + '.*', 'i')
  return { minMaxQuery }
}

const getListingsFilterCharts = async (params) => {
  const { minMaxQuery } = params
  console.log('minMaxQuery ', minMaxQuery)
  const minMaxDataRow = await ListingCollection.aggregate([
    { $match: minMaxQuery },
    {
      $group: {
        _id: 'null',
        placeSizeTo: { $max: '$placeSize' },
        minimumStayFrom: { $min: '$minimumStay' },
        minimumStayTo: { $max: '$minimumStay' },
        noOfBedroomsFrom: { $min: '$noOfBedrooms' },
        noOfBedroomsTo: { $max: '$noOfBedrooms' },
        baseMonthlyRentAmountFrom: { $min: '$baseMonthlyRentAmount' },
        baseMonthlyRentAmountTo: { $max: '$baseMonthlyRentAmount' }
      }
    }
  ])
  const minMaxData = size(minMaxDataRow) ? minMaxDataRow[0] : {}
  if (size(minMaxData)) {
    let {
      baseMonthlyRentAmountTo,
      baseMonthlyRentAmountFrom,
      placeSizeTo,
      minimumStayTo
    } = minMaxData
    if (!baseMonthlyRentAmountTo) baseMonthlyRentAmountTo = 1000
    if (!placeSizeTo) placeSizeTo = 200
    if (!baseMonthlyRentAmountFrom) baseMonthlyRentAmountFrom = 0

    // Get the total monthly rent chart data
    if (baseMonthlyRentAmountTo - baseMonthlyRentAmountFrom < 100) {
      baseMonthlyRentAmountTo += 100
    }
    baseMonthlyRentAmountTo += 10
    minMaxData.baseMonthlyRentAmountFrom = baseMonthlyRentAmountFrom
    minMaxData.baseMonthlyRentAmountTo = baseMonthlyRentAmountTo

    const monthlyRentInterval = Math.round(
      (minMaxData.baseMonthlyRentAmountTo -
        minMaxData.baseMonthlyRentAmountFrom) /
        50
    )
    const monthlyRentProject = await prepareMonthlyRentQuery(
      minMaxData.baseMonthlyRentAmountFrom,
      minMaxData.baseMonthlyRentAmountTo,
      monthlyRentInterval
    )
    minMaxData.monthlyRentList = await ListingCollection.aggregate([
      { $match: minMaxQuery },
      {
        $project: {
          rentAmount: monthlyRentProject
        }
      },
      {
        $group: {
          _id: '$rentAmount',
          count: { $sum: 1 }
        }
      }
    ])

    // Get total stay chart data
    if (minimumStayTo - minMaxData.minimumStayFrom < 100) {
      minimumStayTo += 100
    }
    minMaxData.minimumStayTo = minimumStayTo

    const stayQuery = clone(minMaxQuery)

    stayQuery.minimumStay = { $exists: true }

    const stayInterval = Math.round(
      (minMaxData.minimumStayTo - minMaxData.minimumStayFrom) / 50
    )
    const stayProject = await prepareStayQuery(
      minMaxData.minimumStayFrom,
      minMaxData.minimumStayTo,
      stayInterval
    )
    minMaxData.stayList = await ListingCollection.aggregate([
      { $match: stayQuery },
      {
        $project: {
          stay: stayProject
        }
      },
      {
        $group: {
          _id: '$stay',
          count: { $sum: 1 }
        }
      }
    ])

    // Get the total size chart data
    if (placeSizeTo < 100) {
      placeSizeTo += 100
    }
    minMaxData.placeSizeTo = placeSizeTo
    minMaxData.placeSizeFrom = 0

    const sizeQuery = clone(minMaxQuery)

    sizeQuery.placeSize = { $exists: true }

    const placeSizeInterval = Math.round(
      (minMaxData.placeSizeTo - minMaxData.placeSizeFrom) / 50
    )
    const sizeProject = await prepareTotalSizeRangeQuery(
      minMaxData.placeSizeTo,
      minMaxData.placeSizeFrom,
      placeSizeInterval
    )

    minMaxData.sizeRangeList = await ListingCollection.aggregate([
      { $match: sizeQuery },
      {
        $project: {
          totalPlaceSize: sizeProject
        }
      },
      {
        $group: {
          _id: '$totalPlaceSize',
          count: { $sum: 1 }
        }
      }
    ])
  }
  //TODO: Also getting graph data from here
  console.log('minMaxData ', minMaxData)
  return { minMaxData }
}

const prepareTotalSizeRangeQuery = async (min, max, interval) => {
  const newMin = min + interval //max
  if (max <= min + interval) return min + '-' + newMin

  return {
    $cond: [
      {
        $and: [
          { $gte: ['$baseMonthlyRentAmount', min] },
          { $lt: ['$baseMonthlyRentAmount', newMin] }
        ]
      },
      min + '-' + newMin,
      await prepareTotalSizeRangeQuery(newMin, max, interval)
    ]
  }
}

const prepareMonthlyRentQuery = async (min, max, interval) => {
  const newMin = min + interval //max
  if (max <= min + interval) return min + '-' + newMin

  return {
    $cond: [
      {
        $and: [
          { $gte: ['$baseMonthlyRentAmount', min] },
          { $lt: ['$baseMonthlyRentAmount', newMin] }
        ]
      },
      min + '-' + newMin,
      await prepareMonthlyRentQuery(newMin, max, interval)
    ]
  }
}

const prepareStayQuery = async (min, max, interval) => {
  const newMin = min + interval //max
  if (max <= min + interval) return min + '-' + newMin
  return {
    $cond: [
      {
        $and: [
          { $gte: ['$minimumStay', min] },
          { $lt: ['$minimumStay', newMin] }
        ]
      },
      min + '-' + newMin,
      await prepareStayQuery(newMin, max, interval)
    ]
  }
}

export const listingsFilterCharts = async (req) => {
  const { body } = req
  console.log('body in req ', body)
  const { minMaxQuery } = prepareQueryForListingsFilterCharts(body)
  console.log('minMaxQuery in listingsFilterCharts ', minMaxQuery)
  const { minMaxData } = await getListingsFilterCharts({ minMaxQuery })
  console.log('minMaxData  in listingsFilterCharts ', minMaxData)
  return {
    minMaxData
  }
}

export const getUniqueFieldValueOfListings = async (field = '', query = {}) => {
  const fieldData = (await ListingCollection.distinct(field, query)) || []
  return fieldData
}

export const getMovingInOutAwaitingSigningStatus = async (query) => {
  const result = await ListingCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: null,
        propertyIds: { $push: '$_id' }
      }
    },
    {
      $lookup: {
        from: 'property_items',
        localField: 'propertyIds',
        foreignField: 'propertyId',
        as: 'propertyItems'
      }
    },
    {
      $addFields: {
        propertyItems: {
          $filter: {
            input: '$propertyItems',
            as: 'property',
            cond: {
              $and: [
                { $eq: ['$$property.isEsigningInitiate', true] },
                { $in: ['$$property.type', ['in', 'out']] },
                { $ne: ['$$property.movingOutSigningComplete', true] }
              ]
            }
          }
        }
      }
    },
    {
      $unwind: '$propertyItems'
    },
    {
      $project: {
        _id: 0,
        propertyItem: '$propertyItems'
      }
    },
    {
      $lookup: {
        from: 'contracts',
        localField: 'propertyItem.contractId',
        foreignField: '_id',
        as: 'contractInfo'
      }
    },
    {
      $unwind: {
        path: '$contractInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        moveInAwaitingTenantCount: {
          $cond: [
            {
              $gt: [
                {
                  $size: {
                    $filter: {
                      input: {
                        $ifNull: ['$propertyItem.tenantSigningStatus', []]
                      },
                      as: 'tenant',
                      cond: {
                        $and: [
                          { $eq: ['$$tenant.signed', false] },
                          { $eq: ['$propertyItem.type', 'in'] },
                          { $not: { $eq: ['$contractInfo.status', 'closed'] } }
                        ]
                      }
                    }
                  }
                },
                0
              ]
            },
            1,
            0
          ]
        },
        landlordMovingInSigningStatus: {
          $cond: [
            {
              $and: [
                { $eq: ['$propertyItem.landlordSigningStatus.signed', false] },
                { $eq: ['$propertyItem.type', 'in'] },
                { $not: { $eq: ['$contractInfo.status', 'closed'] } }
              ]
            },
            1,
            0
          ]
        },
        agentMovingInSigningStatus: {
          $cond: [
            {
              $and: [
                { $eq: ['$propertyItem.agentSigningStatus.signed', false] },
                { $eq: ['$propertyItem.type', 'in'] },
                { $not: { $eq: ['$contractInfo.status', 'closed'] } }
              ]
            },
            1,
            0
          ]
        },
        // Moving out
        moveOutAwaitingTenantCount: {
          $cond: [
            {
              $gt: [
                {
                  $size: {
                    $filter: {
                      input: {
                        $ifNull: ['$propertyItem.tenantSigningStatus', []]
                      },
                      as: 'tenant',
                      cond: {
                        $and: [
                          { $eq: ['$$tenant.signed', false] },
                          { $eq: ['$propertyItem.type', 'out'] }
                        ]
                      }
                    }
                  }
                },
                0
              ]
            },
            1,
            0
          ]
        },
        landlordMovingOutSigningStatus: {
          $cond: [
            {
              $and: [
                { $eq: ['$propertyItem.landlordSigningStatus.signed', false] },
                { $eq: ['$propertyItem.type', 'out'] }
              ]
            },
            1,
            0
          ]
        },
        agentMovingOutSigningStatus: {
          $cond: [
            {
              $and: [
                { $eq: ['$propertyItem.agentSigningStatus.signed', false] },
                { $eq: ['$propertyItem.type', 'out'] }
              ]
            },
            1,
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        totalTenantMovingInSigning: {
          $sum: '$moveInAwaitingTenantCount'
        },
        landlordMovingInSigningStatus: {
          $sum: '$landlordMovingInSigningStatus'
        },
        agentMovingInSigningStatus: {
          $sum: '$agentMovingInSigningStatus'
        },
        totalTenantMovingOutSigning: {
          $sum: '$moveOutAwaitingTenantCount'
        },
        landlordMovingOutSigningStatus: {
          $sum: '$landlordMovingOutSigningStatus'
        },
        agentMovingOutSigningStatus: {
          $sum: '$agentMovingOutSigningStatus'
        }
      }
    },
    {
      $project: {
        _id: 0,
        totalTenantMovingInSigning: 1,
        totalTenantMovingOutSigning: 1,
        totalLandlordOrAgentMovingInSigning: {
          $sum: [
            '$landlordMovingInSigningStatus',
            '$agentMovingInSigningStatus'
          ]
        },
        totalLandlordOrAgentMovingOutSigning: {
          $sum: [
            '$landlordMovingOutSigningStatus',
            '$agentMovingOutSigningStatus'
          ]
        }
      }
    }
  ])
  const [movingAwaitingStatus = {}] = result || []
  const {
    totalLandlordOrAgentMovingInSigning = 0,
    totalLandlordOrAgentMovingOutSigning = 0,
    totalTenantMovingInSigning = 0,
    totalTenantMovingOutSigning = 0
  } = movingAwaitingStatus

  return {
    totalLandlordOrAgentMovingInSigning,
    totalLandlordOrAgentMovingOutSigning,
    totalTenantMovingInSigning,
    totalTenantMovingOutSigning
  }
}

export const countTotalAvailableListingsForEachUpcommingMonth = async (
  req = {}
) => {
  const { body = {} } = req
  const { partnerId = '' } = body
  const currentDateTime = momemt().startOf('day').toDate()

  const matchQuery = {
    listed: true,
    deleted: { $ne: true },
    availabilityStartDate: { $gte: currentDateTime }
  }

  if (partnerId) {
    appHelper.validateId({ partnerId })
    matchQuery.partnerId = partnerId
  }

  const aggregationPipeline = [
    {
      $match: matchQuery
    },
    {
      $addFields: {
        formatedAvailabilityStartDate: {
          $dateToString: {
            format: '%Y-%m',
            date: '$availabilityStartDate'
          }
        }
      }
    },
    {
      $group: {
        _id: '$formatedAvailabilityStartDate',
        totalListingsAvailable: { $sum: 1 }
      }
    },
    {
      $project: {
        _id: 0,
        availabilityStartDate: '$_id',
        totalListingsAvailable: 1
      }
    },
    {
      $sort: { availabilityStartDate: 1 }
    }
  ]

  const listingsCount = await ListingCollection.aggregate(aggregationPipeline)
  return { data: listingsCount }
}
