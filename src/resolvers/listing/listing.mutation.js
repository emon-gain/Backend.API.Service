import { listingService } from '../services'

export default {
  async addListing(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const listing = await listingService.addListing(req)
    return listing
  },

  async updateListing(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const listing = await listingService.updateListing(req)
    return listing
  },

  // For lambda
  async updateFinnDataForListing(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const listing = await listingService.updateFinnDataForListing(req)
    return listing
  },

  async updateListingBasePrice(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const listing = await listingService.updateListingBasePrice(req)
    return listing
  },

  async updateListingPlaceIds(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const listing = await listingService.updateListingPlaceIds(req)
    return listing
  },

  async disableSingleListing(parent, args, context) {
    const { req } = context
    const { partnerId } = args
    //req.session.startTransaction()
    const response = await listingService.disableListingForPartner(partnerId)
    return response
  },

  async dailyAvailabilityListing(_, __, context) {
    const { req } = context
    //req.session.startTransaction()
    const response = await listingService.dailyListingAvailabilityService()
    return response
  },

  async addOrRemoveListingFromFavourite(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await listingService.addOrRemoveListingFromFavourite(req)
  }
}
