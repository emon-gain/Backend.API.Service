import { listingHelper } from '../helpers'

export default {
  async listings(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const listings = await listingHelper.queryListings(req)
    return listings
  },

  async listingPreview(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const listingPreview = await listingHelper.queryListingPreview(req)
    return listingPreview
  },
  // For lambda listings-bridge-finn
  async finnData(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = queryData
    const finn = await listingHelper.queryFinn(req)
    return finn
  },

  async listingsForAppAdmin(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const listings = await listingHelper.queryListingsForAppAdmin(req)
    return listings
  },

  async getListingsForPublicSite(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const publicSiteListings = await listingHelper.queryListingsForPublicSite(
      req
    )
    return publicSiteListings
  },

  async listingDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const listing = await listingHelper.listingDetails(req)
    return listing
  },

  async listingsUniqueCities() {
    const uniqueCities = await listingHelper.getListingsUniqueCities()
    return uniqueCities
  },

  async listingsDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const listingsDropdown = await listingHelper.queryListingsDropdown(req)
    return listingsDropdown
  },
  async listingsDropdownForAddTenant(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const listingsDropdown = await listingHelper.listingsDropdownForAddTenant(
      req
    )
    return listingsDropdown
  },

  async listingsFilterCharts(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await listingHelper.listingsFilterCharts(req)
  },
  async availableTotalListingsForUpcommingMonth(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await listingHelper.countTotalAvailableListingsForEachUpcommingMonth(
      req
    )
  }
}
