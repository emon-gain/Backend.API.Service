import { propertyHelper } from '../helpers'

export default {
  async properties(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 30, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await propertyHelper.queryProperties(req)
  },

  async propertyDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const propertyDetails = await propertyHelper.queryPropertyDetails(req)
    return propertyDetails
  },
  async getPropertyForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const property = await propertyHelper.queryForPropertyExcelCreator(req)
    return property
  },
  async getPropertyUtilities(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await propertyHelper.queryGetPropertyUtilities(req)
  },
  async getPropertyIssues(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 30, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await propertyHelper.queryPropertyIssues(req)
  },

  async getPropertyInfoForPartnerAdminDashboard(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await propertyHelper.queryPropertyInfoForPartnerDashboard(req)
  },

  async janitorDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 30, skip = 0, sort = { name: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await propertyHelper.queryJanitorDropdown(req)
  },

  async getRentRollReportForExcelManager(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 30, skip = 0, sort = { 'location.name': 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await propertyHelper.queryRentRollReportForExcelManager(req)
  },

  async getAllIssuesForPartnerPublicSite(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await propertyHelper.getAllIssuesForPartnerPublicSite(req)
  },

  async getPropertyIssuesForPartnerPublic(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { issueType: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await propertyHelper.getPropertyIssuesForPartnerPublic(req)
  }
}
