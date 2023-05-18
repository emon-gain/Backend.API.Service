import { tenantHelper } from '../helpers'

export default {
  // For Xledger
  async getTenantForXledger(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await tenantHelper.queryTenantForXledger(req)
  },
  //For pogo
  async tenants(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    //req.session.startTransaction()
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const tenants = await tenantHelper.queryTenants(req)
    return tenants
  },

  async tenantsForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    //req.session.startTransaction()
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const tenants = await tenantHelper.queryTenantsForPartnerApp(req)
    return tenants
  },

  async tenantsDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit, skip } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const tenantsDropdown = await tenantHelper.queryTenantsDropdown(req)
    return tenantsDropdown
  },

  // lambda
  async getTenantForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const tenants = await tenantHelper.queryForTenantExcelCreator(req)
    return tenants
  },

  async tenantDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const tenantDetails = await tenantHelper.queryTenantDetails(req)
    return tenantDetails
  },

  async interestForms(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const {
      limit = 50,
      skip = 0,
      sort = { 'properties.createdAt': -1 }
    } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await tenantHelper.queryInterestForms(req)
  },

  async getProspects(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const {
      limit = 50,
      skip = 0,
      sort = { 'properties.createdAt': -1 }
    } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const prospects = await tenantHelper.getProspects(req)
    return prospects
  },

  async getATenantForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const tenant = await tenantHelper.queryTenant(req)
    return tenant
  },

  async getTenantKycFormData(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const depositAccount = await tenantHelper.getTenantKycFormData(req)
    return depositAccount
  },

  // TODO:: Later need to write test cases.
  async previewInterestForm(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await tenantHelper.getInterestFormPreview(req)
  },

  async getTenantSsn(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await tenantHelper.getTenantSSN(req)
  }
}
