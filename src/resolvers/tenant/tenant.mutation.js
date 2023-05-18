import { tenantService } from '../services'

export default {
  async addTenant(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdTenant = await tenantService.createTenant(req)
    return createdTenant
  },

  async updateTenantAbout(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateTenant = await tenantService.updateTenantAbout(req)
    return updateTenant
  },

  async updateTenantPropertyStatus(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const updateTenant = await tenantService.updateTenantPropertyStatus(req)
    return updateTenant
  },

  async updateTenantType(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateTenant = await tenantService.updateTenantTypeStatus(req)
    return updateTenant
  },

  async updateTenant(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateTenant = await tenantService.updateTenantInfo(req)
    return updateTenant
  },

  //For lambda accounting bridge pogo
  async updateTenantForPogo(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateTenant = await tenantService.updateTenantForPogo(req)
    return updateTenant
  },

  //For public app
  async addInterestForm(parent, args, context) {
    const { inputData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.addInterestForm(req)
  },

  async removeInterestForm(parent, args, context) {
    const { partnerId } = args
    const { req } = context
    //req.session.startTransaction()
    const fileIds = await tenantService.removeTenantInterestForm(
      partnerId,
      req.session
    )
    return fileIds
  },

  async addFilesInTenants(parent, args, context) {
    const { inputData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.addFilesInTenants(req)
  },

  async askForCreditRating(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.askForCreditRating(req)
  },

  async addTenantCreditRatingInfo(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.addTenantCreditRatingInfo(req)
  },

  async updateAndAddTenantKYCForm(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.updateAndAddTenantKYCForm(req)
  },

  async downloadTenants(parent, args, context) {
    const { req } = context
    const { inputData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    //req.session.startTransaction()
    req.body = {
      query: JSON.parse(JSON.stringify(inputData)),
      options: { limit, skip, sort }
    }
    return await tenantService.downloadTenants(req)
  },

  async addTenantsSsnOrLandloadOrgId(parent, args, context) {
    const { inputData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.tenantsAddSsnOrLandloardOrgId(req)
  },

  // TODO:: Need to write test case
  async uploadTenantAvatarKey(parent, args, context) {
    const { inputData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    return await tenantService.uploadTenantProfileAvatarKey(req)
  },

  async submitAskForCreditRating(parent, args, context) {
    const { inputData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.submitAskForCreditRating(req)
  },

  // TODO:: Need to write test case
  async deleteInterestForm(parent, args, context) {
    const { inputData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await tenantService.deleteTenantInterestForm(req)
  }
}
