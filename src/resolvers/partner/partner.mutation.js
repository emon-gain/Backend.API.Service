import { partnerService } from '../services'

export default {
  async addPartner(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdPartner = await partnerService.createPartner(req)
    return createdPartner
  },

  async addPartnerUser(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdPartnerUser = await partnerService.addPartnerUser(req)
    return createdPartnerUser
  },

  async updatePartner(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartner = await partnerService.updatePartner(req)
    return updatedPartner
  },

  async activatePartnerStatus(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerUser = await partnerService.activatePartner(req)
    return updatedPartnerUser
  },

  async deactivatePartnerStatus(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerUser = await partnerService.deactivatePartner(req)
    return updatedPartnerUser
  },

  async updatePartnerLogo(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const updatedPartnerUser = await partnerService.updatePartnerLogo(req)
    return updatedPartnerUser
  },

  async updatePartnerFinnId(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerUser = await partnerService.updatePartnerFinnId(req)
    return updatedPartnerUser
  },

  async updatePartnerUserEmployeeId(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerUser = await partnerService.updatePartnerUserEmployeeId(
      req
    )
    return updatedPartnerUser
  },

  async updatePartnerUserStatus(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await partnerService.updatePartnerUserStatus(req)
  },

  async updatePartnerTransaction(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerUser = await partnerService.updatePartnerTransaction(
      req
    )
    return updatedPartnerUser
  },

  async updatePartnerFunctionality(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await partnerService.updatePartnerFunctionality(req)
  }
}
