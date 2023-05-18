import { integrationService } from '../services'

export default {
  async addIntegration(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.createIntegration(req)
  },

  async updateOrRemoveIntegration(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await integrationService.updateOrRemoveIntegration(req)
  },

  async enableOrDisablePogoIntegration(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.enableOrDisablePogo(req)
  },

  async enableOrDisableIntegration(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.enableOrDisableIntegration(req)
  },

  async updateIntegrationFromLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.updateIntegrationFromLambda(req)
  },

  async checkPogoIntegrationStatusForPartnerAop(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.checkPogoIntegrationStatusForPartnerAop(req)
  },

  // For xledger and others integrations
  async checkIntegrationStatusForPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.checkIntegrationStatusForPartnerAop(req)
  },

  async addIntegrationForXledger(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.createIntegrationForXledger(req)
  },

  async updateOrRemoveIntegrationItem(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await integrationService.updateOrRemoveIntegrationItem(req)
  }
}
