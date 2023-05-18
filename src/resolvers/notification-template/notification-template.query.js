import { notificationTemplateHelper } from '../helpers'

export default {
  async notificationTemplates(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const notificationTemplates =
      await notificationTemplateHelper.queryNotificationTemplates(req)
    return notificationTemplates
  },

  async getNotificationTemplatesForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const notificationTemplates =
      await notificationTemplateHelper.queryNotificationTemplatesForLambda(req)
    return notificationTemplates
  },

  async notificationTemplatesVariables() {
    const notificationTemplatesVariables =
      await notificationTemplateHelper.queryNotificationTemplatesVariables()
    return notificationTemplatesVariables
  },

  async getESigningTemplateContent(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const response =
      await notificationTemplateHelper.getESigningTemplateContent(req)
    return response
  },

  async getLeaseTemplateDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { 'title.en': 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }

    return await notificationTemplateHelper.getLeaseTemplateDropdown(req)
  },

  async getESigningTemplateForPartner(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await notificationTemplateHelper.getESigningTemplateForPartner(req)
  },

  // TODO:: Need to write test cases
  async previewMovingEsignPdf(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await notificationTemplateHelper.previewMovingEsignPdf(req)
  }
}
