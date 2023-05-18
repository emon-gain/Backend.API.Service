import { notificationTemplateService } from '../services'

export default {
  async addNotificationTemplate(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const createdNotificationTemplate =
      await notificationTemplateService.createNotificationTemplate(req)
    return createdNotificationTemplate
  },

  async cloneNotificationTemplate(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const clonedNotificationTemplate =
      await notificationTemplateService.cloneNotificationTemplate(req)
    return clonedNotificationTemplate
  },

  async updateNotificationTemplate(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await notificationTemplateService.updateNotificationTemplate(req)
  },

  async removeNotificationTemplate(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await notificationTemplateService.removeNotificationTemplate(req)
  }
}
