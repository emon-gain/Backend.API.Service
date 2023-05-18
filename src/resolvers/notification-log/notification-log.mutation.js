import { notificationLogService } from '../services'

export default {
  async updateNotificationLogs(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedNotificationLog =
      await notificationLogService.updateNotificationLogForLambdaService(req)
    return updatedNotificationLog
  },

  async addNotificationLogsForLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdNotificationLog =
      await notificationLogService.createNotificationLogsForLambdaService(req)
    return createdNotificationLog
  },

  async retryFailedNotificationLogs(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedNotificationLog =
      await notificationLogService.retryFailedNotificationLogs(req)
    return updatedNotificationLog
  },

  async updateNotificationLogWithSNSResponse(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedNotificationLog =
      await notificationLogService.updateNotificationLogWithSNSResponse(req)
    return updatedNotificationLog
  },

  async updateNotificationLogAttachments(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const numberOfUpdated =
      await notificationLogService.updateNotificationLogAttachments(req)
    return numberOfUpdated
  },

  async addNotificationLogForLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdNotificationLog =
      await notificationLogService.createNotificationLogForLambdaService(req)
    return createdNotificationLog
  },

  async sendMailToAll(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await notificationLogService.sendMailToAll(req)
    return response
  },

  async addNotificationLogAndUpdateQueueForLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response =
      await notificationLogService.createNotificationLogAndUpdateQueue(req)
    return response
  },

  async createNotificationLogsAndUpdateAppQueue(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response =
      await notificationLogService.createNotificationLogsAndUpdateAppQueue(req)
    return response
  },

  async resendEmailOrSms(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await notificationLogService.resendEmailOrSms(req)
    return response
  }
}
