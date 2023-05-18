import { notificationLogHelper } from '../helpers'

export default {
  async notificationLogs(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const notificationLogs = await notificationLogHelper.queryNotificationLogs(
      req
    )
    return notificationLogs
  },

  async getNotificationSendToInfo(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))

    const notificationSendToInfo =
      await notificationLogHelper.getNotificationSendToInfo(req)
    return notificationSendToInfo
  },

  async getVariablesDataByEvent(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))

    const variablesData = await notificationLogHelper.getVariablesDataForLambda(
      req
    )
    console.log(
      '++++ Checking for variablesData before returning: ',
      variablesData
    )
    return variablesData
  },

  async notificationLogsForAdminApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const notificationLogs =
      await notificationLogHelper.queryNotificationLogsForAdminApp(req)
    return notificationLogs
  },

  async notificationLogDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await notificationLogHelper.getNotificationLogDetails(req)
  },

  async notificationLogsForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 100, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const notificationLogs =
      await notificationLogHelper.queryNotificationLogsForLambda(req)
    return notificationLogs
  },

  async notificationLogCount() {
    const response = await notificationLogHelper.countNotificationLogForLambda()
    return response
  },

  async getESigningVariablesData(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const response = await notificationLogHelper.getESigningVariablesData(req)
    return response
  },

  async getSendToInfoForAccountsOrTenants(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const response =
      await notificationLogHelper.getSendToInfoForAccountsOrTenants(req)
    return response
  },

  async getEmptyPhoneNumbersInfo(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return notificationLogHelper.getEmptyPhoneNumbersInfo(req)
  },

  async getSendMailToAllInfo(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    return notificationLogHelper.getSendMailToInfo(req)
  }
}
