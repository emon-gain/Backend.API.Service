import { appQueueService } from '../services'

export default {
  async updateAppQueue(parent, args, context) {
    const { req } = context
    const { queryData = {}, updatingData = {} } = args
    req.body = {
      queryData: JSON.parse(JSON.stringify(queryData)),
      updatingData: JSON.parse(JSON.stringify(updatingData))
    }
    //req.session.startTransaction()
    const updatedAppQueue = await appQueueService.appQueueUpdate(req)
    return updatedAppQueue
  },

  async updateAppQueues(parent, args, context) {
    const { req } = context
    const { queryData = {}, updatingData = {} } = args
    req.body = {
      queryData: JSON.parse(JSON.stringify(queryData)),
      updatingData: JSON.parse(JSON.stringify(updatingData))
    }
    //req.session.startTransaction()

    const updatedAppQueues = await appQueueService.appQueuesUpdate(req)
    return updatedAppQueues
  },

  async addAppQueue(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appQueue = await appQueueService.createAppQueue(req)
    return appQueue
  },

  async addAppQueues(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appQueue = await appQueueService.createAppQueues(req)
    return appQueue
  },

  async cleanUpAppQueue(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const response = await appQueueService.cleanUpQueueService(req)
    return response
  },

  //For Lambda accountingBridgePogo #10175
  async createQueueItemsForPogoIntegratedPartners(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appQueues =
      await appQueueService.createQueueToStartIntegrationOrExternalIdPartnerCheckService(
        req
      )
    return appQueues
  },
  // For type xledger with partnerId
  async createQueueItemsForXledgerIntegratedPartners(parent, args, context) {
    const { req } = context
    const { optionData = {} } = args
    const {
      limit = 50,
      skip = 0,
      sort = {
        createdAt: 1
      }
    } = optionData
    req.body = { sort, skip, limit }
    return await appQueueService.createQueueItemsForXledgerIntegratedPartners(
      req
    )
  },

  async createQueueItemsForExternalIdTransactionCheck(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const queues =
      await appQueueService.createQueueItemsForExternalIdTransactionCheckService(
        req
      )
    return queues
  },
  // For lambda notifier only
  async updateAppQueuesForNotifier(parent, args, context) {
    const { req } = context
    const { params = {} } = args
    req.body = JSON.parse(JSON.stringify(params))
    //req.session.startTransaction()

    const numberOfUpdated =
      await appQueueService.updateAppQueuesDataForNotifierLambda(req)
    return numberOfUpdated
  },

  async updateAppQueuesToNew(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { queryData: JSON.parse(JSON.stringify(queryData)) }
    //req.session.startTransaction()

    const updatedAppQueues = await appQueueService.appQueuesUpdateToNew(req)
    return updatedAppQueues
  },

  async makeDIDueAndSendDINotification(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await appQueueService.makeDIDueAndSendDINotification(req)
  },

  async addSequentialAppQueues(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    // There is no need to use await in returning method
    return appQueueService.addSequentialAppQueuesForRequest(req)
  },

  async cleanUpSequentialAppQueues(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await appQueueService.cleanUpSequentialAppQueues(req)
    return response
  }
}
