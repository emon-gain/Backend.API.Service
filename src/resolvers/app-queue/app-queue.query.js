import { appQueueHelper } from '../helpers'
// import { getAppQueueDataForApphealthNotification } from './app-queue.helper'

export default {
  async getAppQueueForLambda(parent, args, context) {
    const { req } = context
    const { optionData = {}, queryData = {} } = args

    req.queryData = JSON.parse(JSON.stringify(queryData))
    req.optionData = JSON.parse(JSON.stringify(optionData))
    //req.session.startTransaction()

    const appQueues = await appQueueHelper.getAndUpdateQueues(req)

    return appQueues
  },

  async getAppQueuesForAppHealth(parent, args) {
    const { partnerId, type } = args
    const appQueues = await appQueueHelper.prepareAppQueuesForAppHealth(
      partnerId,
      type
    )
    return appQueues
  },

  async getSingleAppQueue(parent, args) {
    const { queueId } = args
    const AppQueue = await appQueueHelper.getQueueItemById(queueId)
    return AppQueue
  },

  //For accounting bridge pogo #10175
  async checkExistingAppQueueForPogo(parent, args, context) {
    const { queryData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(queryData))
    return await appQueueHelper.getExistingAppQueueForPogo(req)
  },

  async getAppQueues(parent, args, context) {
    const { queryData = {}, optionData = {} } = args
    const { req } = context
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      options: { limit, skip, sort },
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await appQueueHelper.getAppQueuesForQuery(req)
  },

  async getDistinctAppQueues(parent, args, context) {
    const { queryData = {} } = args
    const { req } = context
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    return await appQueueHelper.getAppQueuesByDistinct(req)
  },

  async getAppQueuesForAppHealthNotification(parent, args, context) {
    const { queryData = {} } = args
    const { req } = context
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    return await appQueueHelper.getAppQueueDataForApphealthNotification(req)
  },

  async getSequentialAppQueuesForLambda(parent, args, context) {
    const { req } = context
    const { optionData = {}, queryData = {} } = args

    req.queryData = JSON.parse(JSON.stringify(queryData))
    req.optionData = JSON.parse(JSON.stringify(optionData))
    //req.session.startTransaction()

    const appQueues =
      await appQueueHelper.getAndUpdateSequentialQueuesForLambda(req)

    return appQueues
  }
}
