import { notificationHelper } from '../helpers'

export default {
  // TODO: Need to write test case
  async notificationList(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 15, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await notificationHelper.notificationList(req)
  }
}
