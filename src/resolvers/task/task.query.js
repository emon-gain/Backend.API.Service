import { taskHelper } from '../helpers'

export default {
  async tasks(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const tasks = await taskHelper.queryTasks(req)
    return tasks
  },
  async taskDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await taskHelper.queryTaskDetails(req)
  }
}
