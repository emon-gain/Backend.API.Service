import { xledgerLogService } from '../services'

export default {
  async createXledgerLog(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await xledgerLogService.createXledgerLog(req)
  },

  async resetXledgerLog(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await xledgerLogService.resetXledgerLog(req)
  },

  async updateXledgerLog(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))

    //req.session.startTransaction()
    return await xledgerLogService.updateXledgerLog(req)
  },

  // TODO: Need to wrtie test case
  async updateXledgerInfo(parent, args, context) {
    const { req } = context
    const { queryData = {}, updateData = {} } = args
    req.queryData = JSON.parse(JSON.stringify(queryData))
    req.updateData = JSON.parse(JSON.stringify(updateData))
    //req.session.startTransaction()
    return await xledgerLogService.updateXledgerInfoByContext(req)
  }
}
