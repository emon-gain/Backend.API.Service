import { appHealthHelper } from '../helpers'

export default {
  async appHealths(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const appHealths = await appHealthHelper.queryAppHealths(req)
    return appHealths
  },
  async singleAppHealthError(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const appHealths = await appHealthHelper.getAppHealthError(req)
    return appHealths
  },

  async getDataForAppHealthNotification() {
    const appHealths = await appHealthHelper.prepareDataForAppHealthNotification
    return appHealths
  },

  async getAppHealthErrors() {
    return await appHealthHelper.prepareAppHealthError()
  }
}
