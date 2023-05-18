import { settingHelper } from '../helpers'

export default {
  async settings(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const settings = await settingHelper.querySettings(req)
    return settings
  },
  // Setting for user lambda
  async getSettingForLambda() {
    const setting = await settingHelper.getSettingDataForLambda()
    return setting
  },
  // Setting for public user
  async getSettingForPublicApp() {
    return await settingHelper.getSettingForPublicApp()
  }
}
