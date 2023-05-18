import { partnerSettingHelper } from '../helpers'

export default {
  async partnerSettings(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const partnerSettings = await partnerSettingHelper.queryPartnerSettings(req)
    return partnerSettings
  },

  async getPartnerSettingForPartnerApp(parent, args, context) {
    const { req } = context
    return await partnerSettingHelper.queryPartnerSetting(req)
  },

  async getAPartnerSetting(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const partnerSetting = await partnerSettingHelper.getAPartnerSettingInfo(
      req
    )
    return partnerSetting
  },
  async getBankAccountNumbers(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const accountNumbers = await partnerSettingHelper.getBankAccountNumbers(req)
    return accountNumbers
  },
  async getPartnerSettingForPublicApp(parent, args, context) {
    console.log(' == getPartnerSettingForPublicApp == ')
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const accountNumbers =
      await partnerSettingHelper.getPartnerSettingForPublicApp(req)
    return accountNumbers
  }
}
