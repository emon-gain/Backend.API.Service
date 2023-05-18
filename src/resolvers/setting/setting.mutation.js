import { settingService } from '../services'

export default {
  async updateAppInfoSetting(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedSetting = await settingService.updateAppInfo(req)
    return updatedSetting
  },
  async updateExternalLinksSetting(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedSetting = await settingService.updateExternalLinks(req)
    return updatedSetting
  },
  async updateOpenExchangeInfoSettings(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await settingService.updateOpenExchange(req)
    return response
  },

  async updateSettingsForUpgradeScripts(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await settingService.updateSettingsForUpgradeScripts(req)
    if (response) {
      return {
        msg: 'Success'
      }
    }
  }
}
