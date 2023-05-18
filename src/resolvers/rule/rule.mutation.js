import { ruleService } from '../services'

export default {
  async addNotificationSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdNotificationSetting = await ruleService.addNotificationSetting(
      req
    )
    return createdNotificationSetting
  },
  async removeNotificationSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedNotificationSetting =
      await ruleService.removeNotificationSetting(req)
    return removedNotificationSetting
  },
  async updateNotificationSetting(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedNotificationSetting =
      await ruleService.updateNotificationSetting(req)
    return updatedNotificationSetting
  },
  async resetToDefaultSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await ruleService.resetNotificationSetting(req)
  }
}
