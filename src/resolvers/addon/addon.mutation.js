import { addonService } from '../services'

export default {
  async addAddon(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addon = await addonService.createAddon(req)
    return addon
  },

  async updateAddon(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addons = await addonService.updateAddon(req)
    return addons
  },

  async removeAddon(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const deletedAddons = await addonService.deleteAddon(req)
    return deletedAddons
  }
}
