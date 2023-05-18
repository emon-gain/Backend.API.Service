import { addonHelper } from '../helpers'

export default {
  async addons(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: queryData,
      options: { limit, skip, sort }
    }
    const addons = await addonHelper.queryAddons(req)
    return addons
  }
}
