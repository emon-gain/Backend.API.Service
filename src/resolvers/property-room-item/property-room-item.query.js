import { propertyRoomItemHelper } from '../helpers'

export default {
  async propertyRoomItems(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const propertyRoomItems =
      await propertyRoomItemHelper.queryPropertyRoomItems(req)
    return propertyRoomItems
  }
}
