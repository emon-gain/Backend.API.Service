import { propertyItemHelper } from '../helpers'

export default {
  async propertyItems(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const propertyItems = await propertyItemHelper.queryPropertyItems(req)
    return propertyItems
  },
  async propertyUtilityDetails(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await propertyItemHelper.queryPropertyUtilityDetails(req)
  },
  async propertyItemForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    return await propertyItemHelper.queryPropertyItemForLambda(req)
  },
  async getMovingInOutDataForESigningCleaner(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    return propertyItemHelper.getMovingInOutDataForESigningCleaner(req)
  }
}
