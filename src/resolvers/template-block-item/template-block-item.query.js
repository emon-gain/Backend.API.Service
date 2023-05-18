import { templateBlockItemHelper } from '../helpers'

export default {
  async templateBlockItems(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const templateBlockItems =
      await templateBlockItemHelper.queryTemplateBlockItems(req)
    return templateBlockItems
  }
}
