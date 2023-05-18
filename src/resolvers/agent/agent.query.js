import { userHelper } from '../helpers'

export default {
  async agents(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: queryData,
      options: { limit, skip, sort }
    }
    const agents = await userHelper.queryAgents(req)
    return agents
  },

  async agentsDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: queryData,
      options: { limit, skip }
    }
    const agentsDropdown = await userHelper.queryAgentsDropdown(req)
    return agentsDropdown
  }
}
