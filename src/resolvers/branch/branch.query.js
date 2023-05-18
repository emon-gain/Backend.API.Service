import { branchHelper } from '../helpers'

export default {
  async branches(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const branches = await branchHelper.queryBranches(req)
    return branches
  },
  async branchAndUserRoles(parent, args, context) {
    const { req } = context
    return await branchHelper.queryForBranchAndUserRoles(req)
  },
  async branchesDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit, skip } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const branchesDropdown = await branchHelper.queryBranchesDropdown(req)
    return branchesDropdown
  }
}
