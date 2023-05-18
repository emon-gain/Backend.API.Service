import { branchService } from '../services'

export default {
  async addBranch(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await branchService.createBranch(req)
  },
  async updateBranch(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await branchService.updateBranch(req)
  }
}
