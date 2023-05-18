import { evictionCaseService } from '../../services'

export default {
  async createOrUpdateEvictionCase(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await evictionCaseService.createOrUpdateEvictionCase(req)
    return response
  },

  async updateOrRemoveEvictionCase(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await evictionCaseService.updateOrRemoveEvictionCase(req)
    return response
  }
}
