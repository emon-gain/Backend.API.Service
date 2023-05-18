import { apiKeyService } from '../services'

export default {
  async addApiKeys(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const apiKeys = await apiKeyService.createApiKey(req)
    return apiKeys
  },
  async resetApiKey(parent, args, context) {
    const { req } = context
    return await apiKeyService.resetApiKey(req)
  }
}
