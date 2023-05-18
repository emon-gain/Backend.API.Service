import { apiKeyHelper } from '../helpers'

export default {
  async getApiKey(parent, args, context) {
    const { req } = context
    return await apiKeyHelper.getApiKey(req)
  }
}
