import { conversationMessageHelper } from '../helpers'

export default {
  async getConversationMessages(parent, args, context) {
    const { req } = context
    const { queryData, optionData } = args
    const { limit = 20, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, sort, skip }
    }
    return await conversationMessageHelper.getConversationMessagesForQuery(req)
  }
}
