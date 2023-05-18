import { conversationHelper } from '../helpers'

export default {
  async conversations(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const conversations = await conversationHelper.getConversations(req)
    return conversations
  },

  async getConversationsForChat(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 10, skip = 0, sort = { lastMessageAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const conversationsForChat =
      await conversationHelper.getConversationsForChat(req)
    return conversationsForChat
  }
}
