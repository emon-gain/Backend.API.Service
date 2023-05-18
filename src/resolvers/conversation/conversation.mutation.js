import { conversationService } from '../services'

export default {
  async addConversations(parent, args, context) {
    console.log('Started mutation for adding conversations')
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const conversation = await conversationService.addOrUpdateConversation(req)
    return conversation
  },

  async updateConversation(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const conversation = await conversationService.updateConversationById(req)
    return conversation
  }
}
