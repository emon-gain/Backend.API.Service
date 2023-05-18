import { conversationMessageService } from '../services'

export default {
  async addConversationMessage(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const message = await conversationMessageService.addConversationMessage(req)
    return message
  },
  // For lambda
  async addConversationMessagesByLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const message =
      await conversationMessageService.addConversationMessagesByLambda(req)
    return message
  },
  async sendConversationNotification(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await conversationMessageService.sendConversationNotification(req)
  }
}
