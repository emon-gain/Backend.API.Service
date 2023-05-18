import { notificationService } from '../services'

export default {
  async updateNotification(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const notification = await notificationService.updateNotification(req)
    return notification
  }
}
