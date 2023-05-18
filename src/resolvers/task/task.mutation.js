import { taskService } from '../services'

export default {
  async addTask(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const tasks = await taskService.addTask(req)
    return tasks
  },

  async updateTask(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const tasks = await taskService.updateTask(req)
    return tasks
  }
}
