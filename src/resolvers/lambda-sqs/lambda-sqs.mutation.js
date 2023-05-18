import { lambdaSqsService } from '../services'

export default {
  async addLambdaSqs(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdLambdaSqs = await lambdaSqsService.createLambdaSqs(req)
    return createdLambdaSqs
  },
  async removeLambdaSqs(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedLambdaSqs = await lambdaSqsService.removeLambdaSqs(req)
    return removedLambdaSqs
  },
  async removeLambdaSqsById(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedLambdaSqs = await lambdaSqsService.removeLambdaSqsById(req)
    return removedLambdaSqs
  },
  async updateLambdaSqsStatus(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedLambdaSqs = await lambdaSqsService.updateLambdaSqsStatus(req)
    return updatedLambdaSqs
  }
}
