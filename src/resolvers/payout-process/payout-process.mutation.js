import { payoutProcessService } from '../services'

export default {
  async updatePayoutProcess(parent, args, context) {
    const { req } = context
    const { inputData } = JSON.parse(JSON.stringify(args))
    req.body = inputData
    //req.session.startTransaction()
    return await payoutProcessService.updatePayoutProcess(req)
  },
  async updatePayoutAndPayoutProcess(parent, args, context) {
    const { req } = context
    const { inputData } = JSON.parse(JSON.stringify(args))
    req.body = inputData
    //req.session.startTransaction()
    return await payoutProcessService.updatePayoutAndPayoutProcess(req)
  },
  async updatePayoutProcessForPaymentLambda(parent, args, context) {
    const { req } = context
    const { inputData } = JSON.parse(JSON.stringify(args))
    req.body = inputData
    //req.session.startTransaction()
    return await payoutProcessService.updatePayoutProcessForPaymentLambda(req)
  }
}
