import { payoutProcessHelper } from '../helpers'

export default {
  async payoutProcessesForPaymentLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await payoutProcessHelper.queryPayoutProcessesForLambda(req)
  }
}
