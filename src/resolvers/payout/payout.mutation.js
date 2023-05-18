import { payoutService } from '../services'

export default {
  async addPayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const payouts = await payoutService.addEstimatedPayout(req)
    return payouts
  },

  async updatePayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const payouts = await payoutService.updatePayout(req)
    return payouts
  },

  // This mutation is only for lambda
  async addInvoiceLostInfo(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const payouts = await payoutService.addInvoiceLostInfoInPayout(req)
    return payouts
  },

  //For Payments Lambda #10482
  async approvePendingPayouts(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await payoutService.approvePendingPayouts(req)
  },

  async getEsigningDoc(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await payoutService.getEsigningDoc(req)
  },
  async addAppQueueForPayoutAndPaymentEsigning(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await payoutService.addAppQueueForPayoutAndPaymentEsigning(req)
  },
  async downloadPayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const result = await payoutService.downloadPayout(req)
    return result
  },
  async updatePayoutStatusAsEstimated(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const payout = await payoutService.updatePayoutStatusAsEstimated(req)
    return payout
  },
  async updatePayoutsForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const payout = await payoutService.updatePayoutForLambda(req)
    return payout
  },
  async updatePayoutPauseStatus(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const payout = await payoutService.updatePayoutPauseStatus(req)
    return payout
  },
  async createEstimatedPayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await payoutService.createEstimatedPayoutService(req)
  },
  // This api not used we currently using serialId generated app queue for adding bank references
  async addBankReferences(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await payoutService.addBankReferences(req)
  },

  async createOrAdjustEstimatedPayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await payoutService.createOrAdjustEstimatedPayoutService(req)
  },

  async updatePayoutDate(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await payoutService.updateEstimatedPayoutDate(req)
  }
}
