import { partnerPayoutService } from '../services'

export default {
  //For Payments lambda #10482
  async generatePartnerPayoutsDaily(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    return await partnerPayoutService.dailyGeneratePartnerPayouts(req)
  },

  async generatePartnerRefundPaymentsDaily(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    return await partnerPayoutService.dailyGeneratePartnerRefundPayments(req)
  },

  async initiatePartnerPayout(parent, args, context) {
    console.log('+++ Initiating partner payout')
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await partnerPayoutService.initiatePartnerPayout(req)
  },

  async initiatePartnerRefundPayment(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return partnerPayoutService.initiatePartnerRefundPayment(req)
  },

  async updatePartnerPayout(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()

    return partnerPayoutService.updatePartnerPayout(req)
  },

  async createPartnerPayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()

    const response = partnerPayoutService.createPartnerPayout(req)
    return response
  }
}
