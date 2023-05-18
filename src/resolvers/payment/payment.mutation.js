import { paymentService } from '../services'

export default {
  async matchPaymentsWithInvoices(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.matchPaymentsWithInvoices(req)
    return response
  },

  async identifyBankPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.identifyBankPayment(req)
    return response
  },

  async addManualPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.addManualPayment(req)
    return response
  },

  async removePayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.removePayment(req)
    return response
  },

  async cancelRefundPayment(parent, args, context) {
    // It's using in meteor
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.cancelRefundPayment(req)
    return response
  },

  async createRefundPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.createRefundPayment(req)
    return response
  },

  async createInvoiceRefundPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.createInvoiceRefundPayment(req)
    return response
  },
  // DI means Deposit Insurance
  async updateDIPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.updateDIPayment(req)
    return response
  },

  async updateBankRefundPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.updateBankRefundPayment(req)
    return response
  },

  async updatePayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.updateManualPayment(req)
    return response
  },

  async linkUnspecifiedPayment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.linkUnspecifiedPayment(req)
    return response
  },

  async testIncomingPaymentsForAPartner(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await paymentService.testIncomingPaymentsForAPartner(req)
    return response
  }
}
