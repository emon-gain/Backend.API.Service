import { invoicePaymentService } from '../services'

export default {
  async addInvoicePayments(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const invoices = await invoicePaymentService.addInvoicePayments(req)
    return invoices
  },
  async updatePaymentsForLambda(parent, args, context) {
    // Used in payment lambda
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const response = await invoicePaymentService.updatePaymentsForLambda(req)
    return response
  },

  // TODO:: Later need to write test cases.
  async downloadInvoicePayments(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await invoicePaymentService.downloadInvoicePayments(req)
  },

  async approvePendingRefundPayments(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoicePaymentService.updateApprovedPendingRefundPayments(req)
  },

  // TODO:: Later need to write test cases.
  async markPaymentAsEstimated(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoicePaymentService.markPaymentAsEstimated(req)
  },
  async createPayoutsOrRefundPaymentsForTest(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoicePaymentService.createTestPayoutsOrPayments(req)
  }
}
