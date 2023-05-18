import { invoicePaymentHelper } from '../helpers'

export default {
  async invoicePayments(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoicePayments = await invoicePaymentHelper.queryInvoicePayments(req)
    return invoicePayments
  },
  async getInvoicePaymentDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.query = JSON.parse(JSON.stringify(queryData))
    return await invoicePaymentHelper.queryInvoicePaymentDetails(req)
  },
  async getInvoicePaymentSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const invoicePaymentsSummary =
      await invoicePaymentHelper.queryInvoicePaymentsSummary(req)
    return invoicePaymentsSummary
  },
  async getPaymentForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const payment = await invoicePaymentHelper.queryForPaymentExcelCreator(req)
    return payment
  },
  async getInvoicePaymentsForXml(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const payment = await invoicePaymentHelper.queryForPaymentXml(req)
    return payment
  },
  async getInvoicePaymentsForPaymentLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const payment = await invoicePaymentHelper.queryForPaymentLambda(req)
    return payment
  },
  async getRelationalDataForAddPayment(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const payment = await invoicePaymentHelper.getRelationalDataForAddPayment(
      req
    )
    return payment
  },

  // TODO:: Later need to write test cases.
  async getPendingPaymentsList(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await invoicePaymentHelper.pendingPaymentsList(req)
  },

  async getCollectionIdsForApproval(parent, args, context) {
    const { queryData = {} } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(queryData))
    return await invoicePaymentHelper.getCollectionIdsForApproval(req)
  },

  async insurancePaymentsSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await invoicePaymentHelper.insurancePaymentsSummaryQuery(req)
  },

  async insurancePaymentsList(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await invoicePaymentHelper.insurancePaymentsList(req)
  }
}
