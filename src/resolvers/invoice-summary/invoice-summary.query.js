import { invoiceSummaryHelper } from '../helpers'

export default {
  async invoiceSummaries(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoiceSummaries = await invoiceSummaryHelper.queryInvoiceSummaries(
      req
    )
    return invoiceSummaries
  },

  async invoiceSummaryInfo(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { queryData: JSON.parse(JSON.stringify(queryData)) }
    const invoiceSummaryInfo =
      await invoiceSummaryHelper.queryInvoiceSummaryInfo(req)
    return invoiceSummaryInfo
  }
}
