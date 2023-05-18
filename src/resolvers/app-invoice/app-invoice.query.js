import { appInvoiceHelper } from '../helpers'

export default {
  async appInvoiceDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await appInvoiceHelper.appInvoiceDetails(req)
  },
  async appInvoiceForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await appInvoiceHelper.appInvoiceForLambda(req)
  },
  async appInvoiceDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await appInvoiceHelper.appInvoiceDropdownQuery(req)
  }
}
