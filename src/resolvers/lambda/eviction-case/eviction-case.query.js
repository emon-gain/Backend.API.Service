import { evictionCaseHelper } from '../../helpers'

export default {
  async getContractWithInvoices(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const contractWithInvoices =
      await evictionCaseHelper.getContractWithInvoices(req)
    return contractWithInvoices
  }
}
