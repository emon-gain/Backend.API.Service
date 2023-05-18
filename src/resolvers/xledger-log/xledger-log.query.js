import { xledgerLogHelper } from '../helpers'
export default {
  async queryXledgerIntegrationStatus(parents, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await xledgerLogHelper.getXledgerStatusForPartnerApp(req)
  },

  async integrationLogDetailsForPartnerApp(parents, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await xledgerLogHelper.logDetailsForPartnerApp(req)
  }
}
