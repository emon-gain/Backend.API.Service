import { partnerUsageHelper } from '../helpers'

export default {
  async partnerUsages(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const partnerUsagesResult = await partnerUsageHelper.queryPartnerUsages(req)
    return partnerUsagesResult
  },

  async partnerUsageTypes(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await partnerUsageHelper.queryPartnerUsageTypes(req)
  }
}
