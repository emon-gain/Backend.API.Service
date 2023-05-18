import { partnerPayoutHelper } from '../helpers'

export default {
  async partnerPayouts(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const partnerPayouts = await partnerPayoutHelper.queryPartnerPayouts(req)
    return partnerPayouts
  },
  async getPayoutSigners(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const signers = await partnerPayoutHelper.queryPartnerPayoutsSigners(req)
    return signers
  },
  async getPartnerPayoutForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const response = await partnerPayoutHelper.getPartnerPayoutForLambda(req)
    return response
  },
  async getPartnerPayoutsDataForESigningCleaner(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return partnerPayoutHelper.getPartnerPayoutsDataForESigningCleaner(req)
  }
}
