import { creditRatingHelper } from '../../helpers'

export default {
  async getCreditRatingEnabledPartners(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 20, skip = 0, sort = { serial: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const creditRatingEnabledPartners =
      await creditRatingHelper.creditRatingEnabledPartners(req)

    return creditRatingEnabledPartners
  },

  async getPartnerIdsToRemoveCreditRating(parent, args, context) {
    const { req } = context
    const partnerIds =
      await creditRatingHelper.getPartnerIdsToRemoveCreditRatingInfo(req)

    return partnerIds
  }
}
