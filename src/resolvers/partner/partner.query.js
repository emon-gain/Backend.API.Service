import { partnerHelper } from '../helpers'

export default {
  async partners(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const partners = await partnerHelper.queryPartners(req)
    return partners
  },

  async getPartnersForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }

    const partners = await partnerHelper.queryPartnersForLambda(req)
    return partners
  },

  async partnerIds(parent, args) {
    const { limit = 100, skip = 0 } = args
    const partners = await partnerHelper.getPartnerIds(limit, skip)
    return partners
  },

  async partnersSubDomain(parent, args, context) {
    const { req } = context
    const { queryData } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const subDomainInfo = await partnerHelper.queryPartnersSubDomain(req)
    return subDomainInfo
  },

  async getSelfServicePartner() {
    return partnerHelper.getSelfServicePartner()
  }
}
