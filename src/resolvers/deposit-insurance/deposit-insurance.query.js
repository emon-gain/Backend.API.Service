import { depositInsuranceHelper } from '../helpers'

export default {
  async getDepositInsuranceXmlGeneratingData(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const depositInsuranceXmlGeneratingData =
      await depositInsuranceHelper.getDepositInsuranceXmlGeneratingData(req)
    return depositInsuranceXmlGeneratingData
  },

  async depositInsurances(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 30, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const depositInsurances =
      await depositInsuranceHelper.queryDepositInsurances(req)
    return depositInsurances
  },

  async depositInsuranceSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const depositInsuranceSummary =
      await depositInsuranceHelper.depositInsuranceSummary(req)
    return depositInsuranceSummary
  }
}
