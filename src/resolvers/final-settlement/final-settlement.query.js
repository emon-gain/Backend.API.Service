import { finalSettlementHelper } from '../helpers'

export default {
  async finalSettlements(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const finalSettlements = await finalSettlementHelper.queryFinalSettlements(
      req
    )
    return finalSettlements
  },
  async finalSettlementSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await finalSettlementHelper.queryFinalSettlementSummary(req)
  },
  async checkIfFinalSettlementNeededForContract(parent, args) {
    const { contractId } = args
    try {
      return await finalSettlementHelper.checkIfFinalSettlementNeeded(
        contractId
      )
    } catch (e) {
      console.log(
        'Error when checking final settlement for contract ',
        contractId,
        e
      )
      throw new Error(e)
    }
  }
}
