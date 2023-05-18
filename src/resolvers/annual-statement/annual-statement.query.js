import { annualStatementHelper } from '../helpers'

export default {
  async annualStatements(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await annualStatementHelper.queryAnnualStatements(req)
  },

  // For lambda xml creator
  async annualStatementInfoForXmlCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 20, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const annualStatements =
      await annualStatementHelper.queryAnnualStatementForXmlCreator(req)
    return annualStatements
  },

  // For lambda xml creator
  async partnerAndUserInfo(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const annualStatements =
      await annualStatementHelper.queryPartnerAndUserForXmlCreator(req)
    return annualStatements
  },

  async getAnnualStatementYear(parent, args, context) {
    const { req } = context
    const statementYear = annualStatementHelper.getAnnualStatementYear(req)
    return statementYear
  },

  async getDataForAnnualStatement(parent, args) {
    const { contractId, statementYear } = args
    console.log(args)
    const annualStatements =
      await annualStatementHelper.queryForAnnualStatementData(
        contractId,
        statementYear
      )
    return annualStatements
  },

  async getContractIdsForAnnualStatement(parent, args) {
    const { statementYear, dataToSkip } = args
    const contractIds =
      await annualStatementHelper.getContractIdForAnnualStatements(
        statementYear,
        dataToSkip
      )
    return contractIds
  }
}
