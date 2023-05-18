import { correctionHelper } from '../helpers'

export default {
  async corrections(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const corrections = await correctionHelper.queryCorrections(req)
    return corrections
  },

  async correctionsSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await correctionHelper.correctionsSummary(req)
  },

  async correctionDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const correction = await correctionHelper.correctionDetails(req)
    return correction
  },

  async getCorrectionForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const landlordReport =
      await correctionHelper.queryCorrectionsForExcelCreator(req)
    return landlordReport
  },

  async getActiveCorrection(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: {
        limit,
        skip,
        sort
      }
    }
    return await correctionHelper.queryActiveCorrection(req)
  }
}
