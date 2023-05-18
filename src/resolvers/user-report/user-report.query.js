import { userReportHelper } from '../helpers'

export default {
  async getReportDataForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const report = await userReportHelper.queryReportsForExcelCreator(req)
    return report
  },

  async tenantBalanceReport(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const tenantBalanceReport =
      await userReportHelper.queryReportsForTenantBalance(req)

    return tenantBalanceReport
  },

  async tenantBalanceReportSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const tenantBalanceReportSummary =
      await userReportHelper.tenantBalanceReportSummary(req)

    return tenantBalanceReportSummary
  },

  async landLordBalanceReport(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData

    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const landLordBalanceReport =
      await userReportHelper.queryReportsForLandlordBalance(req)
    return landLordBalanceReport
  },

  async landLordBalanceReportSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const landlordBalanceReportSummary =
      await userReportHelper.landlordBalanceReportSummary(req)

    return landlordBalanceReportSummary
  }
}
