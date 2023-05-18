import { rentSpecificationReportHelper } from '../helpers'

export default {
  async getRentSpecificationReportsForExcel(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const rentSpecificationReports =
      await rentSpecificationReportHelper.getRentSpecificationReportsForExcel(
        req
      )
    return rentSpecificationReports
  },
  async getContractDataForRentSpecificationReports(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: JSON.parse(JSON.stringify(optionData))
    }
    const contractData =
      await rentSpecificationReportHelper.getContractDataForRentSpecificationReports(
        req
      )
    return contractData
  },

  async getRentSpecificationReport(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }

    const report =
      await rentSpecificationReportHelper.getRentSpecificationReport(req)

    return report
  },

  async getRentSpecificationReportSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await rentSpecificationReportHelper.getRentSpecificationReportSummary(
      req
    )
  }
}
