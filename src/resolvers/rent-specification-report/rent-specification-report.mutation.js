import { rentSpecificationReportService } from '../services'

export default {
  async addRentSpecificationReports(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const rentSpecificationReports =
      await rentSpecificationReportService.addRentSpecificationReports(req)
    return rentSpecificationReports
  },

  async resetRentSpecificationReports(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const resetRentSpecificationReports =
      await rentSpecificationReportService.resetRentSpecificationReports(req)
    return resetRentSpecificationReports
  },

  async downloadRentSpecificationReports(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await rentSpecificationReportService.downloadRentSpecificationReports(
      req
    )
  }
}
