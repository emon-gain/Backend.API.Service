import { userReportService } from '../services'

export default {
  async addUserReport(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdUserReport = await userReportService.createUserReport(req)
    return createdUserReport
  },

  async removeUserReport(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedUserReport = await userReportService.removeUserReport(req)
    return removedUserReport
  },

  async downloadTenantOrLandlordBalanceReport(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const downloadReport =
      await userReportService.downloadTenantOrLandlordBalanceReport(req)
    return downloadReport
  }
}
