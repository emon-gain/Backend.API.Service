import { commissionService } from '../services'

export default {
  async downloadCommission(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const commissionQueue = await commissionService.downloadCommission(req)
    return commissionQueue
  },

  async addInvoiceCommissions(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await commissionService.addInvoiceCommissionsService(req)
  }
}
