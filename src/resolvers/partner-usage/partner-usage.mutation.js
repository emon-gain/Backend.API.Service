import { partnerUsageService } from '../services'

export default {
  async addPartnerUsage(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const partnerUsages = await partnerUsageService.createPartnerUsage(req)
    return partnerUsages
  },

  async addPartnerUsageData(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await partnerUsageService.addPartnerUsageData(req)
  }
}
