import { powerOfficeLogService } from '../services'
// For lambda accounting bridge pogo #10175
export default {
  async removePowerOfficeLog(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await powerOfficeLogService.removePowerOfficeLog(req)
    return response
  },

  async resetPowerOfficeLog(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await powerOfficeLogService.resetPowerOfficeLog(req)
    return response
  },

  async createPowerOfficeLog(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await powerOfficeLogService.createPowerOfficeLog(req)
    return response
  },

  async updatePowerOfficeLog(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await powerOfficeLogService.updatePowerOfficeLog(req)
    return response
  }
}
