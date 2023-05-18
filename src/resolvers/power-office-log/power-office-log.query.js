import { powerOfficeLogHelper } from '../helpers'

export default {
  //For lambda accounting bridge pogo #10175
  async powerOfficeLogs(parent, args, context) {
    const { req } = context
    const { queryData } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await powerOfficeLogHelper.queryPowerOfficeLog(req)
  },

  async pogoIntegrationLogsForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await powerOfficeLogHelper.queryPowerOfficeLogForPartnerApp(req)
  },

  async pogoLogDetailsForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await powerOfficeLogHelper.queryLogDetailsForPartnerApp(req)
  }
}
