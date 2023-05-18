import { integrationHelper } from '../helpers'

export default {
  async integration(parent, args, context) {
    const { req } = context
    const { queryData } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const response = await integrationHelper.queryIntegration(req)
    return response
  },
  async getIntegrationForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await integrationHelper.queryIntegrationForPartnerApp(req)
  },
  async getPogoAccountList(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await integrationHelper.queryPogoIntegrationAccountList(req)
  },
  async getPogoSubledgerSeriesList(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await integrationHelper.queryPogoIntegrationSubledgerList(req)
  },
  async getPogoDepartmentList(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await integrationHelper.queryPogoIntegrationBranchList(req)
  },
  async getPogoProjectList(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await integrationHelper.queryPogoIntegrationGroupList(req)
  },
  async checkPogoIntegrationStatus(parent, args, context) {
    try {
      const { req } = context
      const { queryData = {} } = args
      req.body = JSON.parse(JSON.stringify(queryData))
      return await integrationHelper.queryPogoIntegrationStatus(req)
    } catch (err) {
      throw err
    }
  },

  async getXledgersInfo(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, cursor } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, cursor }
    }
    return await integrationHelper.queryXledgerIntegrationInfos(req)
  },

  async getIntegration(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await integrationHelper.queryIntegrationData(req)
  }
}
