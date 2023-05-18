import { dashboardHelper } from '../helpers'

export default {
  async dashboardActivePartners(parent, args) {
    const { queryData = {} } = args
    const activePartnerInfo = await dashboardHelper.getActivePartnerInfo(
      queryData
    )
    return activePartnerInfo
  },

  async dashboardActiveProperties(parent, args) {
    const { queryData = {} } = args
    const activePropertyInfo = await dashboardHelper.getActivePropertyInfo(
      queryData
    )
    return activePropertyInfo
  },

  async dashboardAppHealthInfo(parent, args) {
    const { queryData = {} } = args
    const appHealthInfo = await dashboardHelper.getAppHealthInfo(queryData)
    return appHealthInfo
  },

  async dashboardFailedPayouts(parent, args) {
    const { queryData = {} } = args
    const failedPayoutInfo = await dashboardHelper.getFailedPayoutInfo(
      queryData
    )
    return failedPayoutInfo
  },

  async dashboardListings(parent, args) {
    const { queryData = {} } = args
    const listingInfo = await dashboardHelper.getListingInfo(queryData)
    return listingInfo
  },

  async dashboardPartnerUsageInfo(parent, args) {
    const { queryData = {} } = args
    const partnerUsageInfo = await dashboardHelper.getPartnerUsageInfo(
      queryData
    )
    return partnerUsageInfo
  },

  async dashboardPartnerUsageGraphData(parent, args) {
    const { queryData = {} } = args
    const partnerUsageInfo = await dashboardHelper.getPartnerUsageGraphData(
      queryData
    )
    return partnerUsageInfo
  },

  async dashboardRetentionRate(parent, args) {
    const { queryData = {} } = args
    const retentionRate = await dashboardHelper.getRetentionRate(queryData)
    return retentionRate
  },

  async dashboardUnitPerAgents(parent, args) {
    const { queryData = {} } = args
    const upaInfo = await dashboardHelper.getUnitPerAgentInfo(queryData)
    return upaInfo
  },

  async dashboardUPAGraphData(parent, args) {
    const { queryData = {} } = args
    const upaGraphData = await dashboardHelper.getUnitPerAgentGraphData(
      queryData
    )
    return upaGraphData
  },

  async dashboardUsers() {
    const userInfo = await dashboardHelper.getUserInfo()
    return userInfo
  },

  async awaitingStatusForPartnerDashboard(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await dashboardHelper.getPartnerDashboardAwaitingStatus(req)
  },

  async failedStatusForPartnerDashboard(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await dashboardHelper.getPartnerDashboardFailedStatus(req)
  },

  async getPartnerDashboardInvoiceAndPaymentChartInfo(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await dashboardHelper.getPartnerDashboardChartInfo(req)
  }
}
