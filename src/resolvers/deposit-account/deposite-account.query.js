import { depositAccountHelper } from '../helpers'

export default {
  async depositAccounts(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const depositAccounts = await depositAccountHelper.queryDepositAccounts(req)
    return depositAccounts
  },

  async depositAccountsSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const depositAccounts = await depositAccountHelper.depositAccountsSummary(
      req
    )
    return depositAccounts
  },

  async fetchTenantDataForDepositAccountSubmit(parent, args) {
    const { tenantId, contractId } = args
    return await depositAccountHelper.fetchDataForDepositAccount(
      tenantId,
      contractId
    )
  },

  async getTenantLeaseSigningUrlForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const signingUrl =
      await depositAccountHelper.getTenantLeaseSigningUrlForLambda(req)
    return signingUrl
  },

  async getADepositAccountForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const depositAccount =
      await depositAccountHelper.getADepositAccountForLambda(req)
    return depositAccount
  },
  async isShowTestNotificationForDepositAccount(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    //req.session.startTransaction()
    return await depositAccountHelper.getIsShowTestNotification(req)
  }
}
