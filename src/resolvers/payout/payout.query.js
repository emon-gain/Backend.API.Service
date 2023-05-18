import { payoutHelper } from '../helpers'

export default {
  async getPayouts(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoice = await payoutHelper.queryForGetPayouts(req)
    return invoice
  },

  async payoutDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await payoutHelper.queryPayoutDetails(req)
  },

  async getPayoutsForApphealth(parent, args, context) {
    const { req } = context
    const { contractId = '' } = args
    req.body = {
      contractId
    }
    const payout = await payoutHelper.preparePayoutForApphealth(req)
    return payout[0]
  },

  async getLandlordReportForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const landlordReport =
      await payoutHelper.queryForLandlordReportExcelCreator(req)
    return landlordReport
  },

  async getPayoutForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const payoutList = await payoutHelper.queryForPayoutExcelCreator(req)
    return payoutList
  },

  async payoutsSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await payoutHelper.queryPayoutsSummary(req)
  },

  async bankReferencesDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, limit = 50 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, limit }
    }
    return await payoutHelper.queryBankReferencesDropdown(req)
  },
  async getDataForIdfy(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await payoutHelper.getDataForIdfy(req)
  },

  // TODO:: Later need to write test cases.
  async getPendingPayoutsList(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await payoutHelper.getPendingPayoutsList(req)
  }
}
