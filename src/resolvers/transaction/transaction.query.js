import { transactionHelper } from '../helpers'
export default {
  async getTransactionForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const transaction =
      await transactionHelper.queryTransactionsForExcelCreator(req)
    return transaction
  },

  //gor lambda accounting bridge pogo
  async transactionsForPogo(parent, args, context) {
    const { req } = context
    const { queryData } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(queryData))
    const data = await transactionHelper.getTransactionForPOGO(req)
    return data
  },
  // get transaction for xledger
  async getTransactionForXledger(parent, args, context) {
    const { req } = context
    const { queryData, optionData = {} } = args
    const { limit = 1000, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      options: { limit, skip, sort },
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await transactionHelper.getTransactionForXledger(req)
  },

  async invalidVoucherTransactions(parent, args, context) {
    const { req } = context
    const { queryData } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    //req.session.startTransaction()
    const data = await transactionHelper.getInvalidVoucherTransactions(req)
    return data
  },
  async getTransactionsForPartnerAPI(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 1000, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      options: { limit, skip, sort },
      query: JSON.parse(JSON.stringify(queryData))
    }
    const response = await transactionHelper.getTransactionsForPartnerAPI(req)
    return response
  },
  async getLedgerAccountInfoFromUnsyncTransactions(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await transactionHelper.getLedgerAccountInfoFromUnsyncTransactions(
      req
    )
  },
  async getLedgerAccountsFromUnsyncTransactions(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await transactionHelper.getLedgerAccountsFromUnsyncTransactions(req)
  },
  async getTaxCodesFromUnsyncTransactions(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await transactionHelper.getTaxCodesFromUnsyncTransactions(req)
  },
  async getDetailedBalanceReport(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { sort = { 'tenantInfo.name': 1 }, skip = 0, limit = 30 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { sort, skip, limit }
    }
    return await transactionHelper.getDetailedBalanceReport(req)
  },

  async getDetailedBalanceReportForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { tenantInfo_name: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const transaction =
      await transactionHelper.queryDetailedBalnaceReportForExcelCreator(req)
    return transaction
  },
  // TODO::Need to write test cases
  async getUniqueFieldValueFromTransactions(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await transactionHelper.getUniqueFieldValueFromTransactions(req)
  }
}
