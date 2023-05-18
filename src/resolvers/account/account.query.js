import { accountHelper } from '../helpers'
import {sessionInit} from "../../test/test.server";
import {Product} from "../../models/product";

export default {
  async accounts(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    console.log(req.session.transaction.isActive)
    // await req.session.abortTransaction()
    // await //req.session.startTransaction();
    console.log(req.session.transaction.state)
    console.log(req.test)
    await Product.updateOne({_id: "643067d2a84817109a69159e"}, {
      $set: {
        price: 300
      }
    }, {
      session: req.session
    })
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }

    const accounts = await accountHelper.queryAccounts(req)
    // await req.session.commitTransaction();
    return accounts
  },

  // For Dropdown api with unique name
  async accountsDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    const accountsDropdown = await accountHelper.queryAccountsDropdown(req)
    return accountsDropdown
  },

  // For lambda
  async getAccountInfoForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const accounts = await accountHelper.queryForAccountExcelCreator(req)
    return accounts
  },

  async accountDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const accountDetails = await accountHelper.queryAccountDetails(req)
    return accountDetails
  },

  async getAccountIdsForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await accountHelper.queryAccountIdsForLambda(req)
  },

  async getAccountsForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const accounts = await accountHelper.queryAccountsForPartnerApp(req)
    return accounts
  },

  async accountForPaymentXml(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const accountDetails = await accountHelper.queryAccountForPaymentXml(req)
    return accountDetails
  },

  // TODO:: Later need to write test cases.
  async getInvoiceAccountNumbers(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await accountHelper.getInvoiceAccountNumbers(req)
  },

  // For Xledger
  async getAccountForXledger(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await accountHelper.queryForAccountXledger(req)
  }
}
