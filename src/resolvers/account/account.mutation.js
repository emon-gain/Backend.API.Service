import { accountService } from '../services'

export default {
  async addAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdAccount = await accountService.createAccount(req)
    return createdAccount
  },

  async updateAccountAbout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const updatedAccount = await accountService.updateAccountAbout(req)
    return updatedAccount
  },

  async updateAccountLogo(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.updateAccountLogo(req)
    return response
  },

  async updateAccountStatus(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const response = await accountService.updateAccountStatus(req)
    return response
  },

  async updateAccountBranchInfo(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.updateAccountBranchInfo(req)
    return response
  },

  async addBankAccountForAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.addBankAccount(req)
    return response
  },

  async removeBankAccountForAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.removeBankAccount(req)
    return response
  },

  async updateBankAccountForAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.updateBankAccount(req)
    return response
  },

  async changeContactPersonForAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.updateContactPerson(req)
    return response
  },

  async updateAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.updateAccountInfo(req)
    return response
  },

  //For lambda accounting bridge pogo
  async updateAccountForPogo(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.updateAccountForPogo(req)
    return response
  },
  // For lambda
  async updateAccountsTotalActiveProperties(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedAccount =
      await accountService.updateAccountsTotalActiveProperties(req)
    return updatedAccount
  },

  async downloadAccounts(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await accountService.downloadAccounts(req)
    return response
  }
}
