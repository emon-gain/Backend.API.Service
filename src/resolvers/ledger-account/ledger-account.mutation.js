import { ledgerAccountService } from '../services'

export default {
  async addLedgerAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const ledgerAccount = await ledgerAccountService.createLedgerAccount(req)
    return ledgerAccount
  },

  async updateLedgerAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedLedgerAccount = await ledgerAccountService.updateLedgerAccount(
      req
    )
    return updatedLedgerAccount
  },

  async removeLedgerAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedLedgerAccount = await ledgerAccountService.removeLedgerAccount(
      req
    )
    return removedLedgerAccount
  }
}
