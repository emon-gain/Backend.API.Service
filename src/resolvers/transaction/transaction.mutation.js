import { transactionService } from '../services'

export default {
  // This mutation is only for lambda
  async addTransactionForLostInvoice(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const transaction = await transactionService.addTransactionForLostInvoice(
      req
    )
    return transaction
  },

  // This mutation is only for lambda
  async addTransactionForRemoveLossRecognition(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const transaction =
      await transactionService.addTransactionForRemoveLossRecognition(req)
    return transaction
  },

  // For lambda accounting bridge POGO #10175
  async updatePartnerTransactionsWithValidVoucherNo(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await transactionService.updatePartnerTransactionsWithValidVoucherNo(
      req
    )
  },

  async updateTransactionsForPogo(parent, args, context) {
    const { req } = context
    const { inputData } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await transactionService.updateTransactionsForPogo(req)
  },
  // both legacy and regular transaction event
  async addRentInvoiceTransaction(parent, args, context) {
    console.log(
      ' **************** STARTED addRentInvoiceTransaction *****************'
    )
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initRentInvoiceTransaction(req)
    return addedTransactions
  },
  // Regular transaction event
  async addInvoiceMoveToFeesTransaction(parent, args, context) {
    console.log(
      '**************** STARTED addInvoiceFeesMoveToTransaction *****************'
    )
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initInvoiceMoveToFeesTransaction(req)
    return addedTransactions
  },
  // both legacy and regular transaction event
  async addInvoiceLostTransaction(parent, args, context) {
    console.log(
      ' **************** STARTED addInvoiceLostLegacyTransaction *****************'
    )
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initInvoiceLostTransaction(req)
    return addedTransactions
  },
  async addLandlordInvoiceTransaction(parent, args, context) {
    console.log(
      ' **************** STARTED addLandlordInvoiceTransaction *****************'
    )
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initLandlordInvoiceTransaction(req)
    return addedTransactions
  },
  async addPayoutsTransaction(parent, args, context) {
    console.log(
      ' **************** STARTED addPayoutsLegacyTransaction *****************'
    )
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initPayoutsLegacyTransaction(req)
    return addedTransactions
  },
  async addPaymentsTransactions(parent, args, context) {
    console.log(
      ' **************** STARTED addPaymentsLegacyTransactions *****************'
    )
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initPaymentsLegacyTransaction(req)
    return addedTransactions
  },
  async addReminderAndCollectionNoticeTransaction(parent, args, context) {
    console.log(
      ' **************** STARTED addReminderAndCollectionNoticeTransaction *****************'
    )
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initReminderAndCollectionNoticeTransaction(req)
    return addedTransactions
  },
  async revertLostRecognitionTransactions(parent, args, context) {
    console.log('**************** STARTED revertLostRecognitionTransactions')
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initRevertLostRecognitionTransactions(req)
    return addedTransactions
  },
  async addEvictionNoticeTransaction(parent, args, context) {
    console.log('**************** STARTED addEvictionNoticeTransaction')
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initAddEvictionNoticeTransaction(req)
    return addedTransactions
  },
  async revertInvoiceFeesTransaction(parent, args, context) {
    console.log('**************** STARTED revertInvoiceFeesTransaction')
    const { inputData } = args
    const { req } = context
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedTransactions =
      await transactionService.initRevertInvoiceFeesTransaction(req)
    return addedTransactions
  },
  async updateTransactionForPartnerAPI(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedTransaction =
      await transactionService.updateTransactionForPartnerAPI(req)
    return updatedTransaction
  },
  async updateTransactionSerials(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedTransaction =
      await transactionService.updateTransactionSerials(req)
    return updatedTransaction
  },
  async downloadAccountTransactions(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const result = await transactionService.downloadAccountTransactions(req)
    return result
  },

  async downloadLandlordReport(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const downloadInvoice = await transactionService.downloadLandlordReport(req)
    return downloadInvoice
  },

  async downloadTransactions(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await transactionService.downloadTransactions(req)
  },

  async downloadDetailedBalanceReport(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await transactionService.downloadDetailedBalanceReport(req)
  }
}
