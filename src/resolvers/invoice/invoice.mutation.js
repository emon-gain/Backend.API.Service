import { invoiceService } from '../services'

export default {
  async addInvoices(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const invoices = await invoiceService.handleRequestForInvoiceCreation(req)
    return invoices
  },

  async addVippsStatusToNew(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const invoices = await invoiceService.addVippsStatusToNew(req)
    return invoices
  },

  async sendInvoiceToVipps(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const invoices = await invoiceService.sendInvoiceToVipps(req)
    return invoices
  },

  async sendInvoiceOrDisableNotification(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const invoices = await invoiceService.sendInvoiceOrDisableNotification(req)
    return invoices
  },

  async updateInvoiceAndAddLogsForVipps(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateInfo =
      await invoiceService.updateInvoiceAndAddLogForVippsService(req)
    console.log(updateInfo)
    if (updateInfo !== true) {
      return updateInfo
    } else {
      return {
        msg: 'Invoice Updated and Log Added',
        code: 'Success'
      }
    }
  },

  async downloadRentOrLandlordInvoice(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const downloadInvoice = await invoiceService.downloadRentOrLandlordInvoice(
      req
    )
    return downloadInvoice
  },

  async addSerialIds(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await invoiceService.handleRequestForSerialIdsCreation(req)
    return response
  },

  async updateAnInvoiceForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await invoiceService.updateAnInvoiceForLambda(req)
    return response
  },

  async updateAnAppInvoiceForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await invoiceService.updateAnAppInvoiceForLambda(req)
    return response
  },

  async updateInvoiceStatusOrInvoiceTag(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.updateInvoiceStatusOrInvoiceTag(req)
  },

  async creditInvoice(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.creditInvoice(req)
  },

  async updateInvoiceStatusToLost(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await invoiceService.updateInvoiceStatusToLost(req)
    return response
  },

  async addCorrectionInvoice(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const invoice = await invoiceService.addCorrectionInvoice(req)
    return invoice
  },

  async updateInvoiceDueDelayDate(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await invoiceService.updateInvoiceDueDelayDate(req)
    return response
  },

  async createLandlordCreditNote(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.createLandlordCreditNoteService(req)
  },

  async createManualInvoices(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.createManualInvoicesService(req)
  },

  async createRentInvoices(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.createRentInvoicesService(req)
  },
  //TODO:: Need to write test cases
  async createCreditNote(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.createCreditNote(req)
  },
  //TODO:: Need to write test cases
  async removeInvoiceLossRecognition(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.removeLossRecognition(req)
  },

  async createLandlordInvoiceForExtraPayout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.createLandlordInvoiceForExtraPayout(req)
  },

  async createLandlordInvoiceOrCreditNote(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.createLandlordInvoiceOrCreditNote(req)
  },
  //TODO:: Need to write test cases
  async removeInvoiceFees(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await invoiceService.removeInvoiceFees(req)
  },
  async createQForAddingMissingInvoiceSerialIds(parent, args, context) {
    const { req } = context
    const { queryData, optionData } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: JSON.parse(JSON.stringify(optionData))
    }
    //req.session.startTransaction()
    return invoiceService.createQForAddingMissingInvoiceSerialIds(req)
  },

  async updateInvoiceForCompelloEInvoice(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateInfo = await invoiceService.updateInvoiceForCompelloEInvoice(
      req
    )
    return updateInfo
  }
}
