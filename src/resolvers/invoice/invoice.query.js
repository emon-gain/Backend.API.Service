import { invoiceHelper } from '../helpers'

export default {
  async previewInvoices(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { ...queryData }
    const invoices = await invoiceHelper.handleGetRequestForInvoices(req)
    return invoices
  },

  async getMissingInvoicesForManualInvoice(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { ...queryData }
    const invoices = await invoiceHelper.getMissingInvoicesForManualInvoice(req)
    return invoices
  },
  // Rent Invoice
  async invoices(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoices = await invoiceHelper.queryInvoices(req)
    return invoices
  },

  async checkForInvoiceErrorAppHealth(parent, args, context) {
    const { req } = context
    const { contractId } = args
    req.body = {
      contractId
    }
    const invoices = await invoiceHelper.invoiceErrorHelper(req)
    return invoices
  },

  async getInvoiceForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = null, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForLambdaHelper(req)
    return invoices
  },

  async landlordInvoices(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoices = await invoiceHelper.queryLandlordInvoices(req)
    return invoices
  },

  async getInvoiceSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await invoiceHelper.getInvoiceSummary(req)
  },

  async getInvoiceSummaryForPartnerDashboard(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await invoiceHelper.getInvoiceSummaryForPartnerDashboard(req)
  },

  async getInvoiceForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const invoice = await invoiceHelper.queryForInvoiceExcelCreator(req)
    return invoice
  },
  async getInvoiceDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const invoice = await invoiceHelper.getInvoiceDetails(req)
    return invoice
  },
  async getInvoiceDataForVipps(parent, args, context) {
    const { req } = context
    const { queryData = {}, option = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      option
    }
    const invoice = await invoiceHelper.prepareInvoiceDataForVipps(req)
    console.log('Invoice data returns with', invoice)
    return invoice
  },
  async fetchVippsInvoiceId(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const invoice = await invoiceHelper.getVippsInvoiceData(req)
    if (!invoice) {
      return {
        msg: 'Vipps invoice id not found',
        code: 'Error'
      }
    }
    return invoice
  },
  async getInvoiceWithFileForVipps(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const invoice = await invoiceHelper.getInvoiceWithFileForVippsHelper(req)
    return invoice
  },
  async getInvoicesForDuePreReminderNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForDuePreReminderNotice(req)
    return invoices
  },
  async getInvoicesForFirstReminderNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForFirstReminderNotice(req)
    return invoices
  },
  async getInvoicesForSecondReminderNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForSecondReminderNotice(req)
    return invoices
  },
  async getInvoicesForCollectionNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForCollectionNotice(req)
    return invoices
  },
  async getInvoicesForEvictionNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForEvictionNotice(req)
    return invoices
  },
  async getInvoicesForEvictionReminderNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices = await invoiceHelper.getInvoicesForEvictionReminderNotice(
      req
    )
    return invoices
  },
  async getInvoicesForEvictionDueReminderNotice(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { skip = 0, sort = { createdAt: 1 } } = optionData || {}
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { skip, sort }
    }
    const invoices =
      await invoiceHelper.getInvoicesForEvictionDueReminderNotice(req)
    return invoices
  },
  async getInvoiceForAppHealthNotification(parent, args) {
    const { partnerId, contractId } = args
    const invoice =
      await invoiceHelper.prepareDataInvoicesForAppHealthNotification(
        partnerId,
        contractId
      )
    return invoice
  },
  async getInvoicesForUpdatingInvoiceStatus(parent, args, context) {
    const { req } = context
    const { optionData = {} } = args
    req.body = { optionData }
    return await invoiceHelper.getInvoicesForUpdatingInvoiceStatus(req)
  },

  async invoicesDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await invoiceHelper.getInvoicesForDropdown(req)
  },
  async getInvoiceDataForCompelloEInvoice(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const invoice = await invoiceHelper.getInvoiceDataForB2CCompelloEInvoice(
      req
    )
    return invoice
  },
  async getInvoiceDataForB2BCompelloEInvoice(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const invoice = await invoiceHelper.getInvoiceDataForB2BCompelloEInvoice(
      req
    )
    return invoice
  }
}
