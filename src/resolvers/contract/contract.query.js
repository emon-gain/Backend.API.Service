import { contractHelper } from '../helpers'

export default {
  async contracts(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const contracts = await contractHelper.queryContracts(req)
    return contracts
  },

  async contractIdsForLeaseLambda(parent, args) {
    const { queryData } = args
    return contractHelper.contractIdsForLambdaRelatedWork(queryData)
  },

  async contractIdForLambda(parent, args) {
    const { partnerId, type, dataToSkip } = args
    const contractIds = await contractHelper.contractIds(
      partnerId,
      type,
      dataToSkip
    )
    return contractIds
  },

  async getSingleLeaseForPublicSite(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const lease = await contractHelper.querySingleLeaseForPublicSite(req)
    return lease
  },

  async getAllInvoicePaymentForALease(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const allInvoicePayments =
      await contractHelper.getAllInvoicePaymentForSingleLease(req)
    return allInvoicePayments
  },

  async getFullPayoutHistory(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await contractHelper.queryPayoutHistoryForLease(req)
  },

  async getAllInvoiceForPartnerPublicSite(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const allInvoices = await contractHelper.getAllInvoiceForSingleLease(req)
    return allInvoices
  },

  async gettingAllIssuesForTenantLease(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const allInvoices = await contractHelper.gettingAllIssuesForTenantLease(req)
    return allInvoices
  },

  async getJournal(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const journal = await contractHelper.getJournalReport(req)
    return journal
  },

  async turnoverJournalSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const summary = await contractHelper.queryJournalSummary(req)
    return summary
  },

  async getJournalForExcelCreator(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const landlordReport = await contractHelper.queryForJournalExcelCreator(req)
    return landlordReport
  },

  async evictions(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await contractHelper.evictions(req)
  },

  async evictionsSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await contractHelper.evictionsSummary(req)
  },

  async leases(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip }
    }
    return await contractHelper.queryLeases(req)
  },

  async getLeaseTenants(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await contractHelper.queryLeaseTenants(req)
  },

  async getAContract(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    return await contractHelper.getAContractInfo(req)
  },
  // Only use for finalSettlement details
  async leaseDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await contractHelper.queryLeaseDetails(req)
  },
  // Lease details for partner app only
  async leaseDetailsForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    const leaseDetails = await contractHelper.leaseDetailsForPartnerApp(req)
    return leaseDetails
  },

  async leaseDropdown(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { leaseSerial: 1 } } = optionData

    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { sort, skip, limit }
    }
    return contractHelper.queryLeaseDropdown(req)
  },

  async assignments(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: {
        sort,
        skip,
        limit
      }
    }
    return await contractHelper.queryAssignments(req)
  },

  async assignmentDetails(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await contractHelper.queryAssignmentDetails(req)
  },

  async leaseListForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const {
      sort = { 'rentalMeta.createdAt': -1 },
      skip = 0,
      limit = 50
    } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { sort, skip, limit }
    }
    return await contractHelper.queryLeaseListForPartnerApp(req)
  },

  async journalChangeLogForPartnerApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const {
      limit = 50,
      skip = 0,
      sort = { 'history.newUpdatedAt': -1 }
    } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await contractHelper.journalChangeLogForPartnerApp(req)
  },

  async getLeaseStatusForPartnerDashboard(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await contractHelper.queryLeaseStatusForPartnerDashboard(req)
  },

  async getContractDataForCpiSettlement(parent, args) {
    const { contractId } = args
    return contractHelper.contractDataForCpiSettlement(contractId)
  },

  async getInvoicePreview(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await contractHelper.queryPreviewInvoices(req)
  },

  async getContractWithFileDataForDepositAccount(parent, args) {
    const { contractId, tenantId } = args
    const contractData = await contractHelper.getContractDataWithFile(
      contractId,
      tenantId
    )
    return contractData[0] || {}
  },

  async getTenantIdForDepositAccount(parent, args) {
    const { contractId } = args
    const contractData =
      await contractHelper.getTenantIdForDepositAccountSubmit(contractId)
    return contractData[0]?.tenants || []
  },

  async checkContractDuration(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))

    return await contractHelper.checkContractDuration(req)
  },

  // TODO:: Later need to write test cases.
  async janitorOverviewList(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await contractHelper.janitorOverviewList(req)
  },

  // TODO:: Need to write test cases.
  async getJanitorDashboardMovingInOutList(parent, args, context) {
    const { req } = context
    const { optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      options: { limit, skip, sort }
    }
    return await contractHelper.janitorDashboardMovingInOutList(req)
  },

  async agedDebtorsReport(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const {
      limit = 50,
      skip = 0,
      sort = { 'propertyInfo.location.name': 1 }
    } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await contractHelper.queryAgedDebtorsReport(req)
  },

  async agedDebtorsReportSummary(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await contractHelper.queryAgedDebtorsReportSummary(req)
  },

  async agedDebtorsReportForExcelManager(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const {
      limit = 200,
      skip = 0,
      sort = { 'propertyInfo.location.name': 1 }
    } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await contractHelper.queryAgedDebtorsReportForExcelManager(req)
  }
}
