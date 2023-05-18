import {
  compact,
  difference,
  each,
  extend,
  find,
  forEach,
  isString,
  map,
  omit,
  pick,
  size
} from 'lodash'
import moment from 'moment-timezone'
import { TransactionCollection } from '../models'
import {
  accountHelper,
  accountingHelper,
  addonHelper,
  appHelper,
  apiKeyHelper,
  appQueueHelper,
  branchHelper,
  commissionHelper,
  contractHelper,
  correctionHelper,
  invoiceHelper,
  integrationHelper,
  ledgerAccountHelper,
  listingHelper,
  partnerHelper,
  partnerSettingHelper,
  paymentHelper,
  payoutHelper,
  payoutProcessHelper,
  powerOfficeLogHelper,
  taxCodeHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import { CustomError } from '../common'

export const getTransactionById = async (id, session) => {
  const transaction = await TransactionCollection.findById(id).session(session)
  return transaction
}

export const getTransaction = async (query, session) => {
  const transaction = await TransactionCollection.findOne(query).session(
    session
  )
  return transaction
}

export const getTransactions = async (
  query,
  session,
  options = {},
  populate = []
) => {
  const transactions = await TransactionCollection.find(query)
    .session(session)
    .populate(populate)
    .sort(options.sort)
    .limit(options.limit)
  return transactions
}

export const countTransactionsForAField = async (field, query = {}) => {
  const total = await TransactionCollection.distinct(field, query)
  return total.length || 0
}

export const getTransactionsUniqueFieldValue = async (field, query = {}) => {
  const uniqueFields = await TransactionCollection.distinct(field, query)
  return uniqueFields || []
}

export const getAccountingPeriodsForQuery = async (query = {}) => {
  console.log(query, 'getAccountingPeriodsForQuery')
  const periodsList = await TransactionCollection.aggregate([
    { $match: query },
    {
      $group: {
        _id: '$period',
        createdAtDates: { $addToSet: '$createdAt' }
      }
    },
    {
      $project: {
        _id: 0,
        createdAtDates: 1,
        period: '$_id'
      }
    }
  ])
  return periodsList
}

export const countTransactions = async (query, session) => {
  const transactionCount = await TransactionCollection.find(query)
    .session(session)
    .countDocuments()
  return transactionCount
}

export const getTransactionsWithSelectedFields = async (
  params = {},
  session
) => {
  const { options = {}, populate = [], query, select } = params || {}
  const { limit = 50, skip = 0, sort = { createdAt: 1 } } = options || {}

  const transactions = await TransactionCollection.find(query)
    .select(select)
    .populate(populate)
    .sort(sort)
    .skip(skip)
    .limit(limit)
    .session(session)
  return transactions
}

// TODO: not tested yet
const getLedgerAccountDataForTransaction = async (params, session) => {
  const { partnerId, ledgerAccountId, type } = params
  const data = {}
  // Find Ledger Account, and accountNumber & taxCodeId from it
  const query = { _id: ledgerAccountId, partnerId }
  const ledgerAccount = await ledgerAccountHelper.getLedgerAccount(
    query,
    session
  )
  const accountNumber = ledgerAccount && ledgerAccount.getAccountNumber()
  const taxCodeId = ledgerAccount && ledgerAccount.getTaxCodeId()
  // Set debit / credit accountCode, textCodeId, taxCode, taxPercentage
  if (type === 'credit' && accountNumber) {
    data.creditAccountCode = accountNumber
  }
  if (type === 'debit' && accountNumber) {
    data.debitAccountCode = accountNumber
  }
  if (taxCodeId) {
    const taxCodeData = await taxCodeHelper.getTaxCode(
      { _id: taxCodeId, partnerId },
      session
    )
    const taxCode = taxCodeData && taxCodeData.getTaxCode()
    const taxPercentage = taxCodeData && taxCodeData.getTaxPercentage()
    if (type === 'credit') {
      data.creditTaxCodeId = taxCodeId
      data.creditTaxCode = taxCode
      data.creditTaxPercentage = taxPercentage
    }
    if (type === 'debit') {
      data.debitTaxCodeId = taxCodeId
      data.debitTaxCode = taxCode
      data.debitTaxPercentage = taxPercentage
    }
  }
  return data
}

// TODO: not tested yet
const getBankLedgerAccountId = async (params, session) => {
  const { partnerId, invoiceId, bankAccountNumber } = params
  let accountingId = ''
  let invoiceAccountNumber
  // Get invoiceAccountNumber from invoice
  const query = { _id: invoiceId, partnerId }
  const invoice = await invoiceHelper.getInvoice(query, session)
  invoiceAccountNumber = invoice && invoice.invoiceAccountNumber
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId,
    session
  )
  // If bankAccountNumber param exists, then set invoiceAccountNumber to bankAccountNumber
  if (bankAccountNumber) {
    invoiceAccountNumber = bankAccountNumber
  }
  const bankAccounts = partnerSetting.getBankAccounts()
  // If invoiceAccountNumber exists, find ledgerAccountId from partner setting, and return
  if (invoiceAccountNumber && size(bankAccounts)) {
    const bankInfo = find(
      bankAccounts,
      (bankAcc) => bankAcc.accountNumber === invoiceAccountNumber
    )
    accountingId = bankInfo && bankInfo.ledgerAccountId
  } else if (!invoiceAccountNumber) {
    const { bankPayment } = partnerSetting
    const { afterFirstMonthACNo } = bankPayment
    const bankInfo = find(
      bankAccounts,
      (bankAcc) => bankAcc.accountNumber === afterFirstMonthACNo
    )
    accountingId = bankInfo && bankInfo.ledgerAccountId
  }
  return accountingId
}

// TODO: Not tested yet
const setDebitCreditAccountAndSubNameForAddon = async (
  options,
  partnerId,
  session
) => {
  const obj = {}
  let accounting
  let query = { _id: options.addonId, partnerId }
  const addon = await addonHelper.getAddon(query, session)
  obj.creditAccountId = addon && addon.creditAccountId
  obj.debitAccountId = addon && addon.debitAccountId
  if (options.addTo === 'payout') {
    query = { type: 'payout_to_landlords', partnerId }
    accounting = await accountingHelper.getAccounting(query, session)
  } else {
    query = { type: 'rent', partnerId }
    accounting = await accountingHelper.getAccounting(query, session)
  }
  obj.subName = accounting && accounting.subName
  return obj
}

const setDebitCreditAccountAndSubNameForOtherAccountTypes = async (
  params,
  session
) => {
  const { partnerId, type, options } = params
  const obj = {}
  let query = { type, partnerId }
  const accounting = await accountingHelper.getAccounting(query, session)
  query = { _id: partnerId, accountType: 'broker' }
  const isBrokerPartner = !!(await partnerHelper.getAPartner(query, session))
  obj.creditAccountId = accounting && accounting.creditAccountId
  obj.debitAccountId = accounting && accounting.debitAccountId
  const ledgerAccParams = {
    partnerId,
    invoiceId: options.invoiceId
  }
  if (
    isBrokerPartner &&
    (type === 'rent_payment' || type === 'final_settlement_payment')
  ) {
    ledgerAccParams.bankAccountNumber = options.bankAccountNumber
    obj.debitAccountId = await getBankLedgerAccountId(ledgerAccParams, session)
  }
  if (isBrokerPartner && type === 'payout_to_landlords') {
    obj.creditAccountId = await getBankLedgerAccountId(ledgerAccParams, session)
  }
  obj.subName = accounting && accounting.subName
  return obj
}

export const getAccountingDataForTransaction = async (params, session) => {
  const { partnerId, accountingType, options = {} } = params
  let debitCreditObj = {}
  let data = {}
  if (accountingType && partnerId) {
    // Set Subtype of transaction
    if (options.addTo === 'payout') {
      data.subType = 'payout_addon'
    } else {
      data.subType = accountingType
    }
    const { transactionSubtype = '' } = options
    if (transactionSubtype) {
      data.subType = transactionSubtype
    }
    // If accountingType = addon set creditAccountId, debitAccountId and subName by following different logic
    if (accountingType === 'addon' && options.addonId) {
      debitCreditObj = await setDebitCreditAccountAndSubNameForAddon(
        options,
        partnerId,
        session
      )
    } else {
      // Else follow different logic for setting creditAccountId, debitAccountId and subName
      const otherAccParams = {
        partnerId,
        type: accountingType,
        options
      }
      debitCreditObj =
        await setDebitCreditAccountAndSubNameForOtherAccountTypes(
          otherAccParams,
          session
        )
    }
    // If exists creditAccountId set credit account data to main data object
    if (debitCreditObj.creditAccountId) {
      const { creditAccountId } = debitCreditObj
      data.creditAccountId = creditAccountId
      const paramsData = {
        partnerId,
        ledgerAccountId: creditAccountId,
        type: 'credit'
      }
      const creditAccountData = await getLedgerAccountDataForTransaction(
        paramsData,
        session
      )
      data = extend(data, creditAccountData)
    }
    // If exists debitAccountId set debit account data to main data object
    if (debitCreditObj.debitAccountId) {
      const { debitAccountId } = debitCreditObj
      data.debitAccountId = debitAccountId
      const paramsData = {
        partnerId,
        ledgerAccountId: debitAccountId,
        type: 'debit'
      }
      const debitAccountData = await getLedgerAccountDataForTransaction(
        paramsData,
        session
      )
      data = extend(data, debitAccountData)
    }
    // Set subName to main data object if exists
    if (debitCreditObj.subName) {
      data.subName = debitCreditObj.subName
    }
  }
  return data
}

export const getFormattedTransactionPeriod = async (periodDate, partnerId) => {
  let formattedDate = ''
  if (periodDate) {
    formattedDate = (
      await appHelper.getActualDate(partnerId, true, periodDate)
    ).format('YYYY-MM')
  }
  return formattedDate
}

export const setAgentInfo = async (params, data, session) => {
  const { agentId, partnerId } = params
  const agent = await userHelper.getUserById(agentId, session)
  if (size(agent)) {
    const agentName = agent.getName()
    const agentEmployeeId = agent.getEmployeeId(partnerId, session)
    if (agentName) {
      data.agentName = agentName
    }
    if (agentEmployeeId) {
      data.employeeId = agentEmployeeId
    }
  }
}

export const setAccountInfo = async (accountId, data, session) => {
  // Fetch Account info
  const account = await accountHelper.getAccountById(accountId, session)
  if (size(account)) {
    const accountObject = {
      accountName: account.name,
      accountSerialId: account.serial,
      accountAddress: account.getAddress(),
      accountZipCode: account.getZipCode(),
      accountCity: account.getCity(),
      accountCountry: account.getCountry()
    }
    // If accountObject property is not falsy, then set that to data object also
    for (const key in accountObject) {
      if (accountObject[key]) {
        data[key] = accountObject[key]
      }
    }
  }
}

export const setAssignmentInfo = async (contractId, data, session) => {
  // Set assignment related data
  const contract = await contractHelper.getContractById(contractId, session)
  if (size(contract)) {
    data.assignmentNumber = contract.getAssignmentNumber()
    data.internalAssignmentId = contract.getInternalAssignmentId()
    data.internalLeaseId = contract.getInternalLeaseId()
  }
}

export const setTenantInfo = async (tenantId, data, session) => {
  const tenant = await tenantHelper.getTenantById(tenantId, session, 'user')
  if (size(tenant)) {
    const tenantUser = tenant.user
    const tenantObject = {
      tenantName:
        tenantUser && tenantUser.getName() ? tenantUser.getName() : '',
      tenantSerialId: tenant.getSerialId(),
      tenantPhoneNumber:
        tenantUser && tenantUser.getPhone() ? tenantUser.getPhone() : '',
      tenantEmailAddress:
        tenantUser && tenantUser.getEmail() ? tenantUser.getEmail() : '',
      tenantAddress: tenant.getAddress(),
      tenantZipCode: tenant.getZipCode(),
      tenantCity: tenant.getCity(),
      tenantCountry: tenant.getCountry()
    }
    // If tenantObject property value is not falsy, then set that to data object also
    for (const key in tenantObject) {
      if (tenantObject[key]) {
        data[key] = tenantObject[key]
      }
    }
  }
}

export const setPropertyInfo = async (propertyId, data, session) => {
  const property = await listingHelper.getListingById(propertyId, session)
  if (size(property)) {
    const propertyObject = {
      locationName: property.getLocationDetail(),
      propertySerialId: property.getSerialId(),
      apartmentId: property.getApartmentId(),
      locationZipCode: property.getPostalCode(),
      locationCity: property.getCity(),
      locationCountry: property.getCountry(),
      propertyGroupId: property.groupId ? property.groupId : ''
    }
    // If propertyObject property value is not falsy, then set that to data object also
    for (const key in propertyObject) {
      if (propertyObject[key]) {
        data[key] = propertyObject[key]
      }
    }
    // Finally if tenant address is not added with data yet, then set property address to tenant (if exists)
    if (!data.tenantAddress && data.locationName) {
      data.tenantAddress = data.locationName
      data.tenantZipCode = data.locationZipCode ? data.locationZipCode : ''
      data.tenantCity = data.locationCity ? data.locationCity : ''
      data.tenantCountry = data.locationCountry ? data.locationCountry : ''
    }
  }
}

export const setBranchInfo = async (branchId, data, session) => {
  const branch = await branchHelper.getBranchById(branchId, session)
  if (size(branch)) {
    if (branch.branchSerialId) {
      data.branchSerialId = branch.branchSerialId
    }
  }
}

export const setInvoiceInfo = async (invoiceId, data, session) => {
  const invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
  if (size(invoice)) {
    const invoiceObject = {
      landlordPayment: !!(data.type === 'payment' && invoice.isFinalSettlement),
      bankAccountNumber: invoice.getAccountNumber(),
      finalSettlementSerialId:
        invoice.isFinalSettlement && invoice.invoiceSerialId,
      invoiceSerialId: !invoice.isFinalSettlement && invoice.invoiceSerialId,
      kidNumber: invoice.getKIDNumber(),
      invoiceDueDate: invoice.dueDate
    }
    // If invoiceObject property value is not falsy, then set that to data object also
    for (const key in invoiceObject) {
      if (invoiceObject[key]) {
        data[key] = invoiceObject[key]
      }
    }
  }
}

// TODO:: Not tested yet
export const setLandlordInvoiceInfo = async (
  landlordInvoiceId,
  data,
  session
) => {
  const landlordInvoice = await invoiceHelper.getInvoiceById(
    landlordInvoiceId,
    session
  )
  if (size(landlordInvoice)) {
    const landlordInvoiceObject = {
      landlordInvoiceSerialId: landlordInvoice.invoiceSerialId,
      kidNumber: landlordInvoice.getKIDNumber(),
      invoiceDueDate: landlordInvoice.dueDate
    }
    // If landlordInvoiceObject property value is not falsy, then set that to data object also
    for (const key in landlordInvoiceObject) {
      if (landlordInvoiceObject[key]) {
        data[key] = landlordInvoiceObject[key]
      }
    }
  }
}

// TODO:: Not tested yet
export const getLandlordInvoiceSerialIdFromPayout = async (payout, session) => {
  let invoiceSerialIds = ''
  const { meta, partnerId } = payout
  if (size(meta)) {
    const landlordInvoiceIds = compact(map(meta, 'landlordInvoiceId'))
    if (size(landlordInvoiceIds) && partnerId) {
      const invoiceQuery = {
        _id: { $in: landlordInvoiceIds },
        partnerId
      }
      const invoices = await invoiceHelper.getInvoices(invoiceQuery, session)
      invoiceSerialIds = size(invoices)
        ? compact(map(invoices, 'invoiceSerialId'))
        : []
      invoiceSerialIds = invoiceSerialIds.join(', ')
    }
  }
  return invoiceSerialIds
}

// TODO:: Not tested yet
export const getTransferInfoByPayoutId = async (payoutId, session) => {
  const query = {
    creditTransferInfo: { $elemMatch: { payoutId } }
  }
  const payoutProcess = await payoutProcessHelper.getPayoutProcess(
    query,
    session
  )
  return size(payoutProcess)
    ? find(payoutProcess.creditTransferInfo, ['payoutId', payoutId])
    : ''
}

// TODO:: Not tested yet
export const getCreditorAccountIdFromPayoutProcess = async (payoutId) => {
  const transferInfo = await getTransferInfoByPayoutId(payoutId)
  return size(transferInfo) ? transferInfo.creditorAccountId : ''
}

// TODO:: Not tested yet
export const getDebtorAccountIdFromPayoutProcess = async (payoutId) => {
  const transferInfo = await getTransferInfoByPayoutId(payoutId)
  return size(transferInfo) ? transferInfo.debtorAccountId : ''
}

// TODO:: Not tested yet
export const setPayoutInfo = async (payoutParams, session) => {
  const { payoutId, partnerId, data, bankInfo } = payoutParams
  const payout = await payoutHelper.getPayout(
    { _id: payoutId, partnerId },
    session
  )
  if (size(payout)) {
    // Firstly modify data object
    const payoutObject = {
      payoutSerialId: payout.serialId,
      landlordInvoiceId: payout.getLandlordInvoiceId(),
      landlordInvoiceSerialId: await getLandlordInvoiceSerialIdFromPayout(
        payout,
        session
      )
    }
    // If payoutObject property value is not falsy, then set that to data object also
    for (const key in payoutObject) {
      if (payoutObject[key]) {
        data[key] = payoutObject[key]
      }
    }
    // Secondly set bank information
    bankInfo.bankAccountNumber = await getCreditorAccountIdFromPayoutProcess(
      payoutId,
      session
    )
    bankInfo.bankAccountNumberForCompanyName =
      await getDebtorAccountIdFromPayoutProcess(payoutId, session)
    bankInfo.bankRef = payout.bankRef
  }
}

export const setAddonInfo = async (addonId, data, session) => {
  const addon = await addonHelper.getAddonById(addonId, session)
  if (size(addon) && addon.name) {
    data.addonName = addon.name
  }
}

// TODO:: Not tested yet
export const setPaymentInfo = async (paymentId, bankInfo, session) => {
  const payment = await paymentHelper.getPaymentById(paymentId, session)
  if (size(payment)) {
    bankInfo.bankAccountNumber =
      payment.meta && payment.meta.cdTrAccountNumber
        ? payment.meta.cdTrAccountNumber
        : ''
    if (payment.type === 'refund' && payment.refundBankRef) {
      bankInfo.bankRef = payment.refundBankRef
    } else if (payment.meta && payment.meta.bankRef) {
      bankInfo.bankRef = payment.meta.bankRef
    }
  }
}

// TODO:: not tested yet
export const setCorrectionInfo = async (correctionParams, session) => {
  const { correctionId, partnerId, invoiceId, bankInfo, data } =
    correctionParams
  let query = { _id: correctionId, partnerId }
  const correction = await correctionHelper.getCorrection(query, session)
  if (size(correction)) {
    if (correction.correctionSerialId) {
      data.correctionSerialId = correction.correctionSerialId
    }
    // For credit note correction
    query = { _id: invoiceId, invoiceType: 'credit_note' }
    const creditNoteInvoice = await invoiceHelper.getInvoice(query, session)
    if (size(creditNoteInvoice) && correction.invoiceId) {
      data.isCreditNoteAddon = true
    }
    const resObj = {
      invoiceId: correction.invoiceId ? correction.invoiceId : ''
    }
    if (!resObj.invoiceId && correction.payoutId) {
      const payout = await payoutHelper.getPayoutById(
        correction.payoutId,
        session
      )
      if (size(payout)) {
        bankInfo.bankAccountNumber =
          await getCreditorAccountIdFromPayoutProcess(payout._id)
        bankInfo.bankAccountForCompanyName =
          await getDebtorAccountIdFromPayoutProcess(payout._id)
        resObj.invoiceId = payout.invoiceId
      }
    }
    return resObj
  }
}

// TODO:: bankAccountNumber test done, bankRef test not done yet
export const setBankInfo = (bankInfo, data) => {
  if (bankInfo.bankAccountNumber) {
    data.bankAccountNumber = bankInfo.bankAccountNumber
  }
  if (bankInfo.bankRef) {
    data.bankRef = bankInfo.bankRef
  }
}

export const getCompanyNameOfBank = async (params, session) => {
  const { bankAccountNumber, invoiceId, partnerId } = params
  // Invoice data
  const query = { _id: invoiceId, partnerId }
  const invoice = await invoiceHelper.getInvoice(query, session)
  let invoiceAccountNumber =
    invoice && invoice.invoiceAccountNumber ? invoice.invoiceAccountNumber : ''
  // Partner Setting data
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId,
    session
  )
  const companyName = partnerSetting && partnerSetting.getCompanyName()
  const bankAccounts = partnerSetting && partnerSetting.getBankAccounts()
  const bankPayment = partnerSetting && partnerSetting.getBankPayment()
  if (!invoiceAccountNumber && bankPayment) {
    invoiceAccountNumber = bankPayment.afterFirstMonthACNo
      ? bankPayment.afterFirstMonthACNo
      : bankPayment.firstMonthACNo
  }
  if (bankAccountNumber) {
    invoiceAccountNumber = bankAccountNumber
  }
  if (invoiceAccountNumber && size(bankAccounts)) {
    const bankInfo = find(
      bankAccounts,
      (bankAcc) => bankAcc.accountNumber === invoiceAccountNumber
    )
    return bankInfo && bankInfo.orgName ? bankInfo.orgName : companyName
  }
}

export const setCompanyName = async (companyParams, session) => {
  const { partnerId, invoiceId, bankInfo, data } = companyParams
  const query = { _id: partnerId, accountType: 'broker' }
  const isBrokerPartner = !!(await partnerHelper.getAPartner(query, session))
  if (isBrokerPartner && invoiceId) {
    const { bankAccountNumberForCompanyName } = bankInfo
    let { bankAccountNumber } = bankInfo
    if (bankAccountNumberForCompanyName) {
      bankAccountNumber = bankAccountNumberForCompanyName
    }
    const getCompanyParams = {
      bankAccountNumber,
      invoiceId,
      partnerId
    }
    data.companyName =
      (await getCompanyNameOfBank(getCompanyParams, session)) || ''
  }
}

export const getTransactionsForLostInvoice = async (data, session) => {
  const { invoiceId, partnerId } = data
  const transactions = await TransactionCollection.aggregate([
    {
      $match: {
        partnerId,
        invoiceId,
        type: 'invoice',
        subType: 'loss_recognition'
      }
    },
    {
      $group: {
        _id: '$subType',
        amount: { $sum: '$amount' }
      }
    }
  ]).session(session)
  return transactions
}

export const prepareDataForLossRecognition = async (data, session) => {
  const { partnerId, invoice, lostDate, lostAmount, transactionEvent } = data
  let transactionData = pick(invoice, [
    'partnerId',
    'contractId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'tenantId',
    'createdBy'
  ])
  if (transactionEvent === 'legacy' && lostDate) {
    transactionData.createdAt = lostDate
  }
  transactionData.amount = lostAmount
  transactionData.invoiceId = invoice._id
  transactionData.type = 'invoice'
  transactionData.period = await getFormattedTransactionPeriod(
    lostDate,
    partnerId
  )
  const params = { partnerId, accountingType: 'loss_recognition' }
  const lossTransactionData = await getAccountingDataForTransaction(
    params,
    session
  )
  transactionData.transactionEvent = transactionEvent
  transactionData = extend(transactionData, lossTransactionData)
  return transactionData
}

export const getTransactionByAggregate = async (query, session) => {
  const aggregate = await TransactionCollection.aggregate(query).session(
    session
  )
  return aggregate
}

export const getTransctionForReport = async (query, groupBy) => {
  const aggregate = await TransactionCollection.aggregate([
    { $match: query },
    {
      $group: {
        _id: groupBy,
        totalAmount: { $sum: '$amount' }
      }
    }
  ])
  return aggregate
}

export const getReminderFeeEvent = async (invoice) => {
  if (!invoice) {
    return ''
  }
  const {
    partnerId = '',
    firstReminderSentAt,
    dueDate,
    secondReminderSentAt
  } = invoice
  const firstReminderSendDaysDiffFromDueDate = (
    await appHelper.getActualDate(partnerId, true, firstReminderSentAt)
  ).diff(await appHelper.getActualDate(partnerId, true, dueDate), 'days')
  const secondReminderSendDaysDiffFromDueDate = secondReminderSentAt
    ? (
        await appHelper.getActualDate(partnerId, true, secondReminderSentAt)
      ).diff(await appHelper.getActualDate(partnerId, true, dueDate), 'days')
    : 0
  if (
    firstReminderSendDaysDiffFromDueDate &&
    firstReminderSendDaysDiffFromDueDate >= 14
  )
    return 'send_first_reminder'
  else if (
    firstReminderSendDaysDiffFromDueDate < 14 &&
    secondReminderSendDaysDiffFromDueDate &&
    secondReminderSendDaysDiffFromDueDate >= 14
  )
    return 'send_second_reminder'
}

export const prepareTransactionQuery = async (params) => {
  const userId = params?.userId
  const partnerId = params?.partnerId || ''
  const accountId = params?.accountId || ''
  const transactionQuery = { partnerId }
  const context = params?.context || ''
  const dateRange = params?.dateRange || null

  //set accountId for landlord Dashboard transaction download
  if (context && context === 'landlordDashboard' && !accountId) {
    const accountIds = []

    const accounts = await accountHelper.getAccounts({
      personId: userId,
      partnerId
    })

    forEach(accounts, function (account) {
      accountIds.push(account._id)
    })

    transactionQuery.accountId = { $in: accountIds }
  } else if (accountId) {
    transactionQuery.accountId = accountId
  }

  //Set download date range in query
  if (size(dateRange)) {
    const startDate = (
      await appHelper.getActualDate(partnerId, true, dateRange.startDate_string)
    )
      .startOf('day')
      .toDate()
    const endDate = (
      await appHelper.getActualDate(partnerId, true, dateRange.endDate_string)
    )
      .endOf('day')
      .toDate()

    transactionQuery.createdAt = { $gte: startDate, $lte: endDate }
  }

  return transactionQuery
}

const getProjectPipelineForTransaction = (params) => {
  const { dateFormat, timeZone, language } = params

  const pipeline = [
    {
      $project: {
        id: '$serialId',
        type: '$type',
        subType: '$subType',
        subName: {
          $cond: {
            if: { $ifNull: ['$subName', false] },
            then: '$subName',
            else: ''
          }
        },
        assignmentNumber: {
          $cond: {
            if: { $ifNull: ['$assignmentNumber', false] },
            then: '$assignmentNumber',
            else: ''
          }
        },
        kidNumber: '$kidNumber',
        invoiceId: '$invoiceSerialId',
        finalSettlementId: {
          $cond: {
            if: { $ifNull: ['$finalSettlementSerialId', false] },
            then: '$finalSettlementSerialId',
            else: ''
          }
        },
        payoutId: {
          $cond: {
            if: { $ifNull: ['$payoutSerialId', false] },
            then: '$payoutSerialId',
            else: ''
          }
        },
        correctionId: {
          $cond: {
            if: { $ifNull: ['$correctionSerialId', false] },
            then: '$correctionSerialId',
            else: ''
          }
        },
        landlordInvoiceId: {
          $cond: {
            if: { $ifNull: ['$landlordInvoiceSerialId', false] },
            then: '$landlordInvoiceSerialId',
            else: ''
          }
        },
        createdAt: {
          $dateToString: {
            format: dateFormat,
            date: '$createdAt',
            timezone: timeZone
          }
        },
        invoiceDueDate: {
          $dateToString: {
            format: dateFormat,
            date: '$invoiceDueDate',
            timezone: timeZone
          }
        },
        period: '$period',
        createdBy: {
          $cond: {
            if: {
              $and: [
                { $ifNull: ['$user', false] },
                { $ne: ['$createdBy', 'SYSTEM'] }
              ]
            },
            then: '$user.profile.name',
            else: appHelper.translateToUserLng('common.appbot', language)
          }
        },
        agent: '$agentName',
        account: '$accountName',
        accountId: '$accountSerialId',
        accountAddress: '$accountAddress',
        accountZipCode: '$accountZipCode',
        accountCity: '$accountCity',
        accountCountry: '$accountCountry',
        tenant: '$tenantName',
        tenantId: '$tenantSerialId',
        tenantAddress: '$tenantAddress',
        tenantZipCode: '$tenantZipCode',
        tenantCity: '$tenantCity',
        tenantCountry: '$tenantCountry',
        tenantPhoneNumber: '$tenantPhoneNumber',
        tenantEmailAddress: '$tenantEmailAddress',
        property: '$locationName',
        propertyZipCode: '$locationZipCode',
        propertyCity: '$locationCity',
        propertyCountry: '$locationCountry',
        apartmentId: '$apartmentId',
        propertyId: '$propertySerialId',
        amount: {
          $cond: {
            if: { $ifNull: ['$amount', false] },
            then: '$amount',
            else: 0
          }
        },
        debit: {
          $cond: {
            if: { $gte: ['$debitAccountCode', 0] },
            then: '$debitAccountCode',
            else: 0
          }
        },
        debitTaxCode: '$debitTaxCode',
        credit: {
          $cond: {
            if: { $gte: ['$creditAccountCode', 0] },
            then: '$creditAccountCode',
            else: 0
          }
        },
        creditTaxCode: '$creditTaxCode',
        branchId: {
          $cond: {
            if: { $ifNull: ['$branchSerialId', false] },
            then: '$branchSerialId',
            else: ''
          }
        },
        internalAssignmentId: {
          $cond: {
            if: { $ifNull: ['$internalAssignmentId', false] },
            then: '$internalAssignmentId',
            else: ''
          }
        },
        internalLeaseId: {
          $cond: {
            if: { $ifNull: ['$internalLeaseId', false] },
            then: '$internalLeaseId',
            else: ''
          }
        },
        employeeId: {
          $cond: {
            if: { $ifNull: ['$employeeId', false] },
            then: '$employeeId',
            else: ''
          }
        },
        bankAccountNumber: '$bankAccountNumber',
        bankRef: '$bankRef',
        externalEntityId: {
          $cond: {
            if: { $ifNull: ['$externalEntityId', false] },
            then: '$externalEntityId',
            else: ''
          }
        },
        propertyGroupId: {
          $cond: {
            if: { $ifNull: ['$propertyGroupId', false] },
            then: '$propertyGroupId',
            else: ''
          }
        },
        companyName: {
          $cond: {
            if: { $eq: ['$partner.accountType', 'broker'] },
            then: '$companyName',
            else: ''
          }
        },
        status: {
          $cond: {
            if: { $ifNull: ['$status', false] },
            then: '$status',
            else: ''
          }
        },
        addonId: '$addonId',
        addonName: '$addonName'
      }
    }
  ]
  return pipeline
}

const getTransactionForExcelManager = async (params) => {
  const { query, options } = params
  const { sort, skip, limit } = options
  const project = getProjectPipelineForTransaction(params)
  const pipeline = [
    {
      $match: query
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $lookup: {
        from: 'users',
        localField: 'createdBy',
        foreignField: '_id',
        as: 'user'
      }
    },
    {
      $unwind: {
        path: '$user',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partner'
      }
    },
    {
      $unwind: {
        path: '$partner',
        preserveNullAndEmptyArrays: true
      }
    },
    ...project
  ]

  const transection = await TransactionCollection.aggregate(pipeline)
  return transection || []
}

export const getTransactionDataForExcelCreator = async (params, options) => {
  const { partnerId = '', userId = '' } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })

  const transactionQuery = await prepareTransactionQuery(params)
  const userInfo = await userHelper.getAnUser({ _id: params.userId })
  const userLanguage = userInfo.getLanguage()
  const dataCount = await countTransactions(transactionQuery)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const queryData = {
    query: transactionQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage
  }
  const transactions = await getTransactionForExcelManager(queryData)
  if (size(transactions)) {
    for (const transaction of transactions) {
      let subType = transaction.subType
      let transactionType = await appHelper.translateToUserLng(
        'transactions.type.' + transaction.type,
        userLanguage
      )

      //Set addon subtype text
      if (subType === 'addon' && transaction.type === 'correction') {
        if (transaction.payoutId) subType = 'payout_addon'
        else if (transaction.isCreditNoteAddon) subType = 'credit_note_addon'
        else subType = 'invoice_addon'
      }
      if (transaction.subType === 'addon_commission') subType = 'invoice_addon'
      subType = await appHelper.translateToUserLng(
        'transactions.sub_type.' + subType,
        userLanguage
      )
      if (
        (transaction.subType === 'addon' ||
          transaction.subType === 'payout_addon' ||
          transaction.subType === 'addon_commission') &&
        transaction.addonId &&
        transaction.addonName
      ) {
        subType = subType + ': ' + transaction.addonName
      }

      //If transaction is 'loss_recognition' then show type and subType is same text
      if (transaction.subType === 'loss_recognition') transactionType = subType

      transaction.type = transactionType || ''
      transaction.subType = subType || ''
    }
  }

  return {
    data: transactions,
    total: dataCount
  }
}

export const queryTransactionsForExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  const { skip, limit } = options
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_transactions') {
    const transactionData = await getTransactionDataForExcelCreator(
      queueInfo.params,
      {
        skip,
        limit,
        sort: { createdAt: -1 }
      }
    )
    return transactionData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

//For lambda accounting bridge POGO #10175

export const getTransactionsQuery = async (paramsQuery, session) => {
  const query = {}
  let transactionIds =
    size(paramsQuery) && size(paramsQuery.transactionIds)
      ? paramsQuery.transactionIds
      : paramsQuery['transactionIds[]']

  if (size(paramsQuery._id)) query._id = paramsQuery._id
  if (size(paramsQuery.partnerId)) query.partnerId = paramsQuery.partnerId
  if (size(paramsQuery.accountId)) query.accountId = paramsQuery.accountId
  if (size(transactionIds)) {
    if (isString(transactionIds)) transactionIds = [transactionIds]
    query._id = { $in: transactionIds }
  }

  // Skip the processing type transactions
  if (paramsQuery.skipProcessing) {
    const powerOfficeLogQuery = {
      type: 'transaction',
      status: 'processing',
      partnerId: query.partnerId
    }

    if (size(query.accountId)) {
      powerOfficeLogQuery.accountId = query.accountId
    }

    const processingTRLogData = await powerOfficeLogHelper.getPowerOfficeLog(
      powerOfficeLogQuery,
      {
        sort: { createdAt: -1 },
        limit: 1
      },
      session
    )

    if (
      size(processingTRLogData) &&
      size(processingTRLogData[0].transactionDate)
    ) {
      paramsQuery.fromDate = processingTRLogData[0].transactionDate
    }
  }

  if (size(paramsQuery.fromDate)) {
    const fromDate = moment(paramsQuery.fromDate).toDate()
    query.fromDate = moment(
      await appHelper.getActualDate(query.partnerId, true, fromDate)
    )
      .startOf('day')
      .toDate()
  }

  if (size(paramsQuery.date)) {
    const createdDate = moment(paramsQuery.date, 'YYYY-MM-DD').toDate()
    const startOFDay = moment(
      await appHelper.getActualDate(query.partnerId, true, createdDate)
    )
      .startOf('day')
      .toDate()
    const endOFDay = moment(
      await appHelper.getActualDate(query.partnerId, true, createdDate)
    )
      .endOf('day')
      .toDate()

    query.createdAt = { $gte: startOFDay, $lte: endOFDay }
  }
  return query
}

export const createTransactionFieldNameForApi = async (
  transaction,
  accountId
) => {
  const transactionData = {
    _id: transaction._id,
    id: transaction.serialId,
    tenantId: transaction.tenantId,
    accountId: transaction.accountId,
    invoiceId: transaction.invoiceId,
    amount: transaction.amount,
    type: transaction.type,
    period: transaction.period,
    subType: transaction.subType,
    addonName: transaction.addonName ? transaction.addonName : null,
    branchSerialId: transaction.branchSerialId,
    propertyGroupId: transaction.propertyGroupId,
    creditAccountId: transaction.creditAccountId,
    creditAccountCode: transaction.creditAccountCode,
    creditTaxCodeId: transaction.creditTaxCodeId,
    creditTaxCode: transaction.creditTaxCode,
    creditTaxPercentage: transaction.creditTaxPercentage,
    debitAccountId: transaction.debitAccountId,
    debitAccountCode: transaction.debitAccountCode,
    debitTaxCodeId: transaction.debitTaxCodeId,
    debitTaxCode: transaction.debitTaxCode,
    debitTaxPercentage: transaction.debitTaxPercentage,
    assignmentNumber: transaction.assignmentNumber,
    locationName: transaction.locationName,
    propertySerialId: transaction.propertySerialId,
    invoiceSerialId: transaction.invoiceSerialId,
    invoiceDueDate: transaction.invoiceDueDate,
    bankAccountNumber: transaction.bankAccountNumber,
    bankRef: transaction.bankRef,
    companyName: transaction.companyName,
    powerOffice: transaction.powerOffice,
    createdAt: await appHelper.getActualDate(
      transaction.partnerId,
      false,
      transaction.createdAt
    ),
    createdDate: moment(
      await appHelper.getActualDate(
        transaction.partnerId,
        true,
        transaction.createdAt
      )
    ).format('YYYY-MM-DD')
  }
  const tenantInfo =
    size(transaction) && size(transaction.tenant) ? transaction.tenant : {}
  const accountInfo =
    size(transaction) && size(transaction.account) ? transaction.account : {}

  if (size(tenantInfo)) {
    if (accountId) {
      const powerOfficeExist = (tenantInfo.powerOffice || []).find(
        (item) => item.accountId === accountId
      )
      if (!powerOfficeExist) {
        throw new CustomError(
          400,
          `Tenant ${tenantInfo._id} not synced to power office for accountId ${accountId}`
        )
      }
    } else {
      if (!size(tenantInfo.powerOffice)) {
        throw new CustomError(
          400,
          `Tenant ${tenantInfo._id} not synced to power office`
        )
      }
    }
    transactionData.tenantInfo = {
      _id: tenantInfo._id,
      name: size(tenantInfo.name) ? tenantInfo.name : '',
      powerOffice: tenantInfo.powerOffice
    }
  }
  if (size(accountInfo)) {
    if (!size(accountInfo.powerOffice) && !accountId) {
      throw new CustomError(
        400,
        `Account ${accountInfo._id} not synced to power office`
      )
    }
    transactionData.accountInfo = {
      _id: accountInfo._id,
      name: size(accountInfo.name) ? accountInfo.name : '',
      powerOffice: accountInfo.powerOffice || {}
    }
  }

  return transactionData
}

const prepareQueryForTransactionListForPogo = async (query, session) => {
  const partnerId = size(query.partnerId) ? query.partnerId : ''
  const accountId = size(query.accountId) ? query.accountId : ''
  const fromDate = query.fromDate || ''
  const todayDate = moment(await appHelper.getActualDate(partnerId, true))
    .startOf('day')
    .toDate()
  const matchQuery = { partnerId, powerOffice: { $exists: false } }
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting(
    {
      partnerId
    },
    session
  )
  const partnerTimeZone =
    size(partnerSettings) &&
    size(partnerSettings.dateTimeSettings) &&
    size(partnerSettings.dateTimeSettings.timezone)
      ? partnerSettings.dateTimeSettings.timezone
      : 'Europe/Oslo'
  if (fromDate)
    matchQuery.createdAt = {
      $gte: fromDate,
      $lte: todayDate
    }
  else matchQuery.createdAt = { $lte: todayDate }
  if (size(accountId)) matchQuery.accountId = accountId
  const transactionsGroupByDate = await TransactionCollection.aggregate([
    { $match: matchQuery },
    {
      $group: {
        _id: {
          $dateToString: {
            format: '%Y-%m-%d',
            date: '$createdAt',
            timezone: partnerTimeZone
          }
        }
      }
    },
    {
      $sort: {
        _id: 1
      }
    }
  ]).session(session)

  if (size(transactionsGroupByDate) && size(transactionsGroupByDate[0]._id)) {
    const startDate = moment(
      await appHelper.getActualDate(
        partnerSettings,
        true,
        transactionsGroupByDate[0]._id
      )
    )
      .startOf('day')
      .toDate()
    const endDate = moment(
      await appHelper.getActualDate(
        partnerSettings,
        true,
        transactionsGroupByDate[0]._id
      )
    )
      .endOf('day')
      .toDate()

    matchQuery.createdAt = { $gte: startDate, $lte: endDate }
    matchQuery.serialId = { $exists: true }
    return { matchQuery, transactionDate: transactionsGroupByDate[0]._id }
  }
}

const prepareQueryForTransactionListForXledger = async (
  query,
  partnerSettings
) => {
  const { partnerId, accountId, fromDate } = query
  const preparedQuery = { partnerId, xledger: { $exists: false } }
  const partnerTimeZone =
    partnerSettings?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const todayDate = (await appHelper.getActualDate(partnerSettings, true))
    .startOf('day')
    .toDate()
  if (fromDate)
    preparedQuery.createdAt = {
      $gte: await appHelper.getActualDate(partnerSettings, false, fromDate),
      $lte: todayDate
    }
  else preparedQuery.createdAt = { $lte: todayDate }
  if (accountId) preparedQuery.accountId = accountId

  const transactionsGroupByDate = await TransactionCollection.aggregate([
    { $match: preparedQuery },
    {
      $group: {
        _id: {
          $dateToString: {
            format: '%Y-%m-%d',
            date: '$createdAt',
            timezone: partnerTimeZone
          }
        }
      }
    },
    {
      $sort: {
        _id: 1
      }
    },
    { $limit: 1 }
  ])

  if (size(transactionsGroupByDate) && size(transactionsGroupByDate[0]._id)) {
    const startDate = (
      await appHelper.getActualDate(
        partnerSettings,
        true,
        transactionsGroupByDate[0]._id
      )
    )
      .startOf('day')
      .toDate()
    const endDate = (
      await appHelper.getActualDate(
        partnerSettings,
        true,
        transactionsGroupByDate[0]._id
      )
    )
      .endOf('day')
      .toDate()

    preparedQuery.createdAt = { $gte: startDate, $lte: endDate }
    preparedQuery.serialId = { $exists: true }
  }

  return preparedQuery
}

const getTransactionListForPogoVoucher = async (params, session) => {
  if (params) {
    params.skipProcessing = true
  }
  const query = await getTransactionsQuery(params, session)
  const preparedQuery = await prepareQueryForTransactionListForPogo(
    query,
    session
  )
  const transactionsToAdd = []
  let date = ''
  if (size(preparedQuery)) {
    const { transactionDate, matchQuery } = preparedQuery
    date = transactionDate
    const transactions = await getTransactions(
      matchQuery,
      session,
      {
        sort: { serialId: 1 },
        limit: 1000
      },
      ['tenant', 'account']
    )
    console.log('transactions found', size(transactions))
    for (const transaction of transactions) {
      transactionsToAdd.push(
        await createTransactionFieldNameForApi(transaction, query.accountId)
      )
    }
  }

  console.log(
    'transactions',
    query.partnerId,
    query.accountId,
    date,
    size(transactionsToAdd)
  )

  return {
    data: transactionsToAdd,
    metaData: {
      transactionDate: date,
      total: transactionsToAdd.length
    }
  }
}

const getStartPeriodSyncPipeline = (partnerTimeZone) => [
  {
    $addFields: {
      periodDateString: {
        $concat: ['$period', '-01']
      }
    }
  },
  {
    $addFields: {
      periodDate: {
        $dateFromString: {
          dateString: '$periodDateString',
          format: '%Y-%m-%d',
          timezone: partnerTimeZone
        }
      }
    }
  },
  {
    $addFields: {
      periodStart: {
        $abs: {
          $dateDiff: {
            startDate: '$createdAt',
            endDate: '$periodDate',
            unit: 'month',
            timezone: partnerTimeZone
          }
        }
      }
    }
  }
]
const getPipelineForTaxCodeXledgerInfo = (mapXledgerTaxCodes) => [
  {
    $addFields: {
      debitTaxCodeXledgerInfo: {
        $first: {
          $filter: {
            input: { $ifNull: [mapXledgerTaxCodes, []] },
            as: 'item',
            cond: {
              $eq: ['$$item.taxCodeId', '$debitTaxCodeId']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      creditTaxCodeXledgerInfo: {
        $first: {
          $filter: {
            input: { $ifNull: [mapXledgerTaxCodes, []] },
            as: 'item',
            cond: {
              $eq: ['$$item.taxCodeId', '$creditTaxCodeId']
            }
          }
        }
      }
    }
  }
]

const getPipelineForDebitCreditAccountXledgerInfo = (mapXledgerAccounts) => [
  {
    $addFields: {
      debitAccountXledgerInfo: {
        $first: {
          $filter: {
            input: { $ifNull: [mapXledgerAccounts, []] },
            as: 'item',
            cond: {
              $eq: ['$$item.accountingId', '$debitAccountId']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      creditAccountXledgerInfo: {
        $first: {
          $filter: {
            input: { $ifNull: [mapXledgerAccounts, []] },
            as: 'item',
            cond: {
              $eq: ['$$item.accountingId', '$creditAccountId']
            }
          }
        }
      }
    }
  }
]

const getTransactionListForXledgerVoucher = async (params) => {
  const { query = {}, options } = params
  const { limit } = options
  const { accountId, partnerId } = query
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting(
    {
      partnerId
    },
    null,
    ['partner']
  )
  if (!size(partnerSettings) || !size(partnerSettings.partner))
    throw new CustomError(404, 'Partner settings not found')
  const partnerTimeZone =
    partnerSettings?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const partnerType = partnerSettings.partner.accountType
  const preparedQuery = await prepareQueryForTransactionListForXledger(
    query,
    partnerSettings
  )
  const integrationQuery = {
    partnerId
  }
  if (partnerType === 'direct') {
    appHelper.checkRequiredFields(['accountId'], query)
    integrationQuery.accountId = accountId
  }
  const integrationInfo = await integrationHelper.getAnIntegration(
    integrationQuery
  )
  let mappedIntegration = integrationInfo
  if (partnerType === 'direct' && !integrationInfo.isGlobal) {
    mappedIntegration = await integrationHelper.getAnIntegration({
      partnerId,
      isGlobal: true
    })
  }
  const partnerUrl = await appHelper.getPartnerURL(
    preparedQuery.partnerId,
    true
  )
  const pipeline = [{ $match: preparedQuery }, { $limit: limit }]
  if (integrationInfo?.enabledPeriodSync === true) {
    pipeline.push(...getStartPeriodSyncPipeline(partnerTimeZone))
  }

  const transactions = await TransactionCollection.aggregate([
    ...pipeline,
    ...getTenantPipelineForXledgerTransaction(accountId),
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        as: 'account'
      }
    },
    appHelper.getUnwindPipeline('account'),
    ...getPipelineForTaxCodeXledgerInfo(mappedIntegration?.mapXledgerTaxCodes),
    ...getPipelineForDebitCreditAccountXledgerInfo(
      mappedIntegration?.mapXledgerAccounts
    ),
    ...getPipelineForGlObjectDbIds(mappedIntegration),
    ...getPipelineForXledgerTransactionText(
      mappedIntegration?.mapXledgerTransactionText
    ),
    ...getFinalProjectPipelineForXledgerTransaction(partnerTimeZone, partnerUrl)
  ])
  return transactions
}

const getPipelineForXledgerTransactionText = (
  mapXledgerTransactionText = []
) => {
  const concatArr = []
  const fieldObject = {
    id: '$serialId',
    type: '$type',
    subType: '$subType',
    subName: '$subName',
    assignmentNumber: '$assignmentNumber',
    kid: '$kidNumber'
  }
  for (const xledgerText of mapXledgerTransactionText) {
    if (xledgerText.type === 'text') {
      concatArr.push(xledgerText.value)
    } else {
      concatArr.push({
        $toString: { $ifNull: [fieldObject[xledgerText.value], ''] }
      })
    }
  }
  const pipeline = []
  if (size(concatArr)) {
    pipeline.push({
      $addFields: {
        text: {
          $concat: concatArr
        }
      }
    })
  }
  return pipeline
}

const getPipelineForGlObjectDbIds = (integrationInfo = {}) => {
  const {
    mapXledgerGlObjects = {},
    mapXledgerBranches = [],
    mapXledgerGroups = [],
    mapXledgerInternalAssignmentIds = [],
    mapXledgerInternalLeaseIds = [],
    mapXledgerEmployeeIds = []
  } = integrationInfo
  const mappedArrAddFields = {}
  const glObjectAddFields = {}
  for (const [key, value] of Object.entries(mapXledgerGlObjects)) {
    if (value === 'branch') {
      mappedArrAddFields.mapXledgerBranches = mapXledgerBranches
      glObjectAddFields[key] = {
        $first: {
          $filter: {
            input: { $ifNull: ['$mapXledgerBranches', []] },
            as: 'mapBranch',
            cond: {
              $eq: ['$$mapBranch.branchId', '$branchId']
            }
          }
        }
      }
    } else if (value === 'group') {
      mappedArrAddFields.mapXledgerGroups = mapXledgerGroups
      glObjectAddFields[key] = {
        $first: {
          $filter: {
            input: { $ifNull: ['$mapXledgerGroups', []] },
            as: 'mapGroup',
            cond: {
              $eq: ['$$mapGroup.propertyGroupId', '$propertyGroupId']
            }
          }
        }
      }
    } else if (value === 'internalAssignmentId') {
      mappedArrAddFields.mapXledgerInternalAssignmentIds =
        mapXledgerInternalAssignmentIds
      glObjectAddFields[key] = {
        $first: {
          $filter: {
            input: { $ifNull: ['$mapXledgerInternalAssignmentIds', []] },
            as: 'mapAssignmentId',
            cond: {
              $eq: [
                '$$mapAssignmentId.internalAssignmentId',
                '$internalAssignmentId'
              ]
            }
          }
        }
      }
    } else if (value === 'internalLeaseId') {
      mappedArrAddFields.mapXledgerInternalLeaseIds = mapXledgerInternalLeaseIds
      glObjectAddFields[key] = {
        $first: {
          $filter: {
            input: { $ifNull: ['$mapXledgerInternalLeaseIds', []] },
            as: 'mapLeaseId',
            cond: {
              $eq: ['$$mapLeaseId.internalLeaseId', '$internalLeaseId']
            }
          }
        }
      }
    } else if (value === 'agentEmployeeId') {
      mappedArrAddFields.mapXledgerEmployeeIds = mapXledgerEmployeeIds
      glObjectAddFields[key] = {
        $first: {
          $filter: {
            input: { $ifNull: ['$mapXledgerEmployeeIds', []] },
            as: 'mapEmployeeId',
            cond: {
              $eq: ['$$mapEmployeeId.employeeId', '$employeeId']
            }
          }
        }
      }
    }
  }
  let pipeline = []
  if (size(mappedArrAddFields) && size(glObjectAddFields)) {
    pipeline = [
      {
        $addFields: mappedArrAddFields
      },
      {
        $addFields: glObjectAddFields
      },
      {
        $addFields: {
          glObject1DbId: '$glObject1.glObjectDbId',
          glObject2DbId: '$glObject2.glObjectDbId',
          glObject3DbId: '$glObject3.glObjectDbId',
          glObject4DbId: '$glObject4.glObjectDbId',
          glObject5DbId: '$glObject5.glObjectDbId'
        }
      }
    ]
  }
  return pipeline
}

const getFinalProjectPipelineForXledgerTransaction = (
  partnerTimeZone,
  partnerUrl
) => [
  {
    $project: {
      _id: 1,
      postedDate: {
        $dateToString: {
          date: '$createdAt',
          format: '%Y-%m-%d',
          timezone: partnerTimeZone
        }
      },
      invoiceDate: {
        $dateToString: {
          date: '$createdAt',
          format: '%Y-%m-%d',
          timezone: partnerTimeZone
        }
      },
      trRegNumber: '$serialId',
      text: 1,
      amount: 1,
      invoiceAmount: '$amount',
      invoiceNumber: '$invoiceSerialId',
      exIdentifier: '$kidNumber',
      dueDate: {
        $dateToString: {
          date: '$invoiceDueDate',
          format: '%Y-%m-%d',
          timezone: partnerTimeZone,
          onNull: null
        }
      },
      paymentReference: '$invoiceSerialId',
      invoiceFileUrl: {
        $concat: [
          partnerUrl,
          '/invoices/rent-invoices?invoiceId=',
          '$invoiceId'
        ]
      },
      periodStart: 1,
      type: 1,
      subType: 1,
      tenantXledgerId: '$tenant.tenantXledger.id',
      accountXledgerId: '$account.xledger.id',
      debitAccountXledgerId: '$debitAccountXledgerInfo.xledgerId',
      creditAccountXledgerId: '$creditAccountXledgerInfo.xledgerId',
      debitXledgerTaxCodeId: '$debitTaxCodeXledgerInfo.xledgerId',
      creditXledgerTaxCodeId: '$creditTaxCodeXledgerInfo.xledgerId',
      glObject1DbId: 1,
      glObject2DbId: 1,
      glObject3DbId: 1,
      glObject4DbId: 1,
      glObject5DbId: 1
    }
  }
]

const getTenantPipelineForXledgerTransaction = (accountId) => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            tenantXledger: {
              $cond: [
                { $ifNull: [accountId, false] },
                {
                  $first: {
                    $filter: {
                      input: { $ifNull: ['$xledger', []] },
                      as: 'item',
                      cond: {
                        $eq: ['$$item.accountId', accountId]
                      }
                    }
                  }
                },
                { $first: '$xledger' }
              ]
            }
          }
        }
      ],
      as: 'tenant'
    }
  },
  appHelper.getUnwindPipeline('tenant')
]

export const getTransactionForPOGO = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const data = await getTransactionListForPogoVoucher(body, session)
  return data
}

export const getTransactionForXledger = async (req) => {
  const { body, user } = req
  const { query } = body
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], query)
  return await getTransactionListForXledgerVoucher(body)
}

const prepareQueryForTransactionsWithInvalidVoucherNo = (
  accountId,
  partnerId
) => {
  const transactionMatchQuery = {
    partnerId,
    $or: [
      {
        externalEntityId: { $exists: false },
        'powerOffice.id': { $exists: true }
      },
      {
        $and: [
          { externalEntityId: { $exists: true } },
          {
            $expr: {
              $gt: [{ $strLenCP: { $ifNull: ['$externalEntityId', ''] } }, 7]
            }
          }
        ]
      }
    ]
  }

  if (accountId) {
    transactionMatchQuery.accountId = accountId
  }
  return transactionMatchQuery
}

const getPartnerTransactionsWithInvalidVoucherNo = async (
  accountId,
  partnerId
) => {
  const response = []
  const transactionMatchQuery = prepareQueryForTransactionsWithInvalidVoucherNo(
    accountId,
    partnerId
  )
  const transactions = await TransactionCollection.aggregate([
    {
      $match: transactionMatchQuery
    },
    {
      $group: {
        _id: '$powerOffice.id',
        transactionIds: { $push: '$_id' }
      }
    }
  ])

  if (size(transactions)) {
    each(transactions, function (transaction) {
      const powerOfficeVoucherId =
        size(transaction) && size(transaction._id) ? transaction._id : ''
      const transactionIds = size(transaction.transactionIds)
        ? transaction.transactionIds
        : []

      if (size(powerOfficeVoucherId) && size(transactionIds)) {
        const resObj = {}
        resObj.partnerId = partnerId
        if (size(accountId)) {
          resObj.accountId = accountId
        }
        resObj.powerOfficeVoucherId = powerOfficeVoucherId
        resObj.transactionIds = transactionIds
        response.push(resObj)
      }
    })
  }
  return response
}

export const getInvalidVoucherTransactions = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { accountId, partnerId } = body
  const data = await getPartnerTransactionsWithInvalidVoucherNo(
    accountId,
    partnerId
  )
  return { data }
}

export const prepareUpdateTransactionsForPogoData = (data) => {
  const updateData = {}
  if (size(data) && size(data.amount)) updateData.amount = data.amount
  if (size(data) && size(data.powerOffice))
    updateData.powerOffice = data.powerOffice
  if (size(data) && size(data.externalEntityId))
    updateData.externalEntityId = data.externalEntityId
  if (size(data) && size(data.companyName))
    updateData.companyName = data.companyName
  if (size(data) && size(data.type)) updateData.type = data.type

  return updateData
}

export const prepareBasicAppQueueDataForLegacyTransaction = (partnerId) => {
  const appQueue = {
    event: 'partner_transaction_enabled',
    destination: 'accounting',
    priority: 'regular',
    params: { partnerId, startingIndex: 0, transactionEvent: 'legacy' },
    status: 'new'
  }
  console.log(
    'appQueue on prepareBasicAppQueueDataForLegacyTransaction ',
    appQueue
  )
  return appQueue
}

export const getAppQueueDataForRentInvoiceTransactions = async (partnerId) => {
  const invoiceIds = await invoiceHelper.getRentInvoiceIdsForLegacyTransaction(
    partnerId
  )
  if (!size(invoiceIds)) {
    console.log('NO invoice id found for rent invoice legacy transactions!!!')
    return null
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_rent_invoice_legacy_transaction'
  appQueue.params.invoiceIds = invoiceIds
  appQueue.params.lengthOfIds = invoiceIds.length || 0
  return appQueue
}

export const getAppQueueDataForInvoicesLostTransactions = async (partnerId) => {
  const invoiceIds = await invoiceHelper.getLostInvoiceIdsForLegacyTransaction(
    partnerId
  )
  if (!size(invoiceIds)) {
    console.log('NO invoice id found for invoice lost legacy transactions!!!')
    return {}
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_invoice_lost_legacy_transaction'
  appQueue.params.invoiceIds = invoiceIds
  appQueue.params.lengthOfIds = invoiceIds.length || 0
  return appQueue
}

export const getAppQueueDataForCommissionsTransactions = async (partnerId) => {
  const commissionIds =
    await commissionHelper.getCommissionIdsForLegacyTransaction(partnerId)
  if (!size(commissionIds)) {
    console.log('NO commission id found for commission legacy transactions!!!')
    return null
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_commission_legacy_transaction'
  appQueue.params.commissionIds = commissionIds
  appQueue.params.lengthOfIds = commissionIds.length || 0
  return appQueue
}

export const getAppQueueDataForCorrectionsTransactions = async (partnerId) => {
  const correctionIds =
    await correctionHelper.getCorrectionIdsForLegacyTransaction(partnerId)
  if (!size(correctionIds)) {
    console.log('NO correction id found for correction legacy transactions!!!')
    return null
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_correction_legacy_transaction'
  appQueue.params.correctionIds = correctionIds
  appQueue.params.lengthOfIds = correctionIds.length || 0
  return appQueue
}

export const getAppQueueDataForPaymentsTransactions = async (partnerId) => {
  const paymentIds = await paymentHelper.getPaymentIdsForLegacyTransaction(
    partnerId
  )
  if (!size(paymentIds)) {
    console.log('NO payment id found for payment legacy transactions!!!')
    return {}
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_payment_legacy_transaction'
  appQueue.params.paymentIds = paymentIds
  appQueue.params.lengthOfIds = paymentIds.length || 0
  return appQueue
}

export const getAppQueueDataForPayoutsTransactions = async (partnerId) => {
  const payoutIds = await payoutHelper.getPayoutIdsForLegacyTransaction(
    partnerId
  )
  if (!size(payoutIds)) {
    console.log('NO payment id found for payment legacy transactions!!!')
    return {}
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_payout_legacy_transaction'
  appQueue.params.payoutIds = payoutIds
  appQueue.params.lengthOfIds = payoutIds.length || 0
  return appQueue
}

export const getAppQueueDataForLandlordInvoiceTransactions = async (
  partnerId
) => {
  const invoiceIds =
    await invoiceHelper.getLandlordInvoiceIdsForLegacyTransaction(partnerId)
  if (!size(invoiceIds)) {
    console.log(
      'NO invoice id found for landlord invoice legacy transactions!!!'
    )
    return {}
  }
  const appQueue = prepareBasicAppQueueDataForLegacyTransaction(partnerId)
  appQueue.action = 'init_landlord_invoice_legacy_transaction'
  appQueue.params.landlordInvoiceIds = invoiceIds
  appQueue.params.lengthOfIds = invoiceIds.length || 0
  return appQueue
}

export const getLossTransactionByAggregate = async (query) => {
  const result = await TransactionCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: '$subType',
        amount: { $sum: '$amount' }
      }
    }
  ])
  return result
}

export const getExistingLossInvoiceTransaction = async (
  invoiceId,
  partnerId
) => {
  const query = {
    invoiceId,
    partnerId,
    type: 'invoice',
    subType: 'loss_recognition'
  }
  const [existingTransaction = {}] =
    (await getLossTransactionByAggregate(query)) || []
  return existingTransaction
}

export const getPartnerInfoByValidatingApiKey = async (apiKey) => {
  if (!apiKey) throw new CustomError(400, 'The API key is required!')

  const { partnerId = '' } = (await apiKeyHelper.getAnApiKey({ apiKey })) || {}
  if (!size(partnerId))
    throw new CustomError(
      404,
      'The API key is invalid. Please refresh the API key then try again.'
    )

  let partnerInfo = {}
  if (size(partnerId))
    partnerInfo = (await partnerHelper.getAPartner({ _id: partnerId })) || {}

  if (!size(partnerInfo))
    throw new CustomError(404, 'No partner found for the API key!')

  const { enableTransactionsApi, isActive } = partnerInfo
  if (!isActive) throw new CustomError(405, 'Partner is not active!')

  if (!enableTransactionsApi)
    throw new CustomError(
      405,
      'Partner transaction API is deactivated. Please request admin to activate transaction API.'
    )

  return partnerInfo
}

const getProjectForTransactionType = () => {
  const projectForTransactionType = {
    $switch: {
      branches: [
        {
          case: {
            $eq: ['$subType', 'loss_recognition']
          },
          then: 'Tapsføring'
        },
        {
          case: {
            $eq: ['$type', 'invoice']
          },
          then: 'Faktura'
        },
        {
          case: {
            $eq: ['$type', 'credit_note']
          },
          then: 'Kreditnota'
        },
        {
          case: {
            $eq: ['$type', 'payment']
          },
          then: 'Innbetaling'
        },
        {
          case: {
            $eq: ['$type', 'refund']
          },
          then: 'Tilbakebetal'
        },
        {
          case: {
            $eq: ['$type', 'commission']
          },
          then: 'Provisjon'
        },
        {
          case: {
            $eq: ['$type', 'payout']
          },
          then: 'Utbetaling'
        },
        {
          case: {
            $eq: ['$type', 'correction']
          },
          then: 'Korrigering'
        }
      ],
      default: '$type'
    }
  }
  return projectForTransactionType
}

const getProjectForTransactionSubType = () => {
  const projectForTranxSubTypeForAddonNCorrection = {
    $switch: {
      branches: [
        {
          case: { $ifNull: ['$payoutId', false] },
          then: {
            $cond: [
              {
                $and: [
                  { $ifNull: ['$addonId', false] },
                  { $ifNull: ['$addonName', false] }
                ]
              },
              {
                $concat: ['Utbetaling tilleggstjeneste', ': ', '$addonName']
              },
              'Utbetaling tilleggstjeneste'
            ]
          }
        },
        {
          case: { $ifNull: ['$isCreditNoteAddon', false] },
          then: 'Kreditnota addon'
        }
      ],
      default: 'Faktura tilleggstjeneste'
    }
  }

  return {
    $switch: {
      branches: [
        {
          case: {
            $and: [
              { $eq: ['$subType', 'addon'] },
              { $eq: ['$type', 'correction'] }
            ]
          },
          then: projectForTranxSubTypeForAddonNCorrection
        },
        {
          case: {
            $eq: ['$subType', 'addon']
          },
          then: {
            $cond: [
              {
                $and: [
                  { $ifNull: ['$addonId', false] },
                  { $ifNull: ['$addonName', false] }
                ]
              },
              {
                $concat: ['Tillegstjeneste', ': ', '$addonName']
              },
              'Tillegstjeneste'
            ]
          }
        },
        {
          case: {
            $eq: ['$subType', 'payout_addon']
          },
          then: {
            $cond: [
              {
                $and: [
                  { $ifNull: ['$addonId', false] },
                  { $ifNull: ['$addonName', false] }
                ]
              },
              {
                $concat: ['Utbetaling tilleggstjeneste', ': ', '$addonName']
              },
              'Utbetaling tilleggstjeneste'
            ]
          }
        },
        {
          case: { $eq: ['$subType', 'addon_commission'] },
          then: {
            $cond: [
              {
                $and: [
                  { $ifNull: ['$addonId', false] },
                  { $ifNull: ['$addonName', false] }
                ]
              },
              {
                $concat: ['Provisjon tilleggstjeneste', ': ', '$addonName']
              },
              'Provisjon tilleggstjeneste'
            ]
          }
        },
        {
          case: {
            $eq: ['$subType', 'rent']
          },
          then: 'Månedlig leie'
        },
        {
          case: {
            $eq: ['$subType', 'rent_with_vat']
          },
          then: 'Husleie med MVA'
        },
        {
          case: {
            $eq: ['$subType', 'invoice_fee']
          },
          then: 'Fakturabeløp'
        },
        {
          case: {
            $eq: ['$subType', 'invoice_reminder_fee']
          },
          then: 'Purregebyrbeløp'
        },
        {
          case: {
            $eq: ['$subType', 'collection_notice_fee']
          },
          then: 'Inkassogebyrbeløp'
        },
        {
          case: {
            $eq: ['$subType', 'management_commission']
          },
          then: 'Forvaltningsprovisjon'
        },
        {
          case: {
            $eq: ['$subType', 'brokering_commission']
          },
          then: 'Oppdragssprovisjon'
        },
        {
          case: {
            $eq: ['$subType', 'rent_payment']
          },
          then: 'Innbetaling'
        },
        {
          case: {
            $eq: ['$subType', 'payout_to_landlords']
          },
          then: 'Utbetaling til utleier'
        },
        {
          case: {
            $eq: ['$subType', 'loss_recognition']
          },
          then: 'Tapsføring'
        },
        {
          case: {
            $eq: ['$subType', 'eviction_notice_fee']
          },
          then: 'Gebyr begjært fravikelse'
        },
        {
          case: {
            $eq: ['$subType', 'administration_eviction_notice_fee']
          },
          then: 'Gebyr skrive begjæring'
        },
        {
          case: {
            $eq: ['$subType', 'reminder_fee_move_to']
          },
          then: 'Purregebyr flyttet til'
        },
        {
          case: {
            $eq: ['$subType', 'collection_notice_fee_move_to']
          },
          then: 'Inkassovarselgebyr flyttet til'
        },
        {
          case: {
            $eq: ['$subType', 'eviction_notice_fee_move_to']
          },
          then: 'Gebyr begjært fravikelse flyttet til'
        },
        {
          case: {
            $eq: ['$subType', 'administration_eviction_notice_fee_move_to']
          },
          then: 'Gebyr skrive begjæring flyttet til'
        },
        {
          case: {
            $eq: ['$subType', 'unpaid_reminder']
          },
          then: 'Ubetalt purregebyr'
        },
        {
          case: {
            $eq: ['$subType', 'unpaid_collection_notice']
          },
          then: 'Ubetalt inkassovarselgebyr'
        },
        {
          case: {
            $eq: ['$subType', 'unpaid_eviction_notice']
          },
          then: 'Ubetalt gebyr begjært fravikelse'
        },
        {
          case: {
            $eq: ['$subType', 'unpaid_administration_eviction_notice']
          },
          then: 'Ubetalt Gebyr skrive begjæring'
        },
        {
          case: {
            $eq: ['$subType', 'rounded_amount']
          },
          then: 'Øreavrunding'
        },
        {
          case: {
            $eq: ['$subType', 'final_settlement_payment']
          },
          then: 'Betaling av sluttoppgjør'
        }
      ],
      default: '$subType'
    }
  }
}

const getTransactionAggregationPipelines = async (params = {}) => {
  const { accountType, options, partnerId, query } = params
  const { limit = 1000, sort = { createdAt: 1 }, skip = 0 } = options || {}
  const isBrokerPartner = accountType === 'broker'
  const projectForTransactionType = getProjectForTransactionType()
  const projectForTransactionSubType = getProjectForTransactionSubType()
  const { dateTimeSettings = {} } =
    (await partnerSettingHelper.getAPartnerSetting({ partnerId })) || {}
  const { dateFormat = 'DD.MM.YYYY', timezone = 'Europe/Oslo' } =
    dateTimeSettings || {}
  const mongoDateFormat = dateFormat === 'DD.MM.YYYY' ? '%d.%m.%Y' : '%Y.%m.%d'

  return [
    { $match: query },
    {
      $lookup: {
        from: 'users',
        localField: 'createdBy',
        foreignField: '_id',
        as: 'user'
      }
    },
    {
      $unwind: {
        path: '$user',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $sort: sort
    },
    {
      $project: {
        id: { $ifNull: ['$serialId', ''] },
        type: { $ifNull: [projectForTransactionType, ''] },
        subType: { $ifNull: [projectForTransactionSubType, ''] },
        subName: { $ifNull: ['$subName', ''] },
        assignmentNumber: { $ifNull: ['$assignmentNumber', ''] },
        kidNumber: { $ifNull: ['$kidNumber', ''] },
        invoiceId: { $ifNull: ['$invoiceSerialId', ''] },
        finalSettlementId: { $ifNull: ['$finalSettlementSerialId', ''] },
        payoutId: { $ifNull: ['$payoutSerialId', ''] },
        correctionId: { $ifNull: ['$correctionSerialId', ''] },
        landlordInvoiceId: { $ifNull: ['$landlordInvoiceSerialId', ''] },
        createdAt: {
          $ifNull: [
            {
              $dateToString: {
                format: mongoDateFormat,
                date: '$createdAt',
                timezone
              }
            },
            ''
          ]
        },
        invoiceDueDate: {
          $ifNull: [
            {
              $dateToString: {
                format: mongoDateFormat,
                date: '$invoiceDueDate',
                timezone
              }
            },
            ''
          ]
        },
        period: { $ifNull: ['$period', ''] },
        createdBy: {
          $cond: [
            { $ifNull: ['$user.profile.name', false] },
            '$user.profile.name',
            'Appbot'
          ]
        },
        agent: { $ifNull: ['$agentName', ''] },
        account: { $ifNull: ['$accountName', ''] },
        accountId: { $ifNull: ['$accountSerialId', ''] },
        accountAddress: { $ifNull: ['$accountAddress', ''] },
        accountZipCode: { $ifNull: ['$accountZipCode', ''] },
        accountCity: { $ifNull: ['$accountCity', ''] },
        accountCountry: { $ifNull: ['$accountCountry', ''] },
        tenant: { $ifNull: ['$tenantName', ''] },
        tenantId: { $ifNull: ['$tenantSerialId', ''] },
        tenantAddress: { $ifNull: ['$tenantAddress', ''] },
        tenantZipCode: { $ifNull: ['$tenantZipCode', ''] },
        tenantCity: { $ifNull: ['$tenantCity', ''] },
        tenantCountry: { $ifNull: ['$tenantCountry', ''] },
        tenantPhoneNumber: { $ifNull: ['$tenantPhoneNumber', ''] },
        tenantEmailAddress: { $ifNull: ['$tenantEmailAddress', ''] },
        property: { $ifNull: ['$locationName', ''] },
        propertyZipCode: { $ifNull: ['$locationZipCode', ''] },
        propertyCity: { $ifNull: ['$locationCity', ''] },
        propertyCountry: { $ifNull: ['$locationCountry', ''] },
        apartmentId: { $ifNull: ['$apartmentId', ''] },
        propertyId: { $ifNull: ['$propertySerialId', ''] },
        amount: { $ifNull: ['$amount', ''] },
        debit: { $ifNull: ['$debit', '$debitAccountCode'] },
        debitTaxCode: { $ifNull: ['$debitTaxCode', ''] },
        credit: { $ifNull: ['$creditAccountCode', ''] },
        creditTaxCode: { $ifNull: ['$creditTaxCode', ''] },
        branchId: { $ifNull: ['$branchSerialId', ''] },
        internalAssignmentId: { $ifNull: ['$internalAssignmentId', ''] },
        internalLeaseId: { $ifNull: ['$internalLeaseId', ''] },
        employeeId: { $ifNull: ['$employeeId', ''] },
        bankAccountNumber: { $ifNull: ['$bankAccountNumber', ''] },
        bankRef: { $ifNull: ['$bankRef', ''] },
        externalEntityId: { $ifNull: ['$externalEntityId', ''] },
        propertyGroupId: { $ifNull: ['$propertyGroupId', ''] },
        companyName: { $cond: [isBrokerPartner, '$companyName', '$$REMOVE'] },
        status: { $ifNull: ['$status', ''] }
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ]
}

export const getTransactionsForPartnerAPIForQuery = async (
  params = {},
  session
) => {
  const transactionPipelines = await getTransactionAggregationPipelines(params)
  const transactions = await TransactionCollection.aggregate(
    transactionPipelines
  ).session(session)
  return transactions
}

export const getTransactionsForPartnerAPI = async (req) => {
  const { body, session, user } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)

  const { options, query } = body
  const { sort } = options
  if (size(sort)) appHelper.validateSortForQuery(sort)

  const { apiKey = '', period = '' } = query

  const partnerInfo = await getPartnerInfoByValidatingApiKey(apiKey)
  const {
    _id: partnerId,
    accountType,
    enableTransactionsPeriod
  } = partnerInfo || {}

  if (enableTransactionsPeriod && !size(period)) {
    throw new CustomError(400, 'Transaction period is missing!')
  }

  const transactionQuery = {
    partnerId,
    status: { $nin: ['EXPORTED', 'ERROR'] }
  }
  if (period) transactionQuery.period = period

  const params = {
    accountType,
    options,
    partnerId,
    query: transactionQuery
  }
  const transactionsForPartnerAPI = await getTransactionsForPartnerAPIForQuery(
    params,
    session
  )
  const totalTransactionsCount = await countTransactions(transactionQuery)

  return {
    data: transactionsForPartnerAPI,
    total: totalTransactionsCount
  }
}

export const prepareSerialIdAppQueue = async (partnerId, session) => {
  const serialIdAppQueues = await appQueueHelper.getAppQueues(
    {
      action: 'add_transaction_serial',
      'params.partnerId': partnerId,
      status: { $ne: 'completed' }
    },
    session
  )

  if (size(serialIdAppQueues)) return {}
  else {
    const appQueue = {
      event: 'add_transaction_serial',
      destination: 'accounting',
      priority: 'regular',
      params: { partnerId },
      status: 'new',
      delaySeconds: Math.floor(Math.random() * (420 - 180 + 1)) + 180,
      action: 'add_transaction_serial'
    }
    return appQueue
  }
}

export const isTransactionEnabledForPartner = async (partnerId) => {
  if (!partnerId) return false

  const isTransactionEnabled = !!(await partnerHelper.getAPartner({
    _id: partnerId,
    enableTransactions: true
  }))

  return isTransactionEnabled
}

export const getTransactionTotalForAppHealth = async (partnerId, type) => {
  const pipeline = preparePipelineForAppHealthTransactionCommission(
    partnerId,
    type
  )
  const transactionAmount = await TransactionCollection.aggregate(pipeline)
  return transactionAmount
}

const preparePipelineForAppHealthTransactionCommission = (partnerId, type) => {
  let match = {}
  if (typeof type === 'string') {
    match = {
      type,
      partnerId
    }
  } else {
    match = {
      type: {
        $in: type
      },
      partnerId
    }
  }
  const pipeline = [
    { $match: match },
    {
      $group: {
        _id: null,
        totalAmount: { $sum: '$amount' }
      }
    }
  ]
  return pipeline
}

export const getTransactionsForApphealthToCompareInvoice = async (partnerId) =>
  TransactionCollection.aggregate([
    { $match: { type: 'correction', partnerId } },
    {
      $project: {
        amount: 1,
        subType: 1,
        totalRounded: {
          $cond: {
            if: { $eq: ['$subType', 'rounded_amount'] },
            then: '$amount',
            else: 0
          }
        }
      }
    },
    {
      $group: {
        _id: null,
        totalAmount: { $sum: '$amount' },
        totalRoundedAmount: { $sum: '$totalRounded' }
      }
    }
  ])
export const transactionAggregate = async (pipeline) =>
  TransactionCollection.aggregate(pipeline)

export const getLedgerAccountInfoFromUnsyncTransactions = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const pipeline = await getPipelineForLedgerAccountInfo(body)
  const ledgerAccounts = (await TransactionCollection.aggregate(pipeline)) || []
  return {
    data: ledgerAccounts
  }
}

export const getLedgerAccountsFromUnsyncTransactions = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId', 'context'], body)
  const pipeline = await getPipelineForLedgerAccountsInfo(body)
  const ledgerAccountsInfo =
    (await TransactionCollection.aggregate(pipeline)) || []
  return {
    data: ledgerAccountsInfo
  }
}

export const getTaxCodesFromUnsyncTransactions = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId', 'context'], body)
  const pipeline = await getPipelineForTaxCodesInfo(body)
  const taxCodesInfo = (await TransactionCollection.aggregate(pipeline)) || []
  return {
    data: taxCodesInfo
  }
}

const getPipelineForLedgerAccountInfo = async (query) => {
  const { fromDate, partnerId } = query
  const preparedQuery = { partnerId, powerOffice: { $exists: false } }
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (!size(partnerSetting))
    throw new CustomError(
      404,
      `Partner setting not found for partner ${partnerId}`
    )
  if (fromDate) {
    const partnerFromDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      fromDate
    )
    preparedQuery.createdAt = { $gte: partnerFromDate }
  }
  const pipeline = [
    {
      $match: preparedQuery
    },
    {
      $group: {
        _id: null,
        debitAccountCodes: {
          $addToSet: {
            $cond: [
              { $ifNull: ['$debitAccountCode', false] },
              '$debitAccountCode',
              '$$REMOVE'
            ]
          }
        },
        creditAccountCodes: {
          $addToSet: {
            $cond: [
              { $ifNull: ['$creditAccountCode', false] },
              '$creditAccountCode',
              '$$REMOVE'
            ]
          }
        }
      }
    },
    {
      $addFields: {
        accountCodes: {
          $setUnion: ['$debitAccountCodes', '$creditAccountCodes']
        }
      }
    },
    {
      $lookup: {
        from: 'ledger_accounts',
        localField: 'accountCodes',
        foreignField: 'accountNumber',
        as: 'ledgerAccounts',
        pipeline: [
          {
            $match: {
              partnerId
            }
          }
        ]
      }
    },
    {
      $unwind: '$ledgerAccounts'
    },
    {
      $lookup: {
        from: 'tax_codes',
        localField: 'ledgerAccounts.taxCodeId',
        foreignField: '_id',
        as: 'taxCodeInfo'
      }
    },
    {
      $unwind: {
        path: '$taxCodeInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        _id: '$ledgerAccounts._id',
        accountNumber: '$ledgerAccounts.accountNumber',
        vatCode: '$taxCodeInfo.taxCode'
      }
    },
    {
      $group: {
        _id: { accountNumber: '$accountNumber', vatCode: '$vatCode' },
        accountNumber: {
          $first: '$accountNumber'
        },
        vatCode: {
          $first: '$vatCode'
        }
      }
    }
  ]
  return pipeline
}

const getPipelineForLedgerAccountsInfo = async (query) => {
  const { context, fromDate, partnerId } = query
  const preparedQuery = { partnerId, [context]: { $exists: false } }
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (!size(partnerSetting))
    throw new CustomError(
      404,
      `Partner setting not found for partner ${partnerId}`
    )
  if (fromDate) {
    const partnerFromDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      fromDate
    )
    preparedQuery.createdAt = { $gte: partnerFromDate }
  }
  let matchPipeline = []
  if (context === 'xledger') {
    matchPipeline = await getNotMappedAccountPipelineForXledger(partnerId)
  }
  const pipeline = [
    {
      $match: preparedQuery
    },
    {
      $group: {
        _id: null,
        debitAccountIds: {
          $addToSet: {
            $cond: [
              { $ifNull: ['$debitAccountId', false] },
              '$debitAccountId',
              '$$REMOVE'
            ]
          }
        },
        creditAccountIds: {
          $addToSet: {
            $cond: [
              { $ifNull: ['$creditAccountId', false] },
              '$creditAccountId',
              '$$REMOVE'
            ]
          }
        }
      }
    },
    {
      $addFields: {
        accountIds: {
          $setUnion: ['$debitAccountIds', '$creditAccountIds']
        }
      }
    },
    {
      $unwind: '$accountIds'
    },
    ...matchPipeline,
    {
      $lookup: {
        from: 'ledger_accounts',
        localField: 'accountIds',
        foreignField: '_id',
        as: 'ledgerAccountInfo'
      }
    },
    {
      $unwind: '$ledgerAccountInfo'
    },
    {
      $project: {
        _id: '$accountIds',
        accountNumber: '$ledgerAccountInfo.accountNumber'
      }
    }
  ]
  return pipeline
}

const getNotMappedAccountPipelineForXledger = async (partnerId) => {
  const integrationInfo = await integrationHelper.getAnIntegration({
    partnerId,
    type: 'xledger'
  })
  const pipeline = []
  if (size(integrationInfo?.mapXledgerAccounts)) {
    const mappedAccounts = integrationInfo.mapXledgerAccounts.map(
      (item) => item.accountingId
    )
    pipeline.push({
      $match: {
        accountIds: {
          $nin: mappedAccounts
        }
      }
    })
  }
  return pipeline
}

const getNotMappedTaxCodePipelineForXledger = async (partnerId) => {
  const integrationInfo = await integrationHelper.getAnIntegration({
    partnerId,
    type: 'xledger'
  })
  const pipeline = []
  if (size(integrationInfo?.mapXledgerTaxCodes)) {
    const mappedTaxCodes = integrationInfo.mapXledgerTaxCodes.map(
      (item) => item.taxCodeId
    )
    pipeline.push({
      $match: {
        taxCodeIds: {
          $nin: mappedTaxCodes
        }
      }
    })
  }
  return pipeline
}

const getPipelineForTaxCodesInfo = async (query) => {
  const { context, fromDate, partnerId } = query
  const preparedQuery = { partnerId, [context]: { $exists: false } }
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (!size(partnerSetting))
    throw new CustomError(
      404,
      `Partner setting not found for partner ${partnerId}`
    )
  if (fromDate) {
    const partnerFromDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      fromDate
    )
    preparedQuery.createdAt = { $gte: partnerFromDate }
  }
  let matchPipeline = []
  if (context === 'xledger') {
    matchPipeline = await getNotMappedTaxCodePipelineForXledger(partnerId)
  }
  const pipeline = [
    {
      $match: preparedQuery
    },
    {
      $group: {
        _id: null,
        debitTaxCodeIds: {
          $addToSet: {
            $cond: [
              { $ifNull: ['$debitTaxCodeId', false] },
              '$debitTaxCodeId',
              '$$REMOVE'
            ]
          }
        },
        creditTaxCodeIds: {
          $addToSet: {
            $cond: [
              { $ifNull: ['$creditTaxCodeId', false] },
              '$creditTaxCodeId',
              '$$REMOVE'
            ]
          }
        }
      }
    },
    {
      $addFields: {
        taxCodeIds: {
          $setUnion: ['$debitTaxCodeIds', '$creditTaxCodeIds']
        }
      }
    },
    {
      $unwind: '$taxCodeIds'
    },
    {
      $lookup: {
        from: 'tax_codes',
        localField: 'taxCodeIds',
        foreignField: '_id',
        as: 'taxCodeInfo'
      }
    },
    {
      $unwind: '$taxCodeInfo'
    },
    ...matchPipeline,
    {
      $project: {
        _id: '$taxCodeIds',
        taxCode: '$taxCodeInfo.taxCode'
      }
    }
  ]
  return pipeline
}

const getDetailedBalanceReportForQuery = async (params = {}) => {
  const { periodDateRange, query, options } = params
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $project: {
        _id: 1,
        amount: 1,
        createdAt: 1,
        type: 1,
        subType: 1,
        invoiceId: 1,
        correctionId: 1,
        paymentId: 1,
        invoiceSerialId: 1,
        invoiceDueDate: 1,
        propertyId: 1,
        tenantId: 1
      }
    },
    ...getOpeningBalancePipeline(periodDateRange),
    ...appHelper.getCommonTenantInfoPipeline(),
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getBalancePipelineForEachTransaction(),
    ...getClosingBalancePipeline(),
    {
      $unwind: {
        path: '$transactions',
        preserveNullAndEmptyArrays: true
      }
    },
    ...getPropertyInfoPipelineForDetailedBalanceReport(),
    {
      $group: {
        _id: '$_id',
        tenantInfo: {
          $first: '$tenantInfo'
        },
        transactions: {
          $push: {
            $cond: [
              { $ifNull: ['$transactions._id', false] },
              '$transactions',
              '$$REMOVE'
            ]
          }
        },
        openingBalance: {
          $first: '$openingBalance'
        },
        closingBalance: {
          $first: '$closingBalance'
        }
      }
    },
    {
      $sort: sort
    },
    {
      $project: {
        _id: 1,
        openingBalance: 1,
        closingBalance: 1,
        tenantInfo: {
          _id: 1,
          avatarKey: 1,
          name: 1,
          serial: 1
        },
        transactions: {
          _id: 1,
          type: 1,
          subType: 1,
          invoiceId: 1,
          correctionId: 1,
          paymentId: 1,
          invoiceSerialId: 1,
          invoiceDueDate: 1,
          createdAt: 1,
          amount: 1,
          balance: 1,
          propertyInfo: {
            _id: 1,
            apartmentId: 1,
            serial: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              postalCode: 1
            }
          }
        }
      }
    }
  ]
  const balanceReports = (await TransactionCollection.aggregate(pipeline)) || []
  return balanceReports
}

const getBalancePipelineForEachTransaction = () => [
  {
    $addFields: {
      transactions: {
        $function: {
          /* eslint-disable-next-line */
          body: function (transactions, openingBalance) {
            for (let i = 0; i < transactions.length; i++) {
              const transaction = transactions[i]
              openingBalance = openingBalance + transaction.amount
              transaction.balance = openingBalance
            }
            return transactions
          },
          args: ['$transactions', '$openingBalance'],
          lang: 'js'
        }
      }
    }
  }
]

const getOpeningBalancePipeline = (periodDateRange = {}) => {
  let openingBalanceCond = 0
  let transactionsCond = '$$ROOT'
  if (size(periodDateRange)) {
    const { startDate, endDate } = periodDateRange
    openingBalanceCond = {
      $cond: [
        {
          $lt: ['$createdAt', startDate]
        },
        '$amount',
        0
      ]
    }
    transactionsCond = {
      $cond: [
        {
          $and: [
            { $gte: ['$createdAt', startDate] },
            { $lte: ['$createdAt', endDate] }
          ]
        },
        '$$ROOT',
        '$$REMOVE'
      ]
    }
  }
  const pipeline = [
    {
      $addFields: {
        amount: {
          $cond: {
            if: {
              $or: [
                { $in: ['$type', ['payment', 'refund']] },
                { $eq: ['$subType', 'loss_recognition'] }
              ]
            },
            then: { $multiply: ['$amount', -1] },
            else: '$amount'
          }
        }
      }
    },
    {
      $addFields: {
        openingBalance: openingBalanceCond
      }
    },
    {
      $sort: { createdAt: 1 }
    },
    {
      $group: {
        _id: '$tenantId',
        tenantId: {
          $first: '$tenantId'
        },
        openingBalance: {
          $sum: '$openingBalance'
        },
        transactions: {
          $push: transactionsCond
        }
      }
    }
  ]
  return pipeline
}

const getClosingBalancePipeline = () => [
  {
    $addFields: {
      lastTransaction: {
        $last: { $ifNull: ['$transactions', []] }
      }
    }
  },
  {
    $addFields: {
      closingBalance: {
        $ifNull: ['$lastTransaction.balance', '$openingBalance']
      }
    }
  }
]

const getPropertyInfoPipelineForDetailedBalanceReport = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'transactions.propertyId',
      foreignField: '_id',
      as: 'propertyInfo'
    }
  },
  {
    $unwind: {
      path: '$propertyInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      'transactions.propertyInfo': '$propertyInfo'
    }
  }
]

export const getDetailedBalanceReport = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], user)
  const { partnerId } = user
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  const { periodDateRange, preparedQuery } =
    await prepareQueryForDetailedBalanceReport(query)
  const data = await getDetailedBalanceReportForQuery({
    periodDateRange,
    query: preparedQuery,
    options
  })
  const totalDocuments = await countTransactionsForAField('tenantId', {
    partnerId,
    $or: [
      { type: { $in: ['invoice', 'credit_note'] } },
      {
        $and: [
          { type: 'correction' },
          { subType: { $in: ['addon', 'rounded_amount'] } }
        ]
      },
      {
        $and: [
          { type: { $in: ['payment', 'refund'] } },
          { landlordPayment: { $ne: true } }
        ]
      }
    ]
  })
  const filteredDocuments = await countTransactionsForAField(
    'tenantId',
    preparedQuery
  )
  return {
    data,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const prepareQueryForDetailedBalanceReport = async (query = {}) => {
  const { period, partnerId, propertyId, tenantId } = query
  const preparedQuery = {
    partnerId,
    $or: [
      { type: { $in: ['invoice', 'credit_note'] } },
      {
        $and: [
          { type: 'correction' },
          { subType: { $in: ['addon', 'rounded_amount'] } }
        ]
      },
      {
        $and: [
          { type: { $in: ['payment', 'refund'] } },
          { landlordPayment: { $ne: true } }
        ]
      }
    ]
  }
  let periodDateRange = {}
  if (size(period) && period.startDate && period.endDate) {
    const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
      partnerId
    )
    periodDateRange = {
      startDate: (
        await appHelper.getActualDate(partnerSetting, true, period.startDate)
      )
        .startOf('day')
        .toDate(),
      endDate: (
        await appHelper.getActualDate(partnerSetting, true, period.endDate)
      )
        .endOf('day')
        .toDate()
    }
  }
  if (propertyId) preparedQuery.propertyId = propertyId
  if (tenantId) preparedQuery.tenantId = tenantId
  return {
    periodDateRange,
    preparedQuery
  }
}

const prepareSortForDetailedBalanceReport = (sort = {}) => {
  if (sort.tenantInfo_name) {
    sort['tenantInfo.name'] = sort.tenantInfo_name
  }
  return omit(sort, ['tenantInfo_name'])
}

export const prepareSortForDownloadDetailedBalanceReport = (sort = {}) => {
  if (sort['tenantInfo.name']) {
    sort['tenantInfo_name'] = sort['tenantInfo.name']
  }
  return omit(sort, ['tenantInfo.name'])
}

export const queryDetailedBalnaceReportForExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  query.userId = userId
  appHelper.checkRequiredFields(['partnerId'], query)
  appHelper.validateId({ partnerId: query.partnerId })

  options.sort = prepareSortForDetailedBalanceReport(options.sort)

  const { userLanguage = 'en' } = query

  const { preparedQuery, periodDateRange } =
    await prepareQueryForDetailedBalanceReport(query)

  const queryData = {
    query: preparedQuery,
    options,
    periodDateRange,
    language: userLanguage
  }
  const transactions = await getDetailedBalanceReportDatasForExcelManager(
    queryData
  )

  return transactions
}

const getDetailedBalanceReportDatasForExcelManager = async (params) => {
  const { query, options, periodDateRange, language } = params
  const { sort, skip, limit } = options

  const pipeline = [
    {
      $match: query
    },
    {
      $project: {
        _id: 1,
        amount: 1,
        createdAt: 1,
        type: 1,
        subType: 1,
        invoiceSerialId: 1,
        invoiceDueDate: 1,
        propertyId: 1,
        tenantId: 1
      }
    },
    ...getOpeningBalancePipeline(periodDateRange),
    ...getTenantInfoForDetailedBalanceReport(),
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $sort: sort
    },
    ...getTransactionsForDetailedBalanceReport(),
    {
      $unwind: {
        path: '$transactions',
        preserveNullAndEmptyArrays: true
      }
    },
    ...getPropertyInfoPipelineForDetailedBalanceReport(),
    ...getFinalProjectForDetailedBalanceReport(language)
  ]

  const transection = await TransactionCollection.aggregate(pipeline)

  return {
    data: transection || []
  }
}

const getTenantInfoForDetailedBalanceReport = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      as: 'tenantInfo'
    }
  },
  {
    $unwind: {
      path: '$tenantInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getTransactionsForDetailedBalanceReport = () => [
  {
    $addFields: {
      transactions: {
        $function: {
          /* eslint-disable-next-line */
          body: function (transactions, openingBalance) {
            for (let i = 0; i < transactions.length; i++) {
              const transaction = transactions[i]
              openingBalance = openingBalance + transaction.amount
              transaction.balance = openingBalance
            }
            const firstTransaction = transactions[0]
            transactions.unshift({
              type: 'opening_balance',
              subType: 'opening_balance',
              createdAt: firstTransaction?.createdAt,
              amount: 0,
              balance: openingBalance,
              propertyId: firstTransaction?.propertyId || ''
            })
            const lastTransaction = transactions[transactions.length - 1] || {}
            transactions.push({
              type: 'closing_balance',
              subType: 'closing_balance',
              createdAt: lastTransaction?.createdAt, // Should be endDate if last transaction not exist
              amount: 0,
              balance: lastTransaction?.balance || openingBalance,
              propertyId: lastTransaction?.propertyId || ''
            })
            transactions.push({
              type: 'change_in_balance',
              subType: 'change_in_balance',
              amount: 0,
              balance: lastTransaction?.balance || 0 - openingBalance,
              propertyId: lastTransaction?.propertyId || ''
            })
            return transactions
          },
          args: ['$transactions', '$openingBalance'],
          lang: 'js'
        }
      }
    }
  }
]

const getFinalProjectForDetailedBalanceReport = (language) => [
  {
    $project: {
      type: getProjectTypeForDetailedBalanceReport(language),
      subType: getProjectSubTypeForDetailedBalanceReport(language),
      invoiceSerialId: '$transactions.invoiceSerialId',
      createdAt: '$transactions.createdAt',
      invoiceDueDate: '$transactions.invoiceDueDate',
      tenantName: '$tenantInfo.name',
      tenantSerial: '$tenantInfo.serial',
      propertyName: {
        $concat: [
          '$propertyInfo.location.name',
          ', ',
          '$propertyInfo.location.postalCode',
          ', ',
          '$propertyInfo.location.city',
          ', ',
          '$propertyInfo.location.country'
        ]
      },
      apartmentId: '$propertyInfo.apartmentId',
      propertySerial: '$propertyInfo.serial',
      amount: '$transactions.amount',
      balance: '$transactions.balance'
    }
  }
]

const getProjectTypeForDetailedBalanceReport = (language) => {
  const types = [
    'invoice',
    'credit_note',
    'payment',
    'refund',
    'correction',
    'opening_balance',
    'closing_balance',
    'change_in_balance'
  ]
  const branches = []

  for (const type of types) {
    branches.push({
      case: {
        $eq: ['$transactions.type', type]
      },
      then: appHelper.translateToUserLng('transactions.type.' + type, language)
    })
  }

  return {
    $switch: {
      branches,
      default: '$transactions.type'
    }
  }
}

const getProjectSubTypeForDetailedBalanceReport = (language) => {
  const subTypes = [
    'addon',
    'administration_eviction_notice_fee',
    'administration_eviction_notice_fee_move_to',
    'collection_notice_fee',
    'collection_notice_fee_move_to',
    'eviction_notice_fee',
    'eviction_notice_fee_move_to',
    'final_settlement_payment',
    'invoice_fee',
    'invoice_reminder_fee',
    'loss_recognition',
    'payout_addon',
    'reminder_fee_move_to',
    'rent',
    'rent_payment',
    'rent_with_vat',
    'rounded_amount',
    'unpaid_administration_eviction_notice',
    'unpaid_collection_notice',
    'unpaid_eviction_notice',
    'unpaid_reminder',
    'opening_balance',
    'closing_balance',
    'change_in_balance'
  ]
  const branches = []

  for (const subType of subTypes) {
    branches.push({
      case: {
        $eq: ['$transactions.subType', subType]
      },
      then: appHelper.translateToUserLng(
        'transactions.sub_type.' + subType,
        language
      )
    })
  }

  return {
    $switch: {
      branches,
      default: '$transactions.subType'
    }
  }
}

export const getUniqueFieldValueFromTransactions = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['requestFor', 'context'])
  const { body = {} } = req
  const { context, partnerId, requestFor } = body
  const query = {
    partnerId,
    [context]: {
      $exists: true,
      $ne: ''
    }
  }
  let transactionValues = await TransactionCollection.distinct(context, query)
  if (requestFor === 'xledger' && size(transactionValues)) {
    const integrationQuery = {
      partnerId
    }
    const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
    if (isDirectPartner) integrationQuery.isGlobal = true
    const integrationInfo = await integrationHelper.getAnIntegration(
      integrationQuery
    )
    if (!size(integrationInfo))
      throw new CustomError(404, 'Integration info not found')
    transactionValues = filterUniqueDataFromMappedDataForXledger(
      integrationInfo,
      context,
      transactionValues
    )
  }
  return {
    data: transactionValues
  }
}

const filterUniqueDataFromMappedDataForXledger = (
  integrationInfo = {},
  context,
  transactionValues
) => {
  let mappedValues = []
  if (context === 'internalAssignmentId')
    mappedValues = (integrationInfo.mapXledgerInternalAssignmentIds || []).map(
      (item) => item.internalAssignmentId
    )
  else if (context === 'internalLeaseId')
    mappedValues = (integrationInfo.mapXledgerInternalLeaseIds || []).map(
      (item) => item.internalLeaseId
    )
  else if (context === 'employeeId')
    mappedValues = (integrationInfo.mapXledgerEmployeeIds || []).map(
      (item) => item.employeeId
    )
  if (size(mappedValues) && size(transactionValues)) {
    return difference(transactionValues, mappedValues)
  } else {
    return transactionValues
  }
}
