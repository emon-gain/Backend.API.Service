import nid from 'nid'
import moment from 'moment-timezone'
import checkDigit from 'checkdigit'
import {
  assign,
  clone,
  compact,
  difference,
  each,
  extend,
  filter,
  find,
  includes,
  indexOf,
  intersection,
  isArray,
  map,
  pick,
  size,
  sortBy,
  union,
  uniq
} from 'lodash'
import { CustomError } from '../common'
import {
  CommissionCollection,
  ContractCollection,
  CounterCollection,
  InvoiceCollection,
  PayoutCollection
} from '../models'
import {
  accountHelper,
  accountingHelper,
  addonHelper,
  appHelper,
  appInvoiceHelper,
  appQueueHelper,
  annualStatementHelper,
  branchHelper,
  commissionHelper,
  contractHelper,
  dashboardHelper,
  correctionHelper,
  fileHelper,
  invoiceHelper,
  invoicePaymentHelper,
  invoiceSummaryHelper,
  ledgerAccountHelper,
  partnerHelper,
  partnerSettingHelper,
  payoutHelper,
  taxCodeHelper,
  tenantHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import { contractService, invoiceService } from '../services'
import { appPermission } from '../common'

export const getInvoicesViaAggregation = async (pipelines, session) => {
  if (!size(pipelines)) {
    throw new CustomError(
      400,
      'Pipelines missing for invoice aggregation query'
    )
  }

  return await InvoiceCollection.aggregate(pipelines).session(session)
}

export const checkCreditNoteFeesMetaTransaction = (params) => {
  const {
    feeMeta,
    numOfExistingTransactions,
    unpaidCollectionNoticeFees,
    feesTotal,
    type
  } = params
  let { isTransactionExist } = params
  console.log('isTransactionExist 62', isTransactionExist)
  console.log('numOfExistingTransactions 63', numOfExistingTransactions)
  console.log('type 64', type)
  console.log('feeMeta.type 65', feeMeta.type)
  console.log(
    'size(unpaidCollectionNoticeFees) 67',
    size(unpaidCollectionNoticeFees)
  )
  if (
    numOfExistingTransactions &&
    type === 'credit_note' &&
    feeMeta.type === 'unpaid_collection_notice' &&
    size(unpaidCollectionNoticeFees) > 1
  ) {
    const noOfUnpaidCollectionNoticeFees = filter(
      unpaidCollectionNoticeFees,
      (unpaidNoticeFee) =>
        unpaidNoticeFee && unpaidNoticeFee.total === feesTotal
    )
    console.log('numOfExistingTransactions 81', numOfExistingTransactions)
    console.log(
      'noOfUnpaidCollectionNoticeFees 83',
      size(noOfUnpaidCollectionNoticeFees)
    )
    if (numOfExistingTransactions < size(noOfUnpaidCollectionNoticeFees)) {
      isTransactionExist = false
    }
  }
  console.log('isTransactionExist 90', isTransactionExist)
  return isTransactionExist
}

export const getUniqueFieldValue = async (field, query) =>
  (await InvoiceCollection.distinct(field, query)) || []

export const getAccountingAndTransactionSubTypeFromFeesMeta = (feeMetaType) => {
  let accountingType = ''
  let transactionSubtype = ''
  console.log('feeMetaType === ', feeMetaType)
  if (feeMetaType === 'invoice') {
    accountingType = 'invoice_fee'
  } else if (feeMetaType === 'reminder') {
    accountingType = 'invoice_reminder_fee'
  } else if (feeMetaType === 'collection_notice') {
    accountingType = 'collection_notice_fee'
  } else if (feeMetaType === 'eviction_notice') {
    accountingType = 'eviction_notice_fee'
  } else if (feeMetaType === 'administration_eviction_notice') {
    accountingType = 'administration_eviction_notice_fee'
  } else if (
    feeMetaType === 'unpaid_reminder' ||
    feeMetaType === 'reminder_fee_move_to'
  ) {
    accountingType = 'invoice_reminder_fee'
    transactionSubtype = feeMetaType
  } else if (
    feeMetaType === 'collection_notice_fee_move_to' ||
    feeMetaType === 'unpaid_collection_notice'
  ) {
    accountingType = 'collection_notice_fee'
    transactionSubtype = feeMetaType
  } else if (
    feeMetaType === 'unpaid_eviction_notice' ||
    feeMetaType === 'eviction_notice_fee_move_to'
  ) {
    accountingType = 'eviction_notice_fee'
    transactionSubtype = feeMetaType
  } else if (
    feeMetaType === 'unpaid_administration_eviction_notice' ||
    feeMetaType === 'administration_eviction_notice_fee_move_to'
  ) {
    accountingType = 'administration_eviction_notice_fee'
    transactionSubtype = feeMetaType
  }
  console.log('accountingType === ', accountingType)
  console.log('transactionSubtype === ', transactionSubtype)
  return { accountingType, transactionSubtype }
}

export const allowedTypesOfInvoiceFeesMetaForTransaction = [
  'administration_eviction_notice_fee_move_to',
  'administration_eviction_notice',
  'collection_notice_fee_move_to',
  'collection_notice',
  'eviction_notice',
  'eviction_notice_fee_move_to',
  'invoice',
  'reminder_fee_move_to',
  'reminder',
  'unpaid_administration_eviction_notice',
  'unpaid_collection_notice',
  'unpaid_eviction_notice',
  'unpaid_reminder'
]

export const allowedTypesOfInvoiceMoveToFeesTransaction = [
  'administration_eviction_notice_fee_move_to',
  'collection_notice_fee_move_to',
  'eviction_notice_fee_move_to',
  'reminder_fee_move_to'
]

export const getLastInvoiceOfAContract = async (contractId) => {
  const lastInvoice = await InvoiceCollection.findOne({
    contractId,
    invoiceType: 'invoice'
  }).sort({ invoiceEndOn: -1 })
  return lastInvoice
}

export const getAnInvoiceWithSort = async (query, sort, session) =>
  await InvoiceCollection.findOne(query).sort(sort).session(session)

export const isFirstInvoiceOfAContract = async (contractId, session) => {
  const query = {
    contractId,
    leaseCancelled: { $ne: true },
    isCorrectionInvoice: { $ne: true },
    invoiceType: 'invoice'
  }
  const invoice = await getInvoice(query, session)
  console.log('isFirstInvoiceOfAContract:', contractId, invoice)
  return !invoice
}

export const handleGetRequestForInvoices = async (req) => {
  let result = []
  const { body, session, user } = req
  const { partnerId } = user
  const {
    contractId,
    returnPreview,
    today,
    invoiceType,
    isCorrectionInvoice,
    correctionId
  } = body
  if (returnPreview && invoiceType === 'invoice') {
    const contract = await contractHelper.getAContract(
      { _id: contractId, partnerId },
      session
    )
    const previewParams = { contract, today }
    if (isCorrectionInvoice && correctionId) {
      previewParams.correctionId = correctionId
      result = await getCorrectionInvoicePreview(previewParams, session)
    } else if (!isCorrectionInvoice) {
      previewParams.returnEstimatedPayoutPreview = false
      result = await getInvoicesPreview(previewParams, session)
    }
  }
  return result
}

export const isAddEstimatedPayout = (invoiceData) =>
  invoiceData.isFirstInvoice && isNotLandlord(invoiceData)

export const isAddInvoiceCommission = async (invoiceData) => {
  if (!size(invoiceData)) {
    return false
  }
  const { partnerId } = invoiceData
  const isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  return isBrokerPartner && isNotLandlord(invoiceData)
}

export const getBasicInvoiceDataForTenant = async (
  contract,
  date,
  isDemo,
  returnPreview
) => {
  let tenant
  const { _id, accountId, agentId, branchId, partnerId, propertyId } = contract
  const { invoiceFrequency: contractFrequency, tenantId } = contract.rentalMeta
    ? contract.rentalMeta
    : {}
  const tenants =
    contract.rentalMeta &&
    isArray(contract.rentalMeta.tenants) &&
    size(contract.rentalMeta.tenants)
      ? contract.rentalMeta.tenants
      : []
  if (tenantId) {
    tenant = await tenantHelper.getTenantById(tenantId)
  }
  if (!tenant && !returnPreview) {
    throw new CustomError(
      404,
      'Trying to create rent invoice but tenant Id is wrong. contractId: ' +
        contract._id
    )
  }
  const invoiceData = {
    contractId: _id,
    partnerId,
    tenantId,
    accountId,
    agentId,
    propertyId,
    branchId,
    tenants,
    receiver: {
      tenantName: tenant && tenant.name ? tenant.name : ''
    },
    totalPaid: 0,
    status: 'new',
    invoiceType: 'invoice',
    invoiceFrequency: contractFrequency || 1
  }
  return invoiceData
}

export const getInitialDataForRentInvoice = async (
  contractId,
  date,
  isDemo,
  returnPreview
) => {
  const query = {
    _id: contractId,
    hasRentalContract: true,
    rentalMeta: { $exists: true },
    'rentalMeta.status': { $in: ['active', 'upcoming'] }
  }
  const contract = await contractHelper.getAContract(query, undefined, [
    'partner',
    'partnerSetting'
  ])
  if (!(size(contract) && contract.partner && contract.partnerSetting)) {
    throw new CustomError(
      404,
      'contract_is_not_available_for_creating_invoice_now'
    )
  }
  // Fetch Partner Setting
  const partnerSetting = contract.partnerSetting
  // Get actual date depending on the partner setting
  const actualDate = (await appHelper.getActualDate(partnerSetting, true, date))
    .endOf('day')
    .toDate()
  const invoiceBasicData = await getBasicInvoiceDataForTenant(
    contract,
    actualDate,
    isDemo,
    returnPreview
  )
  if (!invoiceBasicData) {
    return false
  }
  return {
    contract,
    partnerSetting,
    actualDate,
    invoiceBasicData
  }
}

export const isFirstInvoiceAllowed = (contract) => {
  let isAllowed = false
  const { rentalMeta } = contract
  const enabledLeaseEsigning = !!(rentalMeta && rentalMeta.enabledLeaseEsigning)
  if (!enabledLeaseEsigning) {
    isAllowed = true
  }
  if (enabledLeaseEsigning && contract.isAllSignCompleted()) {
    isAllowed = true
  }
  return isAllowed
}

export const getFirstInvoiceCreationDate = async (
  date,
  days,
  partnerSetting
) => {
  const invoiceCreationDate = (
    await appHelper.getActualDate(partnerSetting, true, date)
  )
    .subtract(days || 0, 'days')
    .toDate()
  return invoiceCreationDate
}

export const getListOfMonths = async (params = {}) => {
  const {
    endMonthDate,
    invoiceFrequency = 1,
    partnerSetting,
    startMonthDate
  } = params
  const monthList = []
  const startingMonth = (
    await appHelper.getActualDate(partnerSetting, true, startMonthDate)
  ).startOf('month')
  let monthRange = (
    await appHelper.getActualDate(partnerSetting, true, endMonthDate)
  ).endOf('month')
  monthRange = monthRange.diff(startingMonth, 'months')
  for (let i = 0; i <= monthRange; i += invoiceFrequency) {
    const date = (
      await appHelper.getActualDate(partnerSetting, true, startMonthDate)
    )
      .add(i, 'months')
      .set('date', 2)
      .toDate() // Set any date.. We need only month and year.
    monthList.push(date)
  }
  return monthList
}

export const compareNextCPIDateWithInvoiceDueDate = async (
  invoiceBasicData,
  contract
) => {
  if (
    invoiceBasicData.dueDate &&
    contract.rentalMeta &&
    contract.rentalMeta.futureRentAmount &&
    contract.rentalMeta.nextCpiDate
  ) {
    const dueDate = await appHelper.getActualDate(
      contract.partnerId,
      false,
      invoiceBasicData.dueDate
    )
    const nextCpiDate = await appHelper.getActualDate(
      contract.partnerId,
      false,
      contract.rentalMeta.nextCpiDate
    )
    const dueDateMoment = (
      await appHelper.getActualDate(
        contract.partnerId,
        true,
        invoiceBasicData.dueDate
      )
    ).format('YYYY-MM-DD')
    const nextCpiDateMoment = (
      await appHelper.getActualDate(
        contract.partnerId,
        true,
        contract.rentalMeta.nextCpiDate
      )
    ).format('YYYY-MM-DD')
    if (dueDate >= nextCpiDate || dueDateMoment === nextCpiDateMoment) {
      return true
    }
  }
  return false
}

export const isThisMonthInvoiceExist = async (data, session) => {
  const { contract, startOfMonth, endOfMonth } = data
  const query = {
    contractId: contract._id,
    invoiceType: 'invoice',
    leaseCancelled: { $ne: true },
    isCorrectionInvoice: { $ne: true },
    $or: [
      {
        $or: [
          { invoiceFrequency: 1 },
          { invoiceFrequency: { $exists: false } }
        ],
        invoiceMonth: { $gte: startOfMonth, $lte: endOfMonth }
      },
      {
        invoiceFrequency: { $gt: 1 },
        invoiceMonths: { $gte: startOfMonth, $lte: endOfMonth }
      },
      {
        invoiceEndOn: { $gte: endOfMonth }
      }
    ]
  }
  const invoiceExistStatus = await InvoiceCollection.findOne(query).session(
    session
  )
  return !!invoiceExistStatus
}

export const getStartAndEndMonths = async (params = {}) => {
  const {
    invoiceFrequency,
    invoiceMonth,
    manualInvoiceCreateOption,
    partnerSetting
  } = params
  let startOfMonth = (
    await appHelper.getActualDate(partnerSetting, true, invoiceMonth)
  )
    .startOf('month')
    .toDate() // Rent start on
  let endOfMonth = (
    await appHelper.getActualDate(partnerSetting, true, invoiceMonth)
  )
    .add(invoiceFrequency - 1, 'months')
    .endOf('month')
    .toDate() // Rent end on
  if (manualInvoiceCreateOption && manualInvoiceCreateOption.isManualInvoice) {
    if (manualInvoiceCreateOption.startOn) {
      startOfMonth = (
        await appHelper.getActualDate(
          partnerSetting,
          true,
          manualInvoiceCreateOption.startOn
        )
      ).toDate()
    }
    if (manualInvoiceCreateOption.endOn) {
      endOfMonth = (
        await appHelper.getActualDate(
          partnerSetting,
          true,
          manualInvoiceCreateOption.endOn
        )
      ).toDate()
    }
  }
  return {
    startOfMonth,
    endOfMonth
  }
}

export const getInvoiceAccountNumber = async (
  contract,
  partnerSetting,
  booleanParams
) => {
  const { isFirstInvoice, isLandlordInvoice } = booleanParams
  const isDirectPartner = await partnerHelper.getDirectPartnerById(
    contract.partnerId
  )
  if (isDirectPartner && contract.rentalMeta.invoiceAccountNumber) {
    return contract.rentalMeta.invoiceAccountNumber
  }
  const bankPayment =
    partnerSetting && partnerSetting.bankPayment
      ? partnerSetting.bankPayment
      : ''
  const bankPaymentInfo =
    isLandlordInvoice &&
    partnerSetting &&
    size(partnerSetting.landlordBankPayment)
      ? partnerSetting.landlordBankPayment
      : bankPayment
  if (bankPaymentInfo) {
    const afterFirstMonthAcNo = bankPaymentInfo.afterFirstMonthACNo
      ? bankPaymentInfo.afterFirstMonthACNo
      : bankPaymentInfo.firstMonthACNo
    return isFirstInvoice && bankPaymentInfo.firstMonthACNo
      ? bankPaymentInfo.firstMonthACNo
      : afterFirstMonthAcNo
  }
}

export const getBasicInvoiceData = async (data) => {
  const { invoiceParams, startOfMonth, endOfMonth } = data
  const {
    contract,
    ignoreRecurringDueDate,
    invoiceBasicData,
    invoiceCountFromBeginning,
    invoiceFrequency,
    isFirstInvoice,
    partnerSetting,
    returnPreview
  } = invoiceParams
  const invoiceCalculationData = {
    contract,
    endOfMonth,
    invoiceCountFromBeginning,
    invoiceFrequency,
    partnerSetting,
    startOfMonth
  }
  const invoiceCalculationInfo = await getInvoiceCalculationInfo(
    invoiceCalculationData
  )
  const { invoiceStartDate, invoiceEndDate, isNotFullMonth, invoiceMonthDate } =
    invoiceCalculationInfo
  if (invoiceMonthDate) {
    invoiceBasicData.invoiceMonth = invoiceMonthDate
  }
  invoiceBasicData.invoiceStartOn = invoiceStartDate
  invoiceBasicData.invoiceEndOn = invoiceEndDate
  const invoiceMonths = []
  if (invoiceFrequency > 1) {
    for (let i = 0; i < invoiceFrequency; i++) {
      const invoiceMonthDate = (
        await appHelper.getActualDate(
          partnerSetting,
          true,
          invoiceBasicData.invoiceMonth
        )
      )
        .add(i, 'months')
        .set('date', 2)
        .toDate()
      invoiceMonths.push(invoiceMonthDate)
    }
  } else {
    invoiceMonths.push(invoiceBasicData.invoiceMonth)
  }
  invoiceBasicData.invoiceMonths = invoiceMonths
  const isEnabledRecurringDueDate =
    contract.rentalMeta?.isEnabledRecurringDueDate || false
  const contractDueDays = contract?.rentalMeta?.dueDate || 1

  if (!ignoreRecurringDueDate && !isFirstInvoice && isEnabledRecurringDueDate) {
    let recurringDueDate = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
    )
      .set('date', contractDueDays)
      .subtract(1, 'month')
      .toDate()
    const isDueDateOlderThanToday = (
      await appHelper.getActualDate(partnerSetting, true, recurringDueDate)
    ).isBefore(await appHelper.getActualDate(partnerSetting, true, new Date()))

    if (isDueDateOlderThanToday)
      recurringDueDate = await appHelper.getActualDate(
        partnerSetting,
        false,
        contract.rentalMeta.firstInvoiceDueDate
      )
    if (recurringDueDate) invoiceBasicData.dueDate = recurringDueDate
  }
  const booleanParams = {
    isFirstInvoice,
    isLandlordInvoice: false
  }
  invoiceBasicData.invoiceAccountNumber = await getInvoiceAccountNumber(
    contract,
    partnerSetting,
    booleanParams
  )
  // Set rent bill duration date in invoice collection
  const rentBillData = {
    invoiceFrequency,
    invoiceStartDate,
    invoiceEndDate,
    monthlyRentAmount: contract.rentalMeta.monthlyRentAmount,
    partnerSetting,
    isNotFullMonth,
    returnPreview,
    isFirstInvoice
  }
  const rentBillInfo = await getMonthlyRentBillInfo(rentBillData)
  if (!size(rentBillInfo)) {
    return false
  }
  return {
    invoiceData: invoiceBasicData,
    options: rentBillInfo
  }
}

export const getInvoiceDurationInfo = async (invoiceDurationData, session) => {
  const {
    contract,
    partnerSetting,
    invoiceMonth,
    ignoreExistingInvoices,
    invoiceFrequency,
    manualInvoiceCreateOption
  } = invoiceDurationData
  const monthsStartAndEndOn = await getStartAndEndMonths({
    invoiceFrequency,
    invoiceMonth,
    manualInvoiceCreateOption,
    partnerSetting
  })
  let { startOfMonth, endOfMonth } = monthsStartAndEndOn
  if (invoiceFrequency > 1) {
    const isInvoiceDurationRight =
      await checkInvoiceDurationForHigherInvoiceFrequency({
        contractInfo: contract,
        invoiceStartOn: startOfMonth,
        invoiceEndOn: endOfMonth,
        partnerSettings: partnerSetting
      })
    if (!isInvoiceDurationRight) {
      console.log(
        '===== Invoice duration is wrong for invoice frequency:',
        invoiceFrequency,
        ', contractId:',
        contract?._id,
        ', startOfMonth & endOfMonth:',
        startOfMonth,
        '&',
        endOfMonth,
        '====='
      )
      return false
    }
  }
  const invoiceCalculation = contract.rentalMeta?.invoiceCalculation || ''
  let data = {
    contract,
    startOfMonth,
    endOfMonth
  }
  if (!ignoreExistingInvoices) {
    const isThisMonthInvoiceAvailable = await isThisMonthInvoiceExist(
      data,
      session
    )
    if (isThisMonthInvoiceAvailable) {
      const invoicedOn = contract.rentalMeta.invoicedAsOn
      let isInvoiceAlreadyCreated = true
      const lastInvoiceEndDate = await appHelper.getActualDate(
        partnerSetting,
        false,
        invoicedOn
      )
      const nextMonthDate = (
        await appHelper.getActualDate(partnerSetting, true, invoicedOn)
      ).add(invoiceFrequency - 1, 'months')
      const nextMonthStartDate = (
        await appHelper.getActualDate(partnerSetting, true, nextMonthDate)
      )
        .startOf('month')
        .toDate()
      const nextMonthEndDate = (
        await appHelper.getActualDate(partnerSetting, true, nextMonthDate)
      )
        .add(invoiceFrequency - 1, 'months')
        .endOf('month')
        .toDate()
      startOfMonth = (
        await appHelper.getActualDate(partnerSetting, true, invoicedOn)
      )
        .add(1, 'days')
        .toDate()
      const contractEndsDate = contract.rentalMeta.contractEndDate
      if (
        (startOfMonth >= nextMonthStartDate &&
          startOfMonth <= nextMonthEndDate) ||
        startOfMonth > contractEndsDate
      ) {
        return false
      } // If lease will be terminate in this month but invoice not create for some days Then create new invoice for few days before lease terminate
      if (
        (invoiceCalculation === 'prorated_first_month' && contractEndsDate) ||
        (invoiceCalculation === 'prorated_second_month' &&
          contractEndsDate > lastInvoiceEndDate &&
          contractEndsDate <= endOfMonth)
      ) {
        if (endOfMonth > contractEndsDate) {
          endOfMonth = contractEndsDate
        }
        data = {
          contract,
          startOfMonth,
          endOfMonth
        }
        const isThisMonthInvoiceExits = await isThisMonthInvoiceExist(
          data,
          session
        )
        if (startOfMonth <= contractEndsDate && !isThisMonthInvoiceExits) {
          isInvoiceAlreadyCreated = false
        }
      }
      if (isInvoiceAlreadyCreated) {
        return false
      }
    }
  }
  return { startOfMonth, endOfMonth }
}

export const invoiceCountOfAContract = async (contractId, session) => {
  const query = {
    contractId,
    isCorrectionInvoice: { $ne: true },
    leaseCancelled: { $ne: true },
    invoiceType: 'invoice'
  }
  const invoiceCount = await InvoiceCollection.find(query)
    .session(session)
    .countDocuments()
  return invoiceCount
}

export const getInvoiceEndsDate = async (partnerSetting, invoiceStartDate) => {
  //Example: Monthly invoice start date is to be 30 and we are in february and there no 30 days,
  //so we find out how many days this month have = 28 days
  //Since 30 is >= then 28, we then use 28 as invoice end date for feb, but use 30 on ALL other months!
  //Since 30 is >= then 28 is satisfied then invoice month will be February
  let endOfMonth = null
  let isNotFullMonths = false
  let invoiceMonthsDate = null
  const invoiceStartsDay = (
    await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
  ).format('D')
  let invoiceEndDay = (
    await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
  )
    .endOf('month')
    .format('D')
  if (invoiceStartsDay !== 1) {
    invoiceEndDay = invoiceStartsDay - 1
    const nextMonthEndDay = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
    )
      .add(1, 'months')
      .endOf('month')
      .format('D')
    if (invoiceEndDay >= nextMonthEndDay) {
      invoiceEndDay = nextMonthEndDay
      isNotFullMonths = true
      invoiceMonthsDate = (
        await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
      )
        .add(1, 'months')
        .set('date', 2)
        .toDate()
    }
    endOfMonth = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
    )
      .add(1, 'months')
      .set('date', invoiceEndDay)
      .toDate()
  } else {
    endOfMonth = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
    )
      .set('date', invoiceEndDay)
      .toDate()
  }
  const invoiceEndsDate = endOfMonth
  return {
    invoiceEndsDate,
    isNotFullMonths,
    invoiceMonthsDate
  }
}

export const getInvoiceStartDateAndEndDateBasedOnRentalMeta = async (
  partnerSetting,
  invoicedAsOn
) => {
  /* Example: If 1st invoice end date is 10th January 2018 then 2nd invoice start date is 11th January 2018
        So, second invoice start date = (1st invoice end date + 1)
    */
  const startOfMonth = (
    await appHelper.getActualDate(partnerSetting, true, invoicedAsOn)
  )
    .add(1, 'days')
    .toDate()
  const endOfMonth = (
    await appHelper.getActualDate(partnerSetting, true, startOfMonth)
  )
    .endOf('month')
    .toDate()
  return {
    startOfMonth,
    endOfMonth
  }
}
/* Note(getInvoiceCalculationInfo):
     Example: Monthly invoice start date is to be 30 and we are in february and there no 30 days,
     So we find out how many days this month have = 28 days
     Since 30 is >= then 28, we then use 28 as invoice end date for feb, but use 30 on ALL other months!
     Since 30 is >= then 28 is satisfied then invoice month will be February
 */
export const getInvoiceCalculationInfo = async (invoiceCalculationData) => {
  const {
    contract,
    endOfMonth,
    invoiceCountFromBeginning,
    invoiceFrequency,
    partnerSetting,
    startOfMonth
  } = invoiceCalculationData
  const contractStartsDate = contract.rentalMeta.contractStartDate
  const contractEndsDate = contract.rentalMeta.contractEndDate
  let invoiceStartDate = clone(startOfMonth)
  let invoiceEndDate = clone(endOfMonth)
  let isNotFullMonth = false
  let invoiceMonthDate = null
  let invoiceCalculation = 'prorated_first_month'
  if (contract.rentalMeta?.invoiceCalculation) {
    invoiceCalculation = contract.rentalMeta.invoiceCalculation
  } else if (partnerSetting.invoiceCalculation) {
    invoiceCalculation = partnerSetting.invoiceCalculation
  }
  if (
    invoiceCalculation === 'prorated_second_month' &&
    invoiceFrequency === 1
  ) {
    if (invoiceCountFromBeginning === 0) {
      // We can't create invoice before contract start date
      if (startOfMonth < contractStartsDate) {
        invoiceStartDate = contractStartsDate
      }
      const getInvoiceEndDate = await getInvoiceEndsDate(
        partnerSetting,
        invoiceStartDate
      )
      invoiceEndDate = getInvoiceEndDate.invoiceEndsDate
      isNotFullMonth = getInvoiceEndDate.isNotFullMonths
      invoiceMonthDate = getInvoiceEndDate.invoiceMonthsDate
    } else if (invoiceCountFromBeginning === 1) {
      const invoiceStartAndEndDate =
        await getInvoiceStartDateAndEndDateBasedOnRentalMeta(
          partnerSetting,
          contract.rentalMeta.invoicedAsOn
        )
      invoiceStartDate = invoiceStartAndEndDate.startOfMonth
      invoiceEndDate = invoiceStartAndEndDate.endOfMonth
    }
  } // We can't create invoice before contract start date
  if (startOfMonth < contractStartsDate) {
    invoiceStartDate = contractStartsDate
  }
  if (invoiceFrequency > 1) {
    invoiceEndDate = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
    )
      .add(invoiceFrequency - 1, 'months')
      .endOf('month')
      .toDate()
  }
  // We can't create invoice after contract end date
  if (contractEndsDate && invoiceEndDate > contractEndsDate) {
    invoiceEndDate = contractEndsDate
  }
  return {
    invoiceStartDate,
    invoiceEndDate,
    isNotFullMonth,
    invoiceMonthDate
  }
}

export const getMonthlyRentBillInfo = async (rentBillData) => {
  let totalMonthlyRent = 0
  let totalNoOfDaysInMonth = 0
  let totalInvoiceDays = 0
  const {
    invoiceFrequency,
    invoiceStartDate,
    invoiceEndDate,
    monthlyRentAmount,
    partnerSetting,
    isNotFullMonth,
    returnPreview,
    isFirstInvoice
  } = rentBillData
  const options = {
    returnPreview,
    isFirstInvoice
  }
  if (
    invoiceStartDate &&
    invoiceEndDate &&
    !(invoiceEndDate < invoiceStartDate) &&
    monthlyRentAmount &&
    partnerSetting
  ) {
    const startingDays = await appHelper.getActualDate(
      partnerSetting,
      true,
      invoiceStartDate
    )
    let noOfDaysInMonth = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartDate)
    ).daysInMonth()
    const totalDaysOfInvoice =
      (
        await appHelper.getActualDate(partnerSetting, true, invoiceEndDate)
      ).diff(startingDays, 'days') + 1
    if (isNotFullMonth) {
      noOfDaysInMonth = totalDaysOfInvoice
    }
    const perDayRent = monthlyRentAmount / noOfDaysInMonth
    let totalRent = perDayRent * totalDaysOfInvoice
    if (invoiceFrequency > 1) {
      totalRent = await getTotalAmountForDuration({
        endMonthDate: invoiceEndDate,
        monthlyRentAmount,
        partnerSetting,
        startMonthDate: invoiceStartDate
      })
    }
    totalMonthlyRent = await appHelper.convertTo2Decimal(
      totalRent,
      partnerSetting
    )
    totalNoOfDaysInMonth = noOfDaysInMonth
    totalInvoiceDays = totalDaysOfInvoice
  }
  if (totalInvoiceDays > 0 && totalMonthlyRent) {
    options.noOfDaysInMonth = totalNoOfDaysInMonth ? totalNoOfDaysInMonth : 0
    options.monthlyRent = totalMonthlyRent
    options.totalDaysOfInvoice = totalInvoiceDays
  } else {
    return false
  }
  return options
}

export const getTaxPercentageBasedOnCreditAccountId = async (
  creditAccountId
) => {
  if (creditAccountId) {
    const ledgerAccountsInfo =
      (await ledgerAccountHelper.getLedgerAccById(creditAccountId)) || {}
    if (ledgerAccountsInfo.taxCodeId) {
      const taxCodeAccountInfo =
        (await taxCodeHelper.getTaxCodeById(ledgerAccountsInfo.taxCodeId)) || {}
      return taxCodeAccountInfo.taxPercentage || 0
    }
  }
  return 0
}

export const getTaxPercentageBasedOnAccountingType = async (
  accountingType,
  partnerId
) => {
  let newTaxPercentage = 0
  if (!accountingType || !partnerId) {
    return 0
  }
  const query = {
    type: accountingType,
    partnerId
  }
  const accounting = await accountingHelper.getAccounting(query)
  if (accounting && accounting.creditAccountId) {
    const taxPercentage = await getTaxPercentageBasedOnCreditAccountId(
      accounting.creditAccountId
    )
    newTaxPercentage = taxPercentage
  }
  return newTaxPercentage
}

export const getRentTaxPercentage = async (contract, partnerId) => {
  const accountingType =
    contract && contract.rentalMeta && contract.rentalMeta.isVatEnable
      ? 'rent_with_vat'
      : 'rent'
  const rentTaxPercentage = await getTaxPercentageBasedOnAccountingType(
    accountingType,
    partnerId
  )
  return rentTaxPercentage
}

export const prepareAddonsInitialData = async (params = {}) => {
  const { contract, options, partnerSetting, invoiceBasicData } = params
  const contractAddons = contract.addons
  let isThisFirstInvoice = options.isFirstInvoice
  let addonsMetaData = []
  let addonTotalAmount = 0
  let invoiceCommissionableTotal = 0
  if (contract && size(contractAddons)) {
    // Set contract addon info in invoice content
    const updatedInfo = await getAddonsMetaData({
      contractAddons,
      invoiceFrequency: invoiceBasicData.invoiceFrequency || 1,
      invoiceStartOn: invoiceBasicData.invoiceStartOn,
      invoiceEndOn: invoiceBasicData.invoiceEndOn,
      isFirstInvoice: isThisFirstInvoice,
      options,
      partnerSetting
    })
    addonsMetaData = clone(updatedInfo.addonsMetas)
    addonTotalAmount = updatedInfo.addonsTotalAmount
    isThisFirstInvoice = updatedInfo.isFirstInvoice
    invoiceCommissionableTotal = updatedInfo.invoiceCommissionTotal
  } // We have to filter our non-rent type corrections from here ! We will create separate invoices for them
  const invoiceAddonsMeta = await getCorrectionInfoInAddonsMeta(
    addonsMetaData,
    invoiceBasicData
  )
  const addonTotalAmountInDecimal = await getAddonTotalAmountInDecimal(
    addonTotalAmount,
    invoiceAddonsMeta
  )
  addonTotalAmount = addonTotalAmountInDecimal
  // Add correction commissionable total with invoice commission total
  const totalInvoiceCommission =
    invoiceCommissionableTotal +
    (invoiceAddonsMeta.correctionCommissionTotal || 0)
  invoiceCommissionableTotal = totalInvoiceCommission
  return {
    addonTotalAmount,
    isThisFirstInvoice,
    invoiceAddonsMeta,
    invoiceCommissionableTotal
  }
}

export const getInvoiceContent = async (
  contract,
  options,
  invoiceBasicData
) => {
  const monthlyRentAmount =
    options && options.monthlyRent ? options.monthlyRent : 0
  const partnerId = invoiceBasicData ? invoiceBasicData.partnerId : ''
  const invoiceContent = []
  const rentTaxPercentages = await getRentTaxPercentage(contract, partnerId)
  const content = {
    type: 'monthly_rent',
    qty: 1,
    price: monthlyRentAmount,
    total: monthlyRentAmount,
    taxPercentage: rentTaxPercentages
  }
  invoiceContent.push(content)
  return invoiceContent
}

export const getAddonsMetaData = async (params = {}) => {
  const {
    contractAddons,
    invoiceFrequency,
    invoiceStartOn,
    invoiceEndOn,
    isFirstInvoice,
    options,
    partnerSetting
  } = params
  // Set contract addon info in invoice content
  const addonsMetas = []
  let addonsTotalAmount = 0
  let invoiceCommissionTotal = 0
  const nofDaysInMonth = options.noOfDaysInMonth
  const totalDaysInInvoice = options.totalDaysOfInvoice
  for (const contractAddonInfo of contractAddons) {
    if (contractAddonInfo && contractAddonInfo.type === 'lease') {
      const addonInfo =
        (await addonHelper.getAddonById(contractAddonInfo.addonId)) || {}
      const addonItemName = addonInfo && addonInfo.name ? addonInfo.name : ''
      const addonItemQty = 1
      let addonItemPrice = contractAddonInfo.price || 0
      const ledgerAccountsInfo =
        (await ledgerAccountHelper.getLedgerAccById(
          addonInfo.creditAccountId
        )) || {}
      const taxCodeInfo = ledgerAccountsInfo.taxCodeId
        ? await taxCodeHelper.getTaxCodeById(ledgerAccountsInfo.taxCodeId)
        : false
      if (
        (!isFirstInvoice ||
          (isFirstInvoice && contractAddonInfo.isRecurring === true)) &&
        totalDaysInInvoice &&
        nofDaysInMonth &&
        invoiceFrequency === 1
      ) {
        // If invoice is not first invoice then find out addon price by rent active days
        addonItemPrice = addonItemPrice
          ? (addonItemPrice / nofDaysInMonth) * totalDaysInInvoice
          : 0
      } else if (invoiceFrequency > 1 && contractAddonInfo.isRecurring) {
        addonItemPrice = await getTotalAmountForDuration({
          endMonthDate: invoiceEndOn,
          monthlyRentAmount: addonItemPrice,
          partnerSetting,
          startMonthDate: invoiceStartOn
        })
      }
      const addonItemTotal = await appHelper.convertTo2Decimal(addonItemPrice)
      const addAddOns =
        contractAddonInfo.isRecurring === true || isFirstInvoice === true
          ? true
          : false
      if (addAddOns) {
        const priceInDecimal = await appHelper.convertTo2Decimal(addonItemPrice)
        const totalInDecimal = await appHelper.convertTo2Decimal(addonItemTotal)
        const metaData = {
          type: 'addon',
          description: addonItemName,
          qty: addonItemQty,
          taxPercentage:
            taxCodeInfo && taxCodeInfo.taxPercentage
              ? taxCodeInfo.taxPercentage
              : 0,
          price: priceInDecimal,
          total: totalInDecimal,
          addonId: contractAddonInfo.addonId
        }
        addonsMetas.push(metaData)
        if (addonInfo.enableCommission) {
          invoiceCommissionTotal += addonItemTotal
        }
        addonsTotalAmount += addonItemTotal
      }
    }
  }
  return {
    addonsMetas,
    addonsTotalAmount,
    invoiceCommissionTotal,
    isFirstInvoice
  }
}

export const getCorrectionAddons = async (correctionAddon, correction) => {
  let correctionCommissionInTotal = 0
  const correctionAddonInfo = pick(correctionAddon, [
    'addonId',
    'description',
    'taxPercentage'
  ])
  const priceInDecimal = await appHelper.convertTo2Decimal(
    correctionAddon.price || 0
  )
  const totalInDecimal = await appHelper.convertTo2Decimal(
    correctionAddon.total || 0
  )
  correctionAddonInfo.type = 'addon'
  correctionAddonInfo.price = priceInDecimal
  correctionAddonInfo.total = totalInDecimal
  if (!correctionAddonInfo.description) {
    const addon = await addonHelper.getAddonById(correctionAddon.addonId)
    correctionAddonInfo.description = addon && addon.name ? addon.name : ''
  }
  correctionAddonInfo.qty = 1
  correctionAddonInfo.correctionId = correction._id
  if (correctionAddon.hasCommission) {
    correctionCommissionInTotal += correctionAddonInfo.total || 0
  }
  return {
    correctionCommissionInTotal,
    addonsMeta: correctionAddonInfo
  }
}

export const getCorrectionInfo = async (invoiceBasicData) => {
  const query = {
    partnerId: invoiceBasicData.partnerId,
    propertyId: invoiceBasicData.propertyId,
    contractId: invoiceBasicData.contractId,
    invoiceId: { $exists: false },
    addTo: 'rent_invoice',
    correctionStatus: 'active',
    // Filtering out non-rent corrections as this corrections will be added with monthly rent invoice
    isNonRent: { $ne: true }
  }
  const corrections = await correctionHelper.getCorrections(query)
  return corrections
}

export const getCorrectionInfoInAddonsMeta = async (
  addonsMetaData,
  invoiceBasicData
) => {
  let correctionsTotal = 0
  const correctionsIds = []
  let correctionCommissionTotal = 0
  if (!invoiceBasicData.ignoreCorrections) {
    // Ignore during manual invoice preview.
    // Find all expenses which are not linked with any invoice
    const correctionInfo = await getCorrectionInfo(invoiceBasicData)
    for (const correction of correctionInfo) {
      correctionsTotal += correction.amount || 0
      correctionsIds.push(correction._id)
      if (size(correction.addons)) {
        for (const correctionAddon of correction.addons) {
          const correctionAddons = await getCorrectionAddons(
            correctionAddon,
            correction
          )
          const { addonsMeta, correctionCommissionInTotal } = correctionAddons
          correctionCommissionTotal += correctionCommissionInTotal
          addonsMetaData.push(addonsMeta)
        }
      }
    }
  }
  return {
    addonsMetaData,
    correctionsIds,
    correctionsTotal,
    correctionCommissionTotal
  }
}

export const getAddonTotalAmountInDecimal = async (
  addonTotalAmount,
  invoiceAddonsMeta
) => {
  const totalAddonAmount =
    addonTotalAmount + (invoiceAddonsMeta.correctionsTotal || 0)
  const addonTotalAmountInDecimal = await appHelper.convertTo2Decimal(
    totalAddonAmount
  )
  return addonTotalAmountInDecimal
}

export const isAddFutureMonthlyRent = async (contract, partnerSetting) => {
  const partnerStopCPIRegulation = partnerSetting.stopCPIRegulation
  let isStopCpiRegulation = false
  if (partnerSetting && partnerStopCPIRegulation) {
    isStopCpiRegulation = true
  }
  const monthlyRentAmount =
    contract.rentalMeta && contract.rentalMeta.monthlyRentAmount
      ? contract.rentalMeta.monthlyRentAmount
      : 0
  const futureMonthlyRentAmount = await contract.getCPINextMonthlyRentAmount()
  console.log('===> futureMonthlyRentAmount', futureMonthlyRentAmount)
  console.log('===> monthlyRentAmount', monthlyRentAmount)
  if (
    isStopCpiRegulation &&
    monthlyRentAmount &&
    futureMonthlyRentAmount < monthlyRentAmount
  ) {
    return false
  }
  return true
}

export const getDataForUpdatingContractRentAmount = async (
  contract,
  partnerSetting,
  today
) => {
  const partnerId = partnerSetting.partnerId || ''
  const isFutureMonthlyRentAvailable = await isAddFutureMonthlyRent(
    contract,
    partnerSetting
  )
  if (
    contract &&
    contract.rentalMeta &&
    contract.rentalMeta.futureRentAmount &&
    contract.rentalMeta.lastCPINotificationSentOn &&
    partnerId &&
    today &&
    isFutureMonthlyRentAvailable
  ) {
    const nextCpiDate = (
      await appHelper.getActualDate(partnerSetting, true, clone(today))
    )
      .add(12, 'months')
      .endOf('day')
      .toDate()
    const lastCpiDate = (
      await appHelper.getActualDate(partnerSetting, true, clone(today))
    )
      .endOf('day')
      .toDate()
    const rentalMonthlyRentAmount =
      contract && contract.rentalMeta && contract.rentalMeta.futureRentAmount
        ? contract.rentalMeta.futureRentAmount
        : contract.rentalMeta.monthlyRentAmount
    const updateData = {
      'rentalMeta.monthlyRentAmount': rentalMonthlyRentAmount,
      'rentalMeta.lastCpiDate': lastCpiDate,
      'rentalMeta.nextCpiDate': nextCpiDate
    }
    const resetData = {
      'rentalMeta.futureRentAmount': 1,
      'rentalMeta.lastCPINotificationSentOn': 1
    }
    return {
      _id: contract._id,
      partnerId,
      updateData,
      resetData
    }
  }
  return false
}

export const getInvoiceDueDate = async (
  partnerSetting,
  invoiceDueDate,
  testTodayDate
) => {
  const todayDate = await appHelper.getActualDate(
    partnerSetting,
    false,
    testTodayDate
  )
  return invoiceDueDate < todayDate ? todayDate : invoiceDueDate
}

export const prepareDataForCurrentOrNextMonthInvoice = async (
  initials,
  isNextMonthInvoice
) => {
  const { contract, partnerSetting, actualDate, invoiceBasicData } = initials
  // Try to create invoice for next month
  let nextMonthDate = 0
  const contractDueDays = contract.rentalMeta.dueDate
    ? contract.rentalMeta.dueDate
    : 1
  let currentInvoiceDueDate
  if (isNextMonthInvoice) {
    const invoicedAsOn = contract.rentalMeta?.invoicedAsOn
      ? (
          await appHelper.getActualDate(
            partnerSetting,
            true,
            contract.rentalMeta.invoicedAsOn
          )
        )
          .add(1, 'day')
          .toDate()
      : ''
    const haveToCheckWithInvoicedAsOn =
      invoicedAsOn && invoiceBasicData.invoiceFrequency > 1
    nextMonthDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        haveToCheckWithInvoicedAsOn ? invoicedAsOn : actualDate
      )
    )
      .add(haveToCheckWithInvoicedAsOn ? 0 : 1, 'month')
      .toDate()
    currentInvoiceDueDate = (
      await appHelper.getActualDate(partnerSetting, true, nextMonthDate)
    )
      .set('date', contractDueDays)
      .toDate()
  } else {
    nextMonthDate = actualDate
    currentInvoiceDueDate = (
      await appHelper.getActualDate(partnerSetting, true, nextMonthDate)
    )
      .set('date', contractDueDays)
      .toDate()
  }
  const invoiceCreationDate = await appHelper.subtractDays(
    currentInvoiceDueDate,
    partnerSetting.invoiceDueDays,
    partnerSetting
  )
  const isFirstInvoice = false
  // We'll only create invoice if today is the firstInvoiceCreationDate or past
  if (actualDate >= invoiceCreationDate) {
    invoiceBasicData.dueDate = await getInvoiceDueDate(
      partnerSetting,
      clone(currentInvoiceDueDate)
    )
    invoiceBasicData.invoiceMonth = currentInvoiceDueDate
    return {
      contract,
      partnerSetting,
      invoiceBasicData,
      isFirstInvoice
    }
  }
  return false
}

export const getLandlordInvoiceTotalFromCommissionsMeta = (
  invoiceCommissionMeta
) => {
  let landlordInvoiceTotal = 0
  if (size(invoiceCommissionMeta)) {
    each(invoiceCommissionMeta, (commissionMetaInfo) => {
      landlordInvoiceTotal += commissionMetaInfo.total || 0
    })
  }
  return landlordInvoiceTotal
}

export const getContractStartAndEndDate = async (params) => {
  const {
    contract,
    invoiceData,
    correctionData,
    partnerSetting,
    today,
    isLandlordInvoice
  } = params
  let contractStartsDate = contract.rentalMeta.contractStartDate
  let contractEndDate = contract.rentalMeta.contractEndDate
    ? contract.rentalMeta.contractEndDate
    : ''
  let dueDays = partnerSetting.invoiceDueDays || 0
  let landlordInvoiceTotal = 0
  let invoiceStartOn = (
    await appHelper.getActualDate(partnerSetting, true, today)
  )
    .startOf('month')
    .toDate()
  let invoiceEndOn = (
    await appHelper.getActualDate(partnerSetting, true, today)
  )
    .endOf('month')
    .toDate()
  const addonTotalAmount =
    correctionData && correctionData.correctionsTotal
      ? await appHelper.convertTo2Decimal(correctionData.correctionsTotal)
      : 0
  const invoiceCommissionableTotal =
    correctionData && correctionData.correctionCommissionableTotal
      ? correctionData.correctionCommissionableTotal
      : 0 // Add invoice commission in total
  const findLastInvoice = await getLastInvoiceOfAContract(contract._id)
  if (findLastInvoice) {
    // Found last invoice, now set invoice duration for correction invoice
    invoiceStartOn = await appHelper.getActualDate(
      partnerSetting,
      false,
      findLastInvoice.invoiceStartOn
    )
    invoiceEndOn = await appHelper.getActualDate(
      partnerSetting,
      false,
      findLastInvoice.invoiceEndOn
    )
  }
  contractStartsDate = await appHelper.getActualDate(
    partnerSetting,
    false,
    contractStartsDate
  )
  contractEndDate = contractEndDate
    ? await appHelper.getActualDate(partnerSetting, false, contractEndDate)
    : ''
  if (invoiceStartOn < contractStartsDate) {
    // We can't create invoice before contract start date
    invoiceStartOn = contractStartsDate
  } // We can't create invoice after contract end date
  if (contractEndDate && invoiceEndOn > contractEndDate) {
    invoiceEndOn = contractEndDate
  }
  if (invoiceStartOn > invoiceEndOn) {
    invoiceEndOn = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartOn)
    )
      .endOf('month')
      .toDate()
  }
  if (isLandlordInvoice) {
    dueDays = partnerSetting.landlordInvoiceDueDays || 0 // Calculating landlord invoice total for create landlord invoice from commission
    const invoiceCommissionMeta = clone(invoiceData.commissionsMeta)
    const landlordInvoiceTotalFromCommissionMeta =
      await getLandlordInvoiceTotalFromCommissionsMeta(invoiceCommissionMeta)
    landlordInvoiceTotal = landlordInvoiceTotalFromCommissionMeta // Getting landlordInvoiceTotalFromCommissionsMeta
  }
  return {
    invoiceStartOn,
    invoiceEndOn,
    addonTotalAmount,
    invoiceCommissionableTotal,
    dueDays,
    landlordInvoiceTotal
  }
}

export const getInvoiceFeeAndTex = (partnerSetting, isLandlordInvoice) => {
  const feeMetaData = {}
  let invoiceFeeTotal = 0
  let feeTaxTotal = 0
  let invoiceFee =
    partnerSetting.invoiceFee &&
    partnerSetting.invoiceFee.enabled &&
    partnerSetting.invoiceFee.amount
      ? partnerSetting.invoiceFee.amount
      : 0
  let invoiceFeeTax =
    invoiceFee && partnerSetting.invoiceFee.tax
      ? (partnerSetting.invoiceFee.tax / 100) * invoiceFee
      : 0
  if (isLandlordInvoice) {
    invoiceFee =
      partnerSetting.landlordInvoiceFee &&
      partnerSetting.landlordInvoiceFee.enabled &&
      partnerSetting.landlordInvoiceFee.amount
        ? partnerSetting.landlordInvoiceFee.amount
        : 0
    invoiceFeeTax =
      invoiceFee && partnerSetting.landlordInvoiceFee.tax
        ? (partnerSetting.landlordInvoiceFee.tax / 100) * invoiceFee
        : 0
  }
  if (invoiceFee) {
    feeMetaData.amount = invoiceFee
    invoiceFeeTotal += invoiceFee
  }
  if (invoiceFeeTax) {
    feeMetaData.tax = invoiceFeeTax
    invoiceFeeTotal += invoiceFeeTax
    feeTaxTotal += invoiceFeeTax
  }
  return {
    feeMetaData,
    invoiceFeeTotal,
    feeTaxTotal
  }
}

export const getInvoiceFeesMetaData = async (params = {}, session) => {
  const { contract, isLandlordInvoice, isThisFirstInvoice, partnerSetting } =
    params
  const invoiceFeesMeta = []
  let feeTotal = 0
  const invoiceFeeAndTax = getInvoiceFeeAndTex(
    partnerSetting,
    isLandlordInvoice
  )
  const { feeMetaData, invoiceFeeTotal } = invoiceFeeAndTax
  let { feeTaxTotal } = invoiceFeeAndTax
  if (size(feeMetaData)) {
    feeMetaData.type = 'invoice'
    feeMetaData.qty = 1
    feeMetaData.original = true
    feeMetaData.total = invoiceFeeTotal
    feeTotal += invoiceFeeTotal
    invoiceFeesMeta.push(feeMetaData)
  }
  if (!isThisFirstInvoice && !isLandlordInvoice && contract._id) {
    const invoices = await getInvoices(
      {
        contractId: contract._id,
        status: 'paid',
        invoiceType: 'invoice',
        feesMeta: { $elemMatch: { original: false, isPaid: false } }
      },
      session,
      {
        sort: { createdAt: -1 }
      }
    )
    each(invoices, (invoiceInfo) => {
      if (
        invoiceInfo &&
        invoiceInfo.invoiceTotal &&
        invoiceInfo.totalPaid < invoiceInfo.invoiceTotal
      ) {
        each(invoiceInfo.feesMeta || [], (feesInfo) => {
          if (!feesInfo.original && !feesInfo.isPaid) {
            const lastMonthFeeInfo = clone(feesInfo)
            const unpaid = 'unpaid_'
            lastMonthFeeInfo.type = unpaid + lastMonthFeeInfo.type
            lastMonthFeeInfo.original = true
            lastMonthFeeInfo.invoiceId = invoiceInfo._id // Set relation between previous invoice and next invoice for fees
            feeTotal += lastMonthFeeInfo.total || 0 // Calculate fee total
            feeTaxTotal += lastMonthFeeInfo.tax || 0 // Calculate fee tax total
            delete lastMonthFeeInfo.isPaid // Remove isPaid field from last month fees info object
            invoiceFeesMeta.push(lastMonthFeeInfo)
          }
        })
      }
    })
  }
  return {
    invoiceFeesMeta,
    feeTotal,
    feeTaxTotal
  }
}

export const prepareInvoiceData = async (creatingNewInvoiceData) => {
  const {
    contract,
    invoiceData,
    correctionData,
    partnerSetting,
    today,
    isDemo,
    returnPreview,
    enabledNotification,
    data,
    isLandlordInvoice
  } = creatingNewInvoiceData
  const isFirstInvoice = false
  const {
    invoiceStartOn,
    invoiceEndOn,
    addonTotalAmount,
    invoiceCommissionableTotal,
    landlordInvoiceTotal,
    dueDays
  } = await getContractStartAndEndDate(creatingNewInvoiceData)
  // isCorrectionInvoice should only used for correction invoice not for commission. This will fix in future when we re-structure our database
  invoiceData.isCorrectionInvoice = true
  const dateInFormat = await appHelper.getDateFormat(partnerSetting) // Getting Format Date
  if (data && data.dueDate) {
    invoiceData.dueDate = moment(new Date(data.dueDate), dateInFormat).toDate()
  } else {
    invoiceData.dueDate = moment(today, dateInFormat)
      .add(dueDays || 0, 'days')
      .toDate()
  }
  invoiceData.invoiceStartOn = invoiceStartOn
  invoiceData.invoiceEndOn = invoiceEndOn
  const booleanParams = {
    isFirstInvoice,
    isLandlordInvoice
  }
  const invoiceAccountNummber = await getInvoiceAccountNumber(
    contract,
    partnerSetting,
    booleanParams
  )
  invoiceData.invoiceAccountNumber = invoiceAccountNummber
  if (correctionData) {
    invoiceData.addonsMeta = correctionData.addonsMetaData
    invoiceData.correctionsIds = correctionData.correctionsIds
  }
  const feesParams = {
    contract,
    partnerSetting,
    isThisFirstInvoice: isFirstInvoice,
    isLandlordInvoice
  }
  const invoiceFeesMetaData = await getInvoiceFeesMetaData(feesParams)
  if (invoiceFeesMetaData && size(invoiceFeesMetaData.invoiceFeesMeta)) {
    invoiceData.feesMeta = invoiceFeesMetaData.invoiceFeesMeta
  }
  if (isDemo) {
    invoiceData.isDemo = true
  }
  invoiceData.isFirstInvoice = isFirstInvoice
  if (correctionData && correctionData.isNonRent) {
    // If the correction is non-rent, then we have to make the invoice non-rent also
    invoiceData.isNonRentInvoice = true
  }
  return {
    invoiceData,
    partnerSetting,
    monthlyRentAmount: 0,
    addonTotalAmount,
    feeTotal: invoiceFeesMetaData.feeTotal,
    invoiceCommissionableTotal,
    feeTaxTotal: invoiceFeesMetaData.feeTaxTotal,
    options: { returnPreview },
    enabledNotification,
    landlordInvoiceTotal
  }
}

export const getCorrectionDataByCorrectionInfo = async (correctionInfo) => {
  let correctionsTotal = 0
  let correctionsIds = []
  let correctionCommissionableTotal = 0
  const addonsMetaData = []
  let isNonRent = false
  if (correctionInfo) {
    const correctionAddons = correctionInfo.addons || []
    correctionsTotal += correctionInfo.amount || 0
    correctionsIds = [correctionInfo._id]
    if (size(correctionAddons)) {
      for (const correctionAddon of correctionAddons) {
        const correctionAddonInfo = pick(correctionAddon, [
          'addonId',
          'description',
          'taxPercentage'
        ])
        const priceInDecimal =
          (await appHelper.convertTo2Decimal(correctionAddon.price)) * 1
        const totalInDecimal =
          (await appHelper.convertTo2Decimal(correctionAddon.total)) * 1
        correctionAddonInfo.type = 'addon'
        correctionAddonInfo.price = priceInDecimal
        correctionAddonInfo.total = totalInDecimal
        if (!correctionAddonInfo.description) {
          const addon = await addonHelper.getAddonById(correctionAddon.addonId)
          correctionAddonInfo.description =
            addon && addon.name ? addon.name : ''
        }
        correctionAddonInfo.qty = 1
        correctionAddonInfo.correctionId = correctionInfo._id
        if (correctionAddon.hasCommission) {
          correctionCommissionableTotal += correctionAddonInfo.total || 0
        }
        addonsMetaData.push(correctionAddonInfo)
      }
    }
    if (correctionInfo.isNonRent) {
      isNonRent = true
    }
  }
  return {
    addonsMetaData,
    correctionsIds,
    correctionsTotal,
    correctionCommissionableTotal,
    isNonRent
  }
}

export const getCorrectionData = async (correctionId, invoiceData) => {
  const isNotALandlordInvoice = isNotLandlord(invoiceData)
  const query = {
    _id: correctionId,
    partnerId: invoiceData.partnerId,
    propertyId: invoiceData.propertyId,
    contractId: invoiceData.contractId,
    invoiceId: { $exists: false },
    addTo: isNotALandlordInvoice ? 'rent_invoice' : 'payout'
  }
  const correctionInfo = await correctionHelper.getCorrection(query)
  const correctionDataByCorrectionInfo =
    await getCorrectionDataByCorrectionInfo(clone(correctionInfo))
  return correctionDataByCorrectionInfo
}

export const getInvoiceDate = async (date, partnerSetting) => {
  const actualDate = (await appHelper.getActualDate(partnerSetting, true, date))
    .endOf('day')
    .toDate()
  return actualDate
}

export const getInitialDataForCorrectionInvoice = async (
  correctionInvoiceData
) => {
  const {
    contract,
    correctionId,
    isDemo,
    returnPreview,
    isPendingCorrectionInvoice
  } = correctionInvoiceData
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    contract.partnerId
  )
  const today = await getInvoiceDate(new Date(), partnerSetting)
  const invoiceData = await getBasicInvoiceDataForTenant(
    contract,
    today,
    isDemo,
    returnPreview
  )
  const correctionData = await getCorrectionData(correctionId, invoiceData)
  if (isPendingCorrectionInvoice) {
    invoiceData.isPendingCorrection = isPendingCorrectionInvoice
  }
  return {
    partnerSetting,
    today,
    invoiceData,
    correctionData
  }
}

export const isNotLandlord = (invoiceData) => {
  const result = !!(
    size(invoiceData) &&
    indexOf(
      ['landlord_invoice', 'landlord_credit_note'],
      invoiceData.invoiceType
    ) === -1
  )
  return result
}

export const getInvoices = async (query, session, options = {}) => {
  // Do not remove options
  const {
    limit = undefined,
    populate = [],
    sort = { invoiceSerialId: 1 }
  } = options
  const invoices = await InvoiceCollection.find(query)
    .session(session)
    .populate(populate)
    .sort(sort)
    .limit(limit)
  return invoices
}

export const getInvoicesWithSelect = async (query, session, select = {}) =>
  await InvoiceCollection.find(query).session(session).select(select)

export const getInvoice = async (query, session, populate = []) => {
  const invoiceData = await InvoiceCollection.findOne(query)
    .populate(populate)
    .session(session)
  return invoiceData
}

export const getAggregatedInvoices = async (pipelines = [], session) => {
  if (!size(pipelines))
    throw new CustomError(
      400,
      'Pipelines can not be empty to aggregate invoices'
    )

  const invoices = await InvoiceCollection.aggregate(pipelines).session(session)
  return invoices
}

export const getUpdatedFeesMetaData = (params) => {
  const { invoiceFeesMetaData, invoiceData } = params
  let { invoiceTotal, totalTax } = params
  let reminderMoveTo = {}
  let collectionNoticeMoveTo = {}
  let evictionNoticeMoveTo = {}
  let administrationEvictionNoticeMoveTo = {}
  const updateFeesMeta = filter(invoiceFeesMetaData, (metaData) => {
    if (
      metaData &&
      !metaData.isPaid &&
      (metaData.type === 'reminder' ||
        metaData.type === 'collection_notice' ||
        metaData.type === 'eviction_notice' ||
        metaData.type === 'administration_eviction_notice')
    ) {
      metaData.isPaid = true
      metaData.invoiceId = invoiceData._id
      const feeTax = (metaData.tax || 0) * -1
      invoiceTotal -= metaData.amount
      totalTax += feeTax
      const feeMoveTo = {
        qty: metaData.qty,
        amount: metaData.amount * -1,
        tax: feeTax,
        total: metaData.total * -1,
        original: false,
        isPaid: true,
        invoiceId: invoiceData._id
      }
      if (metaData.type === 'reminder') {
        reminderMoveTo = feeMoveTo
        reminderMoveTo.type = 'reminder_fee_move_to'
      } else if (metaData.type === 'collection_notice') {
        collectionNoticeMoveTo = feeMoveTo
        collectionNoticeMoveTo.type = 'collection_notice_fee_move_to'
      } else if (metaData.type === 'eviction_notice') {
        evictionNoticeMoveTo = feeMoveTo
        evictionNoticeMoveTo.type = 'eviction_notice_fee_move_to'
      } else if (metaData.type === 'administration_eviction_notice') {
        administrationEvictionNoticeMoveTo = feeMoveTo
        administrationEvictionNoticeMoveTo.type =
          'administration_eviction_notice_fee_move_to'
      }
    }
    return metaData
  })
  return {
    updateFeesMeta,
    reminderMoveTo,
    collectionNoticeMoveTo,
    evictionNoticeMoveTo,
    administrationEvictionNoticeMoveTo,
    newInvoiceTotal: invoiceTotal,
    newTotalTax: totalTax
  }
}

export const getFeesMetaData = async (invoiceInfo, invoiceData) => {
  const invoiceFeesMetaData =
    invoiceInfo && invoiceInfo.feesMeta ? invoiceInfo.feesMeta : []
  let invoiceTotal = invoiceInfo.invoiceTotal || 0
  let totalTax = invoiceInfo.totalTAX || 0
  const feesMetaParams = {
    invoiceFeesMetaData,
    invoiceData,
    invoiceTotal,
    totalTax
  }
  const {
    updateFeesMeta,
    reminderMoveTo,
    collectionNoticeMoveTo,
    evictionNoticeMoveTo,
    administrationEvictionNoticeMoveTo,
    newInvoiceTotal,
    newTotalTax
  } = getUpdatedFeesMetaData(feesMetaParams)
  invoiceTotal = newInvoiceTotal
  totalTax = newTotalTax
  if (size(reminderMoveTo)) {
    updateFeesMeta.push(reminderMoveTo)
  }
  if (size(collectionNoticeMoveTo)) {
    updateFeesMeta.push(collectionNoticeMoveTo)
  }
  if (size(evictionNoticeMoveTo)) {
    updateFeesMeta.push(evictionNoticeMoveTo)
  }
  if (size(administrationEvictionNoticeMoveTo)) {
    updateFeesMeta.push(administrationEvictionNoticeMoveTo)
  }
  const feesMetaData = {
    invoiceTotal: await appHelper.convertTo2Decimal(
      invoiceTotal,
      invoiceData.partnerId,
      'round'
    ),
    totalTAX: await appHelper.convertTo2Decimal(totalTax),
    feesMeta: updateFeesMeta
  }
  return feesMetaData
}

export const areAllMandatoryFieldsExist = (invoiceData) => {
  const { invoiceType } = invoiceData
  const requiredFields = [
    'partnerId',
    'contractId',
    'invoiceType',
    'status',
    'dueDate',
    'invoiceStartOn',
    'invoiceEndOn'
  ]
  if (invoiceType !== 'invoice') {
    requiredFields.push('rentTotal', 'invoiceTotal')
  }
  if (invoiceType === 'credit_note') {
    requiredFields.push('invoiceId')
  }
  if (invoiceType === 'landlord_invoice') {
    requiredFields.push('receiver')
  }
  if (invoiceType === 'invoice' || invoiceType === 'credit_note') {
    requiredFields.push('invoiceAccountNumber')
  }
  if (invoiceType === 'landlord_credit_note') {
    if (
      !invoiceData.invoiceId &&
      !invoiceData.landlordInvoiceId &&
      !invoiceData.forCorrection
    ) {
      requiredFields.push('invoiceId')
    }
  }
  const availableFields = Object.keys(invoiceData)
  const result = difference(requiredFields, availableFields)
  return result
}

export const isAnnualStatementCreated = async (invoiceData) => {
  const isCorrectionInvoice = invoiceData?.isCorrectionInvoice
  const isCreditNote = ['credit_note', 'landlord_credit_note'].includes(
    invoiceData?.invoiceType
  )

  // If app admin created correction/credit note invoice then inserting data by returning false
  if (isCreditNote || isCorrectionInvoice) {
    let isAdminUser = false

    if (isCreditNote && invoiceData?.createdBy !== 'SYSTEM') {
      isAdminUser = await appPermission.isAppAdmin(invoiceData?.createdBy)
    }

    if (!isAdminUser && isCorrectionInvoice) {
      const correctionsIds = invoiceData?.correctionsIds
      const correctionInfo = await correctionHelper.getCorrection({
        _id: {
          $in: correctionsIds
        }
      })
      const createdByUserId = correctionInfo?.createdBy

      isAdminUser = await appPermission.isAppAdmin(createdByUserId)
    }

    if (isAdminUser) return false
  }
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    invoiceData.partnerId
  )
  if (!partnerSetting) {
    return false
  }
  const startPeriod = Number(
    (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        invoiceData.invoiceStartOn
      )
    )
      .startOf('day')
      .format('YYYY') * 1
  )
  const endPeriod = Number(
    (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        invoiceData.invoiceEndOn
      )
    )
      .endOf('day')
      .format('YYYY') * 1
  )
  const period = startPeriod >= endPeriod ? startPeriod : endPeriod
  const query = {
    partnerId: invoiceData.partnerId,
    contractId: invoiceData.contractId,
    statementYear: { $gte: period }
  }
  const annualStatement = await annualStatementHelper.getAnnualStatement(query)
  return !!annualStatement
}

export const validateInvoiceDataBeforeCreation = async (
  invoiceDataForInsert,
  isLandlordCorrectionInvoice
) => {
  // Checking isAllMandatoryFieldsAreExits or Not
  const missingMandatoryFields =
    areAllMandatoryFieldsExist(invoiceDataForInsert)
  if (size(missingMandatoryFields)) {
    throw new CustomError(
      400,
      `Can not create invoice. Missing mandatory fields [${missingMandatoryFields}]`
    )
  }
  // If isLandlordCorrectionInvoice is true then the annual statement period does not need to be checked. Already we have checked before
  if (isLandlordCorrectionInvoice) return true
  // Checking isAnnualStatementCreated or Not
  const annualStatementStatus = await isAnnualStatementCreated(
    invoiceDataForInsert
  )
  if (
    invoiceDataForInsert &&
    !invoiceDataForInsert.isFinalSettlement &&
    invoiceDataForInsert.contractId &&
    invoiceDataForInsert.invoiceStartOn &&
    invoiceDataForInsert.invoiceEndOn &&
    annualStatementStatus
  ) {
    throw new CustomError(
      400,
      'Annual-Statement already created for this lease'
    )
  }
}

export const isLandlordInvoiceOrLandlordCreditNote = (invoiceType) => {
  if (
    invoiceType &&
    indexOf(['landlord_invoice', 'landlord_credit_note'], invoiceType) !== -1
  ) {
    return true
  }
  return false
}

export const getInvoiceById = async (invoiceId, session) => {
  const invoice = await InvoiceCollection.findById(invoiceId).session(session)
  return invoice
}

export const getInvoiceByIdAndPartnerId = async (id, partnerId) => {
  const query = {
    _id: id,
    partnerId
  }
  const invoiceData = await InvoiceCollection.findOne(query)
  return invoiceData
}

export const getInvoiceSerialId = async (
  invoiceData,
  finalSettlement,
  session
) => {
  const directPartner = await partnerHelper.getDirectPartnerById(
    invoiceData.partnerId,
    session
  )
  const addSettlement = 'final-settlement-invoice-'
  const serialBySettlement = finalSettlement
    ? addSettlement + invoiceData.partnerId
    : invoiceData.partnerId
  const invoiceStartNumber = 'invoice-start-number-'
  const serialByStartNumber = invoiceStartNumber + invoiceData.accountId
  const isDirectPartner = !!(
    directPartner && directPartner.enableInvoiceStartNumber
  )
  const id = isDirectPartner ? serialByStartNumber : serialBySettlement
  const invoiceSerialId = await CounterCollection.incrementCounter(id, session)
  return invoiceSerialId
}

export const getKIDNumber = async (params) => {
  const {
    contractId,
    invoiceSerialId,
    isLandlordInvoice,
    finalSettlement,
    isNonRentInvoice
  } = params
  const leaseInfo = await contractHelper.getContractById(contractId)
  await leaseInfo.populate(['partnerId', 'propertyId']).execPopulate()
  if (leaseInfo) {
    const leasePartnerInfo = await leaseInfo.partner
    const leasePropertyInfo = await leaseInfo.property
    const leaseSerialNumber = leaseInfo.leaseSerial
    if (
      leasePartnerInfo &&
      leasePartnerInfo.serial &&
      leasePropertyInfo &&
      leasePropertyInfo.serial
    ) {
      const getPartnerKID = appHelper.getFixedDigits(leasePartnerInfo.serial, 4)
      const getPropertyKID = appHelper.getFixedDigits(
        leasePropertyInfo.serial,
        5
      )
      let getLeaseKID = ''
      let KID = ''
      if (!invoiceSerialId && leaseSerialNumber && isLandlordInvoice) {
        getLeaseKID = '000'
      } else if (!invoiceSerialId && leaseSerialNumber && isNonRentInvoice) {
        const leaseDigits = appHelper.getFixedDigits(leaseSerialNumber, 2)
        const nine = '9'
        getLeaseKID = nine + leaseDigits
      } else if (!invoiceSerialId && leaseSerialNumber) {
        getLeaseKID = appHelper.getFixedDigits(leaseSerialNumber, 3)
      }
      if (invoiceSerialId) {
        getLeaseKID = appHelper.getFixedDigits(invoiceSerialId, 6)
      }
      if (finalSettlement && invoiceSerialId) {
        const leaseKID = appHelper.getFixedDigits(leaseSerialNumber, 3)
        const threeZero = '000'
        getLeaseKID = leaseKID + threeZero
      }
      if (getPartnerKID && getPropertyKID && getLeaseKID) {
        KID = getPartnerKID + getPropertyKID + getLeaseKID
      }
      if (KID) {
        checkDigit.mod11.create(KID)
        return checkDigit.mod11.apply(KID)
      }
    }
  }
  return null
}

export const addAmountRelatedDataWithInvoice = async (
  params,
  isLandlordInvoice
) => {
  const {
    invoiceData,
    partnerSetting,
    monthlyRentAmount,
    addonTotalAmount,
    feeTotal,
    invoiceCommissionableTotal,
    landlordInvoiceTotal = 0,
    feeTaxTotal
  } = params
  console.log(
    'Checking monthlyRentAmount for prepare invoiceTotal: ',
    monthlyRentAmount
  )
  console.log(
    'Checking addonTotalAmount for prepare invoiceTotal: ',
    addonTotalAmount
  )
  console.log(
    'Checking landlordInvoiceTotal for prepare invoiceTotal: ',
    landlordInvoiceTotal
  )
  console.log('Checking feeTotal for prepare invoiceTotal: ', feeTotal)
  const typesOfInvoice = invoiceData.invoiceType
  const invoiceTotalWithoutFee =
    monthlyRentAmount + addonTotalAmount + landlordInvoiceTotal
  let newInvoiceTotal = invoiceTotalWithoutFee + feeTotal
  console.log('Checking newInvoiceTotal: ', newInvoiceTotal)
  let newRoundedAmount = await appHelper.convertTo2Decimal(
    clone(newInvoiceTotal)
  )
  if (isLandlordInvoice) {
    newInvoiceTotal = await appHelper.convertTo2Decimal(clone(newInvoiceTotal))
  } else if (newInvoiceTotal < 0) {
    // Calculate for rounded amount
    const newInvoiceTotalWithMathAbs = clone(Math.abs(newInvoiceTotal))
    const roundAmount = await appHelper.getRoundedAmount(
      newInvoiceTotalWithMathAbs,
      partnerSetting
    )
    newInvoiceTotal = roundAmount * -1
  } else {
    newInvoiceTotal = await appHelper.getRoundedAmount(
      clone(newInvoiceTotal),
      partnerSetting
    )
  }
  if (invoiceTotalWithoutFee === 0) {
    newInvoiceTotal = 0
    invoiceData.feesMeta = []
  }
  if (typesOfInvoice === 'invoice' || typesOfInvoice === 'credit_note') {
    newRoundedAmount = newInvoiceTotal - newRoundedAmount
  }
  if (
    (typesOfInvoice === 'invoice' || typesOfInvoice === 'credit_note') &&
    newRoundedAmount !== 0
  ) {
    invoiceData.roundedAmount = await appHelper.convertTo2Decimal(
      newRoundedAmount
    )
  } else {
    delete invoiceData.roundedAmount
  }
  console.log('Checking newInvoiceTotal last : ', newInvoiceTotal)
  invoiceData.invoiceTotal = newInvoiceTotal // Calculate invoice total and set invoice total

  if (
    typesOfInvoice === 'credit_note' &&
    invoiceData.invoiceTotal === 0 &&
    params?.partner?.accountType === 'direct' // Direct partners can't create credit notes with zero amount, Ref #13694
  ) {
    return false // Throw error if the credit note total is 0
  }
  invoiceData.rentTotal = newInvoiceTotal // Calculate invoice rent total and set invoice rent total
  const calculateTotalCommission =
    monthlyRentAmount + invoiceCommissionableTotal // Calculate commissionable total
  invoiceData.commissionableTotal = await appHelper.convertTo2Decimal(
    calculateTotalCommission
  )
  const payoutableAmount = monthlyRentAmount + addonTotalAmount // Calculate invoice payout able amount
  invoiceData.payoutableAmount = await appHelper.convertTo2Decimal(
    payoutableAmount
  )
  if (isLandlordInvoice) {
    // Set remaining balance for landlord invoice amount will be balanced
    invoiceData.totalBalanced = 0
    invoiceData.remainingBalance = newInvoiceTotal
  }
  invoiceData.totalTAX = await appHelper.convertTo2Decimal(feeTaxTotal)
  return invoiceData
}

export const getInvoiceBankAccountInfo = (
  partnerSetting,
  invoiceAccountNumber
) => {
  let activeBankInfo = ''
  if (partnerSetting && size(partnerSetting.bankAccounts)) {
    activeBankInfo = find(partnerSetting.bankAccounts, (bankAccountInfo) => {
      if (
        invoiceAccountNumber &&
        bankAccountInfo.accountNumber === invoiceAccountNumber
      ) {
        return bankAccountInfo
      }
    })
  }
  return activeBankInfo
}

export const getSenderInfoForInvoice = async (params = {}) => {
  const { accountId, invoiceAccountNumber, partnerSetting } = params
  let { partner = {} } = params
  let name = ''
  let orgId = ''
  const invoiceSenderData = {}
  if (!size(partner)) {
    partner = await partnerHelper.getPartnerById(partnerSetting.partnerId)
  }
  if (partner.accountType === 'direct') {
    const query = {
      _id: accountId,
      partnerId: partnerSetting.partnerId
    }
    const accountInfo = await accountHelper.getAnAccount(query, null, [
      'person',
      'organization'
    ])
    if (accountInfo) {
      const { organization } = accountInfo
      name = organization && organization.name ? organization.name : ''
      orgId = organization && organization.orgId ? organization.orgId : ''
    }
  } else {
    const bankAccountInfo = getInvoiceBankAccountInfo(
      partnerSetting,
      invoiceAccountNumber
    )
    if (bankAccountInfo && bankAccountInfo.orgName) {
      name = bankAccountInfo.orgName
      orgId = bankAccountInfo.orgId ? bankAccountInfo.orgId : ''
    } else {
      name =
        partnerSetting.companyInfo && partnerSetting.companyInfo.companyName
          ? partnerSetting.companyInfo.companyName
          : ''
      orgId =
        partnerSetting.companyInfo && partnerSetting.companyInfo.organizationId
          ? partnerSetting.companyInfo.organizationId
          : ''
    }
  }
  if (name) {
    invoiceSenderData.companyName = name
  }
  if (orgId) {
    invoiceSenderData.orgId = orgId
  }
  return invoiceSenderData
}

export const getLandLordInvoiceSenderInfo = async (invoiceData) => {
  const partnerId = invoiceData.partnerId ? invoiceData.partnerId : ''
  const commissionsId =
    invoiceData && size(invoiceData.commissionsIds)
      ? invoiceData.commissionsIds[0]
      : ''
  const query = {
    _id: commissionsId,
    partnerId
  }
  const commissionInfo = (await commissionHelper.getCommission(query)) || {}
  let invoiceInfo = {}
  if (commissionInfo.invoiceId) {
    invoiceInfo =
      (await getInvoiceByIdAndPartnerId(commissionInfo.invoiceId, partnerId)) ||
      {}
  }
  const sender = invoiceInfo.sender || invoiceData.sender
  return sender
}

export const processInvoiceDataBeforeCreation = async (params) => {
  const { enabledNotification, partnerSetting } = params
  let { partner } = params
  if (!partner) {
    partner = await partnerHelper.getPartnerById(partnerSetting.partnerId)
    if (!partner) throw new CustomError(404, 'Partner not found')
  }
  // New Code Starts
  const isLandlordInvoice = isLandlordInvoiceOrLandlordCreditNote(
    params.invoiceData.invoiceType
  )
  // New Code ends
  // Returns old invoiceData with new amount related data
  console.log('Checking params before addAmountRelatedDataWithInvoice', params)
  const invoiceData = await addAmountRelatedDataWithInvoice(
    params,
    isLandlordInvoice
  )
  if (!size(invoiceData)) {
    return false
  }
  invoiceData.sender = await getSenderInfoForInvoice({
    accountId: invoiceData.accountId,
    invoiceAccountNumber: invoiceData.invoiceAccountNumber,
    partnerSetting,
    partner
  })
  // Set sender info
  if (invoiceData.invoiceType === 'landlord_invoice') {
    invoiceData.sender = await getLandLordInvoiceSenderInfo(invoiceData)
  }
  if (invoiceData.invoiceTotal === 0) {
    invoiceData.status = 'paid'
    invoiceData.enabledNotification = false
  } else {
    invoiceData.enabledNotification = enabledNotification
  }
  const todayDate = await appHelper.getActualDate(
    partnerSetting,
    false,
    new Date()
  )
  if (
    invoiceData.dueDate &&
    (
      await appHelper.getActualDate(partnerSetting, true, invoiceData.dueDate)
    ).isBefore(todayDate)
  ) {
    invoiceData.dueDate = todayDate
  }
  return invoiceData
}

export const prepareQueryForAddEstimatedPayout = (data) => {
  const { partnerId, propertyId, contractId } = data
  const query = {
    partnerId,
    propertyId,
    _id: contractId
  }
  return query
}

export const getInvoicesPreview = async (params, session) => {
  let { today } = params
  const { contract, returnEstimatedPayoutPreview } = params
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    contract.partnerId,
    session
  )
  today = today ? new Date(today) : new Date()
  today = await getInvoiceDate(today, partnerSettings)
  const invoicesListParams = {
    contract,
    partnerSettings,
    today,
    returnEstimatedPayoutPreview
  }
  const invoicesList = await getInvoicesList(invoicesListParams, session)
  return invoicesList
}

export const getNonCreditedInvoiceQuery = (contract) => {
  const query = {
    contractId: contract._id,
    invoiceType: 'invoice',
    status: { $ne: 'credited' },
    isCorrectionInvoice: { $ne: true }
  }
  return query
}

export const getEndDateForFirstInvoice = async (
  partnerSettings,
  startDate,
  endDate
) => {
  // Example: If 1st invoice start date is 1st January 2018 and contract end date is 28th February 2018
  // So, this invoice duration is 1st to 31th January
  // Another example: If 1st invoice start date is 2nd January 2018 and contract end date is 28th February 2018
  // So, this invoice duration is 2nd January to 1st February
  const invoiceStartDay = (
    await appHelper.getActualDate(partnerSettings, true, startDate)
  ).format('D')
  let invoiceEndDay = (
    await appHelper.getActualDate(partnerSettings, true, endDate)
  )
    .endOf('month')
    .format('D')
  let updatedEndDate = endDate
  if (invoiceStartDay !== 1) {
    invoiceEndDay = invoiceStartDay - 1
    const nextMonthEndDay = (
      await appHelper.getActualDate(partnerSettings, true, startDate)
    )
      .add(1, 'months')
      .endOf('month')
      .format('D')
    if (invoiceEndDay >= nextMonthEndDay) {
      invoiceEndDay = nextMonthEndDay
    }
    updatedEndDate = (
      await appHelper.getActualDate(partnerSettings, true, startDate)
    )
      .add(1, 'months')
      .set('date', invoiceEndDay)
      .toDate()
  }
  return updatedEndDate
}

export const getMainRangesList = async (params) => {
  const {
    contractDueDays,
    firstInvoiceDueDate,
    invoiceCalculation,
    invoiceFrequency,
    isRangesForEstimatedInvoices,
    leaseInvoiceEnd,
    leaseInvoiceStart,
    partnerSettings,
    today
  } = params
  const monthListParams = {
    endMonthDate: leaseInvoiceEnd,
    invoiceFrequency,
    partnerSetting: partnerSettings,
    startMonthDate: leaseInvoiceStart
  }
  const monthsList = await getListOfMonths(monthListParams)
  let mainRangesList = []
  let isFirstMonth = true
  let noOfInvoice = 0
  let lastEndDate = ''
  for (const [index, month] of monthsList.entries()) {
    let startDate = (
      await appHelper.getActualDate(partnerSettings, true, month)
    )
      .startOf('month')
      .toDate() // Rent start
    let endDate = (await appHelper.getActualDate(partnerSettings, true, month))
      .endOf('month')
      .toDate() // Rent end
    startDate = startDate < leaseInvoiceStart ? leaseInvoiceStart : startDate
    endDate = endDate > leaseInvoiceEnd ? leaseInvoiceEnd : endDate

    if (
      isFirstMonth &&
      invoiceCalculation === 'prorated_second_month' &&
      invoiceFrequency === 1
    ) {
      endDate = await getEndDateForFirstInvoice(
        partnerSettings,
        startDate,
        endDate
      )
    } else if (
      noOfInvoice === 1 &&
      invoiceCalculation === 'prorated_second_month' &&
      invoiceFrequency === 1
    ) {
      // Example: If 1st invoice end date is 10th January 2018 then 2nd invoice start date is 11th January 2018
      // So, second invoice start date = (1st invoice end date + 1)
      startDate = (
        await appHelper.getActualDate(partnerSettings, true, lastEndDate)
      )
        .add(1, 'days')
        .toDate()
      endDate = (
        await appHelper.getActualDate(partnerSettings, true, startDate)
      )
        .endOf('month')
        .toDate()
    } else if (invoiceFrequency > 1) {
      endDate = (
        await appHelper.getActualDate(partnerSettings, true, startDate)
      )
        .startOf('month')
        .add(invoiceFrequency - 1, 'months')
        .endOf('month')
        .toDate()
    }
    if (endDate > leaseInvoiceEnd) {
      endDate = leaseInvoiceEnd
    }
    const invoiceDueDate = (
      await appHelper.getActualDate(partnerSettings, true, startDate)
    )
      .set('date', contractDueDays)
      .toDate()
    let invoiceCreationDate = await appHelper.subtractDays(
      invoiceDueDate,
      partnerSettings.invoiceDueDays,
      partnerSettings
    )
    lastEndDate = clone(endDate)
    if (index === 0) {
      invoiceCreationDate = await appHelper.subtractDays(
        firstInvoiceDueDate,
        partnerSettings.invoiceDueDays,
        partnerSettings
      )
    }
    // We'll only create invoice if today is the firstInvoiceCreationDate or past
    if (
      isDatePastOrToday(today, invoiceCreationDate) ||
      isRangesForEstimatedInvoices
    ) {
      mainRangesList.push({
        invoiceStartOn: clone(startDate),
        invoiceEndOn: clone(endDate),
        isFirstInvoice: clone(isFirstMonth)
      })
    }
    isFirstMonth = false
    noOfInvoice++
  }
  mainRangesList = sortBy(mainRangesList, ['invoiceStartOn'])
  return mainRangesList
}

export const getInvoicesRangeList = async (invoiceParams) => {
  const {
    partnerSettings,
    rentalMeta,
    testTodayDate,
    isRangesForEstimatedInvoices
  } = invoiceParams

  let today = testTodayDate ? new Date(testTodayDate) : new Date()
  today = await appHelper.getActualDate(partnerSettings, false, today)
  const invoiceFrequency = parseInt(rentalMeta?.invoiceFrequency || 1)
  const invoiceCreationDate = (
    await appHelper.getActualDate(partnerSettings, true, today)
  )
    .add(invoiceFrequency - 1, 'months')
    .add(partnerSettings.invoiceDueDays || 0, 'days')
    .toDate()
  const contractStart = await appHelper.getActualDate(
    partnerSettings,
    false,
    rentalMeta.contractStartDate
  )
  let leaseInvoiceEnd = ''
  let leaseInvoiceStart = (
    await appHelper.getActualDate(
      partnerSettings,
      true,
      rentalMeta.invoiceStartFrom
    )
  )
    .startOf('month')
    .toDate() // Rent start

  if (leaseInvoiceStart < contractStart) leaseInvoiceStart = contractStart

  if (isRangesForEstimatedInvoices) {
    const totalMonthsForThreeInvoices = invoiceFrequency * 3 - 1

    leaseInvoiceEnd = (
      await appHelper.getActualDate(partnerSettings, true, leaseInvoiceStart)
    )
      .add(totalMonthsForThreeInvoices, 'months')
      .endOf('month')
      .toDate()
  } else {
    if (rentalMeta.contractEndDate) {
      leaseInvoiceEnd = await appHelper.getActualDate(
        partnerSettings,
        false,
        rentalMeta.contractEndDate
      )
    } else {
      leaseInvoiceEnd = (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          invoiceCreationDate
        )
      )
        .endOf('month')
        .toDate()
    }
  }
  const invoiceCalculation = rentalMeta.invoiceCalculation
    ? rentalMeta.invoiceCalculation
    : ''
  const contractDueDays = rentalMeta.dueDate ? rentalMeta.dueDate : 1
  const firstInvoiceDueDate = rentalMeta.firstInvoiceDueDate
    ? rentalMeta.firstInvoiceDueDate
    : 1

  const params = {
    invoiceFrequency,
    leaseInvoiceStart,
    leaseInvoiceEnd,
    partnerSettings,
    invoiceCalculation,
    contractDueDays,
    firstInvoiceDueDate,
    today,
    isRangesForEstimatedInvoices
  }
  const mainRangesList = await getMainRangesList(params)
  return mainRangesList
}

export const getMissingInvoices = async (invoices, partnerSettings) => {
  const invoicesArr = []
  let isAlreadyExists = false
  for (const invoice of invoices) {
    if (invoice) {
      invoicesArr.push({
        invoiceStartOn: await appHelper.getActualDate(
          partnerSettings,
          false,
          invoice.invoiceStartOn
        ),
        invoiceEndOn: await appHelper.getActualDate(
          partnerSettings,
          false,
          invoice.invoiceEndOn
        ),
        isFirstInvoice: invoice.isFirstInvoice
      })

      if (invoice.isFirstInvoice) {
        isAlreadyExists = true
      }
    }
  }
  return { invoicesArr, isAlreadyExists }
}

export const getInvoicesByRange = async (
  invoicesList,
  range,
  partnerSettings
) => {
  const start = await appHelper.getActualDate(
    partnerSettings,
    true,
    clone(range.invoiceStartOn)
  )
  const end = await appHelper.getActualDate(
    partnerSettings,
    true,
    clone(range.invoiceEndOn)
  )
  const format = 'YYYY-MM-DD'
  const rangeStartOn = start.startOf('day').toDate()
  const rangeEndOn = end.startOf('day').toDate()
  const rangeStartOnFormat = start.format(format)
  const rangeEndOnFormat = end.format(format)
  let filteredInvoiceList = []

  for (const invoiceInfo of invoicesList) {
    const invoiceStart = await appHelper.getActualDate(
      partnerSettings,
      true,
      invoiceInfo.invoiceStartOn
    )
    const invoiceEnd = await appHelper.getActualDate(
      partnerSettings,
      true,
      invoiceInfo.invoiceEndOn
    )
    const invoiceStartOn = invoiceStart.startOf('day').toDate()
    const invoiceEndOn = invoiceEnd.startOf('day').toDate()
    const invoiceStartOnFormat = invoiceStart.format(format)
    const invoiceEndOnFormat = invoiceEnd.format(format)
    if (
      invoiceInfo &&
      rangeStartOn &&
      rangeEndOn &&
      (invoiceStartOn > rangeStartOn ||
        invoiceStartOnFormat === rangeStartOnFormat) &&
      (invoiceStartOn < rangeEndOn ||
        invoiceStartOnFormat === rangeEndOnFormat) &&
      (invoiceEndOn > rangeStartOn ||
        invoiceEndOnFormat === rangeStartOnFormat) &&
      (invoiceEndOn < rangeEndOn || invoiceEndOnFormat === rangeEndOnFormat)
    ) {
      filteredInvoiceList.push(invoiceInfo)
    } else if (
      invoiceInfo &&
      rangeStartOn &&
      rangeEndOn &&
      invoiceStartOn < rangeEndOn &&
      invoiceEndOn > rangeEndOn &&
      (invoiceStartOnFormat === rangeStartOnFormat ||
        invoiceStartOn > rangeStartOn)
    ) {
      invoiceInfo.invoiceEndOn = clone(range.invoiceEndOn)
      filteredInvoiceList.push(invoiceInfo)
    } else if (
      invoiceInfo &&
      rangeStartOn &&
      rangeEndOn &&
      invoiceStartOn < rangeStartOn &&
      (invoiceEndOn > rangeStartOn ||
        invoiceEndOnFormat === rangeStartOnFormat) &&
      (invoiceEndOn < rangeEndOn || invoiceEndOnFormat === rangeEndOnFormat)
    ) {
      invoiceInfo.invoiceStartOn = clone(range.invoiceStartOn)
      filteredInvoiceList.push(invoiceInfo)
    }
  }
  filteredInvoiceList = sortBy(filteredInvoiceList, ['invoiceStartOn'])
  return filteredInvoiceList
}

export const prepareStartOfDay = async (date, partnerSettings) => {
  const formattedDate = (
    await appHelper.getActualDate(partnerSettings, true, date)
  )
    .startOf('day')
    .toDate()
  return formattedDate
}

export const isStartAndEndDateInRange = (
  start,
  end,
  rangeStartOn,
  rangeEndOn
) => {
  const isWithinRange =
    start &&
    end &&
    start.startOf('day').toDate() >= rangeStartOn &&
    start.startOf('day').toDate() <= rangeEndOn &&
    end.startOf('day').toDate() >= rangeStartOn &&
    end.startOf('day').toDate() <= rangeEndOn &&
    end.startOf('day').toDate() >= start.startOf('day').toDate()
  return isWithinRange
}

export const getRangesOfInvoices = async (params) => {
  const { invoicesOfSpecificRange, range, partnerSettings } = params
  const missingInvoices = []
  const rangeStartOn = await prepareStartOfDay(
    range.invoiceStartOn,
    partnerSettings
  )
  const rangeEndOn = await prepareStartOfDay(
    range.invoiceEndOn,
    partnerSettings
  )
  const format = 'YYYY-MM-DD'
  let { isFirstInvoice } = params
  let start = await appHelper.getActualDate(
    partnerSettings,
    true,
    range.invoiceStartOn
  )
  let end = await appHelper.getActualDate(
    partnerSettings,
    true,
    range.invoiceEndOn
  )
  for (const invoice of invoicesOfSpecificRange) {
    const invoiceStartOn = await appHelper.getActualDate(
      partnerSettings,
      true,
      invoice.invoiceStartOn
    )
    const invoiceEndOn = await appHelper.getActualDate(
      partnerSettings,
      true,
      invoice.invoiceEndOn
    )
    // Without formatting the date the comparison will not work
    if (
      start &&
      invoiceStartOn.format(format) === start.format(format) &&
      invoiceEndOn.format(format) === end.format(format)
    ) {
      start = null
      end = null
    } else if (
      start &&
      invoiceStartOn.startOf('day').toDate() > start.startOf('day').toDate()
    ) {
      missingInvoices.push({
        invoiceStartOn: start.toDate(),
        invoiceEndOn: await appHelper.subtractDays(
          invoice.invoiceStartOn,
          1,
          partnerSettings
        ),
        isFirstInvoice
      })
      if (isFirstInvoice) {
        isFirstInvoice = false
      }
      start = (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          invoice.invoiceEndOn
        )
      ).add(1, 'days')
    } else if (
      start &&
      invoiceStartOn.format(format) === start.format(format) &&
      invoiceEndOn.startOf('day').toDate() < end.startOf('day').toDate()
    ) {
      start = (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          invoice.invoiceEndOn
        )
      ).add(1, 'days')
    }
  }
  if (isStartAndEndDateInRange(start, end, rangeStartOn, rangeEndOn)) {
    missingInvoices.push({
      invoiceStartOn: start.toDate(),
      invoiceEndOn: end.toDate(),
      isFirstInvoice
    })
  }
  return missingInvoices
}

export const prepareMissingInvoiceByRange = async (params) => {
  const { invoicesList, partnerSettings, invoiceRangeInfo, isFirstInvoice } =
    params
  const invoicesOfRange = await getInvoicesByRange(
    invoicesList,
    invoiceRangeInfo,
    partnerSettings
  )
  const newMissingInvoices = []
  if (size(invoicesOfRange)) {
    const rangesParams = {
      invoicesOfSpecificRange: clone(invoicesOfRange),
      range: invoiceRangeInfo,
      partnerSettings,
      isFirstInvoice
    }
    const excludedInvoicesInRange = await getRangesOfInvoices(rangesParams)
    console.log('excludedInvoicesInRange', excludedInvoicesInRange)
    if (size(excludedInvoicesInRange)) {
      newMissingInvoices.push(...excludedInvoicesInRange)
    }
  } else {
    invoiceRangeInfo.isFirstInvoice = isFirstInvoice
    newMissingInvoices.push(invoiceRangeInfo)
  }
  console.log('prepareMissingInvoiceByRange', {
    invoicesOfRange,
    newMissingInvoices
  })
  return newMissingInvoices
}

export const removeFromMissingInvoices = async (
  newMissingInvoices,
  partnerSettings,
  annualStatementsPeriod
) => {
  const missingInvoices = []
  for (const newMissingInvoiceInfo of newMissingInvoices) {
    const periodForInvoiceStartOn = Number(
      (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          newMissingInvoiceInfo.invoiceStartOn
        )
      ).format('YYYY') * 1
    )
    const periodForInvoiceEndOn = Number(
      (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          newMissingInvoiceInfo.invoiceEndOn
        )
      ).format('YYYY') * 1
    )
    if (
      periodForInvoiceStartOn > annualStatementsPeriod &&
      periodForInvoiceEndOn > annualStatementsPeriod
    ) {
      missingInvoices.push(newMissingInvoiceInfo)
    }
  }
  return missingInvoices
}

export const isAddMissingInvoice = async (
  invoiceRangeList,
  missingInvoiceRange,
  partnerSettings
) => {
  const missingInvoiceStart = await appHelper.getActualDate(
    partnerSettings,
    true,
    clone(missingInvoiceRange.invoiceStartOn)
  )
  const missingInvoiceEnd = await appHelper.getActualDate(
    partnerSettings,
    true,
    clone(missingInvoiceRange.invoiceEndOn)
  )
  const format = 'YYYY-MM-DD'
  const missingInvoiceRangeStartOn = missingInvoiceStart.startOf('day').toDate()
  const missingInvoiceRangeEndOn = missingInvoiceEnd.startOf('day').toDate()
  const missingInvoiceRangeStartOnFormat = missingInvoiceStart.format(format)
  const missingInvoiceRangeEndOnFormat = missingInvoiceEnd.format(format)

  // Without formatting the date equal comparison will not work
  let getMissingInvoiceRange
  for (const invoiceRange of invoiceRangeList) {
    const invoiceStart = await appHelper.getActualDate(
      partnerSettings,
      true,
      clone(invoiceRange.invoiceStartOn)
    )
    const invoiceEnd = await appHelper.getActualDate(
      partnerSettings,
      true,
      clone(invoiceRange.invoiceEndOn)
    )
    const invoiceStartOn = invoiceStart.startOf('day').toDate()
    const invoiceEndOn = invoiceEnd.startOf('day').toDate()
    const invoiceStartOnFormat = invoiceStart.format(format)
    const invoiceEndOnFormat = invoiceEnd.format(format)

    if (
      missingInvoiceRangeStartOn &&
      missingInvoiceRangeEndOn &&
      (missingInvoiceRangeStartOn > invoiceStartOn ||
        missingInvoiceRangeStartOnFormat === invoiceStartOnFormat) &&
      (missingInvoiceRangeStartOn < invoiceEndOn ||
        missingInvoiceRangeStartOnFormat === invoiceEndOnFormat) &&
      (missingInvoiceRangeEndOn > invoiceStartOn ||
        missingInvoiceRangeEndOnFormat === invoiceStartOnFormat) &&
      (missingInvoiceRangeEndOn < invoiceEndOn ||
        missingInvoiceRangeEndOnFormat === invoiceEndOnFormat)
    ) {
      getMissingInvoiceRange = invoiceRange
      break
    }
  }
  return getMissingInvoiceRange
}

export const updateNewMissingInvoiceByPreferredRanges = async (
  preferredRangesParams
) => {
  const { preferredRanges, partnerSettings, invoicesRangeList } =
    preferredRangesParams
  const newMissingInvoices = []
  let { isFirstInvoiceAlreadyExists } = preferredRangesParams
  let isFirstInvoice = false
  for (const preferredRange of preferredRanges) {
    const getMissingInvoiceRange = await isAddMissingInvoice(
      clone(invoicesRangeList),
      preferredRange,
      partnerSettings
    )
    if (size(getMissingInvoiceRange)) {
      // If 4th parameter is true then compare range will be first invoice range
      if (
        !isFirstInvoiceAlreadyExists &&
        getMissingInvoiceRange.isFirstInvoice
      ) {
        isFirstInvoice = true
        isFirstInvoiceAlreadyExists = true
      } else {
        isFirstInvoice = false
      }
      preferredRange.isFirstInvoice = isFirstInvoice
      newMissingInvoices.push(preferredRange)
    }
  }
  return { newMissingInvoices, isFirstInvoiceAlreadyExists }
}

export const updateNewMissingInvoices = async (params) => {
  const { preferredRanges, partnerSettings } = params
  const { invoicesRangeList, invoicesList } = params
  let { isFirstInvoice, newMissingInvoices, isFirstInvoiceAlreadyExists } =
    params
  if (size(invoicesRangeList)) {
    for (const invoiceRangeInfo of invoicesRangeList) {
      if (invoiceRangeInfo.isFirstInvoice && !isFirstInvoiceAlreadyExists) {
        isFirstInvoice = true
      } else {
        isFirstInvoice = false
      }
      const missingInvoiceParams = {
        invoicesList,
        partnerSettings,
        invoiceRangeInfo,
        isFirstInvoice
      }
      let missingInRangeInvoices = await prepareMissingInvoiceByRange(
        missingInvoiceParams
      )
      if (size(missingInRangeInvoices)) {
        missingInRangeInvoices = JSON.parse(
          JSON.stringify(missingInRangeInvoices)
        )
        newMissingInvoices.push(...missingInRangeInvoices)
      }
    }
  }
  if (size(preferredRanges)) {
    const preferredRangesParams = {
      preferredRanges,
      partnerSettings,
      isFirstInvoiceAlreadyExists,
      invoicesRangeList: newMissingInvoices
    }
    ;({ newMissingInvoices, isFirstInvoiceAlreadyExists } =
      await updateNewMissingInvoiceByPreferredRanges(preferredRangesParams))
  }
  return {
    newMissingInvoices,
    isFirstInvoiceAlreadyExists,
    isFirstInvoice
  }
}

export const getCreatableInvoicesByRange = async (params, session) => {
  const {
    contract,
    partnerSettings,
    returnEstimatedPayoutPreview,
    today,
    preferredRanges
  } = params
  const invoiceQuery = getNonCreditedInvoiceQuery(contract)
  let invoices = []
  if (!returnEstimatedPayoutPreview) {
    invoices = await getInvoices(invoiceQuery, session)
  }
  const invoiceRangeParams = {
    partnerSettings,
    rentalMeta: contract.rentalMeta,
    testTodayDate: today
  }
  const invoicesRangeList = await getInvoicesRangeList(invoiceRangeParams)
  console.log('invoicesRangeList', invoicesRangeList)
  const latestAnnualStatement =
    await annualStatementHelper.getAnnualStatementWithSort(
      {
        contractId: contract._id
      },
      {
        statementYear: -1
      }
    )
  const annualStatementsPeriod = latestAnnualStatement?.statementYear || 0
  console.log('annualStatementsPeriod', annualStatementsPeriod)
  let invoicesList = []
  let newMissingInvoices = []
  let isFirstInvoiceAlreadyExists = false
  const isFirstInvoice = false
  let missingInvoices = []
  if (size(invoices)) {
    const { invoicesArr, isAlreadyExists } = await getMissingInvoices(
      invoices,
      partnerSettings
    )
    invoicesList = invoicesArr
    invoicesList = sortBy(invoicesList, ['invoiceStartOn'])
    console.log('invoicesList from created invoices:', invoicesList)
    if (isAlreadyExists) {
      isFirstInvoiceAlreadyExists = true
    }
  }
  const newMissingInvoicesParams = {
    preferredRanges,
    partnerSettings,
    invoicesRangeList,
    invoicesList,
    isFirstInvoice,
    newMissingInvoices,
    isFirstInvoiceAlreadyExists
  }
  ;({ newMissingInvoices } = await updateNewMissingInvoices(
    newMissingInvoicesParams
  ))
  console.log(
    'newMissingInvoices from invoices range list:',
    newMissingInvoices
  )
  // Remove missing invoices which invoice period matched with annual statements period based on contract
  if (annualStatementsPeriod && size(newMissingInvoices)) {
    missingInvoices = await removeFromMissingInvoices(
      newMissingInvoices,
      partnerSettings,
      annualStatementsPeriod
    )
  } else {
    missingInvoices = newMissingInvoices
  }
  missingInvoices = sortBy(missingInvoices, ['invoiceStartOn'])
  console.log('missingInvoices:', missingInvoices)
  return { missingInvoices }
}

export const getDueDate = async (params) => {
  const {
    today,
    currentInvoiceDueDate,
    contract,
    data,
    returnPreview,
    partnerSettings
  } = params
  let dueDate = currentInvoiceDueDate
  let ignoreRecurringDueDate = false
  // We'll only check if invoice is past or today
  if (
    returnPreview &&
    !data.isFirstInvoice &&
    isDatePastOrToday(today, currentInvoiceDueDate)
  ) {
    dueDate = contract.rentalMeta.firstInvoiceDueDate
    ignoreRecurringDueDate = true
  }
  // We'll only check if invoice due date is future and less than firstInvoiceDueDate
  if (
    returnPreview &&
    !data.isFirstInvoice &&
    isDateFutureOrToday(today, currentInvoiceDueDate) &&
    currentInvoiceDueDate < contract.rentalMeta.firstInvoiceDueDate
  ) {
    dueDate = contract.rentalMeta.firstInvoiceDueDate
    ignoreRecurringDueDate = true
  }
  if (data.isFirstInvoice) {
    dueDate = contract.rentalMeta.firstInvoiceDueDate
    ignoreRecurringDueDate = true
  }
  dueDate = await getInvoiceDueDate(partnerSettings, dueDate, today)
  return { dueDate, ignoreRecurringDueDate }
}

export const getRentInvoiceData = async (data, session) => {
  const { contract, returnEstimatedPayoutPreview } = data
  const {
    partnerSetting,
    invoiceBasicData,
    isDemo,
    enabledNotification,
    isFirstInvoice,
    returnPreview,
    ignoreExistingInvoices,
    ignoreRecurringDueDate,
    invoiceCountFromBeginning,
    manualInvoiceCreateOption
  } = data
  const noOfInvoice =
    manualInvoiceCreateOption && manualInvoiceCreateOption.noOfInvoice
      ? manualInvoiceCreateOption.noOfInvoice
      : 0
  if (invoiceBasicData && !invoiceBasicData.invoiceMonth) {
    throw new CustomError(
      500,
      `Invoice month is undefined, contract id: ${contract._id}`
    )
  } // Since we added updated contract, we have to find updated contract info
  const invoiceParams = {
    contract,
    partnerSetting,
    invoiceMonth: invoiceBasicData.invoiceMonth,
    invoiceFrequency: invoiceBasicData.invoiceFrequency,
    ignoreExistingInvoices,
    ignoreRecurringDueDate,
    manualInvoiceCreateOption,
    noOfInvoice,
    invoiceBasicData,
    invoiceCountFromBeginning,
    isFirstInvoice,
    returnPreview,
    returnEstimatedPayoutPreview
  }
  let invoiceDurationInfo = {}
  // Dont need session for estimated payout preview
  if (returnEstimatedPayoutPreview) {
    invoiceDurationInfo = await getInvoiceDurationInfo(invoiceParams)
  } else {
    invoiceDurationInfo = await getInvoiceDurationInfo(invoiceParams, session) // Check If invoice already created for this month
  }
  if (!size(invoiceDurationInfo)) {
    return false
  }
  const { startOfMonth, endOfMonth } = invoiceDurationInfo
  const basicInvoiceData = await getBasicInvoiceData({
    invoiceParams,
    startOfMonth,
    endOfMonth
  })
  if (!basicInvoiceData) {
    return false
  }
  const { invoiceData, options } = basicInvoiceData
  const dataOfAMonth = {
    // CreateMonthlyInvoice and return the result
    contract,
    partnerSetting,
    invoiceBasicData: invoiceData,
    options,
    isDemo,
    enabledNotification
  }
  return dataOfAMonth
}

export const getMonthlyDataOfRentInvoice = async (monthlyInvoiceData) => {
  const {
    contract,
    partnerSetting,
    invoiceBasicData,
    options,
    isDemo,
    enabledNotification
  } = monthlyInvoiceData
  const invoiceContent = await getInvoiceContent(
    contract,
    options,
    invoiceBasicData
  )
  const addonsInitialData = await prepareAddonsInitialData({
    contract,
    options,
    partnerSetting,
    invoiceBasicData
  })
  const {
    addonTotalAmount,
    invoiceAddonsMeta,
    invoiceCommissionableTotal,
    isThisFirstInvoice
  } = addonsInitialData
  if (size(invoiceAddonsMeta.addonsMetaData)) {
    invoiceBasicData.addonsMeta = invoiceAddonsMeta.addonsMetaData
  } else {
    delete invoiceBasicData.addonsMeta
  }
  if (size(invoiceAddonsMeta.correctionsIds)) {
    invoiceBasicData.correctionsIds = invoiceAddonsMeta.correctionsIds
  } else {
    delete invoiceBasicData.correctionsIds
  }
  const feesParams = {
    contract,
    partnerSetting,
    isThisFirstInvoice,
    isLandlordInvoice: false
  }
  const invoiceFeesMetaData = await getInvoiceFeesMetaData(feesParams)
  if (invoiceFeesMetaData && size(invoiceFeesMetaData.invoiceFeesMeta)) {
    invoiceBasicData.feesMeta = invoiceFeesMetaData.invoiceFeesMeta
  } // Set invoice content info
  invoiceBasicData.invoiceContent = invoiceContent
  invoiceBasicData.isFirstInvoice = isThisFirstInvoice
  if (isDemo) {
    invoiceBasicData.isDemo = true
  }
  const data = {
    invoiceData: invoiceBasicData,
    partnerSetting,
    monthlyRentAmount: options.monthlyRent,
    addonTotalAmount,
    feeTotal: invoiceFeesMetaData.feeTotal,
    invoiceCommissionableTotal,
    feeTaxTotal: invoiceFeesMetaData.feeTaxTotal,
    options,
    enabledNotification
  }
  return data
}

export const getPreviewData = async (previewParam, session) => {
  const {
    contract,
    enabledNotification,
    ignoreExistingInvoiceChecking,
    ignoreRecurringDueDate,
    invoiceCountFromBeginning,
    invoiceData,
    isFirstInvoice,
    manualInvoiceCreateOption = {},
    partnerSettings,
    returnPreview,
    today,
    userId
  } = previewParam
  const updateContractData = {}
  const params = {
    contract,
    enabledNotification,
    ignoreExistingInvoices: ignoreExistingInvoiceChecking,
    ignoreRecurringDueDate,
    invoiceCountFromBeginning,
    partnerSetting: partnerSettings,
    returnPreview
  }
  params.isFirstInvoice = isFirstInvoice
  params.manualInvoiceCreateOption = manualInvoiceCreateOption
  if (size(invoiceData)) {
    params.invoiceBasicData = invoiceData
  }
  // Comm: const previewData = await rentInvoiceService.generateInvoice(params);
  // If this month is under next cpi rent amount then we have to update rent amount for invoice
  //Since we added updated contract, we have to find updated contract info
  // In next update we need to done this after all due date calculation
  if (
    invoiceData &&
    invoiceData.dueDate &&
    contract &&
    contract.rentalMeta &&
    contract.rentalMeta.futureRentAmount &&
    contract.rentalMeta.lastCPINotificationSentOn &&
    contract.rentalMeta.nextCpiDate &&
    ((await appHelper.getActualDate(
      partnerSettings,
      false,
      invoiceData.dueDate
    )) >=
      (await appHelper.getActualDate(
        partnerSettings,
        false,
        contract.rentalMeta.nextCpiDate
      )) ||
      (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          invoiceData.dueDate
        )
      ).format('YYYY-MM-DD') ===
        (
          await appHelper.getActualDate(
            partnerSettings,
            true,
            contract.rentalMeta.nextCpiDate
          )
        ).format('YYYY-MM-DD'))
  ) {
    contract.rentalMeta.monthlyRentAmount =
      contract.rentalMeta?.futureRentAmount ||
      contract.rentalMeta.monthlyRentAmount
    const nextCpiDate = (
      await appHelper.getActualDate(partnerSettings, true, today)
    )
      .add(12, 'months')
      .endOf('day')
      .toDate()
    const lastCpiDate = (
      await appHelper.getActualDate(partnerSettings, true, today)
    )
      .endOf('day')
      .toDate()
    contract.rentalMeta.lastCpiDate = lastCpiDate
    contract.rentalMeta.nextCpiDate = nextCpiDate
    delete contract.rentalMeta.futureRentAmount
    delete contract.rentalMeta.lastCPINotificationSentOn
    updateContractData.$set = {
      'rentalMeta.monthlyRentAmount': contract.rentalMeta.monthlyRentAmount,
      'rentalMeta.lastCpiDate': contract.rentalMeta.lastCpiDate,
      'rentalMeta.nextCpiDate': contract.rentalMeta.nextCpiDate
    }
    updateContractData.$unset = {
      'rentalMeta.futureRentAmount': '',
      'rentalMeta.lastCPINotificationSentOn': ''
    }
  }

  const dataOfAMonth = await getRentInvoiceData(params, session)
  if (!dataOfAMonth) return false
  const monthlyInvoiceData = await getMonthlyDataOfRentInvoice(dataOfAMonth)
  const previewData = await processInvoiceDataBeforeCreation(monthlyInvoiceData)
  if (size(previewData)) {
    if (returnPreview) {
      previewData.createdAt = today
    }
    contract.rentalMeta.invoicedAsOn = previewData.invoiceEndOn
  }

  if (
    contract?.rentalMeta?.cpiEnabled &&
    !returnPreview &&
    size(updateContractData) &&
    size(previewData) &&
    previewData.invoiceTotal >= 0
  ) {
    console.log('Contract update logs for invoice', contract?._id)
    const previousContract = await contractHelper.getContractById(contract?._id)
    await contractService.createLeaseChangeLog(
      {
        fieldName: 'monthlyRentAmount',
        CPIBasedIncrement: true,
        contractInfo: contract,
        previousContract,
        userId
      },
      session
    )
    if (
      moment(previousContract.rentalMeta.lastCpiDate).format('YYYY-MM-DD') !==
      moment(contract.rentalMeta.lastCpiDate).format('YYYY-MM-DD')
    ) {
      await contractService.createLeaseChangeLog(
        {
          fieldName: 'lastCpiDate',
          contractInfo: contract,
          previousContract,
          userId
        },
        session
      )
    }
    if (
      moment(previousContract.rentalMeta.nextCpiDate).format('YYYY-MM-DD') !==
      moment(contract.rentalMeta.nextCpiDate).format('YYYY-MM-DD')
    ) {
      await contractService.createLeaseChangeLog(
        {
          fieldName: 'nextCpiDate',
          contractInfo: contract,
          previousContract,
          userId
        },
        session
      )
    }
  }
  return { previewData, updateContractData }
}

export const getCreateableInvoices = async (params, session) => {
  const {
    contract,
    invoiceRanges,
    partnerSettings,
    enabledNotification,
    ignoreExistingInvoiceChecking,
    invoiceData,
    returnPreview,
    returnRegularInvoicePreview,
    testTodayDate,
    userId
  } = params
  const result = []
  let contractUpdateData = {}
  if (size(invoiceRanges)) {
    let count = 0
    const contractDueDays = contract.rentalMeta.dueDate
      ? contract.rentalMeta.dueDate
      : 1
    let today = testTodayDate ? new Date(testTodayDate) : null
    today = await appHelper.getActualDate(partnerSettings, false, today)
    let countInvoice = await invoiceCountOfAContract(contract._id, session)
    if (returnRegularInvoicePreview) {
      countInvoice = 0
    }
    for (const data of invoiceRanges) {
      const currentInvoiceStartOn = await appHelper.getActualDate(
        partnerSettings,
        false,
        data.invoiceStartOn
      )
      let currentInvoiceDueDate = (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          currentInvoiceStartOn
        )
      )
        .set('date', contractDueDays)
        .toDate()
      if (currentInvoiceDueDate < currentInvoiceStartOn) {
        currentInvoiceDueDate = currentInvoiceStartOn
      }
      const dueDateParams = {
        today,
        currentInvoiceDueDate,
        contract,
        data,
        returnPreview,
        partnerSettings
      }
      const { dueDate, ignoreRecurringDueDate } = await getDueDate(
        dueDateParams
      )
      invoiceData.dueDate = dueDate
      invoiceData.invoiceMonth = currentInvoiceStartOn
      if (count) {
        // Add corrections only on 1st invoice during preview.
        invoiceData.ignoreCorrections = true
      }
      const previewParams = {
        contract,
        manualInvoiceCreateOption: {
          startOn: data.invoiceStartOn,
          endOn: data.invoiceEndOn,
          isManualInvoice: true
        },
        enabledNotification,
        invoiceCountFromBeginning: countInvoice,
        ignoreExistingInvoiceChecking,
        ignoreRecurringDueDate,
        invoiceData,
        isFirstInvoice: data.isFirstInvoice,
        partnerSettings,
        returnPreview,
        today,
        userId
      }
      const { previewData, updateContractData } = await getPreviewData(
        previewParams,
        session
      )
      if (size(previewData) && previewData.invoiceTotal >= 0) {
        result.push(JSON.parse(JSON.stringify(previewData)))
        if (size(updateContractData)) {
          contractUpdateData = JSON.parse(JSON.stringify(updateContractData))
        }
        countInvoice++
        count++
      }
    }
  }
  return { result, contractUpdateData }
}

export const getInvoicesList = async (params, session) => {
  const { contract, partnerSettings, today, returnEstimatedPayoutPreview } =
    params
  const basicInvoiceData = await getBasicInvoiceDataForTenant(
    contract,
    today,
    false,
    true
  )
  const firstInvoiceCreationDate = await appHelper.subtractDays(
    contract.rentalMeta.firstInvoiceDueDate,
    partnerSettings.invoiceDueDays,
    partnerSettings
  )
  // Create invoice if today is the firstInvoiceCreationDate or past
  if (isDatePastOrToday(today, firstInvoiceCreationDate)) {
    if (partnerSettings.invoiceCalculation) {
      contract.rentalMeta.invoiceCalculation =
        partnerSettings.invoiceCalculation
    }
    if (returnEstimatedPayoutPreview) {
      partnerSettings.invoiceDueDays = 120
    }
    const creatableInvoiceParams = {
      today,
      contract: clone(contract),
      partnerSettings,
      preferredRanges: null,
      returnEstimatedPayoutPreview
    }
    const invoicesData = await getCreatableInvoicesByRange(
      creatableInvoiceParams,
      session
    )
    let createableInvoices = invoicesData.missingInvoices
    createableInvoices = sortBy(createableInvoices, ['invoiceStartOn'])
    const createableInvoicesParams = {
      contract,
      partnerSettings,
      isDemo: false,
      returnPreview: true,
      enabledNotification: false,
      invoiceData: basicInvoiceData,
      invoiceRanges: createableInvoices,
      returnRegularInvoicePreview: true,
      ignoreExistingInvoiceChecking: true,
      testTodayDate: today,
      returnEstimatedPayoutPreview
    }
    const currentInvoicesData = await getCreateableInvoices(
      createableInvoicesParams,
      session
    )
    const { result } = currentInvoicesData
    return result
  }
  return []
}

export const prepareQueryForInvoiceCommission = (invoiceData) => {
  const query = {
    partnerId: invoiceData.partnerId,
    propertyId: invoiceData.propertyId,
    accountId: invoiceData.accountId,
    agentId: invoiceData.agentId,
    branchId: invoiceData.branchId,
    _id: invoiceData.contractId,
    'rentalMeta.tenantId': invoiceData.tenantId
  }
  return query
}

export const getMonthlyRentTotal = (invoiceContent) => {
  let monthlyRentAmount = 0
  if (size(invoiceContent)) {
    const monthlyRentContent = invoiceContent.find(
      (content) => content.type === 'monthly_rent'
    )
    if (monthlyRentContent) monthlyRentAmount = monthlyRentContent.total
  }
  return monthlyRentAmount
}

export const addBrokeringCommission = async (params) => {
  const { propertyContractInfo, isEstimatedPayouts, monthlyRentAmount } = params
  const { invoiceData } = params
  const brokeringCommissionData = clone(invoiceData)
  const {
    hasBrokeringContract,
    brokeringCommissionAmount,
    brokeringCommissionType
  } = propertyContractInfo || {}
  if (!(hasBrokeringContract && brokeringCommissionAmount)) {
    return false
  }
  let updatedBrokeringCommissionAmount = 0
  if (brokeringCommissionType === 'fixed') {
    updatedBrokeringCommissionAmount = brokeringCommissionAmount
  } else if (brokeringCommissionType === 'percent') {
    updatedBrokeringCommissionAmount =
      monthlyRentAmount * (propertyContractInfo.brokeringCommissionAmount / 100)
  }
  brokeringCommissionData.type = 'brokering_contract'
  brokeringCommissionData.amount = await appHelper.convertTo2Decimal(
    updatedBrokeringCommissionAmount || 0
  )
  if (brokeringCommissionData.amount > 0) {
    if (isEstimatedPayouts) {
      return brokeringCommissionData.amount
    }
  }
}

export const getCorrectionsAddon = async (correctionId) => {
  const correctionInfo = await correctionHelper.getCorrectionById(correctionId)
  const { addons } = correctionInfo || {}
  if (size(addons)) {
    return addons
  }
  return false
}

export const getAddonCommissionTotalOrAddCommission = async (params) => {
  const { propertyContractInfo, addonsMetaData, invoiceAddonInfo } = params
  const { addonCommissionPercent, addonCommissionData, isEstimatedPayouts } =
    params
  const { rentalManagementCommissionAmount } = propertyContractInfo
  const findCommissionableAddon = find(
    addonsMetaData,
    (addonInfo) => addonInfo && addonInfo.addonId === invoiceAddonInfo.addonId
  )
  let { commissionTotal, addonCommissionTotal } = params
  if (size(findCommissionableAddon)) {
    const commissionAmount =
      addonCommissionPercent || rentalManagementCommissionAmount
    commissionTotal -= invoiceAddonInfo.total || 0
    const calculatedCommissionAmount =
      (invoiceAddonInfo.total || 0) * (commissionAmount / 100)

    addonCommissionData.type = 'addon_commission'
    addonCommissionData.amount = await appHelper.convertTo2Decimal(
      calculatedCommissionAmount || 0
    )
    addonCommissionData.addonId = invoiceAddonInfo.addonId
    if (addonCommissionData.amount !== 0) {
      if (isEstimatedPayouts) {
        addonCommissionTotal += addonCommissionData.amount
      }
    }
  }
  return {
    calculatedAddonCommissionTotal: addonCommissionTotal,
    calculatedCommissionTotal: commissionTotal
  }
}

export const addAddonCommission = async (params) => {
  const {
    propertyContractInfo,
    invoiceData,
    contractAddons,
    isEstimatedPayouts
  } = params
  let { commissionTotal } = params
  const { addonsMeta } = invoiceData
  let addonCommissionTotal = 0
  if (!(size(addonsMeta) && propertyContractInfo)) {
    return { addonCommissionTotal, commissionTotal }
  }
  for (const invoiceAddonInfo of addonsMeta) {
    const { addonCommissionPercent, isEnableAddonCommission } =
      (await commissionHelper.getAddonCommissionPercent(invoiceAddonInfo)) || {}
    if (
      isEnableAddonCommission &&
      commissionHelper.isPrepareDataByAddon(
        invoiceAddonInfo,
        propertyContractInfo,
        addonCommissionPercent
      )
    ) {
      const { correctionId } = invoiceAddonInfo
      let addonsMetaData = clone(contractAddons)
      const addonCommissionData = clone(invoiceData)
      if (correctionId) {
        addonsMetaData = await getCorrectionsAddon(correctionId)
      }
      if (size(addonsMetaData)) {
        const addonCommissionParams = {
          propertyContractInfo,
          addonsMetaData,
          commissionTotal,
          invoiceAddonInfo,
          isEstimatedPayouts,
          addonCommissionPercent,
          addonCommissionData,
          addonCommissionTotal
        }
        const { calculatedAddonCommissionTotal, calculatedCommissionTotal } =
          await getAddonCommissionTotalOrAddCommission(addonCommissionParams)
        addonCommissionTotal = calculatedAddonCommissionTotal
        commissionTotal = calculatedCommissionTotal
      }
    }
  }
  return { addonCommissionTotal, commissionTotal }
}

export const addManagementCommission = async (params) => {
  const {
    propertyContractInfo,
    rentalManagementCommissionData,
    invoiceCommissionableTotal,
    isEstimatedPayouts
  } = params
  const { rentalManagementCommissionType, rentalManagementCommissionAmount } =
    propertyContractInfo
  let rentalManagementCommission = 0
  if (rentalManagementCommissionType === 'fixed') {
    rentalManagementCommission = rentalManagementCommissionAmount || 0
  } else if (rentalManagementCommissionType === 'percent') {
    rentalManagementCommission = rentalManagementCommissionAmount
      ? invoiceCommissionableTotal * (rentalManagementCommissionAmount / 100)
      : 0
  }
  // Set rental management type
  rentalManagementCommissionData.type = 'rental_management_contract'
  rentalManagementCommissionData.amount = await appHelper.convertTo2Decimal(
    rentalManagementCommission || 0
  )
  if (rentalManagementCommissionData.amount > 0) {
    if (isEstimatedPayouts) {
      return rentalManagementCommissionData.amount
    }
  }
}

export const addAssignmentAddonIncome = async (
  invoiceData,
  contractAddonsMeta,
  isEstimatedPayouts
) => {
  if (!size(contractAddonsMeta)) {
    return false
  }
  let addonTotalAmount = 0
  for (const addonMetaInfo of contractAddonsMeta) {
    const { type } = addonMetaInfo
    if (type === 'assignment') {
      const addonIncomeData = clone(invoiceData)
      addonIncomeData.type = 'assignment_addon_income'
      addonIncomeData.amount = await appHelper.convertTo2Decimal(
        addonMetaInfo.total || 0
      )
      addonIncomeData.addonId = addonMetaInfo.addonId
      if (addonIncomeData.amount !== 0) {
        if (isEstimatedPayouts) {
          // Add to estimated payout contract
          addonTotalAmount += addonIncomeData.amount
        } else {
          await createCommission(addonIncomeData)
        }
      }
    }
  }
  if (isEstimatedPayouts) {
    return addonTotalAmount
  }
}

export const getManagementAndAddonCommission = async (params) => {
  const { contractAddons, invoiceData } = params
  const { propertyContractInfo, isEstimatedPayouts } = params
  const rentalManagementCommissionData = clone(invoiceData)
  let managementCommission = 0
  let assignmentAddonIncome = 0
  const { addonCommissionTotal, commissionTotal } = await addAddonCommission(
    params
  )

  const managementCommissionParams = {
    propertyContractInfo,
    rentalManagementCommissionData,
    invoiceCommissionableTotal: commissionTotal,
    isEstimatedPayouts
  }
  managementCommission = await addManagementCommission(
    managementCommissionParams
  )
  if (invoiceData.isFirstInvoice && size(contractAddons)) {
    assignmentAddonIncome = await addAssignmentAddonIncome(
      invoiceData,
      contractAddons,
      isEstimatedPayouts
    )
  }
  return { addonCommissionTotal, managementCommission, assignmentAddonIncome }
}

export const getCommissionsTotalObj = (params) => {
  const {
    addonCommissionTotal = 0,
    brokeringCommissionTotal = 0,
    managementCommission = 0,
    assignmentAddonIncome = 0
  } = params
  const result = {
    brokeringCommissionTotal,
    managementCommissionTotal: managementCommission,
    assignmentAddonTotal: assignmentAddonIncome,
    addonsCommissionTotal: addonCommissionTotal,
    total:
      brokeringCommissionTotal +
      addonCommissionTotal +
      managementCommission +
      assignmentAddonIncome
  }
  return result
}

export const createInvoiceCommission = async (
  invoiceData,
  isEstimatedPayouts,
  propertyContractInfo
) => {
  invoiceData.invoiceId = invoiceData._id
  if (!size(propertyContractInfo)) {
    const contractQuery = prepareQueryForInvoiceCommission(invoiceData)
    propertyContractInfo = await contractHelper.getAContract(contractQuery)
  }
  if (!size(propertyContractInfo)) {
    return false
  }
  const invoiceCommissionableTotal = invoiceData.commissionableTotal || 0
  const contractAddons =
    propertyContractInfo && size(propertyContractInfo.addons)
      ? propertyContractInfo.addons
      : []
  let brokeringCommissionTotal = 0
  if (invoiceData.isFirstInvoice) {
    const monthlyRentAmount = getMonthlyRentTotal(invoiceData.invoiceContent)
    const brokeringCommissionParams = {
      invoiceData,
      monthlyRentAmount,
      propertyContractInfo,
      isEstimatedPayouts
    }
    brokeringCommissionTotal = await addBrokeringCommission(
      brokeringCommissionParams
    )
  }
  const managementAndAddonCommissionParams = {
    invoiceData,
    contractAddons,
    commissionTotal: invoiceCommissionableTotal,
    propertyContractInfo,
    invoiceCommissionableTotal,
    isEstimatedPayouts
  }
  const { addonCommissionTotal, managementCommission, assignmentAddonIncome } =
    await getManagementAndAddonCommission(managementAndAddonCommissionParams)
  if (isEstimatedPayouts) {
    const commissionTotals = {
      addonCommissionTotal,
      brokeringCommissionTotal,
      managementCommission,
      assignmentAddonIncome
    }
    const result = getCommissionsTotalObj(commissionTotals)
    return result
  }
  return {}
}

export const calculateFirstEstimatedPayout = (
  invoiceCommissions,
  invoiceInfo,
  invoiceFees
) => {
  const invoiceCommissionTotal =
    invoiceCommissions && invoiceCommissions.total
      ? invoiceCommissions.total
      : 0
  const {
    managementCommissionTotal,
    addonsCommissionTotal,
    assignmentAddonTotal
  } = invoiceCommissions
  const payoutData = {}
  payoutData.firstMonthManagementCommission = managementCommissionTotal
  payoutData.firstMonthPayoutAddons = assignmentAddonTotal
  payoutData.firstMonthAddonsCommission = addonsCommissionTotal
  payoutData.firstRentInvoice = invoiceInfo.payoutableAmount
    ? invoiceInfo.payoutableAmount
    : 0
  payoutData.firstEstimatedPayout =
    invoiceInfo.invoiceTotal - invoiceCommissionTotal - invoiceFees
  return payoutData
}

export const calculateSecondEstimatedPayout = (
  invoiceCommissions,
  invoiceInfo,
  invoiceFees
) => {
  const invoiceCommissionTotal =
    invoiceCommissions && invoiceCommissions.total
      ? invoiceCommissions.total
      : 0
  const {
    managementCommissionTotal,
    addonsCommissionTotal,
    assignmentAddonTotal
  } = invoiceCommissions
  const payoutData = {}
  payoutData.secondEstimatedPayout =
    invoiceInfo.invoiceTotal - invoiceCommissionTotal - invoiceFees
  payoutData.secondMonthManagementCommission = managementCommissionTotal
  payoutData.secondMonthPayoutAddons = assignmentAddonTotal
  payoutData.secondMonthAddonsCommission = addonsCommissionTotal
  payoutData.secondRentInvoice = invoiceInfo.payoutableAmount
    ? invoiceInfo.payoutableAmount
    : 0
  return payoutData
}

export const calculateThirdEstimatedPayout = (
  invoiceCommissions,
  invoiceInfo,
  invoiceFees
) => {
  const invoiceCommissionTotal =
    invoiceCommissions && invoiceCommissions.total
      ? invoiceCommissions.total
      : 0
  const {
    managementCommissionTotal,
    addonsCommissionTotal,
    assignmentAddonTotal
  } = invoiceCommissions
  const payoutData = {}
  payoutData.thirdEstimatedPayout =
    invoiceInfo.invoiceTotal - invoiceCommissionTotal - invoiceFees
  payoutData.thirdMonthManagementCommission = managementCommissionTotal
  payoutData.thirdMonthPayoutAddons = assignmentAddonTotal
  payoutData.thirdMonthAddonsCommission = addonsCommissionTotal
  payoutData.thirdRentInvoice = invoiceInfo.payoutableAmount
    ? invoiceInfo.payoutableAmount
    : 0
  return payoutData
}

export const updateEstimatedPayoutObj = (estimatedPayouts) => {
  const updatedEstimatedPayout = estimatedPayouts
  const { firstEstimatedPayout } = estimatedPayouts
  let { secondEstimatedPayout, thirdEstimatedPayout } = estimatedPayouts
  if (firstEstimatedPayout < 0) {
    updatedEstimatedPayout.secondAmountMovedFromLastPayout =
      firstEstimatedPayout * -1
    secondEstimatedPayout += firstEstimatedPayout
    updatedEstimatedPayout.secondEstimatedPayout = secondEstimatedPayout
    updatedEstimatedPayout.firstEstimatedPayout = 0
  }
  if (secondEstimatedPayout < 0) {
    updatedEstimatedPayout.thirdAmountMovedFromLastPayout =
      secondEstimatedPayout * -1
    thirdEstimatedPayout += secondEstimatedPayout
    updatedEstimatedPayout.thirdEstimatedPayout = thirdEstimatedPayout
    updatedEstimatedPayout.secondEstimatedPayout = 0
  }
  if (thirdEstimatedPayout < 0) {
    updatedEstimatedPayout.thirdEstimatedPayout = 0
  }
  return updatedEstimatedPayout
}

export const calculateEstimatedPayouts = async (invoices) => {
  let estimatedPayouts = {}
  for (const [index, invoiceInfo] of invoices.entries()) {
    const invoiceCommissions = await createInvoiceCommission(invoiceInfo, true)
    const invoiceFees = getTotalInvoiceFees(invoiceInfo)
    if (!index) {
      const firstEstimatedPayoutData = calculateFirstEstimatedPayout(
        invoiceCommissions,
        invoiceInfo,
        invoiceFees
      )
      estimatedPayouts = assign(estimatedPayouts, firstEstimatedPayoutData)
    } else if (index === 1) {
      const secondEstimatedPayoutData = calculateSecondEstimatedPayout(
        invoiceCommissions,
        invoiceInfo,
        invoiceFees
      )
      estimatedPayouts = assign(estimatedPayouts, secondEstimatedPayoutData)
    } else if (index === 2) {
      const thirdEstimatedPayoutData = calculateThirdEstimatedPayout(
        invoiceCommissions,
        invoiceInfo,
        invoiceFees
      )
      estimatedPayouts = assign(estimatedPayouts, thirdEstimatedPayoutData)
    }
  }
  // If estimated payout less then 0 then calculation
  estimatedPayouts = updateEstimatedPayoutObj(estimatedPayouts)
  return estimatedPayouts
}

export const prepareUpdateDataForPayouts = async (estimatedPayouts) => {
  const { firstEstimatedPayout, secondEstimatedPayout, thirdEstimatedPayout } =
    estimatedPayouts
  const updateData = {}
  if (firstEstimatedPayout) {
    updateData['rentalMeta.estimatedPayouts.firstMonth'] =
      await appHelper.convertTo2Decimal(firstEstimatedPayout)
  }
  if (secondEstimatedPayout) {
    updateData['rentalMeta.estimatedPayouts.secondMonth'] =
      await appHelper.convertTo2Decimal(secondEstimatedPayout)
  }
  if (thirdEstimatedPayout) {
    updateData['rentalMeta.estimatedPayouts.thirdMonth'] =
      await appHelper.convertTo2Decimal(thirdEstimatedPayout)
  }
  return updateData
}

export const prepareUpdateDataForManagementCommissions = async (
  estimatedPayouts
) => {
  const { firstMonthManagementCommission, secondMonthManagementCommission } =
    estimatedPayouts
  const { thirdMonthManagementCommission } = estimatedPayouts
  const updateData = {}
  if (firstMonthManagementCommission) {
    updateData['rentalMeta.estimatedPayouts.firstMonthManagementCommission'] =
      await appHelper.convertTo2Decimal(firstMonthManagementCommission)
  }
  if (secondMonthManagementCommission) {
    updateData['rentalMeta.estimatedPayouts.secondMonthManagementCommission'] =
      await appHelper.convertTo2Decimal(secondMonthManagementCommission)
  }
  if (thirdMonthManagementCommission) {
    updateData['rentalMeta.estimatedPayouts.thirdMonthManagementCommission'] =
      await appHelper.convertTo2Decimal(thirdMonthManagementCommission)
  }
  return updateData
}

export const prepareUpdateDataFromLastPayouts = async (estimatedPayouts) => {
  const { secondAmountMovedFromLastPayout, thirdAmountMovedFromLastPayout } =
    estimatedPayouts
  const updateData = {}
  if (secondAmountMovedFromLastPayout) {
    updateData['rentalMeta.estimatedPayouts.secondAmountMovedFromLastPayout'] =
      await appHelper.convertTo2Decimal(secondAmountMovedFromLastPayout)
  }
  if (thirdAmountMovedFromLastPayout) {
    updateData['rentalMeta.estimatedPayouts.thirdAmountMovedFromLastPayout'] =
      await appHelper.convertTo2Decimal(thirdAmountMovedFromLastPayout)
  }
  return updateData
}

export const prepareUpdateDataForAddons = async (estimatedPayouts) => {
  const {
    firstMonthPayoutAddons,
    secondMonthPayoutAddons,
    thirdMonthPayoutAddons
  } = estimatedPayouts
  const {
    firstMonthAddonsCommission,
    secondMonthAddonsCommission,
    thirdMonthAddonsCommission
  } = estimatedPayouts
  const updateData = {}
  if (firstMonthPayoutAddons) {
    updateData['rentalMeta.estimatedPayouts.firstMonthPayoutAddons'] =
      await appHelper.convertTo2Decimal(firstMonthPayoutAddons)
  }
  if (secondMonthPayoutAddons) {
    updateData['rentalMeta.estimatedPayouts.secondMonthPayoutAddons'] =
      await appHelper.convertTo2Decimal(secondMonthPayoutAddons)
  }
  if (thirdMonthPayoutAddons) {
    updateData['rentalMeta.estimatedPayouts.thirdMonthPayoutAddons'] =
      await appHelper.convertTo2Decimal(thirdMonthPayoutAddons)
  }
  if (firstMonthAddonsCommission) {
    updateData['rentalMeta.estimatedPayouts.firstMonthAddonsCommission'] =
      await appHelper.convertTo2Decimal(firstMonthAddonsCommission)
  }
  if (secondMonthAddonsCommission) {
    updateData['rentalMeta.estimatedPayouts.secondMonthAddonsCommission'] =
      await appHelper.convertTo2Decimal(secondMonthAddonsCommission)
  }
  if (thirdMonthAddonsCommission) {
    updateData['rentalMeta.estimatedPayouts.thirdMonthAddonsCommission'] =
      await appHelper.convertTo2Decimal(thirdMonthAddonsCommission)
  }
  return updateData
}

export const prepareUpdateDataForRentInvoices = async (estimatedPayouts) => {
  const { firstRentInvoice, secondRentInvoice, thirdRentInvoice } =
    estimatedPayouts
  const updateData = {}
  if (firstRentInvoice) {
    updateData['rentalMeta.estimatedPayouts.firstRentInvoice'] =
      await appHelper.convertTo2Decimal(firstRentInvoice)
  }
  if (secondRentInvoice) {
    updateData['rentalMeta.estimatedPayouts.secondRentInvoice'] =
      await appHelper.convertTo2Decimal(secondRentInvoice)
  }
  if (thirdRentInvoice) {
    updateData['rentalMeta.estimatedPayouts.thirdRentInvoice'] =
      await appHelper.convertTo2Decimal(thirdRentInvoice)
  }
  return updateData
}

export const prepareContractUpdateDataForEstimatedPayout = async (invoices) => {
  let updateData = {}
  const estimatedPayouts = await calculateEstimatedPayouts(invoices)
  const updateDataForPayouts = await prepareUpdateDataForPayouts(
    estimatedPayouts
  )
  if (size(updateDataForPayouts)) {
    updateData = assign(updateData, updateDataForPayouts)
  }
  const managementCommissionUpdateData =
    await prepareUpdateDataForManagementCommissions(estimatedPayouts)
  if (size(managementCommissionUpdateData)) {
    updateData = assign(updateData, managementCommissionUpdateData)
  }
  const updateDataFromLastPayouts = await prepareUpdateDataFromLastPayouts(
    estimatedPayouts
  )
  if (size(updateDataFromLastPayouts)) {
    updateData = assign(updateData, updateDataFromLastPayouts)
  }
  const addonsUpdateData = await prepareUpdateDataForAddons(estimatedPayouts)
  if (size(addonsUpdateData)) {
    updateData = assign(updateData, addonsUpdateData)
  }
  const rentInvoiceUpdateData = await prepareUpdateDataForRentInvoices(
    estimatedPayouts
  )
  if (size(rentInvoiceUpdateData)) {
    updateData = assign(updateData, rentInvoiceUpdateData)
  }
  return updateData
}

export const getTotalInvoiceFees = (invoiceInfo) => {
  const { feesMeta } = invoiceInfo
  let invoiceFees = 0
  if (!size(feesMeta)) {
    return invoiceFees
  }
  each(feesMeta, (invoiceFee) => {
    invoiceFees += invoiceFee.total
  })
  return invoiceFees
}

export const isDatePastOrToday = (today, date) => !!(today >= date)

export const isDateFutureOrToday = (today, date) => !!(today <= date)

export const createCommission = async (commissionData) => {
  // Note: This is special case. we do not create anything by helper.
  const commission = await CommissionCollection.create(commissionData)
  return commission
}

export const getInvoiceTotalDays = async (data) => {
  const { invoice, partnerSetting } = data
  const endDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    invoice.invoiceEndOn
  )
  const startDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    invoice.invoiceStartOn
  )
  const totalDays = endDate.diff(startDate, 'days') + 1
  return totalDays
}

export const getCreditedDays = async (data) => {
  const { invoice, partnerSetting } = data
  let creditedDays = data.creditedDays || 0
  if (invoice.isPartiallyCredited && !creditedDays) {
    const creditNotes = await InvoiceCollection.find({
      _id: { $in: invoice.creditNoteIds }
    })
    for (const creditNote of creditNotes) {
      creditedDays += await getInvoiceTotalDays({
        partnerSetting,
        invoice: creditNote
      })
    }
    data.creditedDays = creditedDays
  }
  return creditedDays
}

export const getCreditableDaysByTerminationDate = async (
  data,
  terminatedDays = 0
) => {
  const { invoice, contract, partnerSetting, dateFormat } = data
  const { rentalMeta } = contract
  let { terminationDate } = data
  if (terminationDate) {
    terminationDate = await appHelper.getActualDate(
      partnerSetting,
      true,
      terminationDate
    )
    const startDate = await appHelper.getActualDate(
      partnerSetting,
      true,
      invoice.invoiceStartOn
    )
    const endDate = await appHelper.getActualDate(
      partnerSetting,
      true,
      invoice.invoiceEndOn
    )
    if (terminationDate.format(dateFormat) === startDate.format(dateFormat)) {
      if (rentalMeta && rentalMeta.creditWholeInvoice) {
        return 0
      }
      terminatedDays = endDate.diff(terminationDate, 'days')
    } else if (
      terminationDate.toDate() >= startDate.toDate() &&
      endDate.toDate() >= terminationDate.toDate()
    ) {
      terminatedDays = endDate.diff(terminationDate, 'days')
    }
  }
  return terminatedDays
}

export const getCreditableDays = async (data) => {
  const { invoice, invoiceTotalDays } = data
  let creditableDays = invoiceTotalDays
  if (invoice.isPartiallyCredited) {
    const terminatedDays = await getCreditableDaysByTerminationDate(data, 0)
    const creditedDays = await getCreditedDays(data)
    creditableDays -= creditedDays
    if (terminatedDays) {
      creditableDays = terminatedDays - creditedDays
    }
    return creditableDays < 0 ? 0 : creditableDays
  }
  return await getCreditableDaysByTerminationDate(data, invoiceTotalDays)
}

export const getCreditableRent = async (data) => {
  const { invoice, invoiceTotalDays, creditableDays, isCreditFull } = data
  console.log('Checking for data to prepare credit rent: ', data)
  const findMonthlyRent = find(
    invoice.invoiceContent,
    (invoiceContent) => invoiceContent.type === 'monthly_rent'
  )
  let totalRent =
    findMonthlyRent && findMonthlyRent.price ? findMonthlyRent.price : 0
  console.log('Checking for totalRent: ', totalRent)
  console.log(
    'Checking for creditableDays: ',
    creditableDays,
    ' isCreditFull: ',
    isCreditFull
  )
  if (totalRent && !isCreditFull && creditableDays) {
    totalRent = (totalRent / invoiceTotalDays) * creditableDays
  }
  console.log('Checking for totalRent after calc: ', totalRent)
  totalRent = await appHelper.convertTo2Decimal(totalRent)
  console.log('Checking for totalRent after convertTo2Decimal: ', totalRent)
  return totalRent
}

export const getCreditableFees = (data) => {
  const { invoice, isCreditFull, isCreditFullByPartiallyCredited } = data
  let creditableFees = []
  let feeTotal = 0
  let feeTaxTotal = 0
  if (isCreditFull || isCreditFullByPartiallyCredited) {
    creditableFees = map(invoice.feesMeta, (feeMeta = {}) => {
      const newFeeMeta = pick(feeMeta, [
        'type',
        'qty',
        'amount',
        'tax',
        'total',
        'original',
        'isPaid'
      ])
      newFeeMeta.qty *= -1
      newFeeMeta.total = (newFeeMeta.total || 0) * -1
      feeTotal += newFeeMeta.total
      feeTaxTotal += newFeeMeta.tax || 0
      return newFeeMeta
    })
  }
  return {
    feeTotal,
    creditableFees,
    feeTaxTotal
  }
}

export const prepareAddonForFullCredit = (addonMeta) => {
  addonMeta.qty *= -1
  addonMeta.total *= addonMeta.qty
}

export const prepareAddonForPartiallyCredit = async (
  addonMeta,
  invoiceTotalDays,
  creditableDays
) => {
  const addonItemQty = addonMeta.qty * -1
  const addonItemPrice =
    ((addonMeta.total ? addonMeta.total : addonMeta.price) / invoiceTotalDays) *
    creditableDays
  let addonItemTotal = addonItemPrice
  addonItemTotal *= addonItemQty
  const updatedAddon = {
    type: addonMeta.type,
    description: addonMeta.description,
    qty: addonItemQty,
    taxPercentage: addonMeta.taxPercentage,
    price: await appHelper.convertTo2Decimal(addonItemPrice),
    total: await appHelper.convertTo2Decimal(addonItemTotal),
    addonId: addonMeta.addonId
  }
  if (addonMeta.correctionId) {
    updatedAddon.correctionId = addonMeta.correctionId
  }
  return updatedAddon
}

export const getCreditableAddons = async (data) => {
  const {
    invoice,
    contract,
    invoiceTotalDays,
    creditableDays,
    isCreditFull,
    isCreditFullByPartiallyCredited
  } = data
  const creditableAddons = []
  let addonsTotal = 0
  const leaseAddons = contract.addons
  if (size(invoice.addonsMeta)) {
    for (const addonMeta of invoice.addonsMeta) {
      delete addonMeta.payouts
      delete addonMeta.payoutsIds
      delete addonMeta.totalBalanced
      if (isCreditFull) {
        prepareAddonForFullCredit(addonMeta)
        addonsTotal += addonMeta.total
        creditableAddons.push(addonMeta)
      } else if (creditableDays) {
        const leaseAddon = find(
          leaseAddons,
          (addonInfo) =>
            addonInfo.isRecurring && addonInfo.addonId === addonMeta.addonId
        )
        if (leaseAddon && addonMeta.price) {
          const updatedAddon = await prepareAddonForPartiallyCredit(
            addonMeta,
            invoiceTotalDays,
            creditableDays
          )
          addonsTotal += updatedAddon.total
          creditableAddons.push(updatedAddon)
        } else if (isCreditFullByPartiallyCredited) {
          // Need clarification
          prepareAddonForFullCredit(addonMeta)
          addonsTotal += addonMeta.total
          creditableAddons.push(addonMeta)
        }
      }
    }
  }
  addonsTotal = await appHelper.convertTo2Decimal(addonsTotal)
  return { addonsTotal, creditableAddons }
}

export const isCreditFullLease = async (data) => {
  const { contract, partnerSetting, dateFormat } = data
  const { rentalMeta } = contract
  let { terminationDate } = data
  if (!rentalMeta || !terminationDate) {
    return false
  }
  const contractStartDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    rentalMeta.contractStartDate
  )
  terminationDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    terminationDate
  )
  if (
    terminationDate.format(dateFormat) ===
      contractStartDate.format(dateFormat) &&
    rentalMeta.creditWholeInvoice
  ) {
    return true
  }
  return false
}

export const isCreditFullInvoice = async (data) => {
  const { creditableDays, invoiceTotalDays } = data
  const _isCreditFullLease = await isCreditFullLease(data)
  if (creditableDays === invoiceTotalDays || _isCreditFullLease) {
    return true
  }
  return false
}

export const isCreditFullByPartiallyCredited = async (data) => {
  const { invoice, invoiceTotalDays, creditableDays } = data
  let creditFullInvoice = false
  if (invoice.isPartiallyCredited && creditableDays) {
    const creditedDays = await getCreditedDays(data)
    if (invoiceTotalDays === creditedDays + creditableDays) {
      creditFullInvoice = true
    }
  }
  return creditFullInvoice
}

export const getCreditDateRange = async (data) => {
  const {
    invoice,
    partnerSetting,
    creditableDays,
    terminationDate,
    isCreditFull
  } = data
  const dateRange = { invoiceEndOn: invoice.invoiceEndOn }
  if (isCreditFull) {
    dateRange.invoiceStartOn = await appHelper.getActualDate(
      partnerSetting,
      false,
      invoice.invoiceStartOn
    )
  } else {
    dateRange.invoiceStartOn = (
      await appHelper.getActualDate(partnerSetting, true, terminationDate)
    )
      .add(1, 'days')
      .toDate()
  }
  if (invoice.isPartiallyCredited) {
    const partiallyCreditedInvoice = await InvoiceCollection.findOne({
      _id: { $in: invoice.creditNoteIds }
    }).sort({ invoiceStartOn: 1 })
    dateRange.invoiceStartOn = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        partiallyCreditedInvoice.invoiceStartOn
      )
    )
      .subtract(creditableDays, 'days')
      .toDate()
    dateRange.invoiceEndOn = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        partiallyCreditedInvoice.invoiceStartOn
      )
    )
      .subtract(1, 'days')
      .toDate()
  }
  return dateRange
}

export const adjustRoundedLost = async (data, invoiceData) => {
  const {
    feeTotal,
    monthlyRentAmount,
    addonTotalAmount,
    isCreditFullByPartiallyCredited,
    partnerSetting
  } = data
  const { invoiceTotal, creditedAmount } = data.invoice
  if (isCreditFullByPartiallyCredited) {
    let epsRoundDiff =
      invoiceTotal +
        ((creditedAmount || 0) +
          monthlyRentAmount +
          feeTotal +
          addonTotalAmount) || 0
    epsRoundDiff = await appHelper.convertTo2Decimal(
      epsRoundDiff,
      partnerSetting,
      'round'
    )
    if (epsRoundDiff !== 0) {
      invoiceData.invoiceTotal -= epsRoundDiff
      invoiceData.rentTotal -= epsRoundDiff
    }
  }
}

export const prepareCreditNoteData = async (data) => {
  const { invoice, monthlyRent, fees, addons, creditReason } = data
  console.log('Found prepared monthlyRent: ', monthlyRent)
  data.feeTaxTotal = 0
  data.invoiceCommissionableTotal = 0
  data.feeTotal = fees.feeTotal ? fees.feeTotal : 0
  data.addonTotalAmount = addons.addonsTotal ? addons.addonsTotal : 0
  data.monthlyRentAmount = (monthlyRent || 0) * -1
  const creditNoteData = pick(invoice, [
    'contractId',
    'partnerId',
    'tenantId',
    'accountId',
    'propertyId',
    'agentId',
    'branchId',
    'sender',
    'receiver',
    'invoiceMonth',
    'invoiceAccountNumber',
    'tenants'
  ])
  if (invoice.isNonRentInvoice) {
    creditNoteData.isNonRentInvoice = true
  }
  if (creditReason) {
    creditNoteData.creditReason = creditReason
  }
  const { invoiceStartOn, invoiceEndOn } = await getCreditDateRange(data)
  creditNoteData.status = 'new'
  creditNoteData.invoiceType = 'credit_note'
  creditNoteData.invoiceId = invoice._id
  creditNoteData.invoiceStartOn = invoiceStartOn
  creditNoteData.invoiceEndOn = invoiceEndOn
  creditNoteData.fullyCredited = !!(
    data.isCreditFull || data.isCreditFullByPartiallyCredited
  )
  console.log(
    'Checking for creditNoteData.fullyCredited: ',
    creditNoteData.fullyCredited
  )
  creditNoteData.dueDate = invoice.dueDate
  if (size(fees.creditableFees)) {
    creditNoteData.feesMeta = fees.creditableFees
  }
  if (size(addons.creditableAddons)) {
    creditNoteData.addonsMeta = addons.creditableAddons
  }
  const monthlyRentInfo = find(
    invoice.invoiceContent,
    (invoiceContent) => invoiceContent.type === 'monthly_rent'
  )
  console.log('Checking for monthlyRentInfo: ', monthlyRentInfo)
  console.log('Checking for data.monthlyRentAmount: ', data.monthlyRentAmount)
  if (monthlyRentInfo) {
    creditNoteData.invoiceContent = [
      {
        type: 'monthly_rent',
        qty: -1,
        price: monthlyRent,
        total: data.monthlyRentAmount,
        taxPercentage: monthlyRentInfo.taxPercentage || 0
      }
    ]
  }
  return creditNoteData
}

export const getCreditedAmount = async (creditedInvoice, creditNote) => {
  const creditedAmount = await appHelper.convertTo2Decimal(
    (creditedInvoice.creditedAmount || 0) + creditNote.invoiceTotal
  )
  return creditedAmount
}

export const getRemainingAmount = (invoice) => {
  const { invoiceTotal, totalPaid, creditedAmount } = invoice
  return totalPaid - (invoiceTotal + creditedAmount)
}

export const getCreditableBrokeringCommission = async (
  invoice,
  isCreditFull
) => {
  const commission = isCreditFull
    ? await commissionHelper.getCommission({})
    : false
  return commission
}

export const getIdsAndType = (data) => {
  const fields = pick(data, [
    'agentId',
    'branchId',
    'partnerId',
    'accountId',
    'propertyId',
    'tenantId',
    'type'
  ])
  return fields
}

export const getNonRentCorrections = async (contract, session) => {
  const query = {
    partnerId: contract.partnerId,
    propertyId: contract.propertyId,
    contractId: contract._id,
    invoiceId: { $exists: false },
    addTo: 'rent_invoice',
    correctionStatus: 'active',
    isNonRent: true // Find only non rent corrections
  }
  const corrections = await correctionHelper.getCorrections(query, session)
  return corrections
}

export const getLandLord = async (contract) => {
  const landlord = await accountHelper.getAnAccount(
    { _id: contract.accountId },
    null,
    ['person', 'organization']
  )
  if (!landlord) {
    throw new CustomError(
      404,
      `Could not find landlord! accountId is not valid. contractId: ${contract._id}`
    )
  }
  return landlord
}

export const getLandLordInvoiceOrCreditNote = async (
  landlordCreditNoteQuery,
  session
) => {
  const landlordCreditNote = await InvoiceCollection.findOne(
    landlordCreditNoteQuery
  )
    .sort({ createdAt: -1 })
    .session(session)
  return landlordCreditNote
}

export const getInvoiceDataForLandlordInvoice = async (contract) => {
  const invoiceData = await getBasicInvoiceMakingData(contract)
  const landlord = await getLandLord(contract)
  invoiceData.receiver = {
    landlordName: landlord.name || ''
  }
  invoiceData.invoiceType = 'landlord_invoice'
  invoiceData.isPayable = false
  return invoiceData
}

export const getTaxPercentageBasedOnCommissionType = async (
  commissionType,
  partnerId,
  addonId
) => {
  let taxPercentage = 0
  if (
    (commissionType === 'assignment_addon_income' ||
      commissionType === 'addon_commission') &&
    addonId
  ) {
    const addonInfo = await addonHelper.getAddonById(addonId)
    if (addonInfo && addonInfo.creditAccountId) {
      taxPercentage = await getTaxPercentageBasedOnCreditAccountId(
        addonInfo.creditAccountId
      )
    }
  } else {
    const allTypes = {
      brokering_contract: 'brokering_commission',
      rental_management_contract: 'management_commission',
      addon_commission: 'addon_commission'
    }
    const accountingType = allTypes[commissionType]
    taxPercentage = await getTaxPercentageBasedOnAccountingType(
      accountingType,
      partnerId
    )
  }
  return taxPercentage
}

export const getCorrectionInvoiceData = async (params) => {
  const {
    correctionId,
    isDemo,
    returnPreview,
    enabledNotification = false,
    data,
    isPendingCorrectionInvoice
  } = params
  let { contract } = params
  if (!size(contract)) {
    contract = await contractHelper.getContractById(params.contractId)
  }
  if (!size(contract)) {
    throw new CustomError(400, 'ContractId or Contract is required!')
  }
  const correctionInvoiceData = {
    contract,
    correctionId,
    isDemo,
    returnPreview,
    isPendingCorrectionInvoice
  }
  const initialDataForNewInvoice = await getInitialDataForCorrectionInvoice(
    correctionInvoiceData
  )
  const { partnerSetting, today, invoiceData, correctionData } =
    initialDataForNewInvoice
  const creatingNewInvoiceData = {
    contract,
    invoiceData: clone(invoiceData),
    correctionData: clone(correctionData),
    partnerSetting,
    today,
    isDemo,
    returnPreview,
    enabledNotification,
    data
  }
  const preparedData = await prepareInvoiceData(creatingNewInvoiceData)
  return preparedData
}

export const getCorrectionInvoicePreview = async (params, session) => {
  const preparedData = await getCorrectionInvoiceData(params)
  const correction = await processInvoiceDataBeforeCreation(
    preparedData,
    session
  )
  return [correction]
}

export const prepareContractQueryForMissingInvoice = (body) => {
  const { contractId, propertyId, partnerId } = body
  return {
    _id: contractId,
    propertyId,
    partnerId,
    $or: [
      { status: { $ne: 'closed' } },
      {
        status: 'closed',
        finalSettlementStatus: { $nin: ['in_progress', 'completed'] }
      }
    ]
  }
}

export const getContractInfo = async (body) => {
  const query = prepareContractQueryForMissingInvoice(body)
  const contractInfo = await contractHelper.getAContract(query)
  return contractInfo
}

export const isShowMissingInvoicesBasedOnContract = async (contractInfo) => {
  const { _id } = contractInfo
  const isFirstInvoiceForThisContract = await isFirstInvoiceOfAContract(_id)
  if (!isFirstInvoiceForThisContract) {
    return true
  }
  const isCreateFirstInvoice = isFirstInvoiceAllowed(contractInfo)
  if (isFirstInvoiceForThisContract && isCreateFirstInvoice) {
    return true
  }
  return false
}

export const isShowMissingInvoice = async (body) => {
  const contractInfo = await getContractInfo(body)
  const { partnerId } = body
  const partnerSettings = partnerSettingHelper.getSettingByPartnerId(partnerId)
  if (!contractInfo || !partnerSettings) {
    return false
  }
  const _isShowMissingInvoicesBasedOnContract =
    await isShowMissingInvoicesBasedOnContract(contractInfo, partnerSettings)
  const { rentalMeta } = contractInfo
  const { tenantId } = rentalMeta
  return tenantId && _isShowMissingInvoicesBasedOnContract
}

export const prepareStringDate = async (date, partnerSettings) => {
  const actualDate = await appHelper
    .getActualDate(partnerSettings, true, date)
    .toDate()
  return actualDate
}

export const getPreferredRangesForMissingInvoice = async (body) => {
  const { filteredInvoices, partnerId } = body
  const preferredRanges = []
  if (filteredInvoices && size(filteredInvoices)) {
    const partnerSettings =
      partnerSettingHelper.getSettingByPartnerId(partnerId)
    for (const invoice of filteredInvoices) {
      const invoiceStartOn = await prepareStringDate(
        invoice.invoiceStartOn,
        partnerSettings
      )
      const invoiceEndOn = await prepareStringDate(
        invoice.invoiceEndOn,
        partnerSettings
      )
      if (invoiceStartOn && invoiceEndOn) {
        preferredRanges.push({ invoiceStartOn, invoiceEndOn })
      }
    }
  }
  return preferredRanges
}

export const createManualInvoices = async (params) => {
  const { contract, invoiceRanges, returnPreview, enabledNotification } = params
  const { partnerId } = contract
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const today = await getInvoiceDate(new Date(), partnerSettings)
  const invoiceData = await getBasicInvoiceDataForTenant(contract, today, false)
  const { rentalMeta } = contract
  const { firstInvoiceDueDate } = rentalMeta
  const { invoiceDueDays } = partnerSettings
  const firstInvoiceCreationDate = await appHelper.subtractDays(
    firstInvoiceDueDate,
    invoiceDueDays,
    partnerSettings
  )
  const ignoreExistingInvoiceChecking = true
  const returnRegularInvoicePreview = false
  const isDemo = false
  // We'll only create invoice if today is the firstInvoiceCreationDate or past
  if (isDatePastOrToday(today, firstInvoiceCreationDate)) {
    const creatableInvoiceParams = {
      contract,
      invoiceRanges,
      partnerSettings,
      invoiceData,
      isDemo,
      returnPreview,
      enabledNotification,
      ignoreExistingInvoiceChecking,
      returnRegularInvoicePreview
    }
    const invoicesData = await getCreateableInvoices(creatableInvoiceParams)
    return invoicesData.result
  }
}

export const prepareMissingInvoicesForManualInvoices = async (body) => {
  const { contractId, partnerId } = body
  const contractInfo = await contractHelper.getContractById(contractId)
  if (!size(contractInfo)) {
    throw new CustomError(
      404,
      `Contract info not found for contract: ${contractId}`
    )
  }
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  // Only required if anyone remove the invoices, then to show re-calculated invoices
  // Specially when there are corrections on the top invoices, it'll be forward to next if anyone remove the top invoices.
  const preferredRanges = await getPreferredRangesForMissingInvoice(body)
  const creatableInvoiceParams = {
    contract: clone(contractInfo),
    partnerSettings,
    preferredRanges,
    returnEstimatedPayoutPreview: false
  }
  const invoiceData = await getCreatableInvoicesByRange(creatableInvoiceParams)
  const { missingInvoices } = invoiceData
  const manualInvoiceParams = {
    contract: contractInfo,
    invoiceRanges: missingInvoices,
    returnPreview: true
  }
  const manualInvoices = await createManualInvoices(manualInvoiceParams)
  return manualInvoices
}

export const getMissingInvoicesForManualInvoice = async (req) => {
  const { body } = req
  const _isShowMissingInvoices = await isShowMissingInvoice(body)
  if (_isShowMissingInvoices) {
    const invoices = await prepareMissingInvoicesForManualInvoices(body)
    return invoices
  }
  return []
}

export const isAddCommissionChange = (invoice) =>
  invoice.invoiceType === 'invoice' && invoice.isFirstInvoice

export const isUpdateCommissionChangeHistory = (invoice) =>
  invoice.invoiceType === 'invoice' && invoice.isFirstInvoice

export const getLastPaidInvoice = async (query, session) => {
  const invoice = await InvoiceCollection.findOne(query)
    .session(session)
    .sort({ lastPaymentDate: -1 })
  return invoice
}

export const getMissingPayoutId = async (
  landlordInvoiceId,
  contractId,
  session
) => {
  const query = {
    invoiceType: 'credit_note',
    landlordInvoiceId,
    contractId
  }
  let invoice = await getInvoice(query, session)
  let invoiceId = invoice && invoice.invoiceId ? invoice.invoiceId : ''
  let payout = ''
  if (!invoiceId) {
    query.invoiceType = 'invoice'
    invoice = await getInvoice(query, session)
  }
  if (!invoiceId && invoice) {
    invoiceId = invoice._id
  }
  if (invoiceId) {
    payout = await payoutHelper.getPayout({ invoiceId, contractId }, session)
  }
  if (!payout) {
    payout = await payoutHelper.getLastPayout(
      { contractId },
      { createdAt: -1, serialId: -1 },
      session
    )
  }
  const payoutId = payout && payout._id ? payout._id : ''
  return payoutId
}

export const getMissingPayoutFromMetaArray = async (data, session) => {
  const { metaArray, contractId, landlordInvoiceId } = data
  let missingPayoutId = ''
  let isMissingPayoutId = false
  let totalMissingPositiveBalance = 0
  let totalMissingNegativeBalance = 0
  let adjustedPayoutId = ''
  each(metaArray, (meta) => {
    const newRemaining = (meta.total - (meta.totalBalanced || 0)) * 1
    const [newPayoutId] = compact(meta.payoutsIds || [])
    if (!adjustedPayoutId && newPayoutId) adjustedPayoutId = newPayoutId

    if (newRemaining > 0) {
      isMissingPayoutId = true
      totalMissingPositiveBalance += newRemaining
    } else if (newRemaining < 0) {
      isMissingPayoutId = true
      totalMissingNegativeBalance += newRemaining
    }
    if (!missingPayoutId && newPayoutId && isMissingPayoutId)
      missingPayoutId = newPayoutId
  })
  if (!missingPayoutId && isMissingPayoutId && adjustedPayoutId)
    missingPayoutId = adjustedPayoutId

  totalMissingNegativeBalance = Math.abs(totalMissingNegativeBalance || 0)
  if (
    !missingPayoutId &&
    isMissingPayoutId &&
    contractId &&
    landlordInvoiceId
  ) {
    missingPayoutId = await getMissingPayoutId(
      landlordInvoiceId,
      contractId,
      session
    )
  }
  totalMissingPositiveBalance = await appHelper.convertTo2Decimal(
    totalMissingPositiveBalance
  )
  totalMissingNegativeBalance = await appHelper.convertTo2Decimal(
    totalMissingNegativeBalance
  )
  // If remaining positive total and remaining negative total is not equal then
  // Don't adjust remaining commissions/addons
  if (
    (missingPayoutId || isMissingPayoutId) &&
    totalMissingPositiveBalance &&
    totalMissingPositiveBalance !== totalMissingNegativeBalance
  ) {
    missingPayoutId = ''
    isMissingPayoutId = false
  }
  return { missingPayoutId, isMissingPayoutId }
}

export const getAdjustedCommissionsMetaOrAddonsMeta = async (
  params,
  session
) => {
  const { metaArray } = params
  let isAdjusted = false
  const missingPayout = await getMissingPayoutFromMetaArray(params, session)
  const adjustablePayoutId = missingPayout?.missingPayoutId || ''
  const isMissingPayoutId = missingPayout?.isMissingPayoutId || false
  for (const meta of metaArray) {
    const newRemaining = (meta.total - (meta.totalBalanced || 0)) * 1
    const newPayouts = size(meta.payouts) ? meta.payouts : []
    let newPayoutsIds = size(meta.payoutsIds) ? meta.payoutsIds : []
    if (adjustablePayoutId && isMissingPayoutId && newRemaining !== 0) {
      newPayouts.push({
        payoutId: adjustablePayoutId,
        amount: await appHelper.convertTo2Decimal(newRemaining),
        isAdjustedBalance: true
      })
      meta.payouts = clone(newPayouts)
      newPayoutsIds.push(adjustablePayoutId)
      newPayoutsIds = uniq(newPayoutsIds)
      meta.payoutsIds = newPayoutsIds
      meta.totalBalanced = (meta.totalBalanced || 0) + (newRemaining || 0)
      isAdjusted = true
    }
  }
  return isAdjusted ? metaArray : []
}

export const getEvictionNoticeDueDate = async (partnerId, session) => {
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId,
    session
  )
  const { evictionNotice = {} } = partnerSetting || {}
  const evictionNoticeDays = evictionNotice.days || 0
  const evictionDueDateMoment = await appHelper.getActualDate(
    partnerSetting,
    true
  )
  const evictionDueDate = evictionDueDateMoment
    .subtract(evictionNoticeDays, 'days')
    .toDate()
  return evictionDueDate
}

export const getPreviousInvoiceWithEviction = async (data) => {
  const { contractId, partnerId } = data
  const query = {
    partnerId,
    contractId,
    invoiceType: 'invoice',
    status: { $nin: ['paid', 'credited', 'lost'] },
    $or: [
      { evictionNoticeSent: true, evictionNoticeSentOn: { $exists: true } },
      {
        evictionDueReminderSent: true,
        evictionDueReminderNoticeSentOn: { $exists: true }
      }
    ]
  }
  const [invoiceWithEviction] = await getInvoices(query, null, {
    limit: 1,
    sort: { createdAt: 1 }
  })
  return invoiceWithEviction
}

export const getInvoiceEvictionInfo = async (params) => {
  const invoice = await getPreviousInvoiceWithEviction(params)
  if (!size(invoice)) return {}

  const {
    evictionNoticeSent,
    evictionNoticeSentOn,
    evictionDueReminderSent,
    evictionDueReminderNoticeSentOn
  } = invoice

  const evictionTags = {}
  if (evictionNoticeSent) evictionTags.evictionNoticeSent = evictionNoticeSent
  if (evictionNoticeSentOn)
    evictionTags.evictionNoticeSentOn = evictionNoticeSentOn
  if (evictionDueReminderSent)
    evictionTags.evictionDueReminderSent = evictionDueReminderSent
  if (evictionDueReminderNoticeSentOn)
    evictionTags.evictionDueReminderNoticeSentOn =
      evictionDueReminderNoticeSentOn

  return evictionTags
}

export const getMaxInvoiceSerial = async (partnerId) => {
  const maxInvoiceSerial = await InvoiceCollection.aggregate([
    { $match: { partnerId } },
    { $group: { _id: null, invoiceSerialId: { $max: '$invoiceSerialId' } } }
  ])
  return maxInvoiceSerial
}

export const getMaxFinalSettlementInvoiceSerial = async (partnerId) => {
  const maxFinalSettlementInvoiceSerial = await InvoiceCollection.aggregate([
    { $match: { isFinalSettlement: true, partnerId } },
    { $group: { _id: null, invoiceSerialId: { $max: '$invoiceSerialId' } } }
  ])
  return maxFinalSettlementInvoiceSerial
}

export const isFinalSettlementInvoice = async (invoiceId, session) => {
  const query = { _id: invoiceId, isPayable: true, isFinalSettlement: true }
  const isFinalSettlementInvoice = await getInvoice(query, session)
  return !!isFinalSettlementInvoice
}

export const pickTransactionDataFromInvoice = (invoice) =>
  pick(invoice, [
    'partnerId',
    'contractId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'tenantId',
    'landlordInvoiceId',
    'createdBy'
  ])

export const prepareBasicTransactionDataForLandLordInvoice = (
  landLordInvoice,
  transactionEvent
) => {
  const transactionData = pickTransactionDataFromInvoice(landLordInvoice)
  const { _id = '', invoiceType = '', createdAt } = landLordInvoice
  let type = 'invoice'
  if (invoiceType === 'landlord_credit_note') {
    type = 'credit_note'
  }
  transactionData.invoiceId = _id
  transactionData.type = type
  if (transactionEvent === 'legacy') {
    transactionData.createdAt = createdAt
    transactionData.transactionEvent = transactionEvent
  }
  return { transactionData, type }
}

export const getAccountingType = (type) => {
  const accounting = {
    invoice: 'invoice_fee',
    reminder: 'invoice_reminder_fee',
    collection_notice: 'collection_notice_fee'
  }
  if (accounting[type]) {
    return accounting[type]
  }
  return ''
}

export const prepareTransactionCreatedAt = async (params) => {
  const { type, landlordInvoice, transactionEvent } = params
  const { firstReminderSentAt, secondReminderSentAt, evictionNoticeSentOn } =
    landlordInvoice
  let createdAt = ''
  if (transactionEvent === 'legacy' && type !== 'credit_note') {
    if (type === 'reminder') {
      const event = await transactionHelper.getReminderFeeEvent(
        clone(landlordInvoice)
      )
      if (event === 'send_landlord_first_reminder')
        createdAt = firstReminderSentAt
      else if (event === 'send_landlord_second_reminder')
        createdAt = secondReminderSentAt
    }
    if (
      type === 'eviction_notice' ||
      type === 'administration_eviction_notice'
    ) {
      createdAt = evictionNoticeSentOn
    }
  }
  return createdAt
}

export const getInvoiceInfoForFileId = async (
  invoiceId,
  isAppInvoice,
  session
) => {
  const query = { _id: invoiceId }
  let invoiceInfo
  if (isAppInvoice) {
    invoiceInfo = await appInvoiceHelper.getAppInvoice(query, session)
  } else {
    invoiceInfo = await getInvoice(query, session)
  }
  return invoiceInfo
}

export const getSenderInfo = async (
  partnerSettings,
  accountId,
  invoiceAccountNumber
) => {
  let name = '',
    address = '',
    zipCode = '',
    city = '',
    country = '',
    orgId = ''

  if (!size(partnerSettings))
    return { name, address, zipCode, city, country, orgId }

  const { partnerId = '' } = partnerSettings

  const isDirectPartner = !!(await partnerHelper.getDirectPartnerById(
    partnerId
  ))

  if (isDirectPartner) {
    const accountInfo =
      (await accountHelper.getAnAccount({ _id: accountId, partnerId }, null, [
        'organization'
      ])) || {}

    if (size(accountInfo)) {
      const { organization = {} } = accountInfo

      address = accountInfo.address || ''
      zipCode = accountInfo.zipCode || ''
      city = accountInfo.city || ''
      country = accountInfo.country || ''

      name = organization.name || ''
      orgId = organization.orgId || ''
    }
  } else {
    const bankAccountInfo = getInvoiceBankAccountInfo(
      partnerSettings,
      invoiceAccountNumber
    )
    if (size(bankAccountInfo) && bankAccountInfo.orgName) {
      name = bankAccountInfo.orgName
      address = bankAccountInfo.orgAddress || ''
      zipCode = bankAccountInfo.orgZipCode || ''
      city = bankAccountInfo.orgCity || ''
      country = bankAccountInfo.orgCountry || ''
      orgId = bankAccountInfo.orgId || ''
    } else {
      const { companyInfo = {} } = partnerSettings
      name = companyInfo.companyName || ''
      address = companyInfo.postalAddress || ''
      zipCode = companyInfo.postalZipCode || ''
      city = companyInfo.postalCity || ''
      country = companyInfo.postalCountry || ''
      orgId = companyInfo.organizationId || ''
    }
  }

  return { name, address, zipCode, city, country, orgId }
}

export const setDueDateRangeInQueryForExportData = async (
  params,
  partnerSetting
) => {
  const invoiceMainQuery = []

  //Set createdDate range filters in query
  if (
    params.createdDateRange &&
    params.createdDateRange.startDate &&
    params.createdDateRange.endDate
  ) {
    invoiceMainQuery.push({
      createdAt: {
        $gte: params.createdDateRange.startDate,
        $lte: params.createdDateRange.endDate
      }
    })
  }
  if (
    params.createdDateRange &&
    params.createdDateRange.startDate_string &&
    params.createdDateRange.endDate_string
  ) {
    const startDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        params.createdDateRange.startDate_string
      )
    )
      .startOf('day')
      .toDate()

    const endDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        params.createdDateRange.endDate_string
      )
    )
      .endOf('day')
      .toDate()

    if (startDate && endDate) {
      invoiceMainQuery.push({
        dueDate: { $gte: startDate, $lte: endDate }
      })
    }
  }
  //set DueDate range filters in query
  if (
    params.dueDateRange &&
    params.dueDateRange.startDate &&
    params.dueDateRange.endDate
  ) {
    invoiceMainQuery.push({
      dueDate: {
        $gte: params.dueDateRange.startDate,
        $lte: params.dueDateRange.endDate
      }
    })
  }

  if (size(params.invoicePeriod)) {
    const { startDate, endDate } = params.invoicePeriod
    const startDateObj = new Date(startDate)
    const endDateObj = new Date(endDate)
    invoiceMainQuery.push({
      $or: [
        {
          invoiceStartOn: { $gte: startDateObj, $lte: endDateObj }
        },
        {
          invoiceEndOn: { $gte: startDateObj, $lte: endDateObj }
        },
        {
          invoiceStartOn: { $lte: startDateObj },
          invoiceEndOn: { $gte: endDateObj }
        }
      ]
    })
  }

  if (
    params.download &&
    params.dueDateRange &&
    params.dueDateRange.startDate_string &&
    params.dueDateRange.endDate_string
  ) {
    const startDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        params.dueDateRange.startDate_string
      )
    )
      .startOf('day')
      .toDate()

    const endDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        params.dueDateRange.endDate_string
      )
    )
      .endOf('day')
      .toDate()

    if (startDate && endDate) {
      invoiceMainQuery.push({
        dueDate: { $gte: startDate, $lte: endDate }
      })
    }
  }
  return invoiceMainQuery
}

export const prepareInvoiceStatusForQuery = async (params) => {
  const invoicesStatusQuery = []
  const invoicesStatus = compact(params.status)
  let invoiceTenantQuery = {}
  const invoiceMainQuery = []

  //set tenant filters in query
  if (params.tenantId) {
    invoiceTenantQuery = {
      $or: [
        { tenantId: params.tenantId },
        { tenants: { $elemMatch: { tenantId: params.tenantId } } }
      ]
    }
  }

  // Start status query
  const vippsStatus = compact(params.vippsStatus)
  let compelloStatus = null
  const eInvoiceType = process.env.E_INVOICE_TYPE
  if (eInvoiceType === 'compello') {
    compelloStatus = compact(params.vippsStatus)
  }
  let vippsInvoiceStatuses = []

  if (indexOf(invoicesStatus, 'partially_paid') !== -1) {
    invoicesStatusQuery.push({ isPartiallyPaid: true })
  }

  if (indexOf(invoicesStatus, 'overpaid') !== -1) {
    invoicesStatusQuery.push({ isOverPaid: true })
  }

  if (indexOf(invoicesStatus, 'defaulted') !== -1) {
    invoicesStatusQuery.push({ isDefaulted: true })
  }

  if (indexOf(invoicesStatus, 'created') !== -1) {
    invoicesStatusQuery.push({ status: 'new' }) //, invoiceSent: {$ne: true}
    invoicesStatusQuery.push({ status: 'created' }) //, invoiceSent: {$ne: true}

    if (indexOf(invoicesStatus, 'sent') === -1) {
      invoiceMainQuery.push({ invoiceSent: { $ne: true } })
    }
  }

  if (indexOf(invoicesStatus, 'sent') !== -1) {
    invoicesStatusQuery.push({ invoiceSent: true })

    if (indexOf(invoicesStatus, 'partially_paid') === -1) {
      invoiceMainQuery.push({ isPartiallyPaid: { $ne: true } })
    }

    if (indexOf(invoicesStatus, 'defaulted') === -1) {
      invoiceMainQuery.push({ isDefaulted: { $ne: true } })
    }

    if (indexOf(invoicesStatus, 'credited') === -1) {
      invoiceMainQuery.push({ status: { $nin: ['credited'] } })
    }
  }

  if (indexOf(invoicesStatus, 'paid') !== -1) {
    invoicesStatusQuery.push({ status: 'paid' })
  }

  if (indexOf(invoicesStatus, 'overdue') !== -1) {
    invoicesStatusQuery.push({ status: 'overdue' })
  }

  if (indexOf(invoicesStatus, 'lost') !== -1) {
    invoicesStatusQuery.push({ lostMeta: { $exists: true } })
  }

  if (indexOf(invoicesStatus, 'credited') !== -1) {
    invoicesStatusQuery.push({ status: 'credited' })
  }

  if (indexOf(invoicesStatus, 'partially_credited') !== -1) {
    invoicesStatusQuery.push({ isPartiallyCredited: true })
  }

  if (indexOf(invoicesStatus, 'balanced') !== -1) {
    invoicesStatusQuery.push({ status: 'balanced' })
  }

  if (indexOf(invoicesStatus, 'partially_balanced') !== -1) {
    invoicesStatusQuery.push({ isPartiallyBalanced: true })
  }

  if (indexOf(invoicesStatus, 'fees_paid') !== -1) {
    invoicesStatusQuery.push({ feesPaid: true })
  }

  if (indexOf(invoicesStatus, 'fees_due') !== -1) {
    invoicesStatusQuery.push({
      feesMeta: { $exists: true },
      invoiceType: { $ne: 'credit_note' },
      status: { $ne: 'credited' },
      feesPaid: { $ne: true }
    })
  }

  if (indexOf(invoicesStatus, 'eviction_notice') !== -1) {
    invoicesStatusQuery.push({
      evictionNoticeSent: { $exists: true },
      status: { $ne: 'paid' }
    })
  }

  if (indexOf(invoicesStatus, 'eviction_notice_due') !== -1) {
    invoicesStatusQuery.push({
      evictionDueReminderSent: { $exists: true },
      status: { $ne: 'paid' }
    })
  }

  //prepare invoice status query
  if (size(params.status)) {
    const invoiceStatus = [
      'new',
      'created',
      'overdue',
      'paid',
      'credited',
      'lost'
    ]
    const availableStatus = intersection(invoiceStatus, params.status)

    if (indexOf(invoicesStatus, 'created') !== -1) {
      availableStatus.push('new')
    }

    if (size(availableStatus))
      invoicesStatusQuery.push({ status: { $in: availableStatus } })
  }
  //set vipps status
  if (indexOf(vippsStatus, 'sent') !== -1) {
    vippsInvoiceStatuses = union(vippsInvoiceStatuses, [
      'sent',
      'created',
      'pending'
    ])
  }

  if (indexOf(vippsStatus, 'approved') !== -1) {
    vippsInvoiceStatuses = union(vippsInvoiceStatuses, ['approved'])
  }

  if (indexOf(vippsStatus, 'failed') !== -1) {
    vippsInvoiceStatuses = union(vippsInvoiceStatuses, [
      'sending',
      'sending_failed',
      'failed',
      'rejected',
      'expired',
      'deleted',
      'revoked'
    ])
  }

  // Set compello status
  let compelloInvoiceStatuses = []
  if (compelloStatus) {
    if (indexOf(compelloStatus, 'sent') !== -1) {
      compelloInvoiceStatuses = union(compelloInvoiceStatuses, [
        'sent',
        'created',
        'pending'
      ])
    }

    if (indexOf(compelloStatus, 'approved') !== -1) {
      compelloInvoiceStatuses = union(compelloInvoiceStatuses, ['approved'])
    }

    if (indexOf(compelloStatus, 'failed') !== -1) {
      compelloInvoiceStatuses = union(compelloInvoiceStatuses, [
        'sending',
        'sending_failed',
        'failed',
        'rejected',
        'expired',
        'deleted',
        'revoked'
      ])
    }
  }

  if (size(compelloInvoiceStatuses) || size(vippsInvoiceStatuses)) {
    invoiceMainQuery.push({
      $and: [
        {
          $or: [
            { compelloStatus: { $in: compelloInvoiceStatuses } },
            { vippsStatus: { $in: vippsInvoiceStatuses } }
          ]
        }
      ]
    })
  }
  //set lease serial filter
  if (params.contractId && params.leaseSerial) {
    const invoiceQuery = await prepareInvoiceQueryForLeaseFilter(
      params.contractId,
      params.leaseSerial
    )
    invoiceMainQuery.push(invoiceQuery)
  }
  // Payout status query start
  if (size(params.payoutStatus)) {
    const payoutStatus = compact(params.payoutStatus)

    const invoiceIds = map(
      await payoutHelper.getPayouts({
        partnerId: params.partnerId,
        status: { $in: payoutStatus }
      }),
      'invoiceId'
    )

    if (size(invoiceIds)) invoiceMainQuery.push({ _id: { $in: invoiceIds } })
    else invoiceMainQuery.push({ _id: 'nothing' })
  }
  // payout status query end

  if (size(invoiceTenantQuery)) invoiceMainQuery.push(invoiceTenantQuery)
  if (size(invoicesStatusQuery))
    invoiceMainQuery.push({ $or: invoicesStatusQuery })

  return invoiceMainQuery
}

export const prepareInvoicesQueryForExcelCreator = async (
  params,
  partnerSetting
) => {
  let query = {}
  if (size(params)) {
    const partnerId = params.partnerId
    const invoiceMainQuery = [{ partnerId }]

    if (params.invoiceType === 'landlord_invoice') {
      invoiceMainQuery.push({
        isFinalSettlement: { $ne: true },
        invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] }
      })
    } else {
      invoiceMainQuery.push({
        invoiceType: { $nin: ['landlord_invoice', 'landlord_credit_note'] }
      })
    }
    // Set branch filters in query
    if (params.branchId) invoiceMainQuery.push({ branchId: params.branchId })
    // Set agent filters in query
    if (params.agentId) invoiceMainQuery.push({ agentId: params.agentId })
    // Set account filters in query
    if (params.accountId) invoiceMainQuery.push({ accountId: params.accountId })
    // Set property filters in query
    if (params.propertyId)
      invoiceMainQuery.push({ propertyId: params.propertyId })
    // Set accountId for landlordDashboard
    if (params.context && params.context === 'landlordDashboard') {
      const accountIds =
        uniq(
          map(
            await accountHelper.getAccounts({ personId: params.userId }),
            '_id'
          )
        ) || []

      invoiceMainQuery.push({ accountId: { $in: accountIds } })
    }
    // Set DueDate range in query for export data
    const dueDateRangeQuery = await setDueDateRangeInQueryForExportData(
      params,
      partnerSetting
    )

    if (size(dueDateRangeQuery)) {
      invoiceMainQuery.push(...dueDateRangeQuery)
    }
    // Prepare invoice status for query
    const invoiceStatusForQuery = await prepareInvoiceStatusForQuery(params)
    if (size(invoiceStatusForQuery)) {
      invoiceMainQuery.push(...invoiceStatusForQuery)
    }

    if (size(invoiceMainQuery)) query = { $and: invoiceMainQuery }
    else query = { _id: 'nothing' }

    // Set invoice serial number filters in query.
    if (
      params.searchKeyword &&
      params.context !== 'tenantDashboard' &&
      params.context !== 'landlordDashboard'
    ) {
      query = {
        partnerId,
        $or: [
          { invoiceSerialId: params.searchKeyword * 1 },
          { kidNumber: params.searchKeyword }
        ],
        invoiceType:
          params.invoiceType === 'landlord_invoice'
            ? { $in: ['landlord_invoice', 'landlord_credit_note'] }
            : { $nin: ['landlord_invoice', 'landlord_credit_note'] },
        isFinalSettlement:
          params.invoiceType === 'landlord_invoice'
            ? { $ne: true }
            : { $exists: false }
      }
    }
    if (params.contractId) query.contractId = params.contractId
  }

  return query
}

export const getStatusTextForInvoice = (language) => {
  const pipeline = {
    statusText: {
      $switch: {
        branches: [
          {
            case: {
              $and: [
                { $eq: ['$status', 'new'] },
                { $ne: ['$invoiceSent', true] }
              ]
            },
            then: appHelper.translateToUserLng('common.filters.new', language)
          },
          {
            case: {
              $and: [
                { $eq: ['$status', 'created'] },
                { $ne: ['$invoiceSent', true] }
              ]
            },
            then: appHelper.translateToUserLng(
              'common.filters.created',
              language
            )
          },
          {
            case: { $eq: ['$status', 'credited'] },
            then: appHelper.translateToUserLng(
              'common.filters.credited',
              language
            )
          },
          {
            case: { $eq: ['$status', 'lost'] },
            then: appHelper.translateToUserLng('common.filters.lost', language)
          },
          {
            case: {
              $and: [
                { $ne: ['$isPartiallyPaid', true] },
                { $ne: ['$isDefaulted', true] },
                { $eq: ['$invoiceSent', true] },
                {
                  $or: [
                    { $eq: ['$status', 'new'] },
                    { $eq: ['$status', 'created'] }
                  ]
                }
              ]
            },
            then: appHelper.translateToUserLng('common.filters.sent', language)
          },
          {
            case: {
              $eq: ['$status', 'paid']
            },
            then: appHelper.translateToUserLng('common.filters.paid', language)
          },
          {
            case: {
              $and: [
                { $eq: ['$status', 'overdue'] },
                { $ne: ['$isDefaulted', true] }
              ]
            },
            then: appHelper.translateToUserLng(
              'common.filters.unpaid',
              language
            )
          },
          {
            case: {
              $eq: ['$isDefaulted', true]
            },
            then: appHelper.translateToUserLng(
              'common.filters.defaulted',
              language
            )
          },
          {
            case: {
              $eq: ['$status', 'balanced']
            },
            then: appHelper.translateToUserLng(
              'common.filters.balanced',
              language
            )
          }
        ],
        default: null
      }
    },
    tagText: {
      $switch: {
        branches: [
          {
            case: { $eq: ['$isPartiallyPaid', true] },
            then: appHelper.translateToUserLng(
              'common.filters.partially_paid',
              language
            )
          },
          {
            case: { $eq: ['$isOverPaid', true] },
            then: appHelper.translateToUserLng(
              'common.filters.overpaid',
              language
            )
          },
          {
            case: { $eq: ['$isPartiallyCredited', true] },
            then: appHelper.translateToUserLng(
              'common.filters.partially_credited',
              language
            )
          },
          {
            case: { $eq: ['$isPartiallyBalanced', true] },
            then: appHelper.translateToUserLng(
              'common.filters.partially_balanced',
              language
            )
          }
        ],
        default: null
      }
    }
  }
  return pipeline
}

export const getRentInvoiceForExcel = async (queryData) => {
  const {
    query,
    options,
    dateFormat,
    timeZone,
    language = 'no',
    context,
    numberOfDecimal
  } = queryData
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    {
      $project: {
        invoiceSerialId: 1,
        kidNumber: 1,
        createdAt: {
          $dateToString: {
            format: dateFormat,
            date: '$createdAt',
            timezone: timeZone
          }
        },
        invoiceTotal: 1,
        invoiceType: 1,
        roundedAmount: 1,
        creditedAmount: 1,
        totalPaid: 1,
        totalBalanced: 1,
        lostMeta: 1,
        tenantId: 1,
        propertyId: 1,
        accountId: 1,
        invoiceSent: 1,
        isOverPaid: 1,
        isPartiallyPaid: 1,
        isPartiallyCredited: 1,
        isPartiallyBalanced: 1,
        isDefaulted: 1,
        status: 1,
        dueDate: {
          $dateToString: {
            format: dateFormat,
            date: '$dueDate',
            timezone: timeZone
          }
        }
      }
    },
    {
      $addFields: {
        totalLost: {
          $cond: {
            if: { $ifNull: ['$lostMeta.amount', false] },
            then: '$lostMeta.amount',
            else: 0
          }
        },
        totalDue: {
          $round: [
            {
              $cond: [
                {
                  $or: [
                    { $eq: ['$invoiceType', 'landlord_invoice'] },
                    { $eq: ['$invoiceType', 'landlord_credit_note'] }
                  ]
                },
                {
                  $subtract: [
                    { $ifNull: ['$invoiceTotal', 0] },
                    {
                      $add: [
                        { $ifNull: ['$totalPaid', 0] },
                        { $ifNull: ['$totalBalanced', 0] }
                      ]
                    }
                  ]
                },
                {
                  $subtract: [
                    {
                      $add: [
                        { $ifNull: ['$invoiceTotal', 0] },
                        { $ifNull: ['$creditedAmount', 0] }
                      ]
                    },
                    {
                      $add: [
                        { $ifNull: ['$totalPaid', 0] },
                        { $ifNull: ['$lostMeta.amount', 0] }
                      ]
                    }
                  ]
                }
              ]
            },
            {
              $cond: {
                if: { $eq: [numberOfDecimal, 0] },
                then: 0,
                else: 2
              }
            }
          ]
        },
        ...getStatusTextForInvoice(language)
      }
    },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
        foreignField: '_id',
        as: 'tenants'
      }
    },
    {
      $unwind: {
        path: '$tenants',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        as: 'accounts'
      }
    },
    {
      $unwind: {
        path: '$accounts',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        _id: 1,
        invoiceId: '$invoiceSerialId',
        kidNumber: {
          $cond: {
            if: { $ne: [context, 'landlordDashboard'] },
            then: '$kidNumber',
            else: 0
          }
        },
        dueDate: '$dueDate',
        accountId: '$accounts.serial',
        account: '$accounts.name',
        tenantId: '$tenants.serial',
        tenant: '$tenants.name',
        objectId: '$property.serial',
        property: {
          $concat: [
            { $ifNull: ['$property.location.name', ''] },
            {
              $cond: [
                { $ifNull: ['$property.location.postalCode', false] },
                { $concat: [', ', '$property.location.postalCode'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.city', false] },
                { $concat: [', ', '$property.location.city'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.country', false] },
                { $concat: [', ', '$property.location.country'] },
                ''
              ]
            }
          ]
        },
        apartmentId: '$property.apartmentId',
        status: {
          $cond: {
            if: {
              $and: [
                { $ne: ['$statusText', null] },
                { $ne: ['$tagText', null] }
              ]
            },
            then: { $concat: ['$statusText', ', ', '$tagText'] },
            else: {
              $cond: {
                if: '$statusText',
                then: '$statusText',
                else: '$tagText'
              }
            }
          }
        },
        createdAt: 1,
        invoiceTotal: 1,
        roundedAmount: { $ifNull: ['$roundedAmount', 0] },
        totalPaid: { $ifNull: ['$totalPaid', 0] },
        totalLost: 1,
        totalDue: {
          $cond: {
            if: {
              $eq: ['$invoiceType', 'credit_note']
            },
            then: 0,
            else: '$totalDue'
          }
        },
        statusText: 1
      }
    }
  ]

  const invoiceData = await InvoiceCollection.aggregate(pipeline)
  return invoiceData || []
}

export const invoiceDataForExcelCreator = async (params, options) => {
  const { partnerId = {}, userId = {} } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })
  const context = params?.context || ''
  if (params.downloadProcessType === 'download_landlord_invoices') {
    params.invoiceType = 'landlord_invoice'
  }
  const userInfo = await userHelper.getAnUser({ _id: params.userId })
  const userLanguage = userInfo.getLanguage()
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const invoicesQuery = await prepareInvoicesQueryForExcelCreator(
    params,
    partnerSetting
  )
  const dataCount = await countInvoices(invoicesQuery)
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const numberOfDecimal =
    partnerSetting?.invoiceSettings?.numberOfDecimalInInvoice
  const queryData = {
    query: invoicesQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage,
    context,
    numberOfDecimal
  }
  const invoiceData = await getRentInvoiceForExcel(queryData)
  return {
    data: invoiceData,
    total: dataCount
  }
}

export const queryForInvoiceExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { skip, limit, sort } = options
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (
    queueInfo?.params?.downloadProcessType === 'download_rent_invoices' ||
    queueInfo?.params?.downloadProcessType === 'download_landlord_invoices'
  ) {
    const commissionData = await invoiceDataForExcelCreator(queueInfo.params, {
      skip,
      limit,
      sort
    })
    return commissionData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const getTotalDueAmountOfAnInvoice = async (invoice) => {
  if (!size(invoice)) return 0

  const {
    creditedAmount = 0,
    invoiceTotal = 0,
    lostMeta = {},
    totalBalanced = 0,
    totalPaid = 0
  } = invoice
  const invoiceLostAmount = lostMeta?.amount || 0
  let dueTotal = 0
  if (isNotLandlord(invoice)) {
    dueTotal = await appHelper.convertTo2Decimal(
      invoiceTotal - totalPaid + creditedAmount - invoiceLostAmount
    )
  } else {
    dueTotal = await appHelper.convertTo2Decimal(
      invoiceTotal - totalPaid - totalBalanced
    )
  }

  return dueTotal
}

export const getTotalOverDue = async (query) => {
  const [invoiceTotalInfo] = await InvoiceCollection.aggregate([
    {
      $match: {
        ...query,
        status: 'overdue',
        invoiceType: 'invoice'
      }
    },
    {
      $group: {
        _id: null,
        invoiceTotal: { $sum: '$invoiceTotal' },
        paidTotal: { $sum: '$totalPaid' },
        creditedTotal: { $sum: '$creditedAmount' }
      }
    }
  ])

  if (size(invoiceTotalInfo)) {
    const {
      invoiceTotal = 0,
      paidTotal = 0,
      creditedTotal = 0
    } = invoiceTotalInfo
    return invoiceTotal + creditedTotal - paidTotal
  }
}

//Calculate invoice this month
export const getInvoiceThisMonth = async (accountId, partnerId, tenantId) => {
  const startMonth = (await appHelper.getActualDate(partnerId, true))
    .startOf('month')
    .toDate()

  const endMonth = (await appHelper.getActualDate(partnerId, true))
    .endOf('month')
    .toDate()

  let matchData
  if (accountId) {
    matchData = {
      accountId,
      partnerId,
      dueDate: { $gte: startMonth, $lte: endMonth },
      invoiceType: 'invoice'
    }
  } else if (tenantId) {
    matchData = {
      $or: [{ tenantId }, { tenants: { $elemMatch: { tenantId } } }],
      partnerId,
      dueDate: { $gte: startMonth, $lte: endMonth },
      invoiceType: 'invoice'
    }
  }

  let invoiceThisMonth = 0
  const invoicesTotal = await InvoiceCollection.aggregate([
    {
      $match: matchData
    },
    {
      $group: {
        _id: null,
        invoiceTotalAmount: { $sum: '$invoiceTotal' },
        totalCreditedAmount: { $sum: '$creditedAmount' }
      }
    }
  ])
  if (invoicesTotal.length > 0)
    invoiceThisMonth =
      invoicesTotal[0].invoiceTotalAmount +
      (invoicesTotal[0].totalCreditedAmount || 0)

  return invoiceThisMonth
}

const getInvoicesForQuery = async (params = {}, populate = []) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const invoices = await InvoiceCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return invoices
}

export const prepareQueryForInvoiceStatus = (query, status) => {
  const invoicesStatusQuery = []
  const isPartiallyPaid = includes(status, 'partially_paid')
  const isOverpaid = includes(status, 'overpaid')
  const isDefaulted = includes(status, 'defaulted')
  const isSent = includes(status, 'sent')
  const andQuery = []
  if (isPartiallyPaid) {
    invoicesStatusQuery.push({ isPartiallyPaid: true })
  }
  if (isOverpaid) {
    invoicesStatusQuery.push({ isOverPaid: true })
  }
  if (isDefaulted) {
    invoicesStatusQuery.push({ isDefaulted: true })
  }
  if (includes(status, 'created')) {
    invoicesStatusQuery.push({ status: { $in: ['new', 'created'] } })

    if (!isSent) {
      andQuery.push({ invoiceSent: { $ne: true } })
    }
  }
  if (isSent) {
    invoicesStatusQuery.push({ invoiceSent: true })
    if (!isPartiallyPaid) {
      andQuery.push({ isPartiallyPaid: { $ne: true } })
    }
    if (!isDefaulted) {
      andQuery.push({ isDefaulted: { $ne: true } })
    }
    if (!includes(status, 'credited')) {
      andQuery.push({ status: { $ne: 'credited' } })
    }
  }
  if (indexOf(status, 'paid') !== -1) {
    invoicesStatusQuery.push({ status: 'paid' })
  }

  if (indexOf(status, 'overdue') !== -1) {
    invoicesStatusQuery.push({ status: 'overdue' })
  }

  if (indexOf(status, 'lost') !== -1) {
    invoicesStatusQuery.push({ lostMeta: { $exists: true } })
  }

  if (indexOf(status, 'credited') !== -1) {
    invoicesStatusQuery.push({ status: 'credited' })
  }

  if (indexOf(status, 'partially_credited') !== -1) {
    invoicesStatusQuery.push({ isPartiallyCredited: true })
  }

  if (indexOf(status, 'balanced') !== -1) {
    invoicesStatusQuery.push({ status: 'balanced' })
  }

  if (indexOf(status, 'partially_balanced') !== -1) {
    invoicesStatusQuery.push({ isPartiallyBalanced: true })
  }

  if (indexOf(status, 'fees_paid') !== -1) {
    invoicesStatusQuery.push({ feesPaid: true })
  }

  if (indexOf(status, 'fees_due') !== -1) {
    invoicesStatusQuery.push({
      feesMeta: { $exists: true },
      invoiceType: { $ne: 'credit_note' },
      status: { $ne: 'credited' },
      feesPaid: { $ne: true }
    })
  }

  if (indexOf(status, 'eviction_notice') !== -1) {
    invoicesStatusQuery.push({
      evictionNoticeSent: { $exists: true },
      status: { $ne: 'paid' }
    })
  }

  if (indexOf(status, 'eviction_notice_due') !== -1) {
    invoicesStatusQuery.push({
      evictionDueReminderSent: { $exists: true },
      status: { $ne: 'paid' }
    })
  }
  const availableStatus = intersection(
    ['new', 'created', 'overdue', 'paid', 'credited', 'lost'],
    status
  )

  if (indexOf(status, 'created') !== -1) {
    availableStatus.push('new')
  }

  if (size(availableStatus))
    invoicesStatusQuery.push({ status: { $in: availableStatus } })
  if (size(andQuery)) query.push(...andQuery)
  return invoicesStatusQuery
}

export const countInvoices = async (query) => {
  const numberOfInvoices = await InvoiceCollection.countDocuments(query)
  return numberOfInvoices
}

export const getPayoutInvoiceIds = async (query) => {
  const { payoutStatus = [], partnerId = '' } = query
  const payoutQuery = {
    partnerId,
    status: { $in: payoutStatus }
  }
  const invoiceIds = await PayoutCollection.distinct('invoiceId', payoutQuery)
  return invoiceIds
}

export const prepareQueryByFilters = async (invoiceMainQuery, params) => {
  const {
    accountId,
    agentId,
    amount,
    branchId,
    contractId,
    createdDateRange = {},
    dueDateRange = {},
    invoicePeriod = {},
    invoiceSerialId,
    kidNumber = '',
    leaseSerial,
    payoutStatus = [],
    propertyId,
    searchKeyword,
    status = [],
    statusWith = [],
    statusWithout = [],
    tenantId,
    vippsStatus = []
  } = params

  let { compelloStatus = [] } = params
  const eInvoiceType = process.env.E_INVOICE_TYPE
  if (eInvoiceType === 'compello') {
    compelloStatus = params.vippsStatus
  }

  let vippsInvoiceStatuses = []
  if (size(vippsStatus)) {
    if (indexOf(vippsStatus, 'sent') !== -1)
      vippsInvoiceStatuses = union(vippsInvoiceStatuses, [
        'created',
        'pending',
        'sent'
      ])

    if (indexOf(vippsStatus, 'approved') !== -1)
      vippsInvoiceStatuses = union(vippsInvoiceStatuses, ['approved'])

    if (indexOf(vippsStatus, 'failed') !== -1)
      vippsInvoiceStatuses = union(vippsInvoiceStatuses, [
        'deleted',
        'expired',
        'failed',
        'rejected',
        'revoked',
        'sending',
        'sending_failed'
      ])
  }

  let compelloInvoiceStatuses = []
  if (size(compelloStatus)) {
    if (indexOf(compelloStatus, 'sent') !== -1) {
      compelloInvoiceStatuses = union(compelloInvoiceStatuses, [
        'created',
        'pending',
        'sent'
      ])
    }

    if (indexOf(compelloStatus, 'approved') !== -1)
      compelloInvoiceStatuses = union(compelloInvoiceStatuses, ['approved'])

    if (indexOf(compelloStatus, 'failed') !== -1) {
      compelloInvoiceStatuses = union(compelloInvoiceStatuses, [
        'deleted',
        'expired',
        'failed',
        'rejected',
        'revoked',
        'sending',
        'sending_failed'
      ])
    }
  }

  if (size(compelloInvoiceStatuses) || size(vippsInvoiceStatuses)) {
    invoiceMainQuery.push({
      $and: [
        {
          $or: [
            { compelloStatus: { $in: compelloInvoiceStatuses } },
            { vippsStatus: { $in: vippsInvoiceStatuses } }
          ]
        }
      ]
    })
  }
  // Lease serial filter
  if (contractId && leaseSerial) {
    const invoiceQuery = await prepareInvoiceQueryForLeaseFilter(
      contractId,
      leaseSerial
    )
    invoiceMainQuery.push(invoiceQuery)
  }
  if (branchId) invoiceMainQuery.push({ branchId })
  if (agentId) invoiceMainQuery.push({ agentId })
  if (accountId) invoiceMainQuery.push({ accountId })
  if (propertyId) invoiceMainQuery.push({ propertyId })
  const { startDate, endDate } = createdDateRange
  if (startDate && endDate) {
    invoiceMainQuery.push({
      createdAt: {
        $gte: new Date(startDate),
        $lte: new Date(endDate)
      }
    })
  }
  if (dueDateRange.startDate && dueDateRange.endDate) {
    invoiceMainQuery.push({
      dueDate: {
        $gte: new Date(dueDateRange.startDate),
        $lte: new Date(dueDateRange.endDate)
      }
    })
  }
  if (size(invoicePeriod)) {
    const { startDate, endDate } = invoicePeriod
    const startDateObj = new Date(startDate)
    const endDateObj = new Date(endDate)
    invoiceMainQuery.push({
      $or: [
        {
          invoiceStartOn: { $gte: startDateObj, $lte: endDateObj }
        },
        {
          invoiceEndOn: { $gte: startDateObj, $lte: endDateObj }
        },
        {
          invoiceStartOn: { $lte: startDateObj },
          invoiceEndOn: { $gte: endDateObj }
        }
      ]
    })
  }
  if (size(status)) {
    const invoicesStatusQuery = prepareQueryForInvoiceStatus(
      invoiceMainQuery,
      status
    )
    invoiceMainQuery.push({ $or: invoicesStatusQuery })
  }
  if (size(payoutStatus)) {
    const invoiceIds = await getPayoutInvoiceIds(params)
    if (size(invoiceIds)) invoiceMainQuery.push({ _id: { $in: invoiceIds } })
    else invoiceMainQuery.push({ _id: 'nothing' })
  }
  if (tenantId) {
    invoiceMainQuery.push({
      $or: [{ tenantId }, { tenants: { $elemMatch: { tenantId } } }]
    })
  }
  if (size(searchKeyword)) {
    searchKeyword.trim()
    invoiceMainQuery.push({
      $or: [
        { invoiceTotal: parseInt(searchKeyword) },
        { invoiceSerialId: parseInt(searchKeyword) },
        { kidNumber: { $regex: searchKeyword, $options: 'i' } }
      ]
    })
  }

  if (amount) {
    invoiceMainQuery.push({
      invoiceTotal: amount
    })
  }
  if (invoiceSerialId) {
    invoiceMainQuery.push({
      invoiceSerialId
    })
  }
  if (kidNumber) {
    invoiceMainQuery.push({
      kidNumber
    })
  }
  if (size(statusWith)) {
    const invoicesStatusQuery = prepareQueryForInvoiceStatus(params, statusWith)
    invoiceMainQuery.push({ $or: invoicesStatusQuery })
  }
  if (size(statusWithout)) {
    invoiceMainQuery.push({
      $or: {
        $nin: [statusWithout]
      }
    })
  }
  if (size(invoiceMainQuery)) return { $and: invoiceMainQuery }
  return null
}

export const prepareInvoiceQuery = async (query) => {
  const { partnerId } = query
  const invoiceMainQuery = [{ partnerId }]
  invoiceMainQuery.push({
    invoiceType: { $nin: ['landlord_invoice', 'landlord_credit_note'] }
  })
  const invoiceQuery = await prepareQueryByFilters(invoiceMainQuery, query)
  return invoiceQuery
}
export const prepareInvoiceQueryForLambda = async (query) => {
  const {
    compelloStatus = [],
    invoiceTypes = [],
    statusWith = [],
    statusWithout = [],
    vippsStatus = []
  } = query

  const invoiceMainQuery = []
  if (size(statusWithout))
    invoiceMainQuery.push({
      status: { $nin: statusWithout }
    })
  if (size(statusWith)) {
    invoiceMainQuery.push({
      status: { $in: statusWith }
    })
  }
  if (size(vippsStatus)) {
    invoiceMainQuery.push({ vippsStatus: { $in: vippsStatus } })
  }
  if (size(compelloStatus)) {
    invoiceMainQuery.push({ compelloStatus: { $in: compelloStatus } })
  }
  if (size(invoiceTypes)) {
    invoiceMainQuery.push({
      invoiceType: { $in: invoiceTypes }
    })
  }
  const invoiceQuery = {
    $and: invoiceMainQuery
  }
  return invoiceQuery
}
export const prepareLandlordInvoiceQuery = async (query) => {
  const { partnerId } = query
  const invoiceMainQuery = [{ partnerId }]
  invoiceMainQuery.push({
    isFinalSettlement: { $ne: true },
    invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] }
  })
  const landlordInvoiceQuery = await prepareQueryByFilters(
    invoiceMainQuery,
    query
  )
  return landlordInvoiceQuery
}

export const getTotalInvoiceAmount = async (query) => {
  let totalCreditedAmountExpr = '$totalCreditedAmount'
  if (query.isLandlordView) {
    totalCreditedAmountExpr = 0
    delete query.isLandlordView
  }
  const result = await InvoiceCollection.aggregate([
    {
      $match: query
    },
    {
      $addFields: {
        totalDue: {
          $cond: [
            { $eq: ['$invoiceType', 'credit_note'] },
            0,
            {
              $subtract: [
                {
                  $add: [
                    '$invoiceTotal',
                    {
                      $cond: [
                        { $eq: ['$invoiceType', 'invoice'] },
                        { $ifNull: ['$creditedAmount', 0] },
                        0
                      ]
                    }
                  ]
                },
                {
                  $add: [
                    { $ifNull: ['$totalPaid', 0] },
                    { $ifNull: ['$lostMeta.amount', 0] },
                    { $ifNull: ['$totalBalanced', 0] }
                  ]
                }
              ]
            }
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        invoiceTotal: { $sum: '$invoiceTotal' },
        totalPaidAmount: { $sum: '$totalPaid' },
        totalLostAmount: { $sum: '$lostMeta.amount' },
        totalCreditedAmount: { $sum: '$creditedAmount' },
        totalBalancedAmount: { $sum: '$totalBalanced' },
        totalDue: { $sum: '$totalDue' }
      }
    },
    {
      $addFields: {
        finalCreditedAmount: totalCreditedAmountExpr
      }
    },
    {
      $project: {
        totalDue: 1,
        invoiceTotal: 1,
        // invoiceTotal: {
        //   $subtract: ['$invoiceTotal', '$totalLostAmount']
        // },
        totalPaidAmount: 1,
        totalLostAmount: 1,
        totalBalancedAmount: 1
      }
    }
  ])
  return result[0] || {}
}

const getPartnerDashboardInvoiceSummaryData = async (query) => {
  const result = await InvoiceCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: null,
        overPaidInvoicesCount: {
          $sum: {
            $cond: [{ $eq: ['$isOverPaid', true] }, 1, 0]
          }
        },
        invoiceTotalAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$invoiceTotal', 0]
          }
        },
        invoiceTotalPaidAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$totalPaid', 0]
          }
        },
        invoiceCreditedAmount: {
          $sum: {
            $cond: [{ $eq: ['$invoiceType', 'invoice'] }, '$creditedAmount', 0]
          }
        },
        totalLostAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'lost'] },
                  { $eq: ['$invoiceType', 'invoice'] }
                ]
              },
              '$lostMeta.amount',
              0
            ]
          }
        },
        overdueInvoiceTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  { $eq: ['$invoiceType', 'invoice'] }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        overdueInvoiceTotalPaidAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  { $eq: ['$invoiceType', 'invoice'] }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        overdueInvoiceCreditedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  { $eq: ['$invoiceType', 'invoice'] }
                ]
              },
              '$creditedAmount',
              0
            ]
          }
        }
      }
    },
    {
      $addFields: {
        invoiceTotalAmount: {
          $add: [
            { $ifNull: ['$invoiceTotalAmount', 0] },
            { $ifNull: ['$invoiceCreditedAmount', 0] }
          ]
        },
        totalDue: {
          $subtract: [
            {
              $add: [
                { $ifNull: ['$invoiceTotalAmount', 0] },
                { $ifNull: ['$invoiceCreditedAmount', 0] }
              ]
            },
            {
              $add: [
                { $ifNull: ['$invoiceTotalPaidAmount', 0] },
                { $ifNull: ['$totalLostAmount', 0] }
              ]
            }
          ]
        },
        totalOverDue: {
          $subtract: [
            {
              $sum: [
                { $ifNull: ['$overdueInvoiceTotalAmount', 0] },
                { $ifNull: ['$overdueInvoiceCreditedAmount', 0] }
              ]
            },
            { $ifNull: ['$overdueInvoiceTotalPaidAmount', 0] }
          ]
        }
      }
    },
    {
      $addFields: {
        totalOverDuePercentage: {
          $round: [
            {
              $divide: [
                { $multiply: ['$totalOverDue', 100] },
                {
                  $cond: [
                    {
                      $eq: ['$invoiceTotalAmount', 0]
                    },
                    1,
                    '$invoiceTotalAmount'
                  ]
                }
              ]
            },
            2
          ]
        },
        totalDuePercentage: {
          $round: [
            {
              $divide: [
                { $multiply: [{ $ifNull: ['$totalDue', 0] }, 100] },
                {
                  $cond: [
                    { $eq: ['$invoiceTotalAmount', 0] },
                    1,
                    '$invoiceTotalAmount'
                  ]
                }
              ]
            },
            2
          ]
        }
      }
    },
    {
      $project: {
        _id: 0,
        overPaidInvoicesCount: 1,
        invoiceTotalAmount: 1,
        totalDue: 1,
        totalOverDue: 1,
        totalOverDuePercentage: 1,
        totalDuePercentage: 1
      }
    }
  ])
  const {
    invoiceTotalAmount = 0,
    overPaidInvoicesCount = 0,
    totalDue = 0,
    totalOverDue = 0,
    totalOverDuePercentage = 0,
    totalDuePercentage = 0
  } = result[0] || {}

  const totalUnspecifiedPayment =
    await invoicePaymentHelper.countInvoicePayments({
      ...query,
      status: 'unspecified'
    })

  return {
    invoiceTotalAmount,
    overPaidInvoicesCount,
    totalDue,
    totalOverDue,
    totalUnspecifiedPayment,
    totalOverDuePercentage,
    totalDuePercentage
  }
}

export const getInvoicesForLambdaHelper = async (req) => {
  const { body = {} } = req
  const { query } = body

  const invoiceQuery = await prepareInvoiceQueryForLambda(query)
  body.query = invoiceQuery
  return await getInvoicesForQuery(body)
}

export const getBranchInfoPipeline = (userId = '') => [
  {
    $lookup: {
      from: 'branches',
      localField: 'branchId',
      foreignField: '_id',
      let: { userId },
      pipeline: [
        {
          $project: {
            isBranchAdminUser: {
              $cond: [
                {
                  $eq: ['$adminId', '$$userId']
                },
                true,
                false
              ]
            }
          }
        }
      ],
      as: 'branchInfo'
    }
  },
  {
    $unwind: {
      path: '$branchInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getRentInvoiceForQuery = async (body = {}) => {
  const { query, options = {}, userId } = body
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...appHelper.getCommonTenantInfoPipeline(),
    ...appHelper.getCommonPropertyInfoPipeline(),
    ...appHelper.getCommonContractInfoPipeline(),
    ...getBranchInfoPipeline(userId),
    ...invoiceSummaryHelper.showRefundOption('_id'),
    {
      $project: {
        _id: 1,
        invoiceSerialId: 1,
        isNonRentInvoice: 1,
        kidNumber: 1,
        dueDate: 1,
        invoiceTotal: 1,
        totalPaid: 1,
        totalDue: {
          $cond: [
            { $eq: ['$invoiceType', 'credit_note'] },
            0,
            {
              $subtract: [
                {
                  $add: [
                    { $ifNull: ['$invoiceTotal', 0] },
                    { $ifNull: ['$creditedAmount', 0] }
                  ]
                },
                {
                  $add: [
                    { $ifNull: ['$totalPaid', 0] },
                    { $ifNull: ['$lostMeta.amount', 0] }
                  ]
                }
              ]
            }
          ]
        },
        tenantInfo: 1,
        propertyInfo: {
          _id: 1,
          imageUrl: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          },
          listingTypeId: 1,
          propertyTypeId: 1,
          apartmentId: 1
        },
        createdAt: 1,
        // For preparing invoice status
        invoiceType: 1,
        status: 1,
        invoiceSent: 1,
        isPartiallyPaid: 1,
        isDefaulted: 1,
        secondReminderSentAt: 1,
        firstReminderSentAt: 1,
        isOverPaid: 1,
        isPartiallyCredited: 1,
        isPartiallyBalanced: 1,
        delayDate: 1,
        evictionNoticeSent: 1,
        evictionDueReminderSent: 1,
        vippsStatus: 1,
        enabledNotification: 1,
        isFinalSettlementDone: '$contractInfo.isFinalSettlementDone',
        isCollectionNoticeSent: {
          $cond: [{ $ifNull: ['$collectionNoticeSentAt', false] }, true, false]
        },
        isBranchAdminUser: '$branchInfo.isBranchAdminUser',
        showRefundOption: 1,
        paymentId: '$invoicePaymentInfo._id',
        compelloStatus: 1
      }
    }
  ]
  const invoices = (await InvoiceCollection.aggregate(pipeline)) || []
  return invoices
}

export const queryInvoices = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  const { requestFrom = '', propertyId = '' } = query
  const totalDocumentsQuery = {
    invoiceType: { $nin: ['landlord_invoice', 'landlord_credit_note'] },
    partnerId
  }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  const invoiceQuery = await prepareInvoiceQuery(query)
  if (!size(invoiceQuery)) {
    return []
  }
  body.query = invoiceQuery
  body.userId = userId
  const invoices = await getRentInvoiceForQuery(body)
  const filteredDocuments = await countInvoices(body.query)
  const totalDocuments = await countInvoices(totalDocumentsQuery)
  return {
    data: invoices,
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

const getLandlordInvoicesForQuery = async (body) => {
  const { query, options } = body
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonPropertyInfoPipeline(),
    {
      $project: {
        _id: 1,
        invoiceSerialId: 1,
        kidNumber: 1,
        dueDate: 1,
        invoiceTotal: 1,
        totalPaid: 1,
        totalDue: {
          $subtract: [
            { $ifNull: ['$invoiceTotal', 0] },
            {
              $add: [
                { $ifNull: ['$totalPaid', 0] },
                { $ifNull: ['$totalBalanced', 0] }
              ]
            }
          ]
        },
        totalBalanced: 1,
        createdAt: 1,
        accountInfo: 1,
        propertyInfo: {
          _id: 1,
          imageUrl: 1,
          listingTypeId: 1,
          propertyTypeId: 1,
          apartmentId: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          }
        },
        invoiceType: 1,
        status: 1,
        invoiceSent: 1,
        isPartiallyPaid: 1,
        isDefaulted: 1,
        secondReminderSentAt: 1,
        firstReminderSentAt: 1,
        isOverPaid: 1,
        isPartiallyCredited: 1,
        isPartiallyBalanced: 1,
        delayDate: 1,
        evictionNoticeSent: 1,
        evictionDueReminderSent: 1,
        vippsStatus: 1,
        enabledNotification: 1,
        isCollectionNoticeSent: {
          $cond: [{ $ifNull: ['$collectionNoticeSentAt', false] }, true, false]
        },
        compelloStatus: 1
      }
    }
  ]
  const invoices = (await InvoiceCollection.aggregate(pipeline)) || []
  return invoices
}

export const invoiceCalculationForAppHealth = async (partnerId) =>
  InvoiceCollection.aggregate([
    { $unwind: '$addonsMeta' },
    {
      $match: {
        partnerId,
        'addonsMeta.correctionId': { $exists: true }
      }
    },
    {
      $project: {
        correctionId: '$addonsMeta.correctionId',
        amount: '$addonsMeta.total'
      }
    },
    {
      $group: {
        _id: '$correctionId',
        correctionTotal: { $sum: '$amount' }
      }
    },
    {
      $lookup: {
        from: 'transactions',
        localField: '_id',
        foreignField: 'correctionId',
        as: 'transactions',
        pipeline: [
          {
            $project: {
              amount: 1,
              type: 1,
              correctionId: 1,
              subType: 1,
              totalRounded: {
                $cond: {
                  if: { $eq: ['$subType', 'rounded_amount'] },
                  then: '$amount',
                  else: 0
                }
              }
            }
          }
        ]
      }
    },
    {
      $addFields: {
        transactionAmounts: {
          $sum: '$transactions.amount'
        },
        totalRoundedAmount: {
          $sum: '$transactions.totalRounded'
        },
        transactions: '$transactions._id'
      }
    },
    {
      $addFields: {
        transactionAmounts: {
          $subtract: ['$transactionAmounts', '$totalRoundedAmount']
        }
      }
    },
    {
      $addFields: {
        missMatchTransactionsAmount: {
          $abs: {
            $subtract: ['$transactionAmounts', '$correctionTotal']
          }
        }
      }
    },
    {
      $group: {
        _id: null,
        totalTransactions: {
          $sum: '$transactionAmounts'
        },
        totalCorrection: {
          $sum: '$correctionTotal'
        },
        missingAmount: {
          $sum: '$missMatchTransactionsAmount'
        },
        missingTransactionsInCorrection: {
          $push: {
            $cond: {
              if: {
                $gt: [
                  {
                    $abs: '$missMatchTransactionsAmount'
                  },
                  1
                ]
              },
              then: {
                correctionId: '$_id',
                correctionAmount: '$correctionTotal',
                transactions: '$transactions',
                transactionAmounts: '$transactionAmounts'
              },
              else: '$$REMOVE'
            }
          }
        }
      }
    }
  ])

export const invoiceErrorHelper = async (req) => {
  const { body = {} } = req
  const { contractId } = body
  const pipeline = preparePipelineForInvoiceError(contractId)
  const invoices = await ContractCollection.aggregate(pipeline)
  return invoices[0]
}

const preparePipelineForInvoiceError = (contractId) => [
  {
    $match: { _id: contractId }
  },
  {
    $lookup: {
      from: 'annual_statements',
      foreignField: 'contractId',
      localField: '_id',
      as: 'annualStatement',
      pipeline: [
        {
          $sort: {
            statementYear: -1
          }
        },
        {
          $limit: 1
        },
        {
          $project: {
            statementYear: 1
          }
        }
      ]
    }
  },
  {
    $unwind: {
      path: '$annualStatement',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'partner_settings',
      foreignField: 'partnerId',
      localField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: '$partnerSettings'
  },
  {
    $lookup: {
      from: 'invoices',
      foreignField: 'contractId',
      localField: '_id',
      as: 'invoice',
      let: {
        annualStatementYear: '$annualStatement.statementYear'
      },
      pipeline: [
        {
          $addFields: {
            startOnYear: {
              $year: '$invoiceStartOn'
            },
            endOnYear: {
              $year: '$invoiceEndOn'
            },
            annualStatementYear: '$$annualStatementYear'
          }
        },
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$invoiceType', 'invoice'] },
                { $gt: ['$startOnYear', '$$annualStatementYear'] },
                { $gt: ['endOnYear', '$$annualStatementYear'] },
                { $ne: ['$leaseCancelled', true] }
              ]
            }
          }
        },
        {
          $sort: {
            invoiceStartOn: 1,
            invoiceEndOn: 1
          }
        }
      ]
    }
  },
  {
    $addFields: {
      'rentalMeta.invoiceStartFrom': {
        $cond: {
          if: {
            $gt: [
              {
                $size: '$invoice'
              },
              0
            ]
          },
          then: '$rentalMeta.invoiceStartFrom',
          else: '$rentalMeta.firstInvoiceDueDate'
        }
      }
    }
  },
  {
    $addFields: {
      totalInvoice: {
        $size: '$invoice'
      },
      today: {
        $toDate: {
          $dateToString: {
            date: new Date(),
            timezone: '$partnerSettings.dateTimeSettings.timezone'
          }
        }
      }
    }
  },
  {
    $addFields: {
      'rentalMeta.contractEndDate': {
        $ifNull: ['$rentalMeta.contractEndDate', '$today']
      }
    }
  },
  {
    $addFields: {
      today: {
        $subtract: [
          {
            $dateFromParts: {
              year: {
                $year: '$today'
              },
              month: {
                $add: [
                  {
                    $month: '$today'
                  },
                  1
                ]
              }
            }
          },
          86400000
        ]
      }
    }
  },
  {
    $addFields: {
      startDate: '$rentalMeta.invoiceStartFrom',
      endDate: {
        $cond: {
          if: {
            $and: [
              { $ifNull: ['$rentalMeta.contractEndDate', false] },
              { $gte: ['$today', '$rentalMeta.contractEndDate'] }
            ]
          },
          then: '$rentalMeta.contractEndDate',
          else: '$today'
        }
      }
    }
  },
  {
    $addFields: {
      endDate: {
        $cond: {
          if: {
            $and: [
              { $ifNull: ['$rentalMeta.contractEndDate', false] },
              { $gte: ['$today', '$rentalMeta.contractEndDate'] }
            ]
          },
          then: '$endDate',
          else: {
            $dateAdd: {
              startDate: '$endDate',
              unit: 'day',
              amount: '$partnerSettings.invoiceDueDays',
              timezone: '$partnerSettings.dateTimeSettings.timezone'
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      daysDiff: {
        $dateDiff: {
          startDate: '$startDate',
          endDate: '$endDate',
          unit: 'month',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      daysDiff: {
        $cond: {
          if: {
            $ifNull: ['$daysDiff', false]
          },
          then: {
            $add: ['$daysDiff', 1]
          },
          else: 0
        }
      }
    }
  },
  {
    $project: {
      partnerId: 1,
      contractId: '$_id',
      propertyId: 1,
      partnerSettings: 1,
      today: 1,
      'rentalMeta.contractEndDate': 1,
      'rentalMeta.dueDate': 1,
      'rentalMeta.invoiceStartFrom': 1,
      invoiceLen: {
        $size: '$invoice'
      },
      invoice: 1,
      endDate: 1,
      daysDiff: 1,
      invoiceFrequency: {
        $cond: {
          if: { $ifNull: ['$rentalMeta.invoiceFrequency', false] },
          then: '$rentalMeta.invoiceFrequency',
          else: 1
        }
      },
      months: {
        $map: {
          input: { $range: [0, '$daysDiff'] },
          in: {
            $dateAdd: {
              startDate: '$startDate',
              unit: 'month',
              amount: '$$this',
              timezone: '$partnerSettings.dateTimeSettings.timezone'
            }
          }
        }
      }
    }
  }
]

export const queryLandlordInvoices = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  const { propertyId = '', requestFrom = '' } = query
  const totalDocumentsQuery = {
    partnerId,
    isFinalSettlement: { $ne: true },
    invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] }
  }
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    totalDocumentsQuery.propertyId = propertyId
  }
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)
  const invoiceQuery = await prepareLandlordInvoiceQuery(query)
  if (!size(invoiceQuery)) {
    return []
  }
  body.query = invoiceQuery
  const invoices = await getLandlordInvoicesForQuery(body)
  const filteredDocuments = await countInvoices(body.query)
  const totalDocuments = await countInvoices(totalDocumentsQuery)
  return {
    data: invoices,
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

const prepareQueryForInvoiceSummary = async (body) => {
  const { invoiceType } = body
  let preparedQuery = {}
  if (invoiceType === 'landlord_invoice') {
    preparedQuery = await prepareLandlordInvoiceQuery(body)
    preparedQuery.isLandlordView = true
  } else if (invoiceType === 'invoice') {
    preparedQuery = await prepareInvoiceQuery(body)
  }
  return preparedQuery
}

export const getInvoiceSummary = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['invoiceType'], body)
  const { partnerId } = user
  body.partnerId = partnerId
  const preparedQuery = await prepareQueryForInvoiceSummary(body)
  return await getTotalInvoiceAmount(preparedQuery)
}

export const getInvoiceSummaryForPartnerDashboard = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  const preparedQuery = dashboardHelper.prepareQueryForPartnerDashboard(body)
  return await getPartnerDashboardInvoiceSummaryData(preparedQuery)
}

const getInvoiceIdsByAggregate = async (query) => {
  const invoices = await InvoiceCollection.aggregate([
    {
      $match: query
    },
    {
      $group: {
        _id: null,
        invoiceIds: { $addToSet: '$_id' }
      }
    }
  ])
  const [invoicesInfo = {}] = invoices || []
  const { invoiceIds = [] } = invoicesInfo
  return invoiceIds
}

export const getRentInvoiceIdsForLegacyTransaction = async (partnerId) => {
  const invoiceType = { $in: ['invoice', 'credit_note'] }
  const query = {
    partnerId,
    invoiceType
  }
  const invoiceIds = await getInvoiceIdsByAggregate(query)
  return invoiceIds
}

export const getLandlordInvoiceIdsForLegacyTransaction = async (partnerId) => {
  const invoiceType = { $in: ['landlord_invoice', 'landlord_credit_note'] }
  const query = {
    partnerId,
    invoiceType
  }
  const invoiceIds = await getInvoiceIdsByAggregate(query)
  return invoiceIds
}

export const getLostInvoiceIdsForLegacyTransaction = async (partnerId) => {
  const query = {
    partnerId,
    lostMeta: { $exists: true },
    invoiceType: { $in: ['invoice', 'credit_note'] }
  }
  const invoiceIds = await getInvoiceIdsByAggregate(query)
  return invoiceIds
}

const getCreatedAtForReminders = (invoiceInfo, event) => {
  const { secondReminderSentAt, firstReminderSentAt, collectionNoticeSentAt } =
    invoiceInfo
  if (
    event === 'send_second_reminder' ||
    event === 'send_landlord_first_reminder'
  ) {
    return secondReminderSentAt
  } else if (
    event === 'send_collection_notice' ||
    event === 'send_landlord_collection_notice'
  ) {
    return collectionNoticeSentAt
  }
  return firstReminderSentAt
}

const prepareBasicTransactionDataForNotices = async (params, session) => {
  const { body, invoiceInfo, findFeeMeta } = params
  let transactionData = pick(invoiceInfo, [
    'partnerId',
    'contractId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'tenantId',
    'landlordInvoiceId',
    'createdBy'
  ])
  const { _id, partnerId } = invoiceInfo || {}
  transactionData.invoiceId = _id
  transactionData.amount = findFeeMeta.total
  transactionData.type = 'invoice'
  transactionData.period =
    await transactionHelper.getFormattedTransactionPeriod(new Date(), partnerId)
  const { callFromUpgradeScript, eventType, accountingType } = body
  if (callFromUpgradeScript) {
    transactionData.createdAt = getCreatedAtForReminders(invoiceInfo, eventType)
  }
  const accountingParams = {
    partnerId,
    accountingType
  }

  const accountingData =
    await transactionHelper.getAccountingDataForTransaction(
      accountingParams,
      session
    )
  transactionData = extend(transactionData, accountingData)
  return transactionData
}

export const prepareInvoiceRemindersTransactionData = async (
  params,
  session
) => {
  const { invoiceInfo } = params
  const { landlordInvoiceId = '' } = invoiceInfo
  const transactionData = await prepareBasicTransactionDataForNotices(
    params,
    session
  )
  transactionData.landlordInvoiceId = landlordInvoiceId
  return transactionData
}

export const prepareTransactionDataForEvictionNotice = async (
  params,
  session
) => {
  const transactionData = await prepareBasicTransactionDataForNotices(
    params,
    session
  )
  return transactionData
}

export const prepareLossRecognitionTransactionData = async (
  invoiceInfo,
  { amount, date }
) => {
  let transactionData = pick(invoiceInfo, [
    'partnerId',
    'contractId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'tenantId',
    'createdBy'
  ])
  const { _id, partnerId } = invoiceInfo
  transactionData.amount = amount
  transactionData.invoiceId = _id
  transactionData.type = 'invoice'
  transactionData.period =
    await transactionHelper.getFormattedTransactionPeriod(date, partnerId)
  const accountingParams = {
    partnerId,
    accountingType: 'loss_recognition'
  }
  const accountingData =
    await transactionHelper.getAccountingDataForTransaction(accountingParams)
  transactionData = extend(transactionData, accountingData)
  return transactionData
}

const getAccountingTypeByFeeType = (type) => {
  const types = {
    reminder: 'invoice_reminder_fee',
    eviction_notice: 'eviction_notice_fee',
    administration_eviction_notice: 'administration_eviction_notice_fee',
    collection_notice: 'collection_notice_fee'
  }
  return types[type] || ''
}

export const prepareTransactionDataForRevertInvoiceFees = async (
  params,
  session
) => {
  const { invoiceInfo, removedFee, reminderFee } = params
  const { type } = removedFee
  let transactionData = pick(invoiceInfo, [
    'partnerId',
    'contractId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'tenantId',
    'createdBy'
  ])
  const { _id, invoiceStartOn, partnerId } = invoiceInfo
  transactionData.invoiceId = _id
  transactionData.type = 'invoice'
  transactionData.amount = reminderFee
  transactionData.period =
    (await transactionHelper.getFormattedTransactionPeriod(
      invoiceStartOn,
      partnerId
    )) || ''
  const accountingType = getAccountingTypeByFeeType(type)
  const accountingParams = {
    partnerId,
    accountingType
  }
  const accountingData =
    await transactionHelper.getAccountingDataForTransaction(
      accountingParams,
      session
    )
  transactionData = extend(transactionData, accountingData)
  return transactionData
}

export const checkExistingTransaction = async (body, total, session) => {
  const { invoiceIds = [], partnerId = '', accountingType = '' } = body
  const [invoiceId] = invoiceIds
  const existsTransactionQuery = {
    partnerId,
    invoiceId,
    type: 'invoice',
    subType: accountingType,
    amount: total
  }
  const isExistsTransaction = !!(await transactionHelper.getTransaction(
    existsTransactionQuery,
    session
  ))
  return isExistsTransaction
}

export const validateRequiredDataForSerialIdsCreation = (body) => {
  const { params } = body
  appHelper.checkRequiredFields(['params'], body)
  if (!size(params)) throw new CustomError(400, 'Missing params')
  const { accountId, collectionNameStr, partnerId, isAccountWiseSerialId } =
    params
  if (!partnerId) throw new CustomError(400, 'Missing partnerId')
  if (!collectionNameStr)
    throw new CustomError(400, 'Missing collectionNameStr')
  if (isAccountWiseSerialId && !accountId)
    throw new CustomError(400, 'Required accountId for direct partner')
}

export const getInvoiceWithFileForVippsHelper = async (req) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['invoiceId'], query)
  const { invoiceId } = query
  appHelper.validateId({ invoiceId })
  const pipeline = preparePipelineForGettingInvoiceWithFiles({ invoiceId })
  const invoiceResult = await InvoiceCollection.aggregate(pipeline)
  const invoice = invoiceResult[0]
  if (!size(invoice?.files))
    return {
      msg: 'Invoice do not has any files',
      code: 'Error'
    }
  invoice.files.fileUrl = await fileHelper.getFileUrl(invoice.files, 300)
  delete invoice.files.events
  return invoice
}

export const getVippsInvoiceData = async (req) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['invoiceId', 'partnerId'], query)
  const { invoiceId, partnerId } = query
  appHelper.validateId({ invoiceId })
  appHelper.validateId({ partnerId })
  const pipeline = preparePipelineForGettingVippsInvoiceId(query)

  const invoice = await InvoiceCollection.aggregate(pipeline)
  const vippsData = invoice[0] || {}
  if (
    !vippsData.vippsInvoiceId &&
    !(
      vippsData.vippsInvoiceId.includes(vippsData.sender.orgId) &&
      vippsData.vippsInvoiceId.includes(vippsData.organizationId)
    )
  )
    return false
  return vippsData
}

const preparePipelineForGettingInvoiceWithFiles = ({ invoiceId }) => {
  const pipeline = [
    {
      $match: {
        _id: invoiceId
      }
    },
    {
      $unwind: '$pdf'
    },
    {
      $match: {
        'pdf.type': 'invoice_pdf'
      }
    },
    {
      $lookup: {
        from: 'files',
        foreignField: '_id',
        localField: 'pdf.fileId',
        as: 'files'
      }
    },
    {
      $project: {
        _id: 1,
        partnerId: 1,
        pdf: 1,
        files: {
          $first: '$files'
        }
      }
    }
  ]
  return pipeline
}

const preparePipelineForGettingVippsInvoiceId = ({ invoiceId }) => {
  const pipeline = [
    {
      $match: {
        _id: invoiceId
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        foreignField: 'partnerId',
        localField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $unwind: '$partnerSettings'
    },
    {
      $project: {
        sender: 1,
        organizationId: '$partnerSettings.companyInfo.organizationId',
        vippsStatus: 1,
        vippsInvoiceId: {
          $concat: [
            'org-no.',
            {
              $switch: {
                branches: [
                  {
                    case: {
                      $ifNull: [
                        '$partnerSettings.companyInfo.organizationId',
                        false
                      ]
                    },
                    then: '$partnerSettings.companyInfo.organizationId'
                  },
                  {
                    case: {
                      $ifNull: ['$sender.orgId', false]
                    },
                    then: '$sender.orgId'
                  }
                ]
              }
            },
            '.',
            '$_id'
          ]
        }
      }
    }
  ]
  return pipeline
}

const preparePipelineForInvoiceDataForVipps = ({ invoiceId }) => {
  console.log('Fetching data for invoice', invoiceId)
  const pipelineArr = [
    {
      $match: {
        _id: invoiceId
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        foreignField: 'partnerId',
        localField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $lookup: {
        from: 'partners',
        foreignField: '_id',
        localField: 'partnerId',
        as: 'partners'
      }
    },
    {
      $lookup: {
        from: 'tenants',
        foreignField: '_id',
        localField: 'tenantId',
        as: 'tenants'
      }
    },
    {
      $addFields: {
        pdf: { $first: '$pdf' }
      }
    },
    {
      $match: {
        'pdf.type': 'invoice_pdf'
      }
    },
    {
      $lookup: {
        from: 'files',
        foreignField: '_id',
        localField: 'pdf.fileId',
        as: 'files'
      }
    },
    {
      $addFields: {
        tenants: {
          $first: '$tenants'
        },
        partners: {
          $first: '$partners'
        },
        partnerSettings: {
          $first: '$partnerSettings'
        },
        files: {
          $first: '$files'
        }
      }
    },
    {
      $lookup: {
        from: 'users',
        foreignField: '_id',
        localField: 'tenants.userId',
        as: 'users'
      }
    },
    {
      $addFields: {
        users: {
          $first: '$users'
        },
        dueDateWillBe: {
          $dateAdd: {
            startDate: new Date(),
            unit: 'day',
            amount: 3,
            timezone: '$partnerSettings.dateTimeSettings.timezone'
          }
        },
        vippsInvoiceId: {
          $switch: {
            branches: [
              {
                case: {
                  $ifNull: ['$sender.orgId', false]
                },
                then: '$sender.orgId'
              },
              {
                case: {
                  $ifNull: [
                    '$partnerSettings.companyInfo.organizationId',
                    false
                  ]
                },
                then: '$partnerSettings.companyInfo.organizationId'
              }
            ],
            default: ''
          }
        }
      }
    },
    {
      $project: {
        partnerId: 1,
        accountId: 1,
        propertyId: 1,
        agentId: 1,
        branchId: 1,
        vippsStatus: 1,
        contractId: 1,
        vippsInvoiceId: {
          $replaceAll: {
            input: '$vippsInvoiceId',
            find: ' ',
            replacement: ''
          }
        },
        dueDateWillBe: 1,
        tenants: 1,
        kidNumber: 1,
        invoiceAccountNumber: 1,
        tenantId: 1,
        partnerSettings: 1,
        amount: {
          $multiply: ['$invoiceTotal', 100]
        },
        pdf: 1,
        recipientUserInfoLanguage: '$user.profile.language',
        dueDate: 1,
        MSISDN: {
          $cond: {
            if: {
              $and: [
                {
                  $ifNull: [
                    '$users.profile.norwegianNationalIdentification',
                    false
                  ]
                },
                { $ne: ['$users.profile.norwegianNationalIdentification', ''] },
                {
                  $ne: ['$users.profile.norwegianNationalIdentification', null]
                },
                {
                  $ne: [
                    '$users.profile.norwegianNationalIdentification',
                    undefined
                  ]
                }
              ]
            },
            then: '$users.profile.norwegianNationalIdentification',

            else: {
              $replaceOne: {
                input: '$users.profile.phoneNumber',
                find: '+',
                replacement: ''
              }
            }
          }
        },
        invoiceSerialId: 1,
        sender: 1,
        users: 1,
        issuerName: {
          $switch: {
            branches: [
              {
                case: { $ifNull: ['$sender.companyName', false] },
                then: '$sender.companyName'
              },
              {
                case: {
                  $ifNull: ['$partnerSettings.companyInfo.companyName', false]
                },
                then: '$partnerSettings.companyInfo.companyName'
              }
            ],
            default: ''
          }
        },
        files: 1
      }
    },
    {
      $project: {
        recipientUserInfoLanguage: '$user.profile.language',
        partnerId: '$partnerId',
        accountId: '$accountId',
        propertyId: '$propertyId',
        agentId: '$agentId',
        branchId: '$branchId',
        contractId: '$contractId',
        vippsInvoiceId: {
          $concat: ['orgno-no.', '$vippsInvoiceId', '.', '$_id']
        },
        sender: 1,
        tenants: 1,
        vippsStatus: 1,
        organizationId: {
          $replaceAll: {
            input: '$partnerSettings.companyInfo.organizationId',
            find: ' ',
            replacement: ''
          }
        },
        dueDateWillBe: 1,
        users: 1,
        MSISDN: 1,
        nin_no: '$users.profile.norwegianNationalIdentification',
        invoiceSerialId: 1,
        paymentInformation: {
          type: 'kid',
          value: '$kidNumber',
          account: '$invoiceAccountNumber'
        },
        invoiceType: 'invoice',
        due: '$dueDate',
        amount: 1,
        kidNumber: 1,
        issuerName: 1,
        invoicePdfFileId: '$pdf.fileId',
        metadata: {
          partnerId: '$partnerId',
          invoiceId: '$_id',
          tenantId: '$tenantId'
        },
        files: 1
      }
    }
  ]
  return pipelineArr
}

const getInvoiceDetailsFirstProjectPipeline = (isMoreThanOneBranch) => [
  {
    $project: {
      _id: 1,
      invoiceSerialId: 1,
      propertyInfo: {
        _id: 1,
        location: {
          name: 1,
          city: 1,
          country: 1,
          postalCode: 1
        },
        listingTypeId: 1,
        propertyTypeId: 1,
        apartmentId: 1,
        floor: 1
      },
      agentInfo: {
        _id: 1,
        name: 1,
        avatarKey: 1
      },
      tenantInfo: {
        _id: 1,
        name: 1,
        avatarKey: 1
      },
      otherTenantsInfo: 1,
      accountInfo: {
        _id: 1,
        name: 1,
        avatarKey: 1
      },
      payments: {
        _id: 1,
        paymentDate: 1,
        amount: 1,
        paymentInvoiceAmount: 1,
        status: 1
      },
      creditNoteInvoiceInfo: {
        _id: 1,
        invoiceSerialId: 1
      },
      creditNoteInvoicesInfo: {
        _id: 1,
        invoiceSerialId: 1
      },
      branchInfo: {
        $cond: [{ $eq: [isMoreThanOneBranch, true] }, '$branchInfo', null]
      },
      invoiceId: 1,
      invoiceStartOn: 1,
      invoiceEndOn: 1,
      dueDate: {
        $cond: [{ $not: { $ifNull: ['$invoiceId', false] } }, '$dueDate', null]
      },
      kidNumber: 1,
      invoiceAccountNumber: 1,
      createdAt: 1,
      S3PdfFileId: '$pdfFile.fileId',
      isVisibleLostButton: {
        $cond: [
          {
            $and: [
              {
                $not: { $in: ['$status', ['lost', 'credited']] }
              },
              {
                $or: [
                  { $eq: ['$status', 'overdue'] },
                  { $eq: ['$isDefaulted', true] }
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      invoiceTotal: 1,
      // For invoice summary
      // For invoice content
      isCorrectionInvoice: 1,
      contractId: 1,
      invoiceContent: {
        type: 1,
        total: 1
      },
      // For addonsMeta
      addonsMeta: {
        addonId: 1,
        correctionId: 1,
        description: 1,
        total: 1,
        payoutsIds: 1
      },
      // Fees meta
      feesMeta: {
        type: 1,
        invoiceId: 1,
        total: 1
      },
      // Commissions meta
      commissionsMeta: {
        type: 1,
        commissionId: 1,
        total: 1,
        payoutsIds: 1
      },
      // For others
      totalBalanced: 1,
      creditedAmount: 1,
      totalPaid: 1,
      lostAmount: '$lostMeta.amount',
      lastPaymentDate: 1,
      // For generating invoice status
      invoiceType: 1,
      status: 1,
      invoiceSent: 1,
      isPartiallyPaid: 1,
      isDefaulted: 1,
      secondReminderSentAt: 1,
      firstReminderSentAt: 1,
      isOverPaid: 1,
      isPartiallyCredited: 1,
      isPartiallyBalanced: 1,
      delayDate: 1,
      evictionNoticeSent: 1,
      evictionDueReminderSent: 1,
      vippsStatus: 1,
      enabledNotification: 1,
      isCollectionNoticeSent: {
        $cond: [{ $ifNull: ['$collectionNoticeSentAt', false] }, true, false]
      },
      compelloStatus: 1
    }
  }
]

const getInvoicePaymentsPipeline = () => [
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'invoices.invoiceId',
      let: { mainId: '$_id' },
      pipeline: [
        {
          $match: {
            type: 'payment'
          }
        },
        {
          $sort: { paymentDate: 1 }
        },
        {
          $addFields: {
            mainId: '$$mainId'
          }
        },
        {
          $addFields: {
            paymentInvoice: {
              $first: {
                $filter: {
                  input: { $ifNull: ['$invoices', []] },
                  as: 'invoice',
                  cond: {
                    $eq: ['$$invoice.invoiceId', '$mainId']
                  }
                }
              }
            }
          }
        },
        {
          $addFields: {
            paymentInvoiceAmount: '$paymentInvoice.amount'
          }
        }
      ],
      as: 'payments'
    }
  }
]

const getPdfFileUrlPipeline = () => [
  {
    $addFields: {
      pdfFileType: {
        $concat: ['$invoiceType', '_pdf']
      }
    }
  },
  {
    $addFields: {
      pdfFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$pdf', []] },
            as: 'pdfInfo',
            cond: {
              $eq: ['$$pdfInfo.type', '$pdfFileType']
            }
          }
        }
      }
    }
  }
]

const getFeesMetaPipeline = () => [
  appHelper.getUnwindPipeline('feesMeta'),
  {
    $lookup: {
      from: 'invoices',
      localField: 'feesMeta.invoiceId',
      foreignField: '_id',
      as: 'feesInvoice'
    }
  },
  appHelper.getUnwindPipeline('feesInvoice'),
  {
    $group: {
      _id: '$_id',
      feesMeta: {
        $push: {
          type: '$feesMeta.type',
          invoiceId: '$feesMeta.invoiceId',
          total: '$feesMeta.total',
          invoiceSerialId: '$feesInvoice.invoiceSerialId'
        }
      },
      invoiceSerialId: {
        $first: '$invoiceSerialId'
      },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      agentInfo: {
        $first: '$agentInfo'
      },
      tenantInfo: {
        $first: '$tenantInfo'
      },
      otherTenantsInfo: {
        $first: '$otherTenantsInfo'
      },
      accountInfo: {
        $first: '$accountInfo'
      },
      payments: {
        $first: '$payments'
      },
      creditNoteInvoiceInfo: {
        $first: '$creditNoteInvoiceInfo'
      },
      creditNoteInvoicesInfo: {
        $first: '$creditNoteInvoicesInfo'
      },
      branchInfo: {
        $first: '$branchInfo'
      },
      invoiceType: {
        $first: '$invoiceType'
      },
      invoiceId: {
        $first: '$invoiceId'
      },
      invoiceStartOn: {
        $first: '$invoiceStartOn'
      },
      invoiceEndOn: {
        $first: '$invoiceEndOn'
      },
      dueDate: {
        $first: '$dueDate'
      },
      kidNumber: {
        $first: '$kidNumber'
      },
      invoiceAccountNumber: {
        $first: '$invoiceAccountNumber'
      },
      createdAt: {
        $first: '$createdAt'
      },
      S3PdfFileId: {
        $first: '$S3PdfFileId'
      },
      isVisibleLostButton: {
        $first: '$isVisibleLostButton'
      },
      invoiceTotal: {
        $first: '$invoiceTotal'
      },
      isCorrectionInvoice: {
        $first: '$isCorrectionInvoice'
      },
      contractId: {
        $first: '$contractId'
      },
      invoiceContent: {
        $first: '$invoiceContent'
      },
      addonsMeta: {
        $first: '$addonsMeta'
      },
      commissionsMeta: {
        $first: '$commissionsMeta'
      },
      totalBalanced: {
        $first: '$totalBalanced'
      },
      creditedAmount: {
        $first: '$creditedAmount'
      },
      totalPaid: {
        $first: '$totalPaid'
      },
      lostAmount: {
        $first: '$lostAmount'
      },
      lastPaymentDate: {
        $first: '$lastPaymentDate'
      },
      // For generating invoice status
      status: {
        $first: '$status'
      },
      invoiceSent: {
        $first: '$invoiceSent'
      },
      isPartiallyPaid: {
        $first: '$isPartiallyPaid'
      },
      isDefaulted: {
        $first: '$isDefaulted'
      },
      secondReminderSentAt: {
        $first: '$secondReminderSentAt'
      },
      firstReminderSentAt: {
        $first: '$firstReminderSentAt'
      },
      isOverPaid: {
        $first: '$isOverPaid'
      },
      isPartiallyCredited: {
        $first: '$isPartiallyCredited'
      },
      isPartiallyBalanced: {
        $first: '$isPartiallyBalanced'
      },
      delayDate: {
        $first: '$delayDate'
      },
      evictionNoticeSent: {
        $first: '$evictionNoticeSent'
      },
      evictionDueReminderSent: {
        $first: '$evictionDueReminderSent'
      },
      vippsStatus: {
        $first: '$vippsStatus'
      },
      enabledNotification: {
        $first: '$enabledNotification'
      },
      isCollectionNoticeSent: {
        $first: '$isCollectionNoticeSent'
      },
      compelloStatus: {
        $first: '$compelloStatus'
      }
    }
  }
]

const getCommissionsMetaPipeline = () => [
  appHelper.getUnwindPipeline('commissionsMeta'),
  {
    $lookup: {
      from: 'commissions',
      localField: 'commissionsMeta.commissionId',
      foreignField: '_id',
      as: 'commissionInfo'
    }
  },
  appHelper.getUnwindPipeline('commissionInfo'),
  {
    $lookup: {
      from: 'commissions',
      localField: 'commissionInfo.commissionId',
      foreignField: '_id',
      as: 'creditNoteCommissionInfo'
    }
  },
  appHelper.getUnwindPipeline('creditNoteCommissionInfo'),
  {
    $addFields: {
      commissionAddonId: {
        $cond: [
          { $ifNull: ['$commissionInfo.addonId', false] },
          '$commissionInfo.addonId',
          '$creditNoteCommissionInfo.addonId'
        ]
      }
    }
  },
  {
    $lookup: {
      from: 'products_services',
      localField: 'commissionAddonId',
      foreignField: '_id',
      as: 'commissionAddonInfo'
    }
  },
  appHelper.getUnwindPipeline('commissionAddonInfo'),
  {
    $group: {
      _id: '$_id',
      feesMeta: {
        $first: '$feesMeta'
      },
      invoiceSerialId: {
        $first: '$invoiceSerialId'
      },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      agentInfo: {
        $first: '$agentInfo'
      },
      tenantInfo: {
        $first: '$tenantInfo'
      },
      otherTenantsInfo: {
        $first: '$otherTenantsInfo'
      },
      accountInfo: {
        $first: '$accountInfo'
      },
      payments: {
        $first: '$payments'
      },
      creditNoteInvoiceInfo: {
        $first: '$creditNoteInvoiceInfo'
      },
      creditNoteInvoicesInfo: {
        $first: '$creditNoteInvoicesInfo'
      },
      branchInfo: {
        $first: '$branchInfo'
      },
      invoiceType: {
        $first: '$invoiceType'
      },
      invoiceId: {
        $first: '$invoiceId'
      },
      invoiceStartOn: {
        $first: '$invoiceStartOn'
      },
      invoiceEndOn: {
        $first: '$invoiceEndOn'
      },
      dueDate: {
        $first: '$dueDate'
      },
      kidNumber: {
        $first: '$kidNumber'
      },
      invoiceAccountNumber: {
        $first: '$invoiceAccountNumber'
      },
      createdAt: {
        $first: '$createdAt'
      },
      S3PdfFileId: {
        $first: '$S3PdfFileId'
      },
      isVisibleLostButton: {
        $first: '$isVisibleLostButton'
      },
      invoiceTotal: {
        $first: '$invoiceTotal'
      },
      isCorrectionInvoice: {
        $first: '$isCorrectionInvoice'
      },
      contractId: {
        $first: '$contractId'
      },
      invoiceContent: {
        $first: '$invoiceContent'
      },
      addonsMeta: {
        $first: '$addonsMeta'
      },
      commissionsMeta: {
        $push: {
          type: '$commissionsMeta.type',
          commissionId: '$commissionsMeta.commissionId',
          total: '$commissionsMeta.total',
          addonName: '$commissionAddonInfo.name'
        }
      },
      commissionsPayoutIds: {
        $push: '$commissionsMeta.payoutsIds'
      },
      totalBalanced: {
        $first: '$totalBalanced'
      },
      creditedAmount: {
        $first: '$creditedAmount'
      },
      totalPaid: {
        $first: '$totalPaid'
      },
      lostAmount: {
        $first: '$lostAmount'
      },
      lastPaymentDate: {
        $first: '$lastPaymentDate'
      },
      // For generating invoice status
      status: {
        $first: '$status'
      },
      invoiceSent: {
        $first: '$invoiceSent'
      },
      isPartiallyPaid: {
        $first: '$isPartiallyPaid'
      },
      isDefaulted: {
        $first: '$isDefaulted'
      },
      secondReminderSentAt: {
        $first: '$secondReminderSentAt'
      },
      firstReminderSentAt: {
        $first: '$firstReminderSentAt'
      },
      isOverPaid: {
        $first: '$isOverPaid'
      },
      isPartiallyCredited: {
        $first: '$isPartiallyCredited'
      },
      isPartiallyBalanced: {
        $first: '$isPartiallyBalanced'
      },
      delayDate: {
        $first: '$delayDate'
      },
      evictionNoticeSent: {
        $first: '$evictionNoticeSent'
      },
      evictionDueReminderSent: {
        $first: '$evictionDueReminderSent'
      },
      vippsStatus: {
        $first: '$vippsStatus'
      },
      enabledNotification: {
        $first: '$enabledNotification'
      },
      isCollectionNoticeSent: {
        $first: '$isCollectionNoticeSent'
      },
      compelloStatus: {
        $first: '$compelloStatus'
      }
    }
  },
  {
    $addFields: {
      commissionsPayoutIds: {
        $reduce: {
          input: { $ifNull: ['$commissionsPayoutIds', []] },
          initialValue: [],
          in: {
            $concatArrays: ['$$value', '$$this']
          }
        }
      }
    }
  }
]

const getPayoutsPipeline = () => [
  {
    $addFields: {
      payoutsIds: {
        $setUnion: ['$addonsPayoutIds', '$commissionsPayoutIds']
      }
    }
  },
  {
    $lookup: {
      from: 'payouts',
      localField: 'payoutsIds',
      foreignField: '_id',
      as: 'payoutsInfo'
    }
  }
]

const getInvoiceDetailsFinalProjectPipeline = () => [
  {
    $project: {
      _id: 1,
      feesMeta: 1,
      invoiceSerialId: 1,
      leaseSerial: '$contractInfo.leaseSerial',
      propertyInfo: 1,
      agentInfo: 1,
      tenantInfo: 1,
      otherTenantsInfo: 1,
      accountInfo: 1,
      payments: 1,
      creditNoteInvoicesInfo: 1,
      creditNoteInvoiceInfo: 1,
      branchInfo: 1,
      invoiceId: 1,
      invoiceStartOn: 1,
      invoiceEndOn: 1,
      dueDate: 1,
      kidNumber: 1,
      invoiceAccountNumber: 1,
      createdAt: 1,
      S3PdfFileId: 1,
      isVisibleLostButton: 1,
      invoiceTotal: 1,
      isCorrectionInvoice: 1,
      contractId: 1,
      invoiceContent: 1,
      addonsMeta: 1,
      commissionsMeta: 1,
      payoutsInfo: {
        _id: 1,
        serialId: 1
      },
      showRefundOption: 1,
      lastPaymentDate: 1,
      paymentId: '$invoicePaymentInfo._id',
      isFinalSettlementDone: '$contractInfo.isFinalSettlementDone',
      totalBalanced: 1,
      creditedAmount: 1,
      totalPaid: 1,
      lostAmount: 1,
      totalDue: {
        $cond: [
          {
            $in: ['$invoiceType', ['landlord_invoice', 'landlord_credit_note']]
          },
          {
            $subtract: [
              { $ifNull: ['$invoiceTotal', 0] },
              {
                $add: [
                  { $ifNull: ['$totalPaid', 0] },
                  { $ifNull: ['$totalBalanced', 0] }
                ]
              }
            ]
          },
          {
            $cond: [
              { $eq: ['$invoiceType', 'credit_note'] },
              0,
              {
                $subtract: [
                  {
                    $add: [
                      { $ifNull: ['$invoiceTotal', 0] },
                      { $ifNull: ['$creditedAmount', 0] }
                    ]
                  },
                  {
                    $add: [
                      { $ifNull: ['$totalPaid', 0] },
                      { $ifNull: ['$lostAmount', 0] }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      },
      isBranchAdminUser: '$branchInfo.isBranchAdminUser',
      // For generating invoice status
      invoiceType: 1,
      status: 1,
      invoiceSent: 1,
      isPartiallyPaid: 1,
      isDefaulted: 1,
      secondReminderSentAt: 1,
      firstReminderSentAt: 1,
      isOverPaid: 1,
      isPartiallyCredited: 1,
      isPartiallyBalanced: 1,
      delayDate: 1,
      evictionNoticeSent: 1,
      evictionDueReminderSent: 1,
      vippsStatus: 1,
      enabledNotification: 1,
      isCollectionNoticeSent: 1,
      compelloStatus: 1
    }
  }
]

const getCreditNoteInvoicesInfoPipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'creditNoteIds',
      foreignField: '_id',
      as: 'creditNoteInvoicesInfo'
    }
  }
]

const getCreditNoteInvoiceInfoPipeline = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'invoiceId',
      foreignField: '_id',
      as: 'creditNoteInvoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('creditNoteInvoiceInfo')
]

const getAddonsMetaPipeline = () => [
  appHelper.getUnwindPipeline('addonsMeta'),
  {
    $group: {
      _id: '$_id',
      feesMeta: {
        $first: '$feesMeta'
      },
      invoiceSerialId: {
        $first: '$invoiceSerialId'
      },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      agentInfo: {
        $first: '$agentInfo'
      },
      tenantInfo: {
        $first: '$tenantInfo'
      },
      otherTenantsInfo: {
        $first: '$otherTenantsInfo'
      },
      accountInfo: {
        $first: '$accountInfo'
      },
      payments: {
        $first: '$payments'
      },
      creditNoteInvoiceInfo: {
        $first: '$creditNoteInvoiceInfo'
      },
      creditNoteInvoicesInfo: {
        $first: '$creditNoteInvoicesInfo'
      },
      branchInfo: {
        $first: '$branchInfo'
      },
      invoiceType: {
        $first: '$invoiceType'
      },
      invoiceId: {
        $first: '$invoiceId'
      },
      invoiceStartOn: {
        $first: '$invoiceStartOn'
      },
      invoiceEndOn: {
        $first: '$invoiceEndOn'
      },
      dueDate: {
        $first: '$dueDate'
      },
      kidNumber: {
        $first: '$kidNumber'
      },
      invoiceAccountNumber: {
        $first: '$invoiceAccountNumber'
      },
      lastPaymentDate: {
        $first: '$lastPaymentDate'
      },
      createdAt: {
        $first: '$createdAt'
      },
      S3PdfFileId: {
        $first: '$S3PdfFileId'
      },
      isVisibleLostButton: {
        $first: '$isVisibleLostButton'
      },
      invoiceTotal: {
        $first: '$invoiceTotal'
      },
      isCorrectionInvoice: {
        $first: '$isCorrectionInvoice'
      },
      contractId: {
        $first: '$contractId'
      },
      invoiceContent: {
        $first: '$invoiceContent'
      },
      commissionsMeta: {
        $first: '$commissionsMeta'
      },
      commissionsPayoutIds: {
        $first: '$commissionsPayoutIds'
      },
      totalBalanced: {
        $first: '$totalBalanced'
      },
      creditedAmount: {
        $first: '$creditedAmount'
      },
      totalPaid: {
        $first: '$totalPaid'
      },
      lostAmount: {
        $first: '$lostAmount'
      },
      addonsMeta: {
        $push: {
          addonId: '$addonsMeta.addonId',
          correctionId: '$addonsMeta.correctionId',
          description: '$addonsMeta.description',
          total: '$addonsMeta.total'
        }
      },
      addonsPayoutIds: {
        $push: '$addonsMeta.payoutsIds'
      },
      // For generating invoice status
      status: {
        $first: '$status'
      },
      invoiceSent: {
        $first: '$invoiceSent'
      },
      isPartiallyPaid: {
        $first: '$isPartiallyPaid'
      },
      isDefaulted: {
        $first: '$isDefaulted'
      },
      secondReminderSentAt: {
        $first: '$secondReminderSentAt'
      },
      firstReminderSentAt: {
        $first: '$firstReminderSentAt'
      },
      isOverPaid: {
        $first: '$isOverPaid'
      },
      isPartiallyCredited: {
        $first: '$isPartiallyCredited'
      },
      isPartiallyBalanced: {
        $first: '$isPartiallyBalanced'
      },
      delayDate: {
        $first: '$delayDate'
      },
      evictionNoticeSent: {
        $first: '$evictionNoticeSent'
      },
      evictionDueReminderSent: {
        $first: '$evictionDueReminderSent'
      },
      vippsStatus: {
        $first: '$vippsStatus'
      },
      enabledNotification: {
        $first: '$enabledNotification'
      },
      isCollectionNoticeSent: {
        $first: '$isCollectionNoticeSent'
      },
      compelloStatus: {
        $first: '$compelloStatus'
      }
    }
  },
  {
    $addFields: {
      addonsPayoutIds: {
        $reduce: {
          input: { $ifNull: ['$addonsPayoutIds', []] },
          initialValue: [],
          in: {
            $concatArrays: ['$$value', '$$this']
          }
        }
      }
    }
  }
]

const getOtherTenantsInfoPipeline = () => [
  {
    $addFields: {
      tenants: {
        $filter: {
          input: { $ifNull: ['$tenants', []] },
          as: 'tenant',
          cond: {
            $not: {
              $eq: ['$$tenant.tenantId', '$tenantId']
            }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenants.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'tenantUser'
          }
        },
        {
          $unwind: {
            path: '$tenantUser',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$tenantUser.profile.avatarKey'
            )
          }
        }
      ],
      as: 'otherTenantsInfo'
    }
  }
]

const prepareAggregatePipeline = ({
  invoiceId,
  partnerId,
  isMoreThanOneBranch,
  userId
}) => {
  const pipelineArr = [
    {
      $match: {
        _id: invoiceId,
        partnerId
      }
    },
    ...appHelper.getCommonPropertyInfoPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    ...getOtherTenantsInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...invoiceSummaryHelper.getBranchPipeline(userId),
    ...getInvoicePaymentsPipeline(),
    ...getPdfFileUrlPipeline(),
    ...getCreditNoteInvoicesInfoPipeline(),
    ...getCreditNoteInvoiceInfoPipeline(),
    ...getInvoiceDetailsFirstProjectPipeline(isMoreThanOneBranch),
    ...getFeesMetaPipeline(),
    ...getCommissionsMetaPipeline(),
    ...getAddonsMetaPipeline(),
    ...getPayoutsPipeline(),
    ...invoiceSummaryHelper.showRefundOption('_id'),
    ...appHelper.getCommonContractInfoPipeline('contractId'),
    ...getInvoiceDetailsFinalProjectPipeline()
  ]
  return pipelineArr
}

export const getInvoiceDetails = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  const { query } = body
  appHelper.checkRequiredFields(['invoiceId'], query)
  const { invoiceId } = query
  appHelper.validateId({ invoiceId })
  query.partnerId = partnerId
  const branches = (await branchHelper.getBranches({ partnerId })) || []
  const isMoreThanOneBranch = branches.length > 1 ? true : false
  const pipeline = prepareAggregatePipeline({
    ...query,
    isMoreThanOneBranch,
    userId
  })
  const [invoice = {}] = (await InvoiceCollection.aggregate(pipeline)) || []
  return invoice
}

export const getInvoiceForInvoiceTransactionApphealth = async (
  partnerId,
  skip
) => {
  const feesSubtype = [
    'invoice',
    'reminder',
    'collection_notice',
    'eviction_notice',
    'administration_eviction_notice',
    'invoice_fee',
    'invoice_reminder_fee',
    'collection_notice_fee',
    'eviction_notice_fee',
    'administration_eviction_notice_fee',
    'reminder_fee_move_to',
    'collection_notice_fee_move_to',
    'eviction_notice_fee_move_to',
    'administration_eviction_notice_fee_move_to',
    'unpaid_reminder',
    'unpaid_collection_notice',
    'unpaid_eviction_notice',
    'unpaid_administration_eviction_notice'
  ]
  console.log('Skip value used for the query', skip)
  const contracts = await ContractCollection.aggregate([
    {
      $match: { partnerId }
    },
    {
      $skip: skip
    },
    {
      $limit: 500
    },
    {
      $group: {
        _id: null,
        _ids: {
          $push: '$_id'
        }
      }
    }
  ])
  if (!size(contracts)) {
    return {}
  }
  const contractIds = contracts[0]?._ids
  const invoiceData = await InvoiceCollection.aggregate([
    {
      $match: {
        invoiceType: { $in: ['invoice', 'credit_note'] },
        contractId: {
          $in: contractIds
        }
      }
    },
    {
      $addFields: {
        totalFees: {
          $sum: {
            $ifNull: ['$feesMeta.total', 0]
          }
        },
        totalLost: {
          $ifNull: ['$lostMeta.amount', 0]
        }
      }
    },
    {
      $addFields: {
        invoiceAmount: {
          $round: {
            $add: ['$invoiceTotal', '$totalLost']
          }
        }
      }
    },
    {
      $unwind: {
        path: '$invoiceContent',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        totalFees: 1,
        totalLost: 1,
        invoiceAmount: 1,
        invoiceTotal: 1,
        monthlyRentTotal: {
          $cond: {
            if: { $eq: ['$invoiceContent.type', 'monthly_rent'] },
            then: {
              $ifNull: ['$invoiceContent.total', 0]
            },
            else: 0
          }
        },
        partnerId: 1
      }
    },
    {
      $lookup: {
        from: 'transactions',
        localField: '_id',
        foreignField: 'invoiceId',
        as: 'transactions'
      }
    },
    {
      $addFields: {
        transactions: {
          $filter: {
            input: '$transactions',
            as: 'item',
            cond: {
              $or: [
                {
                  $and: [
                    { $eq: ['$$item.type', 'correction'] },
                    { $ne: ['$$item.subType', 'payout_addon'] }
                  ]
                },
                {
                  $in: ['$$item.type', ['invoice', 'credit_note']]
                }
              ]
            }
          }
        }
      }
    },
    {
      $project: {
        'transactions.subType': 1,
        'transactions.amount': 1,
        'transactions._id': 1,
        totalFees: 1,
        totalLost: 1,
        invoiceAmount: 1,
        invoiceTotal: 1,
        monthlyRentTotal: 1,
        partnerId: 1
      }
    },
    {
      $addFields: {
        rentTransactions: {
          $filter: {
            input: '$transactions',
            as: 'item',
            cond: { $in: ['$$item.subType', ['rent', 'rent_with_vat']] }
          }
        },
        feesTransactions: {
          $filter: {
            input: '$transactions',
            as: 'item',
            cond: {
              $in: ['$$item.subType', feesSubtype]
            }
          }
        },
        lostTransactions: {
          $filter: {
            input: '$transactions',
            as: 'item',
            cond: { $in: ['$$item.subType', ['loss_recognition']] }
          }
        }
      }
    },
    {
      $addFields: {
        transactionsTotalAmount: {
          $sum: {
            $ifNull: ['$transactions.amount', 0]
          }
        },
        transactionRentTotal: {
          $sum: {
            $ifNull: ['$rentTransactions.amount', 0]
          }
        },
        transactionFeeTotal: {
          $sum: {
            $ifNull: ['$feesTransactions.amount', 0]
          }
        },
        transactionLostTotal: {
          $sum: {
            $ifNull: ['$lostTransactions.amount', 0]
          }
        }
      }
    },
    {
      $addFields: {
        hasErrorInRent: {
          $cond: {
            if: {
              $gt: [
                {
                  $subtract: [
                    {
                      $round: [
                        {
                          $abs: '$monthlyRentTotal'
                        },
                        2
                      ]
                    },
                    {
                      $round: [
                        {
                          $abs: '$transactionRentTotal'
                        },
                        2
                      ]
                    }
                  ]
                },
                1
              ]
            },
            then: true,
            else: false
          }
        },
        hasErrorInFees: {
          $cond: {
            if: {
              $gt: [
                {
                  $subtract: [
                    {
                      $round: [
                        {
                          $abs: '$totalFees'
                        },
                        2
                      ]
                    },
                    {
                      $round: [
                        {
                          $abs: '$transactionFeesTotal'
                        },
                        2
                      ]
                    }
                  ]
                },
                1
              ]
            },
            then: true,
            else: false
          }
        },
        hasErrorInLost: {
          $cond: {
            if: {
              $gt: [
                {
                  $subtract: [
                    {
                      $round: [
                        {
                          $abs: '$totalLost'
                        },
                        2
                      ]
                    },
                    {
                      $round: [
                        {
                          $abs: '$transactionLostTotal'
                        },
                        2
                      ]
                    }
                  ]
                },
                1
              ]
            },
            then: true,
            else: false
          }
        },
        totalDiff: {
          $subtract: [
            {
              $round: [
                {
                  $abs: '$transactionsTotalAmount'
                },
                2
              ]
            },
            {
              $round: [
                {
                  $abs: '$invoiceAmount'
                },
                2
              ]
            }
          ]
        },
        errorTransactionIds: {
          $cond: {
            if: {
              $gt: [
                {
                  $subtract: [
                    {
                      $round: ['$transactionsTotalAmount', 2]
                    },
                    {
                      $round: ['$invoiceAmount', 2]
                    }
                  ]
                },
                1
              ]
            },
            then: '$transactions._id',
            else: '$$REMOVE'
          }
        },
        hasMissingTransaction: {
          $cond: {
            if: {
              $and: [
                {
                  $eq: [
                    {
                      $size: '$transactions'
                    },
                    0
                  ]
                },
                {
                  $gte: ['$invoiceAmount', 1]
                }
              ]
            },
            then: true,
            else: false
          }
        }
      }
    },
    {
      $addFields: {
        hasErrorInTransaction: {
          $cond: {
            if: {
              $gt: [
                {
                  $size: {
                    $ifNull: ['$errorTransactionIds', []]
                  }
                },
                0
              ]
            },
            then: true,
            else: false
          }
        }
      }
    },
    {
      $group: {
        _id: null,
        totalLost: {
          $sum: '$totalLost'
        },
        totalTransactions: {
          $sum: '$transactionsTotalAmount'
        },
        totalInvoice: {
          $sum: '$invoiceTotal'
        },
        missingTransactionInvoiceIds: {
          $push: {
            $cond: {
              if: { $eq: ['$hasMissingTransaction', true] },
              then: '$_id',
              else: '$$REMOVE'
            }
          }
        },
        badTransactions: {
          $push: {
            $cond: {
              if: {
                $or: [
                  { $eq: ['$hasErrorInRent', true] },
                  { $eq: ['$hasErrorInFees', true] },
                  { $eq: ['$hasErrorInLost', true] },
                  { $eq: ['$hasErrorInTransaction', true] }
                ]
              },
              then: {
                invoiceId: '$_id',
                hasErrorInRent: '$hasErrorInRent',
                hasErrorInFees: '$hasErrorInFees',
                hasErrorInLost: '$hasErrorInLost',
                invoiceAmount: '$invoiceAmount',
                transactionTotal: '$transactionsTotalAmount',
                transactionIds: '$transactions._id',
                totalDiff: '$totalDiff',
                transactions: '$transactions'
              },
              else: '$$REMOVE'
            }
          }
        }
      }
    },
    {
      $addFields: {
        totalInvoice: {
          $add: ['$totalLost', '$totalInvoice']
        }
      }
    }
  ])
  console.log('Invoice data from db', invoiceData)
  return invoiceData[0] || {}
}

export const prepareInvoiceDataForVipps = async (req) => {
  const { body, session } = req
  const { query, option = {} } = body
  let invoiceSendToVippsData = {}
  appHelper.checkRequiredFields(['invoiceId', 'partnerId'], query)
  const { invoiceId, partnerId } = query
  appHelper.validateId({ invoiceId })
  appHelper.validateId({ partnerId })
  console.log('Using queries for fetching data', query)
  const pipeline = preparePipelineForInvoiceDataForVipps(query)
  const invoice = await InvoiceCollection.aggregate(pipeline)
  console.log('Invoice data for vipps', invoice)
  if (size(invoice)) {
    invoiceSendToVippsData = invoice[0] || {}
    if (
      !invoiceSendToVippsData.vippsInvoiceId.includes(
        invoiceSendToVippsData.sender.orgId
      ) &&
      !invoiceSendToVippsData.vippsInvoiceId.includes(
        invoiceSendToVippsData.organizationId
      ) &&
      !option.skipValidation
    ) {
      invoiceSendToVippsData.errorTextKey = 'issuer_org_id_not_found'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote: 'Issuer organization id not found',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    if (
      !invoiceSendToVippsData?.nin_no &&
      !invoiceSendToVippsData?.MSISDN &&
      !option.skipValidation
    ) {
      invoiceSendToVippsData.errorTextKey = 'msisdn_and_nin_not_found'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote: 'Recipient MSISDN and NIN not found',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    if (invoiceSendToVippsData.invoiceSerialId && !option.skipValidation) {
      invoiceSendToVippsData.subject =
        appHelper.translateToUserLng(
          'rent_invoices.vipps_regninger.subject',
          invoiceSendToVippsData.recipientUserInfoLanguage
        ) +
        ' #' +
        invoiceSendToVippsData.invoiceSerialId
    }
    if (!invoiceSendToVippsData.kidNumber && !option.skipValidation) {
      invoiceSendToVippsData.errorTextKey = 'invoice_kid_number'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote: 'Invoice kid number not found',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    if (
      !invoiceSendToVippsData.paymentInformation.account &&
      !option.skipValidation
    ) {
      invoiceSendToVippsData.errorTextKey = 'invoice_account_number'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote:
            'Account number not found and account number must be 11 digits',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    if (
      invoiceSendToVippsData?.due < invoiceSendToVippsData?.dueDateWillBe &&
      !option.skipValidation
    ) {
      invoiceSendToVippsData.errorTextKey = 'invoice_due_date'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote:
            'Invoice due date must be at least 72 hours into the future',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    if (invoiceSendToVippsData?.amount <= 0 && !option.skipValidation) {
      invoiceSendToVippsData.errorTextKey = 'invoice_has_not_amount'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote: 'Invoice has not positive amount',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    if (
      !invoiceSendToVippsData?.issuerName ||
      (invoiceSendToVippsData?.issuerName.length > 40 && !option.skipValidation)
    ) {
      invoiceSendToVippsData.errorTextKey = 'invoice_issuer_name'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote:
            'Invoice issuer name can not be longer than 40 characters',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
      return {
        msg: invoiceSendToVippsData.errorTextKey,
        code: 'Error'
      }
    }
    let fileUrlHash = ''
    if (!invoiceSendToVippsData?.files?.fileUrlHash) fileUrlHash = nid(30)
    else fileUrlHash = invoiceSendToVippsData.files.fileUrlHash

    if (fileUrlHash.length === 0 && !option.skipValidation) {
      invoiceSendToVippsData.errorTextKey = 'invoice_pdf_not_found'
      await updateInvoiceAndLogForFailedVippsHelper(
        {
          invoiceId,
          partnerId,
          vippsEventNote: 'Invoice pdf not found for send to vipps recipient',
          vippsStatus: 'failed',
          vippsEventStatus: 'failed',
          invoiceSendToVippsData
        },
        session
      )
    }
    const fileUrl = appHelper.createDownloadUrl(fileUrlHash)
    invoiceSendToVippsData.fileUrl = fileUrl
    const partnerUrl = await appHelper.getPartnerURL(
      invoiceSendToVippsData.partnerId,
      true,
      session
    )
    invoiceSendToVippsData.commercialInvoice = [
      {
        mimeType: 'application/pdf',
        url: `${partnerUrl}/api/show-invoice?invoice_id=${invoiceId}`
      }
    ]
    return invoiceSendToVippsData
  }
  return {
    msg: 'Invoice not found',
    code: 'Error'
  }
}

export const updateInvoiceAndLogForFailedVippsHelper = async (
  inputDataForUpdateInvoiceAndAddLog,
  session
) => {
  const {
    invoiceId,
    partnerId,
    vippsEventNote,
    vippsStatus,
    vippsEventStatus,
    invoiceSendToVippsData,
    action
  } = inputDataForUpdateInvoiceAndAddLog
  const updateInfo = await invoiceService.updateInvoiceInfoForVippsService(
    {
      invoiceId,
      partnerId,
      vippsStatus,
      vippsEventStatus,
      vippsEventNote
    },
    session
  )
  console.log(updateInfo)
  if (!updateInfo) {
    return {
      msg: 'Invoice Update Failed',
      code: 'Error'
    }
  }
  if (size(invoiceSendToVippsData)) {
    const actionKey = action ? action : 'invoice_sent_to_vipps_error'
    const log = await invoiceService.createInvoiceLogForVipps(
      invoiceSendToVippsData,
      actionKey,
      session
    )
    if (!log) {
      return {
        msg: 'Log Add Failed',
        code: 'Error'
      }
    }
  }
  return true
}

export const prepareDataToUpdateAnInvoiceForLambda = async (
  params = {},
  invoiceInfo = {}
) => {
  const {
    collectionNoticeDueDate,
    collectionNoticeSentAt,
    dueReminderSentAt,
    evictionDueReminderNoticeSentOn,
    evictionDueReminderSent,
    evictionNoticeSent,
    evictionNoticeSentOn,
    firstReminderSentAt,
    feesMeta,
    invoiceKidNumber,
    invoiceTotal,
    invoiceTotalTax,
    isDefaulted,
    pdf,
    pdfEvent,
    pdfStatus,
    secondReminderSentAt,
    status
  } = params || {}

  const addToSetData = {}
  const setData = {}
  const unsetData = {}

  if (collectionNoticeDueDate)
    setData.collectionNoticeDueDate = collectionNoticeDueDate
  if (collectionNoticeSentAt)
    setData.collectionNoticeSentAt = collectionNoticeSentAt
  if (dueReminderSentAt) setData.dueReminderSentAt = dueReminderSentAt
  if (evictionDueReminderSent && evictionDueReminderNoticeSentOn) {
    setData.evictionDueReminderNoticeSentOn = evictionDueReminderNoticeSentOn
    setData.evictionDueReminderSent = evictionDueReminderSent
    unsetData.evictionNoticeSent = 1
  }
  if (evictionNoticeSent && evictionNoticeSentOn) {
    setData.evictionNoticeSent = evictionNoticeSent
    setData.evictionNoticeSentOn = evictionNoticeSentOn
  }
  if (size(feesMeta)) addToSetData.feesMeta = { $each: feesMeta }
  if (firstReminderSentAt) setData.firstReminderSentAt = firstReminderSentAt
  if (invoiceKidNumber) setData.kidNumber = invoiceKidNumber
  if (invoiceTotal) {
    const { invoiceTotal: previousInvoiceTotal = 0, partner = {} } =
      invoiceInfo || {}
    const { partnerSetting = {} } = partner || {}
    const roundedInvoiceTotal =
      (await appHelper.getRoundedAmount(
        invoiceTotal + (previousInvoiceTotal || 0),
        partnerSetting
      )) || 0
    if (roundedInvoiceTotal) setData.invoiceTotal = roundedInvoiceTotal
  }
  if (invoiceTotalTax) {
    const { totalTAX: previousInvoiceTotalTax = 0 } = invoiceInfo || {}
    setData.totalTAX = await appHelper.convertTo2Decimal(
      invoiceTotalTax + (previousInvoiceTotalTax || 0)
    )
  }
  if (isDefaulted) setData.isDefaulted = isDefaulted
  if (pdfStatus) setData.pdfStatus = pdfStatus
  if (size(pdf)) addToSetData.pdf = pdf
  if (size(pdfEvent)) addToSetData.pdfEvents = pdfEvent
  if (secondReminderSentAt) setData.secondReminderSentAt = secondReminderSentAt
  if (status) {
    let canUpdateStatus
    if (status === 'created') {
      canUpdateStatus = invoiceInfo?.status === 'new'
    } else canUpdateStatus = true

    if (canUpdateStatus) setData.status = status
  }

  const updateData = {}
  if (size(addToSetData)) updateData['$addToSet'] = addToSetData
  if (size(setData)) updateData['$set'] = setData
  if (size(unsetData)) updateData['$unset'] = unsetData

  return updateData
}

const getCommonPipeLinesForReminderAndCollectionNotice = (
  query = {},
  options = {}
) => {
  const addFieldsForActualDatesOfInvoice = {
    $addFields: {
      dueDate: {
        $cond: [
          { $ifNull: ['$dueDate', false] },
          getDateFromPartsForInvoiceReminders('$dueDate'),
          false
        ]
      },
      firstReminderSentAt: {
        $cond: [
          { $ifNull: ['$firstReminderSentAt', false] },
          getDateFromPartsForInvoiceReminders('$firstReminderSentAt'),
          false
        ]
      },
      notificationSendingDate: {
        $cond: [
          { $ifNull: ['$notificationSendingDate', false] },
          getDateFromPartsForInvoiceReminders('$notificationSendingDate'),
          false
        ]
      },
      secondReminderSentAt: {
        $cond: [
          { $ifNull: ['$secondReminderSentAt', false] },
          getDateFromPartsForInvoiceReminders('$secondReminderSentAt'),
          false
        ]
      }
    }
  }
  const addFieldsForNotificationSendingDate = {
    $addFields: {
      dueDate: {
        $dateToString: {
          date: '$dueDate',
          timezone: '$timezone'
        }
      },
      firstReminderSentAt: {
        $dateToString: {
          date: '$firstReminderSentAt',
          timezone: '$timezone'
        }
      },
      notificationSendingDate: {
        $dateToString: {
          date: new Date(),
          timezone: '$timezone'
        }
      },
      secondReminderSentAt: {
        $dateToString: {
          date: '$secondReminderSentAt',
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsForPartnerSettings = {
    $addFields: {
      invoiceCollectionNoticeDays:
        '$partnerSettings.invoiceCollectionNotice.days',
      invoiceCollectionNoticeFeeAmount:
        '$partnerSettings.collectionNoticeFee.amount',
      invoiceCollectionNoticeFeeTax: {
        $multiply: [
          { $divide: ['$partnerSettings.collectionNoticeFee.tax', 100] },
          '$partnerSettings.collectionNoticeFee.amount'
        ]
      },
      invoiceCollectionNoticeNewDueDays:
        '$partnerSettings.invoiceCollectionNotice.newDueDays',
      invoiceDuePreReminderDays: '$partnerSettings.duePreReminder.days',
      invoiceFirstReminderDays: '$partnerSettings.invoiceFirstReminder.days',
      invoiceReminderFeeAmount: '$partnerSettings.reminderFee.amount',
      invoiceReminderFeeTax: {
        $multiply: [
          { $divide: ['$partnerSettings.reminderFee.tax', 100] },
          '$partnerSettings.reminderFee.amount'
        ]
      },
      invoiceSecondReminderDays: '$partnerSettings.invoiceSecondReminder.days',
      landlordCollectionNoticeDays:
        '$partnerSettings.landlordInvoiceCollectionNotice.days',
      landlordCollectionNoticeFeeAmount:
        '$partnerSettings.landlordCollectionNoticeFee.amount',
      landlordCollectionNoticeFeeTax: {
        $multiply: [
          {
            $divide: ['$partnerSettings.landlordCollectionNoticeFee.tax', 100]
          },
          '$partnerSettings.landlordCollectionNoticeFee.amount'
        ]
      },
      landlordDuePreReminderDays:
        '$partnerSettings.landlordDuePreReminder.days',
      landlordReminderFeeTax: {
        $multiply: [
          {
            $divide: ['$partnerSettings.landlordReminderFee.tax', 100]
          },
          '$partnerSettings.landlordReminderFee.amount'
        ]
      },
      landlordFirstReminderDays:
        '$partnerSettings.landlordInvoiceFirstReminder.days',
      landlordReminderFeeAmount: '$partnerSettings.landlordReminderFee.amount',
      landlordSecondReminderDays:
        '$partnerSettings.landlordInvoiceSecondReminder.days',
      isInvoiceCollectionNoticeEnabled:
        '$partnerSettings.invoiceCollectionNotice.enabled',
      isInvoiceCollectionNoticeFeeEnabled:
        '$partnerSettings.collectionNoticeFee.enabled',
      isInvoiceDuePreReminderEnabled: '$partnerSettings.duePreReminder.enabled',
      isInvoiceFirstReminderEnabled:
        '$partnerSettings.invoiceFirstReminder.enabled',
      isInvoiceReminderFeeEnabled: '$partnerSettings.reminderFee.enabled',
      isInvoiceSecondReminderEnabled:
        '$partnerSettings.invoiceSecondReminder.enabled',
      isLandlordCollectionNoticeEnabled:
        '$partnerSettings.landlordInvoiceCollectionNotice.enabled',
      isLandlordDuePreReminderEnabled:
        '$partnerSettings.landlordDuePreReminder.enabled',
      isLandlordFirstReminderEnabled:
        '$partnerSettings.landlordInvoiceFirstReminder.enabled',
      isLandlordSecondReminderEnabled:
        '$partnerSettings.landlordInvoiceSecondReminder.enabled',
      isLandlordCollectionNoticeFeeEnabled:
        '$partnerSettings.landlordCollectionNoticeFee.enabled',
      isLandlordReminderFeeEnabled:
        '$partnerSettings.landlordReminderFee.enabled',
      partnerSettings: '$$REMOVE',
      timezone: '$partnerSettings.dateTimeSettings.timezone'
    }
  }
  const basicMatchQueryData = {
    ...query,
    isDefaulted: { $ne: true },
    $or: [
      { invoiceType: 'invoice' },
      { invoiceType: 'landlord_invoice', isPayable: true }
    ],
    status: { $nin: ['paid', 'credited', 'lost'] }
  }
  const lookupForPartnerSettings = {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  }
  const matchQueryPipeLineForDelayDate = {
    $match: {
      $or: [
        { delayDate: { $exists: false } },
        { $expr: { $lt: ['$delayDate', '$notificationSendingDate'] } }
      ]
    }
  }
  const projectForInvoiceData = {
    $project: {
      collectionNoticeDueDate: 1,
      invoiceCollectionNoticeFeeAmount: 1,
      invoiceCollectionNoticeFeeTax: 1,
      invoiceReminderFeeAmount: 1,
      invoiceReminderFeeTax: 1,
      invoiceType: 1,
      landlordCollectionNoticeFeeAmount: 1,
      landlordCollectionNoticeFeeTax: 1,
      landlordReminderFeeAmount: 1,
      landlordReminderFeeTax: 1,
      isInvoiceCollectionNoticeFeeEnabled: {
        $cond: [
          {
            $and: [
              { $eq: ['$isInvoiceCollectionNoticeFeeEnabled', true] },
              { $gte: ['$invoiceCollectionNoticeDays', 14] }
            ]
          },
          true,
          false
        ]
      },
      isInvoiceReminderNoticeFeeEnabled: options?.isFirstReminderNotice
        ? {
            $and: [
              { $eq: ['$isInvoiceReminderFeeEnabled', true] },
              { $gte: ['$invoiceFirstReminderDays', 14] }
            ]
          }
        : {
            $cond: [
              {
                $and: [
                  { $eq: ['$isInvoiceReminderFeeEnabled', true] },
                  { $lt: ['$invoiceFirstReminderDays', 14] },
                  {
                    $gte: [
                      {
                        $add: [
                          '$invoiceFirstReminderDays',
                          '$invoiceSecondReminderDays'
                        ]
                      },
                      14
                    ]
                  }
                ]
              },
              true,
              false
            ]
          },
      isLandlordCollectionNoticeFeeEnabled: {
        $cond: [
          {
            $and: [
              { $eq: ['$isLandlordCollectionNoticeFeeEnabled', true] },
              { $gte: ['$landlordCollectionNoticeDays', 14] }
            ]
          },
          true,
          false
        ]
      },
      isLandlordReminderNoticeFeeEnabled: options?.isFirstReminderNotice
        ? {
            $and: [
              { $eq: ['$isLandlordReminderFeeEnabled', true] },
              { $gte: ['$landlordFirstReminderDays', 14] }
            ]
          }
        : {
            $cond: [
              {
                $and: [
                  { $eq: ['$isLandlordReminderFeeEnabled', true] },
                  { $lt: ['$landlordFirstReminderDays', 14] },
                  {
                    $gte: [
                      {
                        $add: [
                          '$landlordFirstReminderDays',
                          '$landlordSecondReminderDays'
                        ]
                      },
                      14
                    ]
                  }
                ]
              },
              true,
              false
            ]
          },
      notificationSendingDate: 1,
      partnerId: 1
    }
  }
  const unwindForPartnerSettings = {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  }

  return {
    addFieldsForActualDatesOfInvoice,
    addFieldsForNotificationSendingDate,
    addFieldsForPartnerSettings,
    basicMatchQueryData,
    lookupForPartnerSettings,
    matchQueryPipeLineForDelayDate,
    projectForInvoiceData,
    unwindForPartnerSettings
  }
}

const getDateFromPartsForInvoiceReminders = (fieldName = '') => ({
  $dateFromParts: {
    year: { $year: { $toDate: fieldName } },
    month: { $month: { $toDate: fieldName } },
    day: { $dayOfMonth: { $toDate: fieldName } },
    hour: 23,
    minute: 59,
    second: 59,
    millisecond: 999,
    timezone: '$timezone'
  }
})

const getAggregationPipeLinesForDuePreReminderNotice = (
  query = {},
  options = {}
) => {
  const {
    addFieldsForActualDatesOfInvoice,
    addFieldsForNotificationSendingDate,
    addFieldsForPartnerSettings,
    basicMatchQueryData,
    lookupForPartnerSettings,
    matchQueryPipeLineForDelayDate,
    projectForInvoiceData,
    unwindForPartnerSettings
  } = getCommonPipeLinesForReminderAndCollectionNotice(query) || {}
  const addFieldsForDuePreReminderNoticeDate = {
    $addFields: {
      invoiceDuePreReminderSendingDate: {
        $dateSubtract: {
          startDate: '$dueDate',
          unit: 'day',
          amount: '$invoiceDuePreReminderDays',
          timezone: '$timezone'
        }
      },
      landlordDuePreReminderSendingDate: {
        $dateSubtract: {
          startDate: '$dueDate',
          unit: 'day',
          amount: '$landlordDuePreReminderDays',
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsForDuePreReminderNoticeActualDate = {
    $addFields: {
      invoiceDuePreReminderSendingDate: {
        $cond: [
          { $ifNull: ['$invoiceDuePreReminderSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$invoiceDuePreReminderSendingDate'
          ),
          false
        ]
      },
      landlordDuePreReminderSendingDate: {
        $cond: [
          { $ifNull: ['$landlordDuePreReminderSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$landlordDuePreReminderSendingDate'
          ),
          false
        ]
      }
    }
  }
  const addFieldsForDuePreReminderEnabled = {
    $addFields: {
      isInvoiceDuePreReminderDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $not: { $ifNull: ['$dueReminderSentAt', false] } },
              { $eq: ['$isInvoiceDuePreReminderEnabled', true] },
              { $eq: ['$invoiceType', 'invoice'] },
              { $gte: ['$dueDate', '$notificationSendingDate'] },
              { $ne: ['$invoiceDuePreReminderSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$invoiceDuePreReminderSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      isLandlordDuePreReminderDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $not: { $ifNull: ['$dueReminderSentAt', false] } },
              { $eq: ['$isLandlordDuePreReminderEnabled', true] },
              { $eq: ['$invoiceType', 'landlord_invoice'] },
              { $gte: ['$dueDate', '$notificationSendingDate'] },
              { $ne: ['$landlordDuePreReminderSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$landlordDuePreReminderSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      }
    }
  }
  const matchQueryForDuePreReminderDate = {
    $match: {
      $or: [
        {
          isInvoiceDuePreReminderDatePastOrToday: true,
          invoiceType: 'invoice'
        },
        {
          isLandlordDuePreReminderDatePastOrToday: true,
          invoiceType: 'landlord_invoice'
        }
      ]
    }
  }

  return [
    { $match: basicMatchQueryData },
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    addFieldsForNotificationSendingDate,
    addFieldsForActualDatesOfInvoice,
    matchQueryPipeLineForDelayDate,
    addFieldsForDuePreReminderNoticeDate,
    addFieldsForDuePreReminderNoticeActualDate,
    addFieldsForDuePreReminderEnabled,
    matchQueryForDuePreReminderDate,
    { $sort: options?.sort || { createdAt: 1 } },
    projectForInvoiceData,
    { $skip: options?.skip || 0 }
  ]
}

const getAggregationPipeLinesForFirstReminderNotice = (
  query = {},
  options = {}
) => {
  const {
    addFieldsForActualDatesOfInvoice,
    addFieldsForNotificationSendingDate,
    addFieldsForPartnerSettings,
    basicMatchQueryData,
    lookupForPartnerSettings,
    matchQueryPipeLineForDelayDate,
    projectForInvoiceData,
    unwindForPartnerSettings
  } =
    getCommonPipeLinesForReminderAndCollectionNotice(query, {
      isFirstReminderNotice: true
    }) || {}
  const addFieldsForFirstReminderNoticeDate = {
    $addFields: {
      invoiceFirstReminderSendingDate: {
        $dateAdd: {
          startDate: '$dueDate',
          unit: 'day',
          amount: '$invoiceFirstReminderDays',
          timezone: '$timezone'
        }
      },
      landlordFirstReminderSendingDate: {
        $dateAdd: {
          startDate: '$dueDate',
          unit: 'day',
          amount: '$landlordFirstReminderDays',
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsForFirstReminderNoticeActualDate = {
    $addFields: {
      invoiceFirstReminderSendingDate: {
        $cond: [
          { $ifNull: ['$invoiceFirstReminderSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$invoiceFirstReminderSendingDate'
          ),
          false
        ]
      },
      landlordFirstReminderSendingDate: {
        $cond: [
          { $ifNull: ['$landlordFirstReminderSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$landlordFirstReminderSendingDate'
          ),
          false
        ]
      }
    }
  }
  const addFieldsForFirstReminderEnabled = {
    $addFields: {
      isInvoiceFirstReminderDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $not: { $ifNull: ['$firstReminderSentAt', false] } },
              { $eq: ['$isInvoiceFirstReminderEnabled', true] },
              { $eq: ['$invoiceType', 'invoice'] },
              { $gte: ['$notificationSendingDate', '$dueDate'] },
              { $ne: ['$invoiceFirstReminderSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$invoiceFirstReminderSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      isLandlordFirstReminderDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $not: { $ifNull: ['$firstReminderSentAt', false] } },
              { $eq: ['$isLandlordFirstReminderEnabled', true] },
              { $eq: ['$invoiceType', 'landlord_invoice'] },
              { $gte: ['$notificationSendingDate', '$dueDate'] },
              { $ne: ['$landlordFirstReminderSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$landlordFirstReminderSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      }
    }
  }
  const matchQueryForFirstReminderDate = {
    $match: {
      $or: [
        {
          isInvoiceFirstReminderDatePastOrToday: true,
          invoiceType: 'invoice'
        },
        {
          isLandlordFirstReminderDatePastOrToday: true,
          invoiceType: 'landlord_invoice'
        }
      ]
    }
  }

  return [
    { $match: basicMatchQueryData },
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    addFieldsForNotificationSendingDate,
    addFieldsForActualDatesOfInvoice,
    matchQueryPipeLineForDelayDate,
    addFieldsForFirstReminderNoticeDate,
    addFieldsForFirstReminderNoticeActualDate,
    addFieldsForFirstReminderEnabled,
    matchQueryForFirstReminderDate,
    { $sort: options?.sort || { createdAt: 1 } },
    projectForInvoiceData,
    { $skip: options?.skip || 0 }
  ]
}

const getAggregationPipeLinesForSecondReminderNotice = (
  query = {},
  options = {}
) => {
  const {
    addFieldsForActualDatesOfInvoice,
    addFieldsForNotificationSendingDate,
    addFieldsForPartnerSettings,
    basicMatchQueryData,
    lookupForPartnerSettings,
    matchQueryPipeLineForDelayDate,
    projectForInvoiceData,
    unwindForPartnerSettings
  } = getCommonPipeLinesForReminderAndCollectionNotice(query) || {}

  const addFieldsForSecondReminderNoticeDate = {
    $addFields: {
      invoiceSecondReminderSendingDate: {
        $dateAdd: {
          startDate: '$firstReminderSentAt',
          unit: 'day',
          amount: '$invoiceSecondReminderDays',
          timezone: '$timezone'
        }
      },
      landlordSecondReminderSendingDate: {
        $dateAdd: {
          startDate: '$firstReminderSentAt',
          unit: 'day',
          amount: '$landlordSecondReminderDays',
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsForSecondReminderNoticeActualDate = {
    $addFields: {
      invoiceSecondReminderSendingDate: {
        $cond: [
          { $ifNull: ['$invoiceSecondReminderSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$invoiceSecondReminderSendingDate'
          ),
          false
        ]
      },
      landlordSecondReminderSendingDate: {
        $cond: [
          { $ifNull: ['$landlordSecondReminderSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$landlordSecondReminderSendingDate'
          ),
          false
        ]
      }
    }
  }
  const addFieldsForSecondReminderEnabled = {
    $addFields: {
      isInvoiceSecondReminderDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $ifNull: ['$firstReminderSentAt', false] },
              { $not: { $ifNull: ['$secondReminderSentAt', false] } },
              { $eq: ['$isInvoiceSecondReminderEnabled', true] },
              { $eq: ['$invoiceType', 'invoice'] },
              { $gte: ['$notificationSendingDate', '$dueDate'] },
              { $ne: ['$invoiceSecondReminderSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$invoiceSecondReminderSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      isLandlordSecondReminderDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $ifNull: ['$firstReminderSentAt', false] },
              { $not: { $ifNull: ['$secondReminderSentAt', false] } },
              { $eq: ['$isLandlordSecondReminderEnabled', true] },
              { $eq: ['$invoiceType', 'landlord_invoice'] },
              { $gte: ['$notificationSendingDate', '$dueDate'] },
              { $ne: ['$landlordSecondReminderSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$landlordSecondReminderSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      }
    }
  }
  const matchQueryForSecondReminderDate = {
    $match: {
      $or: [
        {
          isInvoiceSecondReminderDatePastOrToday: true,
          invoiceType: 'invoice'
        },
        {
          isLandlordSecondReminderDatePastOrToday: true,
          invoiceType: 'landlord_invoice'
        }
      ]
    }
  }

  return [
    {
      $match: { ...basicMatchQueryData, firstReminderSentAt: { $exists: true } }
    },
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    addFieldsForNotificationSendingDate,
    addFieldsForActualDatesOfInvoice,
    matchQueryPipeLineForDelayDate,
    addFieldsForSecondReminderNoticeDate,
    addFieldsForSecondReminderNoticeActualDate,
    addFieldsForSecondReminderEnabled,
    matchQueryForSecondReminderDate,
    { $sort: options?.sort || { createdAt: 1 } },
    projectForInvoiceData,
    { $skip: options?.skip || 0 }
  ]
}

const getAggregationPipeLinesForCollectionNotice = (
  query = {},
  options = {}
) => {
  const {
    addFieldsForActualDatesOfInvoice,
    addFieldsForNotificationSendingDate,
    addFieldsForPartnerSettings,
    basicMatchQueryData,
    lookupForPartnerSettings,
    matchQueryPipeLineForDelayDate,
    projectForInvoiceData,
    unwindForPartnerSettings
  } = getCommonPipeLinesForReminderAndCollectionNotice(query) || {}

  const addFieldsForFirstOrSecondReminderDate = {
    $addFields: {
      previousNoticeDateForLandlordInvoice: {
        $cond: [
          {
            $and: [
              { $eq: ['$isLandlordFirstReminderEnabled', true] },
              { $eq: ['$isLandlordSecondReminderEnabled', true] }
            ]
          },
          '$secondReminderSentAt',
          {
            $cond: [
              { $eq: ['$isLandlordFirstReminderEnabled', true] },
              '$firstReminderSentAt',
              false
            ]
          }
        ]
      },
      previousNoticeDateForRentInvoice: {
        $cond: [
          {
            $and: [
              { $eq: ['$isInvoiceFirstReminderEnabled', true] },
              { $eq: ['$isInvoiceSecondReminderEnabled', true] }
            ]
          },
          '$secondReminderSentAt',
          {
            $cond: [
              { $eq: ['$isInvoiceFirstReminderEnabled', true] },
              '$firstReminderSentAt',
              false
            ]
          }
        ]
      }
    }
  }
  const addFieldsForCollectionNoticeDate = {
    $addFields: {
      invoiceCollectionNoticeSendingDate: {
        $cond: [
          { $ifNull: ['$previousNoticeDateForRentInvoice', false] },
          {
            $dateAdd: {
              startDate: '$previousNoticeDateForRentInvoice',
              unit: 'day',
              amount: '$invoiceCollectionNoticeDays',
              timezone: '$timezone'
            }
          },
          false
        ]
      },
      landlordCollectionNoticeSendingDate: {
        $cond: [
          { $ifNull: ['$previousNoticeDateForLandlordInvoice', false] },
          {
            $dateAdd: {
              startDate: '$previousNoticeDateForLandlordInvoice',
              unit: 'day',
              amount: '$landlordCollectionNoticeDays',
              timezone: '$timezone'
            }
          },
          false
        ]
      }
    }
  }
  const addFieldsForCollectionNoticeActualDate = {
    $addFields: {
      invoiceCollectionNoticeSendingDate: {
        $cond: [
          { $ifNull: ['$invoiceCollectionNoticeSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$invoiceCollectionNoticeSendingDate'
          ),
          false
        ]
      },
      landlordCollectionNoticeSendingDate: {
        $cond: [
          { $ifNull: ['$landlordCollectionNoticeSendingDate', false] },
          getDateFromPartsForInvoiceReminders(
            '$landlordCollectionNoticeSendingDate'
          ),
          false
        ]
      }
    }
  }
  const addFieldsForCollectionNoticeEnabled = {
    $addFields: {
      isInvoiceCollectionNoticeDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $not: { $ifNull: ['$collectionNoticeSentAt', false] } },
              { $eq: ['$isInvoiceCollectionNoticeEnabled', true] },
              { $eq: ['$invoiceType', 'invoice'] },
              { $gte: ['$notificationSendingDate', '$dueDate'] },
              { $ne: ['$invoiceCollectionNoticeSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$invoiceCollectionNoticeSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      isLandlordCollectionNoticeDatePastOrToday: {
        $cond: [
          {
            $and: [
              { $not: { $ifNull: ['$collectionNoticeSentAt', false] } },
              { $eq: ['$isLandlordCollectionNoticeEnabled', true] },
              { $eq: ['$invoiceType', 'landlord_invoice'] },
              { $gte: ['$notificationSendingDate', '$dueDate'] },
              { $ne: ['$landlordCollectionNoticeSendingDate', false] },
              {
                $gte: [
                  '$notificationSendingDate',
                  '$landlordCollectionNoticeSendingDate'
                ]
              }
            ]
          },
          true,
          false
        ]
      }
    }
  }
  const matchQueryForCollectionNoticeDate = {
    $match: {
      $or: [
        {
          isInvoiceCollectionNoticeDatePastOrToday: true,
          invoiceType: 'invoice'
        },
        {
          isLandlordCollectionNoticeDatePastOrToday: true,
          invoiceType: 'landlord_invoice'
        }
      ]
    }
  }

  return [
    { $match: basicMatchQueryData },
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    {
      $addFields: {
        collectionNoticeDueDate: {
          $dateAdd: {
            startDate: new Date(),
            unit: 'day',
            amount: '$invoiceCollectionNoticeNewDueDays',
            timezone: '$timezone'
          }
        }
      }
    },
    addFieldsForNotificationSendingDate,
    addFieldsForActualDatesOfInvoice,
    matchQueryPipeLineForDelayDate,
    addFieldsForFirstOrSecondReminderDate,
    addFieldsForCollectionNoticeDate,
    addFieldsForCollectionNoticeActualDate,
    addFieldsForCollectionNoticeEnabled,
    matchQueryForCollectionNoticeDate,
    { $sort: options?.sort || { createdAt: 1 } },
    projectForInvoiceData,
    { $skip: options?.skip || 0 }
  ]
}

export const getInvoicesForDuePreReminderNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines = getAggregationPipeLinesForDuePreReminderNotice(
    query,
    options
  )

  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

export const getInvoicesForFirstReminderNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines = getAggregationPipeLinesForFirstReminderNotice(
    query,
    options
  )

  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

export const getInvoicesForSecondReminderNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines = getAggregationPipeLinesForSecondReminderNotice(
    query,
    options
  )

  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

export const getInvoicesForCollectionNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines = getAggregationPipeLinesForCollectionNotice(
    query,
    options
  )

  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

const getAggregationPipeLinesForEvictionNotice = (query = {}, options = {}) => {
  const matchPipeLine = {
    $match: {
      ...query,
      invoiceType: 'invoice',
      isNonRentInvoice: { $ne: true },
      status: 'overdue'
    }
  }
  const lookupForPartner = {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  }
  const unwindForPartner = {
    $unwind: { path: '$partner', preserveNullAndEmptyArrays: true }
  }
  const matchForPartner = {
    $match: { 'partner.isActive': true }
  }
  const lookupForPartnerSettings = {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  }
  const unwindForPartnerSettings = {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  }
  const addFieldsForPartnerSettings = {
    $addFields: {
      evictionDueDays: '$partnerSettings.evictionNotice.days',
      isEvictionNoticeEnabled: '$partnerSettings.evictionNotice.enabled',
      partnerSettings: '$$REMOVE',
      requiredTotalOverDue: {
        $cond: [
          {
            $ifNull: [
              '$partnerSettings.evictionNotice.requiredTotalOverDue',
              false
            ]
          },
          '$partnerSettings.evictionNotice.requiredTotalOverDue',
          0
        ]
      },
      timezone: '$partnerSettings.dateTimeSettings.timezone'
    }
  }
  const matchQueryForPartnerSettings = {
    $match: { isEvictionNoticeEnabled: true }
  }
  const addFieldsForTodayDate = {
    $addFields: {
      todayDate: {
        $dateAdd: {
          startDate: new Date(),
          unit: 'day',
          amount: 0,
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsForTotalOverDue = {
    $addFields: {
      totalOverDue: {
        $subtract: [
          {
            $add: [
              { $ifNull: ['$invoiceTotal', 0] },
              { $ifNull: ['$creditedAmount', 0] }
            ]
          },
          {
            $add: [
              { $ifNull: ['$totalPaid', 0] },
              { $ifNull: ['$lostMeta.amount', 0] }
            ]
          }
        ]
      }
    }
  }
  const addFieldsEvictionNoticeDate = {
    $addFields: {
      evictionNoticeSendingDate: {
        $dateSubtract: {
          startDate: '$todayDate',
          unit: 'day',
          amount: '$evictionDueDays',
          timezone: '$timezone'
        }
      }
    }
  }
  const matchQueryForDueDate = {
    $match: {
      $expr: {
        $and: [
          { $lte: ['$dueDate', '$evictionNoticeSendingDate'] },
          { $gt: ['$totalOverDue', '$requiredTotalOverDue'] }
        ]
      }
    }
  }
  const groupByContract = {
    $group: {
      _id: '$contractId',
      evictionNoticeSendingDate: { $first: '$evictionNoticeSendingDate' },
      evictionDueDays: { $first: '$evictionDueDays' },
      invoicesWithoutEvictionCase: {
        $push: {
          $cond: [
            {
              $and: [
                { $not: { $ifNull: ['$evictionDueReminderSentOn', false] } },
                { $not: { $ifNull: ['$evictionDueReminderSent', false] } },
                { $not: { $ifNull: ['$evictionNoticeSentOn', false] } },
                { $not: { $ifNull: ['$evictionNoticeSent', false] } }
              ]
            },
            {
              contractId: '$contractId',
              createdAt: '$createdAt',
              dueDate: '$dueDate',
              delayDate: '$delayDate',
              invoiceId: '$_id',
              partnerId: '$partnerId'
            },
            null
          ]
        }
      },
      invoicesWithEvictionCase: {
        $push: {
          $cond: [
            {
              $and: [
                { $not: { $ifNull: ['$evictionDueReminderSentOn', false] } },
                { $not: { $ifNull: ['$evictionDueReminderSent', false] } },
                { $ifNull: ['$evictionNoticeSentOn', false] },
                { $eq: ['$evictionNoticeSent', true] }
              ]
            },
            {
              contractId: '$contractId',
              createdAt: '$createdAt',
              dueDate: '$dueDate',
              delayDate: '$delayDate',
              invoiceId: '$_id',
              partnerId: '$partnerId'
            },
            null
          ]
        }
      },
      isEvictionNoticeEnabled: { $first: '$isEvictionNoticeEnabled' },
      timezone: { $first: '$timezone' },
      todayDate: { $first: '$todayDate' }
    }
  }
  const addFieldsForEvictionInvoices = {
    $addFields: {
      invoicesWithoutEvictionCase: {
        $filter: {
          input: '$invoicesWithoutEvictionCase',
          as: 'invoice',
          cond: { $ifNull: ['$$invoice', false] }
        }
      },
      invoicesWithEvictionCase: {
        $filter: {
          input: '$invoicesWithEvictionCase',
          as: 'invoice',
          cond: { $ifNull: ['$$invoice', false] }
        }
      }
    }
  }
  const addFieldsEvictionInvoicesCount = {
    $addFields: {
      invoicesWithoutEvictionCase: { $first: '$invoicesWithoutEvictionCase' },
      invoicesWithEvictionCase: { $first: '$invoicesWithEvictionCase' },
      invoicesWithoutEvictionCaseCount: {
        $size: '$invoicesWithoutEvictionCase'
      },
      invoicesWithEvictionCaseCount: { $size: '$invoicesWithEvictionCase' }
    }
  }
  const matchForEvictionInvoicesCount = {
    $match: {
      invoicesWithoutEvictionCaseCount: { $gt: 0 },
      invoicesWithEvictionCaseCount: 0
    }
  }
  const addFieldsForEvictionDueDate = {
    $addFields: {
      evictionNoticeDateForDueDate: {
        $dateSubtract: {
          startDate: '$invoicesWithoutEvictionCase.dueDate',
          unit: 'day',
          amount: '$evictionDueDays',
          timezone: '$timezone'
        }
      },
      evictionNoticeDateForDelayDueDate: {
        $dateAdd: {
          startDate: '$invoicesWithoutEvictionCase.delayDate',
          unit: 'day',
          amount: '$evictionDueDays',
          timezone: '$timezone'
        }
      }
    }
  }
  const matchQueryForDelayDueAndDueDate = {
    $match: {
      $expr: {
        $or: [
          {
            $and: [
              { $ifNull: ['$evictionNoticeDateForDelayDueDate', false] },
              { $ne: ['$evictionNoticeDateForDelayDueDate', null] },
              { $lt: ['$evictionNoticeDateForDelayDueDate', '$todayDate'] }
            ]
          },
          {
            $and: [
              { $eq: ['$evictionNoticeDateForDelayDueDate', null] },
              { $lte: ['$evictionNoticeDateForDueDate', '$todayDate'] }
            ]
          }
        ]
      }
    }
  }
  const projectForInvoice = {
    $project: {
      _id: '$invoicesWithoutEvictionCase.invoiceId',
      partnerId: '$invoicesWithoutEvictionCase.partnerId'
    }
  }

  return [
    matchPipeLine,
    lookupForPartner,
    unwindForPartner,
    matchForPartner,
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    matchQueryForPartnerSettings,
    addFieldsForTodayDate,
    addFieldsForTotalOverDue,
    addFieldsEvictionNoticeDate,
    matchQueryForDueDate,
    { $sort: options?.sort || { createdAt: 1 } },
    groupByContract,
    addFieldsForEvictionInvoices,
    addFieldsEvictionInvoicesCount,
    matchForEvictionInvoicesCount,
    addFieldsForEvictionDueDate,
    matchQueryForDelayDueAndDueDate,
    projectForInvoice,
    { $skip: options?.skip || 0 }
    // { $limit: options?.limit || 50 }
  ]
}

const getAggregationPipeLinesForEvictionReminderNotice = (
  query = {},
  options = {}
) => {
  const matchPipeLine = {
    $match: {
      ...query,
      evictionNoticeSentOn: { $exists: true },
      invoiceType: 'invoice',
      isNonRentInvoice: { $ne: true },
      status: 'overdue'
    }
  }
  const lookupForPartner = {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  }
  const unwindForPartner = {
    $unwind: { path: '$partner', preserveNullAndEmptyArrays: true }
  }
  const matchForPartner = {
    $match: { 'partner.isActive': true }
  }
  const lookupForPartnerSettings = {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  }
  const unwindForPartnerSettings = {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  }
  const addFieldsForPartnerSettings = {
    $addFields: {
      evictionReminderDueDays: '$partnerSettings.evictionReminderNotice.days',
      isEvictionReminderNoticeEnabled:
        '$partnerSettings.evictionReminderNotice.enabled',
      partnerSettings: '$$REMOVE',
      timezone: '$partnerSettings.dateTimeSettings.timezone'
    }
  }
  const matchQueryForPartnerSettings = {
    $match: { isEvictionReminderNoticeEnabled: true }
  }
  const addFieldsForTodayDate = {
    $addFields: {
      todayDate: {
        $dateAdd: {
          startDate: new Date(),
          unit: 'day',
          amount: 0,
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsEvictionDaysInInvoice = {
    $addFields: {
      isDatePastToSendEvictionReminderNotice: {
        $cond: [
          {
            $gt: [
              {
                $dateDiff: {
                  startDate: '$evictionNoticeSentOn',
                  endDate: '$todayDate',
                  unit: 'day',
                  timezone: '$timezone'
                }
              },
              '$evictionReminderDueDays'
            ]
          },
          true,
          false
        ]
      }
    }
  }
  const matchQueryForDueDate = {
    $match: { isDatePastToSendEvictionReminderNotice: true }
  }
  const groupByContract = {
    $group: {
      _id: '$contractId',
      invoice: {
        $first: {
          invoiceId: '$_id',
          partnerId: '$partnerId'
        }
      }
    }
  }
  const lookupForContract = {
    $lookup: {
      from: 'contracts',
      localField: '_id',
      foreignField: '_id',
      as: 'contract'
    }
  }
  const unwindForContract = {
    $unwind: { path: '$contract', preserveNullAndEmptyArrays: true }
  }
  const addFieldsForEvictionCaseStatus = {
    $addFields: {
      contract: '$$REMOVE',
      evictionCases: {
        $filter: {
          input: '$contract.evictionCases',
          as: 'evictionCase',
          cond: {
            $and: [
              { $eq: ['$$evictionCase.status', 'new'] },
              {
                $in: ['$invoice.invoiceId', '$$evictionCase.evictionInvoiceIds']
              }
            ]
          }
        }
      }
    }
  }
  const matchForEvictionCase = {
    $match: { evictionCases: { $gt: { $size: 0 } } }
  }
  const projectForInvoice = {
    $project: {
      _id: '$invoice.invoiceId',
      partnerId: '$invoice.partnerId'
    }
  }

  return [
    matchPipeLine,
    lookupForPartner,
    unwindForPartner,
    matchForPartner,
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    matchQueryForPartnerSettings,
    addFieldsForTodayDate,
    addFieldsEvictionDaysInInvoice,
    matchQueryForDueDate,
    { $sort: options?.sort || { createdAt: 1 } },
    groupByContract,
    lookupForContract,
    unwindForContract,
    addFieldsForEvictionCaseStatus,
    matchForEvictionCase,
    projectForInvoice,
    { $skip: options?.skip || 0 }
    // { $limit: options?.limit || 50 }
  ]
}

const getAggregationPipeLinesForEvictionDueReminderNotice = (
  query = {},
  options = {}
) => {
  const matchPipeLine = {
    $match: {
      ...query,
      evictionDueReminderNoticeSentOn: { $exists: false },
      evictionNoticeSentOn: { $exists: true },
      evictionNoticeSent: true,
      invoiceType: 'invoice',
      isNonRentInvoice: { $ne: true },
      status: { $nin: ['paid', 'credited', 'lost'] }
    }
  }
  const lookupForPartner = {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  }
  const unwindForPartner = {
    $unwind: { path: '$partner', preserveNullAndEmptyArrays: true }
  }
  const matchForPartner = {
    $match: { 'partner.isActive': true }
  }
  const lookupForPartnerSettings = {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  }
  const unwindForPartnerSettings = {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  }
  const addFieldsForPartnerSettings = {
    $addFields: {
      administrationEvictionFeeAmount:
        '$partnerSettings.administrationEvictionFee.amount',
      administrationEvictionFeeTax:
        '$partnerSettings.administrationEvictionFee.tax',
      evictionFeeAmount: '$partnerSettings.evictionFee.amount',
      evictionReminderDueDays:
        '$partnerSettings.evictionDueReminderNotice.days',
      isAdministrationEvictionFeeEnabled:
        '$partnerSettings.administrationEvictionFee.enabled',
      isCreateEvictionPackage:
        '$partnerSettings.evictionDueReminderNotice.isCreateEvictionPackage',
      isEvictionFeeEnabled: '$partnerSettings.evictionFee.enabled',
      isEvictionDueReminderNoticeEnabled:
        '$partnerSettings.evictionDueReminderNotice.enabled',
      partnerSettings: '$$REMOVE',
      timezone: '$partnerSettings.dateTimeSettings.timezone'
    }
  }
  const matchQueryForPartnerSettings = {
    $match: { isEvictionDueReminderNoticeEnabled: true }
  }
  const addFieldsForTodayDate = {
    $addFields: {
      todayDate: {
        $dateAdd: {
          startDate: new Date(),
          unit: 'day',
          amount: 0,
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsEvictionNoticeDate = {
    $addFields: {
      evictionDueReminderNoticeSendingDate: {
        $dateSubtract: {
          startDate: '$todayDate',
          unit: 'day',
          amount: '$evictionReminderDueDays',
          timezone: '$timezone'
        }
      }
    }
  }
  const matchQueryForDueDate = {
    $match: {
      $expr: {
        $and: [
          {
            $lte: [
              '$evictionNoticeSentOn',
              '$evictionDueReminderNoticeSendingDate'
            ]
          }
        ]
      }
    }
  }
  const groupByContract = {
    $group: {
      _id: '$contractId',
      invoice: {
        $first: {
          contractId: '$contractId',
          createdAt: '$createdAt',
          dueDate: '$dueDate',
          delayDate: '$delayDate',
          feesMeta: '$feesMeta',
          invoiceId: '$_id',
          partnerId: '$partnerId'
        }
      },
      administrationEvictionFeeAmount: {
        $first: '$administrationEvictionFeeAmount'
      },
      administrationEvictionFeeTax: { $first: '$administrationEvictionFeeTax' },
      evictionFeeAmount: { $first: '$evictionFeeAmount' },
      evictionReminderDueDays: { $first: '$evictionReminderDueDays' },
      isAdministrationEvictionFeeEnabled: {
        $first: '$isAdministrationEvictionFeeEnabled'
      },
      isCreateEvictionPackage: { $first: '$isCreateEvictionPackage' },
      isEvictionFeeEnabled: { $first: '$isEvictionFeeEnabled' }
    }
  }
  const projectForInvoice = {
    $project: {
      _id: '$invoice.invoiceId',
      administrationEvictionFeeAmount: 1,
      administrationEvictionFeeTax: 1,
      contractId: '$_id',
      evictionFeeAmount: 1,
      evictionReminderDueDays: 1,
      feesMeta: '$invoice.feesMeta',
      isAdministrationEvictionFeeEnabled: 1,
      isCreateEvictionPackage: 1,
      isEvictionFeeEnabled: 1,
      partnerId: '$invoice.partnerId'
    }
  }

  return [
    matchPipeLine,
    lookupForPartner,
    unwindForPartner,
    matchForPartner,
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    matchQueryForPartnerSettings,
    addFieldsForTodayDate,
    addFieldsEvictionNoticeDate,
    matchQueryForDueDate,
    { $sort: options?.sort || { createdAt: 1 } },
    groupByContract,
    projectForInvoice,
    { $skip: options?.skip || 0 }
    // { $limit: options?.limit || 50 }
  ]
}

export const getInvoicesForEvictionNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines = getAggregationPipeLinesForEvictionNotice(
    query,
    options
  )
  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

export const getInvoicesForEvictionReminderNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines = getAggregationPipeLinesForEvictionReminderNotice(
    query,
    options
  )

  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

export const getInvoicesForEvictionDueReminderNotice = async (req) => {
  const { body = {}, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query = {}, options = {} } = body || {}
  const { partnerId } = query || {}

  if (partnerId) appHelper.validateId({ partnerId })

  const aggregationPipeLines =
    getAggregationPipeLinesForEvictionDueReminderNotice(query, options)

  const invoices = await InvoiceCollection.aggregate(aggregationPipeLines)
  return invoices
}

export const prepareDataInvoicesForAppHealthNotification = async (
  partnerId
) => {
  const compareDate = (
    await appHelper.getActualDate(partnerId, true, new Date())
  )
    .subtract(6, 'hours')
    .toDate()
  const query = {
    partnerId,
    createdAt: { $gte: new Date('2020-01-01'), $lte: compareDate },
    invoiceSent: { $ne: true },
    status: { $nin: ['paid', 'credited'] },
    $or: [
      { enabledNotification: true, invoiceType: 'invoice' },
      {
        enabledNotification: true,
        invoiceType: 'credit_note',
        disabledPartnerNotification: { $ne: true }
      },
      {
        enabledNotification: true,
        invoiceType: 'landlord_invoice',
        isPayable: true,
        isFinalSettlement: true,
        disabledPartnerNotification: { $ne: true }
      },
      {
        enabledNotification: { $exists: false },
        invoiceType: 'credit_note',
        disabledPartnerNotification: { $ne: true }
      },
      { enabledNotification: { $exists: false }, invoiceType: 'invoice' },
      {
        enabledNotification: { $exists: false },
        invoiceType: 'landlord_invoice',
        isPayable: true,
        isFinalSettlement: true,
        disabledPartnerNotification: { $ne: true }
      }
    ]
  }
  const invoice = await InvoiceCollection.find(query)
  return invoice
}

export const getFirstThreeInvoicesRangeList = async (
  partnerId,
  contractInfo
) => {
  const rentalMeta = contractInfo?.rentalMeta
  const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const params = {
    partnerSettings,
    rentalMeta,
    testTodayDate: false,
    isRangesForEstimatedInvoices: true
  }
  const invoicesRangeList = await getInvoicesRangeList(params)

  return invoicesRangeList
}

export const getCommissionAndCorrectionInvoiceIdsForQuery = async (
  query = {},
  session
) => {
  if (!size(query))
    throw new CustomError(
      400,
      'Query is required for getting commission and correction invoiceIds'
    )

  const groupedInvoiceIds =
    (await getAggregatedInvoices(
      [
        { $match: query },
        {
          $group: {
            _id: null,
            commissionInvoiceIds: {
              $push: {
                $cond: [
                  { $ifNull: ['$commissionsMeta.commissionId', false] },
                  '$_id',
                  '$$REMOVE'
                ]
              }
            },
            correctionInvoiceIds: {
              $push: {
                $cond: [
                  { $ifNull: ['$addonsMeta.addonId', false] },
                  '$_id',
                  '$$REMOVE'
                ]
              }
            }
          }
        }
      ],
      session
    )) || []

  return size(groupedInvoiceIds) && groupedInvoiceIds[0]
    ? groupedInvoiceIds[0]
    : {}
}

export const getInvoiceIdsForLeaseFilter = async (contractId, leaseSerial) => {
  const invoiceQuery = await prepareInvoiceQueryForLeaseFilter(
    contractId,
    leaseSerial
  )
  const invoiceIds = await getUniqueFieldValue('_id', invoiceQuery)

  return invoiceIds
}

export const prepareInvoiceQueryForLeaseFilter = async (
  contractId,
  leaseSerial
) => {
  let query = {}
  const contractInfo = await contractHelper.getContractById(contractId)
  if (size(contractInfo)) {
    const rentalMetaHistory = size(contractInfo.rentalMetaHistory)
      ? contractInfo.rentalMetaHistory
      : []
    const rentalMeta = size(contractInfo.rentalMeta)
      ? contractInfo.rentalMeta
      : {}
    const rentalMetaHistoryObj = rentalMetaHistory.find(
      (history) => history.leaseSerial === leaseSerial
    )
    const partnerSettingsInfo =
      await partnerSettingHelper.getSettingByPartnerId(contractInfo.partnerId)

    if (size(rentalMetaHistoryObj)) {
      const createdAtDate = await appHelper.getActualDate(
        partnerSettingsInfo,
        false,
        rentalMetaHistoryObj.createdAt
      )
      const cancelAtDate = await appHelper.getActualDate(
        partnerSettingsInfo,
        false,
        rentalMetaHistoryObj.cancelledAt
      )

      query = {
        contractId: contractInfo._id,
        createdAt: { $gte: createdAtDate, $lte: cancelAtDate },
        tenantId: rentalMetaHistoryObj.tenantId
      }
    } else {
      const rentalMetaCreatedAt = await appHelper.getActualDate(
        partnerSettingsInfo,
        false,
        rentalMeta.createdAt
      )

      query = {
        contractId: contractInfo._id,
        createdAt: { $gte: rentalMetaCreatedAt },
        tenantId: rentalMeta.tenantId
      }
    }
  }
  return query
}

export const prepareInvoiceStatusUpdatingData = async (
  invoice,
  previousPaidTotal
) => {
  const {
    delayDate = '',
    partnerId,
    status,
    totalPaid = 0,
    totalBalanced = 0,
    isFinalSettlement,
    isPayable,
    invoiceTotal = 0,
    invoiceType,
    remainingBalance,
    rentTotal = 0
  } = invoice
  const invoiceTotalDue = (await getTotalDueAmountOfAnInvoice(invoice)) || 0
  const date = (await appHelper.getActualDate(partnerId, true, null))
    .startOf('day')
    .toDate()
  const invoiceLostTotal = invoice.lostMeta?.amount || 0
  const invoiceCreditedAmount = (invoice.creditedAmount || 0) * -1 || 0

  let dueDate = invoice.dueDate || ''
  let totalPaidAmount =
    totalPaid + invoiceLostTotal + invoiceCreditedAmount || 0

  let isPartiallyPaid = false
  let isOverPaid = false
  let isPartiallyBalanced = false

  const isNotALandlordInvoice = isNotLandlord(clone(invoice))
  console.log('totalPaidAmount', totalPaidAmount)
  console.log('invoiceTotal', invoiceTotal)
  console.log('rentTotal', rentTotal)
  console.log('status', status)
  const updateData = {}

  if (!isNotALandlordInvoice) totalPaidAmount = totalPaid + totalBalanced || 0

  let updatingStatus =
    (totalPaidAmount === 0 ||
      (totalPaidAmount && totalPaidAmount < rentTotal)) &&
    status === 'paid'
      ? 'created'
      : status
  console.log('updatingStatus', updatingStatus)
  console.log('isNotALandlordInvoice', isNotALandlordInvoice)
  if (!isNotALandlordInvoice && delayDate) dueDate = clone(delayDate)

  // If current date is grater than invoice dueDate
  // So invoice status will be 'overdue'.
  if (status !== 'credited' && dueDate && dueDate < date)
    updatingStatus = 'overdue'

  // Invoice status will be paid, if paid total is equal or greater than invoice total or rent total
  if (
    totalPaidAmount >= invoiceTotal ||
    totalPaidAmount >= rentTotal ||
    invoiceTotalDue < 0
  ) {
    if (status !== 'credited' && invoiceType !== 'landlord_credit_note')
      updatingStatus = 'paid'
    console.log('updatingStatus', updatingStatus)
    // When total paid amount is larger from invoice total then isOverPaid tag will be true
    if (
      (totalPaid > invoiceTotal ||
        status === 'credited' ||
        invoiceTotalDue < 0) &&
      invoiceType !== 'landlord_credit_note'
    )
      isOverPaid = true
    console.log('updatingStatus', updatingStatus)
    if (totalPaid === 0 && status === 'credited') isOverPaid = false
    console.log('updatingStatus', updatingStatus)
    if (
      !isNotALandlordInvoice &&
      status !== 'credited' &&
      totalPaidAmount === invoiceTotal &&
      totalPaid === 0
    ) {
      updatingStatus = 'balanced'
      isOverPaid = false
    }
    console.log('updatingStatus', updatingStatus)
  } else if (
    totalPaid &&
    totalPaidAmount &&
    totalPaidAmount < rentTotal &&
    invoiceTotalDue >= 0
  )
    isPartiallyPaid = true
  else if (totalPaidAmount === 0 && status === 'credited') isOverPaid = false

  // Added 'feesPaid' tag for 'fees due' and 'fees paid' filters
  if (
    size(invoice?.feesMeta) &&
    (updatingStatus === 'paid' || updatingStatus === 'credited')
  ) {
    if (updatingStatus === 'paid' && totalPaidAmount >= invoiceTotal)
      updateData.feesPaid = true
    if (updatingStatus === 'credited' && isOverPaid) updateData.feesPaid = true
  } else updateData.feesPaid = false

  if (
    !isNotALandlordInvoice &&
    totalPaidAmount !== 0 &&
    totalPaidAmount !== invoiceTotal
  )
    isPartiallyBalanced = true

  updateData.status = updatingStatus
  updateData.isPartiallyPaid = isPartiallyPaid
  updateData.isOverPaid = isOverPaid

  if (!isNotALandlordInvoice)
    updateData.isPartiallyBalanced = isPartiallyBalanced

  if (
    invoiceType === 'landlord_invoice' &&
    remainingBalance > 0 &&
    totalPaid !== previousPaidTotal
  ) {
    let newAmount = 0

    if (totalPaid > previousPaidTotal)
      newAmount = (totalPaid - previousPaidTotal) * -1
    else newAmount = previousPaidTotal - totalPaid

    updateData.remainingBalance = await appHelper.convertTo2Decimal(
      remainingBalance + (newAmount || 0)
    )
  }

  if (!isNotALandlordInvoice && updatingStatus === 'balanced')
    updateData.isPartiallyCredited = false

  if (
    invoiceType === 'landlord_invoice' &&
    remainingBalance !== 0 &&
    (isFinalSettlement || isPayable) &&
    totalPaidAmount &&
    totalPaidAmount >= invoiceTotal
  )
    updateData.remainingBalance = 0

  // If newly partiallyPaid, then re-check for is overpaid and paid
  if (!invoice.isPartiallyPaid && updateData?.isPartiallyPaid) {
    const paidTotal = invoiceCreditedAmount + (updateData.totalPaid || 0) || 0

    if (invoiceTotal === paidTotal) {
      updateData.status = 'paid'
      updateData.isPartiallyPaid = false
    } else if (paidTotal > invoiceTotal) {
      updateData.status = 'paid'
      updateData.isOverPaid = true
      updateData.isPartiallyPaid = false
    }
  }
  return updateData
}

export const calculateInvoiceStatusBaseOnTotalPaid = (invoiceData) => {
  const invoiceTotal = invoiceData.invoiceTotal || 0
  const creditedAmount = invoiceData.creditedAmount
    ? invoiceData.creditedAmount * -1
    : 0
  const paidTotal = creditedAmount + (invoiceData.totalPaid || 0) || 0
  const updateData = {}

  if (invoiceTotal === paidTotal) {
    updateData.status = 'paid'
    updateData.isPartiallyPaid = false
  } else if (paidTotal > invoiceTotal) {
    updateData.status = 'paid'
    updateData.isOverPaid = true
    updateData.isPartiallyPaid = false
  }
  return updateData
}

export const prepareEvictionInfoForInvoice = async (
  invoice,
  updatingData = {}
) => {
  if (!size(invoice)) return false

  const { contractId, invoiceType, partnerId } = invoice
  const params = { contractId, partnerId }
  if (invoiceType === 'invoice') {
    const evictionTagsForInvoice = await getInvoiceEvictionInfo(params)

    if (size(evictionTagsForInvoice)) {
      console.log(
        '===  Prepared evictionInfo for based on previous invoice ==='
      )
      const { setData = {} } = updatingData
      updatingData['setData'] = { ...setData, ...evictionTagsForInvoice }
      return updatingData
    }
    return null
  }
}

const getPipelinesForUpdatingInvoiceStatus = () => {
  const addFieldsForDueAmount = {
    $addFields: {
      totalDue: {
        $round: [
          {
            $cond: [
              {
                $or: [
                  { $eq: ['$invoiceType', 'landlord_invoice'] },
                  { $eq: ['$invoiceType', 'landlord_credit_note'] }
                ]
              },
              {
                $subtract: [
                  { $ifNull: ['$invoiceTotal', 0] },
                  {
                    $add: [
                      { $ifNull: ['$totalPaid', 0] },
                      { $ifNull: ['$totalBalanced', 0] }
                    ]
                  }
                ]
              },
              {
                $subtract: [
                  {
                    $add: [
                      { $ifNull: ['$invoiceTotal', 0] },
                      { $ifNull: ['$creditedAmount', 0] }
                    ]
                  },
                  {
                    $add: [
                      { $ifNull: ['$totalPaid', 0] },
                      { $ifNull: ['$lostMeta.amount', 0] }
                    ]
                  }
                ]
              }
            ]
          },
          {
            $cond: {
              if: { $eq: [2, 0] },
              then: 0,
              else: 2
            }
          }
        ]
      }
    }
  }

  const basicMatchQuery = {
    $match: {
      $or: [
        {
          invoiceType: 'invoice',
          invoiceTotal: 0,
          status: { $in: ['new', 'created'] }
        },
        {
          invoiceTotal: { $gt: 0 },
          $or: [
            { invoiceType: 'invoice' },
            {
              invoiceType: 'landlord_invoice',
              remainingBalance: { $gt: 0 },
              isPayable: true
            }
          ],
          status: { $in: ['new', 'created'] }
        },
        {
          totalDue: { $lte: -1 },
          isOverPaid: { $ne: true },
          status: { $nin: ['credited', 'lost'] }
        }
      ]
    }
  }
  const lookupForPartnerSettings = {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  }
  const unwindForPartnerSettings = {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  }
  const addFieldsForPartnerSettings = {
    $addFields: {
      partnerSettings: '$$REMOVE',
      timezone: '$partnerSettings.dateTimeSettings.timezone'
    }
  }
  const addFieldsForTodayDate = {
    $addFields: {
      todayDate: {
        $dateFromParts: {
          year: { $year: new Date() },
          month: { $month: new Date() },
          day: { $dayOfMonth: new Date() },
          hour: 0,
          minute: 0,
          second: 0,
          millisecond: 0,
          timezone: '$timezone'
        }
      }
    }
  }
  const addFieldsForStatus = {
    $addFields: {
      willBeOverdue: {
        $cond: [
          {
            $and: [
              { $gt: ['$totalDue', 0] },
              { $gt: ['$invoiceTotal', 0] },
              {
                $or: [
                  {
                    $and: [
                      { $lt: ['$dueDate', '$todayDate'] },
                      { $eq: ['$invoiceType', 'invoice'] }
                    ]
                  },
                  {
                    $and: [
                      { $lt: ['$delayDate', '$todayDate'] },
                      { $eq: ['$invoiceType', 'landlord_invoice'] }
                    ]
                  }
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      willBePaid: {
        $cond: [
          {
            $or: [
              {
                $and: [
                  { $lt: ['$dueDate', '$todayDate'] },
                  { $eq: ['$invoiceType', 'invoice'] },
                  { $eq: ['$invoiceTotal', 0] }
                ]
              },
              {
                $eq: ['$totalDue', 0]
              }
            ]
          },
          true,
          false
        ]
      },
      willBeOverPaid: {
        $cond: [
          {
            $and: [
              { $eq: ['$isOverPaid', false] },
              { $lte: ['$totalDue', -1] },
              {
                $or: [
                  { $eq: ['$invoiceType', 'invoice'] },
                  {
                    $and: [
                      { $eq: ['$invoiceType', 'landlord_invoice'] },
                      { $eq: ['$remainingBalance', 0] },
                      { $eq: ['$isPayable', true] }
                    ]
                  }
                ]
              }
            ]
          },
          true,
          false
        ]
      }
    }
  }

  const matchForStatus = {
    $match: {
      $or: [
        { willBeOverdue: true },
        { willBePaid: true },
        { willBeOverPaid: true }
      ]
    }
  }

  const groupByContract = {
    $group: {
      _id: '$contractId',
      partnerId: { $first: '$partnerId' },
      willBeOverdueInvoiceIds: {
        $push: { $cond: ['$willBeOverdue', '$_id', '$$REMOVE'] }
      },
      willBePaidInvoiceIds: {
        $push: { $cond: ['$willBePaid', '$_id', '$$REMOVE'] }
      },
      willBeOverPaidInvoiceIds: {
        $push: { $cond: ['$willBeOverPaid', '$_id', '$$REMOVE'] }
      }
    }
  }

  const projectPipeline = {
    $project: {
      _id: 0,
      contractId: '$_id',
      partnerId: '$partnerId',
      willBeOverdueInvoiceIds: {
        $cond: [
          { $gt: [{ $size: '$willBeOverdueInvoiceIds' }, 0] },
          '$willBeOverdueInvoiceIds',
          '$$REMOVE'
        ]
      },
      willBePaidInvoiceIds: {
        $cond: [
          { $gt: [{ $size: '$willBePaidInvoiceIds' }, 0] },
          '$willBePaidInvoiceIds',
          '$$REMOVE'
        ]
      },
      willBeOverPaidInvoiceIds: {
        $cond: [
          { $gt: [{ $size: '$willBeOverPaidInvoiceIds' }, 0] },
          '$willBeOverPaidInvoiceIds',
          '$$REMOVE'
        ]
      }
    }
  }

  return [
    addFieldsForDueAmount,
    basicMatchQuery,
    lookupForPartnerSettings,
    unwindForPartnerSettings,
    addFieldsForPartnerSettings,
    addFieldsForTodayDate,
    addFieldsForStatus,
    matchForStatus,
    groupByContract,
    projectPipeline,
    {
      $sort: { partnerId: 1 }
    }
  ]
}

export const getInvoicesForUpdatingInvoiceStatus = async (req) => {
  const { user = {} } = req || {}
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const pipelines = getPipelinesForUpdatingInvoiceStatus()
  const invoices = await InvoiceCollection.aggregate(pipelines)

  return invoices
}

export const getTotalCommissionAmountForBrokeringContract = async (
  contractId
) => {
  const result = await InvoiceCollection.aggregate([
    {
      $match: {
        contractId
      }
    },
    {
      $group: {
        _id: null,
        invoiceIds: {
          $push: '$_id'
        }
      }
    },
    {
      $lookup: {
        from: 'commissions',
        localField: 'invoiceIds',
        foreignField: 'invoiceId',
        as: 'commissions'
      }
    },
    {
      $addFields: {
        commissions: {
          $filter: {
            input: { $ifNull: ['$commissions', []] },
            as: 'commission',
            cond: {
              $eq: ['$$commission.type', 'brokering_contract']
            }
          }
        }
      }
    },
    {
      $unwind: {
        path: '$commissions',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: null,
        total: {
          $sum: '$commissions.amount'
        }
      }
    }
  ])
  const [data = {}] = result || []
  const { total = 0 } = data
  return total
}

export const prepareInvoiceStatusLostUpdateData = async (
  body = {},
  invoice = {}
) => {
  const { creditedAmount = 0, invoiceTotal = 0, totalPaid = 0 } = invoice
  const lostAmount = invoiceTotal - totalPaid + creditedAmount
  const lostMeta = {
    date: await appHelper.getActualDate(body.partnerId, false),
    amount: await appHelper.convertTo2Decimal(lostAmount)
  }

  return {
    $set: {
      status: 'lost',
      lostMeta
    },
    $unset: {
      isDefaulted: 1
    }
  }
}

export const validateCreditInvoiceData = async (body = {}, roles = []) => {
  const {
    contract,
    invoice = {},
    isPartlyCredited,
    partnerId,
    partnerSetting
  } = body
  if (isPartlyCredited) {
    appHelper.checkRequiredFields(['options', 'totalCreditAmount'], body)
    appHelper.checkRequiredFields(['remainingAmount'], body.options)
  }
  const { invoiceEndOn, invoiceStartOn } = invoice
  if (contract?.isFinalSettlementDone)
    throw new CustomError(400, 'Final settlement already done')
  if (!roles.includes('app_manager')) {
    const periodForInvoiceStartOn = (
      await appHelper.getActualDate(partnerSetting, true, invoiceStartOn)
    ).format('YYYY')
    const periodForInvoiceEndOn = (
      await appHelper.getActualDate(partnerSetting, true, invoiceEndOn)
    ).format('YYYY')
    const annualStatements =
      (await annualStatementHelper.getAnnualStatements({
        contractId: contract._id,
        partnerId,
        $or: [
          {
            statementYear: {
              $gte: parseInt(periodForInvoiceStartOn)
            }
          },
          {
            statementYear: {
              $gte: parseInt(periodForInvoiceEndOn)
            }
          }
        ]
      })) || []
    if (size(annualStatements)) {
      throw new CustomError(
        400,
        'You can not credit this invoice for this period'
      )
    }
  }
}

export const getNecessaryDataForCreditNote = async (body) => {
  const { invoiceId, partnerId, voidPayment: voidPaymentInput } = body

  const invoiceInfo = await invoiceHelper.getInvoice(
    {
      _id: invoiceId,
      partnerId,
      invoiceType: 'invoice',
      status: { $nin: ['credited', 'lost'] }
    },
    undefined,
    [
      'contract',
      'commissions',
      {
        path: 'partner',
        populate: 'partnerSetting'
      }
    ]
  )
  if (!size(invoiceInfo)) {
    throw new CustomError(404, 'Please provide correct invoice')
  }
  body.invoice = invoiceInfo
  body.invoiceCommissions = invoiceInfo.commissions
  console.log('Found commissions', size(invoiceInfo.commissions))
  if (!size(invoiceInfo.contract)) {
    throw new CustomError(404, 'Contract not found')
  }
  body.contract = invoiceInfo.contract
  if (
    !size(invoiceInfo.partner) ||
    !size(invoiceInfo.partner?.partnerSetting)
  ) {
    throw new CustomError(404, 'Please provide correct partner')
  }
  body.partner = invoiceInfo.partner
  body.partnerSetting = invoiceInfo.partner.partnerSetting
  if (voidPaymentInput) {
    body.voidPayment = !!(await invoicePaymentHelper.getInvoicePayment({
      'invoices.invoiceId': invoiceId
    }))
  }
}

export const prepareLandlordCreditNoteCreateData = async (data) => {
  const { contract, creditNote, invoice, partnerSetting } = data
  const landlordInvoiceData = pick(creditNote, [
    'partnerId',
    'propertyId',
    'contractId',
    'accountId',
    'agentId',
    'branchId',
    'tenantId',
    'dueDate',
    'invoiceMonth',
    'invoiceStartOn',
    'invoiceEndOn',
    'tenants',
    'createdAt',
    'createdBy'
  ])
  const landlord = await accountHelper.getAccountById(creditNote.accountId)
  const landlordInvoiceAccountNumber =
    await invoiceHelper.getInvoiceAccountNumber(contract, partnerSetting, {
      isFirstInvoice: false,
      isLandlordInvoice: true
    })

  landlordInvoiceData.totalPaid = 0
  landlordInvoiceData.status = 'new'
  landlordInvoiceData.invoiceType = 'landlord_credit_note'
  landlordInvoiceData.invoiceAccountNumber = landlordInvoiceAccountNumber
  landlordInvoiceData.receiver = {
    landlordName: landlord && landlord.name ? landlord.name : ''
  }

  landlordInvoiceData.commissionableTotal = 0
  landlordInvoiceData.totalTAX = 0
  landlordInvoiceData.isFirstInvoice = false
  landlordInvoiceData.isPartiallyPaid = false
  landlordInvoiceData.isOverPaid = false
  landlordInvoiceData.enabledNotification = false
  landlordInvoiceData.isPayable = false
  landlordInvoiceData.totalBalanced = 0
  landlordInvoiceData.fullyCredited = true

  if (invoice && invoice.landlordInvoiceId)
    landlordInvoiceData.invoiceId = invoice.landlordInvoiceId
  return landlordInvoiceData
}

export const getBasicInvoiceMakingData = async (contract = {}) => {
  const invoiceData = {}
  const tenant = await tenantHelper.getTenantById(
    contract?.rentalMeta?.tenantId
  )
  if (!tenant)
    throw new CustomError(
      404,
      'Trying to create rent invoice but tenant Id is wrong. contractId: ' +
        contract._id
    )

  //set initial date for the invoice

  invoiceData.contractId = contract._id
  invoiceData.partnerId = contract.partnerId
  invoiceData.tenantId = contract.rentalMeta.tenantId
  invoiceData.accountId = contract.accountId
  invoiceData.agentId = contract.agentId //should we change the agentId form contract once the property owner update?
  invoiceData.propertyId = contract.propertyId
  invoiceData.branchId = contract.branchId
  invoiceData.tenants = contract?.rentalMeta?.tenants || []

  invoiceData.receiver = {
    tenantName: tenant.name
  }

  invoiceData.totalPaid = 0
  invoiceData.status = 'new'
  invoiceData.invoiceType = 'invoice'
  invoiceData.invoiceFrequency = contract?.rentalMeta?.invoiceFrequency || 1

  return invoiceData
}

export const prepareDataForPartialCreditInvoice = async (data, session) => {
  const {
    contract,
    enabledNotification,
    invoice: mainInvoice,
    options = {},
    partnerSetting,
    totalCreditAmount,
    userId,
    voidPayment
  } = data
  if ((mainInvoice.invoiceTotal || 0) < totalCreditAmount) {
    throw new CustomError(
      400,
      'Total credited amount must not be greater than ' +
        mainInvoice.invoiceTotal
    )
  }
  const invoiceData = await getBasicInvoiceMakingData(contract)
  invoiceData.invoiceMonth = mainInvoice.invoiceMonth
  invoiceData.invoiceStartOn = mainInvoice.invoiceStartOn
  invoiceData.invoiceEndOn = mainInvoice.invoiceEndOn
  // Previous logic for dueDate
  // const contractDueDays = contract?.rentalMeta?.dueDate || 1
  // const today = await appHelper.getActualDate(partnerSetting, true)
  // const currentInvoiceDueDate = today.set('date', contractDueDays).toDate()
  // invoiceData.dueDate = await invoiceHelper.getInvoiceDueDate(
  //   partnerSetting,
  //   currentInvoiceDueDate
  // )
  invoiceData.dueDate = mainInvoice.dueDate
  invoiceData.isFirstInvoice = mainInvoice.isFirstInvoice
  invoiceData.enabledNotification = enabledNotification
  invoiceData.invoiceAccountNumber =
    await invoiceHelper.getInvoiceAccountNumber(contract, partnerSetting, {
      isFirstInvoice: invoiceData.isFirstInvoice
    })

  invoiceData.sender = await invoiceHelper.getSenderInfoForInvoice({
    accountId: invoiceData.accountId,
    invoiceAccountNumber: mainInvoice.invoiceAccountNumber,
    partnerSetting
  })

  // Add invoice content
  let invoiceContentTotal = 0
  let addonsMetaTotal = 0
  if (size(mainInvoice.invoiceContent)) {
    const newInvoiceContent = []
    let index = 0
    for (const content of mainInvoice.invoiceContent) {
      const creditedAmount = size(options?.invoiceContent)
        ? options.invoiceContent[index]?.creditedAmount || 0
        : 0
      const mainContentAmount = content.total
      content.price = await appHelper.convertTo2Decimal(
        content.price - creditedAmount
      )
      content.total = await appHelper.convertTo2Decimal(
        content.total - creditedAmount
      )
      if (content.total < 0) {
        throw new CustomError(
          400,
          'Credited rent amount must not be greater than ' + mainContentAmount
        )
      }
      if (content.total > 0) {
        newInvoiceContent.push(content)
        invoiceContentTotal += content.total
      }
      index++
    }
    invoiceData.invoiceContent = newInvoiceContent
  }

  // Add addons
  if (size(mainInvoice.addonsMeta)) {
    const newAddonsMeta = []
    let index = 0
    for (const addon of mainInvoice.addonsMeta) {
      const creditedAmount = size(options.addonsMeta)
        ? options.addonsMeta[index]?.creditedAmount || 0
        : 0
      const mainAddonAmount = addon.total
      addon.price = await appHelper.convertTo2Decimal(
        addon.price - creditedAmount
      )
      addon.total = await appHelper.convertTo2Decimal(
        addon.total - creditedAmount
      )
      if (addon.total < 0) {
        throw new CustomError(
          400,
          'Credited addon amount must not be greater than ' + mainAddonAmount
        )
      }
      if (addon.total !== 0) {
        newAddonsMeta.push(addon)
        addonsMetaTotal += addon.total
      }
      index++
    }
    invoiceData.addonsMeta = newAddonsMeta
  }

  // Add fees
  const feesParams = {
    contract,
    partnerSetting,
    isThisFirstInvoice: mainInvoice.isFirstInvoice
  }
  const { invoiceFeesMeta, feeTotal, feeTaxTotal } =
    await invoiceHelper.getInvoiceFeesMetaData(feesParams, session)

  if (size(invoiceFeesMeta)) {
    invoiceData.feesMeta = invoiceFeesMeta
  }

  // Before creating new invoice have to check invoice total amount is right
  const calculatedInvoiceTotal = await appHelper.getRoundedAmount(
    invoiceContentTotal + addonsMetaTotal + feeTotal,
    partnerSetting
  )
  if (calculatedInvoiceTotal) {
    const newInvoiceTotalWithDecimal = await appHelper.convertTo2Decimal(
      calculatedInvoiceTotal
    )
    const newRoundedInvoiceTotal = await appHelper.getRoundedAmount(
      calculatedInvoiceTotal,
      partnerSetting
    )
    const roundedAmount = newRoundedInvoiceTotal - newInvoiceTotalWithDecimal

    if (newRoundedInvoiceTotal)
      invoiceData.invoiceTotal = newRoundedInvoiceTotal
    if (roundedAmount !== 0) invoiceData.roundedAmount = roundedAmount
  }
  invoiceData.rentTotal = invoiceData.invoiceTotal

  if (mainInvoice.commissionableTotal) {
    invoiceData.commissionableTotal = await appHelper.convertTo2Decimal(
      invoiceContentTotal + addonsMetaTotal
    )
  }

  if (mainInvoice.payoutableAmount) {
    invoiceData.payoutableAmount = await appHelper.convertTo2Decimal(
      invoiceContentTotal + addonsMetaTotal
    )
  }

  if (mainInvoice.totalTAX) {
    invoiceData.totalTAX = await appHelper.convertTo2Decimal(feeTaxTotal || 0)
  }

  // set earlier invoice ID
  if (voidPayment) {
    invoiceData.earlierInvoiceId = mainInvoice._id
  }

  if (mainInvoice.isCorrectionInvoice) invoiceData.isCorrectionInvoice = true
  if (mainInvoice.correctionsIds)
    invoiceData.correctionsIds = mainInvoice.correctionsIds
  if (mainInvoice.isNonRentInvoice) invoiceData.isNonRentInvoice = true
  invoiceData.createdBy = userId

  return invoiceData
}

export const getRequiredDataForCreateCorrectionInvoice = async (
  body = {},
  session
) => {
  const { correctionId, partnerId } = body
  const correction = await correctionHelper.getCorrection(
    { _id: correctionId, partnerId },
    session,
    ['contract', 'partner', 'partnerSetting']
  )

  if (!size(correction)) {
    throw new CustomError(
      404,
      'Could not find any correction by this correctionId!'
    )
  }

  if (correction.addTo !== 'rent_invoice') {
    throw new CustomError(400, 'Could not create invoice for this correction')
  }

  if (correction.invoiceId) {
    throw new CustomError(400, 'Correction is not available for update')
  }
  const { contract, partner, partnerSetting } = correction

  if (!size(contract)) {
    throw new CustomError(404, 'Could not find Contract!')
  }
  if (!contract?.rentalMeta?.tenantId) {
    throw new CustomError(406, 'Contract not found for create invoice')
  }

  if (!size(partner) || !size(partnerSetting)) {
    throw new CustomError(404, 'Please provide correct partner')
  }

  return {
    contract,
    correction,
    partner,
    partnerSetting
  }
}

export const prepareCreateCorrectionInvoiceData = async (data = {}) => {
  const {
    contract,
    correction,
    dueDate,
    enabledNotification,
    invoiceData,
    partner,
    partnerSetting,
    today,
    userId
  } = data
  const correctionData = await getCorrectionDataByCorrectionInfo(correction)
  const creatingNewInvoiceData = {
    contract,
    correctionData,
    enabledNotification,
    data: { dueDate },
    invoiceData,
    partnerSetting,
    today
  }
  const preparedData = await prepareInvoiceData(creatingNewInvoiceData)
  preparedData.partner = partner
  const newInvoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(
    preparedData
  )
  newInvoiceData.createdBy = userId
  return newInvoiceData
}

export const validateInvoiceDelayDueDate = async (body) => {
  const { delayDate } = body
  const { invoiceId, partnerId } = body
  const invoice = await invoiceHelper.getInvoice(
    { _id: invoiceId, partnerId },
    null,
    [{ path: 'partner', populate: ['partnerSetting'] }]
  )
  if (!invoice) throw new CustomError(404, 'Invoice not found')
  if (delayDate) {
    const { partnerSetting } = invoice.partner || {}
    if (!partnerSetting) throw new CustomError(404, 'Partner not found')
    if (
      invoice.delayDate &&
      moment(invoice.delayDate).isSame(moment(delayDate), 'day')
    ) {
      throw new CustomError(400, 'Invoice already updated with this delay date')
    }

    const today = await appHelper.getActualDate(
      partnerSetting,
      false,
      new Date()
    )

    if (today > invoice.dueDate) invoice.dueDate = today
    if (moment(invoice.dueDate).isAfter(moment(delayDate))) {
      throw new CustomError(
        400,
        'Delay date should be before the invoice due date'
      )
    }

    const contract = await contractHelper.getContractById(invoice.contractId)
    if (
      contract.rentalMeta?.contractEndDate &&
      moment(contract.rentalMeta.contractEndDate).isBefore(moment(delayDate))
    ) {
      throw new CustomError(
        400,
        'Delay date should be after the contract end date'
      )
    }
  } else if (!delayDate && !invoice.delayDate) {
    throw new CustomError(400, 'Delay date is required')
  }
  body.previous = invoice
  return body
}

const getFinalSettlementDoneContractIds = async (body) => {
  const { partnerId, query = {} } = body
  const { openFrom, propertyId } = query
  const preparedQuery = {
    partnerId,
    status: 'closed',
    isFinalSettlementDone: true
  }
  if (openFrom === 'property' && propertyId)
    preparedQuery.propertyId = propertyId
  return await contractHelper.getUniqueFieldValue('_id', preparedQuery)
}

const prepareQueryForDropdownInvoices = async (body) => {
  const { query = {}, partnerId } = body
  const {
    contractId,
    isPayable,
    propertyId,
    searchKeyword,
    serialId,
    tenantId
  } = query
  const contractIds = await getFinalSettlementDoneContractIds(body)
  const invoiceQuery = {
    partnerId
  }
  if (tenantId) invoiceQuery.tenantId = tenantId
  if (propertyId) invoiceQuery.propertyId = propertyId
  if (serialId) invoiceQuery['invoiceSerialId'] = serialId
  if (isPayable) {
    invoiceQuery['$or'] = [
      {
        invoiceType: 'landlord_invoice',
        isPayable: true
      },
      {
        invoiceType: 'invoice',
        contractId: contractId
          ? contractId
          : size(contractIds)
          ? { $nin: contractIds }
          : undefined
      }
    ]
  } else {
    invoiceQuery.invoiceType = 'invoice'
    if (contractId) invoiceQuery.contractId = contractId
    else if (size(contractIds)) invoiceQuery.contractId = { $nin: contractIds }
  }

  if (searchKeyword) {
    invoiceQuery.invoiceSerialId = searchKeyword
  }

  return invoiceQuery
}

const queryInvoicesForDropdown = async (body, invoiceQuery) => {
  const { options } = body
  const { limit, skip, sort } = options
  if (size(sort)) appHelper.validateSortForQuery(sort)
  const invoices = await InvoiceCollection.aggregate([
    {
      $match: invoiceQuery
    },
    {
      $addFields: {
        isPartnerInvoice: true
      }
    },
    ...(size(sort) ? [{ $sort: sort }] : []),
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $project: {
        creditedAmount: 1,
        dueDate: 1,
        invoiceMonth: 1,
        invoiceSerialId: 1,
        invoiceTotal: 1,
        invoiceType: 1,
        isPartnerInvoice: 1,
        isPayable: 1,
        lostMeta: 1,
        propertyId: 1,
        status: 1,
        tenantId: 1,
        totalBalanced: 1,
        totalPaid: 1
      }
    }
  ])

  return invoices || []
}

export const getInvoicesForDropdown = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const { partnerId } = body
  const invoiceQuery = await prepareQueryForDropdownInvoices(body)
  const invoices = await queryInvoicesForDropdown(body, invoiceQuery)
  const filteredDocuments = await countInvoices(invoiceQuery)
  const totalDocuments = await countInvoices({
    partnerId
  })
  return {
    data: invoices,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

const getTotalAmountForDuration = async (params = {}) => {
  let totalRent = 0
  const { endMonthDate, monthlyRentAmount, partnerSetting, startMonthDate } =
    params
  const listOfMonths = await getListOfMonths(params)
  if (size(listOfMonths)) {
    for (const [index] of listOfMonths.entries()) {
      let monthStartDate = (
        await appHelper.getActualDate(partnerSetting, true, startMonthDate)
      )
        .add(index, 'month')
        .toDate()
      if (index > 0) {
        monthStartDate = (
          await appHelper.getActualDate(partnerSetting, true, monthStartDate)
        )
          .startOf('month')
          .toDate()
      }

      let monthEndDate = (
        await appHelper.getActualDate(partnerSetting, true, monthStartDate)
      )
        .endOf('month')
        .toDate()
      if (
        (
          await appHelper.getActualDate(partnerSetting, true, monthEndDate)
        ).isAfter(
          await appHelper.getActualDate(partnerSetting, true, endMonthDate)
        )
      ) {
        monthEndDate = endMonthDate
      }

      const actualDaysInMonth =
        (
          await appHelper.getActualDate(partnerSetting, true, monthEndDate)
        ).diff(
          await appHelper.getActualDate(partnerSetting, true, monthStartDate),
          'days'
        ) + 1
      const totalDaysInMonth = (
        await appHelper.getActualDate(partnerSetting, true, monthStartDate)
      ).daysInMonth()
      let totalAmountOfMonth = monthlyRentAmount

      if (actualDaysInMonth !== totalDaysInMonth) {
        totalAmountOfMonth =
          (monthlyRentAmount / totalDaysInMonth) * actualDaysInMonth
      }

      totalRent += totalAmountOfMonth
    }
  }
  return totalRent
}

export const prepareEstimatedPayoutMetaData = async (params = {}) => {
  const { invoices = [], contract } = params
  const updateData = {}
  let firstEstimatedPayout = 0
  let firstMonthManagementCommission = 0
  let firstMonthPayoutAddons = 0
  let firstMonthAddonsCommission = 0
  let firstRentInvoice = 0
  let secondEstimatedPayment = 0
  let secondMonthManagementCommission = 0
  let secondMonthPayoutAddons = 0
  let secondMonthAddonsCommission = 0
  let secondRentInvoice = 0
  let thirdEstimatedPayment = 0
  let thirdMonthManagementCommission = 0
  let thirdMonthPayoutAddons = 0
  let thirdMonthAddonsCommission = 0
  let thirdRentInvoice = 0
  let secondAmountMovedFromLastPayout = 0
  let thirdAmountMovedFromLastPayout = 0
  for (let i = 0; i < invoices.length; i++) {
    const invoiceInfo = invoices[i]
    const invoiceCommissions = await createInvoiceCommission(
      invoiceInfo,
      true,
      contract
    )
    const invoiceFees = getTotalInvoiceFees(invoiceInfo)
    // calculate the first, second and third estimated payout
    const invoiceCommissionTotal = invoiceCommissions.total || 0
    const managementCommissionTotal =
      invoiceCommissions.managementCommissionTotal || 0
    const assignmentAddonTotal = invoiceCommissions.assignmentAddonTotal || 0
    const addonsCommissionTotal = invoiceCommissions.addonsCommissionTotal || 0

    if (i === 0) {
      firstEstimatedPayout =
        invoiceInfo.invoiceTotal - invoiceCommissionTotal - invoiceFees
      firstMonthManagementCommission = managementCommissionTotal
      firstMonthPayoutAddons = assignmentAddonTotal
      firstMonthAddonsCommission = addonsCommissionTotal
      firstRentInvoice = invoiceInfo.payoutableAmount
        ? invoiceInfo.payoutableAmount
        : 0
    } else if (i === 1) {
      secondEstimatedPayment =
        invoiceInfo.invoiceTotal - invoiceCommissionTotal - invoiceFees
      secondMonthManagementCommission = managementCommissionTotal
      secondMonthPayoutAddons = assignmentAddonTotal
      secondMonthAddonsCommission = addonsCommissionTotal
      secondRentInvoice = invoiceInfo.payoutableAmount
        ? invoiceInfo.payoutableAmount
        : 0
    } else if (i === 2) {
      thirdEstimatedPayment =
        invoiceInfo.invoiceTotal - invoiceCommissionTotal - invoiceFees
      thirdMonthManagementCommission = managementCommissionTotal
      thirdMonthPayoutAddons = assignmentAddonTotal
      thirdMonthAddonsCommission = addonsCommissionTotal
      thirdRentInvoice = invoiceInfo.payoutableAmount
        ? invoiceInfo.payoutableAmount
        : 0
    }
  }

  // estimated payout less then 0 then calculation
  if (firstEstimatedPayout < 0) {
    secondAmountMovedFromLastPayout = firstEstimatedPayout * -1
    secondEstimatedPayment = secondEstimatedPayment + firstEstimatedPayout
    firstEstimatedPayout = 0
  }
  if (secondEstimatedPayment < 0) {
    thirdAmountMovedFromLastPayout = secondEstimatedPayment * -1
    thirdEstimatedPayment = thirdEstimatedPayment + secondEstimatedPayment
    secondEstimatedPayment = 0
  }
  if (thirdEstimatedPayment < 0) {
    thirdEstimatedPayment = 0
  }

  if (firstEstimatedPayout)
    updateData['rentalMeta.estimatedPayouts.firstMonth'] =
      await appHelper.convertTo2Decimal(firstEstimatedPayout)
  if (secondEstimatedPayment)
    updateData['rentalMeta.estimatedPayouts.secondMonth'] =
      await appHelper.convertTo2Decimal(secondEstimatedPayment)
  if (thirdEstimatedPayment)
    updateData['rentalMeta.estimatedPayouts.thirdMonth'] =
      await appHelper.convertTo2Decimal(thirdEstimatedPayment)

  if (firstMonthManagementCommission)
    updateData['rentalMeta.estimatedPayouts.firstMonthManagementCommission'] =
      await appHelper.convertTo2Decimal(firstMonthManagementCommission)
  if (secondMonthManagementCommission)
    updateData['rentalMeta.estimatedPayouts.secondMonthManagementCommission'] =
      await appHelper.convertTo2Decimal(secondMonthManagementCommission)
  if (thirdMonthManagementCommission)
    updateData['rentalMeta.estimatedPayouts.thirdMonthManagementCommission'] =
      await appHelper.convertTo2Decimal(thirdMonthManagementCommission)

  if (firstMonthPayoutAddons)
    updateData['rentalMeta.estimatedPayouts.firstMonthPayoutAddons'] =
      await appHelper.convertTo2Decimal(firstMonthPayoutAddons)
  if (secondMonthPayoutAddons)
    updateData['rentalMeta.estimatedPayouts.secondMonthPayoutAddons'] =
      await appHelper.convertTo2Decimal(secondMonthPayoutAddons)
  if (thirdMonthPayoutAddons)
    updateData['rentalMeta.estimatedPayouts.thirdMonthPayoutAddons'] =
      await appHelper.convertTo2Decimal(thirdMonthPayoutAddons)

  if (firstMonthAddonsCommission)
    updateData['rentalMeta.estimatedPayouts.firstMonthAddonsCommission'] =
      await appHelper.convertTo2Decimal(firstMonthAddonsCommission)
  if (secondMonthAddonsCommission)
    updateData['rentalMeta.estimatedPayouts.secondMonthAddonsCommission'] =
      await appHelper.convertTo2Decimal(secondMonthAddonsCommission)
  if (thirdMonthAddonsCommission)
    updateData['rentalMeta.estimatedPayouts.thirdMonthAddonsCommission'] =
      await appHelper.convertTo2Decimal(thirdMonthAddonsCommission)

  if (secondAmountMovedFromLastPayout)
    updateData['rentalMeta.estimatedPayouts.secondAmountMovedFromLastPayout'] =
      await appHelper.convertTo2Decimal(secondAmountMovedFromLastPayout)
  if (thirdAmountMovedFromLastPayout)
    updateData['rentalMeta.estimatedPayouts.thirdAmountMovedFromLastPayout'] =
      await appHelper.convertTo2Decimal(thirdAmountMovedFromLastPayout)

  if (firstRentInvoice)
    updateData['rentalMeta.estimatedPayouts.firstRentInvoice'] =
      await appHelper.convertTo2Decimal(firstRentInvoice)
  if (secondRentInvoice)
    updateData['rentalMeta.estimatedPayouts.secondRentInvoice'] =
      await appHelper.convertTo2Decimal(secondRentInvoice)
  if (thirdRentInvoice)
    updateData['rentalMeta.estimatedPayouts.thirdRentInvoice'] =
      await appHelper.convertTo2Decimal(thirdRentInvoice)
  return updateData
}

const checkInvoiceDurationForHigherInvoiceFrequency = async (params = {}) => {
  const { contractInfo, invoiceStartOn, invoiceEndOn, partnerSettings } = params
  const { _id: contractId, rentalMeta = {} } = contractInfo || {}
  const {
    contractStartDate,
    contractEndDate,
    invoiceStartFrom,
    invoiceFrequency = 1
  } = rentalMeta || {}

  const contractStart = await appHelper.getActualDate(
    partnerSettings,
    false,
    contractStartDate
  )
  const contractEnd = size(contractEndDate)
    ? await appHelper.getActualDate(partnerSettings, false, contractEndDate)
    : (await appHelper.getActualDate(partnerSettings, true, new Date()))
        .add(3, 'years')
        .toDate()
  let invoiceStart = await appHelper.getActualDate(
    partnerSettings,
    false,
    invoiceStartFrom
  )

  if (invoiceStart < contractStart) invoiceStart = contractStart

  const stringOfContractEndDate = (
    await appHelper.getActualDate(partnerSettings, true, contractEndDate)
  ).format('YYYY-MM')
  const stringOfInvoiceStartOn = invoiceStartOn
    ? (
        await appHelper.getActualDate(partnerSettings, true, invoiceStartOn)
      ).format('YYYY-MM')
    : false
  const stringOfInvoiceEndOn = invoiceEndOn
    ? (
        await appHelper.getActualDate(partnerSettings, true, invoiceEndOn)
      ).format('YYYY-MM')
    : false
  const listOfMonthsInDate =
    (await getListOfMonths({
      endMonthDate: contractEnd,
      invoiceFrequency,
      partnerSetting: partnerSettings,
      startMonthDate: invoiceStart
    })) || []
  const listOfStartingMonthsInString = []
  for (const month of listOfMonthsInDate) {
    listOfStartingMonthsInString.push(
      (await appHelper.getActualDate(partnerSettings, true, month)).format(
        'YYYY-MM'
      )
    )
  }
  const listOfEndingMonthsInString = []
  for (const month of listOfMonthsInDate) {
    listOfEndingMonthsInString.push(
      (await appHelper.getActualDate(partnerSettings, true, month))
        .add(invoiceFrequency - 1, 'months')
        .format('YYYY-MM')
    )
  }
  const isInvoiceStartOnRight = !!includes(
    listOfStartingMonthsInString,
    stringOfInvoiceStartOn
  )
  const isInvoiceEndOnRight = !!(
    includes(listOfEndingMonthsInString, stringOfInvoiceEndOn) ||
    stringOfContractEndDate === stringOfInvoiceEndOn
  )

  console.log(
    '+++ Checking invoice start on date string and list of months string, stringOfInvoiceStartOn:',
    stringOfInvoiceStartOn,
    ', listOfStartingMonthsInString:',
    listOfStartingMonthsInString,
    ', contractId:',
    contractId,
    '+++'
  )
  console.log(
    '+++ Checking invoice end on date string and list of months string, stringOfInvoiceEndOn:',
    stringOfInvoiceEndOn,
    ', stringOfContractEndDate:',
    stringOfContractEndDate,
    ', listOfEndingMonthsInString:',
    listOfEndingMonthsInString,
    ', contractId:',
    contractId,
    '+++'
  )

  return !!(isInvoiceStartOnRight && isInvoiceEndOnRight)
}

export const getTotalDueAmount = async (invoice) => {
  if (!size(invoice))
    throw new CustomError(404, 'Invoice not found while getting TotalDueAmount')
  const {
    creditedAmount = 0,
    invoiceTotal = 0,
    lostMeta = {},
    totalBalanced = 0,
    totalPaid = 0
  } = invoice
  const invoiceLostAmount = lostMeta?.amount || 0
  let dueTotal = 0

  dueTotal = await appHelper.convertTo2Decimal(
    invoiceTotal - totalPaid + creditedAmount - invoiceLostAmount
  )

  if (!isNotLandlord(clone(invoice))) {
    dueTotal = await appHelper.convertTo2Decimal(
      invoiceTotal - totalPaid - totalBalanced
    )
  }

  return dueTotal
}

export const getRefundableInvoices = async (params = {}, session) => {
  const { contractId, terminationDate } = params
  const invoiceQuery = {
    contractId,
    invoiceType: 'invoice',
    isCorrectionInvoice: { $ne: true },
    status: { $nin: ['credited', 'lost'] }
  }
  if (terminationDate) {
    invoiceQuery.invoiceEndOn = {
      $gt: terminationDate
    }
  }
  const invoices = await getInvoices(invoiceQuery, session)
  return size(invoices) ? invoices : []
}

export const prepareCreateLandlordInvoiceForExtraPayoutData = async (
  body = {}
) => {
  const { contractId, partnerId, propertyId } = body

  const contractInfo = await contractHelper.getAContract(
    { _id: contractId, partnerId, propertyId },
    null,
    ['partnerSetting']
  )

  if (!size(contractInfo)) {
    throw new CustomError(404, 'Please provide valid lease: ' + contractId)
  }
  const { partnerSetting = {} } = contractInfo

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner settings not found')
  }

  const payoutInfo = await payoutHelper.getPayout({
    amount: { $lt: 0 },
    contractId,
    partnerId,
    status: 'estimated'
  })

  if (!size(payoutInfo)) return false

  let invoiceData = await getInvoiceDataForLandlordInvoice(contractInfo)

  const today = await getInvoiceDate(new Date(), partnerSetting)
  let invoiceStartOn = (
    await appHelper.getActualDate(partnerSetting, true, today)
  )
    .startOf('month')
    .toDate()

  let invoiceEndOn = await appHelper.getActualDate(partnerSetting, true, today)

  const [lastInvoice] = await getInvoices(
    { contractId, invoiceType: 'invoice' },
    null,
    { limit: 1, sort: { invoiceSerialId: -1 } }
  )

  if (size(lastInvoice)) {
    invoiceStartOn = await appHelper.getActualDate(
      partnerSetting,
      false,
      lastInvoice.invoiceStartOn
    )
    invoiceEndOn = await appHelper.getActualDate(
      partnerSetting,
      false,
      lastInvoice.invoiceEndOn
    )
  }
  const dateFormat = await appHelper.getDateFormat(partnerSetting)
  const dueDate = moment(today, dateFormat)
    .add(partnerSetting?.landlordInvoiceDueDays || 0, 'days')
    .toDate()

  const landlordInvoiceAccountNumber = await getInvoiceAccountNumber(
    contractInfo,
    partnerSetting,
    {
      isFirstInvoice: invoiceData.isFirstInvoice,
      isLandlordInvoice: true
    }
  )

  const feesParams = {
    contract: contractInfo,
    partnerSetting,
    isThisFirstInvoice: invoiceData.isFirstInvoice,
    isLandlordInvoice: true
  }

  const invoiceFeesMetaData = await getInvoiceFeesMetaData(feesParams)
  const feesMeta = invoiceFeesMetaData?.invoiceFeesMeta
  const finalSettlementAmount = Math.abs(clone(payoutInfo.amount || 0))
  const invoiceContent = [
    {
      type: 'final_settlement',
      qty: 1,
      price: finalSettlementAmount,
      total: finalSettlementAmount,
      taxPercentage: 0
    }
  ]

  if (size(feesMeta)) invoiceData.feesMeta = feesMeta

  invoiceData = {
    ...invoiceData,
    dueDate,
    invoiceAccountNumber: landlordInvoiceAccountNumber,
    invoiceContent,
    invoiceEndOn,
    invoiceStartOn,
    isFinalSettlement: true,
    isFirstInvoice: false,
    isPayable: true
  }

  const requiredData = {
    addonTotalAmount: 0,
    enabledNotification: true,
    invoiceCommissionableTotal: 0,
    invoiceData,
    feeTaxTotal: invoiceFeesMetaData.feeTaxTotal,
    feeTotal: invoiceFeesMetaData.feeTotal,
    landlordInvoiceTotal: 0,
    monthlyRentAmount: finalSettlementAmount,
    options: { returnPreview: false },
    partnerSetting
  }
  return { finalSettlementAmount, requiredData, payoutInfo }
}

export const getNecessaryDataForCreateLandlordInvoiceOrCreditNote = async (
  body
) => {
  const { invoiceId, partnerId } = body

  const invoiceInfo = await invoiceHelper.getInvoice(
    {
      _id: invoiceId,
      partnerId,
      $or: [
        {
          invoiceType: 'landlord_invoice',
          status: { $ne: 'credited' },
          forCorrection: { $ne: true }
        },
        {
          invoiceType: 'landlord_credit_note',
          forCorrection: true,
          isCreditedForCancelledCorrection: { $ne: true }
        }
      ]
    },
    undefined,
    ['contract', 'partner', 'partnerSetting']
  )
  if (!size(invoiceInfo)) {
    throw new CustomError(404, 'Please provide correct invoice info')
  }
  const {
    contract,
    isCreditedForCancelledCorrection,
    status,
    partner,
    partnerSetting
  } = invoiceInfo
  if (
    !status ||
    indexOf(['credited', 'lost'], status) !== -1 ||
    isCreditedForCancelledCorrection
  ) {
    throw new CustomError(400, 'Invoice is not available for create')
  }

  if (!size(contract)) {
    throw new CustomError(404, 'Contract not found')
  }

  if (!size(partner) || !size(partnerSetting)) {
    throw new CustomError(404, 'Please provide correct partner')
  }
  body.invoice = invoiceInfo
  body.contract = contract
  body.partner = partner
  body.partnerSetting = partnerSetting
}

export const prepareLandlordCreditInvoiceOrNoteData = async (data) => {
  const { addons, fees, invoice } = data
  const creditNoteData = pick(invoice, [
    'accountId',
    'agentId',
    'branchId',
    'contractId',
    'invoiceAccountNumber',
    'invoiceMonth',
    'partnerId',
    'propertyId',
    'receiver',
    'sender',
    'tenantId',
    'tenants'
  ])

  let userLanguage = 'en'

  if (invoice.invoiceType === 'landlord_invoice') {
    const accountInfo =
      (await accountHelper.getAnAccount(
        { _id: creditNoteData.accountId },
        undefined,
        ['person']
      )) || {}

    const language = accountInfo?.person?.profile?.language
    if (language) userLanguage = language
  } else {
    const tenantInfo =
      (await tenantHelper.getATenant(
        { _id: creditNoteData.tenantId },
        undefined,
        ['user']
      )) || {}
    const language = tenantInfo?.user?.profile?.language
    if (language) userLanguage = language
  }

  creditNoteData.creditReason = appHelper.translateToUserLng(
    'common.credit_reason',
    userLanguage
  )

  const { invoiceStartOn, invoiceEndOn } = await getCreditDateRange(data)
  creditNoteData.invoiceStartOn = invoiceStartOn
  creditNoteData.invoiceEndOn = invoiceEndOn
  creditNoteData.status = 'new'
  creditNoteData.invoiceType = 'landlord_credit_note'
  creditNoteData.invoiceId = invoice._id
  creditNoteData.dueDate = invoice.dueDate

  creditNoteData.fullyCredited = !!(
    data.isCreditFull || data.isCreditFullByPartiallyCredited
  )
  if (invoice.isNonRentInvoice) {
    creditNoteData.isNonRentInvoice = true
  }
  if (size(fees.creditableFees)) {
    creditNoteData.feesMeta = fees.creditableFees
  }
  if (size(addons.creditableAddons)) {
    creditNoteData.addonsMeta = addons.creditableAddons
  }

  if (size(invoice.correctionsIds) && creditNoteData.addonsMeta)
    creditNoteData.correctionsIds = invoice.correctionsIds

  if (invoice.invoiceType === 'landlord_credit_note') {
    creditNoteData.invoiceType = 'landlord_invoice'
    creditNoteData.forCorrection = true
    creditNoteData.status = 'credited'
    creditNoteData.isPartiallyCredited = false
    creditNoteData.creditedAmount = invoice.invoiceTotal
    creditNoteData.creditNoteIds = [invoice._id]
    delete creditNoteData.invoiceId
    delete creditNoteData.creditReason
  }

  return creditNoteData
}

export const prepareCreateLandlordInvoiceOrNoteData = async (data = {}) => {
  const { partnerSetting } = data
  data.today = await appHelper.getActualDate(partnerSetting, false)
  data.dateFormat = 'YYYY-MM-DD'

  data.invoiceTotalDays = await getInvoiceTotalDays(data)
  data.creditableDays = await getCreditableDays(data)
  data.isCreditFull = true
  data.isCreditFullByPartiallyCredited = await isCreditFullByPartiallyCredited(
    data
  )

  data.monthlyRent = await getCreditableRent(data)
  data.fees = getCreditableFees(data)
  data.addons = await getCreditableAddons(data)
  data.feeTaxTotal = 0
  data.invoiceCommissionableTotal = 0
  data.feeTotal = data.fees?.feeTotal ? data.fees.feeTotal : 0
  data.addonTotalAmount = data.addons?.addonsTotal ? data.addons.addonsTotal : 0
  data.monthlyRentAmount = (data.monthlyRent || 0) * -1
  data.enabledNotification = false

  data.invoiceData = await prepareLandlordCreditInvoiceOrNoteData(data)
  return data
}

export const prepareDataForRemoveInvoiceFees = async (body) => {
  const { removeType } = body
  const type = removeType
  let logAction = ''
  let transactionSubType = ''
  let moveToType = ''
  if (removeType === 'reminder') {
    logAction = 'removed_reminder_fee'
    transactionSubType = 'invoice_reminder_fee'
    moveToType = 'reminder_fee_move_to'
  }

  if (removeType === 'collection_notice') {
    logAction = 'removed_collection_notice'
    transactionSubType = 'collection_notice_fee'
    moveToType = 'collection_notice_fee_move_to'
  }

  if (removeType === 'eviction_notice') {
    logAction = 'removed_eviction_notice_fee'
    transactionSubType = 'eviction_notice_fee'
    moveToType = 'eviction_notice_fee_move_to'
  }

  if (removeType === 'administration_eviction_notice') {
    logAction = 'removed_administration_eviction_notice'
    transactionSubType = 'administration_eviction_notice_fee'
    moveToType = 'administration_eviction_notice_fee_move_to'
  }
  return {
    logAction,
    moveToType,
    type,
    transactionSubType
  }
}

export const isCreditableInvoice = async (invoiceId, session) => {
  const hasInProgressCommissions = await appQueueHelper.getAnAppQueue(
    {
      event: 'add_invoice_commissions',
      status: { $ne: 'completed' },
      'params.invoiceId': invoiceId
    },
    session
  )

  if (size(hasInProgressCommissions)) {
    throw new CustomError(
      400,
      'Commission creation process for this invoice is in progress, please try again later'
    )
  }
  return true
}

export const getMissingInvoiceSerialIdsQueueParams = async (
  query = {},
  options = {}
) =>
  InvoiceCollection.aggregate()
    .match({ ...query, invoiceSerialId: { $exists: false } })
    .sort(options?.sort || { createdAt: 1 })
    .lookup({
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    })
    .addFields({ partner: { $first: '$partner' } })
    .addFields({
      isAccountWiseSerialId: {
        $and: [
          { $eq: ['$partner.accountType', 'direct'] },
          { $eq: ['$partner.enableInvoiceStartNumber', true] }
        ]
      }
    })
    .group({
      _id: null,
      queueParams: {
        $addToSet: {
          accountId: {
            $cond: [
              {
                $and: [
                  { $eq: ['$isAccountWiseSerialId', true] },
                  { $ne: ['$isFinalSettlement', true] }
                ]
              },
              '$accountId',
              '$$REMOVE'
            ]
          },
          isAccountWiseSerialId: {
            $cond: [
              {
                $and: [
                  { $eq: ['$isAccountWiseSerialId', true] },
                  { $ne: ['$isFinalSettlement', true] }
                ]
              },
              '$isAccountWiseSerialId',
              false
            ]
          },
          isFinalSettlementInvoice: {
            $cond: [
              { $eq: ['$isFinalSettlement', true] },
              '$isFinalSettlement',
              false
            ]
          },
          partnerId: '$partnerId'
        }
      }
    })
    .addFields({
      queueParams: {
        $slice: ['$queueParams', options?.skip || 0, options?.limit || 50]
      }
    })
    .unwind('$queueParams')
    .replaceRoot('$queueParams')
    .lookup({
      from: 'app_queues',
      localField: 'partnerId',
      foreignField: 'params.partnerId',
      let: {
        isAccountWiseSerialId: '$isAccountWiseSerialId',
        isFinalSettlementInvoice: '$isFinalSettlementInvoice'
      },
      pipeline: [
        {
          $match: {
            action: 'add_serialIds',
            'params.collectionNameStr': 'invoices',
            status: { $ne: 'completed' },
            noOfRetry: { $lt: 5 }
          }
        },
        {
          $addFields: {
            isAccountWiseSerialId: {
              $cond: [
                { $ifNull: ['$params.isAccountWiseSerialId', false] },
                true,
                false
              ]
            },
            isFinalSettlementInvoice: {
              $cond: [
                { $ifNull: ['$params.isFinalSettlementInvoice', false] },
                true,
                false
              ]
            }
          }
        },
        {
          $addFields: {
            isMatched: {
              $cond: [
                {
                  $and: [
                    {
                      $eq: ['$isAccountWiseSerialId', '$$isAccountWiseSerialId']
                    },
                    {
                      $eq: [
                        '$isFinalSettlementInvoice',
                        '$$isFinalSettlementInvoice'
                      ]
                    }
                  ]
                },
                true,
                false
              ]
            }
          }
        },
        {
          $match: { isMatched: true }
        },
        { $sort: { createdAt: -1 } },
        { $limit: 1 }
      ],
      as: 'queue'
    })
    .addFields({ queue: { $first: '$queue' } })
    .match({ 'queue._id': { $exists: false } })

export const getInvoiceDataForB2CCompelloEInvoice = async (req = {}) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['invoiceId', 'partnerId'], query)
  let invoice = {}

  if (query.sendToCompello) {
    const pipeline = preparePipelineForB2CCompelloEInvoice(query)
    const invoiceArray = await getInvoicesViaAggregation(pipeline)
    invoice = (invoiceArray && invoiceArray[0]) || {}
    if (!size(invoice)) {
      const hasInvoice = await getInvoice({
        _id: query.invoiceId
      })
      console.log('Invoice for Compello', hasInvoice)
      if (hasInvoice) {
        throw new CustomError(
          404,
          'Invoice is not available for sending to Compello'
        )
      }
    } else if (invoice?.file) {
      invoice.invoicePdfFileKey = fileHelper.getFileKey(invoice?.file)
    }
  } else {
    invoice = await getInvoice({
      _id: query.invoiceId,
      partnerId: query.partnerId
    })
  }
  if (!size(invoice)) {
    throw new CustomError(404, 'Invoice not found')
  }
  return invoice
}

export const getInvoiceDataForB2BCompelloEInvoice = async (req = {}) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['invoiceId', 'partnerId'], query)

  const pipeline = preparePipelineForB2BCompelloEInvoice(query)
  const invoiceArray = await getInvoicesViaAggregation(pipeline)
  const invoice = (invoiceArray && invoiceArray[0]) || {}
  if (!size(invoice)) {
    const hasInvoice = await getInvoice({
      _id: query.invoiceId
    })
    console.log('Invoice for B2B Compello', hasInvoice)
    if (hasInvoice) {
      throw new CustomError(
        404,
        'Invoice is not available for sending to Compello'
      )
    }
  }

  if (!size(invoice)) {
    throw new CustomError(404, 'Invoice not found')
  }

  // if (!size(invoice?.files)) {
  //   throw new CustomError(404, "Invoice don't has any files")
  // }

  invoice.fileUrl = invoice?.files?.fileUrlHash
    ? await appHelper.createDownloadUrl(invoice?.files?.fileUrlHash)
    : ''
  return invoice
}

export const preparePipelineForB2CCompelloEInvoice = ({
  invoiceId,
  partnerId
}) => {
  const invoicePipelines = [
    {
      $match: {
        _id: invoiceId,
        partnerId
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        foreignField: 'partnerId',
        localField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $lookup: {
        from: 'partners',
        foreignField: '_id',
        localField: 'partnerId',
        as: 'partners'
      }
    },
    {
      $lookup: {
        from: 'contracts',
        foreignField: '_id',
        localField: 'contractId',
        as: 'contracts'
      }
    },
    { $unwind: '$contracts' },
    {
      $lookup: {
        from: 'tenants',
        foreignField: '_id',
        localField: 'tenantId',
        as: 'tenants'
      }
    },
    {
      $unwind: {
        path: '$pdf',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: {
        'pdf.type': 'invoice_pdf'
      }
    },
    {
      $lookup: {
        from: 'files',
        foreignField: '_id',
        localField: 'pdf.fileId',
        as: 'files'
      }
    },
    {
      $addFields: {
        partners: {
          $first: '$partners'
        },
        partnerSettings: {
          $first: '$partnerSettings'
        },
        file: {
          $first: '$files'
        }
      }
    },
    {
      $lookup: {
        from: 'users',
        foreignField: '_id',
        localField: 'tenants.userId',
        as: 'users'
      }
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        pipeline: [
          {
            $lookup: {
              from: 'users',
              localField: 'personId',
              foreignField: '_id',
              as: 'person'
            }
          },
          {
            $unwind: {
              path: '$person',
              preserveNullAndEmptyArrays: true
            }
          }
        ],
        as: 'account'
      }
    },
    {
      $unwind: {
        path: '$account',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        accountAddress: {
          $cond: {
            if: {
              $and: [
                { $ifNull: ['$account.person', false] },
                { $ifNull: ['$account.person._id', false] },
                { $eq: ['$account.person._id', '$account.personId'] }
              ]
            },
            then: {
              address1: '$account.person.profile.hometown',
              city: '$account.person.profile.city',
              postCode: '$account.person.profile.zipCode'
            },
            else: {
              address1: '$account.address',
              city: '$account.city',
              postCode: '$account.zipCode'
            }
          }
        },
        partnerAddress: {
          address1: '$partnerSettings.companyInfo.officeAddress',
          city: '$partnerSettings.companyInfo.postalCity',
          postCode: '$partnerSettings.companyInfo.postalZipCode'
        }
      }
    },
    {
      $addFields: {
        users: {
          $first: '$users'
        },
        propertyLocation: {
          $concat: [
            { $ifNull: ['$property.location.name', ''] },
            {
              $cond: [
                { $ifNull: ['$property.location.postalCode', false] },
                { $concat: [', ', '$property.location.postalCode'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.city', false] },
                { $concat: [', ', '$property.location.city'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.country', false] },
                { $concat: [', ', '$property.location.country'] },
                ''
              ]
            }
          ]
        },
        invoiceMonth: {
          $let: {
            vars: {
              monthsInString: [
                '',
                'Jan',
                'Feb',
                'Mar',
                'Apr',
                'May',
                'Jun',
                'Jul',
                'Aug',
                'Sep',
                'Oct',
                'Nov',
                'Dec'
              ]
            },
            in: {
              $arrayElemAt: [
                '$$monthsInString',
                {
                  $toInt: {
                    $dateToString: {
                      date: { $first: '$invoiceMonths' },
                      format: '%m',
                      onNull: 0
                    }
                  }
                }
              ]
            }
          }
        },
        companyAddress: {
          $cond: {
            if: { $eq: ['$partners.accountType', 'broker'] },
            then: '$partnerAddress',
            else: '$accountAddress'
          }
        },
        invoiceYear: {
          $year: { $first: { $ifNull: ['$invoiceMonths', [new Date()]] } }
        }
      }
    },
    {
      $addFields: {
        orderDescription: {
          $concat: [
            'House rental',
            ' ',
            '$invoiceMonth',
            ' ',
            { $toString: '$invoiceYear' }
          ]
        },
        tenantInfo: { $first: '$tenants' },
        summary: {
          currencyId: 'NOK',
          taxAmount: '$totalTAX',
          taxBase: { $subtract: ['$invoiceTotal', '$totalTAX'] },
          total: '$invoiceTotal',
          totalExclTax: {
            $subtract: ['$invoiceTotal', '$totalTAX']
          },
          totalInclTax: '$invoiceTotal',
          totalTax: '$totalTAX'
        }
      }
    },
    {
      $project: {
        accountId: 1,
        agentId: 1,
        channel: 'EINVOICE',
        branchId: '$branchId',
        createdAt: 1,
        compelloStatus: 1,
        contractId: '$contractId',
        documentCode: '380',
        dueDate: '$dueDate',
        invoiceDate: '$createdAt',
        identifier: {
          $concat: ['$partnerId', '-', '$_id']
        },
        invoicee: {
          address1: '$tenantInfo.billingAddress',
          city: '$tenantInfo.city',
          countryCode: 'NO',
          customerNo: '$users._id',
          email: { $first: '$users.emails.address' },
          name: '$users.profile.name',
          nationalId: '$users.profile.norwegianNationalIdentification',
          mobile: '$users.profile.phoneNumber',
          postCode: '$tenantInfo.zipCode'
        },
        invoicer: {
          address1: '$companyAddress.address1',
          city: '$companyAddress.city',
          companyRegNo: '$sender.orgId',
          countryCode: 'NO',
          name: '$sender.companyName',
          postCode: '$companyAddress.postCode'
        },
        invoiceNo: { $toString: '$invoiceSerialId' },
        invoicePdfFileId: '$pdf.fileId',
        file: 1,
        invoiceType: 'invoice',
        languageID: 'NO',
        lineDetails: [
          {
            currencyId: 'NOK',
            supplierProductCode: '$propertyId',
            supplierProductText: '$propertyLocation'
          }
        ],
        messageType: 'B2C',
        orderDescription: 1,
        partnerId: '$partnerId',
        paymentInfo: {
          KID: '$kidNumber',
          paymentAccountInfo: {
            accountNumber: '$invoiceAccountNumber'
          },
          paymentAmount: {
            currencyId: 'NOK',
            amount: '$invoiceTotal'
          }
        },
        propertyId: '$propertyId',
        summary: 1
      }
    }
  ]
  return invoicePipelines
}

export const preparePipelineForB2BCompelloEInvoice = ({
  invoiceId,
  partnerId
}) => {
  const invoicePipelines = [
    {
      $match: {
        _id: invoiceId,
        partnerId
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        foreignField: 'partnerId',
        localField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $lookup: {
        from: 'partners',
        foreignField: '_id',
        localField: 'partnerId',
        as: 'partners'
      }
    },
    {
      $lookup: {
        from: 'contracts',
        foreignField: '_id',
        localField: 'contractId',
        as: 'contracts'
      }
    },
    { $unwind: '$contracts' },
    {
      $lookup: {
        from: 'tenants',
        foreignField: '_id',
        localField: 'tenantId',
        as: 'tenants'
      }
    },
    {
      $unwind: {
        path: '$pdf',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: {
        $or: [
          {
            'pdf.type': 'invoice_pdf'
          },
          {
            'pdf.type': 'credit_note_pdf'
          }
        ]
      }
    },
    {
      $lookup: {
        from: 'files',
        foreignField: '_id',
        localField: 'pdf.fileId',
        as: 'files'
      }
    },
    {
      $addFields: {
        partners: {
          $first: '$partners'
        },
        partnerSettings: {
          $first: '$partnerSettings'
        },
        files: {
          $first: '$files'
        }
      }
    },
    {
      $lookup: {
        from: 'users',
        foreignField: '_id',
        localField: 'tenants.userId',
        as: 'users'
      }
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'property.ownerId',
        foreignField: '_id',
        as: 'managerInfo'
      }
    },
    {
      $addFields: {
        managerInfo: { $first: '$managerInfo' }
      }
    },
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        pipeline: [
          {
            $lookup: {
              from: 'users',
              localField: 'personId',
              foreignField: '_id',
              as: 'person'
            }
          },
          {
            $unwind: {
              path: '$person',
              preserveNullAndEmptyArrays: true
            }
          }
        ],
        as: 'account'
      }
    },
    {
      $unwind: {
        path: '$account',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        accountAddress: {
          $cond: {
            if: {
              $and: [
                { $ifNull: ['$account.person', false] },
                { $ifNull: ['$account.person._id', false] },
                { $eq: ['$account.person._id', '$account.personId'] }
              ]
            },
            then: {
              address1: '$account.person.profile.hometown',
              city: '$account.person.profile.city',
              postCode: '$account.person.profile.zipCode'
            },
            else: {
              address1: '$account.address',
              city: '$account.city',
              postCode: '$account.zipCode'
            }
          }
        },
        partnerAddress: {
          address1: '$partnerSettings.companyInfo.officeAddress',
          city: '$partnerSettings.companyInfo.postalCity',
          postCode: '$partnerSettings.companyInfo.postalZipCode'
        }
      }
    },
    {
      $addFields: {
        users: {
          $first: '$users'
        },
        propertyLocation: {
          $concat: [
            { $ifNull: ['$property.location.name', ''] },
            {
              $cond: [
                { $ifNull: ['$property.location.postalCode', false] },
                { $concat: [', ', '$property.location.postalCode'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.city', false] },
                { $concat: [', ', '$property.location.city'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.country', false] },
                { $concat: [', ', '$property.location.country'] },
                ''
              ]
            }
          ]
        },
        invoiceMonth: {
          $let: {
            vars: {
              monthsInString: [
                '',
                'Jan',
                'Feb',
                'Mar',
                'Apr',
                'May',
                'Jun',
                'Jul',
                'Aug',
                'Sep',
                'Oct',
                'Nov',
                'Dec'
              ]
            },
            in: {
              $arrayElemAt: [
                '$$monthsInString',
                {
                  $toInt: {
                    $dateToString: {
                      date: { $first: '$invoiceMonths' },
                      format: '%m',
                      onNull: 0
                    }
                  }
                }
              ]
            }
          }
        },
        companyAddress: {
          $cond: {
            if: { $eq: ['$partners.accountType', 'broker'] },
            then: '$partnerAddress',
            else: '$accountAddress'
          }
        },
        invoiceYear: {
          $year: { $first: { $ifNull: ['$invoiceMonths', [new Date()]] } }
        }
      }
    },
    {
      $addFields: {
        userEmail: {
          $first: '$users.emails.address'
        },
        tenantInfo: { $first: '$tenants' }
      }
    },
    {
      $addFields: {
        orderDescription: {
          $concat: [
            'House rental',
            ' ',
            '$invoiceMonth',
            ' ',
            { $toString: '$invoiceYear' }
          ]
        },
        taxTotal: {
          taxAmount: '$totalTAX',
          taxSubTotal: [
            {
              taxableAmount: '$totalTAX',
              taxAmount: '$totalTAX',
              taxCategory: {
                ID: '1',
                percent: 0
              }
            }
          ]
        },
        buyerReference: '$tenantId',
        documentCurrencyCode: 'NOK',
        accountingSupplierParty: {
          party: {
            endpointID: '$sender.orgId',
            partyName: {
              name: '$sender.companyName'
            },
            postalAddress: {
              streetName: '$companyAddress.address1',
              cityName: '$companyAddress.city',
              postalZone: '$companyAddress.postCode',
              country: {
                identificationCode: 'NO'
              }
            },
            partyTaxScheme: [
              {
                companyID: '$sender.orgId',
                taxScheme: {
                  ID: 'VAT'
                }
              }
            ],
            partyLegalEntity: {
              registrationName: '$sender.companyName',
              companyID: '$sender.orgId'
            },
            contact: {
              name: '$managerInfo.profile.name',
              telephone: '$managerInfo.profile.phoneNumber',
              electronicMail: { $first: '$managerInfo.emails.address' }
            }
          }
        },
        accountingCustomerParty: {
          party: {
            endpointID: '$users.profile.organizationNumber',
            partyName: {
              Name: '$users.profile.name'
            },
            postalAddress: {
              streetName: '$tenantInfo.billingAddress',
              cityName: '$tenantInfo.city',
              postalZone: '$tenantInfo.zipCode',
              country: {
                identificationCode: 'NO'
              }
            },
            partyTaxScheme: [
              {
                companyID: '$users.profile.organizationNumber',
                taxScheme: {
                  ID: 'VAT'
                }
              }
            ],
            partyLegalEntity: {
              registrationName: '$users.profile.name',
              companyID: '$users.profile.organizationNumber'
            },
            contact: {
              name: '$users.profile.name',
              telephone: '$users.profile.phoneNumber',
              electronicMail: '$userEmail'
            }
          }
        },
        legalMonetaryTotal: {
          allowanceTotalAmount: { $ifNull: ['$invoiceTotal', 0] },
          chargeTotalAmount: { $ifNull: ['$invoiceTotal', 0] },
          prepaidAmount: { $ifNull: ['$totalPaid', 0] },
          payableRoundingAmount: { $ifNull: ['$roundedAmount', 0] },
          payableAmount: { $ifNull: ['$payoutableAmount', 0] },
          taxExclusiveAmount: {
            $subtract: ['$invoiceTotal', '$totalTAX']
          },
          taxInclusiveAmount: { $ifNull: ['$invoiceTotal', 0] }
        },
        paymentMeans: [
          {
            paymentMeansCode: '31',
            paymentID: '$kidNumber',
            payeeFinancialAccount: {
              id: '$invoiceAccountNumber'
            }
          }
        ]
      }
    },
    {
      $addFields: {
        invoiceLines: [
          {
            ID: '$_id',
            quantity: 1,
            invoicePeriod: {
              startDate: '$invoiceStartOn',
              endDate: '$invoiceEndOn'
            },
            price: {
              priceAmount: '$invoiceTotal',
              baseQuantity: 1
            },
            orderLineReference: {
              lineID: '1'
            },
            item: {
              name: '$orderDescription',
              classifiedTaxCategory: {
                ID: '1',
                percent: 0,
                taxScheme: {
                  ID: '1'
                }
              }
            }
          }
        ],
        identifier: {
          $concat: ['$partnerId', '-', '$_id']
        },
        id: { $toString: '$invoiceSerialId' },
        note: '$creditReason',
        creditNoteLines: [
          {
            ID: '$_id',
            accountingCost: 'AccountingCost',
            invoicePeriod: {
              endDate: '$invoiceEndOn',
              startDate: '$invoiceStartOn'
            },
            item: {
              classifiedTaxCategory: {
                ID: '1',
                percent: 0,
                taxScheme: {
                  ID: '1'
                }
              },
              name: '$orderDescription'
            },
            note: '$creditReason',
            orderLineReference: {
              lineID: '1'
            },
            price: {
              baseQuantity: 1,
              priceAmount: '$invoiceTotal'
            },
            quantity: 1,
            quantityUnitCode: 'EA'
          }
        ]
      }
    },
    {
      $project: {
        accountId: 1,
        accountingCustomerParty: 1,
        accountingSupplierParty: 1,
        agentId: 1,
        branchId: 1,
        buyerReference: 1,
        creditNoteLines: 1,
        compelloStatus: 1,
        contractId: 1,
        createdAt: 1,
        documentCurrencyCode: 1,
        dueDate: 1,
        files: 1,
        fileName: '$files.name',
        id: 1,
        identifier: 1,
        invoiceDate: '$createdAt',
        invoiceLines: 1,
        invoiceType: 1,
        invoicePdfFileId: '$pdf.fileId',
        issueDate: '$createdAt',
        languageID: 'NO',
        legalMonetaryTotal: 1,
        note: 1,
        orderReference: { ID: '$orderDescription' },
        partnerId: 1,
        paymentMeans: 1,
        propertyId: 1,
        taxTotal: 1
      }
    }
  ]

  return invoicePipelines
}
