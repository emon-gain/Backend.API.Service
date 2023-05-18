import {
  assign,
  clone,
  cloneDeep,
  compact,
  extend,
  find,
  flattenDepth,
  includes,
  indexOf,
  map,
  omit,
  pick,
  size,
  sortBy,
  uniq
} from 'lodash'

import { CustomError } from '../common'
import {
  CommissionCollection,
  InvoiceCollection,
  PayoutCollection
} from '../models'
import {
  appHelper,
  appQueueHelper,
  commissionHelper,
  contractHelper,
  correctionHelper,
  counterHelper,
  fileHelper,
  invoiceHelper,
  invoiceSummaryHelper,
  logHelper,
  partnerHelper,
  partnerSettingHelper,
  payoutHelper,
  transactionHelper,
  userHelper,
  vippsHelper
} from '../helpers'
import {
  appInvoiceService,
  appQueueService,
  commissionService,
  contractService,
  correctionService,
  counterService,
  finalSettlementService,
  invoiceSummaryService,
  logService,
  partnerSettingService,
  paymentService,
  payoutService,
  transactionService
} from '../services'

/* Common Invoice Service Starts
 * */
export const updateInvoice = async (query, data, session) => {
  if (!size(data)) throw new CustomError(404, 'No data found for update')

  const updatedInvoice = await InvoiceCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })

  return updatedInvoice
}

export const updateInvoiceWithPipeline = async (query, pipeline, session) => {
  if (!size(pipeline)) throw new CustomError(404, 'No data found for update')

  const updatedInvoice = await InvoiceCollection.findOneAndUpdate(
    query,
    pipeline,
    {
      runValidators: true,
      new: true,
      session
    }
  )

  return updatedInvoice
}

export const updateInvoices = async (query, data, session) => {
  await InvoiceCollection.updateMany(query, data, {
    runValidators: true,
    new: true,
    session
  })
}

export const createAnInvoice = async (data, session) => {
  if (!size(data)) throw new CustomError(404, 'No data found to create invoice')
  const [invoice] = await InvoiceCollection.create([data], {
    session
  })
  if (!size(invoice)) throw new CustomError(404, 'Unable to create invoice')
  return invoice
}

export const addInvoiceEndDateInContract = async (invoiceData, session) => {
  if (invoiceData && invoiceData.contractId && invoiceData.invoiceEndOn) {
    console.log('======= Updated contract', invoiceData)
    const updatedContract = await contractService.updateContract(
      {
        _id: invoiceData.contractId
      },
      {
        $set: {
          'rentalMeta.invoicedAsOn': invoiceData.invoiceEndOn
        }
      },
      session
    )
    if (!updatedContract)
      throw new CustomError(404, 'Unable to update contract invoicedAsOn')
  }
}

export const setInvoiceIdInCorrection = async (invoiceData, session) => {
  let result = true
  const isNotLandLord = invoiceHelper.isNotLandlord(invoiceData)
  if (
    size(invoiceData) &&
    isNotLandLord &&
    size(invoiceData.correctionsIds) &&
    invoiceData._id
  ) {
    const correctionArraySize = size(invoiceData.correctionsIds)
    const query = { _id: { $in: invoiceData.correctionsIds } }
    const data = {
      $set: {
        invoiceId: invoiceData._id
      }
    }
    const response = await correctionService.updateCorrections(
      query,
      data,
      session
    )
    if (response.nModified !== correctionArraySize) {
      result = false
    }
  }
  if (!result) {
    throw new CustomError(
      500,
      `Could not update correction with invoiceId: ${invoiceData._id}`
    )
  }
}

export const createInvoiceLog = async (invoice, session) => {
  const options = {
    action: 'added_new_invoice',
    context: 'invoice'
  }
  const invoiceLogData = logHelper.prepareInvoiceLogData(invoice, options)
  if (invoiceLogData) {
    await logService.createLog(invoiceLogData, session)
  } else {
    throw new CustomError(
      500,
      `Could not prepare log data for invoiceId: ${invoice._id}`
    )
  }
}

const errorsTextKeys = [
  'invoice_kid_number',
  'invoice_account_number',
  'invoice_due_date',
  'invoice_has_not_amount',
  'invoice_issuer_name',
  'invoice_pdf_not_found',
  'invoice_data_not_found',
  'issuer_org_id_not_found',
  'msisdn_and_nin_not_found',
  'recipient_token_not_found',
  'invalid_recipient',
  'invalid_tenant'
]

export const createInvoiceLogForVipps = async (invoice, action, session) => {
  const options = {
    action,
    context: 'invoice'
  }
  if (includes(errorsTextKeys, invoice?.errorTextKey)) {
    options.errorTextKey = invoice.errorTextKey
  } else options.errorText = invoice.errorTextKey

  const invoiceLogData = logHelper.prepareInvoiceLogData(invoice, options)
  if (invoiceLogData) return await logService.createLog(invoiceLogData, session)
  else {
    throw new CustomError(
      500,
      `Could not prepare log data for invoiceId: ${invoice._id}`
    )
  }
}

export const createInvoiceDelayDateLog = async (data, session) => {
  const { invoice, previous, createdBy } = data
  const options = {
    action: 'updated_due_delay',
    context: 'invoice',
    previousDoc: previous
  }
  const logData = logHelper.prepareInvoiceDelayDateLogData(invoice, options)
  if (logData) {
    if (createdBy) logData.createdBy = createdBy
    await logService.createLog(logData, session)
  } else {
    throw new CustomError(
      500,
      `Could not prepare log data for invoice delay date, invoiceId: ${invoice._id}`
    )
  }
}

export const createInvoiceLostLog = async (invoice = {}, session, userId) => {
  const logData = logHelper.prepareInvoiceLostLogData(invoice, userId)
  if (logData) {
    await logService.createLog(logData, session)
  } else {
    throw new CustomError(
      500,
      `Could not prepare log data for lost invoice, invoiceId: ${invoice._id}`
    )
  }
}

export const createRemovedInvoiceLostLog = async (data, session) => {
  const options = {
    action: 'removed_lost_invoice',
    context: 'invoice'
  }
  const logData = logHelper.prepareRemovedInvoiceLostLogData(data, options)
  if (logData) {
    await logService.createLog(logData, session)
  } else {
    throw new CustomError(
      500,
      `Could not prepare log data for removed lost invoice, invoiceId: ${data.invoice._id}`
    )
  }
}

export const updateInvoiceFeesMeta = async (invoiceData, session) => {
  const appQueuesDataForTransaction = []
  const promiseArr = []
  if (invoiceData && invoiceData.partnerId && invoiceData._id) {
    const invoiceFeesMeta = invoiceData.feesMeta ? invoiceData.feesMeta : []
    const processedInvoice = {}
    for (const feesMetaInfo of invoiceFeesMeta) {
      if (
        feesMetaInfo &&
        feesMetaInfo.invoiceId &&
        (feesMetaInfo.type === 'unpaid_reminder' ||
          feesMetaInfo.type === 'unpaid_collection_notice' ||
          feesMetaInfo.type === 'unpaid_eviction_notice' ||
          feesMetaInfo.type === 'unpaid_administration_eviction_notice')
      ) {
        if (processedInvoice[feesMetaInfo.invoiceId]) {
          continue
        } else {
          processedInvoice[feesMetaInfo.invoiceId] = true
        }
        const query = {
          _id: feesMetaInfo.invoiceId,
          partnerId: invoiceData.partnerId
        }
        const invoiceInfo = await invoiceHelper.getInvoice(query, session)
        // When collection notice or reminder fee move to other invoice then change invoice total, total tax, feesMeta
        // Add reminder or collection notice fee move to info.
        const feesMetaData = await invoiceHelper.getFeesMetaData(
          invoiceInfo,
          invoiceData
        )
        // Updating invoice invoice total, total tax, and feesMetaS
        promiseArr.push(
          updateInvoice(
            {
              _id: feesMetaInfo.invoiceId,
              partnerId: invoiceData.partnerId
            },
            {
              $set: feesMetaData
            },
            session
          )
        )
        if (
          invoiceInfo.evictionDueReminderSent &&
          invoiceInfo.evictionDueReminderNoticeSentOn &&
          invoiceInfo.invoiceTotal !== feesMetaData.invoiceTotal
        ) {
          promiseArr.push(
            appQueueService.createAppQueueForProcessingEvictionCase(
              invoiceInfo,
              session
            )
          )
        }
        appQueuesDataForTransaction.push({
          action: 'add_invoice_move_to_fees_transaction',
          destination: 'accounting',
          event: 'add_new_transaction',
          params: {
            feesMeta: (feesMetaData.feesMeta || []).map((item) =>
              omit(item, ['invoiceId'])
            ),
            invoiceIds: [feesMetaInfo.invoiceId],
            partnerId: invoiceData.partnerId,
            transactionEvent: 'regular'
          },
          priority: 'regular'
        })
      }
    }
  }
  console.log(
    'Checking for app queues for transactions: ',
    appQueuesDataForTransaction
  )
  if (size(appQueuesDataForTransaction)) {
    await appQueueService.insertAppQueueItems(
      appQueuesDataForTransaction,
      session
    )
  }
  if (size(promiseArr)) await Promise.all(promiseArr)
}

export const addLandlordInvoicesForCommission = async (params, session) => {
  const {
    adjustmentNotNeeded,
    contract,
    dueDate,
    invoiceEndOn,
    invoiceId,
    invoiceStartOn,
    partnerId,
    partner,
    partnerSetting,
    userId
  } = params
  const today = await invoiceHelper.getInvoiceDate(new Date(), partnerSetting)
  const invoiceData = await invoiceHelper.getInvoiceDataForLandlordInvoice(
    contract
  )
  const invoiceParams = {
    adjustmentNotNeeded,
    contract,
    dueDate,
    enabledNotification: false,
    invoiceEndOn,
    invoiceId,
    invoiceStartOn,
    invoiceData,
    isDemo: false,
    partner,
    partnerId,
    partnerSetting,
    returnPreview: false,
    today,
    userId
  }
  return await createLandlordInvoiceForCommission(invoiceParams, session)
}

export const removeDefaultedTagFromInvoice = async (invoice = {}, session) => {
  const { _id, partnerId, propertyId, isDefaulted } = invoice
  if (isDefaulted) {
    const query = {
      _id,
      partnerId,
      propertyId
    }
    const data = { isDefaulted: false }
    const updatedInvoice = (await updateInvoice(query, data, session)) || {}
    await contractService.removeDefaultedTagFromContract(
      updatedInvoice,
      session
    )
    return updatedInvoice.isDefaulted
  }
}
/* Common Invoice Service Ends
 * */

/* Rent Invoice Starts
 * */
export const createRentInvoice = async (invoiceData, session) => {
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
  const [invoice] = await InvoiceCollection.create([invoiceData], { session })
  await addInvoiceEndDateInContract(invoice.toObject(), session) // For Rent Invoice and Credit Note Invoice
  await invoiceSummaryService.createInvoiceSummary(invoice.toObject(), session) // For Rent Invoice and Credit Note Invoice
  await updateInvoiceFeesMeta(invoice.toObject(), session) // For all type invoices
  await setInvoiceIdInCorrection(invoice.toObject(), session) // For Rent Invoice and Credit Note Invoice
  await createInvoiceLog(invoice.toObject(), session) // For all types of Invoice
  await paymentService.adjustBetweenPaymentsAndInvoices(
    { ...invoice.toObject(), processType: 'matchPaymentsWithInvoices' },
    session
  ) // Payment Service
  // Check if the transaction is enabled for invoice partner
  if (await partnerHelper.isTransactionEnabledOfAPartner(invoice.partnerId)) {
    await addInvoiceTransactions(invoice.toObject(), 'regular', session) // For all type of invoices
  }
  // Only for rent type Invoice
  if (invoiceHelper.isAddEstimatedPayout(invoice.toObject())) {
    await addEstimatedPayoutInContract(invoice.toObject(), session)
  }
  if (invoiceHelper.isAddCommissionChange(invoice.toObject())) {
    // TODO: Init add invoice commission and add landlord process by app-queue
  }
  // If (invoiceHelper.isUpdateCommissionChangeHistory(invoice.toObject())) {
  //     // TODO: Init update commission history process in contract by app-queue
  //     // Should run only for first invoice which is created manually
  // }
  return invoice
}

export const createMonthlyRentInvoice = async (monthlyInvoiceData, session) => {
  const data = await invoiceHelper.getMonthlyDataOfRentInvoice(
    monthlyInvoiceData
  )
  // Checking and creating non-rent correction invoices here.
  await createNonRentInvoices(monthlyInvoiceData, session)
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(data)
  const invoice = await createRentInvoice(invoiceData, session)
  return invoice
}

export const updateContractRentAmountFromFutureRentAmount = async (
  contractUpdatedData,
  session
) => {
  const { _id, partnerId, updateData, resetData } = contractUpdatedData
  const query = { _id, partnerId }
  const data = { $set: updateData, $unset: resetData }
  const contract = await contractService.updateContract(query, data, session)
  return contract
}

export const updateContractRentFromFutureRent = async (
  contractRentUpdateData,
  session
) => {
  const { invoiceBasicData, contract, partnerSetting } = contractRentUpdateData
  const isInRange = await invoiceHelper.compareNextCPIDateWithInvoiceDueDate(
    invoiceBasicData,
    contract
  )
  if (isInRange) {
    // Update Contract Collection
    const todayDate = contract.rentalMeta.nextCpiDate
    const contractUpdatedData =
      await invoiceHelper.getDataForUpdatingContractRentAmount(
        contract,
        partnerSetting,
        todayDate
      )
    if (size(contractUpdatedData)) {
      await updateContractRentAmountFromFutureRentAmount(
        contractUpdatedData,
        session
      )
    }
  }
}

export const generateRentInvoice = async (data, session) => {
  let { contract } = data
  const { partnerSetting, invoiceBasicData, returnPreview } = data
  if (!returnPreview) {
    if (invoiceBasicData && contract) {
      const contractRentUpdateData = {
        invoiceBasicData,
        contract,
        partnerSetting
      }
      await updateContractRentFromFutureRent(contractRentUpdateData, session)
    }
    contract = await contractHelper.getContractById(contract._id, session) // Reassigning contract variable
  }
  data.contract = contract
  const dataOfAMonth = await invoiceHelper.getRentInvoiceData(data, session)
  if (!dataOfAMonth) {
    return false
  }
  const invoice = await createMonthlyRentInvoice(dataOfAMonth, session)
  return invoice
}

export const createFirstRentInvoices = async (firstInvoicesData, session) => {
  const { initials, enabledNotification, userId } = firstInvoicesData
  let resultData = []
  // Create first invoices month wise
  const { contract, partnerSetting, actualDate, invoiceBasicData } = initials
  const firstInvoiceCreationDate =
    await invoiceHelper.getFirstInvoiceCreationDate(
      contract.rentalMeta.firstInvoiceDueDate,
      partnerSetting.invoiceDueDays,
      partnerSetting
    )
  let isFirstInvoice = true
  // We'll only create invoice if today is the firstInvoiceCreationDate or past
  if (actualDate >= firstInvoiceCreationDate) {
    // We we create the fist invoice
    // We also have to check if there is anything to create for past months
    // We have to check the invoiceStartFrom month
    // Create invoices for the invoiceStartFrom month to this month
    const creatableInvoicesData = []
    let contractUpdateData = {}

    // Adding first month end date when today or actual date is older than invoice start from
    const firstMonthEndDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        contract.rentalMeta.invoiceStartFrom
      )
    )
      .startOf('month')
      .add((contract.rentalMeta.invoiceFrequency || 1) - 1, 'months')
      .endOf('month')
      .toDate()
    const monthListParams = {
      endMonthDate:
        actualDate > contract.rentalMeta.invoiceStartFrom
          ? actualDate
          : firstMonthEndDate,
      partnerSetting,
      startMonthDate: contract.rentalMeta.invoiceStartFrom
    }
    const monthsList = await invoiceHelper.getListOfMonths(monthListParams)
    let invoiceCountFromBeginning = 0
    for (const month of monthsList) {
      if (invoiceCountFromBeginning) {
        invoiceBasicData.ignoreCorrections = true
      }
      // All past invoices due date will be same as first invoice due date
      invoiceBasicData.dueDate = contract.rentalMeta.firstInvoiceDueDate
      invoiceBasicData.invoiceMonth = month
      const previewParams = {
        contract,
        enabledNotification,
        ignoreExistingInvoiceChecking: false,
        ignoreRecurringDueDate: true,
        invoiceCountFromBeginning,
        invoiceData: invoiceBasicData,
        isFirstInvoice,
        partnerSettings: partnerSetting,
        returnPreview: false,
        today: actualDate
      }
      const { previewData, updateContractData } =
        await invoiceHelper.getPreviewData(previewParams, session)
      if (size(previewData) && previewData.invoiceTotal >= 0) {
        creatableInvoicesData.push(JSON.parse(JSON.stringify(previewData)))
        if (size(updateContractData)) {
          contractUpdateData = JSON.parse(JSON.stringify(updateContractData))
        }
        invoiceCountFromBeginning++
      }
      isFirstInvoice = false
    }
    if (size(creatableInvoicesData)) {
      resultData = await addRentInvoices(
        {
          contract,
          contractUpdateData,
          enabledNotification,
          invoicesData: creatableInvoicesData,
          partnerSetting,
          userId
        },
        session
      )
    }
  }
  return resultData
}

/*
 *  We have to create invoice before the x days of due date
 *  for first / past invoice we will see the firstInvoiceDueDate
 *  Otherwise we will see the contract.dueDate
 */
export const createNextMonthRentInvoice = async (data, session) => {
  const { ignoreCorrections, initials, enabledNotification, userId } = data
  // Create Next Month invoices Here
  const isNextMonthInvoice = true
  const nextMonthsInvoiceData =
    await invoiceHelper.prepareDataForCurrentOrNextMonthInvoice(
      initials,
      isNextMonthInvoice
    )
  if (!size(nextMonthsInvoiceData)) {
    return false
  }
  const { contract, partnerSetting, invoiceBasicData, isFirstInvoice } =
    nextMonthsInvoiceData
  if (ignoreCorrections) {
    invoiceBasicData.ignoreCorrections = ignoreCorrections
  }
  const countInvoice = await invoiceHelper.invoiceCountOfAContract(
    contract._id,
    session
  )
  const previewParams = {
    contract,
    enabledNotification,
    ignoreExistingInvoiceChecking: false,
    invoiceCountFromBeginning: countInvoice,
    invoiceData: invoiceBasicData,
    isFirstInvoice,
    partnerSettings: partnerSetting,
    returnPreview: false
  }
  const { previewData, updateContractData } =
    await invoiceHelper.getPreviewData(previewParams, session)
  const creatableInvoicesData = []
  let contractUpdateData = {}
  let nextMonthsInvoice = []
  if (size(previewData) && previewData.invoiceTotal >= 0) {
    creatableInvoicesData.push(JSON.parse(JSON.stringify(previewData)))
    if (size(updateContractData)) {
      contractUpdateData = JSON.parse(JSON.stringify(updateContractData))
    }
    nextMonthsInvoice = await addRentInvoices(
      {
        contract,
        contractUpdateData,
        enabledNotification,
        invoicesData: creatableInvoicesData,
        partnerSetting,
        userId
      },
      session
    )
  }
  return nextMonthsInvoice
}

export const createCurrentMonthRentInvoice = async (data, session) => {
  const { initials, enabledNotification, userId } = data
  const isNextMonthInvoice = false
  const currentMonthsInvoiceData =
    await invoiceHelper.prepareDataForCurrentOrNextMonthInvoice(
      initials,
      isNextMonthInvoice
    )
  if (currentMonthsInvoiceData) {
    const { contract, partnerSetting, isFirstInvoice, invoiceBasicData } =
      currentMonthsInvoiceData
    const countInvoice = await invoiceHelper.invoiceCountOfAContract(
      contract._id,
      session
    )
    const previewParams = {
      contract,
      enabledNotification,
      invoiceData: invoiceBasicData,
      isFirstInvoice,
      invoiceCountFromBeginning: countInvoice,
      partnerSettings: partnerSetting,
      returnPreview: false,
      today: initials.actualDate
    }
    const { previewData, updateContractData } =
      await invoiceHelper.getPreviewData(previewParams, session)
    const creatableInvoicesData = []
    let contractUpdateData = {}
    let thisMonthInvoice = []
    if (size(previewData) && previewData.invoiceTotal >= 0) {
      creatableInvoicesData.push(JSON.parse(JSON.stringify(previewData)))
      if (size(updateContractData)) {
        contractUpdateData = JSON.parse(JSON.stringify(updateContractData))
      }
      thisMonthInvoice = await addRentInvoices(
        {
          contract,
          contractUpdateData,
          enabledNotification,
          invoicesData: creatableInvoicesData,
          partnerSetting,
          userId
        },
        session
      )
    }
    return thisMonthInvoice
  }
}

export const startFirstRentInvoicesCreationProcess = async (
  initialData,
  session
) => {
  const { initials, enabledNotification, userId } = initialData
  const result = []
  const firstInvoicesData = {
    initials,
    enabledNotification,
    userId
  } // Create first invoices
  const firstInvoices = await createFirstRentInvoices(
    firstInvoicesData,
    session
  )
  if (size(firstInvoices)) {
    result.push(...firstInvoices)
  }
  // 1st Invoice already created?. Lets create next months invoice
  if (size(result)) {
    const nextMonthInvoiceData = {
      ignoreCorrections: true,
      initials,
      enabledNotification,
      userId
    } // Create Next Months Invoices
    const nextMonthInvoice = await createNextMonthRentInvoice(
      nextMonthInvoiceData,
      session
    )
    if (size(nextMonthInvoice)) {
      result.push(...nextMonthInvoice)
    }
  }
  return result
}

export const startCurrentOrNextMonthRentInvoiceCreationProcess = async (
  initialData,
  session
) => {
  const { initials, enabledNotification, userId } = initialData
  const result = []

  // Let's create regular invoice of the contract for current month.Create Current Months Invoice
  const data = {
    initials,
    enabledNotification,
    userId
  }
  const currentMonthInvoice = await createCurrentMonthRentInvoice(data, session)
  let ignoreCorrections = false
  if (size(currentMonthInvoice)) {
    result.push(...currentMonthInvoice)
    ignoreCorrections = true
  } //  Let's create regular invoice of the contract for next month
  const nextMonthInvoice = await createNextMonthRentInvoice(
    {
      ...data,
      ignoreCorrections
    },
    session
  )
  if (size(nextMonthInvoice)) {
    result.push(...nextMonthInvoice)
  }
  return result
}

export const createRentInvoices = async (data, session) => {
  let result // Process request body
  const today = data.today ? new Date(data.today) : new Date()
  const { contractId, isDemo = false, enabledNotification, userId } = data
  if (!contractId) {
    throw new CustomError(400, 'Contract id is missing')
  } // Prepare necessary data for rent invoice
  const initials = await invoiceHelper.getInitialDataForRentInvoice(
    contractId,
    today,
    isDemo
  )
  if (!initials) {
    throw new CustomError(
      500,
      'Initial Data processing failed for rent invoice'
    )
  }
  const initialData = {
    initials,
    contractId,
    isDemo,
    enabledNotification,
    userId
  }
  if (await invoiceHelper.isFirstInvoiceOfAContract(contractId, session)) {
    result = await startFirstRentInvoicesCreationProcess(initialData, session)
  } else {
    result = await startCurrentOrNextMonthRentInvoiceCreationProcess(
      initialData,
      session
    )
  }
  return result
}

export const addEstimatedPayoutInContract = async (invoiceData, session) => {
  const query = invoiceHelper.prepareQueryForAddEstimatedPayout(invoiceData)
  const contractInfo = await contractHelper.getAContract(query, session)
  if (!size(contractInfo)) {
    return false
  }
  const previewParams = {
    contract: contractInfo,
    today: new Date(),
    returnEstimatedPayoutPreview: true
  }
  const invoices = await invoiceHelper.getInvoicesPreview(
    previewParams,
    session
  )
  if (!size(invoices)) {
    return false
  }
  const updateData =
    await invoiceHelper.prepareContractUpdateDataForEstimatedPayout(invoices)
  const updateQuery = {
    _id: contractInfo._id,
    partnerId: contractInfo.partnerId
  }
  const updatedContract = await contractService.updateContract(
    updateQuery,
    updateData,
    session
  )
  if (!updatedContract) {
    return false
  }

  return true
}

export const updateCurrentInvoicesEvictionDueReminder = async (
  invoice = {},
  session
) => {
  const {
    _id,
    contractId,
    partnerId,
    propertyId,
    evictionDueReminderSent,
    evictionDueReminderNoticeSentOn
  } = invoice
  if (evictionDueReminderSent && evictionDueReminderNoticeSentOn) {
    const query = {
      _id: { $ne: _id },
      contractId,
      partnerId,
      propertyId,
      invoiceType: 'invoice',
      status: { $nin: ['paid', 'credited', 'lost'] },
      evictionNoticeSentOn: { $exists: true },
      evictionNoticeSent: true,
      evictionDueReminderNoticeSentOn: { $exists: false }
    }
    const updateData = {
      $set: {
        evictionDueReminderSent: true,
        evictionDueReminderNoticeSentOn
      },
      $unset: { evictionNoticeSent: 1, evictionNoticeSentOn: 1 }
    }
    await updateInvoices(query, updateData, session)
  }
}

export const resetInvoiceEvictionTag = async (invoice = {}, session) => {
  const { _id: invoiceId, contractId, partnerId, propertyId } = invoice
  const evictionDueDate = await invoiceHelper.getEvictionNoticeDueDate(
    partnerId,
    session
  )
  const query = {
    _id: { $ne: invoiceId },
    partnerId,
    contractId,
    propertyId,
    invoiceType: 'invoice',
    status: { $nin: ['paid', 'credited', 'lost'] },
    evictionNoticeSent: true,
    evictionNoticeSentOn: { $exists: true }
  }
  const dueInvoiceQuery = assign(clone(query), {
    dueDate: { $exists: true, $lte: evictionDueDate }
  })
  const hasDueInvoice = await invoiceHelper.getInvoice(dueInvoiceQuery, session)
  if (!hasDueInvoice) {
    const updateData = {
      $unset: {
        evictionNoticeSent: 1,
        evictionNoticeSentOn: 1
      }
    }
    await updateInvoices(query, updateData, session)
  }
}

/* Rent Invoice Ends
 * */

/* Rent Credit Note Starts
 * */
export const updateCreditedInvoice = async (params, session) => {
  const {
    creditNote,
    notUpdateDefaultedContract,
    partnerSetting,
    voidPayment
  } = params
  const creditedInvoice = await invoiceHelper.getInvoiceById(
    creditNote.invoiceId
  )
  let updatedCreditedInvoice = JSON.parse(JSON.stringify(creditedInvoice))
  if (
    updatedCreditedInvoice.invoiceType === 'landlord_invoice' &&
    updatedCreditedInvoice.remainingBalance !== 0
  ) {
    const { landlordInvoice } =
      await payoutService.addLinkBetweenLandlordInvoiceAndPayouts(
        updatedCreditedInvoice,
        partnerSetting,
        session
      )
    updatedCreditedInvoice = JSON.parse(JSON.stringify(landlordInvoice))
  }

  const creditInvoiceUpdateData = {}
  const creditedInvoiceData = {}
  if (size(creditedInvoice.creditNoteIds)) {
    creditedInvoiceData.creditNoteIds = [
      ...creditedInvoice.creditNoteIds,
      creditNote._id
    ]
  } else {
    creditedInvoiceData.creditNoteIds = [creditNote._id]
  }
  if (voidPayment) {
    creditedInvoiceData.voidExistingPayment = true
  }
  creditedInvoiceData.creditedAmount = await invoiceHelper.getCreditedAmount(
    creditedInvoice,
    creditNote
  )
  if (creditNote.fullyCredited) {
    creditedInvoiceData.status = 'credited'
    creditedInvoiceData.isPartiallyCredited = false
    creditInvoiceUpdateData.$unset = { isDefaulted: 1 }
    delete updatedCreditedInvoice.isDefaulted
    // Change isDefaulted for both invoice and contract, if certain conditions are met.
    // remove defaulted tag from contract
    if (!notUpdateDefaultedContract) {
      const isDefaultedInvoiceExist = await invoiceHelper.getInvoice(
        {
          contractId: creditNote.contractId,
          isDefaulted: true
        },
        session
      )
      if (!size(isDefaultedInvoiceExist)) {
        await contractService.updateContract(
          {
            _id: creditNote.contractId,
            partnerId: creditNote.partnerId,
            propertyId: creditNote.propertyId
          },
          { $set: { isDefaulted: false } },
          session
        )
      }
    }
  } else {
    creditedInvoiceData.isPartiallyCredited = true
  }
  creditInvoiceUpdateData.$set = creditedInvoiceData
  updatedCreditedInvoice = {
    ...updatedCreditedInvoice,
    ...creditedInvoiceData
  }
  //remove or update eviction case when invoice due amount is less then 0
  if (
    creditedInvoice.evictionDueReminderSent &&
    creditedInvoice.evictionDueReminderNoticeSentOn
  ) {
    const previousDue = await invoiceHelper.getTotalDueAmountOfAnInvoice(
      creditedInvoice
    )
    const newDue = await invoiceHelper.getTotalDueAmountOfAnInvoice(
      updatedCreditedInvoice
    )
    if (previousDue !== newDue) {
      await appQueueService.createAppQueueForProcessingEvictionCase(
        updatedCreditedInvoice,
        session
      )
    }
  }
  // After update process starts
  let invoiceStatusUpdateData = {}
  let calculatedInvoiceStatus = {}
  const invoiceUpdateData = {
    setData: {}
  }
  if (
    (updatedCreditedInvoice.status === 'credited' &&
      updatedCreditedInvoice.status !== creditedInvoice.status) ||
    (updatedCreditedInvoice.isPartiallyCredited &&
      creditedInvoice.isPartiallyCredited !== true)
  ) {
    // updateInvoiceStatus
    invoiceStatusUpdateData =
      await invoiceHelper.prepareInvoiceStatusUpdatingData(
        updatedCreditedInvoice
      )
    if (
      updatedCreditedInvoice.isPartiallyCredited &&
      !creditedInvoice.isPartiallyCredited
    ) {
      calculatedInvoiceStatus =
        invoiceHelper.calculateInvoiceStatusBaseOnTotalPaid(
          updatedCreditedInvoice
        )
    }
    if (
      updatedCreditedInvoice.status !== 'paid' &&
      (invoiceStatusUpdateData.status === 'paid' ||
        calculatedInvoiceStatus.status === 'paid')
    ) {
      invoiceUpdateData.setData.status = 'paid'
      await startProcessForInvoiceAfterGettingPaid(
        updatedCreditedInvoice,
        invoiceUpdateData,
        session
      )
      if (
        updatedCreditedInvoice.isFinalSettlement ||
        updatedCreditedInvoice.isPayable
      ) {
        await paymentService.checkFinalSettlementProcessAndUpdateContractFinalSettlementStatus(
          updatedCreditedInvoice,
          session
        )
      }
    }
    if (
      invoiceHelper.isNotLandlord(updatedCreditedInvoice) &&
      updatedCreditedInvoice.status !== 'balanced' &&
      invoiceStatusUpdateData.status === 'balanced'
    ) {
      const updateData = await addMissingPayoutIdInLandlordInvoiceOrCreditNote(
        updatedCreditedInvoice,
        session,
        true
      )
      invoiceUpdateData.setData = {
        ...invoiceUpdateData.setData,
        ...updateData
      }
    }
    if (
      updatedCreditedInvoice.status !== 'overdue' &&
      invoiceStatusUpdateData.status === 'overdue'
    ) {
      await invoiceHelper.prepareEvictionInfoForInvoice(
        updatedCreditedInvoice,
        invoiceUpdateData
      )
    }
    if (
      invoiceHelper.isNotLandlord(updatedCreditedInvoice) &&
      !updatedCreditedInvoice.voidExistingPayment
    ) {
      await appQueueService.createAppQueueForMatchPayment(
        {
          action: 'updated_invoice_status_to_credited',
          contractId: updatedCreditedInvoice.contractId,
          partnerId: updatedCreditedInvoice.partnerId
        },
        session
      )
    }
    if (
      updatedCreditedInvoice.status === 'credited' &&
      creditedInvoice.status !== 'credited'
    ) {
      await payoutService.updatePayouts(
        { invoiceId: updatedCreditedInvoice._id },
        { $set: { invoiceCredited: true } },
        session
      )
      await payoutService.setInvoicePaidInFinalSettlementPayout(
        updatedCreditedInvoice,
        session
      )
    }
    if (
      (updatedCreditedInvoice.status === 'credited' ||
        invoiceStatusUpdateData.status !== 'paid') &&
      creditedInvoice.status === 'paid' &&
      invoiceHelper.isNotLandlord(updatedCreditedInvoice)
    ) {
      await payoutService.removeInvoicePaidInfoFromPayout(
        updatedCreditedInvoice,
        session
      )
    }
  }
  // To update creditedInvoice
  updatedCreditedInvoice = await updateInvoice(
    {
      _id: creditNote.invoiceId
    },
    {
      $set: {
        ...creditInvoiceUpdateData.$set,
        ...invoiceStatusUpdateData,
        ...invoiceUpdateData.setData,
        ...calculatedInvoiceStatus
      },
      $unset: {
        ...creditInvoiceUpdateData.$unset,
        ...invoiceUpdateData.unsetData
      }
    },
    session
  )
  return updatedCreditedInvoice
}

export const updateInvoiceSummary = async (data, session) => {
  const { creditNote, feeTotal } = data
  const { invoiceId, partnerId } = creditNote
  const invoiceSummary = await invoiceSummaryHelper.getInvoiceSummary(
    { invoiceId, partnerId },
    session
  )
  console.log(
    'Invoice summary and invoice credit note data',
    invoiceId,
    size(data?.creditNote),
    size(invoiceSummary)
  )
  if (invoiceSummary && creditNote) {
    const updateData = {}
    updateData.feesAmount = await appHelper.convertTo2Decimal(
      invoiceSummary.feesAmount + feeTotal
    )
    updateData.invoiceAmount = await appHelper.convertTo2Decimal(
      invoiceSummary.invoiceAmount + creditNote.invoiceTotal
    )
    console.log(
      'Invoice summary and invoice total',
      creditNote?.invoiceTotal,
      invoiceSummary?.invoiceAmount
    )
    console.log('Invoice summary updatable data', updateData)
    await invoiceSummaryService.updateInvoiceSummary(
      { _id: invoiceSummary._id },
      updateData,
      session
    )
  }
}

export const updatePayoutForCreditedInvoice = async (creditNote, session) => {
  const { fullyCredited, invoiceId } = creditNote
  if (fullyCredited) {
    const data = { invoiceCredited: true }
    await payoutService.updateAPayout({ invoiceId }, data, session)
  }
}

export const updateCreditedCommission = async (commission, session) => {
  const { commissionId, _id, amount } = commission
  const query = { _id: commissionId }
  const data = {
    refundCommissionId: _id,
    refundCommissionAmount: amount
  }
  await commissionService.updateCommission(query, data, session)
}

export const creditBrokeringCommission = async (data, session) => {
  const { creditNote, invoice, invoiceCommissions = [], userId } = data
  const isCreditFull = creditNote.fullyCredited
  if (isCreditFull) {
    const brokeringCommission = invoiceCommissions.find(
      (item) => item.type === 'brokering_contract'
    )
    if (brokeringCommission) {
      const creditBrokeringCommission =
        invoiceHelper.getIdsAndType(brokeringCommission)
      creditBrokeringCommission.amount = brokeringCommission.amount * -1
      creditBrokeringCommission.invoiceId = creditNote._id
      creditBrokeringCommission.commissionId = brokeringCommission._id
      creditBrokeringCommission.createdBy = userId
      creditBrokeringCommission.serialId =
        await counterService.incrementCounter(
          'commission-' + invoice.partnerId,
          session
        )
      const commission = await commissionService.addCommissionToCollection(
        creditBrokeringCommission,
        session
      )
      return commission
    }
  }
}

export const creditAddonCommission = async (data, session) => {
  const {
    creditableDays,
    creditNote,
    invoice,
    invoiceCommissions = [],
    invoiceTotalDays,
    isCreditFull,
    userId
  } = data
  const commissions = invoiceCommissions.filter(
    (item) => item.type === 'addon_commission'
  )
  const commissionsData = []
  for (const addonCommission of commissions) {
    if (addonCommission) {
      let commissionAmount = 0
      if (isCreditFull) {
        commissionAmount = addonCommission.amount || 0
      } else if (
        !isCreditFull &&
        invoiceTotalDays &&
        creditableDays &&
        addonCommission.amount
      ) {
        commissionAmount =
          (addonCommission.amount / invoiceTotalDays) * creditableDays
      }
      if (commissionAmount) {
        const creditableCommission =
          invoiceHelper.getIdsAndType(addonCommission)
        creditableCommission.amount = commissionAmount * -1
        creditableCommission.commissionId = addonCommission._id
        creditableCommission.invoiceId = creditNote._id
        creditableCommission.createdBy = userId
        creditableCommission.serialId = await counterService.incrementCounter(
          'commission-' + invoice.partnerId,
          session
        )
        commissionsData.push(creditableCommission)
      }
    }
  }
  if (size(commissionsData)) {
    return await commissionService.addCommissionsToCollection(
      commissionsData,
      session
    )
  }
}

export const creditManagementCommission = async (data, session) => {
  const {
    invoice,
    creditNote,
    isCreditFull,
    creditableDays,
    invoiceCommissions = [],
    invoiceTotalDays,
    userId = ''
  } = data
  const managementCommission = invoiceCommissions.find(
    (item) => item.type === 'rental_management_contract'
  )
  if (managementCommission) {
    let commissionAmount = 0
    if (isCreditFull) {
      commissionAmount = managementCommission.amount || 0
    } else if (
      !isCreditFull &&
      invoiceTotalDays &&
      creditableDays &&
      managementCommission.amount
    ) {
      commissionAmount =
        (managementCommission.amount / invoiceTotalDays) * creditableDays
    }
    if (commissionAmount) {
      const creditableCommission =
        invoiceHelper.getIdsAndType(managementCommission)
      creditableCommission.amount = commissionAmount * -1
      creditableCommission.commissionId = managementCommission._id
      creditableCommission.invoiceId = creditNote._id
      creditableCommission.createdBy = userId
      creditableCommission.serialId = await counterService.incrementCounter(
        'commission-' + invoice.partnerId,
        session
      )
      return await commissionService.addCommissionToCollection(
        creditableCommission,
        session
      )
    }
  }
}

export const creditAssignmentAddonIncome = async (data, session) => {
  const { creditNote, invoice, invoiceCommissions = [], userId } = data
  const isCreditFull = creditNote.fullyCredited
  if (invoice.isFirstInvoice && isCreditFull) {
    const commissions = invoiceCommissions.filter(
      (item) => item.type === 'assignment_addon_income'
    )
    const addCommissionsArr = []
    for (const addonIncome of commissions) {
      if (addonIncome && addonIncome.amount) {
        const commissionAmount = addonIncome.amount || 0
        const creditableCommission = invoiceHelper.getIdsAndType(addonIncome)
        creditableCommission.amount = commissionAmount * -1
        creditableCommission.commissionId = addonIncome._id
        creditableCommission.invoiceId = creditNote._id
        creditableCommission.createdBy = userId
        creditableCommission.serialId = await counterService.incrementCounter(
          'commission-' + invoice.partnerId,
          session
        )
        addCommissionsArr.push(creditableCommission)
      }
    }
    if (size(addCommissionsArr)) {
      return await commissionService.addCommissionsToCollection(
        addCommissionsArr,
        session
      )
    }
  }
}

export const creditPayout = async (data, session) => {
  // Create payout only if all payouts are completed.
  // We won't create payout for closed contract
  // So, check final settlement and pass parameter for estimated payout create.
  // If contract already closed then it will be final settlement
  const { contract, creditNote, partner } = data
  let holdQueue = false
  if (partner && partner.accountType === 'broker') {
    // const payoutData = {
    //   contract,
    //   invoice: creditNote,
    //   isFinalSettlement: !!(
    //     contract.rentalMeta && contract.rentalMeta.status === 'closed'
    //   ),
    //   partnerSetting: data.partnerSetting,
    //   userId: creditNote.createdBy
    // }
    const params = {
      contractId: contract._id,
      invoiceId: creditNote._id,
      isFinalSettlement: !!(
        contract.rentalMeta && contract.rentalMeta.status === 'closed'
      ),
      partnerId: contract.partnerId
    }
    await appQueueService.createAnAppQueueToCreateOrAdjustEstimatedPayout(
      params,
      session
    )
    holdQueue = true
    // await payoutService.createEstimatedPayout(payoutData, session)
  }
  return holdQueue
}

export const adjustPayout = async (data, session) => {
  const { creditNote, invoice, isPartlyCredited } = data
  if (!creditNote) {
    return false
  }
  const query = {
    invoiceId: invoice._id,
    partnerId: invoice.partnerId,
    status: 'estimated'
  }

  const payout = await payoutHelper.getPayout(query, session)

  const params = { doNotSetMetaForCreatingPayout: false, holdQueue: false }
  if (size(payout)) {
    await adjustEstimatedPayout(payout, creditNote, session)
    params.doNotSetMetaForCreatingPayout = true
    // For partial crediting invoice we need to first balance credit note with new invoice payout
  } else if (!isPartlyCredited) {
    params.holdQueue = await creditPayout(data, session)
  }
  return params
}

export const adjustEstimatedPayout = async (payout, creditNote, session) => {
  const updatePayoutData = {}
  const payoutMeta = payout && size(payout.meta) ? payout.meta : []
  const totalPayoutableAmount = creditNote.payoutableAmount || 0
  payoutMeta.push({
    type:
      creditNote.invoiceType === 'credit_note'
        ? 'credit_rent_invoice'
        : 'rent_invoice',
    amount: await appHelper.convertTo2Decimal(creditNote.payoutableAmount),
    invoiceId: creditNote._id
  })
  updatePayoutData.estimatedAmount = await appHelper.convertTo2Decimal(
    (payout.estimatedAmount || 0) + totalPayoutableAmount || 0
  )
  updatePayoutData.amount = await appHelper.convertTo2Decimal(
    (payout.amount || 0) + totalPayoutableAmount || 0
  )
  updatePayoutData.meta = payoutMeta
  if (updatePayoutData.amount === 0) {
    if (payout.isFinalSettlement) {
      const willBeComplete =
        await payoutHelper.isFinalSettlementPayoutWillBeCompleted(
          {
            ...payout,
            meta: payoutMeta
          },
          session
        )
      if (willBeComplete) updatePayoutData.status = 'completed'
    } else {
      updatePayoutData.status = 'completed'
    }
    if (updatePayoutData.status === 'completed') {
      updatePayoutData.paymentStatus = 'balanced'
      await payoutService.afterUpdateProcessForNewlyCompletedPayout(
        payout,
        payoutMeta,
        session
      )
    }
  }
  const updatedPayout = await payoutService.updateAPayout(
    { _id: payout._id },
    updatePayoutData,
    session
  )
  // After update process starts
  await payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
    payout,
    updatedPayout,
    session
  )
  if (updatedPayout.amount < 0) {
    await payoutService.updateNextEstimatedPayoutInfo(
      {
        payoutData: updatePayoutData,
        payoutMetaType: 'unpaid_expenses_and_commissions'
      },
      session
    )
  }
}

export const createLandlordCreditNote = async (data, session) => {
  const { contract, creditNote, creditCommissions, invoice, partnerSetting } =
    data
  let landlordCreditNote = null
  const commissionList = creditCommissions
  if (size(commissionList)) {
    const commissionMeta = []
    const newCommissionsIds = []
    let newLandlordInvoiceTotal = 0
    for (const commission of commissionList) {
      const newComTotal = await appHelper.convertTo2Decimal(
        commission.amount || 0
      )
      const newCommissionMeta = {
        type: commission.type,
        description: '',
        taxPercentage:
          await invoiceHelper.getTaxPercentageBasedOnCommissionType(
            commission.type,
            invoice.partnerId,
            commission.addonId
          ),
        qty: -1,
        price: newComTotal * -1,
        total: newComTotal,
        commissionId: commission._id
      }
      newCommissionsIds.push(commission._id)
      newLandlordInvoiceTotal += newComTotal
      commissionMeta.push(newCommissionMeta)
    }
    const landlordInvoiceData =
      await invoiceHelper.prepareLandlordCreditNoteCreateData(data, session)
    landlordInvoiceData.invoiceTotal = newLandlordInvoiceTotal
    landlordInvoiceData.rentTotal = newLandlordInvoiceTotal
    landlordInvoiceData.payoutableAmount = newLandlordInvoiceTotal
    landlordInvoiceData.remainingBalance = newLandlordInvoiceTotal
    landlordInvoiceData.commissionsMeta = commissionMeta
    landlordInvoiceData.commissionsIds = newCommissionsIds
    landlordInvoiceData.sender = creditNote.sender
    // await invoiceHelper.validateInvoiceDataBeforeCreation(landlordInvoiceData)
    landlordCreditNote = await createAnInvoice(landlordInvoiceData, session)
    // Implementation of after insert hook of landlordCreditNote
    await appQueueService.createAppQueueForAddingSerialId(
      'invoices',
      landlordCreditNote,
      session
    )
    if (landlordCreditNote.invoiceId) {
      await setLandlordInvoiceIdInCommissions(landlordCreditNote, session)
      const { landlordInvoice } =
        await payoutService.addLinkBetweenLandlordInvoiceAndPayouts(
          JSON.parse(JSON.stringify(landlordCreditNote)),
          partnerSetting,
          session
        )
      landlordCreditNote = landlordInvoice
      await updateCreditedInvoice(
        {
          creditNote: landlordCreditNote,
          voidPayment: false,
          partnerSetting
        },
        session
      )
      if (
        landlordCreditNote.contractId &&
        landlordCreditNote.invoiceTotal < 0
      ) {
        await payoutService.adjustBetweenPayoutsAndLandlordInvoices(
          {
            contractId: contract._id,
            partnerId: contract.partnerId,
            propertyId: contract.propertyId,
            isFinalSettlement: contract.status === 'closed',
            partnerSetting
          },
          session
        )
      }
    }
    // To update newly created credit note
    await updateInvoice(
      { _id: creditNote._id },
      {
        $set: {
          landlordInvoiceId: landlordCreditNote._id
        }
      },
      session
    )
  }
  return landlordCreditNote
}

/**
 * Commissions:
 * -----------
 * If we refund the full invoice, then the associated commissions will be refunded fully
 * For part refund, only management commissions will be refunded by calculating the days
 * Brokering commission will be refund only if we refund full invoice
 *
 * Add-ons:
 * -------
 * For full invoice refund, all add-ons will be refunded
 * For part refund, only recurring addOns will be refunded by calculating the days
 *
 * Fees:
 * ----
 * For full invoice refund, refund the original fees
 * For part invoice refund, don't refund any fees
 * Newly added fees will not be refunded
 *
 * Payout:
 * -------
 * Refund only affected days
 *
 **/

export const createRentCreditNote = async (data, session) => {
  const {
    contract,
    isPartlyCredited,
    options,
    partner,
    partnerSetting,
    userId,
    voidPayment
  } = data
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(data)
  if (!size(invoiceData)) {
    throw new CustomError(400, 'Cannot credit this invoice')
  }
  invoiceData.createdBy = userId
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
  await invoiceHelper.adjustRoundedLost(data, invoiceData)
  const oldInvoiceTotalAmount =
    data.invoice?.invoiceTotal - (Math.abs(data.invoice?.creditedAmount) || 0)
  const creditNoteTotalAmount = Math.abs(invoiceData?.invoiceTotal)

  console.log(
    '===> Invoice and creditNote Total Amount',
    oldInvoiceTotalAmount,
    creditNoteTotalAmount
  )

  if (oldInvoiceTotalAmount < creditNoteTotalAmount) {
    throw new CustomError(
      400,
      'Credit note total amount cannot be greater than main invoice total amount'
    )
  }
  const creditNote = await createAnInvoice(invoiceData, session)
  if (!size(creditNote))
    throw new CustomError(400, 'Unable to create creditNote invoice')
  // After insert process starts
  await appQueueService.createAppQueueForAddingSerialId(
    'invoices',
    creditNote,
    session
  )
  // await updateInvoiceFeesMeta(creditNote, session)
  // Need to update on one place
  data.invoice = await updateCreditedInvoice(
    {
      creditNote,
      notUpdateDefaultedContract: data.notUpdateDefaultedContract,
      voidPayment,
      partnerSetting
    },
    session
  )
  // After insert process end
  data.creditNote = creditNote
  console.log(
    'Successfully created credit note and initialized creditNote data',
    size(data?.creditNote)
  )
  await updateInvoiceSummary(data, session)
  data.creditCommissions = []
  // Need to optimize create commissions part
  const brokeringCommission = await creditBrokeringCommission(data, session)
  if (size(brokeringCommission)) {
    data.creditCommissions.push(brokeringCommission)
  }
  const addonCommissions = await creditAddonCommission(data, session)
  if (size(addonCommissions)) {
    data.creditCommissions = data.creditCommissions.concat(addonCommissions)
  }
  const managementCommission = await creditManagementCommission(data, session)
  if (size(managementCommission)) {
    data.creditCommissions.push(managementCommission)
  }
  const assignmentCommissions = await creditAssignmentAddonIncome(data, session)
  if (size(assignmentCommissions)) {
    data.creditCommissions = data.creditCommissions.concat(
      assignmentCommissions
    )
  }
  console.log('Commissions data', JSON.stringify(data.creditCommissions))
  const payoutAdjustingResult = (await adjustPayout(data, session)) || {}
  let { holdQueue } = payoutAdjustingResult || {}
  // const landlordCreditNote = await createLandlordCreditNote(data, session)
  if (size(data.creditCommissions)) {
    await appQueueService.createAnAppQueueToCreateLandlordCreditNote(
      {
        creditNoteId: creditNote._id,
        contractId: contract._id,
        hold: holdQueue,
        partnerId: contract.partnerId
      },
      session
    )
    holdQueue = true
  }
  let partlyCreditInvoice = null
  console.log({ isPartlyCredited, remainingAmount: options?.remainingAmount })
  if (isPartlyCredited && options.remainingAmount !== 0) {
    const partlyCreditInvoiceData =
      await invoiceHelper.prepareDataForPartialCreditInvoice(data, session)
    partlyCreditInvoice = await createAnInvoice(
      partlyCreditInvoiceData,
      session
    )
    // After insert hook implementation of partly credit invoice
    await setInvoiceIdInCorrection(partlyCreditInvoice, session)
    await updateInvoiceFeesMeta(partlyCreditInvoice, session)
    await invoiceSummaryService.createInvoiceSummary(
      partlyCreditInvoice.toObject(),
      session
    )
    if (partner.accountType === 'broker') {
      // const commissionParams = {
      //   invoiceData: partlyCreditInvoice,
      //   partner,
      //   partnerSetting,
      //   propertyContractInfo: contract,
      //   userId
      // }
      await appQueueService.createAnAppQueueToAddInvoiceCommissions(
        {
          adjustmentNotNeeded:
            !!payoutAdjustingResult?.doNotSetMetaForCreatingPayout,
          contractId: contract._id,
          hold: holdQueue,
          invoiceId: partlyCreditInvoice._id,
          partnerId: contract.partnerId
        },
        session
      )
      // await commissionService.addInvoiceCommissions(commissionParams, session)
      // const payoutData = {
      //   contract: data.contract,
      //   invoice: partlyCreditInvoice,
      //   isFinalSettlement: false,
      //   partnerSetting: data.partnerSetting,
      //   userId: data.userId
      // }
      const params = {
        contractId: contract._id,
        hold: true,
        invoiceId: partlyCreditInvoice._id,
        isFinalSettlement: false,
        meta: !payoutAdjustingResult?.doNotSetMetaForCreatingPayout
          ? [
              {
                type:
                  creditNote.invoiceType === 'credit_note'
                    ? 'credit_rent_invoice'
                    : 'rent_invoice',
                amount: await appHelper.convertTo2Decimal(
                  creditNote.payoutableAmount
                ),
                invoiceId: creditNote._id
              }
            ]
          : undefined,
        partnerId: contract.partnerId
      }
      await appQueueService.createAnAppQueueToCreateEstimatedPayout(
        params,
        session
      )
      // await payoutService.createEstimatedPayout(payoutData, session)
      holdQueue = true
    }
    await addInvoiceEndDateInContract(partlyCreditInvoice, session)
    if (partlyCreditInvoice.isFirstInvoice) {
      await appQueueService.createAnAppQueueToCheckCommissionChanges(
        {
          contractId: contract._id,
          hold: holdQueue,
          partnerId: contract.partnerId
        },
        session
      )
    }
  }
  const invoices = [creditNote]
  if (partlyCreditInvoice) invoices.push(partlyCreditInvoice)
  return invoices
}

export const findCreditNoteInvoiceAndCheckCommissionChanges = async (
  params = {},
  session
) => {
  const { contract, partnerSetting } = params
  const contractId = contract._id
  const hasCreditNoteInvoice = await invoiceHelper.getAnInvoiceWithSort(
    {
      contractId,
      status: 'credited',
      invoiceType: 'invoice',
      isFirstInvoice: true
    },
    {
      createdAt: -1
    },
    session
  )
  if (hasCreditNoteInvoice) {
    const landlordCreditNote = await invoiceHelper.getAnInvoiceWithSort(
      {
        contractId,
        status: 'balanced',
        invoiceType: 'landlord_credit_note'
      },
      { createdAt: -1 },
      session
    )
    const landlordInvoice = await invoiceHelper.getAnInvoiceWithSort(
      {
        contractId,
        status: 'balanced',
        invoiceType: 'landlord_invoice'
      },
      { createdAt: -1 },
      session
    )
    const oldCommissionsMeta = landlordCreditNote?.commissionsMeta || []
    const oldCommission = oldCommissionsMeta.find(
      (commission) => commission.type === 'brokering_contract'
    )
    const oldTotalNegValue = oldCommission?.total || 0
    const oldTotal =
      oldTotalNegValue !== 0 ? oldTotalNegValue * -1 : oldTotalNegValue

    const newCommissionsMeta = landlordInvoice?.commissionsMeta || []
    const newCommission = newCommissionsMeta.find(
      (commission) => commission.type === 'brokering_contract'
    )
    const newTotal = newCommission?.total || 0

    if (oldTotal !== newTotal) {
      await contractService.addHistoryToContractForCommissionChanges(
        {
          contract,
          newCommission: newTotal,
          oldCommission: oldTotal,
          partnerSetting
        },
        session
      )
    }
  }
}

export const prepareDataAndCreateCreditNote = async (data, session) => {
  data.monthlyRent = await invoiceHelper.getCreditableRent(data)
  data.fees = invoiceHelper.getCreditableFees(data)
  data.addons = await invoiceHelper.getCreditableAddons(data)
  data.invoiceData = await invoiceHelper.prepareCreditNoteData(data)
  console.log('Checking data.monthlyRent', data.monthlyRent)
  return await createRentCreditNote(data, session)
}

export const creditRentInvoice = async (data, session) => {
  const { enabledNotification, invoice, requestFrom, today } = data
  data.today = today ? new Date(today) : new Date()
  data.dateFormat = 'YYYY-MM-DD'
  data.enabledNotification = enabledNotification || false
  if (
    !invoice ||
    !invoice.status ||
    ['credited', 'lost'].includes(invoice.status) ||
    invoice.invoiceType !== 'invoice' ||
    (requestFrom !== 'cancelCorrection' &&
      invoice.isCreditedForCancelledCorrection)
  ) {
    throw new CustomError(400, 'Could not create credit note')
  }
  await invoiceHelper.isCreditableInvoice(invoice?._id, session)
  data.invoiceTotalDays = await invoiceHelper.getInvoiceTotalDays(data)
  // Termination date only available for lease termination
  data.creditableDays = await invoiceHelper.getCreditableDays(data)
  data.isCreditFull = await invoiceHelper.isCreditFullInvoice(data)
  data.isCreditFullByPartiallyCredited =
    await invoiceHelper.isCreditFullByPartiallyCredited(data)
  console.log('===> Creating credit rent invoice, data: ', {
    contractId: data?.contractId,
    invoiceTotalDays: data.invoiceTotalDays,
    creditableDays: data.creditableDays,
    isCreditFull: data.isCreditFull,
    isCreditFullByPartiallyCredited: data.isCreditFullByPartiallyCredited
  })
  if (
    !data.creditableDays &&
    !data.isCreditFullByPartiallyCredited &&
    !data.isCreditFull
  )
    return {}
  return await prepareDataAndCreateCreditNote(data, session)
}
/* Rent Credit Note Ends
 * */

/* Correction Invoice Starts
 * */
export const createCorrectionInvoice = async (params, session) => {
  const preparedData = await invoiceHelper.getCorrectionInvoiceData(params)
  let invoice
  if (!preparedData.invoiceData.isNonRentInvoice) {
    const correction = await invoiceHelper.processInvoiceDataBeforeCreation(
      preparedData
    )
    invoice = await createRentInvoice(correction, session)
  } else {
    invoice = await createNonRentInvoice(preparedData, session)
  }
  return [invoice]
}
/* Correction Invoice Ends
 * */

/* Non Rent Invoice Starts
 * */
export const createNonRentInvoice = async (data, session) => {
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(data)
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
  const [nonRentInvoice] = await InvoiceCollection.create([invoiceData], {
    session
  })
  await updateInvoiceFeesMeta(nonRentInvoice.toObject(), session) // For all type invoices
  await createInvoiceLog(nonRentInvoice.toObject(), session) // For all types of Invoice
  await paymentService.adjustBetweenPaymentsAndInvoices(
    { ...nonRentInvoice.toObject(), processType: 'matchPaymentsWithInvoices' },
    session
  ) // Payment Service
  await setInvoiceIdInCorrection(nonRentInvoice.toObject(), session) // For correction and non rent invoice
  // Check if the transaction is enabled for invoice partner
  if (
    await partnerHelper.isTransactionEnabledOfAPartner(nonRentInvoice.partnerId)
  ) {
    await addInvoiceTransactions(nonRentInvoice.toObject(), 'regular', session) // For all type of invoices
  }
  return nonRentInvoice
}

export const createNonRentInvoices = async (data, session) => {
  const { isDemo = null, enabledNotification = null } = data
  let { contract } = data
  if (!size(contract)) {
    contract = await contractHelper.getContractById(data.contractId)
  }
  if (!size(contract)) {
    throw new CustomError(400, 'ContractId or Contract is required!')
  }
  const resultData = []
  // Get all the non-rent corrections first
  const corrections = await invoiceHelper.getNonRentCorrections(
    contract,
    session
  )
  // If corrections exists, create separate non-rent invoice for each correction
  const correctionInvoiceData = {
    contract,
    isDemo,
    enabledNotification
  }
  if (size(corrections)) {
    for (const correction of corrections) {
      correctionInvoiceData.correctionId = correction._id
      const result = await createCorrectionInvoice(
        correctionInvoiceData,
        session
      )
      if (size(result)) {
        resultData.push(...result)
      }
    }
  }
  return size(resultData) ? resultData : false
}
/* Non Rent Invoice Ends
 * */

/* Landlord Invoice Starts
 * */
export const setLandlordInvoiceIdInInvoices = async (
  landlordInvoice,
  session
) => {
  const { partnerId, commissionsIds } = landlordInvoice
  const landlordInvoiceId = landlordInvoice._id
  if (partnerId && landlordInvoiceId && size(commissionsIds)) {
    const query = {
      _id: { $in: commissionsIds },
      partnerId
    }
    const commissions = await commissionHelper.getCommissions(query, session)
    let invoiceIds = map(commissions, 'invoiceId')
    invoiceIds = compact(uniq(invoiceIds))
    if (size(invoiceIds)) {
      const updateQuery = { _id: { $in: invoiceIds }, partnerId }
      const updateData = { $set: { landlordInvoiceId } }
      await updateInvoices(updateQuery, updateData, session)
    }
  }
}

export const setLandlordInvoiceIdInCommissions = async (
  landlordInvoice,
  session
) => {
  const { partnerId, commissionsIds } = landlordInvoice
  const landlordInvoiceId = landlordInvoice._id
  if (partnerId && landlordInvoiceId && size(commissionsIds)) {
    const query = { _id: { $in: commissionsIds }, partnerId }
    await commissionService.updateCommissions(
      query,
      { landlordInvoiceId },
      session
    )
  }
}

export const setLandlordInvoiceIdInCorrections = async (
  landlordInvoice,
  session
) => {
  const { partnerId, correctionsIds } = landlordInvoice
  const landlordInvoiceId = landlordInvoice._id
  if (partnerId && landlordInvoiceId && size(correctionsIds)) {
    const query = { _id: { $in: correctionsIds }, partnerId }
    const data = { landlordInvoiceId }
    await correctionService.updateCorrections(query, data, session)
  }
}

export const updateLandlordCreditNoteForCancelledCorrection = async (
  landlordInvoice,
  session
) => {
  const { forCorrection, creditNoteIds } = landlordInvoice
  const invoiceId = landlordInvoice._id
  if (forCorrection && invoiceId && size(creditNoteIds)) {
    const query = { _id: { $in: creditNoteIds } }
    const updateData = {
      isCreditedForCancelledCorrection: true,
      fullyCredited: true,
      invoiceId
    }
    await updateInvoices(query, updateData, session)
  }
}

export const createLandlordInvoice = async (data, session) => {
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(
    data,
    session
  )
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
  const [invoice] = await InvoiceCollection.create([invoiceData], { session })
  await appQueueService.createAppQueueForAddingSerialId(
    'invoices',
    invoice,
    session
  )
  await updateInvoiceFeesMeta(invoice, session)
  await setLandlordInvoiceIdInInvoices(invoice, session)
  await setLandlordInvoiceIdInCommissions(invoice, session)
  await setLandlordInvoiceIdInCorrections(invoice, session)
  await updateLandlordCreditNoteForCancelledCorrection(invoice, session) // Todo: test case will be implemented while working on landlord creditnote
  return invoice
}

export const createLandlordInvoiceAndAfterHooksProcessForCorrection = async (
  data = {},
  contract = {},
  session
) => {
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(
    data,
    session
  )
  await invoiceHelper.validateInvoiceDataBeforeCreation(
    invoiceData,
    data?.isLandlordCorrectionInvoice
  )
  let invoice = await createAnInvoice(invoiceData, session)
  if (!size(invoice)) return {}

  await appQueueService.createAppQueueForAddingSerialId(
    'invoices',
    invoice,
    session
  )
  await updateInvoiceFeesMeta(invoice, session)
  await setLandlordInvoiceIdInInvoices(invoice, session)
  await setLandlordInvoiceIdInCorrections(invoice, session)

  const { partnerSetting } = data
  const { landlordInvoice } =
    await payoutService.addLinkBetweenLandlordInvoiceAndPayouts(
      invoice.toObject(),
      partnerSetting,
      session
    )

  invoice = landlordInvoice
  if (invoice.invoiceTotal < 0) {
    await payoutService.adjustBetweenPayoutsAndLandlordInvoices(
      {
        contractId: contract._id,
        isFinalSettlement: contract.status === 'closed',
        partnerId: contract.partnerId,
        partnerSetting,
        propertyId: contract.propertyId
      },
      session
    )
  }
  return invoice
}

export const createLandlordInvoiceForCorrection = async (data, session) => {
  const {
    contractId,
    propertyId,
    correctionId,
    isDemo,
    returnPreview,
    enabledNotification,
    contract,
    partnerSetting,
    invoiceData,
    today
  } = data
  const correctionQuery = {
    contractId,
    propertyId,
    payoutId: { $exists: false },
    addTo: 'payout',
    correctionStatus: 'active'
  }
  if (correctionId) {
    correctionQuery._id = correctionId
    delete correctionQuery.payoutId
  }
  const corrections = await correctionHelper.getCorrections(
    correctionQuery,
    session
  )
  const invoicePromiseArray = []
  for (const correction of corrections) {
    const correctionData =
      await invoiceHelper.getCorrectionDataByCorrectionInfo(correction)
    const creatingNewInvoiceData = {
      contract,
      invoiceData,
      correctionData,
      partnerSetting,
      today,
      isDemo,
      returnPreview,
      enabledNotification
    }
    creatingNewInvoiceData.isLandlordInvoice = true
    const preparedData = await invoiceHelper.prepareInvoiceData(
      creatingNewInvoiceData
    )
    if (data?.isLandlordCorrectionInvoice) {
      preparedData.isLandlordCorrectionInvoice = true
    }
    invoicePromiseArray.push(
      createLandlordInvoiceAndAfterHooksProcessForCorrection(
        preparedData,
        contract,
        session
      )
    )
  }

  if (size(invoicePromiseArray)) {
    return await Promise.all(invoicePromiseArray)
  }
  return []
}

export const createLandlordInvoiceForCommission = async (data, session) => {
  const {
    adjustmentNotNeeded,
    contract,
    dueDate,
    enabledNotification,
    invoiceEndOn,
    invoiceId,
    invoiceStartOn,
    invoiceData,
    isDemo,
    partner,
    partnerId,
    partnerSetting,
    returnPreview,
    today,
    userId
  } = data
  if (!invoiceId) {
    throw new CustomError(
      400,
      'InvoiceId is required for landlord commission invoice!'
    )
  }
  const commissions = await commissionHelper.getCommissions(
    { invoiceId, partnerId },
    session
  )
  if (!size(commissions)) return null
  invoiceData.commissionsMeta = []
  invoiceData.commissionsIds = []
  for (const commission of commissions) {
    const newComTotal = await appHelper.convertTo2Decimal(
      commission.amount || 0
    )
    const commissionMeta = {
      type: commission.type,
      description: '',
      taxPercentage: await invoiceHelper.getTaxPercentageBasedOnCommissionType(
        commission.type,
        partnerId,
        commission.addonId
      ),
      qty: 1,
      price: newComTotal,
      total: newComTotal,
      commissionId: commission._id
    }
    invoiceData.commissionsMeta.push(commissionMeta)
    invoiceData.commissionsIds.push(commission._id)
  }
  const creatingNewInvoiceData = {
    contract,
    data: {
      dueDate
    },
    enabledNotification,
    invoiceData,
    isLandlordInvoice: true,
    isDemo,
    partnerSetting,
    returnPreview,
    today
  }
  const preparedData = await invoiceHelper.prepareInvoiceData(
    creatingNewInvoiceData
  )
  if (invoiceEndOn) preparedData.invoiceData.invoiceEndOn = invoiceEndOn
  if (invoiceStartOn) preparedData.invoiceData.invoiceStartOn = invoiceStartOn
  preparedData.partner = partner
  const landlordInvoiceData =
    await invoiceHelper.processInvoiceDataBeforeCreation(preparedData)
  landlordInvoiceData.createdBy = userId
  let invoice = await createAnInvoice(landlordInvoiceData, session)
  await appQueueService.createAppQueueForAddingSerialId(
    'invoices',
    invoice,
    session
  )
  await updateInvoiceFeesMeta(invoice, session)
  await setLandlordInvoiceIdInInvoices(invoice, session)
  await setLandlordInvoiceIdInCommissions(invoice, session)
  if (!adjustmentNotNeeded) {
    const { landlordInvoice } =
      await payoutService.addLinkBetweenLandlordInvoiceAndPayouts(
        invoice.toObject(),
        partnerSetting,
        session
      )
    invoice = landlordInvoice
  }
  if (invoice.invoiceTotal < 0) {
    await payoutService.adjustBetweenPayoutsAndLandlordInvoices(
      {
        contractId: contract._id,
        isFinalSettlement: contract.status === 'closed',
        partnerId: contract.partnerId,
        partnerSetting,
        propertyId: contract.propertyId
      },
      session
    )
  }
  return invoice
}

export const createLandlordInvoices = async (data, session) => {
  const {
    contract,
    contractId,
    landlordInvoiceFor,
    partnerId,
    partnerSetting
  } = data
  if (!contractId || !partnerId) {
    throw new CustomError(400, 'ContractId and PartnerId are required!')
  }
  data.contract = size(contract)
    ? contract
    : await contractHelper.getContractById(contractId, session)
  data.partnerSetting = size(partnerSetting)
    ? partnerSetting
    : await partnerSettingHelper.getSettingByPartnerId(partnerId, session)
  data.today = await invoiceHelper.getInvoiceDate(
    new Date(),
    data.partnerSetting
  )
  data.invoiceData = await invoiceHelper.getInvoiceDataForLandlordInvoice(
    data.contract
  )
  let invoices = []
  if (landlordInvoiceFor === 'payoutCorrections') {
    invoices = await createLandlordInvoiceForCorrection(data, session)
  } else if (landlordInvoiceFor === 'commission') {
    const landlordInvoice = await createLandlordInvoiceForCommission(
      data,
      session
    )
    invoices.push(landlordInvoice)
  }
  return invoices
}

export const addMissingPayoutIdInLandlordInvoiceOrCreditNote = async (
  invoice = {},
  session,
  returnData
) => {
  const { _id: landlordInvoiceId, contractId, partnerId } = invoice
  const updateData = {}

  let { commissionsMeta, addonsMeta } = invoice
  const params = {
    metaArray: commissionsMeta,
    contractId,
    landlordInvoiceId
  }
  if (size(invoice.commissionsMeta)) {
    commissionsMeta =
      await invoiceHelper.getAdjustedCommissionsMetaOrAddonsMeta(
        params,
        session
      )
  }
  if (size(invoice.addonsMeta)) {
    params.metaArray = addonsMeta
    addonsMeta = await invoiceHelper.getAdjustedCommissionsMetaOrAddonsMeta(
      params,
      session
    )
  }
  if (size(commissionsMeta)) updateData.commissionsMeta = commissionsMeta

  if (size(addonsMeta)) updateData.addonsMeta = addonsMeta
  if (returnData) {
    return updateData
  }
  if (size(updateData)) {
    const query = { _id: invoice._id, partnerId }
    const updatedInvoice = await updateInvoice(query, updateData, session)
    return updatedInvoice
  }
}
/* Landlord Invoice Ends
 * */

/** These blocks of code will be used or modified later
export const createLandlordCreditNoteInvoice = async (data, session) => {
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(data);
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData);
  const [invoice] = await InvoiceCollection.create([invoiceData], { session });
  console.log(`--- Created Landlord Credit Note Invoice for contract: ${invoice.contractId}, start: ${toISO(invoice.invoiceStartOn)}, end: ${toISO(invoice.invoiceEndOn)}---`);
  await updateInvoiceFeesMeta(invoice, session);
  await createInvoiceLog(invoice, session);
  return invoice;
};
*/

export const handleRequestForInvoiceCreation = async (req) => {
  let result = []
  const { body, session, user } = req
  const { partnerId } = user
  body.partnerId = partnerId
  const { invoiceType, isCorrectionInvoice, isNonRentInvoice } = body
  if (invoiceType === 'invoice') {
    if (isCorrectionInvoice && isNonRentInvoice) {
      result = await createNonRentInvoices(body, session)
    } else if (isCorrectionInvoice) {
      result = await createCorrectionInvoice(body, session)
    } else {
      result = await createRentInvoices(body, session)
    }
  } else if (invoiceType === 'credit_note') {
    await invoiceHelper.getNecessaryDataForCreditNote(body)
    result = await creditRentInvoice(body, session)
  } else if (invoiceType === 'landlord_invoice') {
    result = await createLandlordInvoices(body, session)
  } else if (invoiceType === 'landlord_credit_note') {
    // Call specific function
  } else {
    throw new CustomError(400, 'Request body must include a valid invoiceType')
  }
  return result
}

/* Using Queue starts
 * */
export const insertInQueueForInvoiceLostInfoInPayout = async (
  invoice = {},
  session
) => {
  const { _id, contractId, partnerId, invoiceType } = invoice
  const queueData = {
    event: 'lost_invoice',
    action: 'add_invoice_lost_info_in_payout',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId,
      invoiceType
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForVippsStatusToNew = async (
  invoice = {},
  session
) => {
  const { _id, contractId, partnerId } = invoice
  const queueData = {
    event: 'created_invoice_pdf',
    action: 'add_vipps_status_to_new',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForInvoiceSendToVipps = async (
  invoice = {},
  session
) => {
  const { _id, contractId, partnerId } = invoice
  const queueData = {
    event: 'added_vipps_status_to_new',
    action: 'send_invoice_to_vipps',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForInitiateInoviceSend = async (
  invoice = {},
  session
) => {
  const { _id, contractId, partnerId } = invoice
  const queueData = {
    action: 'send_invoice_to_vipps',
    event: 'initiate_invoice_send_to_vipps',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForAddVippsCountInPartnerUsage = async (
  invoice = {},
  session
) => {
  const { _id, partnerId, branchId, createdAt } = invoice
  const queueData = {
    event: 'sent_invoice_to_vipps',
    action: 'vipps_invoice_count_for_partner_usage',
    priority: 'regular',
    destination: 'invoice',
    params: {
      partnerId,
      branchId,
      createdAt,
      meta: {
        invoiceId: _id
      },
      type: 'vipps_invoice',
      total: 1
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForLostInvoiceTransaction = async (
  invoice = {},
  session
) => {
  const { _id, contractId, partnerId, invoiceType } = invoice
  const queueData = {
    event: 'lost_invoice',
    action: 'add_transaction_for_lost_invoice',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId,
      invoiceType
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForRemoveLossRecognitionTransaction = async (
  invoice = {},
  lostMeta,
  session
) => {
  const { _id, contractId, partnerId, invoiceType } = invoice
  const queueData = {
    event: 'removed_lost_invoice',
    action: 'add_transaction_for_remove_loss_recognition',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId,
      invoiceType,
      lostMeta
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForSendInvoiceOrDisableNotification = async (
  invoice = {},
  session
) => {
  const { _id, contractId, partnerId } = invoice
  const queueData = {
    event: 'processed_pdf_or_landlord_payable',
    action: 'send_invoice_or_disable_notification',
    priority: 'regular',
    destination: 'invoice',
    params: {
      invoiceId: _id,
      contractId,
      partnerId
    }
  }
  const queue = await appQueueService.insertInQueue(queueData, session)
  return queue
}

export const insertInQueueForSendingInvoiceNotification = async (
  invoice = {},
  session
) => {
  const {
    _id: collectionId,
    invoiceType,
    isFinalSettlement,
    partnerId
  } = invoice || {}
  if (!(collectionId && invoiceType && partnerId))
    throw new CustomError(
      400,
      'Missing required data to create app queue for sending invoice'
    )

  const event = isFinalSettlement
    ? 'send_final_settlement'
    : `send_${invoiceType}`

  const previousInvoiceSendingAppQueue = await appQueueHelper.getAnAppQueue(
    {
      event,
      'params.collectionId': collectionId,
      'params.partnerId': partnerId
    },
    session
  )
  if (previousInvoiceSendingAppQueue) return false

  const appQueueData = {
    action: 'send_notification',
    event,
    destination: 'notifier',
    params: {
      collectionId,
      collectionNameStr: 'invoices',
      partnerId
    },
    priority: 'regular'
  }

  return await appQueueService.insertInQueue(appQueueData, session)
}
/* Using Queue ends
 * */

/* Vipps Starts
 * */
export const updateInvoiceForVipps = async (data, session) => {
  const {
    invoiceId,
    partnerId,
    vippsStatus,
    vippsEventStatus,
    vippsEventNote
  } = data
  const updateData = {}
  const query = { _id: invoiceId, partnerId }
  if (vippsStatus) {
    updateData.$set = { vippsStatus }
  }
  updateData.$push = {
    vippsEvents: {
      status: vippsEventStatus,
      createdAt: new Date(),
      note: vippsEventNote
    }
  }
  const updatedInvoice = await updateInvoice(query, updateData, session)
  return updatedInvoice
}

export const addVippsStatusToNew = async (req) => {
  const { body, session } = req
  const { contractId, invoiceId } = body
  const contract = await contractHelper.getContractById(contractId, session)
  const { disableVipps } =
    contract && contract.rentalMeta ? contract.rentalMeta : {}
  const invoice = await invoiceHelper.getInvoiceById(invoiceId)
  if ((await vippsHelper.hasAccessForVipps(invoice)) && !disableVipps) {
    const params = {
      invoiceId,
      partnerId: invoice.partnerId,
      vippsStatus: 'new',
      vippsEventStatus: 'new',
      vippsEventNote: 'add vipps status to new'
    }
    const updatedInvoice = await updateInvoiceForVipps(params, session)
    await insertInQueueForInvoiceSendToVipps(updatedInvoice, session)
    return updatedInvoice
  }
}

export const sendInvoiceToVipps = async (req) => {
  const { body, session } = req
  const { invoiceId, partnerId } = body
  if (await vippsHelper.isEnabledVippsRegninger(partnerId)) {
    const query = {
      _id: invoiceId,
      partnerId,
      invoiceType: 'invoice',
      status: { $in: ['new', 'created', 'overdue'] },
      isDefaulted: { $ne: true },
      isPartiallyPaid: { $ne: true },
      vippsStatus: { $in: ['new', 'sending'] },
      enabledNotification: { $ne: false }
    }
    const invoice = await invoiceHelper.getInvoice(query, session)
    if (invoice) {
      const params = {
        invoiceId,
        partnerId,
        vippsStatus: 'sending',
        vippsEventStatus: 'sending',
        vippsEventNote: 'Vipps invoice sending'
      }
      const updatedInvoice = await updateInvoiceForVipps(params, session)
      await insertInQueueForInitiateInoviceSend(updatedInvoice, session)
      return updatedInvoice
    } else {
      throw new CustomError(
        500,
        `Invalid invoice. Vipps invoice sending. invoice id: ${invoiceId} and partner id: ${partnerId}`
      )
    }
  }
}
/* Vipps Ends
 * */

/* Transactions Starts
 * */
// Transaction for rounded amount
export const createTransactionForInvoiceRoundedAmount = async (
  params,
  session
) => {
  const { data, invoice, transactions, transactionEvent } = params
  const { partnerId, addonsMeta = [] } = invoice
  for (const addon of addonsMeta) {
    if (addon.correctionId) {
      data.type = 'correction'
      data.correctionId = addon.correctionId
    }
  }
  const { type } = data
  const invoiceId = invoice._id
  const query = {
    partnerId,
    invoiceId,
    type,
    subType: 'rounded_amount',
    amount: invoice.roundedAmount
  }
  const isTransactionExist = !!(await transactionHelper.getTransaction(
    query,
    session
  ))
  if (!isTransactionExist) {
    const accountingParams = {
      partnerId,
      accountingType: 'rounded_amount'
    }
    let roundedData = await transactionHelper.getAccountingDataForTransaction(
      accountingParams,
      session
    )
    roundedData.amount = invoice.roundedAmount
    roundedData.transactionEvent = transactionEvent
    if (size(roundedData)) {
      roundedData = extend(roundedData, data)
      const roundedTransaction = await transactionService.createTransaction(
        roundedData,
        session
      )
      if (size(roundedTransaction)) {
        transactions.push(roundedTransaction)
      }
    }
  }
}

// Transaction for invoice addons meta
export const createTransactionsForInvoiceAddonsMeta = async (
  params,
  session
) => {
  let { data } = params
  // We are creating a clone of base data object here, just to avoid modifying the referenced data object
  data = cloneDeep(data)
  const { invoice, transactions, transactionEvent } = params
  const { partnerId } = invoice
  const { type } = data
  const invoiceId = invoice._id
  for (const addon of invoice.addonsMeta) {
    // Prepare addon transaction check query
    const query = {
      partnerId,
      invoiceId,
      addonId: addon.addonId,
      amount: addon.total
    }
    // If addonMeta has correctionId, set transaction type and correctionId based on that
    if (addon.correctionId) {
      data.type = 'correction'
      data.correctionId = addon.correctionId
      query.type = 'correction'
      query.correctionId = addon.correctionId
    } else {
      query.type = type
      data.type = type
    }
    // If transaction already not exist, get accounting data for transaction
    const isTransactionExist = !!(await transactionHelper.getTransaction(
      query,
      session
    ))
    if (!isTransactionExist) {
      const accountingParams = {
        partnerId,
        accountingType: 'addon',
        options: addon
      }
      let addonTransactionData =
        await transactionHelper.getAccountingDataForTransaction(
          accountingParams,
          session
        )
      addonTransactionData.amount = addon.total
      addonTransactionData.addonId = addon.addonId
      addonTransactionData.transactionEvent = transactionEvent
      addonTransactionData = extend(addonTransactionData, data)
      if (addon.correctionId) {
        addonTransactionData.period =
          await transactionHelper.getFormattedTransactionPeriod(
            invoice.createdAt,
            invoice.partnerId
          )
      }
      const addonTransaction = await transactionService.createTransaction(
        addonTransactionData,
        session
      )
      if (size(addonTransaction)) {
        transactions.push(addonTransaction)
      }
    }
  }
}

// Transaction for invoice fees meta
export const createTransactionsForInvoiceFeesMeta = async (params, session) => {
  const { data, invoice, transactions, transactionEvent } = params
  const { _id: invoiceId, partnerId } = invoice
  // Prepare allowedType & unpaidCollectionNoticeFees array
  // if (type === 'credit_note') {
  //   unpaidCollectionNoticeFees = filter(
  //     invoice.feesMeta,
  //     (fee) => fee && fee.type === 'unpaid_collection_notice'
  //   )
  // }
  const transactionPeriod = (
    await appHelper.getActualDate(partnerId, true, new Date())
  ).format('YYYY-MM')

  for (const feeMeta of invoice.feesMeta) {
    console.log(
      '=====> Checking fee meta for creating transaction, feeMeta:',
      feeMeta,
      'invoiceId:',
      invoiceId,
      '<====='
    )
    if (
      indexOf(
        invoiceHelper.allowedTypesOfInvoiceFeesMetaForTransaction,
        feeMeta.type
      ) !== -1
    ) {
      const feesTotal = feeMeta.total || feeMeta.amount * feeMeta.qty
      // Check allowed type, and Set accounting type & transaction subType based on type fees meta type
      const { accountingType = '', transactionSubtype = '' } =
        invoiceHelper.getAccountingAndTransactionSubTypeFromFeesMeta(
          feeMeta.type
        )
      // Prepare fees meta transaction query to find out existing transaction
      // Credit note invoice has multiple unpaid collection notice fees; So need to add multiple transactions for same amount, type and subtype
      // const creditCheckParams = {
      //   feeMeta,
      //   unpaidCollectionNoticeFees,
      //   feesTotal,
      //   type
      // }
      // May be needed later:
      // isTransactionExist =
      //   invoiceHelper.checkCreditNoteFeesMetaTransaction(creditCheckParams)
      // If not existing fees meta transaction and has fees total, get accounting data & create transaction
      if (feesTotal) {
        const accountingParams = {
          partnerId,
          accountingType,
          options: { transactionSubtype }
        }
        let feeTransactionData =
          await transactionHelper.getAccountingDataForTransaction(
            accountingParams,
            session
          )
        feeTransactionData = extend(feeTransactionData, data)
        feeTransactionData.amount = feesTotal
        if (feeMeta.type !== 'invoice') {
          feeTransactionData.period = transactionPeriod
        }
        feeTransactionData.transactionEvent = transactionEvent
        console.log(
          '=====> Checking fee transaction data for creating transaction, feeMeta:',
          feeTransactionData,
          'invoiceId:',
          invoiceId,
          '<====='
        )
        const feesTransaction = await transactionService.createTransaction(
          feeTransactionData,
          session
        )
        console.log(
          '=====> Checking fee inserted transaction ID for creating transaction, feeMeta:',
          feesTransaction?._id,
          'invoiceId:',
          invoiceId,
          '<====='
        )
        if (size(feesTransaction)) {
          transactions.push(feesTransaction)
        }
      }
    }
  }
}

// Transaction for invoice move to fees
export const createTransactionsForInvoiceMoveToFees = async (
  params,
  session
) => {
  const { data, feesMeta = [], invoiceId, partnerId, transactionEvent } = params
  const transactionPeriod = (
    await appHelper.getActualDate(partnerId, true, new Date())
  ).format('YYYY-MM')

  const transactions = []
  for (const feeMeta of feesMeta) {
    console.log(
      '=====> Checking fee meta for creating transaction, feeMeta:',
      feeMeta,
      'invoiceId:',
      invoiceId,
      '<====='
    )
    const { amount, qty, total, type } = feeMeta || {}
    if (
      includes(invoiceHelper.allowedTypesOfInvoiceMoveToFeesTransaction, type)
    ) {
      const { accountingType = '', transactionSubtype = '' } =
        invoiceHelper.getAccountingAndTransactionSubTypeFromFeesMeta(type) || {}
      const feesTotal = total || amount * qty

      if (feesTotal) {
        const accountingParams = {
          partnerId,
          accountingType,
          options: { transactionSubtype }
        }
        const accountingData =
          await transactionHelper.getAccountingDataForTransaction(
            accountingParams,
            session
          )
        const feeTransactionData = {
          ...cloneDeep(accountingData),
          ...cloneDeep(data),
          amount: feesTotal,
          transactionEvent
        }
        feeTransactionData.period = transactionPeriod
        console.log(
          '=====> Checking fee transaction data for creating transaction, transactionData:',
          feeTransactionData,
          'invoiceId:',
          invoiceId,
          '<====='
        )
        const [feesTransaction] =
          (await transactionService.createTransaction(
            feeTransactionData,
            session
          )) || []
        console.log(
          '=====> Checking fee inserted transaction ID for creating transaction, transactionId:',
          feesTransaction?._id,
          'invoiceId:',
          invoiceId,
          '<====='
        )
        if (size(feesTransaction)) transactions.push(feesTransaction)
      }
    }
  }

  return transactions
}

const createTransactionsForInvoiceContents = async (params, session) => {
  const { data, invoice, transactions, transactionEvent } = params
  const { _id, contractId, partnerId, invoiceContent } = invoice
  for (const content of invoiceContent) {
    // Check if transaction already exists, if exists do nothing
    const query = {
      partnerId,
      invoiceId: _id,
      type: data.type,
      amount: content.total,
      subType: { $in: ['rent', 'rent_with_vat'] }
    }
    console.log('query isExistsTransaction', query)
    const isExistsTransaction = !!(await transactionHelper.getTransaction(
      query,
      session
    ))
    console.log('isExistsTransaction', isExistsTransaction)
    console.log('content.type', content.type)
    if (content.type === 'monthly_rent' && !isExistsTransaction) {
      // Get accounting data for transaction
      const contract = await contractHelper.getAContract(
        { _id: contractId, partnerId },
        session
      )
      console.log('check contract ', contract)
      const accountingType =
        contract && contract.rentalMeta && contract.rentalMeta.isVatEnable
          ? 'rent_with_vat'
          : 'rent'
      console.log('accountingType ', accountingType)
      // Write transactionHelper for get account data for transaction
      const accountingParams = {
        partnerId: invoice.partnerId,
        accountingType
      }
      let invoiceTransactionData =
        await transactionHelper.getAccountingDataForTransaction(
          accountingParams,
          session
        )
      invoiceTransactionData.amount = content.total
      // Extend "data" to "invoiceTransactionData"
      if (size(invoiceTransactionData) && invoiceTransactionData.amount) {
        invoiceTransactionData = extend(invoiceTransactionData, data)
        // Finally call addNewTransaction
        invoiceTransactionData.transactionEvent = transactionEvent
        const transaction = await transactionService.createTransaction(
          invoiceTransactionData,
          session
        )
        console.log('size(transaction) ', size(transaction))
        if (size(transaction)) {
          transactions.push(transaction)
        }
      }
    }
  }
}

export const prepareInitialDataForInvoiceTransaction = async (
  data,
  invoice,
  transactionEvent
) => {
  data.type = 'invoice'
  if (invoice.invoiceType === 'credit_note') {
    data.type = 'credit_note'
  }
  data.invoiceId = invoice._id
  data.period = await transactionHelper.getFormattedTransactionPeriod(
    invoice.invoiceStartOn,
    invoice.partnerId
  )
  if (transactionEvent === 'legacy') {
    data.createdAt = invoice.createdAt
  }
}

// Add transactions for an invoice
export const addInvoiceTransactions = async (
  invoice,
  transactionEvent,
  session
) => {
  // Prepare initial data for transaction from invoice data
  const transactions = []
  const data = pick(invoice, [
    'partnerId',
    'contractId',
    'agentId',
    'branchId',
    'accountId',
    'propertyId',
    'tenantId',
    'createdBy'
  ])
  await prepareInitialDataForInvoiceTransaction(data, invoice, transactionEvent)
  const params = {
    data,
    invoice,
    transactions,
    transactionEvent
  }
  const {
    invoiceContent = [],
    feesMeta = [],
    addonsMeta = [],
    roundedAmount = 0
  } = invoice
  // Create transactions for each invoice content
  if (size(invoiceContent)) {
    await createTransactionsForInvoiceContents(params, session)
  }
  // Create transactions for each fees meta
  if (size(feesMeta)) {
    await createTransactionsForInvoiceFeesMeta(params, session)
  }
  // Create transactions for each addons meta
  if (size(addonsMeta)) {
    await createTransactionsForInvoiceAddonsMeta(params, session)
  }
  // Add transaction for rounded amount
  if (roundedAmount) {
    await createTransactionForInvoiceRoundedAmount(params, session)
  }
  // Return all newly created transactions
  return flattenDepth(transactions)
}

export const addCorrectionTransactionsByCorrectionIds = async (
  transactionEvent,
  addonsMeta,
  landlordInvoiceId,
  session
) => {
  for (const addon of addonsMeta) {
    const { correctionId } = addon
    const correctionInfo = await correctionHelper.getCorrection(
      { _id: correctionId },
      session
    )
    correctionInfo.landlordInvoiceId = landlordInvoiceId
    await correctionService.addCorrectionTransaction(
      correctionInfo,
      transactionEvent,
      addon,
      session
    )
  }
}

export const addCommissionTransactionsByCommissionsMeta = async (
  commissionsMeta,
  transactionEvent,
  session
) => {
  await Promise.all(
    map(commissionsMeta, async (meta) => {
      const commissionInfo = await commissionHelper.getCommission(
        { _id: meta.commissionId },
        session
      )
      await commissionService.addCommissionTransaction(
        commissionInfo,
        transactionEvent,
        session
      )
    })
  )
}

export const addTransactionForEachFeeMeta = async (params, session) => {
  const {
    feeMeta,
    transactionData,
    invoiceType,
    landlordInvoice,
    transactionEvent
  } = params
  const { type, total, amount, qty } = feeMeta
  const accountingType = invoiceHelper.getAccountingType(type)
  const { partnerId, _id, invoiceStartOn } = landlordInvoice
  const feesTotal = total || amount * qty
  const feesMetaTransactionQuery = {
    partnerId,
    invoiceId: _id,
    type: invoiceType,
    amount: feesTotal,
    subType: accountingType
  }
  const isTransactionExists = await transactionHelper.getTransaction(
    feesMetaTransactionQuery,
    session
  )
  if (isTransactionExists || !feesTotal) {
    return false
  }
  let feeTransactionData =
    await transactionHelper.getAccountingDataForTransaction(
      partnerId,
      accountingType,
      {}
    )
  const transactionPeriod =
    await transactionHelper.getFormattedTransactionPeriod(
      invoiceStartOn,
      partnerId
    )
  if (transactionPeriod) {
    transactionData.period = transactionPeriod
  }
  feeTransactionData = extend(feeTransactionData, clone(transactionData))
  const createdAtParams = { type, landlordInvoice, transactionEvent }
  feeTransactionData.createdAt =
    await invoiceHelper.prepareTransactionCreatedAt(createdAtParams)
  feeTransactionData.amount = feesTotal
  await transactionService.createTransaction(feeTransactionData, session)
}

export const createLandlordInvoiceFeesTransaction = async (
  landLordInvoice,
  transactionEvent,
  session
) => {
  const { feesMeta = [] } = landLordInvoice
  if (!size(feesMeta)) {
    return []
  }
  const { transactionData, type } =
    invoiceHelper.prepareBasicTransactionDataForLandLordInvoice(
      landLordInvoice,
      transactionEvent
    )
  const allowedType = ['invoice', 'reminder', 'collection_notice']
  const promiseAll = []
  for (let i = 0; i < feesMeta.length; i++) {
    const feeMeta = feesMeta[i]
    if (!allowedType.includes(feeMeta.type)) {
      break
    }
    const transaction = await addTransactionForEachFeeMeta(
      {
        feeMeta,
        transactionData,
        invoiceType: type,
        landlordInvoice: landLordInvoice,
        transactionEvent
      },
      session
    )
    promiseAll.push(transaction)
  }
  await Promise.all(promiseAll)
}

export const addLandlordInvoiceTransaction = async (
  landLordInvoice,
  transactionEvent,
  session
) => {
  const {
    commissionsMeta = [],
    addonsMeta = [],
    _id: landlordInvoiceId
  } = landLordInvoice
  if (size(addonsMeta)) {
    await addCorrectionTransactionsByCorrectionIds(
      transactionEvent,
      addonsMeta,
      landlordInvoiceId,
      session
    )
  }
  if (size(commissionsMeta)) {
    await addCommissionTransactionsByCommissionsMeta(
      commissionsMeta,
      transactionEvent,
      session
    )
  }
  await createLandlordInvoiceFeesTransaction(
    landLordInvoice,
    transactionEvent,
    session
  )
}
/* Transactions Ends
 * */

/* Notifications Starts
 * */

export const updateInvoiceAndAddLogForVippsService = async (req) => {
  const { body, session } = req
  const updateInfo =
    await invoiceHelper.updateInvoiceAndLogForFailedVippsHelper(
      {
        invoiceId: body.invoiceId,
        partnerId: body.partnerId,
        vippsEventNote: body.vippsEventNote,
        vippsStatus: body.vippsStatus,
        vippsEventStatus: body.vippsEventStatus,
        action: body.action,
        invoiceSendToVippsData: body.invoiceSendToVippsData
      },
      session
    )
  return updateInfo
}

export const sendInvoiceOrDisableNotification = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['contractId', 'invoiceId', 'partnerId'], body)

  const { contractId, invoiceId, partnerId } = body
  if (!(contractId && invoiceId && partnerId))
    throw new CustomError(
      400,
      'Invalid input data for sending invoice or disable notification'
    )

  const query = { _id: invoiceId, partnerId, contractId }
  console.log(
    '=====> Checking invoice query for sending invoice or disable notification, query:',
    query,
    '<====='
  )

  const invoice = await invoiceHelper.getInvoice(query, session, [
    { path: 'partner', populate: ['partnerSetting'] }
  ])
  if (!size(invoice)) {
    throw new CustomError(
      404,
      'Could not find invoice for sending invoice or disable notification'
    )
  }

  const {
    enabledNotification,
    invoiceSent,
    invoiceType,
    isFinalSettlement,
    isPayable,
    partner
  } = invoice || {}
  const { partnerSetting } = partner || {}

  let isSendNotification = true
  if (size(partnerSetting?.partnerId)) {
    if (invoiceType === 'credit_note') {
      isSendNotification = partnerSetting.isSendCreditNoteNotification()
    } else if (invoiceType === 'landlord_invoice' && isFinalSettlement) {
      isSendNotification = partnerSetting.isSendFinalSettlementNotification()
    }
  }

  if (!invoiceSent && !isSendNotification) {
    console.log(
      '=====> Disabling invoice notification because of:',
      { invoiceId, invoiceSent, isSendNotification },
      '<====='
    )
    return await updateInvoice(
      query,
      { $set: { disabledPartnerNotification: true } },
      session
    )
  }

  const isEnabledNotificationForInvoice =
    enabledNotification !== false || // In invoice enableNotification can be undefined that's why checking enableNotification not equal to false
    (invoiceType === 'landlord_invoice' && isPayable)

  console.log(
    '=====> Checking invoice sending conditions:',
    {
      invoiceId,
      invoiceSent,
      isEnabledNotificationForInvoice,
      isSendNotification
    },
    '<====='
  )

  if (!invoiceSent && isSendNotification && isEnabledNotificationForInvoice) {
    console.log(
      '=====> Conditions passed for creating invoice notification app queue:',
      invoiceId,
      '<====='
    )
    await insertInQueueForSendingInvoiceNotification(invoice, session)
  }

  return invoice
}

export const addFileIdInInvoicePdf = async (params, session) => {
  const { invoiceId, pdfType, isAppInvoice, fileId } = params
  const invoiceInfo = await invoiceHelper.getInvoiceInfoForFileId(
    invoiceId,
    isAppInvoice,
    session
  )
  if (!(size(invoiceInfo) && fileId && pdfType)) {
    return null
  }
  const attachmentTypes = fileHelper.getAttachmentPdfTypes()
  const pdfQuery = {
    type: pdfType,
    fileId: { $exists: true, $nin: ['', null, false, 0] }
  }
  if (indexOf(attachmentTypes, pdfType) !== -1) {
    pdfQuery.fileId = fileId
  }
  const invoiceUpdateQuery = {
    _id: invoiceId,
    pdf: { $not: { $elemMatch: pdfQuery } }
  }
  const updateData = { $push: { pdf: { type: pdfType, fileId } } }
  if (isAppInvoice) {
    const updated = await appInvoiceService.updateAppInvoice(
      invoiceUpdateQuery,
      updateData,
      session
    )
    return updated
  }
  // if isAppInvoice is false
  const updatedInvoice = await updateInvoice(
    invoiceUpdateQuery,
    updateData,
    session
  )
  return updatedInvoice
}

const attachmentTypesAlreadyAddedInInvoice = [
  'app_invoice_pdf',
  'credit_note_pdf',
  'invoice_attachment_pdf',
  'landlord_credit_note_pdf',
  'landlord_invoice_attachment_pdf'
]

export const addFileIdsInInvoice = async (notificationLog, session) => {
  const { attachmentsMeta = [], invoiceId = '' } = notificationLog
  if (!(size(attachmentsMeta) && invoiceId)) {
    return false
  }
  const allowedAttachments = fileHelper.getAttachmentPdfTypes()
  const promiseAll = []
  for (const attachment of attachmentsMeta) {
    const { type, fileId } = attachment
    if (
      !attachmentTypesAlreadyAddedInInvoice.includes(type) &&
      allowedAttachments.includes(type) &&
      fileId
    ) {
      const params = { invoiceId, pdfType: type, isAppInvoice: false, fileId }
      promiseAll.push(await addFileIdInInvoicePdf(params, session))
    }
  }
  await Promise.all(promiseAll)
}

const getInvoiceInfoBasedOnInputData = async (
  invoiceIds = [],
  partnerId = '',
  session
) => {
  const [invoiceId] = invoiceIds
  const invoiceInfo = await invoiceHelper.getInvoice(
    { _id: invoiceId, partnerId },
    session
  )
  if (!size(invoiceInfo)) {
    return []
  }
  return invoiceInfo
}

export const addReminderAndCollectionNoticeTransaction = async (
  body,
  session
) => {
  const { invoiceIds = [], partnerId = '', metaType = '' } = body
  const invoiceInfo = await getInvoiceInfoBasedOnInputData(
    invoiceIds,
    partnerId,
    session
  )
  const { feesMeta } = invoiceInfo
  if (!size(feesMeta)) {
    throw new CustomError(
      404,
      `Fees meta not found for invoiceId: ${invoiceIds}`
    )
  }
  const feeMeta =
    find(feesMeta, ({ type, original }) => type === metaType && !original) || {}
  const { type = '', total } = feeMeta || {}
  if (!type) {
    throw new CustomError(
      404,
      `Fee meta type not found for invoiceId: ${invoiceIds}`
    )
  }
  const [invoiceId] = invoiceIds
  const isExistingTransaction = await invoiceHelper.checkExistingTransaction(
    body,
    total,
    session
  )
  if (isExistingTransaction) {
    throw new CustomError(
      405,
      `Transaction already exists in transactions for reminder fee. invoiceId: ${invoiceId}`
    )
  }
  const params = {
    body,
    invoiceInfo,
    findFeeMeta: feeMeta
  }
  const transactionData =
    await invoiceHelper.prepareInvoiceRemindersTransactionData(params, session)
  if (!size(transactionData)) {
    return []
  }
  const addedTransaction = await transactionService.createTransaction(
    transactionData,
    session
  )
  return size(addedTransaction) ? [invoiceId] : []
}

export const revertLostRecognitionTransaction = async (body, session) => {
  const { invoiceIds = [], partnerId = '', lostMeta = {} } = body
  const [invoiceId] = invoiceIds

  const invoiceInfo = await invoiceHelper.getInvoice(
    { _id: invoiceId, partnerId },
    session
  )
  if (!size(invoiceInfo)) {
    throw new CustomError(404, 'Invoice not found for revert lost recognition')
  }
  const { status } = invoiceInfo
  let { amount = 0 } = lostMeta
  amount = amount * -1
  if (status !== 'lost' && amount) {
    const existingLossInvoiceTransaction =
      await transactionHelper.getExistingLossInvoiceTransaction(
        invoiceId,
        partnerId
      )
    if (
      existingLossInvoiceTransaction &&
      existingLossInvoiceTransaction.amount <= 0
    ) {
      throw new CustomError(
        405,
        `Transaction exists for invoiceId: ${invoiceId}`
      )
    }
    lostMeta.amount = amount
    const transactionData =
      await invoiceHelper.prepareLossRecognitionTransactionData(
        invoiceInfo,
        lostMeta
      )
    if (!size(transactionData)) {
      return []
    }
    const addedTransaction = await transactionService.createTransaction(
      transactionData,
      session
    )
    return size(addedTransaction) ? [invoiceId] : []
  }
  return []
}

export const revertInvoiceFeesTransaction = async (body, session) => {
  const {
    invoiceIds = [],
    partnerId = '',
    removedFee = {},
    subType = ''
  } = body
  const [invoiceId] = invoiceIds
  const invoiceInfo = await invoiceHelper.getInvoice(
    { _id: invoiceId, partnerId },
    session
  )
  if (!size(invoiceInfo)) {
    throw new CustomError(404, 'InvoiceInfo not found for revert invoice fee')
  }
  if (!size(removedFee)) {
    throw new CustomError(404, 'RemovedFee not found from app queue params')
  }
  const { total = 0, amount = 0 } = removedFee
  const reminderFee = (total || amount) * -1
  const existingTransactionQuery = {
    partnerId,
    invoiceId,
    amount: reminderFee,
    type: 'invoice',
    subType
  }
  const { _id: existingTransactionId = '' } =
    (await transactionHelper.getTransaction(
      existingTransactionQuery,
      session
    )) || {}
  if (existingTransactionId) {
    throw new CustomError(
      405,
      'transaction already exists. transactionId: ',
      existingTransactionId
    )
  }
  const params = {
    invoiceInfo,
    removedFee,
    reminderFee
  }
  const transactionData =
    await invoiceHelper.prepareTransactionDataForRevertInvoiceFees(
      params,
      session
    )
  if (!size(transactionData)) {
    return []
  }
  const addedTransaction = await transactionService.createTransaction(
    transactionData,
    session
  )
  return size(addedTransaction) ? invoiceIds : ''
}

export const addEvictionNoticeTransaction = async (body, session) => {
  const { invoiceIds = [], partnerId = '', metaType = '' } = body
  const invoiceInfo = await getInvoiceInfoBasedOnInputData(
    invoiceIds,
    partnerId,
    session
  )
  const { feesMeta = [] } = invoiceInfo
  if (!size(feesMeta)) {
    throw new CustomError(404, 'fees meta not found for eviction notice fee')
  }
  const feeMeta = find(feesMeta, ({ type }) => type === metaType) || {}
  if (!size(feeMeta)) {
    throw new CustomError(404, 'fee meta not found')
  }
  const { total } = feeMeta
  const [invoiceId] = invoiceIds
  const isExisting = await invoiceHelper.checkExistingTransaction(
    body,
    total,
    session
  )
  if (isExisting) {
    throw new CustomError(
      405,
      `Transaction already exists for eviction reminder fee. invoiceId: ${invoiceId}`
    )
  }
  const params = {
    body,
    invoiceInfo,
    findFeeMeta: feeMeta
  }
  const transactionData =
    await invoiceHelper.prepareTransactionDataForEvictionNotice(params, session)
  if (!size(transactionData)) {
    return []
  }
  const addedTransaction = await transactionService.createTransaction(
    transactionData,
    session
  )
  return size(addedTransaction) ? [invoiceId] : []
}

const getCollectionInstanceForSerialIdCreation = (collectionNameStr) => {
  if (!collectionNameStr)
    throw new CustomError(400, 'Missing collectionNameStr')

  if (collectionNameStr === 'invoices') return InvoiceCollection
  else if (collectionNameStr === 'payouts') return PayoutCollection
  else if (collectionNameStr === 'commissions') return CommissionCollection
}

const createSerialIdsAndUpdateCounter = async (params, session) => {
  const { collectionNameStr, collectionQuery, counterQuery, limit } = params

  const collectionInstance =
    getCollectionInstanceForSerialIdCreation(collectionNameStr)
  console.log('## collectionQuery', collectionQuery)
  const collectionData = await collectionInstance
    .find(collectionQuery, { _id: 1, partnerId: 1 })
    .sort({ createdAt: 1 })
    .limit(limit)
    .session(session)
  console.log('### collectionData', collectionData)
  console.log(`### Number of ${collectionNameStr}`, size(collectionData))
  // IF no data found, Make the queue status completed
  if (!size(collectionData))
    return { status: 200, isCompleted: true, addDelaySeconds: false }

  const hasMaxCollectionData = size(collectionData) === limit
  const counter = await counterHelper.getACounter(counterQuery, session)
  let next_val = counter?.next_val || 0
  console.log('### Current counter info:', counter?._id, next_val)
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    collectionQuery.partnerId,
    session
  )
  let lastBankReference = partnerSetting?.lastBankReference
  for (let i = 0; i < collectionData.length; i++) {
    console.log('### Number of loop', i)
    // Increment next_val by 1
    next_val++
    // Set SerialId
    let updatingData = {}
    if (collectionNameStr === 'invoices') {
      updatingData = { $set: { invoiceSerialId: next_val } }
    } else if (collectionNameStr === 'payouts') {
      lastBankReference = payoutHelper.getRandomBankReference(lastBankReference)
      updatingData = {
        $set: { serialId: next_val, bankReferenceId: lastBankReference }
      }
    } else if (collectionNameStr === 'commissions') {
      updatingData = { $set: { serialId: next_val } }
    }
    console.log('### Collection updating Data', updatingData)
    const response = await collectionInstance.findOneAndUpdate(
      { _id: collectionData[i]._id },
      updatingData,
      {
        runValidators: true,
        new: true,
        session
      }
    )
    console.log(`### ${collectionNameStr} updated: ${response._id}`)
    if (size(response)) {
      if (collectionNameStr === 'invoices') {
        // Create a new app queue to start next process of invoice creation
        const appQueue = await appQueueService.createAnAppQueue(
          {
            action: 'init_after_process_of_invoice_creation',
            destination: 'invoice',
            event: 'init_after_process_of_invoice_creation',
            params: {
              invoiceId: response._id,
              partnerId: response.partnerId
            },
            priority: 'immediate',
            status: 'new'
          },
          session
        )
        console.log('### created appQueue', appQueue[0]?._id)
      }
    }
  }

  // Update or Create counter with max next_val
  if (counter?._id) {
    const updatedCounter = await counterService.updateACounter(
      { _id: counter._id },
      { $set: { next_val } },
      session
    )
    console.log('### Updated counter', updatedCounter._id, next_val)
  } else {
    const createdCounter = await counterService.createACounter(
      { _id: counterQuery, next_val },
      session
    )
    console.log('### Created counter', createdCounter[0]?._id, next_val)
  }

  if (collectionNameStr === 'payouts') {
    await partnerSettingService.updateAPartnerSetting(
      {
        _id: partnerSetting._id
      },
      { lastBankReference },
      session
    )
  }

  // Lambda will do request for next 100 invoice, until isCompleted = false
  const response = hasMaxCollectionData
    ? { status: 200, isCompleted: false, addDelaySeconds: false }
    : { status: 200, isCompleted: false, addDelaySeconds: true }

  return response
}

export const handleRequestForSerialIdsCreation = async (req) => {
  const { body, session } = req
  invoiceHelper.validateRequiredDataForSerialIdsCreation(body)
  const { limit = 100, params, queueId } = body
  console.log('### queueId', queueId)
  const {
    accountId,
    collectionNameStr,
    isFinalSettlementInvoice,
    partnerId,
    isAccountWiseSerialId = false
  } = params

  const counterQuery =
    collectionNameStr === 'invoices'
      ? isAccountWiseSerialId
        ? { _id: `invoice-start-number-${accountId}` }
        : isFinalSettlementInvoice
        ? { _id: `final-settlement-invoice-${partnerId}` }
        : { _id: partnerId }
      : collectionNameStr === 'payouts'
      ? { _id: `payout-${partnerId}` }
      : collectionNameStr === 'commissions'
      ? { _id: `commission-${partnerId}` }
      : null // Next condition will be here. (collectionNameStr === '**')

  const collectionQuery = { partnerId }

  if (collectionNameStr === 'invoices') {
    if (isAccountWiseSerialId) collectionQuery.accountId = accountId
    collectionQuery.invoiceSerialId = { $exists: false }
    collectionQuery.isFinalSettlement = isFinalSettlementInvoice || {
      $ne: true
    }
  } else if (
    collectionNameStr === 'payouts' ||
    collectionNameStr === 'commissions'
  )
    collectionQuery.serialId = { $exists: false }
  else throw new CustomError(400, 'Invalid collectionNameStr')

  const response = await createSerialIdsAndUpdateCounter(
    { collectionNameStr, collectionQuery, counterQuery, limit },
    session
  )
  return response
}

export const downloadRentOrLandlordInvoice = async (req) => {
  const { body, session, user } = req
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  const invoiceData = JSON.parse(JSON.stringify(body))
  const { downloadType = '' } = body
  if (!size(downloadType))
    throw new CustomError(400, 'Download type is required')
  let prepareInvoiceQuery = []

  if (downloadType === 'landlord_invoice')
    prepareInvoiceQuery = await invoiceHelper.prepareLandlordInvoiceQuery(body)
  else prepareInvoiceQuery = await invoiceHelper.prepareInvoiceQuery(body)

  await appHelper.isMoreOrLessThanTargetRows(
    InvoiceCollection,
    prepareInvoiceQuery,
    {
      moduleName: 'Invoices'
    }
  )

  const {
    accountId,
    agentId,
    branchId,
    createdDateRange,
    compelloStatus,
    dueDateRange,
    invoicePeriod,
    payoutStatus,
    propertyId,
    searchKeyword,
    sort = { createdAt: -1 },
    status,
    tenantId,
    vippsStatus
  } = invoiceData

  appHelper.validateSortForQuery(sort)
  const params = {}
  const userInfo = await userHelper.getUserById(userId)
  const userLanguage = userInfo?.profile?.language
  params.userLanguage = userLanguage

  if (tenantId) {
    appHelper.validateId({ tenantId })
    params.tenantId = tenantId
  }
  if (accountId) {
    appHelper.validateId({ accountId })
    params.accountId = accountId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    params.agentId = agentId
  }
  if (branchId) {
    appHelper.validateId({ branchId })
    params.branchId = branchId
  }
  if (propertyId) {
    appHelper.validateId({ propertyId })
    params.propertyId = propertyId
  }
  if (size(createdDateRange)) {
    params.createdDateRange = {
      startDate: new Date(createdDateRange.startDate),
      endDate: new Date(createdDateRange.endDate)
    }
  }
  if (size(dueDateRange)) {
    params.dueDateRange = {
      startDate: new Date(dueDateRange.startDate),
      endDate: new Date(dueDateRange.endDate)
    }
  }
  if (size(status)) params.status = status
  if (size(payoutStatus)) params.payoutStatus = payoutStatus
  if (size(vippsStatus)) params.vippsStatus = vippsStatus
  if (size(compelloStatus)) params.compelloStatus = compelloStatus
  if (size(invoicePeriod)) {
    params.invoicePeriod = {
      startDate: new Date(invoicePeriod.startDate),
      endDate: new Date(invoicePeriod.endDate)
    }
  }
  if (searchKeyword) params.searchKeyword = searchKeyword

  params.partnerId = partnerId
  params.userId = userId
  params.sort = sort
  params.downloadProcessType =
    downloadType === 'landlord_invoice'
      ? 'download_landlord_invoices'
      : 'download_rent_invoices'
  params.invoiceType = downloadType

  const queueData = {
    action: 'download_email',
    event: 'download_email',
    priority: 'immediate',
    destination: 'excel-manager',
    status: 'new',
    params
  }

  const payoutQueue = await appQueueService.createAnAppQueue(queueData, session)
  if (size(payoutQueue)) {
    return {
      status: 200,
      message:
        'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
    }
  } else {
    throw new CustomError(404, `Unable to download payout`)
  }
}

export const updateInvoiceInfoForVippsService = async (updateInfo, session) => {
  const {
    invoiceId,
    partnerId,
    vippsStatus,
    vippsEventStatus,
    vippsEventNote
  } = updateInfo
  const updateData = {}
  const set = {}
  if (vippsStatus) set.vippsStatus = vippsStatus

  if (size(set)) updateData['$set'] = set

  updateData['$push'] = {
    vippsEvents: {
      status: vippsEventStatus,
      createdAt: new Date(),
      note: vippsEventNote
    }
  }

  if (invoiceId && partnerId && size(updateData)) {
    const invoiceUpdateInfo = await InvoiceCollection.findOneAndUpdate(
      {
        _id: invoiceId
      },
      updateData,
      {
        new: true,
        runValidators: true,
        session
      }
    )
    console.log('invoiceUpdateInfo', invoiceUpdateInfo)
    return invoiceUpdateInfo
  } else {
    return false
  }
}

const updateEvictionInvoicesWithDueReminderNoticeTag = async (
  invoiceInfo = {},
  session
) => {
  const {
    _id: invoiceId,
    contractId,
    evictionDueReminderNoticeSentOn
  } = invoiceInfo || {}
  const evictionInvoicesQuery = {
    _id: { $ne: invoiceId },
    contractId,
    evictionNoticeSent: true,
    evictionNoticeSentOn: { $exists: true },
    evictionDueReminderNoticeSentOn: { $exists: false },
    invoiceType: 'invoice',
    status: { $nin: ['paid', 'credited', 'lost'] }
  }
  const evictionInvoicesUpdatingData = {
    $set: {
      evictionDueReminderSent: true,
      evictionDueReminderNoticeSentOn
    },
    $unset: { evictionNoticeSent: 1, evictionNoticeSentOn: 1 }
  }
  const updatedInvoices = await updateInvoices(
    evictionInvoicesQuery,
    evictionInvoicesUpdatingData,
    session
  )
  return updatedInvoices
}

export const updateAnInvoiceForLambda = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['invoiceId'], body)

  const {
    invoiceId,
    needToCreateInvoiceLog,
    needToUpdateKidNumber,
    needToUpdateInvoiceSummary
  } = body

  const invoiceInfo = await invoiceHelper.getInvoice({ _id: invoiceId }, null, [
    { path: 'partner', populate: ['partnerSetting'] }
  ])
  if (!size(invoiceInfo)) throw new CustomError(404, 'Invoice not found')

  const { contractId, invoiceSerialId } = invoiceInfo || {}
  if (needToUpdateKidNumber) {
    const {
      invoiceType,
      isFinalSettlement,
      isNonRentInvoice,
      partner = {}
    } = invoiceInfo || {}
    const { partnerSetting } = partner || {}
    const isSameKIDNumber = partnerSetting?.sameKIDNumber || false
    const isLandlordInvoice = [
      'landlord_invoice',
      'landlord_credit_note'
    ].includes(invoiceType)
    if (isSameKIDNumber) {
      body.invoiceKidNumber = await invoiceHelper.getKIDNumber({
        contractId,
        invoiceSerialId: null,
        isLandlordInvoice,
        finalSettlement: isFinalSettlement,
        isNonRentInvoice
      })
    } else {
      body.invoiceKidNumber = await invoiceHelper.getKIDNumber({
        contractId,
        invoiceSerialId,
        isLandlordInvoice,
        finalSettlement: isFinalSettlement
      })
    }
  }

  const updateData = await invoiceHelper.prepareDataToUpdateAnInvoiceForLambda(
    body,
    invoiceInfo
  )
  const updatedInvoice = await updateInvoice(
    { _id: invoiceId },
    updateData,
    session
  )

  // Have to set eviction due reminder to other due invoices
  const { evictionDueReminderNoticeSentOn, evictionDueReminderSent } =
    body || {}
  if (
    size(updatedInvoice) &&
    evictionDueReminderNoticeSentOn &&
    evictionDueReminderSent
  ) {
    await updateEvictionInvoicesWithDueReminderNoticeTag(
      updatedInvoice,
      session
    )
  }

  // If invoice total is updated then have to update invoice summary
  if (body?.invoiceTotal) {
    await invoiceSummaryService.updateInvoiceInfoInInvoiceSummary(
      updatedInvoice
    )
  }
  if (needToUpdateInvoiceSummary) {
    await invoiceSummaryService.updateInvoiceSummary(
      { invoiceId },
      { $set: { invoiceSerialId } },
      session
    )
  }
  if (needToCreateInvoiceLog) await createInvoiceLog(updatedInvoice, session)

  const populationArray = [
    {
      path: 'account',
      populate: {
        path: 'person'
      }
    },
    {
      path: 'contract'
    },
    {
      path: 'partner',
      populate: {
        path: 'partnerSetting'
      }
    },
    {
      path: 'tenant',
      populate: {
        path: 'user'
      }
    }
  ]
  const populatedInvoice = await updatedInvoice
    .populate(populationArray)
    .execPopulate()
  const E_INVOICE_TYPE = process.env.E_INVOICE_TYPE || 'vipps'
  populatedInvoice.eInvoiceType = E_INVOICE_TYPE
  return populatedInvoice
}

const prepareAppInvoiceUpdateData = (body) => {
  const { pdf, pdfEvent, status } = body
  const addToSetData = {}
  const setData = {}
  if (size(pdf)) addToSetData.pdf = pdf
  if (size(pdfEvent)) addToSetData.pdfEvents = pdfEvent
  if (status) setData.status = status

  const updateData = {}
  if (size(addToSetData)) updateData['$addToSet'] = addToSetData
  if (size(setData)) updateData['$set'] = setData

  return updateData
}

export const updateAnAppInvoiceForLambda = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['invoiceId'], body)
  const updateData = prepareAppInvoiceUpdateData(body)
  const { invoiceId } = body
  const updatedAppInvoice = await appInvoiceService.updateAppInvoice(
    { _id: invoiceId },
    updateData,
    session
  )
  return updatedAppInvoice
}

export const updateInvoiceStatus = async (
  invoice,
  previousPaidTotal,
  session
) => {
  if (!size(invoice)) return false
  const { _id: invoiceId, partnerId } = invoice
  const updatingData = await invoiceHelper.prepareInvoiceStatusUpdatingData(
    invoice,
    previousPaidTotal
  )

  if (size(updatingData)) {
    await updateInvoice(
      { _id: invoiceId, partnerId },
      { $set: updatingData },
      session
    )
  }
}

const updateInvoiceStatusToOverdue = async (params = {}, session) => {
  const {
    contractId,
    partnerId,
    willBeOverdueInvoiceIds: invoiceIds
  } = params || {}

  const invoices = await invoiceHelper.getInvoices(
    {
      _id: { $in: invoiceIds },
      contractId,
      partnerId,
      status: { $in: ['new', 'created'] }
    },
    session
  )
  if (!size(invoices))
    throw new CustomError(
      404,
      'could_not_find_invoices_for_updating_status_to_overdue'
    )

  const updatedInvoices = []
  for (const invoice of invoices) {
    const { _id: invoiceId } = invoice || {}
    const initialUpdatingData = { setData: { status: 'overdue' } }
    await invoiceHelper.prepareEvictionInfoForInvoice(
      invoice,
      initialUpdatingData
    )
    console.log(
      '====> Checking initial invoice updating data:',
      initialUpdatingData,
      'for invoiceId:',
      invoiceId,
      '<===='
    )
    const invoiceUpdatingData = {}
    if (size(initialUpdatingData?.setData))
      invoiceUpdatingData['$set'] = initialUpdatingData.setData
    if (size(initialUpdatingData?.unsetData))
      invoiceUpdatingData['$unset'] = initialUpdatingData.unsetData

    console.log(
      '====> Checking invoice updating data:',
      size(invoiceUpdatingData) && JSON.stringify(invoiceUpdatingData),
      'for invoiceId:',
      invoiceId,
      '<===='
    )
    const updatedInvoice = await updateInvoice(
      { _id: invoiceId },
      invoiceUpdatingData,
      session
    )

    if (!size(updatedInvoice?._id))
      throw new CustomError(404, 'Could not update invoice status to overdue')

    updatedInvoices.push(updatedInvoice)
  }

  return updatedInvoices
}

const updateInvoiceStatusToPaid = async (params = {}, session) => {
  const {
    contractId,
    partnerId,
    willBePaidInvoiceIds: invoiceIds
  } = params || {}

  const invoices = await invoiceHelper.getInvoices(
    {
      _id: { $in: invoiceIds },
      contractId,
      partnerId,
      status: { $in: ['new', 'created'] }
    },
    session
  )
  if (!size(invoices))
    throw new CustomError(
      404,
      'Could not find invoices for updating status to paid'
    )

  const updatedInvoices = []
  for (const invoice of invoices) {
    const { _id: invoiceId } = invoice || {}
    const initialUpdatingData = { setData: { status: 'paid' } }
    await startProcessForInvoiceAfterGettingPaid(
      invoice,
      initialUpdatingData,
      session
    )
    console.log(
      '====> Checking initial invoice updating data:',
      initialUpdatingData,
      'for invoiceId:',
      invoiceId,
      '<===='
    )
    const invoiceUpdatingData = {}
    if (size(initialUpdatingData?.setData))
      invoiceUpdatingData['$set'] = initialUpdatingData.setData
    if (size(initialUpdatingData?.unsetData))
      invoiceUpdatingData['$unset'] = initialUpdatingData.unsetData

    console.log(
      '====> Checking invoice updating data:',
      size(invoiceUpdatingData) && JSON.stringify(invoiceUpdatingData),
      'for invoiceId:',
      invoiceId,
      '<===='
    )
    const updatedInvoice = await updateInvoice(
      { _id: invoiceId },
      invoiceUpdatingData,
      session
    )

    if (!size(updatedInvoice?._id))
      throw new CustomError(404, 'Could not update invoice status to paid')

    updatedInvoices.push(updatedInvoice)
  }

  return updatedInvoices
}

const updateInvoiceStatusToOverPaid = async (params = {}, session) => {
  const {
    contractId,
    partnerId,
    willBeOverPaidInvoiceIds: invoiceIds
  } = params || {}
  console.log(
    'Start updating invoice status to over paid',
    contractId,
    invoiceIds
  )
  await updateInvoices(
    {
      _id: { $in: invoiceIds },
      contractId,
      partnerId,
      status: { $nin: ['credited', 'lost'] },
      isOverPaid: false
    },
    {
      status: 'paid',
      isPartiallyPaid: false,
      isOverPaid: true
    },
    session
  )

  await appQueueService.createAppQueueForMatchPayment(
    {
      action: 'updated_payment',
      contractId,
      partnerId
    },
    session
  )
  return invoiceIds
}

export const updateInvoiceStatusOrInvoiceTag = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['contractId', 'partnerId'], body)

  const {
    contractId,
    partnerId,
    willBeOverdueInvoiceIds,
    willBeOverPaidInvoiceIds,
    willBePaidInvoiceIds
  } = body

  if (
    !(
      contractId &&
      partnerId &&
      (size(willBeOverdueInvoiceIds) ||
        size(willBeOverPaidInvoiceIds) ||
        size(willBePaidInvoiceIds))
    )
  )
    throw new CustomError(
      400,
      'Invalid input data for updating invoices status'
    )

  if (size(willBeOverdueInvoiceIds)) {
    await updateInvoiceStatusToOverdue(body, session)
  }
  if (size(willBePaidInvoiceIds)) await updateInvoiceStatusToPaid(body, session)
  if (size(willBeOverPaidInvoiceIds)) {
    await updateInvoiceStatusToOverPaid(body, session)
  }

  return true
}

export const startProcessForInvoiceAfterGettingPaid = async (
  invoiceRawData,
  updatingData,
  session
) => {
  if (!size(updatingData?.setData)) return false

  const invoice = JSON.parse(JSON.stringify(invoiceRawData || {}))

  if (!invoice?._id) {
    console.log(
      '====> Could not find invoice data in parameter, invoiceRawData:',
      invoiceRawData,
      '<===='
    )
    return false
  }
  const { setData } = updatingData
  const updatedInvoice = { ...invoice, ...setData }
  const {
    evictionDueReminderSent,
    evictionDueReminderNoticeSentOn,
    evictionNoticeSent,
    evictionNoticeSentOn,
    isDefaulted
  } = updatedInvoice
  console.log('updatedInvoice', updatedInvoice)

  // Adding invoice paidInfo in payout
  if (invoiceHelper.isNotLandlord(updatedInvoice))
    await payoutService.addInvoicePaidInfoInPayout(updatedInvoice, session)
  // Remove defaulted tag from invoice
  if (isDefaulted) {
    updatingData['setData'].isDefaulted = false

    // Remove defaulted tag from contract
    // Change isDefaulted for both invoice and contract, if certain conditions are met.
    await contractService.removeDefaultedTagFromContract(
      { ...updatedInvoice, isDefaulted: false },
      session
    )
  }
  // Update eviction process invoices if there has no invoice with eviction due
  if (evictionDueReminderSent && evictionDueReminderNoticeSentOn) {
    await appQueueService.createAppQueueForProcessingEvictionCase(
      updatedInvoice,
      session
    )
  }
  // Reset other invoices eviction notice sent tag if certain conditions are met
  if (
    evictionNoticeSent &&
    evictionNoticeSentOn &&
    !(evictionDueReminderSent || evictionDueReminderNoticeSentOn)
  ) {
    await resetInvoiceEvictionTag(invoice, session)

    updatingData['unsetData'] = {
      evictionNoticeSent: 1,
      evictionNoticeSentOn: 1
    }
  }

  await payoutService.setInvoicePaidInFinalSettlementPayout(
    updatedInvoice,
    session,
    true
  )
}

export const startAfterProcessForInvoiceTotalPaidChange = async (
  params,
  session
) => {
  const {
    isFromMatchPayment = false,
    newTotalPaid: totalPaid,
    newLastPaymentDate: lastPaymentDate = undefined,
    oldInvoice
  } = params
  console.log(
    `Started invoice total paid updating for invoice id ${oldInvoice?._id}`
  )
  console.log(`New invoice total paid amount is ${totalPaid}`)

  if (!size(oldInvoice)) throw new CustomError(404, 'Invoice not found')

  const { _id: invoiceId } = oldInvoice

  if (totalPaid !== oldInvoice.totalPaid) {
    const isNotALandlordInvoice = invoiceHelper.isNotLandlord(oldInvoice)
    const previousPaidTotal = oldInvoice.totalPaid
    const preInvoice = clone(oldInvoice)
    preInvoice.totalPaid = totalPaid

    const statusUpdatingData =
      await invoiceHelper.prepareInvoiceStatusUpdatingData(
        preInvoice,
        previousPaidTotal
      )

    const updatingData = {
      setData: { ...statusUpdatingData, totalPaid }
    }
    if (lastPaymentDate) updatingData.setData.lastPaymentDate = lastPaymentDate

    console.log('updatingData post', updatingData)

    if (oldInvoice.status !== 'paid' && statusUpdatingData?.status === 'paid') {
      await startProcessForInvoiceAfterGettingPaid(
        preInvoice,
        updatingData,
        session
      )
    } else if (
      isNotALandlordInvoice &&
      oldInvoice.status === 'paid' &&
      statusUpdatingData?.status !== 'paid'
    ) {
      await payoutService.removeInvoicePaidInfoFromPayout(preInvoice, session)
    }

    if (
      oldInvoice.status !== 'credited' &&
      statusUpdatingData?.status === 'credited'
    ) {
      await payoutService.addInvoiceCreditedTagInPayout(invoiceId, session)
    }

    if (
      statusUpdatingData.status === 'overdue' &&
      oldInvoice.status !== 'overdue'
    ) {
      const { contractId, invoiceType, partnerId } = preInvoice
      await invoiceHelper.prepareEvictionInfoForInvoice(
        { contractId, invoiceType, partnerId },
        updatingData
      )
    }
    // Updating invoice
    const data = {}
    const { setData, unsetData } = updatingData
    if (size(setData)) data['$set'] = setData
    if (size(unsetData)) data['$unset'] = unsetData
    console.log('data for invoice update', data)
    if (!size(data))
      throw new CustomError(404, 'Something went wrong while updating payment')

    const updatedInvoice = await updateInvoice(
      { _id: invoiceId },
      data,
      session
    )

    if (!size(updatedInvoice))
      throw new CustomError(404, 'Unable to update invoice')

    console.log(
      `Updated invoice with totalPaid and other information. invoiceId: ${updatedInvoice._id}`
    )

    // Remove or Update eviction case when invoice due amount is changed
    const oldDueAmount = await invoiceHelper.getTotalDueAmount(oldInvoice)
    const currentDueAmount = await invoiceHelper.getTotalDueAmount(
      updatedInvoice
    )

    if (
      updatedInvoice.evictionDueReminderNoticeSentOn &&
      updatedInvoice.evictionDueReminderSent &&
      oldInvoice.evictionDueReminderNoticeSentOn &&
      oldInvoice.evictionDueReminderSent &&
      oldDueAmount !== currentDueAmount
    ) {
      await appQueueService.createAppQueueForProcessingEvictionCase(
        updatedInvoice,
        session
      )
    }

    if (
      !oldInvoice.isOverPaid &&
      updatedInvoice.invoiceType === 'invoice' &&
      updatedInvoice.status === 'paid' &&
      updatedInvoice.isOverPaid &&
      !isFromMatchPayment
    ) {
      await appQueueService.createAppQueueForMatchPayment(
        {
          action: 'updated_payment',
          contractId: updatedInvoice.contractId,
          partnerId: updatedInvoice.partnerId
        },
        session
      )
    }
    if (
      !isNotALandlordInvoice &&
      oldInvoice.status !== 'balanced' &&
      updatedInvoice.status === 'balanced'
    ) {
      console.log('Called addMissingPayoutIdInLandlordInvoiceOrCreditNote')
      await addMissingPayoutIdInLandlordInvoiceOrCreditNote(
        updatedInvoice,
        session
      )
    }
    console.log(
      '====> Checking invoice data for settling final settlement, invoice:',
      {
        invoiceId,
        isFinalSettlement: updatedInvoice.isFinalSettlement,
        isPayable: updatedInvoice.isPayable,
        status: updatedInvoice.status
      },
      '<===='
    )
    if (
      (updatedInvoice.isFinalSettlement || updatedInvoice.isPayable) &&
      updatedInvoice.status === 'paid' &&
      updatedInvoice.status !== oldInvoice.status
    ) {
      console.log(
        '====> Passed final settlement checking for invoiceId:',
        invoiceId,
        '<===='
      )
      await paymentService.checkFinalSettlementProcessAndUpdateContractFinalSettlementStatus(
        updatedInvoice,
        session
      )
    }
  }
}

export const createTransactionAppQueueForLostInvoice = async (
  invoice = {},
  session
) => {
  if (!size(invoice)) return false

  const appQueueData = {
    action: 'add_invoice_lost_regular_transaction',
    destination: 'accounting',
    event: 'add_new_transaction',
    params: {
      invoiceIds: [invoice._id],
      partnerId: invoice.partnerId,
      transactionEvent: 'regular',
      lostMeta: invoice.lostMeta
    },
    priority: 'regular'
  }
  return appQueueService.createAnAppQueue(appQueueData, session)
}

export const updateInvoiceStatusToLost = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['invoiceId'])
  const { body, session } = req
  const getRunningQueues = await appQueueHelper.getAppQueues({
    status: {
      $ne: 'completed'
    },
    'params.invoiceIds': {
      $in: [body.invoiceId]
    },
    'params.partnerId': body.partnerId,
    action: {
      $in: [
        'revert_invoice_lost_regular_transaction',
        'add_invoice_lost_regular_transaction'
      ]
    },
    event: 'add_new_transaction'
  })
  if (size(getRunningQueues)) {
    throw new CustomError(
      400,
      'Revert lost invoice as regular invoice still in processing. Please try after sometime.'
    )
  }
  const invoiceQuery = {
    _id: body.invoiceId,
    partnerId: body.partnerId
  }
  const invoice = await invoiceHelper.getInvoice(invoiceQuery)
  if (!invoice) throw new CustomError(404, 'Invoice not found')
  if (
    invoice.status === 'lost' ||
    invoice.status === 'credited' ||
    !(invoice.isDefaulted || invoice.status === 'overdue')
  ) {
    throw new CustomError(400, 'Could not update invoice status to lost')
  }
  const updateData = await invoiceHelper.prepareInvoiceStatusLostUpdateData(
    body,
    invoice
  )
  const updatedInvoice = await updateInvoice(invoiceQuery, updateData, session)

  // After insert process
  const isNotLandLord = invoiceHelper.isNotLandlord(invoice)
  if (isNotLandLord) {
    await createTransactionAppQueueForLostInvoice(updatedInvoice, session)
    // Invoice Lost info update in payout
    await payoutService.updateInvoiceLostInfoInPayout(updatedInvoice, session)
  }

  if (
    invoice.evictionDueReminderSent &&
    invoice.evictionDueReminderNoticeSentOn
  ) {
    await appQueueService.createAppQueueForProcessingEvictionCase(
      updatedInvoice,
      session
    )
  }
  await createInvoiceLostLog(updatedInvoice, session, body.userId)
  return updatedInvoice
}

export const creditInvoice = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['invoiceId', 'creditReason'])
  const { body, session, user } = req
  console.log('Started credit invoice create', body?.invoiceId)
  appHelper.compactObject(body)
  const { roles } = user
  await invoiceHelper.getNecessaryDataForCreditNote(body)
  await invoiceHelper.validateCreditInvoiceData(body, roles)
  await creditRentInvoice(body, session)
  return {
    result: true
  }
}

export const updateInvoiceStatusWhenTotalPaidOrTotalBalancedChange = async (
  previousInvoice,
  updatedInvoice,
  session
) => {
  const { totalPaid, totalBalanced } = updatedInvoice
  const invoiceUpdateData = {
    setData: {}
  }
  if (
    totalPaid !== previousInvoice.totalPaid ||
    totalBalanced !== previousInvoice.totalBalanced
  ) {
    const invoiceStatusUpdateData =
      await invoiceHelper.prepareInvoiceStatusUpdatingData(
        updatedInvoice,
        previousInvoice.totalPaid
      )
    if (
      updatedInvoice.status !== 'paid' &&
      invoiceStatusUpdateData.status === 'paid'
    ) {
      invoiceUpdateData.setData.status = 'paid'
      await startProcessForInvoiceAfterGettingPaid(
        updatedInvoice,
        invoiceUpdateData,
        session
      )
      if (updatedInvoice.isFinalSettlement || updatedInvoice.isPayable) {
        await paymentService.checkFinalSettlementProcessAndUpdateContractFinalSettlementStatus(
          updatedInvoice,
          session
        )
      }
    }
    if (
      updatedInvoice.status === 'paid' &&
      invoiceStatusUpdateData.status !== 'paid' &&
      invoiceHelper.isNotLandlord(updatedInvoice)
    ) {
      await payoutService.removeInvoicePaidInfoFromPayout(
        updatedInvoice,
        session
      )
    }
    if (
      invoiceHelper.isNotLandlord(updatedInvoice) &&
      updatedInvoice.status !== 'balanced' &&
      invoiceStatusUpdateData.status === 'balanced'
    ) {
      const updateData = await addMissingPayoutIdInLandlordInvoiceOrCreditNote(
        updatedInvoice,
        session,
        true
      )
      invoiceUpdateData.setData = {
        ...invoiceUpdateData.setData,
        ...updateData
      }
    }
    if (
      updatedInvoice.status !== 'overdue' &&
      invoiceStatusUpdateData.status === 'overdue'
    ) {
      await invoiceHelper.prepareEvictionInfoForInvoice(
        updatedInvoice,
        invoiceUpdateData
      )
    }
    //remove or update eviction case when invoice due amount is less then 0
    if (
      previousInvoice.evictionDueReminderSent &&
      previousInvoice.evictionDueReminderNoticeSentOn
    ) {
      const previousDue = await invoiceHelper.getTotalDueAmountOfAnInvoice(
        previousInvoice
      )
      const newDue = await invoiceHelper.getTotalDueAmountOfAnInvoice(
        updatedInvoice
      )
      if (previousDue !== newDue) {
        await appQueueService.createAppQueueForProcessingEvictionCase(
          updatedInvoice,
          session
        )
      }
    }
    invoiceUpdateData.setData = {
      ...invoiceStatusUpdateData,
      ...invoiceUpdateData.setData
    }
    const defaultedInvoice = await invoiceHelper.getInvoice(
      {
        contractId: updatedInvoice.contractId,
        isDefaulted: true
      },
      session
    )
    if (!size(defaultedInvoice)) {
      await contractService.updateContract(
        {
          _id: updatedInvoice.contractId
        },
        {
          $set: { isDefaulted: false }
        },
        session
      )
    }
  }
  return invoiceUpdateData
}

export const initAfterInsertProcessOfCorrectionInvoice = async (
  data = {},
  invoice,
  session
) => {
  const { contract, noContractUpdate, noSerialIdQueue, partner } = data
  let { hold } = data
  await updateInvoiceFeesMeta(invoice, session)
  await setInvoiceIdInCorrection(invoice, session)
  if (!noSerialIdQueue) {
    await appQueueService.createAppQueueForAddingSerialId(
      'invoices',
      invoice,
      session
    )
  }
  await invoiceSummaryService.createInvoiceSummary(invoice, session)
  if (!noContractUpdate) {
    await addInvoiceEndDateInContract(invoice, session)
  }
  if (partner.accountType === 'broker') {
    await appQueueService.createAnAppQueueToAddInvoiceCommissions(
      {
        contractId: contract._id,
        hold,
        invoiceId: invoice._id,
        partnerId: contract.partnerId
      },
      session
    )
    hold = true
    if (invoice.isPendingCorrection) {
      // This is only for final settlement
      await appQueueService.createAnAppQueueToCreateOrAdjustEstimatedPayout(
        {
          contractId: contract._id,
          hold,
          invoiceId: invoice._id,
          isFinalSettlement: false,
          partnerId: contract.partnerId
        },
        session
      )
      // hold = await adjustPendingCorrectionInvoiceOrAddEstimatedPayout(
      //   invoice,
      //   hold,
      //   session
      // )
    } else {
      const params = {
        contractId: contract._id,
        hold,
        invoiceId: invoice._id,
        partnerId: contract.partnerId
      }
      await appQueueService.createAnAppQueueToCreateEstimatedPayout(
        params,
        session
      )
    }
  }
  return invoice
}

export const adjustPendingCorrectionInvoiceOrAddEstimatedPayout = async (
  invoice = {},
  hold,
  session
) => {
  const payout = await payoutHelper.getPayout(
    {
      contractId: invoice.contractId,
      partnerId: invoice.partnerId,
      propertyId: invoice.propertyId,
      status: 'estimated'
    },
    session
  )
  if (size(payout)) {
    const updatePayoutData = {}
    const totalPayoutAmount = payout.amount + invoice.payoutableAmount
    updatePayoutData.estimatedAmount =
      (await appHelper.convertTo2Decimal(totalPayoutAmount)) || 0
    updatePayoutData.amount = updatePayoutData.estimatedAmount
    if (updatePayoutData.amount === 0) {
      if (payout.isFinalSettlement) {
        const payoutMeta = [
          ...payout.meta,
          {
            type: 'rent_invoice',
            amount: invoice.payoutableAmount,
            invoiceId: invoice._id
          }
        ]
        const willBeComplete =
          await payoutHelper.isFinalSettlementPayoutWillBeCompleted(
            {
              ...payout,
              meta: payoutMeta
            },
            session
          )
        if (willBeComplete) updatePayoutData.status = 'completed'
      } else {
        updatePayoutData.status = 'completed'
      }
      if (updatePayoutData.status === 'completed') {
        updatePayoutData.paymentStatus = 'balanced'
        if (size(payout.correctionsIds)) {
          await correctionService.updateCorrections(
            {
              _id: {
                $in: payout.correctionsIds
              }
            },
            {
              $set: {
                status: 'paid',
                payoutId: payout._id
              }
            },
            session
          )
        }
        await payoutService.createLogForUpdatedPayout(
          {
            ...payout,
            ...updatePayoutData
          },
          session,
          {
            context: 'payout',
            partnerId: payout.partnerId,
            collectionId: payout._id
          }
        )
      }
    }
    const payoutUpdateData = {
      $set: updatePayoutData,
      $push: {
        meta: {
          type: 'rent_invoice',
          amount: invoice.payoutableAmount,
          invoiceId: invoice._id
        }
      }
    }
    const updatedPayout = await payoutService.updateAPayout(
      { _id: payout._id },
      payoutUpdateData,
      session
    )
    // After update process starts
    await payoutService.checkPayoutUpdatedDataToUpdateInvoiceSummary(
      payout,
      updatedPayout,
      session
    )
    if (updatedPayout.amount < 0) {
      await payoutService.updateNextEstimatedPayoutInfo(
        {
          payoutData: updatePayoutData,
          payoutMetaType: 'unpaid_expenses_and_commissions'
        },
        session
      )
    }
  } else {
    await appQueueService.createAnAppQueueToCreateEstimatedPayout(
      {
        contractId: invoice.contractId,
        hold,
        invoiceId: invoice._id,
        partnerId: invoice.partnerId
      },
      session
    )
    return true
  }
}

export const addCorrectionInvoice = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['correctionId'])
  const { body = {}, session } = req
  const requiredData =
    await invoiceHelper.getRequiredDataForCreateCorrectionInvoice(body, session)
  const { contract, partnerSetting } = requiredData
  const today = await invoiceHelper.getInvoiceDate(new Date(), partnerSetting)
  const invoiceDataInfo = await invoiceHelper.getBasicInvoiceDataForTenant(
    contract,
    today
  )
  const invoiceData = await createACorrectionInvoice(
    { ...body, ...requiredData, invoiceData: invoiceDataInfo, today },
    session
  )
  return invoiceData
}

export const createACorrectionInvoice = async (data, session) => {
  const invoiceData = await invoiceHelper.prepareCreateCorrectionInvoiceData(
    data,
    session
  )
  if (!size(invoiceData)) {
    throw new CustomError(400, "Couldn't create this invoice")
  }
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
  const invoice = await createAnInvoice(invoiceData, session)
  if (!size(invoice)) {
    throw new CustomError(400, 'Unable to create correction invoice')
  }
  await initAfterInsertProcessOfCorrectionInvoice(
    data,
    invoice.toObject(),
    session
  )
  return invoice
}

export const updateInvoiceDueDelayDate = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['invoiceId'])
  const { body, session } = req
  await invoiceHelper.validateInvoiceDelayDueDate(body)
  const { delayDate, invoiceId, partnerId, previous, userId } = body
  const updateData = delayDate
    ? { $set: { delayDate } }
    : { $unset: { delayDate: 1 } }

  const invoice = await updateInvoice(
    { _id: invoiceId, partnerId },
    updateData,
    session
  )
  await createInvoiceDelayDateLog(
    { createdBy: userId, invoice, previous },
    session
  )
  return invoice
}

export const createLandlordCreditNoteService = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['creditNoteId'], body)
  const { creditNoteId } = body
  const creditNote = await invoiceHelper.getInvoice(
    {
      _id: creditNoteId
    },
    undefined,
    ['contract', 'commissions', 'invoice', 'partnerSetting']
  )
  if (!size(creditNote)) {
    throw new CustomError(404, 'Credit note not found')
  } else if (!size(creditNote.contract)) {
    throw new CustomError(404, 'Credit note contract not found')
  } else if (!size(creditNote.invoice)) {
    throw new CustomError(404, 'Credited invoice not found')
  } else if (!size(creditNote.partnerSetting)) {
    throw new CustomError(404, 'Partner settings not found')
  }
  const params = {
    contract: creditNote.contract,
    creditNote,
    creditCommissions: creditNote.commissions,
    invoice: creditNote.invoice,
    partnerSetting: creditNote.partnerSetting
  }
  await createLandlordCreditNote(params, session)
  return {
    result: true
  }
}

export const createAppQueueForRentInvoice = async () => {}

export const createManualInvoicesService = async (req) => {
  const { user = {} } = req
  const { roles = [] } = user
  if (roles.includes('lambda_manager')) {
    req.user.partnerId = req.body.partnerId
    req.user.userId = req.body.userId
  }
  appHelper.validatePartnerAppRequestData(req, ['propertyId', 'contractId'])
  const { body, session } = req
  const {
    contractId,
    enabledNotification,
    partnerId,
    propertyId,
    preferredRanges = [],
    returnPreview,
    userId
  } = body
  const contract = await contractHelper.getAContract(
    {
      _id: contractId,
      partnerId,
      propertyId,
      hasRentalContract: true,
      rentalMeta: { $exists: true },
      'rentalMeta.status': { $in: ['active', 'upcoming'] }
    },
    undefined,
    ['partner', 'partnerSetting']
  )
  if (
    !size(contract) ||
    !size(contract.partnerSetting) ||
    !size(contract.partner)
  ) {
    throw new CustomError(404, 'Please provide correct lease')
  }
  if (contract.isFinalSettlementDone)
    throw new CustomError(400, 'Final settlement already done for this lease')
  return await createManualInvoices(
    {
      contract,
      enabledNotification,
      preferredRanges,
      returnPreview,
      userId
    },
    session
  )
}

export const createManualInvoices = async (params = {}, session) => {
  const {
    contract,
    enabledNotification,
    preferredRanges,
    returnPreview,
    userId
  } = params
  const { partnerId } = contract
  const partnerSettings = contract.partnerSetting
  const partnerType = contract.partner.accountType
  const invoiceListParams = {
    contract,
    partnerSettings,
    returnEstimatedPayoutPreview: false,
    preferredRanges
  }
  const { missingInvoices } = await invoiceHelper.getCreatableInvoicesByRange(
    invoiceListParams,
    session
  )
  const firstInvoiceCreationDate = await appHelper.subtractDays(
    contract.rentalMeta?.firstInvoiceDueDate,
    partnerSettings.invoiceDueDays,
    partnerSettings
  )
  // We'll only create invoice if today is the firstInvoiceCreationDate or past
  const today = await invoiceHelper.getInvoiceDate(new Date(), partnerSettings)
  const invoiceData = await invoiceHelper.getBasicInvoiceDataForTenant(
    contract,
    today,
    false,
    returnPreview
  )
  if (invoiceHelper.isDatePastOrToday(today, firstInvoiceCreationDate)) {
    const creatableInvoiceParams = {
      contract: JSON.parse(JSON.stringify(contract)),
      invoiceRanges: missingInvoices,
      partnerSettings,
      invoiceData,
      isDemo: false,
      returnPreview,
      enabledNotification,
      ignoreExistingInvoiceChecking: true,
      returnRegularInvoicePreview: false,
      userId
    }
    const invoicesData = await invoiceHelper.getCreateableInvoices(
      creatableInvoiceParams
    )
    let result = invoicesData.result
    const promiseArr = []
    if (!returnPreview && size(result)) {
      const contractUpdateData = invoicesData.contractUpdateData
      // To create non rent invoices
      await createNonRentInvoicesForManualInvoice(
        {
          contract,
          enabledNotification,
          partner: contract.partner,
          partnerSetting: partnerSettings,
          userId
        },
        session
      )
      let singleInvoice = null
      let isFirstInvoiceExist = false
      let hold = false
      for (singleInvoice of result) {
        await invoiceHelper.validateInvoiceDataBeforeCreation(singleInvoice)
        singleInvoice.createdBy = userId
        const options = {
          partnerType,
          hold
        }
        promiseArr.push(
          createManualRentInvoice(singleInvoice, options, session)
        )
        if (singleInvoice.isFirstInvoice) {
          isFirstInvoiceExist = true
        }
        if (partnerType === 'broker') {
          hold = true
        }
      }
      if (size(promiseArr)) {
        result = await Promise.all(promiseArr)
      }
      let contractSetData = {}
      if (isFirstInvoiceExist) {
        await appQueueService.createAnAppQueueToCheckCommissionChanges(
          {
            contractId: contract._id,
            hold,
            partnerId: contract.partnerId
          },
          session
        )
        // For addEstimatedPayoutMeta and send email
        contractSetData = await addEstimatedPayoutMeta({
          creatableInvoiceParams,
          invoiceListParams,
          partnerSettings: JSON.parse(JSON.stringify(partnerSettings))
        })
        if (partnerSettings?.notifications?.nextScheduledPayouts) {
          const firstInvoice = result.find(
            (item) => item.isFirstInvoice === true
          )
          await sendEstimatedPayoutMail(firstInvoice, session)
        }
      }
      // To set last invoice invoiceEndOn in contract collection
      contractSetData['rentalMeta.invoicedAsOn'] = singleInvoice.invoiceEndOn
      contractUpdateData.$set = {
        ...contractUpdateData.$set,
        ...contractSetData
      }
      await contractService.updateContract(
        {
          _id: contract._id
        },
        contractUpdateData,
        session
      )
      await appQueueService.createAppQueueForAddingSerialId(
        'invoices',
        {
          partnerId,
          accountId: contract.accountId
        },
        session
      )
    }
    return result
  }
}

export const sendEstimatedPayoutMail = async (invoiceData, session) => {
  const appQueueData = {
    action: 'send_notification',
    destination: 'notifier',
    event: 'send_next_schedule_payout',
    params: {
      partnerId: invoiceData.partnerId,
      collectionId: invoiceData._id,
      collectionNameStr: 'invoices'
    },
    priority: 'regular'
  }
  await appQueueService.insertInQueue(appQueueData, session)
}

export const addRentInvoices = async (params, session) => {
  const {
    contract,
    contractUpdateData,
    enabledNotification,
    invoicesData,
    partnerSetting,
    userId
  } = params
  // To create non rent invoices
  await createNonRentInvoicesForManualInvoice(
    {
      contract,
      enabledNotification,
      partner: contract.partner,
      partnerSetting,
      userId
    },
    session
  )
  let singleInvoice = null
  let isFirstInvoiceExist = false
  const promiseArr = []
  const partnerType = contract.partner.accountType
  let result = []
  let hold = false
  for (singleInvoice of invoicesData) {
    await invoiceHelper.validateInvoiceDataBeforeCreation(singleInvoice)
    singleInvoice.createdBy = userId
    const options = {
      partnerType,
      hold
    }
    promiseArr.push(createManualRentInvoice(singleInvoice, options, session))
    if (singleInvoice.isFirstInvoice) {
      isFirstInvoiceExist = true
    }
    if (partnerType === 'broker') {
      hold = true
    }
  }
  if (size(promiseArr)) {
    result = await Promise.all(promiseArr)
  }
  let contractSetData = {}
  if (isFirstInvoiceExist) {
    await appQueueService.createAnAppQueueToCheckCommissionChanges(
      {
        contractId: contract._id,
        hold,
        partnerId: contract.partnerId
      },
      session
    )
    // For addEstimatedPayoutMeta and send email
    const creatableInvoiceParams = {
      contract: JSON.parse(JSON.stringify(contract)),
      invoiceData: {},
      isDemo: false
    }
    const invoiceListParams = {
      contract,
      partnerSettings: partnerSetting,
      returnEstimatedPayoutPreview: false
    }
    contractSetData = await addEstimatedPayoutMeta({
      creatableInvoiceParams,
      invoiceListParams,
      partnerSettings: JSON.parse(JSON.stringify(partnerSetting))
    })
    if (partnerSetting?.notifications?.nextScheduledPayouts) {
      const firstInvoice = result.find((item) => item.isFirstInvoice === true)
      await sendEstimatedPayoutMail(firstInvoice, session)
    }
  }
  // To set last invoice invoiceEndOn in contract collection
  contractSetData['rentalMeta.invoicedAsOn'] = singleInvoice.invoiceEndOn
  contractUpdateData.$set = {
    ...contractUpdateData.$set,
    ...contractSetData
  }
  await contractService.updateContract(
    {
      _id: contract._id
    },
    contractUpdateData,
    session
  )
  await appQueueService.createAppQueueForAddingSerialId(
    'invoices',
    {
      partnerId: contract.partnerId,
      accountId: contract.accountId
    },
    session
  )
  return result
}

export const createManualRentInvoice = async (
  invoiceData,
  options = {},
  session
) => {
  const { hold, partnerType } = options
  const invoice = await createAnInvoice(invoiceData, session)
  await setInvoiceIdInCorrection(invoice, session)
  await updateInvoiceFeesMeta(invoice, session)
  await invoiceSummaryService.createInvoiceSummary(
    JSON.parse(JSON.stringify(invoice)),
    session
  )
  if (partnerType === 'broker') {
    await appQueueService.createAnAppQueueToAddInvoiceCommissions(
      {
        adjustmentNotNeeded: true,
        contractId: invoice.contractId,
        invoiceId: invoice._id,
        partnerId: invoice.partnerId,
        hold
      },
      session
    )
    await appQueueService.createAnAppQueueToCreateEstimatedPayout(
      {
        contractId: invoice.contractId,
        invoiceId: invoice._id,
        isFinalSettlement: false,
        partnerId: invoice.partnerId,
        hold: true
      },
      session
    )
  }
  return invoice
}

export const addEstimatedPayoutMeta = async (params = {}, session) => {
  const { creatableInvoiceParams, invoiceListParams, partnerSettings } = params
  partnerSettings.invoiceDueDays = 120
  const { missingInvoices } = await invoiceHelper.getCreatableInvoicesByRange(
    {
      ...invoiceListParams,
      partnerSettings,
      preferredRanges: null,
      returnEstimatedPayoutPreview: true
    },
    session
  )
  let invoiceRanges = sortBy(missingInvoices, ['invoiceStartOn'])
  invoiceRanges = invoiceRanges.slice(0, 3)
  const invoicesData = await invoiceHelper.getCreateableInvoices({
    ...creatableInvoiceParams,
    invoiceRanges,
    partnerSettings,
    returnPreview: true,
    enabledNotification: false,
    ignoreExistingInvoiceChecking: true,
    returnRegularInvoicePreview: true
  })
  const invoices = invoicesData.result
  if (size(invoices)) {
    const estimatedPayoutMeta =
      await invoiceHelper.prepareEstimatedPayoutMetaData({
        invoices,
        contract: invoiceListParams.contract
      })
    return estimatedPayoutMeta
  }
}

export const createNonRentInvoicesForManualInvoice = async (
  params,
  session
) => {
  const { contract, enabledNotification, partner, partnerSetting, userId } =
    params
  const corrections = await invoiceHelper.getNonRentCorrections(
    contract,
    session
  )
  if (size(corrections)) {
    const promiseArr = []
    const today = await invoiceHelper.getInvoiceDate(new Date(), partnerSetting)
    const invoiceData = await invoiceHelper.getBasicInvoiceDataForTenant(
      contract,
      today
    )
    let hold = false
    for (const correction of corrections) {
      const body = {
        contract,
        correction,
        enabledNotification,
        hold,
        invoiceData,
        noContractUpdate: true,
        noSerialIdQueue: true,
        partner,
        partnerSetting,
        today,
        userId
      }
      promiseArr.push(createACorrectionInvoice(body, session))
      hold = true
    }

    if (size(promiseArr)) await Promise.all(promiseArr)
  }
}

export const createRentInvoicesService = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['contractId'], body)
  const { contractId, enabledNotification, today, userId } = body
  const invoices = await createRentInvoices(
    {
      contractId,
      enabledNotification,
      today,
      userId
    },
    session
  )
  return invoices
}

export const createCreditNote = async (req) => {
  const { body, user, session } = req
  appHelper.checkUserId(user.userId)
  appHelper.compactObject(body, true)
  appHelper.checkRequiredFields(['userId', 'partnerId', 'invoiceId'], body)
  await invoiceHelper.getNecessaryDataForCreditNote(body)
  await creditRentInvoice(body, session)
  return {
    result: true
  }
}

export const createCreditNoteInvoices = async (params = {}, session) => {
  const {
    contractId,
    creditWholeInvoice,
    enabledNotification,
    partnerId,
    partnerSetting,
    terminationDate,
    userId
  } = params
  const terminationDateTZ = (
    await appHelper.getActualDate(partnerSetting, true, terminationDate)
  )
    .endOf('day')
    .toDate()
  const refundableInvoices = await invoiceHelper.getRefundableInvoices(
    {
      contractId,
      terminationDate: terminationDateTZ,
      partnerSetting
    },
    session
  )
  const promiseArr = []
  const fullyCreditInvoices = []
  let hold = false
  for (const invoice of refundableInvoices) {
    if (terminationDateTZ < invoice.invoiceStartOn) {
      fullyCreditInvoices.push(invoice._id)
    }
    promiseArr.push(
      appQueueService.createAppQueueForCreateCreditNote(
        {
          contractId,
          notUpdateDefaultedContract: true,
          enabledNotification,
          hold,
          invoiceId: invoice._id,
          partnerId,
          terminationDate,
          userId
        },
        session
      )
    )
    hold = true
  }
  if (size(promiseArr)) {
    await Promise.all(promiseArr)
  }
  // To set isDefaulted false in contract
  let updateDefaultedContract = false
  if (creditWholeInvoice) {
    updateDefaultedContract = true
  } else if (size(fullyCreditInvoices)) {
    const defaultedInvoice = await invoiceHelper.getInvoice(
      {
        _id: {
          $nin: fullyCreditInvoices
        },
        contractId,
        isDefaulted: true
      },
      session
    )
    if (!size(defaultedInvoice)) {
      updateDefaultedContract = true
    }
  }
  if (updateDefaultedContract) {
    await contractService.updateContract(
      {
        _id: contractId,
        isDefaulted: true
      },
      { $set: { isDefaulted: false } },
      session
    )
  }
}

export const removeLossRecognition = async (req = {}) => {
  await appHelper.validatePartnerAppRequestData(req, ['invoiceId'])
  const { body = {}, session } = req
  const { invoiceId, partnerId, userId } = body

  const getRunningQueues = await appQueueHelper.getAppQueues({
    status: {
      $ne: 'completed'
    },
    'params.invoiceIds': {
      $in: [invoiceId]
    },
    'params.partnerId': partnerId,
    event: 'add_new_transaction',
    action: {
      $in: [
        'revert_invoice_lost_regular_transaction',
        'add_invoice_lost_regular_transaction'
      ]
    }
  })
  if (size(getRunningQueues)) {
    throw new CustomError(
      400,
      'Mark invoice as lost still in processing. Please try after sometime.'
    )
  }

  const invoiceInfo = await invoiceHelper.getInvoice(
    {
      _id: invoiceId,
      partnerId
    },
    undefined,
    ['contract', 'partner']
  )

  if (
    !size(invoiceInfo) ||
    !size(invoiceInfo.partner) ||
    !size(invoiceInfo.contract)
  ) {
    throw new CustomError(404, 'Invoice not found')
  }
  if (!(invoiceInfo.status === 'lost' && size(invoiceInfo.lostMeta))) {
    throw new CustomError(400, 'Please provide valid invoice')
  }
  const status =
    invoiceInfo?.invoiceTotal > invoiceInfo?.totalPaid ? 'overdue' : 'paid'
  const updatingData = {
    setData: {
      isDefaulted: true,
      status
    }
  }
  if (status === 'paid') {
    await startProcessForInvoiceAfterGettingPaid(
      invoiceInfo,
      updatingData,
      session
    )
    if (invoiceInfo.isFinalSettlement || invoiceInfo.isPayable) {
      const contractQuery =
        await contractHelper.getContractQueryForFinalSettlement({
          contractId: invoiceInfo.contractId,
          isManualFinalSettlement: !!invoiceInfo?.contract?.terminatedByUserId,
          partnerId
        })
      const contractInfo = await contractHelper.getAContract(contractQuery)
      await finalSettlementService.checkProcessAndChangeFinalSettlementStatusToCompleted(
        contractInfo,
        session
      )
    }
  } else {
    await invoiceHelper.prepareEvictionInfoForInvoice(invoiceInfo, updatingData)
  }
  if (invoiceHelper.isNotLandlord(invoiceInfo)) {
    if (invoiceInfo?.partner?.enableTransactions) {
      await createAppQueueToRevertLossRecognition(
        {
          invoiceId: invoiceInfo._id,
          partnerId,
          lostMeta: invoiceInfo.lostMeta
        },
        session
      )
    } else {
      console.log(
        '+++ Transaction is not enabled for partner, invoiceId:',
        invoiceInfo._id,
        ', partnerId:',
        invoiceInfo.partnerId,
        '+++'
      )
    }
  }
  await createRemoveLossRecognitionLog(
    {
      invoice: invoiceInfo,
      userId
    },
    session
  )
  const updatedInvoice = await updateInvoice(
    { _id: invoiceId, partnerId },
    {
      $set: updatingData.setData,
      $unset: {
        ...updatingData.unsetData,
        lostMeta: 1
      }
    },
    session
  )

  return {
    ...updatedInvoice,
    isCollectionNoticeSent: updatedInvoice?.collectionNoticeSentAt
      ? true
      : false,
    isFinalSettlementDone: invoiceInfo?.contract?.isFinalSettlementDone
  }
}

const createAppQueueToRevertLossRecognition = async (params = {}, session) => {
  const { invoiceId, partnerId, lostMeta } = params
  const appQueueData = {
    action: 'revert_invoice_lost_regular_transaction',
    destination: 'accounting',
    event: 'add_new_transaction',
    params: {
      invoiceIds: [invoiceId],
      partnerId,
      transactionEvent: 'regular',
      lostMeta
    },
    priority: 'regular'
  }
  await appQueueService.insertInQueue(appQueueData, session)
}

const createRemoveLossRecognitionLog = async (params = {}, session) => {
  const { invoice, userId } = params
  const {
    _id,
    accountId,
    contractId,
    invoiceSerialId,
    lostMeta,
    partnerId,
    propertyId,
    tenantId
  } = invoice
  const logData = {
    accountId,
    action: 'removed_lost_invoice',
    context: 'invoice',
    contractId,
    createdBy: userId,
    invoiceId: _id,
    invoiceSerialId,
    meta: [
      {
        field: 'amount',
        value: lostMeta?.amount || 0
      }
    ],
    partnerId,
    propertyId,
    tenantId,
    visibility: ['invoice', 'account', 'property', 'tenant']
  }
  await logService.createLog(logData, session)
}

export const payoutAfterCompleteProcessForFinalStatement = async (
  updatedPayout = {},
  session
) => {
  if (!size(updatedPayout)) return false

  await payoutService.createLogForUpdatedPayout(updatedPayout, session, {
    action: 'updated_payout',
    context: 'payout'
  })

  const { _id: payoutId, partnerId } = updatedPayout

  // When payout status is completed then summary will be paid
  await invoiceSummaryService.updateInvoiceSummary(
    { payoutId, partnerId },
    { $set: { isPaid: true } },
    session
  )

  // When payout status is completed then expense status will be paid
  if (size(updatedPayout.correctionIds)) {
    await correctionService.updateCorrections(
      {
        _id: { $in: updatedPayout.correctionIds }
      },
      {
        $set: {
          status: 'paid',
          payoutId
        }
      },
      session
    )
  }
  return true
}

export const createLandlordInvoiceAndAfterHooksProcessForFinalSettlement =
  async (data = {}, session) => {
    const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(
      data,
      session
    )
    await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
    let invoice = await createAnInvoice(invoiceData, session)
    if (!size(invoice)) return {}

    await appQueueService.createAppQueueForAddingSerialId(
      'invoices',
      invoice,
      session
    )
    await updateInvoiceFeesMeta(invoice, session)
    await setLandlordInvoiceIdInInvoices(invoice, session)
    await setLandlordInvoiceIdInCorrections(invoice, session)

    const { partnerSetting } = data
    const { landlordInvoice } =
      await payoutService.addLinkBetweenLandlordInvoiceAndPayouts(
        invoice.toObject(),
        partnerSetting,
        session
      )

    invoice = landlordInvoice
    if (invoice.invoiceTotal < 0) {
      await payoutService.adjustBetweenPayoutsAndLandlordInvoices(
        {
          contractId: invoice.contractId,
          isFinalSettlement: true,
          partnerId: invoice.partnerId,
          partnerSetting,
          propertyId: invoice.propertyId
        },
        session
      )
    }
    return invoice
  }

export const createLandlordInvoiceForExtraPayout = async (req) => {
  const { body = {}, session } = req
  appHelper.compactObject(body, true)
  appHelper.checkRequiredFields(
    ['contractId', 'partnerId', 'propertyId', 'userId'],
    body
  )
  const preparedData =
    await invoiceHelper.prepareCreateLandlordInvoiceForExtraPayoutData(body)
  const { contractId, partnerId, userId } = body

  if (size(preparedData)) {
    const {
      requiredData,
      payoutInfo = {},
      finalSettlementAmount
    } = preparedData
    const invoice =
      await createLandlordInvoiceAndAfterHooksProcessForFinalSettlement(
        requiredData,
        session
      )

    if (size(invoice)) {
      const payoutMetaInfo = payoutInfo.meta || []
      const updateData = {
        $set: {
          meta: [
            ...payoutMetaInfo,
            {
              type: 'final_settlement_invoiced',
              landlordInvoiceId: invoice._id,
              amount: finalSettlementAmount
            }
          ],
          amount: 0,
          status: 'completed'
        }
      }

      const updatedPayout = await payoutService.updateAPayout(
        { _id: payoutInfo._id, partnerId },
        updateData,
        session
      )

      if (updatedPayout?._id && payoutInfo?.status !== 'completed') {
        await payoutAfterCompleteProcessForFinalStatement(
          updatedPayout,
          session
        )
      }
    }
  }

  await appQueueService.insertInQueue(
    {
      action: 'generate_refund_payment_on_lease_termination',
      destination: 'lease',
      event: 'generate_final_settlement',
      params: {
        contractId,
        userId
      },
      priority: 'immediate'
    },
    session
  )

  return {
    code: 200,
    msg: 'Create landlord Invoice for extra payout done'
  }
}

export const createLandlordInvoiceOrCreditNote = async (req) => {
  const { body = {}, session } = req
  appHelper.compactObject(body, true)
  appHelper.checkRequiredFields(
    ['correctionId', 'invoiceId', 'partnerId'],
    body
  )
  console.log(
    'Start creating landlord credit invoice or credit note, body:',
    body
  )
  await invoiceHelper.getNecessaryDataForCreateLandlordInvoiceOrCreditNote(body)
  const preparedData =
    await invoiceHelper.prepareCreateLandlordInvoiceOrNoteData(body)
  await createLandlordCreditInvoiceOrCreditNote(preparedData, session)

  return {
    code: 201,
    msg: 'Successfully Created landlord Invoice or credit note'
  }
}

export const createLandlordCreditInvoiceOrCreditNote = async (
  data = {},
  session
) => {
  const invoiceData = await invoiceHelper.processInvoiceDataBeforeCreation(
    data,
    session
  )
  await invoiceHelper.validateInvoiceDataBeforeCreation(invoiceData)
  let invoice = await createAnInvoice(invoiceData, session)
  if (!size(invoice)) {
    throw new CustomError(
      400,
      'Unable to create landlord creditNote or landlord invoice'
    )
  }

  await appQueueService.createAppQueueForAddingSerialId(
    'invoices',
    invoice,
    session
  )
  await updateInvoiceFeesMeta(invoice, session)
  await setLandlordInvoiceIdInInvoices(invoice, session)
  await setLandlordInvoiceIdInCorrections(invoice, session)

  const { partnerSetting } = data
  const { landlordInvoice } =
    await payoutService.addLinkBetweenLandlordInvoiceAndPayouts(
      invoice.toObject(),
      partnerSetting,
      session
    )
  invoice = landlordInvoice

  if (
    invoice.invoiceType === 'landlord_invoice' &&
    size(invoice.creditNoteIds)
  ) {
    await updateInvoices(
      { _id: { $in: invoice.creditNoteIds } },
      {
        $set: {
          isCreditedForCancelledCorrection: true,
          fullyCredited: true,
          invoiceId: invoice._id
        }
      },
      session
    )
  }

  if (invoice.invoiceTotal < 0) {
    await payoutService.adjustBetweenPayoutsAndLandlordInvoices(data, session)
  }

  if (invoice.invoiceType === 'landlord_credit_note') {
    invoice = await updateCreditedInvoice(
      {
        creditNote: invoice,
        partnerSetting
      },
      session
    )
  }
  return invoice
}

export const removeInvoiceFees = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['invoiceId', 'removeType'])
  const { body = {}, session } = req
  const { invoiceId, partnerId, userId } = body
  const { logAction, moveToType, transactionSubType, type } =
    await invoiceHelper.prepareDataForRemoveInvoiceFees(body)

  const invoiceInfo = await invoiceHelper.getInvoice(
    {
      _id: invoiceId,
      partnerId
    },
    session,
    ['contract']
  )
  if (!invoiceInfo) throw new CustomError(400, 'Invoice not found')

  const { invoiceTotal = 0, feesMeta = [] } = invoiceInfo
  const removeFeesMeta = feesMeta.find((fee) => fee.type === type)
  if (!removeFeesMeta)
    throw new CustomError(400, 'Remove invoice fees type not found')
  const movedFee = feesMeta.find((fee) => fee.type === moveToType)
  if (movedFee) throw new CustomError(400, 'Invoice fee already moved')

  const newInvoiceTotal = invoiceTotal - removeFeesMeta.total

  let updatedInvoice = await updateInvoice(
    {
      _id: invoiceId,
      partnerId,
      status: { $nin: ['lost', 'credited', 'balanced', 'cancelled'] },
      $or: [
        { invoiceType: 'invoice' },
        { invoiceType: 'landlord_invoice', isFinalSettlement: true }
      ]
    },
    {
      $set: { invoiceTotal: newInvoiceTotal },
      $pull: { feesMeta: { type } }
    },
    session
  )
  if (!updatedInvoice)
    throw new CustomError(400, 'Failed to remove invoice fees')
  if (updatedInvoice) {
    const paramData = {
      userId,
      invoiceInfo,
      logAction,
      removeFeesMeta,
      transactionSubType
    }
    await createAnAppQueueForRemoveInvoiceFees(paramData, session)
    await createLogForRemoveInvoiceFees(paramData, session)
    if (updatedInvoice.invoiceType === 'invoice') {
      if (invoiceInfo.invoiceTotal !== updatedInvoice.invoiceTotal) {
        await invoiceSummaryService.updateInvoiceInfoInInvoiceSummary(
          updatedInvoice,
          session
        )
        const invoiceStatusUpdateData =
          invoiceHelper.calculateInvoiceStatusBaseOnTotalPaid(updatedInvoice)
        const updatingData = {
          setData: invoiceStatusUpdateData
        }
        if (
          invoiceStatusUpdateData.status === 'paid' &&
          invoiceInfo.status !== 'paid'
        ) {
          await startProcessForInvoiceAfterGettingPaid(
            updatedInvoice,
            updatingData,
            session
          )
        }
        if (size(updatingData.setData) || size(updatingData.unsetData)) {
          updatedInvoice = await updateInvoice(
            {
              _id: invoiceId
            },
            {
              $set: updatingData.setData,
              $unset: updatingData.unsetData
            },
            session
          )
        }
        if (
          (updatedInvoice.isFinalSettlement || updatedInvoice.isPayable) &&
          updatedInvoice.status === 'paid' &&
          invoiceInfo.status !== 'paid'
        ) {
          await finalSettlementService.checkProcessAndChangeFinalSettlementStatusToCompleted(
            invoiceInfo.contract,
            session
          )
        }
        //remove or update eviction case when invoice due amount is less then 0
        if (
          invoiceInfo.evictionDueReminderSent &&
          invoiceInfo.evictionDueReminderNoticeSentOn &&
          updatedInvoice.evictionDueReminderSent &&
          updatedInvoice.evictionDueReminderNoticeSentOn
        ) {
          const previousDue = await invoiceHelper.getTotalDueAmountOfAnInvoice(
            invoiceInfo
          )
          const newDue = await invoiceHelper.getTotalDueAmountOfAnInvoice(
            updatedInvoice
          )
          if (previousDue !== newDue) {
            await appQueueService.createAppQueueForProcessingEvictionCase(
              updatedInvoice,
              session
            )
          }
        }
      }
    }
  }
  return updatedInvoice
}

export const createAnAppQueueForRemoveInvoiceFees = async (
  paramData,
  session
) => {
  const { invoiceInfo, removeFeesMeta, transactionSubType } = paramData
  const isTransactionEnabledForPartner = await partnerHelper.getAPartner({
    _id: invoiceInfo.partnerId,
    enableTransactions: true
  })
  if (!isTransactionEnabledForPartner)
    throw new CustomError(400, 'Transaction is not enabled for partner')
  const appQueueData = {
    action: 'revert_invoice_fees_regular_transaction',
    event: 'add_new_transaction',
    destination: 'accounting',
    status: 'new',
    params: {
      invoiceIds: [invoiceInfo._id],
      partnerId: invoiceInfo.partnerId,
      transactionEvent: 'regular',
      removedFee: removeFeesMeta ? removeFeesMeta : undefined,
      subType: transactionSubType ? transactionSubType : undefined
    },
    priority: 'regular'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
}

const createLogForRemoveInvoiceFees = async (paramData, session) => {
  const { invoiceInfo, logAction, removeFeesMeta, userId } = paramData
  const {
    accountId,
    contractId,
    partnerId,
    propertyId,
    tenantId,
    invoiceSerialId
  } = invoiceInfo

  const logData = {
    action: logAction,
    partnerId,
    context: 'invoice',
    createdBy: userId,
    invoiceId: invoiceInfo._id
  }
  if (accountId) logData.accountId = accountId
  if (contractId) logData.contractId = contractId
  if (propertyId) logData.propertyId = propertyId
  if (tenantId) logData.tenantId = tenantId

  const metaData = []
  if (removeFeesMeta.amount)
    metaData.push({ field: 'amount', value: removeFeesMeta.amount })
  if (invoiceSerialId)
    metaData.push({
      field: 'invoiceSerialId',
      value: invoiceSerialId
    })
  if (size(metaData)) logData.meta = metaData

  logData.visibility = ['invoice', 'account', 'property', 'tenant']
  await logService.createLog(logData, session)
}

const prepareAndCreateAppQueuesForMissingInvoiceSerialIds = async (
  queueParams = [],
  session
) => {
  const appQueuesData = []
  for (let i = 0; i < size(queueParams); i++) {
    const {
      accountId,
      isAccountWiseSerialId,
      isFinalSettlementInvoice,
      partnerId
    } = queueParams[i] || {}

    if (partnerId) {
      const collectionNameStr = 'invoices'
      const sequentialCategoryId = accountId ? accountId : partnerId

      const params = { collectionNameStr, partnerId }
      if (accountId && isAccountWiseSerialId) {
        params.accountId = accountId
        params.isAccountWiseSerialId = isAccountWiseSerialId
      }
      if (isFinalSettlementInvoice) {
        params.isFinalSettlementInvoice = isFinalSettlementInvoice
      }

      appQueuesData.push({
        action: 'add_serialIds',
        destination: 'invoice',
        event: 'add_serialIds',
        isSequential: true,
        params,
        priority: 'regular',
        sequentialCategory: `add_${collectionNameStr}_serial_ids_${sequentialCategoryId}`
      })
    } else {
      console.log(
        '+++ Could not find required data to create queue for adding missing invoice serial Ids for:',
        queueParams[i],
        '+++'
      )
      continue
    }
  }

  const createdAppQueues = await appQueueService.createMultipleAppQueues(
    appQueuesData,
    session
  )
  if (size(appQueuesData) !== size(createdAppQueues)) {
    throw new CustomError(405, 'Something went wrong when creating app queues')
  }

  return size(createdAppQueues)
}

export const createQForAddingMissingInvoiceSerialIds = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { query = {}, options = {} } = body

  const queueParams =
    (await invoiceHelper.getMissingInvoiceSerialIdsQueueParams(
      query || {},
      options || {}
    )) || []
  if (!size(queueParams)) {
    console.log('====> No missing invoice serial IDs found <====')
    return 0
  }

  return prepareAndCreateAppQueuesForMissingInvoiceSerialIds(
    queueParams,
    session
  )
}

export const updateInvoiceForCompelloEInvoice = async (req) => {
  const { body = {}, session } = req
  appHelper.checkRequiredFields(
    ['compelloStatus', 'invoiceId', 'partnerId'],
    body
  )
  const invoice = await invoiceHelper.getInvoice({
    _id: body.invoiceId,
    partnerId: body.partnerId
  })
  if (!size(invoice)) {
    throw new CustomError(404, 'Invoice not found')
  }

  const {
    compelloStatus,
    compelloEventStatus,
    compelloEventNote,
    invoiceId,
    partnerId
  } = body

  const updateData = {
    $push: {
      compelloEvents: {
        status: compelloEventStatus,
        createdAt: new Date(),
        note: compelloEventNote
      }
    }
  }
  if (compelloStatus) updateData.$set = { compelloStatus }

  const updatedInvoice = await updateInvoice(
    {
      _id: invoiceId,
      partnerId
    },
    updateData,
    session
  )
  if (!updatedInvoice) {
    throw new CustomError(400, 'Unable to update invoice')
  }

  if (body.hasCreatableLog) {
    const options = {
      action: body.action || 'invoice_sent_to_compello_error',
      context: 'invoice',
      errorTextKey: body.errorTextKey
    }
    const log = await createInvoiceLogForCompello(invoice, options, session)
    if (!log) throw new CustomError(400, 'Unable to create log data')
  }
  return {
    code: 200,
    msg: 'Successfully updated invoice'
  }
}

const compelloInvoiceErrorsTextKeys = [
  'invoice_account_number',
  'invoice_has_not_amount',
  'invoice_kid_number',
  'invoicer_address_not_found',
  'invoice_address_not_found',
  'invoicer_org_id_not_found',
  'invoice_data_not_found',
  'invoice_pdf_not_found',
  'phone_or_nid_not_found',
  // For B2B
  'customer_address_not_found',
  'supplier_address_not_found'
]

export const createInvoiceLogForCompello = async (
  invoice = {},
  options = {},
  session
) => {
  if (includes(compelloInvoiceErrorsTextKeys, invoice?.errorTextKey)) {
    options.errorTextKey = invoice.errorTextKey
  } else options.errorText = invoice.errorTextKey

  const invoiceLogData = logHelper.prepareCompelloInvoiceLogData(
    invoice,
    options
  )

  if (!size(invoiceLogData)) {
    throw new CustomError(
      400,
      `Could not prepare log data for invoiceId: ${invoice._id}`
    )
  }

  const invoiceLog = await logService.createLog(invoiceLogData, session)
  return invoiceLog
}
