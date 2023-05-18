import { map, pick, size, uniq } from 'lodash'
import { InvoiceCollection, TransactionCollection } from '../models'
import {
  accountHelper,
  appHelper,
  invoiceHelper,
  partnerHelper,
  transactionHelper,
  commissionHelper,
  correctionHelper,
  paymentHelper,
  payoutHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  correctionService,
  invoiceService,
  commissionService,
  paymentService,
  payoutService
} from '../services'
import { CustomError } from '../common'

/*
 * TransactionService.createTransaction method is directly modifying data object using various set methods of transactionHelper.
 * We are passing data & bankInfo object to a set method, and that method is directly modifying / adding property of / to it.
 * */
// IMPORTANT: MAKE SURE YOU PASS PARAM SESSION WHERE NEEDED, WHILE WORKING WITH THIS METHOD
export const createTransaction = async (data, session) => {
  const {
    partnerId,
    agentId,
    accountId,
    tenantId,
    propertyId,
    branchId,
    contractId,
    payoutId,
    correctionId,
    addonId,
    paymentId,
    landlordInvoiceId,
    invoiceId
  } = data
  const bankInfo = {
    bankAccountNumber: '',
    bankAccountNumberForCompanyName: '',
    bankRef: ''
  }
  if (await partnerHelper.isTransactionEnabledOfAPartner(partnerId)) {
    await transactionHelper.setAgentInfo({ agentId, partnerId }, data, session)
    await transactionHelper.setAccountInfo(accountId, data, session)
    await transactionHelper.setAssignmentInfo(contractId, data, session)
    await transactionHelper.setTenantInfo(tenantId, data, session)
    await transactionHelper.setPropertyInfo(propertyId, data, session)
    await transactionHelper.setBranchInfo(branchId, data, session)
    // Need Session for setInvoiceInfo
    await transactionHelper.setInvoiceInfo(invoiceId, data, session)
    // Need Session for setLandlordInvoiceInfo
    await transactionHelper.setLandlordInvoiceInfo(
      landlordInvoiceId,
      data,
      session
    )
    const payoutParams = {
      payoutId,
      partnerId,
      data,
      bankInfo
    }
    // TODO: Please distribute session properly, while working with payout here
    await transactionHelper.setPayoutInfo(payoutParams, session)
    await transactionHelper.setAddonInfo(addonId, data, session)
    await transactionHelper.setPaymentInfo(paymentId, bankInfo, session)
    const correctionParams = {
      correctionId,
      partnerId,
      invoiceId,
      bankInfo,
      data
    }
    // Correction gets an update after the creation of invoice. So correction needs session
    await transactionHelper.setCorrectionInfo(correctionParams, session)
    transactionHelper.setBankInfo(bankInfo, data)
    const companyParams = {
      partnerId,
      invoiceId,
      bankInfo,
      data
    }
    await transactionHelper.setCompanyName(companyParams, session)
    delete data.transactionEvent
    // Insert transaction data into TransactionCollection
    const transaction = await TransactionCollection.create([data], { session })
    return transaction
  }
}

export const addLostInvoiceTransactionByInvoice = async (
  invoice = {},
  transactionEvent,
  session
) => {
  const { lostMeta = {} } = invoice
  const lostAmount = lostMeta.amount
  const lostDate = lostMeta.date
  const { _id: invoiceId, partnerId } = invoice
  const body = { invoiceId, partnerId }
  if (!(invoice && lostAmount)) {
    return false
  }
  const transactions = await transactionHelper.getTransactionsForLostInvoice(
    body,
    session
  )
  if (!size(transactions) || transactions[0].amount === 0) {
    const params = {
      partnerId,
      invoice,
      lostDate,
      lostAmount,
      transactionEvent
    }
    const transactionData =
      await transactionHelper.prepareDataForLossRecognition(params)
    const [transaction] = await createTransaction(transactionData, session)
    return transaction
  }
  throw new CustomError(
    405,
    `Transaction already exists for lost invoice with invoiceId: ${invoiceId}`
  )
}

export const addTransactionForLostInvoice = async (req) => {
  const { body, session } = req
  const { invoiceId, partnerId, contractId, callFromUpgradeScript } = body
  const query = {
    _id: invoiceId,
    partnerId,
    contractId
  }
  const invoice = await invoiceHelper.getInvoice(query, session)
  let { lostMeta } = invoice || {}
  lostMeta = lostMeta || {}
  const lostAmount = lostMeta.amount
  const lostDate = lostMeta.date
  if (invoice && lostAmount) {
    const transactions = await transactionHelper.getTransactionsForLostInvoice(
      body,
      session
    )
    if (!size(transactions) || transactions[0].amount === 0) {
      const params = {
        partnerId,
        invoice,
        lostDate,
        lostAmount,
        callFromUpgradeScript
      }
      const transactionData =
        await transactionHelper.prepareDataForLossRecognition(params, session)
      const [transaction] = await createTransaction(transactionData, session)
      return transaction
    }
    console.log(
      `--- Transaction already exists for lost invoice with invoiceId: ${invoiceId} ---`
    )
  }
}

export const addTransactionForRemoveLossRecognition = async (req) => {
  const { body, session } = req
  const { invoiceId, partnerId, contractId, lostMeta = {} } = body
  const lostAmount = (lostMeta.amount || 0) * -1
  const lostDate = lostMeta.date
  const query = {
    _id: invoiceId,
    partnerId,
    contractId
  }
  const invoice = (await invoiceHelper.getInvoice(query, session)) || {}
  if (invoice.status !== 'lost' && !invoice.lostMeta && lostAmount) {
    const transactions = await transactionHelper.getTransactionsForLostInvoice(
      body,
      session
    )
    if (size(transactions) && transactions[0].amount > 0) {
      const params = {
        partnerId,
        invoice,
        lostDate,
        lostAmount
      }
      const transactionData =
        await transactionHelper.prepareDataForLossRecognition(params, session)
      const [transaction] = await createTransaction(transactionData, session)
      return transaction
    }
    console.log(
      `--- Transaction already exists for removing loss recognition with invoiceId: ${invoiceId} ---`
    )
  }
}

export const removeOldDataTagFromCorrections = async (partnerId, session) => {
  await correctionService.removeOldDataTagFromCorrections(partnerId, session)
}

export const addInvoicesTransactions = async (body, session) => {
  const { invoiceIds = [], partnerId, transactionEvent } = body
  const query = {
    _id: { $in: invoiceIds },
    partnerId,
    invoiceType: { $in: ['invoice', 'credit_note'] }
  }
  const invoices = await invoiceHelper.getInvoices(query, session)
  if (!size(invoices)) {
    throw new CustomError(404, 'Invoice not found')
  }
  const completedIds = []
  for (const invoice of invoices) {
    const invoiceData = invoice.toObject()
    const transactions = await invoiceService.addInvoiceTransactions(
      invoiceData,
      transactionEvent,
      session
    )
    console.log('transactions ', transactions)
    if (size(transactions)) {
      completedIds.push(invoiceData._id)
    }
  }
  return completedIds
}

export const addInvoiceMoveToFeesTransactions = async (body = {}, session) => {
  const {
    feesMeta = [],
    invoiceIds = [],
    partnerId,
    transactionEvent
  } = body || {}
  const [invoiceId] = invoiceIds || []
  if (!(invoiceId && size(feesMeta)))
    throw new CustomError(400, 'Missing required data')

  const query = {
    _id: invoiceId,
    'feesMeta.type': { $in: map(feesMeta, 'type') },
    invoiceType: { $in: ['invoice', 'credit_note'] },
    partnerId
  }
  const invoice = await invoiceHelper.getInvoice(query, session)
  if (!size(invoice)) throw new CustomError(404, 'Could not find invoice')

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
  await invoiceService.prepareInitialDataForInvoiceTransaction(
    data,
    invoice,
    transactionEvent
  )
  const transactions =
    await invoiceService.createTransactionsForInvoiceMoveToFees(
      {
        data,
        feesMeta,
        invoiceId,
        partnerId,
        transactionEvent
      },
      session
    )

  if (size(transactions)) return [invoiceId]
  else return []
}

export const addInvoicesLostTransaction = async (body, session) => {
  const { invoiceIds, partnerId, transactionEvent } = body
  if (!size(invoiceIds)) {
    throw new CustomError(
      400,
      'Invoice ids required to add lost invoices transaction'
    )
  }
  const query = {
    _id: { $in: invoiceIds },
    partnerId,
    status: { $in: ['lost', 'paid', 'credited'] },
    lostMeta: { $exists: true },
    invoiceType: { $in: ['invoice', 'credit_note'] }
  }
  const invoices = await invoiceHelper.getInvoices(query, session)
  if (!size(invoices)) {
    throw new CustomError(404, 'No invoices found for invoice lost transaction')
  }
  const addedInvoiceIds = []
  for (const invoice of invoices) {
    const invoiceData = invoice.toObject()
    await addLostInvoiceTransactionByInvoice(
      invoiceData,
      transactionEvent,
      session
    )
    addedInvoiceIds.push(invoiceData._id)
  }
  return addedInvoiceIds
}

export const addCommissionsTransactions = async (body, session) => {
  const { commissionIds = [], partnerId, transactionEvent } = body
  const query = { _id: { $in: commissionIds }, partnerId }
  const commissions = await commissionHelper.getCommissions(query)
  if (!size(commissions)) {
    return []
  }
  const addedCommissions = []
  for (const commission of commissions) {
    const commissionData = commission.toObject()
    const transaction = await commissionService.addCommissionTransaction(
      commissionData,
      transactionEvent,
      session
    )
    if (size(transaction)) {
      addedCommissions.push(commission._id)
    }
  }
  return addedCommissions
}

export const addCorrectionsTransactions = async (body, session) => {
  const { correctionIds = [], partnerId, transactionEvent } = body
  const query = { _id: { $in: correctionIds }, addTo: 'payout', partnerId }
  const corrections = await correctionHelper.getCorrections(query, session)
  if (!size(corrections)) {
    return []
  }
  let completedCorrectionIds = []
  for (const correction of corrections) {
    const correctionData = correction.toObject()
    const completedIds = await correctionService.addCorrectionTransaction(
      correctionData,
      transactionEvent,
      '',
      session
    )
    completedCorrectionIds = completedCorrectionIds.concat(completedIds)
  }
  return uniq(completedCorrectionIds)
}

export const addPaymentsTransactions = async (body, session) => {
  const {
    partnerId,
    paymentIds,
    transactionEvent,
    paymentAmount,
    previousPaymentData,
    removalPaymentData = {}
  } = body
  // Returns registered payments and completed refund payments
  let allPayments = []
  if (size(removalPaymentData)) {
    allPayments.push(removalPaymentData) // As payment already deleted, take paymentInfo from appQueue
  } else {
    allPayments = await paymentHelper.getPaymentsForTransaction(
      partnerId,
      paymentIds,
      session
    )
  }
  if (!size(allPayments)) {
    throw new CustomError(404, 'No payments found for transaction')
  }
  const completedPaymentIds = []
  for (const payment of allPayments) {
    const params = { payment, paymentAmount, transactionEvent }
    if (transactionEvent === 'legacy') {
      params.callFromUpgradeScript = true
      const { _id: paymentId, invoiceId, propertyId, amount, type } = payment
      const query = {
        partnerId,
        paymentId,
        invoiceId,
        propertyId,
        amount,
        type
      }
      const existsPaymentTransaction =
        !!(await transactionHelper.getTransaction(query, session))
      if (existsPaymentTransaction) {
        break
      }
    }
    if (size(previousPaymentData)) params.payment = previousPaymentData
    else if (size(removalPaymentData)) params.payment = removalPaymentData
    const transaction = await paymentService.addPaymentTransaction(
      params,
      session
    )
    console.log('Checking for completed transaction: ', transaction)
    if (size(transaction)) {
      completedPaymentIds.push(payment._id)
    }
  }
  return completedPaymentIds
}

export const addPayoutsTransactions = async (
  payoutIds,
  partnerId,
  transactionEvent,
  session
) => {
  const payouts = await payoutHelper.getPayoutsForTransaction(
    payoutIds,
    partnerId,
    session
  )
  console.log('payouts ', payouts)
  if (!size(payouts)) {
    throw new CustomError(404, 'No payouts found for transaction')
  }
  const completedPayoutIds = []
  for (const payout of payouts) {
    const addedTransaction = await payoutService.addPayoutTransaction(
      payout,
      transactionEvent,
      session
    )
    if (size(addedTransaction)) {
      completedPayoutIds.push(payout._id)
    }
  }
  return completedPayoutIds
}

export const addLandlordInvoicesTransactions = async (body) => {
  const { invoiceIds, partnerId, transactionEvent } = body
  const query = {
    _id: { $in: invoiceIds },
    partnerId,
    invoiceType: { $in: ['landlord_invoice', 'landlord_credit_note'] }
  }
  const landlordInvoices = await invoiceHelper.getInvoices(query)
  if (!size(landlordInvoices)) {
    throw new CustomError(404, 'No landlord invoices found for transaction')
  }
  const completedInvoiceIds = []
  for (const landLordInvoice of landlordInvoices) {
    await invoiceService.addLandlordInvoiceTransaction(
      landLordInvoice,
      transactionEvent
    )
    completedInvoiceIds.push(landLordInvoice._id)
  }
  return completedInvoiceIds
}

export const initiateLegacyTransactions = async (partnerId) => {
  if (!partnerId) {
    console.log("Can't run initiateLegacyTransactions SQS.")
    return false
  }
  const session = await require('mongoose').startSession()
  session.startTrcompletedansaction()
  try {
    await addInvoicesTransactions(partnerId, session)
    await addInvoicesLostTransaction(partnerId, session)
    await addCommissionsTransactions(partnerId, session)
    await addCorrectionsTransactions(partnerId, session)
    await addPaymentsTransactions(partnerId, session)
    await addPayoutsTransactions(partnerId, session)
    await addLandlordInvoicesTransactions(partnerId, session)
    await session.commitTransaction()
  } catch (err) {
    await session.abortTransaction()
    throw new CustomError(500, err.message)
  } finally {
    session.endSession()
  }
}

const updateTransactions = async (query, data, session) => {
  const result = await TransactionCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  if (result) {
    return result.nModified
  } else {
    return 0
  }
}

const updateTransactionPowerOfficeInfo = async (
  transactionIds,
  voucherNo,
  session
) => {
  if (size(transactionIds) && size(voucherNo)) {
    const query = { _id: { $in: transactionIds } }
    const updateData = { externalEntityId: voucherNo }

    return await updateTransactions(query, { $set: updateData }, session)
  } else {
    return 0
  }
}

export const updatePartnerTransactionsWithValidVoucherNo = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(
    ['partnerId', 'transactionIds', 'voucherNo'],
    body
  )
  const { transactionIds, voucherNo } = body
  const data = await updateTransactionPowerOfficeInfo(
    transactionIds,
    voucherNo,
    session
  )
  return data
}

export const updateTransaction = async (query, data, session) => {
  const response = await TransactionCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  return response
}

export const updateTransactionsForPogo = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const transactionQuery = await transactionHelper.getTransactionsQuery(
    body,
    session
  )
  const updateData =
    transactionHelper.prepareUpdateTransactionsForPogoData(body)
  if (!size(updateData)) {
    throw new CustomError(404, 'No update data found')
  }
  const numUpdated = await updateTransactions(
    transactionQuery,
    updateData,
    session
  )
  if (!numUpdated) {
    throw new CustomError(400, 'Transactions not updated. Something went wrong')
  }
  const updatedTransactions = await transactionHelper.getTransactions(
    transactionQuery,
    session
  )
  const transactionsDataForApi = []
  for (const transactionInfo of updatedTransactions) {
    transactionsDataForApi.push(
      await transactionHelper.createTransactionFieldNameForApi(
        transactionInfo,
        transactionQuery.accountId
      )
    )
  }

  return {
    data: transactionsDataForApi,
    metaData: {
      totalDocuments: updatedTransactions.length
    }
  }
}

export const initRentInvoiceTransaction = async (req) => {
  const { body, session } = req
  const completedTransactions = await addInvoicesTransactions(body, session)
  return { completedTransactions }
}

export const initInvoiceMoveToFeesTransaction = async (req) => {
  const { body, session } = req
  const completedTransactions = await addInvoiceMoveToFeesTransactions(
    body,
    session
  )
  return { completedTransactions }
}

export const initInvoiceLostTransaction = async (req) => {
  const { body, session } = req
  const completedTransactions = await addInvoicesLostTransaction(body, session)
  return { completedTransactions }
}

export const initLandlordInvoiceTransaction = async (req) => {
  const { body } = req
  const completedTransactions = await addLandlordInvoicesTransactions(body)
  return { completedTransactions }
}

export const initCommissionsTransaction = async (req) => {
  const { body, session } = req
  const completedTransactions = await addCommissionsTransactions(body, session)
  return { completedTransactions }
}

export const initCorrectionsTransaction = async (req) => {
  const { body, session } = req
  const completedTransactions = await addCorrectionsTransactions(body, session)
  return { completedTransactions }
}

export const initPayoutsLegacyTransaction = async (req) => {
  const { body, session } = req
  const { payoutIds = [], partnerId, transactionEvent } = body
  console.log('transactionEvent ', transactionEvent)
  const completedTransactions = await addPayoutsTransactions(
    payoutIds,
    partnerId,
    transactionEvent,
    session
  )
  return { completedTransactions }
}

export const initPaymentsLegacyTransaction = async (req) => {
  const { body, session } = req
  const completedTransactions = await addPaymentsTransactions(body, session)
  return { completedTransactions }
}

export const initReminderAndCollectionNoticeTransaction = async ({
  body,
  session
}) => {
  const completedTransactions =
    await invoiceService.addReminderAndCollectionNoticeTransaction(
      body,
      session
    )
  return { completedTransactions }
}

export const initRevertLostRecognitionTransactions = async ({
  body,
  session
}) => {
  const completedTransactions =
    await invoiceService.revertLostRecognitionTransaction(body, session)
  return { completedTransactions }
}

export const initAddEvictionNoticeTransaction = async ({ body, session }) => {
  const completedTransactions =
    await invoiceService.addEvictionNoticeTransaction(body, session)
  return { completedTransactions }
}

export const initRevertInvoiceFeesTransaction = async ({ body, session }) => {
  const completedTransactions =
    await invoiceService.revertInvoiceFeesTransaction(body, session)
  return { completedTransactions }
}

export const updateATransaction = async (query, data, session) => {
  const response = await TransactionCollection.findOneAndUpdate(query, data, {
    session,
    runValidators: true,
    new: true
  })
  if (!size(response)) {
    throw new CustomError(404, `Can not find transaction for update`)
  }
  return response
}

const updateTransactionForPartnerAPIForSerialId = async (
  params = {},
  session
) => {
  const { externalEntityId, partnerId, serialId, status = '' } = params
  const transactionStatus = status.toUpperCase()

  if (!serialId) throw new CustomError(400, 'Transaction ID is missing!')

  if (!(transactionStatus || externalEntityId))
    throw new CustomError(400, 'Status or externalEntityId is missing!')

  // Validating status
  if (
    transactionStatus &&
    !(transactionStatus === 'EXPORTED' || transactionStatus === 'ERROR')
  )
    throw new CustomError(
      400,
      'Status field only supports EXPORTED or ERROR value!'
    )

  const transactionQuery = { partnerId, serialId }
  const updateData = {}
  if (transactionStatus) {
    transactionQuery.status = { $nin: ['EXPORTED', 'ERROR'] }
    updateData.status = transactionStatus
  }
  if (externalEntityId) {
    transactionQuery.externalEntityId = { $ne: externalEntityId }
    updateData.externalEntityId = externalEntityId
  }

  const updatedTransaction = await updateATransaction(
    transactionQuery,
    updateData,
    session
  )
  return updatedTransaction
}

export const updateTransactionForPartnerAPI = async (req) => {
  const { body, session, user } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)

  const {
    apiKey = '',
    serialId = '',
    externalEntityId = '',
    status = ''
  } = body
  const { _id: partnerId = '', accountType = '' } =
    await transactionHelper.getPartnerInfoByValidatingApiKey(apiKey)

  const updatedTransaction = await updateTransactionForPartnerAPIForSerialId(
    {
      accountType,
      externalEntityId,
      partnerId,
      serialId,
      status
    },
    session
  )

  const [transaction] =
    (await transactionHelper.getTransactionsForPartnerAPIForQuery(
      {
        accountType,
        partnerId,
        query: {
          _id: updatedTransaction._id,
          partnerId
        }
      },
      session
    )) || []

  return transaction
}

export const updateTransactionSerials = async (req) => {
  const { body, session } = req
  const { partnerId, queueId } = body

  let { startingSerialId = 0 } = body
  console.log(
    '====> Checking default startingSerialId:',
    startingSerialId,
    'for partnerId:',
    partnerId,
    '<===='
  )
  if (!startingSerialId) {
    const [lastTransaction] =
      (await transactionHelper.getTransactions(
        { partnerId, serialId: { $exists: true } },
        session,
        { sort: { serialId: -1 }, limit: 1 }
      )) || []
    if (size(lastTransaction) && lastTransaction.serialId)
      startingSerialId = lastTransaction.serialId + 1
    else startingSerialId += 1

    console.log(
      '====> Checking updated startingSerialId:',
      startingSerialId,
      'for partnerId:',
      partnerId,
      '<===='
    )
  }

  const limitOfAddingSerialId = 2000
  const options = { sort: { createdAt: 1 }, limit: limitOfAddingSerialId }
  const query = { partnerId, serialId: { $exists: false } }
  const transactions =
    await transactionHelper.getTransactionsWithSelectedFields(
      { options, query, select: { _id: 1 } },
      session
    )
  const lengthOfTransactions = size(transactions)

  if (lengthOfTransactions) {
    const bulkUpdatingArray = []
    for (const [index, transaction] of transactions.entries()) {
      const { _id = '' } = transaction
      const serialId = startingSerialId + index

      bulkUpdatingArray.push({
        updateOne: { filter: { _id }, update: { $set: { serialId } } }
      })
    }

    const { result: transactionUpdatedResult = {} } = size(bulkUpdatingArray)
      ? await updateBulkTransactions(bulkUpdatingArray)
      : {}
    const { nModified: transactionUpdatedCount = 0 } =
      transactionUpdatedResult || {}
    console.log(
      '====> Checking transactionUpdatedCount:',
      transactionUpdatedCount,
      'for partnerId:',
      partnerId,
      '<===='
    )
    const nextTransactionSerialId = transactionUpdatedCount + startingSerialId
    console.log(
      '====> Checking nextTransactionSerialId:',
      nextTransactionSerialId,
      'for partnerId:',
      partnerId,
      '<===='
    )
    const isAppQueueUpdated = transactionUpdatedCount
      ? await appQueueService.updateAnAppQueue(
          { _id: queueId },
          {
            $set: { 'params.startingSerialId': nextTransactionSerialId }
          },
          session
        )
      : {}

    const success =
      !!size(isAppQueueUpdated) &&
      transactionUpdatedCount === lengthOfTransactions
    return { success, isAllCompleted: false }
  } else return { success: true, isAllCompleted: true }
}

const updateBulkTransactions = async (bulkArray = []) => {
  if (!size(bulkArray))
    throw new CustomError(400, 'Bulk array is empty for updating transactions')
  const response = await TransactionCollection.bulkWrite(bulkArray, {
    ordered: true
  })
  console.log('response', response)
  return response
}

export const downloadAccountTransactions = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['accountId'], body)
  const { accountId, dateRange } = body
  appHelper.validateId({ accountId })
  body.partnerId = partnerId
  body.userId = userId
  if (size(dateRange)) {
    const { endDate, startDate } = dateRange
    body.dateRange = {
      startDate_string: new Date(startDate),
      endDate_string: new Date(endDate)
    }
  }
  const transactionQuery = await transactionHelper.prepareTransactionQuery(body)
  await appHelper.isMoreOrLessThanTargetRows(
    TransactionCollection,
    transactionQuery,
    {
      moduleName: 'Transactions',
      rejectEmptyList: true
    }
  )

  const params = {
    accountId,
    download: true,
    downloadProcessType: 'download_transactions',
    userId,
    partnerId
  }
  if (size(dateRange)) params.dateRange = body.dateRange
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'

  const queueData = {
    action: 'download_email',
    event: 'download_email',
    destination: 'excel-manager',
    priority: 'immediate',
    params,
    status: 'new'
  }
  await appQueueService.createAnAppQueue(queueData, session)
  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

const prepareDownloadLandlordReportQuery = async (params) => {
  const { accountId, dateRange, partnerId, userId } = params
  const transactionQuery = { partnerId }

  if (!accountId) {
    const accountIds = []
    const accounts =
      (await accountHelper.getAccounts({
        personId: userId,
        partnerId
      })) || []

    if (accounts.length > 0) {
      for (const account of accounts) {
        accountIds.push(account._id)
      }
    }
    transactionQuery.accountId = { $in: accountIds }
  } else {
    transactionQuery.accountId = { $in: [accountId] }
  }

  if (size(dateRange)) {
    const { startDate, endDate } = dateRange
    transactionQuery.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }

  return transactionQuery
}

export const downloadLandlordReport = async (req) => {
  const { body } = req
  appHelper.validatePartnerAppRequestData(req)
  const { userId, partnerId } = body
  body.downloadProcessType = 'download_landlord_reports'
  body.context = 'landlordReports'
  const userInfo = (await userHelper.getAnUser({ _id: userId })) || {}
  body.userLanguage = userInfo?.profile?.language || 'en'
  body.isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  const landlordInvoiceQuery = await prepareDownloadLandlordReportQuery(body)
  await appHelper.isMoreOrLessThanTargetRows(
    InvoiceCollection,
    landlordInvoiceQuery
  )
  if (size(body.dateRange)) {
    const { startDate, endDate } = body.dateRange
    body.dateRange = {
      startDate_string: startDate,
      endDate_string: endDate
    }
  }

  const appQueueData = {
    action: 'download_email',
    destination: 'excel-manager',
    event: 'download_email',
    params: body,
    priority: 'immediate',
    status: 'new'
  }

  await appQueueService.createAnAppQueue(appQueueData)

  return {
    status: 202,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

export const downloadTransactions = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const { dateRange, userId, partnerId } = body
  body.downloadProcessType = 'download_transactions'
  if (size(dateRange)) {
    const { endDate, startDate } = dateRange
    body.dateRange = {
      startDate_string: new Date(startDate),
      endDate_string: new Date(endDate)
    }
  }
  const invoicesQuery = await transactionHelper.prepareTransactionQuery(body)
  body.isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  await appHelper.isMoreOrLessThanTargetRows(
    TransactionCollection,
    invoicesQuery,
    {
      moduleName: 'Transactions',
      rejectEmptyList: true
    }
  )
  const userInfo = await userHelper.getAnUser({ _id: userId })
  body.userLanguage = userInfo?.profile?.language || 'en'

  const appQueueData = {
    action: 'download_email',
    destination: 'excel-manager',
    event: 'download_email',
    params: body,
    priority: 'immediate'
  }

  await appQueueService.createAnAppQueue(appQueueData)

  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

export const downloadDetailedBalanceReport = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { period, propertyId, tenantId, sort } = body
  body.partnerId = partnerId

  const { preparedQuery = {} } =
    await transactionHelper.prepareQueryForDetailedBalanceReport(body)

  if (size(period)) {
    preparedQuery['createdAt'] = {
      $lte: new Date(period.endDate)
    }
  }

  await appHelper.isMoreOrLessThanTargetRows(
    TransactionCollection,
    preparedQuery,
    {
      moduleName: 'Transactions'
    }
  )

  const params = {
    downloadProcessType: 'download_detailed_balance_report',
    partnerId,
    userId
  }

  if (size(period)) {
    const { endDate, startDate } = period
    params.period = {
      startDate: new Date(startDate),
      endDate: new Date(endDate)
    }
  }
  if (propertyId) params.propertyId = propertyId
  if (tenantId) params.tenantId = tenantId
  if (size(sort))
    params.sort =
      transactionHelper.prepareSortForDownloadDetailedBalanceReport(sort)
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'

  const queueData = {
    action: 'download_email',
    event: 'download_email',
    destination: 'excel-manager',
    priority: 'immediate',
    params,
    status: 'new'
  }

  await appQueueService.createAnAppQueue(queueData)

  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}
