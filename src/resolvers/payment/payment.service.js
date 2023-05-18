import {
  assign,
  clone,
  difference,
  find,
  head,
  includes,
  indexOf,
  map,
  omit,
  partition,
  pick,
  pull,
  size,
  union
} from 'lodash'

import { appPermission, CustomError } from '../common'
import { InvoicePaymentCollection } from '../models'
import {
  appHelper,
  appInvoiceHelper,
  contractHelper,
  depositInsuranceHelper,
  invoiceHelper,
  logHelper,
  partnerHelper,
  paymentHelper
} from '../helpers'
import {
  appInvoiceService,
  appQueueService,
  depositInsuranceService,
  finalSettlementService,
  logService,
  invoiceService,
  invoicePaymentService,
  payoutService,
  transactionService
} from '../services'

// TODO:: update invoice totalPaid, status[new,paid,overdue], lastPaymentDate, isOverpaid, isPartiallyPaid, feesPaid
export const updateInvoice = async (payment, invoice, session) => {
  const paymentAmount = payment.amount
  const lastPaymentDate = payment.paymentDate
  const totalPaid = invoice.totalPaid + paymentAmount
  const creditedAmount = invoice.creditedAmount || 0
  const invoiceTotalAmount = invoice.invoiceTotal + creditedAmount
  const invoiceDueDate = invoice.dueDate
  const today = (await appHelper.getActualDate(invoice.partnerId, true, null))
    .startOf('day')
    .toDate()
  console.log('invoiceDueDate', invoiceDueDate)
  console.log('today', today)
  const isOverPaid = false
  const isPartiallyPaid = false
  const feesPaid = false

  const dataForInvoiceUpdate = {
    totalPaid,
    lastPaymentDate,
    isOverPaid,
    isPartiallyPaid,
    feesPaid
  }

  if (invoiceTotalAmount === totalPaid) {
    dataForInvoiceUpdate.status = 'paid'
    dataForInvoiceUpdate.feesPaid = true
  } else if (invoiceTotalAmount < totalPaid) {
    dataForInvoiceUpdate.status = 'paid'
    dataForInvoiceUpdate.feesPaid = true
    dataForInvoiceUpdate.isOverPaid = true
  } else if (invoiceTotalAmount > totalPaid && invoiceDueDate < today) {
    dataForInvoiceUpdate.status = 'overdue'
    dataForInvoiceUpdate.isPartiallyPaid = true
  } else if (invoiceTotalAmount > totalPaid) {
    dataForInvoiceUpdate.status = 'new'
    dataForInvoiceUpdate.isPartiallyPaid = true
  }

  if (invoice.status === 'credited') delete dataForInvoiceUpdate.status
  console.log(
    `=== Updating invoice with: ${JSON.stringify(dataForInvoiceUpdate)} ===`
  )
  const updatedInvoice = await invoiceService.updateInvoice(
    { _id: invoice._id },
    dataForInvoiceUpdate,
    session
  )
  if (!size(updatedInvoice)) {
    throw new CustomError(
      404,
      `Unable to update invoice for invoiceId: ${invoice._id}`
    )
  }

  if (updatedInvoice.status === 'paid') {
    await payoutService.addInvoicePaidInfoInPayout(updatedInvoice, session)
    await invoiceService.removeDefaultedTagFromInvoice(updatedInvoice, session)
    await payoutService.setInvoicePaidInFinalSettlementPayout(
      updatedInvoice,
      session
    )
  }
}

export const updateAPayment = async (query, data, session) => {
  if (!size(data)) throw new CustomError(404, 'No data found for update')

  return InvoicePaymentCollection.findOneAndUpdate(query, data, {
    new: true,
    runValidators: true,
    session
  })
}

export const updateAPaymentAndCreateTransactionQueue = async (
  params,
  session
) => {
  const { oldPayment, queryData, updatingData } = params || {}

  const { invoiceId, status } = oldPayment || {}
  const updatingInvoicesArray = updatingData['$set'].invoices || []
  if (invoiceId && status === 'registered' && size(updatingInvoicesArray)) {
    const invoiceInPayment = find(updatingInvoicesArray, { invoiceId })

    if (!invoiceInPayment && updatingInvoicesArray[0].invoiceId) {
      console.log(
        '====> Main invoiceId is removed from payment, invoiceId:',
        {
          oldInvoiceId: oldPayment.invoiceId,
          newInvoiceId: updatingInvoicesArray[0].invoiceId
        },
        '<===='
      )
      updatingData['$set'].invoiceId = updatingInvoicesArray[0].invoiceId
    }
  }

  const updatedPayment = await updateAPayment(queryData, updatingData, session)

  if (!size(updatedPayment))
    throw new CustomError(404, 'Could not update payment')

  return await checkAndCreatePaymentTransactionsForMatchPayments(
    oldPayment,
    updatedPayment,
    session
  )
}

export const addLinkBetweenPaymentsAndInvoices = async (params, session) => {
  console.log(
    `=== Started addLinkBetweenPaymentsAndInvoices with ${JSON.stringify(
      params
    )}===`
  )
  const invoicesWithPayments =
    await paymentHelper.getUnManagedInvoicesWithPayments(params, session) // Getting invoicePayments based on contractId
  console.log(
    `=== Total unManagedPayments found ${size(invoicesWithPayments)} ===`
  )

  if (!size(invoicesWithPayments)) return false

  for (const invoiceWithPayments of invoicesWithPayments) {
    const {
      _id: invoiceId,
      invoice: oldInvoice,
      payments
    } = invoiceWithPayments || {}

    let invoiceTotalDue = invoiceWithPayments?.invoiceTotalDue || 0
    let newTotalPaid = oldInvoice?.totalPaid || 0
    let newLastPaymentDate = new Date()
    console.log(`=== invoiceTotalDue: ${invoiceTotalDue} ===`)
    console.log(`=== newTotalPaid: ${newTotalPaid} ===`)
    for (const payment of payments) {
      const { _id: paymentId, amount, paymentDate } = payment || {}

      const invoicesObjForPayment = { amount, invoiceId }

      const remaining = await appHelper.convertTo2Decimal(
        amount - invoiceTotalDue || 0
      )
      console.log('=== Remaining ===', remaining)

      if (remaining > 0 && remaining <= amount)
        invoicesObjForPayment.remaining = remaining

      console.log(
        `=== Adding invoice in payment with: ${JSON.stringify(
          invoicesObjForPayment
        )} ===`
      )

      await updateAPaymentAndCreateTransactionQueue(
        {
          oldPayment: omit(payment, ['invoice', 'invoiceTypes', 'totalDue']),
          queryData: { _id: paymentId },
          updatingData: { $set: { invoices: [invoicesObjForPayment] } }
        },
        session
      )

      if (invoiceTotalDue - amount < 0) invoiceTotalDue = 0
      else invoiceTotalDue -= amount
      newTotalPaid += amount
      newLastPaymentDate = paymentDate
    }

    // Updating invoice totalPaid, status, lastPaymentDate, isOverpaid, isPartiallyPaid & feesPaid flags.
    newTotalPaid = await appHelper.convertTo2Decimal(newTotalPaid || 0)

    console.log(
      `=== Updating invoice with: ${JSON.stringify({
        invoiceId,
        newTotalPaid,
        newLastPaymentDate
      })} ===`
    )
    await invoiceService.startAfterProcessForInvoiceTotalPaidChange(
      {
        isFromMatchPayment: true,
        oldInvoice,
        newTotalPaid,
        newLastPaymentDate
      },
      session
    )
  }
}

export const updateNewlyPaidInvoices = async (
  newlyPaidInvoices,
  partnerId,
  session
) => {
  console.log(
    `=== Total newlyPaidInvoices found. ${size(newlyPaidInvoices)} ===`
  )

  for (const invoice of newlyPaidInvoices) {
    const oldInvoice = await invoiceHelper.getInvoice({ _id: invoice._id })

    const { lastPaymentDate, totalPaid } = invoice || {}
    const newTotalPaid = await appHelper.convertTo2Decimal(totalPaid || 0)
    await invoiceService.startAfterProcessForInvoiceTotalPaidChange(
      {
        isFromMatchPayment: true,
        oldInvoice,
        newTotalPaid,
        newLastPaymentDate: lastPaymentDate
      },
      session
    )
  }
}

export const forwardOverPaidAmountsToNonPaidInvoices = async (
  params,
  session
) => {
  console.log(
    `=== Started forwardOverPaidAmountsToNonPaidInvoices with ${JSON.stringify(
      params
    )} ===`
  )
  const { partnerId } = params
  const paymentsWithRemaining = await paymentHelper.getPaymentsWithRemaining(
    params,
    session
  )
  console.log(
    `=== Total paymentsWithRemaining found ${size(paymentsWithRemaining)} ===`
  )
  for (const payment of paymentsWithRemaining) {
    const invoiceItems = paymentHelper.getRemainingInvoiceIdAndAmount(payment)
    console.log(
      `=== Total number of remaining found ${size(
        invoiceItems
      )} for paymentId: ${payment._id}, invoicesArray: ${JSON.stringify(
        invoiceItems
      )} ===`
    )
    // Removing remaining from payment (amount - remaining)
    paymentHelper.getPaymentInvoicesArrayWithoutRemaining(payment)

    for (const invoiceItem of invoiceItems) {
      const newlyPaidInvoices = []
      const { invoiceId, remaining } = invoiceItem
      console.log(
        `=== Getting overPaid invoice for invoiceId: ${invoiceId} ===`
      )
      const overPaidInvoice = await paymentHelper.getOverPaidInvoiceById(
        invoiceId,
        session
      )

      if (!size(overPaidInvoice))
        throw new CustomError(
          404,
          `Overpaid invoice not found for invoiceId: ${invoiceId}`
        )

      let partiallyCreditInvoice
      if (overPaidInvoice.voidExistingPayment) {
        params.earlierInvoiceId = overPaidInvoice._id
        partiallyCreditInvoice = await invoiceHelper.getInvoice(
          {
            earlierInvoiceId: overPaidInvoice._id
          },
          session
        )
      }

      params.isNonRentInvoice = !!overPaidInvoice.isNonRentInvoice
      // An Non-RentInvoice remaining payment will be forward only on others nonPaid NonRentInvoice
      // And a RentInvoice remaining payment will be forward only on others nonPaid RentInvoice
      let nonPaidInvoices = await paymentHelper.getNonPaidInvoices(
        params,
        session
      )
      if (size(partiallyCreditInvoice)) {
        nonPaidInvoices = [partiallyCreditInvoice, ...nonPaidInvoices]
      }
      const paymentData = {
        remaining,
        payment,
        nonPaidInvoices
      }
      console.log(
        `=== Total nonPaidInvoices found ${size(nonPaidInvoices)} ===`
      )
      if (size(nonPaidInvoices)) {
        const actualPaidAmount = await appHelper.convertTo2Decimal(
          (overPaidInvoice.totalPaid -= remaining)
        )
        const { invoiceTotal = 0, creditedAmount = 0 } = overPaidInvoice
        const totalAmountToPay = invoiceTotal + (creditedAmount || 0)
        const updateData = { totalPaid: actualPaidAmount }
        if (totalAmountToPay === actualPaidAmount) updateData.isOverPaid = false
        // Updating invoice totalPaid and maybe isOverPaid field
        console.log(
          `=== Updating invoice with: ${JSON.stringify(updateData)} ===`
        )
        await invoiceService.updateInvoice(
          { _id: invoiceId },
          updateData,
          session
        )
        await paymentHelper.forwardRemainingAmount(
          paymentData,
          newlyPaidInvoices
        )
        // Updating payment invoices array with new amount.remaining and maybe invoice
        console.log(
          `=== Updating invoicePayment with: ${JSON.stringify(
            payment.invoices
          )} ===`
        )
        await updateAPaymentAndCreateTransactionQueue(
          {
            oldPayment: payment,
            queryData: { _id: payment._id },
            updatingData: { $set: { invoices: payment.invoices } }
          },
          session
        )
        await updateNewlyPaidInvoices(newlyPaidInvoices, partnerId, session)
      }
    }
  }
}

export const adjustCreditedOverPaidInvoices = async (params, session) => {
  console.log(
    `=== Started adjustCreditedOverPaidInvoices with ${JSON.stringify(
      params
    )} ===`
  )

  const overPaidInvoicesWithPayments =
    await paymentHelper.getCreditedOverPaidInvoicesWithPayments(params, session)

  console.log(
    `=== Total overPaidCredited invoice found ${size(
      overPaidInvoicesWithPayments
    )} ===`
  )
  if (!size(overPaidInvoicesWithPayments)) return false

  for (const invoiceWithPayments of overPaidInvoicesWithPayments) {
    const { _id: invoiceId, payments } = invoiceWithPayments || {}

    console.log(
      `=== Total payments found ${size(
        payments
      )} for overPaidCredited invoiceId: ${invoiceId} ===`
    )

    // Invoice might be paid by multiple payments
    let invoiceTotalAmount = invoiceWithPayments?.invoiceTotalAmount || 0
    console.log(
      `=== Invoice actual payable amount is ${invoiceTotalAmount} for invoiceId: ${invoiceId} ===`
    )
    for (const payment of payments) {
      const { invoice, paymentId } = payment || {}

      if (size(invoice)) {
        console.log(
          `=== InvoiceInfo ${JSON.stringify(
            invoice
          )} found from paymentId: ${paymentId} ===`
        )

        const { amount: paymentAmount, remaining: paymentRemaining } =
          invoice || {}
        const remaining = paymentAmount - invoiceTotalAmount

        let updatingRemainingAmount = 0
        if (remaining > 0 && remaining <= paymentAmount) {
          updatingRemainingAmount =
            (await appHelper.convertTo2Decimal(remaining)) || 0
        }

        const paymentUpdatingData = {}
        if (updatingRemainingAmount) {
          paymentUpdatingData['$set'] = {
            'invoices.$.remaining': updatingRemainingAmount
          }
        } else if (paymentRemaining) {
          paymentUpdatingData['$unset'] = { 'invoices.$.remaining': 1 }
        }

        console.log(
          `=== Updating invoicePayment with ${JSON.stringify(
            paymentUpdatingData
          )} for paymentId: ${paymentId} ===`
        )
        if (size(paymentUpdatingData)) {
          await updateAPayment(
            {
              _id: paymentId,
              'invoices.invoiceId': invoice.invoiceId
            },
            paymentUpdatingData,
            session
          )
        }

        if (invoiceTotalAmount - paymentAmount < 0) invoiceTotalAmount = 0
        else invoiceTotalAmount -= paymentAmount
      }
    }
  }
}

export const adjustBetweenPaymentsAndInvoices = async (params, session) => {
  try {
    const { processType } = params || {}

    if (
      !includes(
        [
          'addLinkBetweenPaymentsAndInvoices',
          'adjustCreditedOverPaidInvoices',
          'forwardOverPaidAmountsToNonPaidInvoices',
          'matchPaymentsWithInvoices'
        ],
        processType
      )
    ) {
      throw new CustomError(
        400,
        'Found invalid process type for matching payments'
      )
    }

    if (
      includes(
        ['matchPaymentsWithInvoices', 'addLinkBetweenPaymentsAndInvoices'],
        processType
      )
    ) {
      console.log('=== Started addLinkBetweenPaymentsAndInvoices ===')
      await addLinkBetweenPaymentsAndInvoices(params, session)
      console.log('=== Completed addLinkBetweenPaymentsAndInvoices ===')
    }

    if (
      includes(
        ['matchPaymentsWithInvoices', 'adjustCreditedOverPaidInvoices'],
        processType
      )
    ) {
      console.log('=== Started adjustCreditedOverPaidInvoices ===')
      await adjustCreditedOverPaidInvoices(params, session)
      console.log('=== Completed adjustCreditedOverPaidInvoices ===')
    }

    if (
      includes(
        [
          'matchPaymentsWithInvoices',
          'forwardOverPaidAmountsToNonPaidInvoices'
        ],
        processType
      )
    ) {
      console.log('=== Started forwardOverPaidAmountsToNonPaidInvoices ===')
      await forwardOverPaidAmountsToNonPaidInvoices(params, session)
      console.log('=== Completed forwardOverPaidAmountsToNonPaidInvoices ===')
    }
  } catch (err) {
    throw new CustomError(
      err?.statusCode || err?.code || 504,
      err?.message || 'Error found while adjustBetweenPaymentsAndInvoices'
    )
  }
}

export const addPaymentTransaction = async (params, session) => {
  const transactionData = await paymentHelper.prepareTransactionData(
    params,
    session
  )
  console.log('Checking for prepared transaction data: ', transactionData)
  if (!size(transactionData)) {
    return false
  }
  const addedTransaction = await transactionService.createTransaction(
    transactionData,
    session
  )
  return addedTransaction
}

export const updatePayments = async (query, updateData, session) => {
  if (!(size(query) && size(updateData)))
    throw new CustomError(400, 'Missing query or data for update payments')
  const updatedPayments = await InvoicePaymentCollection.updateMany(
    query,
    updateData,
    { session }
  )
  if (updatedPayments.nModified === 0) {
    throw new CustomError(500, 'Payments cannot update')
  }
  return true
}

export const createPaymentLog = async (action, options, session) => {
  // added_new_payment (Done)
  // removed_payment (Done)
  // updated_payment (Done)
  // updated_refunded_payment (Done)
  // canceled_refund_payment (Done)
  if (!action)
    throw new CustomError(404, 'Action is required for payment log creation')

  if (!size(options)) {
    throw new CustomError(404, 'Options is required for payment log creation')
  }

  if (action === 'removed_payment') {
    const logData = await logHelper.prepareLogDataForRemovePayment(
      { action, options },
      session
    )
    if (options.userId) logData.createdBy = options.userId
    else logData.createdBy = 'SYSTEM'

    const { _id: logId } = await logService.createLog(logData, session)
    console.log(
      `=== Created activity log. logId: ${logId}, action: ${action} ==`
    )
  } else {
    const { collectionId, context, partnerId } = options

    if (!(collectionId || partnerId)) {
      throw new CustomError(
        404,
        `Required collectionId or partnerId for ${action} log creation`
      )
    }

    let logData = { action, context, partnerId }
    const query = {
      _id: collectionId,
      $or: [{ appPartnerId: partnerId }, { partnerId }]
    }

    if (action === 'added_new_payment') {
      logData = await logHelper.prepareLogDataForNewPayment(
        { logData, options, query },
        session
      )
    } else if (action === 'canceled_refund_payment') {
      logData = await logHelper.prepareLogDataForCanceledRefundPayment(
        { logData, options, query },
        session
      )
    } else if (action === 'updated_payment') {
      logData = await logHelper.prepareLogDataForUpdatePayment(
        { logData, options, query },
        session
      )
    } else if (action === 'updated_refunded_payment') {
      logData = await logHelper.prepareLogDataForUpdateRefundPayment(
        { logData, options, query },
        session
      )
    }
    if (options.userId) logData.createdBy = options.userId
    else logData.createdBy = 'SYSTEM'

    const { _id: logId } = await logService.createLog(logData, session)
    console.log(
      `=== Created activity log. logId: ${logId}, action: ${action} ==`
    )
  }
}

export const matchPaymentsWithInvoices = async (req = {}) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  paymentHelper.checkRequiredDataInMatchPaymentsReq(body)

  const { event, params, processType } = body
  const { contractId, partnerId } = params

  if (event !== 'match_payments_with_invoices')
    throw new CustomError(400, 'Invalid event name found')

  const contractInfo = await contractHelper.getAContract(
    { _id: contractId, partnerId },
    session
  )
  if (!size(contractInfo))
    throw new CustomError(404, 'Could not find contractInfo with contractId')

  await adjustBetweenPaymentsAndInvoices({ ...params, processType }, session)
  return true
}

export const identifyAndUpdateBankPayment = async (payment, session) => {
  let newData = {} // Keep Eye on this variable. Used benefits of reference
  const { _id: paymentId, amount, meta, paymentDate } = payment

  if (size(meta)) {
    const { cdTrAccountNumber = '', kidNumber = '' } = meta

    // ## Identifying deposit insurance payment && find partnerId from partnerSetting by cdTrAccountNumber
    if (cdTrAccountNumber) {
      await paymentHelper.identifyDIBankPayment(
        { cdTrAccountNumber, kidNumber },
        newData
      )

      if (!newData.appInvoiceId) {
        // Finding partnerId from partnerSetting by cdTrAccountNumber
        await paymentHelper.findPartnerSettingsByACNumber(
          cdTrAccountNumber,
          newData
        )
      }
    } // End of Identifying deposit insurance payment && find partnerId from partnerSetting by cdTrAccountNumber

    const { partnerId = '' } = newData // ## partnerId comes from identifyDIBankPayment method (JS reference)

    // ## Find invoice by kid number and set invoice info in payments
    if (kidNumber && partnerId) {
      await paymentHelper.identifyBankPaymentByKidNumberAndAccountNumber(
        { invoiceAccountNumber: cdTrAccountNumber, kidNumber, partnerId },
        newData
      )
    } // End of Find invoice by kid number and set invoice info in payments

    // ## Make payment status 'unspecified', if the payment is done after completed the final settlement OR
    // Don't have due rent amount of any final settlement contract invoice which has 'in_progress' status
    newData = await paymentHelper.isItAfterFinalSettlementPayment(newData) // Maybe replacing newData with new value
  } // End of if (size(meta)) block

  // If matched payment then add paymentDate based on partners timezone
  if (newData?.status === 'registered' && newData?.partnerId) {
    console.log(
      '=== Set: paymentDate in the payment based on partners timezone ==='
    )
    newData.paymentDate = await appHelper.getActualDate(
      newData.partnerId,
      false,
      paymentDate
    )
  }

  if (!newData.status) newData.status = 'unspecified'

  const {
    contractId,
    partnerId,
    status: paymentStatus
  } = (await updateAPayment({ _id: paymentId }, { $set: newData }, session)) ||
  {}
  console.log(`=== Updated payment with status: ${newData.status} ===`)

  if (contractId && partnerId && paymentStatus === 'registered') {
    await appQueueService.createAppQueueForMatchPayment({
      action: 'added_new_bank_payment',
      contractId,
      partnerId
    })
  }

  const {
    appInvoiceId,
    depositInsuranceId,
    invoiceId = '',
    isDepositInsurancePayment,
    status
  } = newData

  if (appInvoiceId && depositInsuranceId && isDepositInsurancePayment) {
    // Update appInvoice totalPaid & status
    await updateAppInvoiceTotalPaidAndInitAfterUpdateHook(
      { appInvoiceId, amount },
      session
    )
    // Update depositInsurance totalPaymentAmount & paymentsMeta
    await updateTotalPaymentAmountAndPaymentsMetaOfDI(
      {
        amount,
        depositInsuranceId,
        paymentDate: newData.paymentDate,
        paymentId
      },
      session
    )
  }

  return appInvoiceId
    ? { appInvoiceId, status }
    : invoiceId
    ? { invoiceId, status }
    : { status }
}

export const identifyBankPayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  paymentHelper.checkRequiredDataInIdentifyBankPaymentReq(body)

  const { action, event, params } = body

  if (
    !(event === 'identify_bank_payment' && action === 'identify_bank_payment')
  )
    throw new CustomError(400, 'Invalid event or action name found')

  const { paymentId } = params
  const payment = await paymentHelper.getPayment({ _id: paymentId })
  if (!size(payment)) throw new CustomError(404, 'Payment does not exists')

  const { status, paymentType } = payment
  if (!(status === 'new' && paymentType === 'bank'))
    throw new CustomError(404, 'Invalid data found in payment')

  return await identifyAndUpdateBankPayment(payment, session)
}

const addInvoiceManualPayment = async (params, session) => {
  const {
    amount,
    invoiceId,
    partnerId,
    paymentDate,
    paymentReason = undefined,
    paymentToAccountNumber,
    userId
  } = params

  // Only partner admin can do this request
  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  )
    throw new CustomError(400, 'Permission denied')
  if (
    await paymentHelper.isDoneFinalSettlementBasedOnInvoiceId(
      invoiceId,
      partnerId
    )
  )
    throw new CustomError(405, 'Final settlement is done for this invoice')

  const invoiceInfo = await invoiceHelper.getInvoice({
    _id: invoiceId,
    partnerId
  })

  if (size(invoiceInfo)) {
    const {
      creditedAmount = 0,
      invoiceTotal,
      invoiceType,
      isFinalSettlement,
      isPayable,
      totalPaid
    } = invoiceInfo

    const invoiceData = pick(invoiceInfo, [
      'accountId',
      'agentId',
      'branchId',
      'contractId',
      'propertyId',
      'tenantId',
      'tenants'
    ])

    const paymentData = { ...invoiceData }

    if (
      invoiceType === 'landlord_invoice' &&
      (isFinalSettlement || isPayable)
    ) {
      paymentData.isFinalSettlement = true
    }

    if (paymentToAccountNumber) {
      const partner = await partnerHelper.getAPartner(
        { _id: params.partnerId },
        null,
        ['partnerSetting']
      )

      if (!size(partner))
        throw new CustomError(
          404,
          `Partner doesn't exists with partnerId: ${params.partnerId}`
        )

      const { accountType, partnerSetting = {} } = partner

      if (!size(partnerSetting))
        throw new CustomError(
          404,
          `PartnerSetting doesn't exists with partnerId: ${params.partnerId}`
        )

      const isDirectPartner = accountType === 'direct'
      const paymentMeta = { cdTrAccountNumber: paymentToAccountNumber }

      if (isDirectPartner) {
        const { companyInfo = {} } = partnerSetting
        if (companyInfo?.companyName)
          paymentMeta.cdTrName = companyInfo.companyName
        if (companyInfo?.officeAddress)
          paymentMeta.cdTrAddress = companyInfo.officeAddress
      } else {
        const { bankAccounts = {} } = partnerSetting
        let bankAccountInfo = {}

        if (bankAccounts)
          bankAccountInfo = find(bankAccounts, {
            accountNumber: paymentToAccountNumber
          })

        if (size(bankAccountInfo)) {
          const { orgAddress, orgName } = bankAccountInfo
          if (orgName) paymentMeta.cdTrName = orgName
          if (orgAddress) paymentMeta.cdTrAddress = orgAddress
        }
      }
      paymentData.meta = paymentMeta || {}
    }

    paymentData.amount = amount
    paymentData.invoiceId = invoiceId
    paymentData.partnerId = params.partnerId
    paymentData.paymentDate = paymentDate
    paymentData.paymentReason = paymentReason
    paymentData.paymentType = 'manual'
    paymentData.status = 'registered'
    paymentData.type = 'payment'
    paymentData.createdBy = userId

    // Preparing invoices array for payment
    const invoicesObjForPayment = { amount, invoiceId }

    const totalDue = invoiceTotal - totalPaid + creditedAmount // Preparing invoice totalDue
    const remaining = amount - (totalDue > 0 ? totalDue : 0) // Preparing remaining amount

    if (remaining > 0 && remaining <= amount)
      // Set remaining amount in the invoices array of the payment
      invoicesObjForPayment.remaining = remaining

    paymentData.invoices = [invoicesObjForPayment]

    const [payment] = await invoicePaymentService.insertAnInvoicePayment(
      paymentData,
      session
    )
    if (!size(payment)) throw new CustomError(404, 'Unable to add payment')
    console.log(
      `=== New payment added. paymentId: ${payment._id}, paymentStatus: ${payment.status} ===`
    )
    //  Init payment's after insert hook
    const { _id: paymentId, contractId, partnerId } = payment
    // Payment log create
    const options = {
      partnerId,
      collectionId: paymentId,
      context: 'payment',
      userId
    }
    console.log(`=== Creating payment log. paymentId: ${paymentId} ===`)
    await createPaymentLog('added_new_payment', options, session)
    // End of Payment log creation
    // Add new payment transaction for registered payment
    console.log(`=== Creating payment transaction. paymentId: ${paymentId} ===`)
    await appQueueService.createAppQueueForPaymentTransaction(
      { payment },
      session
    )
    // End of payment transaction
    // Start of invoice totalPaid update
    console.log(`=== Updating invoice totalPaid and other status ===`)
    const newTotalPaid = await appHelper.convertTo2Decimal(
      totalPaid + amount || 0
    )
    await invoiceService.startAfterProcessForInvoiceTotalPaidChange(
      {
        isFromMatchPayment: true, // To avoid extra match payments creation
        newTotalPaid,
        newLastPaymentDate: paymentDate,
        oldInvoice: invoiceInfo
      },
      session
    )
    // End of invoice totalPaid update
    // Do match payments
    console.log(`=== Matching payment. paymentId: ${paymentId} ===`)
    // Over paid amount will be forward by match payment appQueue
    await appQueueService.createAppQueueForMatchPayment(
      { action: 'added_new_payment', contractId, partnerId },
      session
    )
    // End of match payments

    return payment
  } else throw new CustomError(404, "Invoice doesn't exists")
}

const updateAppInvoiceTotalPaidAndInitAfterUpdateHook = async (
  params,
  session
) => {
  const { amount, appInvoiceId } = params
  if (!appInvoiceId)
    throw new CustomError(400, 'Required appInvoiceId to update appInvoice')

  const appInvoice = await appInvoiceHelper.getAppInvoice({ _id: appInvoiceId })

  if (!size(appInvoice))
    throw new CustomError(
      404,
      `AppInvoiceId doesn't exists. appInvoiceId: ${appInvoiceId}`
    )

  if (amount) {
    const updatedAppInvoice = await appInvoiceService.updateAppInvoice(
      { _id: appInvoiceId },
      { $inc: { totalPaid: amount } },
      session
    )

    console.log(
      `=== Updated appInvoice totalPaid to: ${updatedAppInvoice.totalPaid}. appInvoiceId: ${updatedAppInvoice._id} ===`
    )
    try {
      await appInvoiceService.initAfterUpdateProcessForAppInvoice(
        appInvoice,
        updatedAppInvoice,
        session
      )
    } catch (err) {
      console.log('Catch error', err)
      throw new CustomError(404, err.message)
    }
  }
}

const updateTotalPaymentAmountAndPaymentsMetaOfDI = async (params, session) => {
  const { amount, depositInsuranceId, paymentDate, paymentId } = params
  if (amount && depositInsuranceId && paymentId) {
    const metaData = {
      id: paymentId,
      paymentAmount: amount,
      paymentDate: paymentDate || new Date()
    }
    await depositInsuranceService.updateADepositInsurance(
      { _id: depositInsuranceId },
      {
        $inc: { totalPaymentAmount: amount },
        $push: { payments: metaData }
      },
      session
    )
    console.log(
      '=== Incrementing totalPaymentAmount and updating payments with new paymentInfo in deposit insurance ==='
    )
  }
}

const addDepositInsuranceManualPayment = async (params, session) => {
  const {
    amount,
    appInvoiceId,
    paymentDate,
    paymentReason = undefined,
    userId
  } = params
  // Only app admin can do this request
  if (!(await appPermission.isAppAdmin(userId)))
    throw new CustomError(400, 'Permission denied')

  const invoiceInfo = await appInvoiceHelper.getAppInvoice({
    _id: appInvoiceId
  })

  if (size(invoiceInfo)) {
    const invoiceData = pick(invoiceInfo, [
      'accountId',
      'agentId',
      'branchId',
      'contractId',
      'depositInsuranceId',
      'propertyId',
      'serialId',
      'tenantId'
    ])

    const paymentData = { ...invoiceData }

    paymentData.amount = amount
    paymentData.appInvoiceId = appInvoiceId
    paymentData.appPartnerId = invoiceInfo?.partnerId
    paymentData.isDepositInsurancePayment = true
    paymentData.meta = {
      kidNumber: invoiceInfo?.kidNumber,
      cdTrName: invoiceInfo?.sender?.companyName,
      cdTrAccountNumber: invoiceInfo?.invoiceAccountNumber
    }
    paymentData.paymentDate = paymentDate
    paymentData.paymentReason = paymentReason
    paymentData.paymentType = 'manual'
    paymentData.status = 'registered'
    paymentData.type = 'payment'
    paymentData.createdBy = userId

    const [payment] = await invoicePaymentService.insertAnInvoicePayment(
      paymentData,
      session
    )
    console.log(
      `=== New payment added for DI. paymentId: ${payment._id}, paymentStatus: ${payment.status} ===`
    )
    // Init payment's after insert hook
    if (size(payment)) {
      const {
        _id: paymentId,
        appInvoiceId,
        appPartnerId,
        amount,
        depositInsuranceId,
        isDepositInsurancePayment,
        status
      } = payment
      // Payment log create
      if (appPartnerId) {
        const options = {
          partnerId: appPartnerId,
          collectionId: paymentId,
          context: 'payment',
          userId
        }
        console.log(`=== Creating payment log. paymentId: ${paymentId} ===`)
        await createPaymentLog('added_new_payment', options, session)
        // End of Payment log creation

        if (status === 'registered') {
          if (appInvoiceId && depositInsuranceId && isDepositInsurancePayment) {
            // Update appInvoice totalPaid & status
            await updateAppInvoiceTotalPaidAndInitAfterUpdateHook(
              { appInvoiceId, amount },
              session
            )
            // Update depositInsurance totalPaymentAmount & paymentsMeta
            await updateTotalPaymentAmountAndPaymentsMetaOfDI(
              {
                amount,
                depositInsuranceId,
                paymentDate,
                paymentId
              },
              session
            )
          } else
            throw new CustomError(
              405,
              'DepositInsuranceId not found in the invoice'
            )
        }
      }
    }
    return payment
  } else throw new CustomError(404, 'AppInvoice does not exist')
}

export const addManualPayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { roles, partnerId, userId } = user
  appHelper.checkUserId(userId)

  if (!roles.includes('lambda_manager')) {
    body.userId = userId
    if (body.paymentFor === 'invoice') body.partnerId = partnerId
  }

  paymentHelper.checkRequiredDataInAddManualPaymentReq(body)

  const { paymentFor } = body

  if (paymentFor === 'invoice')
    return await addInvoiceManualPayment(body, session)
  else if (paymentFor === 'appInvoice')
    return await addDepositInsuranceManualPayment(body, session)
}

const initAfterUpdateProcessForCanceledRefundPayment = async (
  updatePayment,
  session
) => {
  const {
    _id: paymentId,
    amount,
    isManualRefund,
    partnerId = '',
    refundStatus,
    type = '',
    userId // It is assigned to updatedPayment object
  } = updatePayment

  if (type === 'refund' && refundStatus === 'canceled') {
    const oldPayment = await paymentHelper.getPayment({ _id: paymentId }) // Finding without session

    if (size(oldPayment) && oldPayment.refundStatus !== 'canceled') {
      // Creating Log for canceled refund payment
      const options = {
        collectionId: paymentId,
        context: 'payment',
        partnerId,
        userId
      }
      await createPaymentLog('canceled_refund_payment', options, session)

      // Creating Transaction (if isManualRefund) for canceled refund payment
      if (isManualRefund) {
        console.log(
          `=== Creating transaction for canceled refund(manual) payment. paymentId: ${paymentId} ===`
        )
        await appQueueService.createAppQueueForPaymentTransaction(
          { amount: amount * -1, payment: updatePayment },
          session
        )
      }
    }
  }
}

export const cancelEstimatedOrFailedOrManualRefundPayment = async (
  params,
  session
) => {
  const { partnerId, paymentId, refundType, userId } = params

  const cancelRefundPaymentQuery = { _id: paymentId, partnerId }

  if (refundType === 'estimated')
    cancelRefundPaymentQuery.refundStatus = { $in: ['estimated'] }
  else if (refundType === 'failed')
    cancelRefundPaymentQuery.refundStatus = { $in: ['failed'] }
  else cancelRefundPaymentQuery.refundStatus = { $in: ['completed'] }

  const refundPaymentInfo =
    (await paymentHelper.getPayment(cancelRefundPaymentQuery)) || {}

  if (!size(refundPaymentInfo)) return false

  const { paymentId: originalPaymentId, refundPaymentStatus = '' } =
    refundPaymentInfo
  // OriginalPaymentId => is the ID of the main payment, which has been refunded
  if (originalPaymentId && refundPaymentStatus !== 'paid') {
    const originalPaymentInfo = await paymentHelper.getPayment({
      _id: originalPaymentId,
      partnerId
    })

    if (!size(originalPaymentInfo)) return false // Every refund payment must have the original payment data

    if (indexOf(originalPaymentInfo.refundPaymentIds, paymentId) !== -1) {
      const refundPaymentIds = pull(
        originalPaymentInfo.refundPaymentIds,
        paymentId
      )
      const refundedAmount = await appHelper.convertTo2Decimal(
        originalPaymentInfo.refundedAmount - refundPaymentInfo.amount
      )
      const refundedMeta = partition(originalPaymentInfo.refundedMeta, {
        refundPaymentId: paymentId
      })[1]

      const partiallyRefunded = refundedAmount > 0

      const refunded = originalPaymentInfo.amount + refundedAmount === 0

      const query = { _id: paymentId, partnerId }
      const updateData = { $set: { refundStatus: 'canceled' } }

      if (refundType === 'estimated') query.refundStatus = 'estimated'
      else if (refundType === 'failed') query.refundStatus = 'failed'
      else {
        query.refundStatus = 'completed'
        updateData['$unset'] = { refundPaymentStatus: 1 }
      }
      //  Refund payment updating with canceled refundStatus
      const updatedRefundPayment = await updateAPayment(
        query,
        updateData,
        session
      )
      if (userId) updatedRefundPayment.userId = userId
      if (size(updatedRefundPayment)) {
        // Creating Log and Transaction (if isManualRefund) for canceled refund payment
        await initAfterUpdateProcessForCanceledRefundPayment(
          updatedRefundPayment,
          session
        )
        // Undo all refund related fields from the original payment of refund payment
        const updatedOriginalPayment = await updateAPayment(
          {
            _id: originalPaymentId,
            partnerId
          },
          {
            $set: {
              partiallyRefunded,
              refunded,
              refundedAmount,
              refundedMeta,
              refundPaymentIds
            }
          },
          session
        )

        return size(updatedOriginalPayment) ? updatedRefundPayment : false
      } else return false
    }
  } else return false
}

export const removeAPayment = async (query, session) => {
  if (!size(query))
    throw new CustomError(404, 'Query not found to remove a payment')

  const response = await InvoicePaymentCollection.findOneAndRemove(query, {
    new: true,
    session
  })

  return response
}

export const removePayments = async (query, session) => {
  if (!size(query))
    throw new CustomError(404, 'Query not found to remove payments')

  const response = await InvoicePaymentCollection.deleteMany(query).session(
    session
  )

  return response
}

export const getInvoicePaidTotal = async (params, session) => {
  try {
    const { paymentId, invoiceId, isAllCalculate = false } = params

    let total = 0
    console.log('getInvoicePaidTotal')
    console.log('paymentId', paymentId)
    console.log('invoiceId', invoiceId)
    console.log('isAllCalculate', isAllCalculate)
    if (paymentId && invoiceId) {
      const paymentQuery = {
        invoices: { $elemMatch: { invoiceId } },
        type: 'payment'
      }

      if (!isAllCalculate) paymentQuery._id = { $ne: paymentId }
      console.log('isAllCalculate', paymentQuery)
      const payments = await InvoicePaymentCollection.find(paymentQuery, {
        invoices: { $elemMatch: { invoiceId } }
      }).session(session)
      console.log('payments', payments)
      for (const payment of payments) {
        if (size(payment.invoices) && payment?.invoices[0]) {
          console.log('pre total', total)
          total += payment.invoices[0].amount || 0
          console.log('post total', total)
        }
      }
    }

    return total
  } catch (err) {
    console.log('Catch error', err)
  }
}

const getInvoiceTotalPaidAndUpdateRemainingAmount = async (
  invoice,
  session
) => {
  const {
    _id: invoiceId,
    creditedAmount,
    invoiceTotal,
    lostMeta
  } = invoice || {}
  const payments = await paymentHelper.getPaymentsWithProjection(
    {
      invoices: { $elemMatch: { invoiceId } },
      type: 'payment'
    },
    session,
    {
      projection: { invoices: { $elemMatch: { invoiceId } } },
      sort: { paymentDate: 1 }
    }
  )

  // Invoice total paid will be updated for each payment
  let invoiceTotalPaid = 0

  let invoicePayableAmount =
    invoiceTotal - (lostMeta?.amount || 0) + (creditedAmount || 0)
  if (size(payments)) {
    for (const payment of payments) {
      const { _id: paymentId, invoices } = payment || {}
      const { amount: paymentAmount, remaining: paymentRemaining } =
        head(invoices) || {} // Here we have only one element of invoices by projection

      // Adding payment amount to invoice total paid
      invoiceTotalPaid += paymentAmount || 0

      // Trying to set or remove remaining amount
      let updatingRemainingAmount = 0

      const remaining = paymentAmount - invoicePayableAmount
      if (remaining > 0 && remaining <= paymentAmount) {
        updatingRemainingAmount = await appHelper.convertTo2Decimal(remaining)
      }

      const paymentUpdatingData = {}
      if (updatingRemainingAmount) {
        paymentUpdatingData['$set'] = {
          'invoices.$.remaining': updatingRemainingAmount
        }
      } else if (paymentRemaining) {
        paymentUpdatingData['$unset'] = { 'invoices.$.remaining': 1 }
      }
      if (size(paymentUpdatingData)) {
        const updatedPayment = await updateAPayment(
          { _id: paymentId, 'invoices.invoiceId': invoiceId },
          paymentUpdatingData,
          session
        )
        if (!updatedPayment?._id) {
          throw new CustomError(
            405,
            'Could not update payment remaining amount!'
          )
        }

        console.log(
          '====> Checking payment remaining updating status, isPaymentUpdated:',
          !!updatedPayment?._id,
          ', paymentUpdatingData:',
          updatedPayment?.invoices,
          ', invoiceId:',
          invoiceId,
          ', paymentId:',
          paymentId,
          '<===='
        )
      }

      if (invoicePayableAmount - paymentAmount < 0) invoicePayableAmount = 0
      else invoicePayableAmount -= paymentAmount
    }
  }

  return invoiceTotalPaid
}

const updateInvoiceForPaymentRemove = async (payment, session) => {
  const { _id: removedPaymentId, invoices = [], partnerId } = payment
  console.log(
    `=== Removing paid amount from invoices for removedPaymentId: ${removedPaymentId} ===`
  )

  if (!partnerId) {
    console.log(
      `=== Don't have partnerId in this payment. removedPaymentId: ${removedPaymentId} ===`
    )
    return false
  }

  if (size(invoices)) {
    for (const invoice of invoices) {
      const { invoiceId } = invoice
      const oldInvoice = await invoiceHelper.getInvoice(
        { _id: invoiceId },
        null
      )
      const invoicePaymentsTotal =
        await getInvoiceTotalPaidAndUpdateRemainingAmount(oldInvoice, session)
      console.log('invoicePaymentsTotal', invoicePaymentsTotal)
      const totalPaid = await appHelper.convertTo2Decimal(invoicePaymentsTotal)

      console.log(
        `=== Updated invoice totalPaid with ${totalPaid} for invoiceId: ${invoiceId} ===`
      )

      await invoiceService.startAfterProcessForInvoiceTotalPaidChange(
        { oldInvoice, newTotalPaid: totalPaid },
        session
      )
    }
  } else
    console.log(
      `=== Don't have invoices info to remove paid amount for this payment. removedPaymentId: ${removedPaymentId} ===`
    )
}

export const initAfterRemoveProcessOfPayment = async (
  removedPayments,
  session
) => {
  for (const payment of removedPayments) {
    const { _id: removedPaymentId, amount, partnerId, status, type } = payment
    if (removedPaymentId && partnerId && type === 'payment') {
      // Removing paid amount from invoice totalPaid
      await updateInvoiceForPaymentRemove(payment, session)

      if (status === 'registered') {
        // Remove payment transaction
        console.log(
          `=== Creating payment remove transaction. removedPaymentId: ${removedPaymentId} ===`
        )
        await appQueueService.createAppQueueForPaymentTransaction(
          { action: 'remove', amount: amount * -1, payment },
          session
        )
        // End of payment transaction
      }
    }
  }
}

const removeInvoicePayment = async (params, session) => {
  const { partnerId, paymentId, userId } = params
  appHelper.checkRequiredFields(['partnerId'], params)
  // Only partner admin can do this request
  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  )
    throw new CustomError(400, 'Permission denied')

  const payment =
    (await paymentHelper.getPayment({ _id: paymentId, partnerId })) || {}

  if (!size(payment)) throw new CustomError(404, `Payment doesn't exists`)

  const {
    invoiceId,
    isManualRefund,
    paymentType,
    refundPaymentIds,
    refundStatus,
    type
  } = payment

  if (
    invoiceId &&
    (await paymentHelper.isDoneFinalSettlementBasedOnInvoiceId(
      invoiceId,
      partnerId
    ))
  )
    throw new CustomError(405, 'Final settlement is done for this payment')

  let removablePaymentIds = [paymentId]
  console.log('removablePaymentIds', removablePaymentIds)
  console.log('refundPaymentIds', refundPaymentIds)
  if (type === 'payment' && size(refundPaymentIds)) {
    // Don't remove original payment info if refund payment already in_progress or paid to tenant
    const isRefundStatusPaidOrInProgress = !!(await paymentHelper.getPayment({
      _id: { $in: refundPaymentIds },
      paymentType: 'bank',
      refundStatus: { $in: ['in_progress', 'completed'] }
    }))

    if (isRefundStatusPaidOrInProgress) {
      throw new CustomError(
        405,
        "Can't remove this payment. It has been completed or completing soon the refund process"
      )
    }
    removablePaymentIds = union(removablePaymentIds, refundPaymentIds)
    console.log('removablePaymentIds', removablePaymentIds)
  } else if (type === 'refund') {
    // Don't remove refund payment info if refund payment already in_progress or paid to tenant
    if (
      paymentType === 'bank' &&
      indexOf(['in_progress', 'completed'], refundStatus) !== -1
    ) {
      throw new CustomError(
        405,
        "Can't remove this payment. It has been completed or completing soon the refund process"
      )
    }

    // Revert refund payment info to original payment
    const params = { partnerId, paymentId, userId }
    let isCanceledRefundPayment = false

    if (isManualRefund) {
      params.refundType = 'manual_refund'
      isCanceledRefundPayment =
        !!(await cancelEstimatedOrFailedOrManualRefundPayment(params, session))
    } else {
      params.refundType = refundStatus
      isCanceledRefundPayment =
        !!(await cancelEstimatedOrFailedOrManualRefundPayment(params, session))
    }
    // Don't remove refund payment if refund payment info not moved to original payment
    if (!isCanceledRefundPayment) {
      throw new CustomError(
        405,
        "Can't remove this payment. This refund payment not moved to original payment"
      )
    }
  }

  // Remove payments and create payment removed log
  console.log(
    `=== Removing payments for removablePaymentIds: ${removablePaymentIds} ===`
  )
  const removablePayments = await paymentHelper.getPayments(
    { _id: { $in: removablePaymentIds } },
    session
  )

  const { deletedCount } = await removePayments(
    { _id: { $in: removablePaymentIds } },
    session
  )

  // Creating payment removed log
  if (size(removablePayments) === deletedCount) {
    console.log(
      `=== Payments removed for removablePaymentIds: ${removablePaymentIds} ===`
    )
    // Preparing Log Creation data for main payment (Not refund payment)
    const removedPaymentLogData = await paymentHelper.getRemovedPaymentLogData(
      paymentId,
      partnerId
    )

    if (size(removedPaymentLogData)) {
      console.log(
        `=== Found removedPaymentLogData, Crating payment removing log ===`
      )
      const options = { logData: removedPaymentLogData, partnerId, userId }
      await createPaymentLog('removed_payment', options, session)
    }

    await initAfterRemoveProcessOfPayment(removablePayments, session)
    return payment
  } else throw new CustomError(404, 'Unable to remove payments')
}

const removeDepositInsurancePayment = async (params, session) => {
  const { paymentId, userId } = params
  // Only app admin can do this request
  if (!(await appPermission.isAppAdmin(userId)))
    throw new CustomError(400, 'Permission denied')
  const payment = await paymentHelper.getPayment({ _id: paymentId })
  if (!size(payment)) throw new CustomError(404, 'Payment not found')
  const { amount, appInvoiceId } = payment

  if (appInvoiceId) {
    const appInvoice = await appInvoiceHelper.getAppInvoice({
      _id: appInvoiceId
    })
    if (!size(appInvoice)) throw new CustomError(404, 'Invoice not found')
    const { totalPaid } = appInvoice
    const newTotalPaid = totalPaid - amount
    const updatedAppInvoice = await appInvoiceService.updateAppInvoice(
      { _id: appInvoiceId },
      { $set: { totalPaid: newTotalPaid } },
      session
    )
    if (size(updatedAppInvoice)) {
      console.log(
        "=== Remove totalPaid from appInvoice based on removed payment's amount ==="
      )
      await appInvoiceService.initAfterUpdateProcessForAppInvoice(
        appInvoice,
        updatedAppInvoice,
        session
      )
    }
  }
  const removedDIPayment = await removeAPayment({ _id: paymentId }, session)

  if (!size(removedDIPayment))
    throw new CustomError(404, 'Unable to remove this payment')

  console.log(
    `=== Removed deposit insurance payment. removedDIPaymentId: ${removedDIPayment._id} ===`
  )

  await depositInsuranceService.removePaymentInfoBasedOnPayment(
    removedDIPayment,
    session
  )

  return removedDIPayment
}

export const removePayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { roles, partnerId, userId } = user
  appHelper.checkUserId(userId)

  if (!roles.includes('lambda_manager')) {
    body.userId = userId
    if (body.paymentFor === 'invoice') body.partnerId = partnerId
  }

  appHelper.checkRequiredFields(['paymentFor', 'paymentId', 'userId'], body)

  const { paymentFor } = body

  if (paymentFor === 'invoice') return await removeInvoicePayment(body, session)
  else if (paymentFor === 'appInvoice')
    return await removeDepositInsurancePayment(body, session)
}

export const cancelRefundPayment = async (req = {}) => {
  const { body, session, user = {} } = req

  const { roles } = user
  appHelper.checkUserId(user.userId)

  if (!roles.includes('lambda_manager')) {
    body.userId = user.userId
    body.partnerId = user.partnerId
  }

  appHelper.checkRequiredFields(
    ['partnerId', 'paymentId', 'refundType', 'userId'],
    body
  )

  const { partnerId, paymentId, refundType, userId } = body

  // Only partner admin can do this request
  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  )
    throw new CustomError(400, 'Permission denied')

  const paymentQuery = { _id: paymentId, partnerId }

  refundType === 'manual'
    ? (paymentQuery.refundStatus = 'completed')
    : (paymentQuery.refundStatus = { $in: ['estimated', 'failed'] })

  const payment = (await paymentHelper.getPayment(paymentQuery)) || {}

  if (!size(payment)) throw new CustomError(404, `Payment doesn't exists`)

  const { invoiceId, refundStatus } = payment

  if (
    invoiceId &&
    (await paymentHelper.isDoneFinalSettlementBasedOnInvoiceId(
      invoiceId,
      partnerId
    ))
  )
    throw new CustomError(405, 'Final settlement is done for this payment')

  console.log(
    `=== Canceling refund payment from refundStatus: ${refundStatus} ===`
  )
  const canceledPayment = await cancelEstimatedOrFailedOrManualRefundPayment(
    { partnerId, paymentId, refundType: refundStatus, userId },
    session
  )
  if (size(canceledPayment)) return canceledPayment
  else throw new CustomError(405, 'Unable to cancel this refund payment')
}

const updateMainPaymentBasedOnNewRefundPayment = async (
  refundPaymentInfo,
  session
) => {
  const { paymentId: mainPaymentId } = refundPaymentInfo

  const mainPaymentInfo = await paymentHelper.getPayment(
    { _id: mainPaymentId },
    session
  ) // Getting Main Payment Information

  if (!size(mainPaymentInfo))
    throw new CustomError(404, 'Main payment does not exist')

  const {
    refundedAmount = 0,
    refundedMeta = [],
    refundPaymentIds = []
  } = mainPaymentInfo

  const previousRefundedAmount = refundedAmount + refundPaymentInfo.amount

  refundPaymentIds.push(refundPaymentInfo._id)

  refundedMeta.push({
    refundPaymentId: refundPaymentInfo._id,
    amount: refundPaymentInfo.amount,
    refundedAt: refundPaymentInfo.paymentDate
  })

  const mainPaymentUpdatingData = {
    refundedAmount: previousRefundedAmount,
    refundedMeta,
    refundPaymentIds
  }

  if (previousRefundedAmount + mainPaymentInfo.amount === 0) {
    mainPaymentUpdatingData.refunded = true
    mainPaymentUpdatingData.partiallyRefunded = false
  } else {
    mainPaymentUpdatingData.partiallyRefunded = true
    mainPaymentUpdatingData.refunded = false
  }
  console.log(
    `=== Updating Main paymentInfo based on created refund payment. mainPaymentUpdatingData: ${JSON.stringify(
      mainPaymentUpdatingData
    )} ===`
  )
  const updatedMainPaymentInfo = await updateAPayment(
    { _id: mainPaymentId },
    { $set: { ...mainPaymentUpdatingData } },
    session
  )
  if (!size(updatedMainPaymentInfo))
    throw new CustomError(404, 'Unable to update main payment info')

  const { partnerId, status, type } = updatedMainPaymentInfo
  console.log('type: ', type)
  console.log('status: ', status)
  console.log('partnerId: ', partnerId)
  if (type === 'payment' && partnerId && status === 'registered') {
    console.log('updateInvoiceTotalPaidAndPaymentInvoiceArray')
    await updateInvoiceTotalPaidAndPaymentInvoiceArray(
      mainPaymentInfo,
      updatedMainPaymentInfo,
      session
    )
  }
}

const getCompletedRefundTotal = async (refundMetas, session) => {
  let totalRefundAmount = 0

  if (size(refundMetas)) {
    const refundPaymentIds = map(refundMetas, 'refundPaymentId')

    if (!size(refundPaymentIds)) return totalRefundAmount

    const pipeline = [
      {
        $match: {
          _id: { $in: refundPaymentIds },
          type: 'refund',
          refundStatus: 'completed',
          refundPaymentStatus: 'paid'
        }
      },
      { $group: { _id: null, totalRefundAmount: { $sum: '$amount' } } }
    ]

    const [response] = await InvoicePaymentCollection.aggregate(
      pipeline
    ).session(session)

    totalRefundAmount = size(response) ? response.totalRefundAmount : 0
  }

  return totalRefundAmount
}

export const updatePaymentInvoicesArrayForCompletedRefund = async (
  refundPayment,
  session
) => {
  if (size(refundPayment) && refundPayment.paymentId) {
    // First fetch the main payment data
    const paymentData = await paymentHelper.getPayment(
      { _id: refundPayment.paymentId },
      session
    )
    if (!size(paymentData))
      throw new CustomError(404, 'Main payment does not exist')

    // From refundMeta, get the amount that has to be subtracted from the payment amount, if the refund is (paid + completed)
    const completedRefundTotal = await getCompletedRefundTotal(
      paymentData.refundedMeta,
      session
    )
    // Addition of the negative refund payment will ultimately result in subtraction from payment amount
    const netPaymentAmount = paymentData.amount + completedRefundTotal
    // Form a new invoice array for that payment
    if (netPaymentAmount < paymentData.amount) {
      let newInvoicesArray = []
      let currentPaymentAmount = netPaymentAmount
      const { _id: mainPaymentId, invoices, partnerId, refunded } = paymentData

      for (const invoice of invoices) {
        const amount = invoice.amount
        let newAmount = 0

        if (
          currentPaymentAmount > 0 &&
          amount > 0 &&
          currentPaymentAmount >= amount
        ) {
          newAmount = amount
        } else if (
          currentPaymentAmount > 0 &&
          amount > 0 &&
          currentPaymentAmount < amount
        ) {
          newAmount = currentPaymentAmount
        }
        if (newAmount > 0) {
          const invObj = {
            invoiceId: invoice.invoiceId,
            amount: await appHelper.convertTo2Decimal(newAmount)
          }
          currentPaymentAmount = currentPaymentAmount - newAmount

          newInvoicesArray.push(invObj)
        } else if (newAmount === 0) {
          newInvoicesArray.push({
            invoiceId: invoice.invoiceId,
            amount: 0
          })
        }
      }
      if (
        size(invoices) === 1 &&
        !size(newInvoicesArray) &&
        netPaymentAmount === 0 &&
        refunded
      ) {
        invoices[0].amount = 0
        delete invoices[0].remaining
        newInvoicesArray = invoices
      }
      // Update the invoice payment collection with that invoice array
      if (size(newInvoicesArray)) {
        console.log(
          `=== Updating invoices array in payment newInvoicesArray: ${JSON.stringify(
            newInvoicesArray
          )} ===`
        )
        const updatedPaymentInfo = await updateAPayment(
          { _id: mainPaymentId, partnerId },
          { $set: { invoices: newInvoicesArray } },
          session
        )
        await updateInvoiceTotalPaidAndPaymentInvoiceArray(
          paymentData, // Here paymentData is the oldPayment Bcoz it is updated above
          updatedPaymentInfo.toObject(),
          session
        )
      }
    }
  }
}

export const checkFinalSettlementProcessAndUpdateContractFinalSettlementStatus =
  async (refundPayment, session) => {
    if (!size(refundPayment))
      throw new CustomError(404, 'Payment data not found')

    const { contractId = '', partnerId = '' } = refundPayment
    console.log(
      '====> Checking params for updating final settlement, params:',
      { contractId, partnerId },
      '<===='
    )
    if (contractId && partnerId) {
      const { terminatedByUserId = '' } =
        (await contractHelper.getAContract(
          {
            _id: contractId,
            partnerId
          },
          session
        )) || {}
      const isManualFinalSettlement = !!terminatedByUserId
      console.log(
        '====> Checking isManualFinalSettlement for final settlement, isManualFinalSettlement:',
        isManualFinalSettlement,
        '<===='
      )

      const contractQuery =
        await contractHelper.getContractQueryForFinalSettlement({
          contractId,
          isManualFinalSettlement,
          partnerId
        })
      console.log(
        '====> Checking contract query for final settlement, contractQuery:',
        contractQuery,
        '<===='
      )
      const contract = size(contractQuery)
        ? await contractHelper.getAContract(contractQuery, session)
        : {}
      if (size(contract)) {
        console.log(
          '====> Found contract for final settlement query, contractId:',
          contract?._id,
          '<===='
        )
        await finalSettlementService.checkProcessAndChangeFinalSettlementStatusToCompleted(
          contract,
          session
        )
      }
    }
  }

export const initAfterInsertProcessOfRefundPayment = async (
  refundPayment,
  session
) => {
  if (!size(refundPayment))
    throw new CustomError(404, 'Refund payment not found')

  const {
    isManualRefund,
    partnerId,
    paymentId,
    refundPaymentStatus,
    refundStatus,
    type
  } = refundPayment

  if (partnerId && paymentId && type === 'refund') {
    // Adding refundPayment related fields in the main payment
    await updateMainPaymentBasedOnNewRefundPayment(refundPayment, session)
    // Checking only if the refund is manual for invoice refund payment
    if (
      isManualRefund &&
      refundPaymentStatus === 'paid' &&
      refundStatus === 'completed'
    ) {
      // Update invoice total for manual refund payment
      await updatePaymentInvoicesArrayForCompletedRefund(refundPayment, session)
      // Creating appQueue for refund payment transaction
      await appQueueService.createAppQueueForPaymentTransaction(
        { payment: refundPayment },
        session
      )
      // Checking final settlement process and updating contract final settlement status
      await checkFinalSettlementProcessAndUpdateContractFinalSettlementStatus(
        refundPayment,
        session
      )
    }
  }
}

export const prepareDataAndCreateRefundPayment = async (params, session) => {
  // Meteor function name:: getRefundPaymentIdAndCreateRefundPayment
  const refundPaymentData =
    await paymentHelper.prepareRefundPaymentCreationData(params)
  if (size(refundPaymentData)) {
    const [insertedPayment] =
      await invoicePaymentService.insertAnInvoicePayment(
        refundPaymentData,
        session
      )
    console.log('refundPaymentData', refundPaymentData)
    if (size(insertedPayment)) {
      const { _id: refundPaymentId, amount, partnerId } = insertedPayment

      console.log(
        `=== Created new refund payment. refundPaymentId: ${refundPaymentId}, amount: ${amount} ===`
      )

      if (partnerId && refundPaymentId) {
        console.log(
          `=== Creating new refund payment creation log. refundPaymentId: ${refundPaymentId}, partnerId: ${partnerId} ===`
        )
        await createPaymentLog(
          'added_new_payment',
          {
            collectionId: refundPaymentId,
            context: 'payment',
            partnerId,
            userId: params?.userId
          },
          session
        )
      }
      await initAfterInsertProcessOfRefundPayment(insertedPayment, session)
      return insertedPayment
    } else
      console.log(
        `=== Failed to create refund payment for paymentId: ${params?.paymentInfo?._id} with refundAmount: ${params?.refundedAmount} ===`
      )
  }
}

export const createRefundPayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { roles } = user
  appHelper.checkUserId(user.userId)

  if (!roles.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }

  appHelper.checkRequiredFields(
    ['partnerId', 'paymentId', 'paymentRefundData', 'userId'],
    body
  )

  const { partnerId, paymentRefundData, paymentId, userId } = body

  if (!size(paymentRefundData))
    throw new CustomError(400, 'Required paymentRefundData')

  const { refundAmount, paymentType } = paymentRefundData

  if (!paymentType)
    throw new CustomError(400, 'Missing paymentType in paymentRefundData')

  if (paymentType === 'bank') {
    appHelper.checkRequiredFields(
      ['refundAmount', 'refundToAccountName', 'refundToAccountNumber'],
      paymentRefundData
    )
    paymentRefundData.refundStatus = 'estimated'
  } else if (paymentType === 'manual') {
    paymentRefundData.isManualRefund = true
    paymentRefundData.refundStatus = 'completed'
    paymentRefundData.refundPaymentStatus = 'paid'
  }

  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  )
    // Only partner admin can do this request
    throw new CustomError(400, 'Permission denied')

  const paymentInfo =
    (await paymentHelper.getPayment({ _id: paymentId, partnerId })) || {}

  if (!size(paymentInfo)) throw new CustomError(404, `Payment doesn't exists`)

  const { amount, invoiceId } = paymentInfo

  if (refundAmount > amount)
    throw new CustomError(
      400,
      'RefundingAmount cannot be greater than the payment amount'
    )

  if (
    invoiceId &&
    (await paymentHelper.isDoneFinalSettlementBasedOnInvoiceId(
      invoiceId,
      partnerId
    ))
  )
    throw new CustomError(405, 'Final settlement is done for this payment')

  return await prepareDataAndCreateRefundPayment(
    { paymentInfo, paymentRefundData, refundedAmount: refundAmount, userId },
    session
  )
}

export const createInvoiceRefundPayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { roles } = user
  appHelper.checkUserId(user.userId)

  if (!roles.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }

  await paymentHelper.validatedUserInputDataForInvoiceRefundPaymentCreationReq(
    body
  )

  const { invoiceId, partnerId, paymentRefundData, userId } = body
  const { refundAmount, paymentType } = paymentRefundData

  let totalRefundedAmount = clone(refundAmount || 0)

  if (invoiceId && partnerId && totalRefundedAmount) {
    const insertedPayments = []

    const payments = await paymentHelper.getPaymentsToRefund(
      invoiceId,
      partnerId,
      totalRefundedAmount
    )

    if (size(payments)) {
      for (const payment of payments) {
        // Find multiple payments and find out the refundable amount from each payment
        let newRefundedAmount = 0
        let newPaymentAmount = payment.amount || 0
        const refundFromInvoiceAmount =
          (await paymentHelper.getInvoiceRefundedAmount(
            invoiceId,
            partnerId,
            payment._id
          )) || 0

        if (paymentType === 'manual') {
          paymentRefundData.isManualRefund = true
          paymentRefundData.refundPaymentStatus = 'paid'
          paymentRefundData.refundStatus = 'completed'
        } else paymentRefundData.refundStatus = 'estimated'

        // Set the amount as the payment amount of that invoice. i.e in payment invoices array
        newPaymentAmount = refundFromInvoiceAmount || 0

        if (totalRefundedAmount > 0 && newPaymentAmount > 0) {
          if (totalRefundedAmount >= newPaymentAmount)
            newRefundedAmount = clone(newPaymentAmount)
          else if (totalRefundedAmount < newPaymentAmount)
            newRefundedAmount = clone(totalRefundedAmount)

          totalRefundedAmount = totalRefundedAmount - clone(newRefundedAmount)
        }

        if (newRefundedAmount > 0) {
          paymentRefundData.refundAmount = clone(newRefundedAmount)

          const paymentInfo = clone(payment)
          const refundedAmount = clone(newRefundedAmount)

          const insertedPayment = await prepareDataAndCreateRefundPayment(
            {
              invoiceId,
              paymentInfo,
              paymentRefundData,
              refundedAmount,
              userId
            },
            session
          )
          if (!size(insertedPayment)) {
            totalRefundedAmount =
              totalRefundedAmount + clone(newRefundedAmount) || 0
          } else insertedPayments.push(insertedPayment)
        }
      }
    }
    return insertedPayments
  } else
    throw new CustomError(
      400,
      'Missing required data to create invoice refund payment'
    )
}

export const updateDIPayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { roles } = user
  appHelper.checkUserId(user.userId)

  if (!roles.includes('lambda_manager')) body.userId = user.userId

  paymentHelper.checkRequiredDataInDIPaymentUpdateReq(body)

  const {
    amount,
    appInvoiceId,
    paymentDate,
    paymentId,
    paymentReason,
    userId
  } = body

  // Only app admin can do this request
  if (!(await appPermission.isAppAdmin(userId)))
    throw new CustomError(400, 'Permission denied')

  let updatedPayment = null

  if (appInvoiceId && paymentId) {
    const payment = await paymentHelper.getPayment({ _id: paymentId })

    if (!size(payment)) throw new CustomError(404, 'Payment does not exists')

    const appInvoice = await appInvoiceHelper.getAppInvoice({
      _id: appInvoiceId
    })
    if (!size(appInvoice)) throw new CustomError(404, 'Invoice does not exists')

    if (appInvoiceId === payment.appInvoiceId) {
      // Updating in same DI invoice and payment
      const updateData = { amount, paymentDate, paymentReason }
      updatedPayment = await updateAPayment(
        { _id: paymentId },
        { $set: updateData },
        session
      )
      console.log(
        `=== Updated payment info for DI. paymentId: ${updatedPayment._id} ===`
      )
      if (size(updatedPayment)) {
        // Updating totalPaid amount and status in appInvoice
        const actualTotalPaidAmount =
          appInvoice.totalPaid - payment.amount + amount
        const updatedAppInvoice = await appInvoiceService.updateAppInvoice(
          { _id: appInvoiceId },
          { $set: { totalPaid: actualTotalPaidAmount } },
          session
        )
        await appInvoiceService.initAfterUpdateProcessForAppInvoice(
          appInvoice,
          updatedAppInvoice,
          session
        )
        // Updating totalPaymentAmount and payment array in deposit insurance
        if (updatedPayment.depositInsuranceId) {
          const depositInsurance =
            await depositInsuranceHelper.getADepositInsurance({
              _id: updatedPayment.depositInsuranceId
            })
          if (size(depositInsurance)) {
            const { totalPaymentAmount, payments = [] } = depositInsurance
            const actualTotalPaymentAmount =
              totalPaymentAmount -
              appInvoice.totalPaid +
              updatedAppInvoice.totalPaid

            // Removing old payment info from payments array
            const newPaymentsArray = payments?.filter(
              (obj) =>
                obj.id !== paymentId && obj.paymentAmount !== payment.amount
            )

            // Adding new payment info in payments array
            newPaymentsArray.push({
              id: updatedPayment._id,
              paymentAmount: updatedPayment.amount,
              paymentDate: updatedPayment.paymentDate
            })
            await depositInsuranceService.updateADepositInsurance(
              { _id: updatedPayment.depositInsuranceId },
              {
                $set: {
                  totalPaymentAmount: actualTotalPaymentAmount,
                  payments: newPaymentsArray
                }
              },
              session
            )
            console.log(
              `===Updated depositInsurance totalPaymentAmount and updated payments array based on new changes ===`
            )
          }
        }
      }
    } else {
      // Trying to move payment into another DI Invoice (Set payment data for new appInvoiceId)
      let updateData = { amount, appInvoiceId, paymentDate, paymentReason }
      const appInvoiceData = pick(appInvoice, [
        'accountId',
        'agentId',
        'branchId',
        'contractId',
        'propertyId',
        'tenantId',
        'tenants'
      ])

      updateData = assign(updateData, appInvoiceData)

      if (payment.status === 'unspecified' && appInvoiceId) {
        updateData.paymentType = 'manual'
        updateData.status = 'registered'
      }

      updatedPayment = await updateAPayment(
        {
          _id: paymentId
        },
        { $set: updateData },
        session
      )
      console.log(
        `=== Updated payment info for DI. paymentId: ${updatedPayment._id} for new appInvoiceId: ${appInvoiceId} ===`
      )

      if (size(updatedPayment)) {
        // Updating totalPaid amount and status in appInvoice
        const actualTotalPaidAmount = appInvoice.totalPaid + amount
        const updatedAppInvoice = await appInvoiceService.updateAppInvoice(
          { _id: appInvoiceId },
          { $set: { totalPaid: actualTotalPaidAmount } },
          session
        )
        await appInvoiceService.initAfterUpdateProcessForAppInvoice(
          appInvoice,
          updatedAppInvoice,
          session
        )
        // Updating totalPaymentAmount and payment array in deposit insurance
        if (updatedPayment.depositInsuranceId) {
          const oldAppInvoice = await appInvoiceHelper.getAppInvoice({
            _id: payment.appInvoiceId
          })
          const depositInsurance =
            await depositInsuranceHelper.getADepositInsurance({
              _id: updatedPayment.depositInsuranceId
            })
          if (size(depositInsurance)) {
            const { totalPaymentAmount, payments = [] } = depositInsurance
            const actualTotalPaymentAmount =
              totalPaymentAmount -
              oldAppInvoice.totalPaid +
              updatedAppInvoice.totalPaid

            // Removing old payment info from payments array
            const newPaymentsArray = payments?.filter(
              (obj) =>
                obj.id !== payment._id && obj.paymentAmount !== payment.amount
            )
            // Adding new payment info in payments array
            newPaymentsArray.push({
              id: updatedPayment._id,
              paymentAmount: updatedPayment.amount,
              paymentDate: updatedPayment.paymentDate
            })
            await depositInsuranceService.updateADepositInsurance(
              { _id: updatedPayment.depositInsuranceId },
              {
                $set: {
                  totalPaymentAmount: actualTotalPaymentAmount,
                  payments: newPaymentsArray
                }
              },
              session
            )
            console.log(
              `===Updated depositInsurance totalPaymentAmount and updated payments array based on new changes ===`
            )
          }
        }
      }
    }
  }
  return updatedPayment
}

export const updateBankRefundPayment = async (req = {}) => {
  const { body, session, user = {} } = req
  const { roles } = user
  appHelper.checkUserId(user.userId)

  if (!roles.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }
  paymentHelper.checkRequiredDataInRefundPaymentUpdateReq(body)
  const {
    partnerId,
    paymentId,
    refundToAccountName,
    refundToAccountNumber,
    userId
  } = body

  // Only partner admin can do this request
  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  )
    throw new CustomError(400, 'Permission denied')

  const payment = await paymentHelper.getPayment({ _id: paymentId, partnerId })

  if (!size(payment)) throw new CustomError(404, 'Payment does not exists')

  const { invoiceId, refundPaymentStatus, refundStatus, type } = payment

  if (type !== 'refund')
    throw new CustomError(405, 'Payment must be refund type')

  if (!(refundStatus === 'estimated' || refundStatus === 'failed'))
    throw new CustomError(
      405,
      "Payment refundStatus must be 'estimated' or 'failed'"
    )

  if (refundPaymentStatus === 'paid')
    throw new CustomError(405, "You can't update 'paid' refund payments")

  if (
    await paymentHelper.isDoneFinalSettlementBasedOnInvoiceId(
      invoiceId,
      partnerId
    )
  )
    throw new CustomError(405, 'Final settlement is done for this payment')

  if (!payment.refundToAccountNumber)
    throw new CustomError(405, 'Missing refundToAccountNumber in this payment')

  const updatingData = {}

  if (refundToAccountName)
    updatingData.refundToAccountName = refundToAccountName

  if (refundToAccountNumber)
    updatingData.refundToAccountNumber = refundToAccountNumber

  const updatedPayment = await updateAPayment(
    { _id: paymentId },
    { $set: updatingData },
    session
  )
  console.log(
    `=== Updated refund payment info. paymentId: ${updatedPayment._id} ===`
  )

  return updatedPayment
}

const checkAndCreatePaymentTransactionsForManualPaymentUpdate = async (
  oldPayment,
  updatedPayment,
  session
) => {
  if (!(oldPayment.type === 'payment' || updatedPayment.type === 'payment'))
    throw new CustomError(404, 'Invalid payment type found')

  if (
    size(oldPayment.invoices) &&
    ((oldPayment.invoiceId &&
      updatedPayment.invoiceId !== oldPayment.invoiceId) ||
      (oldPayment.tenantId && oldPayment.tenantId !== updatedPayment.tenantId))
  ) {
    console.log(
      '====> Matched invoiceId or tenantId updating cond for creating payment transaction, paymentData:',
      {
        oldTenantId: oldPayment.amount,
        newTenantId: updatedPayment.tenantId,
        oldInvoiceId: oldPayment.invoiceId,
        newInvoiceId: updatedPayment.invoiceId
      },
      '<===='
    )
    // Creating transaction for old payment amount, if invoiceId or tenantId has been changed
    await appQueueService.createAppQueueForPaymentTransaction(
      { action: 'update', amount: oldPayment.amount * -1, payment: oldPayment },
      session
    )

    // Creating transaction for new payment amount, if invoiceId or tenantId has been changed
    await appQueueService.createAppQueueForPaymentTransaction(
      { amount: updatedPayment.amount, payment: updatedPayment },
      session
    )
  } else if (
    updatedPayment.status === 'registered' &&
    updatedPayment.amount !== oldPayment.amount
  ) {
    console.log(
      '====> Matched amount updating cond for creating payment transaction, amount',
      {
        oldAmount: oldPayment.amount,
        newAmount: updatedPayment.amount
      },
      '<===='
    )
    let transactionAmount = updatedPayment.amount - oldPayment.amount

    if (oldPayment.status === 'unspecified')
      transactionAmount = updatedPayment.amount

    // Creating transaction for amount updated on a registered payment
    await appQueueService.createAppQueueForPaymentTransaction(
      {
        amount: transactionAmount,
        isDifferentAmount: true,
        payment: updatedPayment
      },
      session
    )
  } else if (
    updatedPayment.status === 'registered' &&
    oldPayment.status !== 'registered'
  ) {
    console.log(
      '====> Matched status updating cond for creating payment transaction, status:',
      {
        oldStatus: oldPayment.status,
        newStatus: updatedPayment.status
      },
      '<===='
    )
    // Creating transaction for status updated to registered
    await appQueueService.createAppQueueForPaymentTransaction(
      {
        amount: updatedPayment.amount,
        payment: updatedPayment
      },
      session
    )
  }
}

const checkAndCreatePaymentTransactionsForMatchPayments = async (
  oldPayment,
  updatedPayment,
  session
) => {
  if (!(oldPayment.type === 'payment' || updatedPayment.type === 'payment'))
    throw new CustomError(404, 'Invalid payment type found')

  if (!(oldPayment.invoiceId && updatedPayment.invoiceId)) {
    throw new CustomError(
      400,
      'InvoiceId is missing for creating payment transaction'
    )
  }

  if (!size(oldPayment.invoices) && size(updatedPayment.invoices)) {
    console.log(
      '====> Matched invoices array adding cond for creating payment transaction, invoices:',
      {
        oldInvoices: oldPayment.invoices,
        newInvoices: updatedPayment.invoices
      },
      '<===='
    )
    await appQueueService.createAppQueueForPaymentTransaction(
      {
        amount: updatedPayment.amount,
        payment: updatedPayment
      },
      session
    )
  } else if (oldPayment.invoiceId !== updatedPayment.invoiceId) {
    console.log(
      '====> Matched invoiceId updating cond for creating payment transaction, invoiceId:',
      {
        oldInvoiceId: oldPayment.invoiceId,
        newInvoiceId: updatedPayment.invoiceId
      },
      '<===='
    )
    await appQueueService.createAppQueueForPaymentTransaction(
      { action: 'update', amount: oldPayment.amount * -1, payment: oldPayment },
      session
    )
    await appQueueService.createAppQueueForPaymentTransaction(
      { amount: updatedPayment.amount, payment: updatedPayment },
      session
    )
  }

  return true
}

const updateInvoicesTotalPaid = async (params, session) => {
  const { payment, paymentInvoices, type } = params
  console.log('updateInvoicesTotalPaid', 'type', type)

  if (size(payment) && size(paymentInvoices)) {
    const { _id: paymentId, paymentDate } = payment

    for (const invoice of paymentInvoices) {
      const { invoiceId } = invoice
      const invoiceUpdateData = { lastPaymentDate: paymentDate }
      let invoicePaymentsTotal = 0

      if (type === 'addPayment') {
        // Calculate invoice paid total
        invoicePaymentsTotal = await getInvoicePaidTotal(
          {
            paymentId,
            invoiceId,
            isAllCalculate: true
          },
          session
        )

        // Added payment
        invoiceUpdateData.totalPaid = await appHelper.convertTo2Decimal(
          invoicePaymentsTotal || 0
        )
      } else if (type === 'editPayment') {
        invoicePaymentsTotal = await getInvoicePaidTotal(
          { paymentId, invoiceId },
          session
        )
        // Edited payment
        invoiceUpdateData.totalPaid = await appHelper.convertTo2Decimal(
          (invoice.amount || 0) + invoicePaymentsTotal
        )
      } else if (type === 'removePayment') {
        // Calculate invoice paid total
        invoicePaymentsTotal = await getInvoicePaidTotal(
          {
            paymentId,
            invoiceId,
            isAllCalculate: true
          },
          session
        )
        // Removed totalPaid amount
        invoiceUpdateData.totalPaid = await appHelper.convertTo2Decimal(
          invoicePaymentsTotal || 0
        )
      }
      console.log('TotalPaid updating data', invoiceUpdateData)
      if (size(invoiceUpdateData)) {
        const { lastPaymentDate, totalPaid = 0 } = invoiceUpdateData
        if (
          totalPaid ||
          (type === 'removePayment' && !totalPaid) ||
          (!totalPaid && payment.refunded) ||
          (type === 'editPayment' && !totalPaid && payment.partiallyRefunded)
        ) {
          const oldInvoice = await invoiceHelper.getInvoice(
            { _id: invoiceId },
            session
          )
          if (!size(oldInvoice)) throw new CustomError(404, 'Invoice not found')
          console.log('oldInvoice', oldInvoice)
          if (size(oldInvoice)) {
            console.log('New total paid', totalPaid)
            console.log('oldInvoice total paid', oldInvoice.totalPaid)
            await invoiceService.startAfterProcessForInvoiceTotalPaidChange(
              {
                oldInvoice,
                newTotalPaid: totalPaid,
                newLastPaymentDate: lastPaymentDate
              },
              session
            )
          }
        }
      }
    }
  }
}

const revertInvoicesTotalPaid = async (params, session) => {
  const { payment, paymentInvoices, prevInvoices } = params
  console.log('revertInvoicesTotalPaid')
  if (size(payment) && size(paymentInvoices) <= size(prevInvoices)) {
    const paymentInvoiceIds = map(paymentInvoices, 'invoiceId')
    const prevInvoiceIds = map(prevInvoices, 'invoiceId')
    const removeInvoiceIds = difference(prevInvoiceIds, paymentInvoiceIds)
    const revertInvoices = []

    // Prepare invoices array for remove paidTotal from invoice
    if (size(removeInvoiceIds)) {
      for (const invoiceId of removeInvoiceIds) {
        const invoiceObj = find(
          prevInvoices,
          (obj) => obj.invoiceId === invoiceId
        )

        if (invoiceObj) revertInvoices.push(invoiceObj)
      }
      console.log('revertInvoices', revertInvoices)
      //Update invoice totalPaid amount for revert payment
      if (size(revertInvoices)) {
        await updateInvoicesTotalPaid(
          { payment, paymentInvoices: revertInvoices, type: 'removePayment' },
          session
        )
      }
    }
  }
}

const updateInvoicePaymentForOverPaid = async (payment, session) => {
  const { _id: paymentId, amount, invoiceId, partnerId } = payment
  console.log('updateInvoicePaymentForOverPaid')
  let paymentAmount = amount || 0

  if (paymentId && invoiceId && partnerId) {
    const invoiceInfo = await invoiceHelper.getInvoice({
      _id: invoiceId,
      partnerId
    })

    if (!size(invoiceInfo)) return false

    const {
      contractId = '',
      invoiceType,
      isFinalSettlement,
      isNonRentInvoice,
      isPayable,
      status
    } = invoiceInfo

    let paymentInvoices = []

    // Over paid payment is distributed another unpaid invoices
    if (contractId) {
      const unpaidInvoiceQuery = {
        _id: { $nin: [invoiceId] },
        contractId,
        partnerId,
        invoiceType: 'invoice',
        status: { $nin: ['credited', 'paid'] }
      }

      if (
        invoiceType === 'landlord_invoice' &&
        (isFinalSettlement || isPayable)
      ) {
        unpaidInvoiceQuery.invoiceType = 'landlord_invoice'
        if (isFinalSettlement) unpaidInvoiceQuery.isFinalSettlement = true
        if (isPayable) unpaidInvoiceQuery.isPayable = true
      } else {
        unpaidInvoiceQuery.isNonRentInvoice = isNonRentInvoice
          ? true
          : { $ne: true }
      }

      console.log('=== unpaidInvoiceQuery ===', { ...unpaidInvoiceQuery })

      const unPaidInvoices = await invoiceHelper.getInvoices(
        unpaidInvoiceQuery,
        session,
        { sort: { invoiceStartOn: 1 } }
      )

      console.log('=== unPaidInvoices ===', { ...unPaidInvoices })

      // Prepare first invoices array
      const isLastArray = !size(unPaidInvoices)
      console.log('=== invoice status ===', status)
      if (
        (indexOf(['paid', 'credited'], status) !== -1 && isLastArray) ||
        indexOf(['paid', 'credited'], status) === -1
      ) {
        const paymentInvoiceInfo = await paymentHelper.getPaymentInvoiceInfo({
          invoiceInfo,
          paymentAmount: clone(paymentAmount),
          isLastIndex: clone(isLastArray)
        })
        console.log('=== paymentInvoiceInfo 1 ===', { ...paymentInvoiceInfo })
        paymentAmount = paymentInvoiceInfo?.paymentAmount
        paymentInvoices = [paymentInvoiceInfo?.invoice]
      }

      if (size(unPaidInvoices)) {
        // Payment forward each unpaid invoices
        for (const [index, invoiceInfo] of unPaidInvoices.entries()) {
          const isLastIndex = size(unPaidInvoices) === index + 1
          const paymentInvoiceInfo = await paymentHelper.getPaymentInvoiceInfo({
            invoiceInfo,
            paymentAmount: clone(paymentAmount),
            isLastIndex: clone(isLastIndex)
          })
          console.log('=== paymentInvoiceInfo 2 ===', { ...paymentInvoiceInfo })
          if (paymentInvoiceInfo?.paidAmount > 0) {
            paymentAmount = paymentInvoiceInfo.paymentAmount
            paymentInvoices.push(paymentInvoiceInfo.invoice)
          }
        }
      }
    }
    console.log(
      'Doing updateInvoicePaymentForOverPaid with paymentInvoicesArray',
      paymentInvoices
    )
    if (size(paymentInvoices)) {
      const updatedPayment = await updateAPayment(
        { _id: paymentId },
        { $set: { invoices: paymentInvoices } },
        session
      )

      if (!size(updatedPayment))
        throw new CustomError(404, 'Unable to update payment')

      await updateInvoiceTotalPaidAndPaymentInvoiceArray(
        payment,
        updatedPayment.toObject(),
        session
      )
    }
  }
}

const updateInvoicesArrayForEditPayment = async (
  oldPayment,
  updatedPayment,
  session
) => {
  console.log('updateInvoicesArrayForEditPayment')
  if (!size(oldPayment) || !size(updatedPayment)) return false

  const { newInvoicesArray = [] } =
    await paymentHelper.prepareInvoicesArrayDataForEditPayment(
      oldPayment,
      updatedPayment
    )

  const paymentId = updatedPayment._id
  const partnerId = updatedPayment.partnerId
  console.log(
    'updateInvoicesArrayForEditPayment newInvoicesArray',
    newInvoicesArray
  )
  // Update changes payment invoices array
  if (size(newInvoicesArray)) {
    const updatedPaymentInfo = await updateAPayment(
      { _id: paymentId, partnerId },
      { $set: { invoices: newInvoicesArray } },
      session
    )

    if (!size(updatedPaymentInfo))
      throw new CustomError(404, 'Unable to update payment')

    console.log(
      `=== Updated invoicesArray for paymentId: ${updatedPaymentInfo._id} ===`
    )
    await updateInvoiceTotalPaidAndPaymentInvoiceArray(
      updatedPayment, // Here updatedPayment is the oldPayment Bcoz it is updated above
      updatedPaymentInfo.toObject(),
      session
    )
  }
}

const updateInvoiceTotalPaidAndPaymentInvoiceArray = async (
  oldPayment,
  updatedPayment,
  session
) => {
  if (!(oldPayment.type === 'payment' && updatedPayment.type === 'payment'))
    throw new CustomError(404, 'Invalid payment type found')

  if (updatedPayment.status === 'registered') {
    const prevInvoices = oldPayment.invoices
    const docInvoices = updatedPayment.invoices
    const countPreviousRefundedMeta = size(oldPayment.refundedMeta)
    const countDocRefundedMeta = size(updatedPayment.refundedMeta)

    console.log('oldPayment', oldPayment)
    console.log('updatedPayment', updatedPayment)
    console.log('===')
    console.log('prevInvoices', prevInvoices)
    console.log('docInvoices', docInvoices)
    console.log('countPreviousRefundedMeta', countPreviousRefundedMeta)
    console.log('countDocRefundedMeta', countDocRefundedMeta)

    if (
      (updatedPayment.amount === oldPayment.amount &&
        ((!updatedPayment.refunded && !updatedPayment.partiallyRefunded) ||
          ((updatedPayment.refunded || updatedPayment.partiallyRefunded) &&
            countDocRefundedMeta &&
            countDocRefundedMeta === countPreviousRefundedMeta))) ||
      (updatedPayment.amount !== oldPayment.amount &&
        oldPayment.status === 'unspecified')
    ) {
      console.log('1')
      if (!size(prevInvoices) && size(docInvoices)) {
        console.log('1.1')
        await updateInvoicesTotalPaid(
          {
            payment: updatedPayment,
            paymentInvoices: docInvoices,
            type: 'addPayment'
          },
          session
        )
      } else if (!size(docInvoices)) {
        console.log('1.2')
        if (
          updatedPayment.invoiceId !== oldPayment.invoiceId &&
          size(prevInvoices)
        ) {
          console.log('1.2.1')
          // Revert previous invoices total paid amount
          await revertInvoicesTotalPaid(
            {
              payment: updatedPayment,
              paymentInvoices: docInvoices,
              prevInvoices
            },
            session
          )
        }
        await updateInvoicePaymentForOverPaid(clone(updatedPayment), session)
      } else {
        console.log('1.3')
        // If payment amount less than previous payment amount
        const changeInvoices = paymentHelper.getChangedPaymentInvoicesArray(
          docInvoices,
          prevInvoices
        )
        console.log('changeInvoices', changeInvoices)
        // Update changes in invoice totalPaid amount
        await updateInvoicesTotalPaid(
          {
            payment: updatedPayment,
            paymentInvoices: changeInvoices,
            type: 'editPayment'
          },
          session
        )
        // Revert invoices totalPaid amount
        await revertInvoicesTotalPaid(
          {
            payment: updatedPayment,
            paymentInvoices: docInvoices,
            prevInvoices
          },
          session
        )
      }
    } else if (
      updatedPayment.amount === oldPayment.amount &&
      (updatedPayment.refunded || updatedPayment.partiallyRefunded) &&
      countDocRefundedMeta &&
      countDocRefundedMeta < countPreviousRefundedMeta
    ) {
      console.log('2')
      if (size(updatedPayment) && size(docInvoices)) {
        console.log('2.1')
        // Update changes invoices totalPaid amount
        await updateInvoicesTotalPaid(
          {
            payment: updatedPayment,
            paymentInvoices: docInvoices,
            type: 'editPayment'
          },
          session
        )
      }
    } else if (
      updatedPayment.amount !== oldPayment.amount ||
      ((updatedPayment.refunded || updatedPayment.partiallyRefunded) &&
        countDocRefundedMeta !== countPreviousRefundedMeta)
    ) {
      console.log('3')
      if (
        updatedPayment.invoiceId !== oldPayment.invoiceId &&
        !size(docInvoices) &&
        size(prevInvoices)
      ) {
        console.log('3.1')
        // Revert previous invoices total paid amount
        await revertInvoicesTotalPaid(
          {
            payment: updatedPayment,
            paymentInvoices: docInvoices,
            prevInvoices
          },
          session
        )
        await updateInvoicePaymentForOverPaid(clone(updatedPayment), session)
      } else {
        console.log('3.2')
        // Update payment invoices array
        await updateInvoicesArrayForEditPayment(
          oldPayment,
          updatedPayment,
          session
        )
      }
    }
  }
}

const initAfterUpdateProcessForManualPayment = async (
  oldPayment,
  updatedPayment,
  session
) => {
  if (!size(oldPayment)) throw new CustomError(404, 'Missing oldPaymentInfo')
  if (!size(updatedPayment))
    throw new CustomError(404, 'Missing updatedPaymentInfo')

  const { partnerId } = updatedPayment

  if (!partnerId)
    throw new CustomError(404, 'PartnerId does not exists in payment')

  // Create transactions based on payment changes
  await checkAndCreatePaymentTransactionsForManualPaymentUpdate(
    oldPayment,
    updatedPayment,
    session
  )
  console.log(
    'checkAndCreatePaymentTransactionsForManualPaymentUpdate completed'
  )
  // Todo :: Pending invoice after hooks
  // Update invoice's 'totalPaid' & 'lastPaymentDate' and payment's 'invoiceArray'
  // based on 'registered' payment changes
  await updateInvoiceTotalPaidAndPaymentInvoiceArray(
    oldPayment,
    updatedPayment,
    session
  )
}

export const updateManualPayment = async (req) => {
  const { body, session, user = {} } = req
  const { roles, userId } = user
  appHelper.checkUserId(userId)

  if (!roles.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = userId
  }
  paymentHelper.checkRequiredDataInManualPaymentUpdateReq(body)

  const { partnerId, paymentId, paymentDate } = body

  const { data, haveToCreateLog, haveToUpdateTransaction, oldPaymentInfo } =
    await paymentHelper.prepareDataForManualPaymentUpdate(body)

  const updatedPayment = await updateAPayment({ _id: paymentId }, data, session)

  if (!size(updatedPayment))
    throw new CustomError(404, 'Unable to update payment')

  // Payment updating log will be created only if 'payment Date' or 'amount' or 'paymentToAccountNumber' is changed
  if (haveToCreateLog) {
    const options = {
      collectionId: paymentId,
      context: 'payment',
      partnerId,
      previousDoc: oldPaymentInfo,
      userId: body.userId
    }
    await createPaymentLog('updated_payment', options, session)
  }
  // Transactions 'createdAt' & 'period' will be updated, if 'payment Date' is changed
  if (haveToUpdateTransaction) {
    const result = await transactionService.updateTransaction(
      { partnerId, paymentId },
      {
        $set: {
          createdAt: paymentDate,
          period: (
            await appHelper.getActualDate(partnerId, true, paymentDate)
          ).format('YYYY-MM')
        }
      },
      session
    )
    console.log(
      `=== ${result.nModified} Transactions updated for changing payment date on payment ${paymentId}, partner ${partnerId} ===`
    )
  }

  await initAfterUpdateProcessForManualPayment(
    oldPaymentInfo.toObject(),
    updatedPayment.toObject(),
    session
  )

  return updatedPayment
}

export const createRefundPaymentForFinalSettlement = async (
  params = {},
  session
) => {
  const { paymentId, partnerId, refundPaymentData, userId } = params
  let refundedAmount = refundPaymentData.amount || 0
  if (paymentId && partnerId && refundedAmount) {
    const { contractId, propertyId } = refundPaymentData
    let payments = []
    if (contractId && propertyId) {
      payments = await paymentHelper.getPayments(
        {
          contractId,
          partnerId,
          propertyId
        },
        session,
        { createdAt: -1 }
      )
    }
    if (size(payments)) {
      const promiseArr = []
      for (const payment of payments) {
        //find multiple payments and find out the refundable amount from each payment
        let newRefundedAmount = 0
        let newPaymentAmount = payment.amount || 0
        const newPaymentRefundedAmount = payment.refundedAmount || 0
        const newRefundPaymentData = { refundStatus: 'estimated' }

        if (payment.meta && payment.meta.dbTrAccountNumber) {
          newRefundPaymentData.refundToAccountNumber =
            payment.meta.dbTrAccountNumber
        }
        if (payment.meta && payment.meta.dbTrName) {
          newRefundPaymentData.refundToAccountName = payment.meta.dbTrName
        }
        newPaymentAmount = newPaymentAmount + newPaymentRefundedAmount || 0

        if (refundedAmount > 0 && newPaymentAmount > 0) {
          if (refundedAmount >= newPaymentAmount) {
            newRefundedAmount = newPaymentAmount
          } else if (refundedAmount < newPaymentAmount) {
            newRefundedAmount = refundedAmount
          }

          refundedAmount = refundedAmount - newRefundedAmount
        }

        if (newRefundedAmount > 0) {
          newRefundPaymentData.refundAmount = newRefundedAmount
          promiseArr.push(
            prepareDataAndCreateRefundPayment(
              {
                paymentInfo: clone(payment),
                paymentRefundData: clone(newRefundPaymentData),
                refundedAmount: newRefundedAmount,
                userId
              },
              session
            )
          )
        }
      }
      if (size(promiseArr)) await Promise.all(promiseArr)
    }
  }
}

export const linkUnspecifiedPayment = async (req) => {
  const { body, session, user = {} } = req
  const { roles } = user
  appHelper.checkUserId(user.userId)

  if (!roles.includes('lambda_manager')) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }

  appHelper.checkRequiredFields(
    ['invoiceId', 'partnerId', 'paymentId', 'userId'],
    body
  )

  const { invoiceId, partnerId, paymentId, userId } = body
  appHelper.validateId({ partnerId })
  appHelper.validateId({ paymentId })
  appHelper.validateId({ invoiceId })

  if (
    await paymentHelper.isDoneFinalSettlementBasedOnInvoiceId(
      invoiceId,
      partnerId
    )
  )
    throw new CustomError(405, 'Final settlement is done for this invoice')

  if (
    !(
      (await appPermission.isPartnerAdmin(userId, partnerId)) ||
      (await appPermission.isPartnerAccounting(userId, partnerId))
    )
  ) {
    // Only partner admin can do this request
    throw new CustomError(400, 'Permission denied')
  }

  const invoice = await invoiceHelper.getInvoice({ _id: invoiceId, partnerId })
  if (!size(invoice)) throw new CustomError(404, 'Invoice does not exists')
  const payment = await paymentHelper.getPayment({ _id: paymentId, partnerId })
  if (!size(payment)) throw new CustomError(404, 'Payment does not exists')

  if (payment.status !== 'unspecified')
    throw new CustomError(404, 'Invalid payment status found')

  const data = {
    status: 'registered',
    accountId: invoice.accountId,
    agentId: invoice.agentId,
    branchId: invoice.branchId,
    contractId: invoice.contractId,
    invoiceId: invoice._id,
    propertyId: invoice.propertyId,
    tenantId: invoice.tenantId
  }

  const updatedPayment = await updateAPayment(
    { _id: paymentId, partnerId },
    data,
    session
  )

  if (!updatedPayment._id)
    throw new CustomError(404, 'Unable to update payment')

  await initAfterUpdateProcessForManualPayment(
    payment.toObject(),
    updatedPayment.toObject(),
    session
  )

  return updatedPayment
}

export const testIncomingPaymentsForAPartner = async (req) => {
  if (process.env.STAGE === 'production') {
    throw new CustomError(400, 'This action can not be done in production!')
  }

  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)

  appHelper.checkRequiredFields(['partnerId'], body)
  appHelper.validateId({ partnerId: body.partnerId })

  const partner = await partnerHelper.getAPartner(
    { _id: body.partnerId },
    session
  )
  if (!partner?._id) {
    throw new CustomError(400, 'Please provide correct partnerId!')
  }
  if (body?.isAppInvoice && body?.isInvoice) {
    throw new CustomError(400, 'Please provide single type of invoice!')
  }
  if (!(body?.isAppInvoice || body?.isInvoice)) {
    throw new CustomError(400, 'Please provide any type of invoice!')
  }
  if (body?.limit < 1 || body?.limit > 500) {
    throw new CustomError(400, 'Invoice limit range is from 1 to 500!')
  }

  const Ntfctn =
    (await paymentHelper.getPaymentsJSONDataForTestIncomingPayment(body)) || []
  if (!size(Ntfctn)) {
    throw new CustomError(400, 'No invoice found!')
  }

  const messageId = Math.floor(100000 + Math.random() * 90000000000)
  const { organizationId } =
    (await appHelper.getSettingsInfoByFieldName('appInfo')) || {}
  const xmlFileData = {
    Document: {
      '@xmlns:xs': 'http://www.w3.org/2001/XMLSchema',
      '@xmlns': 'urn:iso:std:iso:20022:tech:xsd:pain.001.001.05',
      BkToCstmrDbtCdtNtfctn: {
        GrpHdr: {
          MsgId: messageId,
          CreDtTm: new Date(),
          MsgRcpt: {
            Nm: 'Unite Living AS',
            Id: {
              OrgId: {
                Othr: {
                  Id: organizationId,
                  SchmeNm: { Cd: 'CUST' }
                }
              }
            }
          }
        },
        Ntfctn
      }
    }
  }
  const xmlFileName = `ISO_CAMT054.${messageId}-1.xml`
  const [appQueue] =
    (await appQueueService.createAnAppQueue(
      {
        action: 'prepare_xml_and_send_to_nets_server',
        destination: 'payments',
        event: 'prepare_xml_and_send_to_nets_server',
        params: { ...body, xmlFileData, xmlFileName },
        priority: 'regular'
      },
      session
    )) || []
  return appQueue
}
