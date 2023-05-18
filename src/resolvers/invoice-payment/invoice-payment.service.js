import { assign, compact, find, includes, map, omit, size } from 'lodash'
import nid from 'nid'

import {
  appHelper,
  counterHelper,
  invoicePaymentHelper,
  payoutHelper,
  partnerPayoutHelper,
  partnerSettingHelper
} from '../helpers'
import {
  appQueueService,
  counterService,
  payoutService,
  partnerPayoutService,
  paymentService
} from '../services'
import { CustomError } from '../common'

import { InvoicePaymentCollection } from '../models'

export const createRefundPaymentUpdatedLog = async (
  doc = {},
  session,
  refundPaymentFeedbackHistory = {}
) => {
  if (!size(doc?.partnerId)) return false

  const options = {
    collectionId: doc._id,
    context: 'payment',
    partnerId: doc.partnerId
  }
  if (size(refundPaymentFeedbackHistory))
    options.refundPaymentFeedbackHistory = refundPaymentFeedbackHistory

  return await paymentService.createPaymentLog(
    'updated_refunded_payment',
    options,
    session
  )
}

export const handleFailedRefundPaymentToRetry = async (doc, session) => {
  if (doc?.status && doc?.refundStatus !== 'failed') return false

  const { numberOfFails = 0 } = doc
  const { retryFailedPayouts = {} } =
    (await partnerSettingHelper.getSettingByPartnerId(
      doc.partnerId,
      session
    )) || {}
  const { days, enabled = false } = retryFailedPayouts || {}
  // If partner enabled to retry payout then make refund payment status to estimated
  if (days && days > numberOfFails && enabled) {
    await updateAnInvoicePayment(
      { _id: doc._id },
      {
        $inc: { numberOfFails: 1 },
        $set: {
          refundStatus: 'estimated',
          sentToNETS: false
        }
      },
      session
    )
  }
}

export const initRefundPaymentAfterUpdateProcessForStatus = async (
  doc,
  previous,
  session
) => {
  console.log(
    '====> Checking previous payment data, paymentId:',
    doc?._id,
    ', previous:',
    previous,
    '<===='
  )
  if (!(size(doc) && size(previous))) return false

  if (
    doc.refundStatus &&
    previous?.refundStatus &&
    doc.refundStatus !== previous.refundStatus
  ) {
    if (includes(['completed', 'failed', 'in_progress'], doc.refundStatus)) {
      console.log(
        '====> Refund status is updated so adding log, paymentId:',
        doc?._id,
        '<===='
      )
      await createRefundPaymentUpdatedLog(doc, session)
    }

    if (doc.refundStatus === 'failed') {
      console.log(
        '====> Refund status is updated to failed so handling retrying, paymentId:',
        doc?._id,
        '<===='
      )
      await handleFailedRefundPaymentToRetry(doc, session)
    }
  }

  if (
    doc.refundStatus === 'completed' &&
    doc.refundPaymentStatus === 'paid' &&
    doc.refundPaymentStatus !== previous.refundPaymentStatus &&
    !doc.isManualRefund
  ) {
    console.log(
      '====> Refund payment status is updated so handling after updating processes, paymentId:',
      doc?._id,
      '<===='
    )
    // Update invoice total for manual refund payment
    await paymentService.updatePaymentInvoicesArrayForCompletedRefund(
      doc,
      session
    )
    // Creating appQueue for refund payment transaction
    await appQueueService.createAppQueueForPaymentTransaction(
      { payment: doc },
      session
    )
    // Checking final settlement process and updating contract final settlement status
    await paymentService.checkFinalSettlementProcessAndUpdateContractFinalSettlementStatus(
      doc,
      session
    )
  }

  return true
}

export const updateRefundPaymentStatusByCreditTransferData = async (
  creditTransferData = [],
  session
) => {
  if (!size(creditTransferData))
    throw new CustomError(
      400,
      'CreditTransferData is required to update payout status'
    )

  const updatedRefundPayments = []
  for (const creditTransferObj of creditTransferData) {
    const { bankRef, bookingDate, paymentId, status } = creditTransferObj || {}
    const paymentUpdatingSetData = {}
    if (status === 'ACCP' || status === 'booked') {
      paymentUpdatingSetData.refundStatus = 'completed'
      paymentUpdatingSetData.refundPaymentStatus = bookingDate
        ? 'paid'
        : 'pending'
      if (status === 'booked') paymentUpdatingSetData.bookingDate = bookingDate
      if (bankRef) paymentUpdatingSetData.bankRef = bankRef
    } else if (status === 'RJCT') paymentUpdatingSetData.refundStatus = 'failed'

    console.log(
      '====> Updated refund payment and processing after update works now, paymentId:',
      paymentId,
      ', paymentUpdatingSetData:',
      paymentUpdatingSetData,
      '<===='
    )

    if (paymentId && size(paymentUpdatingSetData)) {
      const previousRefundPayment =
        await invoicePaymentHelper.getInvoicePayment({ _id: paymentId })
      const updatedRefundPayment = await updateAnInvoicePayment(
        { _id: paymentId, bookingDate: { $exists: false } },
        { $set: paymentUpdatingSetData },
        session
      )

      if (updatedRefundPayment?._id) {
        await initRefundPaymentAfterUpdateProcessForStatus(
          updatedRefundPayment,
          previousRefundPayment,
          session
        )
        updatedRefundPayments.push(updatedRefundPayment)
      }
    }
  }

  return updatedRefundPayments
}

export const insertAnInvoicePayment = async (data, session) => {
  const invoicePayment = await InvoicePaymentCollection.create([data], {
    session
  })
  return invoicePayment
}

export const insertInvoicePayments = async (data, session) =>
  await InvoicePaymentCollection.create(data, {
    session
  })

export const updateInvoicePayments = async (query, data, session) => {
  const response = await InvoicePaymentCollection.updateMany(query, data, {
    session
  })
  return response
}

export const updateAnInvoicePayment = async (query, data, session) => {
  const updatedInvoicePayment = await InvoicePaymentCollection.findOneAndUpdate(
    query,
    data,
    {
      new: true,
      runValidators: true,
      session
    }
  )
  return updatedInvoicePayment
}

const preparePaymentInsertData = async (data) => {
  const orArr = []
  for (let i = 0; i < data.length; i++) {
    const { receivedFileName, nodeIndex } = data[i]
    orArr.push({
      receivedFileName,
      nodeIndex
    })
  }
  const existQuery = {
    $or: orArr
  }
  const existingPayments =
    (await invoicePaymentHelper.getInvoicePayments(existQuery)) || []
  if (!size(existingPayments)) {
    return data
  }
  const paymentInsertArr = map(data, (payment) => {
    // Removes payments which already exists
    if (
      !find(existingPayments, {
        receivedFileName: payment.receivedFileName,
        nodeIndex: payment.nodeIndex
      })
    ) {
      return payment
    }
  })
  return compact(paymentInsertArr)
}

const updatePartnerPayoutForApprovePayments = async (params, session) => {
  const { selectedPendingPaymentIds, partnerId, userId } = params

  const partnerPayoutQuery = {
    partnerId,
    type: 'refund_payment',
    status: 'pending_for_approval',
    hasRefundPayments: true,
    paymentIds: { $exists: false }
  }

  const [partnerPayout] = await partnerPayoutHelper.getPartnerPayouts(
    partnerPayoutQuery,
    { sort: { createdAt: -1 }, limit: 1 }
  )
  let partnerPayoutId = ''
  if (size(partnerPayout)) {
    const payoutData = {
      status: 'waiting_for_signature',
      events: {
        createdAt: new Date(),
        status: 'added_refund_payments_ids',
        note: `Added ${selectedPendingPaymentIds} payments for getting approval`
      },
      payoutIds: null,
      hasPayouts: null,
      paymentIds: selectedPendingPaymentIds,
      hasRefundPayments: true
    }
    const updatedPartnerPayout =
      await partnerPayoutService.updateAPartnerPayout(
        { _id: partnerPayout._id },
        payoutData,
        session
      )
    partnerPayoutId = updatedPartnerPayout._id
  } else {
    const partnerPayoutsData = {
      partnerId,
      type: 'refund_payment',
      status: 'waiting_for_signature',
      events: [
        {
          status: 'added_refund_payments_ids',
          note: `Added ${size(
            selectedPendingPaymentIds
          )} payments for getting approval`,
          createdAt: new Date()
        }
      ],
      paymentIds: selectedPendingPaymentIds,
      hasRefundPayments: true
    }
    const [partnerPayout] = await partnerPayoutService.insertAPartnerPayout(
      partnerPayoutsData,
      session
    )
    partnerPayoutId = partnerPayout._id
  }
  if (partnerPayoutId) {
    const queueData = {
      action: 'generate_pending_payment_esigning_doc',
      event: 'payment_approved',
      destination: 'payments',
      priority: 'immediate',
      params: {
        partnerId,
        partnerPayoutId,
        pendingPaymentIds: selectedPendingPaymentIds,
        userId
      }
    }
    const [createdAppQueue] = await appQueueService.createAnAppQueue(
      queueData,
      session
    )
    return createdAppQueue
  }
}

const validateInputDataForApprovePendingPayments = async (params) => {
  const { selectedPendingPaymentIds = [] } = params
  appHelper.checkRequiredFields(['selectedPendingPaymentIds'], params)

  const wrongPayment = await invoicePaymentHelper.getInvoicePayment({
    _id: { $in: selectedPendingPaymentIds },
    refundStatus: 'pending_for_approval'
  })
  if (!size(wrongPayment)) {
    throw new CustomError(404, 'wrong payment')
  }
}

export const updateApprovedPendingRefundPayments = async (req) => {
  const { body = {}, user, session } = req
  appHelper.validatePartnerAppRequestData(req)
  const isValidSSN = await appHelper.validateUserSSN(user.userId, session)
  if (!isValidSSN) {
    throw new CustomError(
      400,
      'Please add right norwegian national identification to approve refund payments'
    )
  }
  await validateInputDataForApprovePendingPayments(body)
  await invoicePaymentHelper.canApproveDirectRemittances(user)
  const { partnerId, userId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  body.userId = userId
  const { selectedPendingPaymentIds = [] } = body

  const updateResponse = await updateInvoicePayments(
    {
      _id: { $in: selectedPendingPaymentIds },
      refundStatus: 'pending_for_approval'
    },
    { $set: { refundStatus: 'waiting_for_signature' } },
    session
  )
  if (
    !(
      size(updateResponse) &&
      updateResponse.nModified &&
      updateResponse.nModified === selectedPendingPaymentIds.length
    )
  ) {
    throw new CustomError(
      405,
      'Could not update payment status to waiting_for_signature'
    )
  }

  const response = await updatePartnerPayoutForApprovePayments(body, session)
  return {
    data: response
  }
}

export const addInvoicePayments = async (req) => {
  const { body, session } = req
  const { data } = body
  if (!size(data)) {
    throw new CustomError(400, 'Payment data is required')
  }

  const paymentsArr = await preparePaymentInsertData(data)
  if (!size(paymentsArr))
    throw new CustomError(400, 'Payments are already exists')

  const payments = await insertInvoicePayments(paymentsArr, session)
  if (!size(payments)) return []

  const appQueuesData = []
  for (const payment of payments) {
    if (payment?._id) {
      appQueuesData.push({
        _id: nid(17),
        action: 'identify_bank_payment',
        createdAt: new Date(),
        destination: 'payments',
        event: 'identify_bank_payment',
        params: {
          paymentId: payment._id,
          netsReceivedFileId: payment.netsReceivedFileId
        },
        priority: 'regular',
        status: 'new'
      })
    }
  }

  if (size(appQueuesData) !== size(payments))
    throw new CustomError(405, 'Could not app queues data for all payments')

  await appQueueService.createMultipleAppQueues(appQueuesData, session)

  return payments
}

export const updatePaymentsForLambda = async (req = {}) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['paymentUpdateData'], body)

  const { paymentUpdateData = [] } = body
  if (!size(paymentUpdateData)) return false

  const updatedPayments = []
  for (const updatingData of paymentUpdateData) {
    const { paymentId = '', feedbackInfo = {} } = updatingData || {}

    const updatedPayment = await updateAnInvoicePayment(
      { _id: paymentId },
      { $push: { feedbackStatusLog: feedbackInfo } },
      session
    )

    if (updatedPayment?._id) {
      await createRefundPaymentUpdatedLog(updatedPayment, session, feedbackInfo)

      updatedPayments.push(updatedPayment)
    }
  }

  return !!size(updatedPayments)
}

export const downloadInvoicePayments = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const invoicesQuery = await invoicePaymentHelper.prepareInvoicePaymentsQuery(
    body
  )
  await appHelper.isMoreOrLessThanTargetRows(
    InvoicePaymentCollection,
    invoicesQuery,
    {
      moduleName: 'Payments',
      rejectEmptyList: true
    }
  )
  body.downloadProcessType = 'download_payments'
  const { createdDateRange, dateRange, sort = { createdAt: -1 } } = body
  if (size(dateRange)) {
    body.dateRange = {
      startDate: new Date(dateRange.startDate),
      endDate: new Date(dateRange.endDate)
    }
  }
  if (size(createdDateRange)) {
    body.createdDateRange = {
      startDate: new Date(createdDateRange.startDate),
      endDate: new Date(createdDateRange.endDate)
    }
  }
  if (size(sort)) {
    appHelper.validateSortForQuery(sort)
  }
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

export const markPaymentAsEstimated = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['paymentId'])
  const { body, session } = req
  const { partnerId, paymentId } = body
  const updatedPayment = await updateAnInvoicePayment(
    {
      _id: paymentId,
      partnerId,
      refundStatus: { $in: ['pending_for_approval', 'waiting_for_signature'] }
    },
    {
      $set: { refundStatus: 'estimated' }
    },
    session
  )
  if (!size(updatedPayment))
    throw new CustomError(400, 'Unable to update payment')
  return {
    _id: updatedPayment._id,
    refundStatus: updatedPayment.refundStatus
  }
}

const prepareCollectionDataForPayouts = async (payoutInfo, params, session) => {
  const {
    isInvoiceCredited = false,
    isAdvancedPayout = false,
    limit = 50,
    dateTwoDaysBefore
  } = params
  const collections = []
  const insertData = omit(payoutInfo, [
    'bookingDate',
    '_id',
    'createdAt',
    'createdBy'
  ])
  if (isAdvancedPayout) {
    assign(insertData, {
      advancedPayout: true,
      payoutDate: dateTwoDaysBefore
    })
  } else if (isInvoiceCredited) {
    assign(insertData, {
      invoiceCredited: true,
      payoutDate: dateTwoDaysBefore
    })
  } else {
    assign(insertData, {
      invoicePaid: true,
      payoutDate: dateTwoDaysBefore,
      invoicePaidOn: dateTwoDaysBefore,
      invoicePaidAfterPayoutDate: false
    })
  }
  insertData.status = 'estimated'
  const lastSerial = await counterHelper.getACounter(
    {
      _id: `payout-${payoutInfo.partnerId}`
    },
    session
  )

  let serialId = lastSerial?.next_val
  for (let i = 0; i < limit; i++) {
    serialId += 1
    collections.push({ ...insertData, serialId })
  }

  await counterService.updateACounter(
    {
      _id: `payout-${payoutInfo.partnerId}`
    },
    { $set: { next_val: serialId + 1 } },
    session
  )
  return collections
}

const prepareCollectionDataForPayments = (paymentInfo, params) => {
  const { dateTwoDaysBefore, limit = 50 } = params
  const insertData = {
    ...omit(paymentInfo, ['_id', 'createdAt', 'createdBy']),
    refundStatus: 'estimated',
    sentToNETS: false,
    paymentDate: dateTwoDaysBefore,
    refundToAccountName: 'Unite Living AS',
    refundToAccountNumber: '15062501178'
  }
  const collectionData = []
  for (let i = 0; i < limit; i++) {
    collectionData.push(insertData)
  }
  return collectionData
}

const prepareAndInsertCollectionData = async (body, session) => {
  const { paymentId, payoutId } = body
  if (payoutId && paymentId)
    throw new CustomError(400, 'Please use only one type of collectionId!')
  if (body?.limit < 1 || body?.limit > 500) {
    throw new CustomError(
      400,
      'Payment or payout limit range is from 1 to 500!'
    )
  }
  let payoutInfo = {}
  let paymentInfo = {}
  if (payoutId) {
    payoutInfo = (await payoutHelper.getPayout({ _id: payoutId })) || {}
    if (!size(payoutInfo))
      throw new CustomError(500, 'Could not find any payout with this payoutId')
  } else {
    paymentInfo = await invoicePaymentHelper.getInvoicePayment({
      _id: paymentId
    })
    if (!size(paymentInfo))
      throw new CustomError(
        500,
        'Could not find any payout with this paymentId'
      )
  }
  const dateTwoDaysBefore = (
    await appHelper.getActualDate('', true, new Date())
  )
    .subtract(2, 'days')
    .toDate()
  let partnerId = ''
  body.dateTwoDaysBefore = dateTwoDaysBefore
  if (payoutId) {
    const collectionData = await prepareCollectionDataForPayouts(
      payoutInfo.toObject(),
      body,
      session
    )
    if (!size(collectionData))
      throw new CustomError(
        500,
        `Could not prepare collectionData with payoutId ${payoutId}`
      )
    partnerId = payoutInfo.partnerId
    await payoutService.createMultiplePayouts(collectionData, session)
  } else if (paymentId) {
    const collectionData = prepareCollectionDataForPayments(
      paymentInfo,
      body,
      session
    )
    if (!size(collectionData))
      throw new CustomError(
        500,
        `Could not prepare collectionData with paymentId ${paymentId}`
      )
    partnerId = paymentInfo.partnerId
    await insertInvoicePayments(collectionData, session)
  }

  const [partnerPayout = {}] =
    (await partnerPayoutService.insertAPartnerPayout(
      {
        partnerId,
        type: payoutId ? 'payout' : paymentId ? 'refund_payment' : null,
        status: 'created',
        events: [{ status: 'created', createdAt: new Date() }],
        hasPayouts: payoutId ? false : undefined,
        hasRefundPayments: paymentId ? false : undefined
      },
      session
    )) || []
  const { _id: partnerPayoutId } = partnerPayout || {}
  const appQueueData = {
    action: 'ready_partner_payout',
    event: payoutId
      ? 'initiate_payout_process'
      : paymentId
      ? 'initiate_refund_payment_process'
      : null,
    destination: 'payments',
    priority: 'immediate',
    params: {
      partnerPayoutId,
      partnerId
    },
    status: 'new'
  }
  const [appQueue] =
    (await appQueueService.createAnAppQueue(appQueueData, session)) || []
  return appQueue
}

export const createTestPayoutsOrPayments = async (req) => {
  if (process.env.STAGE === 'production') {
    throw new CustomError(400, 'This action can not be done in production!')
  }

  const { body = {}, session } = req
  const isSuccess = await prepareAndInsertCollectionData(body, session)
  return isSuccess
}
