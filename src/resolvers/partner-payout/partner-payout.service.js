import { cloneDeep, head, map, size } from 'lodash'

import { PartnerPayoutCollection } from '../models'
import {
  appHelper,
  invoicePaymentHelper,
  partnerHelper,
  partnerPayoutHelper,
  partnerSettingHelper,
  payoutHelper
} from '../helpers'
import {
  invoicePaymentService,
  partnerPayoutService,
  payoutService,
  appQueueService
} from '../services'

import { CustomError } from '../common/error'

export const insertPartnerPayouts = async (data = [], session) => {
  const partnerPayouts = await PartnerPayoutCollection.create(data, session)
  return partnerPayouts
}

export const insertAPartnerPayout = async (data, session) => {
  const partnerPayout = await PartnerPayoutCollection.create([data], session)
  return partnerPayout
}

export const updateAPartnerPayout = async (query, data, session) => {
  const partnerPayout = await PartnerPayoutCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      runValidators: true,
      new: true
    }
  )
  return partnerPayout
}

export const updatePartnerPayouts = async (query, data, session) => {
  const response = await PartnerPayoutCollection.updateMany(query, data, {
    session
  })
  return response
}

export const dailyGeneratePartnerPayouts = async (req) => {
  console.log('+++ generatePartnerPayoutsDaily +++')
  const { session, user } = req
  appHelper.checkUserId(user.userId)
  const partners = await partnerHelper.getPartners(
    {
      isActive: true,
      accountType: 'broker'
    },
    session
  )
  console.log('+++ Found number of partners ==> ', size(partners))
  const partnerPayoutsData = []
  for (const partner of partners) {
    partnerPayoutsData.push({
      partnerId: partner._id,
      type: 'payout',
      status: 'created',
      events: [{ status: 'created', createdAt: new Date() }],
      hasPayouts: false
    })
  }
  console.log('partnerPayoutsData ', partnerPayoutsData)
  if (!size(partnerPayoutsData)) {
    throw new CustomError(405, `Could not prepare partner payouts data`)
  }
  const partnerPayouts = await insertPartnerPayouts(partnerPayoutsData, session)
  console.log('+++ Inserted partnerPayouts ==> ', partnerPayouts)
  if (!size(partnerPayouts)) {
    throw new CustomError(405, `Could not add partner payouts`)
  }
  const partnerPayoutQueues = []
  for (const partnerPayout of partnerPayouts) {
    partnerPayoutQueues.push(
      partnerPayoutHelper.prepareDataToCreateAQueueForInitiatePayoutJob(
        'initiate_payout_process',
        partnerPayout
      )
    )
  }
  console.log('partnerPayoutQueues === ', partnerPayoutQueues)
  if (size(partnerPayoutQueues)) {
    await appQueueService.createMultipleAppQueues(partnerPayoutQueues, session)
    return 'Success'
  }
}

export const dailyGeneratePartnerRefundPayments = async (req) => {
  const { session, user } = req
  appHelper.checkUserId(user.userId)

  const activePartners = await partnerHelper.getPartners({ isActive: true })

  const partnerPayoutsData = []
  for (const partner of activePartners) {
    partnerPayoutsData.push({
      partnerId: partner._id,
      type: 'refund_payment',
      status: 'created',
      events: [{ status: 'created', createdAt: new Date() }],
      hasRefundPayments: false
    })
  }
  const partnerPayouts = await partnerPayoutService.insertPartnerPayouts(
    partnerPayoutsData,
    session
  )

  const partnerPayoutQueues = []
  for (const partnerPayout of partnerPayouts) {
    partnerPayoutQueues.push(
      partnerPayoutHelper.prepareDataToCreateAQueueForInitiatePayoutJob(
        'initiate_refund_payment_process',
        partnerPayout
      )
    )
  }
  await appQueueService.insertAppQueueItems(partnerPayoutQueues, session)
  return 'Success'
}

export const initiatePartnerPayout = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerPayoutId', 'partnerId'], body)
  const { partnerId, partnerPayoutId } = body
  //at first set the status to the partner payouts collection
  const params = {
    status: 'processing',
    eventStatus: 'job_started'
  }
  const updateData =
    partnerPayoutHelper.prepareDataToUpdatePartnerPayout(params)
  await updateAPartnerPayout({ _id: partnerPayoutId }, updateData, session)
  console.log(
    '-- Job started for partner payout and updated partner payout status to processing'
  )
  let orQuery = []

  const settings = await partnerSettingHelper.getAPartnerSetting({ partnerId })
  console.log('+++ Checking partner settings', settings)
  if (!size(settings)) {
    const params = {
      status: 'error',
      eventStatus: 'failed',
      eventNote: 'No settings found for the partner.'
    }
    const updateData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(params)
    await updateAPartnerPayout({ _id: partnerPayoutId }, updateData, session)
    throw new CustomError(404, 'No settings found for the partner')
  }

  const payoutDate = await appHelper.getActualDate(settings, false)
  console.log('+++ Checking payoutDate', payoutDate)
  const todayDateOfMonth = (
    await appHelper.getActualDate(settings, true, payoutDate)
  )
    .endOf('day')
    .toDate()
  //1. ================= Pay the payout of paid invoices on the standard payout date =============
  //only pay the invoices which are paid and credited before the standard payout date.
  console.log('+++ Checking todayDateOfMonth', todayDateOfMonth)
  orQuery.push({
    invoicePaid: true,
    payoutDate: { $lte: todayDateOfMonth },
    invoicePaidOn: { $lte: todayDateOfMonth },
    invoicePaidAfterPayoutDate: { $ne: true }
  })

  orQuery.push({
    invoiceCredited: true,
    payoutDate: { $lte: todayDateOfMonth }
  })
  console.log('+++ Checking orQuery on 185', orQuery)
  //2. ================= Apply the setting: do not wait for next payout date if rent invoice is paid after payout date - Payout after x days of payment =============

  const query =
    await partnerPayoutHelper.prepareQueryWhenRentInvoiceIsPaidAfterPayoutDate(
      settings,
      payoutDate,
      todayDateOfMonth
    )
  console.log('+++ Checking query on 194', query)
  orQuery = [...orQuery, ...query]

  //3. ================= Payout to landlord before tenant's payment - max x months ===============
  //we'll find all active/closed contracts for advance payment.

  const advancePayMonths =
    size(settings.payout) &&
    settings.payout.enabled &&
    settings.payout.payBeforeMonth
      ? settings.payout.payBeforeMonth
      : 0
  console.log('+++ Checking advancePayMonths', advancePayMonths)
  if (advancePayMonths) {
    //we got the payout ids where we have to consider the advance payout.
    //find the unpaid payouts and set advancedPayout flag.

    const unpaidPayoutIds =
      await partnerPayoutHelper.getUnpaidPayoutIdsForAdvancePayMonth(
        partnerId,
        advancePayMonths,
        todayDateOfMonth
      )
    console.log('+++ Checking unpaidPayoutIds', unpaidPayoutIds)
    if (size(unpaidPayoutIds)) {
      await payoutService.updatePayouts(
        {
          _id: { $in: unpaidPayoutIds },
          partnerId
        },
        { $set: { advancedPayout: true } },
        session
      )
      console.log('+++ updated Payouts')
      //Since its not updated directly for session
      orQuery.push({
        _id: { $in: unpaidPayoutIds }
      })
    }
    orQuery.push({
      payoutDate: { $lte: todayDateOfMonth },
      advancedPayout: true
    })
    console.log('+++ Checking orQuery on 237', orQuery)
  }

  //Finally =============== find the payouts and prepare the payout process data.
  const payoutQuery = await partnerPayoutHelper.getEstimatedPayoutsQuery(
    partnerId,
    orQuery
  )
  console.log('+++ Checking payoutQuery', payoutQuery)
  const estimatedPayouts = await payoutHelper.getPayouts(payoutQuery)
  console.log('+++ Checking estimatedPayouts', estimatedPayouts)
  const estimatedPayoutIds = map(estimatedPayouts, '_id')

  if (size(estimatedPayouts)) {
    const payoutParams = {
      status: 'pending_for_approval',
      eventStatus: 'payouts_found',
      eventNote:
        'Sending notification for approving ' +
        estimatedPayouts.length +
        ' pending payouts',
      payoutIds: undefined,
      hasPayouts: true
    }
    const updatedData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(payoutParams)
    await updateAPartnerPayout({ _id: partnerPayoutId }, updatedData, session)
    const pendingPayouts = await payoutService.updatePayouts(
      { _id: { $in: estimatedPayoutIds } },
      { $set: { status: 'pending_for_approval' } },
      session
    )
    console.log(
      '-- Found Estimated payouts and updated payouts to pending for approval'
    )
    if (
      size(pendingPayouts) &&
      pendingPayouts.nModified &&
      pendingPayouts.nModified > 0
    ) {
      const params = {
        partnerSetting: settings,
        estimatedPayoutIds,
        collectionName: 'payouts'
      }
      console.log(`// Found ${estimatedPayoutIds.length} estimated payouts`)
      // Sending notification to direct remittance approving users
      await sendPendingDirectRemittanceNoticeForApproval(params, session)
    }
  } else {
    const payoutParams = {
      status: 'completed',
      eventStatus: 'no_payouts_found',
      eventNote: 'No payouts found.'
    }
    const updatedData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(payoutParams)
    await updateAPartnerPayout({ _id: partnerPayoutId }, updatedData, session)
  }
  return 'Success'
}

export const initiatePartnerRefundPayment = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId', 'partnerPayoutId'], body)
  const { partnerId, partnerPayoutId } = body
  const payoutParams = {
    status: 'processing',
    eventStatus: 'job_started'
  }
  const updateData =
    partnerPayoutHelper.prepareDataToUpdatePartnerPayout(payoutParams)
  await updateAPartnerPayout({ _id: partnerPayoutId }, updateData, session)
  console.log('-- Job started for refund payment process')
  const settings = await partnerSettingHelper.getAPartnerSetting({ partnerId })
  if (!settings) {
    const params = {
      status: 'error',
      eventStatus: 'failed',
      eventNote: 'No settings found for the partner.'
    }
    const updateData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(params)
    await updateAPartnerPayout({ _id: partnerPayoutId }, updateData, session)
    // return false
    throw new CustomError(404, 'No settings found for the partner.')
  }

  const paymentDate = (await appHelper.getActualDate(settings, true))
    .endOf('day')
    .toDate()
  const refundPaymentQuery = {
    partnerId,
    type: 'refund',
    refundStatus: 'estimated',
    sentToNETS: { $ne: true },
    amount: { $lt: 0 },
    refundToAccountNumber: { $exists: true, $nin: [null, 0, false, ''] },
    paymentDate: {
      $lte: paymentDate
    }
  }

  //Finally =============== find the refund payments and prepare the refund payments process data.
  const estimatedPayments = await invoicePaymentHelper.getInvoicePayments(
    refundPaymentQuery
  )
  const estimatedPaymentIds = map(estimatedPayments, '_id')
  if (size(estimatedPayments)) {
    const payoutParams = {
      status: 'pending_for_approval',
      eventStatus: 'refund_payments_found',
      eventNote:
        'Sending notification for approving ' +
        estimatedPayments.length +
        ' pending payments',
      payoutIds: undefined,
      hasPayouts: false,
      paymentIds: undefined,
      hasRefundPayments: true
    }
    const updateData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(payoutParams)
    await updateAPartnerPayout({ _id: partnerPayoutId }, updateData, session)
    const pendingPayments = await invoicePaymentService.updateInvoicePayments(
      { _id: { $in: estimatedPaymentIds } },
      { $set: { refundStatus: 'pending_for_approval' } },
      session
    )
    console.log(
      '-- Found estimated payments and updated payments status to pending for approval '
    )
    if (size(pendingPayments) && pendingPayments.nModified) {
      console.log(
        `// Found ${estimatedPaymentIds.length} estimated payments for partner payout Id: ${partnerPayoutId} `
      )
      // Sending notification to direct remittance approving users
      const params = {
        partnerSetting: settings,
        estimatedPayoutIds: estimatedPaymentIds,
        collectionName: 'payments'
      }
      await sendPendingDirectRemittanceNoticeForApproval(params, session)
    }
  } else {
    const payoutParams = {
      status: 'completed',
      eventStatus: 'no_refund_payments_found',
      eventNote: 'No refund payments found.'
    }
    const updateData =
      partnerPayoutHelper.prepareDataToUpdatePartnerPayout(payoutParams)
    await updateAPartnerPayout({ _id: partnerPayoutId }, updateData, session)
  }
  return 'Success'
}

const prepareAppQueueDataForPendingApproval = (params) => {
  const { partnerSetting, estimatedPayoutIds, collectionName } = params
  const event = getEventName(collectionName)
  return {
    event,
    action: 'send_notification',
    destination: 'notifier',
    priority: 'regular',
    params: {
      partnerId: partnerSetting.partnerId,
      collectionId: head(estimatedPayoutIds),
      collectionNameStr: collectionName
    },
    status: 'new'
  }
}

const getEventName = (collectionName) => {
  if (collectionName === 'payouts') {
    return 'send_pending_payout_for_approval'
  } else if (collectionName === 'payments') {
    return 'send_pending_payment_for_approval'
  }
}

export const sendPendingDirectRemittanceNoticeForApproval = async (
  params,
  session
) => {
  const { partnerSetting } = params
  const persons = partnerPayoutHelper.getAllowedPersonsToApprove(partnerSetting)
  if (!size(persons)) {
    return false
  }

  const basicAppQueueData = prepareAppQueueDataForPendingApproval(params)
  const appQueuesList = []
  for (const personId of persons) {
    const appQueueData = cloneDeep(basicAppQueueData)
    appQueueData.params.options = {
      userId: personId
    }
    appQueuesList.push(appQueueData)
  }
  if (size(appQueuesList)) {
    await appQueueService.createMultipleAppQueues(appQueuesList, session)
    console.log(
      '-- Sent direct remittance approval notifications to allowed accounting persons'
    )
    return true
  }
  return false
}

//For Payments Lambda #10482
export const updatePartnerPayout = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerPayoutId'], body)

  const updateData = partnerPayoutHelper.prepareDataToUpdatePartnerPayout(body)
  const query = partnerPayoutHelper.prepareQueryToUpdatePartnerPayout(body)
  const updatedPartnerPayout = await updateAPartnerPayout(
    query,
    updateData,
    session
  )
  if (!size(updatedPartnerPayout)) {
    throw new CustomError(400, 'Partner payout not updated')
  }
  return updatedPartnerPayout
}

export const createPartnerPayout = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  if (!size(body))
    throw new CustomError(
      400,
      'Missing required data is required to create partner payout'
    )

  const [createdPartnerPayout] =
    (await insertAPartnerPayout(body, session)) || []
  if (!size(createdPartnerPayout))
    throw new CustomError(400, 'Partner payout could not be created')

  return createdPartnerPayout
}
