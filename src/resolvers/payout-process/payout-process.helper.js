import { filter, reduce, size } from 'lodash'
import moment from 'moment-timezone'
import { PayoutProcessCollection } from '../models'
import { appHelper } from '../helpers'

export const getPayoutProcess = async (query, session) => {
  const payoutProcess = await PayoutProcessCollection.findOne(query).session(
    session
  )
  return payoutProcess
}

export const getPayoutProcessIds = async (query) => {
  const payoutProcessIds = await PayoutProcessCollection.distinct('_id', query)
  return payoutProcessIds
}

export const countPayoutProcesses = async (query) => {
  const NumOfPayoutProcess = await PayoutProcessCollection.find(
    query
  ).countDocuments()
  return NumOfPayoutProcess
}

export const getPayoutProcessesForQuery = async (params = {}) => {
  const { options = {}, populate = [], query = {} } = params || {}
  const { limit, skip, sort } = options
  const payoutProcesses = await PayoutProcessCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return payoutProcesses
}

export const prepareQueryToUpdatePayoutProcess = (body) => {
  const { payoutProcessId, partnerId } = body
  const prepareQuery = {}
  if (size(payoutProcessId)) prepareQuery._id = payoutProcessId
  if (size(partnerId)) prepareQuery.partnerId = partnerId
  return prepareQuery
}

export const prepareDataToUpdatePayoutProcess = (body, payoutProcessInfo) => {
  const {
    creditTransferInfo,
    feedbackCreatedAt,
    feedbackStatusLog,
    processingStartedAt,
    sentFileName,
    sentFileStatus
  } = body

  const updatingPushData = {}
  const updatingSetData = {}
  if (size(creditTransferInfo))
    updatingSetData.creditTransferInfo = creditTransferInfo
  // From feedback status log have to prepare new status and feedback created at value for updating payout process
  if (size(feedbackStatusLog)) {
    updatingPushData.feedbackStatusLog = {
      $each: feedbackStatusLog,
      $sort: { createdAt: 1 }
    }

    const allFeedbackStatusLog = filter(
      [...feedbackStatusLog, ...(payoutProcessInfo?.feedbackStatusLog || [])],
      (item) => item?.status !== 'bank_feedback'
    )
    const { createdAt, status } =
      reduce(allFeedbackStatusLog, (a, b) =>
        new Date(a.createdAt) > new Date(b.createdAt) ? a : b
      ) || {}

    const lastFeedbackCreatedAt = feedbackCreatedAt
      ? feedbackCreatedAt
      : payoutProcessInfo?.feedbackCreatedAt || null
    const isCreatedAtValid =
      createdAt &&
      (!lastFeedbackCreatedAt ||
        moment(createdAt).isAfter(lastFeedbackCreatedAt))
    console.log(
      '====> Checking payout process status updating data:',
      {
        createdAt,
        isCreatedAtValid,
        lastFeedbackCreatedAt,
        status
      },
      '<===='
    )
    if (isCreatedAtValid) {
      updatingSetData.status = status
      updatingSetData.feedbackCreatedAt = createdAt
    }
  }

  if (processingStartedAt)
    updatingSetData.processingStartedAt = processingStartedAt
  if (size(sentFileName)) updatingSetData.sentFileName = sentFileName
  if (size(sentFileStatus)) updatingSetData.sentFileStatus = sentFileStatus

  const updatingData = {}
  if (size(updatingPushData)) updatingData['$push'] = updatingPushData
  if (size(updatingSetData)) updatingData['$set'] = updatingSetData

  return updatingData
}

const preparePayoutProcessesQuery = (query) => {
  const {
    endToEndId = '',
    endToEndIds = [],
    groupHeaderMsgId = '',
    payoutProcessId = '',
    payoutProcessIds = [],
    processingStartedAt,
    sentFileStatus,
    sentFileName
  } = query
  const preparedQuery = {}
  if (payoutProcessId) preparedQuery._id = payoutProcessId
  if (size(payoutProcessIds)) preparedQuery._id = { $in: payoutProcessIds }
  if (endToEndId) {
    preparedQuery.creditTransferInfo = {
      $elemMatch: { paymentEndToEndId: endToEndId }
    }
  }
  if (size(endToEndIds)) {
    preparedQuery.creditTransferInfo = {
      $elemMatch: { paymentEndToEndId: { $in: endToEndIds } }
    }
  }
  if (groupHeaderMsgId) preparedQuery.groupHeaderMsgId = groupHeaderMsgId
  if (size(processingStartedAt)) {
    preparedQuery.processingStartedAt = {
      $lt: processingStartedAt
    }
  }
  if (size(sentFileStatus)) preparedQuery.sentFileStatus = sentFileStatus
  if (size(sentFileName)) preparedQuery.sentFileName = sentFileName

  return preparedQuery
}

export const queryPayoutProcessesForLambda = async (req) => {
  const { body, user } = req
  const { query = {} } = body
  appHelper.checkUserId(user.userId)

  body.query = preparePayoutProcessesQuery(query)
  body.populate = ['partnerSettings']
  return await getPayoutProcessesForQuery(body)
}
