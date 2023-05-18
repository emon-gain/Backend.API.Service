import { map, size } from 'lodash'

import { CustomError } from '../common'
import { AppQueueCollection } from '../models'
import { appHelper, fileHelper } from '../helpers'
import { appQueueService } from '../services'

export const getFrontItemOfQueue = async () => {
  const frontItem = await AppQueueCollection.findOne({ status: 'new' }).sort({
    createdAt: 1
  })
  return frontItem
}

export const getAnAppQueue = async (query, session) => {
  const appQueue = await AppQueueCollection.findOne(query).session(session)
  return appQueue
}

export const getAppQueues = async (query, session) => {
  const appQueues = await AppQueueCollection.find(query).session(session)
  return appQueues
}

export const countAppQueues = async (query) => {
  const numberOfAppQueues = await AppQueueCollection.countDocuments(query)
  return numberOfAppQueues
}

export const getAppQueuesWithOptions = async (query, options, session) => {
  const { limit, skip, sort } = options
  const appQueues = await AppQueueCollection.find(query)
    .sort(sort)
    .skip(skip)
    .limit(limit)
    .session(session)
  return appQueues
}

export const prepareAppQueuesForAppHealth = async (partnerId, type) => {
  let match = {}
  if (type === 'other') {
    match = {
      'params.partnerId': {
        $exists: false
      }
    }
  } else {
    match = {
      'params.partnerId': partnerId
    }
  }
  const pipeline = pipelineForApphealtAppQueues(match)
  const data = await AppQueueCollection.aggregate(pipeline)
  console.log(data)
  return data[0]
}

const pipelineForApphealtAppQueues = (match) => {
  const pipeline = [
    {
      $match: match
    },
    {
      $group: {
        _id: null,
        failedInfo: {
          $push: {
            $cond: {
              if: {
                $and: [
                  { $eq: ['$status', 'failed'] },
                  { $eq: ['$noOfRetry', 5] }
                ]
              },
              then: {
                queueId: '$_id',
                queueEventType: '$event',
                queueActionType: '$action'
              },
              else: null
            }
          }
        },
        totalFailedAppQueues: {
          $sum: {
            $cond: {
              if: {
                $and: [
                  { $eq: ['$status', 'failed'] },
                  { $eq: ['$noOfRetry', 5] }
                ]
              },
              then: 1,
              else: 0
            }
          }
        },
        count: {
          $sum: 1
        }
      }
    },
    {
      $addFields: {
        failedInfo: {
          $filter: {
            input: '$failedInfo',
            as: 'd',
            cond: {
              $ifNull: ['$$d.queueId', false]
            }
          }
        }
      }
    }
  ]
  return pipeline
}

export const getAndUpdateQueues = async (req) => {
  const { optionData, queryData, session } = req
  const { limit = 20, skip = 0, sort = { updatedAt: 1 } } = optionData
  const { destination, priority, queueId, status } = queryData

  const query = {
    isSequential: { $ne: true },
    sequentialCategory: { $exists: false }
  }
  if (destination) query.destination = destination
  if (priority) query.priority = priority
  if (queueId) query._id = queueId
  query.status = status ? status : 'new'

  const appQueues =
    (await AppQueueCollection.find(query).sort(sort).skip(skip).limit(limit)) ||
    []
  if (!size(appQueues)) return []

  const queueIds = map(appQueues, '_id') || []

  await appQueueService.updateAppQueues(
    { _id: { $in: queueIds } },
    { $set: { flightAt: new Date(), status: 'on_flight' } },
    session
  )

  return await getAppQueues(
    { _id: { $in: queueIds }, status: 'on_flight' },
    session
  )
}

export const getAndUpdateSequentialQueuesForLambda = async (req) => {
  const { optionData, queryData, session } = req
  const { limit = 20, sort = { updatedAt: 1 } } = optionData
  const { destination, status } = queryData

  const [appQueuesObject = {}] = await AppQueueCollection.aggregate([
    {
      $match: {
        destination,
        isSequential: true,
        sequentialCategory: { $exists: true },
        status
      }
    },
    { $sort: sort },
    { $group: { _id: '$sequentialCategory', queueId: { $first: '$_id' } } },
    { $limit: limit },
    { $group: { _id: null, queueIds: { $push: '$queueId' } } }
  ])
  console.log('=== appQueuesObject ===', appQueuesObject)

  const { queueIds } = appQueuesObject || {}

  console.log('=== QIds with sequentialCategory', queueIds)

  if (!size(queueIds)) return []

  await appQueueService.updateAppQueues(
    { _id: { $in: queueIds } },
    { $set: { flightAt: new Date(), status: 'on_flight' } },
    session
  )

  return await getAppQueues(
    { _id: { $in: queueIds }, status: 'on_flight' },
    session
  )
}

export const getQueueItemById = async (queueId) => {
  const queueItem = await AppQueueCollection.findOne({ _id: queueId })
  return queueItem
}

export const validateDataForAddingAppQueue = (params) => {
  const requiredFields = [
    'event',
    'action',
    'params',
    'destination',
    'priority'
  ]
  appHelper.checkRequiredFields(requiredFields, params)

  // Validating data for ContactUs related app-queue creation
  if (params.event === 'contact_us') {
    const queueParams = params.params || {}
    if (!size(queueParams))
      throw new CustomError(400, "Queue params can't be empty")

    const requiredFields = ['email', 'message', 'name', 'subject']
    appHelper.checkRequiredFields(requiredFields, queueParams)

    const subjectEnum = ['help', 'feedback', 'other']
    if (!subjectEnum.includes(queueParams.subject))
      throw new CustomError(400, 'Invalid subject name')
  }
}

export const validateDataForAddingAppQueues = (params) => {
  const { data } = params

  if (!size(data)) throw new CustomError(400, 'Required data missing')
  else {
    for (const dataObj of data) {
      validateDataForAddingAppQueue(dataObj)
    }
  }
}

export const prepareAppQueueUpdateData = (params) => {
  const {
    delaySeconds,
    status,
    errorDetails,
    params: updatedParams,
    priority,
    startingIndex,
    isManuallyCompleted,
    isUnsetNoOfRetry
  } = params
  let updateData = {}
  if (typeof delaySeconds === 'number') updateData.delaySeconds = delaySeconds

  if (size(status)) updateData.status = status
  if (priority) updateData.priority = priority

  if (size(status) && status === 'failed') updateData.priority = 'regular'
  else if (size(status) && status === 'completed')
    updateData.completedAt = new Date()
  else if (size(status) && status === 'processing')
    updateData.processStartedAt = new Date()

  if (size(errorDetails)) {
    updateData.errorDetails = errorDetails
  }

  if (size(updatedParams)) {
    const {
      collectionsData = [],
      contractData = [],
      dataToSkip = 0,
      isLastAppQueue = false,
      hasError,
      startingSerialId = null
    } = updatedParams
    if (size(collectionsData))
      updateData['params.collectionsData'] = collectionsData
    if (size(contractData)) updateData['params.contractData'] = contractData
    if (typeof dataToSkip === 'number')
      updateData['params.dataToSkip'] = dataToSkip
    if (isLastAppQueue) updateData['params.isLastAppQueue'] = isLastAppQueue
    if (typeof startingSerialId === 'number')
      updateData['params.startingSerialId'] = startingSerialId
    if (updatedParams.hasOwnProperty('hasError')) {
      updateData['params.hasError'] = hasError
    }
  }

  if (isManuallyCompleted) updateData.isManuallyCompleted = isManuallyCompleted

  if (startingIndex && status === 'new') {
    updateData = {
      'params.startingIndex': startingIndex,
      status: 'new'
    }
  }

  if (isUnsetNoOfRetry) updateData.noOfRetry = 0

  return updateData
}

export const getAQueueItem = async (query, session) => {
  const queueItem = await AppQueueCollection.findOne(query).session(session)
  return queueItem
}

export const getExistingAppQueueForPogo = async (req) => {
  const { body } = req
  appHelper.checkRequiredFields(['_id', 'partnerId', 'action'], body)
  const { _id, directPartnerAccountId, partnerId, action } = body
  const preparedQuery = {
    _id: { $ne: _id },
    action,
    destination: 'accounting-pogo',
    'params.partnerId': partnerId,
    status: { $nin: ['completed', 'failed'] }
  }
  if (directPartnerAccountId) {
    preparedQuery['params.directPartnerAccountId'] = directPartnerAccountId
  }
  const appQueues = await getAppQueues(preparedQuery)
  const response = { exists: false }
  if (size(appQueues)) {
    response.exists = true
  }
  return response
}

const prepareAppQueuesQuery = (query = {}) => {
  const {
    _id: queueId,
    action,
    createdBy,
    destination,
    event,
    events = [],
    ignoreQueueIds = [],
    ignoreStatuses = [],
    isManuallyCompleted,
    isSequential = false,
    isTransactionAppQueueComplete = false,
    params,
    priority,
    status,
    statusWithNotCompleted = false
  } = query || {}

  const preparedQuery = {}
  if (queueId) preparedQuery._id = queueId
  if (action) preparedQuery.action = action
  if (createdBy) preparedQuery.createdBy = createdBy
  if (destination) preparedQuery.destination = destination
  if (event) preparedQuery.event = event
  if (size(events)) preparedQuery.event = { $in: events }
  if (size(ignoreQueueIds)) preparedQuery._id = { $nin: ignoreQueueIds }
  if (size(ignoreStatuses)) preparedQuery.status = { $nin: ignoreStatuses }
  if (size(isManuallyCompleted))
    preparedQuery.isManuallyCompleted = isManuallyCompleted
  if (isSequential) {
    preparedQuery.isSequential = true
    preparedQuery.sequentialCategory = { $exists: true }
  }
  if (isTransactionAppQueueComplete) preparedQuery.status = { $ne: 'completed' }
  if (size(params)) {
    Object.keys(params).forEach((key) => {
      preparedQuery[`params.${key}`] = params[key]
    })
  }
  if (priority) preparedQuery.priority = priority
  if (status) preparedQuery.status = status
  if (statusWithNotCompleted)
    preparedQuery.status = { $nin: ['completed', 'failed', 'hold'] }

  return preparedQuery
}

export const getAppQueuesForQuery = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)

  const query = prepareAppQueuesQuery(body.query)
  console.log('## getAppQueuesForQuery', query)
  const appQueues = await getAppQueuesWithOptions(query, body.options, session)
  return appQueues
}

export const getAppQueueDataForApphealthNotification = async (req) => {
  const { body } = req
  let { query = {} } = body
  query = {
    ...query,
    'params.partnerId': query.params.partnerId
  }
  delete query.params
  console.log(query)
  const appQueues = await AppQueueCollection.find(query)
  return appQueues
}

export const getAppQueuesByDistinct = async (req) => {
  const { body, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { query = {} } = body

  appHelper.checkRequiredFields(['distinctField', 'status'], query)

  const { distinctField, isSequential = false, priority, status } = query
  const preparedQuery = {
    isSequential: { $exists: false },
    sequentialCategory: { $exists: false },
    status
  }

  if (isSequential) {
    preparedQuery.isSequential = isSequential
    preparedQuery.sequentialCategory = { $exists: true }
  } else {
    appHelper.checkRequiredFields(['priority'], query)
    preparedQuery.priority = priority
  }

  console.log('=== PreparedQuery: ', preparedQuery, '===')

  const appQueues = await AppQueueCollection.distinct(
    distinctField,
    preparedQuery
  )
  appQueues[distinctField] = appQueues
  return appQueues
}

export const getAppQueueDataForPdfGeneration = (
  params = {},
  fileInfo = {}
) => ({
  action: 'generate_pdf',
  destination: 'pdf-creator',
  event: 'generate_pdf',
  params: {
    bucket: fileHelper.bucket,
    callBackParams: {
      fileId: fileInfo._id,
      type: fileInfo.type,
      ...params
    },
    fileKey: fileInfo.fileKey,
    fileName: fileInfo.name,
    html: fileInfo.content,
    partnerId: params.partnerId
  },
  priority: params.priority || 'regular'
})

export const getAppQueueDataForESigningGeneration = (params = {}) => {
  const appQueueData = {
    action: 'handle_e_signing',
    destination: 'esigner',
    event: 'create_document',
    params,
    priority: 'immediate'
  }
  return appQueueData
}
