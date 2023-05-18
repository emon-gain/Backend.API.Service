import { assign, difference, isEqual, omit, pick, size, union } from 'lodash'
import moment from 'moment-timezone'

import {
  appHelper,
  commentHelper,
  contractHelper,
  listingHelper,
  logHelper,
  partnerSettingHelper,
  propertyItemHelper,
  propertyRoomHelper,
  taskHelper,
  userHelper
} from '../helpers'
import { CustomError } from '../common'
import {
  appQueueService,
  commentService,
  logService,
  propertyItemService,
  propertyRoomService
} from '../services'
import { TaskCollection } from '../models'

export const createATask = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, `Unable to create Task`)
  }
  const task = await TaskCollection.create([data], { session })
  return task
}

export const createTaskLog = async (task, session) => {
  const logData = taskHelper.prepareAddLogData(task)
  const insertedLog = await logService.createLog(logData, session)
  return insertedLog
}

export const initAfterInsertProcesses = async (insertedTask, session) => {
  await createTaskLog(insertedTask, session)
}
const getNewlyCreatedTaskInfo = async (insertedTask, session, userId) => {
  const user = (await userHelper.getUserById(userId, session)) || {}
  const avatarKey = userHelper.getAvatar(user)
  const newTaskData = {
    ...insertedTask.toObject(),
    assignToInfo: [
      {
        _id: userId,
        name: user.profile?.name,
        avatarKey
      }
    ]
  }
  return newTaskData
}

export const addTask = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['title'], body)
  const { userId, partnerId } = user
  body.userId = userId
  body.partnerId = partnerId

  // TODO:: incomplete for partner public app

  const taskData = taskHelper.prepareTaskData(body)
  const [insertedTask] = await createATask(taskData, session)
  if (size(insertedTask)) {
    await initAfterInsertProcesses(insertedTask, session)
  }
  const insertedTaskData = await getNewlyCreatedTaskInfo(
    insertedTask,
    session,
    userId
  )
  return insertedTaskData
}

export const createTaskChangeLog = async (params) => {
  const { currentTask, previousTask, session, userId } = params
  const changeLogData = await taskHelper.prepareChangeLogData(
    currentTask,
    previousTask,
    userId
  )
  if (!size(changeLogData.changes)) return false
  const insertedLog = await logService.createLog(changeLogData, session)
  return insertedLog
}

export const createAssigneeUpdateLog = async (params) => {
  const { currentTask, previousTask, session, userId } = params
  const logData = taskHelper.prepareAssigneeUpdateLogData(
    currentTask,
    previousTask,
    userId
  )
  console.log('logData createAssigneeUpdateLog', logData)
  const insertedLog = await logService.createLog(logData, session)
  return insertedLog
}

export const initAfterUpdateProcesses = async (paramsData) => {
  const { currentTask, previousTask, session } = paramsData
  console.log('=== previousTask', JSON.stringify(previousTask))
  console.log('=== currentTask', JSON.stringify(currentTask))
  if (currentTask.status !== previousTask.status) {
    // If task status changed then update moving in out protocol status
    console.log('=== Updating Issues Status For Task ===')
    await updateIssuesStatusForTask(currentTask, session)
  }
  const { newAssignee, removedAssignee } = taskHelper.updatedAssignee(
    currentTask,
    previousTask
  )
  if (size(newAssignee) || size(removedAssignee)) {
    //Create task assignee log
    await createAssigneeUpdateLog(paramsData)
  }
  //Send task assignee notification
  if (size(newAssignee)) {
    const params = {
      assignTo: newAssignee,
      collectionNameStr: 'tasks',
      collectionId: currentTask._id,
      partnerId: currentTask.partnerId
    }
    await sendTaskAssignNotification(params, session)
  }

  //Create log when change the task
  await createTaskChangeLog(paramsData)
}

export const updateTaskCollection = async (query, updateData, session) => {
  const updatedTask = await TaskCollection.findOneAndUpdate(query, updateData, {
    session,
    new: true,
    runValidators: true
  })
  if (!size(updatedTask)) throw new CustomError(404, 'Unable to update task')
  return updatedTask
}

export const updateTask = async (req) => {
  const { body, session, user } = req
  await appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  await appHelper.checkRequiredFields(['taskId'], body)
  const { taskId } = body
  await appHelper.validateId({ taskId })
  const { userId, partnerId } = user
  body.userId = userId
  body.partnerId = partnerId
  // TODO:: incomplete for partner public app
  const query = taskHelper.prepareTaskQuery(body)
  const previousTask = await taskHelper.getATask(query)
  if (!size(previousTask)) throw new CustomError(404, 'Task not found')

  const updateData = await taskHelper.prepareTaskUpdateData(body, previousTask)
  console.log('=== updateData', updateData)
  const updatedTask = await updateTaskCollection(query, updateData, session)
  if (!size(updatedTask)) throw new CustomError(404, 'Unable to update task')
  const params = { currentTask: updatedTask, previousTask, session, userId }
  await initAfterUpdateProcesses(params)
  const updatedTaskData = await taskHelper.getUpdatedTaskInfo(query, session)
  return updatedTaskData
}

export const updateIssuesStatusForTask = async (taskInfo, session) => {
  let status = ''
  if (taskInfo.status === 'open') status = 'issues'
  else if (taskInfo.status === 'closed') status = 'ok'
  console.log('=== taskId', taskInfo._id)
  console.log('=== status', status)
  if (taskInfo.propertyRoomItemId && !taskInfo.furnitureId) {
    const pipeline = [
      { $match: { 'items.taskId': taskInfo._id } },
      {
        $lookup: {
          from: 'property_items',
          localField: 'movingId',
          foreignField: '_id',
          as: 'movingProtocolInfo'
        }
      },
      {
        $unwind: {
          path: '$movingProtocolInfo',
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $addFields: {
          hasMovingProtocolInfo: {
            $and: [
              { $ifNull: ['$movingProtocolInfo._id', false] },
              { $ifNull: ['$movingProtocolInfo.isEsigningInitiate', false] },
              {
                $or: [
                  { $ifNull: ['$movingProtocolInfo.moveInCompleted', false] },
                  { $ifNull: ['$movingProtocolInfo.moveOutCompleted', false] }
                ]
              }
            ]
          }
        }
      },
      { $match: { hasMovingProtocolInfo: false } },
      { $group: { _id: null, roomIds: { $push: '$_id' } } }
    ]
    const [result = {}] =
      (await propertyRoomHelper.getPropertyRoomsByAggregation(pipeline)) || []
    const { roomIds = [] } = result
    console.log('=== roomIds', roomIds)
    if (size(roomIds)) {
      const propertyRoomQuery = {
        _id: { $in: roomIds },
        'items.taskId': taskInfo._id
      }
      const response = await propertyRoomService.updateAPropertyRoom(
        propertyRoomQuery,
        { 'items.$.status': status },
        session
      )
      console.log('=== property room updated response', response)
    }
  } else if (!taskInfo.propertyRoomItemId && taskInfo.furnitureId) {
    const propertyItemQuery = {
      moveInCompleted: { $exists: false },
      moveOutCompleted: { $exists: false },
      isEsigningInitiate: { $exists: false },
      'inventory.furniture': { $elemMatch: { taskId: taskInfo._id } }
    }
    await propertyItemService.updateAPropertyItem(
      propertyItemQuery,
      {
        'inventory.furniture.$.status': status
      },
      session
    )
  }
  return taskInfo
}

export const sendTaskAssignNotification = async (params, session) => {
  const queueData = {
    action: 'send_notification',
    destination: 'notifier',
    event: 'send_task_notification',
    params,
    priority: 'regular'
  }
  await appQueueService.insertInQueue(queueData, session)
}

export const addOrUpdateTaskForFurniture = async (params, session) => {
  const {
    createdTaskId,
    description,
    existFurnitureInfo,
    partnerId,
    previousAssignType,
    propertyId,
    responsibleForFixing,
    status,
    userId
  } = params
  let newTaskId = ''
  const propertyInfo = await listingHelper.getAListing({
    _id: propertyId
  })
  if (!size(propertyInfo)) throw new CustomError(404, 'Property not found')
  params.agentId = propertyInfo.agentId
  params.accountId = propertyInfo.accountId
  if (createdTaskId) {
    const preparedData = await taskHelper.prepareTaskUpdateDataForFurniture(
      params
    )
    const previousTask = await taskHelper.getATask({ _id: createdTaskId })
    const updatedTask = await updateTaskCollection(
      { _id: createdTaskId },
      preparedData,
      session
    )
    if (updatedTask)
      await createTaskLogForUpdateTask(updatedTask, previousTask, session)

    const commentQuery = {
      taskId: createdTaskId,
      isMovingInOutProtocolTaskComment: true
    }
    const commentInfo = await commentHelper.getAComment(commentQuery)
    const commentUpdateData = {}
    if (size(commentInfo) && responsibleForFixing) {
      const commentSetData = {}
      if (responsibleForFixing === 'landlord')
        commentSetData.landlordPartnerId = partnerId
      else if (responsibleForFixing === 'tenant')
        commentSetData.tenantPartnerId = partnerId
      else commentSetData.partnerId = partnerId
      commentUpdateData.$set = commentSetData
      commentUpdateData.$unset = preparedData.$unset
    } else if (description) {
      if (!size(commentInfo)) {
        const commentAddData = {
          taskId: createdTaskId,
          context: 'task',
          content: '<p>' + description + '</p>',
          isMovingInOutProtocolTaskComment: true,
          createdBy: userId
        }
        if (previousAssignType === 'landlord')
          commentAddData.landlordPartnerId = partnerId
        else commentAddData.partnerId = partnerId
        await commentService.createAComment(commentAddData, session)
      } else {
        commentUpdateData.$set = {
          content: '<p>' + description + '</p>'
        }
      }
    }
    if (size(commentInfo) && size(commentUpdateData)) {
      await commentService.updateAComment(
        commentQuery,
        commentUpdateData,
        session
      )
    }
  } else if (
    status === 'open' &&
    responsibleForFixing &&
    responsibleForFixing !== 'noActionRequired'
  ) {
    const { description } = existFurnitureInfo
    const preparedTaskData = await taskHelper.prepareNewTaskDataForFurniture(
      params
    )
    const [createdTask = {}] = await createATask(preparedTaskData, session)
    newTaskId = createdTask._id
    if (newTaskId) {
      if (size(description)) {
        const commentData = {
          taskId: newTaskId,
          context: 'task',
          content: '<p>' + description + '</p>',
          isMovingInOutProtocolTaskComment: true,
          createdBy: userId
        }
        if (responsibleForFixing === 'landlord') {
          commentData.landlordPartnerId = partnerId
        } else if (responsibleForFixing === 'tenant') {
          commentData.tenantPartnerId = partnerId
        } else {
          commentData.partnerId = partnerId
        }
        await commentService.createAComment(commentData, session)
      }
      await createTaskLogForCreateTask(createdTask, session)
    }
  }
  return newTaskId
}

const updateCommentForTask = async (
  createdTaskId,
  paramsForCommentUpdate,
  session
) => {
  const commentUpdatedData = {
    $set: {}
  }

  if (size(paramsForCommentUpdate)) {
    const { content, assigneeType, partnerId, unsetData } =
      paramsForCommentUpdate

    if (content) {
      commentUpdatedData.$set.content = content
    }

    if (partnerId) {
      if (assigneeType === 'landlord') {
        commentUpdatedData.$set.landlordPartnerId = partnerId
      } else if (assigneeType === 'tenant') {
        commentUpdatedData.$set.tenantPartnerId = partnerId
      } else {
        commentUpdatedData.$set.partnerId = partnerId
      }
    }

    if (size(unsetData)) commentUpdatedData['$unset'] = unsetData
  }

  return await commentService.updateAComment(
    {
      taskId: createdTaskId,
      isMovingInOutProtocolTaskComment: true
    },
    commentUpdatedData,
    session
  )
}

const createCommentLog = async (comment = {}, session) => {
  const {
    isMovingInOutProtocolTaskComment,
    landlordPartnerId,
    partnerId,
    tenantPartnerId
  } = comment

  const options = {
    collectionId: comment._id,
    context: 'comment'
  }

  if (isMovingInOutProtocolTaskComment)
    options.isMovingInOutProtocolTaskLog = true

  if (partnerId) options.partnerId = partnerId
  if (landlordPartnerId) options.landlordPartnerId = landlordPartnerId
  else if (tenantPartnerId) options.tenantPartnerId = tenantPartnerId

  const logData = {
    action: 'added_new_comment',
    context: 'comment',
    landlordPartnerId: options.landlordPartnerId,
    partnerId: options.partnerId,
    tenantPartnerId: options.tenantPartnerId,
    commentId: comment._id,
    visibility: ['comment'],
    accountId: comment.accountId,
    propertyId: comment.propertyId,
    agentId: comment.agentId,
    tenantId: comment.tenantId,
    branchId: comment.branchId,
    taskId: comment.taskId,
    contractId: comment.contractId,
    isMovingInOutProtocolTaskLog: options.isMovingInOutProtocolTaskLog
  }

  await logService.createLog(logData, session)
}

const addCommentForTask = async (params, session) => {
  const { partnerId, taskId, assigneeType, description, userId } = params
  const commentData = {
    taskId,
    context: 'task',
    createdBy: userId,
    content: '<p>' + description + '</p>',
    isMovingInOutProtocolTaskComment: true
  }

  if (assigneeType === 'landlord') {
    commentData.landlordPartnerId = partnerId
  } else if (assigneeType === 'tenant') {
    commentData.tenantPartnerId = partnerId
  } else {
    commentData.partnerId = partnerId
  }

  const insertedComment = await commentService.createAComment(
    commentData,
    session
  )

  // create comment log and send task comment notification for after insert comment
  await createCommentLogAndSendTaskCommentNotification(
    insertedComment,
    partnerId,
    session
  )

  return insertedComment
}

const getChangesFieldsArray = async (
  changesFields,
  collectionData,
  options
) => {
  const partnerId = options.partnerId ? options.partnerId : ''
  const previousDoc = options.previousDoc || null
  const collectionName = options.collectionName ? options.collectionName : ''
  const changesArray = []

  if (size(changesFields)) {
    for (const field of changesFields) {
      const fieldName = field.fieldName
      let type = 'text'
      const oldText =
        previousDoc && previousDoc[fieldName] ? previousDoc[fieldName] : ''
      const newText = collectionData[fieldName]
      let oldDate = ''
      let newDate = ''

      if (fieldName === 'dueDate') {
        type = 'date'
        oldDate = oldText
          ? (await appHelper.getActualDate(partnerId, true, oldText)).toDate()
          : ''
        newDate = newText
          ? (await appHelper.getActualDate(partnerId, true, newText)).toDate()
          : ''
      }

      if (
        fieldName === 'assignTo' ||
        fieldName === 'accountId' ||
        fieldName === 'propertyId' ||
        fieldName === 'tenantId'
      )
        type = 'foreignKey'

      const changeData = {
        field: fieldName,
        type
      }

      if (type === 'date') {
        changeData.oldDate = oldDate
        changeData.newDate = newDate
      } else {
        changeData.oldText = oldText
        changeData.newText = newText
      }

      console.log('collectionName ', collectionName, 'fieldName ', fieldName)
      console.log('field.newId ', field.newId)
      if (
        collectionName === 'task' &&
        fieldName === 'assignTo' &&
        field.newId
      ) {
        delete changeData.oldText
        delete changeData.newText

        changeData.newId = field.newId
        if (field.oldId) changeData.oldId = field.oldId
      }

      changesArray.push(changeData)
    }
  }
  console.log('changesArray ', changesArray)
  return changesArray
}

const actionCreatedLog = async (query, logData, options) => {
  const { session } = options
  let metaData = []

  if (options.metaData) metaData = options.metaData

  if (options.collectionName === 'task') {
    logData.taskId = options.collectionId
  }

  if (options.contractId) logData.contractId = options.contractId
  if (options.agentId) logData.agentId = options.agentId
  if (options.accountId) logData.accountId = options.accountId
  if (options?.isMovingInOutProtocolTaskLog)
    logData.isMovingInOutProtocolTaskLog = options.isMovingInOutProtocolTaskLog

  const collectionData = await taskHelper.getATask(query, session)

  const newLogData = pick(collectionData, [
    'accountId',
    'propertyId',
    'agentId',
    'tenantId',
    'branchId',
    'contractId',
    'invoiceId',
    'payoutId',
    'taskId',
    'createdBy'
  ])
  //set tenantId to the log data from collection rental meta for tenant e-signed status log
  if (
    collectionData &&
    collectionData.rentalMeta &&
    collectionData.rentalMeta.tenantId
  )
    newLogData.tenantId = collectionData.rentalMeta.tenantId

  logData = assign(logData, newLogData) //extend log data.
  logData.visibility = logHelper.getLogVisibility(options, collectionData)

  if (size(metaData)) logData.meta = metaData

  if (options && options.errorText) logData.errorText = options.errorText

  if (options && options.commentId) logData.commentId = options.commentId

  return logData
}

const actionUpdatedLog = async (query, logData, options) => {
  const { session } = options
  const changesFields =
    options && options.changesFields ? options.changesFields : []
  let changesArray = []
  const metaData = []

  const collectionData = await taskHelper.getATask(query, session)
  const newLogData = pick(collectionData, [
    'accountId',
    'propertyId',
    'agentId',
    'tenantId',
    'branchId',
    'createdBy'
  ])

  logData.isChangeLog = true
  logData = assign(logData, newLogData) //extend log data.
  logData.visibility = logHelper.getLogVisibility(options, collectionData)

  logData.taskId = options.collectionId
  changesArray = await getChangesFieldsArray(
    changesFields,
    collectionData,
    options
  )
  if (collectionData.createdBy) logData.agentId = collectionData.createdBy

  if (size(metaData)) logData.meta = metaData
  if (size(changesArray)) logData.changes = changesArray
  if (options?.isMovingInOutProtocolTaskLog)
    logData.isMovingInOutProtocolTaskLog = options.isMovingInOutProtocolTaskLog

  return logData
}

const createLogForCreateOrUpdateTask = async (action, options, session) => {
  const collectionId = options.collectionId
  const partnerId = options.partnerId
  const landlordPartnerId = options.landlordPartnerId
  const tenantPartnerId = options.tenantPartnerId
  let logData = pick(options, [
    'partnerId',
    'context',
    'landlordPartnerId',
    'tenantPartnerId'
  ])
  options.session = session
  let query = { partnerId }
  if (landlordPartnerId) query = { landlordPartnerId }
  if (tenantPartnerId) query = { tenantPartnerId }
  if (options.logData) logData = options.logData

  if (action && (partnerId || landlordPartnerId || tenantPartnerId)) {
    logData.action = action

    if (collectionId) {
      query._id = options.collectionId

      if (action === 'created_task') {
        logData = await actionCreatedLog(query, logData, options)
      }

      if (action === 'updated_task' || action === 'assignee_updated') {
        logData = await actionUpdatedLog(query, logData, options)
      }
    }

    await logService.createLog(logData, session)
  }
}

const createTaskLogForUpdateTask = async (doc = {}, previous = {}, session) => {
  const {
    isMovingInOutProtocolTask,
    landlordPartnerId,
    tenantPartnerId,
    partnerId
  } = doc

  const changesFields = []
  let action = 'created_task'
  const options = {
    collectionId: doc._id,
    collectionName: 'task',
    context: 'task'
  }

  if (isMovingInOutProtocolTask)
    options.isMovingInOutProtocolTaskLog = isMovingInOutProtocolTask

  if (landlordPartnerId) options.landlordPartnerId = landlordPartnerId
  else if (tenantPartnerId) options.tenantPartnerId = tenantPartnerId
  else options.partnerId = partnerId

  if (size(previous)) {
    action = 'updated_task'

    options.previousDoc = previous

    //Prepare changes fields array
    const currentDueDate = doc.dueDate
      ? moment(doc.dueDate).format('YYYY-MM-DD')
      : ''
    const prevDueDate = previous.dueDate
      ? moment(previous.dueDate).format('YYYY-MM-DD')
      : ''

    if (doc.title !== previous.title) changesFields.push({ fieldName: 'title' })
    if (doc.status !== previous.status)
      changesFields.push({ fieldName: 'status' })
    if (doc.accountId !== previous.accountId)
      changesFields.push({ fieldName: 'accountId' })
    if (doc.propertyId !== previous.propertyId)
      changesFields.push({ fieldName: 'propertyId' })
    if (doc.tenantId !== previous.tenantId)
      changesFields.push({ fieldName: 'tenantId' })
    if (currentDueDate !== prevDueDate)
      changesFields.push({ fieldName: 'dueDate' })
    if (doc?.assignTo[0] !== previous?.assignTo[0]) {
      if (previous?.assignTo > doc?.assignTo) {
        action = 'assignee_removed'
        changesFields.push({
          fieldName: 'assignTo',
          newId: previous.assignTo[0],
          type: 'foreignKey'
        })
      } else {
        action = 'assignee_updated'
        changesFields.push({
          fieldName: 'assignTo',
          newId: doc.assignTo[0],
          oldId: previous.assignTo[0]
        })
      }
    }
    if (size(changesFields)) options.changesFields = changesFields
    else return false
  }

  await createLogForCreateOrUpdateTask(action, options, session)
}

const afterUpdateTaskHook = async (params, session) => {
  const { previous, doc, userId } = params
  const { partnerId } = doc
  const taskId = doc?._id
  if (size(doc) && size(previous)) {
    //Create notification log change after task
    //Now send notify to only change task assignee
    const currentAssignee = doc.assignTo ? doc.assignTo : []
    const previousAssignee = previous.assignTo ? previous.assignTo : []
    const diffAssignTo = difference(currentAssignee, previousAssignee)
    //Send task assignee notification
    if (size(diffAssignTo)) {
      const params = {
        currentTask: doc,
        previousTask: previous,
        userId,
        session
      }
      await createAssigneeUpdateLog(params)
      //Send task assignee notification
      const taskAssigneeParams = {
        assignTo: diffAssignTo,
        collectionNameStr: 'tasks',
        collectionId: doc._id,
        partnerId,
        options: { assignTo: diffAssignTo, taskId }
      }
      const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
        partnerId
      })

      if (size(partnerSetting)) {
        const isSendTaskNotification =
          !!partnerSetting.notifications?.taskNotification

        if (isSendTaskNotification) {
          await sendTaskAssignNotification(taskAssigneeParams, session)
        }
      }
    }

    //Create log when change the task
    await createTaskLogForUpdateTask(doc, previous, session)
  }
}

const createTaskLogForCreateTask = async (doc, session) => {
  const {
    isMovingInOutProtocolTask,
    landlordPartnerId,
    tenantPartnerId,
    partnerId
  } = doc

  const action = 'created_task'
  const options = {
    collectionId: doc._id,
    collectionName: 'task',
    context: 'task'
  }

  if (isMovingInOutProtocolTask)
    options.isMovingInOutProtocolTaskLog = isMovingInOutProtocolTask

  if (landlordPartnerId) options.landlordPartnerId = landlordPartnerId
  else if (tenantPartnerId) options.tenantPartnerId = tenantPartnerId
  else options.partnerId = partnerId

  await createLogForCreateOrUpdateTask(action, options, session)
}

const createCommentLogAndSendTaskCommentNotification = async (
  comment,
  partnerId,
  session
) => {
  const { context, taskId } = comment

  //Create log when a new comment
  await createCommentLog(comment, session)

  //Create notification log add a comment
  if (taskId && context === 'task') {
    //Send comment notification
    const taskInfo = await taskHelper.getATask(
      { _id: taskId, partnerId },
      session
    )

    const assignTo = taskInfo && taskInfo.assignTo ? taskInfo.assignTo : ''
    if (!assignTo) return false

    const partnerSettingsInfo = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })

    const isSendTaskNotification = size(partnerSettingsInfo)
      ? partnerSettingsInfo.notifications?.taskNotification
      : false

    if (isSendTaskNotification) {
      await commentService.sendTaskCommentNotification(
        taskInfo.assignTo,
        comment,
        session
      )
    }
  }
}

const addOrUpdateTask = async (params, session) => {
  const {
    title,
    description,
    status,
    dueDate,
    assignTo,
    partnerId,
    accountId,
    propertyId,
    tenantId,
    furnitureId,
    assigneeType,
    propertyRoomItemId,
    createdTaskId,
    userId
  } = params

  let createdTaskData = {}
  if (createdTaskId) {
    createdTaskData = await taskHelper.getATask({ _id: createdTaskId })
  }
  let newTaskId = ''
  let isTaskCreated = false
  let isTaskUpdated = false

  // Updating previously created task
  if (createdTaskId && size(createdTaskData)) {
    let unsetData = {}
    const updatedData = {}

    if (status && createdTaskData?.status !== status) {
      updatedData.status = status
    }
    const pullData = {}
    if (assignTo && !isEqual(createdTaskData?.assignTo, assignTo)) {
      if (!size(assignTo)) {
        const [removedId] =
          difference(createdTaskData?.assignTo, assignTo) || []
        if (removedId) pullData.assignTo = removedId
      } else updatedData.assignTo = assignTo

      if (assigneeType === 'landlord') {
        updatedData.landlordPartnerId = partnerId
        unsetData = {
          tenantPartnerId: 1,
          partnerId: 1
        }
      } else if (assigneeType === 'tenant') {
        updatedData.tenantPartnerId = partnerId
        unsetData = {
          landlordPartnerId: 1,
          partnerId: 1
        }
      } else {
        updatedData.partnerId = partnerId
        unsetData = {
          landlordPartnerId: 1,
          tenantPartnerId: 1
        }
      }
    }
    if (title && title !== createdTaskData?.title) {
      updatedData.title = title
    }
    if (description) {
      const content = '<p>' + description + '</p>'
      const commentInfo = await commentHelper.getAComment({
        taskId: createdTaskId,
        isMovingInOutProtocolTaskComment: true
      })
      const oldContent = commentInfo?.content
      let paramsForCommentUpdate = {}

      if (oldContent && content !== oldContent)
        paramsForCommentUpdate.content = content
      if (updatedData?.assignTo)
        paramsForCommentUpdate = { assigneeType, partnerId, unsetData }

      if (size(paramsForCommentUpdate) && size(commentInfo)) {
        await updateCommentForTask(
          createdTaskId,
          paramsForCommentUpdate,
          session
        )
      } else if (!size(commentInfo)) {
        await addCommentForTask(
          {
            partnerId,
            taskId: createdTaskId,
            userId,
            assigneeType,
            description
          },
          session
        )
      }
    }
    if (dueDate && !moment(createdTaskData?.dueDate).isSame(dueDate)) {
      updatedData.dueDate = dueDate
    }
    const updateData = {}
    if (size(updatedData)) updateData.$set = updatedData
    if (size(unsetData)) updateData.$unset = unsetData
    if (size(pullData)) updateData.$pull = pullData

    isTaskUpdated = await updateTaskCollection(
      { _id: createdTaskId },
      updateData,
      session
    )
    if (isTaskUpdated) {
      await afterUpdateTaskHook(
        { previous: createdTaskData, doc: isTaskUpdated, userId },
        session
      )
    }
  } else if (
    !(createdTaskId && size(createdTaskData)) &&
    assigneeType &&
    assigneeType !== 'noActionRequired'
  ) {
    // Didn't find previously created task for this room item, so creating now
    const taskCreationData = {
      title,
      status,
      dueDate,
      assignTo,
      accountId,
      propertyId,
      tenantId,
      isMovingInOutProtocolTask: true,
      createdBy: userId
    }

    if (assigneeType === 'landlord') {
      taskCreationData.landlordPartnerId = partnerId
    } else if (assigneeType === 'tenant') {
      taskCreationData.tenantPartnerId = partnerId
    } else {
      taskCreationData.partnerId = partnerId
    }

    if (propertyRoomItemId && !furnitureId) {
      taskCreationData.propertyRoomItemId = propertyRoomItemId
    }

    const [newTask = {}] = await createATask(taskCreationData, session)
    newTaskId = newTask?._id

    if (newTaskId) {
      isTaskCreated = true

      if (isTaskCreated) {
        // Implement after create hook for task
        await createTaskLogForCreateTask(newTask, session)
      }

      if (description) {
        const isCommentCreated = await addCommentForTask(
          {
            partnerId,
            taskId: newTaskId,
            userId,
            assigneeType,
            description
          },
          session
        )

        // Implement after insert comment
        if (size(isCommentCreated)) {
          await createCommentLogAndSendTaskCommentNotification(
            isCommentCreated,
            partnerId,
            session
          )
        }
      }
    }
  }

  return { newTaskId, isTaskUpdated, isTaskCreated }
}

const getPropertyItemsForMovingInOut = async (query, options) => {
  const queryOptions = {}
  let isReturnLastCreatedItem = false

  if (options && options.sort) queryOptions.sort = options.sort
  if (options && options.findLastCreatedItem) isReturnLastCreatedItem = true

  const itemParams = {
    query,
    params: queryOptions
  }
  const allPropertyItems = await propertyItemHelper.getPropertyItemsForQuery(
    itemParams
  )

  if (size(allPropertyItems) && isReturnLastCreatedItem)
    return allPropertyItems[0]
  else if (size(allPropertyItems)) return allPropertyItems
}

const afterUpdatePropertyRoomHook = async (roomInfo, session) => {
  const {
    _id,
    contractId,
    movingId,
    partnerId,
    propertyId,
    propertyRoomId,
    newFiles
  } = roomInfo
  const query = { partnerId, propertyId }

  let updateRoomQuery = '',
    updateRoomInfo = ''

  if (contractId && movingId) {
    updateRoomQuery = { ...query, _id: propertyRoomId }
    updateRoomInfo = omit(JSON.parse(JSON.stringify(roomInfo)), [
      '_id',
      'createdAt',
      'createdBy',
      'contractId',
      'movingId',
      'propertyRoomId'
    ])

    if (newFiles) updateRoomInfo.files = union(updateRoomInfo.files, newFiles)
  } else if (!contractId && !movingId) {
    const activeContractInfo = contractHelper.getAContract({
      ...query,
      status: 'active'
    })

    if (activeContractInfo && activeContractInfo._id) {
      const propertyItemQuery = {
        ...query,
        type: { $in: ['in', 'out'] },
        contractId: activeContractInfo._id,
        isEsigningInitiate: { $exists: false },
        $or: [
          { moveInCompleted: { $exists: false } },
          { moveOutCompleted: { $exists: false } }
        ]
      }
      const lastPropertyItems = getPropertyItemsForMovingInOut(
        propertyItemQuery,
        {
          findLastCreatedItem: true,
          sort: { createdAt: -1 }
        }
      )

      if (lastPropertyItems) {
        updateRoomQuery = {
          ...query,
          movingId: lastPropertyItems._id,
          propertyRoomId: _id
        }
        updateRoomInfo = omit(roomInfo, ['_id', 'createdAt', 'createdBy'])
      }
    }
  }

  if (updateRoomQuery && updateRoomInfo)
    await propertyRoomService.updateAPropertyRoom(
      updateRoomQuery,
      {
        $set: updateRoomInfo
      },
      session
    )
}

export const addOrUpdateTaskForRoomItems = async (
  params,
  roomInfo,
  session
) => {
  const { roomItemId, partnerId, userId } = params

  const propertyRoomItemData = roomInfo?.items
    ? roomInfo.items.find((item) => item.id === roomItemId)
    : {}

  const title = propertyRoomItemData?.title
  const description = propertyRoomItemData?.description
  const assigneeType = propertyRoomItemData?.responsibleForFixing
  const status =
    taskHelper.getTaskStatus(assigneeType, propertyRoomItemData?.status) ||
    'open'
  const dueDate = propertyRoomItemData?.dueDate
    ? propertyRoomItemData?.dueDate
    : moment().add(7, 'days').toDate()
  const propertyId = roomInfo?.propertyId
  const propertyData =
    (await listingHelper.getAListing({ _id: propertyId })) || {}

  const agentId = propertyData?.agentId
  const accountId = propertyData?.accountId

  let movingInOutProtocolInfo = {}

  if (size(roomInfo)) {
    movingInOutProtocolInfo = await propertyRoomHelper.getAPropertyRoom({
      propertyRoomId: roomInfo._id
    })
  }

  let contractId = ''

  if (size(movingInOutProtocolInfo))
    contractId = movingInOutProtocolInfo.contractId

  let contractData = {}
  if (contractId.length > 0) {
    contractData = await contractHelper.getAContract({
      _id: contractId,
      partnerId
    })
  }

  const tenantId = contractData?.rentalMeta?.tenantId

  const assignTo =
    (await taskHelper.getTaskAssignTo({
      assigneeType,
      accountId,
      agentId,
      tenantId
    })) || []
  const paramsForAddOrUpdate = {
    title,
    description,
    status,
    dueDate,
    assignTo,
    partnerId,
    accountId,
    propertyId,
    tenantId,
    assigneeType,
    propertyRoomItemId: roomItemId,
    createdTaskId: propertyRoomItemData?.taskId,
    userId
  }
  const result = await addOrUpdateTask(paramsForAddOrUpdate, session)
  const { newTaskId, isTaskUpdated, isTaskCreated } = result

  if (size(result) && isTaskCreated && newTaskId) {
    let isUpdated = await propertyRoomService.updateAPropertyRoom(
      { _id: roomInfo._id, 'items.id': roomItemId },
      { $set: { 'items.$.taskId': newTaskId } },
      session
    )

    if (size(movingInOutProtocolInfo)) {
      isUpdated = await propertyRoomService.updateAPropertyRoom(
        { propertyRoomId: roomInfo._id, 'items.id': roomItemId },
        { $set: { 'items.$.taskId': newTaskId } },
        session
      )
    }
    // Implement after update property room hook
    if (size(isUpdated)) {
      await afterUpdatePropertyRoomHook(isUpdated, session)
    }

    return newTaskId
  } else if (size(result) && !isTaskCreated && isTaskUpdated) {
    return isTaskUpdated._id
  }
}

export const updateMultipleTasks = async (query, data, session) => {
  const response = await TaskCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  if (response.nModified > 0) {
    return response
  }
}
