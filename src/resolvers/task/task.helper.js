import { difference, each, extend, omit, pick, size } from 'lodash'
import moment from 'moment'

import {
  accountHelper,
  appHelper,
  listingHelper,
  logHelper,
  partnerSettingHelper,
  propertyItemHelper,
  tenantHelper
} from '../helpers'
import { CustomError } from '../common'
import { TaskCollection } from '../models'

export const prepareTaskData = (body) => {
  const insertData = body
  const { userId } = body
  insertData.createdBy = userId
  insertData.assignTo = [userId]
  insertData.status = 'open'
  delete insertData.userId
  return insertData
}

const getAssignToPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'assignTo',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'assignToInfo'
    }
  }
]

const getClosedByPipeline = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'closedBy',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'closedByUserInfo'
    }
  },
  appHelper.getUnwindPipeline('closedByUserInfo')
]

export const getTaskForQuery = async (params) => {
  const { query, options = {}, userId } = params
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        dueDate: { $ifNull: ['$dueDate', '$createdAt'] },
        isDueDateExist: {
          $cond: [{ $ifNull: ['$dueDate', false] }, true, false]
        }
      }
    },
    {
      $sort: {
        status: -1,
        ...sort
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getAssignToPipeline(),
    ...getClosedByPipeline(),
    {
      $project: {
        _id: 1,
        assignToInfo: 1,
        createdAt: 1,
        closedOn: 1,
        closedByUserInfo: 1,
        dueDate: {
          $cond: ['$isDueDateExist', '$dueDate', '$$REMOVE']
        },
        starredByMe: {
          $cond: [
            {
              $in: [userId, { $ifNull: ['$starredBy', []] }]
            },
            true,
            false
          ]
        },
        status: 1,
        title: 1
      }
    }
  ]
  const tasks = await TaskCollection.aggregate(pipeline)
  return tasks
}

const prepareQueryForTasksQuery = async (query) => {
  const { context, partnerId, taskDueDate, taskStatus, userId } = query
  const partnerSetting =
    (await partnerSettingHelper.getSettingByPartnerId(partnerId)) || {}

  if (taskStatus === 'assignedToMe' || context === 'janitorDashboard') {
    query.assignTo = userId
  } else if (taskStatus === 'assignedToOther') {
    query.$and = [
      {
        $or: [
          { assignTo: { $ne: userId } },
          { 'assignTo.1': { $exists: true } }
        ]
      }
    ]
  } else if (taskStatus === 'starred') {
    query.starredBy = userId
  } else if (taskStatus === 'done') {
    query.status = 'closed'
  }

  const startDay = (await appHelper.getActualDate(partnerSetting, true, null))
    .startOf('day')
    .toDate()
  const endDay = (await appHelper.getActualDate(partnerSetting, true, null))
    .endOf('day')
    .toDate()

  if (taskDueDate === 'expired') {
    query.dueDate = {
      $lt: startDay
    }
  } else if (taskDueDate === 'today') {
    query.dueDate = { $gte: startDay, $lte: endDay }
  } else if (taskDueDate === 'tomorrow') {
    query.dueDate = {
      $gt: endDay,
      $lte: (await appHelper.getActualDate(partnerSetting, true, null))
        .add(1, 'day')
        .endOf('day')
        .toDate()
    }
  } else if (taskDueDate === 'sevenDays') {
    query.dueDate = {
      $gt: startDay,
      $lte: (await appHelper.getActualDate(partnerSetting, true, null))
        .add(7, 'day')
        .endOf('day')
        .toDate()
    }
  }
  return omit(query, [
    'context',
    'partnerId',
    'taskDueDate',
    'taskStatus',
    'userId'
  ])
}

export const queryTasks = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { userId, partnerId } = user
  query.userId = userId
  query.partnerId = partnerId

  console.log('checking for partnerId: ', partnerId)
  if (partnerId) {
    query.$or = [
      { partnerId },
      { landlordPartnerId: partnerId, isMovingInOutProtocolTask: true },
      { tenantPartnerId: partnerId, isMovingInOutProtocolTask: true }
    ]
  }
  console.log('checking for query before prepare: ', query)
  body.query = await prepareQueryForTasksQuery(query)
  console.log('checking for query after prepare: ', body.query)
  const tasks = await getTaskForQuery({
    ...body,
    userId
  })
  const filteredDocuments = await countTasks(body.query)
  const totalDocuments = await countTasks({ partnerId })
  return {
    data: tasks,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const lookupListingInfoForTaskDetails = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              postalCode: 1
            },
            apartmentId: 1,
            listingTypeId: 1,
            propertyTypeId: 1,
            imageUrl: 1
          }
        }
      ],
      as: 'listingInfo'
    }
  },
  appHelper.getUnwindPipeline('listingInfo')
]

const lookupAssigneesInfoForTaskDetails = () => ({
  $lookup: {
    from: 'users',
    localField: 'assignTo',
    foreignField: '_id',
    pipeline: [
      {
        $project: {
          name: '$profile.name',
          avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
        }
      }
    ],
    as: 'assigneesInfo'
  }
})

const finalProjection = () => ({
  $project: {
    _id: 1,
    createdBy: 1,
    title: 1,
    status: 1,
    dueDate: 1,
    createdAt: 1,
    updatedAt: 1,
    listingInfo: 1,
    accountInfo: 1,
    tenantInfo: 1,
    assigneesInfo: 1,
    landlordPartnerId: 1,
    tenantPartnerId: 1
  }
})

const getTaskDetailsForQuery = async (params) => {
  const { taskId, partnerId } = params
  const query = { _id: taskId }
  if (partnerId) {
    query.$or = [
      { partnerId },
      { landlordPartnerId: partnerId, isMovingInOutProtocolTask: true },
      { tenantPartnerId: partnerId, isMovingInOutProtocolTask: true }
    ]
  }
  const pipeline = [
    {
      $match: query
    },
    ...lookupListingInfoForTaskDetails(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    lookupAssigneesInfoForTaskDetails(),
    finalProjection()
  ]

  const [taskDetails] = (await TaskCollection.aggregate(pipeline)) || []
  if (!size(taskDetails))
    throw new CustomError(404, "Doesn't found task details data")
  return taskDetails
}

export const queryTaskDetails = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['taskId'], body)
  const { partnerId } = user
  body.partnerId = partnerId
  return await getTaskDetailsForQuery(body)
}

export const prepareTaskUpdateData = async (body, previousTask) => {
  const {
    accountId,
    assigneeId,
    dueDate,
    partnerId,
    removeAssigneeId,
    starredByMe,
    status,
    title,
    userId
  } = body
  let { propertyId, tenantId } = body
  const addToSetData = {}
  const updateData = {}
  const unsetData = {}
  const pullData = {}
  const setData = {}

  if (title) setData.title = title
  if (dueDate) setData.dueDate = dueDate
  if (status && status !== previousTask.status) {
    setData.status = status
    if (status === 'closed') {
      setData.closedBy = userId
      setData.closedOn = new Date()
    }
  }
  const { assignTo = [] } = previousTask
  if (assigneeId) {
    appHelper.validateId({ assigneeId })
    const alreadyAssigned = assignTo.find((userID) => userID === assigneeId)

    if (alreadyAssigned)
      throw new CustomError(409, 'Assignee already assigned to this task')
    else addToSetData.assignTo = assigneeId
  }

  if (removeAssigneeId && size(assignTo)) {
    appHelper.validateId({ removeAssigneeId })
    const hasRemovableId = assignTo.find(
      (userID) => userID === removeAssigneeId
    )
    if (hasRemovableId) pullData.assignTo = hasRemovableId
    else throw new CustomError(404, 'Assignee not assigned to this task')
  }

  if (accountId === 'remove') unsetData.accountId = 1
  else if (accountId && previousTask.accountId !== accountId) {
    appHelper.validateId({ accountId })
    setData.accountId = accountId
    // reset tenant if this tenant is not from that account
    if (previousTask.tenantId) {
      const tenant = await tenantHelper.getATenant({
        _id: previousTask.tenantId,
        partnerId,
        'properties.accountId': accountId
      })
      if (!tenant) tenantId = 'remove'
    }
    // remove property
    if (previousTask.propertyId) propertyId = 'remove'
  }

  if (propertyId === 'remove') unsetData.propertyId = 1
  else if (propertyId && previousTask.propertyId !== propertyId) {
    appHelper.validateId({ propertyId })
    setData.propertyId = propertyId
    if (!previousTask.accountId) {
      const listing = await listingHelper.getAListing({
        _id: propertyId,
        partnerId
      })
      if (listing?.accountId) setData.accountId = listing.accountId
    }
  }

  if (tenantId === 'remove') unsetData.tenantId = 1
  else if (tenantId && previousTask.tenantId !== tenantId) {
    appHelper.validateId({ tenantId })
    setData.tenantId = tenantId
  }

  if (starredByMe) addToSetData.starredBy = userId
  else if (starredByMe === false) pullData.starredBy = userId

  if (size(unsetData)) updateData.$unset = unsetData
  if (size(setData)) updateData.$set = setData
  if (size(addToSetData)) updateData.$addToSet = addToSetData
  if (size(pullData)) updateData.$pull = pullData

  return updateData
}

export const prepareTaskQuery = (body) => {
  const { taskId, partnerId } = body
  const query = {
    _id: taskId,
    $or: [
      { partnerId },
      { tenantPartnerId: partnerId, isMovingInOutProtocolTask: true },
      { landlordPartnerId: partnerId, isMovingInOutProtocolTask: true }
    ]
  }
  return query
}

export const updatedAssignee = (currentTask, previousTask) => {
  const { currentAssignee, previousAssignee } = getCurrentAndPrevAssignee(
    currentTask,
    previousTask
  )
  const newAssignee = difference(currentAssignee, previousAssignee)
  const removedAssignee = difference(previousAssignee, currentAssignee)

  return { newAssignee, removedAssignee }
}

export const prepareBasicLogData = (task) => {
  const logData = pick(task, [
    'accountId',
    'partnerId',
    'agentId',
    'landLordPartnerId',
    'tenantPartnerId',
    'propertyId'
  ])
  const { _id, createdBy } = task
  logData.context = 'task'
  logData.taskId = _id
  logData.agentId = createdBy ? createdBy : ''
  return logData
}

export const prepareAddLogData = (task) => {
  const basicLogData = prepareBasicLogData(task)
  let logData = {}
  logData = extend(logData, basicLogData)
  logData.action = 'created_task'
  logData.createdBy = task.assignTo[0]
  const options = { action: 'created_task', context: 'task' }
  logData.visibility = logHelper.getLogVisibility(options, task)
  return logData
}

export const getDueDateChangedField = async (currentTask, previousTask) => {
  const { partnerId } = currentTask
  const currentDueDate = currentTask.dueDate
  const prevDueDate = previousTask.dueDate
  const currentFormattedDueDate = currentDueDate
    ? moment(currentDueDate).format('YYYY-MM-DD')
    : ''
  const prevFormattedDueDate = prevDueDate
    ? moment(prevDueDate).format('YYYY-MM-DD')
    : ''
  if (currentFormattedDueDate !== prevFormattedDueDate) {
    return {
      field: 'dueDate',
      type: 'date',
      oldDate: prevDueDate
        ? (await appHelper.getActualDate(partnerId, true, prevDueDate)).toDate()
        : '',
      newDate: currentDueDate
        ? (
            await appHelper.getActualDate(partnerId, true, currentDueDate)
          ).toDate()
        : ''
    }
  }
  return null
}

export const getChangedField = (currentTask, previousTask, field) => {
  const currentTaskFieldName =
    currentTask && currentTask[field] ? currentTask[field] : ''
  const previousTaskFieldName =
    previousTask && previousTask[field] ? previousTask[field] : ''
  let type = 'text'
  if (
    field === 'assignTo' ||
    field === 'accountId' ||
    field === 'propertyId' ||
    field === 'tenantId'
  ) {
    type = 'foreignKey'
  }
  if (currentTaskFieldName !== previousTaskFieldName) {
    return {
      field,
      type,
      oldText: previousTaskFieldName,
      newText: currentTaskFieldName
    }
  }
  return null
}

export const getChangesArray = async (currentTask, previousTask) => {
  const changedFields = []
  const dueDateChangedField = await getDueDateChangedField(
    currentTask,
    previousTask
  )
  if (size(dueDateChangedField)) {
    changedFields.push(dueDateChangedField)
  }
  const fieldNames = ['title', 'status', 'accountId', 'propertyId', 'tenantId']
  each(fieldNames, (fieldName) => {
    const changedField = getChangedField(currentTask, previousTask, fieldName)
    if (size(changedField)) {
      changedFields.push(changedField)
    }
  })
  return changedFields
}

export const prepareChangeLogData = async (
  currentTask,
  previousTask,
  userId
) => {
  const basicLogData = prepareBasicLogData(currentTask)
  const options = { action: 'updated_task', context: 'task' }
  let logData = {}
  logData = extend(logData, basicLogData)
  logData.action = 'updated_task'
  logData.isChangeLog = true
  logData.visibility = logHelper.getLogVisibility(options, currentTask)
  logData.changes = await getChangesArray(currentTask, previousTask)
  if (userId) logData.createdBy = userId
  return logData
}

export const getCurrentAndPrevAssignee = (currentTask, previousTask) => {
  const currentAssignee = size(currentTask.assignTo) ? currentTask.assignTo : []
  const previousAssignee = size(previousTask.assignTo)
    ? previousTask.assignTo
    : []
  return { currentAssignee, previousAssignee }
}

export const getActionAndChanges = (currentTask, previousTask) => {
  const { currentAssignee, previousAssignee } = getCurrentAndPrevAssignee(
    currentTask,
    previousTask
  )
  let action = ''
  const changes = {
    field: 'assignTo',
    type: 'foreignKey'
  }

  if (size(currentAssignee) > size(previousAssignee)) {
    action = 'assignee_added'
    changes.newId = difference(currentAssignee, previousAssignee)[0]
  } else if (size(currentAssignee) === size(previousAssignee)) {
    action = 'assignee_updated'
    changes.newId = difference(currentAssignee, previousAssignee)[0]
    changes.oldId = difference(previousAssignee, currentAssignee)[0]
  } else if (size(currentAssignee) < size(previousAssignee)) {
    action = 'assignee_removed'
    changes.newId = difference(previousAssignee, currentAssignee)[0]
  }
  return { action, changes }
}

export const prepareAssigneeUpdateLogData = (
  currentTask,
  previousTask,
  userId
) => {
  const { action, changes } = getActionAndChanges(currentTask, previousTask)
  const logData = prepareBasicLogData(currentTask)
  const options = { action, context: 'task' }
  logData.action = action
  logData.isChangeLog = true
  logData.visibility = logHelper.getLogVisibility(options, currentTask)
  logData.changes = [changes]
  if (userId) logData.createdBy = userId
  return logData
}
export const getATask = async (query, session, populate = []) => {
  const task = await TaskCollection.findOne(query)
    .session(session)
    .populate(populate)
  return task
}

export const countTasks = async (query, session) => {
  const noOfTasks = await TaskCollection.countDocuments(query).session(session)
  return noOfTasks
}

const finalTaskProjection = () => ({
  $project: {
    _id: 1,
    accountInfo: 1,
    assigneesInfo: 1,
    closedBy: 1,
    closedByUserInfo: 1,
    closedOn: 1,
    createdAt: 1,
    createdBy: 1,
    dueDate: 1,
    listingInfo: 1,
    starredBy: 1,
    status: 1,
    tenantInfo: 1,
    title: 1
  }
})

export const getUpdatedTaskInfo = async (query, session) => {
  const pipeline = [
    {
      $match: query
    },
    ...lookupListingInfoForTaskDetails(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    lookupAssigneesInfoForTaskDetails(),
    ...getClosedByPipeline(),
    finalTaskProjection()
  ]
  const [taskDetails] =
    (await TaskCollection.aggregate(pipeline).session(session)) || []
  return taskDetails
}

export const prepareTaskUpdateDataForFurniture = async (params = {}) => {
  const {
    accountId,
    agentId,
    dueDate,
    furnitureId,
    partnerId,
    propertyId,
    responsibleForFixing,
    status,
    title
  } = params
  const updateData = {}
  const setData = {}
  let unsetData = {}
  if (status) setData.status = status
  if (responsibleForFixing) {
    if (responsibleForFixing === 'landlord') {
      const accountInfo = await accountHelper.getAccountById(accountId)
      setData.assignTo = [accountInfo?.personId]
      setData.landlordPartnerId = partnerId
      unsetData = {
        tenantPartnerId: '',
        partnerId: ''
      }
    } else if (responsibleForFixing === 'tenant') {
      const movingProtocolInfo = await propertyItemHelper.getAPropertyItem(
        {
          contractId: { $exists: true },
          'inventory.furniture.id': furnitureId,
          propertyId
        },
        null,
        ['contract']
      )
      const tenantId = movingProtocolInfo?.contract?.rentalMeta?.tenantId
      const tenantInfo = await tenantHelper.getTenantById(tenantId)
      setData.assignTo = [tenantInfo?.userId]
      setData.tenantPartnerId = partnerId
      unsetData = {
        landlordPartnerId: 1,
        partnerId: 1
      }
    } else {
      if (responsibleForFixing === 'agent') setData.assignTo = [agentId]
      setData.partnerId = partnerId
      unsetData = {
        landlordPartnerId: '',
        tenantPartnerId: ''
      }
    }
  }
  if (title) setData.title = title
  if (dueDate) setData.dueDate = dueDate
  if (size(setData)) updateData.$set = setData
  if (size(unsetData)) updateData.$unset = unsetData
  return updateData
}

export const prepareNewTaskDataForFurniture = async (params) => {
  const {
    accountId,
    agentId,
    dueDate,
    existFurnitureInfo,
    furnitureId,
    partnerId,
    propertyId,
    responsibleForFixing,
    status,
    userId
  } = params
  const { title } = existFurnitureInfo
  const updateData = {
    isMovingInOutProtocolTask: true
  }
  const movingProtocolInfo = await propertyItemHelper.getAPropertyItem(
    {
      contractId: { $exists: true },
      'inventory.furniture.id': furnitureId,
      propertyId
    },
    null,
    ['contract']
  )
  const tenantId = movingProtocolInfo?.contract?.rentalMeta?.tenantId
  const tenantInfo = await tenantHelper.getTenantById(tenantId)
  if (responsibleForFixing === 'landlord') {
    const accountInfo = await accountHelper.getAccountById(accountId)
    updateData.assignTo = [accountInfo?.personId]
    updateData.landlordPartnerId = partnerId
  } else if (responsibleForFixing === 'tenant') {
    updateData.assignTo = [tenantInfo?.userId]
    updateData.tenantPartnerId = partnerId
  } else if (responsibleForFixing === 'agent') {
    updateData.assignTo = [agentId]
    updateData.partnerId = partnerId
  }
  if (accountId) updateData.accountId = accountId
  if (dueDate) updateData.dueDate = dueDate
  else updateData.dueDate = moment().add(7, 'days').toDate()
  if (furnitureId) updateData.furnitureId = furnitureId
  if (propertyId) updateData.propertyId = propertyId
  if (status) updateData.status = status
  if (tenantId) updateData.tenantId = tenantId
  if (title) updateData.title = title
  if (userId) updateData.createdBy = userId
  return updateData
}

export const getTaskStatus = (assigneeType, propertyRoomItemStatus) => {
  let status = 'open'

  if (
    propertyRoomItemStatus === 'ok' ||
    propertyRoomItemStatus === 'notApplicable' ||
    assigneeType === 'noActionRequired'
  ) {
    status = 'closed'
  } else if (propertyRoomItemStatus === 'issues') {
    status = 'open'
  }

  return status
}

export const getTaskAssignTo = async (params) => {
  const { assigneeType, accountId, agentId, tenantId } = params
  let assignTo = []

  if (assigneeType === 'tenant' && tenantId) {
    const tenantInfo = (await tenantHelper.getATenant({ _id: tenantId })) || {}
    const { userId } = tenantInfo

    assignTo = userId ? [userId] : []
  } else if (assigneeType === 'landlord' && accountId) {
    const accountInfo =
      (await accountHelper.getAnAccount({ _id: accountId })) || {}
    const { personId } = accountInfo

    assignTo = personId ? [personId] : []
  } else if (assigneeType === 'agent' && agentId) {
    assignTo = agentId ? [agentId] : []
  }

  return assignTo
}
