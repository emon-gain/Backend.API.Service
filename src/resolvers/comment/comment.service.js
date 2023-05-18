import { size } from 'lodash'
import { CommentCollection } from '../models'
import {
  appHelper,
  commentHelper,
  partnerSettingHelper,
  taskHelper,
  userHelper
} from '../helpers'
import { appQueueService, logService } from '../services'
import { CustomError } from '../common'

export const updateAComment = async (query, data, session) => {
  if (!size(data)) throw new CustomError(404, 'No data found to update comment')
  const updatedComment = await CommentCollection.findOneAndUpdate(query, data, {
    new: true,
    runValidators: true,
    session
  })
  if (!size(updatedComment)) throw new CustomError(404, 'Comment not found')
  return updatedComment
}

export const createCommentLog = async (comment, session) => {
  const logData = commentHelper.prepareCommentCreateLogData(comment)
  const log = await logService.createLog(logData, session)
  return log
}
export const sendTaskCommentNotification = async (
  assignTo,
  comment,
  session
) => {
  const appQueueData = {
    action: 'send_notification',
    event: 'send_task_notification',
    destination: 'notifier',
    params: {
      collectionId: comment._id,
      collectionNameStr: 'comments',
      options: { assignTo, taskId: comment.taskId },
      partnerId: comment.partnerId
    },
    priority: 'regular'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
  return true
}

export const initAfterInsertProcess = async (comment, session) => {
  await createCommentLog(comment, session)
  if (comment.taskId) {
    const partnerId = comment.partnerId
    const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })
    const isSendTaskNotification =
      partnerSettings?.notifications?.taskNotification
    if (!isSendTaskNotification) return false

    const taskInfo = await taskHelper.getATask({
      _id: comment.taskId,
      partnerId
    })
    const assignTo =
      taskInfo && size(taskInfo.assignTo) ? taskInfo.assignTo : ''
    if (!assignTo) return false
    await sendTaskCommentNotification(assignTo, comment, session)
  }
}

export const createAComment = async (data, session) => {
  const [comment] = await CommentCollection.create([data], { session })
  if (!comment) throw new CustomError(500, 'Unable to create a comment')
  return comment
}
export const addComment = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['content'], body)
  body.createdBy = userId
  body.partnerId = partnerId
  const commentQuery = commentHelper.prepareAddCommentQuery(body)
  const comment = await createAComment(commentQuery, session)
  await initAfterInsertProcess(comment, session)
  const commentInfo = comment.toObject()
  commentInfo.userInfo = await userHelper.getAnUserWithAvatar({ _id: userId })
  return commentInfo
}
