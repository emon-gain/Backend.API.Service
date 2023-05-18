import { size } from 'lodash'
import { CustomError } from '../common'
import { NotificationCollection } from '../models'
import { appHelper } from '../helpers'

export const updateANotification = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await NotificationCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return response
}

export const removeANotification = async (query, session) => {
  const response = await NotificationCollection.findOneAndDelete(query, {
    session
  })
  return response
}

export const removeNotifications = async (query, session) => {
  if (!size(query))
    throw new CustomError(
      400,
      'Query must be required while removing notifications'
    )
  const response = await NotificationCollection.deleteMany(query, {
    session
  })
  console.log('=== Notifications Removed ===', response)
  return response
}

export const updateNotification = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { _id, data } = body
  const result = await updateANotification({ _id }, data, session)
  if (!size(result)) {
    throw new CustomError(404, `Could not update notification`)
  }
  console.log(`--- Updated Notification for Id: ${result._id} ---`)
  return result
}

export const insertANotification = async (data, session) => {
  const notifications = await NotificationCollection.create([data], {
    session
  })
  if (!size(notifications)) {
    throw new CustomError(400, 'Unable to create notification')
  }
  return notifications[0]
}
