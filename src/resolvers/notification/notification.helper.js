import { NotificationCollection } from '../models'
import { appHelper, conversationHelper } from '../helpers'

export const countNotifications = async (query, session) => {
  const countedNotifications = await NotificationCollection.countDocuments(
    query
  ).session(session)
  return countedNotifications
}

export const getUserInfoPipelineForNotificationList = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            name: 1,
            userId: 1,
            properties: 1
          }
        }
      ],
      as: 'tenantInfo'
    }
  },
  appHelper.getUnwindPipeline('tenantInfo'),
  {
    $lookup: {
      from: 'users',
      localField: 'tenantInfo.userId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey'),
            status: 1
          }
        }
      ],
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo')
]
const getProjectForNotificationList = {
  $project: {
    _id: 1,
    createdAt: 1,
    isRead: 1,
    partnerId: 1,
    propertyId: 1,
    propertyInfo: {
      apartmentId: '$propertyInfo.apartmentId',
      location: '$propertyInfo.location'
    },
    tenantId: 1,
    tenantInfo: {
      name: '$tenantInfo.name',
      avatarKey: '$userInfo.avatarKey',
      status: '$userInfo.status',
      userId: '$tenantInfo.userId'
    },
    type: 1
  }
}

const getInterestFormNotificationList = async (interestFormQuery, body) => {
  const { options = {} } = body
  const { limit, skip, sort } = options
  const notifications = await NotificationCollection.aggregate([
    {
      $match: interestFormQuery
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getUserInfoPipelineForNotificationList(),
    ...appHelper.getCommonPropertyInfoPipeline(),
    getProjectForNotificationList
  ])
  return notifications || []
}

const prepareQueriesForNotification = async (body, user) => {
  const { userId } = user
  const { partnerId } = body
  body.query = {
    userId,
    partnerId
  }
  const interestFormQuery = { partnerId, type: 'interestedFormSubmitted' }
  const { conversationQuery } =
    await conversationHelper.preparedQueryForConversation(body)
  conversationQuery.lastMessageAt = { $exists: true }
  return {
    interestFormQuery,
    conversationQuery
  }
}

const getNotificationListMetaData = async (
  user,
  conversationQuery,
  interestFormQuery
) => {
  const { userId } = user
  let totalNotifications = 0
  let totalUnReadNotifications = 0

  const totalInterestFormNotification = await countNotifications(
    interestFormQuery
  )
  const totalConversationList = await conversationHelper.countConversations(
    conversationQuery
  )

  const totalUnReadInterestFormNotification = await countNotifications({
    ...interestFormQuery,
    isRead: false
  })
  const totalUnReadConversationCount =
    await conversationHelper.countConversations({
      ...conversationQuery,
      unreadBy: userId
    })

  totalNotifications = totalInterestFormNotification + totalConversationList
  totalUnReadNotifications =
    totalUnReadInterestFormNotification + totalUnReadConversationCount

  return {
    totalNotifications,
    totalUnReadNotifications
  }
}

export const notificationList = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body, user } = req
  const { interestFormQuery, conversationQuery } =
    await prepareQueriesForNotification(body, user)
  const notifications = await getInterestFormNotificationList(
    interestFormQuery,
    body
  )
  console.log('notifications: ', notifications)
  const conversationList = await conversationHelper.getNotificationMessageList(
    conversationQuery,
    body,
    user
  )
  console.log('conversationList: ', conversationList)
  const notificationList = prepareNotificationListData(
    notifications,
    conversationList
  )
  console.log('notificationList: ', notificationList)
  const { totalNotifications, totalUnReadNotifications } =
    await getNotificationListMetaData(
      user,
      conversationQuery,
      interestFormQuery
    )
  return {
    data: notificationList,
    metaData: {
      totalNotifications,
      totalUnReadNotifications
    }
  }
}

const prepareNotificationListData = (notifications, conversationList) => {
  const notReadNotifications = []
  const readNotifications = []
  const notReadConversations = []
  const readConversations = []

  notifications.forEach((notification) => {
    if (notification.isRead) {
      readNotifications.push(notification)
    } else {
      notReadNotifications.push(notification)
    }
  })

  conversationList.forEach((conversation) => {
    if (conversation.isRead) {
      readConversations.push(conversation)
    } else {
      notReadConversations.push(conversation)
    }
  })

  const notificationNotReadList = [
    ...notReadNotifications,
    ...notReadConversations
  ].sort((a, b) => {
    const firstItem = new Date(a.lastMessageAt || a.createdAt)
    const secondItem = new Date(b.lastMessageAt || b.createdAt)
    return secondItem - firstItem
  })

  const notificationReadList = [
    ...readNotifications,
    ...readConversations
  ].sort((a, b) => {
    const firstItem = new Date(a.lastMessageAt || a.createdAt)
    const secondItem = new Date(b.lastMessageAt || b.createdAt)
    return secondItem - firstItem
  })
  return notificationNotReadList.concat(notificationReadList)
}
