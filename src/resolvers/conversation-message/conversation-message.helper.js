import { size } from 'lodash'
import { ConversationMessageCollection, UserCollection } from '../models'
import { appHelper, conversationHelper } from '../helpers'
import { CustomError } from '../common'
import settingJson from '../../../settings.json'

export const getAConversationMessage = async (query = {}, session) => {
  const conversationMessage = await ConversationMessageCollection.findOne(
    query
  ).session(session)
  return conversationMessage
}

export const getConversationMessages = async (query = {}, session) => {
  const conversationMessages = await ConversationMessageCollection.find(
    query
  ).session(session)
  return conversationMessages
}

export const prepareNewMessageLogData = (message, data) => {
  const { partnerId, accountId, propertyId, tenantId } = data
  const logData = {
    partnerId,
    action: 'new_message',
    context: 'conversation'
  }
  logData.messageId = message._id
  logData.conversationId = message.conversationId ? message.conversationId : ''
  const visibility = []
  if (accountId) {
    visibility.push('account')
    logData.accountId = accountId
  }
  if (propertyId) {
    visibility.push('property')
    logData.propertyId = propertyId
  }
  if (tenantId) {
    visibility.push('tenant')
    logData.tenantId = tenantId
  }

  logData.visibility = visibility
  return logData
}

export const prepareConversationUpdateData = async (
  conversationMessage,
  session
) => {
  const { content, conversationId, isFile, createdBy } = conversationMessage
  const updateData = {
    lastMessageAt: new Date(),
    published: true
  }
  if (!isFile) {
    updateData.lastMessage = content
  }
  const conversation = await conversationHelper.getAConversation(
    {
      _id: conversationId
    },
    session
  )
  if (!size(conversation)) return updateData
  const { participants = [], uniqueInteraction } = conversation
  if (!participants.find(({ userId }) => userId === createdBy) && createdBy) {
    participants.push({ userId: createdBy })
    updateData.participants = participants
  }
  updateData.unreadBy = participants
    .map(({ userId }) => userId)
    .filter((userId) => userId && userId !== createdBy)
  // If any conversation gets messages from both side? then uniqueInteraction = true;
  if (!uniqueInteraction) {
    const query = { conversationId, createdBy: { $ne: createdBy } }
    const messageOfAnotherUser = await getAConversationMessage(query, session)
    if (size(messageOfAnotherUser)) {
      updateData.uniqueInteraction = true
      updateData.uniqueInteractionAt = new Date()
    }
  }
  return updateData
}

const prepareAggregatePipeLineForMessages = (query, options) => {
  const { sort, skip, limit } = options
  return [
    {
      $match: query
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
    {
      $lookup: {
        from: 'conversations',
        as: 'conversationInfo',
        let: { conversationId: '$conversationId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$_id', '$$conversationId']
              }
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$conversationInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'listings',
        as: 'listingInfo',
        let: { listingId: '$conversationInfo.listingId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$_id', '$$listingId']
              }
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$listingInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        as: 'userInfo',
        let: { createdBy: '$createdBy' },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$_id', '$$createdBy']
              }
            }
          },
          {
            $project: {
              'profile.name': 1,
              'profile.active': 1,
              'profile.currency': 1,
              'profile.roomForRent': 1,
              'profile.gender': 1,
              'profile.avatarKey':
                appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
            }
          }
        ]
      }
    },
    {
      $unwind: {
        path: '$userInfo',
        preserveNullAndEmptyArrays: true
      }
    }
  ]
}

const getConversationMessagesByAggregate = async (pipeLine, session) => {
  if (!size(pipeLine)) {
    throw new CustomError(400, 'Can not find any pipeline for aggregate')
  }
  const messages = await ConversationMessageCollection.aggregate(
    pipeLine
  ).session(session)
  return messages
}

export const getConversationMessagesForQuery = async (req) => {
  const { body, user } = req
  const { query, options } = body
  appHelper.checkRequiredFields(['userId'], user)
  const { partnerId, userId } = user
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  }
  appHelper.checkRequiredFields(['conversationId'], query)
  const { conversationId } = query
  appHelper.validateId({ conversationId })
  query.userId = userId
  const conversationQuery =
    await conversationHelper.getConversationsAccessQuery(query)
  conversationQuery._id = conversationId
  if (partnerId) conversationQuery.partnerId = partnerId
  const conversation = await conversationHelper.getAConversation(
    conversationQuery
  )
  if (!conversation) throw new CustomError(404, 'Conversation not found')
  const pipeLine = prepareAggregatePipeLineForMessages(
    { conversationId },
    options
  )
  const messages = await getConversationMessagesByAggregate(pipeLine)
  const newMessages = []

  if (size(messages)) {
    for (const message of messages) {
      if (message.isFile) {
        message.fileUrl = await getConversationFileUrl(message)
      }
      newMessages.push(message)
    }
  }

  const totalDocuments = await countConversationMessages({ conversationId })
  return { data: newMessages, metaData: { totalDocuments } }
}

export const getConversationMessageWithSelect = async (query, select = []) => {
  const messages = await ConversationMessageCollection.find(query).select(
    select
  )
  return messages
}

export const countConversationMessages = async (query, session) => {
  const numberOfConversationMessages = await ConversationMessageCollection.find(
    query
  )
    .session(session)
    .countDocuments()
  return numberOfConversationMessages
}

export const getConversationFileUrl = (message) => {
  const { conversationId, content } = message
  const { folder } = settingJson.S3.Directives['Conversations'] // Get Conversations directory from settings
  const domain = appHelper.getCDNDomain(process.env.STAGE)
  const fileUrl =
    content && conversationId
      ? `${domain}/${folder}/${conversationId}/${content}`
      : ''
  return fileUrl
}

export const getUserInfoForConversationMessage = async (userQuery) => {
  const query = [
    { $match: userQuery },
    {
      $project: {
        name: '$profile.name',
        avatar: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
      }
    }
  ]

  const user = await UserCollection.aggregate(query)
  const userInfo = size(user) ? user[0] : {}
  return userInfo
}

export const getConversationIdsByQuery = async (query = {}) => {
  const conversationIds = await ConversationMessageCollection.distinct(
    'conversationId',
    query
  )
  return conversationIds
}
