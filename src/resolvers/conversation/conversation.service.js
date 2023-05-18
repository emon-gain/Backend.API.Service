import { difference, map, size } from 'lodash'

import { appHelper, conversationHelper, pusherHelper } from '../helpers'
import { AnalyticCollection, ConversationCollection } from '../models'
import { CustomError } from '../common'

export const updateAConversation = async (query, data, session) => {
  const updatedConversation = await ConversationCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  return updatedConversation
}

export const removeAConversation = async (query, session) => {
  const response = await ConversationCollection.findOneAndDelete(query, {
    session
  })
  return response
}

export const updateConversation = async (conversation, body, session) => {
  const { userId } = body
  const { _id, createdBy } = conversation
  if (createdBy !== userId) {
    const updatedConversation = await updateAConversation(
      { _id },
      { $set: { published: true } },
      session
    )
    return updatedConversation
  }
  return conversation
}

export const addConversation = async (body, session) => {
  console.log('++++ preparing data for add conversation ', body)
  const addData = await conversationHelper.prepareAddData(body, session)
  console.log('addData before conversation ', addData)
  const [conversation] = await ConversationCollection.create([addData], {
    session
  })
  return conversation
}

export const insertAnalytics = async (doc, session) => {
  const insertData = {
    createdBy: doc.createdBy,
    event: 'Unique user interactions occurred',
    data: { conversationId: doc._id, participants: doc.participants }
  }
  const inserted = await AnalyticCollection.create([insertData], { session })
  return inserted
}

export const initAfterUpdateProcess = async (previous, doc, session) => {
  if (!previous.uniqueInteraction && doc.uniqueInteraction) {
    await insertAnalytics(doc, session) // UniqueInteraction occurred. set log
  }
}

export const addOrUpdateConversation = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId, userId } = user
  delete body.partnerId
  delete body.userId
  if (userId) body.senderId = userId
  if (partnerId) body.partnerId = partnerId
  const existingConversation = await conversationHelper.getExistingConversation(
    body,
    session
  )
  console.log('Checking existing conversation: ', existingConversation)
  let conversation = {}
  if (size(existingConversation)) {
    conversation = await updateConversation(existingConversation, body, session)
    console.log('Checking conversation after update: ', conversation)
    if (size(conversation)) {
      await initAfterUpdateProcess(existingConversation, conversation, session)
    }
    console.log('Checking initAfterUpdateProcess finished ')
  } else {
    console.log('Adding conversation ')
    conversation = await addConversation(body, session)
    console.log('Added conversation ')
  }
  console.log('Checking conversation after add or update: ', conversation)
  if (size(conversation)) {
    console.log('Preparing for initiate pusher ')
    const channelList = difference(map(conversation.participants, 'userId'), [
      userId
    ])
    console.log('Prepared channelList ', channelList)
    await pusherHelper.pusherTrigger(channelList, 'new-conversation', {
      _id: conversation?._id
    })
    console.log('pusher Triggered ')
  }
  console.log('Conversation add or update process completed successfully')
  return [conversation]
}

export const updateConversationById = async (req) => {
  const { body = {}, session, user } = req
  const { conversationId, updateType } = body
  const { userId, partnerId } = user

  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['conversationId', 'updateType', 'data'], body)
  if (!body?.data) body.data = {}
  body.data.userId = userId
  body.data.partnerId = partnerId

  const updateData = await conversationHelper.prepareUpdateData(body)
  const updatedConversation = await updateAConversation(
    { _id: conversationId },
    updateData,
    session
  )
  console.log('Updating conversation', body)
  if (updateType === 'typingStatus' && size(updatedConversation)) {
    const usersInfo = await conversationHelper.getUsersInfoForConversation(
      {
        _id: conversationId
      },
      session
    )
    const conversationUserData = size(usersInfo) ? usersInfo[0] : {}
    const result = await pusherHelper.pusherTrigger(
      `private-${conversationId}`,
      'typing-event',
      conversationUserData
    )
    console.log('Pusher trigger Successfully')
    if (size(result) && size(updatedConversation)) return updatedConversation
  }
  return updatedConversation
}

export const updateConversations = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update conversations')
  }

  const updatedConversation = await ConversationCollection.updateMany(
    query,
    data,
    {
      session
    }
  )
  return updatedConversation
}

export const removeConversations = async (query, session) => {
  if (!size(query))
    throw new CustomError(
      400,
      'Query must be required while removing conversations'
    )
  const response = await ConversationCollection.deleteMany(query, {
    session
  })
  console.log('=== Conversations Removed ===', response)
  return response
}
