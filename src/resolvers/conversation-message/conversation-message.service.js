import { difference, find, map, size, sortBy } from 'lodash'
import nid from 'nid'

import { ConversationMessageCollection } from '../models'
import {
  appQueueHelper,
  appHelper,
  conversationMessageHelper,
  conversationHelper,
  notificationLogHelper,
  userHelper,
  pusherHelper
} from '../helpers'
import { appQueueService, conversationService } from '../services'
import { CustomError } from '../common'
import { logService } from '../services'

export const createNewMessageLog = async (
  conversationMessage,
  data,
  session
) => {
  const logData = conversationMessageHelper.prepareNewMessageLogData(
    conversationMessage,
    data
  )
  const insertedLog = await logService.createLog(logData, session)
  return insertedLog
}

export const updateConversationForMessage = async (
  conversationMessage,
  session
) => {
  const { conversationId } = conversationMessage
  const updateData =
    await conversationMessageHelper.prepareConversationUpdateData(
      conversationMessage,
      session
    )
  const updatedConversation = await conversationService.updateAConversation(
    { _id: conversationId },
    updateData,
    session
  )

  return updatedConversation
}

export const initAfterInsertProcess = async (
  conversationMessage = {},
  session
) => {
  await updateConversationForMessage(conversationMessage, session)
  // Todo:: file upload part is pending  "uploader/copy-url-to-s3"
  const { conversationId, partnerId } = conversationMessage
  // await insertInQueueForSendChatNotification(conversationMessage, session) // Send chat notification
  const params = { conversationId }
  const queryData = {
    action: 'send_chat_unread_message_notification_to_user',
    event: 'send_chat_unread_message_notification_to_user',
    params,
    status: { $ne: 'completed' }
  }

  if (partnerId) params.partnerId = partnerId
  const alreadyExistsAppQueue = await appQueueHelper.getAnAppQueue(
    queryData,
    session
  )

  if (!size(alreadyExistsAppQueue)) {
    await createAppQueueForSendChatNotification(params, session)
  }
}

export const insertInQueueForSendChatNotification = async (doc, session) => {
  const { conversationId = '', createdBy } = doc

  const creator = await userHelper.getAnUser({ _id: createdBy }, session)
  const conversation = await conversationHelper.getAConversation(
    {
      _id: conversationId
    },
    session
  )

  if (size(conversation) && size(creator)) {
    const { participants, partnerId: conversationPartnerId = '' } = conversation

    let getCreatorPartnerInfo

    if (conversationPartnerId) {
      getCreatorPartnerInfo = find(
        creator.partners || [],
        (creatorPartnerInfo) =>
          size(creatorPartnerInfo) &&
          creatorPartnerInfo.partnerId === conversationPartnerId
      )
    }

    const creatorPartnerId = getCreatorPartnerInfo?.partnerId || ''

    // Find message list according to conversation id and get last 10 messages with descending order
    let conversationMessageList = await ConversationMessageCollection.find({
      conversationId
    })
      .session(session)
      .sort({ createdAt: -1 })
      .limit(10)

    // Sort by createdAt with ascending order
    conversationMessageList = sortBy(conversationMessageList, 'createdAt')

    // Send message to the user's who are not online
    const participantUserId = map(participants, 'userId').filter(
      (participant) => participant && participant !== createdBy //exclude the creator
    )

    const participantActiveUsers = await Promise.all(
      participantUserId.map(
        async (participant) =>
          await userHelper.getAnUser(
            { _id: participant, 'profile.active': true },
            session
          )
      )
    )

    const participantOfflineUsers = participantActiveUsers.filter(
      (participant) => !(size(participant) && participant.isOnline()) // Exclude the online users
    )

    // Get identity ids according to userId
    const emailContentByIdentityId = {}
    const identityIds = conversation.allIdentityIds()
    const participantsIdentity = conversation.identity || []

    let isNewIdentityId = false

    const emailParticipants = await Promise.all(
      participantOfflineUsers.map(async (participant) => {
        // If participant identity not found then,
        // create new identity and push in participants identity array

        if (
          size(participant) &&
          size(identityIds) &&
          !identityIds[participant._id]
        ) {
          identityIds[participant._id] = nid(17)
          isNewIdentityId = true
          participantsIdentity.push({
            id: identityIds[participant._id],
            userId: participant._id
          })
        }

        // Prepare message content according to sent to user
        let left_user = true,
          right_user = false,
          messageBoxPosition = '',
          messageContent =
            "<table cellpadding='2' cellspacing='0' width='100%'>"

        for (const conversationMessageInfo of conversationMessageList) {
          const messageCreator = await userHelper.getAnUser(
            {
              _id: conversationMessageInfo.createdBy
            },
            session
          )

          const senderUserAvatar = messageCreator?.getAvatar()
          let newMessage = conversationMessageInfo.contentHTML()
          let isSameAsMessageCreatorPartner = false

          //if conversation, current message creator and old message creator will be same partner then return true
          //if current message creator is not partner user;
          // If conversation and old message creator will be same partner then return true
          if (
            conversationPartnerId &&
            (!creatorPartnerId ||
              (creatorPartnerId && creatorPartnerId === conversationPartnerId))
          )
            isSameAsMessageCreatorPartner = !!(await userHelper.getAnUser(
              {
                _id: conversationMessageInfo.createdBy,
                'partners.partnerId': conversationPartnerId
              },
              session
            ))

          // Check file is attached in message
          if (conversationMessageInfo.isFile) {
            if (conversationMessageInfo.isImageFile()) {
              const messageImage =
                conversationMessageInfo.getMessageImage('gallery')
              newMessage = "<img src='" + messageImage + "' alt=''/>"
            } else {
              newMessage = appHelper.translateToUserLng(
                'views.conversation.sent_a_file',
                participant.getLanguage()
              )
            }
          }

          // Find out the sender avatar placement in emails content
          if (
            (!creatorPartnerId &&
              (participant._id === conversationMessageInfo.createdBy ||
                isSameAsMessageCreatorPartner)) ||
            (creatorPartnerId && !isSameAsMessageCreatorPartner)
          ) {
            left_user = false
            right_user = true
            messageBoxPosition = 'float:right;'
          } else {
            left_user = true
            right_user = false
            messageBoxPosition = ''
          }

          messageContent += "<tr><td style='text-align: right; width: 70px;'>"
          if (left_user)
            messageContent +=
              "<img style='width: 40px; height: 40px; border-radius: 50%; margin-right: 10px;' src='" +
              senderUserAvatar +
              "' alt='User' title='User'/>"

          messageContent +=
            "</td><td><div class='message-container' style='" +
            messageBoxPosition +
            " color: #787878; background-color: #eaeaea; border-radius: 5px; font-size: 16px; line-height: 22px; padding: 10px; display: inline-block;'>" +
            newMessage +
            "</div></td><td style='text-align: center; width:80px;'>"

          if (right_user)
            messageContent +=
              "<img style='width: 40px; height: 40px; border-radius: 50%; margin-right: 20px;' src='" +
              senderUserAvatar +
              "' alt='User' title='User'/>"
          messageContent += '</td></tr>'
        }
        messageContent += '</table>'
        emailContentByIdentityId[identityIds[participant._id]] = messageContent
        // End of message content

        return {
          id: identityIds[participant._id],
          email: participant.getEmail(),
          language: participant.getLanguage()
        }
      })
    )

    // Update participants identity information
    if (isNewIdentityId)
      await conversationService.updateAConversation(
        { _id: conversation._id },
        { $set: { identity: participantsIdentity } },
        session
      )

    const mailDomain = process.env.UL_SEND_TO_Domain || ''

    if (size(emailParticipants) && mailDomain) {
      for (const emailInfo of emailParticipants) {
        const params = {
          doc,
          collectionName: 'conversation-messages',
          conversation,
          messageContent: emailContentByIdentityId[emailInfo.id],
          emailInfo,
          notifyType: 'email',
          emailHeaders: { 'Reply-To': emailInfo.id + '@' + mailDomain }
        }
        await prepareAndCreateAppQueueForChatNotification(params, session)
      }
    }
  }
}

const prepareAndCreateAppQueueForChatNotification = async (params, session) => {
  const { doc, conversation, emailHeaders, emailInfo, notifyType } = params
  const { _id: collectionId = '', conversationId } = doc || {}
  const identityId = emailInfo?.id || ''
  console.log('Creating queue for notification', conversationId)
  if (
    !(
      size(doc) ||
      size(conversation) ||
      collectionId ||
      conversationId ||
      identityId
    )
  )
    return false

  const event = 'send_chat_notification'

  // Prepare variables value
  const variablesData = await notificationLogHelper.getVariablesData(
    event,
    doc,
    params
  )

  let sendToUserId = ''
  // Find send to notify userId
  if (size(conversation.identity)) {
    const sendToIdentity = find(
      conversation.identity,
      (identity) => identity.id === identityId
    )

    if (sendToIdentity && sendToIdentity.userId)
      sendToUserId = sendToIdentity.userId
  }
  console.log({ sendToUserId })
  if (!sendToUserId) return false

  const user = await userHelper.getAnUser({ _id: sendToUserId }, session)

  const sendToUserInfo =
    await notificationLogHelper.getNotificationSendToUserInfo(user, notifyType)

  // Creating appQueue for send_chat_notification
  const queueData = {
    event,
    action: 'send_notification',
    priority: 'regular',
    destination: 'notifier',
    params: { emailHeaders, sendToUserInfo, variablesData }
  }

  if (conversation.partnerId)
    queueData.params.partnerId = conversation.partnerId

  const result = await appQueueService.insertInQueue(queueData, session)
  console.log('Successfully created queue for notification', result?._id)
}

export const createConversationMessage = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(
      400,
      'Can not create conversation-message without data'
    )
  }
  const message = await ConversationMessageCollection.create([data], {
    session
  })
  return message
}

export const createConversationMessages = async (
  messagesData = [],
  session
) => {
  const messages = []
  for (const messageData of messagesData) {
    const [message] = await createConversationMessage(messageData, session)
    if (size(message)) messages.push(message)
  }
  return messages
}

export const addConversationMessage = async (req) => {
  const { body, session, user } = req
  const { accountId, content, conversationId, isFile, propertyId, tenantId } =
    body
  const { partnerId, userId } = user

  const messageData = {
    conversationId,
    createdBy: userId,
    content,
    isFile
  }

  const data = {
    conversationId,
    data: {
      userId,
      partnerId,
      accountId,
      propertyId,
      tenantId
    }
  }
  const conversation = await conversationHelper.getConversationInfo(data)
  if (!size(conversation)) throw new CustomError(404, `Conversation not found`)

  const [conversationMessageData] = await createConversationMessage(
    messageData,
    session
  )
  const conversationMessage = size(conversationMessageData)
    ? conversationMessageData.toObject()
    : {}
  if (size(conversationMessage)) {
    if (isFile) {
      conversationMessage.fileUrl =
        await conversationMessageHelper.getConversationFileUrl({
          conversationId,
          content
        })
    }

    conversationMessage.userInfo =
      await conversationMessageHelper.getUserInfoForConversationMessage({
        _id: userId
      })
    try {
      await pusherHelper.pusherTrigger(
        `private-${conversationId}`,
        'message',
        conversationMessage
      )
      const channelList = difference(
        map(conversation?.participants, 'userId'),
        [userId]
      )
      await pusherHelper.pusherTrigger(channelList, 'new-conversation', {
        _id: conversation?._id
      })
    } catch (error) {
      throw new CustomError(
        error?.code || 500,
        `Internal server error when adding a new message, error: ${error?.message}`
      )
    }
    await createNewMessageLog(conversationMessageData, data, session)
    await initAfterInsertProcess(conversationMessageData, session)
  }
  return [conversationMessage]
}

export const addConversationMessagesByLambda = async (req) => {
  const { body, session } = req
  appHelper.compactObject(body)
  const requiredFields = ['conversationId', 'identityUserId']
  appHelper.checkRequiredFields(requiredFields, body)
  const {
    attachmentsFileName,
    conversationId,
    identityUserId,
    messageContent
  } = body
  appHelper.validateId({ conversationId })
  appHelper.validateId({ identityUserId })

  if (!(messageContent || size(attachmentsFileName)))
    throw new CustomError(400, `Missing content`)

  const conversationMessagesData = []
  if (identityUserId) {
    if (messageContent) {
      conversationMessagesData.push({
        conversationId,
        createdBy: identityUserId,
        content: messageContent
      })
    }
    // For attachments
    for (const attachmentFileName of attachmentsFileName) {
      conversationMessagesData.push({
        conversationId,
        createdBy: identityUserId,
        content: attachmentFileName,
        isFile: true
      })
    }
  }
  const conversationMessages = await createConversationMessages(
    conversationMessagesData,
    session
  )

  const userInfo =
    await conversationMessageHelper.getUserInfoForConversationMessage({
      _id: identityUserId
    })

  for (const conversationMessage of conversationMessages) {
    await initAfterInsertProcess(conversationMessage, session)
    const conversationMessageData = conversationMessage.toObject()

    if (conversationMessage.isFile) {
      conversationMessageData.fileUrl =
        await conversationMessageHelper.getConversationFileUrl({
          conversationId,
          content: conversationMessage.content
        })
    }
    const pusherData = { ...conversationMessageData, userInfo }
    await pusherHelper.pusherTrigger(
      `private-${conversationId}`,
      'message',
      pusherData
    )
  }
  const conversation = await conversationHelper.getAConversation({
    _id: conversationId
  })
  const channelList = map(conversation.participants, 'userId')
  if (size(channelList))
    await pusherHelper.pusherTrigger(channelList, 'new-conversation', {
      _id: conversation?._id
    })
  return conversationMessages
}

const createAppQueueForSendChatNotification = async (params = {}, session) => {
  const queueData = {
    action: 'send_chat_unread_message_notification_to_user',
    delaySeconds: 300,
    destination: 'user',
    event: 'send_chat_unread_message_notification_to_user',
    status: 'new',
    params,
    priority: 'regular'
  }
  await appQueueService.insertInQueue(queueData, session)
}

export const prepareMessageContendAccordingToSentUser = async (
  {
    conversationMessageList,
    conversationPartnerId,
    creatorPartnerId,
    participant
  },
  session
) => {
  let left_user = true,
    right_user = false,
    messageBoxPosition = '',
    messageContent = "<table cellpadding='2' cellspacing='0' width='100%'>"

  for (const conversationMessageInfo of conversationMessageList) {
    const messageCreator = await userHelper.getAnUser(
      {
        _id: conversationMessageInfo.createdBy
      },
      session
    )

    const senderUserAvatar = messageCreator?.getAvatar()
    let newMessage = conversationMessageInfo.contentHTML()
    let isSameAsMessageCreatorPartner = false

    //if conversation, current message creator and old message creator will be same partner then return true
    //if current message creator is not partner user;
    // If conversation and old message creator will be same partner then return true
    if (
      conversationPartnerId &&
      (!creatorPartnerId ||
        (creatorPartnerId && creatorPartnerId === conversationPartnerId))
    )
      isSameAsMessageCreatorPartner = !!(await userHelper.getAnUser(
        {
          _id: conversationMessageInfo.createdBy,
          'partners.partnerId': conversationPartnerId
        },
        session
      ))

    // Check file is attached in message
    if (conversationMessageInfo.isFile) {
      if (conversationMessageInfo.isImageFile()) {
        const messageImage = conversationMessageInfo.getMessageImage('gallery')
        newMessage = "<img src='" + messageImage + "' alt=''/>"
      } else {
        newMessage = appHelper.translateToUserLng(
          'views.conversation.sent_a_file',
          participant.getLanguage()
        )
      }
    }

    // Find out the sender avatar placement in emails content
    if (
      (!creatorPartnerId &&
        (participant._id === conversationMessageInfo.createdBy ||
          isSameAsMessageCreatorPartner)) ||
      (creatorPartnerId && !isSameAsMessageCreatorPartner)
    ) {
      left_user = false
      right_user = true
      messageBoxPosition = 'float:right;'
    } else {
      left_user = true
      right_user = false
      messageBoxPosition = ''
    }

    messageContent += "<tr><td style='text-align: right; width: 70px;'>"
    if (left_user)
      messageContent +=
        "<img style='width: 40px; height: 40px; border-radius: 50%; margin-right: 10px;' src='" +
        senderUserAvatar +
        "' alt='User' title='User'/>"

    messageContent +=
      "</td><td><div class='message-container' style='" +
      messageBoxPosition +
      " color: #787878; background-color: #eaeaea; border-radius: 5px; font-size: 16px; line-height: 22px; padding: 10px; display: inline-block;'>" +
      newMessage +
      "</div></td><td style='text-align: center; width:80px;'>"

    if (right_user)
      messageContent +=
        "<img style='width: 40px; height: 40px; border-radius: 50%; margin-right: 20px;' src='" +
        senderUserAvatar +
        "' alt='User' title='User'/>"
    messageContent += '</td></tr>'
  }
  messageContent += '</table>'
  return messageContent
}

export const prepareNotificationContentAndCreateNotification = async (
  params = {},
  session
) => {
  const {
    conversationMessageList,
    conversationPartnerId,
    creatorPartnerId,
    participantUsers
  } = params
  let { conversation } = params
  const mailDomain = process.env.UL_SEND_TO_Domain || ''
  if (!mailDomain) {
    throw new CustomError(404, 'Mail domain not found')
  }

  // Get identity ids according to userId
  const identityIds = conversation?.allIdentityIds()
  const participantsIdentity = conversation?.identity || []
  let isNewIdentityId = false

  const emailParticipants = await Promise.all(
    participantUsers.map(async (participant) => {
      // If participant identity not found then,
      // create new identity and push in participants identity array
      if (size(participant) && identityIds && !identityIds[participant._id]) {
        identityIds[participant._id] = nid(17)
        isNewIdentityId = true
        participantsIdentity.push({
          id: identityIds[participant._id],
          userId: participant._id
        })
      }

      const messageContent = await prepareMessageContendAccordingToSentUser(
        {
          conversationMessageList,
          conversationPartnerId,
          creatorPartnerId,
          participant
        },
        session
      )

      const emailInfo = {
        email: participant.getEmail(),
        id: identityIds[participant._id],
        language: participant.getLanguage()
      }
      return {
        emailInfo,
        messageContent
      }
    })
  )

  console.log({ isNewIdentityId })

  // Update participants identity information
  if (isNewIdentityId) {
    conversation = await conversationService.updateAConversation(
      { _id: conversation._id },
      { $set: { identity: participantsIdentity } },
      session
    )
  }

  if (size(emailParticipants)) {
    console.log('Found email participants', size(emailParticipants))
    const promiseArray = []
    for (const emailParticipant of emailParticipants) {
      const { emailInfo = {}, messageContent } = emailParticipant || {}
      const params = {
        collectionName: 'conversation-messages',
        conversation,
        doc: conversationMessageList[0],
        emailHeaders: { 'Reply-To': emailInfo.id + '@' + mailDomain },
        emailInfo,
        messageContent,
        notifyType: 'email'
      }
      promiseArray.push(
        prepareAndCreateAppQueueForChatNotification(params, session)
      )
    }
    if (size(promiseArray)) await Promise.all(promiseArray)
  }
  return isNewIdentityId
}

export const sendConversationNotification = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['conversationId'], body)
  const { conversationId, partnerId } = body
  console.log('Sending conversation notification', conversationId)
  const conversationQuery = { _id: conversationId }
  if (partnerId) conversationQuery.partnerId = partnerId

  const conversation = await conversationHelper.getAConversation(
    conversationQuery,
    session
  )

  if (!size(conversation)) {
    throw new CustomError(404, 'Conversation not found')
  }
  console.log('Found unread users', conversation.unreadBy)
  if (size(conversation.unreadBy)) {
    const participantUsers = await userHelper.getUsers({
      _id: { $in: conversation.unreadBy },
      'profile.active': true,
      'profile.disableMessageNotification': { $ne: true }
    })
    console.log('Participant users size', size(participantUsers))
    if (size(participantUsers)) {
      // Find message list according to conversation id and get last 10 messages with descending order
      let conversationMessageList = await ConversationMessageCollection.find({
        conversationId
      })
        .session(session)
        .sort({ createdAt: -1 })
        .limit(10)

      // Sort by createdAt with ascending order
      conversationMessageList = sortBy(conversationMessageList, 'createdAt')
      if (size(conversationMessageList)) {
        const creator = await userHelper.getAnUser(
          { _id: conversationMessageList[0]?.createdBy },
          session
        )

        const conversationPartnerId = conversation?.partnerId
        let creatorPartnerId = ''

        if (conversationPartnerId) {
          const creatorPartnerInfo = find(
            creator?.partners || [],
            (creatorPartnerInfo) =>
              size(creatorPartnerInfo) &&
              creatorPartnerInfo.partnerId === conversationPartnerId
          )
          if (size(creatorPartnerInfo)) creatorPartnerId = conversationPartnerId
        }
        console.log({ creatorPartnerId, conversationPartnerId })
        await prepareNotificationContentAndCreateNotification(
          {
            conversation,
            conversationMessageList,
            conversationPartnerId,
            creatorPartnerId,
            participantUsers
          },
          session
        )
      }
    }
  }
  return {
    msg: 'Notification send to all Successfully',
    code: 200
  }
}
