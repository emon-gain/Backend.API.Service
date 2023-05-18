import { clone, extend, filter, find, indexOf, isEmpty, size } from 'lodash'

import { appPermission, CustomError } from '../common'
import {
  AccountCollection,
  ContractCollection,
  ConversationCollection,
  ListingCollection,
  RoomMateGroupCollection,
  TenantCollection,
  UserCollection
} from '../models'
import {
  appHelper,
  contractHelper,
  userHelper,
  conversationMessageHelper
} from '../helpers'

export const getUpcomingContractIdByPropertyId = async (
  partnerId,
  propertyId
) => {
  const contract = await ContractCollection.findOne({
    partnerId,
    propertyId,
    status: 'upcoming'
  })
  return contract ? contract._id : ''
}

export const findUserByEmail = async (email) => {
  const user = await UserCollection.findOne({
    $or: [{ 'emails.address': email }, { 'services.facebook.email': email }]
  })
  return user
}

export const prepareTenantProperties = async (
  propertyId,
  partnerId,
  contractId,
  createdBy
) => {
  const propertyInfo = await ListingCollection.findOne({
    _id: propertyId,
    partnerId
  })
  if (propertyInfo) {
    const { _id, accountId, branchId, agentId } = propertyInfo
    const tenantData = {
      propertyId: _id,
      accountId,
      branchId,
      agentId,
      status: 'interested',
      createdAt: new Date(),
      createdBy
    }
    if (contractId) {
      tenantData.contractId = contractId
    }
    return tenantData
  }
  return {}
}

export const updateTenantInfo = async (updateData) => {
  const {
    tenantInfo,
    tenantData,
    tenantAddData,
    contractId,
    isShowError,
    session
  } = updateData
  console.log('tenantData on update: ', tenantData)
  const { type } = tenantInfo
  if (type === 'archived') {
    throw new CustomError(401, 'this tenant is archived')
  }
  let activeProperties = clone(tenantInfo.properties)
  console.log('activeProperties on update: ', activeProperties)
  const findPartnerProperty = find(activeProperties, (activeProperty) => {
    if (
      activeProperty &&
      activeProperty.propertyId === tenantData.propertyId &&
      activeProperty.accountId === tenantData.accountId &&
      activeProperty.branchId === tenantData.branchId &&
      activeProperty.agentId === tenantData.agentId &&
      ((contractId && contractId === activeProperty.contractId) || !contractId)
    ) {
      return activeProperty
    }
  })
  console.log('findPartnerProperty on update: ', findPartnerProperty)
  // For closed contract
  if (
    findPartnerProperty &&
    findPartnerProperty.status === 'closed' &&
    (!isShowError || isShowError)
  ) {
    activeProperties = filter(activeProperties, (tenantProperty) => {
      if (
        tenantProperty &&
        tenantProperty.propertyId === findPartnerProperty.propertyId &&
        tenantProperty.accountId === findPartnerProperty.accountId &&
        tenantProperty.branchId === findPartnerProperty.branchId &&
        tenantProperty.agentId === findPartnerProperty.agentId &&
        ((contractId && contractId === tenantProperty.contractId) ||
          !contractId) &&
        tenantProperty.status !== 'active'
      ) {
        tenantProperty.status = 'invited'
      }
      return tenantProperty
    })
  } else if (
    findPartnerProperty &&
    isShowError &&
    findPartnerProperty.status !== 'closed'
  ) {
    throw new CustomError(401, 'you have already added')
  }
  console.log('findPartnerProperty on update: ', findPartnerProperty)
  if (!findPartnerProperty) {
    activeProperties.push(tenantData)
  }
  tenantAddData.properties = activeProperties
  console.log('tenantAddData on update: ', tenantAddData)
  const isUpdate = await TenantCollection.findOneAndUpdate(
    { _id: tenantInfo._id },
    { $set: tenantAddData },
    { session }
  )
  console.log('isUpdate: ', isUpdate)
  if (isUpdate) {
    return tenantInfo._id
  }
  console.log('returning false: ')
  return false
}

export const addOrUpdateUser = async (userId, email, name, session) => {
  const existingUser = await findUserByEmail(email)
  const personUserData = {}
  if (existingUser && name && existingUser.getName() !== name) {
    personUserData['profile.name'] = name
  }
  if (existingUser && size(personUserData)) {
    await UserCollection.updateOne(
      { _id: existingUser._id },
      { $set: personUserData },
      { session }
    )
  }
  if (!existingUser && !userId && email) {
    const userData = {
      email: appHelper.getLowerCase(email),
      profile: {
        name
      }
    }
    const user = await UserCollection.create([userData], { session })
    return user[0]._id
  }
  return ''
}

export const addTenant = async (tenantData, session) => {
  console.log('executing add tenant with tenant data: ', tenantData)
  let contractId = ''
  let tenantId = ''
  const { partnerId, email, name, listingId, userId } = tenantData
  let { senderId } = tenantData
  const tenantAddData = {
    partnerId,
    name,
    userId: senderId,
    status: 'interested'
  }
  const propertyId = listingId
  console.log('partnerId', partnerId)
  console.log('propertyId', propertyId)
  if (partnerId && propertyId) {
    contractId = await getUpcomingContractIdByPropertyId(partnerId, propertyId)
  }
  console.log('email: ', email)
  if (partnerId && email && propertyId) {
    const newUserId = await addOrUpdateUser(userId, email, name, session)
    console.log('newUserId: ', newUserId)
    if (newUserId) {
      senderId = newUserId
    }
    const tenantPropertiesData = await prepareTenantProperties(
      propertyId,
      partnerId,
      contractId,
      senderId
    )
    console.log('tenantPropertiesData: ', tenantPropertiesData)
    if (size(tenantPropertiesData)) {
      tenantAddData.properties = [tenantPropertiesData]
    }
    const tenantInfo = await TenantCollection.findOne({
      userId: senderId,
      partnerId
    })
    console.log('tenantInfo: ', tenantInfo)
    if (size(tenantInfo)) {
      const updateData = {
        tenantInfo,
        tenantAddData,
        tenantData: tenantPropertiesData,
        contractId,
        session
      }
      tenantId = await updateTenantInfo(updateData)
      console.log('tenantInfo udpated tenantId: ', tenantId)
      return tenantId
    }
    tenantAddData.type = 'active'
    const addedTenant = await TenantCollection.create([tenantAddData], {
      session
    })
    console.log('addedTenant udpated ', addedTenant)
    const [newTenant] = addedTenant
    return newTenant._id
  }
}

export const prepareAddDataByListingId = async (body, session) => {
  const { listingId, senderId } = body
  let { receiverId } = body
  let mainAppUserId = ''
  const listing = await getListing(listingId)
  console.log('Found  listing', listing)
  if (!size(listing)) {
    return {}
  }
  const { ownerId, partnerId, accountId } = listing
  receiverId = ownerId
  const listingOwner = await listing.ownerOrGroupInfo()
  console.log('Found  listingOwner', listingOwner)
  const userInfo = await UserCollection.findOne({ _id: senderId })
  console.log('Found  userInfo', userInfo)
  if (
    listingOwner.isGroup &&
    userInfo.getRoommateGroupId() !== listingOwner._id
  ) {
    receiverId = listingOwner._id
  }
  if (receiverId) {
    mainAppUserId = receiverId
  }
  const conversationData = {}
  console.log('Found  partnerId', partnerId)
  if (partnerId) {
    conversationData.propertyId = listingId
    conversationData.partnerId = partnerId
    conversationData.accountId = accountId
    console.log('Found  userInfo 2', userInfo)
    if (userInfo) {
      const tenantAddData = {
        partnerId,
        email: userInfo.getEmail(),
        name: userInfo.getName(),
        listingId,
        userId: senderId,
        status: 'interested',
        senderId
      }
      console.log('Found  tenantAddData', tenantAddData)
      const tenantId = await addTenant(tenantAddData, session)
      console.log('Found  tenantId', tenantId)
      if (tenantId) {
        conversationData.tenantId = tenantId
      }
    }
  } else {
    conversationData.listingId = listingId
  }
  console.log('Found  conversationData', conversationData)
  console.log('Found  mainAppUserId', mainAppUserId)
  console.log('Found  receiverId', receiverId)
  return {
    conversationData,
    mainAppUserId,
    receiverId
  }
}

export const getTenantUserId = async (tenantId, partnerId) => {
  const tenant = await TenantCollection.findOne({ _id: tenantId, partnerId })
  if (tenant && tenant.userId) {
    return tenant.userId
  }
  return ''
}

export const getAccountPersonId = async (accountId, partnerId) => {
  const account = await AccountCollection.findOne({ _id: accountId, partnerId })

  if (account && account.personId) {
    return account.personId
  }
}

export const prepareAddDataByPartnerId = async (body) => {
  const { partnerId, propertyId, tenantId, senderId, chatWith, contractId } =
    body
  let { accountId } = body
  const conversationData = { partnerId }
  let participantsData = []
  if (chatWith === 'tenant_landlord' || chatWith === 'tenant_agent') {
    participantsData = [{ userId: senderId, isVisibleInMainApp: true }] // Added 'isVisibleInMainApp' means Sender user see this conversation from main app
  }
  if (chatWith === 'tenant_landlord') {
    conversationData.hideForPartner = true
  }
  if (propertyId) {
    conversationData.propertyId = propertyId
    const property = await ListingCollection.findOne({
      _id: propertyId,
      partnerId
    })
    const { agentId } = property
    if (chatWith === 'tenant_landlord' && property && property.accountId) {
      conversationData.accountId = property.accountId
    } else if (chatWith === 'tenant_agent' && agentId) {
      conversationData.agentId = agentId
    } else if (property && property.accountId) {
      accountId = property.accountId || ''
    }
  }
  let receiverId = ''
  let mainAppUserId = ''
  console.log('Checking tenantId for conversation: ', tenantId)
  console.log('Checking accountId for conversation: ', accountId)
  if (tenantId) {
    conversationData.tenantId = tenantId
    const tenantUserId = await getTenantUserId(tenantId, partnerId)
    if (tenantUserId) {
      receiverId = tenantUserId
      mainAppUserId = tenantUserId
    }
  } else if (accountId) {
    conversationData.accountId = accountId
    const accountPersonId = await getAccountPersonId(accountId, partnerId)
    console.log('Checking accountPersonId for conversation: ', accountPersonId)
    if (accountPersonId) {
      receiverId = accountPersonId
      mainAppUserId = accountPersonId
    }
  }
  if (contractId) {
    conversationData.contractId = contractId
  }
  console.log('Checking conversationData for conversation: ', conversationData)
  return {
    conversationData,
    receiverId,
    mainAppUserId,
    participantsData
  }
}

export const prepareParticipantsByReceiver = async (receiverData) => {
  const { receiverId, participantsData, mainAppUser, chatWith } = receiverData
  let { isGroup } = receiverData
  // Find and check receiver is user or group
  let receiver = await UserCollection.findOne({ _id: receiverId })
  if (!receiver) {
    receiver = RoomMateGroupCollection.findOne({ _id: receiverId })
    if (!receiver) {
      throw new CustomError(404, 'Participant not found')
    }
    participantsData.push({ groupId: receiverId })
    isGroup = true
  } else {
    const mainAppVisibility = !!mainAppUser
    if (chatWith === 'tenant_landlord' || chatWith === 'tenant_agent') {
      participantsData.push({ userId: receiverId })
    } else {
      participantsData.push({
        userId: receiverId,
        isVisibleInMainApp: mainAppVisibility
      })
    }
  }
  return { updatedParticipants: participantsData, isGroup }
}

export const prepareAddData = async (body, session) => {
  const { listingId, partnerId, senderId, chatWith } = body
  let { receiverId } = body
  let mainAppUser = ''
  let participantsData = [{ userId: senderId }]
  let addData = {}
  console.log(
    'listingId, partnerId, senderId, chatWith ',
    listingId,
    partnerId,
    senderId,
    chatWith
  )
  if (listingId) {
    const listingAddData = await prepareAddDataByListingId(body, session)
    console.log('listingAddData ', listingAddData)
    if (listingAddData.conversationData) {
      addData = extend(addData, listingAddData.conversationData)
    }
    if (listingAddData.mainAppUserId) {
      mainAppUser = listingAddData.mainAppUserId
    }
    if (listingAddData.receiverId) {
      receiverId = listingAddData.receiverId || ''
    }
  } else if (partnerId) {
    console.log('Checking req body for conversation: ', body)
    const partnerAddData = await prepareAddDataByPartnerId(body, session)
    console.log('Checking add data for conversation: ', partnerAddData)
    if (partnerAddData.conversationData) {
      addData = extend(addData, partnerAddData.conversationData)
    }
    if (partnerAddData.receiverId) {
      receiverId = partnerAddData.receiverId || ''
    }
    if (partnerAddData.mainAppUserId) {
      mainAppUser = partnerAddData.mainAppUserId
    }
    if (size(partnerAddData.participantsData)) {
      participantsData = partnerAddData.participantsData || []
    }
  }
  if (isEmpty(receiverId)) {
    throw new CustomError(403, 'Conversation requires at least two people')
  }
  const isGroup = false
  const receiverData = {
    receiverId,
    participantsData,
    isGroup,
    mainAppUser,
    chatWith
  }
  const { updatedParticipants } = await prepareParticipantsByReceiver(
    receiverData
  )
  console.log('updatedParticipants ', updatedParticipants)
  participantsData = updatedParticipants
  addData.participants = participantsData
  console.log('addData ', addData)
  if (senderId) addData.createdBy = senderId
  console.log('addData ', addData)
  return addData
}

export const getListing = async (listingId) => {
  const listing = await ListingCollection.findOne({ _id: listingId })
  return listing
}

export const prepareQueryByListingId = async (listingId) => {
  // Check is it a valid listing id and determine the receiver is a group or user
  const listing = await getListing(listingId)
  if (!size(listing)) {
    return {}
  }
  const query = {}
  const { partnerId } = listing
  // If find partnerId with the listing
  // Prepare conversation data as partner conversation
  if (partnerId) {
    query.propertyId = listingId
  } else {
    query.listingId = listingId
  }
  return query
}

export const prepareQueryByPartnerId = (body) => {
  const { partnerId, chatWith } = body
  const query = { partnerId }

  if (chatWith === 'tenant_landlord') {
    query.hideForPartner = { $exists: true }
  } else {
    query.hideForPartner = { $exists: false }
  }
  return query
}

export const prepareQueryByPropertyInfo = async (propertyId, chatWith) => {
  const property = await getListing(propertyId)
  const propertyQuery = { propertyId }
  const { accountId, agentId } = property
  const propertyAccountId = accountId
  if (chatWith === 'tenant_landlord' && accountId) {
    propertyQuery.accountId = accountId // For tenant and landlord chat
  } else if (chatWith === 'tenant_agent' && agentId) {
    propertyQuery.agentId = agentId // For tenant and agent chat
  }
  return { propertyQuery, propertyAccountId }
}

export const prepareQueryByPartnerInfo = async (body) => {
  const { propertyId, contractId, tenantId } = body
  let { accountId } = body
  let query = {}
  const partnerQuery = prepareQueryByPartnerId(body)
  query = extend(query, partnerQuery)
  if (propertyId) {
    const { propertyQuery, propertyAccountId } =
      await prepareQueryByPropertyInfo(propertyId)
    query = extend(query, propertyQuery)
    if (propertyAccountId) {
      accountId = propertyAccountId
    }
    if (contractId) {
      query.contractId = contractId
    }
    if (tenantId) {
      query.tenantId = tenantId
    } else if (accountId && !propertyId) {
      query.accountId = accountId
    }
  }
  if (tenantId) query.tenantId = tenantId
  else if (accountId && !propertyId) query.accountId = accountId
  if (contractId) query.contractId = contractId
  return query
}

export const prepareParticipantsQuery = async (body) => {
  const {
    partnerId,
    propertyId,
    accountId,
    tenantId,
    senderId,
    receiverId,
    listingId
  } = body
  const query = {}
  if ((partnerId && !propertyId && !accountId && !tenantId) || !partnerId) {
    // Find conversation by user, group and listing
    query.participants = {
      $all: [
        { $elemMatch: { userId: senderId } },
        {
          $elemMatch: { $or: [{ groupId: receiverId }, { userId: receiverId }] }
        }
      ]
    }
  }
  const isGroup = !(await UserCollection.findOne({ _id: receiverId }))
  // Message is not sent to any group and not to any listing and not to any partner(ex: account, property, tenant),
  // So the total participants should be 2
  if (!isGroup && !listingId && !partnerId) {
    query.participants.$size = 2
  }
  return query
}

export const prepareConversationQuery = async (body) => {
  const { listingId, partnerId } = body
  let query = {}
  if (listingId) {
    const listingQuery = await prepareQueryByListingId(listingId)
    query = extend(query, listingQuery)
  } else if (partnerId) {
    const partnerQuery = await prepareQueryByPartnerInfo(body)
    query = extend(query, partnerQuery)
  } else {
    query.listingId = { $exists: false } // It's private message. don't select the listing related conversation.
  }
  const participantsQuery = await prepareParticipantsQuery(body)
  if (size(participantsQuery)) {
    query = extend(query, participantsQuery)
  }
  return query
}

export const getExistingConversation = async (body, session) => {
  const query = await prepareConversationQuery(body)
  const conversation = await ConversationCollection.findOne(query)
    .sort({ createdAt: -1 })
    .session(session)
  return conversation
}

export const prepareParticipantsAndRoommateGroupId = async (data) => {
  const { userId } = data
  const userInfo = await UserCollection.findOne({ _id: userId })
  const participantsElemMatch = [{ userId }]
  let roommateGroupId = ''
  if (userInfo && userInfo.profile && userInfo.profile.roommateGroupId) {
    roommateGroupId = userInfo.profile.roommateGroupId || ''
    participantsElemMatch.push({ groupId: userInfo.profile.roommateGroupId })
  }
  return { participantsElemMatch, roommateGroupId }
}

export const getConversationAccessQuery = async (data) => {
  const { userId, partnerId, accountId, propertyId, tenantId, contractId } =
    data
  const { participantsElemMatch } = await prepareParticipantsAndRoommateGroupId(
    data
  )
  const { roommateGroupId } = await prepareParticipantsAndRoommateGroupId(data)
  let accessOrQuery = []
  if (partnerId) {
    const accessQuery = { partnerId }
    if (accountId) {
      accessQuery.accountId = accountId
    }
    if (propertyId) {
      accessQuery.propertyId = propertyId
    }
    if (tenantId) {
      accessQuery.tenantId = tenantId
    }
    if (contractId) {
      accessQuery.contractId = contractId
    }
    if (accountId || propertyId || tenantId || contractId) {
      return accessQuery // Conversation list access query for account, property and tenant view
    }
    accessOrQuery = [
      {
        createdBy: userId,
        published: { $in: [false, null] },
        partnerId
      },
      {
        published: true,
        participants: { $elemMatch: { $or: participantsElemMatch } },
        partnerId
      }
    ]
  } else {
    participantsElemMatch.push({
      userId,
      isVisibleInMainApp: { $exists: true }
    })
    const tenantParticipant = [{ userId, tenantId: { $exists: true } }] // If tenantId exist in participants array
    if (roommateGroupId) {
      tenantParticipant.push({ groupId: roommateGroupId })
    }
    // All conversation list access query for main app
    accessOrQuery = [
      {
        createdBy: userId,
        published: { $in: [false, null] },
        partnerId: { $exists: false }
      },
      {
        createdBy: userId,
        published: { $in: [false, null] },
        partnerId: { $exists: true }
      },
      {
        published: true,
        participants: { $elemMatch: { $or: participantsElemMatch } }
      }
    ]
  }
  return { $or: accessOrQuery }
}

export const isPartnerTenant = async (userId, partnerId) => {
  const user = await UserCollection.findOne({ _id: userId })
  if (user && size(user.partners)) {
    return !!find(
      user.partners,
      (partner) => partner.partnerId === partnerId && partner.type === 'tenant'
    )
  }
}

export const isPartnerLandlord = async (userId, partnerId) => {
  const user = await UserCollection.findOne({ _id: userId })
  if (user && size(user.partners)) {
    return !!find(
      user.partners,
      (partner) => partner.partnerId === partnerId && partner.type === 'account'
    )
  }
}

export const getConversationInfo = async (body) => {
  const { data, conversationId } = body
  const { userId, partnerId } = data
  const conversationQuery = [{ _id: conversationId }]
  const conversationAccessQuery = await getConversationAccessQuery(data)

  if (size(conversationAccessQuery)) {
    conversationQuery.push(conversationAccessQuery)
  }
  // For hide 'tenant' and 'landlord' conversation
  if (
    !(
      (await isPartnerTenant(userId, partnerId)) ||
      (await isPartnerLandlord(userId, partnerId))
    )
  ) {
    conversationQuery.push({ hideForPartner: { $exists: false } })
  }

  const conversation = await ConversationCollection.findOne({
    $and: conversationQuery
  })
  return conversation
}

export const prepareArchiveUpdateData = (body, conversationInfo) => {
  const { data } = body
  const { userId } = data
  if (conversationInfo) {
    const { archivedBy } = conversationInfo
    if (size(archivedBy) && indexOf(archivedBy, userId) !== -1) {
      return { $pull: { archivedBy: userId } }
    }
    return { $addToSet: { archivedBy: userId } }
  }
  return {}
}

export const prepareFavoriteUpdateData = (body, conversationInfo) => {
  const { data } = body
  const { userId } = data
  if (conversationInfo) {
    const { favoriteBy } = conversationInfo
    if (size(favoriteBy) && indexOf(favoriteBy, userId) !== -1) {
      return { $pull: { favoriteBy: userId } }
    }
    return { $addToSet: { favoriteBy: userId } }
  }
  return {}
}

export const prepareAddParticipantUpdateData = (body, conversationInfo) => {
  const { data } = body
  const { participantId } = data
  if (conversationInfo && participantId) {
    return { $addToSet: { participants: { userId: participantId } } } // Pushing new participant in conversation participants
  }
  return {}
}
export const prepareMessageSeenQuery = (body, conversationInfo) => {
  const { data } = body
  const { userId } = data
  if (size(conversationInfo)) {
    const { unreadBy } = conversationInfo
    if (size(unreadBy) && indexOf(unreadBy, userId) !== -1) {
      return { $pull: { unreadBy: userId } }
    }
  }
  return {}
}

export const prepareTypingStatusUpdateData = async (body) => {
  const { conversationId, data } = body
  const { userId, isTyping = false } = data
  let hasUserStatus = false
  let userStatusData = []
  const conversation = await getAConversation({ _id: conversationId })
  if (conversation && size(conversation.userStatus)) {
    userStatusData = filter(conversation.userStatus, (userStatusInfo) => {
      if (userStatusInfo && userStatusInfo.userId === userId) {
        userStatusInfo.isTyping = isTyping
        hasUserStatus = true
      }
      return userStatusInfo
    })
    if (!hasUserStatus) {
      userStatusData.push({ userId, isTyping })
    }
  } else {
    userStatusData.push({ userId, isTyping })
  }
  console.log('Conversation typing status data', userStatusData)
  return { $set: { userStatus: userStatusData } }
}

export const prepareUpdateData = async (body) => {
  const { updateType } = body
  const conversationInfo = await getConversationInfo(body)

  const updateData = {
    archive: prepareArchiveUpdateData,
    favorite: prepareFavoriteUpdateData,
    typingStatus: prepareTypingStatusUpdateData,
    addParticipant: prepareAddParticipantUpdateData,
    seen: prepareMessageSeenQuery
  }
  if (updateData[updateType]) {
    const result = await updateData[updateType](body, conversationInfo)
    return result
  }
  return {}
}

export const getConversationsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const conversations = await ConversationCollection.find(query)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return conversations
}

export const countConversations = async (query, session) => {
  const numberOfConversation = await ConversationCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfConversation
}

export const getConversations = async (req) => {
  const { body, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { participantIdentityId } = query

  if (participantIdentityId) {
    query['identity.id'] = participantIdentityId
    delete query.participantIdentityId
  }

  const conversationsData = await getConversationsForQuery(body)
  const filteredDocuments = await countConversations(query)
  const totalDocuments = await countConversations({})
  return {
    data: conversationsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const getSortSkipLimit = (params) => {
  const { limit, skip, sort } = params

  const result = [
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ]

  return result
}

const getListingInfo = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            title: 1,
            listed: 1,
            location: 1,
            images: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  {
    $lookup: {
      from: 'listings',
      localField: 'listingId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            title: 1,
            listed: 1,
            location: 1,
            images: 1
          }
        }
      ],
      as: 'listingInfo'
    }
  },
  {
    $addFields: {
      listingInfo: { $setUnion: ['$listingInfo', '$propertyInfo'] }
    }
  },
  { $unwind: { path: '$listingInfo', preserveNullAndEmptyArrays: true } }
]

const getContractPipeline = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            status: 1,
            contractStartDate: '$rentalMeta.contractStartDate',
            contractEndDate: '$rentalMeta.contractEndDate',
            partnerId: 1
          }
        }
      ],
      as: 'contractInfo'
    }
  },
  { $unwind: { path: '$contractInfo', preserveNullAndEmptyArrays: true } }
]

const getPartnerPipeline = () => [
  {
    $lookup: {
      from: 'partners',
      localField: 'contractInfo.partnerId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            name: 1,
            subDomain: 1
          }
        }
      ],
      as: 'partnerInfo'
    }
  },
  { $unwind: { path: '$partnerInfo', preserveNullAndEmptyArrays: true } }
]

const getPipelineForOtherParticipantInfo = (userId) => [
  {
    $addFields: {
      loggedInUser: userId
    }
  },
  {
    $addFields: {
      participantOtherUsers: {
        $filter: {
          input: '$participants',
          as: 'user',
          cond: {
            $ne: ['$$user.userId', '$loggedInUser']
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'users',
      localField: 'participantOtherUsers.userId',
      foreignField: '_id',
      let: { conversationId: '$_id' },
      pipeline: [
        {
          $addFields: {
            conversationId: '$$conversationId'
          }
        },
        {
          $lookup: {
            from: 'conversation-messages',
            localField: 'conversationId',
            foreignField: 'conversationId',
            let: { createdByUserId: '$_id' },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [{ $eq: ['$createdBy', '$$createdByUserId'] }]
                  }
                }
              },
              { $sort: { createdAt: -1 } },
              { $limit: 1 }
            ],
            as: 'lastMessage'
          }
        },
        {
          $addFields: {
            lastMessageInfo: {
              $first: '$lastMessage'
            }
          }
        },
        {
          $project: {
            'profile.name': 1,
            'profile.roomForRent': 1,
            'profile.occupation': 1,
            'profile.work': 1,
            'profile.avatarKey': 1,
            'lastMessageInfo._id': 1,
            'lastMessageInfo.content': 1,
            'lastMessageInfo.createdAt': 1,
            'lastMessageInfo.isFile': 1
          }
        },
        {
          $addFields: {
            'profile.avatar': contractHelper.getAvatarKeyPipeline(
              '$profile.avatarKey',
              'assets/default-image/user-primary.png'
            )
          }
        }
      ],
      as: 'usersInfo'
    }
  }
]

const getPipelineForConversationMessage = () => [
  {
    $lookup: {
      from: 'conversation-messages',
      localField: '_id',
      foreignField: 'conversationId',
      pipeline: [
        {
          $sort: {
            createdAt: -1
          }
        },
        {
          $skip: 0
        },
        {
          $limit: 20
        }
      ],
      as: 'conversationMessage'
    }
  }
]

const getConversationsInboxForQuery = async (params) => {
  const { query, options } = params
  const { preparedQuery } = query

  const pipeline = []

  const match = {
    $match: preparedQuery
  }

  pipeline.push(match)
  pipeline.push(...getSortSkipLimit(options))
  pipeline.push(...getListingInfo())
  pipeline.push(
    ...appHelper.getListingFirstImageUrl('$listingInfo.images', 'listingInfo')
  )
  pipeline.push(...getContractPipeline())
  pipeline.push(...getPartnerPipeline())
  pipeline.push(
    ...getPipelineForOtherParticipantInfo(preparedQuery['participants.userId'])
  )
  pipeline.push(...getPipelineForConversationMessage())

  const allConversations = await ConversationCollection.aggregate(pipeline)

  return allConversations
}
export const getConversationsAccessQuery = async (params) => {
  const { accountId, contractId, userId, partnerId, propertyId, tenantId } =
    params
  let accessOrQuery = []

  if (
    partnerId &&
    (await appPermission.canAccessPartnerConversation(userId, partnerId))
  ) {
    const accessQuery = { partnerId }
    if (accountId) accessQuery.accountId = accountId
    if (propertyId) accessQuery.propertyId = propertyId
    if (tenantId) accessQuery.tenantId = tenantId
    if (contractId) accessQuery.contractId = contractId
    //Conversation list access query for account, property and tenant view
    if (accountId || propertyId || tenantId || contractId) return accessQuery
    //All conversation list access query for partner
    accessOrQuery = [
      {
        createdBy: userId,
        published: { $in: [false, null] },
        partnerId
      },
      {
        published: true,
        participants: { $elemMatch: { $or: [{ userId }] } },
        partnerId
      }
    ]
  } else {
    //All conversation list access query for public app
    accessOrQuery = [
      {
        createdBy: userId,
        published: { $in: [false, null] },
        partnerId: { $exists: false }
      },
      {
        createdBy: userId,
        published: { $in: [false, null] },
        partnerId: { $exists: true }
      },
      {
        published: true,
        participants: {
          $elemMatch: {
            $or: [{ userId }, { userId, isVisibleInMainApp: { $exists: true } }]
          }
        }
      }
    ]
  }
  return { $or: accessOrQuery }
}

export const preparedQueryForConversation = async (params) => {
  const { query } = params
  const preparedQuery = {}
  const conversationAccessQuery = await getConversationsAccessQuery(query)
  const {
    isFavorite,
    isLease,
    isListed,
    listingId,
    partnerId,
    searchKeyword,
    userId
  } = query

  if (isFavorite) preparedQuery.favoriteBy = { $in: [userId] }
  if (isListed) {
    preparedQuery['$or'] = [
      { listingId: { $exists: true } },
      { contractId: { $exists: false }, propertyId: { $exists: true } }
    ]
  }
  if (isLease) preparedQuery.contractId = { $exists: true }
  if (listingId) {
    const listing = await getListing(listingId)
    const listingPartnerId =
      listing && listing.partnerId ? listing.partnerId : ''
    if (listingPartnerId) preparedQuery.propertyId = listingId
    else preparedQuery.listingId = listingId
  }

  if (searchKeyword) {
    const keyword = new RegExp(searchKeyword, 'i')
    const userQuery = { 'profile.name': keyword }
    const userIds = await userHelper.getUserIdsByQuery(userQuery)
    if (size(userIds)) {
      preparedQuery.published = true
      preparedQuery.participants = {
        $all: [
          { $elemMatch: { userId } },
          { $elemMatch: { userId: { $in: userIds } } }
        ]
      }
    }
    const conversationIds =
      await conversationMessageHelper.getConversationIdsByQuery({
        content: keyword
      })

    if (size(conversationIds)) preparedQuery._id = { $in: conversationIds }
    else if (!size(conversationIds) && !size(userIds))
      preparedQuery._id = { $in: ['nothing'] }
  }

  let conversationQuery = {}

  if (size(preparedQuery))
    conversationQuery = { $and: [preparedQuery, conversationAccessQuery] }
  else conversationQuery = conversationAccessQuery
  const totalDocumentsQuery = { ...conversationAccessQuery }

  if (partnerId) {
    conversationQuery['partnerId'] = partnerId
    totalDocumentsQuery['partnerId'] = partnerId
  }
  // else {
  //   conversationQuery['partnerId'] = { $exists: false }
  //   totalDocumentsQuery['partnerId'] = { $exists: false }
  // }

  //For hide 'tenant' and 'landlord' conversation
  if (
    !(
      (await appPermission.isPartnerTenant(userId, partnerId)) ||
      (await appPermission.isPartnerLandlord(userId, partnerId))
    )
  ) {
    conversationQuery.hideForPartner = { $exists: false }
    totalDocumentsQuery.hideForPartner = { $exists: false }
  }
  return { conversationQuery, totalDocumentsQuery }
}

export const getConversationsForChat = async (req) => {
  const { body, user = {} } = req
  const { userId = '', partnerId } = user
  appHelper.checkUserId(userId)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  query.userId = userId

  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  }

  const { conversationQuery, totalDocumentsQuery } =
    await preparedQueryForConversation(body)
  query.preparedQuery = conversationQuery

  const conversationsForChatData = await getConversationsInboxForQuery(body)
  const filteredDocuments = await countConversations(conversationQuery)
  const totalDocuments = await countConversations(totalDocumentsQuery)

  return {
    data: conversationsForChatData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getAConversation = async (query = {}, session) => {
  const conversation = await ConversationCollection.findOne(query).session(
    session
  )
  return conversation
}

export const getUsersInfoForConversation = async (
  conversationQuery,
  session
) => {
  const query = [
    { $match: conversationQuery },
    { $unwind: '$userStatus' },
    {
      $lookup: {
        from: 'users',
        localField: 'userStatus.userId',
        foreignField: '_id',
        as: 'user'
      }
    },
    { $unwind: '$user' },
    {
      $addFields: {
        user: '$$REMOVE',
        userStatus: {
          name: '$user.profile.name',
          avatar: appHelper.getUserAvatarKeyPipeline('$user.profile.avatarKey')
        }
      }
    },
    {
      $group: {
        _id: '$_id',
        userInfo: { $push: '$userStatus' }
      }
    },
    {
      $project: {
        userInfo: 1
      }
    }
  ]

  const usersInfo = await ConversationCollection.aggregate(query).session(
    session
  )
  return usersInfo
}

const notificationMessageListProject = {
  $project: {
    accountId: 1,
    contractId: 1,
    createdAt: { $ifNull: ['$lastMessageAt', '$createdAt'] },
    isRead: 1,
    lastMessage: 1,
    lastMessageAt: 1,
    listingId: 1,
    listingInfo: 1,
    partnerId: 1,
    propertyId: 1,
    published: 1,
    tenantId: 1,
    type: 1,
    unreadBy: 1,
    lastMessageInfo: {
      _id: '$userInfo._id',
      avatarUrl: '$userInfo.avatarUrl',
      name: '$userInfo.profile.name',
      lastMessage: '$lastMessage',
      lastMessageAt: '$lastMessageAt'
    },
    usersInfo: 1
  }
}

export const getNotificationMessageList = async (query, body, user) => {
  const { userId } = user
  const { options = {} } = body
  const { limit, skip, sort } = options
  console.log(' userId ', userId)
  console.log(' query ', query)
  console.log(' limit, skip, sort ', limit, skip, sort)
  const conversationList = await ConversationCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: { lastMessageAt: -1 }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $addFields: {
        isRead: {
          $cond: [
            { $in: [userId, { $ifNull: ['$unreadBy', []] }] },
            false,
            true
          ]
        },
        type: 'message'
      }
    },
    {
      $lookup: {
        from: 'conversation-messages',
        localField: '_id',
        foreignField: 'conversationId',
        pipeline: [
          {
            $match: {
              createdBy: {
                $ne: userId
              }
            }
          },
          { $sort: { createdAt: -1 } },
          { $limit: 1 }
        ],
        as: 'messages'
      }
    },
    {
      $addFields: {
        message: { $first: '$messages' }
      }
    },
    {
      $match: {
        message: { $exists: true }
      }
    },
    {
      $lookup: {
        from: 'users',
        as: 'userInfo',
        localField: 'message.createdBy',
        foreignField: '_id'
      }
    },
    {
      $addFields: {
        userInfo: { $first: '$userInfo' }
      }
    },
    {
      $addFields: {
        'userInfo.avatarUrl': contractHelper.getAvatarKeyPipeline(
          '$userInfo.profile.avatarKey',
          'assets/default-image/user-primary.png'
        )
      }
    },
    ...getListingInfo(),
    ...appHelper.getListingFirstImageUrl('$listingInfo.images', 'listingInfo'),
    ...getPipelineForOtherParticipantInfo(userId),
    notificationMessageListProject
  ])
  console.log('conversationList ', conversationList)
  return conversationList || []
}
