import {
  each,
  find,
  isArray,
  isBoolean,
  isString,
  map,
  omit,
  size
} from 'lodash'
import moment from 'moment-timezone'
import validator from 'validator'
import { validateNorwegianIdNumber } from 'norwegian-national-id-validator'
import { AppRoleCollection, UserCollection } from '../models'
import {
  accountHelper,
  appHelper,
  appRoleHelper,
  branchHelper,
  contractHelper,
  tenantHelper,
  userReportHelper
} from '../helpers'
import { CustomError } from '../common'
import settingJson from './../../../settings.json'

export const getUserIdsByQuery = async (query = {}) => {
  const userIds = await UserCollection.distinct('_id', query)
  return userIds
}

export const getUserById = async (id, session) => {
  const user = await UserCollection.findOne({ _id: id }).session(session)
  return user
}

export const getUsers = async (query, session) => {
  const users = await UserCollection.find(query).session(session)
  return users
}

export const getUsersWithSelect = async (query, select = []) => {
  const users = await UserCollection.find(query).select(select)
  return users
}

export const getUserByEmail = async (email, session) => {
  const user = await UserCollection.findOne({
    $or: [{ 'emails.address': email }, { 'services.facebook.email': email }]
  }).session(session)
  return user
}

export const getAnUser = async (query, session) => {
  const user = await UserCollection.findOne(query).session(session)
  return user
}

export const prepareUsersQueryBasedOnFilters = async (query) => {
  const {
    activity,
    age,
    createdDateRange,
    defaultSearch,
    email,
    gender,
    hasAboutMe = '',
    hasCoverPhoto = '',
    hasProfilePicture = '',
    homeAndListing,
    name,
    norwegianNationalIdentification,
    partnerId,
    partnerUserType,
    partnerUserStatus,
    phoneNumber,
    reported,
    status
  } = query

  if (size(partnerId)) {
    appHelper.validateId({ partnerId })
    if (partnerUserType && size(partnerUserStatus))
      query.partners = {
        $elemMatch: {
          partnerId,
          type: partnerUserType,
          status: { $in: partnerUserStatus }
        }
      }
    else if (partnerUserType)
      query.partners = { $elemMatch: { partnerId, type: partnerUserType } }
    else query['partners.partnerId'] = partnerId
  }
  // Set gender filters in query
  if (
    gender &&
    (gender === 'male' || gender === 'female' || gender === 'others')
  )
    query['profile.gender'] = gender
  // Set activity filters in query
  if (status === 'activated') query['profile.active'] = true
  if (status === 'deactivated') query['profile.active'] = false
  // Set age filters in query
  if (size(age)) {
    const { minimum, maximum } = age
    const timeZoneName = moment.tz.guess()
    const timeZoneValue = moment.tz(timeZoneName).format('ZZ') / 100
    query['profile.birthday'] = {
      $gte: moment(new Date())
        .startOf('day')
        .subtract(maximum, 'years')
        .add(timeZoneValue, 'hours')
        .toDate(),
      $lte: moment(new Date())
        .endOf('day')
        .subtract(minimum, 'years')
        .add(timeZoneValue, 'hours')
        .toDate()
    }
  }
  // Set activity filters in query
  if (
    activity &&
    (activity === 'today' ||
      activity === 'lastWeek' ||
      activity === 'lastMonth' ||
      activity === 'lastYear')
  ) {
    const { startDate, endDate } =
      (await appHelper.autoDateGenerator({
        eventName: activity,
        partnerIdOrSettings: partnerId
      })) || {}
    query['status.lastLogin.date'] = {
      $gte: startDate,
      $lte: endDate
    }
  }
  // Set createdAt filters in query
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: new Date(createdDateRange.startDate),
      $lte: new Date(createdDateRange.endDate)
    }
  }
  // Set reported filters in query
  if (reported) {
    const usersInfo = await userReportHelper.getUserReports()
    const userIds = map(usersInfo, 'reportedUser')
    query._id = { $in: userIds }
  }
  // Set Home & Listing filters in query
  if (size(homeAndListing)) {
    const { hasHome, hasListing } = homeAndListing
    // Set hasHome filters in query
    if (hasHome === 'yes') query['profile.roomForRent'] = true
    if (hasHome === 'no') query['profile.roomForRent'] = { $in: [false, null] }
    // Set hasListings filters in query
    if (hasListing === 'yes') query['profile.hasListing'] = true
    if (hasListing === 'no')
      query['profile.hasListing'] = { $in: [false, null] }
  }
  if (norwegianNationalIdentification) {
    query['profile.norwegianNationalIdentification'] = {
      $regex: new RegExp('.*' + norwegianNationalIdentification + '.*', 'i')
    }
  }
  if (name) {
    query['profile.name'] = { $regex: new RegExp('.*' + name + '.*', 'i') }
  }
  if (email) {
    query['emails.address'] = { $regex: new RegExp('.*' + email + '.*', 'i') }
  }
  if (phoneNumber) {
    query['profile.phoneNumber'] = {
      $regex: new RegExp('.*' + phoneNumber.replace('+', '') + '.*', 'i')
    }
  }
  if (defaultSearch) {
    const defaultSearchExp = new RegExp(
      '.*' + defaultSearch.replace('+', '') + '.*',
      'i'
    )
    query.$or = [
      { 'profile.norwegianNationalIdentification': defaultSearch },
      {
        'profile.name': { $regex: defaultSearchExp }
      },
      { 'emails.address': { $regex: defaultSearchExp } },
      { 'profile.phoneNumber': { $regex: defaultSearchExp } }
    ]
  }
  if (hasProfilePicture === 'yes') {
    query['profile.avatarKey'] = { $exists: true, $nin: ['', null] }
  } else if (hasProfilePicture === 'no') {
    query['profile.avatarKey'] = { $exists: false }
  }
  if (hasAboutMe === 'yes') {
    query['profile.aboutMe'] = { $exists: true, $nin: ['', null] }
  } else if (hasAboutMe === 'no') {
    query['profile.aboutMe'] = { $exists: false }
  }
  if (hasCoverPhoto === 'yes') {
    query['profile.cover'] = { $exists: true, $nin: ['', null] }
  } else if (hasCoverPhoto === 'no') {
    query['profile.cover'] = { $exists: false }
  }
  const usersQuery = omit(query, [
    'activity',
    'age',
    'createdDateRange',
    'defaultSearch',
    'email',
    'gender',
    'hasAboutMe',
    'hasCoverPhoto',
    'hasProfilePicture',
    'homeAndListing',
    'name',
    'norwegianNationalIdentification',
    'partnerId',
    'phoneNumber',
    'partnerUserType',
    'partnerUserStatus',
    'reported',
    'status'
  ])

  return usersQuery
}

const getPipelineForPartnersInfoForUserQuery = () => [
  {
    $unwind: {
      path: '$partners',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'partners',
      localField: 'partners.partnerId',
      foreignField: '_id',
      as: 'partnerInfo'
    }
  },
  {
    $unwind: {
      path: '$partnerInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      'partners.name': '$partnerInfo.name',
      'partners.subDomain': '$partnerInfo.subDomain'
    }
  },
  {
    $group: {
      _id: '$_id',
      profile: { $first: '$profile' },
      createdAt: { $first: '$createdAt' },
      partners: {
        $push: '$partners'
      },
      customerId: { $first: '$customerId' },
      email: { $first: '$email' },
      status: { $first: '$status' },
      services: { $first: '$services' }
    }
  }
]

const getUserReportPipelineForUserQuery = () => [
  {
    $lookup: {
      from: 'userReport',
      localField: '_id',
      foreignField: 'reportedUser',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'reporter',
            foreignField: '_id',
            pipeline: [...appHelper.getUserEmailPipeline()],
            as: 'reporterInfo'
          }
        },
        {
          $unwind: {
            path: '$reporterInfo',
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $addFields: {
            reporterEmail: '$reporterInfo.email'
          }
        }
      ],
      as: 'userReport'
    }
  }
]

const getFinalProjectPipelineForUserQuery = () => [
  {
    $project: {
      _id: 1,
      profile: {
        name: 1,
        phoneNumber: 1,
        hometown: 1,
        active: 1,
        avatarKey: appHelper.getUserAvatarKeyPipeline(
          '$profile.avatarKey',
          'assets/default-image/user-primary.png'
        )
      },
      customerId: 1,
      email: 1,
      createdAt: 1,
      partners: {
        $filter: {
          input: '$partners',
          as: 'partner',
          cond: { $ifNull: ['$$partner.partnerId', false] }
        }
      },
      userReport: {
        _id: 1,
        reportedByAdmin: 1,
        reportedUser: 1,
        reporter: 1,
        reporterEmail: 1
      },
      status: 1,
      isFacebookUser: {
        $cond: [{ $ifNull: ['$services.facebook.id', false] }, true, false]
      }
    }
  }
]

export const getUsersForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const users = await UserCollection.aggregate([
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
    ...appHelper.getUserEmailPipeline(),
    ...getPipelineForPartnersInfoForUserQuery(),
    ...getUserReportPipelineForUserQuery(),
    {
      $sort: sort
    },
    ...getFinalProjectPipelineForUserQuery()
  ])
  return users
}

export const getAgentsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const agents = await AppRoleCollection.aggregate([
    {
      $match: {
        type: 'partner_agent',
        partnerId: query.partnerId
      }
    },
    {
      $project: {
        users: '$users',
        name: 1
      }
    },
    {
      $unwind: '$users'
    },
    {
      $lookup: {
        from: 'users',
        localField: 'users',
        foreignField: '_id',
        as: 'agent'
      }
    },
    {
      $project: {
        agent: {
          _id: 1,
          profile: {
            name: 1
          }
        }
      }
    },
    {
      $unwind: '$agent'
    },
    {
      $group: {
        _id: '$agent._id',
        profile: {
          $first: '$agent.profile'
        }
      }
    },
    {
      $limit: limit
    },
    {
      $skip: skip
    },
    {
      $sort: sort
    }
  ])
  return agents
}

const prepareQueryForAgentsDropdown = async (query) => {
  const { accountId, branchId, partnerId, searchString } = query
  let userIds = []
  if (branchId || accountId) {
    if (branchId) {
      const branchInfo = await branchHelper.getABranch({
        _id: branchId,
        partnerId
      })
      userIds = branchInfo?.agents || []
    }
    if (accountId) {
      const accountInfo = (await accountHelper.getAccountById(accountId)) || {}
      const { agentId } = accountInfo
      if (agentId) userIds.push(agentId)
    }
  } else {
    const roleInfo = await appRoleHelper.getAppRole({
      partnerId,
      type: 'partner_agent'
    })
    userIds = roleInfo?.users || []
  }
  const preparedQuery = {
    _id: {
      $in: userIds
    }
  }
  if (searchString)
    preparedQuery['profile.name'] = new RegExp('.*' + searchString + '.*', 'i')
  return preparedQuery
}

export const getAgentsForDropdownQuery = async (req) => {
  const { options, query } = req
  const { limit, skip } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: {
        'profile.name': 1
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $project: {
        _id: 1,
        name: '$profile.name',
        avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
      }
    }
  ]

  const agentsData = (await UserCollection.aggregate(pipeline)) || []
  return agentsData
}

export const queryAgents = async (req) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['partnerId'], query)
  const { partnerId } = query
  appHelper.validateId({ partnerId })
  const agentsData = await getAgentsForQuery(body)
  const filteredDocuments = await countFilteredAgents(body.query)
  const totalDocuments = await countAgents({})
  return {
    data: agentsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryAgentsDropdown = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId = '', partnerId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const { options, query } = body

  query.partnerId = partnerId
  const preparedQuery = await prepareQueryForAgentsDropdown(query)
  const agentsDropdownData = await getAgentsForDropdownQuery({
    query: preparedQuery,
    options
  })

  // To count filter dropdown documents
  const filteredDocuments = await countUsers(preparedQuery)
  const totalDocuments = await countUsers({
    partners: {
      $elemMatch: {
        partnerId
      }
    }
  })

  return {
    data: agentsDropdownData,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const queryUsers = async (req) => {
  const { body, user = {} } = req
  const { query, options } = body
  const { partnerId = '' } = user
  if (size(partnerId)) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  }
  appHelper.validateSortForQuery(options.sort)
  body.query = await prepareUsersQueryBasedOnFilters(query)
  const usersData = await getUsersForQuery(body)
  const filteredDocuments = await countUsers(body.query)
  const totalDocuments = await countUsers({})
  return {
    data: usersData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const countFilteredAgents = async (query, session) => {
  const countedAgents = await AppRoleCollection.find(query)
    .session(session)
    .countDocuments()
  return countedAgents
}

export const countAgents = async () => {
  const countedAgents = await AppRoleCollection.aggregate([
    {
      $match: {
        type: 'partner_agent',
        partnerId: {
          $ne: null
        }
      }
    },
    {
      $project: {
        users: 1
      }
    },
    {
      $unwind: '$users'
    },
    {
      $group: {
        _id: '$users'
      }
    },
    {
      $count: 'totalAgents'
    }
  ])

  return countedAgents[0].totalAgents ? countedAgents[0].totalAgents : 0
}

export const countUsers = async (query, session) => {
  const countedUsers = await UserCollection.countDocuments(query).session(
    session
  )
  return countedUsers
}

export const checkForActiveLease = async (userId) => {
  let query = {}
  const tenantQuery = {}
  const accountQuery = {}
  const tenantIds = await tenantHelper.getTenantIdsByQuery({
    userId
  })
  if (size(tenantIds)) {
    tenantQuery['$or'] = [
      { 'rentalMeta.tenantId': { $in: tenantIds } },
      { 'rentalMeta.tenants.tenantId': { $in: tenantIds } }
    ]
  }
  const accountIds = await accountHelper.getAccountIdsByQuery({
    personId: userId
  })
  if (size(accountIds)) {
    accountQuery.accountId = { $in: accountIds }
  }
  if (size(tenantQuery) && size(accountQuery)) {
    query = {
      $or: [{ ...tenantQuery }, { ...accountQuery }],
      'rentalMeta.status': 'active'
    }
  } else if (size(tenantQuery) || size(accountQuery)) {
    query = {
      ...tenantQuery,
      ...accountQuery,
      'rentalMeta.status': 'active'
    }
  }
  if (!size(query)) {
    return false
  }
  const contract = await contractHelper.getAContract(query)
  if (size(contract)) {
    return true
  } else {
    return false
  }
}

export const queryMyProfile = async (userInfoOrId) => {
  const user = isString(userInfoOrId)
    ? await getAnUser({ _id: userInfoOrId })
    : userInfoOrId

  if (!size(user)) {
    throw new CustomError(404, 'Profile data not found')
  }

  console.log('=== Emails', user?.emails)
  const verifiedEmails = []
  const nonVerifiedEmails = []

  user?.emails.find((data) => {
    if (data.verified === true) verifiedEmails.push(data.address)
    if (data.verified === false) nonVerifiedEmails.push(data.address)
  })

  const myProfileData = {
    name: user?.profile?.name,
    email: size(verifiedEmails) ? verifiedEmails[0] : '',
    gender: user?.profile?.gender,
    birthday: user?.profile?.birthday,
    phoneNumber: user?.profile?.phoneNumber,
    norwegianNationalIdentification:
      user?.profile?.norwegianNationalIdentification,
    occupation: user?.profile?.occupation,
    hometown: user?.profile?.hometown,
    active: user?.profile?.active,
    avatar: getAvatar(user) || '',
    coverImages: getCoverImages(user) || [],
    aboutMe: user?.profile?.aboutMe,
    hasActiveLease: await checkForActiveLease(user._id),
    isFacebookUser: user?.services?.facebook?.id ? true : false,
    termsAcceptedOn: user?.profile?.termsAcceptedOn,
    language: user?.profile?.language,
    pressEnterToSendMessage: !!user?.profile?.settings?.pressEnterToSendMessage,
    disableMessageNotification:
      user?.profile?.disableMessageNotification ?? false,
    verifiedEmails,
    nonVerifiedEmails,
    isUserDeletable: await isUserDeletable(user._id, false)
  }
  return myProfileData
}

export const isUserAppManager = async (userId) => {
  const query = { type: 'app_manager', users: { $in: [userId] } }
  const appManager = await appRoleHelper.getAppRole(query)
  if (size(appManager)) {
    return true
  }
  return false
}

export const isPartnersActiveUser = async (userId, partnerId) => {
  const user = await getUserById(userId)
  if (!size(user)) {
    return false
  }
  const { _id = '', partners = [] } = user
  if (user.isActive() && (await isUserAppManager(_id))) {
    return true
  } else if (size(partners)) {
    return !!find(
      partners,
      (partner) =>
        partner.partnerId === partnerId &&
        partner.type === 'user' &&
        partner.status === 'active'
    )
  }
  return false
}

export const existingEmployeeId = async (params, session) => {
  const { partnerId, partnerEmployeeId } = params
  const query = {
    partners: {
      $exists: true,
      $elemMatch: { partnerId, employeeId: partnerEmployeeId }
    }
  }
  const countedUsers = await countUsers(query, session)
  return countedUsers
}

export const prepareUserProfileUpdatingData = async (body, session) => {
  appHelper.checkRequiredFields(['data'], body)
  const { _id, data = {} } = body
  const {
    name,
    norwegianNationalIdentification,
    phoneNumber,
    hometown,
    email,
    language,
    pressEnterToSendMessage
  } = data
  const updateData = {}

  if (name) updateData['profile.name'] = name
  updateData['profile.phoneNumber'] = phoneNumber
  updateData['profile.hometown'] = hometown
  if (language) updateData['profile.language'] = language
  if (email) {
    const isUserExists = await getAnUser(
      {
        _id: { $ne: _id },
        'emails.address': email
      },
      session
    )
    if (isUserExists) throw new CustomError(401, 'Email already exists')
    updateData['emails.0.address'] = appHelper.getLowerCase(data.email)
  }
  if (isBoolean(pressEnterToSendMessage)) {
    updateData['profile.settings.pressEnterToSendMessage'] =
      pressEnterToSendMessage
  }

  if (data.hasOwnProperty('norwegianNationalIdentification')) {
    if (norwegianNationalIdentification) {
      const userData = await getAnUser({
        _id: { $ne: _id },
        'profile.norwegianNationalIdentification':
          norwegianNationalIdentification
      })

      if (size(userData)) throw new CustomError(409, 'NID already exists')

      const isValidNID = validateNorwegianIdNumber(
        norwegianNationalIdentification
      )
      if (!isValidNID) {
        throw new CustomError(400, 'Invalid NID number')
      }
      updateData['profile.norwegianNationalIdentification'] =
        norwegianNationalIdentification
    } else {
      updateData['profile.norwegianNationalIdentification'] = ''
    }
  }

  return updateData
}

export const prepareUserProfilePictureOrCoverImage = async (body) => {
  if (!size(body)) {
    throw new CustomError(400, 'Input data can not be empty')
  }
  const { _id = '', avatarKey = '', images = '' } = body
  appHelper.validateId({ _id })
  let query = {}
  let toUpdate = {}

  if (size(images)) {
    query = {
      _id,
      'profile.images': { $exists: false }
    }
    toUpdate = {
      $addToSet: { 'profile.images': images },
      $unset: { 'profile.coverKey': '' }
    }
  } else if (size(avatarKey)) {
    query = {
      _id,
      'profile.avatarKey': { $exists: false }
    }
    toUpdate = {
      $addToSet: { 'profile.avatarKey': avatarKey },
      $unset: { 'profile.picture': '' }
    }
  } else {
    throw new CustomError(400, 'Update request not valid')
  }
  return { query, toUpdate }
}

export const prepareUserInfoUpdateForLambda = (body) => {
  if (!size(body)) {
    throw new CustomError(400, 'Input data can not be empty')
  }
  const {
    avatarKey,
    birthday,
    email,
    gender,
    hometown,
    name,
    picture,
    userId
  } = body
  appHelper.validateId({ userId })
  const query = { _id: userId }
  const toUpdate = {}
  if (name) toUpdate['profile.name'] = name
  if (avatarKey) toUpdate['profile.avatarKey'] = avatarKey
  if (picture) toUpdate['profile.picture'] = picture
  if (birthday) toUpdate['profile.birthday'] = birthday
  if (hometown) toUpdate['profile.hometown'] = hometown
  if (gender) toUpdate['profile.gender'] = gender

  let data = { $set: toUpdate }
  if (email) {
    data = {
      ...data,
      $addToSet: {
        emails: {
          address: email,
          verified: true
        }
      }
    }
  }
  return { query, data }
}

export const getAvatar = (user = {}) => {
  const { profile = {} } = user
  const domain = appHelper.getCDNDomain()
  if (size(profile) && profile.avatarKey) {
    return `${domain}/${profile.avatarKey}`
  } else if (size(profile) && profile.picture) {
    if (profile.picture.data) {
      return profile.picture.data.url
    } else {
      return profile.picture
    }
  } else {
    return `${domain}/assets/default-image/user-primary.png`
  }
}

export const getCoverImages = (user = {}) => {
  const { _id, profile = {} } = user
  if (size(profile.images)) {
    const { folder = '' } = settingJson.S3.Directives['CoverImage']
    const path = appHelper.getCDNDomain() + '/' + folder + '/' + _id + '/'
    return profile.images.map((name) => ({ name, url: path + name }))
  }
}

export const validatePhoneNumber = (phoneNumber) => {
  if (
    !validator.isNumeric(phoneNumber.substr(1)) ||
    phoneNumber.substr(0, 1) !== '+'
  ) {
    throw new CustomError(400, 'Phone number is not valid')
  }
}

export const validateGender = (gender) => {
  if (!(gender === 'male' || gender === 'female' || gender === 'others')) {
    throw new CustomError(400, 'Gender is not valid')
  }
}

export const validateNorwayianNID = async (requestNID, existingNID) => {
  if (requestNID !== existingNID) {
    const userData = await getAnUser({
      'profile.norwegianNationalIdentification': requestNID
    })
    if (size(userData)) {
      throw new CustomError(400, 'NID already exists')
    } else {
      const isValidNID = validateNorwegianIdNumber(requestNID)
      if (!isValidNID) {
        throw new CustomError(400, 'Invalid NID number')
      }
    }
  }
}

export const prepareDataForMyProfileGeneralInfoUpdate = async (user, body) => {
  const { userId } = user
  appHelper.validateId({ userId })
  if (!size(body)) throw new CustomError(400, 'Input data can not be empty')
  const {
    gender,
    occupation,
    phoneNumber,
    birthday,
    norwegianNationalIdentification,
    active
  } = body
  const requestedMyProfileGeneralData = {}
  if (phoneNumber) {
    validatePhoneNumber(phoneNumber)
    requestedMyProfileGeneralData.phoneNumber = phoneNumber
  }

  if (gender) {
    validateGender(gender)
    requestedMyProfileGeneralData.gender = gender
  }
  if (occupation) {
    requestedMyProfileGeneralData.occupation = occupation
  }

  const myData = await getAnUser({ _id: userId })
  const profileData = myData?.profile
  const existingNorwegianNationalIdentification =
    profileData.norwegianNationalIdentification

  if (norwegianNationalIdentification) {
    await validateNorwayianNID(
      norwegianNationalIdentification,
      existingNorwegianNationalIdentification
    )
    requestedMyProfileGeneralData.norwegianNationalIdentification =
      norwegianNationalIdentification
  }
  if (birthday) {
    requestedMyProfileGeneralData.birthday = new Date(birthday).toISOString()
  }
  if (active === true || active === false) {
    requestedMyProfileGeneralData.active = active
  }

  const data = {}
  each(requestedMyProfileGeneralData, function (value, key) {
    data['profile.' + key] = value
  })

  const myProfileData = {}
  myProfileData['$set'] = data
  return myProfileData
}

export const prepareDataForPublicSiteProfileUpdate = async (
  body,
  user = {}
) => {
  const {
    birthday,
    disableMessageNotification,
    gender,
    name,
    norwegianNationalIdentification,
    phoneNumber
  } = body
  let data
  if (gender) {
    validateGender(gender)
    data = { $set: { 'profile.gender': gender } }
  } else if (body.hasOwnProperty('phoneNumber')) {
    if (phoneNumber !== '') {
      validatePhoneNumber(phoneNumber)
      data = { $set: { 'profile.phoneNumber': phoneNumber } }
    } else {
      data = { $unset: { 'profile.phoneNumber': 1 } }
    }
  } else if (birthday) {
    const validBirthday = new Date(birthday).toISOString()
    data = { $set: { 'profile.birthday': validBirthday } }
  } else if (size(name)) {
    data = { $set: { 'profile.name': name } }
  } else if (size(norwegianNationalIdentification)) {
    const { userId } = user
    const userData = await getAnUser({ _id: userId })
    const existingNorwegianNationalIdentification =
      userData?.profile?.norwegianNationalIdentification ?? ''
    await validateNorwayianNID(
      norwegianNationalIdentification,
      existingNorwegianNationalIdentification
    )
    data = {
      $set: {
        'profile.norwegianNationalIdentification':
          norwegianNationalIdentification
      }
    }
  } else if (body.hasOwnProperty('disableMessageNotification')) {
    data = {
      $set: {
        'profile.disableMessageNotification': disableMessageNotification
      }
    }
  }
  return data
}

const prepareQueryForGetSingleUserData = (query = {}) => {
  const { email, userId } = query
  const preparedQuery = {}
  if (userId) preparedQuery._id = userId
  if (email)
    preparedQuery.$or = [
      { 'emails.address': email },
      { 'services.facebook.email': email }
    ]
  return preparedQuery
}

const tenantPipelineForUser = (partnerId) => [
  {
    $lookup: {
      from: 'tenants',
      localField: '_id',
      foreignField: 'userId',
      pipeline: [
        {
          $match: {
            partnerId
          }
        }
      ],
      as: 'tenantInfo'
    }
  },
  appHelper.getUnwindPipeline('tenantInfo')
]

export const getSingleUserData = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query = {} } = body
  const { userId } = query
  const { partnerId, roles = [] } = user
  let isRequestFromPartnerApp = false
  if (roles.includes('lambda_manager')) appHelper.validateId({ userId })
  else {
    appHelper.checkRequiredFields(['partnerId'], user)
    appHelper.validateId({ partnerId })
    isRequestFromPartnerApp = true
  }
  const preparedQuery = prepareQueryForGetSingleUserData(query)
  const pipeline = [
    {
      $match: preparedQuery
    }
  ]
  if (isRequestFromPartnerApp) {
    pipeline.push(...tenantPipelineForUser(partnerId))
  }
  pipeline.push({
    $project: {
      _id: 1,
      emails: 1,
      services: 1,
      customerId: 1,
      status: 1,
      profile: 1,
      identity: 1,
      roles: 1,
      favorite: 1,
      registeredAt: 1,
      partners: 1,
      partnersInfo: 1,
      interestFormMeta: 1,
      createdAt: 1,
      imgUrl: 1,
      tenantId: '$tenantInfo._id',
      userReport: 1
    }
  })
  const [usersData = {}] = (await UserCollection.aggregate(pipeline)) || []
  return {
    data: usersData
  }
}

export const getDistinctUserIds = async (query, session) => {
  const userIds =
    (await UserCollection.distinct('_id', query).session(session)) || []
  return userIds
}

const prepareQueryForUsersDropDown = (query) => {
  const { partnerId, searchString } = query
  const preparedQuery = {}
  if (searchString) {
    preparedQuery['profile.name'] = new RegExp('.*' + searchString + '.*', 'i')
  }
  if (partnerId) preparedQuery['partners.partnerId'] = partnerId
  return preparedQuery
}

const getUsersDropDown = async (body) => {
  const { query, options = {} } = body
  const { limit, skip } = options
  const users = await UserCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: {
        'profile.name': 1
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $project: {
        _id: 1,
        name: '$profile.name',
        avatar: appHelper.getUserAvatarKeyPipeline(
          '$profile.avatarKey',
          'assets/default-image/user-primary.png'
        )
      }
    }
  ])
  return users
}

export const queryUsersDropDown = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { query = {} } = body
  const { partnerId } = user
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  }
  body.query = prepareQueryForUsersDropDown(query)
  const users = await getUsersDropDown(body)
  const filteredDocuments = await countUsers(body.query)
  const totalDocuments = await countUsers()
  return {
    data: users,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const checkUserDeletableOrNot = async (query) => {
  const { _id } = query
  const user = await getAnUser({
    _id,
    'partners.partnerId': { $exists: true }
  })
  if (size(user)) return false
  const tenant = await tenantHelper.getATenant({ userId: _id })
  if (size(tenant)) return false
  const account = await accountHelper.getAnAccount({ personId: _id })
  if (size(account)) return false

  return true
}

export const checkForUserExistingNID = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['userNID'], body)
  const { userNID } = body
  const isUserExists = !!(await getAnUser({
    'profile.norwegianNationalIdentification': userNID
  }))
  return {
    result: isUserExists
  }
}

const prepareQueryForPartnerAppUsersDropdown = (params = {}) => {
  const { assigneeIds = [], partnerId, searchString, status, type } = params
  const query = {}
  const elemMatchQuery = {
    partnerId
  }
  if (size(assigneeIds)) query._id = { $nin: assigneeIds }
  if (searchString)
    query['profile.name'] = new RegExp('.*' + searchString + '.*', 'i')
  if (type) elemMatchQuery.type = type
  if (status) elemMatchQuery.status = status
  query.partners = {
    $elemMatch: elemMatchQuery
  }
  return query
}

const getUsersPartnerAppDropdown = async (body) => {
  const { query, options = {} } = body
  const { limit, skip } = options
  const users = await UserCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: {
        'profile.name': 1
      }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $project: {
        _id: 1,
        name: '$profile.name',
        avatar: appHelper.getUserAvatarKeyPipeline(
          '$profile.avatarKey',
          'assets/default-image/user-primary.png'
        )
      }
    }
  ])
  return users
}

export const queryPartnerAppUsersDropdown = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  const { query } = body
  query.partnerId = partnerId
  body.query = prepareQueryForPartnerAppUsersDropdown(query)
  const users = await getUsersPartnerAppDropdown(body)
  const filteredDocuments = await countUsers(body.query)
  const totalDocuments = await countUsers({
    partners: {
      $elemMatch: {
        partnerId
      }
    }
  })
  return {
    data: users,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const getAnUserWithAvatar = async (query = {}) => {
  const user = await getAnUser(query)
  if (!user) return {}
  const userInfo = {
    _id: user._id,
    avatarKey: getAvatar(user),
    name: user.profile?.name
  }
  return userInfo
}

export const validateUserTokenHelper = async (req) => {
  const { body = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId', 'token'], body)
  const { userId, partnerId, token } = body
  const isValidToken = !!(await getAnUser({
    _id: userId,
    partners: {
      $elemMatch: {
        partnerId,
        token
      }
    }
  }))

  return {
    isValidToken
  }
}

export const isNewInvitedUser = async (userId) => {
  let isNewUser = false
  if (userId) {
    const userInfo = (await getAnUser({ _id: userId })) || {}
    if (
      userInfo &&
      (!size(userInfo.services) ||
        (size(userInfo.services) &&
          !userInfo.services.facebook &&
          !userInfo.services.password))
    )
      isNewUser = true
  }
  return isNewUser
}

const getUserWithTokenEmail = async (userId, token, partnerId) => {
  let user
  if (partnerId) {
    user = await getAnUser({
      _id: userId,
      partners: { $elemMatch: { partnerId, token } }
    })
    if (isArray(user?.partners)) {
      user.partners = user.partners.filter((partner) => {
        if (partner.partnerId === partnerId && partner.token === token) {
          return partner
        }
      })
    }
  } else {
    user = await getAnUser({
      _id: userId,
      'emails.token': token
    })
    if (isArray(user?.emails)) {
      user.emails = user.emails.filter((email) => email.token === token)
    }
  }
  return user
}

export const isValidToken = async (userId, token, partnerId) => {
  let isValid = false
  const user = await getUserWithTokenEmail(userId, token, partnerId)
  if (size(user)) {
    const now = new Date()
    let expires = ''
    let partnerToken = ''
    let uniteUserToken = ''
    let partner = {}
    let email = {}

    if (partnerId) {
      partner = user.partners
      partnerToken = partner[0].token
      expires = partner[0].expires

      if (partner && partner[0].status !== 'invited') return false

      if (!token) token = partnerToken
    } else {
      email = user.emails
      uniteUserToken = email[0].token
      expires = email[0].expires

      if (!token) token = uniteUserToken
    }

    if (now > expires) isValid = false
    else isValid = true
  }

  return isValid
}

export const isUserDeletable = async (userId, allowError = true) => {
  const user = await UserCollection.aggregate([
    { $match: { _id: userId, partners: { $exists: true } } },
    {
      $lookup: {
        from: 'tenants',
        localField: '_id',
        foreignField: 'userId',
        pipeline: [{ $project: { _id: 1 } }],
        as: 'tenant'
      }
    },
    { $unwind: { path: '$tenant', preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        from: 'accounts',
        localField: '_id',
        foreignField: 'personId',
        pipeline: [{ $project: { _id: 1 } }],
        as: 'account'
      }
    },
    { $unwind: { path: '$account', preserveNullAndEmptyArrays: true } },
    {
      $addFields: {
        tenantId: {
          $cond: [{ $ifNull: ['$tenant', false] }, '$tenant._id', false]
        },
        accountId: {
          $cond: [{ $ifNull: ['$account', false] }, '$account._id', false]
        }
      }
    },
    { $project: { tenantId: 1, accountId: 1 } }
  ])

  console.log('=== User', user)
  if (!size(user)) return true

  const [{ accountId = '', tenantId = '' }] = user || []
  console.log('=== TenantId', tenantId, '=== AccountId', accountId)
  let isUserDeletable = true

  if (accountId || tenantId) {
    const leaseQuery = {
      $or: [{ 'rentalMeta.tenants.tenantId': tenantId }, { accountId }]
    }
    console.log('===  leaseQuery', leaseQuery)
    const lease = await contractHelper.getAContract(leaseQuery)
    console.log('=== lease', lease)
    if (size(lease)) isUserDeletable = false
    console.log('=== isUserDeletable', isUserDeletable)
  }
  if (!isUserDeletable && allowError)
    throw new CustomError(403, 'User have active or upcoming lease')
  else return isUserDeletable
}
