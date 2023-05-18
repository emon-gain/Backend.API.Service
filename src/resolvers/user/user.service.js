import { find, isArray, isEmpty, size } from 'lodash'
import crypto from 'crypto'
import { CustomError, appPermission } from '../common'
import {
  accountService,
  conversationService,
  listingService,
  notificationService,
  tenantService,
  userReportService
} from '../services'
import { UserCollection } from '../models'
import { accountHelper, appHelper, tenantHelper, userHelper } from '../helpers'
import settingJson from '../../../settings.json'

export const createAnUser = async (data, session) => {
  const createdUserData = await UserCollection.create([data], { session })
  if (isEmpty(createdUserData)) {
    throw new CustomError(404, `Unable to create user`)
  }
  return createdUserData
}

export const updateAnUser = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedUserData = await UserCollection.findOneAndUpdate(query, data, {
    session,
    new: true,
    runValidators: true
  })
  if (!size(updatedUserData)) {
    throw new CustomError(404, `Unable to update user`)
  }
  return updatedUserData
}

export const updateMultipleUser = async (queryData, inputData) => {
  await UserCollection.updateMany(queryData, inputData)
}

export const updateMyProfileGeneralData = async (query, data, session) => {
  const updatedUser = await updateAnUser(query, data, session)
  const updatedMyProfileData = {
    gender: updatedUser?.profile?.gender,
    occupation: updatedUser?.profile?.occupation,
    phoneNumber: updatedUser?.profile?.phoneNumber,
    birthday: updatedUser?.profile?.birthday,
    norwegianNationalIdentification:
      updatedUser?.profile?.norwegianNationalIdentification,
    active: updatedUser?.profile?.active
  }
  return updatedMyProfileData
}

export const addRelationBetweenUserAndPartner = async (params, session) => {
  const { userId, partnerId, type } = params
  if (userId && partnerId && type) {
    let updatedUserData = {}
    const userInfo = await userHelper.getAnUser({ _id: userId }, session)
    const partnersArray =
      size(userInfo) && size(userInfo.partners) ? userInfo.partners : []
    const partnerInfo = find(
      partnersArray,
      (partner) =>
        partner && partner.partnerId === partnerId && partner.type === type
    )
    if (size(partnerInfo) && partnerInfo.status !== 'active') {
      // Adding partners-status to active for user
      const query = {
        _id: userId,
        partners: { $elemMatch: { partnerId, type } }
      }
      const updatingData = { 'partners.$.status': 'active' }
      updatedUserData = await updateAnUser(query, updatingData, session)
    } else {
      // Adding partners Obj with addToSet condition for user
      const updateData = { partners: { partnerId, type, status: 'active' } }
      updatedUserData = await updateAnUser(
        { _id: userId },
        { $addToSet: updateData },
        session
      )
    }
    if (!size(updatedUserData)) {
      throw new CustomError(404, `Unable to update user`)
    }
  }
}

export const updateUsersListingStatus = async (_id, session) => {
  const updateData = { $set: { 'profile.hasListing': true } }
  const updatedUser = await updateAnUser({ _id }, updateData, session)
  return updatedUser
}

export const updateUsers = async (query, updateData, session) => {
  const updatedUsers = await UserCollection.updateMany(query, updateData, {
    new: true,
    session,
    runValidators: true
  })
  return updatedUsers
}

export const createAnUserWithNameAndEmail = async (params, session) => {
  const { name, email, profile = {} } = params
  const userObject = {
    emails: [{ address: email.toLowerCase(), verified: false }]
  }
  userObject.profile = {
    name,
    currency: 'NOK',
    maxRent: 5000,
    roomForRent: false,
    active: true,
    movingIn: new Date().setDate(1),
    ...profile
  }
  const createdUser = await createAnUser(userObject, session)
  return createdUser
}

export const prepareDataAndQueryForPasswordUpdate = (body) => {
  const { _id, password } = body
  appHelper.validateId({ _id })
  const query = { _id }
  const encryptPassword = crypto.Hash('sha256').update(password).digest('hex')
  const data = {
    $set: { 'services.password.bcrypt': encryptPassword }
  }
  return { query, data }
}

export const activateUserStatus = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['_id'], body)
  const { _id } = body
  appHelper.validateId({ _id })
  if (userId && (await appPermission.isAppAdmin(userId))) {
    const updateData = { $set: { 'profile.active': true } }
    const activateUserData = await updateAnUser({ _id }, updateData, session)
    return activateUserData
  } else throw new CustomError(401, 'user unauthorized')
}

export const deactivateUserStatus = async (req) => {
  const { body = {}, session, user = {} } = req
  const { _id } = body
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['_id'], body)
  appHelper.validateId({ _id })
  if (userId !== _id) {
    const hasActiveLease = await userHelper.checkForActiveLease(_id)
    if (hasActiveLease) {
      throw new CustomError(
        400,
        `Couldn't de-active user, user has active lease`
      )
    }
    const updateData = { $set: { 'profile.active': false } }
    const deactivateUserData = await updateAnUser({ _id }, updateData, session)
    //To unpublish all listings of this user
    await listingService.updateListings(
      { ownerId: _id },
      { $set: { listed: false } },
      session
    )
    return deactivateUserData
  } else throw new CustomError(404, 'You can not deactivate your own account')
}

export const removeAnUser = async (query, session) => {
  if (!size(query))
    throw new CustomError(400, 'Query must be required while removing user')

  const response = await UserCollection.findOneAndDelete(query, { session })
  console.log('=== User Removed ===', response)
  return response
}

export const removeAnUserById = async (query, session) => {
  let result
  if (query) {
    const isDeletable = await userHelper.checkUserDeletableOrNot(query)
    if (!isDeletable)
      throw new CustomError(
        403,
        'User exists in DTMS partner account, delete is not supported!'
      )
    const { _id } = query
    const listingQuery = { ownerId: _id }
    const notificationQuery = { $or: [{ owner: _id }, { appliedTo: _id }] }
    const conversationQuery = { 'participants.userId': _id }
    const userReportQuery = { reportedUser: _id }
    await listingService.removeAListing(listingQuery, session)
    await notificationService.removeANotification(notificationQuery, session)
    await conversationService.removeAConversation(conversationQuery, session)
    await userReportService.removeAnUserReport(userReportQuery, session)
    result = await removeAnUser(query, session)
  }
  return result
}

export const removedUser = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['_id'], body)
  const { _id } = body
  appHelper.validateId({ _id })
  const removedUser = await removeAnUserById({ _id }, session)
  if (!removedUser) {
    throw new CustomError(404, `Could not delete user`)
  }
  return removedUser
}

export const updateAnUserProfile = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  const roles = user.roles || []
  if (!roles.includes('app_admin')) {
    body._id = user.userId
  }
  const userId = body._id
  const updateData = await userHelper.prepareUserProfileUpdatingData(
    body,
    session
  )

  if (!size(updateData)) {
    throw new CustomError(400, 'Nothing to update')
  }

  const updatedUser = await updateAnUser(
    { _id: userId },
    { $set: updateData },
    session
  )
  return updatedUser
}

export const updateMyProfileGeneralInfo = async (req) => {
  const { body, session, user = {} } = req
  const data = await userHelper.prepareDataForMyProfileGeneralInfoUpdate(
    user,
    body
  )
  const updatedProfileGeneralData = await updateMyProfileGeneralData(
    { _id: user.userId },
    data,
    session
  )
  return updatedProfileGeneralData
}
export const updateProfileImageForPublicApp = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  if (!size(body)) throw new CustomError(400, 'Input data can not be empty')
  const { avatarKey = '' } = body
  if (!size(avatarKey)) throw new CustomError(400, 'Invalid image')
  const query = { _id: userId }
  const { folder } = settingJson.S3.Directives['ProfileImage']
  const data = {
    $set: { 'profile.avatarKey': `${folder}/${userId}/${avatarKey}` }
  }
  const updatedUser = await updateAnUser(query, data, session)
  updatedUser.profile.avatarKey = userHelper.getAvatar(updatedUser)
  return updatedUser
}

export const updateProfileInfoForPublicApp = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  if (!size(body)) throw new CustomError(400, 'Input data can not be empty')
  const data = await userHelper.prepareDataForPublicSiteProfileUpdate(
    body,
    user
  )
  if (!size(data)) throw new CustomError(400, 'Invalid input data')
  const query = { _id: userId }
  const updatedUser = await updateAnUser(query, data, session)
  if (size(body.name) && updatedUser.profile.name === body.name) {
    // Update Account name and Tenant name if user profile name update
    const account = await accountHelper.getAnAccount({
      personId: updatedUser._id,
      type: 'person'
    })
    if (size(account)) {
      const accountQuery = { personId: updatedUser._id, type: 'person' }
      const accountData = { $set: { name: updatedUser.profile.name } }
      await accountService.updateAccounts(accountQuery, accountData, session)
    }
    const tenant = await tenantHelper.getATenant({ userId: updatedUser._id })
    if (size(tenant)) {
      const tenantQuery = { userId: updatedUser._id }
      const tenantData = { $set: { name: updatedUser.profile.name } }
      await tenantService.updateTenant(tenantQuery, tenantData, session)
    }
  }
  return updatedUser
}

export const updateUserPassword = async (req) => {
  const { session, body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const requiredFields = ['_id', 'password']
  appHelper.checkRequiredFields(requiredFields, body)
  const { query, data } = prepareDataAndQueryForPasswordUpdate(body)
  const updatedUserData = await updateAnUser(query, data, session)
  return updatedUserData
}

export const updateAProfilePictureOrCoverImageByLambda = async (req) => {
  const { session, body } = req
  const response = await userHelper.prepareUserProfilePictureOrCoverImage(body)
  const { query, toUpdate } = response
  const updatedUser = await updateAnUser(query, toUpdate, session)
  if (size(updatedUser)) {
    return { statusCode: 200, message: 'profile update successful' }
  }
}

export const userInfoUpdateForLambda = async (req) => {
  const { session, body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const response = userHelper.prepareUserInfoUpdateForLambda(body)
  const { query, data } = response
  const updatedUser = await updateAnUser(query, data, session)
  if (size(updatedUser)) {
    return { statusCode: 200, message: 'profile update successful' }
  }
}

export const updateMatchableFieldStatus = async (user = {}, session) => {
  const { birthday, gender, hasMatchableFields, targetCity } = user?.profile
  if ((!hasMatchableFields && birthday, gender, targetCity, user.getEmail())) {
    const updateData = { $set: { 'profile.hasMatchableFields': true } }
    await updateAnUser({ _id: user._id }, updateData, session)
  }
}

export const updateUserActiveStatus = async (req) => {
  const { session, body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { accountStatus = null } = body
  if (!(accountStatus === false || accountStatus === true)) {
    throw new CustomError(400, 'Missing Required fields')
  }
  const updateData = { $set: { 'profile.active': accountStatus } }
  const query = { _id: userId }
  const updatedStatus = await updateAnUser(query, updateData, session)
  //To unpublish all listings of this user
  if (accountStatus === false) {
    const hasActiveLease = await userHelper.checkForActiveLease(userId)
    if (hasActiveLease) {
      throw new CustomError(
        400,
        `Couldn't de-active user, user has active lease`
      )
    }
    await listingService.updateListings(
      { ownerId: userId },
      { $set: { listed: false } },
      session
    )
  }
  return updatedStatus
}

export const userTermsAccepted = async (req) => {
  const { session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  console.log('UserId', userId)

  const existsTermsAcceptation = await userHelper.getAnUser({
    _id: userId,
    'profile.termsAcceptedOn': { $exists: true }
  })
  if (!size(existsTermsAcceptation)) {
    const response = await updateAnUser(
      { _id: userId },
      { $set: { 'profile.termsAcceptedOn': new Date() } },
      session
    )
    return !!response
  } else return true
}

export const updateUser = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedUserData = await UserCollection.findOneAndUpdate(query, data, {
    session,
    new: true,
    runValidators: true
  })
  return updatedUserData
}

export const activateInvitedUser = async (params, session) => {
  const { userId, partnerId, token } = params
  const userInfo = (await userHelper.getAnUser({ _id: userId })) || {}
  let isActivate = false

  if (size(userInfo)) {
    //for partner user invitation
    if (partnerId && isArray(userInfo.partners)) {
      //set activate partner user status
      const partners = userInfo.partners.map((partner) => {
        if (partner.partnerId === partnerId && partner.type === 'user')
          partner.status = 'active'
        return partner
      })
      if (size(partners)) {
        isActivate = await updateAnUser(
          {
            _id: userId
          },
          { $set: { partners } },
          session
        )
      }
    } else if (isArray(userInfo.emails)) {
      //set verified unite user emails
      const uniteUserEmails = userInfo.emails.map((emailInfo) => {
        if (emailInfo.token === token && emailInfo.verified === false)
          emailInfo.verified = true
        return emailInfo
      })
      if (size(uniteUserEmails)) {
        //update unite user email verification status
        isActivate = await updateAnUser(
          {
            _id: userId
          },
          { $set: { emails: uniteUserEmails } },
          session
        )
      }
    }
  }

  return isActivate
}

export const removeTokenAndActivateInvitedUser = async (params, session) => {
  const activateUser = await activateInvitedUser(params, session)
  if (activateUser) return await removeInvitedUserToken(params, session)
}

export const removeInvitedUserToken = async (params, session) => {
  const { userId, partnerId, token } = params
  if (userId && token) {
    const userInfo =
      (await userHelper.getAnUser({ _id: userId }, session)) || {}
    const newPartners = []
    const newEmailsData = []

    if (size(userInfo)) {
      const updateData = {}

      if (partnerId && isArray(userInfo.partners)) {
        userInfo.partners.forEach((partner) => {
          const partnerData = JSON.parse(JSON.stringify(partner))
          if (partnerData.token === token) {
            delete partnerData.token
            delete partnerData.expires
          }
          newPartners.push(partnerData)
        })
        updateData.partners = newPartners
      } else if (isArray(userInfo.emails)) {
        //remove token from unite users emails
        userInfo.emails.forEach((emailInfo) => {
          const emailData = JSON.parse(JSON.stringify(emailInfo))

          if (emailData.token === token) {
            delete emailData.token
            delete emailData.expires
          }

          newEmailsData.push(emailData)
        })

        updateData.emails = newEmailsData
      }
      return await updateAnUser({ _id: userId }, { $set: updateData }, session)
    } else return false
  } else return false
}

export const verifyUserInvitation = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['userId', 'token'], body)
  const { partnerId, token, userId } = body
  const isValidToken = await userHelper.isValidToken(userId, token, partnerId)

  let isNew = false
  let isActivate = false
  let isInvalidToken = false

  if (isValidToken) {
    const newUser = await userHelper.isNewInvitedUser(userId)

    if (newUser) isNew = true
    else {
      const userActivated = await removeTokenAndActivateInvitedUser(
        body,
        session
      )
      if (size(userActivated)) isActivate = true
    }
  } else isInvalidToken = true

  return {
    isNewUser: isNew,
    isActivate,
    isInvalidToken
  }
}

export const cancelEmailChangingReq = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['email'], body)
  const { userId } = user
  const { email = '' } = body

  if (!email) throw new CustomError(400, 'Email is required')
  let updatedUser = null

  const query = { _id: userId, emails: { address: email, verified: false } }
  const userInfo = await userHelper.getAnUser(query)
  if (size(userInfo)) {
    const data = { $pull: { emails: { verified: false } } }
    updatedUser = await updateAnUser(query, data, session)
  } else throw new CustomError(404, 'Email already removed or verified')

  return await userHelper.queryMyProfile(updatedUser)
}

const removeUserAndUserRelatedData = async (userId, session) => {
  await listingService.removeListings({ ownerId: userId }, session)
  await notificationService.removeNotifications(
    { $or: [{ owner: userId }, { appliedTo: userId }] },
    session
  )
  await conversationService.removeConversations(
    { 'participants.userId': userId },
    session
  )
  await userReportService.removeUserReports({ reportedUser: userId }, session)
  await tenantService.removeATenant({ userId }, session)
  await accountService.removeAnAccount({ personId: userId }, session)
  return await removeAnUser({ _id: userId }, session)
}
export const deleteMyAccount = async (req) => {
  const { session, user } = req
  appHelper.checkRequiredFields(['userId'], user)

  const { userId } = user

  const userInfo = await userHelper.getAnUser({ _id: userId })

  if (!userInfo) throw new CustomError(404, 'User does not exist')

  await userHelper.isUserDeletable(userId)

  const response = await removeUserAndUserRelatedData(userId, session)

  console.log('==> response', response)
  return { result: !!response }
}
