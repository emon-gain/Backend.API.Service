import { userService } from '../services'

export default {
  async activateUserStatus(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const activatedUser = await userService.activateUserStatus(req)
    return activatedUser
  },
  async deactivateUserStatus(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const deactivatedUser = await userService.deactivateUserStatus(req)
    return deactivatedUser
  },
  async removeUser(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removeUser = await userService.removedUser(req)
    return removeUser
  },
  async updateUserPassword(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateUser = await userService.updateUserPassword(req)
    return updateUser
  },
  async updateUserProfile(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedUser = await userService.updateAnUserProfile(req)
    return updatedUser
  },
  async updateMyProfileGeneralInfo(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedProfileGeneralInfo =
      await userService.updateMyProfileGeneralInfo(req)
    return updatedProfileGeneralInfo
  },
  async updateProfilePictureOrCoverImageForLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateProfileImage =
      await userService.updateAProfilePictureOrCoverImageByLambda(req)
    return updateProfileImage
  },
  async updateUserInfoForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updateProfileImage = await userService.userInfoUpdateForLambda(req)
    return updateProfileImage
  },
  async updateProfileForPublicApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedUser = await userService.updateProfileInfoForPublicApp(req)
    return updatedUser
  },
  async updateProfileImageForPublicApp(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedUser = await userService.updateProfileImageForPublicApp(req)
    return updatedUser
  },

  async manageUserStatusForPublicSite(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const userActiveStatus = await userService.updateUserActiveStatus(req)
    return userActiveStatus
  },

  async userTermsAccepted(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const response = await userService.userTermsAccepted(req)
    return response
  },

  async verifyUserInvitation(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await userService.verifyUserInvitation(req)
  },

  async cancelEmailChangingReq(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await userService.cancelEmailChangingReq(req)
  },

  async deleteMyAccount(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    return await userService.deleteMyAccount(req)
  }
}
