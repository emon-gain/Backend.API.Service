"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _services = require("../services");
var _default = {
  async activateUserStatus(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const activatedUser = await _services.userService.activateUserStatus(req);
    return activatedUser;
  },
  async deactivateUserStatus(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const deactivatedUser = await _services.userService.deactivateUserStatus(req);
    return deactivatedUser;
  },
  async removeUser(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const removeUser = await _services.userService.removedUser(req);
    return removeUser;
  },
  async updateUserPassword(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updateUser = await _services.userService.updateUserPassword(req);
    return updateUser;
  },
  async updateUserProfile(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updatedUser = await _services.userService.updateAnUserProfile(req);
    return updatedUser;
  },
  async updateMyProfileGeneralInfo(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updatedProfileGeneralInfo = await _services.userService.updateMyProfileGeneralInfo(req);
    return updatedProfileGeneralInfo;
  },
  async updateProfilePictureOrCoverImageForLambda(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updateProfileImage = await _services.userService.updateAProfilePictureOrCoverImageByLambda(req);
    return updateProfileImage;
  },
  async updateUserInfoForLambda(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData = {}
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updateProfileImage = await _services.userService.userInfoUpdateForLambda(req);
    return updateProfileImage;
  },
  async updateProfileForPublicApp(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData = {}
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updatedUser = await _services.userService.updateProfileInfoForPublicApp(req);
    return updatedUser;
  },
  async updateProfileImageForPublicApp(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const updatedUser = await _services.userService.updateProfileImageForPublicApp(req);
    return updatedUser;
  },
  async manageUserStatusForPublicSite(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    const userActiveStatus = await _services.userService.updateUserActiveStatus(req);
    return userActiveStatus;
  },
  async userTermsAccepted(parent, args, context) {
    const {
      req
    } = context;
    req.session.startTransaction();
    const response = await _services.userService.userTermsAccepted(req);
    return response;
  },
  async verifyUserInvitation(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    return await _services.userService.verifyUserInvitation(req);
  },
  async cancelEmailChangingReq(parent, args, context) {
    const {
      req
    } = context;
    const {
      inputData
    } = args;
    req.body = JSON.parse(JSON.stringify(inputData));
    req.session.startTransaction();
    return await _services.userService.cancelEmailChangingReq(req);
  },
  async deleteMyAccount(parent, args, context) {
    const {
      req
    } = context;
    req.session.startTransaction();
    return await _services.userService.deleteMyAccount(req);
  }
};
exports.default = _default;