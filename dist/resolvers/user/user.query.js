"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _helpers = require("../helpers");
var _default = {
  async users(parent, args, context) {
    const {
      req
    } = context;
    const {
      queryData = {},
      optionData = {}
    } = args;
    const {
      limit = 50,
      skip = 0,
      sort = {
        createdAt: 1
      }
    } = optionData;
    req.body = {
      query: queryData,
      options: {
        limit,
        skip,
        sort
      }
    };
    const users = await _helpers.userHelper.queryUsers(req);
    return users;
  },
  async myProfile(parent, args, context) {
    const {
      req
    } = context;
    const {
      user = {}
    } = req;
    _helpers.appHelper.checkUserId(user.userId);
    const profileData = await _helpers.userHelper.queryMyProfile(user.userId);
    return profileData;
  },
  async getASingleUser(parent, args, context) {
    const {
      req
    } = context;
    const {
      queryData = {}
    } = args;
    req.body = {
      query: queryData
    };
    const user = await _helpers.userHelper.getSingleUserData(req);
    return user;
  },
  async usersDropDown(parent, args, context) {
    const {
      req
    } = context;
    const {
      queryData = {},
      optionData = {}
    } = args;
    const {
      limit = 50,
      skip = 0
    } = optionData;
    req.body = {
      query: queryData,
      options: {
        limit,
        skip
      }
    };
    const users = await _helpers.userHelper.queryUsersDropDown(req);
    return users;
  },
  async checkUserNID(parent, args, context) {
    const {
      req
    } = context;
    const {
      queryData = {}
    } = args;
    req.body = JSON.parse(JSON.stringify(queryData));
    const result = await _helpers.userHelper.checkForUserExistingNID(req);
    return result;
  },
  async usersDropdownForPartnerApp(parent, args, context) {
    const {
      req
    } = context;
    const {
      queryData = {},
      optionData = {}
    } = args;
    const {
      limit = 50,
      skip = 0
    } = optionData;
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: {
        limit,
        skip
      }
    };
    return await _helpers.userHelper.queryPartnerAppUsersDropdown(req);
  },
  async validateUserToken(parent, args, context) {
    const {
      req
    } = context;
    const {
      queryData = {}
    } = args;
    req.body = JSON.parse(JSON.stringify(queryData));
    return await _helpers.userHelper.validateUserTokenHelper(req);
  }
};
exports.default = _default;