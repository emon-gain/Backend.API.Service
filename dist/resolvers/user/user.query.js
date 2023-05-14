"use strict";

var _user = require("../../models/user");
async function users(source, args, context) {
  // console.log(context);
  try {
    const users = await _user.User.find().session(context.session);
    return users;
  } catch (error) {
    console.error(error);
    throw new Error('Failed to fetch users');
  }
}
async function user(parent, {
  id
}, {
  session
}) {
  try {
    const user = await _user.User.findOne({
      _id: id
    }).session(session);
    console.log(session.transaction.state);
    if (!user) {
      throw new Error('User not found');
    }
    return user;
  } catch (error) {
    console.error(error);
    throw new Error(`Failed to fetch user with id: ${id}`);
  }
}
module.exports = {
  users,
  user
};