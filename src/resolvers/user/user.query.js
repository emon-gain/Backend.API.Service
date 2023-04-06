import { User } from '../../models/user';

async function users() {
  try {
    const users = await User.find();
    return users;
  } catch (error) {
    console.error(error);
    throw new Error('Failed to fetch users');
  }
}

async function user(parent, { id }) {
  try {
    const user = await User.findOne({ _id: id });
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
  user,
};
