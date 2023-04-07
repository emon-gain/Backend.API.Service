"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.User = void 0;
var _mongoose = _interopRequireDefault(require("mongoose"));
var _bcrypt = _interopRequireDefault(require("bcrypt"));
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
const userSchema = new _mongoose.default.Schema({
  name: {
    type: String,
    required: true,
    trim: true
  },
  email: {
    type: String,
    required: true,
    unique: true,
    lowercase: true,
    trim: true
  },
  productCount: {
    type: Number
  },
  password: {
    type: String,
    required: true,
    minlength: 6
  }
});
userSchema.pre('save', async function () {
  if (this.isModified('password')) {
    this.password = await _bcrypt.default.hash(this.password, 10);
  }
});
userSchema.methods.encryptPassword = async function (password) {
  return await _bcrypt.default.hash(password, 10);
};
userSchema.methods.verifyPassword = async function (password) {
  return await _bcrypt.default.compare(password, this.password);
};
const User = _mongoose.default.model('User', userSchema);
exports.User = User;