"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.disconnectDB = exports.connectDB = void 0;
var _mongoose = _interopRequireDefault(require("mongoose"));
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
const connectDB = async () => {
  try {
    await _mongoose.default.connect(process.env.MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });
    console.log('MongoDB connected');
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
};
exports.connectDB = connectDB;
const disconnectDB = async () => {
  try {
    await _mongoose.default.disconnect();
    console.log('MongoDB disconnected');
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
};
exports.disconnectDB = disconnectDB;