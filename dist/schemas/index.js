"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createMergedSchema = void 0;
var _fs = _interopRequireDefault(require("fs"));
var _path = _interopRequireDefault(require("path"));
var _merge = require("@graphql-tools/merge");
var _loadFiles = require("@graphql-tools/load-files");
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
const createMergedSchema = (0, _merge.mergeTypeDefs)((0, _loadFiles.loadFilesSync)(_path.default.join(__dirname, './*.gql')));
exports.createMergedSchema = createMergedSchema;