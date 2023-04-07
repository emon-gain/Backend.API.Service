"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createMergedResolvers = void 0;
var _merge = require("@graphql-tools/merge");
var _loadFiles = require("@graphql-tools/load-files");
var _path = _interopRequireDefault(require("path"));
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
console.log((0, _loadFiles.loadFilesSync)(_path.default.join(__dirname, './*/index.js')));
const createMergedResolvers = (0, _merge.mergeResolvers)((0, _loadFiles.loadFilesSync)(_path.default.join(__dirname, './*/index.js')));
exports.createMergedResolvers = createMergedResolvers;