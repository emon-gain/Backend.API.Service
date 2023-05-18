"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getUserFromJWT = exports.getUser = exports.authDirective = void 0;
var _utils = require("@graphql-tools/utils");
var _jsonwebtoken = _interopRequireDefault(require("jsonwebtoken"));
var _lodash = require("lodash");
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
const authDirective = (directiveName, getUserFn) => {
  const typeDirectiveArgumentMaps = {};
  return {
    authDirectiveTransformer: schema => (0, _utils.mapSchema)(schema, {
      [_utils.MapperKind.TYPE]: type => {
        const authDirective = (0, _utils.getDirective)(schema, type, directiveName)?.[0];
        if (authDirective) {
          typeDirectiveArgumentMaps[type.name] = authDirective;
        }
        return undefined;
      },
      [_utils.MapperKind.OBJECT_FIELD]: (fieldConfig, _fieldName, typeName) => {
        const authDirective = (0, _utils.getDirective)(schema, fieldConfig, directiveName)?.[0] ?? typeDirectiveArgumentMaps[typeName];
        if (authDirective) {
          const {
            requires
          } = authDirective;
          if (requires) {
            const {
              resolve
            } = fieldConfig;
            fieldConfig.resolve = function (source, args, context, info) {
              const user = getUserFn(context.authToken, requires);
              if (!user) {
                throw new Error('not authorized');
              }
              console.log(user);
              return resolve(source, args, context, info);
            };
            return fieldConfig;
          }
        }
      }
    })
  };
};
exports.authDirective = authDirective;
const getUser = (token, requiredRoles) => {
  const [bearer, jwtToken] = token.split(' ');
  console.log(jwtToken, requiredRoles);
  const jwtData = _jsonwebtoken.default.verify(jwtToken, process.env.JWT_SECRET);
  if (jwtData) {
    return !!(0, _lodash.intersection)(jwtData.roles, requiredRoles);
  }
};
exports.getUser = getUser;
const getUserFromJWT = token => {
  const [bearer, jwtToken] = token.split(' ');
  const jwtData = _jsonwebtoken.default.verify(jwtToken, process.env.JWT_SECRET);
  return jwtData;
};
exports.getUserFromJWT = getUserFromJWT;