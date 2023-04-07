"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getUser = exports.authDirective = void 0;
var _utils = require("@graphql-tools/utils");
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
              resolve = defaultFieldResolver
            } = fieldConfig;
            fieldConfig.resolve = function (source, args, context, info) {
              const user = getUserFn(context.authToken);
              if (!user.hasRole(requires)) {
                throw new Error('not authorized');
              }
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
const getUser = token => {
  const roles = ['UNKNOWN', 'USER', 'REVIEWER', 'ADMIN'];
  return {
    hasRole: role => {
      const tokenIndex = roles.indexOf(token);
      const roleIndex = roles.indexOf(role);
      return roleIndex >= 0 && tokenIndex >= roleIndex;
    }
  };
};
exports.getUser = getUser;