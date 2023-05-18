import { mapSchema, getDirective, MapperKind } from '@graphql-tools/utils';
import jwt from 'jsonwebtoken'
import {intersection} from "lodash";

export const authDirective = (directiveName, getUserFn) => {
  const typeDirectiveArgumentMaps = {};

  return {
    authDirectiveTransformer: (schema) =>
      mapSchema(schema, {
        [MapperKind.TYPE]: (type) => {
          const authDirective = getDirective(schema, type, directiveName)?.[0];
          if (authDirective) {
            typeDirectiveArgumentMaps[type.name] = authDirective;
          }

          return undefined;
        },
        [MapperKind.OBJECT_FIELD]: (fieldConfig, _fieldName, typeName) => {
          const authDirective =
            getDirective(schema, fieldConfig, directiveName)?.[0] ??
            typeDirectiveArgumentMaps[typeName];
          if (authDirective) {
            const { requires } = authDirective;

            if (requires) {
              const { resolve } = fieldConfig;

              fieldConfig.resolve = function (source, args, context, info) {
                const user = getUserFn(context.authToken, requires);
                if (!user) {
                  throw new Error('not authorized');
                }
                console.log(user)

                return resolve(source, args, context, info);
              };

              return fieldConfig;
            }
          }
        },
      }),
  };
};

export const getUser = (token, requiredRoles) => {

  const [bearer, jwtToken] = token.split(' ')
  console.log(jwtToken, requiredRoles)
  const jwtData = jwt.verify(jwtToken, process.env.JWT_SECRET)
  if (jwtData) {
    return !!intersection(jwtData.roles, requiredRoles)
  }
};

export const getUserFromJWT = (token) => {
  const [bearer, jwtToken] = token.split(' ')
  const jwtData = jwt.verify(jwtToken, process.env.JWT_SECRET)
  return jwtData
}
