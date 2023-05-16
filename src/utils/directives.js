import { mapSchema, getDirective, MapperKind } from '@graphql-tools/utils';

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
              const { resolve = defaultFieldResolver } = fieldConfig;

              fieldConfig.resolve = function (source, args, context, info) {
                const user = getUserFn(context.authToken);
                console.log(user.hasRole(requires))
                if (!user.hasRole(requires)) {
                  throw new Error('not authorized');
                }

                return resolve(source, args, context, info);
              };

              return fieldConfig;
            }
          }
        },
      }),
  };
};

export const getUser = (token) => {
  const roles = ['UNKNOWN', 'USER', 'REVIEWER', 'ADMIN'];
  return {
    hasRole: (role) => {
      const tokenIndex = roles.indexOf(token);
      const roleIndex = roles.indexOf(role);
      return roleIndex >= 0 && tokenIndex >= roleIndex;
    },
  };
};
