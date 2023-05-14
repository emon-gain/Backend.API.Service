"use strict";

var _server = require("@apollo/server");
var _apolloServerCore = require("apollo-server-core");
var _resolvers = require("./resolvers");
var _database = require("./utils/database");
var _schemas = require("./schemas");
var _schema = require("@graphql-tools/schema");
var _dotenv = _interopRequireDefault(require("dotenv"));
var _express = require("@apollo/server/express4");
var _mongoose = _interopRequireDefault(require("mongoose"));
var _drainHttpServer = require("@apollo/server/plugin/drainHttpServer");
var _express2 = _interopRequireDefault(require("express"));
var _http = _interopRequireDefault(require("http"));
var _cors = _interopRequireDefault(require("cors"));
var _bodyParser = require("body-parser");
var _directives = require("./utils/directives");
var _mock = require("@graphql-tools/mock");
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
const {
  authDirectiveTransformer
} = (0, _directives.authDirective)('auth', _directives.getUser);
_dotenv.default.config();
const typeDefs = _schemas.createMergedSchema;
const resolvers = _resolvers.createMergedResolvers;
let schema = (0, _schema.makeExecutableSchema)({
  typeDefs,
  resolvers
});
const app = (0, _express2.default)();
const httpServer = _http.default.createServer(app);
schema = authDirectiveTransformer(schema);
const server = new _server.ApolloServer({
  schema,
  plugins: [{
    requestDidStart: ({
      contextValue
    }) => ({
      didEncounterErrors: async ({
        errors,
        request
      }) => {
        // Rollback transaction on errors
        if (errors) {
          console.log('Error');
          await contextValue.session.abortTransaction();
          await contextValue.session.endSession();
        }
      },
      willSendResponse: async ({
        response,
        request
      }) => {
        // Commit transaction on successful response
        // console.log();
        if (!response.body.singleResult.errors) {
          await contextValue.session.commitTransaction();
          // console.log(contextValue.session.transaction.state);
          await contextValue.session.endSession();
        }
      }
    })
  }, (0, _drainHttpServer.ApolloServerPluginDrainHttpServer)({
    httpServer
  }), (0, _apolloServerCore.ApolloServerPluginInlineTrace)()]
});
(0, _database.connectDB)().then(async () => {
  console.log('Before start');
  await server.start();
  app.use('/graphql', (0, _cors.default)(), (0, _bodyParser.json)(), (0, _express.expressMiddleware)(server, {
    context: async ({
      req
    }) => {
      const session = await _mongoose.default.startSession();
      // console.log(session.transaction.state);
      session.startTransaction();
      return {
        authToken: req.headers.authtoken,
        session
      };
    }
  }));
  await new Promise(resolve => httpServer.listen({
    port: 4000
  }, resolve));
  console.log(`🚀 Server ready at http://localhost:4000/graphql`);
}).catch(err => console.log(`Failed to connect to database: ${err.message}`));