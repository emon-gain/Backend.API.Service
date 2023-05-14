import { ApolloServer } from '@apollo/server';
import { ApolloServerPluginInlineTrace } from "apollo-server-core";
import { createMergedResolvers } from './resolvers';
import { connectDB } from './utils/database';
import { createMergedSchema } from './schemas';
import { makeExecutableSchema } from '@graphql-tools/schema';
import dotenv from 'dotenv';
import { expressMiddleware } from '@apollo/server/express4';
import mongoose from 'mongoose';

import { ApolloServerPluginDrainHttpServer } from '@apollo/server/plugin/drainHttpServer';
import express from 'express';
import http from 'http';
import cors from 'cors';
import { json } from 'body-parser';
import { authDirective, getUser } from './utils/directives';
import {addMocksToSchema} from "@graphql-tools/mock";
const { authDirectiveTransformer } = authDirective('auth', getUser);

dotenv.config();
const typeDefs = createMergedSchema;
const resolvers = createMergedResolvers;

let schema = makeExecutableSchema({ typeDefs, resolvers });
const app = express();
const httpServer = http.createServer(app);
schema = authDirectiveTransformer(schema);
const server = new ApolloServer({
  schema,
  plugins: [
    {
      requestDidStart: ({ contextValue }) => ({
        didEncounterErrors: async ({ errors, request }) => {
          // Rollback transaction on errors
          if (errors) {
            console.log('Error');
            await contextValue.session.abortTransaction();
            await contextValue.session.endSession();
          }
        },
        willSendResponse: async ({ response, request }) => {
          // Commit transaction on successful response
          // console.log();
          if (!response.body.singleResult.errors) {
            await contextValue.session.commitTransaction();
            // console.log(contextValue.session.transaction.state);
            await contextValue.session.endSession();
          }
        },
      }),
    },
    ApolloServerPluginDrainHttpServer({ httpServer }),
      ApolloServerPluginInlineTrace()
  ],
});

connectDB()
  .then(async () => {
    console.log('Before start');
    await server.start();
    app.use(
      '/graphql',
      cors(),
      json(),
      expressMiddleware(server, {
        context: async ({ req }) => {
          const session = await mongoose.startSession();
          // console.log(session.transaction.state);
          session.startTransaction();
          return { authToken: req.headers.authtoken, session };
        },
      })
    );
    await new Promise((resolve) => httpServer.listen({ port: 4000 }, resolve));
    console.log(`🚀 Server ready at http://localhost:4000/graphql`);
  })
  .catch((err) => console.log(`Failed to connect to database: ${err.message}`));
