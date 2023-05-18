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
import {authDirective, getUser, getUserFromJWT} from './utils/directives';
import {addMocksToSchema} from "@graphql-tools/mock";
import fs from "fs";
import {cloneDeep} from "lodash";
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
      requestDidStart: async ({ contextValue }) => {
          const session = await mongoose.startSession()
          return {
              async willSendResponse({ context }) {
                  console.log(session.transaction.isActive)
                  if (session.transaction.isActive) {
                      try {
                          await session.commitTransaction();
                          console.log(session.transaction.state)
                      } catch (error) {
                          console.log(error)
                          await session.abortTransaction();
                          throw error;
                      } finally {
                          await session.endSession();
                      }
                      console.log(session.transaction.state)
                  }
              },
              async didEncounterErrors({ context, errors }) {
                  if (errors) {
                      await session.abortTransaction();
                      await session.endSession();
                  }
                  console.log(session.transaction.state)

              },
              async executionDidStart(requestContext) {
                  await session.startTransaction()
                  console.log(await session.id)
                  cloneDeep(requestContext.contextValue.req.session)
                  requestContext.contextValue.req.session = session
              },
          }
      },
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

          const user = getUserFromJWT(req.headers.authorization)

            req.user = user
            // console.log(req.headers.authorization)
          return { authToken: req.headers.authorization, req };
        },
      })
    );
    await new Promise((resolve) => httpServer.listen({ port: 4000 }, resolve));
    console.log(`🚀 Server ready at http://localhost:4000/graphql`);
  })
  .catch((err) => console.log(`Failed to connect to database: ${err.message}`));
