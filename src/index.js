import { ApolloServer } from '@apollo/server';
import { createMergedResolvers } from './resolvers';
import { connectDB } from './utils/database';
import { createMergedSchema } from './schemas';
import { makeExecutableSchema } from '@graphql-tools/schema';
import dotenv from 'dotenv';
import { expressMiddleware } from '@apollo/server/express4';

import { ApolloServerPluginDrainHttpServer } from '@apollo/server/plugin/drainHttpServer';
import express from 'express';
import http from 'http';
import cors from 'cors';
import { json } from 'body-parser';

dotenv.config();
const typeDefs = createMergedSchema;
const resolvers = createMergedResolvers;

const schema = makeExecutableSchema({ typeDefs, resolvers });
const app = express();
const httpServer = http.createServer(app);
const server = new ApolloServer({
  schema,
  plugins: [ApolloServerPluginDrainHttpServer({ httpServer })],
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
        context: async ({ req }) => ({ token: req.headers.token }),
      })
    );
    await new Promise((resolve) => httpServer.listen({ port: 4000 }, resolve));
    console.log(`🚀 Server ready at http://localhost:4000/graphql`);
  })
  .catch((err) =>
    logger.error(`Failed to connect to database: ${err.message}`)
  );
