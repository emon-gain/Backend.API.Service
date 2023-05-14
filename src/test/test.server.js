import { ApolloServer } from '@apollo/server';
import {createMergedSchema} from '../schemas/index'
import {createMergedResolvers} from '../resolvers/index'
import {makeExecutableSchema} from "@graphql-tools/schema";
import mongoose from "mongoose";
import {addMocksToSchema} from "@graphql-tools/mock";
const typeDefs = createMergedSchema;
const resolvers = createMergedResolvers;
let schema = makeExecutableSchema({ typeDefs, resolvers });



export const start = async () => {
    await testConnectDB()

    const server = new ApolloServer({
        schema,
        context: async ({ req }) => {


            return {
                user: req.headers.user,
                session: req.headers.session
            };
        },
        plugins: [
            {
                requestDidStart: ({ contextValue }) => ({
                    didEncounterErrors: async ({ errors, request }) => {
                        // Rollback transaction on errors
                        if (errors) {
                            console.log('Error', errors);
                            await contextValue.session.abortTransaction();
                            await contextValue.session.endSession();
                        }
                    },
                    willSendResponse: async ({ response, request }) => {
                        // Commit transaction on successful response
                        // console.log();
                        if (!response.body.singleResult.errors) {
                            console.log(contextValue.session.transaction.state);
                            await contextValue.session.commitTransaction();
                            console.log(contextValue.session.transaction.state);
                            await contextValue.session.endSession();
                        }
                    },
                }),
            },
        ],
    })
    return server
}

export const testConnectDB = async () => {
    try {
        await mongoose.connect(process.env.TEST_MONGODB_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        console.log('MongoDB connected');
    } catch (error) {
        console.error(error.message);
        process.exit(1);
    }
}

export const sessionInit = async () => {
    const session = await mongoose.startSession();
    console.log(session.transaction.state);
    session.startTransaction();
    return session
}

export const testRequest = async (query='', variables= {}, user = {}) => {
    const response = await (await start()).executeOperation({
        query,
        variables
    }, {
        contextValue:  {
            user,
            session: await sessionInit()
        },
    })
    return response
}