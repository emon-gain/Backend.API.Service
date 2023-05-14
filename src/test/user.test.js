import { expect } from 'chai'
import {sessionInit, start, testConnectDB, testRequest} from "./test.server";
import mongoose from "mongoose";

describe('User test suit', async () => {
    it('should return all user', async () => {

        const response = await testRequest(`
            mutation CreateUser($input: CreateUserInput!) {
  createUser(input: $input) {
    id
  }
}

            `,{
            "input": {
                "email": "emon9@gmail.com",
                    "name": "Emon",
                    "password": "123456"
            }
        }, {
            role: 'USER'
        })
        console.log(response)
    });
});