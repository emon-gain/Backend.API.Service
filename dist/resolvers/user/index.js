"use strict";

const query = require('./user.query');
const mutation = require('./user.mutation');
module.exports = {
  Query: {
    ...query
  },
  Mutation: {
    ...mutation
  }
};