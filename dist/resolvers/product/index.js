"use strict";

const query = require('./product.query');
const mutation = require('./product.mutation');
module.exports = {
  Query: {
    ...query
  },
  Mutation: {
    ...mutation
  }
};