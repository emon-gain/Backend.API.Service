const query = require('./product.query');
// const mutation = require('./user.mutation');

module.exports = {
  Query: {
    ...query,
  },
  // Mutation: {
  //   ...mutation,
  // },
};
