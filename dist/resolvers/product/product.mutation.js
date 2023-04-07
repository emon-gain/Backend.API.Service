"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createProduct = void 0;
var _product = require("../../models/product");
const createProduct = async (source, {
  name,
  price
}, {
  session
}) => {
  try {
    console.log(price);
    const product = await _product.Product.create([{
      name,
      price
    }], {
      session
    });
    console.log(session.transaction.state);
    console.log(product);
    throw new Error('Test Error');
    return product;
  } catch (error) {
    console.error(error);
    throw new Error('Failed to create product');
  }
};
exports.createProduct = createProduct;