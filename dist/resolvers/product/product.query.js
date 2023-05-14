"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.products = exports.product = void 0;
var _product = require("../../models/product");
const product = async (parent, {
  id
}) => {
  return _product.Product.findOne({
    _id: id
  });
};
exports.product = product;
const products = async (parent, args) => {
  return _product.Product.find();
};
exports.products = products;