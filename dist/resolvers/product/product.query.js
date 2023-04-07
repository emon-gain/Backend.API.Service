"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.product = void 0;
var _product = require("../../models/product");
const product = async (parent, {
  id
}) => {
  return _product.Product.find(product => product.id === id);
};
exports.product = product;