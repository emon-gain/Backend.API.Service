"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Product = void 0;
const mongoose = require('mongoose');
const productSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  description: {
    type: String
  },
  price: {
    type: Number,
    required: true
  },
  imageUrl: {
    type: String
  },
  createdAt: {
    type: Date,
    default: Date.now
  },
  updatedAt: {
    type: Date,
    default: Date.now
  }
});
const Product = mongoose.model('Product', productSchema);
exports.Product = Product;