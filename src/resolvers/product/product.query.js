import { Product } from '../../models/product';

export const product = async (parent, { id }) => {
  return Product.findOne({_id: id});
};

export const products = async (parent, args) => {
  return Product.find();
};