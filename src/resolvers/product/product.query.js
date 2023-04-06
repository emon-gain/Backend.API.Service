import { Product } from '../../models/product';

export const product = async (parent, { id }) => {
  return Product.find((product) => product.id === id);
};
