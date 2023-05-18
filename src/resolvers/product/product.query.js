import { Product } from '../../models/product';

// export const product = async (parent, { id }) => {
//   return Product.findOne({_id: id});
// };
//
// export const products = async (parent, args) => {
//
// };

export default {
  async products() {
    console.log('Here')
    const product = await Product.find();
    console.log(product)
    return product
  },

  async product(parent, { id }) {
    return Product.findOne({_id: id});
  }
}
