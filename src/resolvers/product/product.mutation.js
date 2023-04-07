import { Product } from '../../models/product';

export const createProduct = async (source, { name, price }, { session }) => {
  try {
    console.log(price);
    const product = await Product.create(
      [
        {
          name,
          price,
        },
      ],
      { session }
    );
    console.log(session.transaction.state);
    console.log(product);
    throw new Error('Test Error');
    return product;
  } catch (error) {
    console.error(error);
    throw new Error('Failed to create product');
  }
};
