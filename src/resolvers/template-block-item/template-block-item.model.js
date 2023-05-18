import mongoose from 'mongoose'
import { BlockItemSchema } from '../models'

BlockItemSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

export const BlockItemCollection = mongoose.model(
  'template_block_items',
  BlockItemSchema
)
