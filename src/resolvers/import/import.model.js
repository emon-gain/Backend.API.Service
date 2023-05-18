import mongoose from 'mongoose'
import { ImportSchema } from '../models'

ImportSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

export const ImportCollection = mongoose.model('imports', ImportSchema)
