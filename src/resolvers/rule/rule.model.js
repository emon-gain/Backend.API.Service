import mongoose from 'mongoose'
import { RuleSchema } from '../models'

RuleSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

export const RuleCollection = mongoose.model('rules', RuleSchema)
