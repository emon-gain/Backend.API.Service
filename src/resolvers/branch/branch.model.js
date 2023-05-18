import mongoose from 'mongoose'
import { BranchSchema } from '../models'

BranchSchema.virtual('adminUser', {
  ref: 'users',
  localField: 'adminId',
  foreignField: '_id',
  justOne: true
})

BranchSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

BranchSchema.virtual('agentsInfo', {
  ref: 'users',
  localField: 'agents',
  foreignField: '_id'
})

export const BranchCollection = mongoose.model('branches', BranchSchema)
