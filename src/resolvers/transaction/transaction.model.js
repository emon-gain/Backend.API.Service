import mongoose from 'mongoose'
import { TransactionSchema } from '../models'

TransactionSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

TransactionSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

export const TransactionCollection = mongoose.model(
  'transactions',
  TransactionSchema
)
