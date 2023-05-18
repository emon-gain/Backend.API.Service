import mongoose from 'mongoose'
import { DepositAccountSchema } from '../models'

DepositAccountSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

DepositAccountSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

DepositAccountSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

export const DepositAccountCollection = mongoose.model(
  'deposit_accounts',
  DepositAccountSchema
)
