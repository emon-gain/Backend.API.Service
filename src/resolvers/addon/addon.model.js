import mongoose from 'mongoose'
import { AddonSchema } from '../models'

AddonSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

AddonSchema.virtual('debitAccount', {
  ref: 'ledger_accounts',
  localField: 'debitAccountId',
  foreignField: '_id',
  justOne: true
})

AddonSchema.virtual('creditAccount', {
  ref: 'ledger_accounts',
  localField: 'creditAccountId',
  foreignField: '_id',
  justOne: true
})

AddonSchema.virtual('ledgerAccounts', {
  ref: 'ledger_accounts',
  localField: 'partnerId',
  foreignField: 'partnerId'
})

export const AddonCollection = mongoose.model('products_services', AddonSchema)
