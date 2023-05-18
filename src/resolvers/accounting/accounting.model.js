import mongoose from 'mongoose'
import { AccountingSchema } from '../models'

AccountingSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

AccountingSchema.virtual('creditAccount', {
  ref: 'ledger_accounts',
  localField: 'creditAccountId',
  foreignField: '_id',
  justOne: true
})

AccountingSchema.virtual('debitAccount', {
  ref: 'ledger_accounts',
  localField: 'debitAccountId',
  foreignField: '_id',
  justOne: true
})

export const AccountingCollection = mongoose.model(
  'accounting',
  AccountingSchema,
  'accounting'
)
