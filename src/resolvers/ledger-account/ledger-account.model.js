import mongoose from 'mongoose'
import { LedgerAccountSchema } from '../models'

LedgerAccountSchema.virtual('taxCodeInfo', {
  ref: 'tax_codes',
  localField: 'taxCodeId',
  foreignField: '_id',
  justOne: true
})

LedgerAccountSchema.virtual('taxCode', {
  ref: 'tax_codes',
  localField: 'taxCodeId',
  foreignField: '_id',
  justOne: true
})

LedgerAccountSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

LedgerAccountSchema.methods = {
  getAccountNumber() {
    return this.accountNumber ? this.accountNumber : 0
  },

  getTaxCodeId() {
    return this.taxCodeId ? this.taxCodeId : ''
  }
}

export const LedgerAccountCollection = mongoose.model(
  'ledger_accounts',
  LedgerAccountSchema
)
