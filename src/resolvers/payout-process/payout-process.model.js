import mongoose from 'mongoose'
import { PayoutProcessSchema } from '../models'

PayoutProcessSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PayoutProcessSchema.virtual('partnerSettings', {
  ref: 'partner_settings',
  localField: 'partnerId',
  foreignField: 'partnerId',
  justOne: true
})

export const PayoutProcessCollection = mongoose.model(
  'payouts-process',
  PayoutProcessSchema,
  'payouts-process'
)
