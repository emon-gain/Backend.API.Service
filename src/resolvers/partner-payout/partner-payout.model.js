import mongoose from 'mongoose'
import { PartnerPayoutSchema } from '../models'

PartnerPayoutSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PartnerPayoutSchema.virtual('payoutProcess', {
  ref: 'payouts-process',
  localField: 'payoutProcessId',
  foreignField: '_id',
  justOne: true
})

export const PartnerPayoutCollection = mongoose.model(
  'partner-payouts',
  PartnerPayoutSchema
)
