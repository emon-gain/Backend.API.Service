import mongoose from 'mongoose'
import { DepositInsuranceSchema } from '../models'

DepositInsuranceSchema.virtual('appInvoice', {
  ref: 'app_invoices',
  localField: '_id',
  foreignField: 'depositInsuranceId',
  justOne: true
})

export const DepositInsuranceCollection = mongoose.model(
  'deposit_insurance',
  DepositInsuranceSchema,
  'deposit_insurance'
)
