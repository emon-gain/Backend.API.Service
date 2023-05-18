import mongoose from 'mongoose'
import { IntegrationSchema } from '../models'

IntegrationSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})
export const IntegrationCollection = mongoose.model(
  'integration',
  IntegrationSchema
)
