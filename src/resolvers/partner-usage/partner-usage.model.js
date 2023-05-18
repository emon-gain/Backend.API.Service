import mongoose from 'mongoose'
import { PartnersUsagesSchema } from '../models'

export const PartnerUsageCollection = mongoose.model(
  'partners_usages',
  PartnersUsagesSchema
)
