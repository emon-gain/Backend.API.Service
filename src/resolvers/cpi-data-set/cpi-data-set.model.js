import mongoose from 'mongoose'
import { CpiDataSetSchema } from '../models'

export const CpiDataSetCollection = mongoose.model(
  'cpi-data-set',
  CpiDataSetSchema,
  'cpi-data-set'
)
