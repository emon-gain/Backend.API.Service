import mongoose from 'mongoose'
import { PowerOfficeLogSchema } from '../models'

export const PowerOfficeLogCollection = mongoose.model(
  'power-office-log',
  PowerOfficeLogSchema,
  'power-office-log'
)
