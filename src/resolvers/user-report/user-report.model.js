import mongoose from 'mongoose'
import { UserReportSchema } from '../models'

export const UserReportCollection = mongoose.model(
  'userReport',
  UserReportSchema,
  'userReport'
)
