import mongoose from 'mongoose'
import { AnalyticSchema } from '../models'

export const AnalyticCollection = mongoose.model('analytics', AnalyticSchema)
