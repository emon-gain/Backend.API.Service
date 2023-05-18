import mongoose from 'mongoose'
import { NotificationSchema } from '../models'

export const NotificationCollection = mongoose.model(
  'notifications',
  NotificationSchema
)
