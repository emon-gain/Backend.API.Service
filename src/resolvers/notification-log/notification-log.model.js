import mongoose from 'mongoose'
import { NotificationLogSchema } from '../models'

NotificationLogSchema.virtual('user', {
  ref: 'users',
  localField: 'toUserId',
  foreignField: '_id',
  justOne: true
})

NotificationLogSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

export const NotificationLogCollection = mongoose.model(
  'notification_logs',
  NotificationLogSchema
)
