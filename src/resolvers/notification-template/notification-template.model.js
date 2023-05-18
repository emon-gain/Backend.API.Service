import mongoose from 'mongoose'
import { NotificationTemplateSchema } from '../models'

NotificationTemplateSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})
NotificationTemplateSchema.virtual('createdUser', {
  ref: 'users',
  localField: 'createdBy',
  foreignField: '_id',
  justOne: true
})

export const NotificationTemplateCollection = mongoose.model(
  'notification_templates',
  NotificationTemplateSchema
)
