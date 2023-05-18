import mongoose from 'mongoose'
import { AppRoleSchema } from '../models'

AppRoleSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

AppRoleSchema.virtual('managers', {
  ref: 'users',
  localField: 'users',
  foreignField: '_id'
})

export const AppRoleCollection = mongoose.model('app_roles', AppRoleSchema)
