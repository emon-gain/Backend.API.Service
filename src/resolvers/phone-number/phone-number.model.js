import mongoose from 'mongoose'
import { PhoneNumberSchema } from '../models'

PhoneNumberSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PhoneNumberSchema.virtual('user', {
  ref: 'users',
  localField: 'createdBy',
  foreignField: '_id',
  justOne: true
})

export const PhoneNumberCollection = mongoose.model(
  'phone_numbers',
  PhoneNumberSchema
)
