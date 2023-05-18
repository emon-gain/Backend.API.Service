import mongoose from 'mongoose'
import { PropertyRoomSchema } from '../models'

PropertyRoomSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PropertyRoomSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

PropertyRoomSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

PropertyRoomSchema.virtual('propertyItem', {
  ref: 'property_items',
  localField: 'propertyItemId',
  foreignField: '_id',
  justOne: true
})

PropertyRoomSchema.virtual('propertyRoom', {
  ref: 'property_rooms',
  localField: 'propertyRoomId',
  foreignField: '_id',
  justOne: true
})

export const PropertyRoomCollection = mongoose.model(
  'property_rooms',
  PropertyRoomSchema
)
