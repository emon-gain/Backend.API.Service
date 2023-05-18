import mongoose from 'mongoose'
import { PropertyRoomItemSchema } from '../models'

PropertyRoomItemSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PropertyRoomItemSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

export const PropertyRoomItemCollection = mongoose.model(
  'property_room_items',
  PropertyRoomItemSchema
)
