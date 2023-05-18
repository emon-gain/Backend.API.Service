import mongoose from 'mongoose'
import { RoomMateGroupSchema } from '../models'

export const RoomMateGroupCollection = mongoose.model(
  'roommate_groups',
  RoomMateGroupSchema
)
