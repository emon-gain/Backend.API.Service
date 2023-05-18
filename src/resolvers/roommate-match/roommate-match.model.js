import mongoose from 'mongoose'
import { RoomMateMatchSchema } from '../models'

export const RoomMateMatchCollection = mongoose.model(
  'roommate_matches',
  RoomMateMatchSchema
)
