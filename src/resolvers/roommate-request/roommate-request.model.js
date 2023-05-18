import mongoose from 'mongoose'
import { RoomMateRequestSchema } from '../models'

export const RoomMateRequestCollection = mongoose.model(
  'roommate_request',
  RoomMateRequestSchema
)
