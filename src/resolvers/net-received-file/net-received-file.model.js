import mongoose from 'mongoose'
import { NetReceivedFileSchema } from '../models'

export const NetReceivedFileCollection = mongoose.model(
  'nets-received-files',
  NetReceivedFileSchema
)
