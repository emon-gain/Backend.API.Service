import mongoose from 'mongoose'
import { ApiKeySchema } from '../models'

export const ApiKeyCollection = mongoose.model(
  'api_key',
  ApiKeySchema,
  'api_key'
)
