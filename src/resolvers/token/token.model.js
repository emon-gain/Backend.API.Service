import mongoose from 'mongoose'
import { TokenSchema } from '../models'

export const TokenCollection = mongoose.model('tokens', TokenSchema)
