import mongoose from 'mongoose'
import { Message } from '../common'

export const CounterSchema = new mongoose.Schema(
  {
    _id: {
      type: String,
      immutable: true
    },
    next_val: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  {
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
