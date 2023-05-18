import mongoose from 'mongoose'
import { Id } from '../common'

export const TokenSchema = new mongoose.Schema(
  [
    Id,
    {
      userId: {
        type: String,
        required: true
      },
      tokenType: {
        type: String,
        required: true
      },
      token: {
        type: String,
        required: true
      },
      email: { type: String },
      expired: {
        type: Date,
        required: true
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false
  }
)
