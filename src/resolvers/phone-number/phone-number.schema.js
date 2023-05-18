import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const PhoneNumberSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      phoneNumber: {
        type: String,
        index: true,
        required: true
      },
      remainingBalance: {
        type: Number
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PhoneNumberSchema.index({ createdAt: 1 })
