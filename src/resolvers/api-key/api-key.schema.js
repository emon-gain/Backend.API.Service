import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const ApiKeySchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      apiKey: {
        type: String
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

ApiKeySchema.index({ createdAt: 1 })
