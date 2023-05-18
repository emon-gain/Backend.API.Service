import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

export const TaxCodeSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      taxCode: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      name: {
        type: String
      },
      taxPercentage: {
        type: Number
      },
      enable: {
        type: Boolean
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

TaxCodeSchema.index({ createdAt: 1 })
