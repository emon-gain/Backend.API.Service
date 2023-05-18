import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

export const AddonSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true
      },
      isRecurring: {
        type: Boolean
      },
      enableCommission: {
        type: Boolean
      },
      allowPriceEdit: {
        type: Boolean
      },
      name: {
        type: String,
        index: true,
        required: true
      },
      price: {
        type: Number
      },
      type: {
        type: String,
        enum: ['lease', 'assignment']
      },
      debitAccountId: {
        type: String
      },
      creditAccountId: {
        type: String
      },
      enable: {
        type: Boolean
      },
      productName: {
        type: String,
        index: true
      },
      commissionPercentage: {
        type: Number,
        index: true,
        max: 100,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      isNonRent: {
        type: Boolean,
        immutable: true
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: true
  }
)

AddonSchema.index({ createdAt: 1 })
