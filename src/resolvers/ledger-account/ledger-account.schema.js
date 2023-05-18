import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

export const LedgerAccountSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      accountNumber: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      accountName: {
        type: String
      },
      taxCodeId: {
        type: String
      },
      enable: {
        type: Boolean
      },
      mapAccounts: {
        type: [Object],
        default: undefined
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: true
  }
)

LedgerAccountSchema.index({ createdAt: 1 })
