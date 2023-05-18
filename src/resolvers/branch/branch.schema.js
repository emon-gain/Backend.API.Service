import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

export const BranchSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      name: {
        type: String,
        index: true,
        required: true
      },
      partnerId: {
        type: String,
        index: true,
        required: true
      },
      adminId: {
        type: String,
        index: true,
        required: true
      },
      agents: {
        type: [String],
        index: true,
        default: undefined
      },
      branchSerialId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

BranchSchema.index({ createdAt: 1 })
