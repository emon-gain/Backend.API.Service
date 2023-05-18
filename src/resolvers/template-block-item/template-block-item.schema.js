import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const BlockItemSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true
      },
      title: {
        type: String
      },
      blockType: {
        type: String
      },
      templateType: {
        type: String
      },
      category: {
        type: [String],
        default: undefined
      },
      content: {
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

BlockItemSchema.index({ createdAt: 1 })
