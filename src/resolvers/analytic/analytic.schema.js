import mongoose from 'mongoose'
import { Id } from '../common'

export const AnalyticSchema = new mongoose.Schema(
  [
    Id,
    {
      createdBy: {
        type: String,
        immutable: true,
        index: true,
        default: 'SYSTEM'
      },
      type: {
        type: String,
        enum: ['event', 'page'],
        default: 'event',
        index: true
      },

      event: {
        type: String
      },

      page: {
        type: String
      },

      data: {
        type: Object
      },

      isSentToAnalytics: {
        type: Boolean,
        index: true,
        default: false
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
