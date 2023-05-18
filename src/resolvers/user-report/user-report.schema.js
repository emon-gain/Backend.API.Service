import mongoose from 'mongoose'
import { Id } from '../common'

export const UserReportSchema = new mongoose.Schema(
  [
    Id,
    {
      reporter: {
        type: String,
        required: true
      },
      reportedUser: {
        type: String,
        required: true
      },
      reportedByAdmin: {
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

UserReportSchema.index({ createdAt: 1 })
