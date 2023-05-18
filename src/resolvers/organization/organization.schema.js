import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const OrganizationSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      accountId: {
        type: String
      },
      name: {
        type: String,
        require: true
      },
      orgId: {
        type: String,
        optional: true
      },
      image: {
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

OrganizationSchema.index({ createdAt: 1 })
