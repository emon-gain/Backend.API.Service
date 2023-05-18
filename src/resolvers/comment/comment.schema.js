import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const CommentSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      content: {
        type: String
      },
      context: {
        type: String,
        required: true,
        enum: ['account', 'property', 'tenant', 'task', 'landlordDashboard'],
        index: true
      },
      partnerId: {
        type: String,
        index: true
      },
      accountId: {
        type: String,
        index: true
      },
      propertyId: {
        type: String,
        index: true
      },
      tenantId: {
        type: String,
        index: true
      },
      taskId: {
        type: String,
        index: true
      },
      landlordPartnerId: {
        type: String,
        index: true
      },
      tenantPartnerId: {
        type: String,
        index: true
      },
      contractId: {
        type: String
      },
      isMovingInOutProtocolTaskComment: {
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

CommentSchema.index({ createdAt: 1 })
