import mongoose from 'mongoose'
import { Id } from '../common'

export const NotificationSchema = new mongoose.Schema(
  [
    Id,
    {
      owner: {
        type: String,
        required: true,
        default: 'SYSTEM'
      },
      type: {
        type: String,
        required: true,
        enum: [
          'roommateFound',
          'userReported',
          'accountDeactivated',
          'roommateRequestAccepted',
          'interestedFormSubmitted'
        ]
      },
      appliedTo: {
        type: String
      },
      isRead: {
        type: Boolean,
        default: false
      },
      event: {
        type: String
      },
      onlyForAdmin: {
        type: Boolean,
        index: true,
        default: false
      },
      tenantId: {
        type: String
      },
      partnerId: {
        type: String
      },
      propertyId: {
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

NotificationSchema.index({ createdAt: 1 })
NotificationSchema.index({
  isRead: 1,
  appliedTo: 1,
  onlyForAdmin: 1
})
