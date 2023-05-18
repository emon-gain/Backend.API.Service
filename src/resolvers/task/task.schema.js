import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const TaskSchema = new mongoose.Schema(
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
      status: {
        type: String,
        enum: ['open', 'closed'],
        required: true
      },
      dueDate: {
        type: Date,
        index: true
      },
      accountId: {
        type: String,
        index: true
      },
      assignTo: {
        type: [String],
        index: true,
        default: undefined
      },
      starredBy: {
        type: [String],
        default: undefined
      },
      tenantId: {
        type: String,
        index: true
      },
      propertyId: {
        type: String,
        index: true
      },
      closedBy: {
        type: String,
        index: true
      },
      closedOn: {
        type: Date,
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
      propertyRoomItemId: {
        type: String
      },
      furnitureId: {
        type: String
      },
      isMovingInOutProtocolTask: {
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

TaskSchema.index({ createdAt: 1 })
TaskSchema.index({
  partnerId: 1,
  tenantId: 1,
  dueDate: 1
})
