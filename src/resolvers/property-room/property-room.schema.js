import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const roomItemSchemas = new mongoose.Schema(
  {
    id: {
      type: String
    },
    status: {
      type: String,
      enum: ['ok', 'issues', 'notApplicable']
    },
    title: {
      type: String
    },
    description: {
      type: String
    },
    dueDate: {
      type: Date
    },
    responsibleForFixing: {
      type: String
    },
    taskId: {
      type: String
    }
  },
  { _id: false }
)

export const PropertyRoomSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      name: {
        type: String
      },
      propertyRoomId: {
        type: String
      },
      propertyId: {
        type: String
      },
      partnerId: {
        type: String
      },
      contractId: {
        type: String
      },
      propertyItemId: {
        type: String
      },
      movingId: {
        type: String
      },
      moveInRoomId: {
        type: String
      },
      type: {
        type: String
      },
      files: {
        type: [String],
        default: undefined
      },
      items: {
        type: [roomItemSchemas],
        default: undefined
      },
      newFiles: {
        type: [String],
        default: undefined
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PropertyRoomSchema.index({ createdAt: 1 })
