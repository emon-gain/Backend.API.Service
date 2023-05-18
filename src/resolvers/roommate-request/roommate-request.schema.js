import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const RoomMateRequestSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      requestedById: {
        type: String,
        required: true
      },
      requestedByType: {
        type: String,
        enum: ['user', 'group'],
        required: true
      },
      requestedToId: {
        type: String,
        required: true
      },
      requestedToType: {
        type: String,
        enum: ['user', 'group'],
        required: true
      },
      status: {
        type: String,
        enum: ['pending', 'accepted', 'cancelled', 'rejected'],
        default: 'pending'
        // Pending = created but not accepted/cancelled yet
        // Cancelled = cancelled by sender
        // Rejected = rejected by receipt
      },
      reviewedBy: {
        // User id, who has accepted or cancelled the request. Useful to track data for groups
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
