import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const PropertyRoomItemSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      name: {
        type: String
      },
      partnerId: {
        type: String
      },
      roomTypes: {
        type: [String],
        default: undefined
      },
      propertyId: {
        type: String
      },
      movingId: {
        type: String
      },
      isEnable: {
        type: Boolean
      },
      isCustomRoomItem: {
        type: Boolean
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: true
  }
)

PropertyRoomItemSchema.index({ createdAt: 1 })
