import mongoose from 'mongoose'
import { Id } from '../common'

const MatchesUsersSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      index: true
    },
    groupId: {
      type: String,
      index: true
    }
  },
  { _id: false }
)

export const RoomMateMatchSchema = new mongoose.Schema(
  [
    Id,
    {
      users: {
        type: [MatchesUsersSchema],
        default: undefined
      },
      rank: {
        type: Number,
        index: true
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
