import { isUndefined } from 'lodash'
import mongoose from 'mongoose'
import { Id, Message } from '../common'

const RoommateGroupsLocationSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true
    },
    placeId: {
      type: String,
      required: true
    }
  },
  { _id: false }
)

export const RoomMateGroupSchema = new mongoose.Schema(
  [
    Id,
    {
      name: {
        type: String,
        index: true
      },
      roommates: {
        type: [String],
        required: true,
        default: undefined
      },
      location: {
        type: RoommateGroupsLocationSchema,
        index: true
      },
      movingIn: {
        type: Date
      },
      description: {
        type: String
      },
      images: {
        type: [String],
        default: undefined
      },
      gender: {
        type: String,
        index: true,
        enum: ['male', 'female', 'all']
      },
      lookingForRoommate: {
        type: Boolean,
        index: true
      },
      roommatesGender: {
        type: String,
        index: true,
        enum: ['male', 'female', 'all']
      },
      personalities: {
        type: [String],
        index: true,
        default: undefined
      },
      interests: {
        type: [String],
        index: true,
        default: undefined
      },
      partying: {
        type: String,
        index: true
      },
      keepingSpace: {
        type: String,
        index: true
      },
      hasHome: {
        type: Boolean
      },
      listingId: {
        type: String,
        index: true
      },
      age: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      reMatching: {
        type: Boolean,
        default() {
          // Get the fields which are responsible to call the re-match
          let reMatching = false
          const instance = this
          const fields = [
            'roommates',
            'lookingForRoommate',
            'location.name',
            'location.placeId',
            'gender',
            'roommatesGender',
            'hasHome',
            'age'
          ]
          fields.forEach((field) => {
            if (
              !isUndefined(instance[field]) &&
              (field !== 'lookingForRoommate' ||
                (field === 'lookingForRoommate' && instance[field] === true))
            ) {
              reMatching = true
            }
          })
          if (reMatching) {
            return true
          }
        }
      },
      lastLoginDate: {
        type: Date,
        index: true
      },
      favorite: {
        type: [String],
        index: true,
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
