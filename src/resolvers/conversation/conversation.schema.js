import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const ParticipantsSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      index: true
    },
    groupId: {
      type: String,
      index: true
    },
    isVisibleInMainApp: {
      type: Boolean,
      index: true
    }
  },
  { _id: false }
)

const ParticipantsIdentitySchema = new mongoose.Schema(
  {
    id: {
      type: String,
      index: true,
      required: true
    },
    userId: {
      type: String,
      index: true,
      required: true
    }
  },
  { _id: false }
)

const UserStatusSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      index: true,
      required: true
    },
    lastReadAt: {
      type: Date,
      index: true
    },
    isTyping: {
      type: Boolean,
      index: true
    }
  },
  { _id: false }
)

export const ConversationSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      participants: {
        type: [ParticipantsSchema],
        required: true,
        default: undefined
      },
      userStatus: {
        type: [UserStatusSchema],
        default: undefined
      },
      lastMessageAt: {
        type: Date,
        index: true
      },
      lastMessage: {
        type: String
      },
      listingId: {
        type: String,
        index: true
      },
      // User can archive only his/her side. other party will not be effected for this
      // When a new message receive on this conversation, we'll reset the archive status
      // To reset the archive status, we'll use lastMessageAt field
      archivedBy: {
        type: [String], // List of userIds
        index: true,
        default() {
          if (this.lastMessageAt) {
            return []
          } // Message received, reset this field
        }
      },
      favoriteBy: {
        type: [String], // List of userIds
        index: true,
        default: undefined
      },
      published: {
        type: Boolean
      },
      unreadBy: {
        type: [String], // List of userIds
        index: true,
        default: undefined
      },
      uniqueInteraction: {
        type: Boolean,
        index: true
      },
      identity: {
        type: [ParticipantsIdentitySchema], // List of participants identity
        index: true,
        default: undefined
      },
      uniqueInteractionAt: {
        type: Date,
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
      agentId: {
        type: String,
        index: true
      },
      hideForPartner: {
        type: Boolean
      },
      contractId: {
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

ConversationSchema.index({ createdAt: 1 })
ConversationSchema.index({ 'participants.userId': 1 })
ConversationSchema.index({ 'participants.groupId': 1 })
ConversationSchema.index({ 'participants.isVisibleInMainApp': 1 })
ConversationSchema.index({ 'userStatus.userId': 1 })
ConversationSchema.index({ 'userStatus.lastReadAt': 1 })
ConversationSchema.index({ 'userStatus.isTyping': 1 })
ConversationSchema.index({ 'identity.id': 1 })
ConversationSchema.index({ 'identity.userId': 1 })
ConversationSchema.index({ listingId: 1, createdAt: -1, participants: 1 })
ConversationSchema.index({ createdBy: 1, published: 1, lastMessageAt: -1 })
ConversationSchema.index({
  published: 1,
  'participants.userId': 1,
  lastMessageAt: -1
})
ConversationSchema.index({
  published: 1,
  'participants.groupId': 1,
  lastMessageAt: -1
})
ConversationSchema.index({
  createdBy: 1,
  partnerId: 1,
  published: 1,
  unreadBy: 1,
  lastMessageAt: -1
})
ConversationSchema.index({
  createdBy: 1,
  published: 1,
  unreadBy: 1,
  partnerId: 1
})
ConversationSchema.index({
  createdBy: 1,
  published: 1,
  partnerId: 1,
  archivedBy: 1
})
ConversationSchema.index({
  published: 1,
  'participants.userId': 1,
  lastMessageAt: -1,
  'participants.isVisibleInMainApp': 1
})
ConversationSchema.index({
  createdBy: 1,
  published: 1,
  lastMessageAt: -1,
  partnerId: 1
})
ConversationSchema.index({
  published: 1,
  'participants.userId': 1,
  lastMessageAt: -1,
  hideForPartner: 1,
  'participants.isVisibleInMainApp': 1
})
ConversationSchema.index({
  createdBy: 1,
  published: 1,
  lastMessageAt: -1,
  partnerId: 1,
  hideForPartner: 1
})
