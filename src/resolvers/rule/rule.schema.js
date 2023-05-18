import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const NotifyToSchema = new mongoose.Schema(
  {
    id: {
      type: String,
      index: true,
      required: true
    },
    type: {
      type: String,
      enum: ['email', 'sms'],
      default: 'email'
    },
    templateUniqueId: {
      type: String,
      index: true,
      required: true
      // Make a relation with template uniqueId;
      // When partner copy any admin template then admin template uniqueId never change for rules between template relationship
    },
    required: {
      type: Boolean
    }
  },
  { _id: false }
)

NotifyToSchema.virtual('templateInfo', {
  ref: 'notification_templates',
  localField: 'templateUniqueId',
  foreignField: 'uniqueId',
  justOne: true
})

const todoNotifyToSchema = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true
    },
    enabled: {
      type: Boolean
    },
    days: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

export const RuleSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      country: {
        type: String,
        index: true,
        required: true
      },
      event: {
        type: String,
        index: true,
        required: true
      },
      notifyTo: {
        type: [NotifyToSchema],
        index: true,
        default: undefined
      },
      status: {
        type: String,
        required: true,
        enum: ['active', 'inactive']
      },
      partnerId: {
        type: String,
        index: true
      },
      todoNotifyTo: {
        type: [todoNotifyToSchema],
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

RuleSchema.index({ createdAt: 1 })
