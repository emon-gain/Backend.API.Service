import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const NotificationLogInfosType = new mongoose.Schema(
  {
    logId: {
      type: String
    },
    type: {
      type: String
    },
    sendTo: {
      type: String
    },
    sendToUserId: {
      type: String
    }
  },
  { _id: false }
)

const AppQueueHistory = new mongoose.Schema(
  {
    status: {
      type: String,
      enum: ['hold', 'on_flight', 'sent', 'processing', 'failed'],
      required: true
    },
    noOfRetry: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      },
      required: true
    },
    flightAt: {
      type: Date,
      required: true
    },
    errorDetails: Object
  },
  { _id: false }
)

export const AppQueueSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      event: {
        type: String,
        required: true
        // enum: ['credit-rating']
      },
      action: {
        type: String,
        required: true
        // enum: ['handle_credit_rating']
      },
      status: {
        type: String,
        enum: [
          'new',
          'on_flight',
          'sent',
          'processing',
          'completed',
          'failed',
          'hold'
        ],
        required: true,
        default: 'new'
      },
      errorDetails: Object,
      delaySeconds: {
        type: Number,
        default: 0,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      params: {
        type: Object,
        required: true
      },
      destination: {
        type: String,
        required: true
      },
      priority: {
        type: String,
        enum: ['immediate', 'regular'],
        required: true
      },
      noOfRetry: {
        type: Number,
        default: 0,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      retriedAt: {
        type: Date
      },
      processStartedAt: {
        type: Date,
        index: true
      },
      flightAt: {
        type: Date
      },
      completedAt: {
        type: Date
      },
      isManuallyCompleted: {
        type: Boolean
      },
      // lambda notifier
      notificationLogInfos: {
        type: [NotificationLogInfosType],
        default: undefined
      },
      totalNotificationLogs: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      history: {
        type: [AppQueueHistory],
        default: undefined
      },
      isSequential: {
        type: Boolean
      },
      sequentialCategory: {
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
