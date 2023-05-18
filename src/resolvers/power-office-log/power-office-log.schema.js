import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const PowerOfficeEventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true,
      default: new Date()
    },
    status: {
      type: String
    },
    note: {
      type: String
    }
  },
  { _id: false }
)

const PowerOfficeError = new mongoose.Schema(
  {
    type: {
      type: String
    },
    errorText: {
      type: String
    },
    reason: {
      type: String
    },
    transactionIds: {
      type: [String],
      default: undefined
    }
  },
  { _id: false }
)

export const PowerOfficeLogSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      status: {
        type: String
      },
      type: {
        type: String
      },
      errorType: {
        type: String
      },
      partnerId: {
        type: String
      },
      tenantId: {
        type: String
      },
      accountId: {
        type: String
      },
      transactionIds: {
        type: [String],
        index: true,
        default: undefined
      },
      transactionDate: {
        type: String
      },
      powerOfficeId: {
        type: String
      },
      hasError: {
        type: Boolean
      },
      // Mongoose does't allow field name called "errors"
      errorsMeta: {
        type: [PowerOfficeError],
        default: undefined
      },
      retries: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      lastUpdatedAt: {
        type: Date
      },
      powerOfficeEvents: {
        type: [PowerOfficeEventsSchema],
        default: undefined
      },
      powerOfficeVoucherId: {
        type: String
      },
      processingAt: {
        type: Date
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PowerOfficeLogSchema.index({ createdAt: 1 })
