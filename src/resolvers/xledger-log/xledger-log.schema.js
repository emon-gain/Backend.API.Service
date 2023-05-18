import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const XledgerEventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true,
      default: new Date()
    },
    note: {
      type: String
    },
    status: {
      type: String
    }
  },
  { _id: false }
)

const XledgerError = new mongoose.Schema(
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

export const XledgerLogSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      accountId: {
        type: String
      },
      errorType: {
        type: String
      },
      hasError: {
        type: Boolean
      },
      partnerId: {
        type: String
      },
      processingAt: {
        type: Date
      },
      status: {
        type: String,
        enum: ['processing', 'success', 'error']
      },
      type: {
        type: String,
        enum: [
          'account',
          'tenant',
          'transaction',
          'update_account',
          'update_tenant'
        ]
      },
      tenantId: {
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
      // Mongoose does't allow field name called "errors"
      errorsMeta: {
        type: [XledgerError],
        default: undefined
      },
      xledgerEvents: {
        type: [XledgerEventsSchema],
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

XledgerLogSchema.index({ createdAt: 1 })
