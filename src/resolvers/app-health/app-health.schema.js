import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

export const AppHealthSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      collectionTotal: {
        type: Number
      },
      transactionTotal: {
        type: Number
      },
      type: {
        type: String,
        enum: ['transaction', 'sqs', 'accuracy', 'notifications']
      },
      status: {
        type: String,
        enum: ['success', 'error']
      },
      errorDetails: {
        type: [Object],
        default: undefined
      },
      missingAmount: {
        type: Number
      },
      sqsMessageType: {
        type: [String],
        default: undefined
      },
      sqsMessageCount: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      hasSqsError: {
        type: Boolean
      },
      hasPdfError: {
        type: Boolean
      },
      filesCount: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      contractId: {
        type: String
      },
      propertyId: {
        type: String
      },
      invoiceId: {
        type: String
      },
      context: {
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

AppHealthSchema.index({ createdAt: 1 })
