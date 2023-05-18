import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

export const AnnualStatementSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      agentId: {
        type: String,
        index: true,
        required: true
      },
      accountId: {
        type: String,
        index: true,
        required: true
      },
      tenantId: {
        type: String
      },
      propertyId: {
        type: String,
        index: true,
        required: true
      },
      partnerId: {
        type: String,
        index: true,
        required: true
      },
      contractId: {
        type: String,
        index: true,
        required: true
      },
      branchId: {
        type: String,
        required: true
      },
      statementYear: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      rentTotalExclTax: {
        type: Number
      },
      rentTotalTax: {
        type: Number
      },
      rentTotal: {
        type: Number
      },
      landlordTotalExclTax: {
        type: Number
      },
      landlordTotalTax: {
        type: Number
      },
      landlordTotal: {
        type: Number
      },
      totalPayouts: {
        type: Number
      },
      status: {
        type: String,
        enum: ['created', 'failed', 'completed']
      },
      fileId: {
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

AnnualStatementSchema.index({ createdAt: 1 })
