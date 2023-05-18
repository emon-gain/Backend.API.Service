import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const incomingPaymentsSchema = new mongoose.Schema(
  {
    id: {
      type: String
    },
    paymentReference: {
      type: String
    },
    paymentAmount: {
      type: Number,
      required: true
    },
    currentBalance: {
      type: Number,
      required: true
    },
    paymentDate: {
      type: Date,
      index: true
    }
  },
  { _id: false }
)

export const DepositAccountSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String
      },
      branchId: {
        type: String
      },
      contractId: {
        type: String
      },
      propertyId: {
        type: String
      },
      tenantId: {
        type: String
      },
      referenceNumber: {
        type: String
      },
      bankNotificationId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      bankNotificationType: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      bankAccountNumber: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      totalPaymentAmount: {
        type: Number,
        default: 0,
        required: true
      },
      payments: {
        type: [incomingPaymentsSchema],
        default: undefined
      },
      depositAmount: {
        type: Number,
        required: true
      }
    }
  ],
  {
    timestamps: true,
    toJSON: { virtuals: true },
    versionKey: false
  }
)

DepositAccountSchema.index({ createdAt: 1 })
DepositAccountSchema.index({ 'payments.paymentDate': 1 })
