import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const IncomingPaymentsSchema = new mongoose.Schema(
  {
    id: {
      type: String
    },
    paymentAmount: {
      type: Number
    },
    paymentDate: {
      type: Date,
      index: true
    }
  },
  { _id: false, toJSON: { virtuals: true } }
)

const CreationResult = new mongoose.Schema(
  {
    createdAt: {
      type: Date
    },
    resultCode: {
      type: String
    },
    reasons: {
      type: [String],
      default: undefined
    },
    amount: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    entityId: {
      type: String
    },
    insuranceNo: {
      type: String
    },
    policyUrl: {
      type: String
    }
  },
  { _id: false, toJSON: { virtuals: true } }
)

export const DepositInsuranceSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        required: true
      },
      contractId: {
        type: String,
        required: true
      },
      accountId: {
        type: String
      },
      agentId: {
        type: String
      },
      propertyId: {
        type: String
      },
      branchId: {
        type: String
      },
      tenantId: {
        type: String,
        required: true
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
        type: Number
      },
      payments: {
        type: [IncomingPaymentsSchema],
        default: undefined
      },
      depositAmount: {
        type: Number,
        required: true
      },
      depositInsuranceAmount: {
        type: Number,
        required: true
      },
      kidNumber: {
        type: String
      },
      status: {
        type: String,
        enum: [
          'created',
          'due',
          'partially_paid',
          'paid',
          'overpaid',
          'sent',
          'registered',
          'failed'
        ],
        required: true
      },
      isActive: {
        type: Boolean
      },
      creationResult: {
        type: CreationResult
      },
      paymentReminderSentAt: {
        type: Date
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false
  }
)

DepositInsuranceSchema.index({ createdAt: 1 })
DepositInsuranceSchema.index({ 'payments.paymentDate': 1 })
