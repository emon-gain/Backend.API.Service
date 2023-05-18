import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'
import { FeedbackHistorySchema } from '../models'

const PaymentMeta = new mongoose.Schema(
  {
    kidNumber: {
      type: String
    },
    dbTrName: {
      type: String
    },
    dbTrAddress: {
      type: String
    },
    dbTrAccountNumber: {
      type: String
    },
    cdTrName: {
      type: String
    },
    cdTrAddress: {
      type: String
    },
    cdTrAccountNumber: {
      type: String
    },
    settlementDate: {
      type: Date
    },
    bankRef: {
      type: String
    }
  },
  { _id: false }
)

const InvoicesContent = new mongoose.Schema(
  {
    invoiceId: {
      type: String,
      required: true
    },
    amount: {
      type: Number,
      required: true
    },
    remaining: {
      type: Number
    }
  },
  { _id: false }
)

const RefundedPaymentMeta = new mongoose.Schema(
  {
    refundPaymentId: {
      type: String
    },
    amount: {
      type: Number
    },
    refundedAt: {
      type: Date
    }
  },
  { _id: false }
)

export const InvoicePaymentSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      contractId: {
        type: String,
        index: true
      },
      agentId: {
        type: String,
        index: true
      },
      branchId: {
        type: String,
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
      invoiceId: {
        type: String,
        index: true
      },
      amount: {
        type: Number
      },
      paymentDate: {
        type: Date,
        index: true
      },
      paymentType: {
        type: String,
        enum: ['manual', 'bank']
      },
      status: {
        type: String,
        enum: ['new', 'registered', 'unspecified']
      },
      note: {
        type: String
      },
      meta: {
        type: PaymentMeta
      },
      receivedFileName: {
        type: String
      },
      netsReceivedFileId: {
        type: String
      },
      invoices: {
        type: [InvoicesContent],
        default: undefined
      },
      nodeIndex: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      type: {
        type: String,
        enum: ['payment', 'refund']
      },
      refundedAmount: {
        type: Number
      },
      refundToAccountNumber: {
        type: String
      },
      refundToAccountName: {
        type: String
      },
      refunded: {
        type: Boolean
      },
      refundedMeta: {
        type: [RefundedPaymentMeta],
        default: undefined
      },
      partiallyRefunded: {
        type: Boolean
      },
      sentToNETS: {
        type: Boolean
      },
      sentToNETSOn: {
        type: Date
      },
      numberOfFails: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      refundStatus: {
        type: String,
        enum: [
          'created',
          'pending_for_approval',
          'waiting_for_signature',
          'approved',
          'estimated',
          'in_progress',
          'completed',
          'failed',
          'canceled'
        ]
      },
      paymentId: {
        type: String
      },
      refundPaymentIds: {
        type: [String],
        default: undefined
      },
      bookingDate: {
        type: Date
      },
      refundPaymentStatus: {
        type: String,
        enum: ['pending', 'paid']
      },
      feedbackStatusLog: {
        type: [FeedbackHistorySchema], // Multiple feedback history
        default: undefined
      },
      tenants: {
        type: [TenantsIdSchemas],
        default: undefined
      },
      paymentReason: {
        type: String
      },
      additionalTaxInfo: {
        type: String
      },
      isManualRefund: {
        type: Boolean
      },
      manualRefundDate: {
        type: Date
      },
      manualRefundReason: {
        type: String
      },
      isFinalSettlement: {
        type: Boolean
      },
      refundBankRef: {
        type: String
      },
      isDepositInsurancePayment: {
        type: Boolean
      },
      depositInsuranceId: {
        type: String
      },
      appInvoiceId: {
        type: String
      },
      // We can not use partnerId for deposit insurance payments, because we won't calculate these payments for partners
      appPartnerId: {
        type: String
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: true
  }
)

InvoicePaymentSchema.index({ createdAt: 1 })
InvoicePaymentSchema.index({ refundPaymentStatus: 1, partnerId: 1 })
InvoicePaymentSchema.index({ status: 1, partnerId: 1 })
InvoicePaymentSchema.index({ 'invoices.invoiceId': 1, type: 1, id: 1 })
InvoicePaymentSchema.index({ partnerId: 1, createdAt: -1 })
InvoicePaymentSchema.index({
  partnerId: 1,
  status: 1,
  createdAt: -1,
  paymentDate: 1
})
InvoicePaymentSchema.index({
  receivedFileName: 1,
  nodeIndex: 1
})
