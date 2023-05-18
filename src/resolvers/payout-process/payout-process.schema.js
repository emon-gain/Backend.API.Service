import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const FeedbackHistorySchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true
    },
    status: {
      type: String,
      required: true
    },
    reason: {
      type: String
    },
    receivedFileName: {
      type: String
    },
    netsReceivedFileId: {
      type: String
    }
  },
  { _id: false }
)

const CreditTransferSchema = new mongoose.Schema(
  {
    contractId: {
      type: String,
      required: true
    },
    accountId: {
      type: String,
      required: true
    },
    payoutId: {
      type: String
    },
    paymentInstrId: {
      type: String,
      required: true
    },
    paymentEndToEndId: {
      type: String,
      required: true
    },
    creditorAccountId: {
      type: String,
      required: true
    },
    debtorAccountId: {
      type: String
    },
    status: {
      type: String,
      required: true
    },
    amount: {
      type: Number,
      required: true
    },
    reason: {
      type: String
    },
    paymentReferenceId: {
      type: String
    },
    bookingDate: {
      type: Date
    },
    paymentId: {
      // For payment refundable
      type: String
    },
    bankRef: {
      type: String
    }
  },
  { _id: false }
)

export const PayoutProcessSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      sentFileName: {
        type: String
      },
      sentFileStatus: {
        type: String,
        enum: [
          'not_created',
          'created',
          'processing',
          'sent_to_sftp',
          'processed'
        ]
      },
      processingStartedAt: {
        type: Date
      },
      partnerId: {
        type: String,
        index: true
      },
      groupHeaderMsgId: {
        type: String,
        index: true,
        required: true
      },
      paymentInfoId: {
        type: String,
        required: true
      },
      requestExecuteDate: {
        type: Date,
        required: true
      },
      debtorAccountId: {
        // This should be removing future version. we are not using this.
        type: String
      },
      creditTransferInfo: {
        type: [CreditTransferSchema], // Multiple payments transfer to landlords
        required: true,
        default: undefined
      },
      status: {
        type: String,
        enum: ['new', 'ASICE_OK', 'ACCP', 'ACTC', 'PART', 'RJCT', 'completed']
        // If status is PART, then check all creditTransfer statuses (If RJCT+booked is not equal to total credit transfer))
        // "completed" means (If status is PART, then check all creditTransfer statuses (If RJCT+booked is equal to total credit transfer))
      },
      bookedAt: {
        type: Date
      },
      feedbackCreatedAt: {
        // Last feedback
        type: Date
      },
      feedbackStatusLog: {
        type: [FeedbackHistorySchema], // Multiple feedback history
        default: undefined
      },
      payoutIds: {
        type: [String],
        default: undefined
      },
      partnerPayoutId: {
        type: String
      },
      paymentIds: {
        // For payment refundable
        type: [String],
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

PayoutProcessSchema.index({ createdAt: 1 })
PayoutProcessSchema.index({
  sentFileStatus: 1,
  sentFileName: 1
})
PayoutProcessSchema.index({
  'creditTransferInfo.payoutId': 1
})
PayoutProcessSchema.index({
  'creditTransferInfo.paymentEndToEndId': 1
})
