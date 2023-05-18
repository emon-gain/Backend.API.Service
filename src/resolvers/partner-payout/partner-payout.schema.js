import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const EventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true
    },
    status: {
      type: String,
      enum: [
        'created',
        'job_started',
        'failed',
        'payouts_found', // There are payouts, which to be processed
        'no_payouts_found',
        'refund_payments_found', // There are refund payments, which to be processed
        'no_refund_payments_found',
        'added_payout_ids',
        'added_refund_payments_ids',
        'pending_for_approval',
        'approved',
        'divided',
        'ready', // Ready to prepare payouts proccess data
        'asice_prepared',
        'created_s3_to_sftp_job',
        'sent_to_s3',
        's3_to_sftp',
        'asice_approved',
        'nets_received',
        'nets_partially_accepted',
        'nets_rejected',
        'nets_accepted'
      ]
    },
    note: {
      type: String
    }
  },
  { _id: false }
)

const directRemittanceSigningMetaSchema = new mongoose.Schema(
  {
    documentId: {
      type: String
    },
    externalDocumentId: {
      type: String
    },
    signers: {
      type: [Object],
      default: undefined
    }
  },
  { _id: false }
)

const directRemittanceSigningStatusSchema = new mongoose.Schema(
  {
    authenticationReference: {
      type: String
    },
    categoryPurposeCode: {
      type: String
    },
    idfySignerId: {
      type: String
    },
    internalUrl: {
      type: String
    },
    signingUrl: {
      type: String
    },
    signed: {
      type: Boolean
    },
    signedAt: {
      type: Date
    },
    userId: {
      type: String
    }
  },
  { _id: false }
)

export const PartnerPayoutSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true,
        required: true
      },
      type: {
        type: String,
        required: true,
        enum: ['payout', 'refund_payment']
      },
      hasPayouts: {
        type: Boolean
      },
      events: {
        type: [EventsSchema],
        default: undefined
      },
      status: {
        type: String,
        enum: [
          'created',
          'pending_for_approval',
          'waiting_for_signature',
          'approved',
          'processing',
          'sent',
          'error',
          'asice_approved',
          'validated',
          'accepted',
          'failed',
          'partially_completed',
          'completed'
        ]
      },
      payoutIds: {
        type: [String],
        default: undefined
      },
      payoutProcessId: {
        type: String
      },
      paymentIds: {
        // For refundable payments
        type: [String],
        default: undefined
      },
      hasRefundPayments: {
        type: Boolean
      },
      directRemittanceESigningInitiatedAt: { type: Date },
      directRemittanceIDFYDocumentId: {
        type: String
      },
      directRemittanceSigningMeta: {
        type: directRemittanceSigningMetaSchema,
        default: undefined
      },
      directRemittanceSigningStatus: {
        type: [directRemittanceSigningStatusSchema],
        default: undefined
      },
      partnerPayoutId: {
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

PartnerPayoutSchema.index({ createdAt: 1 })
