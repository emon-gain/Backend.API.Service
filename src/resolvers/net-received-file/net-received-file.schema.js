import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const PaymentsTransactionSchema = new mongoose.Schema(
  {
    transactionType: {
      type: String,
      enum: ['CRDT', 'DBIT']
    },
    paymentId: {
      type: String
    },
    partnerId: {
      type: String
    },
    status: {
      type: String,
      enum: ['new', 'registered', 'unspecified']
    }
  },
  { _id: false, toJSON: { virtuals: true } }
)

PaymentsTransactionSchema.virtual('paymentInfo', {
  ref: 'invoice-payments',
  localField: 'paymentId',
  foreignField: '_id',
  justOne: true
})

PaymentsTransactionSchema.virtual('partnerInfo', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

export const NetReceivedFileSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      receivedFileName: {
        type: String,
        required: true
      },
      receivedFileKey: {
        type: String,
        required: true
      },
      status: {
        type: String,
        required: true,
        enum: ['created', 'processing', 'processed']
      },
      fileType: {
        type: String,
        enum: ['CstmrPmtStsRpt', 'BkToCstmrDbtCdtNtfctn']
      },
      isCreditTransaction: {
        type: Boolean
      },
      isDebitTransaction: {
        type: Boolean
      },
      payments: {
        type: [PaymentsTransactionSchema],
        default: undefined
      },
      invalidFile: {
        type: Boolean
      },
      moveFailed: {
        type: Boolean
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

NetReceivedFileSchema.index({ createdAt: 1 })
