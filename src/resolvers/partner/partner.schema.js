import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const PartnerSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      name: {
        type: String,
        required: true
      },
      subDomain: {
        type: String,
        index: true,
        required: true
      },
      ownerId: {
        type: String,
        index: true,
        required: true
      },
      isActive: {
        type: Boolean,
        index: true,
        required: true
      },
      accountType: {
        type: String,
        index: true,
        required: true,
        enum: ['broker', 'direct']
      },
      country: { type: String, index: true, default: undefined },
      isSelfService: { type: Boolean, index: true, default: undefined },
      sms: {
        type: Boolean
      },
      phoneNumber: {
        type: String
      },
      logo: {
        type: String
      },
      siteLogo: {
        type: String
      },
      serial: {
        type: Number,
        index: true
      },
      enableTransactions: {
        type: Boolean
      },
      enableFinn: {
        type: Boolean
      },
      finnId: {
        type: String
      },
      finnLogo: {
        type: String
      },
      enableTransactionsApi: {
        type: Boolean
      },
      enableTransactionsPeriod: {
        type: Boolean
      },
      enableInvoiceStartNumber: {
        type: Boolean
      },
      enableAnnualStatement: {
        type: Boolean
      },
      enableSkatteetaten: {
        type: Boolean
      },
      enableBrokerJournals: {
        type: Boolean
      },
      isPowerOfficeProcessCompleted: {
        type: Boolean
      },
      enableDepositAccount: {
        type: Boolean
      },
      enableCreditRating: {
        type: Boolean
      },
      enableRecurringDueDate: {
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

PartnerSchema.index({ createdAt: 1 })
PartnerSchema.index({ accountType: 1, isSelfService: 1 })
