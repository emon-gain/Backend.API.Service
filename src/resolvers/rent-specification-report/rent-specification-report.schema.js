import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const addonSchema = new mongoose.Schema(
  {
    addonId: {
      type: String,
      index: true,
      required: true
    },
    addonName: {
      type: String,
      index: true,
      required: true
    },
    addonTotal: {
      type: Number,
      index: true,
      required: true
    }
  },
  { _id: false }
)

export const RentSpecificationReportSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true,
        required: true
      },
      accountId: {
        type: String,
        index: true,
        required: true
      },
      branchId: {
        type: String,
        index: true,
        required: true
      },
      agentId: {
        type: String,
        index: true,
        required: true
      },
      propertyId: {
        type: String,
        index: true,
        required: true
      },
      contractId: {
        type: String,
        index: true,
        required: true
      },
      tenantId: {
        type: String,
        index: true,
        required: true
      },
      contractStartDate: {
        type: Date,
        index: true,
        required: true
      },
      contractEndDate: {
        type: Date,
        index: true
      },
      accountingPeriod: {
        type: Date,
        index: true,
        required: true
      },
      transactionPeriod: {
        type: Date,
        index: true,
        required: true
      },
      rent: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      rentWithVat: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      estimatedAddonsMeta: {
        type: [addonSchema],
        index: true
      },
      totalEstimatedAddons: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      totalMonthly: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      months: {
        type: Number,
        index: true,
        required: true,
        default: 0,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      days: {
        type: Number,
        index: true,
        required: true,
        default: 0,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      estimatedTotalPeriod: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      totalRent: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      totalRentWithVat: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      actualAddonsMeta: {
        type: [addonSchema],
        index: true
      },
      totalActualAddons: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      totalFees: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      totalCorrections: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      actualTotalPeriod: {
        type: Number,
        index: true,
        required: true,
        default: 0
      },
      hasTransactions: {
        type: Boolean,
        index: true,
        required: true,
        default: false
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
