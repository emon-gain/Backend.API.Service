import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'

const AddonsInfoSchemas = new mongoose.Schema(
  {
    id: {
      type: String,
      index: true,
      required: true
    },
    addonId: {
      type: String,
      index: true,
      required: true
    },
    hasCommission: {
      type: Boolean
    },
    description: {
      type: String
    },
    tax: {
      type: Number
    },
    taxPercentage: {
      type: Number
    },
    creditTaxCodeId: {
      type: String
    },
    creditTaxCode: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    price: {
      type: Number
    },
    total: {
      type: Number
    },
    debitAccountId: {
      type: String
    },
    creditAccountId: {
      type: String
    },
    isNonRent: {
      type: Boolean
    }
  },
  { _id: false }
)

export const CorrectionSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      agentId: {
        type: String,
        index: true,
        required: true
      },
      branchId: {
        type: String,
        index: true,
        required: true
      },
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
      propertyId: {
        type: String,
        index: true,
        required: true
      },
      tenantId: {
        type: String,
        index: true,
        required: true
      },
      contractId: {
        type: String
      },
      amount: {
        type: Number,
        required: true
      },
      expenseDate: {
        type: Date
      },
      note: {
        type: String
      },
      files: {
        type: [String],
        default: undefined
      },
      status: {
        type: String,
        enum: ['unpaid', 'paid']
      },
      invoiceSummaryId: {
        // For relation between expense and invoice summary
        // In expense view, if need to any data display of invoice summary then need to relation between expense and invoice summary
        type: String,
        index: true
      },
      payoutId: {
        type: String,
        index: true
      },
      addons: {
        type: [AddonsInfoSchemas],
        default: undefined
      },
      addTo: {
        type: String,
        enum: ['payout', 'rent_invoice']
      },
      // Adding "non_rent_invoice" in addTo is not a good solution since the invoice creation logic is very much inter-connected.
      // For avoiding complicated scenarios, we are just going to add a flag if the correction  is non-rent type.
      // Then we will just check "isNonRent" flag while creating the invoice to create non-rent correction invoice.
      isNonRent: {
        type: Boolean
      },
      correctionSerialId: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      invoiceId: {
        type: String
      },
      tenants: {
        type: [TenantsIdSchemas],
        default: undefined
      },
      landlordInvoiceId: {
        type: String
      },
      correctionStatus: {
        type: String,
        enum: ['active', 'cancelled']
      },
      cancelledAt: {
        type: Date
      },
      isMissingTransactionForLandlordCreditNote: {
        type: Boolean
      },
      isVisibleToTenant: {
        type: Boolean
      },
      isVisibleToLandlord: {
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

CorrectionSchema.index({ createdAt: 1 })
CorrectionSchema.index({ 'addons.id': 1 })
CorrectionSchema.index({ 'addons.addonId': 1 })
