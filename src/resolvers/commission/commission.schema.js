import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'

export const CommissionSchema = new mongoose.Schema(
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
        index: true
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
      invoiceId: {
        type: String,
        index: true,
        required: true
      },
      amount: {
        type: Number,
        required: true
      },
      type: {
        type: String,
        enum: [
          'brokering_contract',
          'rental_management_contract',
          'addon_commission',
          'assignment_addon_income'
        ],
        required: true
      },
      note: {
        type: String
      },
      payoutId: {
        type: String,
        index: true
      },
      refundCommissionId: {
        type: String
      },
      refundCommissionAmount: {
        type: Number
      },
      commissionId: {
        type: String
      },
      addonId: {
        type: String
      },
      tenants: {
        type: [TenantsIdSchemas],
        default: undefined
      },
      serialId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      landlordInvoiceId: {
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

CommissionSchema.index({ createdAt: 1 })
