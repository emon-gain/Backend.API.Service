import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'

export const InvoiceSummarySchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true,
        required: true
      },
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
      payoutId: {
        type: String,
        index: true
      },
      correctionsIds: {
        type: [String],
        index: true,
        default: undefined
      },
      expensesIds: {
        type: [String],
        index: true
      },
      feesIds: {
        type: [String],
        index: true,
        default: undefined
      },
      commissionsIds: {
        type: [String],
        index: true,
        default: undefined
      },
      invoiceAmount: {
        type: Number
      },
      commissionsAmount: {
        type: Number
      },
      feesAmount: {
        type: Number
      },
      correctionsAmount: {
        type: Number
      },
      expensesAmount: {
        type: Number
      },
      payoutAmount: {
        type: Number
      },
      dueDate: {
        type: Date,
        required: true
      },
      invoiceSerialId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      isPaid: {
        type: Boolean
      },
      payoutBalanced: {
        type: Boolean
      },
      tenants: {
        type: [TenantsIdSchemas],
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

InvoiceSummarySchema.index({ createdAt: 1 })
InvoiceSummarySchema.index({ partnerId: 1, invoiceId: 1, createdAt: -1 })
InvoiceSummarySchema.index({
  partnerId: 1,
  agentId: 1,
  tenantId: 1,
  invoiceId: 1,
  createdAt: -1
})
InvoiceSummarySchema.index({
  partnerId: 1,
  agentId: 1,
  'tenants.tenantId': 1,
  invoiceId: 1,
  createdAt: -1
})
