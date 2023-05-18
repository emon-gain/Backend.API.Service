import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const PartnersUsagesMetaSchema = new mongoose.Schema(
  {
    invoiceId: {
      type: String
    }
  },
  { _id: false }
)

export const PartnersUsagesSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        required: true
      },
      type: {
        type: String,
        enum: [
          'active_agents',
          'active_agents_with_active_properties',
          'active_properties',
          'active_users',
          'credit_rating',
          'deposit_account',
          'esign',
          'finn',
          'outgoing_sms',
          'parking_lots',
          'vipps_invoice',
          'compello_invoice'
        ],
        required: true
      },
      branchId: {
        type: String
      },
      tenantId: {
        type: String
      },
      notificationLogId: {
        type: String
      },
      total: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      totalMessages: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      errorReason: {
        type: String
      },
      meta: {
        type: PartnersUsagesMetaSchema
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PartnersUsagesSchema.index({ createdAt: 1 })
PartnersUsagesSchema.index({ partnerId: 1, type: 1 })
