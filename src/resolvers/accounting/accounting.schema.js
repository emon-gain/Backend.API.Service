import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const AccountingSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true
      },
      type: {
        type: String,
        enum: [
          'rent',
          'rent_payment',
          'payout_to_landlords',
          'brokering_commission',
          'management_commission',
          'addon_commission',
          'invoice_fee',
          'invoice_reminder_fee',
          'collection_notice_fee',
          'loss_recognition',
          'eviction_notice_fee',
          'administration_eviction_notice_fee',
          'rent_with_vat',
          'rounded_amount',
          'final_settlement_payment'
        ]
      },
      subName: {
        type: String
      },
      debitAccountId: {
        type: String
      },
      creditAccountId: {
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

AccountingSchema.index({ createdAt: 1 })
