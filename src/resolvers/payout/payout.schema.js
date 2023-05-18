import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'
import { FeedbackHistorySchema } from '../models'

const LastMonthDueSchemas = new mongoose.Schema(
  {
    commission: {
      type: Number
    },
    expense: {
      type: Number
    }
  },
  { _id: false }
)

const PayoutMetaSchemas = new mongoose.Schema(
  {
    type: {
      type: String,
      enum: [
        'rent_invoice',
        'expenses',
        'corrections',
        'brokering_commission',
        'addon_commission',
        'management_commission',
        'unpaid_earlier_payout',
        'unpaid_expenses_and_commissions',
        'moved_to_next_payout',
        'credit_rent_invoice',
        'credit_brokering_commission',
        'credit_addon_commission',
        'credit_management_commission',
        'addons',
        'credit_addons',
        'landlord_invoice',
        'final_settlement_invoiced',
        'final_settlement_invoiced_cancelled'
      ]
    },
    amount: {
      type: Number
    },
    invoiceId: {
      type: String
    },
    correctionsIds: {
      type: [String],
      default: undefined
    },
    expensesIds: {
      type: [String],
      default: undefined
    },
    commissionId: {
      type: String
    },
    payoutId: {
      type: String
    },
    addonsIds: {
      type: [String],
      default: undefined
    },
    commissionsIds: {
      type: [String],
      default: undefined
    },
    landlordInvoiceId: {
      type: String
    }
  },
  { _id: false }
)

export const PayoutSchema = new mongoose.Schema(
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
        type: String,
        index: true,
        required: true
      },
      invoiceId: {
        type: String,
        index: true
      },
      invoicePaid: {
        type: Boolean
      },
      invoicePaidOn: {
        type: Date
      },
      commissionsIds: {
        type: [String],
        index: true,
        default: undefined
      },
      correctionsIds: {
        type: [String],
        index: true,
        default: undefined
      },
      expensesIds: {
        type: [String],
        index: true,
        default: undefined
      },
      estimatedAmount: {
        // Tt'll be used in draft mode. Finally we'll check the 'amount' field for actual payout.
        type: Number,
        required: true
      },
      amount: {
        type: Number // Actual payout amount
      },
      sentToNETS: {
        type: Boolean
      },
      sentToNETSOn: {
        type: Date
      },
      status: {
        type: String,
        enum: [
          'estimated',
          'pending_for_approval',
          'waiting_for_signature',
          'approved',
          'in_progress',
          'completed',
          'failed'
        ]
      },
      meta: {
        type: [PayoutMetaSchemas],
        default: undefined
      },
      payoutDate: {
        type: Date
      },
      advancedPayout: {
        type: Boolean
      },
      note: {
        type: String,
        index: true
      },
      serialId: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      lastMonthDueMeta: {
        type: LastMonthDueSchemas
      },
      invoicePaidAfterPayoutDate: {
        type: Boolean
      },
      bankReferenceId: {
        type: String
      },
      bookingDate: {
        type: Date
      },
      paymentStatus: {
        type: String,
        enum: ['balanced', 'pending', 'paid']
      },
      feedbackStatusLog: {
        type: [FeedbackHistorySchema], // Multiple feedback history
        default: undefined
      },
      addonsIds: {
        type: [String],
        default: undefined
      },
      numberOfFails: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      invoiceCredited: {
        type: Boolean
      },
      tenants: {
        type: [TenantsIdSchemas],
        default: undefined
      },
      isFinalSettlement: {
        type: Boolean
      },
      newPayoutDate: {
        type: Date
      },
      bankRef: {
        type: String
      },
      invoiceLost: {
        type: Boolean
      },
      invoiceLostOn: {
        type: Date
      },
      holdPayout: {
        type: Boolean
      },
      previousMeta: {
        type: Object
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PayoutSchema.index({ createdAt: 1 })
PayoutSchema.index(
  {
    partnerId: 1,
    status: 1,
    invoicePaid: 1,
    invoicePaidAfterPayoutDate: 1,
    sentToNETS: 1,
    amount: 1,
    payoutDate: 1,
    invoicePaidOn: 1
  },
  { name: 'partner_payout_status' }
)
PayoutSchema.index({
  status: 1,
  paymentStatus: 1,
  partnerId: 1
})
PayoutSchema.index({
  amount: 1,
  createdAt: -1
})
PayoutSchema.index({
  serialId: 1,
  createdAt: -1
})
