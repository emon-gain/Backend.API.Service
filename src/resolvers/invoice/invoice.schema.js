import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'

const InvoiceReceiver = new mongoose.Schema(
  {
    tenantName: {
      type: String
    },
    landlordName: {
      type: String
    }
    // Add more fields here
    // We'll save static data although there will be reference id
  },
  { _id: false }
)

const InvoiceSender = new mongoose.Schema(
  {
    companyName: {
      type: String,
      required: true
    },
    companyAddress: {
      type: String
    },
    orgId: {
      type: String
    }
    // Add more fields here
    // We'll save static data although there will be reference id
  },
  { _id: false }
)

const PayoutsBalanced = new mongoose.Schema(
  {
    payoutId: {
      type: String
    },
    amount: {
      type: Number
    },
    isAdjustedBalance: {
      type: Boolean
    }
  },
  { _id: false }
)

const InvoiceContent = new mongoose.Schema(
  {
    type: {
      // N.B.: commissions types are "brokering_contract", "rental_management_contract", "addon_commission" and "assignment_addon_income" for landlord invoice create
      type: String,
      enum: [
        'monthly_rent',
        'addon',
        'brokering_contract',
        'rental_management_contract',
        'addon_commission',
        'assignment_addon_income',
        'final_settlement'
      ]
    },
    description: {
      type: String
    },
    qty: {
      type: Number,
      required: true,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    price: {
      type: Number,
      required: true
    },
    tax: {
      type: Number
    },
    taxPercentage: {
      type: Number
    },
    total: {
      type: Number,
      required: true
    },
    addonId: {
      type: String
    },
    correctionId: {
      type: String
    },
    commissionId: {
      type: String
    },
    vat: {
      // Old schema
      type: Number
    },
    productServiceId: {
      // Old schema
      type: String
    },
    totalBalanced: {
      type: Number
      // Had decimal: true
    },
    payoutsIds: {
      type: [String],
      default: undefined
    },
    payouts: {
      type: [PayoutsBalanced],
      default: undefined
    }
  },
  { _id: false }
)

const FeeContent = new mongoose.Schema(
  {
    type: {
      type: String,
      required: true,
      enum: [
        'invoice',
        'reminder',
        'collection_notice',
        'postal',
        'unpaid_reminder',
        'unpaid_collection_notice',
        'reminder_fee_move_to',
        'collection_notice_fee_move_to',
        'eviction_notice',
        'administration_eviction_notice',
        'unpaid_eviction_notice',
        'unpaid_administration_eviction_notice',
        'eviction_notice_fee_move_to',
        'administration_eviction_notice_fee_move_to'
      ]
    },
    qty: {
      type: Number,
      required: true,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    amount: {
      type: Number
    },
    tax: {
      type: Number
    },
    total: {
      type: Number
    },
    original: {
      type: Boolean
    },
    isPaid: {
      type: Boolean
    },
    invoiceId: {
      type: String
    }
  },
  { _id: false }
)

const pdfTypes = [
  'collection_notice_attachment_pdf',
  'collection_notice_pdf',
  'credit_note_attachment_pdf',
  'credit_note_pdf',
  'email_attachment_pdf',
  'eviction_due_reminder_notice_attachment_pdf',
  'eviction_notice_attachment_pdf',
  'first_reminder_attachment_pdf',
  'first_reminder_pdf',
  'invoice_attachment_pdf',
  'invoice_pdf',
  'landlord_collection_notice_attachment_pdf',
  'landlord_credit_note_attachment_pdf',
  'landlord_credit_note_pdf',
  'landlord_first_reminder_attachment_pdf',
  'landlord_invoice_attachment_pdf',
  'landlord_invoice_pdf',
  'landlord_pre_reminder_attachment_pdf',
  'landlord_second_reminder_attachment_pdf',
  'pre_reminder_attachment_pdf',
  'pre_reminder_pdf',
  'second_reminder_attachment_pdf',
  'second_reminder_pdf'
]

const PdfMetaSchema = new mongoose.Schema(
  {
    enContent: {
      // English template content
      type: String
    },
    noContent: {
      // Norwegian template content
      type: String
    },
    enS3FileName: {
      // English s3 file name
      type: String
    },
    noS3FileName: {
      // Norwegian s3 file name
      type: String
    },
    enBuffer: {
      type: String
    },
    noBuffer: {
      type: String
    }
  },
  { _id: false }
)

const PdfSchema = new mongoose.Schema(
  {
    type: {
      type: String,
      enum: pdfTypes
    },
    fileId: {
      type: String
    }
  },
  { _id: false }
)

const PdfEventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true
    },
    status: {
      type: String,
      enum: ['created', 'processed', 'failed']
    },
    note: {
      type: String
    }
  },
  { _id: false }
)

const VippsInvoiceEventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true
    },
    status: {
      type: String,
      enum: [
        'new',
        'sending',
        'sending_failed',
        'failed',
        'sent',
        'created',
        'rejected',
        'pending',
        'expired',
        'approved',
        'deleted',
        'revoked'
      ]
    },
    note: {
      type: String
    }
  },
  { _id: false }
)

const CompelloInvoiceEventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true
    },
    note: {
      type: String
    },
    status: {
      type: String,
      enum: [
        'approved',
        'created',
        'deleted',
        'expired',
        'failed',
        'new',
        'pending',
        'rejected',
        'handled',
        'sending',
        'sending_failed',
        'sent'
      ]
    }
  },
  { _id: false }
)
export const InvoiceSchema = new mongoose.Schema(
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
      invoiceSerialId: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      status: {
        type: String,
        enum: [
          'new',
          'created',
          'overdue',
          'paid',
          'credited',
          'lost',
          'balanced',
          'cancelled'
        ]
      },
      dueDate: {
        type: Date
      },
      receiver: {
        type: InvoiceReceiver
      },
      sender: {
        type: InvoiceSender
      },
      invoiceSentAt: {
        type: Date
      },
      dueReminderSentAt: {
        type: Date
      },
      firstReminderSentAt: {
        type: Date
      },
      secondReminderSentAt: {
        type: Date
      },
      collectionNoticeSentAt: {
        type: Date
      },
      collectionNoticeDueDate: {
        type: Date
      },
      invoiceContent: {
        type: [InvoiceContent],
        default: undefined
      },
      addonsMeta: {
        type: [InvoiceContent],
        default: undefined
      },
      totalTAX: {
        type: Number
      },
      invoiceTotal: {
        // All amount including invoice fees.
        type: Number
      },
      roundedAmount: {
        type: Number
      },
      rentTotal: {
        type: Number // InvoiceTotal including original fees.
      },
      payoutableAmount: {
        type: Number // PayoutableAmount = monthly rent + addons (excluding fees)
        // Had decimal: true
      },
      commissionableTotal: {
        // Rent total + commission able addons
        type: Number
        // Had decimal: true
      },
      totalPaid: {
        type: Number
        // Had decimal: true
      },
      feesMeta: {
        // All fees related data
        type: [FeeContent],
        default: undefined
      },
      lastPaymentDate: {
        // We need this because of payout.. Don't wait for next payout date. Pay after x days.
        type: Date
      },
      kidNumber: {
        type: String,
        index: true
      },
      invoiceAccountNumber: {
        type: String,
        index: true
      },
      isOverPaid: {
        type: Boolean
      },
      isPartiallyPaid: {
        type: Boolean
      },
      isDefaulted: {
        type: Boolean
      },
      pdf: {
        type: [PdfSchema],
        index: true,
        default: undefined
      },
      pdfMeta: {
        type: PdfMetaSchema
      },
      s3PdfFileName: {
        type: String
      },
      invoiceMonth: {
        type: Date
      },
      invoiceMonths: {
        type: [Date],
        default: undefined
      },
      invoiceFrequency: {
        type: Number
      },
      invoiceStartOn: {
        type: Date
      },
      invoiceEndOn: {
        type: Date
      },
      isFirstInvoice: {
        type: Boolean
      },
      isDemo: {
        type: Boolean
      },
      invoiceType: {
        type: String,
        enum: [
          'invoice',
          'credit_note',
          'landlord_invoice',
          'landlord_credit_note'
        ]
      },
      invoiceId: {
        type: String
      },
      creditNoteId: {
        type: String
      },
      creditNoteIds: {
        type: [String],
        default: undefined
      },
      correctionsIds: {
        type: [String],
        default: undefined
      },
      fullyCredited: {
        type: Boolean
      },
      isPartiallyCredited: {
        type: Boolean
      },
      lostMeta: {
        type: Object
      },
      creditedAmount: {
        // Credited invoice amount
        type: Number
      },
      isReadyCreditedContent: {
        // If invoice not sent notification to tenant/account and contract is terminated/cancel then
        // Add isReadyCreditedContent tag for notify to tenant/account
        type: Boolean
      },
      isSentCredited: {
        // If created invoice sent notification to tenant/account then add isSentCredited tag
        type: Boolean
      },
      creditReason: {
        type: String
      },
      leaseCancelled: {
        // This field will be true when any upcoming lease gets cancelled and that has invoices
        // We'll find this field to check existing invoices. If it contains true, then it'll not be counted as active invoice.
        type: Boolean
      },
      delayDate: {
        type: Date
      },
      enabledNotification: {
        type: Boolean,
        default: undefined
      },
      invoiceSent: {
        // If created invoice sent notification to tenant/account then add invoiceSent tag
        type: Boolean
      },
      feesPaid: {
        type: Boolean
      },
      isCorrectionInvoice: {
        // This field will true when create correction invoice
        type: Boolean
      },
      evictionNoticeSentOn: {
        type: Date
      },
      evictionDueReminderNoticeSentOn: {
        type: Date
      },
      tenants: {
        type: [TenantsIdSchemas],
        default: undefined
      },
      evictionNoticeSent: {
        type: Boolean
      },
      evictionDueReminderSent: {
        type: Boolean
      },
      vippsEvents: {
        type: [VippsInvoiceEventsSchema],
        default: undefined
      },
      vippsStatus: {
        type: String,
        enum: [
          'new',
          'sending',
          'sending_failed',
          'failed',
          'sent',
          'created',
          'rejected',
          'pending',
          'expired',
          'approved',
          'deleted',
          'revoked'
        ]
      },
      pdfStatus: {
        type: String,
        enum: ['created', 'processed', 'failed']
      },
      pdfEvents: {
        type: [PdfEventsSchema],
        default: undefined
      },
      newTenantId: {
        type: String
      },
      commissionsMeta: {
        // Landlord invoice create by commission
        type: [InvoiceContent],
        default: undefined
      },
      totalBalanced: {
        type: Number
      },
      remainingBalance: {
        type: Number
      },
      isPartiallyBalanced: {
        type: Boolean
      },
      commissionsIds: {
        type: [String],
        default: undefined
      },
      isPayable: {
        type: Boolean
      },
      landlordInvoiceId: {
        type: String
      },
      isFinalSettlement: {
        type: Boolean
      },
      isPendingCorrection: {
        type: Boolean
      },
      isCreditedForCancelledCorrection: {
        type: Boolean
      },
      forCorrection: {
        type: Boolean
      },
      isRegeneratePdf: {
        type: Boolean
      },
      disabledPartnerNotification: {
        type: Boolean
      },
      isNonRentInvoice: {
        type: Boolean
      },
      earlierInvoiceId: {
        type: String
      },
      voidExistingPayment: {
        type: Boolean
      },
      previousMeta: {
        type: Object
      },
      compelloEvents: {
        type: [CompelloInvoiceEventsSchema],
        default: undefined
      },
      compelloStatus: {
        type: String,
        enum: [
          'approved',
          'created',
          'deleted',
          'expired',
          'failed',
          'new',
          'pending',
          'rejected',
          'handled',
          'sending',
          'sending_failed',
          'sent'
        ]
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
InvoiceSchema.index({ invoiceTotal: 1 })
InvoiceSchema.index({ invoiceType: 1 })
InvoiceSchema.index({ isPayable: 1 })
InvoiceSchema.index({ isOverPaid: 1 })
InvoiceSchema.index({ remainingBalance: 1 })
InvoiceSchema.index({ status: 1 })
InvoiceSchema.index({ totalDue: 1 })
InvoiceSchema.index({ createdAt: 1 })
InvoiceSchema.index({ invoiceType: 1, agentId: 1, partnerId: 1, status: 1 })
InvoiceSchema.index({ propertyId: 1, status: 1, invoiceType: 1 })
InvoiceSchema.index({ invoiceType: 1, status: 1, dueDate: 1 })
InvoiceSchema.index({ partnerId: 1, invoiceType: 1, agentId: 1, createdAt: 1 })
InvoiceSchema.index({ invoiceType: 1, partnerId: 1, status: 1 })
InvoiceSchema.index({ isDefaulted: 1, partnerId: 1 })
InvoiceSchema.index({ isOverPaid: 1, partnerId: 1 })
InvoiceSchema.index({ isPartiallyCredited: 1, partnerId: 1 })
InvoiceSchema.index({ isPartiallyPaid: 1, partnerId: 1 })
InvoiceSchema.index({ status: 1, partnerId: 1 })
InvoiceSchema.index({ partnerId: 1, invoiceType: 1, createdAt: 1 })
InvoiceSchema.index({ partnerId: 1, invoiceType: 1 })
InvoiceSchema.index({ partnerId: 1, agentId: 1, isDefaulted: 1, createdAt: -1 })
InvoiceSchema.index({ partnerId: 1, agentId: 1, isOverPaid: 1, createdAt: -1 })
InvoiceSchema.index({
  partnerId: 1,
  agentId: 1,
  isPartiallyCredited: 1,
  createdAt: -1
})
InvoiceSchema.index({
  partnerId: 1,
  agentId: 1,
  isPartiallyPaid: 1,
  createdAt: -1
})
InvoiceSchema.index({ partnerId: 1, agentId: 1, status: 1, createdAt: -1 })
InvoiceSchema.index({ partnerId: 1, isDefaulted: 1, createdAt: -1 })
InvoiceSchema.index({ partnerId: 1, isOverPaid: 1, createdAt: -1 })
InvoiceSchema.index({ partnerId: 1, isPartiallyCredited: 1, createdAt: -1 })
InvoiceSchema.index({ partnerId: 1, isPartiallyPaid: 1, createdAt: -1 })
InvoiceSchema.index({ partnerId: 1, status: 1, createdAt: -1 })
InvoiceSchema.index({ accountId: 1, status: 1, invoiceType: 1 })
InvoiceSchema.index({ contractId: 1, invoiceEndOn: 1 })
InvoiceSchema.index({ contractId: 1, invoiceMonth: 1 })
InvoiceSchema.index({ accountId: 1, partnerId: 1, invoiceType: 1, dueDate: 1 })
InvoiceSchema.index({ partnerId: 1, invoiceType: 1, status: 1, isDefaulted: 1 })
InvoiceSchema.index({ creditNoteId: 1, creditNoteIds: 1 })
InvoiceSchema.index({
  invoiceSent: 1,
  partnerId: 1,
  status: 1,
  isPartiallyPaid: 1,
  isDefaulted: 1
})
InvoiceSchema.index({
  status: 1,
  partnerId: 1,
  invoiceSent: 1
})
InvoiceSchema.index({
  partnerId: 1,
  agentId: 1,
  invoiceSent: 1,
  status: 1,
  createdAt: -1,
  isPartiallyPaid: 1,
  isDefaulted: 1
})
InvoiceSchema.index({
  partnerId: 1,
  agentId: 1,
  status: 1,
  createdAt: -1,
  invoiceSent: 1
})
InvoiceSchema.index({
  partnerId: 1,
  invoiceSent: 1,
  status: 1,
  createdAt: -1,
  isPartiallyPaid: 1,
  isDefaulted: 1
})
InvoiceSchema.index({
  partnerId: 1,
  status: 1,
  createdAt: -1,
  invoiceSent: 1
})
InvoiceSchema.index({
  contractId: 1,
  invoiceType: 1,
  leaseCancelled: 1
})
InvoiceSchema.index({
  id: 1,
  partnerId: 1
})
InvoiceSchema.index({
  tenantId: 1,
  status: 1,
  invoiceType: 1
})
InvoiceSchema.index({
  evictionDueReminderSent: 1,
  partnerId: 1
})
InvoiceSchema.index({
  evictionNoticeSent: 1,
  partnerId: 1
})
InvoiceSchema.index({
  partnerId: 1,
  feesPaid: 1,
  feesMeta: 1,
  invoiceType: 1,
  status: 1
})
InvoiceSchema.index({
  isOverPaid: 1,
  feesMeta: 1
})
InvoiceSchema.index({
  status: 1,
  feesMeta: 1
})
InvoiceSchema.index({
  partnerId: 1,
  invoiceType: 1,
  branchId: 1,
  createdAt: 1
})
InvoiceSchema.index({
  invoiceType: 1,
  branchId: 1,
  partnerId: 1,
  status: 1
})
InvoiceSchema.index({
  partnerId: 1,
  createdAt: -1,
  feesPaid: 1,
  feesMeta: 1,
  invoiceType: 1,
  status: 1
})
InvoiceSchema.index({
  'tenants.tenantId': 1,
  invoiceType: 1,
  status: 1
})
InvoiceSchema.index({
  evictionDueReminderSent: 1,
  partnerId: 1,
  status: 1
})
InvoiceSchema.index({
  evictionNoticeSent: 1,
  partnerId: 1,
  status: 1
})
InvoiceSchema.index({
  tenantId: 1,
  invoiceType: 1,
  status: 1
})
InvoiceSchema.index({
  partnerId: 1,
  id: 1
})
