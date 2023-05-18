import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message, TenantsIdSchemas } from '../common'

const InvoiceReceiver = new mongoose.Schema(
  {
    tenantName: {
      type: String
    },
    landlordName: {
      type: String
    }
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
  },
  { _id: false }
)

const InvoiceContent = new mongoose.Schema(
  {
    type: {
      //N.B.: commissions types are "brokering_contract", "rental_management_contract", "addon_commission" and "assignment_addon_income" for landlord invoice create
      type: String,
      enum: ['deposit_insurance']
    },
    description: {
      type: String
    },
    qty: {
      type: Number,
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
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    total: {
      type: Number,
      required: true
    }
  },
  { _id: false }
)

const FeeContent = new mongoose.Schema(
  {
    type: {
      type: String,
      required: true,
      enum: ['invoice', 'reminder', 'postal', 'unpaid_reminder']
    },
    qty: {
      type: Number
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

const PdfMetaSchema = new mongoose.Schema(
  {
    enContent: {
      //english template content
      type: String
    },
    noContent: {
      //norwegian template content
      type: String
    },
    enS3FileName: {
      //english s3 file name
      type: String
    },
    noS3FileName: {
      //norwegian s3 file name
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

const pdfTypes = ['app_invoice_pdf']

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
      type: Date
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

export const AppInvoiceSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        required: true,
        index: true
      },
      accountId: {
        type: String,
        required: true,
        index: true
      },
      propertyId: {
        type: String,
        required: true,
        index: true
      },
      tenantId: {
        type: String,
        required: true,
        index: true
      },
      contractId: {
        type: String,
        required: true,
        index: true
      },
      depositInsuranceId: {
        type: String
      },
      isDepositInsurancePayment: {
        type: Boolean
      },
      serialId: {
        type: Number
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
      totalTAX: {
        type: Number
      },
      invoiceTotal: {
        //all amount including invoice fees.
        type: Number,
        default: 0
      },
      roundedAmount: {
        type: Number
      },
      depositInsuranceAmount: {
        type: Number
      },
      totalPaid: {
        type: Number,
        default: 0
      },
      feesMeta: {
        //all fees related data
        type: [FeeContent],
        default: undefined
      },
      lastPaymentDate: {
        //we need this because of payout.. Don't wait for next payout date. Pay after x days.
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
        enum: ['app_invoice']
      },
      invoiceId: {
        type: String
      },
      paymentId: {
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
        type: Object,
        blackbox: true
      },
      creditedAmount: {
        //credited invoice amount
        type: Number
      },
      isReadyCreditedContent: {
        //if invoice not sent notification to tenant/account and contract is terminated/cancel then
        //add isReadyCreditedContent tag for notify to tenant/account
        type: Boolean
      },
      isSentCredited: {
        //if created invoice sent notification to tenant/account then add isSentCredited tag
        type: Boolean
      },
      creditReason: {
        type: String
      },
      leaseCancelled: {
        //This field will be true when any upcoming lease gets cancelled and that has invoices
        //We'll find this field to check existing invoices. If it contains true, then it'll not be counted as active invoice.
        type: Boolean
      },
      delayDate: {
        type: Date
      },
      enabledNotification: {
        type: Boolean
      },
      invoiceSent: {
        //if created invoice sent notification to tenant/account then add invoiceSent tag
        type: Boolean
      },
      feesPaid: {
        type: Boolean
      },
      tenants: {
        type: [TenantsIdSchemas],
        default: undefined
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
      totalBalanced: {
        type: Number
      },
      remainingBalance: {
        type: Number
      },
      isPartiallyBalanced: {
        type: Boolean
      },
      isPayable: {
        type: Boolean
      },
      isRegeneratePdf: {
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

AppInvoiceSchema.index({ createdAt: 1 })
