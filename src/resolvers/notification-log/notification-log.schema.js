import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const attachmentMetaTypes = [
  'app_invoice_pdf',
  'collection_notice_attachment_pdf',
  'collection_notice_pdf',
  'credit_note_attachment_pdf',
  'credit_note_pdf',
  'download_attachment_xml',
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
  'lease_statement_pdf',
  'moving_in_ics',
  'moving_out_ics',
  'pre_reminder_attachment_pdf',
  'pre_reminder_pdf',
  'second_reminder_attachment_pdf',
  'second_reminder_pdf'
]

const AttachmentMetaSchema = new mongoose.Schema(
  {
    name: {
      type: String
    },
    content: {
      type: String
    },
    isInvoice: {
      type: Boolean
    },
    lang: {
      type: String,
      enum: ['en', 'no']
    },
    type: {
      type: String,
      enum: attachmentMetaTypes
    },
    fileId: {
      type: String
    },
    status: {
      type: String,
      enum: ['preparing', 'ready', 'running', 'done', 'moved_to_attachment']
    },
    id: {
      type: String
    },
    notificationLogId: {
      type: String
    },
    attachmentMetaId: {
      type: String
    },
    fileKey: {
      type: String
    }
  },
  { _id: false }
)

const AttachmentFilesSchema = new mongoose.Schema(
  {
    originalName: {
      type: String,
      required: true
    },
    s3FileName: {
      type: String,
      required: true
    },
    content: {
      type: String
    },
    isInvoice: {
      type: Boolean
    }
  },
  { _id: false }
)

const HistorySchema = new mongoose.Schema(
  {
    oldToEmail: {
      type: String
    },
    toEmail: {
      type: String
    },
    changedAt: {
      type: Date
    },
    oldToPhoneNumber: {
      type: String
    },
    toPhoneNumber: {
      type: String
    }
  },
  { _id: false }
)
const eventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date
    },
    status: {
      type: String
    },
    note: {
      type: String
    }
  },
  { _id: false }
)

export const NotificationLogSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      event: {
        type: String,
        required: true
      },
      type: {
        type: String,
        index: true,
        enum: ['email', 'sms'],
        required: true
      },
      status: {
        type: String,
        enum: [
          'new',
          'waiting_for_attachments',
          'ready',
          'preprocessing',
          'processing',
          'queued',
          'sent',
          'failed',
          'rejected',
          'bounced',
          'soft-bounced',
          'deferred'
        ],
        required: true
      },
      subject: {
        type: String,
        index: true
      },
      fromName: {
        type: String,
        default: 'Unite Living'
      },
      fromPhoneNumber: {
        type: String
      },
      toUserId: {
        type: String,
        index: true
      },
      toEmail: {
        type: String,
        index: true
      },
      toPhoneNumber: {
        type: String,
        index: true
      },
      content: {
        type: String,
        required: true
      },
      totalAttachment: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      attachments: {
        type: [AttachmentFilesSchema],
        default: undefined
      },
      attachmentsMeta: {
        type: [AttachmentMetaSchema],
        default: undefined
      },
      partnerId: {
        type: String
      },
      invoiceId: {
        type: String
      },
      accountId: {
        type: String
      },
      tenantId: {
        type: String
      },
      propertyId: {
        type: String
      },
      payoutId: {
        type: String
      },
      paymentId: {
        type: String
      },
      agentId: {
        type: String
      },
      branchId: {
        type: String
      },
      sentAt: {
        type: Date
      },
      emailHeaders: {
        type: Object
      },
      repairing: {
        type: Boolean
      },
      contractId: {
        type: String
      },
      sendTo: {
        type: String
      },
      msgOpenCount: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      msgClickCount: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      messageCost: {
        type: Number
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
      rejectReason: {
        type: String
      },
      history: {
        type: [HistorySchema],
        default: undefined
      },
      isResend: {
        type: Boolean
      },
      movingId: {
        type: String
      },
      annualStatementId: {
        type: String
      },
      statementId: {
        type: String
      },
      taskId: {
        type: String
      },
      commentId: {
        type: String
      },
      notificationLogId: {
        type: String
      },
      depositPaymentId: {
        type: String
      },
      isCorrectionInvoice: {
        type: Boolean
      },
      SESMsgId: {
        type: String,
        index: true
      },
      complaint: {
        type: Boolean
      },
      doNotSend: {
        type: Boolean
      },
      processStartedAt: {
        type: Date
      },
      events: {
        type: [eventsSchema]
      },
      retryCount: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

NotificationLogSchema.index({ createdAt: 1 })
NotificationLogSchema.index({
  invoiceId: 1,
  partnerId: 1,
  'attachmentsMeta.type': 1
})
NotificationLogSchema.index({
  'attachmentsMeta.attachmentMetaId': 1,
  'attachmentsMeta.notificationLogId': 1,
  id: 1
})
NotificationLogSchema.index({
  mandrillMsgId: 1,
  type: 1
})
NotificationLogSchema.index({
  'attachmentsMeta.id': 1,
  'attachmentsMeta.type': 1,
  'attachmentsMeta.fileId': 1
})
NotificationLogSchema.index({
  partnerId: 1,
  status: 1,
  type: 1
})
NotificationLogSchema.index({
  partnerId: 1,
  subject: 1
})
NotificationLogSchema.index({
  status: 1,
  type: 1
})
NotificationLogSchema.index({
  SESMsgId: 1,
  status: 1
})
NotificationLogSchema.index({
  annualStatementId: 1,
  contractId: 1,
  event: 1,
  partnerId: 1
})
NotificationLogSchema.index(
  {
    event: 1,
    partnerId: 1,
    toUserId: 1,
    type: 1,
    status: 1,
    sentAt: 1
  },
  {
    name: 'notification_log_filters_for_admin_app'
  }
)
NotificationLogSchema.index({
  subject: 'text'
})
NotificationLogSchema.index({
  contractId: 1,
  event: 1,
  invoiceId: 1,
  payoutId: 1
})
