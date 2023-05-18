import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const fileTypes = [
  'assignment_pdf',
  'lease_pdf',
  'invoice_pdf',
  'credit_note_pdf',
  'pre_reminder_pdf',
  'first_reminder_pdf',
  'second_reminder_pdf',
  'collection_notice_pdf',
  'invoice_attachment_pdf',
  'credit_note_attachment_pdf',
  'pre_reminder_attachment_pdf',
  'first_reminder_attachment_pdf',
  'second_reminder_attachment_pdf',
  'collection_notice_attachment_pdf',
  'eviction_notice_attachment_pdf',
  'eviction_due_reminder_notice_attachment_pdf',
  'email_attachment_pdf',
  'excel_attachment',
  'esigning_assignment_pdf',
  'esigning_lease_pdf',
  'landlord_invoice_pdf',
  'landlord_invoice_attachment_pdf',
  'landlord_pre_reminder_pdf',
  'landlord_first_reminder_pdf',
  'landlord_second_reminder_pdf',
  'landlord_collection_notice_pdf',
  'landlord_pre_reminder_attachment_pdf',
  'landlord_first_reminder_attachment_pdf',
  'landlord_second_reminder_attachment_pdf',
  'landlord_collection_notice_attachment_pdf',
  'landlord_credit_note_pdf',
  'landlord_credit_note_attachment_pdf',
  'esigning_moving_in_pdf',
  'esigning_moving_out_pdf',
  'moving_in_ics',
  'moving_out_ics',
  'lease_statement_pdf',
  'deposit_account_contract_pdf',
  'esigning_assignment_xml',
  'esigning_lease_xml',
  'correction_invoice_pdf',
  'eviction_document_pdf',
  'xml_attachment',
  'payouts_approval_esigning_pdf',
  'payments_approval_esigning_pdf',
  'esigning_deposit_insurance_pdf',
  'app_invoice_pdf'
]

const fileEventsSchema = new mongoose.Schema(
  {
    createdAt: {
      type: Date,
      required: true
    },
    status: {
      type: String,
      enum: [
        'created',
        'queued',
        'save_to_path_not_found',
        'upload_failed_to_s3',
        'upload_to_s3',
        'processing',
        'processing_failed',
        'received_message_from_sqs',
        'invalid_processing_file',
        'processed',
        'failed',
        'error_occurred_from_s3'
      ]
    },
    note: {
      type: String
    }
  },
  { _id: false }
)

export const FileSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true
      },
      title: {
        type: String,
        index: true,
        required: true
      },
      name: {
        type: String,
        index: true,
        required: true
      },
      jsonFileName: {
        type: String
      },
      size: {
        type: Number,
        required: true
      },
      directive: {
        type: String,
        index: true,
        required: true
      },
      context: {
        type: String,
        index: true,
        required: true
      },
      propertyId: {
        type: String,
        index: true
      },
      accountId: {
        type: String,
        index: true
      },
      tenantId: {
        type: String,
        index: true
      },
      agentId: {
        type: String
      },
      taskId: {
        type: String,
        index: true
      },
      contractId: {
        type: String,
        index: true
      },
      type: {
        type: String,
        enum: fileTypes
      },
      status: {
        type: String,
        enum: ['created', 'processing', 'processed', 'failed']
      },
      events: {
        type: [fileEventsSchema],
        default: undefined
      },
      invoiceId: {
        type: String
      },
      notificationLogId: {
        type: String
      },
      attachmentId: {
        type: String
      },
      eventStatus: {
        type: String,
        enum: ['created', 'processing', 'processed']
      },
      isOldInvoicePdf: {
        type: Boolean
      },
      landlordPartnerId: {
        type: String,
        index: true
      },
      s3FilePath: {
        type: String
      },
      tenantPartnerId: {
        type: String,
        index: true
      },
      movingId: {
        type: String
      },
      fileUrlHash: {
        type: String,
        index: true
      },
      signerId: {
        type: String
      },
      isExistingFile: {
        type: Boolean
      },
      partnerPayoutId: {
        type: String
      },
      userId: {
        type: String
      },
      importRefId: {
        type: String
      },
      importCollectionName: {
        type: String
      },
      assignmentSerial: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      leaseSerial: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      isVisibleToLandlord: {
        type: Boolean
      },
      isVisibleToTenant: {
        type: Boolean
      },
      directRemittanceApprovalUserIds: {
        type: [String],
        default: undefined
      },
      isFileInUse: {
        type: Boolean
      },
      // When need to identify which files are uploaded from partner public site
      uploadedBy: {
        type: String,
        enum: ['landlord', 'tenant']
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

FileSchema.index({ createdAt: 1 })
FileSchema.index({ partnerId: 1, createdAt: -1 })
FileSchema.index({ partnerId: 1, id: 1 })
FileSchema.index({ status: 1, 'events.status': 1, 'events.createdAt': 1 })
FileSchema.index({ partnerId: 1, status: 1, createdAt: 1 })
FileSchema.index({ eventStatus: 1, type: 1, createdAt: 1 })
FileSchema.index({ movingId: 1, type: 1 })
FileSchema.index({ partnerPayoutId: 1 })
