import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

/**
 * Lambda SQS
 - Created: Set the initial status when created the collection data
 - Sent: Sent the message to SQS Queue
 - Failed: Failed to sent the message to SQS Queue
 - Processing: Lambda received the SQS message, and then set the Lambda SQS status to processing. Set processingStartedAt time.
 - Lambda done, then delete the Lambda SQS data.
 */
export const LambdaSqsSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      status: {
        type: String,
        enum: ['created', 'sent', 'failed', 'processing'],
        required: true
      },
      partnerId: {
        type: String
      },
      params: {
        type: Object,
        required: true
      },
      actionType: {
        type: String,
        enum: [
          'download_email',
          'download_xml_email',
          'handle_credit_rating',
          'handle_finn_listing',
          'generate_pdf',
          'generate_signature',
          'handle_e_signing',
          'handle_deposit_insurance'
        ]
      },
      processStartedAt: {
        type: Date
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
