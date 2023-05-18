import mongoose from 'mongoose'
import { Id } from '../common'

const PreviewMetaSchema = new mongoose.Schema(
  {
    url: {
      type: String
    },
    title: {
      type: String
    },
    description: {
      type: String
    },
    image: {
      type: String
    },
    s3FileName: {
      type: String
    }
  },
  { _id: false }
)

export const ConversationMessageSchema = new mongoose.Schema(
  [
    Id,
    {
      createdBy: {
        type: String,
        immutable: true,
        index: true,
        required: true
      },
      conversationId: {
        type: String,
        index: true,
        required: true
      },
      content: {
        type: String
      },
      isFile: {
        type: Boolean
      },
      previewMeta: {
        type: PreviewMetaSchema
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

ConversationMessageSchema.index({ createdAt: 1 })
ConversationMessageSchema.index({
  conversationId: 1,
  createdAt: 1,
  createdBy: 1
})
