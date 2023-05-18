import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const ImportSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true,
        required: true
      },
      importRefId: {
        type: String,
        index: true
      },
      fileKey: {
        type: String,
        index: true
      },
      fileBucket: {
        type: String,
        index: true
      },
      isImportingFromError: {
        type: Boolean,
        index: true
      },
      collectionName: {
        type: String,
        index: true
      },
      collectionId: {
        type: String,
        index: true
      },
      jsonData: {
        type: Object,
        index: true
      },
      hasError: {
        type: Boolean,
        index: true,
        default: false
      },
      errorMessage: {
        type: String,
        index: true
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
