import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const CpiDataSetSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      cpiDataSet: {
        type: Object,
        required: true
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

CpiDataSetSchema.index({ createdAt: 1 })
