import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

export const AppRoleSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      name: {
        type: String,
        required: true
      },
      users: {
        type: [String], // Array of user ids
        index: true,
        default: undefined
      },
      type: {
        type: String,
        index: true,
        enum: [
          'app_admin',
          'app_manager',
          'partner_accounting',
          'partner_admin',
          'partner_agent',
          'partner_janitor',
          'lambda_manager'
        ],
        required: true
      },
      partnerId: {
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

AppRoleSchema.index({ createdAt: 1 })
