import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const xledgerSchema = new mongoose.Schema(
  {
    code: {
      type: String
    },
    hasError: {
      type: Boolean
    },
    hasUpdateError: {
      type: Boolean
    },
    id: {
      type: String
    },
    syncedAt: {
      type: Date
    }
  },
  { _id: false }
)
const powerOfficeSchema = new mongoose.Schema(
  {
    id: {
      type: String
    },
    code: {
      type: String
    },
    hasError: {
      type: Boolean
    },
    syncedAt: {
      type: Date
    }
  },
  { _id: false }
)

export const AccountSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      type: {
        type: String,
        index: true,
        enum: ['person', 'organization'],
        required: true
      },
      organizationId: {
        type: String,
        index: true
      },
      personId: {
        type: String, // User id.. for both person/organization we need a person id.
        index: true
      },
      name: {
        type: String
      },
      address: {
        type: String
      },

      partnerId: {
        type: String,
        index: true,
        required: true
      },
      branchId: {
        type: String,
        index: true,
        required: true
      },
      agentId: {
        type: String,
        index: true
      },
      status: {
        type: String,
        enum: ['in_progress', 'active', 'archived']
      },
      totalActiveProperties: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      lastUpdate: {
        type: Date
      },
      invoiceAccountNumber: {
        type: String
      },
      serial: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      bankAccountNumbers: {
        type: [String],
        default: undefined
      },
      aboutText: {
        type: String
      },
      norwegianNationalIdentification: {
        type: String
      },
      city: {
        type: String
      },
      country: {
        type: String
      },
      zipCode: {
        type: String
      },
      backupAddress: {
        type: String
      },
      xledger: {
        type: xledgerSchema
      },
      powerOffice: {
        type: powerOfficeSchema
      },
      vatRegistered: {
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

AccountSchema.index({ createdAt: 1 })
AccountSchema.index({ partnerId: 1, _id: 1 })
