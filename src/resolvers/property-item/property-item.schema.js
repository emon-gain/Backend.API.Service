import mongoose from 'mongoose'
import { CreatedBySchemas, Id, Message } from '../common'

const keyListSchemas = new mongoose.Schema(
  {
    id: {
      type: String
    },
    kindOfKey: {
      type: String
    },
    numberOfKey: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    numberOfKeysReturned: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const metersSchemas = new mongoose.Schema(
  {
    id: {
      type: String
    },
    numberOfMeter: {
      type: String
    },
    typeOfMeter: {
      type: String
    },
    measureOfMeter: {
      type: Number,
      min: 0
    },
    date: {
      type: Date
    }
  },
  { _id: false }
)

const inventoryFurnitureSchemas = new mongoose.Schema(
  {
    id: {
      type: String
    },
    name: {
      type: String
    },
    title: {
      type: String
    },
    quantity: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    status: {
      type: String,
      enum: ['ok', 'issues', 'notApplicable']
    },
    description: {
      type: String
    },
    responsibleForFixing: {
      type: String
    },
    dueDate: {
      type: Date
    },
    taskId: {
      type: String
    }
  },
  { _id: false }
)

const keysSchemas = new mongoose.Schema(
  {
    keysList: {
      type: [keyListSchemas],
      default: undefined
    },
    files: {
      type: [String],
      default: undefined
    }
  },
  { _id: false }
)

const meterReadingSchemas = new mongoose.Schema(
  {
    meters: {
      type: [metersSchemas],
      default: undefined
    },
    files: {
      type: [String],
      default: undefined
    }
  },
  { _id: false }
)

const inventorySchemas = new mongoose.Schema(
  {
    furniture: {
      type: [inventoryFurnitureSchemas],
      default: undefined
    },
    isFurnished: {
      type: Boolean
    },
    isPartiallyFurnished: {
      type: Boolean
    },
    files: {
      type: [String],
      default: undefined
    }
  },
  { _id: false }
)

const landlordSigningStatusSchema = new mongoose.Schema(
  {
    idfySignerId: {
      type: String
    },
    landlordId: {
      type: String
    },
    internalUrl: {
      type: String
    },
    signingUrl: {
      type: String
    },
    signed: {
      type: Boolean
    },
    signedAt: {
      type: Date
    }
  },
  { _id: false }
)

const agentSigningStatusSchema = new mongoose.Schema(
  {
    idfySignerId: {
      type: String
    },
    agentId: {
      type: String
    },
    internalUrl: {
      type: String
    },
    signingUrl: {
      type: String
    },
    signed: {
      type: Boolean
    },
    signedAt: {
      type: Date
    }
  },
  { _id: false }
)

const tenantSigningStatusSchema = new mongoose.Schema(
  {
    idfySignerId: {
      type: String
    },
    tenantId: {
      type: String
    },
    internalUrl: {
      type: String
    },
    signingUrl: {
      type: String
    },
    signed: {
      type: Boolean
    },
    signedAt: {
      type: Date
    }
  },
  { _id: false }
)

const signingMetaSchema = new mongoose.Schema(
  {
    signedTime: {
      type: String
    },
    signers: {
      type: [Object],
      default: undefined
    },
    documentId: {
      type: String
    },
    externalDocumentId: {
      type: String
    },
    signer: {
      type: Object
    }
  },
  { _id: false }
)

export const PropertyItemSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      propertyId: {
        type: String
      },
      partnerId: {
        type: String
      },
      tenantId: {
        type: String
      },
      agentId: {
        type: String
      },
      contractId: {
        type: String
      },
      type: {
        type: String,
        enum: ['in', 'out']
      },
      keys: {
        type: keysSchemas
      },
      meterReading: {
        type: meterReadingSchemas
      },
      inventory: {
        type: inventorySchemas
      },
      createdAt: {
        type: Date
      },
      createdBy: {
        type: String
      },
      movingInPdfGenerated: {
        type: Boolean
      },
      landlordSigningStatus: {
        type: landlordSigningStatusSchema
      },
      tenantSigningStatus: {
        type: [tenantSigningStatusSchema],
        default: undefined
      },
      agentSigningStatus: {
        type: agentSigningStatusSchema
      },
      idfyMovingInDocId: {
        type: String
      },
      movingSigningMeta: {
        type: signingMetaSchema
      },
      movingOutPdfGenerated: {
        type: Boolean
      },
      draftMovingInDoc: {
        type: Boolean
      },
      draftMovingOutDoc: {
        type: Boolean
      },
      isEsigningInitiate: {
        type: Boolean
      },
      esigningInitiatedAt: {
        type: Date
      },
      movingInSigningComplete: {
        type: Boolean
      },
      moveInCompleted: {
        type: Boolean
      },
      moveOutCompleted: {
        type: Boolean
      },
      signatureMechanism: {
        type: String
      },
      movingOutSigningComplete: {
        type: Boolean
      },
      fileIdForIcsFile: {
        type: String
      },
      isEsignReminderSentToAgentForMoveIn: {
        type: Boolean
      },
      isEsignReminderSentToTenantForMoveIn: {
        type: Boolean
      },
      isEsignReminderSentToAgentForMoveOut: {
        type: Boolean
      },
      isEsignReminderSentToTenantForMoveOut: {
        type: Boolean
      },
      isEsignReminderSentToLandlordForMoveIn: {
        type: Boolean
      },
      isEsignReminderSentToLandlordForMoveOut: {
        type: Boolean
      },
      idfyErrorEvents: {
        type: [Object],
        default: undefined
      },
      eSignReminderToTenantForMoveOutSentAt: {
        type: Date
      },
      eSignReminderToLandlordForMoveOutSentAt: {
        type: Date
      },
      eSignReminderToAgentForMoveOutSentAt: {
        type: Date
      },
      eSignReminderToTenantForMoveInSentAt: {
        type: Date
      },
      eSignReminderToLandlordForMoveInSentAt: {
        type: Date
      },
      eSignReminderToAgentForMoveInSentAt: {
        type: Date
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PropertyItemSchema.index({ createdAt: 1 })
