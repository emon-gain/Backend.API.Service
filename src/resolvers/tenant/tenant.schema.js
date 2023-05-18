import mongoose from 'mongoose'
import validator from 'validator'
import { Id, Message } from '../common'

const EmployerMetaSchema = new mongoose.Schema(
  {
    employerName: {
      type: String
    },
    employerPhoneNumber: {
      type: String
    },
    workingPeriod: {
      type: String
    }
  },
  { _id: false }
)

const previousEmployerMetaSchema = new mongoose.Schema(
  {
    reference: {
      type: String
    },
    previousLandlordName: {
      type: String
    },
    previousLandlordPhoneNumber: {
      type: String
    },
    previousLanlordEmail: {
      type: String,
      trim: true,
      lowercase: true,
      maxlength: 100,
      validate(value) {
        if (!validator.isEmail(value)) {
          throw new Error(Message.emailError)
        }
      }
    }
  },
  { _id: false }
)

const InterestFormMetaSchema = new mongoose.Schema(
  {
    movingFrom: {
      type: String
    },
    employerMeta: {
      type: EmployerMetaSchema
    },
    previousEmployerMeta: {
      type: previousEmployerMetaSchema
    }
  },
  { _id: false }
)

const TenantsPropertiesSchemas = new mongoose.Schema(
  {
    propertyId: {
      type: String,
      index: true,
      required: true
    },
    accountId: {
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
      index: true,
      required: true
    },
    contractId: {
      type: String,
      index: true
    },
    status: {
      type: String,
      enum: [
        'invited',
        'interested',
        'offer',
        'signed',
        'upcoming',
        'active',
        'rejected',
        'closed',
        'not_interested',
        'in_progress'
      ]
    },
    createdAt: {
      type: Date,
      index: true,
      required: true
    },
    createdBy: {
      type: String,
      index: true,
      required: true
    },
    wantsRentFrom: {
      type: Date
    },
    numberOfTenant: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    interestFormMeta: {
      type: InterestFormMetaSchema
    },
    fileIds: {
      type: [String]
    },
    preferredLengthOfLease: {
      type: String
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
    accountId: {
      type: String
    },
    syncedAt: {
      type: Date
    }
  },
  { _id: false }
)

const kycFormSchema = new mongoose.Schema(
  {
    contractId: {
      type: String
    },
    referenceNumber: {
      type: String
    },
    depositAmount: {
      type: Number
    },
    isSubmitted: {
      type: Boolean
    },
    isFormSubmitted: {
      type: Boolean
    },
    createdAt: {
      type: Date
    },
    status: {
      type: String,
      enum: ['new', 'regenerated', 'cancel']
    },
    formData: {
      type: Object
    }
  },
  { _id: false }
)

const xledgerSchema = new mongoose.Schema(
  {
    accountId: {
      type: String
    },
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

const bankContractFilesSchema = new mongoose.Schema(
  {
    contractId: {
      type: String
    },
    fileId: {
      type: String
    }
  },
  { _id: false }
)

const depositAccountMetaSchema = new mongoose.Schema(
  {
    kycForms: {
      type: [kycFormSchema],
      default: undefined
    },
    bankContractFiles: {
      type: [bankContractFilesSchema],
      default: undefined
    }
  },
  { _id: false }
)

export const TenantSchema = new mongoose.Schema(
  [
    Id,
    {
      name: {
        type: String,
        index: true,
        required: true
      },
      type: {
        type: String,
        index: true,
        enum: ['active', 'archived'],
        required: true
      },
      userId: {
        type: String,
        index: true,
        required: true
      },
      partnerId: {
        type: String,
        index: true,
        required: true
      },
      properties: {
        type: [TenantsPropertiesSchemas],
        index: true,
        required: true,
        default: undefined
      },
      serial: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      billingAddress: {
        type: String
      },
      aboutText: {
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
      powerOffice: {
        type: [powerOfficeSchema],
        default: undefined
      },
      depositAccountMeta: {
        type: depositAccountMetaSchema
      },
      lastUpdate: {
        type: Date
      },
      creditRatingInfo: {
        type: Object
      },
      creditRatingTermsAcceptedOn: {
        type: Date
      },
      isAskForCreditRating: {
        type: Boolean
      },
      tokenAskForCreditRatingUrl: {
        type: String
      },
      createdBy: {
        // Default createdBy immutable is true but here need to false
        type: String,
        index: true,
        default: 'SYSTEM'
      },
      xledger: {
        type: [xledgerSchema],
        default: undefined
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

TenantSchema.index({ createdAt: 1 })
TenantSchema.index({
  partnerId: 1,
  id: 1
})
TenantSchema.index({
  partnerId: 1,
  name: 1
})
TenantSchema.index({
  'properties.contractId': 1,
  'properties.propertyId': 1,
  'properties.status': 1
})
