import mongoose from 'mongoose'
import validator from 'validator'
import { Id, Message } from '../common'

const AppInfoSchema = new mongoose.Schema(
  {
    appName: {
      type: String
    },
    appSlogan: {
      type: String
    },
    companyName: {
      type: String
    },
    organizationId: {
      type: String
    },
    address: {
      type: String
    },
    phoneNumber: {
      type: String
    },
    email: {
      type: String,
      trim: true,
      lowercase: true,
      maxlength: 100,
      validate(value) {
        if (!validator.isEmail(value)) {
          throw new Error(Message.emailError)
        }
      }
    },
    website: {
      type: String
    }
  },
  { _id: false }
)

const externalLinksSchema = new mongoose.Schema(
  {
    linkedIn: {
      type: String
    },
    facebook: {
      type: String
    },
    twitter: {
      type: String
    },
    instagram: {
      type: String
    },
    googlePlus: {
      type: String
    }
  },
  { _id: false }
)

const ListingSettingsPropertySchemas = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true
    },
    id: {
      type: String,
      required: true
    }
  },
  { _id: false }
)

export const OpenExchangeInfoSchemas = new mongoose.Schema(
  {
    base: {
      type: String,
      required: true
    },
    rates: {
      type: Object,
      required: true
    }
  },
  { _id: false }
)

const CurrencySettingsSchemas = new mongoose.Schema(
  {
    currency: {
      type: String,
      required: true
    },
    amountVariations: {
      type: Number,
      required: true
    }
  },
  { _id: false }
)

const KeepSpaceHabitsSchemas = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true
    },
    name: {
      type: String,
      required: true
    },
    isDefault: {
      type: Boolean,
      required: true
    },
    rank: {
      type: Number,
      required: true,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const PartyHabitsSchemas = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true
    },
    name: {
      type: String,
      required: true
    },
    isDefault: {
      type: Boolean,
      required: true
    },
    rank: {
      type: Number,
      required: true,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const PersonalitiesSchemas = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true
    },
    name: {
      type: String,
      required: true
    }
  },
  { _id: false }
)

const InterestsSchemas = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true
    },
    name: {
      type: String,
      required: true
    }
  },
  { _id: false }
)

export const SettingSchema = new mongoose.Schema(
  [
    Id,
    {
      appInfo: {
        type: AppInfoSchema
      },
      externalLinks: {
        type: externalLinksSchema
      },
      listingTypes: {
        type: [ListingSettingsPropertySchemas],
        default: undefined
      },
      propertyTypes: {
        type: [ListingSettingsPropertySchemas],
        default: undefined
      },
      facilities: {
        type: [ListingSettingsPropertySchemas],
        default: undefined
      },
      includedInRent: {
        type: [ListingSettingsPropertySchemas],
        default: undefined
      },
      openExchangeInfo: {
        type: OpenExchangeInfoSchemas
      },
      currencySettings: {
        type: [CurrencySettingsSchemas],
        default: undefined
      },
      keepingSpace: {
        type: [KeepSpaceHabitsSchemas],
        default: undefined
      },
      partyHabits: {
        type: [PartyHabitsSchemas],
        default: undefined
      },
      personalities: {
        type: [PersonalitiesSchemas],
        default: undefined
      },
      interests: {
        type: [InterestsSchemas],
        default: undefined
      },
      version: {
        type: String
      },
      cpiDataSet: {
        type: Object
      },
      upgradeScripts: {
        type: [String],
        default: undefined
      },
      isCompleted: {
        type: Boolean
      },
      productionDataUpdated: {
        type: Boolean
      },
      isUpdatedAllListingAddresses: {
        type: Boolean
      },
      isAllUpdatedAddress: {
        type: Boolean
      },
      isUpdatedAllAddresses: {
        type: Boolean
      },
      runningDBUpgrade: {
        type: Boolean
      },
      bankAccountNumber: {
        type: String
      }
    }
  ],
  {
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
