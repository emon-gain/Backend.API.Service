import mongoose from 'mongoose'
import { AddonsSchemas } from '../models'
import { Id, Message } from '../common'

const ListingBedroomsSchemas = new mongoose.Schema(
  {
    id: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    bedroomSize: {
      type: Number,
      index: true
    },
    bedroomFurnished: {
      type: Boolean,
      index: true
    }
  },
  { _id: false }
)

const ListingTempUserSchemas = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true
    },
    gender: {
      type: String
    },
    birthday: {
      type: Date
    },
    publish: {
      type: Boolean
    }
  },
  { _id: false }
)

const ListingLocationSchemas = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true
    },
    placeId: {
      type: String
    },
    city: {
      type: String
    },
    cityPlaceId: {
      type: String,
      index: true
    },
    lat: {
      type: Number,
      index: true
    },
    lng: {
      type: Number,
      index: true
    },
    country: {
      type: String
    },
    streetNumber: {
      type: String
    },
    sublocality: {
      type: String,
      optional: true
    },
    countryShortName: {
      type: String
    },
    postalCode: {
      type: String
    }
  },
  { _id: false }
)

const finnMessageDocSchemas = new mongoose.Schema(
  {
    elementName: {
      type: String
    },
    message: {
      type: String
    }
  },
  { _id: false }
)

const serialList = new mongoose.Schema(
  {
    orderSerial: {
      type: String
    },
    createdAt: {
      type: Date
    },
    statisticsURL: {
      type: String
    }
  },
  { _id: false }
)

const finnMessageSchemas = new mongoose.Schema(
  {
    fatal: {
      type: [finnMessageDocSchemas],
      default: undefined
    },
    // Mongoose does't allow field name called "errors"
    errorsMeta: {
      type: [finnMessageDocSchemas],
      default: undefined
    },
    warnings: {
      type: [finnMessageDocSchemas],
      default: undefined
    },
    info: {
      type: [finnMessageDocSchemas],
      default: undefined
    }
  },
  { _id: false }
)

const finnDataSchemas = new mongoose.Schema(
  {
    adURL: {
      type: String
    },
    statisticsURL: {
      type: String
    },
    adSendTime: {
      type: Date
    },
    adUpdateTimes: {
      type: [Date],
      default: undefined
    },
    isShareAtFinn: {
      type: Boolean
    },
    disableFromFinn: {
      type: Boolean
    },
    messages: {
      type: finnMessageSchemas
    },
    finnShareAt: {
      type: Date
    },
    finnArchivedAt: {
      type: Date
    },
    finnErrorRequest: {
      type: String
    },
    serial: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    serialHistory: {
      type: [serialList],
      default: undefined
    },
    updateType: {
      type: String
    },
    isPublishing: {
      type: Boolean
    },
    isRePublishing: {
      type: Boolean
    },
    isArchiving: {
      type: Boolean
    },
    requestedAt: {
      type: Date
    }
  },
  { _id: false }
)

const listingImagesSchemas = new mongoose.Schema(
  {
    imageName: {
      type: String
    },
    title: {
      type: String,
      maxlength: 80
    },
    rotate: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

// Remove this schema after update listing location

const BackupListingLocationSchemas = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true
    },
    placeId: {
      type: String
    },
    city: {
      type: String
    },
    cityPlaceId: {
      type: String
    },
    lat: {
      type: Number
    },
    lng: {
      type: Number
    },
    country: {
      type: String
    },
    streetNumber: {
      type: String
    },
    sublocality: {
      type: String
    },
    countryShortName: {
      type: String
    },
    postalCode: {
      type: String
    }
  },
  { _id: false }
)

export const ListingSchema = new mongoose.Schema(
  [
    Id,
    {
      ownerId: {
        type: String,
        index: true
      },
      janitorId: {
        type: String,
        index: true
      },
      title: {
        type: String,
        maxlength: 80,
        index: true
      },
      listingTypeId: {
        type: String,
        index: true,
        required: true
      },
      propertyTypeId: {
        type: String,
        index: true
      },
      location: {
        type: ListingLocationSchemas,
        index: true,
        required: true
      },
      placeIds: {
        type: [String],
        index: true,
        default: undefined
      },
      availabilityStartDate: {
        type: Date,
        index: true,
        required: true
      },
      availabilityEndDate: {
        type: Date,
        index: true
      },
      minimumStay: {
        type: Number,
        index: true,
        min: 0,
        default: 0,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      monthlyRentAmount: {
        type: Number,
        required: true,
        index: true,
        min: 0
      },
      depositAmount: {
        type: Number,
        index: true,
        min: 0
      },
      baseMonthlyRentAmount: {
        // USD conversion of monthlyRentAmount
        type: Number,
        index: true
      },
      currency: {
        type: String
      },
      listed: {
        type: Boolean,
        index: true
      },
      liveThere: {
        type: Boolean
      },
      description: {
        type: String
      },
      images: {
        type: [listingImagesSchemas],
        default: undefined
      },
      placeSize: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        },
        default: 0
      },
      noOfBedrooms: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      gender: {
        type: String,
        enum: ['male', 'female', 'all'],
        index: true
      },
      noOfAvailableBedrooms: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      availableBedrooms: {
        type: [ListingBedroomsSchemas],
        index: true,
        default: undefined
      },
      livingRoom: {
        type: Boolean,
        index: true
      },
      noOfLivingRoom: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      livingRoomFurnished: {
        type: Boolean,
        index: true
      },
      kitchen: {
        type: Boolean,
        index: true
      },
      noOfKitchen: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      bathroom: {
        type: Boolean,
        index: true
      },
      noOfBathroom: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      facilities: {
        type: [String],
        index: true,
        default: undefined
      },
      includedInRent: {
        type: [String],
        index: true,
        default: undefined
      },
      favorite: {
        type: [String],
        index: true,
        default: undefined
      },
      tempUser: {
        type: ListingTempUserSchemas
      },
      listedAt: {
        type: Date,
        index: true
      },
      pageView: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      disabledAt: {
        type: Date,
        index: true
      },
      agentId: {
        type: String,
        index: true
      },
      branchId: {
        type: String,
        index: true
      },
      partnerId: {
        type: String,
        index: true
      },
      accountId: {
        type: String,
        index: true
      },
      propertyStatus: {
        type: String,
        /**
         Here are the statuses to be used:
         1. Active = When a new property has been created
         2. Archived = Whenever the property has been cancelled/closed
         3. Maintenance = When landlord contract(s) has been created
         **/
        enum: ['active', 'maintenance', 'archived']
      },
      hasActiveLease: {
        type: Boolean
      },
      hasUpcomingLease: {
        type: Boolean
      },
      hasAssignment: {
        type: Boolean
      },
      hasProspects: {
        type: Boolean
      },
      leaseStartDate: {
        type: Date,
        index: true
      },
      leaseEndDate: {
        type: Date,
        index: true
      },
      floor: {
        type: String
      },
      apartmentId: {
        type: String
      },
      gnr: {
        type: String
      },
      bnr: {
        type: String
      },
      snr: {
        type: String
      },
      addedInFinn: {
        type: Boolean
      },
      deleted: {
        type: Boolean
      },
      addons: {
        type: [AddonsSchemas],
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
      aboutText: {
        type: String
      },
      finn: {
        type: finnDataSchemas
      },
      furnished: {
        type: String,
        enum: ['furnished', 'partially_furnished', 'unfurnished']
      },
      hasInProgressLease: {
        type: Boolean
      },
      backupLocation: {
        type: BackupListingLocationSchemas
      },
      isUpdatedAddress: {
        type: Boolean
      },
      videoUrl: {
        type: String
      },
      view360Url: {
        type: String
      },
      groupId: {
        type: String
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

ListingSchema.index({ createdAt: 1 })
ListingSchema.index({ 'location.cityPlaceId': 1 })
ListingSchema.index({ 'location.lat': 1 })
ListingSchema.index({ 'location.lng': 1 })
ListingSchema.index({ 'availableBedRooms.bedroomSize': 1 })
ListingSchema.index({ 'availableBedRooms.bedroomFurnished': 1 })
ListingSchema.index({
  listed: 1,
  listedAt: -1,
  monthlyRentAmount: 1,
  availabilityStartDate: 1
})
ListingSchema.index({
  listed: 1,
  availabilityStartDate: 1,
  availabilityEndDate: 1
})
ListingSchema.index({
  listed: 1,
  'location.cityPlaceId': 1,
  listedAt: -1,
  id: 1
})
//Listing filters start
ListingSchema.index({
  listed: 1,
  'location.cityPlaceId': 1,
  listingTypeId: 1,
  listedAt: -1,
  baseMonthlyRentAmount: 1,
  availabilityStartDate: 1,
  placeSize: 1
})
ListingSchema.index({
  listed: 1,
  listingTypeId: 1,
  'location.lat': 1,
  'location.lng': 1,
  baseMonthlyRentAmount: 1,
  availabilityStartDate: 1,
  placeSize: 1
})
ListingSchema.index({
  listed: 1,
  listingTypeId: 1,
  'location.lat': 1,
  'location.lng': 1,
  baseMonthlyRentAmount: 1,
  placeSize: 1
})
ListingSchema.index({
  _id: 1,
  'location.cityPlaceId': 1
})
ListingSchema.index({
  listed: 1,
  'location.cityPlaceId': 1,
  listingTypeId: 1,
  availabilityStartDate: 1,
  placeSize: 1
})
ListingSchema.index({
  listed: 1,
  listingTypeId: 1,
  listedAt: -1,
  'location.lat': 1,
  'location.lng': 1,
  baseMonthlyRentAmount: 1,
  availabilityStartDate: 1,
  placeSize: 1,
  noOfAvailableBedrooms: 1
})
//End of listing filter
ListingSchema.index({
  'tempUser.id': 1
})
ListingSchema.index({
  listed: 1,
  placeSize: 1
})
ListingSchema.index({
  ownerId: 1,
  availabilityStartDate: 1
})
ListingSchema.index({
  _id: 1,
  ownerId: 1
})
ListingSchema.index({
  'addons.productServiceId': 1
})
ListingSchema.index({
  partnerId: 1,
  placeSize: 1
})
ListingSchema.index({
  listed: 1,
  baseMonthlyRentAmount: 1,
  placeSize: 1,
  deleted: 1
})
ListingSchema.index({
  listed: 1,
  createdAt: -1
})
ListingSchema.index({
  partnerId: 1,
  propertyStatus: 1,
  listingTypeId: 1,
  placeSize: 1
})
ListingSchema.index({
  listed: 1,
  listedAt: -1,
  'location.lat': 1,
  'location.lng': 1,
  baseMonthlyRentAmount: 1,
  availabilityStartDate: 1,
  placeSize: 1,
  deleted: 1
})
