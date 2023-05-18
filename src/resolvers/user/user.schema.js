import { isUndefined } from 'lodash'
import mongoose from 'mongoose'
import validator from 'validator'
import { Id, Message } from '../common'

function minValidation(value) {
  if (!value) {
    return true
  }
  return !(value.length >= 1 && value.length < 3)
}

function maxValidation(value) {
  if (!value) {
    return true
  }
  return !(value.length > 9)
}

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
    previousLandlordEmail: {
      type: String
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
    },
    fileIds: {
      type: [String],
      default: undefined
    }
  },
  { _id: false }
)

const UserProfileSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      index: true
    },
    nameUpdatedAt: {
      type: Date
    },
    gender: {
      type: String,
      enum: ['male', 'female', 'others'],
      index: true
    },
    birthday: {
      type: Date,
      index: true
    },
    partying: {
      type: String
    },
    keepingSpace: {
      type: String
    },
    phoneNumber: {
      type: String
    },
    norwegianNationalIdentification: {
      type: String
    },
    hometown: {
      type: String
    },
    hometownPlaceId: {
      type: String
    },
    occupation: {
      type: String
    },
    language: {
      type: String,
      default: 'no'
    },
    nationality: {
      type: String
    },
    targetCity: {
      type: String
    },
    movingIn: {
      type: Date,
      index: true
    },
    targetCityPlaceId: {
      type: String,
      index: true
    },
    picture: {
      type: Object
    },
    aboutMe: {
      type: String,
      maxLength: 1000
    },
    active: {
      type: Boolean,
      index: true
    },
    maxRent: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    currency: {
      type: String
    },
    maxRentCalculated: {
      type: Number,
      default() {
        if (this.maxRent && this.currency) {
          return this.maxRent
        }
      }
    },
    maxRoommates: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    roomForRent: {
      type: Boolean
    },
    roommatesGender: {
      type: String,
      enum: ['male', 'female', 'all'],
      index: true
    },
    streetAddress: {
      type: String
    },
    personalities: {
      type: [String],
      default: undefined,
      validate: [
        { validator: minValidation, msg: 'Minimum length 3 is required' },
        { validator: maxValidation, msg: 'Max length 9 is allowed' }
      ]
    },
    interests: {
      type: [String],
      default: undefined,
      validate: [
        { validator: minValidation, msg: 'Minimum length 3 is required' },
        { validator: maxValidation, msg: 'Max length 9 is allowed' }
      ]
    },
    hasMatchableFields: {
      type: Boolean,
      index: true
    },
    hasListing: {
      type: Boolean,
      index: true
    },
    onlyLandlord: {
      type: Boolean,
      index: true
    },
    lookingForRoommate: {
      type: Boolean,
      index: true
    },
    reMatching: {
      type: Boolean,
      index: true,
      default() {
        let reMatching = false
        const instance = this
        const fields = [
          'lookingForRoommate',
          'targetCityPlaceId',
          'birthday',
          'gender',
          'roommatesGender',
          'hasMatchableFields'
        ]
        fields.forEach((field) => {
          if (
            !isUndefined(instance[field]) &&
            (field !== 'lookingForRoommate' ||
              (field === 'lookingForRoommate' && instance[field] === true))
          ) {
            reMatching = true
          }
        })
        if (reMatching) {
          return true
        }
      }
    },
    reRanking: {
      type: Boolean,
      index: true,
      default() {
        let ranking = false
        const instance = this
        const fields = [
          'avatarKey',
          'coverKey',
          'aboutMe',
          'hometown',
          'occupation',
          'phoneNumber',
          'personalities',
          'interests',
          'partying',
          'keepingSpace'
        ]
        fields.forEach((field) => {
          if (!isUndefined(instance[field])) {
            ranking = true
          }
        })
        if (instance.reMatching) {
          ranking = false
        }
        if (ranking) {
          return true
        }
      }
    },
    avatarKey: {
      type: String
    },
    cover: {
      type: String
    },
    coverKey: {
      type: String
    },
    settings: {
      type: Object
    },
    roommateGroupId: {
      type: String,
      index: true
    },
    images: {
      type: [String],
      default: undefined
    },
    regCompleted: {
      type: Boolean
    },
    userTypeIsUndefined: {
      type: Boolean
    },
    hotJarIds: {
      type: [String],
      default: undefined
    },
    isDemoUser: {
      type: Boolean
    },
    termsAcceptedOn: {
      type: Date
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
    loginVersion: {
      type: String,
      index: true
    },
    isSmoker: {
      type: Boolean
    },
    hasPets: {
      type: Boolean
    },
    disableMessageNotification: {
      type: Boolean,
      default: false
    },
    organizationNumber: {
      type: String
    }
  },
  { _id: false }
)

const UsersPartnerSchema = new mongoose.Schema(
  {
    partnerId: {
      type: String,
      required: true
    },
    type: {
      type: String,
      enum: ['user', 'account', 'tenant'],
      required: true
    },
    status: {
      type: String,
      enum: ['invited', 'active', 'inactive'],
      required: true
    },
    token: {
      type: String
    },
    expires: {
      type: Date
    },
    employeeId: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false, toJSON: { virtuals: true } }
)

const UserEmailSchema = new mongoose.Schema(
  {
    address: {
      type: String,
      required: true,
      index: true,
      trim: true,
      maxlength: 100,
      validate: [validator.isEmail, 'Email is invalid']
    },
    verified: {
      type: Boolean,
      required: true
    },
    token: {
      type: String
    },
    expires: {
      type: Date
    }
  },
  { _id: false }
)

export const UserSchema = new mongoose.Schema(
  [
    Id,
    {
      emails: {
        type: [UserEmailSchema],
        default: undefined
      },
      profile: {
        type: UserProfileSchema
      },
      services: {
        type: Object
      },
      customerId: {
        type: String
      },
      status: {
        type: Object
      },
      identity: {
        type: Object
      },
      roles: {
        type: [String],
        default: undefined
      },
      favorite: {
        type: [String],
        index: true,
        default: undefined
      },
      registeredAt: {
        type: Date,
        index: true
      },
      partners: {
        type: [UsersPartnerSchema],
        index: true,
        default: undefined
      },
      interestFormMeta: {
        type: InterestFormMetaSchema
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

UserSchema.index({ createdAt: 1 })
UserSchema.index({
  'services.resume.loginTokens.hashedToken': 1
})
UserSchema.index({
  'services.resume.loginTokens.token': 1
})
UserSchema.index({
  'services.resume.haveLoginTokensToDelete': 1
})
UserSchema.index({
  'services.resume.loginTokens.when': 1
})
UserSchema.index({
  'services.password.reset.when': 1
})
UserSchema.index({
  'services.email.verificationTokens.token': 1
})
UserSchema.index({
  'services.password.reset.token': 1
})
UserSchema.index({
  'services.facebook.id': 1
})
UserSchema.index({
  'services.facebook.email': 1
})
UserSchema.index({
  'services.facebook.name': 1
})
UserSchema.index({
  _id: 1,
  'profile.movingIn': 1
})
UserSchema.index({
  'identify.email': 1,
  createdAt: -1
})
UserSchema.index({
  'services.password.reset.reason': 1,
  'services.password.reset.when': 1
})
UserSchema.index({
  'profile.services.facebook': 1
})
UserSchema.index(
  {
    _id: 1,
    'profile.targetCityPlaceId': 1,
    'profile.gender': 1,
    'profile.lookingForRoommate': 1,
    'profile.active': 1,
    'profile.roommatesGender': 1,
    'profile.roommateGroupId': 1,
    'profile.birthday': 1
  },
  { name: 'uninite_users_profile' }
)
UserSchema.index(
  {
    _id: 1,
    'profile.targetCityPlaceId': 1,
    'profile.gender': 1,
    'profile.lookingForRoommate': 1,
    'profile.active': 1,
    'profile.roommatesGender': 1,
    'profile.roomForRent': 1,
    'profile.roommateGroupId': 1,
    'profile.birthday': 1
  },
  { name: 'uninite_users_profile2' }
)
UserSchema.index(
  {
    _id: 1,
    'profile.targetCityPlaceId': 1,
    'profile.lookingForRoommate': 1,
    'profile.active': 1,
    'profile.roommatesGender': 1,
    'profile.roommateGroupId': 1,
    'profile.birthday': 1
  },
  { name: 'user_filters' }
)
UserSchema.index({
  _id: 1,
  'profile.images': 1
})
UserSchema.index({
  'profile.targetCityPlaceId': 1,
  'status.lastLogin.date': 1
})
UserSchema.index({
  roles: 1,
  createdAt: -1,
  'profile.name': 1
})
UserSchema.index({
  roles: 1,
  createdAt: -1,
  'profile.email': 1
})
UserSchema.index({
  _id: 1,
  'services.resume.loginTokens.hashedToken': 1
})
UserSchema.index({
  createdAt: -1,
  'profile.name': 1
})
UserSchema.index({
  createdAt: -1,
  'services.facebook.name': 1
})
UserSchema.index({
  'partners.partnerId': 1,
  'partners.type': 1
})
UserSchema.index({
  'services.password.enroll.when': 1
})
UserSchema.index({
  'services.password.enroll.token': 1
})
