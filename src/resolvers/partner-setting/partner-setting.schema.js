import mongoose from 'mongoose'
import validator from 'validator'
import { CreatedBySchemas, Id, Message } from '../common'

const PartnerCompanyInfoSchema = new mongoose.Schema(
  {
    companyName: {
      type: String
    },
    organizationId: {
      type: String
    },
    officeAddress: {
      type: String
    },
    officeZipCode: {
      type: String
    },
    officeCity: {
      type: String
    },
    officeCountry: {
      type: String
    },
    postalAddress: {
      type: String
    },
    postalZipCode: {
      type: String,
      optional: true
    },
    postalCity: {
      type: String
    },
    postalCountry: {
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
    },
    isLogoLinkedToWebsite: { type: Boolean },
    backupOfficeAddress: {
      type: String
    },
    backupPostalAddress: {
      type: String
    },
    lastUpdate: {
      type: Date
    }
  },
  { _id: false }
)

const PartnerBankInfoSchema = new mongoose.Schema(
  {
    id: {
      type: String
    },
    accountNumber: {
      type: String
    },
    bic: {
      type: String
    },
    vatRegistered: {
      type: Boolean
    },
    orgName: {
      type: String
    },
    orgId: {
      type: String
    },
    orgAddress: {
      type: String
    },
    orgZipCode: {
      type: String
    },
    orgCity: {
      type: String
    },
    orgCountry: {
      type: String
    },
    ledgerAccountId: {
      type: String
    },
    backupOrgAddress: {
      type: String
    }
  },
  { _id: false }
)

const CurrencySettingsSchema = new mongoose.Schema(
  {
    decimalSeparator: {
      type: String
    },
    thousandSeparator: {
      type: String
    },
    numberOfDecimal: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    currencySymbol: {
      type: String
    },
    currencyPosition: {
      type: String
    }
  },
  { _id: false }
)

const SmsSettingsSchema = new mongoose.Schema(
  {
    smsSenderName: {
      type: String
    }
  },
  { _id: false }
)

const DatetimeSettingsSchema = new mongoose.Schema(
  {
    dateFormat: {
      type: String
    },
    timeFormat: {
      type: String
    },
    timezone: {
      type: String
    }
  },
  { _id: false }
)

const PropertySettingsSchema = new mongoose.Schema(
  {
    soonEndingMonths: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    movingInOutProtocol: {
      type: Boolean
    },
    enabledMoveInEsignReminder: {
      type: Boolean
    },
    esignReminderNoticeDaysForMoveIn: {
      type: Number,
      optional: true
    },
    enabledMoveOutEsignReminder: {
      type: Boolean
    },
    esignReminderNoticeDaysForMoveOut: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    enabledGroupId: {
      type: Boolean
    }
  },
  { _id: false }
)

const AssignmentSettingsSchema = new mongoose.Schema(
  {
    internalAssignmentId: {
      type: Boolean
    },
    enableEsignAssignment: {
      type: Boolean
    },
    enabledAssignmentEsignReminder: {
      type: Boolean
    },
    esignReminderNoticeDays: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    enabledShowAssignmentFilesToLandlord: {
      type: Boolean
    }
  },
  { _id: false }
)

const naturalLeaseTerminationSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    days: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const LeaseSettingsSchema = new mongoose.Schema(
  {
    internalLeaseId: {
      type: Boolean
    },
    enableEsignLease: {
      type: Boolean
    },
    naturalLeaseTermination: {
      type: naturalLeaseTerminationSchema
    },
    esignReminderNoticeDays: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    enabledLeaseESigningReminder: {
      type: Boolean
    },
    enabledShowLeaseFilesToTenant: {
      type: Boolean
    },
    depositType: {
      type: String,
      optional: true
    }
  },
  { _id: false }
)

// Disabled listing
const disabledListingSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    days: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

// RemoveProspect
const RemoveProspectsSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    months: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const DeleteInterestFormSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    months: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const RemoveCreditRatingSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    months: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const AutomaticCreditRatingSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    }
  },
  { _id: false }
)

const ListingSettingsSchema = new mongoose.Schema(
  {
    disabledListing: {
      type: disabledListingSchema
    }
  },
  { _id: false }
)

const TenantSettingSchema = new mongoose.Schema(
  {
    removeProspects: {
      type: RemoveProspectsSchema
    },
    deleteInterestForm: {
      type: DeleteInterestFormSchema
    },
    removeCreditRating: {
      type: RemoveCreditRatingSchema
    },
    automaticCreditRating: {
      type: AutomaticCreditRatingSchema
    }
  },
  { _id: false }
)

const PartnerInvoiceReminderTypes = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    days: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    newDueDays: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const DefaultFindHomeLocationSchema = new mongoose.Schema(
  {
    defaultMapLocation: {
      type: String
    },
    defaultMapZoom: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    lat: {
      type: Number
    },
    lng: {
      type: Number
    },
    cityPlaceId: {
      type: String
    }
  },
  { _id: false }
)

const DirectRemittanceApprovalSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    categoryPurposeCode: {
      type: String,
      enum: ['OTHR', 'SALA']
    },
    persons: {
      type: [String]
    },
    isEnableMultipleSigning: {
      type: Boolean
    }
  },
  { _id: false }
)

const EvictionNoticeSettingSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    days: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    isCreateEvictionPackage: {
      type: Boolean
    },
    requiredTotalOverDue: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const TenantPaysAllDueDuringEvictionSchema = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    }
  },
  { _id: false }
)

const PartnerInvoiceFeeTypes = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    amount: {
      type: Number
    },
    tax: {
      type: Number
    }
  },
  { _id: false }
)

const PartnerPayoutTerm = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    payBeforeMonth: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    days: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const PartnerBankPaymentTypes = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    firstMonthACNo: {
      type: String // First month account number
    },
    afterFirstMonthACNo: {
      type: String // After first month account number
    }
  },
  { _id: false }
)

const PartnerCPISettlementTypes = new mongoose.Schema(
  {
    enabled: {
      type: Boolean
    },
    months: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const PartnerNotifications = new mongoose.Schema(
  {
    leaseTerminated: {
      type: Boolean
    },
    creditNote: {
      type: Boolean
    },
    sentAssignment: {
      type: Boolean
    },
    sentWelcomeLease: {
      type: Boolean
    },
    leaseTerminatedByTenant: {
      type: Boolean
    },
    leaseTerminatedByLandlord: {
      type: Boolean
    },
    leaseScheduleTerminatedByTenant: {
      type: Boolean
    },
    leaseScheduleTerminatedByLandlord: {
      type: Boolean
    },
    soonEndingLease: {
      type: Boolean
    },
    naturalLeaseTermination: {
      type: Boolean
    },
    nextScheduledPayouts: {
      type: Boolean
    },
    landlordCreditNote: {
      type: Boolean
    },
    finalSettlementInvoice: {
      type: Boolean
    },
    annualStatement: {
      type: Boolean
    },
    interestForm: {
      type: Boolean,
      optional: true
    },
    depositAccount: {
      type: Boolean
    },
    depositIncomingPayment: {
      type: Boolean
    },
    taskNotification: {
      type: Boolean
    },
    appHealthNotification: {
      type: Boolean
    },
    wrongSSNNotification: {
      type: Boolean
    },
    depositInsurance: {
      type: Boolean
    }
  },
  { _id: false }
)

const PartnerInvoiceSettingsSchema = new mongoose.Schema(
  {
    enabledCompelloRegninger: {
      type: Boolean
    },
    enabledVippsRegninger: {
      type: Boolean
    },
    numberOfDecimalInInvoice: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    }
  },
  { _id: false }
)

const DepositInsuranceSettingSchema = new mongoose.Schema(
  {
    paymentReminder: {
      enabled: {
        type: Boolean
      },
      days: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      }
    }
  },
  { _id: false }
)

export const PartnerSettingSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true
      },
      companyInfo: {
        type: PartnerCompanyInfoSchema
      },
      bankAccounts: {
        type: [PartnerBankInfoSchema],
        default: undefined
      },
      currencySettings: {
        type: CurrencySettingsSchema
      },
      smsSettings: {
        type: SmsSettingsSchema
      },
      dateTimeSettings: {
        type: DatetimeSettingsSchema
      },
      propertySettings: {
        type: PropertySettingsSchema
      },
      assignmentSettings: {
        type: AssignmentSettingsSchema
      },
      leaseSetting: {
        type: LeaseSettingsSchema
      },
      depositInsuranceSetting: {
        type: DepositInsuranceSettingSchema,
        default: undefined
      },
      bankPayment: {
        type: PartnerBankPaymentTypes
      },
      invoiceFee: {
        type: PartnerInvoiceFeeTypes
      },
      reminderFee: {
        type: PartnerInvoiceFeeTypes
      },
      evictionFee: {
        type: PartnerInvoiceFeeTypes
      },
      administrationEvictionFee: {
        type: PartnerInvoiceFeeTypes
      },
      collectionNoticeFee: {
        type: PartnerInvoiceFeeTypes
      },
      postalFee: {
        type: PartnerInvoiceFeeTypes
      },
      enableDepositInsurance: {
        type: Boolean // Use uninte's landlord insurance including deposit?
      },
      invoiceCalculation: {
        type: String
      },
      invoiceDueDays: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      duePreReminder: {
        type: PartnerInvoiceReminderTypes
      },
      invoiceFirstReminder: {
        type: PartnerInvoiceReminderTypes
      },
      evictionNotice: {
        type: EvictionNoticeSettingSchema
      },
      evictionReminderNotice: {
        type: EvictionNoticeSettingSchema
      },
      evictionDueReminderNotice: {
        type: EvictionNoticeSettingSchema
      },
      tenantPaysAllDueDuringEviction: {
        type: TenantPaysAllDueDuringEvictionSchema
      },
      invoiceSecondReminder: {
        type: PartnerInvoiceReminderTypes
      },
      invoiceCollectionNotice: {
        type: PartnerInvoiceReminderTypes
      },
      standardPayoutDate: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      customPayoutDays: {
        type: PartnerPayoutTerm
      },
      payout: {
        type: PartnerPayoutTerm
      },
      country: {
        type: String // NO, US... country code
      },
      lastBankReference: {
        type: String
      },
      sameKIDNumber: {
        type: Boolean
      },
      notifications: {
        type: PartnerNotifications
      },
      retryFailedPayouts: {
        type: PartnerPayoutTerm
      },
      CPISettlement: {
        type: PartnerCPISettlementTypes
      },
      invoiceSettings: {
        type: PartnerInvoiceSettingsSchema
      },
      listingSetting: {
        type: ListingSettingsSchema
      },
      tenantSetting: {
        type: TenantSettingSchema
      },
      landlordBankPayment: {
        type: PartnerBankPaymentTypes
      },
      landlordInvoiceFee: {
        type: PartnerInvoiceFeeTypes
      },
      landlordReminderFee: {
        type: PartnerInvoiceFeeTypes
      },
      landlordCollectionNoticeFee: {
        type: PartnerInvoiceFeeTypes
      },
      landlordPostalFee: {
        type: PartnerInvoiceFeeTypes
      },
      landlordInvoiceDueDays: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      landlordDuePreReminder: {
        type: PartnerInvoiceReminderTypes
      },
      landlordInvoiceFirstReminder: {
        type: PartnerInvoiceReminderTypes
      },
      landlordInvoiceSecondReminder: {
        type: PartnerInvoiceReminderTypes
      },
      landlordInvoiceCollectionNotice: {
        type: PartnerInvoiceReminderTypes
      },
      enabledPowerOfficeIntegration: {
        type: Boolean
      },
      defaultFindHomeLocation: {
        type: DefaultFindHomeLocationSchema
      },
      isAllAccountSynced: {
        type: Boolean
      },
      isAllTenantSynced: {
        type: Boolean
      },
      isUpdatedAddress: {
        type: Boolean
      },
      allowedDomains: {
        type: [String],
        default: undefined
      },
      stopCPIRegulation: {
        type: Boolean
      },
      directRemittanceApproval: {
        type: DirectRemittanceApprovalSchema
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

PartnerSettingSchema.index({ createdAt: 1 })
