import mongoose from 'mongoose'
import { CreatedBySchemas, TenantsIdSchemas, Id, Message } from '../common'

export const AddonsSchemas = new mongoose.Schema(
  {
    isRecurring: {
      type: Boolean
    },
    hasCommission: {
      type: Boolean
    },
    type: {
      type: String,
      enum: ['lease', 'assignment']
    },
    addonId: {
      type: String,
      required: true
    },
    tax: {
      type: Number
    },
    taxPercentage: {
      type: Number
    },
    price: {
      type: Number
    },
    total: {
      type: Number
    },
    productServiceId: {
      type: String
    }
  },
  { _id: false }
)

const estimatedPayoutsSchemas = new mongoose.Schema(
  {
    firstMonth: {
      type: Number,
      min: 0
    },
    secondMonth: {
      type: Number,
      min: 0
    },
    thirdMonth: {
      type: Number,
      min: 0
    },
    firstMonthManagementCommission: {
      type: Number
    },
    secondMonthManagementCommission: {
      type: Number
    },
    thirdMonthManagementCommission: {
      type: Number
    },
    firstMonthPayoutAddons: {
      type: Number
    },
    secondMonthPayoutAddons: {
      type: Number
    },
    thirdMonthPayoutAddons: {
      type: Number
    },
    firstMonthAddonsCommission: {
      type: Number
    },
    secondMonthAddonsCommission: {
      type: Number
    },
    thirdMonthAddonsCommission: {
      type: Number
    },
    secondAmountMovedFromLastPayout: {
      type: Number
    },
    thirdAmountMovedFromLastPayout: {
      type: Number
    },
    firstRentInvoice: {
      type: Number
    },
    secondRentInvoice: {
      type: Number
    },
    thirdRentInvoice: {
      type: Number
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
    },
    attachmentPadesFileCreatedAt: {
      type: Date
    },
    hasAttachmentPadesFile: {
      type: Boolean
    },
    idfyAttachmentId: {
      type: String
    },
    isSentDepositDataToBank: {
      type: Boolean
    },
    attachmentFileId: {
      type: String
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

const ContractMetaSchemas = new mongoose.Schema(
  {
    status: {
      type: String,
      enum: ['new', 'in_progress', 'signed', 'upcoming', 'active', 'closed'],
      default: 'new'
    },
    eSignReminderToTenantForLeaseSendAt: {
      type: Date
    },
    eSignReminderToLandlordForLeaseSendAt: {
      type: Date
    },
    createdAt: {
      type: Date
    },
    createdBy: {
      type: String
    },
    tenantId: {
      type: String
    },
    contractStartDate: {
      type: Date
    },
    contractEndDate: {
      type: Date
    },
    minimumStay: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    monthlyRentAmount: {
      type: Number
    },
    depositAmount: {
      type: Number,
      min: 0
    },
    depositInsuranceAmount: {
      type: Number,
      min: 0
    },
    firstInvoiceDueDate: {
      type: Date
    },
    dueDate: {
      type: Number
    },
    movingInDate: {
      type: Date
    },
    isMovedIn: {
      type: Boolean
    },
    fileIds: {
      type: [String],
      default: undefined
    },
    noticePeriod: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    noticeInEffect: {
      type: String
    },
    terminateReasons: {
      type: String
    },
    terminateComments: {
      type: String
    },
    invoiceAccountNumber: {
      type: String
    },
    cancelled: {
      type: Boolean
    },
    cancelledBy: {
      type: String
    },
    cancelledAt: {
      type: Date
    },
    addons: {
      type: [AddonsSchemas],
      default: undefined
    },
    leaseSerial: {
      type: Number,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    invoiceFrequency: {
      type: Number
    },
    invoiceStartFrom: {
      type: Date
    },
    invoicedAsOn: {
      // We need this, when we will create second invoice
      type: Date
    },
    creditWholeInvoice: {
      type: Boolean
    },
    signedAt: {
      type: Date
    },
    cpiEnabled: {
      type: Boolean
    },
    lastCpiDate: {
      type: Date
    },
    nextCpiDate: {
      type: Date
    },
    terminatedBy: {
      type: String,
      enum: ['landlord', 'tenant']
    },
    enabledNotification: {
      type: Boolean
    },
    enabledJointlyLiable: {
      type: Boolean
    },
    disableVipps: {
      type: Boolean
    },
    soonTerminatedNoticeSendDate: {
      type: Date
    },
    naturalTerminatedNoticeSendDate: {
      type: Date
    },
    lastCPINotificationSentOn: {
      type: Date
    },
    futureRentAmount: {
      type: Number
    },
    cpiNotificationSentHistory: {
      type: [Date],
      default: undefined
    },
    invoiceCalculation: {
      type: String
    },
    internalLeaseId: {
      type: String
    },
    tenants: {
      type: [TenantsIdSchemas],
      default: undefined
    },
    isVatEnable: {
      type: Boolean
    },
    estimatedPayouts: {
      type: estimatedPayoutsSchemas
    },
    cpiFromMonth: {
      type: Date
    },
    cpiInMonth: {
      type: Date
    },
    leaseSigningComplete: {
      type: Boolean
    },
    landlordLeaseSigningStatus: {
      type: landlordSigningStatusSchema
    },
    tenantLeaseSigningStatus: {
      type: [tenantSigningStatusSchema],
      default: undefined
    },
    enabledLeaseEsigning: {
      type: Boolean
    },
    leaseSigningMeta: {
      type: signingMetaSchema
    },
    leaseSignatureMechanism: {
      type: String
    },
    isEsignReminderSentToLandlordForLease: {
      type: Boolean
    },
    isEsignReminderSentToTenantForLease: {
      type: Boolean
    },
    enabledAnnualStatement: {
      type: Boolean
    },
    templateId: {
      type: String
    },
    hasLeaseSignerXmlFileInS3: {
      type: Boolean
    },
    leaseSignerXmlInS3At: {
      type: Date
    },

    leasePdfGenerated: {
      type: Boolean
    },
    hasLeasePadesFile: {
      type: Boolean
    },
    leasePadesFileCreatedAt: {
      type: Date
    },
    idfyErrorsForLease: {
      type: [Object],
      default: undefined
    },
    isSendEsignNotify: {
      type: Boolean
    },
    enabledDepositAccount: {
      type: Boolean
    },
    hasSignersAttachmentPadesFile: {
      type: Boolean
    },
    enabledJointDepositAccount: {
      type: Boolean
    },
    depositAccountError: {
      type: [Object],
      default: undefined
    },
    depositType: {
      type: String,
      enum: ['deposit_insurance', 'deposit_account', 'no_deposit']
    },
    depositInsuranceId: {
      type: String
    },
    leaseWelcomeEmailSentAt: {
      type: Date
    },
    leaseWelcomeEmailSentInProgress: {
      type: Boolean
    },
    isEnabledRecurringDueDate: {
      type: Boolean
    },
    disableCompello: {
      type: Boolean
    },
    isDepositAccountCreationTestProcessing: {
      type: Boolean,
      default: false
    },
    isDepositAccountPaymentTestProcessing: {
      type: Boolean,
      default: false
    }
  },
  { _id: false }
)

const listingInfoSchemas = new mongoose.Schema(
  {
    availabilityStartDate: {
      type: Date,
      required: true
    },
    availabilityEndDate: {
      type: Date
    },
    minimumStay: {
      type: Number,
      min: 0,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    monthlyRentAmount: {
      type: Number,
      required: true,
      min: 0
    },
    depositAmount: {
      type: Number,
      min: 0
    }
  },
  { _id: false }
)

const DueCommissionSchemas = new mongoose.Schema(
  {
    dueBrokeringCommission: {
      type: Number
    },
    dueRentalManagementCommission: {
      type: Number
    }
  },
  { _id: false }
)

const PayoutMonthMetaSchemas = new mongoose.Schema(
  {
    payoutId: {
      type: String
    },
    invoiceId: {
      type: String
    },
    invoiceSummaryId: {
      type: String
    },
    payoutAt: {
      type: Date
    }
  },
  { _id: false }
)

const filesSchemas = new mongoose.Schema(
  {
    fileId: {
      type: String
    },
    context: {
      type: String
    },
    serialId: {
      type: String
    }
  },
  { _id: false }
)

const AnnualStatementMetaSchemas = new mongoose.Schema(
  {
    id: {
      type: String
    },
    fileId: {
      type: String
    },
    startDate: {
      type: Date
    },
    endDate: {
      type: Date
    },
    status: {
      type: String,
      enum: ['created', 'failed', 'completed']
    },
    reason: {
      type: String
    },
    createdAt: {
      type: Date
    }
  },
  { _id: false }
)

const historySchema = new mongoose.Schema(
  {
    name: {
      type: String
    },
    oldValue: {
      type: String
    },
    newValue: {
      type: String
    },
    oldUpdatedAt: {
      type: Date
    },
    newUpdatedAt: {
      type: Date
    }
  },
  { _id: false }
)

const evictionCasesSchema = new mongoose.Schema(
  {
    evictionInvoiceIds: {
      type: [String],
      default: undefined
    },
    invoiceId: {
      type: String
    },
    status: {
      type: String,
      enum: ['new', 'in_progress', 'completed', 'canceled']
    },
    leaseSerial: {
      type: Number,
      min: 1,
      validate: {
        validator: Number.isInteger,
        message: Message.integerError
      }
    },
    tenants: {
      type: [TenantsIdSchemas],
      default: undefined
    },
    agentId: {
      type: String
    },
    tenantId: {
      type: String
    },
    hasPaid: {
      type: Boolean
    },
    contractStartDate: {
      type: Date
    },
    contractEndDate: {
      type: Date
    },
    firstInvoiceDueDate: {
      type: Date
    },
    dueDate: {
      type: Number
    },
    amount: {
      type: Number
    }
  },
  { _id: false }
)

export const ContractSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
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
        index: true,
        required: true
      },
      accountId: {
        type: String,
        index: true,
        required: true
      },
      propertyId: {
        type: String,
        index: true,
        required: true
      },
      files: {
        type: [filesSchemas],
        default: undefined
      },
      status: {
        type: String,
        enum: ['new', 'in_progress', 'upcoming', 'active', 'closed']
      },
      hasBrokeringContract: {
        type: Boolean,
        index: true,
        required: true
      },
      brokeringCommissionType: {
        type: String,
        enum: ['fixed', 'percent']
      },
      brokeringCommissionAmount: {
        type: Number
      },
      hasRentalManagementContract: {
        type: Boolean,
        index: true,
        required: true
      },
      rentalManagementCommissionType: {
        type: String,
        enum: ['fixed', 'percent']
      },
      rentalManagementCommissionAmount: {
        type: Number
      },
      hasRentalContract: {
        type: Boolean,
        index: true,
        required: true
      },
      rentalCommission: {
        type: Number
      },
      dueCommission: {
        type: DueCommissionSchemas
      },
      assignmentESigningReminderToLandlordSentAt: {
        type: Date
      },
      brokeringMeta: {
        type: ContractMetaSchemas
      },
      rentalManagementMeta: {
        type: ContractMetaSchemas
      },
      rentalMeta: {
        type: ContractMetaSchemas
      },
      listingInfo: {
        type: listingInfoSchemas
      },
      noOfPayoutMonth: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      payoutTo: {
        type: String // Account no.
      },
      addons: {
        type: [AddonsSchemas],
        default: undefined
      },
      assignmentSerial: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      leaseSerial: {
        type: Number,
        index: true,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      isDefaulted: {
        type: Boolean
      },
      cancelledBy: {
        type: String
      },
      cancelledAt: {
        type: Date
      },
      payoutMonthMeta: {
        type: [PayoutMonthMetaSchemas],
        default: undefined
      },
      rentalMetaHistory: {
        type: [ContractMetaSchemas],
        default: undefined
      },
      monthlyPayoutDate: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      internalAssignmentId: {
        type: String
      },
      assignmentFrom: {
        type: Date
      },
      assignmentTo: {
        type: Date
      },
      representativeId: {
        type: String
      },
      signDate: {
        type: Date
      },
      terminatedByUserId: {
        type: String
      },
      enabledEsigning: {
        type: Boolean
      },
      assignmentContractPdfGenerated: {
        type: Boolean
      },
      leaseContractPdfGenerated: {
        type: Boolean
      },
      idfyAssignmentDocId: {
        type: String
      },
      idfyLeaseDocId: {
        type: String
      },
      landlordAssignmentSigningStatus: {
        type: landlordSigningStatusSchema
      },
      agentAssignmentSigningStatus: {
        type: agentSigningStatusSchema
      },
      assignmentSigningMeta: {
        type: signingMetaSchema
      },
      draftAssignmentDoc: {
        type: Boolean
      },
      draftLeaseDoc: {
        type: Boolean
      },
      holdPayout: {
        type: Boolean
      },
      finalSettlementStatus: {
        type: String,
        enum: ['new', 'in_progress', 'completed']
      },
      assignmentSignatureMechanism: {
        type: String
      },
      isEsignReminderSentToLandlordForAssint: {
        type: Boolean
      },
      statementMeta: {
        type: [AnnualStatementMetaSchemas],
        default: undefined
      },
      templateId: {
        type: String
      },
      isFinalSettlementDone: {
        type: Boolean
      },
      hasAssignmentSignerXmlFileInS3: {
        type: Boolean
      },
      assignmentSignerXmlFileInS3At: {
        type: Date
      },
      hasAssignmentPadesFile: {
        type: Boolean
      },
      assignmentPadesFileCreatedAt: {
        type: Date
      },
      idfyErrorsForAssingment: {
        type: [Object],
        default: undefined
      },
      isSendAssignmentPdf: {
        type: Boolean
      },
      assignmentPdfGenerated: {
        type: Boolean
      },
      evictionCases: {
        type: [evictionCasesSchema],
        default: undefined
      },
      history: {
        type: [historySchema],
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

ContractSchema.index({ createdAt: 1 })
ContractSchema.index({ updatedAt: 1 })
ContractSchema.index({ 'rentalMeta.movingInDate': 1 })
ContractSchema.index({ 'rentalMeta.createdAt': 1 })
ContractSchema.index({ 'rentalMeta.createdBy': 1 })
ContractSchema.index({ 'rentalMeta.tenantId': 1 })
ContractSchema.index({ 'rentalMeta.tenants.tenantId': 1 })
ContractSchema.index({ 'rentalMeta.contractStartDate': 1 })
ContractSchema.index({ 'rentalMeta.contractEndDate': 1 })
ContractSchema.index({ 'rentalMeta.minimumStay': 1 })
ContractSchema.index({ 'rentalMeta.monthlyRentAmount': 1 })
ContractSchema.index({ 'rentalMeta.depositAmount': 1 })
ContractSchema.index({ 'rentalMeta.invoiceStartFrom': 1 })
ContractSchema.index({ 'rentalMeta.dueDate': 1 })
ContractSchema.index({ 'rentalMeta.movingInDate': 1 })
ContractSchema.index({ 'rentalMeta.isMovedIn': 1 })
ContractSchema.index({ 'rentalMeta.status': 1 })
ContractSchema.index({ 'listingInfo.availabilityStartDate': 1 })
ContractSchema.index({ 'listingInfo.availabilityEndDate': 1 })
ContractSchema.index({ 'listingInfo.minimumStay': 1 })
ContractSchema.index({ 'listingInfo.monthlyRentAmount': 1 })
ContractSchema.index({ 'listingInfo.depositAmount': 1 })
ContractSchema.index({ 'addons.productServiceId': 1 })
ContractSchema.index({ propertyId: 1, partnerId: 1 })
ContractSchema.index({ partnerId: 1, _id: 1 })
ContractSchema.index({ _id: 1, partnerId: 1, 'rentalMeta.enabledEsigning': 1 })
ContractSchema.index({ _id: 1, partnerId: 1, enabledEsigning: 1 })
ContractSchema.index({
  status: 1,
  'rentalMeta.cpiEnabled': 1,
  'rentalMeta.lastCPINotificationSentOn': 1
})
ContractSchema.index({
  status: 1,
  'rentalMeta.status': 1,
  'rentalMeta.naturalTerminatedNoticeSendDate': 1,
  'rentalMeta.contractEndDate': 1
})
