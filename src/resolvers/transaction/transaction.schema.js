import mongoose from 'mongoose'
import validator from 'validator'
import { Id, Message } from '../common'

const xledgerSchema = new mongoose.Schema(
  {
    creditTrDbId: {
      type: String
    },
    debitTrDbId: {
      type: String
    },
    hasError: {
      type: Boolean
    },
    syncedAt: {
      type: Date
    },
    xledgerLogId: {
      type: String
    }
  },
  { _id: false }
)
const PowerOfficeSchema = new mongoose.Schema(
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
    powerOfficeLogId: {
      type: String
    },
    syncedAt: {
      type: Date
    }
  },
  { _id: false }
)

export const TransactionSchema = new mongoose.Schema(
  [
    Id,
    {
      type: {
        type: String,
        enum: [
          'invoice',
          'credit_note',
          'payment',
          'refund',
          'commission',
          'payout',
          'correction'
        ]
      },
      subType: {
        type: String, // Should be same as accounting schema
        enum: [
          'rent',
          'rent_with_vat',
          'addon',
          'brokering_commission',
          'management_commission',
          'addon_commission',
          'payout_addon',
          'rounded_amount',
          'invoice_fee',
          'invoice_reminder_fee',
          'collection_notice_fee',
          'loss_recognition',
          'eviction_notice_fee',
          'administration_eviction_notice_fee',
          'reminder_fee_move_to',
          'collection_notice_fee_move_to',
          'eviction_notice_fee_move_to',
          'administration_eviction_notice_fee_move_to',
          'unpaid_reminder',
          'unpaid_collection_notice',
          'unpaid_eviction_notice',
          'unpaid_administration_eviction_notice',
          'rent_payment',
          'final_settlement_payment',
          'payout_to_landlords'
        ]
      },
      createdBy: {
        type: String,

        default: 'SYSTEM'
      },
      partnerId: {
        type: String,
        index: true
      },
      invoiceId: {
        type: String,
        index: true
      },
      landlordInvoiceId: {
        type: String,
        index: true
      },
      contractId: {
        type: String,
        index: true
      },
      paymentId: {
        type: String,
        index: true
      },
      payoutId: {
        type: String,
        index: true
      },
      commissionId: {
        type: String,
        index: true
      },
      agentId: {
        type: String,
        index: true
      },
      tenantId: {
        type: String,
        index: true
      },
      branchId: {
        type: String,
        index: true
      },
      accountId: {
        type: String,
        index: true
      },
      propertyId: {
        type: String,
        index: true
      },
      amount: {
        type: Number,
        required: true
      },
      amountExclTax: {
        type: Number
      },
      amountTotalTax: {
        type: Number
      },
      debitAccountId: {
        type: String,
        index: true
      },
      debitAccountCode: {
        type: Number
      },
      creditAccountId: {
        type: String,
        optional: true
      },
      creditAccountCode: {
        type: Number
      },
      debitTaxCodeId: {
        type: String,
        index: true
      },
      debitTaxCode: {
        type: Number
      },
      debitTaxPercentage: {
        type: Number
      },
      creditTaxCodeId: {
        type: String,
        index: true
      },
      creditTaxCode: {
        type: Number
      },
      creditTaxPercentage: {
        type: Number
      },
      addonId: {
        type: String,
        index: true
      },
      correctionId: {
        type: String,
        index: true
      },
      assignmentNumber: {
        type: String
      },
      agentName: {
        type: String
      },
      accountName: {
        type: String
      },
      tenantName: {
        type: String
      },
      locationName: {
        type: String
      },
      branchSerialId: {
        type: String
      },
      internalAssignmentId: {
        type: String
      },
      internalLeaseId: {
        type: String
      },
      employeeId: {
        type: String
      },
      invoiceSerialId: {
        type: String
      },
      landlordInvoiceSerialId: {
        type: String
      },
      payoutSerialId: {
        type: String
      },
      correctionSerialId: {
        type: String
      },
      addonName: {
        type: String
      },
      companyName: {
        type: String
      },
      isCreditNoteAddon: {
        type: Boolean
      },
      bankAccountNumber: {
        type: String
      },
      period: {
        type: String
      },
      status: {
        type: String,
        enum: ['EXPORTED', 'ERROR']
      },
      serialId: {
        type: Number
      },
      kidNumber: {
        type: String
      },
      invoiceDueDate: {
        type: Date
      },
      accountSerialId: {
        type: String
      },
      tenantSerialId: {
        type: String
      },
      propertySerialId: {
        type: String
      },
      apartmentId: {
        type: String
      },
      tenantAddress: {
        type: String
      },
      tenantPhoneNumber: {
        type: String
      },
      tenantEmailAddress: {
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
      subName: {
        type: String
      },
      bankRef: {
        type: String
      },
      landlordPayment: {
        type: Boolean
      },
      finalSettlementSerialId: {
        type: String
      },
      accountAddress: {
        type: String
      },
      accountZipCode: {
        type: String
      },
      accountCity: {
        type: String
      },
      accountCountry: {
        type: String
      },
      tenantZipCode: {
        type: String
      },
      tenantCity: {
        type: String
      },
      tenantCountry: {
        type: String
      },
      locationZipCode: {
        type: String
      },
      locationCity: {
        type: String
      },
      locationCountry: {
        type: String
      },
      powerOffice: {
        type: PowerOfficeSchema
      },
      externalEntityId: {
        type: String
      },
      isUpdatedListingAddress: {
        type: Boolean
      },
      backupTenantAddress: {
        type: String
      },
      propertyGroupId: {
        type: String
      },
      createdAt: {
        type: Date,
        index: true,
        default: new Date()
      },
      isAddedFromUpgradeScript: {
        type: Boolean
      },
      previousMeta: {
        type: Object
      },
      xledger: {
        type: xledgerSchema,
        default: undefined
      }
    }
  ],
  {
    versionKey: false,
    toJSON: { virtuals: true }
  }
)
TransactionSchema.index({
  invoiceId: 1,
  partnerId: 1,
  type: 1,
  subType: 1
})
TransactionSchema.index({
  type: 1,
  partnerId: 1
})
TransactionSchema.index({
  partnerId: 1,
  createdAt: 1
})
TransactionSchema.index({
  partnerId: 1,
  type: 1,
  createdAt: 1
})

TransactionSchema.index({
  partnerId: 1,
  type: 1,
  subType: 1,
  createdAt: 1
})

TransactionSchema.index({
  type: 1,
  subType: 1
})

TransactionSchema.index(
  { type: 1, period: 1, partnerId: 1 },
  {
    name: 'type_1_period_1_partnerId_1'
  }
)
