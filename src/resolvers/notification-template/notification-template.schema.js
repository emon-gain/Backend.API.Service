import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const categoryList = [
  'account_wrong_ssn_notification',
  'agent_assignment_esigning',
  'agent_moving_in_esigning',
  'agent_moving_out_esigning',
  'app_health_status_to_app_admins',
  'ask_for_credit_rating',
  'assignment',
  'chat_notification',
  'collection_notice',
  'contact_us',
  'CPI_settlement',
  'credit_note',
  'deposit_account_created',
  'deposit_incoming_payment',
  'deposit_insurance_created',
  'deposit_insurance_e_signing',
  'deposit_insurance_payment_reminder',
  'deposit_insurance_pdf',
  'email_footer',
  'eviction_document',
  'eviction_due_reminder_notice',
  'eviction_due_reminder_notice_without_eviction_fee',
  'eviction_notice',
  'export_to_email',
  'final_settlement',
  'interest_form',
  'interest_form_invitation',
  'invoice',
  'invoice_reminder',
  'landlord_annual_statement',
  'landlord_assignment_esigning',
  'landlord_collection_notice',
  'landlord_credit_note',
  'landlord_invoice',
  'landlord_invoice_reminder',
  'landlord_lease_esigning',
  'landlord_moving_in_esigning',
  'landlord_moving_out_esigning',
  'lease',
  'lease_contract',
  'lease_soon_ending',
  'lease_terminated_landlord',
  'lease_terminated_tenant',
  'lease_termination_scheduled_landlord',
  'lease_termination_scheduled_tenant',
  'moving_in_esigning',
  'moving_out_esigning',
  'natural_lease_termination_notice',
  'next_schedule_payout',
  'otp_email',
  'partner_user_invitation',
  'payout_confirmation',
  'pdf_footer',
  'pending_payments_approval_esigning',
  'pending_payments_esign_pdf',
  'pending_payouts_approval_esigning',
  'pending_payouts_esign_pdf',
  'send_pending_payment_for_approval',
  'send_pending_payout_for_approval',
  'task_notification',
  'tenant_lease_esigning',
  'tenant_moving_in_esigning',
  'tenant_moving_out_esigning',
  'tenant_wrong_ssn_notification',
  'user_change_email',
  'user_email_verification',
  'user_reset_password',
  'welcome_lease_to'
]

const LanguageSubSchema = new mongoose.Schema(
  {
    en: {
      type: String
    },
    no: {
      type: String
    }
  },
  { _id: false }
)

export const NotificationTemplateSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      title: {
        type: LanguageSubSchema,
        required: true
      },
      content: {
        type: LanguageSubSchema,
        required: true
      },
      subject: {
        type: LanguageSubSchema
      },
      type: {
        type: String,
        enum: [
          'email',
          'sms',
          'attachment',
          'assignment_contract',
          'lease_contract',
          'moving_in_esigning',
          'moving_out_esigning',
          'pdf'
        ],
        index: true,
        required: true
      },
      templateType: {
        type: String,
        enum: ['dtms', 'app']
      },
      category: {
        type: String,
        enum: categoryList
      },
      partnerId: {
        type: String
      },
      copiedBy: {
        type: [String],
        index: true,
        default: undefined
      },
      uniqueId: {
        // This field make a relation with rules;
        // When partner copy any admin template then admin template uniqueId never change for rules between template relationship
        type: String,
        index: true,
        required: true
      },
      attachments: {
        type: [String],
        default: undefined
      },
      isCustom: {
        type: Boolean
      },
      isInvoice: {
        type: Boolean
      },
      isChatNotification: {
        type: Boolean
      },
      isEmailFooter: {
        type: Boolean
      },
      isPdfFooter: {
        type: Boolean
      },
      isTerminatedToLandlord: {
        type: Boolean
      },
      isTerminatedByLandlord: {
        type: Boolean
      },
      isTerminatedToTenant: {
        type: Boolean
      },
      isTerminatedByTenant: {
        type: Boolean
      },
      isCreditNote: {
        type: Boolean
      },
      isReminderInvoice: {
        type: Boolean
      },
      isESigningReminderForLandlordAssignment: {
        type: Boolean
      },
      isESigningReminderForLandlordLease: {
        type: Boolean
      },
      isCollectionInvoice: {
        type: Boolean
      },
      updatedAt: {
        type: Date
      },
      isScheduleTerminatedByLandlord: {
        type: Boolean
      },
      isScheduleTerminatedToLandlord: {
        type: Boolean
      },
      isESigningReminderForTenantLease: {
        type: Boolean
      },
      isESigningReminderForTenantMoveIn: {
        type: Boolean
      },
      isESigningReminderForAgentMoveIn: {
        type: Boolean
      },
      isESigningReminderForTenantMoveOut: {
        type: Boolean
      },
      isESigningReminderForAgentMoveOut: {
        type: Boolean
      },
      isScheduleTerminatedByTenant: {
        type: Boolean
      },
      isScheduleTerminatedToTenant: {
        type: Boolean
      },
      isDownloadTemplate: {
        type: Boolean
      },
      isNaturalTerminationToLandlord: {
        type: Boolean
      },
      isNaturalTerminationToTenant: {
        type: Boolean
      },
      isSoonEndingToLandlord: {
        type: Boolean
      },
      isSoonEndingToTenant: {
        type: Boolean
      },
      isCpiSettlement: {
        type: Boolean
      },
      isEvictionNotice: {
        type: Boolean
      },
      isPendingPayoutForApproval: {
        type: Boolean
      },
      isEvictionDueReminder: {
        type: Boolean
      },
      isEviction: {
        type: Boolean
      },
      isNextSchedulePayout: {
        type: Boolean
      },
      isLeaseNotice: {
        type: Boolean
      },
      isLeaseContractEsigning: {
        type: Boolean
      },
      isAssignmentEsigningPdf: {
        type: Boolean
      },
      isLeaseEsigningPdf: {
        type: Boolean
      },
      isLandlordInvoice: {
        type: Boolean
      },
      isLandlordCreditNote: {
        type: Boolean
      },
      isLandlordInvoiceReminder: {
        type: Boolean
      },
      isLandlordCollectionNotice: {
        type: Boolean
      },
      isLandlordDueReminder: {
        type: Boolean
      },
      isLandlordFirstReminder: {
        type: Boolean
      },
      isLandlordSecondReminder: {
        type: Boolean
      },
      isFinalSettlementInvoice: {
        type: Boolean
      },
      isWelcomeLease: {
        type: Boolean
      },
      isMovingInEsigningPdf: {
        type: Boolean
      },
      isMovingOutEsigningPdf: {
        type: Boolean
      },
      isAnnualStatement: {
        type: Boolean
      },
      isESigningNoticeForTenantMovingOut: {
        type: Boolean
      },
      isESigningNoticeForAgentMovingOut: {
        type: Boolean
      },
      isESigningReminderToLandlordForMoveIn: {
        type: Boolean
      },
      isESigningReminderToLandlordForMoveOut: {
        type: Boolean
      },
      isInterestForm: {
        type: Boolean
      },
      isDepositAccount: {
        type: Boolean
      },
      isDepositIncomingPayment: {
        type: Boolean
      },
      isTaskNotification: {
        type: Boolean
      },
      isInterestFormInvitation: {
        type: Boolean
      },
      isEvictionDocument: {
        type: Boolean
      },
      isEvictionDueReminderWithoutEvictionFee: {
        type: Boolean
      },
      isSendEsignForPayoutsApproval: {
        type: Boolean
      },
      isPendingPaymentForApproval: {
        type: Boolean
      },
      isSendEsignForPaymentsApproval: {
        type: Boolean
      },
      isEsignPdfForPaymentsApproval: {
        type: Boolean
      },
      isEsignPdfForPayoutsApproval: {
        type: Boolean
      },
      isDepositInsuranceEsigning: {
        type: Boolean
      },
      isDepositInsurancePaymentPending: {
        type: Boolean
      },
      isDepositInsurancePdf: {
        type: Boolean
      },
      isPartnerUserInvitation: {
        type: Boolean
      },
      isResetPassword: {
        type: Boolean
      },
      isVerifyEmail: {
        type: Boolean
      },
      isChangeEmail: {
        type: Boolean
      },
      isContactUs: {
        type: Boolean
      },
      isAppInvoice: {
        type: Boolean
      },
      isOtpEmail: {
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

NotificationTemplateSchema.index({ createdAt: 1 })
NotificationTemplateSchema.index({
  'title.no': 1,
  partnerId: 1,
  copiedBy: 1,
  templateType: 1
})
NotificationTemplateSchema.index({
  partnerId: 1,
  'title.no': 1
})
