import mongoose from 'mongoose'
import { CreatedBySchemas, Id } from '../common'

const allowedActions = [
  'added_new_property',
  'updated_property',
  'updated_account',
  'updated_organization',
  'updated_contact_person',
  'added_new_comment',
  'new_message',
  'added_new_invoice',
  'sent_due_reminder_email',
  'sent_due_reminder_sms',
  'sent_first_reminder_email',
  'sent_first_reminder_sms',
  'sent_second_reminder_email',
  'sent_second_reminder_sms',
  'sent_collection_notice_email',
  'sent_collection_notice_sms',
  'lost_invoice',
  'created_new_assignment',
  'terminate_assignment',
  'added_lease',
  'terminate_lease',
  'cancelled_lease',
  'updated_lease',
  'added_new_payment',
  'updated_payment',
  'updated_refunded_payment',
  'removed_payment',
  'canceled_refund_payment',
  'added_new_correction',
  'added_new_commission',
  'added_new_payout',
  'added_new_payout_correction',
  'added_new_payout_commission',
  'updated_payout',
  'updated_payout_info',
  'uploaded_file',
  'updated_contract',
  'removed_file',
  'added_lease_addon',
  'removed_lease_addon',
  'added_assignment_addon',
  'removed_assignment_addon',
  'updated_lease_addon',
  'updated_assignment_addon',
  'created_task',
  'updated_task',
  'updated_contract',
  'updated_tenant',
  'updated_due_delay',
  'sent_assignment_email',
  'sent_termination_email',
  'sent_assignment_sms',
  'sent_invoice_email',
  'sent_invoice_sms',
  'sent_credit_note_sms',
  'sent_termination_sms',
  'sent_schedule_termination_email',
  'sent_schedule_termination_sms',
  'sent_natural_termination_email',
  'sent_natural_termination_sms',
  'sent_soon_ending_email',
  'sent_soon_ending_sms',
  'removed_lost_invoice',
  'removed_reminder_fee',
  'sent_CPI_settlement_notice_email',
  'sent_CPI_settlement_notice_sms',
  'sent_next_schedule_payout_email',
  'sent_next_schedule_payout_sms',
  'sent_eviction_notice_email',
  'sent_eviction_notice_sms',
  'sent_eviction_due_reminder_notice_email',
  'sent_eviction_due_reminder_notice_sms',
  'sent_payout_email',
  'sent_payout_sms',
  'invoice_sent_to_vipps',
  'invoice_sent_to_vipps_error',
  'sent_assignment_esigning_email',
  'sent_assignment_esigning_sms',
  'sent_tenant_lease_esigning_email',
  'sent_tenant_lease_esigning_sms',
  'sent_landlord_lease_esigning_email',
  'sent_landlord_lease_esigning_sms',
  'landlord_signed_assignment_contract',
  'agent_signed_assignment_contract',
  'landlord_signed_lease_contract',
  'tenant_signed_lease_contract',
  'added_lease_tenant',
  'removed_lease_tenant',
  'updated_main_tenant',
  'removed_eviction_notice_fee',
  'removed_administration_eviction_notice',
  'removed_collection_notice',
  'cancelled_a_correction',
  'sent_landlord_invoice_email',
  'publish_to_finn',
  'failed_to_finn',
  'send_welcome_lease_email',
  'send_welcome_lease_sms',
  'updated_joint_deposit_account',
  'updated_jointly_liable',
  'sent_final_settlement_email',
  'sent_final_settlement_sms',
  'tenant_signed_lease_contract',
  'tenant_signed_moving_in',
  'landlord_signed_moving_in',
  'agent_signed_moving_in',
  'added_moving_out',
  'regenerate_assignment_signing',
  'regenerate_lease_signing',
  'sent_tenant_moving_in_esigning_email',
  'sent_tenant_moving_in_esigning_sms',
  'sent_tenant_moving_out_esigning_email',
  'sent_tenant_moving_out_esigning_sms',
  'sent_agent_moving_in_esigning_email',
  'sent_agent_moving_in_esigning_sms',
  'sent_agent_moving_out_esigning_email',
  'sent_agent_moving_out_esigning_sms',
  'sent_landlord_moving_in_esigning_email',
  'sent_landlord_moving_in_esigning_sms',
  'sent_landlord_moving_out_esigning_email',
  'sent_landlord_moving_out_esigning_sms',
  'tenant_signed_moving_out',
  'landlord_signed_moving_out',
  'agent_signed_moving_out',
  'sent_custom_notification_email',
  'sent_custom_notification_sms',
  'sent_assignment_esigning_reminder_notice_email',
  'sent_assignment_esigning_reminder_notice_sms',
  'sent_tenant_lease_esigning_reminder_notice_email',
  'sent_tenant_lease_esigning_reminder_notice_sms',
  'sent_landlord_lease_esigning_reminder_notice_email',
  'sent_landlord_lease_esigning_reminder_notice_sms',
  'sent_tenant_moving_in_esigning_reminder_notice_email',
  'sent_tenant_moving_in_esigning_reminder_notice_sms',
  'sent_agent_moving_in_esigning_reminder_notice_email',
  'sent_agent_moving_in_esigning_reminder_notice_sms',
  'sent_landlord_moving_in_esigning_reminder_notice_email',
  'sent_landlord_moving_in_esigning_reminder_notice_sms',
  'sent_tenant_moving_out_esigning_reminder_notice_email',
  'sent_tenant_moving_out_esigning_reminder_notice_sms',
  'sent_agent_moving_out_esigning_reminder_notice_email',
  'sent_agent_moving_out_esigning_reminder_notice_sms',
  'sent_landlord_moving_out_esigning_reminder_notice_email',
  'sent_landlord_moving_out_esigning_reminder_notice_sms',
  'assignee_added',
  'assignee_updated',
  'assignee_removed',
  'sent_task_notification_email',
  'sent_task_notification_sms',
  'send_landlord_annual_statement_email',
  'send_landlord_annual_statement_sms',
  'added_new_annual_statement',
  'sent_interest_form_email',
  'sent_interest_form_sms',
  'sent_deposit_account_created_email',
  'sent_deposit_account_created_sms',
  'sent_deposit_incoming_payment_email',
  'sent_deposit_incoming_payment_sms',
  'cancel_move_in',
  'cancel_move_out',
  'sent_request_for_deposit_account_creation',
  'deposit_account_created',
  'deposit_insurance_created',
  'add_credit_rating',
  'update_credit_rating',
  'sent_eviction_due_reminder_notice_without_eviction_fee_email',
  'sent_eviction_due_reminder_notice_without_eviction_fee_sms',
  'sent_notification_tenant_pays_all_due_during_eviction_email',
  'sent_notification_tenant_pays_all_due_during_eviction_sms',
  'updated_eviction_case',
  'produced_eviction_document',
  'removed_eviction_case',
  'send_pending_payout_for_approval_email',
  'send_pending_payout_for_approval_sms',
  'send_payouts_approval_esigning_email',
  'send_payouts_approval_esigning_sms',
  'send_notification_ask_for_credit_rating_email',
  'send_notification_ask_for_credit_rating_sms',
  'sent_wrong_ssn_notification_email',
  'sent_deposit_insurance_created_email',
  'sent_app_invoice_email',
  'invoice_sent_to_compello',
  'invoice_sent_to_compello_error',
  'removed_lease_termination'
]

const allowedContext = [
  'property',
  'account',
  'tenant',
  'comment',
  'conversation',
  'organization',
  'task',
  'invoice',
  'app_invoice',
  'payment',
  'payout',
  'correction',
  'commission',
  'contract',
  'landlordDashboard',
  'tenantDashboard',
  'creditRating',
  'lease',
  'assignment',
  'moving_in',
  'moving_out',
  'eviction_document'
]

const LogChangesSchemas = new mongoose.Schema(
  {
    field: {
      type: String,
      required: true
    },
    type: {
      type: String,
      enum: ['text', 'number', 'date', 'foreignKey', 'boolean'],
      required: true
    },
    oldId: {
      type: String
    },
    newId: {
      type: String
    },
    oldText: {
      type: String
    },
    newText: {
      type: String
    },
    oldNumber: {
      type: String
    },
    newNumber: {
      type: String
    },
    oldDate: {
      type: Date
    },
    newDate: {
      type: Date
    }
  },
  { _id: false }
)

const LogMetaSchemas = new mongoose.Schema(
  {
    field: {
      type: String,
      required: true
    },
    value: {
      type: String
    },
    contractId: {
      type: String
    },
    propertyId: {
      type: String
    },
    toEmail: {
      type: String
    }
  },
  { _id: false }
)

export const LogSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      partnerId: {
        type: String,
        index: true
      },
      propertyId: {
        type: String,
        index: true
      },
      propertyIds: {
        type: [String],
        index: true,
        default: undefined
      },
      accountId: {
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
      taskId: {
        type: String,
        index: true
      },

      paymentId: {
        type: String,
        index: true
      },
      fileId: {
        type: String,
        index: true
      },
      commentId: {
        type: String,
        index: true
      },
      invoiceId: {
        type: String,
        index: true
      },
      contractId: {
        type: String,
        index: true
      },
      payoutId: {
        type: String,
        index: true
      },
      correctionId: {
        type: String,
        index: true
      },
      expenseId: {
        type: String,
        index: true
      },
      action: {
        type: String,
        index: true,
        enum: allowedActions
      },
      context: {
        type: String,
        index: true,
        enum: allowedContext
      },
      isChangeLog: {
        type: Boolean,
        index: true,
        default: false
      },
      botType: {
        type: String,
        index: true,
        enum: ['banking_bot', 'sms_bot']
      },
      visibility: {
        type: [String],
        index: true,
        enum: allowedContext,
        default: undefined
      },
      changes: {
        type: [LogChangesSchemas],
        default: undefined
      },
      conversationId: {
        type: String,
        index: true
      },
      messageId: {
        type: String,
        index: true
      },
      meta: {
        type: [LogMetaSchemas],
        default: undefined
      },
      landlordPartnerId: {
        type: String,
        index: true
      },
      notificationLogId: {
        type: String
      },
      tenantPartnerId: {
        type: String,
        index: true
      },
      errorText: {
        type: String
      },
      movingId: {
        type: String,
        index: true
      },
      isResend: {
        type: Boolean
      },
      annualStatementId: {
        type: String
      },
      reason: {
        type: String
      },
      isMovingInOutProtocolTaskLog: {
        type: Boolean
      },
      isPayable: {
        type: Boolean
      },
      commissionId: {
        type: String,
        index: true
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

LogSchema.index({ createdAt: 1 })
LogSchema.index({
  partnerId: 1,
  agentId: 1,
  createdAt: -1
})
LogSchema.index({
  partnerId: 1,
  accountId: 1,
  visibility: 1,
  createdAt: -1
})
LogSchema.index({
  partnerId: 1,
  invoiceId: 1,
  visibility: 1,
  createdAt: -1
})
LogSchema.index({
  partnerId: 1,
  context: 1
})
