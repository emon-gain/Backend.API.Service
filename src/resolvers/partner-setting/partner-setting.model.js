import mongoose from 'mongoose'
import { PartnerSettingSchema } from '../models'

PartnerSettingSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PartnerSettingSchema.virtual('directRemittanceApprovalUsers', {
  ref: 'users',
  localField: 'directRemittanceApproval.persons',
  foreignField: '_id',
  justOne: false
})

PartnerSettingSchema.methods = {
  getCompanyName() {
    return this.companyInfo && this.companyInfo.companyName
      ? this.companyInfo.companyName
      : ''
  },
  getBankAccounts() {
    return this.bankAccounts ? this.bankAccounts : []
  },
  getBankPayment() {
    return this.bankPayment ? this.bankPayment : null
  },
  isSendCreditNoteNotification() {
    return this.notifications && this.notifications.creditNote
  },
  isSendFinalSettlementNotification() {
    return this.notifications && this.notifications.finalSettlementInvoice
  }
}

export const PartnerSettingCollection = mongoose.model(
  'partner_settings',
  PartnerSettingSchema
)
