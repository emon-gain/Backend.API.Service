import mongoose from 'mongoose'
import moment from 'moment-timezone'
import { extend, find, size } from 'lodash'
import {
  AppInvoiceSchema,
  ContractCollection,
  ListingCollection,
  PartnerCollection,
  PartnerSettingCollection,
  TenantCollection
} from '../models'
import { appHelper, paymentHelper } from '../helpers'

AppInvoiceSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

AppInvoiceSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

AppInvoiceSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

AppInvoiceSchema.methods = {
  async getPayments() {
    const invoiceId = this._id
    const payments = await paymentHelper.getPayments({
      contractId: this.contractId,
      partnerId: this.partnerId,
      propertyId: this.propertyId,
      type: 'payment',
      'invoices.invoiceId': invoiceId
    })
    let totalPayment = 0

    if (size(payments)) {
      //Calculate total payments of invoice
      for (const payment of payments) {
        const paymentInvoices = payment.invoices

        if (size(paymentInvoices)) {
          const findPaymentInvoiceInfo = find(
            paymentInvoices,
            function (paymentInvoice) {
              return paymentInvoice.invoiceId === invoiceId
            }
          )
          if (size(findPaymentInvoiceInfo)) {
            totalPayment += findPaymentInvoiceInfo.amount
            //Extend payment invoice info for each payment
            extend(payment, { paymentInvoice: findPaymentInvoiceInfo })
          }
        }
      }
    }

    return { payments, totalPayment }
  },

  async getTenant() {
    let tenantInfo = {}
    if (this.tenantId)
      tenantInfo = await TenantCollection.findOne({ _id: this.tenantId })

    return tenantInfo
  },

  async getPartner() {
    let partnerInfo = {}
    if (this.partnerId)
      partnerInfo = await PartnerCollection.findOne({ _id: this.partnerId })

    return partnerInfo
  },

  async getProperty() {
    let propertyInfo = {}
    if (this.propertyId)
      propertyInfo = await ListingCollection.findOne({ _id: this.propertyId })

    return propertyInfo
  },

  async getInvoiceContractInfo() {
    let contractInfo = {}
    if (this.contractId && this.partnerId)
      contractInfo = await ContractCollection.findOne({
        _id: this.contractId,
        partnerId: this.partnerId
      })

    return contractInfo
  },

  async createdDateText() {
    if (this.createdAt) {
      const dateFormat = await appHelper.getDateFormat(this.partnerId)
      return moment(this.createdAt).format(dateFormat)
    } else return ''
  },

  getInvoiceMonth() {
    return this.invoiceMonth ? moment(this.invoiceMonth).format('MMMM') : ''
  },

  getInvoiceYear() {
    return this.invoiceMonth ? moment(this.invoiceMonth).format('YYYY') : ''
  },

  async getTotalDueAmount(invoice) {
    if (!size(invoice)) invoice = this
    const { invoiceTotal = 0, totalPaid = 0 } = invoice
    const dueTotal = await appHelper.convertTo2Decimal(invoiceTotal - totalPaid)
    return dueTotal
  },

  getInvoiceId() {
    return this.serialId
  },

  async getInternalLeaseId() {
    if (this && this.contractId && this.partnerId) {
      const contractInfo = await ContractCollection.findOne({
        _id: this.contractId,
        partnerId: this.partnerId
      })
      const { rentalMeta = {} } = contractInfo || {}
      const { internalLeaseId = '' } = rentalMeta
      return internalLeaseId
    } else return ''
  },

  async getFirstReminderDate() {
    const dateFormat = await appHelper.getDateFormat(this.partnerId)

    if (this && this.firstReminderSentAt)
      return moment(this.firstReminderSentAt).format(dateFormat)
    else return ''
  },

  async getSecondReminderDate() {
    const dateFormat = await appHelper.getDateFormat(this.partnerId)

    if (this && this.secondReminderSentAt)
      return moment(this.secondReminderSentAt).format(dateFormat)
    else return ''
  },

  getInvoiceReminderFee() {
    if (size(this.feesMeta)) {
      const reminderFee = find(
        this.feesMeta,
        (feeMeta) => feeMeta.type === 'reminder' && !feeMeta.original
      )

      if (size(reminderFee)) return reminderFee.total
      else return ''
    } else return ''
  },

  async getFirstMonthBankAccountNumber() {
    const partnerId = this.partnerId
    const partnerSettings = partnerId
      ? await PartnerSettingCollection.findOne({ partnerId })
      : {}
    const { bankPayment = {} } = partnerSettings || {}
    const { firstMonthACNo = '' } = bankPayment

    return firstMonthACNo
  }
}

export const AppInvoiceCollection = mongoose.model(
  'app_invoices',
  AppInvoiceSchema,
  'app_invoices'
)
