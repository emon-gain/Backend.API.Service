import { compact, find, map, size } from 'lodash'
import moment from 'moment-timezone'
import mongoose from 'mongoose'
import {
  BranchCollection,
  ListingCollection,
  PartnerCollection,
  PayoutProcessCollection,
  PayoutSchema,
  TenantCollection,
  UserCollection
} from '../models'
import { appHelper, invoiceHelper } from '../helpers'

PayoutSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

PayoutSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

PayoutSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

PayoutSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

PayoutSchema.methods = {
  getLandlordInvoiceId() {
    let landlordInvoiceId = ''
    if (size(this.meta)) {
      const landlordInvoiceIds = compact(map(this.meta, 'landlordInvoiceId'))
      landlordInvoiceId = size(landlordInvoiceIds) ? landlordInvoiceIds[0] : ''
    }
    return landlordInvoiceId
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

  async getBranch() {
    let branchInfo = {}
    if (this.branchId)
      branchInfo = await BranchCollection.findOne({ _id: this.branchId })

    return branchInfo
  },

  async getAgent() {
    let agentInfo = {}
    if (this.agentId)
      agentInfo = await UserCollection.findOne({ _id: this.agentId })

    return agentInfo
  },

  async createdDateText() {
    if (this.createdAt) {
      const dateFormat = await appHelper.getDateFormat(this.partnerId)
      return moment(this.createdAt).format(dateFormat)
    } else return ''
  },

  async getCreditorAccountId() {
    const payoutProcess = await PayoutProcessCollection.findOne({
      creditTransferInfo: { $elemMatch: { payoutId: this._id } }
    })
    const transferInfo = size(payoutProcess)
      ? find(payoutProcess.creditTransferInfo, ['payoutId', this._id])
      : ''
    const bankAccountId = size(transferInfo)
      ? transferInfo.creditorAccountId
      : ''

    return bankAccountId || ''
  },

  async getDebtorAccountId() {
    const payoutProcess = await PayoutProcessCollection.findOne({
      creditTransferInfo: { $elemMatch: { payoutId: this._id } }
    })
    const transferInfo = size(payoutProcess)
      ? find(payoutProcess.creditTransferInfo, ['payoutId', this._id])
      : ''

    const bankAccountId = size(transferInfo) ? transferInfo.debtorAccountId : ''

    return bankAccountId || ''
  },

  async getLastUnpaidPayouts() {
    if (this && size(this.meta)) {
      let totalUnpaidPayouts = 0

      for (const payoutMeta of this.meta) {
        if (payoutMeta.type === 'unpaid_earlier_payout' && payoutMeta.amount) {
          totalUnpaidPayouts = totalUnpaidPayouts + payoutMeta.amount
        }
      }

      return totalUnpaidPayouts
        ? await appHelper.convertTo2Decimal(totalUnpaidPayouts)
        : 0
    } else return 0
  },

  async getInvoice() {
    if (this.invoiceId)
      return await invoiceHelper.getInvoice({ _id: this.invoiceId }, null)
    else return false
  }
}

export const PayoutCollection = mongoose.model('payouts', PayoutSchema)
