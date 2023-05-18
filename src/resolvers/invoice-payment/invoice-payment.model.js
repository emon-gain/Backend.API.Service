import mongoose from 'mongoose'
import {
  BranchCollection,
  InvoicePaymentSchema,
  ListingCollection,
  PartnerCollection,
  TenantCollection,
  UserCollection
} from '../models'

InvoicePaymentSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

InvoicePaymentSchema.methods = {
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
  }
}

export const InvoicePaymentCollection = mongoose.model(
  'invoice-payments',
  InvoicePaymentSchema
)
