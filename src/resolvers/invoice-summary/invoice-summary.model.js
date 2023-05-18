import mongoose from 'mongoose'
import { InvoiceSummarySchema } from '../models'

InvoiceSummarySchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

InvoiceSummarySchema.virtual('payout', {
  ref: 'payouts',
  localField: 'payoutId',
  foreignField: '_id',
  justOne: true
})

export const InvoiceSummaryCollection = mongoose.model(
  'invoice-summary',
  InvoiceSummarySchema,
  'invoice-summary'
)
