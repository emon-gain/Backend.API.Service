import mongoose from 'mongoose'
import { LogSchema } from '../models'

LogSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('task', {
  ref: 'tasks',
  localField: 'taskId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('file', {
  ref: 'files',
  localField: 'fileId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('comment', {
  ref: 'comments',
  localField: 'commentId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('correction', {
  ref: 'expenses',
  localField: 'correctionId',
  foreignField: '_id',
  justOne: true
})

LogSchema.virtual('payout', {
  ref: 'payouts',
  localField: 'payoutId',
  foreignField: '_id',
  justOne: true
})

export const LogCollection = mongoose.model('logs', LogSchema)
