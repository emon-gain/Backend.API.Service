import mongoose from 'mongoose'
import { AnnualStatementSchema } from '../models'

AnnualStatementSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

AnnualStatementSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

AnnualStatementSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

AnnualStatementSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

AnnualStatementSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

AnnualStatementSchema.virtual('file', {
  ref: 'files',
  localField: 'fileId',
  foreignField: '_id',
  justOne: true
})

export const AnnualStatementCollection = mongoose.model(
  'annual_statements',
  AnnualStatementSchema
)
