import mongoose from 'mongoose'

import { CommissionSchema } from '../models'

CommissionSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

CommissionSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

CommissionSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

CommissionSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

CommissionSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

CommissionSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

CommissionSchema.virtual('payout', {
  ref: 'payouts',
  localField: 'payoutId',
  foreignField: '_id',
  justOne: true
})

export const CommissionCollection = mongoose.model(
  'commissions',
  CommissionSchema
)
