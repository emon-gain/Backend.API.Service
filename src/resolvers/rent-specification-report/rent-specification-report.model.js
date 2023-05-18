import mongoose from 'mongoose'
import { RentSpecificationReportSchema } from '../models'

RentSpecificationReportSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

RentSpecificationReportSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

RentSpecificationReportSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

RentSpecificationReportSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

export const RentSpecificationReportCollection = mongoose.model(
  'rent_specification_reports',
  RentSpecificationReportSchema
)
