import mongoose from 'mongoose'
import { TaxCodeSchema } from '../models'

TaxCodeSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

TaxCodeSchema.methods = {
  getTaxCode() {
    return this.taxCode ? this.taxCode : 0
  },

  getTaxPercentage() {
    return this.taxPercentage ? this.taxPercentage : 0
  }
}

export const TaxCodeCollection = mongoose.model('tax_codes', TaxCodeSchema)
