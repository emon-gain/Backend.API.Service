import { size } from 'lodash'
import mongoose from 'mongoose'
import { CorrectionSchema } from '../models'
import { appHelper, fileHelper, invoiceHelper, userHelper } from '../helpers'
import settingJson from '../../../settings.json'

CorrectionSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('partnerSetting', {
  ref: 'partner_settings',
  localField: 'partnerId',
  foreignField: 'partnerId',
  justOne: true
})

CorrectionSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('invoiceSummary', {
  ref: 'invoice-summary',
  localField: 'invoiceSummaryId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('payout', {
  ref: 'payouts',
  localField: 'payoutId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('user', {
  ref: 'users',
  localField: 'createdBy',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('landLordInvoice', {
  ref: 'invoices',
  localField: 'landLordInvoiceId',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.virtual('filesInfo', {
  ref: 'files',
  localField: 'files',
  foreignField: '_id',
  justOne: true
})

CorrectionSchema.methods = {
  async fileUrls() {
    const correctionImages = []
    if (!size(this.files)) {
      return []
    }
    const directive = settingJson.S3.Directives['Files']
    const { folder } = directive
    const path =
      appHelper.getCDNDomain() +
      '/' +
      folder +
      '/' +
      this.partnerId +
      '/expense/'
    const files = await fileHelper.getFiles({ _id: { $in: this.files } })
    for (const file of files) {
      const { url = '', name = '', _id = '' } = file
      const imageUrl = path + name
      correctionImages.push({
        fullUrl: imageUrl,
        originalUrl: url,
        name,
        _id
      })
    }
    return correctionImages
  },
  async getCreatedByUser() {
    if (this && this.createdBy)
      return await userHelper.getAnUser({ _id: this.createdBy })
  },
  async getCorrectionAmount() {
    let amount = this.amount
    const invoiceId = this.invoiceId

    if (this.addTo === 'rent_invoice' && invoiceId) {
      const invoiceData = await invoiceHelper.getInvoice({ _id: invoiceId })
      if (invoiceData && invoiceData.status === 'credited') amount = 0
    }
    return amount
  }
}

export const CorrectionCollection = mongoose.model('expenses', CorrectionSchema)
