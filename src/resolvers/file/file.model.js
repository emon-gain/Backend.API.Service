import mongoose from 'mongoose'
import {
  BranchCollection,
  FileSchema,
  ListingCollection,
  PartnerCollection,
  TenantCollection,
  UserCollection
} from '../models'
import settingsJSON from '../../../settings.json'
import { appHelper } from '../helpers'

FileSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

FileSchema.methods = {
  async getPartner() {
    let partnerInfo = {}
    if (this.partnerId)
      partnerInfo = await PartnerCollection.findOne({ _id: this.partnerId })

    return partnerInfo
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

  async getProperty() {
    let propertyInfo = {}
    if (this.propertyId)
      propertyInfo = await ListingCollection.findOne({ _id: this.propertyId })

    return propertyInfo
  },

  async getTenant() {
    let tenantInfo = {}
    if (this.tenantId)
      tenantInfo = await TenantCollection.findOne({ _id: this.tenantId })

    return tenantInfo
  },

  getFileImage(size) {
    const { width = '', height = '', fit = '' } = size || {}
    const directive = settingsJSON.S3.Directives['Files']
    const partnerId =
      this.partnerId || this.landlordPartnerId || this.tenantPartnerId

    let path =
      appHelper.getCDNDomain() +
      '/' +
      directive.folder +
      '/' +
      partnerId +
      '/' +
      this.context +
      '/' +
      this.name

    if (!(width || height || fit)) return path
    else return (path += '?w=' + width + '&h=' + height + '&fit=' + fit)
  }
}

export const FileCollection = mongoose.model('files', FileSchema)
