import { size } from 'lodash'
import mongoose from 'mongoose'
import {
  BranchCollection,
  ListingCollection,
  PartnerCollection,
  TenantSchema,
  UserCollection
} from '../models'
import { contractHelper } from '../helpers'

TenantSchema.virtual('user', {
  ref: 'users',
  localField: 'userId',
  foreignField: '_id',
  justOne: true
})

TenantSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

TenantSchema.virtual('partnerSetting', {
  ref: 'partner_settings',
  localField: 'partnerId',
  foreignField: 'partnerId',
  justOne: true
})

TenantSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

TenantSchema.virtual('properties.contract', {
  ref: 'contracts',
  localField: 'properties.contractId',
  foreignField: '_id',
  justOne: true
})

TenantSchema.virtual('properties.branch', {
  ref: 'branches',
  localField: 'properties.branchId',
  foreignField: '_id',
  justOne: true
})

TenantSchema.virtual('properties.property', {
  ref: 'listings',
  localField: 'properties.propertyId',
  foreignField: '_id',
  justOne: true
})

TenantSchema.methods = {
  getAddress() {
    return this.billingAddress ? this.billingAddress : ''
  },

  getZipCode() {
    return this.zipCode ? this.zipCode : ''
  },

  getCity() {
    return this.city ? this.city : ''
  },

  getCountry() {
    return this.country ? this.country : ''
  },

  getSerialId() {
    return this.serial ? this.serial : ''
  },
  async availabilityStartDateTextByStatus(contractId) {
    const contract = contractId
      ? await contractHelper.getAContract({
          _id: contractId,
          partnerId: this.partnerId
        })
      : {}
    if (size(contract)) {
      return (await contract.getFormattedExportDateForContractStartDate()) || ''
    }
  },
  async availabilityEndDateTextByStatus(contractId) {
    const contract = contractId
      ? await contractHelper.getAContract({
          _id: contractId,
          partnerId: this.partnerId
        })
      : {}

    if (size(contract)) {
      const endText =
        (await contract.getFormattedExportDateForContractEndDate()) || ''
      return endText ? endText : 'Undetermined'
    }
  },

  async getUser() {
    let userInfo = {}
    if (this.userId)
      userInfo = await UserCollection.findOne({ _id: this.userId })

    return userInfo
  },

  async getPartner() {
    let partnerInfo = {}
    if (this.partnerId)
      partnerInfo = await PartnerCollection.findOne({ _id: this.partnerId })

    return partnerInfo
  },

  async getBranch(branchId) {
    if (this && size(this.properties)) {
      if (branchId) {
        const branchInfo =
          (await BranchCollection.findOne({ _id: branchId })) || {}
        return branchInfo
      } else {
        const propertyData = this.properties[0]
        if (size(propertyData) && propertyData.branchId) {
          const branchInfo =
            (await BranchCollection.findOne({ _id: propertyData.branchId })) ||
            {}
          return branchInfo
        }
      }
    }
  },

  async getAgent(agentId) {
    if (this && size(this.properties)) {
      if (agentId) {
        const agentInfo = await UserCollection.findOne({ _id: agentId })
        return agentInfo
      } else {
        const propertyData = this.properties[0]
        if (size(propertyData) && propertyData.agentId) {
          const agentInfo = await UserCollection.findOne({
            _id: propertyData.agentId
          })
          return agentInfo
        }
      }
    }
  },

  getTenant() {
    return this
  },

  async getProperty(propertyId) {
    if (this && size(this.properties)) {
      if (propertyId) {
        const propertyInfo = await ListingCollection.findOne({
          _id: propertyId
        })
        return propertyInfo
      }
    } else {
      const propertyData = this.properties[0]
      if (size(propertyData) && propertyData.propertyId) {
        const propertyInfo = await ListingCollection.findOne({
          _id: propertyData.propertyId
        })
        return propertyInfo
      }
    }
  }
}

export const TenantCollection = mongoose.model('tenants', TenantSchema)
