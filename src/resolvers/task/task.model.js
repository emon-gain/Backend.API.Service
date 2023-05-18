import mongoose from 'mongoose'
import {
  BranchCollection,
  ListingCollection,
  PartnerCollection,
  TaskSchema,
  TenantCollection,
  UserCollection
} from '../models'

TaskSchema.virtual('user', {
  ref: 'users',
  localField: 'createdBy',
  foreignField: '_id',
  justOne: true
})

TaskSchema.methods = {
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
  }
}

export const TaskCollection = mongoose.model('tasks', TaskSchema)
