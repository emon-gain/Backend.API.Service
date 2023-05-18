import mongoose from 'mongoose'
import {
  BranchCollection,
  CommentSchema,
  PartnerCollection,
  UserCollection
} from '../models'

CommentSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

CommentSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

CommentSchema.virtual('property', {
  ref: 'property_items',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

CommentSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

CommentSchema.virtual('task', {
  ref: 'tasks',
  localField: 'taskId',
  foreignField: '_id',
  justOne: true
})

CommentSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

CommentSchema.methods = {
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
  }
}

export const CommentCollection = mongoose.model('comments', CommentSchema)
