import mongoose from 'mongoose'
import { PartnerSchema } from '../models'
import { appHelper } from '../helpers'

PartnerSchema.virtual('owner', {
  ref: 'users',
  localField: 'ownerId',
  foreignField: '_id',
  justOne: true
})

PartnerSchema.virtual('partnerSetting', {
  ref: 'partner_settings',
  localField: '_id',
  foreignField: 'partnerId',
  justOne: true
})

PartnerSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

PartnerSchema.methods = {
  isBroker() {
    if (this && this.accountType === 'broker') return true
  },

  isDirect() {
    if (this && this.accountType === 'direct') return true
  },

  getLogo(size) {
    if (this.logo) {
      const { height = 215, width = 215 } = size || {}

      return (
        appHelper.getCDNDomain() +
        '/partner_logo/' +
        this._id +
        '/' +
        this.logo +
        '?w=' +
        width +
        '&h=' +
        height
      )
    } else return appHelper.getDefaultLogoURL('organization')
  },

  getSiteLogo(size) {
    if (this.siteLogo) {
      const { height = 215, width = 215 } = size || {}

      return (
        appHelper.getCDNDomain() +
        '/partner_logo/' +
        this._id +
        '/' +
        this.siteLogo +
        '?w=' +
        width +
        '&h=' +
        height
      )
    } else return appHelper.getDefaultLogoURL('organization')
  }
}

export const PartnerCollection = mongoose.model('partners', PartnerSchema)
