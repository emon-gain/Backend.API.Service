import mongoose from 'mongoose'

import { OrganizationSchema } from '../models'
import { appHelper } from '../helpers'

OrganizationSchema.methods = {
  getLogo(size, isV1Link = true) {
    if (this.image) {
      const { height = 215, width = 215 } = size || {}

      return (
        appHelper.getCDNDomain() +
        '/partner_logo/' +
        this.partnerId +
        '/accounts/' +
        this.image +
        '?w=' +
        width +
        '&h=' +
        height
      )
    } else {
      if (isV1Link) {
        return appHelper.getDefaultLogoURL('organization')
      }
      return (
        appHelper.getCDNDomain() +
        '/assets/default-image/organization-primary.png'
      )
    }
  }
}

export const OrganizationCollection = mongoose.model(
  'organizations',
  OrganizationSchema
)
