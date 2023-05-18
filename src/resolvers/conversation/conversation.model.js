import mongoose from 'mongoose'
import { each, size } from 'lodash'

import { ConversationSchema, ListingCollection } from '../models'

ConversationSchema.methods = {
  async getListingInfo() {
    const listingId = this.listingId || this.propertyId
    const listingInfo = listingId
      ? await ListingCollection.findOne({ _id: listingId })
      : {}

    return listingInfo || {}
  },

  allIdentityIds() {
    const identityIds = {}

    if (size(this.identity)) {
      each(this.identity, (identityInfo) => {
        if (size(identityInfo) && identityInfo.id && identityInfo.userId)
          identityIds[identityInfo.userId] = identityInfo.id // Prepare identity ids array {'userId':'id'}
      })
    }
    return identityIds
  }
}

export const ConversationCollection = mongoose.model(
  'conversations',
  ConversationSchema
)
