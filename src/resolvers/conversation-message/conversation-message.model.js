import mongoose from 'mongoose'
import { find, size } from 'lodash'

import {
  BranchCollection,
  ConversationCollection,
  ConversationMessageSchema,
  UserCollection
} from '../models'
import { appHelper } from '../helpers'
import settingsJson from '../../../settings.json'

ConversationMessageSchema.methods = {
  async sendToUserInfo(identityId) {
    // Send to info
    if (this.conversationId && identityId) {
      const conversation = await ConversationCollection.findOne({
        _id: this.conversationId
      })
      const { identity } = conversation
      if (size(identity)) {
        const sendToIdentity = find(
          identity,
          (identityObj) => identityObj.id === identityId
        )
        console.log('sendToIdentity ', sendToIdentity)
        const { userId = '' } = sendToIdentity || {}
        console.log('userId ', userId)
        if (userId) {
          const userInfo = (await UserCollection.findOne({ _id: userId })) || {}
          console.log('userInfo ', userInfo)
          return userInfo
        }
      } else return false
    } else return false
  },

  async getBranch() {
    let branchInfo = {}
    if (this.branchId)
      branchInfo = await BranchCollection.findOne({ _id: this.branchId })

    return branchInfo
  },

  contentHTML() {
    const content = this.content

    // If has emojione icon tag
    if (!!(content.match(/background: url/g) && content.match(/emojione/g)))
      return content

    // Convert link to href and new line to br
    const exp =
      /(\b(https?|ftp|file):\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/i

    const removeHref = content.replace('href=')

    const finalContent = removeHref
      .replace(exp, " target='_blank' href='$1'")
      .replace(/\n/g, '<br />')

    return finalContent
  },

  isImageFile() {
    if (this.isFile) {
      const ext = this.content.split('.').pop()
      const allowedExt = ['jpg', 'png', 'jpeg', 'gif', 'svg']
      return allowedExt.includes(ext)
    } else return false
  },

  getMessageImage(size) {
    if (this.isFile) {
      const directiveFolder = settingsJson.S3.Directives['Conversations'].folder
      let path =
        appHelper.getCDNDomain() +
        '/' +
        directiveFolder +
        '/' +
        this.conversationId +
        '/' +
        this.content
      // If size ? Get image size gallery size
      return size ? (path += '?w=' + 215 + '&h=' + 180 + '&fit=' + 'min') : path
    }
  }
}

export const ConversationMessageCollection = mongoose.model(
  'conversation-messages',
  ConversationMessageSchema
)
