import { find, size } from 'lodash'
import mongoose from 'mongoose'
import moment from 'moment-timezone'

import { UserSchema } from '../models'
import { appHelper } from '../helpers'

UserSchema.virtual('userReport', {
  ref: 'userReport',
  localField: '_id',
  foreignField: 'reportedUser',
  justOne: false
})

UserSchema.virtual('partnersInfo', {
  ref: 'partners',
  localField: 'partners.partnerId',
  foreignField: '_id',
  justOne: false
})

UserSchema.methods = {
  getLanguage() {
    return this.profile && this.profile.language
  },

  getName() {
    return this.profile && this.profile.name
  },

  getPhone() {
    return this.profile && this.profile.phoneNumber
      ? this.profile.phoneNumber
      : ''
  },

  getNorwegianNationalIdentification() {
    return this.profile && this.profile.norwegianNationalIdentification
      ? this.profile.norwegianNationalIdentification
      : ''
  },

  getOccupation() {
    if (this.profile && this.profile.occupation) return this.profile.occupation
  },

  getEmail() {
    let result = ''
    const emailArrLength = size(this.emails)
    const facebookEmail =
      this.services && this.services.facebook && this.services.facebook.email
        ? this.services.facebook.email
        : ''
    // First try to set last verified email address
    if (emailArrLength) {
      this.emails.forEach((email) => {
        if (email.verified) {
          result = email.address
        }
      })
    }
    // If no verified email found, try to set facebook email
    if (!result && facebookEmail) {
      result = facebookEmail
    }
    // If both of the above procedure does not find any suitable email, set unverified email address
    if (!result && emailArrLength) {
      result = this.emails[0] && this.emails[0].address
    }
    return result
  },

  getEmployeeId(partnerId) {
    if (size(this.partners)) {
      const partnerData = find(
        this.partners,
        (partner) => partner.partnerId === partnerId
      )
      if (size(partnerData) && partnerData.employeeId) {
        return partnerData.employeeId
      }
    }
  },

  getHometown() {
    return this.profile && this.profile.hometown
  },

  getZipCode() {
    return this.profile && this.profile.zipCode
  },

  getCity() {
    return this.profile && this.profile.city
  },

  getCountry() {
    return this.profile && this.profile.country
  },

  getRoommateGroupId() {
    if (this.profile && this.profile.roommateGroupId) {
      return this.profile.roommateGroupId
    }
    return ''
  },

  isActive() {
    return !!(this.profile && this.profile.active !== false) // If profile.active is undefined or true then user is active
  },

  hasListing() {
    return !!(
      this.profile &&
      this.profile.hasListing &&
      this.profile.hasListing !== false
    )
  },

  isRegisteredByFB() {
    return !!this.services && !!this.services.facebook
  },

  hasPassword() {
    if (this.isRegisteredByFB()) return true
    else if (this.services && this.services.password) return true
    else return false
  },

  getLoginVersion() {
    const profile = this.profile
    const { loginVersion = '' } = profile || {}
    return loginVersion
  },

  isOnline() {
    if (this.status?.lastLogin?.date) {
      const lastOnline = moment(this.status.lastLogin.date)
      const now = moment()
      const duration = now.diff(lastOnline, 'minutes')
      return duration < 2
    } else return false
  },

  getAvatar(size) {
    if (this.profile && this.profile.avatarKey) {
      const width = size ? size.w : 215
      const height = size ? size.h : 215
      const fit = size ? size.fit : 'facearea'

      const userAvatar =
        appHelper.getCDNDomain() +
        '/' +
        this.profile.avatarKey +
        '?w=' +
        width +
        '&h=' +
        height +
        '&fit=' +
        fit +
        '&facepad=3'

      return userAvatar
    } else if (this.profile && this.profile.picture) {
      if (this.profile.picture.data) {
        return this.profile.picture.data.url
      } else {
        return this.profile.picture
      }
    } else {
      if (size)
        return appHelper.getCDNDomain() + '/assets/user-avatar-square.png'
      else
        return (
          appHelper.getCDNDomain() + '/assets/default-image/user-primary.png'
        )
    }
  }
}

export const UserCollection = mongoose.model('users', UserSchema)
