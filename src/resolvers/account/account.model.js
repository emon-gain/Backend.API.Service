import mongoose from 'mongoose'
import { size } from 'lodash'
import { AccountSchema } from '../models'

/*
 * Note: this.person here is an alias of personId, and all instance methods that are using this.person will only work,
 * if you populate (Mongoose populate feature) account with personId.
 * For more technical detail, please visit - https://mongoosejs.com/docs/populate.html
 * */
AccountSchema.virtual('organization', {
  ref: 'organizations',
  localField: 'organizationId',
  foreignField: '_id',
  justOne: true
})

AccountSchema.virtual('person', {
  ref: 'users',
  localField: 'personId',
  foreignField: '_id',
  justOne: true
})

AccountSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

AccountSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

AccountSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

AccountSchema.methods = {
  getAddress() {
    const person = this.person
    if (this.type === 'person' && size(person) && person.getHometown()) {
      return person.getHometown()
    }
    return this.address
  },
  getFullAddress() {
    const person = this.person
    let address = ''

    if (this.type === 'person') {
      if (size(person) && person.getHometown()) address = person.getHometown()

      if (this.getZipCode()) address = address + ', ' + this.getZipCode()
      if (this.getCity()) address = address + ', ' + this.getCity()
      if (this.getCountry()) address = address + ', ' + this.getCountry()
    } else {
      if (this.address) address = this.address
      if (this.zipCode) address = address + ', ' + this.zipCode
      if (this.city) address = address + ', ' + this.city
      if (this.country) address = address + ', ' + this.country
    }

    return address
  },
  getZipCode() {
    const person = this.person
    if (this.type === 'person' && size(person) && person.getZipCode()) {
      return person.getZipCode()
    }
    return this.zipCode
  },

  getCity() {
    const person = this.person
    if (this.type === 'person' && size(person) && person.getCity()) {
      return person.getCity()
    }
    return this.city
  },

  getCountry() {
    const person = this.person
    if (this.type === 'person' && size(person) && person.getCountry()) {
      return person.getCountry()
    }
    return this.country
  }
}

export const AccountCollection = mongoose.model('accounts', AccountSchema)
