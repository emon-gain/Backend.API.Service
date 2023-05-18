import mongoose from 'mongoose'
import { size } from 'lodash'

import {
  BranchCollection,
  ContractCollection,
  ListingCollection,
  PartnerCollection,
  PropertyItemSchema,
  PropertyRoomCollection,
  TenantCollection,
  UserCollection
} from '../models'
import {
  appHelper,
  fileHelper,
  partnerHelper,
  propertyRoomHelper
} from '../helpers'

PropertyItemSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

PropertyItemSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

PropertyItemSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

PropertyItemSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

PropertyItemSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

PropertyItemSchema.methods = {
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
    const contractInfo = await ContractCollection.findOne({
      _id: this.contractId
    })

    if (size(contractInfo))
      agentInfo = await UserCollection.findOne({
        _id: contractInfo.agentId || ''
      })

    return agentInfo
  },

  async getProperty() {
    let propertyInfo = {}

    if (this.propertyId)
      propertyInfo = await ListingCollection.findOne({ _id: this.propertyId })

    return propertyInfo
  },

  async getTenant() {
    const contractInfo = this.contractId
      ? await ContractCollection.findOne({ _id: this.contractId })
      : null
    const { rentalMeta = {} } = contractInfo || {}
    const { tenantId = '' } = rentalMeta
    const tenantInfo = tenantId
      ? await TenantCollection.findOne({ _id: tenantId })
      : {}

    return tenantInfo
  },

  async getPropertyAddons() {
    let propertyAddons = []

    const property = await this.getProperty()
    if (size(property)) propertyAddons = await property.getPropertyAddons()

    return propertyAddons
  },

  async getInventory() {
    const inventory = this.inventory || {}
    const furniture =
      size(inventory) &&
      (inventory.isFurnished || inventory.isPartiallyFurnished) &&
      size(inventory.furniture)
        ? inventory.furniture
        : []

    if (size(furniture)) {
      const furnitureNewArray = []

      for (const furnitureObj of furniture) {
        // Not include the furniture if status is notApplicable/false while creating pdf
        const {
          description,
          name,
          quantity,
          responsibleForFixing,
          status = ''
        } = furnitureObj
        if (status && status !== 'notApplicable') {
          const newFurnitureObj = {}

          newFurnitureObj.furniture_name = name
          newFurnitureObj.furniture_quantity = quantity
          newFurnitureObj.has_issue = status === 'issues'

          if (responsibleForFixing) {
            const partnerId = this.partnerId || ''
            const partnerInfo = partnerId
              ? await partnerHelper.getAPartner({ _id: partnerId }, null, [
                  'owner'
                ])
              : {}
            const { user = {} } = partnerInfo || {}
            const userLang = size(user) ? user.getLanguage() : 'no'

            if (responsibleForFixing === 'noActionRequired')
              newFurnitureObj.responsible_for_fixing =
                appHelper.translateToUserLng(
                  'properties.moving_in.no_action_required',
                  userLang
                )
            else
              newFurnitureObj.responsible_for_fixing =
                appHelper.translateToUserLng(
                  'common.' + furnitureObj.responsibleForFixing,
                  userLang
                )
          }
          if (description) newFurnitureObj.issue_description = description

          furnitureNewArray.push(newFurnitureObj)
        }
      }

      return furnitureNewArray
    }
  },

  async getInventoryImages() {
    if (size(this.inventory) && size(this.inventory.files)) {
      const size = { width: 215, height: 180, fit: 'min' }
      const inventoryImages =
        (await fileHelper.getFileImages(this.inventory.files, size)) || []
      return inventoryImages
    }
  },

  async getKeysImages() {
    if (size(this.keys) && size(this.keys.files)) {
      const size = { width: 215, height: 180, fit: 'min' }
      const inventoryImages =
        (await fileHelper.getFileImages(this.keys.files, size)) || []
      return inventoryImages
    }
  },

  getKeys() {
    const keys = this.keys || {}
    const keysLists = size(keys) && size(keys.keysList) ? keys.keysList : []

    if (size(keysLists)) {
      const keysNewArray = []

      for (const keyObj of keysLists) {
        const { kindOfKey, numberOfKey, numberOfKeysReturned = 0 } = keyObj
        const newKeyObj = {}

        newKeyObj.kind_of_key = kindOfKey
        newKeyObj.number_of_key = numberOfKey
        if (numberOfKeysReturned)
          newKeyObj.number_of_keys_returned = numberOfKeysReturned

        keysNewArray.push(newKeyObj)
      }

      return keysNewArray
    }
  },

  async getMeterReading() {
    const meterReading = this.meterReading || {}
    const meters =
      size(meterReading) && size(meterReading.meters) ? meterReading.meters : []

    if (size(meters)) {
      const meterReadingNewArray = []

      for (const meterObj of meters) {
        const newMeterObj = {}
        const partnerId = this.partnerId || ''
        const dateFormat = await appHelper.getDateFormat(partnerId)
        newMeterObj.number_of_meter = meterObj.numberOfMeter
        newMeterObj.type_of_meter = meterObj.typeOfMeter
        newMeterObj.measure_of_meter = meterObj.measureOfMeter
        newMeterObj.meter_date = (
          await appHelper.getActualDate(partnerId, true, meterObj.date)
        ).format(dateFormat)

        meterReadingNewArray.push(newMeterObj)
      }

      return meterReadingNewArray
    }
  },

  async getMeterReadingImages() {
    if (this.meterReading && size(this.meterReading.files)) {
      const size = { width: 215, height: 180, fit: 'min' }
      const meterReadingImages =
        (await fileHelper.getFileImages(this.meterReading.files, size)) || []
      return meterReadingImages
    }
  },

  async getRooms() {
    const rooms = []
    const query = {
      partnerId: this.partnerId,
      propertyId: this.propertyId,
      contractId: this.contractId,
      movingId: this._id
    }
    const propertyRooms = (await PropertyRoomCollection.find(query)) || []

    if (size(propertyRooms)) {
      for (const propertyRoom of propertyRooms) {
        const roomObj = {}
        roomObj.room_name = propertyRoom.name
        roomObj.items =
          (await propertyRoomHelper.getItemsInfo(propertyRoom)) || []
        roomObj.roomImages =
          (await propertyRoomHelper.getRoomsImages(propertyRoom)) || []

        rooms.push(roomObj)
      }
    }

    return rooms
  }
}

export const PropertyItemCollection = mongoose.model(
  'property_items',
  PropertyItemSchema
)
