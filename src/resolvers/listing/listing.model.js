import { each, filter, find, indexOf, pick, size, sortBy } from 'lodash'
import mongoose from 'mongoose'
import moment from 'moment-timezone'

import {
  AddonCollection,
  BranchCollection,
  ListingSchema,
  PartnerCollection,
  RoomMateGroupCollection,
  SettingCollection,
  UserCollection
} from '../models'
import {
  appHelper,
  contractHelper,
  partnerSettingHelper,
  settingHelper
} from '../helpers'
import * as settings from '../../../settings.json'

ListingSchema.virtual('owner', {
  ref: 'users',
  localField: 'ownerId',
  foreignField: '_id',
  justOne: true
})

ListingSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

ListingSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

ListingSchema.virtual('partnerSetting', {
  ref: 'partner_settings',
  localField: 'partnerId',
  foreignField: 'partnerId',
  justOne: true
})

ListingSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

ListingSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

ListingSchema.methods = {
  getLocationDetail() {
    const location = pick(this.location, [
      'name',
      'postalCode',
      'city',
      'country'
    ])
    let result = ''
    let counter = 0
    for (const key in location) {
      // See this: https://eslint.org/docs/rules/guard-for-in
      if (Object.prototype.hasOwnProperty.call(location, key)) {
        if (location[key]) {
          if (counter) {
            result += ', '
          }
          result += location[key]
          counter++
        }
      }
    }
    return result
  },

  getSerialId() {
    return this.serial ? this.serial : ''
  },

  getApartmentId() {
    return this.apartmentId ? this.apartmentId : ''
  },

  getPostalCode() {
    return this.location && this.location.postalCode
  },

  getCity() {
    return this.location && this.location.city
  },

  getCountry() {
    return this.location && this.location.country
  },

  getLocationName() {
    return this.location && this.location.name
  },

  async getOwnerInfo() {
    if (this.tempUser) {
      return {
        isTempUser: true,
        name: this.tempUser.name,
        avatar: () =>
          // TODO: add absoluteUrl Meteor.absoluteUrl()
          'http://localhost:3000/assets/user-avatar-square.png'
      }
    }
    const user = await UserCollection.findOne({ _id: this.ownerId })
    return user
  },

  async ownerOrGroupInfo() {
    let info = {}
    const groupInfo = RoomMateGroupCollection.findOne({ listingId: this._id })

    if (groupInfo && this.liveThere) {
      info = groupInfo
      info.isGroup = true
    } else {
      info = await this.getOwnerInfo()
    }

    return info
  },

  async facilitiesDetails() {
    const facilities = this.facilities || []
    const listingSetting = await settingHelper.getSettingInfo()
    let defaultFacilities = []

    if (listingSetting && listingSetting.facilities)
      defaultFacilities = listingSetting.facilities

    return sortBy(
      filter(defaultFacilities, function (facilityInfo) {
        if (
          facilityInfo &&
          facilityInfo.id &&
          indexOf(facilities, facilityInfo.id) !== -1
        )
          return facilityInfo
      }),
      'name'
    )
  },

  async propertyTypeInfo() {
    const propertyTypeId = this.propertyTypeId || ''
    const settingInfo = await SettingCollection.findOne({})
    let defaultPropertyTypes = []
    if (size(settingInfo) && size(settingInfo.propertyTypes))
      defaultPropertyTypes = settingInfo.propertyTypes
    const propertyType = defaultPropertyTypes.find(
      (item) => item && propertyTypeId === item.id
    )
    return propertyType
  },

  getListingImages() {
    const listingImages = []
    if (size(this.images)) {
      const directive = settings.S3.Directives['Listings']
      const path =
        appHelper.getCDNDomain() + '/' + directive.folder + '/' + this._id + '/'
      each(this.images, function (uploadedImage) {
        const imageName = uploadedImage.imageName
        const originalImage = path + imageName
        listingImages.push({
          originalUrl: originalImage,
          name: imageName,
          _id: imageName,
          title: uploadedImage.title,
          rotate: uploadedImage.rotate ? uploadedImage.rotate : 0
        })
      })
    }

    return listingImages
  },

  async includedInRentDetails() {
    const activeIncludedInRent = this.includedInRent || []
    let defaultIncludedInRent = []
    const listingSetting = await settingHelper.getSettingInfo()

    if (size(listingSetting) && size(listingSetting.includedInRent))
      defaultIncludedInRent = listingSetting.includedInRent

    return sortBy(
      filter(defaultIncludedInRent, function (includedInRentInfo) {
        if (
          includedInRentInfo &&
          includedInRentInfo.id &&
          indexOf(activeIncludedInRent, includedInRentInfo.id) !== -1
        )
          return includedInRentInfo
      }),
      'name'
    )
  },

  async getActiveOrUpcomingContract() {
    const contract = await contractHelper.getAContract(
      {
        propertyId: this._id,
        partnerId: this.partnerId,
        status: { $in: ['active', 'upcoming'] },
        hasRentalContract: true
      },
      null,
      ['tenant']
    )
    return contract
  },

  async getActiveContract() {
    const activeContract = await contractHelper.getAContract({
      propertyId: this._id,
      partnerId: this.partnerId,
      status: 'active',
      hasRentalContract: true
    })
    return activeContract
  },

  async getUpcomingContract() {
    const upcomingContract = await contractHelper.getAContract({
      propertyId: this._id,
      partnerId: this.partnerId,
      status: 'upcoming',
      hasRentalContract: true
    })
    return upcomingContract
  },

  async getIsCPIEnable() {
    const activeContract = await this.getActiveContract()
    if (size(activeContract)) return activeContract.rentalMeta.cpiEnabled
    const upcomingContract = await this.getUpcomingContract()
    if (size(upcomingContract)) return upcomingContract.rentalMeta.cpiEnabled
    return ''
  },

  async getFormattedExportDateForAvailabilityStartDate(partnerSettings) {
    const partnerSettingsOrId = partnerSettings
      ? partnerSettings
      : this.partnerId

    return (
      await appHelper.getActualDate(
        this.partnerId,
        true,
        this.availabilityStartDate
      )
    ).format(await appHelper.getDateFormat(partnerSettingsOrId))
  },

  async getFormattedExportDateForPropertyStartDate() {
    const activeContract = await this.getActiveContract()
    const upcomingContract = await this.getUpcomingContract()

    if (
      this.propertyStatus === 'active' &&
      (activeContract || upcomingContract)
    ) {
      if (size(activeContract)) {
        return await activeContract.getFormattedExportDateForContractStartDate()
      } else {
        return await upcomingContract.getFormattedExportDateForContractStartDate()
      }
    } else {
      return await this.getFormattedExportDateForAvailabilityStartDate()
    }
  },

  async getFormattedExportDateForAvailabilityEndDate(partnerSettings) {
    const partnerSettingsOrId = partnerSettings
      ? partnerSettings
      : this.partnerId

    if (this.availabilityEndDate)
      return (
        await appHelper.getActualDate(
          this.partnerId,
          true,
          this.availabilityEndDate
        )
      ).format(await appHelper.getDateFormat(partnerSettingsOrId))
    else return appHelper.translateToUserLng('labels.unlimited')
  },

  async getFormattedExportDateForPropertyEndDate() {
    const activeContract = await this.getActiveContract()
    const upcomingContract = await this.getUpcomingContract()

    if (
      this.propertyStatus === 'active' &&
      (activeContract || upcomingContract)
    ) {
      let endText = ''
      if (size(activeContract)) {
        endText =
          await activeContract.getFormattedExportDateForContractEndDate()
      } else {
        endText =
          await upcomingContract.getFormattedExportDateForContractEndDate()
      }
      return endText ? endText : 'Undetermined'
    } else {
      return await this.getFormattedExportDateForAvailabilityEndDate()
    }
  },

  async getListingSettings(key) {
    const setting = await SettingCollection.findOne()

    if (key) {
      if (size(setting) && setting[key]) return setting[key]
      else return []
    } else {
      return setting
    }
  },

  async isSoonEnding() {
    //Here checking leaseEndDate by partnerId here work due not completed
    const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
      partnerId: this.partnerId
    })
    let soonEndingMonths = 4
    soonEndingMonths =
      partnerSettings?.propertySettings?.soonEndingMonths || soonEndingMonths

    const selectedMonth = (
      await appHelper.getActualDate(partnerSettings, true)
    ).add(soonEndingMonths, 'months')

    return !!(await contractHelper.getAContract({
      propertyId: this._id,
      'rentalMeta.contractEndDate': { $lte: selectedMonth._d },
      hasRentalContract: true,
      status: 'active'
    }))
  },

  async getMinimumStayByStatus() {
    const activeContract = await this.getActiveContract()
    const upcomingContract = await this.getUpcomingContract()
    if (
      this.propertyStatus === 'active' &&
      (activeContract || upcomingContract)
    ) {
      if (size(activeContract)) {
        return activeContract.contractMinimumStay()
      } else {
        return upcomingContract.contractMinimumStay()
      }
    } else {
      return this.getMinimumStay()
    }
  },

  async getPriceByStatus() {
    const contract = await this.getActiveContract()
    if (this.propertyStatus === 'active' && size(contract)) {
      return contract.getMonthlyRentAmount()
    } else {
      return this.monthlyRentAmount ? this.monthlyRentAmount : 0
    }
  },

  async depositAmountByStatus() {
    const contract = await this.getActiveContract()
    if (this.propertyStatus === 'active' && size(contract)) {
      return contract.getDepositAmount()
    } else {
      return this.depositAmount ? this.depositAmount : 0
    }
  },

  async getPartner() {
    let partnerInfo = {}
    if (this.partnerId)
      partnerInfo = await PartnerCollection.findOne({ _id: this.partnerId })

    return partnerInfo
  },

  async getPropertyAddons() {
    const addons = this.addons || []
    const addonsInfo = []

    if (size(addons)) {
      for (const addon of addons) {
        const addonInfo = await AddonCollection.findOne({
          _id: addon.addonId,
          partnerId: this.partnerId
        })

        const { name = '' } = addonInfo || {}

        if (name) {
          addonsInfo.push({
            addon_name: name,
            addon_price: addon.total
          })
        }
      }
    }

    return addonsInfo
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

  getListingFirstImage(imageSize) {
    const listingFirstImage = {}

    if (size(this.images)) {
      // Default image size
      const { fit = 'min', height = 180, width = 215 } = imageSize || {}
      // Get image directory from settings
      const directive = settings.S3.Directives['Listings']

      listingFirstImage['name'] =
        this.images[0] && this.images[0].imageName
          ? this.images[0].imageName
          : ''
      listingFirstImage['title'] =
        this.images[0] && this.images[0].title ? this.images[0].title : ''

      let listingFullURL =
        appHelper.getCDNDomain() +
        '/' +
        directive.folder +
        '/' +
        this._id +
        '/' +
        listingFirstImage['name'] +
        '?w=' +
        width +
        '&h=' +
        height +
        '&fit=' +
        fit

      if (this.images[0] && this.images[0].rotate)
        listingFullURL =
          listingFullURL + '&rot=' + this.images[0].rotate.toString()

      listingFirstImage['fullUrl'] = listingFullURL
    } else {
      listingFirstImage['name'] = ''
      listingFirstImage['fullUrl'] =
        appHelper.getCDNDomain() +
        '/assets/default-image/property-secondary.png'
      // appHelper.getDefaultLogoURL('no-home-image')
    }

    return listingFirstImage
  },

  getMinimumStay() {
    return this.minimumStay ? this.minimumStay : 0
  },

  async listingTypeInfo() {
    const typeId = this.listingTypeId || ''
    let defaultListingTypes = []
    const listingSetting = (await SettingCollection.findOne({})) || {}
    const { listingTypes = [] } = listingSetting || {}

    if (size(listingTypes)) defaultListingTypes = listingSetting.listingTypes

    return find(defaultListingTypes, (typeInfo) => {
      if (size(typeInfo) && typeInfo.id && typeId === typeInfo.id)
        return typeInfo
    })
  },

  async availabilityStartDateText(partnerSettings) {
    const partnerSettingsOrId = size(partnerSettings)
      ? partnerSettings
      : this.partnerId
    const dateFormat = await appHelper.getDateFormat(partnerSettingsOrId)

    return moment(this.availabilityStartDate).format(dateFormat)
  },

  async availabilityEndDateText(partnerSettings) {
    const partnerSettingsOrId = size(partnerSettings)
      ? partnerSettings
      : this.partnerId

    const dateFormat = await appHelper.getDateFormat(partnerSettingsOrId)

    if (this.availabilityEndDate)
      return moment(this.availabilityEndDate).format(dateFormat)
    else return appHelper.translateToUserLng('labels.unlimited')
  }
}

export const ListingCollection = mongoose.model('listings', ListingSchema)
