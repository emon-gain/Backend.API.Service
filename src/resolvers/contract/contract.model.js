import mongoose from 'mongoose'
import jsonStat from 'jsonstat-toolkit'
import { find, includes, last, map, size } from 'lodash'
import {
  BranchCollection,
  ContractSchema,
  ListingCollection,
  PartnerCollection,
  PartnerSettingCollection,
  TenantCollection,
  UserCollection
} from '../models'
import {
  appHelper,
  commissionHelper,
  contractHelper,
  invoiceHelper,
  partnerSettingHelper,
  settingHelper
} from '../helpers'

ContractSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('partnerSetting', {
  ref: 'partner_settings',
  localField: 'partnerId',
  foreignField: 'partnerId',
  justOne: true
})

ContractSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('propertyItems', {
  ref: 'property_items',
  localField: '_id',
  foreignField: 'contractId',
  justOne: false
})

ContractSchema.virtual('branch', {
  ref: 'branches',
  localField: 'branchId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('agent', {
  ref: 'users',
  localField: 'agentId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'rentalMeta.tenantId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('propertyRepresentative', {
  ref: 'users',
  localField: 'representativeId',
  foreignField: '_id',
  justOne: true
})

ContractSchema.virtual('depositInsurance', {
  ref: 'deposit_insurance',
  localField: '_id',
  foreignField: 'contractId',
  justOne: true
})

ContractSchema.methods = {
  isAllTenantSignCompleted() {
    const tenantLeaseSigningStatus =
      this.rentalMeta && size(this.rentalMeta.tenantLeaseSigningStatus)
        ? this.rentalMeta.tenantLeaseSigningStatus
        : []
    const tenantsSignArray = size(tenantLeaseSigningStatus)
      ? map(tenantLeaseSigningStatus, 'signed')
      : []
    return !(size(tenantsSignArray) && includes(tenantsSignArray, false))
  },

  isAllSignCompleted() {
    const rentalMeta = this.rentalMeta ? this.rentalMeta : {}
    const { enabledLeaseEsigning } = rentalMeta
    const landlordSigned =
      rentalMeta.landlordLeaseSigningStatus &&
      rentalMeta.landlordLeaseSigningStatus.signed
    const isAllTenantSigned = this.isAllTenantSignCompleted()
    return !!(
      rentalMeta &&
      enabledLeaseEsigning &&
      landlordSigned &&
      isAllTenantSigned
    )
  },

  getMonthlyRentAmount() {
    if (this.rentalMeta && this.rentalMeta.monthlyRentAmount) {
      const contractMonthlyRentAmount = this.rentalMeta.monthlyRentAmount
      return contractMonthlyRentAmount
    }
    return 0
  },

  cpiEnabled() {
    if (this.rentalMeta && this.rentalMeta.cpiEnabled) {
      const cpiEnable = this.rentalMeta.cpiEnabled
      return cpiEnable
    }
    return false
  },

  async getStartingAndEndingDate(partnerSettingsInfo, cpiSettlementMonthIndex) {
    const actualNextCpiDate = (
      await appHelper.getActualDate(
        partnerSettingsInfo,
        true,
        this.rentalMeta.nextCpiDate
      )
    ).format('YYYY-MM')
    const nextCpiDate =
      this.rentalMeta && this.rentalMeta.nextCpiDate ? actualNextCpiDate : ''
    const actualLastCpiDateWithSubtract = (
      await appHelper.getActualDate(
        partnerSettingsInfo,
        true,
        this.rentalMeta.lastCpiDate
      )
    )
      .subtract(cpiSettlementMonthIndex, 'months')
      .format('YYYY-MM')
    const actualLastCpiDate = (
      await appHelper.getActualDate(
        partnerSettingsInfo,
        true,
        this.rentalMeta.lastCpiDate
      )
    ).format('YYYY-MM')
    const lastCpiDateWithoutCpiSettle =
      this.rentalMeta && this.rentalMeta.lastCpiDate ? actualLastCpiDate : ''
    const lastCpiDate =
      this.rentalMeta && this.rentalMeta.lastCpiDate && cpiSettlementMonthIndex
        ? actualLastCpiDateWithSubtract
        : lastCpiDateWithoutCpiSettle
    const from = lastCpiDate ? lastCpiDate.split('-').join('M') : ''
    const till = nextCpiDate ? nextCpiDate.split('-').join('M') : ''
    console.log('get From And Till value', {
      lastCpiDate,
      nextCpiDate
    })
    return {
      from,
      till
    }
  },

  async getCPINextMonthlyRentAmount() {
    const settings = await settingHelper.getSettingInfo()
    let tbl = null
    if (settings && size(settings.cpiDataSet)) {
      const ds = jsonStat(settings.cpiDataSet)
      let TidIndex = {}
      const cpiEnable = this.cpiEnabled()
      const rentAmount = this.getMonthlyRentAmount()
      const amount = rentAmount ? rentAmount : 0
      const partnerSettingsInfo =
        await partnerSettingHelper.getSettingByPartnerId(this.partnerId)
      let cpiSettlementMonthIndex = 0
      if (
        partnerSettingsInfo &&
        partnerSettingsInfo.CPISettlement &&
        partnerSettingsInfo.CPISettlement.months
      ) {
        cpiSettlementMonthIndex = partnerSettingsInfo.CPISettlement.months
      }
      const getFromAndTill = await this.getStartingAndEndingDate(
        partnerSettingsInfo,
        cpiSettlementMonthIndex
      )
      const { from } = getFromAndTill
      let { till } = getFromAndTill
      if (
        settings.cpiDataSet.dataset &&
        settings.cpiDataSet.dataset.dimension.Tid &&
        settings.cpiDataSet.dataset.dimension.Tid.category &&
        settings.cpiDataSet.dataset.dimension.Tid.category.index
      ) {
        TidIndex = settings.cpiDataSet.dataset.dimension.Tid.category.index
      }
      if (size(TidIndex) && !TidIndex[till]) {
        const tidIndexKeys = Object.keys(TidIndex)
        if (size(tidIndexKeys)) {
          till = last(tidIndexKeys)
        }
      }
      if (cpiEnable && from && till && amount && ds) {
        // eslint-disable-next-line new-cap
        const indexFrom = ds.Dataset(0).Data({
          Konsumgrp: 'TOTAL',
          Tid: from,
          ContentsCode: 'KpiIndMnd'
        }).value // Jsontat
        // eslint-disable-next-line new-cap
        const indexTill = ds.Dataset(0).Data({
          Konsumgrp: 'TOTAL',
          Tid: till,
          ContentsCode: 'KpiIndMnd'
        }).value // Jsontat
        const updatedValue = amount * (indexTill / indexFrom)
        if (updatedValue) {
          tbl = updatedValue.toFixed(2)
        }
      }
    }
    const totalBill = tbl ? Math.round(tbl) : 0
    return totalBill
  },

  getAssignmentNumber(serialParam = null) {
    let assignmentSerial = this.assignmentSerial ? this.assignmentSerial : ''
    if (serialParam) {
      assignmentSerial = serialParam
    }
    const partnerSerial =
      this.partner && this.partner.serial ? this.partner.serial : ''
    const propertySerial =
      this.property && this.property.serial ? this.property.serial : ''
    if (partnerSerial && propertySerial && assignmentSerial) {
      return (
        appHelper.getFixedDigits(partnerSerial, 4) +
        appHelper.getFixedDigits(propertySerial, 5) +
        appHelper.getFixedDigits(assignmentSerial, 3)
      )
    }
  },

  getInternalAssignmentId() {
    return this.internalAssignmentId ? this.internalAssignmentId : ''
  },

  getInternalLeaseId() {
    const rentalMeta = this.rentalMeta || {}
    const { internalLeaseId = '' } = rentalMeta
    return internalLeaseId
  },

  isJointlyLiable() {
    return this.rentalMeta && this.rentalMeta.enabledJointlyLiable
      ? this.rentalMeta.enabledJointlyLiable
      : false
  },

  async getFormattedExportDateForContractStartDate() {
    return this.rentalMeta && this.rentalMeta.contractStartDate
      ? await appHelper.getFormattedExportDate(
          this.partnerId,
          this.rentalMeta.contractStartDate
        )
      : ''
  },

  async getFormattedExportDateForContractEndDate() {
    return this.rentalMeta && this.rentalMeta.contractEndDate
      ? await appHelper.getFormattedExportDate(
          this.partnerId,
          this.rentalMeta.contractEndDate
        )
      : ''
  },

  getIsVatEnable() {
    return this.rentalMeta?.isVatEnable
  },

  async getBrokeringCommission() {
    let total = 0

    if (this._id) {
      const invoiceIds = map(
        await invoiceHelper.getInvoices({ contractId: this._id }),
        '_id'
      )
      if (size(invoiceIds)) {
        const commissions = await commissionHelper.getCommissions({
          type: 'brokering_contract',
          invoiceId: { $in: invoiceIds }
        })
        for (const commission of commissions) {
          if (commission) {
            total += commission.amount || 0
          }
        }
      }
    }
    return total
  },

  getAssignmentAddonCommission() {
    let total = 0

    if (size(this.addons)) {
      for (const addon of this.addons) {
        if (addon && addon.type === 'assignment') {
          total += addon.total || 0
        }
      }
    }

    return total
  },

  contractMinimumStay() {
    return this.rentalMeta?.minimumStay || 0
  },

  getDepositAmount() {
    return this.rentalMeta && this.rentalMeta.depositAmount
      ? this.rentalMeta.depositAmount
      : 0
  },

  async getTenant() {
    let tenantInfo = {}
    if (size(this.rentalMeta) && this.rentalMeta.tenantId)
      tenantInfo = await TenantCollection.findOne({
        _id: this.rentalMeta.tenantId
      })

    return tenantInfo
  },

  async getPartner() {
    let partnerInfo = {}
    if (this.partnerId)
      partnerInfo = await PartnerCollection.findOne({ _id: this.partnerId })

    return partnerInfo
  },

  async getProperty() {
    let propertyInfo = {}

    if (this.propertyId)
      propertyInfo = await ListingCollection.findOne({ _id: this.propertyId })

    return propertyInfo
  },

  async getPropertyAddons() {
    let propertyAddons = []

    const property = await this.getProperty()
    if (size(property)) propertyAddons = await property.getPropertyAddons()

    return propertyAddons
  },

  tenants() {
    return this.rentalMeta && this.rentalMeta.tenants
      ? this.rentalMeta.tenants
      : []
  },

  async getTenantsItems() {
    const tenants = this.tenants()
    const tenantsItems = []

    if (!size(tenants)) return tenantsItems

    const propertyInfo = this.propertyId
      ? await ListingCollection.findOne({ _id: this.propertyId })
      : {}

    for (const tenant of tenants) {
      const tenantInfo = await TenantCollection.findOne({
        _id: tenant.tenantId
      })

      if (size(tenantInfo)) {
        let tenantUserEmail = ''
        let tenantUserPhone = ''
        let tenantUserNID = ''
        const { location: propertyLocation = {} } = propertyInfo || {}

        const {
          billingAddress = '',
          city = '',
          country = '',
          name = '',
          serial = '',
          zipCode = ''
        } = tenantInfo

        const tenantUserInfo = await tenantInfo.getUser()

        if (size(tenantUserInfo)) {
          tenantUserEmail = tenantUserInfo.getEmail() || ''
          tenantUserPhone = tenantUserInfo.getPhone() || ''
          tenantUserNID = tenantUserInfo.getNorwegianNationalIdentification()
        }

        const tenant_name = name
        const tenant_id = serial
        const tenant_address = billingAddress
          ? billingAddress
          : propertyLocation.name
        const tenant_zip_code = billingAddress
          ? zipCode
          : propertyLocation.postalCode
        const tenant_city = billingAddress ? city : propertyLocation.city
        const tenant_country = billingAddress
          ? country
          : propertyLocation.country
        const tenant_email = tenantUserEmail
        const tenant_phonenumber = tenantUserPhone
        const tenant_person_id = tenantUserNID

        tenantsItems.push({
          tenant_id,
          tenant_name,
          tenant_address,
          tenant_zip_code,
          tenant_city,
          tenant_country,
          tenant_email,
          tenant_phonenumber,
          tenant_person_id
        })
      }
    }

    return tenantsItems
  },

  async getEvictionFee() {
    if (this && this.partnerId) {
      const partnerSettingInfo = await PartnerSettingCollection.findOne({
        partnerId: this.partnerId
      })

      const { evictionFee = {} } = partnerSettingInfo || {}

      return evictionFee.amount || 0
    }

    return 0
  },

  async getAdministrationEvictionFee() {
    if (this && this.partnerId) {
      const partnerSettingInfo = await PartnerSettingCollection.findOne({
        partnerId: this.partnerId
      })

      const { administrationEvictionFee = {} } = partnerSettingInfo || {}

      return administrationEvictionFee.amount || 0
    }

    return 0
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

  async getLeaseSerialNumber() {
    if (this.leaseSerial && this._id) {
      const serial = await contractHelper.getLeaseNumber(this)
      return serial ? '#' + serial : ''
    }
  },

  async getAssignmentSerialNumber() {
    if (this.assignmentSerial && this._id) {
      const serial = await contractHelper.getAssignmentNumber(this)
      return serial ? '#' + serial : ''
    }
  },

  async getContractLastCpiDate() {
    const rentalMeta = this.rentalMeta
    const lastCpiDate = size(rentalMeta) ? rentalMeta.lastCpiDate : ''
    if (lastCpiDate) {
      const dateFormat = await appHelper.getDateFormat(this.partnerId)

      const actualLastCpiDate = (
        await appHelper.getActualDate(this.partnerId, true, lastCpiDate)
      ).format(dateFormat)

      return actualLastCpiDate || ''
    } else return ''
  },

  async getContractNextCpiDate() {
    const rentalMeta = this.rentalMeta
    const nextCpiDate = size(rentalMeta) ? rentalMeta.nextCpiDate : ''
    if (nextCpiDate) {
      const dateFormat = await appHelper.getDateFormat(this.partnerId)

      const actualNextCpiDate = (
        await appHelper.getActualDate(this.partnerId, true, nextCpiDate)
      ).format(dateFormat)

      return actualNextCpiDate || ''
    } else return ''
  },

  async getTenantLeaseEsigningUrl(tenantId) {
    const tenantLeaseSigningStatus =
      this.rentalMeta && size(this.rentalMeta.tenantLeaseSigningStatus)
        ? this.rentalMeta.tenantLeaseSigningStatus
        : []
    let internalUrl = ''
    let tenantLeaseSigningUrl = ''

    if (size(tenantLeaseSigningStatus)) {
      if (size(tenantLeaseSigningStatus) > 1) {
        const tenantLeaseSigningObj = tenantId
          ? find(tenantLeaseSigningStatus, {
              tenantId
            })
          : {}
        internalUrl =
          size(tenantLeaseSigningObj) && tenantLeaseSigningObj.internalUrl
            ? tenantLeaseSigningObj.internalUrl
            : ''
      } else {
        const tenantLeaseSigningObj = tenantLeaseSigningStatus[0]
        internalUrl =
          size(tenantLeaseSigningObj) && tenantLeaseSigningObj.internalUrl
            ? tenantLeaseSigningObj.internalUrl
            : ''
      }

      tenantLeaseSigningUrl =
        appHelper.getLinkServiceURL() +
        '/e-signing/tenant_lease/' +
        this._id +
        '/' +
        internalUrl
    }

    return tenantLeaseSigningUrl
  },

  async getFirstMonthBankAccountNumber() {
    const partnerId = this.partnerId
    const partnerSettings = partnerId
      ? await PartnerSettingCollection.findOne({ partnerId })
      : {}
    const { bankPayment = {} } = partnerSettings || {}
    const { firstMonthACNo = '' } = bankPayment

    return firstMonthACNo
  },

  async representative() {
    if (this.representativeId) {
      const userInfo =
        (await UserCollection.findOne({ _id: this.representativeId })) || {}
      return userInfo
    }
  }
}

export const ContractCollection = mongoose.model('contracts', ContractSchema)
