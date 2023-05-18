import { clone, extend, find, indexOf, size } from 'lodash'
import mongoose from 'mongoose'
import moment from 'moment-timezone'
import {
  BranchCollection,
  ContractCollection,
  InvoiceSchema,
  ListingCollection,
  PartnerCollection,
  PartnerSettingCollection,
  TenantCollection,
  UserCollection
} from '../models'
import {
  appHelper,
  paymentHelper,
  listingHelper,
  tenantHelper
} from '../helpers'

InvoiceSchema.virtual('account', {
  ref: 'accounts',
  localField: 'accountId',
  foreignField: '_id',
  justOne: true
})

InvoiceSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

InvoiceSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

InvoiceSchema.virtual('partnerSetting', {
  ref: 'partner_settings',
  localField: 'partnerId',
  foreignField: 'partnerId',
  justOne: true
})

InvoiceSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

InvoiceSchema.virtual('tenant', {
  ref: 'tenants',
  localField: 'tenantId',
  foreignField: '_id',
  justOne: true
})

InvoiceSchema.virtual('commissions', {
  ref: 'commissions',
  localField: '_id',
  foreignField: 'invoiceId',
  justOne: false
})

InvoiceSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

InvoiceSchema.methods = {
  getStatusText(userLang) {
    if (!userLang) userLang = 'no'

    let statusText = '',
      tagText = '',
      text = ''

    if (!this.invoiceSent && this.status === 'new')
      statusText = 'common.filters.new'
    else if (!this.invoiceSent && this.status === 'created')
      statusText = 'common.filters.created'
    else if (this && this.status === 'credited')
      statusText = 'common.filters.credited'
    else if (this && this.status === 'lost') statusText = 'common.filters.lost'
    else if (
      !this.isPartiallyPaid &&
      !this.isDefaulted &&
      this.invoiceSent &&
      (this.status === 'new' || this.status === 'created')
    )
      statusText = 'common.filters.sent'
    else if (
      this.status === 'paid' &&
      (this.totalPaid < this.invoiceTotal ||
        this.totalPaid >= this.invoiceTotal)
    )
      statusText = 'common.filters.paid'
    else if (!this.isDefaulted && this.status === 'overdue')
      statusText = 'common.filters.unpaid'
    else if (this.isDefaulted) statusText = 'common.filters.defaulted'
    else if (this && this.status === 'balanced')
      statusText = 'common.filters.balanced'

    if (this.isPartiallyPaid) tagText = 'common.filters.partially_paid'
    else if (this.isOverPaid) tagText = 'common.filters.overpaid'
    else if (this.isPartiallyCredited)
      tagText = 'common.filters.partially_credited'
    else if (this.isPartiallyBalanced)
      tagText = 'common.filters.partially_balanced'

    if (statusText) text = appHelper.translateToUserLng(statusText, userLang)

    if (tagText) {
      text = text ? text + ', ' : ''

      text = text + appHelper.translateToUserLng(tagText, userLang)
    }

    return text
  },

  async getAProperty() {
    if (this && this.propertyId)
      return await listingHelper.getAListing({ _id: this.propertyId })
  },

  async getPayments() {
    const invoiceId = this._id
    const payments = await paymentHelper.getPayments({
      contractId: this.contractId,
      partnerId: this.partnerId,
      propertyId: this.propertyId,
      type: 'payment',
      'invoices.invoiceId': invoiceId
    })
    let totalPayment = 0

    if (size(payments)) {
      //Calculate total payments of invoice
      for (const payment of payments) {
        const paymentInvoices = payment.invoices

        if (size(paymentInvoices)) {
          const findPaymentInvoiceInfo = find(
            paymentInvoices,
            function (paymentInvoice) {
              return paymentInvoice.invoiceId === invoiceId
            }
          )
          if (size(findPaymentInvoiceInfo)) {
            totalPayment += findPaymentInvoiceInfo.amount
            //Extend payment invoice info for each payment
            extend(payment, { paymentInvoice: findPaymentInvoiceInfo })
          }
        }
      }
    }

    return { payments, totalPayment }
  },

  async getTenantName(tenantId) {
    const tenantsInfo = tenantId
      ? await tenantHelper.getATenant({ _id: tenantId })
      : {}
    return tenantsInfo?.name || ''
  },

  getAccountNumber() {
    return this.invoiceAccountNumber || ''
  },

  getKIDNumber() {
    return this.kidNumber ? this.kidNumber : ''
  },

  async getTenant() {
    let tenantInfo = {}
    if (this.tenantId)
      tenantInfo = await TenantCollection.findOne({ _id: this.tenantId })

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

  async getInvoiceContractInfo() {
    let contractInfo = {}
    if (this.contractId && this.partnerId)
      contractInfo = await ContractCollection.findOne({
        _id: this.contractId,
        partnerId: this.partnerId
      })

    return contractInfo
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

  async getNewItemsArray(params) {
    const {
      items = [],
      newItems = [],
      invoiceData = {},
      isAddonMeta = false,
      languageKeyPrefix = '',
      sendToUserLang = ''
    } = params

    const { partnerId = '' } = invoiceData

    if (size(newItems)) {
      for (const itemInfo of newItems) {
        const {
          description = '',
          qty = 3,
          taxPercentage = 0,
          total = 0,
          type = ''
        } = itemInfo

        let itemName =
            type && !isAddonMeta
              ? appHelper.translateToUserLng(
                  languageKeyPrefix + type,
                  sendToUserLang
                )
              : description
              ? description
              : '',
          itemTotal = total,
          itemPrice = taxPercentage ? total / (1 + taxPercentage / 100) : total,
          itemTax = taxPercentage ? (itemPrice * taxPercentage) / 100 : 0

        if (type === 'monthly_rent') {
          const dateFormat = await appHelper.getDateFormat(partnerId)

          const invoiceStartDate = (
            await appHelper.getActualDate(
              partnerId,
              true,
              invoiceData.invoiceStartOn
            )
          ).format(dateFormat)
          const invoiceEndDate = (
            await appHelper.getActualDate(
              partnerId,
              true,
              invoiceData.invoiceEndOn
            )
          ).format(dateFormat)

          itemName += ` ${invoiceStartDate} - ${invoiceEndDate}`
        }

        const convertToCurrencyParams = {
          number: itemPrice,
          partnerSettingsOrId: partnerId,
          showSymbol: false,
          options: { isInvoice: true }
        }
        itemPrice = await appHelper.convertToCurrency(convertToCurrencyParams)

        convertToCurrencyParams.number = itemTax
        itemTax = await appHelper.convertToCurrency(convertToCurrencyParams)

        convertToCurrencyParams.number = itemTotal
        itemTotal = await appHelper.convertToCurrency(convertToCurrencyParams)

        items.push({
          item_name: itemName,
          item_quantity: qty,
          item_price: itemPrice,
          item_tax: itemTax,
          item_total: itemTotal
        })
      }
    }

    return items
  },

  async getInvoiceItems(sendToUserLang, event) {
    const invoiceData = this
    const partnerId = this.partnerId
    let items = []

    if (size(this.invoiceContent) && !this.isCorrectionInvoice) {
      const newItemsArrayParams = {
        items: clone(items),
        newItems: clone(this.invoiceContent),
        invoiceData,
        isAddonMeta: false,
        languageKeyPrefix: 'rent_invoices.',
        sendToUserLang
      }

      items = await this.getNewItemsArray(newItemsArrayParams)
    }
    if (size(this.addonsMeta)) {
      const newItemsArrayParams = {
        items: clone(items),
        newItems: clone(this.addonsMeta),
        invoiceData,
        isAddonMeta: true,
        languageKeyPrefix: '',
        sendToUserLang
      }

      items = await this.getNewItemsArray(newItemsArrayParams)
    }
    if (size(this.commissionsMeta)) {
      const newItemsArrayParams = {
        items: clone(items),
        newItems: clone(this.commissionsMeta),
        invoiceData,
        isAddonMeta: false,
        languageKeyPrefix: 'commissions.types.',
        sendToUserLang
      }

      items = await this.getNewItemsArray(newItemsArrayParams)
    }
    if (size(this.feesMeta)) {
      for (const feeInfo of this.feesMeta) {
        const {
          invoiceId = '',
          original = '',
          qty = 1,
          tax = 0,
          total = 0,
          type = ''
        } = feeInfo

        let feeType = type
            ? appHelper.translateToUserLng(
                'invoice_payments_settings.fees.fields.' + type + '_fee',
                sendToUserLang
              )
            : '',
          feeTotal = total,
          feePrice = tax ? total / (1 + tax / 100) : total,
          feeTax = tax ? (feePrice * tax) / 100 : 0

        if (invoiceId) {
          const invoiceInfo = await InvoiceCollection.findOne({
            _id: invoiceId
          })

          const { invoiceSerialId = '' } = invoiceInfo || {}

          if (invoiceSerialId) {
            feeType =
              feeType +
              ' ' +
              appHelper.translateToUserLng(
                'invoice_payments_settings.fees.of_invoice',
                sendToUserLang
              ) +
              ' #' +
              invoiceSerialId
          }
        }
        const convertToCurrencyParams = {
          number: feePrice,
          partnerSettingsOrId: partnerId,
          showSymbol: false,
          options: { isInvoice: true }
        }

        feePrice = await appHelper.convertToCurrency(convertToCurrencyParams)

        convertToCurrencyParams.number = feeTax
        feeTax = await appHelper.convertToCurrency(convertToCurrencyParams)

        convertToCurrencyParams.number = feeTotal
        feeTotal = await appHelper.convertToCurrency(convertToCurrencyParams)

        const eventList = [
          'send_first_reminder',
          'send_second_reminder',
          'send_collection_notice',
          'send_eviction_notice',
          'send_eviction_due_reminder_notice',
          'send_eviction_due_reminder_notice_without_eviction_fee',
          'eviction_document'
        ]

        if (indexOf(eventList, event) !== -1) {
          items.push({
            item_name: feeType,
            item_quantity: qty,
            item_price: feePrice,
            item_tax: feeTax,
            item_total: feeTotal
          })
        } else {
          if (original) {
            items.push({
              item_name: feeType,
              item_quantity: qty,
              item_price: feePrice,
              item_tax: feeTax,
              item_total: feeTotal
            })
          }
        }
      }
    }

    return items
  },

  async createdDateText() {
    if (this.createdAt) {
      const dateFormat = await appHelper.getDateFormat(this.partnerId)
      return moment(this.createdAt).format(dateFormat)
    } else return ''
  },

  getInvoiceMonth() {
    return this.invoiceMonth ? moment(this.invoiceMonth).format('MMMM') : ''
  },

  getInvoiceYear() {
    return this.invoiceMonth ? moment(this.invoiceMonth).format('YYYY') : ''
  },

  isNotLandlord(invoice) {
    return !!(
      size(invoice) &&
      indexOf(
        ['landlord_invoice', 'landlord_credit_note'],
        invoice.invoiceType
      ) === -1
    )
  },

  async getTotalDueAmount(invoice) {
    if (!size(invoice)) invoice = this
    const {
      creditedAmount = 0,
      invoiceTotal = 0,
      lostMeta = {},
      totalBalanced = 0,
      totalPaid = 0
    } = invoice
    const invoiceLostAmount = lostMeta?.amount || 0
    let dueTotal = 0

    dueTotal = await appHelper.convertTo2Decimal(
      invoiceTotal - totalPaid + creditedAmount - invoiceLostAmount
    )

    if (!this.isNotLandlord(clone(invoice))) {
      dueTotal = await appHelper.convertTo2Decimal(
        invoiceTotal - totalPaid - totalBalanced
      )
    }

    return dueTotal
  },

  getInvoiceId() {
    return this.invoiceSerialId
  },

  async getFirstReminderDate() {
    const dateFormat = await appHelper.getDateFormat(this.partnerId)

    if (this && this.firstReminderSentAt)
      return moment(this.firstReminderSentAt).format(dateFormat)
    else return ''
  },

  async getSecondReminderDate() {
    const dateFormat = await appHelper.getDateFormat(this.partnerId)

    if (this && this.secondReminderSentAt)
      return moment(this.secondReminderSentAt).format(dateFormat)
    else return ''
  },

  async getCollectionNoticeDate() {
    const dateFormat = await appHelper.getDateFormat(this.partnerId)

    if (this && this.collectionNoticeSentAt)
      return moment(this.collectionNoticeSentAt).format(dateFormat)
    else return ''
  },

  getInvoiceReminderFee() {
    if (size(this.feesMeta)) {
      const reminderFee = find(
        this.feesMeta,
        (feeMeta) => feeMeta.type === 'reminder' && !feeMeta.original
      )

      if (size(reminderFee)) return reminderFee.total
      else return ''
    } else return ''
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

  async getInternalLeaseId() {
    if (this && this.contractId && this.partnerId) {
      const contractInfo = await ContractCollection.findOne({
        _id: this.contractId,
        partnerId: this.partnerId
      })
      const { rentalMeta = {} } = contractInfo || {}
      const { internalLeaseId = '' } = rentalMeta
      return internalLeaseId
    } else return ''
  },

  async getEstimatedPayouts() {
    if (this && this.contractId && this.partnerId) {
      const contractInfo =
        (await ContractCollection.findOne({
          _id: this.contractId,
          partnerId: this.partnerId
        })) || {}
      const { rentalMeta = {} } = contractInfo || {}
      const { estimatedPayouts = {} } = rentalMeta

      return estimatedPayouts
    } else return ''
  },

  async getFirstMonthBankAccountNumber() {
    const partnerId = this.partnerId
    const partnerSettings = partnerId
      ? await PartnerSettingCollection.findOne({ partnerId })
      : {}
    const { bankPayment = {} } = partnerSettings || {}
    const { firstMonthACNo = '' } = bankPayment

    return firstMonthACNo
  }
}

export const InvoiceCollection = mongoose.model('invoices', InvoiceSchema)
