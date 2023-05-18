import {
  assign,
  assignIn,
  clone,
  compact,
  difference,
  each,
  find,
  includes,
  indexOf,
  isBoolean,
  isEqual,
  isNumber,
  last,
  map,
  omit,
  pick,
  size,
  sortBy,
  round,
  union
} from 'lodash'
import moment from 'moment-timezone'
import validate from 'validate-norwegian-ssn'

import {
  ContractCollection,
  InvoiceCollection,
  InvoicePaymentCollection,
  ListingCollection,
  PayoutCollection,
  PropertyItemCollection,
  PropertyRoomCollection,
  SettingCollection
} from '../models'
import {
  accountHelper,
  addonHelper,
  appHelper,
  appQueueHelper,
  contractHelper,
  dashboardHelper,
  depositAccountHelper,
  invoiceHelper,
  invoicePaymentHelper,
  listingHelper,
  logHelper,
  partnerHelper,
  partnerSettingHelper,
  payoutHelper,
  tenantHelper,
  userHelper
} from '../helpers'

import { counterService, invoiceService } from '../services'
import { CustomError } from '../common'
import { checkRequiredFields } from '../app/app.helper'

export const getContractDataWithFile = async (
  contractId,
  tenantId,
  session
) => {
  const contract = await ContractCollection.aggregate()
    .match({ _id: contractId })
    .addFields({ tenantId })
    .project({
      partnerId: 1,
      tenantId: 1,
      rentalMeta: 1,
      idfyLeaseDocId: 1,
      tenantLeaseSigningStatus: '$rentalMeta.tenantLeaseSigningStatus',
      tenantSignerInfo: {
        $first: {
          $filter: {
            input: '$rentalMeta.tenantLeaseSigningStatus',
            as: 'item',
            cond: { $eq: ['$$item.tenantId', '$tenantId'] }
          }
        }
      }
    })
    .project({
      tenantId: 1,
      attachmentFileId: '$tenantSignerInfo.attachmentFileId',
      idfyLeaseDocId: 1,
      tenantSignerInfo: 1,
      tenantLeaseSigningStatus: 1,
      partnerId: 1,
      rentalMeta: 1
    })
    .lookup({
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    })
    .lookup({
      from: 'files',
      localField: 'attachmentFileId',
      foreignField: '_id',
      as: 'file'
    })
    .unwind('$file')
    .unwind('$partner')
    .lookup({
      from: 'users',
      localField: 'partner.ownerId',
      foreignField: '_id',
      as: 'owner'
    })
    .addFields({
      owner: {
        $first: '$owner'
      }
    })
    .project({
      attachmentFileId: 1,
      partner: 1,
      tenantSignerInfo: 1,
      idfyLeaseDocId: 1,
      tenantLeaseSigningStatus: 1,
      userLang: '$owner.profile.language',
      'file.type': '$file.type',
      'file.directive': '$file.directive',
      'file.context': '$file.context',
      'file.name': '$file.name'
    })
    .session(session)
  return contract || []
}

export const getTenantIdForDepositAccountSubmit = (contractId) =>
  ContractCollection.aggregate([
    {
      $match: { _id: contractId }
    },
    {
      $addFields: {
        tenants: '$rentalMeta.tenants'
      }
    },
    {
      $unwind: '$tenants'
    },
    {
      $group: {
        _id: '$_id',
        tenants: {
          $push: '$tenants.tenantId'
        },
        rentalMeta: {
          $first: '$rentalMeta'
        }
      }
    },
    {
      $addFields: {
        tenants: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$rentalMeta.enabledJointlyLiable', true] },
                { $eq: ['$rentalMeta.enabledJointDepositAccount', false] }
              ]
            },
            then: '$tenants',
            else: ['$rentalMeta.tenantId']
          }
        },
        rentalMeta: '$$REMOVE'
      }
    }
  ])

export const contractDataForCpiSettlement = async (contractId) => {
  const contractPipeline = cpiSettlementPipeline(contractId)
  const contract = (await ContractCollection.aggregate(contractPipeline)) || []
  if (!size(contract)) throw new CustomError(404, 'Contract not found')
  const settings = await SettingCollection.findOne({})
  if (!size(settings)) throw new CustomError(404, 'Settings not found')
  contract[0].cpiDataSet = settings.cpiDataSet
  if (!size(contract[0])) throw new CustomError(404, 'Contract not found')
  return contract[0]
}

const cpiSettlementPipeline = (contractId) => [
  {
    $match: {
      _id: contractId
    }
  },
  {
    $lookup: {
      from: 'partners',
      foreignField: '_id',
      localField: 'partnerId',
      as: 'partner'
    }
  },
  {
    $lookup: {
      from: 'partner_settings',
      foreignField: 'partnerId',
      localField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: '$partner'
  },
  {
    $unwind: '$partnerSettings'
  }
]

export const getContractPropertyIds = async (query = {}) => {
  const propertyIds = await ContractCollection.distinct('propertyId', query)
  return propertyIds || []
}

export const getUniqueFieldValue = async (field, query) =>
  (await ContractCollection.distinct(field, query)) || []

export const getAContract = async (query, session, populate = []) => {
  const contract = await ContractCollection.findOne(query)
    .populate(populate)
    .session(session)
  return contract
}

export const getAContractForVariablesData = async (query, session) => {
  const populationsData = [
    {
      path: 'account',
      populate: [
        {
          path: 'organization'
        },
        {
          path: 'person'
        }
      ]
    },
    {
      path: 'agent'
    },
    {
      path: 'depositInsurance'
    },
    {
      path: 'partner',
      populate: {
        path: 'partnerSetting'
      }
    },
    {
      path: 'branch'
    },
    {
      path: 'property'
    },
    {
      path: 'propertyRepresentative'
    },
    {
      path: 'tenant',
      populate: {
        path: 'user'
      }
    }
  ]
  const contract = await ContractCollection.findOne(query)
    .populate(populationsData)
    .session(session)
  return contract
}

export const prepareLandlordCreditNoteQuery = (contractId) => ({
  contractId,
  status: 'balanced',
  invoiceType: 'landlord_credit_note'
})

export const findCommissionByType = (commissionsMeta) => {
  if (size(commissionsMeta)) {
    return find(
      commissionsMeta,
      (commission) => commission.type === 'brokering_contract'
    )
  }
  return {}
}

export const getOldCommissionTotal = async (contractId) => {
  const landlordCreditNoteQuery = prepareLandlordCreditNoteQuery(contractId)
  const landlordCreditNote = await invoiceHelper.getLandLordInvoiceOrCreditNote(
    landlordCreditNoteQuery
  )
  const commissionTotal = getBrokeringCommissionTotal(landlordCreditNote)
  return commissionTotal * -1 // Landlord credit note has minus amount of commissionTotal, we need positive commissionTotal
}

export const prepareLandlordInvoiceQuery = (contractId) => ({
  contractId,
  status: 'balanced',
  invoiceType: 'landlord_invoice'
})

export const getBrokeringCommissionTotal = (invoiceInfo) => {
  const { commissionsMeta } = invoiceInfo || []
  const brokeringCommission = findCommissionByType(commissionsMeta)
  const { total = 0 } = brokeringCommission
  return total
}

export const getNewCommissionTotal = async (contractId) => {
  const landlordInvoiceQuery = prepareLandlordInvoiceQuery(contractId)
  const landlordInvoice = await invoiceHelper.getLandLordInvoiceOrCreditNote(
    landlordInvoiceQuery
  )
  return getBrokeringCommissionTotal(landlordInvoice)
}

export const getPreviouslyUpdatedDate = (contract, name) => {
  const { history = [] } = contract || {}
  const previousDataForName = find(history, (data) => data.name === name)
  const { newUpdatedAt = '' } = previousDataForName || {}
  return newUpdatedAt
}

export const getAddonsTotal = (addons) => {
  let total = 0
  if (size(addons)) {
    each(addons, (addon) => {
      if (addon && addon.type === 'assignment') {
        total += addon.total || 0
      }
    })
  }
  return total
}

export const prepareTotalIncomeChangeLog = async (
  contract,
  commissions,
  partnerSetting
) => {
  const { oldCommissionTotal, newCommissionTotal } = commissions
  const { addons, createdAt } = contract
  const oldValue = Number(oldCommissionTotal)
  let newValue = Number(newCommissionTotal)
  let commissionDifference = 0
  let isTotalIncomeIncreased = false
  if (oldValue < newValue) {
    isTotalIncomeIncreased = true
    commissionDifference = newValue - oldValue
  } else {
    commissionDifference = oldValue - newValue
  }
  newValue = Number(newValue + getAddonsTotal(addons) || 0)
  const previousTotalIncome = isTotalIncomeIncreased
    ? newValue - commissionDifference
    : newValue + commissionDifference
  const previouslyUpdatedAt = getPreviouslyUpdatedDate(contract, 'total_income')
  const oldUpdatedAt = previouslyUpdatedAt
    ? previouslyUpdatedAt
    : await appHelper.getActualDate(partnerSetting, false, createdAt)
  return {
    name: 'total_income',
    oldValue: previousTotalIncome,
    oldUpdatedAt,
    newValue,
    newUpdatedAt: await appHelper.getActualDate(partnerSetting, false)
  }
}

export const prepareHistoryAndNames = async (params) => {
  const { contract, newCommissionTotal, oldCommissionTotal } = params
  const { partnerId, createdAt } = contract
  const previouslyUpdatedAt = getPreviouslyUpdatedDate(contract, 'commissions')
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const oldUpdatedAt = previouslyUpdatedAt
    ? previouslyUpdatedAt
    : await appHelper.getActualDate(partnerSetting, false, createdAt)
  const history = []
  const names = ['commissions']
  const commissionChangeLog = {
    name: 'commissions',
    oldUpdatedAt,
    oldValue: oldCommissionTotal,
    newValue: newCommissionTotal,
    newUpdatedAt: await appHelper.getActualDate(partnerSetting, false)
  }
  history.push(commissionChangeLog)
  const totalIncomeChangeLog = await prepareTotalIncomeChangeLog(
    contract,
    {
      oldCommissionTotal,
      newCommissionTotal
    },
    partnerSetting
  )
  if (size(totalIncomeChangeLog)) {
    history.push(totalIncomeChangeLog)
    names.push('total_income')
  }
  return { history, names }
}

export const getContracts = async (query, session, populate = []) => {
  const contracts = await ContractCollection.find(query)
    .populate(populate)
    .session(session)
  return contracts
}

export const getContractById = async (id, session) => {
  const contract = await ContractCollection.findById(id)
    .session(session)
    .populate(['partner', 'property'])
  return contract
}

export const getUpcomingContractIdByPropertyId = async (
  partnerId,
  propertyId,
  session
) => {
  const query = {
    partnerId,
    propertyId,
    status: 'upcoming'
  }

  const contract = await ContractCollection.findOne(query).session(session)
  return contract ? contract._id : ''
}

export const getRemainingDaysOfAContract = async (contractId) => {
  if (contractId) {
    const contract = await getAContract({
      _id: contractId,
      'rentalMeta.contractEndDate': { $exists: true }
    })
    if (size(contract) && contract.partnerId) {
      const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
        partnerId: contract.partnerId
      })
      const today = (
        await appHelper.getActualDate(partnerSetting, true, null)
      ).endOf('day')
      const endDate = (
        await appHelper.getActualDate(
          partnerSetting,
          true,
          contract.rentalMeta.contractEndDate
        )
      ).endOf('day')
      return endDate.diff(today, 'days')
    }
  }
  return ''
}

export const hasEvictionCase = async (invoice, session) => {
  if (!invoice) {
    return false
  }
  const { contractId, partnerId } = invoice
  const query = {
    _id: contractId,
    partnerId,
    evictionCases: {
      $elemMatch: { status: { $nin: ['canceled', 'completed'] } }
    }
  }
  const hasEvictionCase = await getAContract(query, session)
  return !!hasEvictionCase
}

export const getInvoicesForEvictionCase = async (invoice = {}, session) => {
  const { contractId, partnerId, propertyId } = invoice
  const query = {
    contractId,
    partnerId,
    propertyId,
    invoiceType: 'invoice',
    status: { $nin: ['paid', 'credited', 'lost'] },
    evictionDueReminderNoticeSentOn: { $exists: true },
    evictionDueReminderSent: true
  }
  const invoices = await invoiceHelper.getInvoices(query, session)
  return invoices
}

export const isCreateEvictionPackage = async (partnerId, session) => {
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId,
    session
  )
  if (!partnerSetting) {
    return false
  }
  const { evictionDueReminderNotice = {} } = partnerSetting
  const { isCreateEvictionPackage } = evictionDueReminderNotice
  return !!isCreateEvictionPackage
}

export const getEvictionCaseAmount = (invoices = []) => {
  let caseAmount = 0
  for (const invoice of invoices) {
    const { invoiceTotal = 0 } = invoice
    caseAmount += invoiceTotal
  }
  return caseAmount
}

export const prepareEvictionCaseData = async (data, session) => {
  const { invoice, invoicesForEvictionCase } = data
  const { contractId, agentId, tenantId, tenants } = invoice
  const contract = await getContractById(contractId, session)
  if (!contract) {
    return false
  }
  const { leaseSerial, rentalMeta = {} } = contract
  const { contractStartDate, contractEndDate, firstInvoiceDueDate, dueDate } =
    rentalMeta
  const evictionCaseData = {
    invoiceId: invoice._id,
    agentId,
    tenantId,
    tenants,
    leaseSerial,
    contractStartDate,
    contractEndDate,
    firstInvoiceDueDate,
    dueDate
  }
  evictionCaseData.status = 'new'
  evictionCaseData.evictionInvoiceIds = invoicesForEvictionCase.map(
    (invoiceInfo) => invoiceInfo._id
  )
  evictionCaseData.amount = getEvictionCaseAmount(invoicesForEvictionCase)
  return evictionCaseData
}

export const prepareContractAddData = async (property, session) => {
  const { branchId, agentId, partnerId, accountId, _id, createdAt, ownerId } =
    property
  return {
    branchId,
    agentId,
    partnerId,
    accountId,
    propertyId: _id,
    status: 'upcoming',
    hasBrokeringContract: false,
    hasRentalManagementContract: false,
    hasRentalContract: false,
    createdAt,
    createdBy: ownerId,
    assignmentSerial: await counterService.incrementCounter(
      `assignment-${_id}`,
      session
    ),
    rentalMeta: {
      status: 'new'
    }
  }
}

export const prepareQueryForActiveContract = (listing) => {
  const { _id, partnerId } = listing
  return {
    propertyId: _id,
    partnerId,
    status: { $in: ['active', 'upcoming'] }
  }
}

export const getFullLocationName = (listing) => {
  const { location = '' } = listing
  const { name = '' } = location
  let { postalCode = '', city = '', country = '' } = location

  postalCode = postalCode ? `, ${postalCode}` : ''
  city = city ? `, ${city}` : ''
  country = country ? `, ${country}` : ''

  return name + postalCode + city + country
}

export const isLocationChanged = (updatedListing, previousListing) => {
  const currentLocationName = getFullLocationName(updatedListing)
  const previousLocationName = getFullLocationName(previousListing)
  return currentLocationName !== previousLocationName
}

export const prepareLocationHistory = async (params) => {
  const { contract, previousListing, updatedListing } = params
  const { partnerId } = updatedListing
  const currentLocationName = getFullLocationName(updatedListing)
  const previousLocationName = getFullLocationName(previousListing)
  const previouslyUpdatedAt = getPreviouslyUpdatedDate(contract, 'address')
  const history = {
    name: 'address',
    oldValue: previousLocationName,
    oldUpdatedAt: previouslyUpdatedAt
      ? previouslyUpdatedAt
      : await appHelper.getActualDate(
          partnerId,
          false,
          previousListing.createdAt
        ),
    newValue: currentLocationName,
    newUpdatedAt: await appHelper.getActualDate(partnerId, false)
  }
  return history
}

export const getGnrBnr = (listing) => {
  const { gnr = '', bnr = '', snr = '' } = listing
  let text = ''
  if (gnr) {
    text += `gnr-${gnr} `
  } else {
    text += 'gnr- '
  }
  if (bnr) {
    text += `bnr-${bnr} `
  } else {
    text += 'bnr- '
  }
  if (snr) {
    text += `snr-${snr}`
  } else {
    text += 'snr- '
  }
  return text
}

export const isGnrBnrChanged = (updatedListing, previousListing) => {
  const currentGnrBnr = getGnrBnr(updatedListing)
  const previousGnrBnr = getGnrBnr(previousListing)
  return currentGnrBnr !== previousGnrBnr
}

export const prepareGnrBnrHistory = async (params) => {
  const { contract, previousListing, updatedListing } = params
  const { partnerId } = updatedListing
  const currentGnrBnr = getGnrBnr(updatedListing)
  const previousGnrBnr = getGnrBnr(previousListing)
  const previouslyUpdatedAt = getPreviouslyUpdatedDate(contract, 'gnr_bnr_snr')
  const history = {
    name: 'gnr_bnr_snr',
    oldValue: previousGnrBnr,
    oldUpdatedAt: previouslyUpdatedAt
      ? previouslyUpdatedAt
      : await appHelper.getActualDate(
          partnerId,
          false,
          previousListing.createdAt
        ),
    newValue: currentGnrBnr,
    newUpdatedAt: await appHelper.getActualDate(partnerId, false)
  }
  return history
}

export const isListingTypeChanged = (updatedListing, previousListing) =>
  previousListing &&
  previousListing.listingTypeId !== updatedListing.listingTypeId

export const isPropertyTypeChanged = (updatedListing, previousListing) =>
  previousListing &&
  previousListing.propertyTypeId !== updatedListing.propertyTypeId

export const prepareCommonTypeHistory = async (params) => {
  const { contract, name, previousListing, updatedListing } = params
  const { partnerId } = updatedListing
  const previouslyUpdatedAt = getPreviouslyUpdatedDate(contract, name)
  const history = {
    name,
    oldValue:
      name === 'listing_type'
        ? previousListing.listingTypeId
        : previousListing.propertyTypeId,
    oldUpdatedAt: previouslyUpdatedAt
      ? previouslyUpdatedAt
      : await appHelper.getActualDate(
          partnerId,
          false,
          previousListing.createdAt
        ),
    newValue:
      name === 'listing_type'
        ? updatedListing.listingTypeId
        : updatedListing.propertyTypeId,
    newUpdatedAt: await appHelper.getActualDate(partnerId, false)
  }
  return history
}

export const prepareHistoryAndNamesByListing = async (params) => {
  const { previousListing, updatedListing } = params
  const history = []
  const names = []
  const _isLocationChanged = isLocationChanged(updatedListing, previousListing)
  if (_isLocationChanged) {
    const locationHistory = await prepareLocationHistory(params)
    history.push(locationHistory)
    names.push('address')
  }
  const _isGnrBnrChanged = isGnrBnrChanged(updatedListing, previousListing)
  if (_isGnrBnrChanged) {
    const gnrBnrHistory = await prepareGnrBnrHistory(params)
    history.push(gnrBnrHistory)
    names.push('gnr_bnr_snr')
  }
  const _isListingTypeChanged = isListingTypeChanged(
    updatedListing,
    previousListing
  )
  if (_isListingTypeChanged) {
    params.name = 'listing_type'
    const propertyTypeHistory = await prepareCommonTypeHistory(params)
    history.push(propertyTypeHistory)
    names.push(params.name)
  }
  const _isPropertyTypeChanged = isPropertyTypeChanged(
    updatedListing,
    previousListing
  )
  if (_isPropertyTypeChanged) {
    params.name = 'property_type'
    const propertyTypeHistory = await prepareCommonTypeHistory(params)
    history.push(propertyTypeHistory)
    names.push(params.name)
  }
  return { history, names }
}

export const getCommonEvictionQuery = (invoiceId) => {
  const commonQuery = {
    evictionInvoiceIds: { $in: [invoiceId] },
    $or: [{ hasPaid: { $exists: false } }, { hasPaid: false }]
  }
  return clone(commonQuery)
}

export const getEvictionCaseQuery = (contractId, commonQuery) => {
  const evictionCaseQuery = {
    _id: contractId,
    evictionCases: { $elemMatch: commonQuery }
  }
  return evictionCaseQuery
}

export const hasEvictionCases = async (params, session) => {
  const { contractId, invoiceId } = params
  const commonQuery = getCommonEvictionQuery(invoiceId)
  const query = getEvictionCaseQuery(contractId, commonQuery)
  const hasEvictionCases = !!(await getAContract(query, session))
  return hasEvictionCases
}

export const isRemoveEvictionCase = async (params, session) => {
  const { contractId, invoiceId, paidAmount } = params
  const commonQuery = getCommonEvictionQuery(invoiceId)
  commonQuery.status = 'new'
  commonQuery.amount = { $lte: paidAmount }
  const query = getEvictionCaseQuery(contractId, commonQuery)
  const isRemoveEvictionCase = !!(await getAContract(query, session))
  return isRemoveEvictionCase
}

export const isUpdateHasPaid = async (params, session) => {
  const { contractId, invoiceId, paidAmount } = params
  const commonQuery = getCommonEvictionQuery(invoiceId)
  commonQuery.amount = paidAmount
  const query = getEvictionCaseQuery(contractId, commonQuery)
  const hasPaid = !!(await getAContract(query, session))
  return hasPaid
}

export const prepareEvictionCaseRemoveOrUpdateData = async (data, session) => {
  const { invoiceId, paidAmount, isRemoveEvictionCase } = data
  let updateData = {}
  if (isRemoveEvictionCase) {
    const commonQuery = getCommonEvictionQuery(invoiceId)
    commonQuery.status = 'new'
    updateData.$pull = { evictionCases: commonQuery }
  } else {
    updateData = {
      $pull: { 'evictionCases.$.evictionInvoiceIds': invoiceId },
      $inc: { 'evictionCases.$.amount': paidAmount * -1 }
    }
    const hasPaid = await isUpdateHasPaid(data, session)
    if (hasPaid) {
      updateData.$set = { 'evictionCases.$.hasPaid': hasPaid }
    }
  }
  return updateData
}

export const getContractsForQuery = async (params, populate = []) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const contracts = await ContractCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return contracts
}

export const countContracts = async (query, session) => {
  const numberOfBranches = await ContractCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfBranches
}

const prepareQueryForContractId = async (query) => {
  let returnedQuery = {}
  if (query.type === 'before_natural_termination_notice') {
    returnedQuery['status'] = 'active'
    returnedQuery['rentalMeta.status'] = 'active'
    returnedQuery['rentalMeta.naturalTerminatedNoticeSendDate'] = {
      $exists: false
    }
    return returnedQuery
  } else if (query.type === 'daily_soon_ending') {
    returnedQuery['status'] = 'active'
    returnedQuery['rentalMeta.status'] = 'active'
    returnedQuery['rentalMeta.soonTerminatedNoticeSendDate'] = {
      $exists: false
    }
    return returnedQuery
  } else if (query.type === 'daily_send_cpi_notification_send') {
    return {
      'rentalMeta.status': { $in: ['active', 'upcoming'] },
      'rentalMeta.cpiEnabled': true
    }
  } else if (query.type === 'daily_cpi_rent_amount_update') {
    return {
      'rentalMeta.status': { $in: ['active', 'upcoming'] },
      'rentalMeta.cpiEnabled': true,
      'rentalMeta.futureRentAmount': { $exists: true }
    }
  } else if (query.status === 'closed') {
    return {
      status: { $ne: 'closed' },
      hasRentalContract: true,
      'rentalMeta.contractEndDate': { $exists: true }
    }
  } else if (query.status === 'active') {
    return {
      status: 'upcoming',
      hasRentalContract: true,
      'rentalMeta.status': 'upcoming',
      $or: [
        { 'rentalMeta.enabledLeaseEsigning': { $ne: true } },
        {
          'rentalMeta.enabledLeaseEsigning': true,
          'rentalMeta.landlordLeaseSigningStatus.signed': true,
          'rentalMeta.tenantLeaseSigningStatus': {
            $not: {
              $elemMatch: {
                signed: false
              }
            }
          }
        }
      ]
    }
  } else if (query.type === 'daily_final_settlement_checker') {
    return {
      status: 'closed',
      finalSettlementStatus: {
        $ne: 'completed'
      },
      'rentalMeta.contractEndDate': { $exists: true }
    }
  } else if (query.type === 'daily_non_updated_esigning_status') {
    return {
      $or: [
        {
          $and: [
            {
              createdAt: { $gte: moment().subtract(3, 'months').toDate() },
              enabledEsigning: true
            },
            { 'landlordAssignmentSigningStatus.signed': false }
          ]
        },
        {
          $and: [
            {
              createdAt: { $gte: moment().subtract(3, 'months').toDate() },
              enabledEsigning: true
            },
            { 'agentAssignmentSigningStatus.signed': false }
          ]
        },
        {
          $and: [
            {
              'rentalMeta.createdAt': {
                $gte: moment().subtract(3, 'months').toDate()
              },
              'rentalMeta.enabledLeaseEsigning': true
            },
            { 'rentalMeta.landlordLeaseSigningStatus.signed': false }
          ]
        },
        {
          $and: [
            {
              'rentalMeta.createdAt': {
                $gte: moment().subtract(3, 'months').toDate()
              },
              'rentalMeta.enabledLeaseEsigning': true
            },
            {
              'rentalMeta.tenantLeaseSigningStatus': {
                $elemMatch: { signed: false }
              }
            }
          ]
        }
      ]
    }
  } else if (query.type === 'daily_rent_invoice_create') {
    returnedQuery = {
      'rentalMeta.status': { $in: ['active', 'upcoming'] },
      status: { $in: ['active', 'upcoming'] },
      isFinalSettlementDone: { $ne: true }
    }
    if (query.partnerId) returnedQuery.partnerId = query.partnerId
    return returnedQuery
  } else {
    return query
  }
}

export const contractIdsForLambdaRelatedWork = async (query = {}) => {
  const preparedQuery = await prepareQueryForContractId(query)
  console.log(preparedQuery)
  const pipeline = []
  console.log('Query for getting final settlement contract data', preparedQuery)
  pipeline.push({
    $match: preparedQuery
  })
  if (query.type === 'before_natural_termination_notice') {
    pipeline.push(...naturalContractTerminationPipeline)
  } else if (query.type === 'daily_soon_ending') {
    pipeline.push(...dailySoonEnding)
  } else if (query.type === 'daily_cpi_rent_amount_update') {
    pipeline.push(...dailyCpiFutureAmountPipeline)
  } else if (query.type === 'daily_send_cpi_notification_send') {
    pipeline.push(...dailyCpiNotificationSendPipeline)
  } else if (query.type === 'daily_final_settlement_checker') {
    pipeline.push(...dailyFinalSettlementCheckerPipeline)
  } else if (query.status) {
    pipeline.push(...partnerSettingsPipeline)
    if (query.status === 'closed') {
      pipeline.push(...dailyContractClosePipeline)
    } else if (query.status === 'active') {
      pipeline.push(...dailyContractActivePipeline)
    }
  }
  pipeline.push(
    {
      $lookup: {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partners',
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$isActive', true]
              }
            }
          }
        ]
      }
    },
    {
      $unwind: '$partners'
    },
    {
      $group: {
        _id: null,
        contractData: {
          $push: {
            contractId: '$_id',
            partnerId: '$partnerId',
            status: '$status'
          }
        }
      }
    }
  )
  console.log('Pipeline for contract collection', JSON.stringify(pipeline))
  const contractListData = await ContractCollection.aggregate(pipeline)
  console.log('Contract data', contractListData[0]?.contractData)
  return contractListData[0]?.contractData || []
}
export const partnerSettingsPipeline = [
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings',
      pipeline: [
        {
          $project: {
            dateTimeSettings: 1
          }
        }
      ]
    }
  },
  {
    $unwind: '$partnerSettings'
  },
  {
    $addFields: {
      todayNewDate: {
        $toDate: {
          $dateToString: {
            date: new Date(),
            timezone: '$partnerSettings.dateTimeSettings.timezone'
          }
        }
      }
    }
  },
  {
    $addFields: {
      today: {
        $subtract: [
          {
            $dateFromParts: {
              year: {
                $year: '$todayNewDate'
              },
              month: {
                $month: '$todayNewDate'
              },
              day: {
                $add: [
                  {
                    $dayOfMonth: '$todayNewDate'
                  },
                  1
                ]
              },
              timezone: '$partnerSettings.dateTimeSettings.timezone'
            }
          },
          60
        ]
      }
    }
  }
]

const dailyContractActivePipeline = [
  {
    $addFields: {
      isContractNeedToActive: {
        $cond: {
          if: { $lte: ['$rentalMeta.contractStartDate', '$today'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isContractNeedToActive: true
    }
  }
]

const dailyFinalSettlementCheckerPipeline = [
  ...partnerSettingsPipeline,
  {
    $addFields: {
      dateBeforeThreeMonthsFromToday: {
        $dateSubtract: {
          startDate: new Date(),
          unit: 'month',
          amount: 3,
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isContractNeedToFinalSettlement: {
        $cond: {
          if: {
            $lt: [
              '$rentalMeta.contractEndDate',
              '$dateBeforeThreeMonthsFromToday'
            ]
          },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isContractNeedToFinalSettlement: true
    }
  }
]

const dailyContractClosePipeline = [
  {
    $addFields: {
      isContractNeedToClosed: {
        $cond: {
          if: { $lte: ['$rentalMeta.contractEndDate', '$today'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isContractNeedToClosed: true
    }
  }
]

const dailySoonEnding = [
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings',
      pipeline: [
        {
          $project: {
            isSoonEndingLease: '$notifications.soonEndingLease',
            soonEndingMonths: {
              $ifNull: ['$propertySettings.soonEndingMonths', 0]
            },
            dateTimeSettings: 1
          }
        }
      ]
    }
  },
  {
    $unwind: '$partnerSettings'
  },
  {
    $match: {
      'partnerSettings.isSoonEndingLease': true
    }
  },
  {
    $addFields: {
      today: {
        $toDate: {
          $dateToString: {
            date: new Date(),
            timezone: '$partnerSettings.dateTimeSettings.timezone'
          }
        }
      }
    }
  },
  {
    $addFields: {
      contractSoonEndDate: {
        $dateAdd: {
          startDate: '$today',
          unit: 'month',
          amount: '$partnerSettings.soonEndingMonths',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isContractEndNoticeNeedToSend: {
        $cond: {
          if: { $lte: ['$rentalMeta.contractEndDate', '$contractSoonEndDate'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isContractEndNoticeNeedToSend: true,
      'rentalMeta.contractEndDate': {
        $exists: true
      }
    }
  }
]

const dailyCpiFutureAmountPipeline = [
  ...partnerSettingsPipeline,
  {
    $addFields: {
      isContractEndNoticeNeedToSend: {
        $cond: {
          if: {
            $and: [{ $lte: ['$rentalMeta.nextCpiDate', '$today'] }]
          },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isContractEndNoticeNeedToSend: true
    }
  }
]

const dailyCpiNotificationSendPipeline = [
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings',
      pipeline: [
        {
          $project: {
            isCPISettlementEnabled: '$CPISettlement.enabled',
            dateTimeSettings: 1
          }
        }
      ]
    }
  },
  {
    $unwind: '$partnerSettings'
  },
  {
    $match: {
      'partnerSettings.isCPISettlementEnabled': true
    }
  },
  {
    $addFields: {
      nextMonthDate: {
        $dateAdd: {
          startDate: new Date(),
          unit: 'day',
          amount: 31,
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      cpiDate: {
        $dateFromParts: {
          year: { $year: { $toDate: '$nextMonthDate' } },
          month: { $month: { $toDate: '$nextMonthDate' } },
          day: { $dayOfMonth: { $toDate: '$nextMonthDate' } },
          hour: 23,
          minute: 59,
          second: 59,
          millisecond: 999,
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isContractNeedToSendNotification: {
        $cond: {
          if: {
            $lte: ['$rentalMeta.nextCpiDate', '$cpiDate']
          },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $addFields: {
      isContractNeedToRemoveFutureRentAmount: {
        $cond: {
          if: {
            $gt: ['$rentalMeta.nextCpiDate', '$cpiDate']
          },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      $or: [
        {
          isContractNeedToSendNotification: true,
          'rentalMeta.lastCPINotificationSentOn': { $exists: false }
        },
        {
          isContractNeedToRemoveFutureRentAmount: true,
          'rentalMeta.nextCpiDate': { $exists: true },
          'rentalMeta.lastCPINotificationSentOn': { $exists: true },
          'rentalMeta.futureRentAmount': { $exists: true }
        }
      ]
    }
  }
]

const naturalContractTerminationPipeline = [
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings',
      pipeline: [
        {
          $project: {
            isNaturalLeaseTerminationEnabled:
              '$leaseSetting.naturalLeaseTermination.enabled',
            numberOfDays: {
              $ifNull: ['$leaseSetting.naturalLeaseTermination.days', 0]
            },
            dateTimeSettings: 1
          }
        }
      ]
    }
  },
  {
    $unwind: '$partnerSettings'
  },
  {
    $match: {
      'partnerSettings.isNaturalLeaseTerminationEnabled': true
    }
  },
  {
    $addFields: {
      today: {
        $toDate: {
          $dateToString: {
            date: new Date(),
            timezone: '$partnerSettings.dateTimeSettings.timezone'
          }
        }
      }
    }
  },
  {
    $addFields: {
      actualContractEndDate: {
        $dateAdd: {
          startDate: '$today',
          unit: 'day',
          amount: '$partnerSettings.numberOfDays'
        }
      },
      startDate: {
        $subtract: [
          {
            $dateFromParts: {
              year: {
                $year: '$today'
              },
              month: {
                $month: '$today'
              },
              day: {
                $add: [
                  {
                    $dayOfMonth: '$today'
                  },
                  1
                ]
              },
              timezone: '$partnerSettings.dateTimeSettings.timezone'
            }
          },
          86400000
        ]
      }
    }
  },
  {
    $addFields: {
      endDate: {
        $subtract: [
          {
            $dateFromParts: {
              year: {
                $year: '$actualContractEndDate'
              },
              month: {
                $month: '$actualContractEndDate'
              },
              day: {
                $add: [
                  {
                    $dayOfMonth: '$actualContractEndDate'
                  },
                  1
                ]
              },
              timezone: '$partnerSettings.dateTimeSettings.timezone'
            }
          },
          86400000
        ]
      }
    }
  },
  {
    $addFields: {
      isContractEndNoticeNeedToSend: {
        $cond: {
          if: {
            $and: [
              { $gte: ['$rentalMeta.contractEndDate', '$startDate'] },
              { $lte: ['$rentalMeta.contractEndDate', '$endDate'] }
            ]
          },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isContractEndNoticeNeedToSend: true,
      'rentalMeta.contractEndDate': {
        $exists: true
      }
    }
  }
]

export const contractIds = async (partnerId, type, dataToSkip) => {
  let match = {}
  const partner = await partnerHelper.getAPartner({ _id: partnerId })
  if (type === 'payout' && partner.accountType === 'direct') return []
  if (type === 'payout') {
    match = {
      partnerId
    }
  } else if (type === 'invoice') {
    match = {
      partnerId,
      'rentalMeta.status': { $in: ['active', 'upcoming'] },
      'rentalMeta.monthlyRentAmount': { $gt: 0 },
      status: { $in: ['active', 'upcoming'] }
    }
  }
  const pipeline = [
    {
      $match: match
    }
  ]
  if (dataToSkip) {
    pipeline.push(
      {
        $skip: dataToSkip || 0
      },
      {
        $limit: 500
      }
    )
  }
  pipeline.push({
    $group: {
      _id: null,
      contractIds: {
        $push: '$_id'
      }
    }
  })
  const contracts = await ContractCollection.aggregate(pipeline)
  console.log('contracts for app health', contracts[0])
  console.log('contracts for app health', contracts[0]?.contractIds)
  return contracts[0]?.contractIds || []
}

export const queryContracts = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const contractsData = await getContractsForQuery(body, [
    'partner',
    'property',
    'branch',
    'account',
    'agent'
  ])
  const filteredDocuments = await countContracts(query)
  const totalDocuments = await countContracts({})
  return {
    data: contractsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const getTenantProjectPipelineForSingleLease = ({
  dateFormat,
  selectedMonth,
  timeZone
}) => [
  {
    $project: {
      _id: 1,
      propertyId: 1,
      property: '$property.location',
      partner: '$partner.name',
      contractStartDate: '$rentalMeta.contractStartDate',
      contractEndDate: '$rentalMeta.contractEndDate',
      agent: {
        name: '$agent.name',
        avatar: getAvatarKeyPipeline(
          '$agent.avatarKey',
          'assets/default-image/user-primary.png'
        ),
        email: '$agent.email',
        phoneNumber: '$agent.phoneNumber'
      },
      leaseStatus: {
        $cond: [
          { $eq: ['$rentalMeta.status', 'active'] },
          {
            $cond: [
              { $lte: ['$rentalMeta.contractEndDate', selectedMonth._d] },
              'soon_ending',
              'active'
            ]
          },
          '$rentalMeta.status'
        ]
      },
      invoiceStatus: 1,
      amount: 1,
      security: '$rentalMeta.depositType',
      depositAmount: 1,
      depositAccountStatus: depositAccountHelper.getStatusProject(),
      depositAccountNumber: '$depositAccount.bankAccountNumber',
      depositAccountCreatedAt: '$depositAccount.createdAt',
      depositAccountBankName: process.env.DEPOSIT_ACCOUNT_BANK_NAME,
      depositInsuranceStatus: '$depositInsurance.status',
      depositInsuranceBankAccount: '$setting.bankAccountNumber',
      depositInsuranceKid: '$depositInsurance.kidNumber',
      depositInsuranceAmount: '$depositInsurance.depositInsuranceAmount',
      depositInsuranceCreatedAt: '$depositInsurance.createdAt',
      depositInsuranceRefNumber: '$depositInsurance.creationResult.insuranceNo',
      depositInsurancePayable: {
        $cond: [
          {
            $gt: [
              {
                $subtract: [
                  { $ifNull: ['$depositInsurance.depositInsuranceAmount', 0] },
                  { $ifNull: ['$depositInsurance.totalPaymentAmount', 0] }
                ]
              },
              0
            ]
          },
          {
            $subtract: [
              { $ifNull: ['$depositInsurance.depositInsuranceAmount', 0] },
              { $ifNull: ['$depositInsurance.totalPaymentAmount', 0] }
            ]
          },
          0
        ]
      },
      monthlyRentAmount: '$rentalMeta.monthlyRentAmount',
      dueDate: '$rentalMeta.dueDate',
      invoiceAccountNumber: 1,
      kidNumber: 1,
      invoiceDueDate: {
        $cond: [
          {
            $and: [
              // { $eq: ['$numOfInvoice', 1] },
              {
                $in: [
                  '$invoiceStatus',
                  ['eviction_notice', 'overdue', 'to_pay']
                ]
              }
            ]
          },
          {
            $cond: [
              { $ifNull: ['$dueDate', false] },
              {
                $dateToString: {
                  date: '$dueDate',
                  format: dateFormat,
                  timezone: timeZone
                }
              },
              null
            ]
          },
          null
        ]
      },
      nextInvoiceDate: {
        $cond: [
          {
            $and: [
              // { $eq: ['$numOfInvoice', 1] },
              {
                $in: ['$invoiceStatus', ['paid', 'over_paid']]
              }
            ]
          },
          {
            $cond: [
              { $ifNull: ['$invoiceMonth', false] },
              {
                $dateToString: {
                  date: {
                    $dateAdd: {
                      startDate: '$invoiceMonth',
                      unit: 'month',
                      amount: 1,
                      timezone: timeZone
                    }
                  },
                  format: dateFormat,
                  timezone: timeZone
                }
              },
              null
            ]
          },
          null
        ]
      },
      evictionNoticeSentOn: {
        $cond: [
          {
            $and: [
              // { $eq: ['$numOfInvoice', 1] },
              {
                $eq: ['$invoiceStatus', 'eviction_notice_due']
              }
            ]
          },
          {
            $cond: [
              { $ifNull: ['$evictionNoticeSentOn', false] },
              {
                $dateToString: {
                  date: '$evictionNoticeSentOn',
                  format: dateFormat,
                  timezone: timeZone
                }
              },
              null
            ]
          },
          null
        ]
      },
      evictionNoticeFileId: {
        $cond: [
          { $eq: ['$invoiceStatus', 'eviction_notice'] },
          '$evictionNoticeFileId',
          null
        ]
      },
      evictionLetterFileId: {
        $cond: [
          { $eq: ['$invoiceStatus', 'eviction_notice_due'] },
          '$evictionLetterFileId',
          null
        ]
      },
      leaseFileId: '$leaseFile._id',
      //For progress calculation
      leaseContractPdfGenerated: 1,
      enabledLeaseEsigning: '$rentalMeta.enabledLeaseEsigning',
      tenantLeaseSigningStatus: {
        signingUrl: {
          $concat: [
            appHelper.getLinkServiceURL(),
            '/e-signing/tenant_lease/',
            '$_id',
            '/',
            '$tenantLeaseSigningStatus.internalUrl'
          ]
        },
        signed: 1
      },
      landlordLeaseSigningStatus: {
        signed: '$rentalMeta.landlordLeaseSigningStatus.signed'
      },
      //For second step progression
      firstInvoice: {
        amount: '$firstInvoice.invoiceTotal',
        kidNumber: 1,
        status: {
          $switch: {
            branches: [
              {
                case: {
                  $eq: ['$firstInvoice.isPartiallyPaid', true]
                },
                then: 'partially_paid'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isOverPaid', true]
                },
                then: 'overpaid'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isDefaulted', true]
                },
                then: 'defaulted'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isPartiallyCredited', true]
                },
                then: 'partially_credited'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isPartiallyBalanced', true]
                },
                then: 'partially_balanced'
              }
            ],
            default: '$firstInvoice.status'
          }
        },
        accountNumber: '$firstInvoice.invoiceAccountNumber'
      },
      leaseSerial: 1,
      assignmentSerial: 1
    }
  }
]

const getLandlordProjectPipelineForSingleLease = (
  dateFormat,
  timeZone,
  selectedMonth
) => [
  {
    $project: {
      _id: 1,
      propertyId: 1,
      property: '$property.location',
      partner: '$partner.name',
      contractStartDate: '$rentalMeta.contractStartDate',
      contractEndDate: '$rentalMeta.contractEndDate',
      agent: {
        name: '$agent.name',
        avatar: getAvatarKeyPipeline(
          '$agent.avatarKey',
          'assets/default-image/user-primary.png'
        ),
        email: '$agent.email',
        phoneNumber: '$agent.phoneNumber'
      },
      leaseStatus: {
        $cond: [
          { $eq: ['$rentalMeta.status', 'active'] },
          {
            $cond: [
              { $lte: ['$rentalMeta.contractEndDate', selectedMonth._d] },
              'soon_ending',
              'active'
            ]
          },
          '$rentalMeta.status'
        ]
      },
      invoiceStatus: 1,
      rentStatus: 1,
      invoiceAmount: 1,
      amount: 1,
      monthlyRentAmount: '$rentalMeta.monthlyRentAmount',
      dueDate: '$rentalMeta.dueDate',
      security: '$rentalMeta.depositType',
      depositAmount: 1,
      depositAccountStatus: depositAccountHelper.getStatusProject(),
      depositAccountNumber: '$depositAccount.bankAccountNumber',
      depositAccountBankName: process.env.DEPOSIT_ACCOUNT_BANK_NAME,
      depositInsuranceStatus: '$depositInsurance.status',
      depositInsuranceBankAccount: '$depositInsurance.bankAccountNumber',
      depositInsuranceKid: '$depositInsurance.kidNumber',
      depositInsuranceAmount: '$depositInsurance.depositInsuranceAmount',
      payoutTo: 1,
      monthlyPayoutDate: 1,
      // For assignment details
      assignmentFrom: {
        $cond: [
          { $ifNull: ['$assignmentFrom', false] },
          {
            $dateToString: {
              format: dateFormat,
              date: '$assignmentFrom',
              timezone: timeZone
            }
          },
          ''
        ]
      },
      assignmentTo: {
        $cond: [
          { $ifNull: ['$assignmentTo', false] },
          {
            $dateToString: {
              format: dateFormat,
              date: '$assignmentTo',
              timezone: timeZone
            }
          },
          ''
        ]
      },
      brokeringCommissionAmount: 1,
      brokeringCommissionType: 1,
      rentalManagementCommissionType: 1,
      rentalManagementCommissionAmount: 1,
      addons: 1,
      nextPayoutAmount: '$nextPayouts.amount',
      invoicePaidAfterPayoutDate: '$nextPayouts.invoicePaidAfterPayoutDate',
      nextPayoutDate: {
        $cond: [
          { $ifNull: ['$nextPayouts.payoutDate', false] },
          {
            $concat: [
              {
                $dateToString: {
                  date: '$nextPayouts.payoutDate',
                  format: '%d',
                  timezone: timeZone
                }
              },
              ', ',
              {
                $let: {
                  vars: {
                    months: [
                      '',
                      'january',
                      'february',
                      'march',
                      'april',
                      'may',
                      'june',
                      'july',
                      'august',
                      'september',
                      'october',
                      'november',
                      'december'
                    ]
                  },
                  in: {
                    $arrayElemAt: [
                      '$$months',
                      {
                        $toInt: {
                          $dateToString: {
                            date: '$nextPayouts.payoutDate',
                            format: '%m',
                            timezone: timeZone,
                            onNull: 0
                          }
                        }
                      }
                    ]
                  }
                }
              }
            ]
          },
          ''
        ]
      },
      nextPayoutInvoicePaid: '$nextPayouts.invoicePaid',
      leaseFileId: '$leaseFile._id',
      //For lease progress calculation
      leaseContractPdfGenerated: 1,
      enabledLeaseEsigning: '$rentalMeta.enabledLeaseEsigning',
      tenantLeaseSigningStatus: {
        signed: {
          $cond: [
            { $ifNull: ['$rentalMeta.enabledJointlyLiable', false] },
            {
              $cond: [
                { $eq: [{ $size: '$anyNotSignedEsigningStatus' }, 0] },
                true,
                false
              ]
            },
            {
              $cond: [
                { $eq: ['$mainTenantEsigningStatus.signed', true] },
                true,
                false
              ]
            }
          ]
        }
      },
      landlordLeaseSigningStatus: {
        signed: '$rentalMeta.landlordLeaseSigningStatus.signed',
        signingUrl: {
          $concat: [
            appHelper.getLinkServiceURL(),
            '/e-signing/landlord_lease/',
            '$_id',
            '/',
            '$rentalMeta.landlordLeaseSigningStatus.internalUrl'
          ]
        }
      },
      //For assignment progression
      assignmentContractPdfGenerated: 1,
      enabledEsigning: 1,
      agentAssignmentSigningStatus: {
        signed: 1
      },
      landlordAssignmentSigningStatus: {
        signed: 1,
        signingUrl: 1
      },
      //For second step progression
      firstInvoice: {
        amount: '$firstInvoice.invoiceTotal',
        kidNumber: 1,
        status: {
          $switch: {
            branches: [
              {
                case: {
                  $eq: ['$firstInvoice.isPartiallyPaid', true]
                },
                then: 'partially_paid'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isOverPaid', true]
                },
                then: 'overpaid'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isDefaulted', true]
                },
                then: 'defaulted'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isPartiallyCredited', true]
                },
                then: 'partially_credited'
              },
              {
                case: {
                  $eq: ['$firstInvoice.isPartiallyBalanced', true]
                },
                then: 'partially_balanced'
              }
            ],
            default: '$firstInvoice.status'
          }
        }
      },
      leaseSerial: 1,
      assignmentSerial: 1
    }
  }
]

const getDepositAccountPipelineForLease = () => [
  {
    $lookup: {
      from: 'deposit_accounts',
      localField: '_id',
      foreignField: 'contractId',
      as: 'depositAccount'
    }
  },
  {
    $unwind: {
      path: '$depositAccount',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getDepositAmountPipelineForLease = () => [
  {
    $addFields: {
      existanceOfDepositAccount: {
        $ifNull: ['$depositAccount', false]
      }
    }
  },
  {
    $addFields: {
      depositAmount: {
        $cond: [
          { $not: { $eq: ['$existanceOfDepositAccount', false] } },
          '$depositAccount.depositAmount',
          '$rentalMeta.depositAmount'
        ]
      }
    }
  }
]

const getAddonsPipelineForSingleLease = () => [
  {
    $addFields: {
      assignmentAddons: {
        $filter: {
          input: '$addons',
          as: 'addon',
          cond: {
            $eq: ['$$addon.type', 'assignment']
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'products_services',
      localField: 'assignmentAddons.addonId',
      foreignField: '_id',
      as: 'addons'
    }
  },
  {
    $addFields: {
      addonsStr: {
        $reduce: {
          input: { $ifNull: ['$addons', []] },
          initialValue: '',
          in: { $concat: ['$$value', ', ', '$$this.name'] }
        }
      }
    }
  },
  {
    $addFields: {
      addons: {
        $substr: ['$addonsStr', 2, -1]
      }
    }
  }
]

const getNextPayoutPipelineForLease = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: '_id',
      foreignField: 'contractId',
      as: 'payouts'
    }
  },
  {
    $addFields: {
      nextPayouts: {
        $filter: {
          input: { $ifNull: ['$payouts', []] },
          as: 'payout',
          cond: {
            $and: [
              { $eq: ['$$payout.status', 'estimated'] },
              { $eq: ['$$payout.propertyId', '$propertyId'] },
              { $eq: ['$$payout.partnerId', '$partnerId'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$nextPayouts',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $sort: {
      'nextPayouts.serialId': 1
    }
  },
  {
    $limit: 1
  }
]

const getLeaseFileIdPipelineForLease = () => [
  {
    $lookup: {
      from: 'files',
      localField: '_id',
      foreignField: 'contractId',
      let: { propertyId: '$propertyId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$$propertyId', '$propertyId'] },
                { $in: ['$type', ['lease_pdf', 'esigning_lease_pdf']] }
              ]
            }
          }
        },
        {
          $limit: 1
        },
        {
          $project: {
            _id: 1
          }
        }
      ],
      as: 'leaseFile'
    }
  },
  {
    $unwind: {
      path: '$leaseFile',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getDepositInsurancePipelineForLease = () => [
  {
    $lookup: {
      from: 'deposit_insurance',
      localField: 'rentalMeta.depositInsuranceId',
      foreignField: '_id',
      as: 'depositInsurance'
    }
  },
  {
    $unwind: {
      path: '$depositInsurance',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getEsigningStatusOfTenantPipelineForLease = (tenantId) => [
  {
    $addFields: {
      mainTenant: tenantId
    }
  },
  {
    $addFields: {
      esignTenants: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'esignTenant',
          cond: {
            $eq: ['$$esignTenant.tenantId', '$mainTenant']
          }
        }
      }
    }
  },
  {
    $addFields: {
      tenantLeaseSigningStatus: {
        $first: '$esignTenants'
      }
    }
  }
]

const getEsigningStatusOfLandlordPipelineForLease = () => [
  {
    $addFields: {
      mainTenantEsigningStatus: {
        $first: {
          $filter: {
            input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
            as: 'tenantEsign',
            cond: {
              $eq: ['$$tenantEsign.tenantId', '$rentalMeta.tenantId']
            }
          }
        }
      },
      anyNotSignedEsigningStatus: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'tenantEsign',
          cond: {
            $not: { $eq: ['$$tenantEsign.signed', true] }
          }
        }
      }
    }
  }
]

const getFirstRentPipelineForLease = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      as: 'invoices'
    }
  },
  {
    $addFields: {
      firstInvoice: {
        $first: {
          $filter: {
            input: { $ifNull: ['$invoices', []] },
            as: 'invoice',
            cond: {
              $eq: ['$$invoice.isFirstInvoice', true]
            }
          }
        }
      }
    }
  }
]

const getSettingPipelineForLease = () => [
  {
    $lookup: {
      from: 'settings',
      as: 'setting',
      pipeline: []
    }
  },
  {
    $addFields: {
      setting: {
        $first: '$setting'
      }
    }
  }
]

const getEvictionNoticeAndEvictionLetterPipeline = () => [
  {
    $lookup: {
      from: 'files',
      localField: '_id',
      foreignField: 'contractId',
      as: 'contractFiles',
      pipeline: [
        {
          $sort: { createdAt: -1 }
        }
      ]
    }
  },
  {
    $addFields: {
      evictionNoticeFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$invoicePdf', []] },
            as: 'pdf',
            cond: {
              $eq: ['$$pdf.type', 'eviction_notice_attachment_pdf']
            }
          }
        }
      },
      evictionLetterFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$invoicePdf', []] },
            as: 'pdf',
            cond: {
              $eq: ['$$pdf.type', 'eviction_due_reminder_notice_attachment_pdf']
            }
          }
        }
      },
      contractEvictionNoticeFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$contractFiles', []] },
            as: 'file',
            cond: {
              $eq: ['$$file.type', 'eviction_notice_attachment_pdf']
            }
          }
        }
      },
      contractEvictionLetterFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$contractFiles', []] },
            as: 'file',
            cond: {
              $eq: [
                '$$file.type',
                'eviction_due_reminder_notice_attachment_pdf'
              ]
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      evictionNoticeFileId: {
        $cond: [
          { $ifNull: ['$evictionNoticeFile', false] },
          '$evictionNoticeFile.fileId',
          '$contractEvictionNoticeFile._id'
        ]
      },
      evictionLetterFileId: {
        $cond: [
          { $ifNull: ['$evictionLetterFile', false] },
          '$evictionLetterFile.fileId',
          '$contractEvictionLetterFile._id'
        ]
      }
    }
  }
]

const getSingleLeaseForPublicSite = async (params) => {
  const {
    body,
    dateFormat,
    selectedMonth,
    timeZone,
    payBeforeMonth,
    payBeforeMonthEnabled
  } = params
  const { contractId, partnerId, personType, tenantId } = body
  const pipeline = [
    {
      $match: {
        _id: contractId,
        partnerId
      }
    },
    ...getPropertyPipelineForLease(),
    ...getPartnerPipelineForLease(),
    ...getAgentPipelineForLeaseDetails()
  ]
  pipeline.push(...getInvoicePipelineForLeaseDetails())
  if (personType === 'tenant') {
    pipeline.push(...getDepositAccountPipelineForLease())
    pipeline.push(...getDepositAmountPipelineForLease())
    pipeline.push(...depositAccountHelper.getPipelineForSendToBank())
    pipeline.push(...getLeaseFileIdPipelineForLease())
    pipeline.push(...getDepositInsurancePipelineForLease())
    pipeline.push(...getEsigningStatusOfTenantPipelineForLease(tenantId))
    pipeline.push(...getFirstRentPipelineForLease())
    pipeline.push(...getSettingPipelineForLease())
    pipeline.push(...getEvictionNoticeAndEvictionLetterPipeline())
    pipeline.push(
      ...getTenantProjectPipelineForSingleLease({
        dateFormat,
        selectedMonth,
        timeZone
      })
    )
  } else {
    pipeline.push({
      $addFields: {
        invoiceAmount: '$amount',
        rentStatus: '$invoiceStatus'
      }
    })
    pipeline.push(...getTotalPayoutPipelineForLeaseDetails())
    pipeline.push(...getAddonsPipelineForSingleLease())
    pipeline.push(...getNextPayoutPipelineForLease())
    pipeline.push(...getDepositAccountPipelineForLease())
    pipeline.push(...getDepositAmountPipelineForLease())
    pipeline.push(...depositAccountHelper.getPipelineForSendToBank())
    pipeline.push(...getLeaseFileIdPipelineForLease())
    pipeline.push(...getDepositInsurancePipelineForLease())
    pipeline.push(...getEsigningStatusOfLandlordPipelineForLease())
    pipeline.push(...getFirstRentPipelineForLease())
    pipeline.push(
      ...getLandlordProjectPipelineForSingleLease(
        dateFormat,
        timeZone,
        selectedMonth
      )
    )
    pipeline.push({
      $addFields: {
        payBeforeMonthEnabled,
        payBeforeMonth
      }
    })
  }
  const [result] = await ContractCollection.aggregate(pipeline)
  return result
}

const getUserInfoForLease = async (params) => {
  const { personType, partnerId, userId } = params
  const info = {}
  if (personType === 'tenant') {
    const tenantInfo = await tenantHelper.getATenant({
      userId,
      partnerId
    })
    if (!size(tenantInfo)) {
      throw new CustomError(404, 'Tenant not found')
    }
    info.tenantId = tenantInfo._id
  } else {
    const accountInfo = await accountHelper.getAnAccount({
      personId: userId,
      partnerId
    })
    if (!size(accountInfo)) {
      throw new CustomError(404, 'Account not found')
    }
    info.accountId = accountInfo._id
  }
  return info
}

export const querySingleLeaseForPublicSite = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['contractId', 'personType'], body)
  const { contractId } = body
  appHelper.validateId({ contractId })
  const { partnerId, userId } = user
  const { personType } = body
  const { tenantId } = await getUserInfoForLease({
    personType,
    partnerId,
    userId
  })
  body.partnerId = partnerId
  body.tenantId = tenantId
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const payBeforeMonthEnabled = partnerSetting?.payout?.enabled || false
  const payBeforeMonth = partnerSetting?.payout?.payBeforeMonth || 0
  let soonEndingMonths = 4
  soonEndingMonths =
    partnerSetting?.propertySettings?.soonEndingMonths || soonEndingMonths
  const selectedMonth = (
    await appHelper.getActualDate(partnerSetting, true)
  ).add(soonEndingMonths, 'months')
  const result = await getSingleLeaseForPublicSite({
    body,
    dateFormat,
    timeZone,
    selectedMonth,
    payBeforeMonthEnabled,
    payBeforeMonth
  })
  return result
}

const prepareQueryForGettingAllInvoicePayment = async (params) => {
  const { query } = params
  const { contractId = '', propertyId = '', partnerId = '' } = query

  const pipeline = []

  const match = {
    $match: {
      contractId,
      propertyId,
      partnerId
    }
  }

  const initialProject = {
    $project: {
      paymentDate: 1,
      amount: 1,
      createdAt: 1,
      invoices: 1
    }
  }

  const unwindInvoices = {
    $unwind: { path: '$invoices', preserveNullAndEmptyArrays: true }
  }

  const lookupInvoice = {
    $lookup: {
      from: 'invoices',
      localField: 'invoices.invoiceId',
      foreignField: '_id',
      as: 'invoiceInfo'
    }
  }

  const unwindInvoiceInfo = {
    $unwind: { path: '$invoiceInfo', preserveNullAndEmptyArrays: true }
  }

  const projectInvoices = {
    $project: {
      paymentDate: 1,
      amount: 1,
      createdAt: 1,
      'invoiceInfo._id': 1,
      'invoiceInfo.invoiceSerialId': 1
    }
  }

  const addFieldsInvoiceName = {
    $addFields: {
      'invoiceInfo.invoiceName': {
        $concat: [
          'invoice',
          '_',
          {
            $toString: '$invoiceInfo.invoiceSerialId'
          }
        ]
      },
      invoices: []
    }
  }

  const groupInvoiceName = {
    $group: {
      _id: '$_id',
      paymentDate: {
        $first: '$paymentDate'
      },
      amount: {
        $first: '$amount'
      },
      createdAt: {
        $first: '$createdAt'
      },
      invoices: {
        $push: '$invoiceInfo.invoiceName'
      }
    }
  }

  pipeline.push(match)
  pipeline.push(initialProject)
  pipeline.push(unwindInvoices)
  pipeline.push(lookupInvoice)
  pipeline.push(unwindInvoiceInfo)
  pipeline.push(projectInvoices)
  pipeline.push(addFieldsInvoiceName)
  pipeline.push(groupInvoiceName)
  pipeline.push({ $sort: { createdAt: 1 } })

  return pipeline
}

export const getAllInvoicePaymentForSingleLease = async (params) => {
  const { body, user = {} } = params
  const { query } = body
  const { partnerId = '', userId = '' } = user

  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['contractId', 'propertyId'], query)
  appHelper.checkPartnerId(partnerId)
  appHelper.checkUserId(userId)

  query.partnerId = partnerId

  const pipeline = await prepareQueryForGettingAllInvoicePayment({
    query
  })

  const allInvoicePayments = await InvoicePaymentCollection.aggregate(pipeline)

  const totalDocuments = await invoicePaymentHelper.countInvoicePayments(query)

  return {
    data: allInvoicePayments,
    metaData: {
      totalDocuments
    }
  }
}

const prepareQueryForGettingAllInvoice = async (params) => {
  const { query, roles = [] } = params
  const {
    contractId = '',
    partnerId = '',
    propertyId = '',
    neededFileType
  } = query

  const pipeline = []
  const preparedQuery = {
    partnerId,
    contractId,
    propertyId
  }
  if (roles.includes('partner_tenant')) {
    preparedQuery.invoiceType = {
      $in: ['invoice', 'credit_note']
    }
  } else if (roles.includes('partner_landlord')) {
    if (neededFileType === 'tenant') {
      preparedQuery.invoiceType = {
        $in: ['invoice', 'credit_note']
      }
    } else if (neededFileType === 'landlord') {
      preparedQuery.invoiceType = {
        $in: ['landlord_invoice', 'landlord_credit_note']
      }
    }
  }

  const match = {
    $match: preparedQuery
  }

  const fileInfo = {
    $addFields: {
      pdfInfo: {
        $first: {
          $filter: {
            input: '$pdf',
            as: 'singlePdf',
            cond: {
              $eq: ['$$singlePdf.type', 'invoice_pdf']
            }
          }
        }
      }
    }
  }
  // Add correction files for invoice
  // Todo:: Need to write test case for correction files
  const correctionInfo = {
    $lookup: {
      from: 'expenses',
      localField: 'correctionsIds',
      foreignField: '_id',
      pipeline: [
        {
          $group: {
            _id: null,
            files: {
              $push: '$files'
            }
          }
        },
        {
          $addFields: {
            files: {
              $reduce: {
                input: { $ifNull: ['$files', []] },
                initialValue: [],
                in: { $concatArrays: ['$$value', '$$this'] }
              }
            }
          }
        }
      ],
      as: 'correctionInfo'
    }
  }

  const unwindCorrectionInfo = {
    $unwind: {
      path: '$correctionInfo',
      preserveNullAndEmptyArrays: true
    }
  }

  const concatFileIds = {
    $addFields: {
      fileIds: {
        $concatArrays: [
          { $ifNull: ['$correctionInfo.files', []] },
          {
            $map: {
              input: { $ifNull: ['$pdf', []] },
              as: 'pdf',
              in: '$$pdf.fileId'
            }
          }
        ]
      }
    }
  }

  const attachments = {
    $lookup: {
      from: 'files',
      localField: 'fileIds',
      foreignField: '_id',
      as: 'attachments'
    }
  }

  if (neededFileType) {
    let visibleQuery
    if (neededFileType === 'landlord')
      visibleQuery = { isVisibleToLandlord: true }
    else if (neededFileType === 'tenant')
      visibleQuery = { isVisibleToTenant: true }
    attachments['$lookup']['pipeline'] = [
      {
        $match: {
          $or: [
            { type: 'correction_invoice_pdf', ...visibleQuery },
            { type: { $ne: 'correction_invoice_pdf' } }
          ]
        }
      }
    ]
  }
  const projectInvoices = {
    $project: {
      attachments: 1,
      invoiceTotal: 1,
      invoiceSerialId: 1,
      status: 1,
      isOverPaid: 1,
      invoiceMonth: 1,
      fileId: '$pdfInfo.fileId',
      dueDate: 1
    }
  }

  const InvoiceStatus = {
    $project: {
      invoiceTotal: 1,
      invoiceSerialId: 1,
      invoiceMonth: 1,
      status: {
        $cond: {
          if: {
            $and: [{ $eq: ['$isOverPaid', true] }, { $eq: ['$status', 'paid'] }]
          },
          then: 'over_paid',
          else: '$status'
        }
      },
      fileId: 1,
      dueDate: 1,
      attachments: {
        _id: 1,
        createdAt: 1,
        name: 1,
        title: 1,
        type: 1
      }
    }
  }

  pipeline.push(match)
  pipeline.push(fileInfo)
  pipeline.push(correctionInfo)
  pipeline.push(unwindCorrectionInfo)
  pipeline.push(concatFileIds)
  pipeline.push(attachments)
  pipeline.push(projectInvoices)
  pipeline.push(InvoiceStatus)
  pipeline.push({ $sort: { invoiceMonth: 1 } })

  return pipeline
}

export const getAllInvoiceForSingleLease = async (params) => {
  const { body, user = {} } = params
  const { query } = body
  const { partnerId = '', userId = '', roles = [] } = user

  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['contractId', 'propertyId'], query)
  appHelper.checkPartnerId(partnerId)
  appHelper.checkUserId(userId)

  query.partnerId = partnerId

  const pipeline = await prepareQueryForGettingAllInvoice({
    query,
    roles
  })

  const allInvoices = await InvoiceCollection.aggregate(pipeline)

  return {
    data: allInvoices
  }
}
const prepareQueryForPayout = (query) => {
  const prepareQuery = {}
  if (query.contractId) prepareQuery.contractId = query.contractId
  if (query.propertyId) prepareQuery.propertyId = query.propertyId
  return prepareQuery
}

export const queryPayoutHistoryForLease = async (params) => {
  const { body, user = {} } = params
  const { query } = body
  const { partnerId = '', userId = '' } = user

  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['contractId', 'propertyId'], query)
  appHelper.checkPartnerId(partnerId)
  appHelper.checkUserId(userId)

  const preparedQuery = prepareQueryForPayout(query)

  const allPayoutHistory = await PayoutCollection.find(preparedQuery, {
    amount: 1,
    referanse: '$serialId',
    payoutDate: 1,
    status: 1
  }).sort({ payoutDate: -1 })

  return {
    data: JSON.parse(JSON.stringify(allPayoutHistory))
  }
}

export const prepareAssignmentsOrLeasesQuery = async (params) => {
  let query = {}

  if (size(params) && params.partnerId) {
    const assignmentStatus = compact(params.assignmentStatus)
    const leaseStatus = compact(params.leaseStatus)
    const orQuery = []
    const andQuery = { partnerId: params.partnerId }
    let leaseDate = {}
    const type = params.type || ''
    let leaseId = ''
    //Set agentId filters in query
    if (params.agentId) andQuery.agentId = params.agentId
    //Set branchId filters in query
    if (params.branchId) andQuery.branchId = params.branchId
    //Set accountId filters in query
    if (params.accountId) andQuery.accountId = params.accountId
    if (params.propertyId) andQuery.propertyId = params.propertyId
    //Set assignment date range in query
    if (
      size(params?.assignmentDateRange) &&
      params?.assignmentDateRange?.startDate &&
      params?.assignmentDateRange?.endDate
    ) {
      andQuery.createdAt = {
        $gte: new Date(params.assignmentDateRange.startDate),
        $lte: new Date(params.assignmentDateRange.endDate)
      }
    }
    //set lease date range in query
    if (
      size(params?.leaseDateRange) &&
      params?.leaseDateRange?.startDate &&
      params?.leaseDateRange?.endDate
    ) {
      leaseDate = {
        $gte: new Date(params.leaseDateRange.startDate),
        $lte: new Date(params.leaseDateRange.endDate)
      }
      andQuery['rentalMeta.createdAt'] = clone(leaseDate)
    }
    //set assignment/lease address/id filters in query
    if (params.searchKeyword && type) {
      if (!isNaN(params.searchKeyword)) {
        if (type === 'assignment')
          andQuery.assignmentSerial = parseInt(params.searchKeyword)
        if (type === 'lease') {
          leaseId = parseInt(params.searchKeyword)
          andQuery.leaseSerial = clone(leaseId)
        }
      } else {
        let propertyIds = []
        propertyIds = await ListingCollection.distinct('_id', {
          partnerId: params.partnerId,
          'location.name': new RegExp(params.searchKeyword, 'i')
        })
        if (size(propertyIds)) andQuery.propertyId = { $in: propertyIds }
        else andQuery.propertyId = 'nothing'
      }
    }

    if (size(assignmentStatus)) {
      if (indexOf(assignmentStatus, 'archived') !== -1)
        orQuery.push(assignIn({ status: 'closed' }, andQuery))
      if (indexOf(assignmentStatus, 'occupied') !== -1)
        orQuery.push(
          assignIn(
            {
              status: { $in: ['active', 'upcoming'] },
              'rentalMeta.status': { $in: ['active', 'upcoming'] }
            },
            andQuery
          )
        )
      if (indexOf(assignmentStatus, 'vacant') !== -1)
        orQuery.push(
          assignIn(
            {
              status: 'upcoming',
              'rentalMeta.status': { $nin: ['active', 'upcoming', 'closed'] }
            },
            andQuery
          )
        )
    }
    if (
      indexOf(leaseStatus, 'archived') !== -1 ||
      indexOf(leaseStatus, 'vacant') !== -1 ||
      (!size(leaseStatus) && type === 'lease')
    ) {
      orQuery.push(
        assignIn({ 'rentalMeta.status': { $in: ['closed'] } }, andQuery)
      )
      const historyAndQuery = clone(andQuery)
      const rentalMetaHistoryQuery = {
        status: 'closed',
        cancelled: { $exists: false },
        cancelledAt: { $exists: false }
      }
      delete historyAndQuery['rentalMeta.createdAt']
      if (leaseId) {
        rentalMetaHistoryQuery.leaseSerial = leaseId
        delete historyAndQuery.leaseSerial
      }
      if (size(leaseDate)) {
        rentalMetaHistoryQuery.createdAt = leaseDate
        orQuery.push(
          assignIn(
            { rentalMetaHistory: { $elemMatch: rentalMetaHistoryQuery } },
            historyAndQuery
          )
        )
      } else
        orQuery.push(
          assignIn(
            { rentalMetaHistory: { $elemMatch: rentalMetaHistoryQuery } },
            historyAndQuery
          )
        )
    }
    if (indexOf(leaseStatus, 'occupied') !== -1)
      orQuery.push(
        assignIn(
          { 'rentalMeta.status': { $in: ['active', 'upcoming'] } },
          andQuery
        )
      )
    if (!size(leaseStatus) && type === 'lease') {
      orQuery.push(
        assignIn(
          { 'rentalMeta.status': { $in: ['active', 'upcoming', 'closed'] } },
          andQuery
        )
      )
    }
    if (size(orQuery)) query['$or'] = orQuery
    else query = andQuery
  }
  return query
}

export const getAssignmentStatus = (status, rentalMetaInfo, userLanguage) => {
  if (
    status === 'upcoming' &&
    size(rentalMetaInfo) &&
    !rentalMetaInfo.tenantId
  ) {
    return appHelper.translateToUserLng('common.vacant', userLanguage)
  } else if (
    (status === 'active' || status === 'upcoming') &&
    size(rentalMetaInfo) &&
    rentalMetaInfo.tenantId
  ) {
    return appHelper.translateToUserLng('common.occupied', userLanguage)
  } else if (status === 'closed') {
    return appHelper.translateToUserLng('common.archived', userLanguage)
  }
}

export const hasChangeLogs = (contract) => {
  const history = size(contract) && contract.history ? contract.history : []

  return !!size(history)
}

export const getConvertToCurrency = async (amount, currencyOptions) => {
  const balance = await appHelper.convertToCurrency(
    { number: amount },
    currencyOptions
  )
  return balance
}
export const getAssignmentMonthlyRentAmount = (assignment) => {
  const listingInfo = assignment?.listingInfo || {}
  const monthlyRentAmount = listingInfo?.monthlyRentAmount || ''

  return monthlyRentAmount
}

const getGnrBnrAndSnrInfo = (gnrBnrSnr = {}, userLang) => {
  const { gnr, bnr, snr } = gnrBnrSnr
  let text = ''

  if (gnr)
    text +=
      appHelper.translateToUserLng('properties.fields.gnr.title', userLang) +
      ' ' +
      gnr +
      ', '

  if (bnr)
    text +=
      appHelper.translateToUserLng('properties.fields.bnr.title', userLang) +
      ' ' +
      bnr +
      ', '

  if (snr)
    text +=
      appHelper.translateToUserLng('properties.fields.snr.title', userLang) +
      ' ' +
      snr +
      ', '

  return text ? text.substr(0, text.length - 2) : ''
}

export const getAssignmentDataForExcelCreator = async (params, options) => {
  const { partnerId = '', userId = '' } = params
  const userInfo = await userHelper.getAnUser({ _id: userId })
  const contractsQuery = await prepareAssignmentsOrLeasesQuery(params)
  const userLanguage = userInfo?.getLanguage()
  const dataCount = await countContracts(contractsQuery)
  // Currency options
  const currencyOptions = await appHelper.getCurrencyOptions({
    partnerSettingsOrId: partnerId,
    showSymbol: true
  })
  // Get settings
  const setting = await SettingCollection.findOne()
  // Get partner setting
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const queryData = {
    query: contractsQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage
  }
  const contracts = await getJournalForExcelManager(queryData)
  if (size(contracts)) {
    for (const contract of contracts) {
      contract.assignmentId =
        contract.partnerSerial &&
        contract.propertySerial &&
        contract.assignmentSerial
          ? appHelper.getFixedDigits(contract.partnerSerial, 4) +
            appHelper.getFixedDigits(contract.propertySerial, 5) +
            appHelper.getFixedDigits(contract.assignmentSerial, 3)
          : ''
      contract.gnrBnrSnr = getGnrBnrAndSnrInfo(
        { gnr: contract.gnr, bnr: contract.bnr, snr: contract.snr },
        userLanguage
      )
      contract.type = appHelper.translateToUserLng(
        'common.' +
          listingHelper.getListingTypeNameById(contract.listingTypeId, setting),
        userLanguage
      )
      contract.propertyType = appHelper.translateToUserLng(
        'common.' +
          listingHelper.getPropertyTypeNameById(
            contract.propertyTypeId,
            setting
          ) || '',
        userLanguage
      )

      contract.monthlyRentAmount = await getConvertToCurrency(
        contract.monthlyRentAmount,
        currencyOptions
      )
      contract.pursuant = 'Assignment for rent - Emgll § 1-2 (2) nr.2.'
    }
  }
  return {
    data: contracts,
    total: dataCount
  }
}

export const getLeaseRentalMetaHistoryFieldValue = (
  rentalMetaHistory,
  fieldName
) => {
  let rentalMetaHistoryFieldValue

  if (fieldName && size(rentalMetaHistory)) {
    const sortedRentalMetaHistory = sortBy(rentalMetaHistory, ['cancelledAt'])
    const findLastRentalMetaHistory =
      sortedRentalMetaHistory[size(sortedRentalMetaHistory) - 1]

    if (fieldName && findLastRentalMetaHistory[fieldName])
      rentalMetaHistoryFieldValue = findLastRentalMetaHistory[fieldName]
  }

  return rentalMetaHistoryFieldValue
}

export const getAssignmentIdForTurnover = (contract) => {
  const { rentalMeta = {} } = contract
  let assignmentSerial = ''

  if (size(rentalMeta) && !rentalMeta.tenantId) {
    assignmentSerial = getLeaseRentalMetaHistoryFieldValue(
      rentalMeta,
      'assignmentSerial'
    )
  }

  if (!assignmentSerial && size(contract))
    assignmentSerial = contract.assignmentSerial

  const leaseNo =
    appHelper.getFixedDigits(contract?.partnerSerial, 4) +
    appHelper.getFixedDigits(contract?.propertySerial, 5) +
    appHelper.getFixedDigits(assignmentSerial, 3)

  return leaseNo || ''
}

export const getLeaseMetaFieldValue = (leaseInfo, fieldName) => {
  const rentalMetaInfo =
    size(leaseInfo) && size(leaseInfo.rentalMeta) ? leaseInfo.rentalMeta : {}
  let fieldValue =
    fieldName && size(rentalMetaInfo) && rentalMetaInfo[fieldName]
      ? rentalMetaInfo[fieldName]
      : undefined

  if (!fieldValue && size(leaseInfo.rentalMetaHistory)) {
    fieldValue = getLeaseRentalMetaHistoryFieldValue(
      leaseInfo.rentalMetaHistory,
      fieldName
    )
  }

  return fieldValue
}

export const getTurnoverMonthlyRentAmount = (contract) => {
  const rent = getLeaseMetaFieldValue(contract, 'monthlyRentAmount')
  return 12 * (rent || 0)
}

export const getTotalIncome = async (leaseInfo) => {
  let totalIncome = 0

  if (leaseInfo) {
    totalIncome =
      (await leaseInfo.getBrokeringCommission()) +
      leaseInfo.getAssignmentAddonCommission()
  }

  return totalIncome
}

const getGroupForJournalExcelManagerQuery = (queryData) => {
  const { dateFormat, timeZone, language } = queryData
  const pipeline = {
    $group: {
      _id: '$_id',
      gnr: { $first: '$property.gnr' },
      bnr: { $first: '$property.bnr' },
      snr: { $first: '$property.snr' },
      assignmentId: { $first: '' },
      partnerSerial: { $first: '$partner.serial' },
      propertySerial: { $first: '$property.serial' },
      assignmentSerial: { $first: '$assignmentSerial' },
      propertyLocation: { $first: '$property.location' },
      gnrBnrSnr: { $first: '' },
      type: { $first: '' },
      listingTypeId: { $first: '$property.listingTypeId' },
      propertyTypeId: { $first: '$property.propertyTypeId' },
      propertyType: { $first: '' },
      signDate: {
        $first: {
          $dateToString: {
            format: dateFormat,
            date: '$signDate',
            timezone: timeZone
          }
        }
      },
      status: {
        $first: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    { $eq: ['$status', 'upcoming'] },
                    { $ifNull: ['$rentalMeta', false] },
                    { $not: { $ifNull: ['$rentalMeta.tenantId', false] } }
                  ]
                },
                then: appHelper.translateToUserLng('common.vacant', language)
              },
              {
                case: {
                  $and: [
                    {
                      $or: [
                        { $eq: ['$status', 'active'] },
                        { $eq: ['$status', 'upcoming'] }
                      ]
                    },
                    { $ifNull: ['$rentalMeta.tenantId', false] }
                  ]
                },
                then: appHelper.translateToUserLng('common.occupied', language)
              },
              {
                case: {
                  $eq: ['$status', 'closed']
                },
                then: appHelper.translateToUserLng('common.archived', language)
              }
            ],
            default: ''
          }
        }
      },
      changes: {
        $first: {
          $cond: {
            if: {
              $and: [
                { $ifNull: ['$history', false] },
                { $gt: [{ $size: '$history' }, 0] }
              ]
            },
            then: appHelper.translateToUserLng('common.yes', language),
            else: appHelper.translateToUserLng('common.no', language)
          }
        }
      },
      assignmentFrom: {
        $first: {
          $cond: {
            if: { $ifNull: ['$assignmentFrom', false] },
            then: {
              $dateToString: {
                format: dateFormat,
                date: '$assignmentFrom',
                timezone: timeZone
              }
            },
            else: appHelper.translateToUserLng('common.undetermined', language)
          }
        }
      },
      assignmentTo: {
        $first: {
          $cond: {
            if: { $ifNull: ['$assignmentTo', false] },
            then: {
              $dateToString: {
                format: dateFormat,
                date: '$assignmentTo',
                timezone: timeZone
              }
            },
            else: appHelper.translateToUserLng('common.undetermined', language)
          }
        }
      },
      journaled: {
        $first: {
          $cond: {
            if: { $ifNull: ['$createdAt', false] },
            then: {
              $dateToString: {
                format: dateFormat,
                date: '$createdAt',
                timezone: timeZone
              }
            },
            else: ''
          }
        }
      },
      monthlyRentAmount: { $first: '$listingInfo.monthlyRentAmount' },
      pursuant: { $first: '' },
      agent: { $first: '$agent.profile.name' },
      account: { $first: '$account.name' },
      representative: { $first: '$propertyRepresentative.profile.name' },
      leaseId: { $first: '' },
      leaseSerial: { $first: '$leaseSerial' },
      reacted: { $first: '' },
      rentalMeta: { $first: '$rentalMeta' },
      rentalMetaHistory: { $first: '$rentalMetaHistory' },
      tenant: { $first: '$tenant.name' },
      tenantSerial: { $first: '$tenant.serial' },
      commissions: { $first: '$commissions' },
      other: { $sum: '$other' },
      createdAt: { $first: '$createdAt' }
    }
  }
  return pipeline
}

const getJournalForExcelManager = async (queryData) => {
  const { query, options } = queryData
  const { sort, skip, limit } = options
  const group = getGroupForJournalExcelManagerQuery(queryData)
  const pipeline = [
    { $match: query },
    { $sort: sort },
    { $skip: skip },
    { $limit: limit },
    // Tenant
    {
      $lookup: {
        from: 'tenants',
        localField: 'rentalMeta.tenantId',
        foreignField: '_id',
        as: 'tenant'
      }
    },
    {
      $unwind: {
        path: '$tenant',
        preserveNullAndEmptyArrays: true
      }
    },
    // agent
    {
      $lookup: {
        from: 'users',
        localField: 'agentId',
        foreignField: '_id',
        as: 'agent'
      }
    },
    {
      $unwind: {
        path: '$agent',
        preserveNullAndEmptyArrays: true
      }
    },
    // accounts
    {
      $lookup: {
        from: 'accounts',
        localField: 'accountId',
        foreignField: '_id',
        as: 'account'
      }
    },
    {
      $unwind: {
        path: '$account',
        preserveNullAndEmptyArrays: true
      }
    },
    // Property
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $addFields: {
        'property.location': {
          $concat: [
            { $ifNull: ['$property.location.name', ''] },
            {
              $cond: [
                { $ifNull: ['$property.location.postalCode', false] },
                { $concat: [', ', '$property.location.postalCode'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.city', false] },
                { $concat: [', ', '$property.location.city'] },
                ''
              ]
            },
            {
              $cond: [
                { $ifNull: ['$property.location.country', false] },
                { $concat: [', ', '$property.location.country'] },
                ''
              ]
            }
          ]
        }
      }
    },
    // Partner
    {
      $lookup: {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partner'
      }
    },
    {
      $unwind: {
        path: '$partner',
        preserveNullAndEmptyArrays: true
      }
    },
    // Property Representative
    {
      $lookup: {
        from: 'users',
        localField: 'representativeId',
        foreignField: '_id',
        as: 'propertyRepresentative'
      }
    },
    {
      $unwind: {
        path: '$propertyRepresentative',
        preserveNullAndEmptyArrays: true
      }
    },
    // invoices
    {
      $lookup: {
        from: 'invoices',
        localField: '_id',
        foreignField: 'contractId',
        pipeline: [
          {
            $group: {
              _id: null,
              invoiceIds: {
                $push: '$_id'
              }
            }
          },
          // lookup commission
          {
            $lookup: {
              from: 'commissions',
              localField: 'invoiceIds',
              foreignField: 'invoiceId',
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $eq: ['$type', 'brokering_contract']
                    }
                  }
                }
              ],
              as: 'commission'
            }
          },
          {
            $unwind: {
              path: '$commission'
            }
          },
          {
            $group: {
              _id: null,
              commissionAmount: { $sum: '$commission.amount' }
            }
          }
        ],
        as: 'invoice'
      }
    },
    {
      $unwind: {
        path: '$invoice',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$addons',
        preserveNullAndEmptyArrays: true
      }
    },
    // add fields
    {
      $addFields: {
        commissions: '$invoice.commissionAmount', // convert to currency
        other: {
          $cond: {
            if: { $eq: ['$addons.type', 'assignment'] },
            then: '$addons.total',
            else: 0
          }
        }
      }
    },
    // group
    {
      ...group
    },
    {
      $addFields: {
        totalIncome: {
          $add: [{ $ifNull: ['$commissions', 0] }, '$other']
        }
      }
    },
    {
      $sort: sort
    }
  ]
  const journalData = await ContractCollection.aggregate(pipeline)
  return journalData || []
}

export const getTurnoverDataForExcelCreator = async (params, options) => {
  const { partnerId = {}, userId = {} } = params
  const userInfo = await userHelper.getAnUser({ _id: userId }, null)
  const contractsQuery = await prepareAssignmentsOrLeasesQuery(params)
  const userLanguage = userInfo?.getLanguage()
  const dataCount = await countContracts(contractsQuery, null)
  const currencyOptions = await appHelper.getCurrencyOptions({
    partnerSettingsOrId: partnerId,
    showSymbol: true
  })

  const setting = await SettingCollection.findOne()
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const queryData = {
    query: contractsQuery,
    options,
    dateFormat,
    timeZone,
    language: userLanguage
  }

  const contracts = await getJournalForExcelManager(queryData)
  if (size(contracts)) {
    for (const contract of contracts) {
      contract.assignmentId = getAssignmentIdForTurnover({
        rentalMeta: contract.rentalMeta,
        assignmentSerial: contract.assignmentSerial,
        partnerSerial: contract.partnerSerial,
        propertySerial: contract.propertySerial
      })
      const serial =
        contract.propertySerial && contract.tenantSerial && contract.leaseSerial
          ? appHelper.getFixedDigits(contract.propertySerial, 5) +
            appHelper.getFixedDigits(contract.tenantSerial, 4) +
            appHelper.getFixedDigits(contract.leaseSerial, 3)
          : ''
      contract.leaseId = serial
      contract.gnrBnrSnr = getGnrBnrAndSnrInfo(
        { gnr: contract.gnr, bnr: contract.bnr, snr: contract.snr },
        userLanguage
      )
      contract.type = appHelper.translateToUserLng(
        'common.' +
          listingHelper.getListingTypeNameById(contract.listingTypeId, setting),
        userLanguage
      )
      contract.propertyType = appHelper.translateToUserLng(
        'common.' +
          listingHelper.getPropertyTypeNameById(
            contract.propertyTypeId,
            setting
          ) || '',
        userLanguage
      )
      const reactedDate = getLeaseMetaFieldValue(
        {
          rentalMeta: contract.rentalMeta,
          rentalMetaHistory: contract.rentalMetaHistory
        },
        'signedAt'
      )
      contract.reacted = reactedDate
        ? moment(reactedDate)
            .tz(timeZone)
            .format(partnerSetting.dateTimeSettings.dateFormat)
        : ''
      contract.signDate = contract.reacted
      contract.monthlyRentAmount = await getConvertToCurrency(
        getTurnoverMonthlyRentAmount({
          rentalMeta: contract.rentalMeta,
          rentalMetaHistory: contract.rentalMetaHistory
        }),
        currencyOptions
      )
      contract.commissions = await getConvertToCurrency(
        contract.commissions,
        currencyOptions
      )
      contract.other = await getConvertToCurrency(
        contract.other,
        currencyOptions
      )
      contract.totalIncome = await getConvertToCurrency(
        contract.totalIncome,
        currencyOptions
      )
      contract.pursuant = 'Turnover for rent - Emgll § 1-2 (2) nr.2.'
    }
  }
  return {
    data: contracts,
    total: dataCount
  }
}

export const queryForJournalExcelCreator = async (req) => {
  const { body, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (
    queueInfo?.params?.downloadProcessType === 'download_assignment_journals'
  ) {
    const { partnerId = {}, userId = {} } = queueInfo?.params
    appHelper.validateId({ partnerId })
    appHelper.validateId({ userId })
    const journalAssignmentData = await getAssignmentDataForExcelCreator(
      queueInfo.params,
      options
    )
    return journalAssignmentData
  }
  if (queueInfo?.params?.downloadProcessType === 'download_turnover_journals') {
    const { partnerId = {}, userId = {} } = queueInfo?.params
    appHelper.validateId({ partnerId })
    appHelper.validateId({ userId })
    const journalTurnoverData = await getTurnoverDataForExcelCreator(
      queueInfo.params,
      options
    )
    return journalTurnoverData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const getContractInvoiceInfo = async (
  doc,
  invoiceIds,
  isOverDue = false
) => {
  let totalInvoice = 0,
    totalPaid = 0,
    totalDue = 0,
    totalLost = 0,
    totalPaymentAmount = 0,
    totalCredited = 0

  if (size(doc)) {
    const { _id, partnerId } = doc || {}
    const contractId = doc?.contractId ? doc.contractId : _id
    const query = {
      contractId,
      partnerId,
      invoiceType: 'invoice'
    }

    if (size(invoiceIds)) query._id = { $in: invoiceIds }
    if (isOverDue) query.status = 'overdue'

    const totalPaymentAmountInfo = await InvoicePaymentCollection.aggregate([
      {
        $match: {
          contractId,
          partnerId,
          isFinalSettlement: { $ne: true }
        }
      },
      { $unwind: '$invoices' },
      {
        $lookup: {
          from: 'invoices',
          localField: 'invoices.invoiceId',
          foreignField: '_id',
          as: 'paymentInvoices'
        }
      },
      { $unwind: '$paymentInvoices' },
      {
        $project: {
          contractId: 1,
          invoices: {
            invoiceId: 1,
            amount: 1,
            isFinalSettlement: {
              $cond: {
                if: {
                  $and: [{ $eq: ['$paymentInvoices.isFinalSettlement', true] }]
                },
                then: true,
                else: false
              }
            },
            _id: '$paymentInvoices._id'
          }
        }
      },
      { $match: { 'invoices.isFinalSettlement': false } },
      {
        $group: {
          _id: null,
          total: { $sum: '$invoices.amount' }
        }
      }
    ])

    let invoiceDueTotal = null
    invoiceDueTotal = await InvoiceCollection.aggregate([
      { $match: query },
      {
        $group: {
          _id: null,
          invoiceTotalAmount: { $sum: '$invoiceTotal' },
          invoiceTotalPaidAmount: { $sum: '$totalPaid' },
          invoiceTotalLostAmount: { $sum: '$lostMeta.amount' },
          invoiceTotalCreditedAmount: { $sum: '$creditedAmount' }
        }
      }
    ])
    totalPaymentAmount = size(totalPaymentAmountInfo)
      ? totalPaymentAmountInfo[0].total
      : 0
    if (invoiceDueTotal.length > 0) {
      totalInvoice = invoiceDueTotal[0].invoiceTotalAmount || 0
      totalPaid = invoiceDueTotal[0].invoiceTotalPaidAmount || 0
      totalLost = invoiceDueTotal[0].invoiceTotalLostAmount || 0
      totalCredited = invoiceDueTotal[0].invoiceTotalCreditedAmount || 0
    }
    totalDue = totalInvoice - totalPaid - totalLost + totalCredited
  }

  return {
    invoiceTotalAmount: totalInvoice + totalCredited,
    totalPaymentAmount,
    totalDue,
    totalLost
  }
}

export const getLeaseNumber = async (contractInfo) => {
  let leaseNumber = ''

  if (size(contractInfo)) {
    const { leaseSerial = '' } = contractInfo

    const tenantInfo = (await contractInfo.getTenant()) || {}
    const tenantSerial = tenantInfo?.serial || ''
    const propertyInfo = (await contractInfo.getProperty()) || {}
    const propertySerial = propertyInfo?.serial || ''

    if (tenantSerial && propertySerial && leaseSerial) {
      leaseNumber =
        appHelper.getFixedDigits(propertySerial, 5) +
        appHelper.getFixedDigits(tenantSerial, 4) +
        appHelper.getFixedDigits(leaseSerial, 3)

      if (leaseNumber) return leaseNumber
    }
  }

  return leaseNumber
}

export const getAssignmentNumber = async (contractInfo) => {
  let assignmentNumber = ''

  if (size(contractInfo)) {
    const { assignmentSerial = '' } = contractInfo

    const partnerInfo = (await contractInfo.getPartner()) || {}
    const partnerSerial = partnerInfo?.serial || ''
    const propertyInfo = (await contractInfo.getProperty()) || {}
    const propertySerial = propertyInfo?.serial || ''

    if (partnerSerial && propertySerial && assignmentSerial) {
      assignmentNumber =
        appHelper.getFixedDigits(partnerSerial, 4) +
        appHelper.getFixedDigits(propertySerial, 5) +
        appHelper.getFixedDigits(assignmentSerial, 3)

      if (assignmentNumber) return assignmentNumber
    }
  }

  return assignmentNumber
}

const getDateOfLastTwelveMonths = () => {
  const dateOfMonths = []
  for (let i = 12; i >= 0; i--) {
    const date = new Date()
    const startDate = new Date(date.getFullYear(), date.getMonth() - i, 1)
    startDate.setHours(0, 0, 0, 1)
    const endDate = new Date(date.getFullYear(), date.getMonth() - i + 1, 0)
    endDate.setHours(23, 59, 59, 999)
    dateOfMonths.push({ startDate, endDate })
  }
  return dateOfMonths
}

export const getRetentionRateForDashboard = async (partnerType = '') => {
  const dateOfMonths = getDateOfLastTwelveMonths()
  const pipeline = []
  const match = {
    $match: {
      $or: [
        { 'rentalMeta.status': 'active' },
        { 'rentalMeta.invoicedAsOn': { $exists: true } },
        { 'rentalMeta.contractStartDate': { $exists: true } },
        { 'rentalMeta.contractEndDate': { $exists: true } }
      ]
    }
  }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: { _id: null }
  }
  for (let i = 1; i <= 13; i++) {
    group.$group[`month${i}pep`] = {
      $sum: {
        $cond: {
          if: {
            $and: [
              {
                $lt: [
                  '$rentalMeta.contractStartDate',
                  dateOfMonths[i - 1].endDate
                ]
              },
              {
                $gte: [
                  '$rentalMeta.contractEndDate',
                  dateOfMonths[i - 1].endDate
                ]
              }
            ]
          },
          then: 1,
          else: 0
        }
      }
    }
    group.$group[`month${i}npp`] = {
      $sum: {
        $cond: {
          if: {
            $and: [
              {
                $gt: [
                  '$rentalMeta.contractStartDate',
                  dateOfMonths[i - 1].startDate
                ]
              },
              {
                $lt: [
                  '$rentalMeta.contractStartDate',
                  dateOfMonths[i - 1].endDate
                ]
              }
            ]
          },
          then: 1,
          else: 0
        }
      }
    }
  }
  pipeline.push(group)
  const [retentionRate] = await ContractCollection.aggregate(pipeline)
  return retentionRate
}

export const getAvatarKeyPipeline = (path, defaultPath) => ({
  $cond: {
    if: { $ifNull: [path, false] },
    then: {
      $concat: [appHelper.getCDNDomain(), '/', path]
    },
    else: {
      $concat: [appHelper.getCDNDomain(), '/', defaultPath]
    }
  }
})

const getTenantPipelineForEvictions = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'evictionCases.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: 1,
            userId: 1,
            serial: 1
          }
        },
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            pipeline: [
              {
                $project: {
                  _id: 1,
                  profile: {
                    name: 1,
                    avatarKey: getAvatarKeyPipeline(
                      '$profile.avatarKey',
                      'assets/default-image/user-primary.png'
                    )
                  }
                }
              }
            ],
            as: 'user'
          }
        },
        {
          $unwind: {
            path: '$user',
            preserveNullAndEmptyArrays: true
          }
        }
      ],
      as: 'tenant'
    }
  },
  {
    $unwind: {
      path: '$tenant',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getInvoiceSummaryPipelineForEvictions = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'evictionCases.evictionInvoiceIds',
      foreignField: '_id',
      pipeline: [
        {
          $match: {
            status: 'overdue'
          }
        },
        {
          $group: {
            _id: null,
            invoiceTotal: {
              $sum: '$invoiceTotal'
            },
            totalPaid: { $sum: '$totalPaid' },
            totalBalanced: {
              $sum: {
                $cond: [
                  {
                    $in: [
                      '$invoiceType',
                      ['landlord_invoice', 'landlord_credit_note']
                    ]
                  },
                  '$totalBalanced',
                  0
                ]
              }
            },
            creditedAmount: {
              $sum: {
                $cond: [
                  {
                    $not: {
                      $in: [
                        '$invoiceType',
                        ['landlord_invoice', 'landlord_credit_note']
                      ]
                    }
                  },
                  '$creditedAmount',
                  0
                ]
              }
            },
            lostAmount: {
              $sum: {
                $cond: [
                  {
                    $not: {
                      $in: [
                        '$invoiceType',
                        ['landlord_invoice', 'landlord_credit_note']
                      ]
                    }
                  },
                  '$lostMeta.amount',
                  0
                ]
              }
            },
            totalDue: {
              $sum: {
                $subtract: [
                  {
                    $add: [
                      { $ifNull: ['$invoiceTotal', 0] },
                      { $ifNull: ['$creditedAmount', 0] }
                    ]
                  },
                  {
                    $add: [
                      { $ifNull: ['$lostAmount', 0] },
                      { $ifNull: ['$totalPaid', 0] },
                      { $ifNull: ['$totalBalanced', 0] }
                    ]
                  }
                ]
              }
            }
          }
        }
      ],
      as: 'invoice'
    }
  },
  {
    $unwind: {
      path: '$invoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getInvoicePipelineForEvictions = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'evictionCases.invoiceId',
      foreignField: '_id',
      as: 'mainInvoice'
    }
  },
  {
    $unwind: {
      path: '$mainInvoice',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getBranchPipelineForEvictions = () => [
  {
    $lookup: {
      from: 'branches',
      localField: 'branchId',
      foreignField: '_id',
      as: 'branch'
    }
  },
  {
    $unwind: {
      path: '$branch',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getAgentPipelineForEvictions = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'evictionCases.agentId',
      foreignField: '_id',
      as: 'agent'
    }
  },
  {
    $unwind: {
      path: '$agent',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getFilesPipelineForEvictions = () => [
  {
    $lookup: {
      from: 'files',
      localField: 'evictionCases.invoiceId',
      foreignField: 'invoiceId',
      as: 'evictionNoticeFile'
    }
  },
  {
    $addFields: {
      evictionNoticeFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$evictionNoticeFile', []] },
            as: 'file',
            cond: {
              $eq: ['$$file.type', 'eviction_notice_attachment_pdf']
            }
          }
        }
      }
    }
  },
  {
    $lookup: {
      from: 'files',
      localField: '_id',
      foreignField: 'contractId',
      as: 'files',
      pipeline: [
        {
          $sort: {
            createdAt: 1
          }
        }
      ]
    }
  },
  {
    $addFields: {
      contractNoticeFile: {
        $first: {
          $filter: {
            input: { $ifNull: ['$files', []] },
            as: 'file',
            cond: {
              $eq: ['$$file.type', 'eviction_notice_attachment_pdf']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      files: {
        $filter: {
          input: { $ifNull: ['$files', []] },
          as: 'file',
          cond: {
            $or: [
              {
                $and: [
                  { $eq: ['$$file.invoiceId', '$evictionCases.invoiceId'] },
                  { $eq: ['$$file.type', 'eviction_document_pdf'] }
                ]
              },
              {
                $in: ['$$file.type', ['esigning_lease_pdf', 'lease_pdf']]
              }
            ]
          }
        }
      }
    }
  },
  {
    $addFields: {
      evictionNoticeFile: {
        $cond: [
          { $ifNull: ['$evictionNoticeFile', false] },
          ['$evictionNoticeFile'],
          {
            $cond: [
              { $ifNull: ['$contractNoticeFile', false] },
              ['$contractNoticeFile'],
              []
            ]
          }
        ]
      }
    }
  },
  {
    $addFields: {
      files: {
        $concatArrays: [{ $ifNull: ['$files', []] }, '$evictionNoticeFile']
      }
    }
  }
]

const getPropertyPipelineForEvictions = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'property'
    }
  },
  {
    $unwind: {
      path: '$property',
      preserveNullAndEmptyArrays: true
    }
  }
]

const countEvictions = async (query) => {
  const { contractQuery = {}, evictionQuery = {} } = query
  const evictions = await ContractCollection.aggregate([
    {
      $match: {
        evictionCases: {
          $exists: true
        },
        ...contractQuery
      }
    },
    {
      $unwind: '$evictionCases'
    },
    {
      $match: evictionQuery
    }
  ])
  return size(evictions)
}

const getEvictionsForQuery = async (body) => {
  const { query, options } = body
  const { limit, skip, sort } = options
  const { contractQuery, evictionQuery } = query
  const pipeline = [
    {
      $match: {
        evictionCases: {
          $exists: true
        },
        ...contractQuery
      }
    },
    {
      $unwind: '$evictionCases'
    },
    {
      $match: evictionQuery
    },
    ...getInvoiceSummaryPipelineForEvictions(),
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getInvoicePipelineForEvictions(),
    ...getTenantPipelineForEvictions(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...getBranchPipelineForEvictions(),
    ...getAgentPipelineForEvictions(),
    ...getPropertyPipelineForEvictions(),
    ...appHelper.getListingFirstImageUrl('$property.images', 'property'),
    ...getFilesPipelineForEvictions(),
    {
      $project: {
        _id: 1,
        invoiceId: '$evictionCases.invoiceId',
        property: {
          _id: 1,
          imageUrl: 1,
          location: 1,
          apartmentId: 1,
          propertyTypeId: 1,
          listingTypeId: 1,
          serial: 1
        },
        invoice: {
          invoiceTotal: 1,
          totalPaid: 1,
          creditedAmount: 1,
          totalDue: 1
        },
        'invoice.invoiceSerialId': '$mainInvoice.invoiceSerialId',
        leaseSerial: '$evictionCases.leaseSerial',
        status: '$evictionCases.status',
        hasPaid: '$evictionCases.hasPaid',
        tenant: {
          _id: '$tenant._id',
          name: '$tenant.name',
          avatarKey: '$tenant.user.profile.avatarKey',
          serial: 1
        },
        account: '$accountInfo',
        branch: {
          _id: 1,
          name: 1
        },
        agent: {
          _id: 1,
          name: '$agent.profile.name',
          avatarKey: getAvatarKeyPipeline(
            '$agent.profile.avatarKey',
            'assets/default-image/user-primary.png'
          )
        },
        files: {
          _id: 1,
          name: 1,
          title: 1,
          type: 1
        },
        createdAt: 1,
        amount: '$evictionCases.amount'
      }
    }
  ]
  const evictions = await ContractCollection.aggregate(pipeline)
  return evictions
}

const prepareQueryForEvictionsQuery = async (query) => {
  const {
    accountId,
    agentId,
    branchId,
    contractId,
    createdAtDateRange,
    hasPaid,
    leaseSerial,
    partnerId,
    propertyId,
    searchKeyword,
    status,
    tenantId
  } = query
  const contractQuery = {}
  const evictionQuery = {}
  if (partnerId) contractQuery.partnerId = partnerId
  evictionQuery['evictionCases'] = { $exists: true }
  if (status) {
    if (status === 'new') {
      evictionQuery['$or'] = [
        { 'evictionCases.status': { $exists: false } },
        { 'evictionCases.status': status }
      ]
    } else evictionQuery['evictionCases.status'] = status
  }
  if (hasPaid === true) {
    evictionQuery['evictionCases.hasPaid'] = true
  }
  if (hasPaid === false) {
    evictionQuery['evictionCases.hasPaid'] = { $ne: true }
  }
  if (branchId) contractQuery.branchId = branchId
  if (agentId) evictionQuery['evictionCases.agentId'] = agentId
  if (tenantId) evictionQuery['evictionCases.tenantId'] = tenantId
  if (accountId) contractQuery.accountId = accountId
  if (propertyId) contractQuery.propertyId = propertyId
  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    contractQuery.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  // For lease filter
  if (contractId && leaseSerial) {
    contractQuery._id = contractId
    evictionQuery['evictionCases.leaseSerial'] = leaseSerial
  }
  if (searchKeyword) {
    const propertyIds = await listingHelper.getUniqueFieldValueOfListings(
      '_id',
      { partnerId, 'location.name': new RegExp(searchKeyword, 'i') }
    )
    contractQuery.propertyId = { $in: propertyIds }
  }

  return { contractQuery, evictionQuery }
}

export const evictions = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  const { query } = body
  const contractQuery = { partnerId }
  const { propertyId = '', requestFrom = '' } = query
  if (requestFrom === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    contractQuery.propertyId = propertyId
  }

  body.query.partnerId = partnerId
  body.query = await prepareQueryForEvictionsQuery(body.query)
  const { options } = body
  appHelper.validateSortForQuery(options.sort)
  const evictions = await getEvictionsForQuery(body)
  const filteredDocuments = await countEvictions(body.query)
  const totalDocuments = await countEvictions({ contractQuery })
  return {
    data: evictions,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

const getEvictionsSummary = async (query) => {
  const { contractQuery = {}, evictionQuery = {} } = query
  const [evictionSummary] = await ContractCollection.aggregate([
    {
      $match: {
        evictionCases: {
          $exists: true
        },
        ...contractQuery
      }
    },
    {
      $unwind: '$evictionCases'
    },
    {
      $match: evictionQuery
    },
    ...getInvoiceSummaryPipelineForEvictions(),
    {
      $group: {
        _id: null,
        totalInvoiced: {
          $sum: { $ifNull: ['$invoice.invoiceTotal', 0] }
        },
        totalRentDue: {
          $sum: { $ifNull: ['$invoice.totalDue', 0] }
        },
        totalEvictionProduced: {
          $sum: {
            $cond: [{ $not: { $eq: ['$evictionCases.status', 'new'] } }, 1, 0]
          }
        }
      }
    }
  ])
  return evictionSummary
}

export const evictionsSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  const preparedQuery = await prepareQueryForEvictionsQuery(body)
  return await getEvictionsSummary(preparedQuery)
}

const prepareQueryForQueryLeases = async (query) => {
  const { isClosed, partnerId, personType, userId, searchKeyword } = query
  const preparedQuery = {
    partnerId
  }
  let tenant = {}
  if (personType === 'tenant') {
    tenant = await tenantHelper.getATenant({
      userId,
      partnerId
    })
    if (!size(tenant)) throw new CustomError(404, 'Tenant not found')
    if (isClosed) {
      preparedQuery.$or = [
        { 'rentalMeta.tenants.tenantId': tenant._id },
        { 'rentalMetaHistory.tenants.tenantId': tenant._id }
      ]
    } else {
      preparedQuery['rentalMeta.tenants.tenantId'] = tenant._id
    }
  } else {
    const accountIds = await accountHelper.getAccountIdsByQuery({
      personId: userId,
      partnerId
    })
    if (!size(accountIds)) throw new CustomError(404, 'Landlord not found')
    preparedQuery['accountId'] = {
      $in: accountIds
    }
  }
  if (isClosed) {
    preparedQuery.$and = [
      {
        $or: [
          { 'rentalMeta.status': 'closed' },
          { rentalMetaHistory: { $exists: true } }
        ]
      }
    ]
  } else {
    preparedQuery.hasRentalContract = true
    preparedQuery.status = { $ne: 'closed' }
  }

  if (searchKeyword) {
    const propertyIds = await listingHelper.getUniqueFieldValueOfListings(
      '_id',
      { partnerId, 'location.name': new RegExp(searchKeyword, 'i') }
    )
    preparedQuery.propertyId = { $in: propertyIds }
  }
  return { preparedQuery, tenantId: tenant._id }
}

const getAgentPipelineForLeaseDetails = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agentId',
      foreignField: '_id',
      pipeline: [
        {
          $addFields: {
            emails: {
              $ifNull: ['$emails', []]
            }
          }
        },
        {
          $addFields: {
            fbMail: { $ifNull: ['$services.facebook.email', null] },
            verifiedMails: {
              $filter: {
                input: '$emails',
                as: 'email',
                cond: {
                  $eq: ['$$email.verified', true]
                }
              }
            },
            unverifiedMail: {
              $cond: {
                if: { $gt: [{ $size: '$emails' }, 0] },
                then: { $first: '$emails' },
                else: null
              }
            }
          }
        },
        {
          $addFields: {
            verifiedMail: {
              $cond: {
                if: { $gt: [{ $size: '$verifiedMails' }, 0] },
                then: { $last: '$verifiedMails' },
                else: null
              }
            }
          }
        },
        {
          $project: {
            name: '$profile.name',
            email: {
              $switch: {
                branches: [
                  {
                    case: {
                      $and: [
                        { $eq: ['$verifiedMail', null] },
                        { $ne: ['$fbMail', null] }
                      ]
                    },
                    then: '$fbMail'
                  },
                  {
                    case: {
                      $and: [
                        { $eq: ['$verifiedMail', null] },
                        { $ne: ['$unverifiedMail', null] }
                      ]
                    },
                    then: '$unverifiedMail.address'
                  }
                ],
                default: '$verifiedMail.address'
              }
            },
            phoneNumber: '$profile.phoneNumber',
            avatarKey: '$profile.avatarKey'
          }
        }
      ],
      as: 'agent'
    }
  },
  {
    $unwind: {
      path: '$agent',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getAgentPipelineForLease = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agentId',
      foreignField: '_id',
      as: 'agent'
    }
  },
  {
    $unwind: {
      path: '$agent',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getPropertyPipelineForLease = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'property'
    }
  },
  {
    $unwind: {
      path: '$property',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      'property.location.apartmentId': '$property.apartmentId'
    }
  }
]

const getPartnerPipelineForLease = () => [
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: {
      path: '$partner',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getPropertyRoomPipelineForLease = () => [
  {
    $lookup: {
      from: 'property_rooms',
      localField: 'propertyId',
      foreignField: 'propertyId',
      as: 'rooms'
    }
  },
  {
    $unwind: {
      path: '$rooms',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      issues: {
        $filter: {
          input: { $ifNull: ['$rooms.items', []] },
          as: 'item',
          cond: {
            $eq: ['$$item.status', 'issues']
          }
        }
      }
    }
  },
  {
    $addFields: {
      sizeOfIssue: {
        $size: '$issues'
      }
    }
  }
]

const getFinalProjectPipelineForLease = (
  dateFormat,
  timeZone,
  selectedMonth
) => [
  {
    $project: {
      _id: 1,
      propertyId: 1,
      property: '$property.location',
      partner: '$partner.name',
      hasInvoices: 1,
      contractStartDate: {
        $dateToString: {
          format: dateFormat,
          date: '$rentalMeta.contractStartDate',
          timezone: timeZone
        }
      },
      contractEndDate: {
        $cond: [
          { $ifNull: ['$rentalMeta.contractEndDate', false] },
          {
            $dateToString: {
              format: dateFormat,
              date: '$rentalMeta.contractEndDate',
              timezone: timeZone
            }
          },
          'undetermined'
        ]
      },
      agent: {
        _id: 1,
        name: '$agent.profile.name',
        avatar: getAvatarKeyPipeline(
          '$agent.profile.avatarKey',
          'assets/default-image/user-primary.png'
        )
      },
      leaseStatus: {
        $cond: [
          { $eq: ['$rentalMeta.status', 'active'] },
          {
            $cond: [
              { $lte: ['$rentalMeta.contractEndDate', selectedMonth._d] },
              'soon_ending',
              'active'
            ]
          },
          '$rentalMeta.status'
        ]
      },
      toPay: 1,
      numOfIssues: 1,
      isMovedIn: {
        $ifNull: ['$rentalMeta.isMovedIn', false]
      },
      invoiceStatus: 1,
      amount: 1,
      rentalMeta: {
        createdAt: 1
      },
      accountId: 1,
      leaseTerminated: {
        $cond: [
          {
            $and: [
              {
                $ifNull: ['$terminatedByUserId', false]
              },
              {
                $eq: ['$rentalMeta.status', 'active']
              }
            ]
          },
          true,
          false
        ]
      },
      accountInfo: 1,
      coTenants: '$coTenantUser',
      enabledLeaseEsigning: {
        $ifNull: ['$rentalMeta.enabledLeaseEsigning', false]
      },
      mainTenant: '$mainTenantUser',
      landlordSigned: {
        $cond: [
          { $eq: ['$rentalMeta.landlordLeaseSigningStatus.signed', true] },
          true,
          false
        ]
      },
      tenantSigned: {
        $cond: [
          { $eq: ['$rentalMeta.tenantLeaseSigningStatus.signed', true] },
          true,
          false
        ]
      }
    }
  }
]

const getInvoicePipelineForLease = () => [
  {
    $lookup: {
      from: 'invoices',
      foreignField: 'contractId',
      localField: '_id',
      as: 'unfilteredInvoices'
    }
  },
  {
    $addFields: {
      invoices: {
        $filter: {
          input: '$unfilteredInvoices',
          as: 'invoice',
          cond: {
            $and: [
              { $eq: ['$$invoice.invoiceType', 'invoice'] },
              { $eq: ['$$invoice.propertyId', '$propertyId'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$invoices',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: '$_id',
      numOfIssues: {
        $first: '$numOfIssues'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      agent: {
        $first: '$agent'
      },
      accountId: {
        $first: '$accountId'
      },
      partner: {
        $first: '$partner'
      },
      property: {
        $first: '$property'
      },
      partnerId: {
        $first: '$partnerId'
      },
      propertyId: {
        $first: '$propertyId'
      },
      hasInvoices: {
        $first: '$hasInvoices'
      },
      overdue: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      due: {
        $sum: {
          $cond: [
            { $not: { $in: ['$invoices.status', ['paid', 'lost']] } },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      overpaid: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$invoices.status', 'paid'] },
                { $ifNull: ['$invoices.isOverPaid', false] }
              ]
            },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      evictionDue: {
        $sum: {
          $cond: [
            {
              $ifNull: ['$invoices.evictionDueReminderSent', false]
            },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      eviction: {
        $sum: {
          $cond: [
            {
              $ifNull: ['$invoices.evictionNoticeSent', false]
            },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      }
    }
  },
  {
    $addFields: {
      invoiceStatus: {
        $switch: {
          branches: [
            { case: { $ne: ['$evictionDue', 0] }, then: 'eviction_notice_due' },
            { case: { $ne: ['$eviction', 0] }, then: 'eviction_notice' },
            { case: { $ne: ['$overdue', 0] }, then: 'overdue' },
            { case: { $ne: ['$due', 0] }, then: 'to_pay' },
            { case: { $ne: ['$overpaid', 0] }, then: 'over_paid' }
          ],
          default: 'paid'
        }
      },
      amount: {
        $switch: {
          branches: [
            { case: { $ne: ['$evictionDue', 0] }, then: '$evictionDue' },
            { case: { $ne: ['$eviction', 0] }, then: '$eviction' },
            { case: { $ne: ['$overdue', 0] }, then: '$overdue' },
            { case: { $ne: ['$due', 0] }, then: '$due' },
            { case: { $ne: ['$overpaid', 0] }, then: '$overpaid' }
          ],
          default: 0
        }
      }
    }
  }
]

const getTotalPayoutPipelineForLease = () => [
  {
    $lookup: {
      from: 'transactions',
      localField: '_id',
      foreignField: 'contractId',
      as: 'transactions'
    }
  },
  {
    $addFields: {
      filteredTransactions: {
        $filter: {
          input: { $ifNull: ['$transactions', []] },
          as: 'payout',
          cond: {
            $and: [
              { $eq: ['$$payout.partnerId', '$partnerId'] },
              { $eq: ['$$payout.propertyId', '$propertyId'] },
              { $eq: ['$$payout.type', 'payout'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$filteredTransactions',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: '$_id',
      numOfIssues: {
        $first: '$numOfIssues'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      agent: {
        $first: '$agent'
      },
      accountId: {
        $first: '$accountId'
      },
      partner: {
        $first: '$partner'
      },
      partnerId: {
        $first: '$partnerId'
      },
      property: {
        $first: '$property'
      },
      propertyId: {
        $first: '$propertyId'
      },
      amount: {
        $sum: '$filteredTransactions.amount'
      },
      hasInvoices: {
        $first: '$hasInvoices'
      }
    }
  },
  {
    $addFields: {
      invoiceStatus: 'payout'
    }
  }
]

const getInvoicePipelineForLeaseDetails = () => [
  {
    $lookup: {
      from: 'invoices',
      foreignField: 'contractId',
      localField: '_id',
      as: 'unfilteredInvoices'
    }
  },
  {
    $addFields: {
      invoices: {
        $filter: {
          input: '$unfilteredInvoices',
          as: 'invoice',
          cond: {
            $and: [
              { $eq: ['$$invoice.invoiceType', 'invoice'] },
              { $eq: ['$$invoice.propertyId', '$propertyId'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$invoices',
      preserveNullAndEmptyArrays: true
    }
  },
  //Since we need latest invoice for showing kid number in lease details
  {
    $sort: {
      'invoices.invoiceMonth': -1
    }
  },
  {
    $group: {
      _id: '$_id',
      accountId: { $first: '$accountId' },
      numOfIssues: {
        $first: '$numOfIssues'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      agent: {
        $first: '$agent'
      },
      partner: {
        $first: '$partner'
      },
      property: {
        $first: '$property'
      },
      partnerId: {
        $first: '$partnerId'
      },
      propertyId: {
        $first: '$propertyId'
      },
      overdue: {
        $sum: {
          $cond: [
            { $eq: ['$invoices.status', 'overdue'] },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      due: {
        $sum: {
          $cond: [
            { $not: { $in: ['$invoices.status', ['paid', 'lost']] } },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      overpaid: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ['$invoices.status', 'paid'] },
                { $ifNull: ['$invoices.isOverPaid', false] }
              ]
            },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      evictionDue: {
        $sum: {
          $cond: [
            {
              $ifNull: ['$invoices.evictionDueReminderSent', false]
            },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      eviction: {
        $sum: {
          $cond: [
            {
              $ifNull: ['$invoices.evictionNoticeSent', false]
            },
            {
              $subtract: [
                { $ifNull: ['$invoices.totalPaid', 0] },
                {
                  $add: [
                    { $ifNull: ['$invoices.invoiceTotal', 0] },
                    { $ifNull: ['$invoices.creditedAmount', 0] }
                  ]
                }
              ]
            },
            0
          ]
        }
      },
      //For lease details
      numOfInvoice: {
        $sum: 1
      },
      leaseSerial: {
        $first: '$leaseSerial'
      },
      assignmentSerial: {
        $first: '$assignmentSerial'
      },
      payoutTo: {
        $first: '$payoutTo'
      },
      monthlyPayoutDate: {
        $first: '$monthlyPayoutDate'
      },
      brokeringCommissionAmount: {
        $first: '$brokeringCommissionAmount'
      },
      brokeringCommissionType: {
        $first: '$brokeringCommissionType'
      },
      rentalManagementCommissionType: {
        $first: '$rentalManagementCommissionType'
      },
      rentalManagementCommissionAmount: {
        $first: '$rentalManagementCommissionAmount'
      },
      assignmentFrom: {
        $first: '$assignmentFrom'
      },
      assignmentTo: {
        $first: '$assignmentTo'
      },
      addons: {
        $first: '$addons'
      },
      kidNumber: {
        $first: '$invoices.kidNumber'
      },
      invoiceAccountNumber: {
        $first: '$invoices.invoiceAccountNumber'
      },
      dueDate: {
        $first: '$invoices.dueDate'
      },
      invoiceMonth: {
        $first: '$invoices.invoiceMonth'
      },
      invoicePdf: {
        $first: '$invoices.pdf'
      },
      evictionNoticeSentOn: {
        $first: '$invoices.evictionNoticeSentOn'
      },
      leaseContractPdfGenerated: {
        $first: '$leaseContractPdfGenerated'
      },
      //For assignment progression
      assignmentContractPdfGenerated: {
        $first: '$assignmentContractPdfGenerated'
      },
      enabledEsigning: {
        $first: '$enabledEsigning'
      },
      agentAssignmentSigningStatus: {
        $first: '$agentAssignmentSigningStatus'
      },
      landlordAssignmentSigningStatus: {
        $first: '$landlordAssignmentSigningStatus'
      }
    }
  },
  {
    $addFields: {
      invoiceStatus: {
        $switch: {
          branches: [
            { case: { $ne: ['$evictionDue', 0] }, then: 'eviction_notice_due' },
            { case: { $ne: ['$eviction', 0] }, then: 'eviction_notice' },
            { case: { $ne: ['$overdue', 0] }, then: 'overdue' },
            { case: { $ne: ['$due', 0] }, then: 'to_pay' },
            { case: { $ne: ['$overpaid', 0] }, then: 'over_paid' }
          ],
          default: 'paid'
        }
      },
      amount: {
        $switch: {
          branches: [
            { case: { $ne: ['$evictionDue', 0] }, then: '$evictionDue' },
            { case: { $ne: ['$eviction', 0] }, then: '$eviction' },
            { case: { $ne: ['$overdue', 0] }, then: '$overdue' },
            { case: { $ne: ['$due', 0] }, then: '$due' },
            { case: { $ne: ['$overpaid', 0] }, then: '$overpaid' }
          ],
          default: 0
        }
      }
    }
  }
]

const getTotalPayoutPipelineForLeaseDetails = () => [
  {
    $lookup: {
      from: 'transactions',
      localField: '_id',
      foreignField: 'contractId',
      as: 'transactions'
    }
  },
  {
    $addFields: {
      filteredTransactions: {
        $filter: {
          input: { $ifNull: ['$transactions', []] },
          as: 'payout',
          cond: {
            $and: [
              { $eq: ['$$payout.partnerId', '$partnerId'] },
              { $eq: ['$$payout.propertyId', '$propertyId'] },
              { $eq: ['$$payout.type', 'payout'] }
            ]
          }
        }
      }
    }
  },
  {
    $unwind: {
      path: '$filteredTransactions',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: '$_id',
      accountId: { $first: '$accountId' },
      numOfIssues: {
        $first: '$numOfIssues'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      agent: {
        $first: '$agent'
      },
      partner: {
        $first: '$partner'
      },
      partnerId: {
        $first: '$partnerId'
      },
      property: {
        $first: '$property'
      },
      propertyId: {
        $first: '$propertyId'
      },
      amount: {
        $sum: '$filteredTransactions.amount'
      },
      //For lease details
      payoutTo: {
        $first: '$payoutTo'
      },
      leaseSerial: {
        $first: '$leaseSerial'
      },
      assignmentSerial: {
        $first: '$assignmentSerial'
      },
      monthlyPayoutDate: {
        $first: '$monthlyPayoutDate'
      },
      brokeringCommissionAmount: {
        $first: '$brokeringCommissionAmount'
      },
      brokeringCommissionType: {
        $first: '$brokeringCommissionType'
      },
      rentalManagementCommissionType: {
        $first: '$rentalManagementCommissionType'
      },
      rentalManagementCommissionAmount: {
        $first: '$rentalManagementCommissionAmount'
      },
      assignmentFrom: {
        $first: '$assignmentFrom'
      },
      assignmentTo: {
        $first: '$assignmentTo'
      },
      addons: {
        $first: '$addons'
      },
      rentStatus: {
        $first: '$rentStatus'
      },
      invoiceAmount: {
        $first: '$invoiceAmount'
      },
      leaseContractPdfGenerated: {
        $first: '$leaseContractPdfGenerated'
      },
      //For assignment progression
      assignmentContractPdfGenerated: {
        $first: '$assignmentContractPdfGenerated'
      },
      enabledEsigning: {
        $first: '$enabledEsigning'
      },
      agentAssignmentSigningStatus: {
        $first: '$agentAssignmentSigningStatus'
      },
      landlordAssignmentSigningStatus: {
        $first: '$landlordAssignmentSigningStatus'
      }
    }
  },
  {
    $addFields: {
      invoiceStatus: 'payout'
    }
  }
]

const getHasInvoicePipelineForLease = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      as: 'invoicesInfo'
    }
  },
  {
    $addFields: {
      hasInvoices: {
        $cond: [
          {
            $gt: [{ $size: { $ifNull: ['$invoicesInfo', []] } }, 0]
          },
          true,
          false
        ]
      }
    }
  }
]

const getLeaseTenantsInfo = () => [
  {
    $addFields: {
      mainTenant: '$rentalMeta.tenantId',
      coTenants: '$rentalMeta.tenants'
    }
  },
  {
    $addFields: {
      coTenants: {
        $filter: {
          input: '$coTenants',
          as: 'tenant',
          cond: {
            $ne: ['$$tenant.tenantId', '$mainTenant']
          }
        }
      }
    }
  },
  ...getMainTenantLookup(),
  ...getCoTenantsLookup(),
  ...getMainTenantUserInfo()
]

const groupAllTenantTogether = () => [
  {
    $group: {
      _id: '$_id',
      accountId: {
        $first: '$accountId'
      },
      accountInfo: {
        $first: '$accountInfo'
      },
      amount: {
        $first: '$amount'
      },
      agent: {
        $first: '$agent'
      },
      coTenantUser: {
        $push: '$coTenantUser'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      mainTenantUser: {
        $first: '$mainTenantUser'
      },
      invoiceStatus: {
        $first: '$invoiceStatus'
      },
      numOfIssues: {
        $first: '$numOfIssues'
      },
      partner: {
        $first: '$partner'
      },
      partnerId: {
        $first: '$partnerId'
      },
      property: {
        $first: '$property'
      },
      propertyId: {
        $first: '$propertyId'
      },
      toPay: {
        $first: '$toPay'
      },
      hasInvoices: {
        $first: '$hasInvoices'
      }
    }
  }
]
const getLeasesForQuery = async (params) => {
  const {
    query,
    options,
    isClosed,
    personType,
    dateFormat,
    timeZone,
    selectedMonth,
    tenantId
  } = params
  let closedLeasePipeline = []
  if (isClosed) {
    if (personType === 'tenant') {
      closedLeasePipeline = [
        {
          $addFields: {
            rentalMeta: {
              $concatArrays: [
                ['$rentalMeta'],
                { $ifNull: ['$rentalMetaHistory', []] }
              ]
            }
          }
        },
        appHelper.getUnwindPipeline('rentalMeta', false),
        {
          $addFields: {
            rentalMetaTenantExist: {
              $first: {
                $filter: {
                  input: { $ifNull: ['$rentalMeta.tenants', []] },
                  as: 'tenant',
                  cond: {
                    $eq: ['$$tenant.tenantId', tenantId]
                  }
                }
              }
            }
          }
        },
        {
          $match: {
            'rentalMeta.status': 'closed',
            rentalMetaTenantExist: {
              $exists: true
            }
          }
        }
      ]
    } else {
      closedLeasePipeline = [
        {
          $addFields: {
            rentalMeta: {
              $concatArrays: [
                ['$rentalMeta'],
                { $ifNull: ['$rentalMetaHistory', []] }
              ]
            }
          }
        },
        appHelper.getUnwindPipeline('rentalMeta', false),
        {
          $match: {
            'rentalMeta.status': 'closed'
          }
        }
      ]
    }
  }
  const sort = { 'rentalMeta.createdAt': -1 }
  const { skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    ...closedLeasePipeline,
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getAgentPipelineForLease(),
    ...getPropertyPipelineForLease(),
    ...getPartnerPipelineForLease(),
    ...getHasInvoicePipelineForLease(),
    ...getPropertyRoomPipelineForLease(),
    {
      $group: {
        _id: '$_id',
        numOfIssues: {
          $sum: '$sizeOfIssue'
        },
        rentalMeta: {
          $first: '$rentalMeta'
        },
        agent: {
          $first: '$agent'
        },
        partner: {
          $first: '$partner'
        },
        partnerId: {
          $first: '$partnerId'
        },
        property: {
          $first: '$property'
        },
        propertyId: {
          $first: '$propertyId'
        },
        hasInvoices: {
          $first: '$hasInvoices'
        },
        accountId: {
          $first: '$accountId'
        }
      }
    }
  ]
  if (personType === 'tenant') pipeline.push(...getInvoicePipelineForLease())
  else pipeline.push(...getTotalPayoutPipelineForLease())
  pipeline.push(
    ...getLeaseTenantsInfo(),
    ...getCoTenantUserInfo(),
    ...groupAllTenantTogether(),
    ...appHelper.getCommonAccountInfoPipeline()
  )
  pipeline.push(
    ...getFinalProjectPipelineForLease(dateFormat, timeZone, selectedMonth)
  )
  pipeline.push({
    $sort: sort
  })
  const leases = await ContractCollection.aggregate(pipeline)
  return leases
}

const getTenantsProject = () => [
  {
    $project: {
      mainTenant: '$rentalMeta.tenantId',
      coTenants: '$rentalMeta.tenants'
    }
  },
  {
    $project: {
      mainTenant: 1,
      coTenants: {
        $filter: {
          input: '$coTenants',
          as: 'tenant',
          cond: {
            $ne: ['$$tenant.tenantId', '$mainTenant']
          }
        }
      }
    }
  }
]

const getMainTenantLookup = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'mainTenant',
      foreignField: '_id',
      as: 'mainTenant'
    }
  },
  {
    $unwind: {
      path: '$mainTenant',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getCoTenantsLookup = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'coTenants.tenantId',
      foreignField: '_id',
      as: 'coTenants'
    }
  },
  {
    $unwind: {
      path: '$coTenants',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getMainTenantUserInfo = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'mainTenant.userId',
      foreignField: '_id',
      pipeline: [
        {
          $addFields: {
            emails: {
              $ifNull: ['$emails', []]
            }
          }
        },
        {
          $addFields: {
            fbMail: { $ifNull: ['$services.facebook.email', null] },
            verifiedMails: {
              $filter: {
                input: '$emails',
                as: 'email',
                cond: {
                  $eq: ['$$email.verified', true]
                }
              }
            },
            unverifiedMail: {
              $cond: {
                if: { $gt: [{ $size: '$emails' }, 0] },
                then: { $first: '$emails' },
                else: null
              }
            }
          }
        },
        {
          $addFields: {
            verifiedMail: {
              $cond: {
                if: { $gt: [{ $size: '$verifiedMails' }, 0] },
                then: { $last: '$verifiedMails' },
                else: null
              }
            }
          }
        },
        {
          $project: {
            name: '$profile.name',
            email: {
              $switch: {
                branches: [
                  {
                    case: {
                      $and: [
                        { $eq: ['$verifiedMail', null] },
                        { $ne: ['$fbMail', null] }
                      ]
                    },
                    then: '$fbMail'
                  },
                  {
                    case: {
                      $and: [
                        { $eq: ['$verifiedMail', null] },
                        { $ne: ['$unverifiedMail', null] }
                      ]
                    },
                    then: '$unverifiedMail.address'
                  }
                ],
                default: '$verifiedMail.address'
              }
            },
            phoneNumber: '$profile.phoneNumber',
            avatarKey: getAvatarKeyPipeline(
              '$profile.avatarKey',
              'assets/default-image/user-primary.png'
            )
          }
        }
      ],
      as: 'mainTenantUser'
    }
  },
  {
    $unwind: {
      path: '$mainTenantUser',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getCoTenantUserInfo = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'coTenants.userId',
      foreignField: '_id',
      pipeline: [
        {
          $addFields: {
            emails: {
              $ifNull: ['$emails', []]
            }
          }
        },
        {
          $addFields: {
            fbMail: { $ifNull: ['$services.facebook.email', null] },
            verifiedMails: {
              $filter: {
                input: '$emails',
                as: 'email',
                cond: {
                  $eq: ['$$email.verified', true]
                }
              }
            },
            unverifiedMail: {
              $cond: {
                if: { $gt: [{ $size: '$emails' }, 0] },
                then: { $first: '$emails' },
                else: null
              }
            }
          }
        },
        {
          $addFields: {
            verifiedMail: {
              $cond: {
                if: { $gt: [{ $size: '$verifiedMails' }, 0] },
                then: { $last: '$verifiedMails' },
                else: null
              }
            }
          }
        },
        {
          $project: {
            name: '$profile.name',
            email: {
              $switch: {
                branches: [
                  {
                    case: {
                      $and: [
                        { $eq: ['$verifiedMail', null] },
                        { $ne: ['$fbMail', null] }
                      ]
                    },
                    then: '$fbMail'
                  },
                  {
                    case: {
                      $and: [
                        { $eq: ['$verifiedMail', null] },
                        { $ne: ['$unverifiedMail', null] }
                      ]
                    },
                    then: '$unverifiedMail.address'
                  }
                ],
                default: '$verifiedMail.address'
              }
            },
            phoneNumber: '$profile.phoneNumber',
            avatarKey: getAvatarKeyPipeline(
              '$profile.avatarKey',
              'assets/default-image/user-primary.png'
            )
          }
        }
      ],
      as: 'coTenantUser'
    }
  },
  {
    $unwind: {
      path: '$coTenantUser',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getAllCoTenantTogether = () => [
  {
    $addFields: {
      'mainTenant.user': '$mainTenantUser',
      'coTenants.user': '$coTenantUser'
    }
  },
  {
    $project: {
      mainTenantUser: 0,
      coTenantUser: 0
    }
  },
  {
    $group: {
      _id: '$_id',
      mainTenant: {
        $first: '$mainTenant'
      },
      coTenants: {
        $push: '$coTenants'
      }
    }
  },
  {
    $project: {
      'mainTenant.userId': 0,
      'coTenants.userId': 0
    }
  }
]

const getLeaseTenantDetailsForPublicSite = async (query) => {
  const pipeline = []

  const match = {
    $match: {
      _id: query.contractId
    }
  }

  const projectTenants = {
    $project: {
      'mainTenant._id': 1,
      'mainTenant.userId': 1,
      'coTenants._id': 1,
      'coTenants.userId': 1
    }
  }

  pipeline.push(match)
  pipeline.push(...getTenantsProject())
  pipeline.push(...getMainTenantLookup())
  pipeline.push(...getCoTenantsLookup())
  pipeline.push(projectTenants)
  pipeline.push(...getMainTenantUserInfo())
  pipeline.push(...getCoTenantUserInfo())
  pipeline.push(...getAllCoTenantTogether())

  const leaseTenantsInfo = await ContractCollection.aggregate(pipeline)
  return leaseTenantsInfo[0]
}

export const queryLeaseTenants = async (req) => {
  const { body = {}, user } = req
  const { query } = body
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['contractId'], query)
  const tenantsInfo = await getLeaseTenantDetailsForPublicSite(query)
  return {
    data: tenantsInfo
  }
}

export const queryLeases = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body
  appHelper.checkRequiredFields(['personType'], query)
  const { userId, partnerId } = user
  const { preparedQuery, tenantId } = await prepareQueryForQueryLeases({
    ...query,
    userId,
    partnerId
  })
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
    partnerId: preparedQuery.partnerId
  })
  const dateFormat =
    partnerSetting?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timeZone = partnerSetting?.dateTimeSettings?.timezone || 'Europe/Oslo'
  let soonEndingMonths = 4
  soonEndingMonths =
    partnerSetting?.propertySettings?.soonEndingMonths || soonEndingMonths
  const selectedMonth = (
    await appHelper.getActualDate(partnerSetting, true)
  ).add(soonEndingMonths, 'months')
  const leases = await getLeasesForQuery({
    query: preparedQuery,
    options,
    personType: query.personType,
    dateFormat,
    timeZone,
    selectedMonth,
    isClosed: query.isClosed,
    tenantId
  })
  const totalDocuments = await countContracts(preparedQuery)
  return {
    data: leases,
    metaData: {
      totalDocuments,
      filteredDocuments: totalDocuments
    }
  }
}

export const getAContractInfo = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)

  const { query } = body
  appHelper.checkRequiredFields(['contractId'], query)

  const { populate = [] } = query

  const contractInfo = await getAContract(
    { _id: query.contractId },
    session,
    populate
  )
  if (!size(contractInfo))
    throw new CustomError(404, 'ContractInfo is not found by query!')

  return contractInfo
}

const getPropertyPipelineForJournal = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            imageUrl: 1,
            location: {
              name: 1,
              postalCode: 1,
              city: 1,
              country: 1,
              sublocality: 1
            },
            listingTypeId: 1,
            propertyTypeId: 1,
            apartmentId: 1,
            serial: 1,
            gnr: 1,
            bnr: 1,
            snr: 1,
            monthlyRentAmount: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo')
]

const getRepresentativePipelineForAssignmentJournal = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'representativeId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'representativeInfo'
    }
  },
  appHelper.getUnwindPipeline('representativeInfo')
]

const getAssignmentJournalsForPartnerApp = async (query, options, partner) => {
  const historyNameForAssignment = [
    'address',
    'gnr_bnr_snr',
    'property_type',
    'listing_type',
    'assignmentFrom',
    'assignmentTo',
    'assignmentMonthlyRentAmount',
    'agent',
    'account',
    'representative'
  ]
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getPropertyPipelineForJournal(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...getRepresentativePipelineForAssignmentJournal(),
    {
      $addFields: {
        changeLogs: {
          $filter: {
            input: { $ifNull: ['$history', []] },
            as: 'changeLog',
            cond: {
              $in: ['$$changeLog.name', historyNameForAssignment]
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 1,
        propertyInfo: 1,
        partnerInfo: {
          _id: partner._id,
          serial: partner.serial ? partner.serial.toString() : null
        },
        signDate: 1,
        status: 1,
        rentalTenantIdExist: {
          $cond: [{ $ifNull: ['$rentalMeta.tenantId', false] }, true, false]
        },
        changeLogs: {
          $cond: [{ $gt: [{ $size: '$changeLogs' }, 0] }, true, false]
        },
        assignmentFrom: 1,
        assignmentTo: 1,
        createdAt: 1,
        accountInfo: 1,
        agentInfo: 1,
        representativeInfo: 1,
        assignmentSerial: 1
      }
    }
  ]
  const journals = (await ContractCollection.aggregate(pipeline)) || []
  return journals
}

const getTenantPipelineForTurnoverJournal = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'rentalMeta.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $project: {
            _id: 1,
            serial: 1
          }
        }
      ],
      as: 'tenantInfo'
    }
  },
  appHelper.getUnwindPipeline('tenantInfo')
]

const getLeaseSerialPipelineForTurnoverJournal = () => [
  appHelper.getUnwindPipeline('rentalMetaHistory'),
  {
    $sort: {
      'rentalMetaHistory.cancelledAt': -1
    }
  },
  {
    $group: {
      _id: '$_id',
      lastRentalMetaHistory: {
        $first: '$rentalMetaHistory'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      accountId: {
        $first: '$accountId'
      },
      agentId: {
        $first: '$agentId'
      },
      tenantInfo: {
        $first: '$tenantInfo'
      },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      history: {
        $first: '$history'
      },
      addons: {
        $first: '$addons'
      },
      assignmentSerial: {
        $first: '$assignmentSerial'
      },
      signDate: {
        $first: '$signDate'
      },
      createdAt: {
        $first: '$createdAt'
      },
      leaseSerial: {
        $first: '$leaseSerial'
      }
    }
  },
  {
    $addFields: {
      leaseSerial: {
        $switch: {
          branches: [
            {
              case: { $ifNull: ['$rentalMeta.leaseSerial', false] },
              then: '$rentalMeta.leaseSerial'
            },
            {
              case: { $ifNull: ['$lastRentalMetaHistory.leaseSerial', false] },
              then: '$lastRentalMetaHistory.leaseSerial'
            }
          ],
          default: '$leaseSerial'
        }
      },
      signedAt: {
        $cond: [
          { $ifNull: ['$rentalMeta.signedAt', false] },
          '$rentalMeta.signedAt',
          '$lastRentalMetaHistory.signedAt'
        ]
      },
      leaseCreatedAt: {
        $cond: [
          { $ifNull: ['$rentalMeta.createdAt', false] },
          '$rentalMeta.createdAt',
          '$lastRentalMetaHistory.createdAt'
        ]
      },
      yearlyRent: {
        $multiply: [
          12,
          {
            $cond: [
              { $ifNull: ['$rentalMeta.monthlyRentAmount', false] },
              '$rentalMeta.monthlyRentAmount',
              '$lastRentalMetaHistory.monthlyRentAmount'
            ]
          }
        ]
      }
    }
  }
]

const getCommissionPipelineForTurnoverJournal = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $group: {
            _id: null,
            invoiceIds: {
              $push: '$_id'
            }
          }
        },
        {
          $lookup: {
            from: 'commissions',
            localField: 'invoiceIds',
            foreignField: 'invoiceId',
            pipeline: [
              {
                $match: {
                  $expr: {
                    $eq: ['$type', 'brokering_contract']
                  }
                }
              }
            ],
            as: 'commission'
          }
        },
        {
          $unwind: {
            path: '$commission'
          }
        },
        {
          $group: {
            _id: null,
            commissionAmount: { $sum: '$commission.amount' }
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo')
]

const getTurnoverJournalsForPartnerApp = async (query, options, partner) => {
  const historyNameForLease = [
    'address',
    'gnr_bnr_snr',
    'property_type',
    'listing_type',
    'leaseMonthlyRentAmount',
    'signedAt',
    'tenant',
    'agent',
    'account',
    'commissions',
    'other',
    'total_income'
  ]
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getPropertyPipelineForJournal(),
    ...getTenantPipelineForTurnoverJournal(),
    ...getLeaseSerialPipelineForTurnoverJournal(),
    {
      $sort: sort
    },
    ...appHelper.getCommonAccountInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...getCommissionPipelineForTurnoverJournal(),
    {
      $addFields: {
        changeLogs: {
          $filter: {
            input: { $ifNull: ['$history', []] },
            as: 'changeLog',
            cond: {
              $in: ['$$changeLog.name', historyNameForLease]
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 1,
        propertyInfo: 1,
        partnerInfo: {
          _id: partner._id,
          serial: partner.serial ? partner.serial.toString() : null
        },
        assignmentSerial: 1,
        signDate: 1,
        leaseSerial: 1,
        signedAt: 1,
        changeLogs: {
          $cond: [{ $gt: [{ $size: '$changeLogs' }, 0] }, true, false]
        },
        createdAt: 1,
        accountInfo: 1,
        agentInfo: 1,
        tenantInfo: 1,
        leaseCreatedAt: 1,
        yearlyRent: 1,
        commissions: '$invoiceInfo.commissionAmount',
        others: {
          $reduce: {
            input: { $ifNull: ['$addons', []] },
            initialValue: 0,
            in: {
              $add: [
                '$$value',
                {
                  $cond: [
                    { $eq: ['$$this.type', 'assignment'] },
                    { $ifNull: ['$$this.total', 0] },
                    0
                  ]
                }
              ]
            }
          }
        },
        status: '$rentalMeta.status',
        rentalTenantIdExist: {
          $cond: [{ $ifNull: ['$rentalMeta.tenantId', false] }, true, false]
        }
      }
    },
    {
      $addFields: {
        totalIncome: {
          $add: ['$commissions', '$others']
        }
      }
    }
  ]
  const journals = (await ContractCollection.aggregate(pipeline)) || []
  return journals
}

export const getJournalReport = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  appHelper.checkRequiredFields(['type'], query)
  appHelper.validateSortForQuery(options.sort)
  // To check if partner enable broker journals or not
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  if (!partnerInfo?.enableBrokerJournals) {
    throw new CustomError(400, 'Broker journals not enabled for this partner')
  }
  query.partnerId = partnerId
  query.userId = userId
  const { type = '' } = query
  let result = []
  let totalDocuments = 0
  let preparedQuery = {}
  if (type === 'assignment_journals') {
    query.type = 'assignment'
    preparedQuery = await prepareAssignmentsOrLeasesQuery(query)
    result = await getAssignmentJournalsForPartnerApp(
      preparedQuery,
      options,
      partnerInfo
    )
    totalDocuments = await countContracts({ partnerId })
  } else if (type === 'turnover_journals') {
    query.type = 'lease'
    preparedQuery = await prepareAssignmentsOrLeasesQuery(query)
    result = await getTurnoverJournalsForPartnerApp(
      preparedQuery,
      options,
      partnerInfo
    )
    const totalDataQuery = {
      $or: [
        {
          partnerId,
          'rentalMeta.status': { $in: ['active', 'upcoming', 'closed'] }
        },
        {
          partnerId,
          rentalMetaHistory: {
            $elemMatch: {
              status: 'closed',
              cancelled: { $exists: false },
              cancelledAt: { $exists: false }
            }
          }
        }
      ]
    }
    totalDocuments = await countContracts(totalDataQuery)
  } else {
    throw new CustomError(400, 'Invalid journal type')
  }
  const filteredDocuments = await countContracts(preparedQuery)
  return {
    data: result,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

const getPropertyPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getListingFirstImageUrl('$images'),
        {
          $project: {
            _id: 1,
            location: {
              name: 1,
              city: 1,
              country: 1,
              postalCode: 1
            },
            imageUrl: 1
          }
        }
      ],
      as: 'propertyInfo'
    }
  },
  {
    $unwind: {
      path: '$propertyInfo',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getInvoicePaymentPipeline = () => [
  {
    $lookup: {
      from: 'invoice-payments',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$type', 'refund']
            }
          }
        },
        {
          $project: {
            _id: 1,
            amount: 1,
            refundStatus: 1,
            refundPaymentStatus: 1,
            paymentDate: 1
          }
        }
      ],
      as: 'refundPayments'
    }
  }
]

const getPendingPayoutPipeline = () => [
  {
    $lookup: {
      from: 'payouts',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $not: { $eq: ['$status', 'completed'] }
            }
          }
        },
        {
          $project: {
            _id: 1,
            serialId: 1,
            paymentStatus: 1,
            numberOfFails: 1,
            status: 1,
            amount: 1
          }
        }
      ],
      as: 'pendingPayouts'
    }
  }
]

const getLeaseDetails = async (query) => {
  const pipeline = [
    {
      $match: query
    },
    ...getPropertyPipeline(),
    ...getInvoicePaymentPipeline(),
    ...getPendingPayoutPipeline(),
    {
      $project: {
        _id: 1,
        leaseSerial: 1,
        propertyInfo: 1,
        refundPayments: 1,
        pendingPayouts: 1
      }
    }
  ]
  const [details = {}] = (await ContractCollection.aggregate(pipeline)) || []
  const invoicePipeline = [
    {
      $match: {
        contractId: query._id,
        partnerId: query.partnerId,
        invoiceType: { $in: ['invoice', 'landlord_invoice'] }
      }
    },
    {
      $addFields: {
        totalDue: {
          $cond: [
            { $eq: ['$invoiceType', 'invoice'] },
            {
              $subtract: [
                {
                  $add: [
                    { $ifNull: ['$invoiceTotal', 0] },
                    { $ifNull: ['$creditedAmount', 0] }
                  ]
                },
                {
                  $add: [
                    { $ifNull: ['$totalPaid', 0] },
                    { $ifNull: ['$lostMeta.amount', 0] }
                  ]
                }
              ]
            },
            {
              $subtract: [
                {
                  $ifNull: ['$invoiceTotal', 0]
                },
                {
                  $add: [
                    { $ifNull: ['$totalPaid', 0] },
                    { $ifNull: ['$totalBalanced', 0] }
                  ]
                }
              ]
            }
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        invoiceTotalAmount: {
          $sum: {
            $cond: [
              {
                $eq: ['$invoiceType', 'invoice']
              },
              {
                $add: [
                  { $ifNull: ['$invoiceTotal', 0] },
                  { $ifNull: ['$creditedAmount', 0] }
                ]
              },
              0
            ]
          }
        },
        rentInvoices: {
          $push: {
            $cond: [
              {
                $and: [
                  { $eq: ['$invoiceType', 'invoice'] },
                  { $eq: ['$status', 'overdue'] }
                ]
              },
              '$$ROOT',
              '$$REMOVE'
            ]
          }
        },
        landlordInvoices: {
          $push: {
            $cond: [
              {
                $and: [
                  { $eq: ['$invoiceType', 'landlord_invoice'] },
                  { $eq: ['$isPayable', true] },
                  { $not: { $eq: ['$isFinalSettlement', true] } }
                ]
              },
              '$$ROOT',
              '$$REMOVE'
            ]
          }
        },
        FSLandlordInvoices: {
          $push: {
            $cond: [
              {
                $and: [
                  { $eq: ['$invoiceType', 'landlord_invoice'] },
                  { $eq: ['$isFinalSettlement', true] }
                ]
              },
              '$$ROOT',
              '$$REMOVE'
            ]
          }
        }
      }
    },
    {
      $addFields: {
        FSLandlordInvoice: {
          $last: { $ifNull: ['$FSLandlordInvoices', []] }
        },
        rentInvoiceDueTotal: {
          $reduce: {
            input: { $ifNull: ['$rentInvoices', []] },
            initialValue: 0,
            in: {
              $add: ['$$value', '$$this.totalDue']
            }
          }
        },
        landlordInvoiceDueTotal: {
          $reduce: {
            input: { $ifNull: ['$landlordInvoices', []] },
            initialValue: 0,
            in: {
              $add: ['$$value', '$$this.totalDue']
            }
          }
        }
      }
    }
  ]
  const [invoiceInfo = {}] =
    (await InvoiceCollection.aggregate(invoicePipeline)) || []
  details.FSLandlordInvoice = invoiceInfo.FSLandlordInvoice
  details.rentInvoices = invoiceInfo.rentInvoices
  details.rentInvoiceDueTotal = invoiceInfo.rentInvoiceDueTotal
  details.landlordInvoices = invoiceInfo.landlordInvoices
  details.landlordInvoiceDueTotal = invoiceInfo.landlordInvoiceDueTotal
  details.invoiceTotalAmount = invoiceInfo.invoiceTotalAmount
  return details
}

export const queryLeaseDetails = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['contractId'], body)
  const { partnerId } = user
  const { contractId } = body
  return await getLeaseDetails({
    _id: contractId,
    partnerId
  })
}

const getAssignmentShowRegenerateSigningPipeline = (
  enabledAssignmentEsigning
) => [
  {
    $addFields: {
      showRegenerateSigning: {
        $cond: [
          {
            $and: [
              { $eq: ['$assignmentContractPdfGenerated', true] },
              { $eq: [enabledAssignmentEsigning, true] },
              { $in: ['$status', ['upcoming', 'new', 'in_progress']] },
              { $eq: ['$enabledEsigning', true] },
              {
                $lt: [
                  {
                    $size: { $ifNull: ['$assignmentSigningMeta.signers', []] }
                  },
                  2
                ]
              }
            ]
          },
          true,
          false
        ]
      }
    }
  }
]

const getAssignments = async ({
  query,
  options,
  enabledAssignmentEsigning
}) => {
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...appHelper.getCommonAgentInfoPipeline(),
    ...getAssignmentShowRegenerateSigningPipeline(enabledAssignmentEsigning),
    {
      $project: {
        _id: 1,
        agentInfo: 1,
        status: 1,
        assignmentSerial: 1,
        hasRentalContract: 1,
        hasBrokeringContract: 1,
        hasRentalManagementContract: 1,
        enabledEsigning: 1,
        assignmentContractPdfGenerated: 1,
        agentSigned: '$agentAssignmentSigningStatus.signed',
        landlordSigned: '$landlordAssignmentSigningStatus.signed',
        agentSigningUrl: '$agentAssignmentSigningStatus.signingUrl',
        showRegenerateSigning: 1
      }
    }
  ]
  const assignments = (await ContractCollection.aggregate(pipeline)) || []
  return assignments
}

const prepareQueryForAssignmentsQuery = (query) => {
  const { partnerId, propertyId, showClosedAssignment } = query
  const preparedQuery = {
    partnerId,
    propertyId
  }
  if (!showClosedAssignment) preparedQuery.status = { $ne: 'closed' }
  return preparedQuery
}

export const queryAssignments = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body
  appHelper.checkRequiredFields(['propertyId'], query)
  appHelper.validateSortForQuery(options.sort)
  const { partnerId } = user
  const { propertyId } = query
  query.partnerId = partnerId
  const partnerSettingsInfo =
    (await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })) || {}
  body.enabledAssignmentEsigning =
    !!partnerSettingsInfo.assignmentSettings?.enableEsignAssignment
  body.query = prepareQueryForAssignmentsQuery(query)
  const data = await getAssignments(body)
  const totalDocuments = await countContracts({
    partnerId,
    propertyId
  })
  const filteredDocuments = await countContracts(body.query)
  return {
    data,
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

const lookupListingInfo = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo'),
  ...appHelper.getListingFirstImageUrl('$propertyInfo.images', 'propertyInfo')
]

const lookupPartnerSettingInfo = () => [
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'monthlyPayoutDateSetting'
    }
  },
  appHelper.getUnwindPipeline('monthlyPayoutDateSetting')
]

const addonsInfoPipelineForContract = (type) => [
  {
    $addFields: {
      addons: {
        $filter: {
          input: { $ifNull: ['$addons', []] },
          as: 'data',
          cond: { $eq: ['$$data.type', type] }
        }
      }
    }
  },
  appHelper.getUnwindPipeline('addons'),
  {
    $lookup: {
      from: 'products_services',
      localField: 'addons.addonId',
      foreignField: '_id',
      as: 'addonsInfo'
    }
  },
  appHelper.getUnwindPipeline('addonsInfo'),
  {
    $lookup: {
      from: 'ledger_accounts',
      localField: 'addonsInfo.creditAccountId',
      foreignField: '_id',
      as: 'ledgerAccountInfo'
    }
  },
  appHelper.getUnwindPipeline('ledgerAccountInfo'),
  {
    $lookup: {
      from: 'tax_codes',
      localField: 'ledgerAccountInfo.taxCodeId',
      foreignField: '_id',
      as: 'taxCodeInfo'
    }
  },
  appHelper.getUnwindPipeline('taxCodeInfo')
]

const getTenantInfoPipeline = () => [
  {
    $unwind: {
      path: '$rentalMeta.tenants',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'rentalMeta.tenants.tenantId',
      foreignField: '_id',
      as: 'tenantInfo'
    }
  },
  {
    $unwind: {
      path: '$tenantInfo',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'users',
      localField: 'tenantInfo.userId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getUserEmailPipeline(),
        {
          $project: {
            _id: 1,
            name: '$profile.name',
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey'),
            email: 1,
            phoneNumber: '$profile.phoneNumber'
          }
        }
      ],
      as: 'tenantsUser'
    }
  },
  appHelper.getUnwindPipeline('tenantsUser'),
  {
    $group: {
      _id: '$_id',
      accountInfo: {
        $first: '$accountInfo'
      },
      addons: {
        $first: '$addons'
      },
      agentAssignmentSigningStatus: {
        $first: '$agentAssignmentSigningStatus'
      },
      assignmentContractPdfGenerated: {
        $first: '$assignmentContractPdfGenerated'
      },
      assignmentFrom: {
        $first: '$assignmentFrom'
      },
      assignmentTo: {
        $first: '$assignmentTo'
      },
      assignmentSerial: {
        $first: '$assignmentSerial'
      },
      branchInfo: {
        $first: '$branchInfo'
      },
      brokeringCommissionType: {
        $first: '$brokeringCommissionType'
      },
      brokeringCommissionAmount: {
        $first: '$brokeringCommissionAmount'
      },
      createdAt: {
        $first: '$createdAt'
      },
      createdByInfo: {
        $first: '$createdByInfo'
      },
      enabledEsigning: {
        $first: '$enabledEsigning'
      },
      hasBrokeringContract: {
        $first: '$hasBrokeringContract'
      },
      hasRentalContract: {
        $first: '$hasRentalContract'
      },
      hasRentalManagementContract: {
        $first: '$hasRentalManagementContract'
      },
      internalAssignmentId: {
        $first: '$internalAssignmentId'
      },
      landlordAssignmentSigningStatus: {
        $first: '$landlordAssignmentSigningStatus'
      },
      listingInfo: {
        $first: '$listingInfo'
      },
      monthlyPayoutDate: {
        $first: '$monthlyPayoutDate'
      },
      monthlyPayoutDateSetting: {
        $first: '$monthlyPayoutDateSetting'
      },
      ownerInfo: {
        $first: '$ownerInfo'
      },
      payoutTo: {
        $first: '$payoutTo'
      },
      propertyInfo: {
        $first: '$propertyInfo'
      },
      rentalManagementCommissionAmount: {
        $first: '$rentalManagementCommissionAmount'
      },
      rentalManagementCommissionType: {
        $first: '$rentalManagementCommissionType'
      },
      representativeInfo: {
        $first: '$representativeInfo'
      },
      signDate: {
        $first: '$signDate'
      },
      status: {
        $first: '$status'
      },
      tenants: {
        $push: {
          name: '$tenantInfo.name',
          avatarKey: '$tenantsUser.avatarKey',
          email: '$tenantsUser.email',
          phoneNumber: '$tenantsUser.phoneNumber',
          serial: '$tenantInfo.serial'
        }
      }
    }
  }
]

const getAssignmentDetails = async (params) => {
  const { contractId, partnerId } = params

  const assignmentDetailsPipeline = [
    {
      $match: {
        _id: contractId,
        partnerId
      }
    },
    ...appHelper.getCommonUserInfoPipeline('createdBy', 'createdByInfo'),
    ...appHelper.getCommonUserInfoPipeline('agentId', 'ownerInfo'),
    ...appHelper.getCommonUserInfoPipeline(
      'representativeId',
      'representativeInfo'
    ),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appHelper.getCommonAccountInfoPipeline(),
    ...lookupListingInfo(),
    ...lookupPartnerSettingInfo(),
    ...getTenantInfoPipeline(),
    {
      $project: {
        _id: 1,
        status: 1,
        signDate: 1,
        createdAt: 1,
        assignmentSerial: 1,
        hasBrokeringContract: 1,
        brokeringCommissionType: 1,
        brokeringCommissionAmount: 1,
        hasRentalManagementContract: 1,
        rentalManagementCommissionType: 1,
        rentalManagementCommissionAmount: 1,
        assignmentPeriod: {
          assignmentFrom: '$assignmentFrom',
          assignmentTo: '$assignmentTo'
        },
        payoutTo: 1,
        listingInfo: 1,
        monthlyPayoutDate: {
          $cond: {
            if: { $ifNull: ['$monthlyPayoutDate', false] },
            then: '$monthlyPayoutDate',
            else: '$monthlyPayoutDateSetting.standardPayoutDate'
          }
        },
        internalAssignmentId: 1,
        hasRentalContract: 1,
        createdByInfo: 1,
        ownerInfo: 1,
        branchInfo: 1,
        accountInfo: 1,
        representativeInfo: 1,
        propertyInfo: {
          _id: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          },
          imageUrl: 1
        },
        enabledEsigning: 1,
        assignmentContractPdfGenerated: 1,
        agentSigned: '$agentAssignmentSigningStatus.signed',
        landlordSigned: '$landlordAssignmentSigningStatus.signed',
        agentSigningUrl: '$agentAssignmentSigningStatus.signingUrl',
        addons: 1,
        tenants: 1
      }
    },
    // Addon pipeline to get addons name and tax percentage
    ...addonsInfoPipelineForContract('assignment'),
    {
      $group: {
        _id: '$_id',
        // Addons info
        addons: {
          $push: {
            $cond: [
              { $ifNull: ['$addons', false] },
              {
                addonId: '$addons.addonId',
                name: '$addonsInfo.name',
                taxPercentage: { $ifNull: ['$taxCodeInfo.taxPercentage', 0] },
                amount: { $ifNull: ['$addons.price', 0] }
              },
              null
            ]
          }
        },
        status: { $first: '$status' },
        signDate: { $first: '$signDate' },
        createdAt: { $first: '$createdAt' },
        assignmentSerial: { $first: '$assignmentSerial' },
        hasBrokeringContract: { $first: '$hasBrokeringContract' },
        brokeringCommissionType: { $first: '$brokeringCommissionType' },
        brokeringCommissionAmount: { $first: '$brokeringCommissionAmount' },
        hasRentalManagementContract: { $first: '$hasRentalManagementContract' },
        rentalManagementCommissionType: {
          $first: '$rentalManagementCommissionType'
        },
        rentalManagementCommissionAmount: {
          $first: '$rentalManagementCommissionAmount'
        },
        assignmentPeriod: {
          $first: '$assignmentPeriod'
        },
        payoutTo: { $first: '$payoutTo' },
        listingInfo: { $first: '$listingInfo' },
        monthlyPayoutDate: { $first: '$monthlyPayoutDate' },
        internalAssignmentId: { $first: '$internalAssignmentId' },
        hasRentalContract: { $first: '$hasRentalContract' },
        createdByInfo: { $first: '$createdByInfo' },
        ownerInfo: { $first: '$ownerInfo' },
        branchInfo: { $first: '$branchInfo' },
        accountInfo: { $first: '$accountInfo' },
        representativeInfo: { $first: '$representativeInfo' },
        propertyInfo: { $first: '$propertyInfo' },
        enabledEsigning: { $first: '$enabledEsigning' },
        assignmentContractPdfGenerated: {
          $first: '$assignmentContractPdfGenerated'
        },
        agentSigned: { $first: '$agentSigned' },
        landlordSigned: { $first: '$landlordSigned' },
        agentSigningUrl: { $first: '$agentSigningUrl' },
        tenants: {
          $first: '$tenants'
        }
      }
    },
    {
      $addFields: {
        addons: {
          $filter: {
            input: '$addons',
            as: 'addon',
            cond: { $ifNull: ['$$addon', false] }
          }
        }
      }
    }
  ]

  const [assignmentDetails = {}] =
    (await ContractCollection.aggregate(assignmentDetailsPipeline)) || []

  return assignmentDetails
}

export const queryAssignmentDetails = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['contractId'], body)
  const { partnerId } = user
  body.partnerId = partnerId
  return await getAssignmentDetails(body)
}

const getTenantsPipeline = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'rentalMeta.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'userInfo'
          }
        },
        appHelper.getUnwindPipeline('userInfo'),
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$userInfo.profile.avatarKey'
            )
          }
        }
      ],
      as: 'mainTenant'
    }
  },
  {
    $lookup: {
      from: 'tenants',
      localField: 'rentalMeta.tenants.tenantId',
      foreignField: '_id',
      let: { mainTenantId: '$rentalMeta.tenantId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $not: { $eq: ['$_id', '$$mainTenantId'] }
            }
          }
        },
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'userInfo'
          }
        },
        appHelper.getUnwindPipeline('userInfo'),
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$userInfo.profile.avatarKey'
            )
          }
        }
      ],
      as: 'otherTenants'
    }
  },
  {
    $addFields: {
      tenantsInfo: {
        $concatArrays: [
          { $ifNull: ['$mainTenant', []] },
          { $ifNull: ['$otherTenants', []] }
        ]
      }
    }
  }
]

const getAccountPipelineForLeaseList = () => [
  {
    $lookup: {
      from: 'accounts',
      localField: 'accountId',
      foreignField: '_id',
      as: 'accountInfo'
    }
  },
  appHelper.getUnwindPipeline('accountInfo')
]

const getDepositAccountsPipelineForLeaseList = () => [
  {
    $lookup: {
      from: 'deposit_accounts',
      localField: '_id',
      foreignField: 'contractId',
      as: 'depositAccounts'
    }
  }
]

const getDocumentPreparingStatus = () => [
  {
    $addFields: {
      tenantLeaseSigningStatusDetails: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'item',
          cond: {
            $ifNull: ['$$item.idfyAttachmentId', false]
          }
        }
      }
    }
  },
  {
    $addFields: {
      isNotAttachedIdfyFileForLeaseLabel: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'item',
          cond: {
            $and: [
              {
                $eq: ['$$item.tenantId', '$rentalMeta.tenantId']
              },
              {
                $eq: ['$$item.idfyAttachmentId', false]
              }
            ]
          }
        }
      }
    }
  },
  {
    $addFields: {
      isNotAttachedIdfyFile: {
        $cond: [
          {
            $gt: [
              { $size: { $ifNull: ['$tenantLeaseSigningStatusDetails', []] } },
              0
            ]
          },
          false,
          true
        ]
      }
    }
  },
  {
    $addFields: {
      tenantSinged: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'item',
          cond: {
            $cond: [
              {
                $eq: ['$$item.signed', true]
              },
              true,
              false
            ]
          }
        }
      }
    }
  },
  {
    $addFields: {
      isDocumentPreparing: {
        $cond: [
          {
            $eq: ['$isEnabledDepositAccountProcess', false]
          },
          {
            $and: [
              { $eq: ['$rentalMeta.enabledLeaseEsigning', true] },
              {
                $not: {
                  $eq: ['$leaseContractPdfGenerated', true]
                }
              }
            ]
          },
          {
            $cond: [
              {
                $and: [
                  { $eq: ['$isEnabledDepositAccountProcess', true] },
                  { $eq: ['$rentalMeta.enabledJointDepositAccount', true] }
                ]
              },
              {
                $cond: [
                  {
                    $gt: [
                      {
                        $size: {
                          $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []]
                        }
                      },
                      0
                    ]
                  },
                  true,
                  {
                    $cond: [
                      {
                        $gt: [
                          {
                            $size: {
                              $ifNull: [
                                '$isNotAttachedIdfyFileForLeaseLabel',
                                []
                              ]
                            }
                          },
                          0
                        ]
                      },
                      true,
                      false
                    ]
                  }
                ]
              },
              {
                $cond: [
                  {
                    $and: [
                      { $eq: ['$isEnabledDepositAccountProcess', true] },
                      {
                        $eq: ['$rentalMeta.enabledJointDepositAccount', false]
                      },
                      {
                        $eq: [
                          {
                            $size: {
                              $ifNull: [
                                '$rentalMeta.tenantLeaseSigningStatus',
                                []
                              ]
                            }
                          },
                          0
                        ]
                      }
                    ]
                  },
                  true,
                  '$isNotAttachedIdfyFile'
                ]
              }
            ]
          }
        ]
      }
    }
  },
  {
    $addFields: {
      isTenantWaiting: {
        $cond: [
          {
            $and: [
              {
                $eq: ['$isEnabledDepositAccountProcess', true]
              },
              {
                $or: [
                  { $eq: ['$rentalMeta.enabledJointlyLiable', false] },
                  { $eq: ['$rentalMeta.enabledJointDepositAccount', false] }
                ]
              }
            ]
          },
          {
            $and: [
              { $eq: ['$isNotAttachedIdfyFile', false] },
              {
                $eq: [
                  { $ifNull: ['$rentalMeta.leaseSigningComplete', false] },
                  false
                ]
              },
              { $eq: ['$rentalMeta.enabledLeaseEsigning', true] },
              { $eq: ['$leaseContractPdfGenerated', true] }
            ]
          },
          {
            $cond: [
              {
                $and: [
                  {
                    $eq: ['$isEnabledDepositAccountProcess', true]
                  },
                  { $eq: ['$rentalMeta.enabledJointlyLiable', true] },
                  { $eq: ['$rentalMeta.enabledJointDepositAccount', true] },
                  { $eq: ['$isDocumentPreparing', true] }
                ]
              },
              false,
              {
                $cond: [
                  {
                    $and: [
                      {
                        $eq: ['$isEnabledDepositAccountProcess', true]
                      },
                      { $eq: ['$rentalMeta.enabledJointlyLiable', true] },
                      { $eq: ['$rentalMeta.enabledJointDepositAccount', true] },
                      { $eq: ['$isDocumentPreparing', false] }
                    ]
                  },
                  {
                    $eq: [{ $size: { $ifNull: ['$tenantSinged', []] } }, 0]
                  },
                  {
                    $and: [
                      {
                        $eq: ['$isEnabledDepositAccountProcess', false]
                      },
                      {
                        $not: {
                          $eq: ['$rentalMeta.leaseSigningComplete', true]
                        }
                      },
                      { $eq: ['$rentalMeta.enabledLeaseEsigning', true] },
                      { $eq: ['$leaseContractPdfGenerated', true] }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }
    }
  }
]

const getDepositAccountStatusPipelineForLeaseList = (enableDepositAccount) => [
  ...getDepositAccountsPipelineForLeaseList(),
  {
    $addFields: {
      isEnabledDepositAccountProcess: {
        $cond: [
          {
            $and: [
              { $eq: [enableDepositAccount, true] },
              { $eq: ['$rentalMeta.leaseSignatureMechanism', 'bank_id'] },
              { $eq: ['$rentalMeta.enabledDepositAccount', true] },
              { $eq: ['$rentalMeta.depositType', 'deposit_account'] },
              { $ifNull: ['$rentalMeta.depositAmount', false] }
            ]
          },
          true,
          false
        ]
      },
      tenantsFilteredDepositAccounts: {
        $filter: {
          input: { $ifNull: ['$depositAccounts', []] },
          as: 'deposit',
          cond: {
            $in: [
              '$$deposit.tenantId',
              { $ifNull: ['$rentalMeta.tenants.tenantId', []] }
            ]
          }
        }
      },
      tenantFilteredDepositAccount: {
        $filter: {
          input: { $ifNull: ['$depositAccounts', []] },
          as: 'deposit',
          cond: {
            $eq: ['$$deposit.tenantId', '$rentalMeta.tenantId']
          }
        }
      },
      sentToBankFilteredData: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'sign',
          cond: {
            $eq: ['$$sign.isSentDepositDataToBank', true]
          }
        }
      }
    }
  },
  {
    $addFields: {
      isDepositAccountDocumentPreparing: {
        $cond: [
          {
            $not: {
              $eq: ['$rentalMeta.hasSignersAttachmentPadesFile', true]
            }
          },
          true,
          false
        ]
      },
      isDepositAccountCreated: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$rentalMeta.enabledJointlyLiable', true] },
                  {
                    $not: {
                      $eq: ['$rentalMeta.enabledJointDepositAccount', true]
                    }
                  },
                  {
                    $eq: [
                      { $size: { $ifNull: ['$rentalMeta.tenants', []] } },
                      { $size: '$tenantsFilteredDepositAccounts' }
                    ]
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  {
                    $ifNull: ['$rentalMeta.tenantId', false]
                  },
                  { $gt: [{ $size: '$tenantFilteredDepositAccount' }, 0] }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      isDepositDataSentToBank: {
        $switch: {
          branches: [
            {
              case: {
                $eq: [
                  {
                    $size: {
                      $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []]
                    }
                  },
                  0
                ]
              },
              then: false
            },
            {
              case: {
                $and: [
                  {
                    $not: {
                      $eq: ['$rentalMeta.enabledJointDepositAccount', true]
                    }
                  },
                  {
                    $eq: [
                      { $size: { $ifNull: ['$rentalMeta.tenants', []] } },
                      { $size: '$sentToBankFilteredData' }
                    ]
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$rentalMeta.enabledJointDepositAccount', true] },
                  {
                    $gt: [{ $size: '$sentToBankFilteredData' }, 0]
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      isFullPaid: {
        $cond: [
          {
            $gte: [
              {
                $reduce: {
                  input: { $ifNull: ['$depositAccounts', []] },
                  initialValue: 0,
                  in: {
                    $sum: ['$$value', '$$this.totalPaymentAmount']
                  }
                }
              },
              { $ifNull: ['$rentalMeta.depositAmount', Infinity] }
            ]
          },
          true,
          false
        ]
      },
      isDepositAmountPaid: {
        $cond: [
          {
            $and: [
              { $ifNull: ['$depositAccounts', false] },
              {
                $eq: [
                  {
                    $size: {
                      $filter: {
                        input: { $ifNull: ['$depositAccounts', []] },
                        as: 'depositAccount',
                        cond: {
                          $and: [
                            {
                              $ifNull: [
                                '$$depositAccount.totalPaymentAmount',
                                false
                              ]
                            },
                            {
                              $gte: [
                                {
                                  $ifNull: [
                                    '$$depositAccount.totalPaymentAmount',
                                    0
                                  ]
                                },
                                {
                                  $ifNull: ['$$depositAccount.depositAmount', 0]
                                }
                              ]
                            }
                          ]
                        }
                      }
                    }
                  },
                  {
                    $size: { $ifNull: ['$depositAccounts', []] }
                  }
                ]
              }
            ]
          },
          true,
          false
        ]
      },
      isAnyAccountPartiallyPaid: {
        $cond: [
          {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: { $ifNull: ['$depositAccounts', []] },
                    as: 'depositAccount',
                    cond: {
                      $and: [
                        {
                          $ifNull: [
                            '$$depositAccount.totalPaymentAmount',
                            false
                          ]
                        },
                        {
                          $lt: [
                            {
                              $ifNull: [
                                '$$depositAccount.totalPaymentAmount',
                                0
                              ]
                            },
                            { $ifNull: ['$$depositAccount.depositAmount', 0] }
                          ]
                        }
                      ]
                    }
                  }
                }
              },
              0
            ]
          },
          true,
          false
        ]
      }
    }
  },
  {
    $addFields: {
      depositAccountError: {
        $cond: [
          { $eq: ['$isDepositAccountDocumentPreparing', true] },
          '$rentalMeta.depositAccountError',
          null
        ]
      }
    }
  },
  {
    $addFields: {
      tenantsFilteredDepositAccounts: '$$REMOVE',
      tenantFilteredDepositAccount: '$$REMOVE',
      sentToBankFilteredData: '$$REMOVE'
    }
  }
]

const getTotalAddonAmountPipelineForLeaseList = () => [
  {
    $addFields: {
      addonTotal: {
        $reduce: {
          input: { $ifNull: ['$addons', []] },
          initialValue: 0,
          in: {
            $sum: [
              '$$value',
              {
                $cond: [
                  { $eq: ['$$this.type', 'lease'] },
                  { $ifNull: ['$$this.total', 0] },
                  0
                ]
              }
            ]
          }
        }
      }
    }
  }
]

const getSigningStatusPipeline = (accountType, userId) => [
  {
    $addFields: {
      'rentalMeta.tenantLeaseSigningStatus': {
        $cond: [
          {
            $and: [
              { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', false] },
              {
                $ne: [
                  { $isArray: '$rentalMeta.tenantLeaseSigningStatus' },
                  true
                ]
              }
            ]
          },
          ['$rentalMeta.tenantLeaseSigningStatus'],
          { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] }
        ]
      }
    }
  },
  {
    $addFields: {
      tenantsNotSigned: {
        $filter: {
          input: { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] },
          as: 'esign',
          cond: {
            $eq: ['$$esign.signed', false]
          }
        }
      }
    }
  },
  {
    $addFields: {
      tenantSigned: {
        $cond: [
          {
            $and: [
              { $eq: [{ $size: '$tenantsNotSigned' }, 0] },
              {
                $not: {
                  $eq: [
                    {
                      $size: {
                        $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []]
                      }
                    },
                    0
                  ]
                }
              }
            ]
          },
          true,
          false
        ]
      }
    }
  },
  {
    $addFields: {
      showSigningUrl: {
        $cond: [
          {
            $and: [
              { $eq: [accountType, 'direct'] },
              { $eq: ['$accountInfo.agentId', userId] },
              { $not: { $eq: ['$rentalMeta.leaseSigningComplete', true] } },
              { $eq: ['$tenantSigned', true] }
            ]
          },
          true,
          false
        ]
      }
    }
  }
]

const getFirstProjectPipeline = () => [
  {
    $project: {
      _id: 1,
      assignmentStatus: '$status',
      tenantsInfo: 1,
      contractStartDate: '$rentalMeta.contractStartDate',
      contractEndDate: '$rentalMeta.contractEndDate',
      status: '$rentalMeta.status',
      leaseSerial: {
        $ifNull: ['$rentalMeta.leaseSerial', '$leaseSerial']
      },
      isDefaulted: '$rentalMeta.isDefaulted',
      cpiEnabled: '$rentalMeta.cpiEnabled',
      lastCpiDate: '$rentalMeta.lastCpiDate',
      nextCpiDate: '$rentalMeta.nextCpiDate',
      enabledLeaseEsigning: '$rentalMeta.enabledLeaseEsigning',
      leaseContractPdfGenerated: '$rentalMeta.leaseContractPdfGenerated',
      leaseSigningComplete: '$rentalMeta.leaseSigningComplete',
      tenantSigned: 1,
      landlordSigned: {
        $cond: [
          { $eq: ['$rentalMeta.landlordLeaseSigningStatus.signed', true] },
          true,
          false
        ]
      },
      landlordLeaseSigningUrl: {
        $cond: [
          { $eq: ['$showSigningUrl', true] },
          '$rentalMeta.landlordLeaseSigningStatus.signingUrl',
          null
        ]
      },
      monthlyRentAmount: '$rentalMeta.monthlyRentAmount',
      depositAmount: '$rentalMeta.depositAmount',
      isMovedIn: '$rentalMeta.isMovedIn',
      hasSignersAttachmentPadesFile:
        '$rentalMeta.hasSignersAttachmentPadesFile',
      holdPayout: 1,
      rentalMeta: 1,
      addons: 1,
      terminatedByUserId: 1
    }
  }
]

const getFinalProjectPipeline = () => [
  {
    $project: {
      _id: 1,
      tenantsInfo: 1,
      contractStartDate: 1,
      contractEndDate: 1,
      hasSignersAttachmentPadesFile: 1,
      status: 1,
      leaseSerial: 1,
      isDefaulted: 1,
      isDocumentPreparing: 1,
      isTenantWaiting: 1,
      cpiEnabled: 1,
      lastCpiDate: 1,
      nextCpiDate: 1,
      enabledLeaseEsigning: 1,
      leaseContractPdfGenerated: 1,
      leaseSigningComplete: 1,
      tenantSigned: 1,
      landlordSigned: 1,
      landlordLeaseSigningUrl: 1,
      monthlyRentAmount: 1,
      depositAmount: 1,
      isMovedIn: 1,
      holdPayout: 1,
      // For deposit account
      isEnabledDepositAccountProcess: 1,
      isDepositAccountDocumentPreparing: 1,
      isDepositAccountCreated: 1,
      isDepositDataSentToBank: 1,
      isFullPaid: 1,
      isDepositAmountPaid: 1,
      isAnyAccountPartiallyPaid: 1,
      depositAccountError: 1,
      //Deposit insurance
      depositType: '$rentalMeta.depositType',
      depositInsuranceStatus: '$depositInsurance.status',
      depositInsuranceErrors: {
        $cond: [
          { $eq: ['$depositInsurance.status', 'failed'] },
          '$depositInsurance.creationResult.reasons',
          null
        ]
      },
      //For addon
      addonTotal: 1,
      isMovingIn: 1,
      isMovingOut: 1,
      esignInitiatePropertyItems: 1,
      completedPropertyItems: 1,
      isShowNewLease: 1,
      mainTenantId: '$rentalMeta.tenantId',
      showCancelMoveIn: 1,
      showCancelMoveOut: 1,
      isDepositAccountCreationTestProcessing: 1,
      isDepositAccountPaymentTestProcessing: 1,
      leaseTerminated: {
        $and: [
          {
            $ifNull: ['$terminatedByUserId', false]
          },
          {
            $eq: ['$rentalMeta.status', 'active']
          }
        ]
      }
    }
  }
]

const isShowNewLease = ({
  partnerType,
  upcomingContract,
  inProgressContract,
  propertyStatus
}) => [
  {
    $addFields: {
      isShowNewLease: {
        $cond: {
          if: {
            $eq: [partnerType, 'direct']
          },
          then: {
            $cond: {
              if: {
                $and: [
                  {
                    $not: {
                      $ifNull: [upcomingContract?.rentalMeta?.tenantId, false]
                    }
                  },
                  {
                    $not: {
                      $ifNull: [inProgressContract, false]
                    }
                  },
                  { $eq: ['$status', 'active'] },
                  { $ifNull: ['$rentalMeta.contractEndDate', false] },
                  { $not: { $eq: [propertyStatus, 'archived'] } }
                ]
              },
              then: true,
              else: false
            }
          },
          else: {
            $cond: {
              if: {
                $and: [
                  {
                    $ifNull: [upcomingContract, false]
                  },
                  {
                    $not: {
                      $ifNull: [upcomingContract?.rentalMeta?.tenantId, false]
                    }
                  },
                  { $eq: ['$status', 'active'] },
                  { $ifNull: ['$rentalMeta.contractEndDate', false] },
                  { $not: { $eq: [propertyStatus, 'archived'] } }
                ]
              },
              then: true,
              else: false
            }
          }
        }
      }
    }
  }
]

const getLeaselist = async ({
  query,
  options,
  showClosedLease,
  partnerInfo = {},
  partnerSetting = {},
  userId,
  upcomingContract,
  inProgressContract,
  propertyStatus,
  activeContract = {}
}) => {
  const soonEndingMonths =
    partnerSetting?.propertySettings?.soonEndingMonths || 4
  const selectedMonth = (
    await appHelper.getActualDate(partnerSetting, true)
  ).add(soonEndingMonths, 'months')
  const soonEndingHigherRange = selectedMonth._d
  const { sort, skip, limit } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $addFields: {
        'rentalMeta.isDefaulted': '$isDefaulted',
        'rentalMeta.leaseContractPdfGenerated': '$leaseContractPdfGenerated',
        isDepositAccountCreationTestProcessing:
          '$rentalMeta.isDepositAccountCreationTestProcessing',
        isDepositAccountPaymentTestProcessing:
          '$rentalMeta.isDepositAccountPaymentTestProcessing'
      }
    }
  ]
  if (showClosedLease) {
    pipeline.push(
      {
        $addFields: {
          rentalMeta: {
            $cond: [
              { $eq: ['$hasRentalContract', true] },
              ['$rentalMeta'],
              '$$REMOVE'
            ]
          },
          'rentalMetaHistory.isFromRentalMetaHistory': true
        }
      },
      {
        $addFields: {
          rentalMeta: {
            $concatArrays: [
              { $ifNull: ['$rentalMeta', []] },
              {
                $cond: [
                  { $isArray: '$rentalMetaHistory' },
                  '$rentalMetaHistory',
                  []
                ]
              }
            ]
          }
        }
      },
      appHelper.getUnwindPipeline('rentalMeta', false)
    )
  }
  pipeline.push(
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getTenantsPipeline(),
    ...getAccountPipelineForLeaseList(),
    ...getSigningStatusPipeline(partnerInfo.accountType, userId),
    ...getFirstProjectPipeline(),
    ...getDepositAccountStatusPipelineForLeaseList(
      partnerInfo.enableDepositAccount
    ),
    // Need isEnabledDepositAccountProcess from previous pipeline
    ...getDocumentPreparingStatus(),
    ...getDepositInsurancePipelineForLeaseList(),
    ...getTotalAddonAmountPipelineForLeaseList(),
    ...getMovingStatusPipeline(soonEndingHigherRange, activeContract),
    ...isShowNewLease({
      partnerType: partnerInfo.accountType,
      upcomingContract,
      inProgressContract,
      propertyStatus
    }),
    ...getFinalProjectPipeline()
  )
  const leases = (await ContractCollection.aggregate(pipeline)) || []
  return leases
}

const getMovingStatusPipeline = (soonEndingHigherRange, activeContract) => [
  {
    $lookup: {
      from: 'property_items',
      localField: '_id',
      foreignField: 'contractId',
      as: 'propertyItems',
      pipeline: [
        {
          $match: {
            $expr: {
              $in: ['$type', ['in', 'out']]
            }
          }
        },
        {
          $sort: {
            createdAt: -1
          }
        },
        {
          $lookup: {
            from: 'files',
            localField: '_id',
            foreignField: 'movingId',
            as: 'filesInfo'
          }
        },
        {
          $addFields: {
            fileType: {
              $cond: [
                { $eq: ['$type', 'in'] },
                'esigning_moving_in_pdf',
                'esigning_moving_out_pdf'
              ]
            }
          }
        },
        {
          $addFields: {
            fileInfo: {
              $first: {
                $filter: {
                  input: { $ifNull: ['$filesInfo', []] },
                  as: 'file',
                  cond: {
                    $eq: ['$$file.type', '$fileType']
                  }
                }
              }
            }
          }
        },
        {
          $addFields: {
            movingFileId: '$fileInfo._id'
          }
        }
      ]
    }
  },
  {
    $addFields: {
      isSoonEnding: {
        $cond: [
          {
            $and: [
              { $eq: ['$status', 'active'] },
              { $ifNull: ['$rentalMeta.contractEndDate', false] },
              { $lte: ['$rentalMeta.contractEndDate', soonEndingHigherRange] }
            ]
          },
          true,
          false
        ]
      },
      movingInItem: {
        $first: {
          $filter: {
            input: { $ifNull: ['$propertyItems', []] },
            as: 'propertyItem',
            cond: {
              $and: [
                { $eq: ['$$propertyItem.type', 'in'] },
                { $not: { $ifNull: ['$$propertyItem.moveInCompleted', false] } }
              ]
            }
          }
        }
      },
      movingOutItem: {
        $first: {
          $filter: {
            input: { $ifNull: ['$propertyItems', []] },
            as: 'propertyItem',
            cond: {
              $and: [
                { $eq: ['$$propertyItem.type', 'out'] },
                {
                  $not: { $ifNull: ['$$propertyItem.moveOutCompleted', false] }
                }
              ]
            }
          }
        }
      },
      processedItem: {
        $first: {
          $filter: {
            input: { $ifNull: ['$propertyItems', []] },
            as: 'propertyItem',
            cond: {
              $or: [
                {
                  $ifNull: ['$$propertyItem.moveInCompleted', false]
                },
                {
                  $ifNull: ['$$propertyItem.moveOutCompleted', false]
                }
              ]
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      activeContractId: activeContract._id
    }
  },
  {
    $lookup: {
      from: 'property_items',
      localField: 'activeContractId',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$isEsigningInitiate', true]
            }
          }
        }
      ],
      as: 'activeContractPropertyItems'
    }
  },
  {
    $addFields: {
      isMovingIn: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', true] },
                  { $ifNull: ['$movingInItem', false] },
                  {
                    $not: {
                      $ifNull: ['$movingInItem.isEsigningInitiate', false]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'in'] }
                ]
              },
              then: false
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'out'] },
                  {
                    $not: {
                      $ifNull: ['$movingInItem.isEsigningInitiate', false]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $not: { $ifNull: ['$processedItem', false] } },
                  {
                    $not: {
                      $ifNull: ['$movingInItem.isEsigningInitiate', false]
                    }
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      isMovingOut: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', true] },
                  {
                    $not: {
                      $ifNull: ['$movingOutItem.isEsigningInitiate', false]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'in'] },
                  {
                    $not: {
                      $ifNull: ['$movingOutItem.isEsigningInitiate', false]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'out'] }
                ]
              },
              then: false
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $not: { $ifNull: ['$processedItem', false] } }
                ]
              },
              then: false
            }
          ],
          default: false
        }
      },
      isMovingOutForClosedLease: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$assignmentStatus', 'closed'] },
                  { $not: { $ifNull: ['$movingOutItem', false] } }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$assignmentStatus', 'closed'] },
                  { $ifNull: ['$movingOutItem', false] },
                  {
                    $and: [
                      {
                        $ne: ['$movingOutItem.moveOutCompleted', true]
                      },
                      {
                        $ne: ['$movingOutItem.isEsigningInitiate', true]
                      }
                    ]
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      esignInitiatePropertyItems: {
        $filter: {
          input: { $ifNull: ['$propertyItems', []] },
          as: 'propertyItem',
          cond: {
            $and: [
              { $ifNull: ['$$propertyItem.isEsigningInitiate', false] },
              { $not: { $ifNull: ['$$propertyItem.moveInCompleted', false] } },
              { $not: { $ifNull: ['$$propertyItem.moveOutCompleted', false] } }
            ]
          }
        }
      },
      completedPropertyItems: {
        $filter: {
          input: { $ifNull: ['$propertyItems', []] },
          as: 'propertyItem',
          cond: {
            $or: [
              { $eq: ['$$propertyItem.moveOutCompleted', true] },
              { $eq: ['$$propertyItem.moveInCompleted', true] }
            ]
          }
        }
      },
      showCancelMoveIn: {
        $cond: [
          { $eq: ['$movingInItem.isEsigningInitiate', true] },
          true,
          false
        ]
      },
      showCancelMoveOut: {
        $cond: [
          { $eq: ['$movingOutItem.isEsigningInitiate', true] },
          true,
          false
        ]
      }
    }
  },
  {
    $addFields: {
      isMovingIn: {
        $cond: [
          {
            $and: [
              { $eq: ['$isMovingIn', true] },
              { $eq: ['$status', 'upcoming'] },
              {
                $or: [
                  { $not: { $ifNull: ['$activeContractId', false] } },
                  { $gt: [{ $size: '$activeContractPropertyItems' }, 0] }
                ]
              }
            ]
          },
          true,
          {
            $cond: [{ $eq: ['$status', 'upcoming'] }, false, '$isMovingIn']
          }
        ]
      },
      isMovingOut: {
        $cond: [
          { $eq: ['$status', 'closed'] },
          '$isMovingOutForClosedLease',
          '$isMovingOut'
        ]
      },
      esignInitiatePropertyItems: {
        $cond: [
          { $ne: ['$rentalMeta.isFromRentalMetaHistory', true] },
          '$esignInitiatePropertyItems',
          []
        ]
      },
      completedPropertyItems: {
        $cond: [
          { $ne: ['$rentalMeta.isFromRentalMetaHistory', true] },
          '$completedPropertyItems',
          []
        ]
      }
    }
  }
]
const getDepositInsurancePipelineForLeaseList = () => [
  {
    $lookup: {
      from: 'deposit_insurance',
      localField: 'rentalMeta.depositInsuranceId',
      foreignField: '_id',
      as: 'depositInsurance'
    }
  },
  appHelper.getUnwindPipeline('depositInsurance')
]

const prepareQueryForLeaseList = (query) => {
  const { showClosedLease, partnerId, propertyId } = query
  const preparedQuery = {
    partnerId,
    propertyId,
    $or: [{ hasRentalContract: true }, { rentalMetaHistory: { $exists: true } }]
  }
  if (!showClosedLease) {
    delete preparedQuery.$or
    preparedQuery['$and'] = [
      { hasRentalContract: true },
      { 'rentalMeta.status': { $ne: 'closed' } }
    ]
  }
  return preparedQuery
}

const countLeases = async ({ query, showClosedLease }) => {
  const pipeline = [
    {
      $match: query
    }
  ]
  if (showClosedLease) {
    pipeline.push(
      {
        $addFields: {
          rentalMeta: [
            {
              $cond: [
                { $eq: ['$hasRentalContract', true] },
                '$rentalMeta',
                null
              ]
            }
          ]
        }
      },
      {
        $addFields: {
          rentalMeta: {
            $concatArrays: [
              '$rentalMeta',
              { $ifNull: ['$rentalMetaHistory', []] }
            ]
          }
        }
      },
      appHelper.getUnwindPipeline('rentalMeta', false)
    )
  }
  const leases = (await ContractCollection.aggregate(pipeline)) || []
  return leases.length
}

export const queryLeaseListForPartnerApp = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  const { query, options } = body
  appHelper.checkRequiredFields(['propertyId'], query)
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  const partnerInfo = await partnerHelper.getAPartner(
    {
      _id: partnerId
    },
    undefined,
    ['partnerSetting']
  )
  const partnerSetting = partnerInfo.partnerSetting
  const { propertyId, showClosedLease } = query
  const propertyInfo = await listingHelper.getAListing({
    _id: propertyId,
    partnerId
  })
  if (!size(propertyInfo)) {
    throw new CustomError(404, 'Property not found')
  }
  const contracts = await contractHelper.getContracts({
    partnerId,
    propertyId,
    status: {
      $in: ['upcoming', 'in_progress', 'active']
    }
  })
  const upcomingContract = contracts.find(
    (contract) => contract.status === 'upcoming'
  )
  const inProgressContract = contracts.find(
    (contract) => contract.status === 'in_progress'
  )
  const activeContract = contracts.find(
    (contract) => contract.status === 'active'
  )
  const preparedQuery = prepareQueryForLeaseList(query)
  const generalQuery = {
    partnerId,
    propertyId,
    $or: [{ hasRentalContract: true }, { rentalMetaHistory: { $exists: true } }]
  }
  const leases = await getLeaselist({
    query: preparedQuery,
    options,
    showClosedLease,
    partnerInfo,
    partnerSetting,
    userId,
    upcomingContract,
    inProgressContract,
    propertyStatus: propertyInfo.propertyStatus,
    activeContract
  })
  const filteredDocuments = await countLeases({
    query: preparedQuery,
    showClosedLease
  })
  const totalDocuments = await countLeases({
    query: generalQuery,
    showClosedLease: true
  })
  return {
    data: leases,
    metaData: {
      totalDocuments,
      filteredDocuments
    }
  }
}

const getCommissionPipelineForJournalSummary = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: 'contractIds',
      foreignField: 'contractId',
      pipeline: [
        {
          $group: {
            _id: null,
            invoiceIds: { $push: '$_id' }
          }
        },
        {
          $lookup: {
            from: 'commissions',
            localField: 'invoiceIds',
            foreignField: 'invoiceId',
            pipeline: [
              {
                $match: {
                  $expr: {
                    $eq: ['$type', 'brokering_contract']
                  }
                }
              }
            ],
            as: 'commissions'
          }
        },
        appHelper.getUnwindPipeline('commissions', false),
        {
          $group: {
            _id: null,
            totalCommissions: { $sum: '$commissions.amount' }
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo')
]

const getOthersPipelineForJournalSummary = () => [
  {
    $addFields: {
      totalOthers: {
        $reduce: {
          input: { $ifNull: ['$addons', []] },
          initialValue: 0,
          in: {
            $add: [
              '$$value',
              {
                $cond: [
                  { $eq: ['$$this.type', 'assignment'] },
                  { $ifNull: ['$$this.total', 0] },
                  0
                ]
              }
            ]
          }
        }
      }
    }
  }
]

const getJournalSummary = async (query, partnerType = '') => {
  const pipeline = [
    {
      $match: query
    },
    ...getOthersPipelineForJournalSummary(),
    {
      $group: {
        _id: null,
        totalRent: { $sum: '$rentalMeta.monthlyRentAmount' },
        contractIds: { $push: '$_id' },
        totalOthers: { $sum: '$totalOthers' }
      }
    }
  ]
  if (partnerType === 'broker') {
    pipeline.push(...getCommissionPipelineForJournalSummary())
  }
  pipeline.push({
    $project: {
      totalRent: {
        $multiply: [12, '$totalRent']
      },
      totalCommissions: '$invoiceInfo.totalCommissions',
      totalOthers: 1,
      totalIncome: {
        $add: [
          { $ifNull: ['$invoiceInfo.totalCommissions', 0] },
          '$totalOthers'
        ]
      }
    }
  })
  const [summary = {}] = (await ContractCollection.aggregate(pipeline)) || []
  return summary
}

export const queryJournalSummary = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  // To check if partner enable broker journals or not
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  if (!partnerInfo?.enableBrokerJournals) {
    throw new CustomError(400, 'Broker journals not enabled for this partner')
  }
  body.partnerId = partnerId
  body.type = 'lease'
  const query = await prepareAssignmentsOrLeasesQuery(body)
  return await getJournalSummary(query, partnerInfo.accountType)
}

const onlyUserDataPipeline = (localFieldValue = '', asFieldValue = '') => ({
  $lookup: {
    from: 'users',
    localField: localFieldValue,
    foreignField: '_id',
    as: asFieldValue
  }
})

const getAccountInfoPipeline = (localFieldValue, asFieldValue) => [
  {
    $lookup: {
      from: 'accounts',
      localField: localFieldValue,
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'personId',
            foreignField: '_id',
            as: 'person'
          }
        },
        appHelper.getUnwindPipeline('person'),
        {
          $lookup: {
            from: 'organizations',
            localField: 'organizationId',
            foreignField: '_id',
            as: 'organization'
          }
        },
        appHelper.getUnwindPipeline('organization'),
        {
          $project: {
            _id: 1,
            name: 1,
            avatarKey: {
              $cond: [
                { $eq: ['$type', 'person'] },
                appHelper.getUserAvatarKeyPipeline('$person.profile.avatarKey'),
                appHelper.getOrganizationLogoPipeline('$organization.image')
              ]
            }
          }
        }
      ],
      as: asFieldValue
    }
  },
  appHelper.getUnwindPipeline(asFieldValue)
]

const journalChangeLogQuery = async (body) => {
  const { filterData, query, options } = body
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    appHelper.getUnwindPipeline('history'),
    {
      $match: {
        'history.name': { $in: filterData }
      }
    },
    {
      $project: {
        history: 1
      }
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $addFields: {
        // Tenant information will use for turnover journal change log
        tenantOldIds: {
          $cond: [
            { $eq: ['$history.name', 'tenant'] },
            { $split: ['$history.oldValue', ','] },
            []
          ]
        },
        tenantNewIds: {
          $cond: [
            { $eq: ['$history.name', 'tenant'] },
            { $split: ['$history.newValue', ','] },
            []
          ]
        },
        accountUserOldId: {
          $cond: [
            { $eq: ['$history.name', 'account'] },
            '$history.oldValue',
            ''
          ]
        },
        accountUserNewId: {
          $cond: [
            { $eq: ['$history.name', 'account'] },
            '$history.newValue',
            ''
          ]
        },
        agentUserOldId: {
          $cond: [{ $eq: ['$history.name', 'agent'] }, '$history.oldValue', '']
        },
        agentUserNewId: {
          $cond: [{ $eq: ['$history.name', 'agent'] }, '$history.newValue', '']
        },
        representativeUserOldId: {
          $cond: [
            { $eq: ['$history.name', 'representative'] },
            '$history.oldValue',
            ''
          ]
        },
        representativeUserNewId: {
          $cond: [
            { $eq: ['$history.name', 'representative'] },
            '$history.newValue',
            ''
          ]
        }
      }
    },
    // Old tenants info
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantOldIds',
        foreignField: '_id',
        pipeline: [
          onlyUserDataPipeline('userId', 'userInfo'),
          appHelper.getUnwindPipeline('userInfo', false),
          {
            $project: {
              _id: 1,
              name: 1,
              avatarKey: appHelper.getUserAvatarKeyPipeline(
                '$userInfo.profile.avatarKey'
              )
            }
          }
        ],
        as: 'oldTenantInfo'
      }
    },
    // New tenant info
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantNewIds',
        foreignField: '_id',
        pipeline: [
          onlyUserDataPipeline('userId', 'userInfo'),
          appHelper.getUnwindPipeline('userInfo'),
          {
            $project: {
              _id: 1,
              name: 1,
              avatarKey: appHelper.getUserAvatarKeyPipeline(
                '$userInfo.profile.avatarKey'
              )
            }
          }
        ],
        as: 'newTenantInfo'
      }
    },

    // Account user old info
    ...getAccountInfoPipeline('accountUserOldId', 'accountUserOldInfo'),
    // Account user New info
    ...getAccountInfoPipeline('accountUserNewId', 'accountUserNewInfo'),
    // Agent user old info
    ...appHelper.getCommonUserInfoPipeline(
      'agentUserOldId',
      'agentUserOldInfo'
    ),
    // Agent user New info
    ...appHelper.getCommonUserInfoPipeline(
      'agentUserNewId',
      'agentUserNewInfo'
    ),

    // Representative user old info
    ...appHelper.getCommonUserInfoPipeline(
      'representativeUserOldId',
      'representativeUserOldInfo'
    ),
    // Representative user New info
    ...appHelper.getCommonUserInfoPipeline(
      'representativeUserNewId',
      'representativeUserNewInfo'
    ),
    {
      $project: {
        name: '$history.name',
        history: 1,
        oldTenantInfo: 1,
        newTenantInfo: 1,
        accountUserOldInfo: 1,
        accountUserNewInfo: 1,
        agentUserOldInfo: 1,
        agentUserNewInfo: 1,
        representativeUserOldInfo: 1,
        representativeUserNewInfo: 1
      }
    }
  ]
  const changeLog = (await ContractCollection.aggregate(pipeline)) || []
  return changeLog
}

export const journalChangeLogForPartnerApp = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.validateId({ partnerId })
  appHelper.checkUserId(userId)
  const { query, options } = body
  appHelper.checkRequiredFields(['context', 'contractId'], query)
  appHelper.validateSortForQuery(options.sort)
  const { context, contractId } = query
  appHelper.validateId({ contractId })
  body.query = {
    _id: contractId,
    partnerId
  }

  if (context === 'assignment_journals') {
    body.filterData = [
      'account',
      'address',
      'agent',
      'assignmentFrom',
      'assignmentMonthlyRentAmount',
      'assignmentTo',
      'gnr_bnr_snr',
      'listing_type',
      'property_type',
      'representative'
    ]
  } else {
    body.filterData = [
      'account',
      'address',
      'agent',
      'commissions',
      'gnr_bnr_snr',
      'leaseMonthlyRentAmount',
      'listing_type',
      'other',
      'property_type',
      'signedAt',
      'tenant',
      'total_income'
    ]
  }

  const changeLogs = await journalChangeLogQuery(body)
  const { totalDocuments, filteredDocuments } =
    await getCountDocumentForJournalChangeLog(body)
  return { data: changeLogs, metaData: { totalDocuments, filteredDocuments } }
}

const getCountDocumentForJournalChangeLog = async (body) => {
  const { filterData, query } = body
  const data = (await ContractCollection.findOne(query, { history: 1 })) || []
  const totalDocuments = size(data.history)
  const filteredDocuments = data.history.filter((item) =>
    filterData.includes(item.name)
  )

  return { totalDocuments, filteredDocuments: size(filteredDocuments) }
}

const addonsInfoPipeline = () => [
  {
    $addFields: {
      addons: {
        $filter: {
          input: { $ifNull: ['$addons', []] },
          as: 'data',
          cond: { $eq: ['$$data.type', 'lease'] }
        }
      }
    }
  },
  appHelper.getUnwindPipeline('addons'),
  {
    $lookup: {
      from: 'products_services',
      localField: 'addons.addonId',
      foreignField: '_id',
      as: 'addonsInfo'
    }
  },
  appHelper.getUnwindPipeline('addonsInfo'),
  {
    $lookup: {
      from: 'ledger_accounts',
      localField: 'addonsInfo.creditAccountId',
      foreignField: '_id',
      as: 'ledgerAccountInfo'
    }
  },
  appHelper.getUnwindPipeline('ledgerAccountInfo'),
  {
    $lookup: {
      from: 'tax_codes',
      localField: 'ledgerAccountInfo.taxCodeId',
      foreignField: '_id',
      as: 'taxCodeInfo'
    }
  },
  appHelper.getUnwindPipeline('taxCodeInfo')
]

const getMovingPipelineForLeaseDeails = (partnerType) => [
  {
    $lookup: {
      from: 'property_items',
      localField: '_id',
      foreignField: 'contractId',
      as: 'movingInfo',
      pipeline: [
        {
          $lookup: {
            from: 'files',
            localField: '_id',
            foreignField: 'movingId',
            as: 'movingFileInfo'
          }
        },
        appHelper.getUnwindPipeline('movingFileInfo'),
        {
          $addFields: {
            notSignedTenant: {
              $filter: {
                input: { $ifNull: ['$tenantSigningStatus', []] },
                as: 'tenantSign',
                cond: {
                  $eq: ['$$tenantSign.signed', false]
                }
              }
            }
          }
        },
        {
          $addFields: {
            signingUrl: {
              $cond: [
                { $eq: [partnerType, 'broker'] },
                '$agentSigningStatus.signingUrl',
                '$landlordSigningStatus.signingUrl'
              ]
            },
            isTenantSignedOfMoveInOut: {
              $cond: [
                {
                  $and: [
                    { $eq: [{ $size: '$notSignedTenant' }, 0] },
                    {
                      $gt: [
                        { $size: { $ifNull: ['$tenantSigningStatus', []] } },
                        0
                      ]
                    }
                  ]
                },
                true,
                false
              ]
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      movingInInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$movingInfo', []] },
            as: 'item',
            cond: {
              $eq: ['$$item.type', 'in']
            }
          }
        }
      },
      movingOutInfo: {
        $first: {
          $filter: {
            input: { $ifNull: ['$movingInfo', []] },
            as: 'item',
            cond: {
              $eq: ['$$item.type', 'out']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      isShowMoveIn: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $ifNull: ['$movingInInfo.signingUrl', false] },
                  { $ifNull: ['$movingInInfo.agentSigningStatus', false] },
                  {
                    $not: {
                      $eq: ['$movingInInfo.agentSigningStatus.signed', true]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$movingInInfo.moveInCompleted', true] },
                  {
                    $ifNull: ['$movingInInfo.movingFileInfo', false]
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  {
                    $gt: [
                      {
                        $size: {
                          $ifNull: ['$movingInInfo.tenantSigningStatus', []]
                        }
                      },
                      0
                    ]
                  },
                  {
                    $eq: ['$movingInInfo.isTenantSignedOfMoveInOut', false]
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$movingInInfo.isEsigningInitiate', true] },
                  {
                    $not: { $eq: ['$movingInInfo.movingInPdfGenerated', true] }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$movingInInfo.isTenantSignedOfMoveInOut', true] },
                  {
                    $not: {
                      $eq: ['$movingInInfo.movingInSigningComplete', true]
                    }
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      isShowMoveOut: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $ifNull: ['$movingOutInfo.signingUrl', false] },
                  { $ifNull: ['$movingOutInfo.agentSigningStatus', false] },
                  {
                    $not: {
                      $eq: ['$movingOutInfo.agentSigningStatus.signed', true]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$movingOutInfo.moveOutCompleted', true] },
                  {
                    $ifNull: ['$movingOutInfo.movingFileInfo', false]
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$movingOutInfo.isTenantSignedOfMoveInOut', true] },
                  {
                    $not: {
                      $eq: ['$movingOutInfo.movingOutSigningComplete', true]
                    }
                  },
                  { $not: { $eq: ['$movingOutInfo.moveOutCompleted', true] } }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$movingOutInfo.agentSigningStatus.signed', true] },
                  {
                    $not: {
                      $eq: ['$movingOutInfo.movingOutSigningComplete', true]
                    }
                  },
                  {
                    $not: {
                      $eq: ['$movingOutInfo.moveOutCompleted', true]
                    }
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      movingInInfo: {
        $cond: [
          { $ifNull: ['$movingInInfo', false] },
          {
            signingUrl: '$movingInInfo.signingUrl',
            moveInCompleted: '$movingInInfo.moveInCompleted',
            pdfFileId: '$movingInInfo.movingFileInfo._id',
            documentPreparing: {
              $cond: [
                {
                  $and: [
                    { $eq: ['$movingInInfo.isEsigningInitiate', true] },
                    {
                      $not: {
                        $eq: ['$movingInInfo.movingInPdfGenerated', true]
                      }
                    }
                  ]
                },
                true,
                false
              ]
            },
            isTenantSigningStatusExists: {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ['$movingInInfo.tenantSigningStatus', []]
                      }
                    },
                    0
                  ]
                },
                true,
                false
              ]
            },
            isTenantSignedOfMoveInOut:
              '$movingInInfo.isTenantSignedOfMoveInOut',
            movingInSigningComplete: '$movingInInfo.movingInSigningComplete',
            isAgentSignedOfMoveIn: '$movingInInfo.agentSigningStatus.signed',
            isLandlordSignedOfMoveIn:
              '$movingInInfo.landlordSigningStatus.signed',
            isEsigningInitiate: '$movingInInfo.isEsigningInitiate'
          },
          '$$REMOVE'
        ]
      },
      movingOutInfo: {
        $cond: [
          { $ifNull: ['$movingOutInfo', false] },
          {
            signingUrl: '$movingOutInfo.signingUrl',
            moveOutCompleted: '$movingOutInfo.moveOutCompleted',
            pdfFileId: '$movingOutInfo.movingFileInfo._id',
            isEsigningInitiate: '$movingOutInfo.isEsigningInitiate',
            isTenantSignedOfMoveInOut:
              '$movingOutInfo.isTenantSignedOfMoveInOut',
            movingOutSigningComplete: '$movingOutInfo.movingOutSigningComplete',
            isAgentSignedMovingOut: '$movingOutInfo.agentSigningStatus.signed',
            isLandlordSignedOfMoveOut:
              '$movingOutInfo.landlordSigningStatus.signed'
          },
          '$$REMOVE'
        ]
      }
    }
  }
]

const leaseDetailsForAContract = async (query, partnerInfo) => {
  const { contractId, leaseSerial, userId } = query
  const leaseSerialMatchQuery = { $match: {} }

  if (leaseSerial) {
    leaseSerialMatchQuery.$match = {
      'rentalMeta.leaseSerial': leaseSerial
    }
  }

  const pipeline = [
    {
      $match: { _id: contractId, partnerId: partnerInfo._id }
    },
    {
      $addFields: {
        rentalMeta: {
          $cond: [{ $eq: ['$hasRentalContract', true] }, ['$rentalMeta'], null]
        }
      }
    },
    {
      $addFields: {
        rentalMeta: {
          $concatArrays: [
            { $ifNull: ['$rentalMeta', []] },
            { $ifNull: ['$rentalMetaHistory', []] }
          ]
        }
      }
    },
    appHelper.getUnwindPipeline('rentalMeta'),
    {
      $addFields: {
        'rentalMeta.tenantLeaseSigningStatus': {
          $cond: [
            {
              $and: [
                { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', false] },
                {
                  $ne: [
                    { $isArray: '$rentalMeta.tenantLeaseSigningStatus' },
                    true
                  ]
                }
              ]
            },
            ['$rentalMeta.tenantLeaseSigningStatus'],
            { $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', []] }
          ]
        },
        'rentalMeta.leaseSerial': {
          $ifNull: ['$rentalMeta.leaseSerial', '$leaseSerial']
        }
      }
    },
    leaseSerialMatchQuery,
    // Account info
    ...appHelper.getCommonAccountInfoPipeline(),
    // Property info
    ...appHelper.getCommonPropertyInfoPipeline(),
    // Owner info
    ...appHelper.getCommonUserInfoPipeline('agentId', 'ownerInfo'),
    // Representative info
    ...appHelper.getCommonUserInfoPipeline(
      'representativeId',
      'representativeInfo'
    ),
    // Created by
    ...appHelper.getCommonUserInfoPipeline(
      'rentalMeta.createdBy',
      'createdByInfo'
    ),
    // Main tenant info
    {
      $lookup: {
        from: 'tenants',
        localField: 'rentalMeta.tenantId',
        foreignField: '_id',
        pipeline: [
          ...appHelper.getCommonUserInfoPipeline('userId', 'userInfo'),
          {
            $project: {
              _id: 1,
              name: 1,
              avatarKey: '$userInfo.avatarKey'
            }
          }
        ],
        as: 'mainTenantInfo'
      }
    },
    appHelper.getUnwindPipeline('mainTenantInfo'),
    // Other tenant info
    {
      $lookup: {
        from: 'tenants',
        localField: 'rentalMeta.tenants.tenantId',
        foreignField: '_id',
        let: { mainTenant: '$mainTenantInfo._id' },
        pipeline: [
          {
            $match: {
              $expr: {
                $ne: ['$_id', '$$mainTenant']
              }
            }
          },
          ...appHelper.getCommonUserInfoPipeline('userId', 'userInfo'),
          {
            $project: {
              _id: 1,
              name: 1,
              avatarKey: '$userInfo.avatarKey'
            }
          }
        ],
        as: 'otherTenantInfo'
      }
    },
    ...getDepositAccountStatusPipelineForLeaseList(
      partnerInfo.enableDepositAccount
    ),
    // Need isEnabledDepositAccountProcess from previous pipeline
    ...getDocumentPreparingStatus(),
    ...getDepositInsurancePipelineForLeaseList(),
    {
      $addFields: {
        depositInsuranceErrors: {
          $cond: [
            { $eq: ['$depositInsurance.status', 'failed'] },
            '$depositInsurance.creationResult.reasons',
            null
          ]
        }
      }
    },
    ...getSigningStatusPipeline(partnerInfo.accountType, userId),
    {
      $addFields: {
        landlordSigned: {
          $cond: [
            { $eq: ['$rentalMeta.landlordLeaseSigningStatus.signed', true] },
            true,
            false
          ]
        },
        landlordLeaseSigningUrl: {
          $cond: [
            { $eq: ['$showSigningUrl', true] },
            '$rentalMeta.landlordLeaseSigningStatus.signingUrl',
            null
          ]
        }
      }
    },
    ...getMovingPipelineForLeaseDeails(partnerInfo.accountType),
    ...appHelper.getCommonUserInfoPipeline(
      'terminatedByUserId',
      'terminatedByInfo'
    ),
    // Addons info
    ...addonsInfoPipeline(),
    {
      $group: {
        _id: '$_id',
        leaseSerial: { $first: leaseSerial },
        leaseContractPdfGenerated: {
          $first: '$leaseContractPdfGenerated'
        },
        enabledLeaseEsigning: {
          $first: '$rentalMeta.enabledLeaseEsigning'
        },
        leaseSigningComplete: {
          $first: '$rentalMeta.leaseSigningComplete'
        },
        tenantSigned: {
          $first: '$tenantSigned'
        },
        showSigningUrl: {
          $first: '$showSigningUrl'
        },
        landlordSigned: {
          $first: '$landlordSigned'
        },
        createdAt: { $first: '$rentalMeta.createdAt' },
        status: { $first: '$rentalMeta.status' },
        // Rent and invoices
        monthlyRentAmount: { $first: '$rentalMeta.monthlyRentAmount' },
        depositAmount: { $first: '$rentalMeta.depositAmount' },
        firstInvoiceDueDate: { $first: '$rentalMeta.firstInvoiceDueDate' },
        dueDate: { $first: '$rentalMeta.dueDate' },
        invoiceFrequency: { $first: '$rentalMeta.invoiceFrequency' },
        invoiceStartFrom: { $first: '$rentalMeta.invoiceStartFrom' },
        isVatEnable: { $first: '$rentalMeta.isVatEnable' },

        // CPI and others
        cpiEnabled: { $first: '$rentalMeta.cpiEnabled' },
        lastCpiDate: { $first: '$rentalMeta.lastCpiDate' },
        nextCpiDate: { $first: '$rentalMeta.nextCpiDate' },
        movingInDate: { $first: '$rentalMeta.movingInDate' },
        isMovedIn: { $first: '$rentalMeta.isMovedIn' },

        // Deposit
        depositType: { $first: '$rentalMeta.depositType' },
        enabledJointDepositAccount: {
          $first: '$rentalMeta.enabledJointDepositAccount'
        },
        enabledJointlyLiable: { $first: '$rentalMeta.enabledJointlyLiable' },
        // for deposit account
        isEnabledDepositAccountProcess: {
          $first: '$isEnabledDepositAccountProcess'
        },
        isEnabledRecurringDueDate: {
          $first: '$rentalMeta.isEnabledRecurringDueDate'
        },
        isDepositAccountDocumentPreparing: {
          $first: '$isDepositAccountDocumentPreparing'
        },
        isDepositAccountCreated: { $first: '$isDepositAccountCreated' },
        isDepositDataSentToBank: { $first: '$isDepositDataSentToBank' },
        isFullPaid: { $first: '$isFullPaid' },
        isDepositAmountPaid: { $first: '$isDepositAmountPaid' },
        isAnyAccountPartiallyPaid: { $first: '$isAnyAccountPartiallyPaid' },
        depositAccountError: { $first: '$rentalMeta.depositAccountError' },
        //Deposit insurance
        depositInsuranceStatus: { $first: '$depositInsurance.status' },
        depositInsuranceErrors: { $first: '$depositInsuranceErrors' },
        // Notice and others
        contractStartDate: { $first: '$rentalMeta.contractStartDate' },
        contractEndDate: { $first: '$rentalMeta.contractEndDate' },
        noticePeriod: { $first: '$rentalMeta.noticePeriod' },
        noticeInEffect: { $first: '$rentalMeta.noticeInEffect' },
        minimumStay: { $first: '$rentalMeta.minimumStay' },
        signedAt: { $first: '$rentalMeta.signedAt' },
        internalLeaseId: { $first: '$rentalMeta.internalLeaseId' },
        // Addons info
        addons: {
          $push: {
            $cond: [
              { $ifNull: ['$addons', false] },
              {
                addonId: '$addons.addonId',
                allowPriceEdit: '$addonsInfo.allowPriceEdit',
                name: '$addonsInfo.name',
                isRecurring: '$addons.isRecurring',
                taxPercentage: { $ifNull: ['$taxCodeInfo.taxPercentage', 0] },
                amount: { $ifNull: ['$addons.price', 0] }
              },
              '$$REMOVE'
            ]
          }
        },
        accountInfo: { $first: '$accountInfo' },
        propertyInfo: { $first: '$propertyInfo' },
        ownerInfo: { $first: '$ownerInfo' },
        representativeInfo: { $first: '$representativeInfo' },
        createdByInfo: { $first: '$createdByInfo' },
        mainTenantInfo: { $first: '$mainTenantInfo' },
        otherTenantInfo: { $first: '$otherTenantInfo' },
        disableVipps: { $first: '$rentalMeta.disableVipps' },
        disableCompello: { $first: '$rentalMeta.disableCompello' },
        enabledAnnualStatement: {
          $first: '$rentalMeta.enabledAnnualStatement'
        },
        terminateReasons: { $first: '$rentalMeta.terminateReasons' },
        terminateComments: { $first: '$rentalMeta.terminateComments' },
        terminatedByInfo: { $first: '$terminatedByInfo' },
        invoiceAccountNumber: { $first: '$rentalMeta.invoiceAccountNumber' },
        isShowMoveIn: { $first: '$isShowMoveIn' },
        isShowMoveOut: { $first: '$isShowMoveOut' },
        movingInInfo: { $first: '$movingInInfo' },
        movingOutInfo: { $first: '$movingOutInfo' },
        isDocumentPreparing: { $first: '$isDocumentPreparing' },
        isTenantWaiting: { $first: '$isTenantWaiting' },
        landlordLeaseSigningUrl: { $first: '$landlordLeaseSigningUrl' }
      }
    }
  ]

  const [leaseDetails = {}] =
    (await ContractCollection.aggregate(pipeline)) || []
  return leaseDetails
}

export const leaseDetailsForPartnerApp = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['contractId', 'leaseSerial'], body)
  const { contractId } = body
  body.userId = userId
  appHelper.validateId({ contractId })
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  if (!size(partnerInfo)) {
    throw new CustomError(404, 'not found')
  }
  const leaseDetails = await leaseDetailsForAContract(body, partnerInfo)
  if (!size(leaseDetails?._id)) {
    throw new CustomError(404, 'Lease details not found')
  }
  return leaseDetails
}

export const getContractQueryForFinalSettlement = async (params) => {
  const { contractId, isManualFinalSettlement, partnerId } = params

  const query = {}

  if (partnerId) {
    const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })

    if (!size(partnerSettings))
      throw new CustomError(404, 'PartnerSettings does not exists')
    // Final settlement will be in-progress or complete if contract closed before 3 months from today
    // Otherwise final settlement will not working
    // Getting date before 3 months from today
    const terminationDateWillBe = (
      await appHelper.getActualDate(partnerSettings, true, null)
    )
      .subtract(3, 'months')
      .toDate()

    if (!isManualFinalSettlement) {
      query['rentalMeta.contractEndDate'] = {
        $exists: true,
        $lt: terminationDateWillBe
      }
    }
    query.status = 'closed'
    query.partnerId = partnerId
    if (contractId) query._id = contractId
  }

  return query
}

const getPipelineForCountReadyEviction = () => [
  {
    $addFields: {
      countReadyEviction: {
        $cond: [
          {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: { $ifNull: ['$evictionCases', []] },
                    as: 'eviction',
                    cond: {
                      $eq: ['$$eviction.status', 'new']
                    }
                  }
                }
              },
              0
            ]
          },
          1,
          0
        ]
      }
    }
  }
]

const getPipelineForCountFinalSettlementNotDone = () => [
  {
    $addFields: {
      countFinalSettlementNotDone: {
        $cond: [
          {
            $and: [
              { $eq: ['$status', 'closed'] },
              { $eq: ['$rentalMeta.status', 'closed'] },
              { $ne: ['$finalSettlementStatus', 'completed'] }
            ]
          },
          1,
          0
        ]
      }
    }
  }
]

const getLeaseStatus = async (query, date = {}) => {
  const { compareDate, currentDate, selectedMonth } = date
  const result = await ContractCollection.aggregate([
    {
      $match: query
    },
    ...getPipelineForCountReadyEviction(),
    ...getPipelineForCountFinalSettlementNotDone(),
    {
      $group: {
        _id: null,
        soonEndingLease: {
          $push: {
            $cond: {
              if: {
                $and: [
                  { $ifNull: ['$rentalMeta.contractEndDate', false] },
                  { $lte: ['$rentalMeta.contractEndDate', selectedMonth] },
                  { $eq: ['$status', 'active'] },
                  { $eq: ['$rentalMeta.status', 'active'] },
                  { $eq: ['$hasRentalContract', true] }
                ]
              },
              then: '$propertyId',
              else: '$$REMOVE'
            }
          }
        },
        endingLeaseInNextSevenDays: {
          $sum: {
            $cond: {
              if: {
                $and: [
                  { $ifNull: ['$rentalMeta.contractEndDate', false] },
                  { $lt: ['$rentalMeta.contractEndDate', compareDate] },
                  { $gt: ['$rentalMeta.contractEndDate', currentDate] },
                  { $eq: ['$status', 'active'] },
                  { $eq: ['$rentalMeta.status', 'active'] }
                ]
              },
              then: 1,
              else: 0
            }
          }
        },
        upcomingLeaseInNextSevenDays: {
          $sum: {
            $cond: {
              if: {
                $and: [
                  { $ifNull: ['$rentalMeta.contractStartDate', false] },
                  { $lt: ['$rentalMeta.contractStartDate', compareDate] },
                  { $gt: ['$rentalMeta.contractStartDate', currentDate] },
                  { $eq: ['$rentalMeta.status', 'upcoming'] },
                  { $eq: ['$status', 'upcoming'] }
                ]
              },
              then: 1,
              else: 0
            }
          }
        },
        totalEvictionsReadyToSend: {
          $sum: '$countReadyEviction'
        },
        totalFinalSettlementsNotDone: {
          $sum: '$countFinalSettlementNotDone'
        }
      }
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'soonEndingLease',
        foreignField: '_id',
        as: 'listingsInfo'
      }
    },
    {
      $addFields: {
        listingsInfo: {
          $filter: {
            input: { $ifNull: ['$listingsInfo', []] },
            as: 'listing',
            cond: {
              $and: [
                { $ne: ['$$listing.hasInProgressLease', true] },
                { $ne: ['$$listing.hasUpcomingLease', true] }
              ]
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 0,
        endingLeaseInNextSevenDays: '$endingLeaseInNextSevenDays',
        soonEndingLease: {
          $size: '$soonEndingLease'
        },
        soonEndingLeaseWihtoutNewLease: {
          $size: '$listingsInfo'
        },
        totalEvictionsReadyToSend: 1,
        totalFinalSettlementsNotDone: 1,
        upcomingLeaseInNextSevenDays: '$upcomingLeaseInNextSevenDays'
      }
    }
  ])

  const [leaseStatus = {}] = result || []
  return leaseStatus
}

export const queryLeaseStatusForPartnerDashboard = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const preparedQuery = dashboardHelper.prepareQueryForPartnerDashboard(body)

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  let soonEndingMonths = 4
  soonEndingMonths =
    partnerSetting?.propertySettings?.soonEndingMonths || soonEndingMonths
  const selectedMonth = (await appHelper.getActualDate(partnerSetting, true))
    .add(soonEndingMonths, 'months')
    .toDate()
  const currentDate = (
    await appHelper.getActualDate(partnerSetting, true)
  ).toDate()
  const compareDate = (await appHelper.getActualDate(partnerSetting, true))
    .add(7, 'days')
    .startOf('day')
    .toDate()

  return await getLeaseStatus(preparedQuery, {
    compareDate,
    currentDate,
    selectedMonth
  })
}

export const getAssignmentAndLeaseESignStatus = async (query) => {
  const result = await ContractCollection.aggregate([
    {
      $match: {
        ...query,
        status: {
          $nin: ['closed', 'new']
        },
        $or: [
          {
            'rentalMeta.enabledLeaseEsigning': true,
            leaseSigningComplete: { $ne: true }
          },
          { enabledEsigning: true }
        ]
      }
    },
    {
      $addFields: {
        awaitingTenantLeaseCount: {
          $cond: [
            {
              $and: [{ $eq: ['$rentalMeta.status', 'in_progress'] }]
            },
            {
              $cond: {
                if: {
                  $eq: [
                    {
                      $size: {
                        $filter: {
                          input: {
                            $ifNull: [
                              '$rentalMeta.tenantLeaseSigningStatus',
                              []
                            ]
                          },
                          as: 'tenant',
                          cond: {
                            $eq: ['$$tenant.signed', false]
                          }
                        }
                      }
                    },
                    0
                  ]
                },
                then: 0,
                else: 1
              }
            },
            0
          ]
        },
        awaitingLandlordLeaseCount: {
          $cond: [
            {
              $and: [
                { $eq: ['$rentalMeta.status', 'in_progress'] },
                {
                  $eq: ['$rentalMeta.landlordLeaseSigningStatus.signed', false]
                }
              ]
            },
            {
              $cond: {
                if: {
                  $and: [
                    {
                      $gt: [
                        {
                          $size: {
                            $ifNull: [
                              '$rentalMeta.tenantLeaseSigningStatus',
                              []
                            ]
                          }
                        },
                        0
                      ]
                    },
                    {
                      $eq: [
                        {
                          $size: {
                            $filter: {
                              input: {
                                $ifNull: [
                                  '$rentalMeta.tenantLeaseSigningStatus',
                                  []
                                ]
                              },
                              as: 'tenant',
                              cond: {
                                $eq: ['$$tenant.signed', false]
                              }
                            }
                          }
                        },
                        0
                      ]
                    }
                  ]
                },
                then: 1,
                else: 0
              }
            },
            0
          ]
        },
        landlordAssignmentSigningStatus: {
          $cond: [
            { $eq: ['$landlordAssignmentSigningStatus.signed', false] },
            1,
            0
          ]
        },
        agentAssignmentSigningStatus: {
          $cond: [
            { $eq: ['$agentAssignmentSigningStatus.signed', false] },
            1,
            0
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        totalTenantLeaseSigning: {
          $sum: '$awaitingTenantLeaseCount'
        },
        totalLandlordLeaseSigning: {
          $sum: '$awaitingLandlordLeaseCount'
        },
        totalLandlordAssignmentSigning: {
          $sum: '$landlordAssignmentSigningStatus'
        },
        totalAgentAssignmentSigning: {
          $sum: '$agentAssignmentSigningStatus'
        }
      }
    },
    {
      $project: {
        _id: 0,
        totalAgentAssignmentSigning: 1,
        totalLandlordAssignmentSigning: 1,
        totalLandlordLeaseSigning: 1,
        totalTenantLeaseSigning: 1
      }
    }
  ])
  const [signingStatus = {}] = result || []
  const {
    totalAgentAssignmentSigning = 0,
    totalLandlordAssignmentSigning = 0,
    totalLandlordLeaseSigning = 0,
    totalTenantLeaseSigning = 0
  } = signingStatus

  return {
    totalAgentAssignmentSigning,
    totalLandlordAssignmentSigning,
    totalLandlordLeaseSigning,
    totalTenantLeaseSigning
  }
}

export const getNotCreatedMovingInOutLease = async (
  partnerSettingsInfo,
  query
) => {
  const todayDate = (
    await appHelper.getActualDate(partnerSettingsInfo, true, new Date())
  )._d
  const compareDate = (
    await appHelper.getActualDate(partnerSettingsInfo, true, new Date())
  )
    .subtract(7, 'days')
    .startOf('day')._d

  const result = await ContractCollection.aggregate([
    {
      $match: {
        ...query,
        $or: [
          {
            'rentalMeta.contractEndDate': {
              $gte: compareDate,
              $lte: todayDate,
              $exists: true
            },
            status: 'closed'
          },
          {
            'rentalMeta.contractStartDate': {
              $gte: compareDate,
              $lte: todayDate,
              $exists: true
            },
            status: 'active'
          }
        ]
      }
    },
    {
      $group: {
        _id: null,
        activeContractIds: {
          $push: {
            $cond: [{ $eq: ['$status', 'active'] }, '$_id', '$$REMOVE']
          }
        },
        closedContractIds: {
          $push: {
            $cond: [{ $eq: ['$status', 'closed'] }, '$_id', '$$REMOVE']
          }
        },
        activeContractPropertyIds: {
          $addToSet: {
            $cond: [{ $eq: ['$status', 'active'] }, '$propertyId', '$$REMOVE']
          }
        },
        closedContractPropertyIds: {
          $addToSet: {
            $cond: [{ $eq: ['$status', 'closed'] }, '$propertyId', '$$REMOVE']
          }
        }
      }
    },
    {
      $lookup: {
        from: 'property_items',
        localField: 'activeContractIds',
        foreignField: 'contractId',
        pipeline: [
          {
            $match: {
              isEsigningInitiate: true,
              type: 'in'
            }
          },
          {
            $group: {
              _id: null,
              propertyId: {
                $addToSet: '$propertyId'
              }
            }
          }
        ],
        as: 'movingInItems'
      }
    },
    { $unwind: { path: '$movingInItems', preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        from: 'property_items',
        localField: 'closedContractIds',
        foreignField: 'contractId',
        pipeline: [
          {
            $match: {
              isEsigningInitiate: true,
              type: 'out'
            }
          },
          {
            $group: {
              _id: null,
              propertyId: {
                $addToSet: '$propertyId'
              }
            }
          }
        ],
        as: 'movingOutItems'
      }
    },
    { $unwind: { path: '$movingOutItems', preserveNullAndEmptyArrays: true } },
    {
      $project: {
        activeContractPropertyIds: 1,
        closedContractPropertyIds: 1,
        movingInCompletedPropertyIds: {
          $ifNull: ['$movingInItems.propertyId', []]
        },
        movingOutCompletedPropertyIds: {
          $ifNull: ['$movingOutItems.propertyId', []]
        }
      }
    }
  ])
  const [notCreatedMoveInOutLease = {}] = result || []
  const {
    activeContractPropertyIds = [],
    closedContractPropertyIds = [],
    movingInCompletedPropertyIds = [],
    movingOutCompletedPropertyIds = []
  } = notCreatedMoveInOutLease || {}

  const moveInNotCreated = difference(
    activeContractPropertyIds,
    movingInCompletedPropertyIds
  ).length
  const moveOutNotCreated = difference(
    closedContractPropertyIds,
    movingOutCompletedPropertyIds
  ).length

  return {
    moveInNotCreated,
    moveOutNotCreated
  }
}

export const validateDataToCreateAnAssignment = async (body = {}) => {
  appHelper.checkRequiredFields(['userId', 'partnerId'], body)
  const { partnerId, userId } = body
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  if (!isBrokerPartner)
    throw new CustomError(
      400,
      'Only broker partners are allowed to create an assignment'
    )
  appHelper.checkRequiredFields(
    [
      'propertyId',
      'assignmentSignatureMechanism',
      'hasBrokeringContract',
      'brokeringCommissionType',
      'brokeringCommissionAmount',
      'hasRentalManagementContract',
      'rentalManagementCommissionType',
      'rentalManagementCommissionAmount',
      'payoutTo',
      'monthlyPayoutDate',
      'agentId',
      'representativeId',
      'listingInfo',
      'signDate'
    ],
    body
  )
  const { listingInfo = {}, payoutTo, propertyId } = body
  appHelper.validateId({ propertyId })
  appHelper.checkRequiredFields(
    [
      'availabilityStartDate',
      'monthlyRentAmount',
      'minimumStay',
      'depositAmount'
    ],
    listingInfo
  )
  if (payoutTo.length !== 11 || isNaN(payoutTo))
    throw new CustomError(400, 'Please provide a correct account number')
}

export const prepareDataToCreateAnAssignment = (body) => {
  const {
    actionType,
    addons,
    agentId,
    assignmentFrom,
    assignmentTo,
    assignmentSignatureMechanism,
    brokeringCommissionAmount,
    brokeringCommissionType,
    hasBrokeringContract,
    hasRentalManagementContract,
    internalAssignmentId,
    listingInfo,
    monthlyPayoutDate,
    payoutTo,
    partnerId,
    property = {},
    propertyId,
    representativeId,
    rentalCommission,
    rentalManagementCommissionAmount,
    rentalManagementCommissionType,
    signDate,
    templateId,
    userId
  } = body
  let { enabledEsigning } = body
  const contractData = {}
  contractData.branchId = property.branchId
  contractData.agentId = agentId || property.agentId
  contractData.accountId = property.accountId
  contractData.propertyId = propertyId
  contractData.partnerId = partnerId
  if (
    assignmentSignatureMechanism === 'get_assignment' ||
    assignmentSignatureMechanism === 'add_assignment_and_print'
  )
    enabledEsigning = false
  else contractData.assignmentSignatureMechanism = assignmentSignatureMechanism
  contractData.status = enabledEsigning ? 'new' : 'upcoming'
  contractData.enabledEsigning = enabledEsigning
  contractData.templateId = templateId || ''
  if (actionType === 'add_and_print') {
    contractData.isSendAssignmentPdf = true
    contractData.status = 'upcoming'
    delete contractData.assignmentSignatureMechanism
  }
  if (size(addons)) contractData.addons = addons
  contractData.hasRentalContract = false
  contractData.hasBrokeringContract = hasBrokeringContract
  contractData.brokeringCommissionType = brokeringCommissionType
  contractData.brokeringCommissionAmount = brokeringCommissionAmount
  contractData.hasRentalManagementContract = hasRentalManagementContract
  contractData.rentalManagementCommissionType = rentalManagementCommissionType
  contractData.rentalManagementCommissionAmount =
    rentalManagementCommissionAmount
  contractData.rentalCommission = rentalCommission
  contractData.payoutTo = payoutTo
  contractData.monthlyPayoutDate = monthlyPayoutDate
  contractData.internalAssignmentId = internalAssignmentId
  contractData.assignmentFrom = assignmentFrom
  contractData.assignmentTo = assignmentTo
  contractData.representativeId = representativeId
  contractData.signDate = signDate
  contractData.listingInfo = listingInfo
  contractData.createdBy = userId
  contractData.rentalMeta = { status: 'new' }
  return contractData
}

export const getNewlyCreatedAssignmentData = async (contractInfo = {}) => {
  const agent = (await userHelper.getUserById(contractInfo.agentId)) || {}
  if (size(agent)) {
    agent.name = agent.profile?.name
    agent.avatarKey = userHelper.getAvatar(agent)
  }
  return {
    _id: contractInfo._id,
    agentInfo: {
      _id: agent._id,
      avatarKey: agent.avatarKey,
      name: agent.name
    },
    status: contractInfo.status,
    assignmentSerial: contractInfo.assignmentSerial,
    hasBrokeringContract: contractInfo.hasBrokeringContract,
    hasRentalManagementContract: contractInfo.hasRentalManagementContract,
    enabledEsigning: contractInfo.enabledEsigning
  }
}

export const validateDataForRegenerateContractEsigning = async (
  contractType,
  contractInfo = {}
) => {
  if (contractType === 'assignment') {
    if (contractInfo.status !== 'in_progress')
      throw new CustomError(
        400,
        'Regenerate of esigning can be possible in progress stage'
      )
    else if (!contractInfo.enabledEsigning)
      throw new CustomError(400, 'Esigning not enabled for this assignment')
  } else if (contractType === 'lease') {
    if (contractInfo.rentalMeta?.status !== 'in_progress')
      throw new CustomError(
        400,
        'Regenerate of esigning can be possible in progress stage'
      )
    else if (!contractInfo.rentalMeta?.enabledLeaseEsigning)
      throw new CustomError(400, 'Esigning not enabled for this lease')
  }
}

export const prepareUpdateDataForRegenerateContractEsigning = (
  body = {},
  contractInfo = {}
) => {
  const { contractType, signatureMechanism } = body
  const setData = {}
  const unsetData = {}
  const responseData = {
    _id: contractInfo._id
  }
  let event = 'send_assignment_esigning'

  if (contractType === 'assignment') {
    setData.assignmentContractPdfGenerated = false
    setData.assignmentSignatureMechanism = signatureMechanism
    setData.status = 'new'

    unsetData.agentAssignmentSigningStatus = ''
    unsetData.landlordAssignmentSigningStatus = ''
    unsetData.assignmentSigningMeta = ''

    responseData.status = 'new'
    responseData.assignmentContractPdfGenerated = false
    responseData.landlordSigned = false
    responseData.agentSigned = false
  } else {
    setData.leaseContractPdfGenerated = false
    if (
      contractInfo.rentalMeta?.depositType === 'deposit_account' &&
      signatureMechanism === 'bank_id'
    )
      setData['rentalMeta.enabledDepositAccount'] = true
    setData['rentalMeta.leaseSignatureMechanism'] = signatureMechanism

    unsetData['rentalMeta.landlordLeaseSigningStatus'] = ''
    unsetData['rentalMeta.tenantLeaseSigningStatus'] = ''
    unsetData['rentalMeta.leaseSigningMeta'] = ''
    unsetData['rentalMeta.isSendEsignNotify'] = ''
    unsetData['rentalMeta.depositAccountError'] = ''

    // Removing queue status for sending tenant lease e-signing notification if exists
    event = {
      $in: ['send_tenant_lease_esigning', 'send_landlord_lease_esigning']
    }

    responseData.tenantSigned = false
    responseData.leaseContractPdfGenerated = false
    responseData.landlordSigned = false
  }
  const updateData = {}
  if (size(setData)) updateData.$set = setData
  if (size(unsetData)) updateData.$unset = unsetData
  return {
    event,
    responseData,
    updateData
  }
}

export const prepareTerminateAssignmentUpdateData = async (body = {}) => {
  const { contractId, partnerId, propertyId, userId } = body
  const assignmentQuery = { _id: contractId, partnerId, propertyId }
  const contractInfo = await getAContract(assignmentQuery)

  if (!size(contractInfo)) throw new CustomError(404, 'Contract not found')

  if (
    !(
      ['upcoming', 'new', 'in_progress'].includes(contractInfo.status) &&
      !contractInfo.hasRentalContract
    )
  )
    throw new CustomError(404, 'Not possible to terminate this contract')

  const updateData = {
    finalSettlementStatus: 'new',
    status: 'closed'
  }

  if (contractInfo.status === 'upcoming') {
    updateData.cancelledBy = userId
    updateData.cancelledAt = new Date()
  }

  return updateData
}
export const prepareLogDataForTerminateAnAssignment = async (
  body,
  contract
) => {
  const visibility = ['property']
  const logData = pick(body, [
    'contractId',
    'propertyId',
    'partnerId',
    'tenantId'
  ])
  if (contract?.assignmentSerial) {
    logData.accountId = contract.accountId
    logData.meta = [
      { field: 'assignmentSerial', value: contract.assignmentSerial }
    ]
    visibility.push('account')
  }
  logData.action = 'terminate_assignment'
  logData.context = 'property'
  logData.visibility = visibility
  logData.createdBy = body.userId
  return logData
}

export const prepareLogDataForUpdateLeaseAddon = (body) => {
  const {
    addonId,
    addonInfo,
    contractType,
    fieldName,
    previousAddon,
    updatedContract,
    userId
  } = body
  const updatedAddon = find(
    updatedContract.addons,
    (addon) => addon.addonId === addonId
  )
  const action =
    contractType === 'lease'
      ? 'updated_lease_addon'
      : 'updated_assignment_addon'
  const metaData = []
  if (
    updatedContract.assignmentSerial &&
    action === 'updated_assignment_addon'
  ) {
    metaData.push({
      field: 'assignmentSerial',
      value: updatedContract.assignmentSerial
    })
  }
  if (updatedContract.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: updatedContract.leaseSerial })
  }
  metaData.push({ field: 'addonId', value: addonInfo.name })

  const changesArray = [
    {
      field: fieldName,
      type: 'text',
      oldText: previousAddon[fieldName],
      newText: updatedAddon[fieldName]
    }
  ]
  const visibility = ['property', 'account']
  const logData = {
    accountId: updatedContract.accountId,
    action,
    agentId: updatedContract.agentId,
    branchId: updatedContract.branchId,
    changes: changesArray,
    context: 'property',
    createdBy: userId,
    contractId: updatedContract._id,
    isChangeLog: true,
    partnerId: updatedContract.partnerId,
    propertyId: updatedContract.propertyId,
    meta: metaData
  }

  if (updatedContract.rentalMeta?.tenantId) {
    visibility.push('tenant')
    logData.tenantId = updatedContract.rentalMeta.tenantId
  }
  logData.visibility = visibility

  return logData
}

export const prepareQueryAndDataForUpdateLeaseAddon = async (
  body,
  previousContract
) => {
  const {
    addonId,
    addonInfo,
    contractId,
    contractType,
    fieldName,
    isRecurring,
    previousAddon,
    price
  } = body
  if (
    previousAddon.type === 'assignment' &&
    (!addonInfo.allowPriceEdit || contractType === 'lease')
  )
    throw new CustomError(400, 'Addon not allowed to update')
  else if (
    previousAddon.type === 'lease' &&
    (!addonInfo.allowPriceEdit || contractType === 'assignment')
  )
    throw new CustomError(400, 'Addon not allowed to update')

  let data = {}
  if (fieldName === 'price') {
    appHelper.checkRequiredFields(['price'], body)
    if (previousAddon.price !== price) {
      data = {
        'addons.$.price': price,
        'addons.$.total': price
      }
      if (
        previousContract.status !== 'upcoming' &&
        addonInfo.type === 'assignment'
      ) {
        const history = await prepareHistoryForContractAddonChange(
          previousAddon.price,
          previousContract,
          price
        )
        if (size(history)) data.history = history
      }
    }
  } else if (fieldName === 'isRecurring' && previousAddon.type === 'lease') {
    appHelper.checkRequiredFields(['isRecurring'], body)
    if (previousAddon.isRecurring !== isRecurring)
      data = { 'addons.$.isRecurring': isRecurring }
  }
  const query = {
    _id: contractId,
    addons: { $elemMatch: { addonId } }
  }
  if (!size(data)) throw new CustomError(400, 'Nothing to update')
  return { data, query }
}

export const prepareHistoryForContractAddonChange = async (
  previousAddonPrice,
  previousContract,
  price
) => {
  const { createdAt, partnerId } = previousContract
  const preparedHistory = []
  const names = []
  const previousAmount = getAddonsTotal(previousContract.addons)
  const updatedAmount = previousAmount - previousAddonPrice + price
  if (previousAmount !== updatedAmount) {
    const name = 'other'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    const oldUpdatedAt = previouslyUpdatedAt
      ? previouslyUpdatedAt
      : await appHelper.getActualDate(partnerId, false, createdAt)

    const otherChangeLog = {
      name,
      oldValue: previousAmount,
      oldUpdatedAt,
      newValue: updatedAmount,
      newUpdatedAt: await appHelper.getActualDate(partnerId, false)
    }
    names.push(name)
    preparedHistory.push(otherChangeLog)

    const totalIncomeChangeLog = await prepareTotalIncomeForAddonChange(
      previousContract,
      otherChangeLog
    )
    if (size(totalIncomeChangeLog)) {
      preparedHistory.push(totalIncomeChangeLog)
      names.push('total_income')
    }
  }

  let { history = [] } = previousContract || {}
  if (size(history)) {
    history = history.filter(({ name }) => !names.includes(name))
  }
  history = union(history, preparedHistory)
  return history
}

const prepareTotalIncomeForAddonChange = async (contract, otherChangeLog) => {
  let newValue = 0
  const { _id, createdAt, partnerId } = contract
  let isTotalIncomeChanged = false
  let isTotalIncomeIncreased = false
  let otherAmountDifference = 0
  const totalCommissionAmount =
    await invoiceHelper.getTotalCommissionAmountForBrokeringContract(_id)
  if (otherChangeLog) {
    isTotalIncomeChanged = true
    const oldValue = otherChangeLog.oldValue
      ? Number(otherChangeLog.oldValue)
      : 0
    newValue = otherChangeLog.newValue ? Number(otherChangeLog.newValue) : 0
    if (oldValue < newValue) {
      isTotalIncomeIncreased = true
      otherAmountDifference = newValue - oldValue
    } else {
      otherAmountDifference = oldValue - newValue
    }
  }

  if (isTotalIncomeChanged) {
    const name = 'total_income'
    const newTotalIncome = Number(totalCommissionAmount + newValue)
    const previousTotalIncome = isTotalIncomeIncreased
      ? newTotalIncome - otherAmountDifference
      : newTotalIncome + otherAmountDifference

    const previouslyUpdatedAt = getPreviouslyUpdatedDate(contract, name)
    const oldUpdatedAt = previouslyUpdatedAt
      ? previouslyUpdatedAt
      : await appHelper.getActualDate(partnerId, false, createdAt)

    const historyForTotalIncome = {
      name,
      newUpdatedAt: await appHelper.getActualDate(partnerId, false),
      newValue: newTotalIncome,
      oldUpdatedAt,
      oldValue: previousTotalIncome
    }
    return historyForTotalIncome
  }
}

export const prepareLogDataForAddAddonInContract = (body, contractInfo) => {
  const { contractType, newAddon, userId } = body
  const action =
    contractType === 'lease' ? 'added_lease_addon' : 'added_assignment_addon'

  const metaData = [{ field: 'addonId', value: newAddon._id }]
  if (contractType === 'lease') {
    metaData.push({ field: 'leaseSerial', value: contractInfo.leaseSerial })
  } else {
    metaData.push({
      field: 'assignmentSerial',
      value: contractInfo.assignmentSerial
    })
  }

  const logData = {
    accountId: contractInfo.accountId,
    action,
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    contractId: contractInfo._id,
    context: 'property',
    createdBy: userId,
    isChangeLog: false,
    partnerId: contractInfo.partnerId,
    propertyId: contractInfo.propertyId,
    meta: metaData
  }
  const visibility = ['property', 'account']
  if (contractInfo.rentalMeta?.tenantId) {
    visibility.push('tenant')
    logData.tenantId = contractInfo.rentalMeta.tenantId
  }
  logData.visibility = visibility
  return logData
}

export const validateAndPrepareDataForAddAddonInContract = async (body) => {
  const { addonId, contractId, partnerId, price } = body
  const previousContract = await getAContract({ _id: contractId, partnerId })
  if (!size(previousContract)) throw new CustomError(404, 'Contract not found')
  const contractType = previousContract.hasRentalContract
    ? 'lease'
    : 'assignment'
  const addons = previousContract.addons || []
  const isAddonExists = addons.find((addon) => addon.addonId === addonId)
  if (isAddonExists) throw new CustomError(400, 'Addon already exists')

  const newAddon = await addonHelper.getAddonById(addonId)
  if (!size(newAddon)) throw new CustomError(404, 'Addon not found')
  if (price && !newAddon.allowPriceEdit && price !== newAddon.price)
    throw new CustomError(400, 'Addon price is fixed')
  if (contractType !== newAddon.type) {
    throw new CustomError(400, 'Add addon not allowed')
  }

  body.newAddon = newAddon
  body.contractType = contractType

  const addonPrice = price ? price : newAddon.price
  const newAddonData = {
    addonId: newAddon._id,
    hasCommission: newAddon.enableCommission,
    isRecurring: newAddon.isRecurring,
    price: addonPrice,
    total: addonPrice,
    type: newAddon.type
  }
  const data = {
    $push: { addons: newAddonData }
  }
  if (
    previousContract.status !== 'upcoming' &&
    newAddon.type === 'assignment'
  ) {
    const history = await prepareHistoryForContractAddonChange(
      0,
      previousContract,
      addonPrice
    )
    if (size(history)) data.$set = { history }
  }
  return { data, newAddonData }
}

export const prepareEvictionCaseUpdateLogData = async (
  body = {},
  contract = {}
) => {
  const { userId, invoiceId, partnerId, status } = body
  const logData = {
    action: 'updated_eviction_case',
    agentId: contract.agentId,
    branchId: contract.branchId,
    accountId: contract.accountId,
    tenantId: contract.tenantId,
    changes: [
      {
        newText: status,
        type: 'text',
        field: 'evictionCases'
      }
    ],
    context: 'property',
    contractId: contract._id,
    createdBy: userId,
    invoiceId,
    isChangeLog: true,
    partnerId,
    propertyId: contract.propertyId
  }

  logData.visibility = logHelper.getLogVisibility(
    { context: 'property', collectionName: 'contract' },
    contract
  )

  const invoice = await invoiceHelper.getInvoice({ _id: invoiceId })
  const metaData = [
    { field: 'invoiceSerialId', value: invoice?.invoiceSerialId }
  ]
  if (contract.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: contract.leaseSerial })
  }
  logData.meta = metaData
  return logData
}
export const prepareUpdateAssignmentData = async (params) => {
  const {
    assignmentFrom,
    assignmentTo,
    availabilityEndDate,
    availabilityStartDate,
    contractId,
    depositAmount,
    internalAssignmentId,
    minimumStay,
    monthlyPayoutDate,
    monthlyRentAmount,
    partnerId,
    payoutTo
  } = params

  const contract = await getAContract(
    {
      _id: contractId,
      partnerId
    },
    null,
    [{ path: 'partner', populate: ['partnerSetting'] }]
  )

  if (!size(contract)) {
    throw new CustomError(404, 'Contract not found')
  }

  const { partnerSetting } = contract.partner || {}

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner not found')
  }

  if (contract.status === 'closed') {
    throw new CustomError(400, 'Contract not available for update')
  }
  const updateData = {}

  if (isBoolean(params.hasBrokeringContract)) {
    updateData.hasBrokeringContract = params.hasBrokeringContract
  }
  if (params.brokeringCommissionType) {
    updateData.brokeringCommissionType = params.brokeringCommissionType
  }
  if (isNumber(params.brokeringCommissionAmount)) {
    updateData.brokeringCommissionAmount = params.brokeringCommissionAmount
  }

  if (
    isNumber(params.rentalManagementCommissionAmount) ||
    params.rentalManagementCommissionType
  ) {
    if (!contract.hasRentalManagementContract) {
      throw new CustomError(
        400,
        'Management commission is disabled for this partner'
      )
    }

    if (isNumber(params.rentalManagementCommissionAmount)) {
      updateData.rentalManagementCommissionAmount =
        params.rentalManagementCommissionAmount
    }
    if (params.rentalManagementCommissionType) {
      updateData.rentalManagementCommissionType =
        params.rentalManagementCommissionType
    }
  }
  if (params.representativeId)
    updateData.representativeId = params.representativeId
  if (params.agentId) updateData.agentId = params.agentId

  if (internalAssignmentId) {
    if (!partnerSetting.assignmentSettings?.internalAssignmentId) {
      throw new CustomError(
        400,
        'Internal assignment id is disabled for this partner'
      )
    }
    updateData.internalAssignmentId = internalAssignmentId
  }

  if (payoutTo) {
    if (payoutTo.length !== 11) {
      throw new CustomError(400, 'Invalid payout account number')
    }
    updateData.payoutTo = payoutTo
  }

  if (assignmentFrom) {
    const assignmentFromDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      assignmentFrom
    )
    const minDate = (
      await appHelper.getActualDate(partnerSetting, true, contract.assignmentTo)
    )
      .subtract(6, 'months')
      .toDate()

    const maxDate = contract.assignmentTo
    if (!(minDate <= assignmentFromDate <= maxDate)) {
      throw new CustomError(400, 'Invalid assignment from date')
    }
    updateData.assignmentFrom = assignmentFrom
  }

  if (assignmentTo) {
    const assignmentToDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      assignmentTo
    )
    const maxDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        contract.assignmentFrom
      )
    ).add(6, 'months')

    const minDate = contract.assignmentFrom
    if (!(minDate <= assignmentToDate <= maxDate)) {
      throw new CustomError(400, 'Invalid assignment to date')
    }
    updateData.assignmentTo = assignmentTo
  }

  if (availabilityStartDate) {
    const startDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      availabilityStartDate
    )
    const endDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      contract?.listingInfo?.availabilityEndDate
    )
    if (startDate >= endDate) {
      throw new CustomError(
        400,
        'Availability start date should be less than end date'
      )
    }
    updateData['listingInfo.availabilityStartDate'] = startDate
  }

  if (availabilityEndDate) {
    const endDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      availabilityEndDate
    )
    const startDate = await appHelper.getActualDate(
      partnerSetting,
      false,
      contract?.listingInfo?.availabilityStartDate
    )
    if (endDate <= startDate) {
      throw new CustomError(
        400,
        'Availability end date should be greater than start date'
      )
    }
    updateData['listingInfo.availabilityEndDate'] = endDate
  }

  if (depositAmount) updateData['listingInfo.depositAmount'] = depositAmount
  if (minimumStay) updateData['listingInfo.minimumStay'] = minimumStay
  if (monthlyRentAmount) {
    updateData['listingInfo.monthlyRentAmount'] = monthlyRentAmount
  }

  if (monthlyPayoutDate) {
    if (monthlyPayoutDate <= 31) {
      updateData.monthlyPayoutDate = monthlyPayoutDate
    } else {
      throw new CustomError('Monthly payout date should be between 1 to 31')
    }
  }
  params.partnerSettings = partnerSetting
  params.previousContract = contract
  return updateData
}

const prepareLeaseDropdownQuery = (body) => {
  const { partnerId, query } = body
  const { accountId, propertyId, tenantId } = query

  const leaseQuery = {
    partnerId,
    propertyId,
    leaseSerial: { $exists: true },
    $or: [
      { status: { $ne: 'closed' } },
      {
        status: 'closed',
        isFinalSettlementDone: { $ne: true }
      }
    ]
  }
  if (accountId) leaseQuery.accountId = accountId
  if (tenantId) leaseQuery['rentalMeta.tenants.tenantId'] = tenantId
  return leaseQuery
}

const getLeaseInfo = async (query, options) => {
  const { sort, skip, limit } = options
  const leasePipeline = [
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $lookup: {
        from: 'tenants',
        localField: 'rentalMeta.tenantId',
        foreignField: '_id',
        as: 'mainTenantInfo'
      }
    },
    appHelper.getUnwindPipeline('mainTenantInfo'),
    {
      $addFields: {
        leaseSerial: {
          $cond: {
            if: { $ifNull: ['$mainTenantInfo', false] },
            then: {
              $concat: [
                'Lease ',
                { $toString: '$leaseSerial' },
                ' - ',
                '$mainTenantInfo.name'
              ]
            },
            else: {
              $concat: ['Lease ', { $toString: '$leaseSerial' }]
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 1,
        leaseSerial: 1
      }
    }
  ]

  const leaseInfo = (await ContractCollection.aggregate(leasePipeline)) || []
  return leaseInfo
}

export const queryLeaseDropdown = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body = {}, user = {} } = req
  const { query = {}, options = {} } = body
  appHelper.checkRequiredFields(['propertyId'], query)
  const { partnerId } = user
  body.partnerId = partnerId

  const preparedQuery = prepareLeaseDropdownQuery(body)
  const leaseInfo = await getLeaseInfo(preparedQuery, options)
  const totalDocuments = await countContracts(preparedQuery)

  return {
    data: leaseInfo,
    metaData: {
      filteredDocuments: leaseInfo.length,
      totalDocuments
    }
  }
}

export const validateUpdateContractPayoutPauseStatus = async (body) => {
  const { partnerId, contractId } = body
  const contract = await getAContract({
    _id: contractId,
    partnerId
  })
  if (!size(contract)) throw new CustomError(400, 'Contract not found')

  const payout = await payoutHelper.getPayout({
    contractId,
    partnerId,
    holdPayout: true
  })
  if (size(payout)) body.hasHoldPayout = true
  return body
}

export const getContractByAggregate = async (pipeline) => {
  const [contract = {}] = await ContractCollection.aggregate(pipeline)
  return contract
}

export const isAllTenantSignCompleted = (contract) => {
  if (!size(contract)) return false

  const { rentalMeta = {} } = contract
  const tenantLeaseSigningStatus = size(rentalMeta?.tenantLeaseSigningStatus)
    ? rentalMeta?.tenantLeaseSigningStatus
    : []

  const tenantsSignArray = size(tenantLeaseSigningStatus)
    ? map(tenantLeaseSigningStatus, 'signed')
    : []

  return !(size(tenantsSignArray) && includes(tenantsSignArray, false))
}

const getNextCpiMinDate = (lastCpiDate) => {
  if (
    moment().add(32, 'days').toDate() >
    moment(lastCpiDate).add(12, 'months').toDate()
  ) {
    return moment().add(32, 'days')
  }

  return moment(lastCpiDate).add(12, 'months')
}

export const prepareUpdateLeaseTermsData = async (body = {}) => {
  const { partner, partnerId, partnerSettings, previousContract, propertyId } =
    body
  const updateData = {}
  if (isBoolean(body.enabledJointlyLiable)) {
    updateData['rentalMeta.enabledJointlyLiable'] = body.enabledJointlyLiable
  }

  if (isBoolean(body.isVatEnable)) {
    updateData['rentalMeta.isVatEnable'] = body.isVatEnable
  }

  if (isBoolean(body.disableVipps)) {
    updateData['rentalMeta.disableVipps'] = body.disableVipps
  }

  if (isBoolean(body.disableCompello)) {
    updateData['rentalMeta.disableCompello'] = body.disableCompello
  }

  if (isBoolean(body.enabledAnnualStatement)) {
    const isEnableAnnualStatement =
      partnerSettings.notifications?.annualStatement
    if (!isEnableAnnualStatement) {
      throw new CustomError(
        400,
        'Annual statement is disabled for this partner'
      )
    }
    updateData['rentalMeta.enabledAnnualStatement'] =
      body.enabledAnnualStatement
  }

  const propertyInfo = await listingHelper.getListingById(propertyId)

  if (!size(propertyInfo)) throw new CustomError(400, 'Property not found')

  if (isNumber(body.monthlyRentAmount)) {
    updateData['rentalMeta.monthlyRentAmount'] =
      await appHelper.convertTo2Decimal(body.monthlyRentAmount)
  }

  if (body.internalLeaseId) {
    updateData['rentalMeta.internalLeaseId'] = body.internalLeaseId
  }

  if (body.dueDate) {
    const dueDate = body.dueDate
    if (body.isEnabledRecurringDueDate) {
      const isEnabledRecurringDueDate = !!(
        partner?.enableRecurringDueDate ||
        previousContract.rentalMeta?.isEnabledRecurringDueDate
      )
      if (!isEnabledRecurringDueDate) {
        throw new CustomError(
          400,
          'Recurring due date is not enabled for this contract'
        )
      }
    }

    if (dueDate <= 28) {
      updateData['rentalMeta.dueDate'] = dueDate
      updateData['rentalMeta.isEnabledRecurringDueDate'] =
        !!body.isEnabledRecurringDueDate
    } else {
      throw new CustomError(400, 'Recurring due date should be less then 28')
    }
  }

  const today = (
    await appHelper.getActualDate(partnerSettings, true, new Date())
  ).startOf('day')

  if (isBoolean(body.cpiEnabled) || body.lastCpiDate || body.nextCpiDate) {
    const isCPISettlementEnabled = partnerSettings.CPISettlement?.enabled
    if (!isCPISettlementEnabled) {
      throw new CustomError(
        400,
        'CPI settlement is not enabled for this partner'
      )
    }

    if (isBoolean(body.cpiEnabled)) {
      const contractRentalMeta = previousContract?.rentalMeta || {}
      const cpiSettlementDate = contractRentalMeta.signedAt
        ? contractRentalMeta.signedAt
        : contractRentalMeta.contractStartDate
      if (
        body.cpiEnabled &&
        size(contractRentalMeta) &&
        !(contractRentalMeta.lastCpiDate && contractRentalMeta.nextCpiDate)
      ) {
        updateData['rentalMeta.lastCpiDate'] = cpiSettlementDate

        const nextCpiMinDate = getNextCpiMinDate(cpiSettlementDate)

        if (new Date() > nextCpiMinDate.toDate()) {
          updateData['rentalMeta.nextCpiDate'] = moment()
            .add(32, 'days')
            .toDate()
        } else {
          updateData['rentalMeta.nextCpiDate'] = nextCpiMinDate.toDate()
        }
      }
      updateData['rentalMeta.cpiEnabled'] = body.cpiEnabled
    } else {
      if (!previousContract.rentalMeta?.cpiEnabled) {
        throw new CustomError(
          400,
          'CPI settlement is not enabled for this lease'
        )
      }

      if (body.lastCpiDate) {
        const rentalMeta = previousContract.rentalMeta
        const cpiSettlementDate =
          rentalMeta.signedAt || rentalMeta.contractStartDate

        const availabilityLastCPIDate =
          today > moment(cpiSettlementDate)
            ? today.format('YYYY-MM-DD')
            : moment(cpiSettlementDate).format('YYYY-MM-DD')

        const availabilityMinLastCPIDate =
          rentalMeta.signedAt < rentalMeta.contractStartDate
            ? rentalMeta.signedAt
            : rentalMeta.contractStartDate

        const lastCpiDate = moment(body.lastCpiDate).format('YYYY-MM-DD')
        console.log({
          lastCpiDate,
          availabilityLastCPIDate,
          cpiSettlementDate,
          availabilityMinLastCPIDate
        })
        if (availabilityLastCPIDate < lastCpiDate) {
          throw new CustomError(
            400,
            "Last CPI date can't be less then today's date or contract start date"
          )
        }

        if (
          moment(availabilityMinLastCPIDate).format('YYYY-MM-DD') > lastCpiDate
        ) {
          throw new CustomError(
            400,
            "Last CPI date can't be greater then signed date or contract start date"
          )
        }

        const lastCpiDateWithOneYear = moment(body.lastCpiDate).add(
          12,
          'months'
        )
        const nextCpiDate = moment(previousContract.rentalMeta?.nextCpiDate)
        if (lastCpiDateWithOneYear > nextCpiDate) {
          updateData['rentalMeta.nextCpiDate'] = lastCpiDateWithOneYear
        }
        updateData['rentalMeta.lastCpiDate'] = body.lastCpiDate
      } else if (body.nextCpiDate) {
        const nextCpiMinDate = getNextCpiMinDate(
          previousContract.rentalMeta?.lastCpiDate
        ).startOf('day')
        const nextCpiDate = moment(body.nextCpiDate).startOf('day')

        if (nextCpiMinDate > nextCpiDate) {
          throw new CustomError(
            400,
            "Next CPI date can't be less than one year after the last cpi date"
          )
        }
        updateData['rentalMeta.nextCpiDate'] = body.nextCpiDate
      }
    }
  }

  if (body.extendContractEndDate) {
    const hasUpcomingContract = await getAContract({
      partnerId,
      propertyId,
      status: 'upcoming',
      hasRentalContract: true
    })

    const upcomingContractStartDate =
      hasUpcomingContract?.rentalMeta?.contractStartDate
    let maximumAvailabilityEndDate = Infinity
    let minAvailabilityEndDate = ''

    if (upcomingContractStartDate && previousContract.status === 'active') {
      maximumAvailabilityEndDate = (
        await appHelper.getActualDate(
          partnerSettings,
          true,
          upcomingContractStartDate
        )
      )
        .subtract(1, 'days')
        .toDate()
    }

    const contractStartDate = previousContract?.rentalMeta?.contractStartDate
      ? await appHelper.getActualDate(
          partnerSettings,
          false,
          previousContract?.rentalMeta?.contractStartDate
        )
      : ''

    const today = (await appHelper.getActualDate(partnerSettings, true))
      .startOf('day')
      .toDate()

    if (contractStartDate < today) {
      minAvailabilityEndDate = today
    } else minAvailabilityEndDate = contractStartDate

    const endDate = await appHelper.getActualDate(
      partnerSettings,
      true,
      body.extendContractEndDate
    )
    console.log({
      minAvailabilityEndDate,
      maximumAvailabilityEndDate,
      endDate
    })
    if (
      endDate >= minAvailabilityEndDate &&
      endDate <= maximumAvailabilityEndDate
    ) {
      updateData['rentalMeta.contractEndDate'] = body.extendContractEndDate
    } else if (endDate < minAvailabilityEndDate) {
      throw new CustomError(
        400,
        'Contract end date should be greater than start date'
      )
    } else {
      throw new CustomError(
        400,
        'Contract end date should be less than the upcoming contract start date'
      )
    }
  }

  if (body.setMainTenantId) {
    if (body.setMainTenantId === previousContract.rentalMeta?.tenantId) {
      throw new CustomError(
        400,
        'Main tenant already exists with this tenant id'
      )
    }
    const tenantInfo = await tenantHelper.getATenant({
      _id: body.setMainTenantId,
      partnerId: body.partnerId
    })
    if (!size(tenantInfo)) {
      throw new CustomError(404, 'Tenant not found')
    }
    updateData['rentalMeta.tenantId'] = body.setMainTenantId
    updateData['rentalMeta.tenants'] = (
      previousContract.rentalMeta?.tenants || []
    ).filter((item) => item.tenantId !== previousContract.rentalMeta?.tenantId)
  }

  let tenantIds = []
  const oldTenantIds = previousContract.rentalMeta?.tenants || []
  let needToUpdateTenants = false
  if (size(body.removeTenantIds)) {
    if (size(oldTenantIds)) {
      tenantIds = oldTenantIds.filter(
        (tenant) => !body.removeTenantIds.includes(tenant.tenantId)
      )
      needToUpdateTenants = true
    }
  }

  if (size(body.newTenantIds)) {
    const totalTenants = await tenantHelper.countTenants({
      _id: { $in: body.newTenantIds },
      partnerId: body.partnerId
    })
    if (totalTenants !== body.newTenantIds.length) {
      throw new CustomError(404, 'Tenants not found')
    }
    tenantIds = oldTenantIds.filter(
      (tenant) => !body.newTenantIds.includes(tenant.tenantId)
    )
    for (const tenantId of body.newTenantIds) {
      tenantIds.push({
        tenantId
      })
    }
    needToUpdateTenants = true
  }

  if (needToUpdateTenants) {
    updateData['rentalMeta.tenants'] = tenantIds
  }
  return updateData
}

export const validateUpdateLeaseTermsData = async (body) => {
  const { contractId, partnerId, propertyId, leaseSerial } = body
  const contract = await getAContract(
    {
      _id: contractId,
      propertyId,
      partnerId
    },
    null,
    [{ path: 'partner', populate: ['partnerSetting'] }]
  )

  if (!size(contract)) throw new CustomError(400, 'Contract not found')
  if (
    contract.rentalMeta?.status === 'closed' ||
    contract.rentalMeta?.status === 'new'
  ) {
    throw new CustomError(400, 'Contract not available for update')
  }

  if (size(contract.rentalMetaHistory)) {
    const metaHistory = contract.rentalMetaHistory.find(
      (meta) => meta.leaseSerial === leaseSerial
    )
    body.rentalHistoryInfo = metaHistory || {}
    if (metaHistory?.status === 'closed') {
      throw new CustomError(400, 'Contract not available for update')
    }
  }

  const { partnerSetting } = contract.partner || {}

  if (!size(partnerSetting)) {
    throw new CustomError(400, 'Partner not found')
  }
  const partner = await partnerHelper.getAPartner({ _id: partnerId })

  if (!size(partner)) {
    throw new CustomError(400, 'Partner not found')
  }

  body.previousContract = contract
  body.partnerSettings = partnerSetting
  body.partner = partner
  return body
}

export const isOneDayOlderDate = (
  partnerSettings,
  previousDate,
  presentDate
) => {
  const oldDate = appHelper.getActualDate(partnerSettings, true, previousDate)
  const newDate = appHelper
    .getActualDate(partnerSettings, true, presentDate)
    .subtract(1, 'days')

  return oldDate.isBefore(newDate)
}

export const prepareContractAddHistoryChangeLogData = async (
  params = {},
  fieldNames = []
) => {
  const { partnerSettings, previousContract, updatedContract } = params
  const history = []
  const names = []
  const prevRentalMeta = previousContract.rentalMeta || {}
  const updatedRentalMeta = updatedContract.rentalMeta || {}
  const lastCreatedAt = await appHelper.getActualDate(
    partnerSettings,
    false,
    previousContract?.createdAt
  )
  const newUpdatedAt = await appHelper.getActualDate(partnerSettings, false)

  if (indexOf(fieldNames, 'assignmentFrom') !== -1 && params.assignmentFrom) {
    const name = 'assignmentFrom'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    names.push(name)
    history.push({
      name,
      oldValue: previousContract.assignmentFrom
        ? await appHelper.getActualDate(
            partnerSettings,
            false,
            previousContract.assignmentFrom
          )
        : '',
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: await appHelper.getActualDate(
        partnerSettings,
        false,
        updatedContract.assignmentFrom
      ),
      newUpdatedAt
    })
  }
  if (indexOf(fieldNames, 'assignmentTo') !== -1 && params.assignmentTo) {
    const name = 'assignmentTo'

    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    names.push(name)
    history.push({
      name,
      oldValue: previousContract.assignmentTo
        ? await appHelper.getActualDate(
            partnerSettings,
            false,
            previousContract.assignmentTo
          )
        : '',
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: await appHelper.getActualDate(
        partnerSettings,
        false,
        updatedContract.assignmentTo
      ),
      newUpdatedAt
    })
  }
  if (
    indexOf(fieldNames, 'listingInfo.monthlyRentAmount') !== -1 &&
    previousContract.listingInfo?.monthlyRentAmount !==
      updatedContract.listingInfo?.monthlyRentAmount
  ) {
    const name = 'assignmentMonthlyRentAmount'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    names.push(name)
    history.push({
      name,
      oldValue: previousContract.listingInfo?.monthlyRentAmount,
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: updatedContract.listingInfo?.monthlyRentAmount,
      newUpdatedAt
    })
  }

  if (
    indexOf(fieldNames, 'agentId') !== -1 &&
    previousContract.agentId !== updatedContract.agentId
  ) {
    const name = 'agentId'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    names.push(name)
    history.push({
      name,
      oldValue: previousContract.agentId,
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: updatedContract.agentId,
      newUpdatedAt
    })
  }

  if (
    indexOf(fieldNames, 'representativeId') !== -1 &&
    previousContract.representativeId !== updatedContract.representativeId
  ) {
    const name = 'representative'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    names.push(name)
    history.push({
      name,
      oldValue: previousContract.representativeId,
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: updatedContract.representativeId,
      newUpdatedAt
    })
  }

  if (indexOf(fieldNames, 'rentalMeta') !== -1) {
    const name = 'signedAt'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    const isPreviousDateOneDayOlder = isOneDayOlderDate(
      partnerSettings,
      updatedRentalMeta.signedAt,
      updatedRentalMeta.createdAt
    )
    if (!size(previouslyUpdatedAt) && isPreviousDateOneDayOlder) {
      names.push(name)

      const oldUpdatedAt = await appHelper.getActualDate(
        partnerSettings,
        false,
        updatedRentalMeta.signedAt
      )

      history.push({
        name,
        oldValue: '',
        oldUpdatedAt,
        newValue: '',
        newUpdatedAt
      })
    }
  }

  if (
    indexOf(fieldNames, 'rentalMeta.monthlyRentAmount') !== -1 &&
    prevRentalMeta.monthlyRentAmount !== updatedRentalMeta.monthlyRentAmount
  ) {
    const name = 'leaseMonthlyRentAmount'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    names.push(name)
    history.push({
      name,
      oldValue: prevRentalMeta?.monthlyRentAmount * 12,
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: updatedRentalMeta.monthlyRentAmount * 12,
      newUpdatedAt
    })
  }

  const prevTenants = prevRentalMeta?.tenants || []
  const currentTenants = updatedRentalMeta?.tenants || []

  if (
    indexOf(fieldNames, 'rentalMeta.tenants') !== -1 &&
    !isEqual(prevTenants, currentTenants)
  ) {
    const name = 'tenant'
    const previouslyUpdatedAt = getPreviouslyUpdatedDate(previousContract, name)
    let previousTenantIds = ''
    let newTenantIds = ''

    map(prevTenants, (tenant) => (previousTenantIds += tenant.tenantId + ','))
    map(currentTenants, (tenant) => (newTenantIds += tenant.tenantId + ','))
    names.push(name)
    history.push({
      name,
      oldValue: previousTenantIds,
      oldUpdatedAt: previouslyUpdatedAt ? previouslyUpdatedAt : lastCreatedAt,
      newValue: newTenantIds,
      newUpdatedAt
    })
  }
  return size(names) ? { names, history } : null
}

export const prepareJointlyLiableChangeLogData = async (params) => {
  const { previousContract, updatedContract, userId } = params
  const newText = !!updatedContract.rentalMeta?.enabledJointlyLiable
  const oldText = !!previousContract.rentalMeta?.enabledJointlyLiable

  const visibility = logHelper.getLogVisibility(
    { context: 'property', collectionName: 'contract' },
    updatedContract
  )

  const logData = {
    accountId: updatedContract.accountId,
    action: 'updated_jointly_liable',
    agentId: updatedContract.agentId,
    branchId: updatedContract.branchId,
    changes: [
      {
        field: 'enabledJointlyLiable',
        type: 'text',
        oldText,
        newText
      }
    ],
    context: 'property',
    createdBy: userId,
    contractId: updatedContract._id,
    isChangeLog: true,
    meta: [
      {
        field: 'leaseSerial',
        value: updatedContract.leaseSerial
      }
    ],
    partnerId: updatedContract.partnerId,
    propertyId: updatedContract.propertyId,
    visibility
  }
  return logData
}

export const prepareLeaseUpdateChangeLogData = (params) => {
  const {
    action = 'updated_lease',
    fieldName,
    updatedContract,
    previousContract,
    userId
  } = params
  let logData = {
    action,
    context: 'property',
    contractId: updatedContract._id,
    createdBy: userId,
    isChangeLog: true,
    meta: []
  }

  if (updatedContract.rentalMeta && previousContract.rentalMeta) {
    const newValue = updatedContract.rentalMeta[fieldName]
    const oldValue = previousContract.rentalMeta[fieldName]
    logData.tenantId = updatedContract.rentalMeta.tenantId
    const contractLogData = pick(updatedContract, [
      'accountId',
      'agentId',
      'branchId',
      'partnerId',
      'propertyId'
    ])
    const visibility = logHelper.getLogVisibility(
      { context: 'property' },
      updatedContract
    )

    logData = assign(logData, contractLogData) //extend log data.
    logData.visibility = union(visibility, ['tenant'])

    if (updatedContract.leaseSerial) {
      logData.meta = [
        { field: 'leaseSerial', value: updatedContract.leaseSerial }
      ]
    }

    if (
      updatedContract.assignmentSerial &&
      logData.action === 'updated_contract'
    ) {
      logData.meta.push({
        field: 'assignmentSerial',
        value: updatedContract.assignmentSerial
      })
    }

    let type = 'text'
    if (
      fieldName === 'lastCpiDate' ||
      fieldName === 'nextCpiDate' ||
      fieldName === 'contractEndDate'
    )
      type = 'date'

    if (params.CPIBasedIncrement) {
      logData.meta.push({ field: 'basedOnCPI', value: 'true' })
    }

    logData.changes = [
      {
        field: fieldName,
        type,
        oldText: oldValue,
        newText: newValue
      }
    ]
  }
  return logData
}

export const getLeaseUpdateFieldName = (params = {}) => {
  const { previousContract, updatedContract } = params
  const updatedRentalMeta = updatedContract?.rentalMeta || {}
  const previousRentalMeta = previousContract?.rentalMeta || {}

  let fieldName = ''
  const monthlyRent = updatedRentalMeta.monthlyRentAmount
  const previousMonthlyRent = previousRentalMeta.monthlyRentAmount

  if (updatedRentalMeta.disableVipps !== previousRentalMeta.disableVipps)
    fieldName = 'disableVipps'

  if (monthlyRent && monthlyRent !== previousMonthlyRent)
    fieldName = 'monthlyRentAmount'
  if (
    updatedRentalMeta.dueDate &&
    updatedRentalMeta.dueDate !== previousRentalMeta.dueDate
  )
    fieldName = 'dueDate'
  if (updatedRentalMeta.cpiEnabled !== previousRentalMeta.cpiEnabled)
    fieldName = 'cpiEnabled'
  if (
    updatedRentalMeta.nextCpiDate &&
    moment(updatedRentalMeta.nextCpiDate).format('YYYY-MM-DD') !==
      moment(previousRentalMeta.nextCpiDate).format('YYYY-MM-DD')
  )
    fieldName = 'nextCpiDate'
  if (
    updatedRentalMeta.lastCpiDate &&
    moment(updatedRentalMeta.lastCpiDate).format('YYYY-MM-DD') !==
      moment(previousRentalMeta.lastCpiDate).format('YYYY-MM-DD')
  )
    fieldName = 'lastCpiDate'
  if (
    updatedRentalMeta.internalLeaseId &&
    updatedRentalMeta.internalLeaseId !== previousRentalMeta.internalLeaseId
  )
    fieldName = 'internalLeaseId'

  if (
    updatedRentalMeta.status &&
    updatedRentalMeta.status !== previousRentalMeta.status
  )
    fieldName = 'status'
  return fieldName
}

export const getLeaseTenantsUpdateChanges = (params) => {
  const { previousContract, updatedContract } = params
  const updatedRentalMeta = updatedContract?.rentalMeta || {}
  const previousRentalMeta = previousContract?.rentalMeta || {}
  const updateData = {}

  if (
    updatedRentalMeta.tenantId &&
    previousRentalMeta.tenantId &&
    updatedRentalMeta.tenantId !== previousRentalMeta.tenantId
  ) {
    updateData.mainTenantId = updatedRentalMeta.tenantId
    updateData.action = 'updated_main_tenant'
  }

  if (!isEqual(updatedRentalMeta.tenants, previousRentalMeta.tenants)) {
    updateData.tenants = updatedRentalMeta.tenants

    if (size(updatedRentalMeta.tenants) < size(previousRentalMeta.tenants)) {
      const updatedTenants = updatedRentalMeta.tenants || []
      const removedTenant = previousRentalMeta.tenants.filter(
        ({ tenantId: oldTenantId }) =>
          !updatedTenants.some(({ tenantId }) => oldTenantId === tenantId)
      )
      if (size(removedTenant) && removedTenant[0].tenantId) {
        updateData.tenantId = removedTenant[0].tenantId
      }
      updateData.action = 'removed_lease_tenant'
      updateData.oldTenants = removedTenant
    } else if (size(updatedRentalMeta.tenants)) {
      const previousTenants = previousRentalMeta.tenants || []
      const newTenants = updatedRentalMeta.tenants.filter(
        ({ tenantId: oldTenantId }) =>
          !previousTenants.some(({ tenantId }) => oldTenantId === tenantId)
      )
      if (size(newTenants) && newTenants[0].tenantId) {
        updateData.tenantId = newTenants[0].tenantId
        updateData.action = 'added_lease_tenant'
      }
    }
  }
  return updateData
}

export const prepareLeaseTenantsUpdateLogData = (body, params) => {
  const { previousContract, updatedContract, userId } = body
  const logData = pick(updatedContract, [
    'accountId',
    'agentId',
    'branchId',
    'partnerId',
    'propertyId',
    'tenantId'
  ])
  logData.meta = [{ field: 'leaseSerial', value: updatedContract.leaseSerial }]

  if (params.action === 'updated_main_tenant') {
    logData.isChangeLog = true
    logData.changes = [
      {
        field: 'tenantId',
        type: 'foreignKey',
        newText: previousContract.rentalMeta?.tenantId,
        oldText: updatedContract.rentalMeta?.tenantId
      }
    ]
  } else if (
    params.action === 'removed_lease_tenant' ||
    params.action === 'added_lease_tenant'
  ) {
    logData.isChangeLog = false
    logData.meta.push({
      field: 'tenantId',
      value: params.tenantId
    })
  }

  logData.contractId = updatedContract._id
  logData.action = params.action
  logData.createdBy = userId
  logData.context = params.context || 'property'
  logData.visibility = logHelper.getLogVisibility(
    {
      collectionName: 'contract',
      context: 'property',
      tenantId: params.tenantId
    },
    updatedContract
  )
  return logData
}

export const getRequiredDataAndValidateLeaseCreateData = async (body = {}) => {
  const {
    contractId,
    contractStartDate,
    contractEndDate,
    partnerId,
    propertyId
  } = body

  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  if (!size(partnerInfo)) throw new CustomError(404, 'Partner not found')
  let contract = {}

  if (partnerInfo.accountType === 'broker') {
    if (!contractId) {
      throw new CustomError(400, 'Required contractId')
    }
    contract = await getAContract({ _id: contractId, propertyId, partnerId })
    if (!size(contract)) {
      throw new CustomError(404, 'Contract not found')
    }
    if (contract.status !== 'upcoming' || contract?.rentalMeta?.tenantId) {
      throw new CustomError(400, 'Contract is not available for create lease')
    }
  } else delete body?.contractId

  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner settings not found')
  }
  const isDirectPartner = partnerInfo.accountType === 'direct'

  if (!isDirectPartner) {
    const { afterFirstMonthACNo, enabled, firstMonthACNo } =
      partnerSetting?.bankPayment || {}

    if (!(enabled || afterFirstMonthACNo || firstMonthACNo)) {
      throw new CustomError(
        400,
        'Please setup the bank account settings first!'
      )
    }
  }
  const startDate = contractStartDate
    ? await appHelper.getActualDate(partnerSetting, false, contractStartDate)
    : ''
  const endDate = contractEndDate
    ? await appHelper.getActualDate(partnerSetting, false, contractEndDate)
    : ''
  const query = { partnerId, propertyId, status: 'active' }

  if (!isDirectPartner) query._id = { $ne: contractId }

  if (startDate && endDate) {
    query['$or'] = [
      { 'rentalMeta.contractStartDate': { $gte: startDate, $lte: endDate } },
      {
        'rentalMeta.contractStartDate': { $lte: startDate },
        'rentalMeta.contractEndDate': { $gte: endDate }
      },
      {
        'rentalMeta.contractStartDate': { $lte: startDate },
        'rentalMeta.contractEndDate': { $gte: startDate, $lte: endDate }
      },
      { 'rentalMeta.contractEndDate': { $gte: startDate, $lte: endDate } }
    ]
  } else if (startDate && !endDate) {
    query['$or'] = [
      {
        'rentalMeta.contractStartDate': { $lte: startDate },
        'rentalMeta.contractEndDate': { $gte: startDate }
      },
      { 'rentalMeta.contractStartDate': { $gte: startDate } }
    ]
  }

  // Find contract info
  const contractInfo = await getAContract(query)
  if (size(contractInfo)) {
    throw new CustomError(
      400,
      'Contract already exists for this duration ' + startDate
    )
  }

  const today = (
    await appHelper.getActualDate(partnerSetting, true, new Date())
  )
    .endOf('day')
    .toDate()

  const leaseEsigningEnable = partnerSetting.leaseSetting?.enableEsignLease
  if (!leaseEsigningEnable && body.leaseType === 'esigning') {
    throw new CustomError(400, 'E-signing is disabled for this partner')
  }
  if (leaseEsigningEnable && !body.leaseType) {
    body.leaseType = 'lease_and_print'
  }

  if (
    (leaseEsigningEnable || body.leaseType) &&
    !size(body.leaseEsigningPdfContent)
  ) {
    throw new CustomError(400, 'Lease pdf content is required')
  }

  if (
    body.depositType === 'deposit_account' ||
    body.depositType === 'deposit_insurance'
  ) {
    if (body.leaseSignatureMechanism !== 'bank_id') {
      throw new CustomError(
        400,
        'Signature mechanism should be Bank ID if lease type is deposit account or insurance'
      )
    }

    if (body.leaseType !== 'esigning') {
      throw new CustomError(
        400,
        'Deposit account or insurance lease type should be e-signing'
      )
    }
  }

  let status = 'active'
  if (leaseEsigningEnable && body.leaseType === 'esigning') {
    body.enabledLeaseEsigning = true
    status = 'in_progress'
  } else {
    if (startDate && startDate > today) status = 'upcoming' //if start date is future, the status should be upcoming
    delete body.leaseSignatureMechanism
  }

  const upcomingContract =
    (await contractHelper.getAContract({
      propertyId,
      partnerId,
      status: 'upcoming'
    })) || {}

  if (
    isDirectPartner &&
    status === 'upcoming' &&
    size(upcomingContract) &&
    (!size(upcomingContract.rentalMeta) ||
      (upcomingContract.rentalMeta && upcomingContract.rentalMeta.tenantId))
  ) {
    throw new CustomError(400, 'Already upcoming contract exists.')
  }

  // if (isDirectPartner && !size(upcomingContract)) status = 'upcoming'

  const tenantInfo = await tenantHelper.getATenant({
    partnerId,
    _id: body.tenantId
  })

  if (!size(tenantInfo)) {
    throw new CustomError(
      404,
      'Tenant not found, Please provide a valid tenant'
    )
  }

  if (size(contract)) body.contractInfo = contract
  else if (size(upcomingContract)) {
    body.contractInfo = upcomingContract
    body.contractId = upcomingContract._id
  }
  body.status = status
  body.upcomingContract = upcomingContract
  body.partnerSetting = partnerSetting
  body.partnerInfo = partnerInfo
  body.isDirectPartner = isDirectPartner
  return body
}

export const prepareRentalMetaDataForAddLease = async (body) => {
  const { contractInfo, partnerInfo, partnerSetting, isDirectPartner, userId } =
    body

  const preparedData = pick(body, [
    'contractStartDate',
    'depositType',
    'disableVipps',
    'disableCompello',
    'dueDate',
    'enabledNotification',
    'internalLeaseId',
    'invoiceFrequency',
    'isEnabledRecurringDueDate',
    'isMovedIn',
    'isVatEnable',
    'minimumStay',
    'monthlyRentAmount',
    'movingInDate',
    'noticeInEffect',
    'noticePeriod',
    'signedAt',
    'templateId',
    'tenantId'
  ])

  const today = (
    await appHelper.getActualDate(partnerSetting, true, new Date())
  ).startOf('day')
  const todayWithOneYear = (
    await appHelper.getActualDate(partnerSetting, true, new Date())
  )
    .startOf('day')
    .add(1, 'years')

  const todayWithOneYearSubtract = (
    await appHelper.getActualDate(partnerSetting, true, new Date())
  )
    .startOf('day')
    .subtract(1, 'years')

  const contractStartDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    body.contractStartDate
  )

  if (contractInfo.hasRentalContract) {
    throw new CustomError(400, 'Lease already created for this contract')
  }

  if (body.contractStartDate) {
    preparedData.contractStartDate = body.contractStartDate
  }

  if (body.contractEndDate) {
    if (moment(body.contractEndDate).isBefore(body.contractStartDate)) {
      throw new CustomError(
        400,
        'Contract end date can’t before contract start date'
      )
    }
    preparedData.contractEndDate = body.contractEndDate
  }
  if (body.invoiceStartFrom) {
    let contractEndDate = body.contractEndDate
      ? (
          await appHelper.getActualDate(
            partnerSetting,
            true,
            body.contractEndDate
          )
        ).startOf('day')
      : todayWithOneYear

    let newContractStartDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        body.contractStartDate
      )
    ).startOf('day')

    if (newContractStartDate.toDate() < todayWithOneYearSubtract.toDate()) {
      newContractStartDate = todayWithOneYearSubtract
    }
    if (contractEndDate.toDate() > todayWithOneYear.toDate()) {
      contractEndDate = todayWithOneYear
    }

    const invoiceStartFromDate = (
      await appHelper.getActualDate(partnerSetting, true, body.invoiceStartFrom)
    ).startOf('day')

    if (
      (invoiceStartFromDate.isBefore(newContractStartDate, 'month') ||
        invoiceStartFromDate.isAfter(contractEndDate, 'month')) &&
      !invoiceStartFromDate.isSame(newContractStartDate)
    ) {
      throw new CustomError(
        400,
        'Invoice start from can’t before contract end date and can’t after contract start date'
      )
    }
    preparedData.invoiceStartFrom = body.invoiceStartFrom
  }

  if (body.dueDate > 28) {
    throw new CustomError(400, 'Invoice due date should be under 28')
  }

  if (body.firstInvoiceDueDate) {
    const firstInvoiceDueDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        body.firstInvoiceDueDate
      )
    ).format('YYYY-MM-DD')

    if (firstInvoiceDueDate < today.format('YYYY-MM-DD')) {
      throw new CustomError(
        400,
        'First invoice due date can’t before today date'
      )
    }
    preparedData.firstInvoiceDueDate = body.firstInvoiceDueDate
  }

  if (body.cpiEnabled) {
    if (!body.lastCpiDate) {
      throw new CustomError(400, 'Required lastCpiDate')
    }
    if (!body.nextCpiDate) {
      throw new CustomError(400, 'Required nextCpiDate')
    }

    const lastCpiDate = (
      await appHelper.getActualDate(partnerSetting, true, body.lastCpiDate)
    ).format('YYYY-MM-DD')

    const contractStartDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        body.contractStartDate
      )
    ).format('YYYY-MM-DD')

    const signedDate = (
      await appHelper.getActualDate(partnerSetting, true, body.signedAt)
    ).format('YYYY-MM-DD')

    // Start last cpi date validation
    let availabilityLastCPIMinDate = ''
    let availabilityLastCPIMaxDate = ''

    if (body.signedAt && signedDate < contractStartDate)
      availabilityLastCPIMinDate = signedDate
    else availabilityLastCPIMinDate = contractStartDate

    if (today.format('YYYY-MM-DD') > contractStartDate)
      availabilityLastCPIMaxDate = today.format('YYYY-MM-DD')
    else availabilityLastCPIMaxDate = contractStartDate

    if (availabilityLastCPIMinDate > lastCpiDate) {
      throw new CustomError(400, "CPI can't be before rent start date")
    }
    if (availabilityLastCPIMaxDate < lastCpiDate) {
      throw new CustomError(400, "CPI date can't be greater then today's date")
    }

    // Start next cpi date validation
    const todayWithOneMonth = (
      await appHelper.getActualDate(partnerSetting, true)
    )
      .startOf('day')
      .add(32, 'days')
      .format('YYYY-MM-DD')

    const newNextCpiDate = (
      await appHelper.getActualDate(partnerSetting, true, body.nextCpiDate)
    ).format('YYYY-MM-DD')
    const lastCpiWithOneYear = (
      await appHelper.getActualDate(partnerSetting, true, body.lastCpiDate)
    )
      .add(12, 'months')
      .format('YYYY-MM-DD')

    console.log({ newNextCpiDate, lastCpiWithOneYear })

    if (
      todayWithOneMonth >
      (await appHelper.getActualDate(partnerSetting, true, body.lastCpiDate))
        .add(12, 'months')
        .format('YYYY-MM-DD')
    ) {
      if (todayWithOneMonth > newNextCpiDate) {
        throw new CustomError(400, "Next CPI date can't be less than one year")
      }
    } else if (lastCpiWithOneYear > newNextCpiDate) {
      throw new CustomError(400, "Next CPI date can't be less than one year")
    }

    preparedData.lastCpiDate = body.lastCpiDate
    preparedData.cpiEnabled = body.cpiEnabled
    preparedData.nextCpiDate = body.nextCpiDate
  }

  if (body.isMovedIn && !body.movingInDate) {
    preparedData.movingInDate = today
  }

  if (isBoolean(body.enabledDepositAccount)) {
    preparedData.enabledDepositAccount = body.enabledDepositAccount
  }

  if (body.noticePeriod > 12) {
    throw new CustomError(400, 'Invalid invoice notice period')
  }

  if (partnerSetting.leaseSetting?.internalLeaseId && body.internalLeaseId) {
    preparedData.internalLeaseId = body.internalLeaseId
  }

  if (isDirectPartner && body.invoiceAccountNumber) {
    preparedData.invoiceAccountNumber = body.invoiceAccountNumber
  }

  if (size(body.tenants)) {
    const totalTenants = await tenantHelper.countTenants({
      _id: { $in: body.tenants },
      partnerId: body.partnerId
    })
    if (totalTenants !== body.tenants.length) {
      throw new CustomError(404, 'Tenants not found')
    }
    preparedData.tenants = body.tenants.map((item) => ({
      tenantId: item
    }))
  }

  if (isBoolean(body.enabledJointlyLiable)) {
    preparedData.enabledJointlyLiable = body.enabledJointlyLiable
    if (!preparedData.tenants && body.enabledJointlyLiable) {
      throw new CustomError(
        400,
        'Jointly liable allow only for multiple tenants'
      )
    }
  }

  if (body.depositType !== 'no_deposit' && !body.depositAmount) {
    throw new CustomError(400, 'Deposit amount is required')
  }
  preparedData.depositAmount = body.depositAmount

  const { enableCreditRating, enableDepositAccount } = partnerInfo

  if (body.depositType === 'deposit_insurance') {
    if (!enableCreditRating)
      throw new CustomError(
        400,
        'Deposit insurance is disabled for this partner'
      )
    if (contractStartDate.isBefore(today)) {
      throw new CustomError(400, "Rent start date can't be older than today!")
    }
    preparedData.depositInsuranceAmount = round(body.depositAmount * 0.16)
    if (preparedData.tenants) preparedData.enabledJointlyLiable = true
  }

  if (
    body.depositType === 'deposit_account' ||
    body.enabledJointDepositAccount
  ) {
    if (!enableDepositAccount) {
      throw new CustomError(400, 'Deposit account is disabled for this partner')
    }
    if (body.enabledJointDepositAccount) {
      if (body.depositType !== 'deposit_account') {
        throw new CustomError(
          400,
          'Join deposit account allow only for deposit account'
        )
      }
      if (!preparedData.tenants) {
        throw new CustomError(
          400,
          'Join deposit account allow only for multiple tenants'
        )
      }
    }
  }
  if (
    body.depositType === 'deposit_account' &&
    isBoolean(body.enabledJointDepositAccount)
  ) {
    preparedData.enabledJointDepositAccount = body.enabledJointDepositAccount
  }

  if (body.leaseSignatureMechanism) {
    preparedData.leaseSignatureMechanism = body.leaseSignatureMechanism
  }

  if (isBoolean(body.enabledLeaseEsigning)) {
    preparedData.enabledLeaseEsigning = body.enabledLeaseEsigning
  }

  if (!contractInfo.rentalMeta?.invoiceCalculation) {
    const invoiceCalculation = partnerSetting.invoiceCalculation
      ? partnerSetting.invoiceCalculation
      : 'prorated_first_month'
    preparedData.invoiceCalculation = invoiceCalculation
  }
  if (body.isEnabledRecurringDueDate && !partnerInfo?.enableRecurringDueDate) {
    throw new CustomError(
      400,
      'Recurring due date is disabled for this partner'
    )
  }

  preparedData.leaseEndDate = body.contractEndDate
  preparedData.leaseStartDate = body.contractStartDate
  preparedData.status = body.status
  preparedData.createdAt = new Date()
  preparedData.createdBy = userId

  const isSendWelcomeLease = partnerSetting?.notifications?.sentWelcomeLease

  if (
    isSendWelcomeLease &&
    body?.enabledNotification &&
    preparedData.status !== 'in_progress'
  ) {
    preparedData.leaseWelcomeEmailSentInProgress = true
  }
  return preparedData
}

export const validateTenantsBusinessLandlord = async (body = {}) => {
  if (
    body.leaseType === 'esigning' &&
    body.depositAmount &&
    body.depositType !== 'no_deposit' &&
    body.leaseSignatureMechanism === 'bank_id'
  ) {
    const tenantsBusinessLandlord = await getTenantsBusinessLandlord({
      partnerInfo: body.partnerInfo,
      companyInfo: body.partnerSetting?.companyInfo,
      tenantIds: body.tenants,
      accountId: body.contractInfo?.accountId
    })
    if (size(tenantsBusinessLandlord)) return tenantsBusinessLandlord
    if (
      body.partnerInfo?.enableDepositAccount &&
      body.depositType === 'deposit_account'
    ) {
      body.enabledDepositAccount = true
      body.enabledJointDepositAccount = !!body.enabledJointDepositAccount
    }
  } else body.enabledJointDepositAccount = false
  return []
}

const getOrganizationOrPersonAvatar = (accountInfo = {}) => {
  const { type, person, organization } = accountInfo
  if (type === 'person' && size(person)) return accountInfo.person.getAvatar()
  else if (size(organization))
    return accountInfo.organization.getLogo(undefined, false)
  return ''
}

const getTenantsBusinessLandlord = async (params = {}) => {
  const { accountId, companyInfo = {}, partnerInfo, tenantIds } = params
  const { accountType, _id: partnerId, name } = partnerInfo
  const tenantParams = {
    query: {
      _id: { $in: tenantIds },
      partnerId
    },
    projection: '_id name userId'
  }
  const tenantsInfo = await tenantHelper.getTenantsWithProjection(tenantParams)
  const allTenantsListOrBusinessLandlord = []
  for (const tenantInfo of tenantsInfo) {
    const tenantSSN = tenantInfo.user?.profile?.norwegianNationalIdentification
    const isInvalidSSN = !(
      tenantSSN &&
      tenantSSN.length === 11 &&
      validate(tenantSSN)
    )
    const avatarUrl =
      appHelper.getCDNDomain() +
      '/' +
      (tenantInfo?.user?.profile?.avatarKey ||
        'assets/default-image/user-primary.png')
    if (!tenantSSN) {
      allTenantsListOrBusinessLandlord.push({
        _id: tenantInfo._id,
        avatarUrl,
        name: tenantInfo.name,
        userId: tenantInfo.userId
      })
    } else if (isInvalidSSN) {
      allTenantsListOrBusinessLandlord.push({
        _id: tenantInfo._id,
        avatarUrl,
        isInvalidSSN,
        name: tenantInfo.name,
        ssn: tenantSSN,
        userId: tenantInfo.userId
      })
    }
  }
  // For account
  let type = 'partner'
  let orgId = ''
  let businessLandlordId = ''
  let organizationId = ''
  let businessLandlordName = ''
  let avatarUrl = ''
  if (accountType === 'direct' && accountId) {
    type = 'account'
    const accountInfo = await accountHelper.getAnAccount(
      {
        _id: accountId,
        partnerId
      },
      undefined,
      ['person', 'organization']
    )
    if (!size(accountInfo)) throw new CustomError(404, 'Account not found')
    avatarUrl = await getOrganizationOrPersonAvatar(accountInfo)
    orgId = accountInfo.organization?.orgId
    businessLandlordId = accountId
    organizationId = accountInfo.organizationId
    businessLandlordName = accountInfo.name
  } else {
    orgId = companyInfo.organizationId
    businessLandlordId = partnerId
    businessLandlordName = companyInfo.companyName || name
  }
  if (!/^[0-9]{9}$/.test(orgId)) {
    allTenantsListOrBusinessLandlord.push({
      _id: businessLandlordId,
      avatarUrl,
      isInvalidOrgId: true,
      name: businessLandlordName,
      organizationId,
      partnerId,
      type
    })
  }
  return allTenantsListOrBusinessLandlord
}

export const prepareLogDataForNewLease = (params = {}) => {
  const { updatedContract: contract, userId = 'SYSTEM' } = params
  const visibility = logHelper.getLogVisibility(
    { context: 'property' },
    contract
  )

  const logData = {
    accountId: contract.accountId,
    action: 'added_lease',
    agentId: contract.agentId,
    branchId: contract.branchId,
    context: 'property',
    createdBy: userId,
    contractId: contract._id,
    isChangeLog: false,
    meta: [
      {
        field: 'leaseSerial',
        value: contract.leaseSerial
      },
      { field: 'status', value: contract.rentalMeta?.status }
    ],
    partnerId: contract.partnerId,
    propertyId: contract.propertyId,
    visibility
  }
  return logData
}

export const prepareLogDataForAddAddonInNewLease = (params) => {
  const { addons, contractInfo, userId } = params
  const metaData = [
    { field: 'leaseSerial', value: contractInfo.leaseSerial },
    { field: 'addonId', value: addons[0].addonId }
  ]

  const visibility = logHelper.getLogVisibility(
    { context: 'property', collectionName: 'contract' },
    contractInfo
  )
  const logData = {
    accountId: contractInfo.accountId,
    action: 'added_lease_addon',
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    context: 'property',
    contractId: contractInfo._id,
    createdBy: userId,
    isChangeLog: false,
    meta: metaData,
    partnerId: contractInfo.partnerId,
    propertyId: contractInfo.propertyId,
    visibility
  }
  return logData
}

export const prepareLogDataForUpdateLease = (params) => {
  const { fieldName, contractInfo, previousContract, userId } = params

  const metaData = []
  let type = 'text'
  if (contractInfo.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: contractInfo.leaseSerial })
  }

  if (
    fieldName === 'lastCpiDate' ||
    fieldName === 'nextCpiDate' ||
    fieldName === 'contractEndDate'
  )
    type = 'date'

  if (params.CPIBasedIncrement) {
    metaData.push({ field: 'basedOnCPI', value: 'true' })
  }

  const newValue = contractInfo.rentalMeta[fieldName]
  const oldValue = previousContract.rentalMeta[fieldName]

  const changes = [
    {
      field: fieldName,
      type,
      oldText: oldValue,
      newText: newValue
    }
  ]

  const visibility = logHelper.getLogVisibility(
    { context: 'property' },
    contractInfo
  )

  const logData = {
    accountId: contractInfo.accountId,
    action: 'updated_lease',
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    context: 'property',
    contractId: contractInfo._id,
    changes,
    createdBy: userId,
    isChangeLog: true,
    meta: metaData,
    partnerId: contractInfo.partnerId,
    propertyId: contractInfo.propertyId,
    visibility: union(visibility, ['tenant'])
  }
  logData.tenantId = contractInfo.rentalMeta?.tenantId
  return logData
}

export const prepareLogDataForJointDepositAccountChangeLog = (params) => {
  const { contractInfo, previousContract, userId } = params
  const metaData = []
  if (contractInfo?.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: contractInfo.leaseSerial })
  }

  const newValue = !!contractInfo.rentalMeta?.enabledJointDepositAccount
  const oldValue = !!previousContract.rentalMeta?.enabledJointDepositAccount

  const changes = [
    {
      field: 'enabledJointDepositAccount',
      type: 'text',
      oldText: oldValue,
      newText: newValue
    }
  ]

  const visibility = logHelper.getLogVisibility(
    { context: 'property', collectionName: 'contract' },
    contractInfo
  )

  const logData = {
    accountId: contractInfo.accountId,
    action: 'updated_joint_deposit_account',
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    context: 'property',
    contractId: contractInfo._id,
    changes,
    createdBy: userId,
    isChangeLog: true,
    meta: metaData,
    partnerId: contractInfo.partnerId,
    propertyId: contractInfo.propertyId,
    visibility
  }
  return logData
}

export const prepareLeaseTerminateData = async (params) => {
  const { contractId, partnerId, propertyId } = params
  const contractInfo = await getAContract(
    {
      _id: contractId,
      partnerId,
      propertyId
    },
    null,
    [{ path: 'partner', populate: ['partnerSetting'] }]
  )

  if (!size(contractInfo)) {
    throw new CustomError(404, 'Contract not found')
  }

  if (contractInfo.status !== 'active') {
    throw new CustomError(400, 'Lease is not available for terminate')
  }

  const { partnerSetting } = contractInfo.partner || {}

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner setting not found')
  }

  const todayDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    new Date()
  )
  const contractEndDate = await appHelper.getActualDate(
    partnerSetting,
    true,
    params.contractEndDate
  )

  const upcomingContract = await getAContract({
    partnerId,
    propertyId,
    status: 'upcoming'
  })

  if (
    upcomingContract &&
    upcomingContract.hasRentalContract &&
    upcomingContract?.rentalMeta?.contractStartDate
  ) {
    const leaseEndMaxDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        upcomingContract.rentalMeta.contractStartDate
      )
    ).subtract(1, 'days')
    if (leaseEndMaxDate.isBefore(contractEndDate)) {
      throw new CustomError(
        404,
        'Terminate date should not be greater than upcoming contract start date'
      )
    }
  }
  const leaseEndMinDate = (
    await appHelper.getActualDate(
      partnerSetting,
      true,
      contractInfo.rentalMeta?.contractStartDate
    )
  ).startOf('day')
  if (leaseEndMinDate.isAfter(contractEndDate, 'day')) {
    throw new CustomError(
      404,
      'Terminate date should not be less than contract start date'
    )
  }

  let status = ''
  if (contractEndDate <= todayDate) status = 'closed'

  return {
    cancelledBy: params.userId,
    contractEndDate: params.contractEndDate,
    contractId,
    contractInfo,
    creditWholeInvoice: params.creditWholeInvoice,
    enabledNotification: params.enabledNotification,
    partnerSetting,
    propertyId,
    status,
    terminateComment: params.terminateComment,
    terminateReason: params.terminateReason,
    terminatedBy: params.terminatedBy,
    terminatedByUserId: params.userId,
    todayDate,
    upcomingContract
  }
}

export const prepareContractDataForUpdateStatus = async (params = {}) => {
  let { contractInfo } = params
  const {
    contractId,
    cancelledBy = 'SYSTEM',
    status,
    todayDate,
    propertyId,
    partnerId
  } = params

  if (!contractInfo) {
    contractInfo = await getAContract({
      _id: contractId,
      partnerId,
      propertyId
    })
  }
  if (!size(contractInfo)) throw new CustomError(404, 'Contract not found')

  const upcomingContract =
    contractInfo.status === 'upcoming' ? contractInfo : null

  const rentalMeta = contractInfo?.rentalMeta
  const enabledNotification = rentalMeta?.enabledNotification ? true : false
  let isCreateInvoice = false
  let updateData = {}

  if (status) {
    updateData = {
      status,
      'rentalMeta.status': status,
      finalSettlementStatus: 'new'
    }

    if (status === 'active') updateData.hasRentalContract = true
    if (status === 'closed') {
      if (upcomingContract && !upcomingContract.hasRentalContract) {
        delete updateData['rentalMeta.status']
        updateData.cancelledBy = cancelledBy
        updateData.cancelledAt = todayDate
      }

      if (upcomingContract && upcomingContract.hasRentalContract) {
        delete updateData.status
        updateData['rentalMeta.cancelledBy'] = cancelledBy
        updateData['rentalMeta.cancelledAt'] = todayDate
      }
    }
  }

  if (params && params.contractEndDate) {
    updateData['rentalMeta.contractEndDate'] = params.contractEndDate
    if (upcomingContract && upcomingContract.hasRentalContract) {
      delete updateData.status
    }

    if (params.terminatedBy)
      updateData['rentalMeta.terminatedBy'] = params.terminatedBy

    if (params.terminateComment)
      updateData['rentalMeta.terminateComments'] = params.terminateComment

    if (params.terminateReason)
      updateData['rentalMeta.terminateReasons'] = params.terminateReason

    if (params.creditWholeInvoice)
      updateData['rentalMeta.creditWholeInvoice'] = params.creditWholeInvoice

    if (params.terminatedByUserId)
      updateData['terminatedByUserId'] = params.terminatedByUserId

    updateData['rentalMeta.enabledNotification'] = enabledNotification

    const contractEndDate = rentalMeta?.contractEndDate
      ? await appHelper.getActualDate(
          partnerId,
          true,
          rentalMeta.contractEndDate
        )
      : ''
    const paramsContractEndDate = params.contractEndDate
      ? await appHelper.getActualDate(partnerId, true, params.contractEndDate)
      : ''
    const contractStartDate = await appHelper.getActualDate(
      partnerId,
      true,
      rentalMeta?.contractStartDate
    )
    const contractStatus = status ? status : rentalMeta?.status
    console.log({
      contractStatus,
      contractEndDate,
      contractStartDate,
      paramsContractEndDate
    })
    if (
      contractEndDate &&
      contractStartDate &&
      contractStartDate < paramsContractEndDate &&
      contractEndDate !== paramsContractEndDate &&
      contractStatus &&
      ['active', 'upcoming'].includes(contractStatus)
    ) {
      isCreateInvoice = true
    }
  }

  return { enabledNotification, isCreateInvoice, updateData }
}

export const prepareLogDataForTerminateLease = (params = {}) => {
  const metaData = []
  if (params.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: params.leaseSerial })
  }

  if (params.rentalMeta?.contractEndDate) {
    metaData.push({
      field: 'endDate',
      value: params.rentalMeta.contractEndDate
    })
  }

  if (params.rentalMeta?.terminateComment) {
    metaData.push({
      field: 'comment',
      value: params.rentalMeta.terminateComment
    })
  }
  const visibility = logHelper.getLogVisibility({ context: 'property' }, params)

  const logData = {
    accountId: params.accountId,
    action: 'terminate_lease',
    agentId: params.agentId,
    branchId: params.branchId,
    context: 'property',
    contractId: params._id,
    createdBy: params.userId,
    isChangeLog: false,
    meta: metaData,
    partnerId: params.partnerId,
    propertyId: params.propertyId,
    visibility
  }

  return logData
}

export const isCreateCreditNoteInvoice = async (
  updatedContract = {},
  previousContract = {},
  partnerSetting
) => {
  const newDocContractEndDate = updatedContract.rentalMeta?.contractEndDate
    ? await appHelper.getActualDate(
        partnerSetting,
        false,
        updatedContract.rentalMeta.contractEndDate
      )
    : ''
  const previousDocContractEndDate = previousContract.rentalMeta
    ?.contractEndDate
    ? await appHelper.getActualDate(
        partnerSetting,
        false,
        previousContract.rentalMeta.contractEndDate
      )
    : ''

  if (
    newDocContractEndDate &&
    ((previousDocContractEndDate &&
      newDocContractEndDate < previousDocContractEndDate) ||
      (!previousDocContractEndDate && updatedContract.terminatedByUserId))
  )
    return true

  return false
}

export const prepareContractCreateData = (params) => {
  const { accountId, agentId, branchId, createdBy, partnerId, propertyId } =
    params
  return {
    accountId,
    agentId,
    branchId,
    brokeringMeta: {
      status: 'new'
    },
    createdAt: new Date(),
    createdBy,
    hasBrokeringContract: false,
    hasRentalContract: false,
    hasRentalManagementContract: false,
    partnerId,
    propertyId,
    rentalManagementMeta: {
      status: 'new'
    },
    rentalMeta: {
      status: 'new'
    },
    status: 'upcoming'
  }
}

export const hasActiveOrUpcomingContract = (
  activeContract,
  upcomingContract
) => {
  if (!size(activeContract) && !size(upcomingContract)) return false
  if (
    activeContract?.hasRentalContract ||
    upcomingContract?.hasRentalContract
  ) {
    return true
  }
  return false
}

const getPropertyInventoryIssues = async (query) => {
  const { contractId } = query

  const pipeline = [
    {
      $match: {
        contractId
      }
    },
    {
      $addFields: {
        propertyItemIssues: {
          $filter: {
            input: { $ifNull: ['$inventory.furniture', []] },
            as: 'propertyItem',
            cond: {
              $eq: ['$$propertyItem.status', 'issues']
            }
          }
        }
      }
    },
    {
      $match: {
        'propertyItemIssues.0': {
          $exists: true
        }
      }
    },
    {
      $project: {
        _id: 1,
        propertyItemIssues: 1,
        files: '$inventory.files'
      }
    }
  ]
  const inventoryItemIssues =
    (await PropertyItemCollection.aggregate(pipeline)) || []
  return inventoryItemIssues
}

const getPropertyRoomIssues = async (query) => {
  const { contractId } = query
  const pipeline = [
    {
      $match: {
        contractId
      }
    },
    {
      $addFields: {
        issues: {
          $filter: {
            input: { $ifNull: ['$items', []] },
            as: 'roomItem',
            cond: {
              $eq: ['$$roomItem.status', 'issues']
            }
          }
        }
      }
    },
    {
      $match: {
        'issues.0': { $exists: true }
      }
    },
    {
      $project: {
        _id: 1,
        name: 1,
        type: 1,
        files: 1,
        issues: 1
      }
    }
  ]
  const propertyRoomIssues =
    (await PropertyRoomCollection.aggregate(pipeline)) || []
  return propertyRoomIssues
}

export const gettingAllIssuesForTenantLease = async (req) => {
  const { body = {}, user = {} } = req
  const { query } = body
  checkRequiredFields(['userId', 'partnerId'], user)
  checkRequiredFields(['contractId'], query)

  const inventoryItems = await getPropertyInventoryIssues(query)
  const roomItems = await getPropertyRoomIssues(query)

  return {
    inventoryIssues: inventoryItems,
    roomIssues: roomItems
  }
}

export const prepareCancelLeaseUpdateData = async (params) => {
  const { contractInfo, isBrokerPartner, partnerSetting, userId } = params

  const today = await appHelper.getActualDate(partnerSetting, true)
  const rentalMetaHistory = contractInfo.rentalMetaHistory || []

  const updateData = {
    $set: {
      leaseContractPdfGenerated: false,
      'rentalMeta.status': 'closed',
      'rentalMeta.cancelled': true,
      'rentalMeta.cancelledAt': today,
      'rentalMeta.cancelledBy': userId,
      status: 'closed'
    }
  }

  if (isBrokerPartner && contractInfo) {
    const contractData = contractInfo.toObject()
    const rentalMeta = contractData.rentalMeta
    rentalMetaHistory.push({
      ...rentalMeta,
      cancelled: true,
      cancelledAt: today,
      cancelledBy: userId,
      status: 'closed',
      hasRentalContract: false,
      leaseContractPdfGenerated: false,
      leaseSerial: contractData?.leaseSerial
    })
    updateData.$set = {
      hasRentalContract: false,
      rentalMeta: { status: 'new' },
      rentalMetaHistory
    }
    if (size(contractInfo.addons)) {
      updateData['$unset'] = { addons: 1 }
    }
  }
  return updateData
}

export const prepareLogDataForCancelLease = (params) => {
  const { signerId, contractInfo, signerType, userId = 'SYSTEM' } = params
  const logData = {
    accountId: contractInfo.accountId,
    action: 'cancelled_lease',
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    context: 'property',
    contractId: contractInfo._id,
    createdBy: userId,
    meta: [{ field: 'leaseSerial', value: contractInfo?.leaseSerial }],
    partnerId: contractInfo.partnerId,
    propertyId: contractInfo.propertyId,
    visibility: ['property', 'account']
  }

  if (signerId && signerType) {
    logData.reason = `wrong_ssn_of_${signerType}`
    if (signerType === 'tenant') logData.tenantId = signerId
  }

  return logData
}

export const isDepositInsuranceProcessEnabled = async (partnerId) => {
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  return !!partnerInfo?.enableCreditRating
}

export const isEnabledSendEsignNotification = async (
  previousContract = {},
  updatedContract = {}
) => {
  const rentalMeta = updatedContract.rentalMeta
  if (!size(rentalMeta)) return false

  const {
    depositType,
    enabledJointDepositAccount,
    enabledJointlyLiable,
    isSendEsignNotify,
    partnerId,
    tenantId,
    tenantLeaseSigningStatus = []
  } = rentalMeta

  const tenantsIds =
    enabledJointlyLiable && !enabledJointDepositAccount
      ? map(tenantLeaseSigningStatus, 'tenantId')
      : [tenantId]

  const isNotAttachedIdfyFile = find(
    tenantLeaseSigningStatus,
    (tenantInfo) =>
      indexOf(tenantsIds, tenantInfo.tenantId) !== -1 &&
      !tenantInfo.idfyAttachmentId
  )

  const isNotAttachedIdfyFileForTenants = find(
    tenantLeaseSigningStatus,
    (tenantInfo) => tenantInfo && !tenantInfo.idfyAttachmentId
  )

  const hasDepositInsurance = !!(
    depositType === 'deposit_insurance' &&
    !size(isNotAttachedIdfyFileForTenants) &&
    (await isDepositInsuranceProcessEnabled(partnerId))
  )

  const isEnabledDepositAccountProcess =
    await depositAccountHelper.isEnabledDepositAccountProcess({
      partnerInfoOrId: partnerId,
      contractInfoOrId: updatedContract
    })

  const enabledSendEsignNotification =
    (size(previousContract.rentalMeta?.tenantLeaseSigningStatus) &&
      size(tenantLeaseSigningStatus) &&
      isEnabledDepositAccountProcess &&
      !isNotAttachedIdfyFile &&
      !isSendEsignNotify) ||
    (size(previousContract.rentalMeta?.tenantLeaseSigningStatus) &&
      size(tenantLeaseSigningStatus) &&
      hasDepositInsurance &&
      !isSendEsignNotify)

  if (
    (!size(previousContract.rentalMeta?.tenantLeaseSigningStatus) &&
      size(tenantLeaseSigningStatus) &&
      depositType === 'no_deposit') ||
    enabledSendEsignNotification
  ) {
    return { enabledSendEsignNotification }
  }
  return false
}

export const getRequiredDataForCancelLease = async (params) => {
  const { contractId, partnerId, propertyId, userId, signerId, signerType } =
    params
  const query = {
    _id: contractId,
    partnerId,
    propertyId
  }

  const contractInfo = await getAContract(query, null, [
    { path: 'partner', populate: ['partnerSetting'] }
  ])

  if (!size(contractInfo)) {
    throw new CustomError(404, 'Contract not found')
  }

  if (!size(contractInfo?.partner))
    throw new CustomError(404, 'Partner not found')

  const { accountType, partnerSetting } = contractInfo?.partner || {}

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner settings not found')
  }

  if (
    !(
      contractInfo.status === 'upcoming' ||
      contractInfo.status === 'in_progress'
    ) ||
    contractInfo.rentalMeta?.status === 'new'
  ) {
    throw new CustomError(400, 'Lease is not available for cancel') // Don't change this error msg it's using in lease lambda
  }

  const isBrokerPartner = accountType === 'broker'

  return {
    contractInfo,
    isBrokerPartner,
    partnerSetting,
    signerId,
    signerType,
    userId
  }
}

export const queryPreviewInvoices = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { roles = [] } = user
  if (!roles.includes('lambda_manager')) {
    appHelper.validatePartnerAppRequestData(req)
  }
  appHelper.checkRequiredFields(['partnerId'], body)

  const {
    addons = [],
    contractStartDate,
    contractEndDate,
    dueDate,
    firstInvoiceDueDate,
    invoiceFrequency,
    invoiceStartFrom,
    monthlyRentAmount,
    partnerId,
    propertyId,
    tenantId
  } = body
  const partner = await partnerHelper.getAPartner(
    { _id: partnerId },
    undefined,
    ['partnerSetting']
  )
  const contract = {
    addons,
    propertyId,
    partnerId,
    partner,
    partnerSetting: partner?.partnerSetting
  }

  contract['rentalMeta'] = {
    tenantId,
    contractStartDate,
    contractEndDate,
    monthlyRentAmount,
    dueDate,
    firstInvoiceDueDate,
    invoiceStartFrom,
    invoiceFrequency
  }

  return await invoiceService.createManualInvoices({
    contract,
    returnPreview: true
  })
}

export const contractWithFileForDepositAccount = async (contractId) => {
  const contractData = await ContractCollection.aggregate()
    .match({ _id: contractId })
    .addFields({
      signers: {
        $cond: {
          if: {
            $in: [
              '$rentalMeta.enabledJointDepositAccount',
              [null, '', undefined, false]
            ]
          },
          then: '$rentalMeta.tenantLeaseSigningStatus',
          else: {
            $filter: {
              input: '$rentalMeta.tenantLeaseSigningStatus',
              as: 'item',
              cond: { $eq: ['$$item.tenantId', '$rentalMeta.tenantId'] }
            }
          }
        }
      }
    })
    .unwind({
      path: '$signers',
      preserveNullAndEmptyArrays: true
    })
    .lookup({
      from: 'files',
      localField: 'signers.attachmentFileId',
      foreignField: '_id',
      as: 'files'
    })
    .unwind({
      path: '$files',
      preserveNullAndEmptyArrays: true
    })
    .addFields({
      'files.signer': '$signers'
    })
    .group({
      _id: '$_id',
      partnerId: {
        $first: '$partnerId'
      },
      rentalMeta: {
        $first: '$rentalMeta'
      },
      files: {
        $push: '$files'
      },
      signers: {
        $push: '$signers'
      }
    })
  console.log('Contract data inside', contractData)
  return contractData || []
}

const prepareCheckContractDurationQuery = async (params) => {
  const { contractId, partnerId, propertyId } = params
  const { contractEndDate, contractStartDate } = params

  const contractInfo =
    (await getAContract(
      {
        _id: contractId,
        partnerId,
        propertyId
      },
      null,
      [{ path: 'partner', populate: ['partnerSetting'] }]
    )) || {}
  if (!size(contractInfo))
    throw new CustomError(404, 'Could not found contractInfo')

  const { accountType, partnerSetting } = contractInfo?.partner || {}
  const isBrokerPartner = accountType === 'broker'

  const startDate = contractStartDate
    ? await appHelper.getActualDate(partnerSetting, false, contractStartDate)
    : ''
  const endDate = contractEndDate
    ? await appHelper.getActualDate(partnerSetting, false, contractEndDate)
    : ''

  const query = {
    partnerId,
    propertyId,
    status: 'active',
    hasRentalContract: true
  }

  if (isBrokerPartner) query._id = { $ne: contractId }

  if (startDate && endDate) {
    query['$or'] = [
      { 'rentalMeta.contractStartDate': { $gte: startDate, $lte: endDate } },
      {
        'rentalMeta.contractStartDate': { $lte: startDate },
        'rentalMeta.contractEndDate': { $gte: endDate }
      },
      {
        'rentalMeta.contractStartDate': { $lte: startDate },
        'rentalMeta.contractEndDate': { $gte: startDate, $lte: endDate }
      },
      { 'rentalMeta.contractEndDate': { $gte: startDate, $lte: endDate } }
    ]
  } else if (startDate && !endDate) {
    query['$or'] = [
      {
        'rentalMeta.contractStartDate': { $lte: startDate },
        'rentalMeta.contractEndDate': { $gte: startDate }
      },
      { 'rentalMeta.contractStartDate': { $gte: startDate } }
    ]
  }
  return query
}

export const checkContractDuration = async (req) => {
  const { body } = req
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'propertyId'])
  const query = await prepareCheckContractDurationQuery(body)
  const contractInfo = await getAContract(query)
  return !size(contractInfo)
}

const getPipelineOverviewListForTenantInfo = () => [
  {
    $lookup: {
      from: 'tenants',
      localField: 'rentalMeta.tenantId',
      foreignField: '_id',
      pipeline: [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'userInfo'
          }
        },
        appHelper.getUnwindPipeline('userInfo'),
        {
          $project: {
            _id: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey'),
            name: 1,
            serial: 1
          }
        }
      ],
      as: 'tenantInfo'
    }
  },
  appHelper.getUnwindPipeline('tenantInfo')
]

const getPipelineOverviewListForDepositStatus = () => [
  {
    $lookup: {
      from: 'deposit_insurance',
      localField: 'rentalMeta.depositInsuranceId',
      foreignField: '_id',
      as: 'depositInsurance'
    }
  },
  {
    $unwind: {
      path: '$depositInsurance',
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $lookup: {
      from: 'deposit_accounts',
      localField: '_id',
      foreignField: 'contractId',
      as: 'depositAccountInfo'
    }
  },
  {
    $addFields: {
      depositAccountInfo: {
        $filter: {
          input: '$depositAccountInfo',
          as: 'depositAccount',
          cond: {
            $and: [
              {
                $eq: ['$$depositAccount.tenantId', '$rentalMeta.tenantId']
              },
              {
                $gte: [
                  '$$depositAccount.totalPaymentAmount',
                  '$$depositAccount.depositAmount'
                ]
              }
            ]
          }
        }
      }
    }
  },
  {
    $addFields: {
      depositStatus: {
        $switch: {
          branches: [
            {
              case: { $eq: ['$rentalMeta.depositType', 'deposit_insurance'] },
              then: {
                $cond: [
                  {
                    $in: [
                      '$depositInsurance.status',
                      ['paid', 'sent', 'failed', 'registered']
                    ]
                  },
                  'paid',
                  'not paid'
                ]
              }
            },
            {
              case: { $eq: ['$rentalMeta.depositType', 'deposit_account'] },
              then: {
                $cond: [
                  {
                    $gt: [
                      { $size: { $ifNull: ['$depositAccountInfo', []] } },
                      0
                    ]
                  },
                  'paid',
                  'not paid'
                ]
              }
            }
          ],
          default: null
        }
      }
    }
  }
]

export const getPipelineOverviewListForInvoiceStatus = () => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'contractId',
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                {
                  $eq: ['$isFirstInvoice', true]
                },
                {
                  $eq: ['$status', 'paid']
                }
              ]
            }
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  {
    $addFields: {
      invoiceStatus: {
        $cond: [
          { $gt: [{ $size: { $ifNull: ['$invoiceInfo', []] } }, 0] },
          'paid',
          'not paid'
        ]
      }
    }
  }
]

const getJanitorOverviewList = async (query, options = {}) => {
  const { limit, skip, sort } = options
  const overviewList = await ContractCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getPipelineOverviewListForTenantInfo(),
    ...getPipelineOverviewListForDepositStatus(),
    ...getPipelineOverviewListForInvoiceStatus(),
    {
      $project: {
        _id: 1,
        tenantInfo: {
          _id: 1,
          name: 1,
          avatarKey: 1,
          serial: 1
        },
        depositType: '$rentalMeta.depositType',
        depositStatus: 1,
        firstInvoiceStatus: '$invoiceStatus'
      }
    }
  ])
  return overviewList
}

export const janitorOverviewList = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const { query = {}, options = {}, partnerId } = body
  appHelper.checkRequiredFields(['propertyId'], query)
  appHelper.validateSortForQuery(options.sort)
  const { contractId, propertyId } = query
  const preparedQuery = {
    partnerId,
    propertyId,
    'rentalMeta.status': { $ne: 'new' }
  }
  if (contractId) preparedQuery._id = contractId
  const overviewList = await getJanitorOverviewList(preparedQuery, options)
  const totalDocuments = await contractHelper.countContracts({
    partnerId,
    propertyId
  })
  const filteredDocuments = await contractHelper.countContracts(preparedQuery)
  return {
    data: overviewList,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

export const getCPIInMonthDate = async ({ contractInfo, partnerSettings }) => {
  const settings = await SettingCollection.findOne({})

  if (!settings || !size(settings?.cpiDataSet)) {
    throw new CustomError(404, 'Settings not found')
  }

  const cpiDataSet = settings.cpiDataSet

  const nextCpiDate =
    (
      await appHelper.getActualDate(
        partnerSettings,
        true,
        contractInfo.rentalMeta.nextCpiDate
      )
    ).format('YYYY-MM') || ''

  let till = nextCpiDate ? nextCpiDate.split('-').join('M') : ''
  const TidIndex = cpiDataSet?.dataset?.dimension?.Tid?.category?.index || {}

  if (size(TidIndex) && !TidIndex[till]) {
    const tidIndexKeys = Object.keys(TidIndex)
    if (size(tidIndexKeys)) till = last(tidIndexKeys)
  }
  const formatTill = till ? till.split('M').join('-') : ''

  const cpiInMonth = moment(formatTill, 'YYYY-MM').toDate()

  return (await appHelper.getActualDate(partnerSettings, true, cpiInMonth))
    .endOf('day')
    .toDate()
}

export const pipelineForSetIsMovingInOut = () => [
  {
    $addFields: {
      isMovingIn: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', true] },
                  { $ifNull: ['$movingInItem', false] },
                  {
                    $not: {
                      $eq: ['$movingInItem.isEsigningInitiate', true]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'in'] }
                ]
              },
              then: false
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'out'] },
                  {
                    $not: {
                      $eq: ['$movingInItem.isEsigningInitiate', true]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $not: { $ifNull: ['$processedItem', false] } },
                  {
                    $not: {
                      $eq: ['$movingInItem.isEsigningInitiate', true]
                    }
                  }
                ]
              },
              then: true
            }
          ],
          default: false
        }
      },
      isMovingOut: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', true] },
                  {
                    $not: {
                      $eq: ['$movingOutItem.isEsigningInitiate', true]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'in'] },
                  {
                    $not: {
                      $eq: ['$movingOutItem.isEsigningInitiate', true]
                    }
                  }
                ]
              },
              then: true
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $eq: ['$processedItem.type', 'out'] }
                ]
              },
              then: false
            },
            {
              case: {
                $and: [
                  { $eq: ['$isSoonEnding', false] },
                  { $not: { $ifNull: ['$processedItem', false] } }
                ]
              },
              then: false
            }
          ],
          default: false
        }
      }
    }
  }
]

export const propertyMovingProtocolList = async (params) => {
  const { compareDate, options, partnerId, selectedMonth, todayDate } = params
  const { limit, skip, sort } = options
  const movingProtocolList = await ContractCollection.aggregate([
    {
      $match: {
        partnerId,
        status: {
          $ne: 'closed'
        },
        $or: [
          {
            $and: [
              {
                'rentalMeta.contractStartDate': {
                  $gte: todayDate
                }
              },
              {
                'rentalMeta.contractStartDate': {
                  $lte: compareDate
                }
              }
            ]
          },
          {
            $and: [
              {
                'rentalMeta.contractEndDate': {
                  $gte: todayDate
                }
              },
              {
                'rentalMeta.contractEndDate': {
                  $lte: compareDate
                }
              }
            ]
          }
        ]
      }
    },
    {
      $sort: sort
    },
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        pipeline: [
          ...appHelper.getListingFirstImageUrl('$images'),
          {
            $project: {
              _id: 1,
              'location.name': 1,
              'location.city': 1,
              'location.country': 1,
              'location.postalCode': 1,
              'location.streetNumber': 1,
              apartmentId: 1,
              propertyTypeId: 1,
              listingTypeId: 1,
              serial: 1,
              imageUrl: 1
            }
          }
        ],
        as: 'listingInfo'
      }
    },
    {
      $unwind: {
        path: '$listingInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'property_items',
        localField: '_id',
        foreignField: 'contractId',
        pipeline: [
          {
            $match: {
              isEsigningInitiate: { $ne: true }
            }
          },
          {
            $sort: {
              createdAt: -1
            }
          }
        ],
        as: 'propertyItems'
      }
    },
    {
      $addFields: {
        isSoonEnding: {
          $cond: [
            {
              $and: [
                { $eq: ['$status', 'active'] },
                { $ifNull: ['$rentalMeta.contractEndDate', false] },
                { $lte: ['$rentalMeta.contractEndDate', selectedMonth] }
              ]
            },
            true,
            false
          ]
        },
        movingInItem: {
          $first: {
            $filter: {
              input: { $ifNull: ['$propertyItems', []] },
              as: 'propertyItem',
              cond: {
                $and: [
                  { $eq: ['$$propertyItem.type', 'in'] }
                  // { $not: { $eq: ['$$propertyItem.moveInCompleted', true] } }
                ]
              }
            }
          }
        },
        movingOutItem: {
          $first: {
            $filter: {
              input: { $ifNull: ['$propertyItems', []] },
              as: 'propertyItem',
              cond: {
                $and: [
                  { $eq: ['$$propertyItem.type', 'out'] }
                  // { $not: { $eq: ['$$propertyItem.moveOutCompleted', true] } }
                ]
              }
            }
          }
        },
        processedItem: {
          $first: {
            $filter: {
              input: { $ifNull: ['$propertyItems', []] },
              as: 'propertyItem',
              cond: {
                $or: [
                  {
                    $ifNull: ['$$propertyItem.moveInCompleted', false]
                  },
                  {
                    $ifNull: ['$$propertyItem.moveOutCompleted', false]
                  }
                ]
              }
            }
          }
        }
      }
    },
    ...pipelineForSetIsMovingInOut(),
    {
      $project: {
        createdAt: 1,
        movingInContractId: {
          $cond: [
            {
              $and: [
                '$isMovingIn',
                {
                  $cond: [{ $ifNull: ['$movingInItem', false] }, false, true]
                }
              ]
            },
            '$_id',
            '$$REMOVE'
          ]
        },
        movingOutContractId: {
          $cond: [
            {
              $and: [
                '$isMovingOut',
                {
                  $cond: [{ $ifNull: ['$movingOutItem', false] }, false, true]
                }
              ]
            },
            '$_id',
            '$$REMOVE'
          ]
        },
        propertyInfo: {
          _id: '$listingInfo._id',
          apartmentId: '$listingInfo.apartmentId',
          listingTypeId: '$listingInfo.listingTypeId',
          propertyTypeId: '$listingInfo.propertyTypeId',
          serial: '$listingInfo.serial',
          imageUrl: '$listingInfo.imageUrl',
          location: {
            name: '$listingInfo.location.name',
            city: '$listingInfo.location.city',
            country: '$listingInfo.location.country',
            streetNumber: '$listingInfo.location.streetNumber',
            postalCode: '$listingInfo.location.postalCode'
          }
        },
        propertyId: 1
      }
    },
    {
      $group: {
        _id: '$propertyId',
        movingIn: {
          $push: '$movingInContractId'
        },
        movingOut: {
          $push: '$movingOutContractId'
        },
        propertyInfo: {
          $first: '$propertyInfo'
        },
        createdAt: {
          $first: '$createdAt'
        }
      }
    },
    {
      $match: {
        $expr: {
          $or: [
            {
              $gt: [{ $size: '$movingIn' }, 0]
            },
            {
              $gt: [{ $size: '$movingOut' }, 0]
            }
          ]
        }
      }
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ])
  return movingProtocolList
}

export const janitorDashboardMovingInOutList = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const { partnerId } = body

  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const isEnableMovingInOutProtocol =
    partnerSettings?.propertySettings?.movingInOutProtocol || false
  if (!isEnableMovingInOutProtocol)
    return {
      data: [],
      metaData: {
        totalDocuments: 0,
        filteredDocuments: 0
      }
    }

  const soonEndingMonths =
    partnerSettings?.propertySettings?.soonEndingMonths || 4
  const timezone = partnerSettings?.dateTimeSettings?.timezone || ''
  const selectedMonth = moment(new Date(), timezone)
    .add(soonEndingMonths, 'months')
    .toDate()
  const todayDate = moment.tz(new Date(), timezone).startOf('day').toDate()
  const compareDate = moment
    .tz(new Date(), timezone)
    .add(7, 'days')
    .endOf('day')
    .toDate()

  const movingProtocolList =
    (await contractHelper.propertyMovingProtocolList({
      ...body,
      compareDate,
      selectedMonth,
      todayDate
    })) || []
  return {
    data: movingProtocolList,
    metaData: {
      totalDocuments: movingProtocolList.length,
      filteredDocuments: movingProtocolList.length
    }
  }
}

const countAgedDebtorsReport = async (query, dueQuery) => {
  if (!size(dueQuery)) {
    dueQuery = {
      $or: [
        {
          totalDue: {
            $gte: 1
          }
        },
        {
          totalDue: {
            $lte: -1
          }
        }
      ]
    }
  }
  const pipeline = [
    {
      $match: query
    },
    {
      $group: {
        _id: '$contractId',
        dueTotalAmount: {
          $sum: '$invoiceTotal'
        },
        dueTotalPaid: {
          $sum: '$totalPaid'
        },
        dueCreditedAmount: {
          $sum: '$creditedAmount'
        },
        totalLostAmount: {
          $sum: {
            $cond: [{ $eq: ['$status', 'lost'] }, '$lostMeta.amount', 0]
          }
        }
      }
    },
    {
      $addFields: {
        totalDue: {
          $subtract: [
            {
              $add: ['$dueTotalAmount', '$dueCreditedAmount']
            },
            {
              $add: ['$dueTotalPaid', '$totalLostAmount']
            }
          ]
        }
      }
    },
    {
      $match: dueQuery
    },
    {
      $group: {
        _id: null,
        rowNum: { $sum: 1 }
      }
    }
  ]
  const [agedDebtorsReport = {}] =
    (await InvoiceCollection.aggregate(pipeline)) || []
  return agedDebtorsReport.rowNum || 0
}

export const queryAgedDebtorsReport = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  const { query = {}, options } = body
  appHelper.validateSortForQuery(options.sort)
  query.partnerId = partnerId
  body.query = query
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (!size(partnerSetting))
    throw new CustomError(404, 'Partner setting not found')
  const customQuery = prepareQueryForAgeDebtorsReport(query)
  const data = await getAgedDebtorsReport({
    query: customQuery,
    options,
    partnerSetting
  })
  const filteredDocuments = await countAgedDebtorsReport(
    customQuery.preparedQuery,
    customQuery.dueQuery
  )
  const totalDocuments = await countAgedDebtorsReport({
    partnerId,
    invoiceType: 'invoice'
  })
  return {
    data,
    metaData: {
      filteredDocuments,
      totalDocuments
    }
  }
}

const prepareSortForAgedDebtorsReport = (sort = {}) => {
  if (sort.propertyInfo_location_name) {
    sort['propertyInfo.location.name'] = sort.propertyInfo_location_name
  }
  return omit(sort, ['propertyInfo_location_name'])
}

export const queryAgedDebtorsReportForExcelManager = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkUserId(user.userId)
  const { query, options } = body
  appHelper.checkRequiredFields(['partnerId'], query)
  const { partnerId } = query
  appHelper.validateId({ partnerId })
  appHelper.validateSortForQuery(options.sort)
  options.sort = prepareSortForAgedDebtorsReport(options.sort)
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (!size(partnerSetting))
    throw new CustomError(404, 'Partner setting not found')
  const customQuery = prepareQueryForAgeDebtorsReport(query)
  const data = await getAgedDebtorsReport({
    query: customQuery,
    options,
    partnerSetting,
    userLanguage: query.userLanguage,
    forExcelManager: true
  })
  return {
    data
  }
}

const getAgedDebtorsReport = async (params = {}) => {
  const { query, options, partnerSetting, userLanguage, forExcelManager } =
    params
  const { preparedQuery, dueQuery } = query
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: preparedQuery
    },
    ...(await getOverDuePeriodWisePipelineForAgedDebatorsReport(
      partnerSetting,
      '$contractId'
    )),
    {
      $match: dueQuery
    },
    ...getPropertyPipelineForAgedDebatorsReport(),
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getEvictionCasePipelineForAgedDebatorsReport(),
    {
      $addFields: {
        tenantId: '$contractInfo.rentalMeta.tenantId'
      }
    },
    ...appHelper.getCommonTenantInfoPipeline(),
    {
      $sort: sort
    },
    {
      $project: getProjectPipelineForAgedDebtorsReport(
        forExcelManager,
        userLanguage,
        partnerSetting
      )
    }
  ]
  return (await InvoiceCollection.aggregate(pipeline)) || []
}

const getProjectPipelineForAgedDebtorsReport = (
  forExcelManager,
  userLanguage,
  partnerSetting
) => {
  const project = {
    _id: 1,
    totalOverDue: 1,
    totalDue: 1,
    totalZeroToSixOverDue: 1,
    totalSevenToTwentyNineOverDue: 1,
    totalThirtyToFiftyNineOverDue: 1,
    totalSixtyToEightyNineOverDue: 1,
    totalNinetyPlusOverDue: 1
  }
  if (forExcelManager) {
    const numberOfDecimal =
      partnerSetting.currencySettings?.numberOfDecimal || 2
    project.hasEviction = {
      $cond: [
        { $eq: ['$hasEviction', 'yes'] },
        appHelper.translateToUserLng('common.yes', userLanguage),
        appHelper.translateToUserLng('common.no', userLanguage)
      ]
    }
    project.propertyName = '$propertyInfo.location.name'
    project.apartmentId = '$propertyInfo.apartmentId'
    project.tenantName = '$tenantInfo.name'
    project.totalOverDue = {
      $round: ['$totalOverDue', numberOfDecimal]
    }
    project.totalDue = {
      $round: ['$totalDue', numberOfDecimal]
    }
    project.totalZeroToSixOverDue = {
      $round: ['$totalZeroToSixOverDue', numberOfDecimal]
    }
    project.totalSevenToTwentyNineOverDue = {
      $round: ['$totalSevenToTwentyNineOverDue', numberOfDecimal]
    }
    project.totalThirtyToFiftyNineOverDue = {
      $round: ['$totalThirtyToFiftyNineOverDue', numberOfDecimal]
    }
    project.totalSixtyToEightyNineOverDue = {
      $round: ['$totalSixtyToEightyNineOverDue', numberOfDecimal]
    }
    project.totalNinetyPlusOverDue = {
      $round: ['$totalNinetyPlusOverDue', numberOfDecimal]
    }
  } else {
    project.hasEviction = 1
    project.tenantInfo = {
      _id: 1,
      avatarKey: 1,
      name: 1
    }
    project.propertyInfo = {
      _id: 1,
      apartmentId: 1,
      location: {
        name: 1,
        city: 1,
        country: 1,
        postalCode: 1
      }
    }
  }
  return project
}

const prepareOverDueDateRangesForAgedDebatorsReport = async (
  partnerSetting
) => {
  const zeroToSixDay = {
    startDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(6, 'days')
      .startOf('day')
      .toDate(),
    endDate: (await appHelper.getActualDate(partnerSetting, true))
      .endOf('day')
      .toDate()
  }
  const sevenToTwentyNineDay = {
    startDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(29, 'days')
      .startOf('day')
      .toDate(),
    endDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(7, 'days')
      .endOf('day')
      .toDate()
  }
  const thirtyToFiftyNineDay = {
    startDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(59, 'days')
      .startOf('day')
      .toDate(),
    endDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(30, 'days')
      .endOf('day')
      .toDate()
  }
  const sixtyToEightyNineDay = {
    startDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(89, 'days')
      .startOf('day')
      .toDate(),
    endDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(60, 'days')
      .endOf('day')
      .toDate()
  }
  const nintyDayPlus = {
    endDate: (await appHelper.getActualDate(partnerSetting, true))
      .subtract(90, 'days')
      .endOf('day')
      .toDate()
  }
  return {
    zeroToSixDay,
    sevenToTwentyNineDay,
    thirtyToFiftyNineDay,
    sixtyToEightyNineDay,
    nintyDayPlus
  }
}

const getPropertyPipelineForAgedDebatorsReport = () => [
  {
    $lookup: {
      from: 'listings',
      localField: 'propertyId',
      foreignField: '_id',
      as: 'propertyInfo'
    }
  },
  appHelper.getUnwindPipeline('propertyInfo')
]

const getOverDuePeriodWisePipelineForAgedDebatorsReport = async (
  partnerSetting,
  groupBasedOn = null
) => {
  const {
    zeroToSixDay,
    sevenToTwentyNineDay,
    thirtyToFiftyNineDay,
    sixtyToEightyNineDay,
    nintyDayPlus
  } = await prepareOverDueDateRangesForAgedDebatorsReport(partnerSetting)
  return [
    {
      $group: {
        _id: groupBasedOn,
        overDueTotalAmount: {
          $sum: {
            $cond: [{ $eq: ['$status', 'overdue'] }, '$invoiceTotal', 0]
          }
        },
        overDueTotalPaid: {
          $sum: {
            $cond: [{ $eq: ['$status', 'overdue'] }, '$totalPaid', 0]
          }
        },
        overDueCreditedAmount: {
          $sum: {
            $cond: [{ $eq: ['$status', 'overdue'] }, '$creditedAmount', 0]
          }
        },
        // Zero to six days
        overDueZeroToSixTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', zeroToSixDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', zeroToSixDay.endDate]
                  }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        overDueZeroToSixTotalPaid: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', zeroToSixDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', zeroToSixDay.endDate]
                  }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        overDueZeroToSixCreditedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', zeroToSixDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', zeroToSixDay.endDate]
                  }
                ]
              },
              '$creditedAmount',
              0
            ]
          }
        },

        // Seven to twenty nine days
        overDueSevenToTwentyNineTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', sevenToTwentyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', sevenToTwentyNineDay.endDate]
                  }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        overDueSevenToTwentyNineTotalPaid: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', sevenToTwentyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', sevenToTwentyNineDay.endDate]
                  }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        overDueSevenToTwentyNineCreditedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', sevenToTwentyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', sevenToTwentyNineDay.endDate]
                  }
                ]
              },
              '$creditedAmount',
              0
            ]
          }
        },

        // Thirty to fifty nine days
        overDueThirtyToFiftyNineTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', thirtyToFiftyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', thirtyToFiftyNineDay.endDate]
                  }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        overDueThirtyToFiftyNineTotalPaid: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', thirtyToFiftyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', thirtyToFiftyNineDay.endDate]
                  }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        overDueThirtyToFiftyNineCreditedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', thirtyToFiftyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', thirtyToFiftyNineDay.endDate]
                  }
                ]
              },
              '$creditedAmount',
              0
            ]
          }
        },

        // Sixty to eighty nine days
        overDueSixtyToEightyNineTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', sixtyToEightyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', sixtyToEightyNineDay.endDate]
                  }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        overDueSixtyToEightyNineTotalPaid: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', sixtyToEightyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', sixtyToEightyNineDay.endDate]
                  }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        overDueSixtyToEightyNineCreditedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $gte: ['$dueDate', sixtyToEightyNineDay.startDate]
                  },
                  {
                    $lte: ['$dueDate', sixtyToEightyNineDay.endDate]
                  }
                ]
              },
              '$creditedAmount',
              0
            ]
          }
        },

        // Ninety plus days
        overDueNinetyPlusTotalAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $lte: ['$dueDate', nintyDayPlus.endDate]
                  }
                ]
              },
              '$invoiceTotal',
              0
            ]
          }
        },
        overDueNinetyPlusTotalPaid: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $lte: ['$dueDate', nintyDayPlus.endDate]
                  }
                ]
              },
              '$totalPaid',
              0
            ]
          }
        },
        overDueNinetyPlusCreditedAmount: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ['$status', 'overdue'] },
                  {
                    $lte: ['$dueDate', nintyDayPlus.endDate]
                  }
                ]
              },
              '$creditedAmount',
              0
            ]
          }
        },

        dueTotalAmount: {
          $sum: '$invoiceTotal'
        },
        dueTotalPaid: {
          $sum: '$totalPaid'
        },
        dueCreditedAmount: {
          $sum: '$creditedAmount'
        },
        totalLostAmount: {
          $sum: {
            $cond: [{ $eq: ['$status', 'lost'] }, '$lostMeta.amount', 0]
          }
        },
        propertyId: {
          $first: '$propertyId'
        },
        tenantId: {
          $first: '$tenantId'
        }
      }
    },
    {
      $addFields: {
        totalOverDue: {
          $subtract: [
            {
              $add: ['$overDueTotalAmount', '$overDueCreditedAmount']
            },
            '$overDueTotalPaid'
          ]
        },
        totalDue: {
          $subtract: [
            {
              $add: ['$dueTotalAmount', '$dueCreditedAmount']
            },
            {
              $add: ['$dueTotalPaid', '$totalLostAmount']
            }
          ]
        },
        totalZeroToSixOverDue: {
          $subtract: [
            {
              $add: [
                '$overDueZeroToSixTotalAmount',
                '$overDueZeroToSixCreditedAmount'
              ]
            },
            '$overDueZeroToSixTotalPaid'
          ]
        },
        totalSevenToTwentyNineOverDue: {
          $subtract: [
            {
              $add: [
                '$overDueSevenToTwentyNineTotalAmount',
                '$overDueSevenToTwentyNineCreditedAmount'
              ]
            },
            '$overDueSevenToTwentyNineTotalPaid'
          ]
        },
        totalThirtyToFiftyNineOverDue: {
          $subtract: [
            {
              $add: [
                '$overDueThirtyToFiftyNineTotalAmount',
                '$overDueThirtyToFiftyNineCreditedAmount'
              ]
            },
            '$overDueThirtyToFiftyNineTotalPaid'
          ]
        },
        totalSixtyToEightyNineOverDue: {
          $subtract: [
            {
              $add: [
                '$overDueSixtyToEightyNineTotalAmount',
                '$overDueSixtyToEightyNineCreditedAmount'
              ]
            },
            '$overDueSixtyToEightyNineTotalPaid'
          ]
        },
        totalNinetyPlusOverDue: {
          $subtract: [
            {
              $add: [
                '$overDueNinetyPlusTotalAmount',
                '$overDueNinetyPlusCreditedAmount'
              ]
            },
            '$overDueNinetyPlusTotalPaid'
          ]
        }
      }
    }
  ]
}

const getEvictionCasePipelineForAgedDebatorsReport = () => [
  {
    $lookup: {
      from: 'contracts',
      localField: '_id',
      foreignField: '_id',
      as: 'contractInfo'
    }
  },
  appHelper.getUnwindPipeline('contractInfo'),
  {
    $addFields: {
      activeEvictionCase: {
        $first: {
          $filter: {
            input: { $ifNull: ['$contractInfo.evictionCases', []] },
            as: 'evictionCase',
            cond: {
              $eq: ['$$evictionCase.status', 'in_progress']
            }
          }
        }
      }
    }
  },
  {
    $addFields: {
      hasEviction: {
        $cond: [{ $ifNull: ['$activeEvictionCase', false] }, 'yes', 'no']
      }
    }
  }
]

export const prepareQueryForAgeDebtorsReport = (query) => {
  const {
    accountId,
    agentId,
    branchId,
    createdAtDateRange,
    due,
    partnerId,
    propertyId,
    tenantId
  } = query
  const preparedQuery = {
    partnerId,
    invoiceType: 'invoice'
  }
  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    preparedQuery['rentalMeta.createdAt'] = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  if (branchId) preparedQuery.branchId = branchId
  if (agentId) preparedQuery.agentId = agentId
  if (accountId) preparedQuery.accountId = accountId
  if (propertyId) preparedQuery.propertyId = propertyId
  if (tenantId) preparedQuery.tenantId = tenantId

  const dueQuery = {}
  if (due === 'yes') dueQuery.totalDue = { $gte: 1 }
  else if (due === 'no') dueQuery.totalDue = { $lte: -1 }
  else {
    dueQuery.$or = [
      {
        totalDue: {
          $gte: 1
        }
      },
      {
        totalDue: {
          $lte: -1
        }
      }
    ]
  }
  return { preparedQuery, dueQuery }
}

export const queryAgedDebtorsReportSummary = async (req) => {
  const { body = {}, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  if (!size(partnerSetting))
    throw new CustomError(404, 'Partner setting not found')
  const customQuery = prepareQueryForAgeDebtorsReport(body)

  return await getAgedDebtorsReportSummary(customQuery, partnerSetting)
}

const getAgedDebtorsReportSummary = async (query, partnerSetting) => {
  const { preparedQuery, dueQuery } = query
  const pipeline = [
    {
      $match: preparedQuery
    },
    ...(await getOverDuePeriodWisePipelineForAgedDebatorsReport(
      partnerSetting,
      '$contractId'
    )),
    {
      $match: dueQuery
    },
    ...getEvictionCasePipelineForAgedDebatorsReport(),
    {
      $addFields: {
        tenantId: '$contractInfo.rentalMeta.tenantId'
      }
    },
    {
      $group: {
        _id: null,
        totalOverDue: { $sum: '$totalOverDue' },
        totalDue: { $sum: '$totalDue' },
        totalZeroToSixOverDue: { $sum: '$totalZeroToSixOverDue' },
        totalSevenToTwentyNineOverDue: {
          $sum: '$totalSevenToTwentyNineOverDue'
        },
        totalThirtyToFiftyNineOverDue: {
          $sum: '$totalThirtyToFiftyNineOverDue'
        },
        totalSixtyToEightyNineOverDue: {
          $sum: '$totalSixtyToEightyNineOverDue'
        },
        totalNinetyPlusOverDue: { $sum: '$totalNinetyPlusOverDue' },
        activeEvictionNum: {
          $sum: {
            $cond: [{ $eq: ['$hasEviction', 'yes'] }, 1, 0]
          }
        },
        tenantIds: {
          $addToSet: '$tenantId'
        }
      }
    },
    {
      $project: {
        totalOverDue: 1,
        totalDue: 1,
        totalZeroToSixOverDue: 1,
        totalSevenToTwentyNineOverDue: 1,
        totalThirtyToFiftyNineOverDue: 1,
        totalSixtyToEightyNineOverDue: 1,
        totalNinetyPlusOverDue: 1,
        activeEvictionNum: 1,
        numberOfTenant: {
          $size: '$tenantIds'
        }
      }
    }
  ]
  const [reportSummary = {}] =
    (await InvoiceCollection.aggregate(pipeline)) || []
  return reportSummary
}

export const prepareLogDataForCancelTermination = (params = {}) => {
  const { contract, userId = 'SYSTEM' } = params
  if (!size(contract)) return false

  const metaData = []
  if (contract.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: contract.leaseSerial })
  }

  const visibility = logHelper.getLogVisibility(
    { context: 'property' },
    contract
  )

  const logData = {
    accountId: contract.accountId,
    action: 'removed_lease_termination',
    agentId: contract.agentId,
    branchId: contract.branchId,
    context: 'property',
    contractId: contract._id,
    createdBy: userId,
    isChangeLog: false,
    meta: metaData,
    partnerId: contract.partnerId,
    propertyId: contract.propertyId,
    visibility
  }

  return logData
}

export const prepareContractCancelTermination = async (params, session) => {
  const { contractId, partnerId, propertyId } = params
  const contractInfo = await getAContract(
    {
      _id: contractId,
      partnerId,
      propertyId
    },
    session,
    [{ path: 'partner', populate: ['partnerSetting'] }]
  )

  if (!contractInfo?._id) {
    throw new CustomError(404, 'Lease not found')
  }

  if (
    contractInfo?.rentalMeta?.status !== 'active' ||
    !contractInfo?.rentalMeta?.terminatedBy
  ) {
    throw new CustomError(400, 'Lease is not available for cancel termination')
  }

  const { partnerSetting } = contractInfo.partner || {}

  if (!size(partnerSetting)) {
    throw new CustomError(404, 'Partner setting not found')
  }

  const updateData = {
    $unset: {
      'rentalMeta.terminatedBy': 1,
      'rentalMeta.terminateReasons': 1,
      'rentalMeta.terminateComments': 1,
      terminatedByUserId: 1
    }
  }

  let isChangedContractEndDate = false

  if (
    params.contractEndDate &&
    !moment(params.contractEndDate).isSame(
      moment(contractInfo.rentalMeta?.contractEndDate),
      'day'
    )
  ) {
    const paramsContractEndDate = await appHelper.getActualDate(
      partnerSetting,
      true,
      params.contractEndDate
    )

    const todayDate = await appHelper.getActualDate(
      partnerSetting,
      true,
      new Date()
    )

    const upcomingContract = await getAContract({
      partnerId,
      propertyId,
      status: 'upcoming'
    })

    if (
      size(upcomingContract) &&
      upcomingContract.hasRentalContract &&
      upcomingContract?.rentalMeta?.contractStartDate
    ) {
      const leaseEndMaxDate = (
        await appHelper.getActualDate(
          partnerSetting,
          true,
          upcomingContract.rentalMeta.contractStartDate
        )
      ).subtract(1, 'days')
      if (leaseEndMaxDate.isBefore(paramsContractEndDate)) {
        throw new CustomError(
          404,
          'Contract end date should be less than upcoming contract start date'
        )
      }
    }

    const leaseEndMinDate = (
      await appHelper.getActualDate(
        partnerSetting,
        true,
        contractInfo.rentalMeta?.contractStartDate
      )
    ).startOf('day')

    if (leaseEndMinDate.isAfter(paramsContractEndDate, 'day')) {
      throw new CustomError(
        404,
        'Contract end date should be greater than contract start date'
      )
    }

    if (paramsContractEndDate <= todayDate) {
      throw new CustomError(
        400,
        "Contract end date should be greeter then today's date"
      )
    }
    isChangedContractEndDate = true
    updateData.$set = {
      'rentalMeta.contractEndDate': params.contractEndDate
    }
  }
  return {
    isChangedContractEndDate,
    updateData,
    previousContract: contractInfo
  }
}
