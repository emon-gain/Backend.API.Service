import { omit, size } from 'lodash'
import { CustomError } from '../common'
import { AddonCollection } from '../models'
import { appHelper, ledgerAccountHelper, taxCodeHelper } from '../helpers'

export const getAddon = async (query, session) => {
  const addon = await AddonCollection.findOne(query).session(session)
  return addon
}

export const getAddons = async (query, session) => {
  const addons = await AddonCollection.find(query).session(session)
  return addons
}

export const getAddonById = async (id, session) => {
  const addon = await AddonCollection.findById(id).session(session)
  return addon
}

export const validateAddonData = (data = {}) => {
  if (!size(data)) {
    throw new CustomError(400, 'Data can not be empty')
  }
  const { partnerId, debitAccountId, creditAccountId } = data
  if (partnerId) appHelper.validateId({ partnerId })
  if (debitAccountId) appHelper.validateId({ debitAccountId })
  if (creditAccountId) appHelper.validateId({ creditAccountId })
}

export const prepareAddonsQueryBasedOnFilters = (query) => {
  const { createdDateRange, name, priceRange, status, type } = query
  // Set createdAt filters in query
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: createdDateRange.startDate,
      $lte: createdDateRange.endDate
    }
  }
  // Set status filters in query
  if (status === 'activated') query.enable = true
  if (status === 'deactivated') query.enable = false
  // Set price filers in query
  if (size(priceRange))
    query.price = {
      $gte: priceRange.minimum,
      $lte: priceRange.maximum
    }
  if (name) query.name = new RegExp('.*' + name + '.*', 'i')
  if (type) {
    if (type === 'leaseRent') {
      query.type = 'lease'
      query.isNonRent = { $ne: true }
    } else if (type === 'leaseNonRent') {
      query.type = 'lease'
      query.isNonRent = true
    }
  }
  const addonsQuery = omit(query, [
    'appAdmin',
    'createdDateRange',
    'status',
    'priceRange'
  ])
  return addonsQuery
}

export const getAddonsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const addons = await AddonCollection.find(query)
    .populate([
      'partner',
      {
        path: 'debitAccount',
        populate: {
          path: 'taxCodeInfo'
        }
      },
      {
        path: 'creditAccount',
        populate: {
          path: 'taxCodeInfo'
        }
      },
      {
        path: 'ledgerAccounts',
        populate: {
          path: 'taxCodeInfo'
        }
      }
    ])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return addons
}

export const countAddons = async (query, session) => {
  const numberOfAddons = await AddonCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfAddons
}

export const getLedgerAccountsForAddonsQuery = async (partnerId) => {
  const ledgerAccountsArray =
    (await ledgerAccountHelper.getLedgerAccounts({ partnerId })) || []
  if (!size(ledgerAccountsArray)) return ledgerAccountsArray // Return empty array []
  const ledgerAccounts = await Promise.all(
    ledgerAccountsArray.map(async (ledgerAccount) => {
      const { taxCodeId } = ledgerAccount
      if (taxCodeId) {
        ledgerAccount.taxCodeInfo =
          (await taxCodeHelper.getTaxCode({ _id: taxCodeId })) || {}
      }
      return ledgerAccount
    })
  )
  return ledgerAccounts
}

export const queryAddons = async (req) => {
  const { body = {}, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user) // This will use from admin and partner both end so partnerId isn't set as required
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)

  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  } else {
    query.partnerId = { $exists: false }
  }
  body.query = prepareAddonsQueryBasedOnFilters(query)

  const addonsData = await getAddonsForQuery(body)
  const filteredDocuments = await countAddons(body.query)
  const totalDocuments = await countAddons({
    partnerId: query.partnerId
  })

  return {
    data: addonsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const prepareDataForCreateAddon = (body) => {
  const { enableCommission } = body
  if (!enableCommission) delete body.commissionPercentage
  return body
}
