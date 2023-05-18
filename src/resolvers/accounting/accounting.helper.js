import { AccountingCollection } from '../models'
import { appHelper } from '../helpers'
import { size } from 'lodash'
import { CustomError } from '../common'

export const getAccounting = async (query, session) => {
  const accounting = await AccountingCollection.findOne(query).session(session)
  return accounting
}

export const getAccountings = async (query, session, populate = []) => {
  const accounts = await AccountingCollection.find(query)
    .populate(populate)
    .session(session)
  return accounts
}

export const prepareAccountingsQueryBasedOnFilters = (query) => {
  const { appAdmin } = query
  if (appAdmin) {
    query.partnerId = { $exists: false }
    delete query.appAdmin
  }
  return query
}

export const getAccountingsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const accountings = await AccountingCollection.find(query)
    .populate('partner')
    .populate({
      path: 'creditAccount',
      populate: {
        path: 'taxCode'
      }
    })
    .populate({
      path: 'debitAccount',
      populate: {
        path: 'taxCode'
      }
    })
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return accountings
}

export const countAccountings = async (query, session) => {
  const numberOfAccountings = await AccountingCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfAccountings
}

export const queryAccountings = async (req) => {
  const { body, user } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { partnerId } = user
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  }
  body.query = prepareAccountingsQueryBasedOnFilters(query)
  const accountingsData = await getAccountingsForQuery(body)
  const filteredDocuments = await countAccountings(body.query)
  const totalDocuments = partnerId
    ? await countAccountings({ partnerId })
    : await countAccountings({ partnerId: { $exists: false } })
  return {
    data: accountingsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const checkRequiredFieldsForAccountingUpdate = (body) => {
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { _id, data = {} } = body
  const { name, value } = data
  if (!_id) throw new CustomError(400, 'Required _id')
  else appHelper.validateId({ _id })
  if (!name) throw new CustomError(400, 'Required name')
  if (value === null) throw new CustomError(400, 'Required value')
}

export const prepareQueryForAccountingBasedOnPartnerId = (body) => {
  const { _id, partnerId } = body
  const query = { _id }
  if (partnerId) query.partnerId = partnerId
  return query
}

export const prepareAccountingUpdatingData = (body) => {
  const { data } = body
  const { name, value } = data
  let updatingData = {}
  if (name === 'subName') {
    if (!value) throw new CustomError(400, 'Required value')
    updatingData = { subName: value }
  } else if (name === 'debitAccountId') {
    appHelper.validateId({ debitAccountId: value })
    updatingData = { debitAccountId: value }
  } else if (name === 'creditAccountId') {
    appHelper.validateId({ creditAccountId: value })
    updatingData = { creditAccountId: value }
  }
  if (!size(updatingData))
    throw new CustomError(400, 'Invalid name to update accounting')

  return { updatingData }
}
