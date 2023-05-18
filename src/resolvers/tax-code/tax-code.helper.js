import validator from 'validator'

import { TaxCodeCollection } from '../models'
import { appHelper, ledgerAccountHelper } from '../helpers'
import { CustomError } from '../common'

export const getTaxCodeById = async (id, session) => {
  const taxCode = await TaxCodeCollection.findById(id).session(session)
  return taxCode
}

export const getTaxCode = async (query, session) => {
  const texCode = await TaxCodeCollection.findOne(query).session(session)
  return texCode
}

export const getTaxCodes = async (query, session) => {
  const texCodes = await TaxCodeCollection.find(query).session(session)
  return texCodes
}

export const getTaxCodesForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const taxCodes = await TaxCodeCollection.find(query)
    .populate('partner')
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return taxCodes
}

export const countTaxCodes = async (query, session) => {
  const numberOfTaxCodes = await TaxCodeCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfTaxCodes
}

export const queryTaxCodes = async (req) => {
  const { body = {}, user = {} } = req
  const { userId, partnerId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['userId'], user)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)

  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  } else if (body.query.appAdmin) {
    query.partnerId = { $exists: false }
    delete query.appAdmin
  } else {
    throw new CustomError('400', 'Bad Request')
  }

  const taxCodesData = await getTaxCodesForQuery(body)
  const filteredDocuments = await countTaxCodes(query)
  const totalDocuments = partnerId
    ? await countTaxCodes({ partnerId })
    : await countTaxCodes({ partnerId: { $exists: false } })

  const taxCodes = await Promise.all(
    taxCodesData.map(async (taxCode) => {
      const { _id, partnerId = '' } = taxCode
      taxCode.isDeletable = !(await isTaxCodeBeingUsed(_id, partnerId))
      return taxCode
    })
  )
  return { data: taxCodes, metaData: { filteredDocuments, totalDocuments } }
}

export const checkRequiredFieldsForTaxCodeCreation = (body) => {
  const requiredFields = ['name', 'taxCode', 'taxPercentage']
  appHelper.checkRequiredFields(requiredFields, body)
  const { name = '', taxCode, taxPercentage } = body
  if (!name) throw new CustomError(400, 'Required name')
  else if (taxCode === null) throw new CustomError(400, 'Required taxCode')
  else if (taxPercentage === null)
    throw new CustomError(400, 'Required taxPercentage')
}

export const checkRequiredFieldsForTaxCodeUpdate = (body) => {
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { _id } = body
  appHelper.validateId({ _id })
}

export const prepareDataForUpdatingTaxCode = async (params) => {
  const { data, partnerId, session } = params
  const { name, valueString, valueBoolean, valueInt } = data
  let updatingData = {}
  if (!name) {
    throw new CustomError(400, `Required name`)
  }
  if (name === 'taxCode') {
    if (!validator.isInt(`${valueInt}`))
      throw new CustomError(400, 'Value must be an integer')
    const query = {
      partnerId: partnerId ? partnerId : { $exists: false },
      taxCode: valueInt
    }
    const hasTaxCode = !!(await getTaxCode(query, session))
    if (hasTaxCode) {
      throw new CustomError(405, `Tax code already exists`)
    } else updatingData = { taxCode: valueInt }
  } else if (name === 'name') {
    if (!valueString) throw new CustomError(400, 'Required value')
    updatingData = { name: valueString }
  } else if (name === 'taxPercentage') {
    if (typeof valueInt !== 'number')
      throw new CustomError(400, 'Value must be a number')
    updatingData = { taxPercentage: valueInt }
  } else if (name === 'enable') {
    if (!validator.isBoolean(`${valueBoolean}`))
      throw new CustomError(400, 'Value must be a boolean')
    updatingData = { enable: valueBoolean }
  }

  return { updatingData }
}

export const checkRequiredFieldsForTaxCodeRemove = (body) => {
  appHelper.checkRequiredFields(['_id'], body)
  const { _id } = body
  appHelper.validateId({ _id })
}

export const isTaxCodeBeingUsed = async (taxCodeId, partnerId, session) => {
  const accountQuery = {
    taxCodeId,
    partnerId: partnerId ? partnerId : { $exists: false }
  }
  const isLedgerAccountExists = !!(await ledgerAccountHelper.getLedgerAccount(
    accountQuery,
    session
  ))
  return isLedgerAccountExists
}
