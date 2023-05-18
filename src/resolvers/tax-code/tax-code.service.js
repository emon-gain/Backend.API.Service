import { size } from 'lodash'
import { CustomError } from '../common'
import { TaxCodeCollection } from '../models'
import { appHelper, taxCodeHelper } from '../helpers'

export const createATaxCode = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for taxCode creation')
  }
  const createdTaxCode = await TaxCodeCollection.create([data], { session })
  return createdTaxCode
}

export const updateATaxCode = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await TaxCodeCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return response
}

export const removeATaxCode = async (query, session) => {
  const response = await TaxCodeCollection.findOneAndDelete(query, { session })
  return response
}

export const createTaxCode = async (req) => {
  const { body = {}, user = {}, session } = req
  const { userId, partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  taxCodeHelper.checkRequiredFieldsForTaxCodeCreation(body)

  body.createdBy = userId
  const { enable, taxCode } = body
  if (!enable) body.enable = false

  const query = {}
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
    body.partnerId = partnerId
  } else {
    query['partnerId'] = {
      $exists: false
    }
  }
  query.taxCode = taxCode
  const taxCodeInfo = await taxCodeHelper.getTaxCode(query, session)

  if (!taxCodeInfo) {
    const createdTaxCode = await createATaxCode(body, session)
    return createdTaxCode
  } else
    throw new CustomError(405, 'TaxCode already exists, please enter new code')
}

export const updateTaxCode = async (req) => {
  const { body = {}, user = {}, session } = req
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  taxCodeHelper.checkRequiredFieldsForTaxCodeUpdate(body)

  const { _id = '', data = {} } = body
  const query = { _id }

  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  } else {
    query['partnerId'] = {
      $exists: false
    }
  }

  const taxCodeInfo = await taxCodeHelper.getTaxCode(query)
  if (!taxCodeInfo) throw new CustomError(404, "Tax code doesn't exists")
  const params = { data, partnerId, session } // PartnerId will check here for both client request
  const { updatingData } = await taxCodeHelper.prepareDataForUpdatingTaxCode(
    params
  )
  if (size(updatingData)) {
    const updatedTaxCode = await updateATaxCode(query, updatingData, session)
    return updatedTaxCode
  } else throw new CustomError(400, 'Invalid name for tax code update')
}

export const removeTaxCode = async (req) => {
  const { body = {}, user = {}, session } = req
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  taxCodeHelper.checkRequiredFieldsForTaxCodeRemove(body)

  const { _id = '' } = body
  const query = { _id }

  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  } else {
    query['partnerId'] = {
      $exists: false
    }
  }
  const taxCodeInfo = await taxCodeHelper.getTaxCode(query)
  if (!taxCodeInfo) throw new CustomError(404, "Tax code doesn't exists")
  const isTaxCodeBeingUsed = await taxCodeHelper.isTaxCodeBeingUsed(
    _id,
    partnerId, // partnerId will check in this function
    session
  )
  if (isTaxCodeBeingUsed)
    throw new CustomError(405, "You can not delete this, It's being using")

  const removedTaxCode = await removeATaxCode(query, session)
  return removedTaxCode
}
