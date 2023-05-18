import { size, isEmpty } from 'lodash'
import { CustomError } from '../common'
import { LedgerAccountCollection } from '../models'
import { appHelper, ledgerAccountHelper } from '../helpers'

export const createALedgerAccount = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(400, 'No data found for LedgerAccount creation')
  }
  const createdLedgerAccount = await LedgerAccountCollection.create([data], {
    session
  })
  if (isEmpty(createdLedgerAccount)) {
    throw new CustomError(404, 'Unable to create LedgerAccount')
  }
  return createdLedgerAccount
}

export const updateALedgerAccount = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await LedgerAccountCollection.findOneAndUpdate(
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

export const removeALedgerAccount = async (query, session) => {
  const removedLedgerAccount = await LedgerAccountCollection.findOneAndRemove(
    query,
    {
      new: true,
      session
    }
  )
  if (!removedLedgerAccount) {
    throw new CustomError(404, `Could not delete Ledger Account`)
  }
  return removedLedgerAccount
}

export const createLedgerAccount = async (req) => {
  const { body = {}, session, user = {} } = req
  const { userId = '', partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  ledgerAccountHelper.validateLedgerAccountCreationData(body)
  if (size(body)) body.createdBy = userId
  const createdLedgerAccount = await createALedgerAccount(body, session)
  return createdLedgerAccount
}

export const updateLedgerAccount = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['_id', 'data'], body)
  const { _id } = body
  appHelper.validateId({ _id })
  const query = { _id }
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
    body.partnerId = partnerId
  }
  const updateData =
    await ledgerAccountHelper.prepareDataForUpdatingLedgerAccount(body, session)
  const updatedLedgerAccount = await updateALedgerAccount(
    query,
    updateData,
    session
  )
  return updatedLedgerAccount
}

export const removeLedgerAccount = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  if (partnerId) appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['_id'], body)
  const { _id } = body
  appHelper.validateId({ _id })
  await ledgerAccountHelper.validateRemovingLedgerAccount(_id, partnerId)
  const query = partnerId ? { _id, partnerId } : { _id }
  const removedLedgerAccount = await removeALedgerAccount(query, session)
  return removedLedgerAccount
}
