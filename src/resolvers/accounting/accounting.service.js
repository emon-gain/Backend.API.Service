import { isEmpty, size } from 'lodash'
import { CustomError } from '../common'
import { AccountingCollection } from '../models'
import { accountingHelper, appHelper } from '../helpers'

export const createAnAccounting = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for Accounting creation')
  }
  const createdAccounting = await AccountingCollection.create([data], {
    session
  })
  if (isEmpty(createdAccounting)) {
    throw new CustomError(404, 'Unable to create an Accounting')
  }
  return createdAccounting
}

export const updateAnAccounting = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await AccountingCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!response) {
    throw new CustomError(404, `Could not update Accounting`)
  }

  return response
}

export const updateAccounting = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '', partnerId } = user
  appHelper.checkUserId(userId)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  accountingHelper.checkRequiredFieldsForAccountingUpdate(body)
  const query = accountingHelper.prepareQueryForAccountingBasedOnPartnerId(body)
  const { updatingData } = accountingHelper.prepareAccountingUpdatingData(body)
  const updatedAccounting = await updateAnAccounting(
    query,
    updatingData,
    session
  )
  return updatedAccounting
}
