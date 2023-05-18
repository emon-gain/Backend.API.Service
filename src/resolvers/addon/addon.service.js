import { size, isEmpty } from 'lodash'
import { CustomError } from '../common'
import { AddonCollection } from '../models'
import { addonHelper, appHelper } from '../helpers'

export const createAnAddon = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for Addon creation')
  }
  const createdAddon = await AddonCollection.create([data], { session })
  if (isEmpty(createdAddon)) {
    throw new CustomError(404, 'Unable to create Addon')
  }
  return createdAddon
}

export const updateAnAddon = async (query, data, session) => {
  const updatedAddon = await AddonCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!size(updatedAddon)) {
    throw new CustomError(404, 'Addon could not be found')
  }
  return updatedAddon
}

export const deleteAnAddon = async (query, session) => {
  const deletedAddon = await AddonCollection.findOneAndDelete(query, {
    session
  })
  if (!size(deletedAddon)) {
    throw new CustomError(404, 'Addon could not be found')
  }
  return deletedAddon
}

export const createAddon = async (req) => {
  const { body = {}, session, user = {} } = req
  const { userId, partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  const requiredFields = ['name', 'debitAccountId', 'creditAccountId']
  appHelper.checkRequiredFields(requiredFields, body)

  if (partnerId) body.partnerId = partnerId
  addonHelper.validateAddonData(body)
  if (!body.name) throw new CustomError(400, "Name field can't be empty!")
  body.createdBy = userId
  const createData = addonHelper.prepareDataForCreateAddon(body)
  const createdAddon = await createAnAddon(createData, session)
  return createdAddon
}

export const updateAddon = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  const requiredFields = ['_id', 'data']
  appHelper.checkRequiredFields(requiredFields, body)
  const { _id, data } = body
  if (!size(data)) throw new CustomError('404', 'Required data missing!')
  appHelper.validateId({ _id })

  if (partnerId) data.partnerId = partnerId
  addonHelper.validateAddonData(data)

  const query = { _id }
  if (partnerId) query.partnerId = partnerId
  else query['partnerId'] = { $exists: false }

  const updatedAddon = await updateAnAddon(query, data, session)
  return updatedAddon
}

export const deleteAddon = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['_id'], body)
  const { _id } = body
  appHelper.validateId({ _id })

  const query = { _id }
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
  } else {
    query['partnerId'] = {
      $exists: false
    }
  }

  const deletedAddon = await deleteAnAddon(query, session)
  return deletedAddon
}
