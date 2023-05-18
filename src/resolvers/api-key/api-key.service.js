import { size, isEmpty, clone } from 'lodash'
import nid from 'nid'
import { CustomError } from '../common'
import { ApiKeyCollection } from '../models'
import { appHelper, apiKeyHelper } from '../helpers'

export const createAnApiKey = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for ApiKey creation')
  }
  const createdApiKey = await ApiKeyCollection.create([data], { session })
  if (isEmpty(createdApiKey)) {
    throw new CustomError(404, 'Unable to create ApiKey')
  }
  return createdApiKey
}

export const updateAnApiKey = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }

  const updatedApiKeyData = await ApiKeyCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )

  if (!updatedApiKeyData) {
    throw new CustomError(404, `Unable to update Api key`)
  }
  return updatedApiKeyData
}

export const prepareApiKeyData = (body, user) => {
  const data = clone(body)
  const { userId } = user
  if (userId) {
    data.createdBy = userId
  }
  return data
}

export const createApiKey = async (req) => {
  const { body, session, user = {} } = req
  const apiKeyData = prepareApiKeyData(body, user)
  const apiKey = await ApiKeyCollection.create([apiKeyData], { session })
  return apiKey
}

export const resetApiKey = async (req) => {
  const { user } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  let isExistsAPIKey = true
  let randomKey = ''
  while (isExistsAPIKey) {
    randomKey = nid(30)
    isExistsAPIKey = !!(await apiKeyHelper.getAnApiKey({ apiKey: randomKey }))
  }
  if (randomKey && !isExistsAPIKey) {
    return await updateAnApiKey({ partnerId }, { $set: { apiKey: randomKey } })
  }
}
