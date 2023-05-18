import { ApiKeyCollection } from '../models'
import { appHelper } from '../helpers'

export const getAnApiKey = async (query, session) => {
  const apiKey = await ApiKeyCollection.findOne(query).session(session)
  return apiKey
}

export const getApiKey = async (req) => {
  const { user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  const apiKeyInfo = await getAnApiKey({
    partnerId
  })
  return apiKeyInfo?.apiKey
}
