import { TokenCollection } from '../models'
import { appHelper } from '../helpers'

export const createAToken = async (data, session) => {
  const [token] = await TokenCollection.create([data], session)
  return token
}

export const createOTP = async (params = {}, session) => {
  const { userId, email, tokenType, expiredTime } = params
  appHelper.checkRequiredFields(['userId', 'tokenType'], params)
  const token = {}
  token.userId = userId
  token.token = Math.floor(100000 + Math.random() * 900000)
  token.tokenType = tokenType
  token.email = email ? email.toLowerCase() : ''
  token.expired =
    new Date().getTime() +
    (expiredTime ? Number(expiredTime) : 2 * 60 * 60 * 1000)
  const createdToken = await createAToken(token, session)
  return createdToken
}
