import jwt from 'jsonwebtoken'
import { decryptSecret } from './decryption'

export const generateJWTToken = async (signData) => {
  try {
    const JWTSecret = (await decryptSecret('JWT_SECRET')) || ''
    const token = jwt.sign(signData, JWTSecret) || {}
    return { success: true, token }
  } catch (error) {
    return {
      success: false,
      error
    }
  }
}

export const verifyAndDecodeJWTToken = async (token) => {
  try {
    const JWTSecret = (await decryptSecret('JWT_SECRET')) || ''
    const decoded = jwt.verify(token, JWTSecret) || {}
    return { success: true, decoded }
  } catch (error) {
    return {
      success: false,
      error
    }
  }
}
