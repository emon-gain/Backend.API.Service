import AWS from 'aws-sdk'
// import { createErrorResponse } from './error'

AWS.config.update({
  region: process.env.AWS_REGION || 'eu-west-1',
})
const KMS = new AWS.KMS()

const decrypted = {}

export const decryptSecret = async (secretName) => {
  if (decrypted[secretName]) {
    return decrypted[secretName]
  }

  try {
    const encrypted = process.env[secretName] || null
    const params = {
      CiphertextBlob: Buffer.from(encrypted, 'base64'),
      EncryptionContext: {
        KMS_CTX: process.env.KMS_CTX
      }
    }
    const { Plaintext } = await KMS.decrypt(params).promise()
    const decryptedVal = Plaintext ? Plaintext.toString('ascii') : null
    decrypted[secretName] = decryptedVal

    return decryptedVal
  } catch (e) {
    console.log('Error', e)
  }
}
