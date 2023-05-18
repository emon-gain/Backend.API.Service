import AWS from 'aws-sdk'
import mime from 'mime-types'
import crypto from 'crypto'
import { size } from 'lodash'
import { decryptSecret } from './decryption'

export const getAWSConfig = async () => ({
  accessKeyId: (await decryptSecret('V1_AWS_ACCESS_KEY_ID')) || '',
  secretAccessKey: (await decryptSecret('V1_AWS_SECRET_ACCESS_KEY')) || ''
})

export const getS3SignedUrl = async (Key, Bucket, Expires, Download) => {
  try {
    const params = { Key, Bucket }
    if (Expires) params.Expires = Expires
    if (Download) params.ResponseContentDisposition = 'attachment'

    if (Key && Bucket) {
      const awsConfiguration = await getAWSConfig()
      const S3 = new AWS.S3(awsConfiguration)

      return await new Promise((success, reject) => {
        S3.getSignedUrl('getObject', params, (err, data = {}) => {
          if (size(err)) {
            reject(err)
          }
          success(data)
        })
      })
    }
  } catch (error) {
    throw new Error(
      `Error happened when fetching file buffer data from s3, error: ${error?.message}`
    )
  }
}

export const getWritePolicy = async (policyData) => {
  const { directive, duration, filesize, key } = policyData
  const { acl } = directive
  const bucket = process.env.S3_BUCKET
  const contentType = mime.lookup(key)
  const dateObj = new Date()
  const dateExp = new Date(dateObj.getTime() + duration * 1000)
  const policy = {
    expiration:
      dateExp.getUTCFullYear() +
      '-' +
      (dateExp.getUTCMonth() + 1) +
      '-' +
      dateExp.getUTCDate() +
      'T' +
      dateExp.getUTCHours() +
      ':' +
      dateExp.getUTCMinutes() +
      ':' +
      dateExp.getUTCSeconds() +
      'Z',
    conditions: [
      { bucket },
      ['eq', '$key', key],
      { acl },
      ['content-length-range', 0, filesize * 1000000],
      ['starts-with', '$Content-Type', contentType]
    ]
  }
  const policyString = JSON.stringify(policy)
  const policyBase64 = Buffer.from(policyString).toString('base64')
  const secretKey = (await getAWSConfig()).secretAccessKey
  const signature = crypto.createHmac('sha1', secretKey).update(policyBase64)
  const accessKey = (await getAWSConfig()).accessKeyId

  return {
    url: 'https://' + bucket + '.s3.amazonaws.com',
    policyData: [
      { name: 'key', value: key },
      { name: 'Content-Type', value: contentType },
      { name: 'AWSAccessKeyId', value: accessKey },
      { name: 'bucket', value: bucket },
      { name: 'acl', value: acl },
      { name: 'policy', value: policyBase64 },
      { name: 'signature', value: signature.digest('base64') }
    ]
  }
}
