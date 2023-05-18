import { size } from 'lodash'
import Pusher from 'pusher'

import { decryptSecret } from '../../lib/decryption'
import { CustomError } from '../common'

const getPusherConfig = async () => {
  if (process.env.APP_STAGE === 'test' || process.env.NODE_ENV === 'test') {
    return {}
  }
  return {
    appId: await decryptSecret('PUSHER_APP_ID'),
    key: await decryptSecret('PUSHER_KEY'),
    secret: await decryptSecret('PUSHER_SECRET'),
    cluster: await decryptSecret('PUSHER_CLUSTER'),
    forceTLS: true
  }
}

export const pusherTrigger = async (
  channelName,
  eventName = 'message',
  data = {}
) => {
  try {
    if (!(size(data) && channelName))
      throw new CustomError('400', 'Required field missing for pusher trigger')

    const pusherConfig = await getPusherConfig() // Getting pusher config from environment or secret
    const pusher = new Pusher(pusherConfig)
    const result = await pusher.trigger(channelName, eventName, data)
    return result
  } catch (error) {
    if (error?.code === 'ERR_INVALID_ARG_TYPE') {
      console.log('Invalid pusher credentials error', error.message)
    } else {
      throw new CustomError(
        error.statusCode || 404,
        error.message || 'Unable to trigger pusher',
        error
      )
    }
  }
}
