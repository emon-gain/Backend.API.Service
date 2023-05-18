import { size } from 'lodash'
import { AppHealthCollection } from '../models'
import { appHelper } from '../helpers'
import { appQueueService } from '../services'

export const prepareAppHealthData = (body, user) => {
  if (Array.isArray(body)) {
    const appHealthData = []
    for (let i = 0; i < body.length; i++) {
      const singleAppHealthData = body[i]
      const { userId } = user
      singleAppHealthData.createdBy = userId
      appHealthData.push(singleAppHealthData)
    }
    return appHealthData
  }
  const appHealthData = body
  const { userId } = user
  appHealthData.createdBy = userId
  return appHealthData
}

export const createAppHealth = async (req) => {
  const { body, session, user = {} } = req
  const appHealthData = prepareAppHealthData(body, user)
  const createdAppHealth = await AppHealthCollection.create(
    Array.isArray(appHealthData) ? appHealthData : [appHealthData],
    session
  )
  return createdAppHealth
}

export const reRunApphealth = async (req) => {
  const startOfToday = (await appHelper.getActualDate('', true, new Date()))
    .startOf('day')
    .toDate()
  await AppHealthCollection.remove({
    createdAt: {
      $gte: startOfToday
    }
  })
  await appQueueService.removeAppQueueItems({
    destination: 'app-health',
    createdAt: {
      $gte: startOfToday
    }
  })
  const appQueue = await appQueueService.createAnAppQueue(
    {
      event: 'initiate_app_health',
      action: 'initiate_app_health',
      destination: 'app-health',
      priority: 'regular',
      params: {
        workerType: 'app_health_re_run'
      }
    },
    req.session
  )
  return appQueue
}

export const prepareAppHealthUpdateData = (body) => {
  const updateData = {}
  const { updateType, data } = body
  delete body.updateType
  if (updateType === 'payoutError') {
    const { contractId, payoutError, missingAmount } = data
    updateData[`errorDetails.0.payoutError.contractId_${contractId}`] =
      payoutError
    updateData.$set = data
    updateData.$inc = { missingAmount }
  } else {
    updateData.$set = data
  }
  return updateData
}

export const updateAppHealth = async (req) => {
  const { body, session } = req
  const { _id } = body
  const updateData = prepareAppHealthUpdateData(body)
  const updatedAppHealth = await AppHealthCollection.findOneAndUpdate(
    { _id },
    updateData,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  return updatedAppHealth
}

export const prepareAppHealthRemoveQuery = async (body) => {
  const { removeType } = body
  if (removeType === 'removeForReRunAppHealth') {
    return {
      createdAt: {
        $gte: (await appHelper.getActualDate('', true)).startOf('day').toDate(),
        $lte: (await appHelper.getActualDate('', true)).endOf('day').toDate()
      }
    }
  }
  return {
    createdAt: {
      $lte: (await appHelper.getActualDate('', true))
        .subtract(7, 'days')
        .endOf('day')
        .toDate()
    }
  }
}

export const removeAppHealth = async (req) => {
  const { body, session } = req
  const query = await prepareAppHealthRemoveQuery(body)
  const removedAppHealth = await AppHealthCollection.deleteMany(query, {
    session
  })
  if (size(removedAppHealth)) {
    const numberOfDelete = removedAppHealth.n ? removedAppHealth.n : 0
    return { numberOfDelete }
  }
  return null
}
