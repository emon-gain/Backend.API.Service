import { isEmpty } from 'lodash'
import { CustomError } from '../common'
import { LambdaSqsCollection } from '../models'
import { appHelper } from '../helpers'

export const prepareInsertData = (body, header) => {
  const insertData = body
  const { userId } = header
  if (userId) {
    insertData.createdBy = userId
  }
  return insertData
}

export const createLambdaSqs = async (req) => {
  const { body, session, user = {} } = req
  const insertData = prepareInsertData(body, user)
  const lambdaSqs = await LambdaSqsCollection.create([insertData], { session })
  return lambdaSqs
}

export const updateALambdaSqs = async (query, data, session) => {
  if (isEmpty(data)) {
    throw new CustomError(404, 'No data found to update LambdaSqs')
  }
  const updatedLambdaSqs = await LambdaSqsCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (isEmpty(updatedLambdaSqs)) {
    throw new CustomError(404, `Unable to update LambdaSqs`)
  }

  return updatedLambdaSqs
}

export const prepareRemoveQuery = (body) => {
  const { _id, removeType } = body
  if (removeType === 'migration') {
    return {}
  }
  return { _id }
}

export const validateDeleteRequest = (body) => {
  const { _id, removeType } = body
  if (!removeType && !_id) {
    throw new CustomError(400, 'Bad request! removeType or sqsId is required')
  }
  return true
}

export const removeLambdaSqs = async (req) => {
  const { body, session } = req
  validateDeleteRequest(body)
  const query = prepareRemoveQuery(body)
  const removedLambdaSqs = await LambdaSqsCollection.deleteMany(query, {
    session
  })
  return removedLambdaSqs && removedLambdaSqs.n
    ? { numberOfRemove: removedLambdaSqs.n }
    : null
}

export const removeALambdaSqs = async (query, session) => {
  const response = await LambdaSqsCollection.findOneAndDelete(query, {
    session
  })
  if (!response) throw new CustomError(404, "LambdaSqs doesn't exists")
  return response
}

export const removeLambdaSqsById = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['_id'], body)
  const { _id = '' } = body
  appHelper.validateId({ _id })
  const removedLambdaSqs = await removeALambdaSqs({ _id }, session)
  return removedLambdaSqs
}

export const updateLambdaSqsStatus = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['_id', 'status'], body)
  const { _id = '', status = '' } = body
  appHelper.validateId({ _id })
  if (!status) throw new CustomError(400, 'Required status')

  const data = { status }
  if (status === 'processing') data.processStartedAt = new Date()

  const updatedLambdaSqs = await updateALambdaSqs(
    { _id },
    { $set: data },
    session
  )
  return updatedLambdaSqs
}
