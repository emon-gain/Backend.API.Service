import { LambdaSqsCollection } from '../models'
import { appHelper } from '../helpers'

export const getLambdaSqsCount = async (query, session) => {
  const lambdaSqs = await LambdaSqsCollection.find(query)
    .session(session)
    .countDocuments()
  return lambdaSqs
}

export const getALambdaSqs = async (query, session) => {
  const lambdaSqs = await LambdaSqsCollection.findOne(query).session(session)
  return lambdaSqs
}

export const getLambdaSqsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const lambdaSqs = await LambdaSqsCollection.find(query)
    .populate(['partner'])
    .limit(limit)
    .skip(skip)
    .sort(sort)

  return lambdaSqs
}

export const queryLambdaSqs = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const lambdaSqs = await getLambdaSqsForQuery(body)
  const filteredDocuments = await getLambdaSqsCount(query)
  const totalDocuments = await getLambdaSqsCount({})
  return {
    data: lambdaSqs,
    metaData: { filteredDocuments, totalDocuments }
  }
}
