import { size } from 'lodash'
import { appHelper } from '../helpers'
import { BlockItemCollection } from '../models'

export const getTemplateBlockItemsForQuery = async (body) => {
  const { query, options } = body
  const { limit, skip, sort } = options
  const templateBlockItems = await BlockItemCollection.find(query)
    .sort(sort)
    .skip(skip)
    .limit(limit)
  return templateBlockItems
}

export const countTemplateBlockItems = async (query) => {
  const numberOfTemplateBlockItem = await BlockItemCollection.find(
    query
  ).countDocuments()
  return numberOfTemplateBlockItem
}

export const validateTemplateBlockItemQueryData = (query) => {
  const { _id, category } = query
  if (_id) {
    appHelper.validateId({ _id })
  }
  query.partnerId = { $exists: false }
  if (size(category)) query.category = { $in: [category, 'common'] }
  else query.category = 'common'
  return query
}

export const queryTemplateBlockItems = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = validateTemplateBlockItemQueryData(query)
  const templateBlockItemData = await getTemplateBlockItemsForQuery(body)
  const filteredDocuments = await countTemplateBlockItems(body.query)
  const totalDocuments = await countTemplateBlockItems({})
  return {
    data: templateBlockItemData,
    metaData: { filteredDocuments, totalDocuments }
  }
}
