import { find, isBoolean, isInteger, remove, size } from 'lodash'
import { appHelper, partnerSettingHelper } from '../helpers'
import { RuleCollection } from '../models'
import { CustomError } from '../common'

export const getRules = async (query, session) => {
  const rules = await RuleCollection.find(query).session(session)
  return rules
}

export const getRule = async (query, session) => {
  const rule = await RuleCollection.findOne(query).session(session)
  return rule
}

export const getNumberOfRuleByUniqueId = async (uniqueId, session) => {
  if (uniqueId) {
    const ruleQuery = {
      notifyTo: { $elemMatch: { templateUniqueId: uniqueId } }
    }
    const countedRules = await RuleCollection.find(ruleQuery)
      .session(session)
      .countDocuments()
    return countedRules
  }
}

export const getRuleForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const rules = await RuleCollection.find(query)
    .populate(['partner', 'notifyTo.templateInfo'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return rules
}

export const countRules = async (query, session) => {
  const numberOfRules = await RuleCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfRules
}

const prepareQueryDataForRulesQuery = (query) => {
  const { partnerId } = query
  if (!partnerId) {
    query.partnerId = { $exists: false }
  }
  return query
}

export const queryRules = async (req) => {
  const { body, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  const { query, options } = body
  // For Lambda query
  if (size(query.options)) {
    const rule = await getRuleByOptionsForLambda(query.options)
    return { data: [rule] }
  }
  // For Common query
  appHelper.validateSortForQuery(options.sort)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.query.partnerId = partnerId
  }
  body.query = prepareQueryDataForRulesQuery(body.query)
  const rulesData = await getRuleForQuery(body)
  const filteredDocuments = await countRules(query)
  const totalDocuments = await countRules({})
  return {
    data: rulesData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryRulesForPartnerAPP = async (req) => {
  const { body, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { query, options } = body

  appHelper.validateSortForQuery(options.sort)
  appHelper.validateId({ partnerId })
  body.query.partnerId = partnerId

  let rulesData = await getRuleForQuery(body) // Getting rules with partnerId

  if (!size(rulesData)) {
    // No rules found with partnerId, Then Getting default rules
    body.query.partnerId = { $exists: false }
    rulesData = await getRuleForQuery(body)
  }

  const filteredDocuments = await countRules(query)
  const totalDocuments = await countRules({})
  return {
    data: rulesData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getRuleByOptionsForLambda = async (options) => {
  const { event = '', partnerId = '' } = options
  const query = { event, status: 'active' }

  let rule = {}

  if (partnerId) {
    query.partnerId = partnerId
    rule = await getRule(query)
  }

  if (!size(rule)) {
    query.partnerId = { $exists: false }
    rule = await getRule(query)
  }

  if (size(rule)) return rule
  else throw new CustomError(404, "Rule doesn't exists")
}

export const checkRequiredFieldsOfNotifyToObj = (notifyTo) => {
  const requiredFields = ['id', 'type', 'templateUniqueId']
  appHelper.checkRequiredFields(requiredFields, notifyTo)
  const { id = '', type = '', templateUniqueId = '' } = notifyTo
  if (!id) throw new CustomError(400, 'Required id')
  if (!type) throw new CustomError(400, 'Required type')
  if (!templateUniqueId) throw new CustomError(400, 'Required templateUniqueId')
  appHelper.validateId({ templateUniqueId })
}

export const checkRequiredFieldsOfTodoNotifyToArray = (todoNotifyTo = []) => {
  const requiredFields = ['enabled', 'days', 'id']
  appHelper.checkRequiredFields(requiredFields, todoNotifyTo[0])
  const { enabled, days, id } = todoNotifyTo[0]
  if (!isBoolean(enabled)) throw new CustomError(400, 'Value must be a boolean')
  if (!isInteger(days)) throw new CustomError(400, 'Value must be an integer')
  if (!id) throw new CustomError(400, 'Required id')
}

export const validateNotificationSettingCreationData = (data = {}) => {
  const requiredFields = ['country', 'event', 'notifyTo']
  appHelper.checkRequiredFields(requiredFields, data)
  const { country = '', event = '', notifyTo = {}, todoNotifyTo = [] } = data

  if (!country) throw new CustomError(400, 'Required country')
  if (!event) throw new CustomError(400, 'Required event')

  checkRequiredFieldsOfNotifyToObj(notifyTo)

  if (size(todoNotifyTo)) checkRequiredFieldsOfTodoNotifyToArray(todoNotifyTo)
}

export const validateNotificationSettingDeletionData = (data = {}) => {
  appHelper.checkRequiredFields(['_id', 'notifyTo'], data)
  const { _id, notifyTo = {} } = data

  appHelper.validateId({ _id })
  checkRequiredFieldsOfNotifyToObj(notifyTo)
}

export const validateNotificationSettingUpdatingData = (params = {}) => {
  const requiredFields = ['country', 'event', 'todoNotifyTo']
  appHelper.checkRequiredFields(requiredFields, params)
  const { country = '', event = '', todoNotifyTo = [] } = params
  if (!country) throw new CustomError(400, 'Required country')
  if (!event) throw new CustomError(400, 'Required event')
  if (!size(todoNotifyTo)) throw new CustomError(400, 'Required todoNotifyTo')

  checkRequiredFieldsOfTodoNotifyToArray(todoNotifyTo)
}

export const getExistingRuleInfo = async (params, session) => {
  const { country, event, partnerId = '' } = params
  let ruleInfo = {}
  const query = { country, event }

  if (partnerId) {
    const partnerSetting = await partnerSettingHelper.getAPartnerSetting(
      { partnerId },
      session
    )
    query.country =
      partnerSetting && partnerSetting.country
        ? partnerSetting.country
        : country
    query.partnerId = partnerId

    ruleInfo = await getRule(query, session)
  }
  if (!partnerId || !size(ruleInfo)) {
    query.partnerId = { $exists: false }
    ruleInfo = await getRule(query, session)
  }

  return { ruleInfo }
}

export const isNotifyToDataAlreadyExists = (
  oldNotifyToData,
  newNotifyToData
) => {
  const duplicatedNotifyToData = find(
    oldNotifyToData,
    (notifyTo) =>
      notifyTo.type === newNotifyToData.type &&
      notifyTo.id === newNotifyToData.id &&
      notifyTo.templateUniqueId === newNotifyToData.templateUniqueId
  )
  if (size(duplicatedNotifyToData))
    throw new CustomError(405, 'This configuration already exists')
}

export const prepareNotifyToArrayToRemoveNotificationSetting = (
  oldNotifyToArray,
  notifyToObj
) => {
  const removedNotifyToArray = remove(
    oldNotifyToArray,
    (oldNotifyTo) =>
      oldNotifyTo.id === notifyToObj.id &&
      oldNotifyTo.type === notifyToObj.type &&
      oldNotifyTo.templateUniqueId === notifyToObj.templateUniqueId
  )
  if (!size(removedNotifyToArray))
    throw new CustomError(404, 'This configuration does not exists')
  const data = { notifyTo: oldNotifyToArray }
  return { data }
}
