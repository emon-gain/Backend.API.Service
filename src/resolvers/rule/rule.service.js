import { pick, size } from 'lodash'
import { CustomError } from '../common'
import { RuleCollection } from '../models'
import { appHelper, ruleHelper } from '../helpers'

export const createARule = async (data, session) => {
  const [response] = await RuleCollection.create([data], { session })
  await response.populate('notifyTo.templateInfo').execPopulate()

  return [response]
}

export const createRule = async (data, session) => {
  const [response] = await RuleCollection.create([data], { session })
  return response
}

export const updateARule = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update rule')
  }
  const response = await RuleCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  if (size(response)) {
    console.log(
      '=== Rule updating record:',
      'Query',
      JSON.stringify(query),
      'Data',
      JSON.stringify(data),
      'Response',
      JSON.stringify(response)
    )
  }
  await response.populate('notifyTo.templateInfo').execPopulate()
  if (!size(response)) throw new CustomError(404, `Unable to update rule`)

  return response
}

export const removeARule = async (params, session) => {
  const response = await RuleCollection.findOneAndRemove(params, {
    new: true,
    session
  })
  if (!size(response)) throw new CustomError(404, 'Could not delete rule')
  return response
}

export const createOrUpdateARuleToAddNotificationSettingForPartnerAdmin =
  async (params, ruleInfo, session) => {
    const { createdBy, notifyTo, todoNotifyTo = [], partnerId } = params
    if (!ruleInfo.partnerId || ruleInfo.partnerId !== partnerId) {
      // Create a rule for partner admin
      const data = pick(ruleInfo, [
        'event',
        'notifyTo',
        'status',
        'country',
        'todoNotifyTo'
      ])
      data.partnerId = partnerId
      data.createdBy = createdBy
      data.notifyTo.push(notifyTo)
      if (size(todoNotifyTo)) data.todoNotifyTo = todoNotifyTo
      const createdRule = await createARule(data, session)
      return createdRule
    } else if (ruleInfo.partnerId && ruleInfo.partnerId === partnerId) {
      // Update a rule for partner admin
      const query = { _id: ruleInfo._id, partnerId }
      const newNotifyToData = [...ruleInfo.notifyTo]
      newNotifyToData.push(notifyTo)
      const data = { notifyTo: newNotifyToData }
      if (size(todoNotifyTo)) data.todoNotifyTo = todoNotifyTo
      const updatedRule = await updateARule(query, data, session)
      return [updatedRule]
    }
  }

export const createOrUpdateARuleToAddNotificationSetting = async (
  params,
  session
) => {
  const {
    createdBy,
    country,
    event,
    notifyTo,
    todoNotifyTo = [],
    partnerId = ''
  } = params
  // Getting existing ruleInfo
  const { ruleInfo } = await ruleHelper.getExistingRuleInfo(params, session)
  // NotifyTo data won't be duplicated
  if (size(ruleInfo) && size(ruleInfo.notifyTo))
    ruleHelper.isNotifyToDataAlreadyExists(ruleInfo.notifyTo, notifyTo)

  // Should update or create a rule for partner admin
  if (partnerId && size(ruleInfo)) {
    const createdOrUpdatedRule =
      await createOrUpdateARuleToAddNotificationSettingForPartnerAdmin(
        params,
        ruleInfo,
        session
      )
    return createdOrUpdatedRule
  }
  // Should update a rule for app admin
  else if (!partnerId && size(ruleInfo)) {
    // Updating ruleInfo
    const query = { _id: ruleInfo._id }
    const newNotifyToData = [...ruleInfo.notifyTo]
    newNotifyToData.push(notifyTo)
    const data = { notifyTo: newNotifyToData }
    if (size(todoNotifyTo)) data.todoNotifyTo = todoNotifyTo
    const updatedRule = await updateARule(query, data, session)
    return [updatedRule]
  }
  // Should create a rule for app admin or partner admin
  else if (!size(ruleInfo)) {
    // Creating a rule
    const data = {
      status: 'active',
      event,
      country,
      notifyTo,
      createdBy
    }
    if (partnerId) data.partnerId = partnerId
    if (size(todoNotifyTo)) data.todoNotifyTo = todoNotifyTo
    const createdRule = await createARule(data, session)
    return createdRule
  }
}

export const addNotificationSetting = async (req) => {
  const { body = {}, session, user = {} } = req
  const { userId = '', partnerId } = user
  appHelper.checkUserId(userId)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  ruleHelper.validateNotificationSettingCreationData(body)
  body.createdBy = userId
  console.log(`====> Rule add mutation, user: ${JSON.stringify(user)} <====`)
  const createdOrUpdatedRule =
    await createOrUpdateARuleToAddNotificationSetting(body, session)
  console.log(
    `====> Rule add mutation, createdOrUpdatedRule: ${JSON.stringify(
      createdOrUpdatedRule
    )} <====`
  )
  return createdOrUpdatedRule
}

export const createOrUpdateARuleToRemoveNotificationSetting = async (
  params,
  userId,
  session
) => {
  const { _id, notifyTo, partnerId = '' } = params
  // Getting existing ruleInfo
  const ruleInfo = await ruleHelper.getRule({ _id }, session)
  if (size(ruleInfo)) {
    // Create a rule for partner admin, with default configuration of app admin
    if (
      partnerId &&
      (!ruleInfo.partnerId || ruleInfo.partnerId !== partnerId)
    ) {
      const { country, event, status, todoNotifyTo = [] } = ruleInfo
      const { data } =
        ruleHelper.prepareNotifyToArrayToRemoveNotificationSetting(
          ruleInfo.notifyTo,
          notifyTo
        )
      data.partnerId = partnerId
      data.country = country
      data.event = event
      data.status = status
      data.todoNotifyTo = todoNotifyTo
      data.createdBy = userId
      // Create a rule for partner admin
      const createdRule = await createARule(data, session)
      return createdRule[0]
    }
    // Update notifyTo array for both app or partner Admin
    else if (
      (partnerId && ruleInfo.partnerId && ruleInfo.partnerId === partnerId) ||
      !(partnerId && ruleInfo.partnerId)
    ) {
      const query = { _id: ruleInfo._id }
      if (partnerId) query.partnerId = partnerId

      const { data } =
        ruleHelper.prepareNotifyToArrayToRemoveNotificationSetting(
          ruleInfo.notifyTo,
          notifyTo
        )
      // Updating a rule for app admin or partner admin
      const updatedRule = await updateARule(query, data, session)
      return updatedRule
    }
  } else throw new CustomError(404, 'Rule does not exists')
}

export const removeNotificationSetting = async (req) => {
  const { body = {}, session, user = {} } = req
  const { userId = '', partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  ruleHelper.validateNotificationSettingDeletionData(body)
  console.log(`====> Rule remove mutation, user: ${JSON.stringify(user)} <====`)
  const createdOrUpdatedRule =
    await createOrUpdateARuleToRemoveNotificationSetting(body, userId, session)
  console.log(
    `====> Rule remove mutation, createdOrUpdatedRule: ${JSON.stringify(
      createdOrUpdatedRule
    )} <====`
  )
  return createdOrUpdatedRule
}

export const createOrUpdateARuleToUpdateNotificationSettingForPartnerAdmin =
  async (params, ruleInfo, session) => {
    const { createdBy, todoNotifyTo, partnerId } = params
    if (!ruleInfo.partnerId || ruleInfo.partnerId !== partnerId) {
      // Create a rule for partner admin
      const data = pick(ruleInfo, ['event', 'notifyTo', 'status', 'country'])
      data.partnerId = partnerId
      data.createdBy = createdBy
      data.todoNotifyTo = todoNotifyTo

      const createdRule = await createARule(data, session)
      return createdRule[0]
    } else if (ruleInfo.partnerId && ruleInfo.partnerId === partnerId) {
      // Update a rule for partner admin
      const query = { _id: ruleInfo._id, partnerId }
      const data = { todoNotifyTo }

      const updatedRule = await updateARule(query, data, session)
      return updatedRule
    }
  }

export const createOrUpdateARuleToUpdateNotificationSetting = async (
  params,
  session
) => {
  const { createdBy, country, event, partnerId = '', todoNotifyTo } = params

  // Getting existing ruleInfo
  const { ruleInfo } = await ruleHelper.getExistingRuleInfo(params, session)
  // Should update or create a rule for partner admin
  if (partnerId && size(ruleInfo)) {
    const createdOrUpdatedRule =
      await createOrUpdateARuleToUpdateNotificationSettingForPartnerAdmin(
        params,
        ruleInfo,
        session
      )
    return createdOrUpdatedRule
  }
  // Should update todoNotifyTo of a rule for app admin
  else if (!partnerId && size(ruleInfo)) {
    // Updating ruleInfo
    const query = { _id: ruleInfo._id }
    const data = { todoNotifyTo }
    const updatedRule = await updateARule(query, data, session)
    return updatedRule
  }
  // Should create a rule for app admin or partner admin
  else if (!size(ruleInfo)) {
    // Creating a rule
    const data = {
      event,
      country,
      todoNotifyTo,
      status: 'active',
      notifyTo: [],
      createdBy
    }
    if (partnerId) data.partnerId = partnerId
    const createdRule = await createARule(data, session)
    return createdRule[0]
  }
}

export const updateNotificationSetting = async (req) => {
  const { body = {}, session, user = {} } = req
  const { partnerId, userId = '' } = user
  appHelper.checkUserId(userId)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  ruleHelper.validateNotificationSettingUpdatingData(body)
  body.createdBy = userId
  console.log(`====> Rule update mutation, user: ${JSON.stringify(user)} <====`)
  const createdOrUpdatedRule =
    await createOrUpdateARuleToUpdateNotificationSetting(body, session)
  console.log(
    `====> Rule update mutation, createdOrUpdatedRule: ${JSON.stringify(
      createdOrUpdatedRule
    )} <====`
  )
  return createdOrUpdatedRule
}

export const resetNotificationAndGetDefaultRule = async (params, session) => {
  await removeARule(params, session)
  params.partnerId = { $exists: false }
  return await ruleHelper.getRule(params)
}

export const resetNotificationSetting = async (req) => {
  const { body = {}, session, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  appHelper.checkRequiredFields(['event'], body)
  body.partnerId = partnerId
  console.log(`====> Rule reset mutation, user: ${JSON.stringify(user)} <====`)
  const response = await resetNotificationAndGetDefaultRule(body, session)
  console.log(
    `====> Rule reset mutation, response: ${JSON.stringify(response)} <====`
  )
  return response
}

export const processAndCreateARule = async (
  partnerId,
  templateUniqueIds,
  session
) => {
  const RulesQuery = {
    partnerId: { $exists: false },
    notifyTo: { $elemMatch: { templateUniqueId: { $in: templateUniqueIds } } }
  }
  const rules = await ruleHelper.getRules(RulesQuery, session)
  if (size(rules)) {
    for (const ruleInfo of rules) {
      const ruleQuery = {
        partnerId,
        country: ruleInfo.country,
        event: ruleInfo.event,
        notifyTo: {
          $elemMatch: { templateUniqueId: { $in: templateUniqueIds } }
        }
      }
      const isExistsRule = await ruleHelper.getRule(ruleQuery, session)
      // If partner rules settings isn't exit, then copy main rules setting to partner rules settings
      if (!isExistsRule) {
        delete ruleInfo._id
        ruleInfo.partnerId = partnerId
        // Copy admin rules and insert for partner
        const response = await createARule(ruleInfo, session)
        if (size(response)) {
          console.log(`--- Created Rule for Id: ${response[0]._id} ---`)
        }
      }
    }
  }
}
