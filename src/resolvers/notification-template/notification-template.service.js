import { size, map, omit, uniq, union, pick } from 'lodash'
import { CustomError } from '../common'
import { NotificationTemplateCollection } from '../models'
import { appHelper, notificationTemplateHelper, ruleHelper } from '../helpers'
import { ruleService } from '../services'

export const createANotificationTemplate = async (data, session) => {
  const response = await NotificationTemplateCollection.create([data], {
    session
  })
  return response
}

export const updateANotificationTemplate = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await NotificationTemplateCollection.findOneAndUpdate(
    query,
    data,
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return response
}

export const removeANotificationTemplate = async (query, session) => {
  const response = await NotificationTemplateCollection.findOneAndDelete(
    query,
    { session }
  )
  return response
}

export const processDataAndCreateARule = async (params, session) => {
  const { partnerId, type, uniqueId } = params
  const templateUniqueId = uniqueId
  let templateUniqueIds = []
  if (type === 'attachment') {
    const templates = await NotificationTemplateCollection.find({
      type: 'email',
      attachments: { $in: [templateUniqueId] }
    }).session(session)
    const filteredUniqueId = map(templates, 'uniqueId')
    const templateIdsForAttachment = uniq(filteredUniqueId)
    templateUniqueIds = union(templateUniqueIds, templateIdsForAttachment)
  } else {
    templateUniqueIds.push(templateUniqueId)
  }
  // If exists any rules, then copy rules for partner settings
  if (size(templateUniqueIds)) {
    await ruleService.processAndCreateARule(
      partnerId,
      templateUniqueIds,
      session
    )
  }
}

export const createNotificationTemplate = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  notificationTemplateHelper.validateTemplateAddData(body)
  const templateData = notificationTemplateHelper.prepareTemplateAddData(
    user,
    body
  )
  let templates = await createANotificationTemplate(templateData)
  if (size(templates)) {
    templates = JSON.parse(JSON.stringify(templates))
    templates[0].attachments =
      await notificationTemplateHelper.getNotificationTemplates({
        uniqueId: { $in: templates[0].attachments }
      })
    templates[0].isDeletable =
      await notificationTemplateHelper.checkIsDeletable(
        templates[0],
        user.partnerId
      )
    console.log(
      `--- Created Notification-Template for Id: ${templates[0]._id} ---`
    )
    return templates
  }
}

export const cloneNotificationTemplate = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkPartnerId(user, body)
  notificationTemplateHelper.validateTemplateCloneData(body)
  const { partnerId } = body
  const templateData =
    await notificationTemplateHelper.prepareTemplateCloneData(body, session)
  const templates = await createANotificationTemplate(templateData, session)
  if (size(templates)) {
    console.log(
      `--- Created Notification-Template for Id: ${templates[0]._id} ---`
    )
    const updatedDefaultTemplate = await updateANotificationTemplate(
      { _id: body._id },
      { $addToSet: { copiedBy: partnerId } },
      session
    )
    if (size(updatedDefaultTemplate)) {
      console.log(
        `--- Updated a default Notification-Template with _id: ${updatedDefaultTemplate._id} ---`
      )
    }
    return templates
  }
}

export const updateRuleAfterNotificationTemplateInsertion = async (
  template,
  session
) => {
  const partnerId =
    size(template) && template.partnerId ? template.partnerId : ''
  if (partnerId) {
    const templateUniqueId = template.uniqueId
    let templateUniqueIds = []

    if (template.type === 'attachment') {
      templateUniqueIds =
        await notificationTemplateHelper.getDistinctNotificationTemplates(
          'uniqueId',
          {
            type: 'email',
            attachments: { $in: [templateUniqueId] }
          },
          session
        )
    } else {
      templateUniqueIds.push(templateUniqueId)
    }

    //If exists any rules, then copy rules for partner settings
    if (size(templateUniqueIds)) {
      const rules = await ruleHelper.getRules({
        partnerId: { $exists: false },
        notifyTo: {
          $elemMatch: { templateUniqueId: { $in: templateUniqueIds } }
        }
      })
      console.log('=== Rules', JSON.stringify(rules))
      if (size(rules)) {
        for (let i = 0; i < rules.length; i++) {
          const { _id, country, event } = rules[i]
          console.log('rulesId', _id)
          console.log('rulesCountry', country)
          console.log('rulesEvent', event)
          const isExistsRule = await ruleHelper.getRule({
            partnerId,
            country,
            event,
            notifyTo: {
              $elemMatch: { templateUniqueId: { $in: templateUniqueIds } }
            }
          })
          console.log('=== isExistsRule', isExistsRule)
          //If partner rule settings isn't exit, then copy main rule setting to partner rule settings
          if (!isExistsRule) {
            //copy admin rule and insert for partner
            const pickedRules = pick(rules[i], [
              'country',
              'event',
              'notifyTo',
              'status',
              'todoNotifyTo'
            ])
            pickedRules.partnerId = partnerId
            console.log('=== pickedRules', pickedRules)
            const createdRules = await ruleService.createRule(
              pickedRules,
              session
            )
            console.log('=== createdRules', createdRules)
          }
        }
      }
    }
  }
}

const customUpdateNotificationTemplate = async (params, session) => {
  const { updateData, userId, requiredFields } = params
  console.log('=== params', params)
  const { _id, isCopyForPartner, partnerId } = requiredFields
  console.log('=== requiredFields', requiredFields)

  if (_id && size(updateData)) {
    if (isCopyForPartner) {
      const [newTemplate] = await createANotificationTemplate({
        ...omit(updateData, ['_id', 'copiedBy', 'createdAt', 'updatedAt']),
        createdBy: userId
      })
      if (!size(newTemplate)) {
        throw new CustomError(400, 'Notification template not created')
      }
      await updateRuleAfterNotificationTemplateInsertion(newTemplate, session)

      const newTemplateId = newTemplate._id

      if (newTemplateId) {
        console.log('=== newTemplateId found', newTemplateId)
        const updatedTemplate = await updateANotificationTemplate(
          { _id },
          { $addToSet: { copiedBy: partnerId } },
          session
        )
        if (!size(updatedTemplate))
          throw new CustomError(400, 'Notification template not updated')
        return newTemplate
      }
    } else {
      console.log('=== isCopyForPartner is false ===')
      const result = await updateANotificationTemplate(
        { _id },
        updateData,
        session
      )
      return result
    }
  } else {
    throw new CustomError(400, 'Missing data')
  }
}

export const updateNotificationTemplate = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  if (user.partnerId) {
    body['partnerId'] = user.partnerId
  }
  notificationTemplateHelper.validateTemplateUpdateData(body)
  const { updateData, _id, isCopyForPartner } =
    await notificationTemplateHelper.prepareTemplateUpdateData(body, session)
  console.log('=== _id', _id)
  console.log('=== isCopyForPartner', isCopyForPartner)
  console.log('=== UpdatingData', updateData)
  const { partnerId } = body
  const requiredFields = { _id, isCopyForPartner }
  if (partnerId) requiredFields.partnerId = partnerId
  const updatedTemplate = await customUpdateNotificationTemplate(
    {
      userId: user.userId,
      requiredFields,
      updateData
    },
    session
  )
  if (size(updatedTemplate)) {
    console.log(
      `--- Updated a Notification-Template with _id: ${updatedTemplate._id} ---`
    )
    return updatedTemplate
  }
  console.log('=== Updated template', updatedTemplate)
}

const removePartnerTemplate = async (params, session) => {
  const { template, _id } = params
  const { isCustom, type, partnerId, uniqueId } = template
  const query = {
    uniqueId,
    copiedBy: { $in: [partnerId] },
    partnerId: { $exists: false }
  }
  const data = { $pull: { copiedBy: partnerId } }
  const updatedTemplate = await updateANotificationTemplate(
    query,
    data,
    session
  )

  if (size(updatedTemplate)) {
    const removedTemplate = await removeANotificationTemplate(
      { _id, partnerId },
      session
    )

    if (size(removedTemplate) && size(updatedTemplate)) {
      return { _id }
    }
  } else if ((type === 'attachment' || type === 'pdf') && isCustom) {
    const removedTemplate = await removeANotificationTemplate(
      { _id, partnerId },
      session
    )
    if (size(removedTemplate)) return { _id }
  } else {
    throw new CustomError(
      405,
      `Could not delete template with _id: ${_id}, parent template is not found to update`
    )
  }
}

const removeDefaultTemplate = async (params, session) => {
  const { template, _id } = params
  const {
    isAnnualStatement,
    isChatNotification,
    isCreditNote,
    isInvoice,
    type,
    uniqueId
  } = template
  let query = {}

  if (type === 'attachment') {
    query['$or'] = [{ uniqueId }, { attachments: { $in: [uniqueId] } }]
  } else {
    query['uniqueId'] = uniqueId
  }

  const countedTemplates =
    await notificationTemplateHelper.countNotificationTemplates(query, session)
  const countedRules = await ruleHelper.getNumberOfRuleByUniqueId(
    uniqueId,
    session
  )

  let isRemove = true
  if (isAnnualStatement || isChatNotification || isCreditNote || isInvoice) {
    isRemove = false
  }

  // If the attachment template isn't use into others email template
  // If the template isn't update by partner
  if (countedTemplates === 1 && isRemove && !countedRules) {
    query = { _id, partnerId: { $exists: false } }
    const removedTemplate = await removeANotificationTemplate(query, session)
    if (size(removedTemplate)) {
      return { _id }
    }
  } else {
    throw new CustomError(
      405,
      `Could not delete template with _id: ${_id}, it is being used`
    )
  }
}

export const removeNotificationTemplate = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['_id'], body)

  const { _id } = body
  const { partnerId } = user
  appHelper.validateId({ _id })
  const query = { _id }
  if (partnerId) {
    appHelper.validateId({ partnerId })
  }

  const template = await notificationTemplateHelper.getNotificationTemplate(
    query,
    session
  )

  if (!size(template)) {
    throw new CustomError(
      404,
      `Could not find template with _id: ${_id}${
        partnerId ? ' and partnerId: ' + partnerId : ''
      }`
    )
  }

  body.template = template
  let result
  if (partnerId) {
    result = await removePartnerTemplate(body, session) // By partner or app admin
  } else {
    result = await removeDefaultTemplate(body, session) // By app admin only
  }
  if (size(result)) {
    console.log(`--- Deleted Notification Template ${_id} ---`)
    return result
  }
}
