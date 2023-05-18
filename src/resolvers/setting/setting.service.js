import { size } from 'lodash'
import { appPermission, CustomError } from '../common'
import { SettingCollection } from '../models'
import { appHelper, settingHelper } from '../helpers'

export const updateASetting = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await SettingCollection.findOneAndUpdate(
    query,
    { $set: data },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return response
}

export const updateASettingForUpgradeScripts = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  console.log(data)
  const response = await SettingCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  return response
}

export const updateAppInfo = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  await appHelper.checkUserId(userId)
  if (!(await appPermission.isAppAdmin(userId)))
    throw new CustomError(401, 'Unauthorized')
  appHelper.checkRequiredFields(['data'], body)
  const { data } = body
  const { query, updatingData } =
    await settingHelper.prepareAppInfoUpdatingDataAndQuery(data, session)
  const updatedSetting = await updateASetting(query, updatingData, session)
  return updatedSetting
}

export const updateExternalLinks = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  if (!(await appPermission.isAppAdmin(userId)))
    throw new CustomError(401, 'Unauthorized')

  appHelper.checkRequiredFields(['data'], body)
  const { data } = body
  const { query, updatingData } =
    await settingHelper.prepareExternalLinksUpdatingDataAndQuery(data, session)
  const updatedSetting = await updateASetting(query, updatingData, session)
  return updatedSetting
}

export const updateOpenExchange = async (req) => {
  const { body, session, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['base', 'rates'], body)
  const { query, updatingData } =
    await settingHelper.prepareOpenExchangeDataAndQuery(body, session)
  const updatedSetting = await updateASetting(query, updatingData, session)
  if (!updatedSetting) throw new CustomError(400, 'Settings could not updated')
  else {
    return {
      response: true
    }
  }
}

export const updateSettingsForUpgradeScripts = async (req) => {
  const { body } = req
  const { set, addToSet } =
    prepareInputDataForUpdateSettingForUpgradeScripts(body)
  console.log(set)
  console.log(addToSet)
  const updatedSetting = await updateASettingForUpgradeScripts(
    {},
    { $set: set, $addToSet: addToSet }
  )
  console.log(updatedSetting)
  if (!updatedSetting) throw new CustomError(400, 'Settings could not updated')
  else {
    return {
      response: true
    }
  }
}

const prepareInputDataForUpdateSettingForUpgradeScripts = (body) => {
  const set = {}
  const addToSet = {}
  console.log('Body======>', body)
  if (body.newVersion) {
    set.version = body.newVersion
  }
  if (body.reset) {
    set.upgradeScripts = []
  }
  if (body.methodName) {
    addToSet.upgradeScripts = body.methodName
  }
  if (body.runningDBUpgrade) {
    set.runningDBUpgrade = true
  } else if (!body.runningDBUpgrade) set.runningDBUpgrade = false
  return { set, addToSet }
}
