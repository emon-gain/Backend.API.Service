import { size } from 'lodash'
import { SettingCollection } from '../models'
import { appHelper } from '../helpers'
import { CustomError } from '../common'

export const getSettingsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const settings = await SettingCollection.find(query)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return settings
}

export const countSettings = async (query, session) => {
  const numberOfSettings = await SettingCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfSettings
}

export const querySettings = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  let settings = await getSettingsForQuery(body)
  const filteredDocuments = await countSettings(query)
  const totalDocuments = await countSettings({})
  settings = JSON.parse(JSON.stringify(settings))
  return {
    data: settings,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getSettingDataForLambda = async () => {
  const settingData = await getSettingInfo({})
  const partyHabit = settingData.getIsDefaultPartyHabits()
  if (!size(partyHabit)) {
    throw new CustomError(400, 'Party habit not found')
  }
  const keepSpaceHabit = settingData.getIsDefaultKeepingSpaceHabits()
  if (!size(keepSpaceHabit)) {
    throw new CustomError(400, 'Keep space habit not found')
  }
  return { partyHabitId: partyHabit.id, keepSpaceHabitId: keepSpaceHabit.id }
}

export const getSettingForPublicApp = async () => await getSettingInfo({})

export const getSettingInfo = async (query = {}, session) => {
  const settingData = await SettingCollection.findOne(query).session(session)
  return settingData
}

export const prepareAppInfoUpdatingDataAndQuery = async (params, session) => {
  const { name, value } = params
  let updatingData = {}
  if (!name) throw new CustomError(400, 'name is required')
  const dataObj = {
    appName: { 'appInfo.appName': value },
    appSlogan: { 'appInfo.appSlogan': value },
    appCompany: { 'appInfo.companyName': value },
    appOrganizationId: { 'appInfo.organizationId': value },
    appAddress: { 'appInfo.address': value },
    appPhone: { 'appInfo.phoneNumber': value },
    appEmail: { 'appInfo.email': value },
    appWebsite: { 'appInfo.website': value },
    bankAccountNumber: { bankAccountNumber: value }
  }
  if (dataObj[name]) updatingData = dataObj[name]
  if (!size(updatingData))
    throw new CustomError(400, 'Invalid name for app info update')
  const settingInfo = await getSettingInfo({}, session)
  if (!size(settingInfo)) throw new CustomError(404, "Setting doesn't exist")
  const query = { _id: settingInfo._id }
  return { query, updatingData }
}

export const prepareExternalLinksUpdatingDataAndQuery = async (
  params,
  session
) => {
  const { name, value } = params
  let updatingData = {}
  if (!name) throw new CustomError(400, 'name is required')
  const dataObj = {
    linkedInSite: { 'externalLinks.linkedIn': value },
    facebookSite: { 'externalLinks.facebook': value },
    twitterSite: { 'externalLinks.twitter': value },
    instagramSite: { 'externalLinks.instagram': value },
    googlePlusSite: { 'externalLinks.googlePlus': value }
  }
  if (dataObj[name]) updatingData = dataObj[name]
  if (!size(updatingData))
    throw new CustomError(400, 'Invalid name for external links update')
  const settingInfo = await getSettingInfo({}, session)
  if (!size(settingInfo)) throw new CustomError(404, "Setting doesn't exist")
  const query = { _id: settingInfo._id }
  return { query, updatingData }
}

export const prepareOpenExchangeDataAndQuery = async (params, session) => {
  const { base, rates } = params
  let updatingData = {}
  if (!base) throw new CustomError(400, 'base is required')
  if (rates.length === 0) throw new CustomError(400, 'rates is empty')
  updatingData = {
    openExchangeInfo: params
  }
  const settingInfo = await getSettingInfo({}, session)
  if (!size(settingInfo)) throw new CustomError(404, "Setting doesn't exist")
  const query = { _id: settingInfo._id }
  return { query, updatingData }
}
