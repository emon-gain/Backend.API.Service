import { size } from 'lodash'
import nid from 'nid'
import { PartnerUsageCollection } from '../models'
import {
  appHelper,
  listingHelper,
  partnerUsageHelper,
  settingHelper
} from '../helpers'
import { CustomError } from '../common'

export const createAPartnerUsage = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, `No data found to create partner usage`)
  }
  const partnerUsage = await PartnerUsageCollection.create([data], { session })
  return partnerUsage
}

export const createPartnerUsages = async (data = [], session) => {
  if (!size(data)) {
    throw new CustomError(404, `No data found to create partner usage`)
  }
  data.forEach((element) => {
    element._id = nid(17)
  })
  const partnerUsages = await PartnerUsageCollection.insertMany(data, {
    session
  })
  if (!size(partnerUsages)) {
    throw new CustomError(404, `Unable to create partner usage`)
  }
  return partnerUsages
}

export const createPartnerUsage = async (req) => {
  const { body, session, user = {} } = req
  const insertData = partnerUsageHelper.prepareInsertData(body, user)
  const partnerUsage = await PartnerUsageCollection.create([insertData], {
    session
  })
  return partnerUsage
}

export const addPartnerUsageData = async (req) => {
  const { body, session, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  const partnerUsagesData = []
  const setting = await settingHelper.getSettingInfo()
  const parkingId = listingHelper.getListingTypeIdByName('parking', setting)
  const parkingLotsData =
    await partnerUsageHelper.preparePartnerUsagesDataForTotalActiveParkingLots(
      partnerId,
      parkingId
    )
  partnerUsagesData.push(...parkingLotsData)
  const activePropertiesData =
    await partnerUsageHelper.preparePartnerUsagesDataForTotalActiveProperties(
      partnerId,
      parkingId
    )
  partnerUsagesData.push(...activePropertiesData)
  const activeUsersData =
    await partnerUsageHelper.preparePartnerUsagesDataForTotalActiveUsers(
      partnerId
    )
  partnerUsagesData.push(...activeUsersData)
  const activeAgentsData =
    await partnerUsageHelper.preparePartnerUsagesDataForTotalActiveAgents(
      partnerId,
      parkingId
    )
  partnerUsagesData.push(...activeAgentsData)
  if (size(partnerUsagesData)) {
    await createPartnerUsages(partnerUsagesData, session)
  }
  return {
    result: true
  }
}
