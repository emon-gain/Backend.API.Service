import { map, size, uniq } from 'lodash'

import { CustomError } from '../../common'
import { appHelper, partnerSettingHelper, tenantHelper } from '../../helpers'
import { PartnerCollection, PartnerSettingCollection } from '../../models'

export const validateTenantCreditInfoData = (body) => {
  const requiredFields = ['creditRatingInfo', 'partnerId', 'tenantId']
  appHelper.checkRequiredFields(requiredFields, body)
  const {
    createdBy,
    creditRatingInfo = {},
    partnerId = '',
    propertyId,
    tenantId = ''
  } = body
  if (!size(creditRatingInfo))
    throw new CustomError(400, "Can't be empty creditRatingInfo")
  appHelper.validateId({ partnerId })
  appHelper.validateId({ tenantId })
  if (createdBy) appHelper.validateId({ createdBy })
  if (propertyId) appHelper.validateId({ propertyId })
}

export const prepareExpiredCreditRatingsRemovingQueryData = async (
  partnerId,
  session
) => {
  const settingQuery = {
    partnerId,
    'tenantSetting.removeCreditRating.enabled': true
  }
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting(
    settingQuery,
    session
  )
  if (size(partnerSetting)) {
    const creditRatingSettingMonths = partnerSetting.tenantSetting
      .removeCreditRating.months
      ? partnerSetting.tenantSetting.removeCreditRating.months
      : null
    if (creditRatingSettingMonths) {
      const partnerSettingDate = (
        await appHelper.getActualDate(partnerId, true, null)
      )
        .subtract(creditRatingSettingMonths, 'months')
        .endOf('day')
        .toDate()
      const query = {
        partnerId,
        creditRatingInfo: { $exists: true },
        'creditRatingInfo.createdAt': { $lte: partnerSettingDate }
      }
      return query
    } else
      console.log(
        'Credit rating remove setting is enabled, but the month is not available'
      )
  }
}

export const prepareCreditRatingLogAddOrUpdateData = async (
  action,
  options,
  session
) => {
  const { collectionId = null, createdBy = '', propertyId = '' } = options
  const logData = { action }

  if (collectionId) {
    const visibility = []

    const tenantInfo = await tenantHelper.getATenant(
      { _id: collectionId },
      session
    )

    const { creditRatingInfo = {} } = tenantInfo
    const creditRatingGeneralScore =
      size(creditRatingInfo) && creditRatingInfo.CDG2_GENERAL_SCORE
        ? creditRatingInfo.CDG2_GENERAL_SCORE
        : {}
    const creditRatingScore =
      size(creditRatingGeneralScore) && creditRatingGeneralScore.SCORE
        ? creditRatingGeneralScore.SCORE
        : ''

    if (creditRatingScore) {
      const metaData = [
        { field: 'creditRatingScore', value: creditRatingScore },
        {
          field: 'tenantId',
          value: collectionId
        }
      ]
      logData.meta = metaData
    }

    logData.tenantId = collectionId
    visibility.push('tenant')
    if (propertyId) {
      logData.propertyId = propertyId
      visibility.push('property')
    }

    logData.visibility = visibility

    if (createdBy) logData.createdBy = createdBy
  }

  return { logData }
}

export const creditRatingEnabledPartners = async (req) => {
  const { body, user = {} } = req
  const { query, options } = body
  appHelper.checkUserId(user.userId)
  appHelper.validateSortForQuery(options.sort)
  const { creditRating, serial = null } = query

  if (creditRating) {
    const settingQuery = {
      'tenantSetting.removeCreditRating.enabled': true
    }
    const partnerSettings = await partnerSettingHelper.getAllPartnerSettings(
      settingQuery
    )
    const partnerIds = size(partnerSettings)
      ? uniq(map(partnerSettings, 'partnerId')) || []
      : []

    const partnersQuery = {
      _id: { $in: partnerIds },
      enableCreditRating: true,
      isActive: true
    }
    if (serial) partnersQuery.serial = { $gt: serial }

    let partners = await PartnerCollection.find(partnersQuery)
      .limit(options.limit)
      .skip(options.skip)
      .sort(options.sort)

    partners = JSON.parse(JSON.stringify(partners))

    return partners || []
  } else return []
}
export const getPartnerIdsToRemoveCreditRatingInfo = async (req) => {
  const { user = {} } = req
  appHelper.checkUserId(user.userId)

  const response = await PartnerSettingCollection.aggregate([
    {
      $match: {
        'tenantSetting.removeCreditRating.enabled': true,
        'tenantSetting.removeCreditRating.months': { $gte: 1 }
      }
    },
    { $group: { _id: null, partnerIds: { $push: '$partnerId' } } }
  ])

  if (!size(response)) return []

  return response[0].partnerIds || []
}
