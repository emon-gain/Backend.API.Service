import { size } from 'lodash'
import { appHelper, creditRatingHelper, tenantHelper } from '../../helpers'
import { logService, partnerUsageService, tenantService } from '../../services'
import { CustomError } from '../../common'

export const createLogForAddOrUpdateCreditRating = async (params, session) => {
  const {
    createdBy = '',
    hasCreditRating,
    partnerId,
    propertyId = '',
    tenantId
  } = params
  const action = hasCreditRating ? 'update_credit_rating' : 'add_credit_rating'

  const options = {
    partnerId,
    collectionId: tenantId,
    collectionName: 'tenant',
    context: 'creditRating'
  }
  if (propertyId) options.propertyId = propertyId
  if (createdBy) options.createdBy = createdBy

  if (partnerId) {
    const { logData } =
      await creditRatingHelper.prepareCreditRatingLogAddOrUpdateData(
        action,
        options,
        session
      )
    await logService.createLog(logData, session)
  }
}

export const updateTenantCreditInfo = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  creditRatingHelper.validateTenantCreditInfoData(body)
  const { createdBy, creditRatingInfo, partnerId, propertyId, tenantId } = body
  const tenantQuery = { _id: tenantId, partnerId }
  const tenant = await tenantHelper.getATenant(tenantQuery, session)
  if (!size(tenant)) throw new CustomError(404, "Tenant doesn't exists")
  const hasCreditRating = !!size(tenant.creditRatingInfo)
  // Storing credit rating date in order to check expiry setting on partner level.
  creditRatingInfo.createdAt = new Date()
  const updatingData = {
    $set: {
      creditRatingInfo
    }
  }
  const updatedTenant = await tenantService.updateTenant(
    tenantQuery,
    updatingData,
    session
  )
  // addPartnerUsageForCreditRatingCheck
  const partnerUsageCreationData = {
    type: 'credit_rating',
    partnerId,
    tenantId,
    total: 1
  }
  await partnerUsageService.createAPartnerUsage(
    partnerUsageCreationData,
    session
  )
  // Create log for addOrUpdateCreditRating
  const params = { createdBy, hasCreditRating, partnerId, propertyId, tenantId }
  await createLogForAddOrUpdateCreditRating(params, session)

  return updatedTenant
}

export const removeExpiredCreditRatings = async (req) => {
  const { body, user = {}, session } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId = '' } = body
  appHelper.validateId({ partnerId })
  const query =
    await creditRatingHelper.prepareExpiredCreditRatingsRemovingQueryData(
      partnerId,
      session
    )
  if (!query) return { numberOfRemove: 0 }
  else {
    const data = { $unset: { creditRatingInfo: 1 } }
    const response = await tenantService.updateTenants(query, data, session)
    const numberOfRemove = size(response) ? response.nModified : 0
    return { numberOfRemove }
  }
}
