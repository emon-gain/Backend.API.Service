import { size } from 'lodash'
import { CustomError } from '../common'
import { UserReportCollection } from '../models'
import { appHelper, userHelper, userReportHelper } from '../helpers'
import { appQueueService } from '../services'

export const removeAnUserReport = async (query, session) => {
  const response = await UserReportCollection.findOneAndDelete(query, {
    session
  })
  return response
}

export const createAUserReport = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for userReport creation')
  }
  const response = await UserReportCollection.create([data], {
    session
  })
  if (!response) {
    throw new CustomError(404, 'Unable to create userReport')
  }
  return response
}

export const createUserReport = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  userReportHelper.checkRequiredFieldsForUserReportCreationOrDeletion(body)
  const { reportedUserId } = body
  if (userId === reportedUserId)
    throw new CustomError(405, "You can't do report yourself")
  const { userReportCreationData } =
    await userReportHelper.prepareUserReportCreationData(reportedUserId)
  userReportCreationData.reporter = userId

  const createdUserReport = await createAUserReport(
    userReportCreationData,
    session
  )
  return createdUserReport
}

export const removeUserReport = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  userReportHelper.checkRequiredFieldsForUserReportCreationOrDeletion(body)
  const { reportedUserId } = body
  const { query } = await userReportHelper.prepareUserReportDeletionQuery(
    reportedUserId
  )

  const removedUserReport = await removeAnUserReport(query, session)
  if (!removedUserReport) {
    throw new CustomError(404, 'Unable to delete userReport')
  }
  return removedUserReport
}

export const downloadTenantOrLandlordBalanceReport = async (req) => {
  const { body, user = {}, session } = req
  const { userId = '', partnerId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  appHelper.checkRequiredFields(['downloadReportType'], body)

  const {
    accountId,
    agentId,
    branchId,
    lastEnquiryDate,
    propertyId,
    tenantId,
    downloadReportType
  } = body
  const params = {}
  if (accountId) {
    appHelper.validateId({ accountId })
    params.accountId = accountId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    params.agentId = agentId
  }
  if (branchId) {
    appHelper.validateId({ branchId })
    params.branchId = branchId
  }
  if (propertyId) {
    appHelper.validateId({ propertyId })
    params.propertyId = propertyId
  }
  if (tenantId) {
    appHelper.validateId({ tenantId })
    params.tenantId = tenantId
  }
  if (lastEnquiryDate) {
    params.lastEnquiryDate = new Date(lastEnquiryDate)
  }

  params.partnerId = partnerId
  params.userId = userId
  params.reportType = downloadReportType
  params.downloadProcessType = 'download_report'

  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'

  const prepareQueueData = {
    action: 'download_email',
    event: 'excel manager',
    priority: 'immediate',
    destination: 'excel-manager',
    status: 'new',
    partnerId,
    params
  }

  const createdQueue = await appQueueService.createAnAppQueue(
    prepareQueueData,
    session
  )
  if (size(createdQueue)) {
    return {
      status: 200,
      message: 'Your download is in process please check you email'
    }
  } else {
    throw new CustomError(
      404,
      `Unable to download ${downloadReportType} balance report`
    )
  }
}

export const removeUserReports = async (query, session) => {
  if (!size(query))
    throw new CustomError(
      400,
      'Query must be required while removing user reports'
    )
  const response = await UserReportCollection.deleteMany(query, {
    session
  })
  console.log('=== UserReports Removed ===', response)
  return response
}
