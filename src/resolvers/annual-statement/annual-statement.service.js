import { size } from 'lodash'
import { appHelper, annualStatementHelper, userHelper } from '../helpers'
import { CustomError } from '../common'
import { appQueueService } from '../services'
import { AnnualStatementCollection } from '../models'

export const downloadAnnualStatement = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId, userId } = user
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['statementYear'], body)
  const { statementYear = 0 } = body

  const userInfo = await userHelper.getAnUser({ _id: userId })
  const appQueueData = {
    destination: 'xml-creator',
    action: 'download_xml_email',
    event: 'download_xml_email',
    priority: 'immediate',
    status: 'new',
    params: {
      statementYear,
      partnerId,
      downloadProcessType: 'download_annual_statement_xml',
      userId,
      userLanguage: userInfo?.profile?.language || 'en'
    }
  }

  const annualStatementQueue = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  if (size(annualStatementQueue)) {
    return {
      status: 200,
      message:
        'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
    }
  } else {
    throw new CustomError(404, `Unable to download annual statement`)
  }
}

export const addAnnualStatements = async (inputData, session) => {
  const annualStatement = await AnnualStatementCollection.create([inputData], {
    session
  })
  return annualStatement[0]
}

export const annualStatementCreateService = async (req) => {
  const { contractId, statementYear } = req.body
  const annualStatement = await annualStatementHelper.getAnnualStatement({
    contractId,
    statementYear
  })
  if (annualStatement) {
    return {}
  }
  const annualStatements =
    await annualStatementHelper.queryForAnnualStatementData(
      contractId,
      statementYear
    )
  try {
    const annualStatement = await addAnnualStatements(
      annualStatements,
      req.session
    )
    if (annualStatement) {
      annualStatements._id = annualStatement._id
      return annualStatements
    }
  } catch (e) {
    throw new Error(e)
  }
}

export const updateAnAnnualStatement = async (
  queryData = {},
  updatingData = {},
  session
) => {
  const result = await AnnualStatementCollection.findOneAndUpdate(
    queryData,
    updatingData,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!size(result))
    throw new CustomError(404, 'Annual statement is not updated')
  return result
}

export const updateAnnualStatement = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(['annualStatementId', 'fileId'], body)

  const { annualStatementId, fileId } = body
  if (!(annualStatementId && fileId))
    throw new Error(400, 'Invalid data for updating annual statement')
  const updatedAnnualStatement = await updateAnAnnualStatement(
    {
      _id: annualStatementId
    },
    {
      fileId,
      status: 'completed'
    },
    session
  )
  const action = 'send_notification'
  const event = 'send_landlord_annual_statement'
  if (updatedAnnualStatement) {
    const appQueueData = {
      event,
      action,
      destination: 'notifier',
      params: {
        partnerId: updatedAnnualStatement?.partnerId,
        collectionId: updatedAnnualStatement?.contractId,
        collectionNameStr: 'contracts',
        options: {
          annualStatementId
        }
      },
      priority: 'regular'
    }
    const appQueue = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    console.log('App queue created with app queue', appQueue)
    if (appQueue) {
      return updatedAnnualStatement
    } else {
      throw new Error('Something went wrong')
    }
  }
}
