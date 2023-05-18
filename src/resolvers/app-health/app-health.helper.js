import { filter, omit, size } from 'lodash'
import { createAppHealth } from './app-health.service'
import { AppHealthCollection, AppQueueCollection } from '../models'
import { CustomError } from '../common'
import {
  appHelper,
  dashboardHelper,
  commissionHelper,
  invoiceHelper,
  payoutHelper,
  partnerSettingHelper
} from '../helpers'
import { getInvoicePaymentForAppHealth } from '../invoice-payment/invoice-payment.helper'
import { updateAppQueueToCompleted } from '../app-queue/app-queue.service'

export const getAppHeaths = async (query, session) => {
  const appHealths = await AppHealthCollection.find(query).session(session)
  return appHealths
}

export const getAnAppHeath = async (query = {}, session) => {
  const appHealth = await AppHealthCollection.findOne(query).session(session)
  return appHealth
}

export const prepareAppHealthQueryBasedOnFilters = (query) => {
  const { partnerId, updatedDateRange, status, context, type } = query
  if (partnerId) appHelper.validateId({ partnerId })
  if (size(updatedDateRange)) {
    appHelper.validateCreatedAtForQuery(updatedDateRange)
    query.createdAt = {
      $gte: new Date(updatedDateRange.startDate),
      $lte: new Date(updatedDateRange.endDate)
    }
  }
  if (status && !(status === 'success' || status === 'error'))
    throw new CustomError(400, 'status type should be valid')
  if (size(context))
    query.$or = [{ context: { $in: context } }, { context: { $exists: false } }]
  if (size(type)) query.type = { $in: type }
  const invoicePaymentsQuery = omit(query, ['updatedDateRange', 'context'])
  return invoicePaymentsQuery
}

const getErrorPipelineForAppHealth = () => [
  {
    $addFields: {
      firstElem: {
        $first: {
          $cond: [{ $isArray: '$errorDetails' }, '$errorDetails', []]
        }
      },
      secondElem: {
        $arrayElemAt: [
          { $cond: [{ $isArray: '$errorDetails' }, '$errorDetails', []] },
          1
        ]
      }
    }
  },
  {
    $addFields: {
      queueErrorCount: {
        $size: {
          $ifNull: ['$firstElem.queueError', []]
        }
      }
    }
  }
]

const getPartnerPipelineForAppHealth = () => [
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: {
      path: '$partner',
      preserveNullAndEmptyArrays: true
    }
  }
]

const getFinalProjectPipelineForAppHealth = () => [
  {
    $project: {
      partner: {
        _id: 1,
        name: 1,
        subDomain: 1
      },
      partnerId: 1,
      type: 1,
      status: 1,
      context: 1,
      createdAt: 1,
      updatedAt: 1,
      notSentInvoiceContractCount: {
        $add: [
          '$queueErrorCount',
          {
            $size: {
              $ifNull: ['$secondElem.notSentInvoices', []]
            }
          }
        ]
      },
      missingInvoiceContractCount: {
        $size: {
          $ifNull: ['$firstElem.missingInvoices', []]
        }
      },
      queueErrorCount: 1,
      missingAmount: {
        $cond: {
          if: {
            $ifNull: ['$errors', false]
          },
          then: '$errors.missingAmount',
          else: 0
        }
      },
      payoutDiff: {
        $cond: {
          if: {
            $ifNull: ['$errors', false]
          },
          then: {
            $ifNull: ['$errors.payoutDiff', 0]
          },
          else: 0
        }
      },
      hasSqsError: 1,
      sqsMessageCount: 1,
      sqsMessageType: 1,
      hasPdfError: 1,
      totalErrorCount: {
        $size: { $ifNull: ['$errorDetails', []] }
      }
    }
  }
]

export const getAppHealthsForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const appHealths = await AppHealthCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    ...getErrorPipelineForAppHealth(),
    ...getPartnerPipelineForAppHealth(),
    {
      $addFields: {
        errors: {
          $first: '$errorDetails'
        }
      }
    },
    ...getFinalProjectPipelineForAppHealth()
  ])
  return appHealths
}

export const countAppHealths = async (query, session) => {
  const numberOfAppHealths = await AppHealthCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfAppHealths
}

export const queryAppHealths = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = prepareAppHealthQueryBasedOnFilters(query)
  const appHealthsData = await getAppHealthsForQuery(body)
  const filteredDocuments = await countAppHealths(body.query)
  const totalDocuments = await AppHealthCollection.estimatedDocumentCount()
  return {
    data: appHealthsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getAppHeathErrorsByContext = (appHealthErrors, context) =>
  filter(appHealthErrors, (appHealth) => {
    if (appHealth.context === context) return appHealth
  })

export const getErrorObj = (type, value, isErrorIssue) => {
  const errorType =
    type === 'sqs'
      ? 'SQS'
      : type === 'invoice'
      ? 'Invoice'
      : type === 'payout'
      ? 'Payout'
      : ''

  return isErrorIssue
    ? { errors_type: errorType, errors_value: value }
    : { new_issue_type: errorType, new_issue_value: value }
}

const isErrorOfYesterday = async (todayIssues) => {
  const yesterdayIssuesArray = []
  const yesterdayStart = (await appHelper.getActualDate('', true))
    .subtract(1, 'days')
    .startOf('day')
    .toDate()
  const yesterdayEnd = (await appHelper.getActualDate('', true))
    .subtract(1, 'days')
    .endOf('day')
    .toDate()

  if (size(todayIssues)) {
    for (const issue of todayIssues) {
      const query = {
        createdAt: { $gte: yesterdayStart, $lt: yesterdayEnd },
        status: 'error',
        errorDetails: issue.errorDetails
      }
      if (issue.type === 'sqs') query.type = 'sqs'
      else query.context = issue.context

      const previousDayInfo = await getAppHeaths(query)

      if (size(previousDayInfo)) yesterdayIssuesArray.push(issue)
    }
  }

  return yesterdayIssuesArray
}

export const getIssuesOfYesterday = async (appHealthErrors) => {
  const sameErrors = []
  const newErrors = []
  let totalSameErrors = 0

  if (size(appHealthErrors)) {
    const todaySqsInfo = filter(appHealthErrors, { type: 'sqs' })
    const todayInvoiceInfo = getAppHeathErrorsByContext(
      appHealthErrors,
      'invoice'
    )
    const todayPayoutInfo = getAppHeathErrorsByContext(
      appHealthErrors,
      'payout'
    )
    const sameInvoiceErrorOfYesterday = await isErrorOfYesterday(
      todayInvoiceInfo
    )
    const sameSqsErrorOfYesterday = await isErrorOfYesterday(todaySqsInfo)
    const samePayoutErrorOfYesterday = await isErrorOfYesterday(todayPayoutInfo)
    const newInvoiceError =
      size(todayInvoiceInfo) - size(sameInvoiceErrorOfYesterday)
    const newPayoutError =
      size(todayPayoutInfo) - size(samePayoutErrorOfYesterday)
    const newSqsError = size(todaySqsInfo) - size(sameSqsErrorOfYesterday)

    if (size(sameSqsErrorOfYesterday)) {
      sameErrors.push(getErrorObj('sqs', size(sameSqsErrorOfYesterday)))
      totalSameErrors = totalSameErrors + size(sameSqsErrorOfYesterday)
    }

    if (size(sameInvoiceErrorOfYesterday)) {
      sameErrors.push(getErrorObj('invoice', size(sameInvoiceErrorOfYesterday)))
      totalSameErrors = totalSameErrors + size(sameInvoiceErrorOfYesterday)
    }

    if (size(samePayoutErrorOfYesterday)) {
      sameErrors.push(getErrorObj('payout', size(samePayoutErrorOfYesterday)))
      totalSameErrors = totalSameErrors + size(samePayoutErrorOfYesterday)
    }

    if (newInvoiceError) newErrors.push(getErrorObj('invoice', newInvoiceError))
    if (newPayoutError) newErrors.push(getErrorObj('payout', newPayoutError))
    if (newSqsError) newErrors.push(getErrorObj('sqs', newSqsError))
  }

  return { sameErrors, newErrors, totalSameErrors }
}

export const getAppHeathInfoForDashboard = async (
  query = {},
  partnerType = ''
) => {
  const pipeline = []
  const match = { $match: query }
  pipeline.push(match)
  dashboardHelper.preparePipelineForPartner(pipeline, partnerType)
  const group = {
    $group: {
      _id: null,
      // For Transaction
      totalTransactionErrors: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$status', 'error'] },
                { $eq: ['$type', 'transaction'] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      totalTransactions: {
        $sum: {
          $cond: { if: { $eq: ['$type', 'transaction'] }, then: 1, else: 0 }
        }
      },
      // For SQS
      totalSQSErrors: {
        $sum: {
          $cond: {
            if: {
              $and: [{ $eq: ['$status', 'error'] }, { $eq: ['$type', 'sqs'] }]
            },
            then: 1,
            else: 0
          }
        }
      },
      totalSQSs: {
        $sum: {
          $cond: { if: { $eq: ['$type', 'sqs'] }, then: 1, else: 0 }
        }
      },
      // For Payout
      totalNotificationErrors: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$status', 'error'] },
                { $eq: ['$type', 'notifications'] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      totalNotifications: {
        $sum: {
          $cond: { if: { $eq: ['$type', 'notifications'] }, then: 1, else: 0 }
        }
      },
      // For Invoice and Payout
      totalInvoiceAndPayoutErrors: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$status', 'error'] },
                { $eq: ['$type', 'accuracy'] },
                { $in: ['$context', ['invoice', 'payout']] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      totalInvoicesAndPayouts: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'accuracy'] },
                { $in: ['$context', ['invoice', 'payout']] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      }
    }
  }
  pipeline.push(group)
  const [appHealthInfo] = await AppHealthCollection.aggregate(pipeline)
  return appHealthInfo
}

export const prepareDataForAppHealthNotification = async () => {
  const partnerSetting = partnerSettingHelper.getAPartnerSetting({
    'notifications.appHealthNotification': true
  })
  if (partnerSetting) {
    const todayDateStart = (await appHelper.getActualDate('', true, new Date()))
      .startOf('day')
      .toDate()
    const todayDateEnd = (await appHelper.getActualDate('', true, new Date()))
      .endOf('day')
      .toDate()
    const appHealthErrors = getAppHeaths({
      status: 'error',
      createdAt: { $gte: todayDateStart, $lt: todayDateEnd },
      $or: [{ context: { $in: ['invoice', 'payout'] } }, { type: 'sqs' }]
    })
    return appHealthErrors
  }
}

export const prepareAppHealthError = async () => {
  const partnerSetting = partnerSettingHelper.getAPartnerSetting({
    'notifications.appHealthNotification': true
  })
  if (partnerSetting) {
    const todayDateStart = (await appHelper.getActualDate('', true, new Date()))
      .startOf('day')
      .toDate()
    const yesterDayStart = (await appHelper.getActualDate('', true, new Date()))
      .subtract(1, 'day')
      .startOf('day')
      .toDate()
    const yesterDateEnd = (await appHelper.getActualDate('', true, new Date()))
      .subtract(1, 'day')
      .endOf('day')
      .toDate()
    return AppHealthCollection.aggregate(
      appHealthPipeline(todayDateStart, yesterDayStart, yesterDateEnd)
    )
  }
}

const appHealthPipeline = (todayDateStart, yesterDayStart, yesterDayEnd) => [
  {
    $match: {
      status: 'error'
    }
  },
  {
    $unwind: '$errorDetails'
  },
  {
    $group: {
      _id: null,
      collectionId: {
        $first: '$_id'
      },
      partnerId: {
        $first: '$partnerId'
      },
      transactionErrorsTotalBeforeToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'transaction'] },
                { $gte: ['$createdAt', yesterDayStart] },
                { $lte: ['$createdAt', yesterDayEnd] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      transactionErrorsTotalToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'transaction'] },
                { $gte: ['$createdAt', todayDateStart] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      appQueueErrorTotalBeforeToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'sqs'] },
                { $gte: ['$createdAt', yesterDayStart] },
                { $lte: ['$createdAt', yesterDayEnd] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      appQueueErrorTotalToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'sqs'] },
                { $gte: ['$createdAt', todayDateStart] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      notificationErrorBeforeToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'notification'] },
                { $gte: ['$createdAt', yesterDayStart] },
                { $lte: ['$createdAt', yesterDayEnd] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      notificationErrorToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'notification'] },
                { $gte: ['$createdAt', todayDateStart] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      payoutErrorBeforeToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'accuracy'] },
                { $eq: ['$context', 'payout'] },
                { $gte: ['$createdAt', yesterDayStart] },
                { $lte: ['$createdAt', yesterDayEnd] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      payoutErrorToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'accuracy'] },
                { $eq: ['$context', 'payout'] },
                { $gte: ['$createdAt', todayDateStart] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      invoiceErrorBeforeToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'accuracy'] },
                { $eq: ['$context', 'invoice'] },
                { $gte: ['$createdAt', yesterDayStart] },
                { $lte: ['$createdAt', yesterDayEnd] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      },
      invoiceErrorToday: {
        $sum: {
          $cond: {
            if: {
              $and: [
                { $eq: ['$type', 'accuracy'] },
                { $eq: ['$context', 'invoice'] },
                { $gte: ['$createdAt', todayDateStart] }
              ]
            },
            then: 1,
            else: 0
          }
        }
      }
    }
  },
  {
    $addFields: {
      transactionErrorDiff: {
        $subtract: [
          '$transactionErrorsTotalToday',
          '$transactionErrorsTotalBeforeToday'
        ]
      },
      appQueueErrorDiff: {
        $subtract: [
          '$appQueueErrorTotalToday',
          '$appQueueErrorTotalBeforeToday'
        ]
      },
      notificationErrorDiff: {
        $subtract: ['$notificationErrorToday', '$notificationErrorBeforeToday']
      },
      payoutErrorDiff: {
        $subtract: ['$payoutErrorToday', '$payoutErrorBeforeToday']
      },
      invoiceErrorDiff: {
        $subtract: ['$invoiceErrorToday', '$invoiceErrorBeforeToday']
      }
    }
  },
  {
    $project: {
      partnerId: 1,
      appHealthErrorId: '$collectionId',
      total_issues: {
        $add: [
          '$transactionErrorsTotalToday',
          {
            $add: [
              '$appQueueErrorTotalToday',
              {
                $add: [
                  '$notificationErrorToday',
                  {
                    $add: ['$payoutErrorToday', '$invoiceErrorToday']
                  }
                ]
              }
            ]
          }
        ]
      },
      total_issues_before_today: {
        $add: [
          '$transactionErrorsTotalBeforeToday',
          {
            $add: [
              '$appQueueErrorTotalBeforeToday',
              {
                $add: [
                  '$notificationErrorBeforeToday',
                  {
                    $add: [
                      '$payoutErrorBeforeToday',
                      '$invoiceErrorBeforeToday'
                    ]
                  }
                ]
              }
            ]
          }
        ]
      },
      error_issues: {
        $concatArrays: [
          [],
          [
            {
              errors_type: 'transaction',
              errors_value: '$transactionErrorsTotalToday'
            },
            {
              errors_type: 'queue',
              errors_value: '$appQueueErrorTotalToday'
            },
            {
              errors_type: 'notification',
              errors_value: '$notificationErrorToday'
            },
            {
              errors_type: 'payout',
              errors_value: '$payoutErrorToday'
            },
            {
              errors_type: 'invoice',
              errors_value: '$invoiceErrorToday'
            }
          ]
        ]
      },
      new_issues: {
        $concatArrays: [
          [],
          [
            {
              new_issue_type: 'transaction',
              new_issue_value: '$transactionErrorDiff'
            },
            {
              new_issue_type: 'queue',
              new_issue_value: '$appQueueErrorDiff'
            },
            {
              new_issue_type: 'notification',
              new_issue_value: '$notificationErrorDiff'
            },
            {
              new_issue_type: 'payout',
              new_issue_value: '$payoutErrorDiff'
            },
            {
              new_issue_type: 'invoice',
              new_issue_value: '$invoiceErrorDiff'
            }
          ]
        ]
      }
    }
  },
  {
    $addFields: {
      all_issues_are_same: {
        $cond: {
          if: { $eq: ['$total_issues', '$total_issues_before_today'] },
          then: true,
          else: false
        }
      },
      new_issues: {
        $cond: {
          if: { $eq: ['$total_issues', '$total_issues_before_today'] },
          then: '$$REMOVE',
          else: '$new_issues'
        }
      },
      all_issues_are_new: {
        $cond: {
          if: { $eq: ['$total_issues', '$total_issues_before_today'] },
          then: false,
          else: true
        }
      }
    }
  }
]

export const getAppHealthError = async (req) => {
  const { body, user } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { query } = body
  appHelper.compactObject(query)
  appHelper.checkRequiredFields(['appHealthId'], query)
  const { appHealthId } = query
  appHelper.validateId({ appHealthId })
  const appHealth = await getAnAppHeath({ _id: appHealthId })
  return appHealth || []
}

export const checkDailyInvoiceTransactionsHelper = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId, skip, appQueueId } = body
  let { appHealthId } = body
  console.log('START checkDailyInvoiceTransactions for partnerId: ', partnerId)
  const appHealthData =
    await invoiceHelper.getInvoiceForInvoiceTransactionApphealth(
      partnerId,
      skip
    )
  console.log('Invoice data for partner ', partnerId, appHealthData)
  const data = {
    partnerId,
    type: 'transaction',
    context: 'invoice'
  }
  let errorDetails = []
  let appHealthDataExisting
  let totalTransactions = appHealthData?.totalTransactions || 0
  let totalInvoiceAmount = appHealthData?.totalInvoice || 0
  console.log('Newly transaction amount found from db', totalTransactions)
  console.log('Newly invoice amount found from db', totalInvoiceAmount)
  if (size(appHealthId)) {
    appHealthDataExisting = await AppHealthCollection.findOne({
      _id: appHealthId
    })
    console.log('Current app health data', appHealthDataExisting)
    if (size(appHealthDataExisting?.errorDetails)) {
      console.log(
        'Current app health error data',
        appHealthDataExisting.errorDetails
      )
      errorDetails = errorDetails.concat(appHealthDataExisting?.errorDetails)
    }
    totalTransactions += appHealthDataExisting?.transactionTotal || 0
    totalInvoiceAmount += appHealthDataExisting?.collectionTotal || 0
  }
  console.log('Current app health error data after condition', errorDetails)
  console.log('Total transactions', totalTransactions)
  console.log('Total invoice amount', totalInvoiceAmount)
  const missingAmount = Math.abs(totalInvoiceAmount - totalTransactions)
  const hasMissingTransaction = !!size(
    appHealthData?.missingTransactionInvoiceIds
  )
  console.log('Has missing transactions', hasMissingTransaction)
  // let missingTransactions = errorDetails[0]?.
  let missingTransactions = []
  let badTransactions = []
  if (size(errorDetails)) {
    missingTransactions = errorDetails[0]?.missingTransactionInvoiceIds || []
    badTransactions = errorDetails[0]?.badTransactions || []
  }
  let errorObj = {}
  if (hasMissingTransaction === true) {
    missingTransactions = missingTransactions.concat(
      appHealthData?.missingTransactionInvoiceIds
    )
    errorObj = {
      ...errorObj,
      hasMissingTransaction,
      missingTransactionInvoiceIds: missingTransactions
    }
  }
  console.log('Missing amount is', missingAmount)
  if (missingAmount >= 1) {
    if (appHealthData?.badTransactions) {
      badTransactions = badTransactions.concat(appHealthData?.badTransactions)
    }
    errorObj = {
      ...errorObj,
      totalTransactions,
      totalInvoiceAmount,
      badTransactions
    }
  } else {
    delete errorObj.totalTransactions
    delete errorObj.totalInvoiceAmount
    delete errorObj.badTransactions
    if (!size(errorObj)) {
      errorDetails.pop()
    }
  }
  console.log('Single error object', errorObj)
  if (size(errorObj)) {
    errorDetails = [errorObj]
  }
  console.log('Final error details', errorDetails)
  if (errorDetails.length > 0) {
    data.status = 'error'
    data.errorDetails = errorDetails
  } else {
    data.status = 'success'
  }
  console.log('Final error details', errorDetails)
  data.transactionTotal = totalTransactions
  data.collectionTotal = totalInvoiceAmount
  // if (_.size(appHealthData[0].missingTransactions))
  //   errorDetails.push(appHealthData[0].missingTransactions)
  // if (_.size(appHealthData[0].badTransactions))
  //   errorDetails.push(appHealthData[0].badTransactions)
  //
  // if (errorDetails.length >= 1) {
  //   data.errorDetails = errorDetails
  //   data.status = 'error'
  // } else {
  //   data.status = 'success'
  // }
  req.body = data
  let appHealthCreatedData = []
  try {
    if (appHealthId) {
      await AppHealthCollection.updateOne(
        { _id: appHealthId },
        {
          $set: data
        },
        {
          session
        }
      )
      appHealthCreatedData = await AppHealthCollection.find({
        _id: appHealthId
      })
    } else {
      const appHealthCreatedData = (await createAppHealth(req)) || []
      appHealthId = appHealthCreatedData[0]?._id
      console.log('App health data after creation', appHealthCreatedData)
    }
    if (size(appHealthData)) {
      const appHealthUpdatedData = await AppQueueCollection.updateOne(
        {
          _id: appQueueId,
          status: 'processing'
        },
        {
          $inc: {
            'params.dataToSkip': 500
          },
          $set: {
            'params.appHealthId': appHealthId,
            status: 'new'
          }
        },
        session
      )
      console.log('Updated log of app health', appHealthUpdatedData)
      return appHealthCreatedData
    } else {
      await updateAppQueueToCompleted(appQueueId, session)
      return [
        {
          msg: 'Completed'
        }
      ]
    }
  } catch (e) {
    console.log('Error occurred when creating or updating app health', e)
    throw new Error('Error occurred when creating or updating app health')
  }
}

export const dailyPaymentTransactionHelper = async (req) => {
  const { body } = req
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  const paymentsData = await getInvoicePaymentForAppHealth(partnerId)
  console.log('Total payments', paymentsData)

  const data = {
    partnerId,
    type: 'transaction',
    context: 'payment',
    errorDetails: [
      {
        transactionTotal: paymentsData?.totalTransactions || 0,
        collectionTotal: paymentsData?.totalPayment || 0,
        missingAmount: paymentsData?.missingAmount || 0,
        transactionDetails: paymentsData?.missingTransactionsInPayment || 0
      }
    ]
  }

  if (paymentsData?.missingAmount >= 1) data.status = 'error'
  else data.status = 'success'

  if (data) {
    req.body = data
    const insertData = await createAppHealth(req)
    return insertData
  }
}

export const checkDailyCommissionTransactionsHelper = async (req) => {
  const { body } = req
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  console.log('App health for commission for the partner', partnerId)
  const commissionData =
    await commissionHelper.getCommissionForAppHealthCommission(partnerId)
  console.log(
    'Found commission data for the partner',
    partnerId,
    commissionData
  )
  const data = {
    partnerId,
    type: 'transaction',
    context: 'commission',
    errorDetails: [
      {
        transactionTotal: commissionData?.totalTransactions || 0,
        collectionTotal: commissionData?.totalCommission || 0,
        missingAmount: commissionData?.missingAmount || 0,
        transactionDetails: commissionData?.missingTransactionsInCommission || 0
      }
    ]
  }
  commissionData?.missingAmount > 1
    ? (data.status = 'error')
    : (data.status = 'success')
  if (data) {
    req.body = data
    const insertData = await createAppHealth(req)
    console.log(
      'App health data inserted for the partner',
      partnerId,
      insertData
    )
    return insertData
  } else {
    return {
      msg: 'App health not created',
      code: 'ERROR'
    }
  }
}

export const checkDailyPayoutTransactionsHelper = async (req) => {
  const { body } = req
  const { partnerId } = body
  appHelper.checkRequiredFields(['partnerId'], body)
  const payoutAmount = await payoutHelper.payoutForApphealthTransactions(
    partnerId
  )
  const data = {
    partnerId,
    type: 'transaction',
    context: 'payout',
    errorDetails: [
      {
        transactionTotal: payoutAmount?.totalPayout || 0,
        collectionTotal: payoutAmount?.totalAmount || 0,
        missingAmount: payoutAmount?.missingAmount || 0,
        transactionDetails: payoutAmount?.missingTransactionsInPayout || 0
      }
    ]
  }
  payoutAmount?.missingAmount > 1
    ? (data.status = 'error')
    : (data.status = 'success')
  if (data) {
    req.body = data
    const insertData = await createAppHealth(req)
    return insertData
  } else {
    return {
      msg: 'App health not created',
      code: 'ERROR'
    }
  }
}

export const checkDailyCorrectionTransactionsHelper = async (req) => {
  const { body } = req
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body

  const correctionData = await invoiceHelper.invoiceCalculationForAppHealth(
    partnerId
  )

  const data = {
    partnerId,
    type: 'transaction',
    context: 'correction',
    errorDetails: [
      {
        transactionTotal: correctionData[0]?.totalTransactions || 0,
        collectionTotal: correctionData[0]?.totalCorrection || 0,
        missingAmount: correctionData[0]?.missingAmount || 0,
        transactionDetails:
          correctionData[0]?.missingTransactionsInCorrection || 0
      }
    ]
  }

  if (size(correctionData)) {
    if (correctionData[0]?.missingAmount >= 1) data.status = 'error'
    else data.status = 'success'

    if (data) {
      req.body = data
      const insertData = await createAppHealth(req)
      console.log('App health====>', insertData)
      return insertData
    }
  } else {
    return [
      {
        msg: 'No data found'
      }
    ]
  }
}
