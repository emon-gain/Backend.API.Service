import { clone, compact, map, omit, size } from 'lodash'
import moment from 'moment-timezone'
import { PartnerPayoutCollection, PayoutCollection } from '../models'
import { appHelper, contractHelper, payoutProcessHelper } from '../helpers'
import { getS3SignedUrl } from '../../lib/s3'
import settingJson from '../../../settings'

export const getPartnerPayoutsForQuery = async (params) => {
  const { query = {}, options } = params
  const { limit, skip, sort } = options
  const partnerPayouts = await PartnerPayoutCollection.find(query)
    .sort(sort)
    .skip(skip)
    .limit(limit)
    .populate(['partner', 'payoutProcess'])
  return partnerPayouts
}

export const getPartnerPayouts = async (query, options = {}, session) => {
  const { limit, sort } = options
  const partnerPayouts = await PartnerPayoutCollection.find(query)
    .sort(sort)
    .limit(limit)
    .session(session)
  return partnerPayouts
}

export const getAPartnerPayout = async (query, session) => {
  const partnerPayout = await PartnerPayoutCollection.findOne(query).session(
    session
  )
  return partnerPayout
}

export const preparePartnerPayoutsQueryBasedOnFilters = async (query) => {
  const {
    createdDateRange,
    defaultSearchText,
    partnerId,
    sentFileName,
    receivedFileName,
    statuses,
    hasPayout
  } = query
  let searchingData = {}
  if (defaultSearchText) {
    searchingData = {
      $or: [
        {
          sentFileName: new RegExp('.*' + defaultSearchText + '.*', 'i')
        },
        {
          'feedbackStatusLog.receivedFileName': new RegExp(
            '.*' + defaultSearchText + '.*',
            'i'
          )
        }
      ]
    }
  } else if (sentFileName) {
    searchingData = {
      sentFileName: new RegExp('.*' + sentFileName + '.*', 'i')
    }
  } else if (receivedFileName) {
    searchingData = {
      'feedbackStatusLog.receivedFileName': new RegExp(
        '.*' + receivedFileName + '.*',
        'i'
      )
    }
  }
  if (size(searchingData)) {
    const payoutProcessIds = await payoutProcessHelper.getPayoutProcessIds(
      searchingData
    )
    query.payoutProcessId = { $in: payoutProcessIds }
  }
  if (size(statuses)) {
    query.status = { $in: statuses }
  }
  //has payout query
  if (hasPayout) {
    if (hasPayout === 'no') {
      query.hasPayouts = { $in: [null, false] }
      query.hasRefundPayments = { $in: [null, false] }
    } else if (hasPayout === 'yes') {
      query['$or'] = [{ hasPayouts: true }, { hasRefundPayments: true }]
    }
  }
  //set partner selection
  if (partnerId) {
    appHelper.validateId({ partnerId })
  }
  if (size(createdDateRange)) {
    appHelper.validateCreatedAtForQuery(createdDateRange)
    query.createdAt = {
      $gte: moment(createdDateRange.startDate).toDate(),
      $lte: moment(createdDateRange.endDate).toDate()
    }
  }
  const partnerPayoutsQuery = omit(query, [
    'createdDateRange',
    'hasPayout',
    'statuses',
    'defaultSearchText',
    'sentFileName',
    'receivedFileName'
  ])
  return partnerPayoutsQuery
}

export const countPartnerPayouts = async (query) => {
  const numOfPartnerPayouts = await PartnerPayoutCollection.find(query).count()
  return numOfPartnerPayouts
}

export const getFeedBackLogAndUrls = async (payoutProcess) => {
  const { sentFileName = '', feedbackStatusLog = [] } = payoutProcess || {}
  const { folder = '' } = settingJson.S3.Directives['NETS']
  const bucket = process.env.S3_BUCKET || 'uninite-com-local'
  let S3SentFileUrl = ''
  if (sentFileName) {
    S3SentFileUrl = await getS3SignedUrl(
      folder + '/PROCESSED/Sent/' + sentFileName,
      bucket
    )
  }
  if (!size(feedbackStatusLog)) {
    return { S3SentFileUrl }
  }
  const newLogs = []
  for (const feedback of feedbackStatusLog) {
    feedback.S3ReceivedFileUrl = await getS3SignedUrl(
      folder + '/PROCESSED/Received/' + feedback.receivedFileName,
      bucket
    )
    newLogs.push(feedback)
  }
  return { S3SentFileUrl, feedbackStatusLog: newLogs, sentFileName }
}

export const queryPartnerPayouts = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  body.query = await preparePartnerPayoutsQueryBasedOnFilters(query)
  const partnerPayoutsData = await getPartnerPayoutsForQuery(body)
  const filteredDocuments = await countPartnerPayouts(body.query)
  const totalDocuments = await countPartnerPayouts({})
  const partnerPayouts = []
  const payoutsData = JSON.parse(JSON.stringify(partnerPayoutsData))
  for (const partnerPayout of payoutsData) {
    const { payoutProcess = [] } = partnerPayout
    if (size(payoutProcess)) {
      const {
        S3SentFileUrl = '',
        feedbackStatusLog = [],
        sentFileName = ''
      } = await getFeedBackLogAndUrls(payoutProcess)
      partnerPayout.S3SentFileUrl = S3SentFileUrl
      partnerPayout.feedbackStatusLog = feedbackStatusLog
      partnerPayout.sentFileName = sentFileName
    }
    partnerPayouts.push(partnerPayout)
  }
  return {
    data: partnerPayouts,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const prepareDataToUpdatePartnerPayout = (params) => {
  const {
    directRemittanceSigningMeta,
    directRemittanceSigningStatus,
    directRemittanceSignedUserId,
    eventStatus,
    eventNote,
    hasPayouts,
    hasRefundPayments,
    paymentIds,
    payoutIds,
    status
  } = params
  const updateData = {}
  const set = {}

  if (size(status)) set.status = status
  if (size(payoutIds)) set.payoutIds = payoutIds
  if (hasPayouts === true) set.hasPayouts = true
  if (hasPayouts === false) set.hasPayouts = false
  if (size(paymentIds)) set.paymentIds = paymentIds
  if (hasRefundPayments === true) set.hasRefundPayments = true
  if (hasRefundPayments === false) set.hasRefundPayments = false
  if (size(directRemittanceSigningMeta)) {
    set.directRemittanceSigningMeta = directRemittanceSigningMeta
  }
  if (size(directRemittanceSigningStatus)) {
    set.directRemittanceSigningStatus = directRemittanceSigningStatus
  }
  if (directRemittanceSignedUserId) {
    set['directRemittanceSigningStatus.$.signed'] = true
  }

  if (size(set)) updateData['$set'] = set

  if (eventStatus) {
    updateData['$push'] = {
      events: { status: eventStatus, createdAt: new Date(), note: eventNote }
    }
  }

  return updateData
}

export const prepareQueryWhenRentInvoiceIsPaidAfterPayoutDate = async (
  settings,
  payoutDate,
  todayDateOfMonth
) => {
  const orQuery = []

  if (size(settings.customPayoutDays) && settings.customPayoutDays.enabled) {
    const instantPayDate = settings.customPayoutDays.days
      ? settings.customPayoutDays.days
      : 0

    //standard payout date passed? check the waiting days
    //find all payouts where the payment made before instantPayDate days ago.

    const waitingDate = (
      await appHelper.getActualDate(settings, true, payoutDate)
    )
      .subtract(instantPayDate, 'days')
      .endOf('day')
      .toDate()

    orQuery.push({
      invoicePaid: true,
      payoutDate: { $lte: todayDateOfMonth },
      invoicePaidOn: { $lte: waitingDate },
      invoicePaidAfterPayoutDate: true
    })

    orQuery.push({
      invoiceLost: true,
      payoutDate: { $lte: todayDateOfMonth },
      invoiceLostOn: { $lte: waitingDate }
    })
  } else {
    orQuery.push({
      invoicePaid: true,
      payoutDate: { $lte: todayDateOfMonth },
      newPayoutDate: { $lte: todayDateOfMonth },
      invoicePaidAfterPayoutDate: true
    })

    orQuery.push({
      invoiceLost: true,
      payoutDate: { $lte: todayDateOfMonth },
      newPayoutDate: { $lte: todayDateOfMonth }
    })
  }
  return orQuery
}

export const getUnpaidPayoutIdsForAdvancePayMonth = async (
  partnerId,
  advancePayMonths,
  todayDateOfMonth
) => {
  const payoutMatchQuery = {
    $or: [
      {
        partnerId,
        status: { $ne: 'estimated' },
        advancedPayout: true,
        invoicePaid: { $ne: true },
        amount: { $gt: 0 }
      },
      {
        partnerId,
        sentToNETS: { $ne: true },
        payoutDate: { $lte: todayDateOfMonth },
        status: 'estimated',
        advancedPayout: { $ne: true },
        invoicePaid: { $ne: true },
        amount: { $gt: 0 }
      }
    ]
  }
  const payoutProject = {
    contractId: '$contractId',
    status: '$status',
    advancedPayout: '$advancedPayout',
    invoicePaid: '$invoicePaid',
    amount: '$amount',
    createdAt: '$createdAt',
    paidPayoutId: {
      $cond: {
        if: {
          $and: [
            { $ne: ['$status', 'estimated'] },
            { $eq: ['$advancedPayout', true] },
            { $ne: ['$invoicePaid', true] },
            { $gt: ['$amount', 0] }
          ]
        },
        then: '$_id',
        else: ''
      }
    },
    unpaidPayoutId: {
      $cond: {
        if: {
          $and: [
            { $eq: ['$status', 'estimated'] },
            { $ne: ['$advancedPayout', true] },
            { $ne: ['$invoicePaid', true] },
            { $gt: ['$amount', 0] }
          ]
        },
        then: '$_id',
        else: ''
      }
    }
  }
  const payoutGroup = {
    _id: '$contractId',
    paidPayout: { $addToSet: '$paidPayoutId' },
    unpaidPayout: { $addToSet: '$unpaidPayoutId' }
  }
  const payoutSort = { createdAt: 1 }

  const advancedPayoutList = await PayoutCollection.aggregate([
    { $match: payoutMatchQuery },
    { $project: payoutProject },
    { $group: payoutGroup },
    { $sort: payoutSort }
  ])

  const advancedPayoutIds = []
  for (const advancedPayoutInfo of advancedPayoutList) {
    if (size(advancedPayoutInfo)) {
      const paidPayoutCount = size(
        compact(clone(advancedPayoutInfo.paidPayout))
      )
      const unpaidPayoutIds = compact(clone(advancedPayoutInfo.unpaidPayout))

      if (paidPayoutCount < advancePayMonths && size(unpaidPayoutIds)) {
        //we got the old payout according to ascending order(old-to-new)
        //get payout ids from last index of array
        const unpaidIds = unpaidPayoutIds.slice(
          (advancePayMonths - paidPayoutCount) * -1
        )
        advancedPayoutIds.push(...unpaidIds)
      }
    }
  }
  return advancedPayoutIds
}

export const getEstimatedPayoutsQuery = async (partnerId, orQuery) => {
  const holdContracts = await contractHelper.getContracts({
    partnerId,
    holdPayout: true
  })
  console.log('+++ found holdContracts', holdContracts)
  const payoutHoldContractIds = map(holdContracts, '_id')
  console.log('+++ found payoutHoldContractIds', payoutHoldContractIds)
  const payoutQuery = {
    partnerId,
    status: 'estimated',
    sentToNETS: { $ne: true },
    amount: { $gt: 0 },
    holdPayout: { $ne: true },
    contractId: { $nin: payoutHoldContractIds }
  }
  console.log('+++ found payoutQuery', payoutQuery)
  payoutQuery['$or'] = orQuery
  console.log('+++ found payoutQuery with or', payoutQuery)
  return payoutQuery
}

export const prepareDataToCreateAQueueForInitiatePayoutJob = (
  event,
  partnerPayout
) => {
  const { _id, partnerId } = partnerPayout
  return {
    action: 'ready_partner_payout',
    event,
    destination: 'payments',
    priority: 'immediate',
    params: {
      partnerPayoutId: _id,
      partnerId
    }
  }
}

export const prepareQueryToUpdatePartnerPayout = (body) => {
  const { directRemittanceSignedUserId, partnerPayoutId, partnerId } = body
  const preparedQuery = {}
  if (size(partnerPayoutId)) preparedQuery._id = partnerPayoutId
  if (directRemittanceSignedUserId) {
    preparedQuery['directRemittanceSigningStatus.userId'] =
      directRemittanceSignedUserId
  }
  if (size(partnerId)) preparedQuery.partnerId = partnerId
  return preparedQuery
}

export const getAllowedPersonsToApprove = (partnerSetting) =>
  size(partnerSetting) &&
  size(partnerSetting.directRemittanceApproval) &&
  size(partnerSetting.directRemittanceApproval.persons)
    ? partnerSetting.directRemittanceApproval.persons
    : []

export const queryPartnerPayoutsSigners = async (req) => {
  const { body } = req
  const { query } = body
  appHelper.checkRequiredFields(['partnerPayoutId'], query)
  const { partnerPayoutId = '' } = query
  const signers = await PartnerPayoutCollection.aggregate([
    {
      $match: {
        _id: partnerPayoutId
      }
    },
    {
      $lookup: {
        from: 'partner_settings',
        localField: 'partnerId',
        foreignField: 'partnerId',
        as: 'partnerSettings'
      }
    },
    {
      $addFields: {
        partnerOrganizationId: {
          $first: '$partnerSettings.companyInfo.organizationId'
        },
        partnerSettings: '$$REMOVE'
      }
    },
    {
      $unwind: {
        path: '$directRemittanceSigningStatus',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'directRemittanceSigningStatus.userId',
        foreignField: '_id',
        as: 'user'
      }
    },
    {
      $addFields: {
        norwegianNationalIdentification: {
          $first: '$user.profile.norwegianNationalIdentification'
        },
        user: '$$REMOVE'
      }
    },
    {
      $addFields: {
        signer: {
          authenticationReference:
            '$directRemittanceSigningStatus.authenticationReference',
          categoryPurposeCode:
            '$directRemittanceSigningStatus.categoryPurposeCode',
          norwegianNationalIdentification: '$norwegianNationalIdentification',
          partnerOrganizationId: '$partnerOrganizationId',
          userId: '$directRemittanceSigningStatus.userId'
        }
      }
    },
    {
      $replaceRoot: { newRoot: '$signer' }
    }
  ])
  return signers
}

export const getAllPayoutsEsignInfo = async (partnerId, propertyId = false) => {
  const query = [
    {
      $match: {
        partnerId,
        'directRemittanceSigningStatus.signed': false,
        status: 'waiting_for_signature',
        type: 'payout'
      }
    },
    {
      $lookup: {
        from: 'payouts',
        localField: 'payoutIds',
        foreignField: '_id',
        as: 'payouts'
      }
    },
    {
      $unwind: {
        path: '$payouts',
        preserveNullAndEmptyArrays: true
      }
    },
    ...(propertyId ? [{ $match: { 'payouts.propertyId': propertyId } }] : []),
    {
      $unwind: {
        path: '$directRemittanceSigningStatus',
        preserveNullAndEmptyArrays: true
      }
    },
    { $match: { 'directRemittanceSigningStatus.signed': false } },
    {
      $group: {
        _id: '$directRemittanceSigningStatus.userId',
        amount: { $sum: '$payouts.amount' },
        createdAt: { $first: '$createdAt' },
        payoutsApprovalEsigningUrl: {
          $first: '$directRemittanceSigningStatus.signingUrl'
        }
      }
    }
  ]

  const result = await PartnerPayoutCollection.aggregate(query)
  return result || []
}

export const getPartnerPayoutForLambda = async (req) => {
  const { body, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const { query } = body
  const partnerPayoutQuery = await preparePartnerPayoutsQueryBasedOnFilters(
    query
  )
  const partnerPayout = await getAPartnerPayout(partnerPayoutQuery)
  return partnerPayout
}

export const getPartnerPayoutsDataForESigningCleaner = async (req) => {
  const { body, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)

  const pipeline = [
    {
      $match: {
        ...body.query,
        directRemittanceIDFYDocumentId: { $exists: true },
        directRemittanceESigningInitiatedAt: {
          $gte: moment().subtract(3, 'months').toDate(),
          $lte: moment().subtract(1, 'hour').toDate()
        },
        'directRemittanceSigningStatus.signed': false,
        status: 'waiting_for_signature'
      }
    },
    { $sort: { createdAt: 1 } },
    {
      $lookup: {
        from: 'files',
        localField: '_id',
        foreignField: 'partnerPayoutId',
        pipeline: [
          {
            $match: {
              type: {
                $in: [
                  'payments_approval_esigning_pdf',
                  'payouts_approval_esigning_pdf'
                ]
              }
            }
          }
        ],
        as: 'files'
      }
    },
    { $addFields: { fileId: { $first: '$files._id' }, files: '$$REMOVE' } },
    {
      $project: {
        _id: 0,
        directRemittanceIDFYDocumentId: 1,
        fileId: 1,
        partnerId: 1,
        partnerPayoutId: '$_id'
      }
    }
  ]
  return PartnerPayoutCollection.aggregate(pipeline)
}
