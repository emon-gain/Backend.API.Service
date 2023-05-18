import { pick, assign } from 'lodash'
import { appHelper } from '../helpers'
import { CommentCollection } from './comment.model'
import { CustomError } from '../common'

export const getAComment = async (query, session) =>
  await CommentCollection.findOne(query, session)
export const prepareCommentCreateLogData = (comment) => {
  const { _id, context, createdBy } = comment
  let logData = { context: 'comment' }
  logData.commentId = _id
  logData.action = 'added_new_comment'
  logData.visibility = context ? [context] : []
  logData.createdBy = createdBy
  const commentData = pick(comment, [
    'accountId',
    'agentId',
    'commentId',
    'contractId',
    'partnerId',
    'propertyId',
    'taskId',
    'tenantId'
  ])
  logData = assign(logData, commentData)
  return logData
}

export const getCommentsForQuery = async (query, options) => {
  const { limit, skip, sort } = options

  const pipeline = [
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
    {
      $lookup: {
        from: 'users',
        localField: 'createdBy',
        foreignField: '_id',
        pipeline: [
          {
            $project: {
              _id: 1,
              name: '$profile.name',
              avatarKey:
                appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
            }
          }
        ],
        as: 'userInfo'
      }
    },
    appHelper.getUnwindPipeline('userInfo'),
    {
      $project: {
        content: 1,
        createdAt: 1,
        userInfo: 1
      }
    }
  ]

  const comments = await CommentCollection.aggregate(pipeline)
  return comments || []
}

export const countComments = async (query, session) => {
  const numberOfComments = await CommentCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfComments
}

const prepareQueryForComments = (query) => {
  const {
    accountId,
    context,
    contractId,
    partnerId,
    propertyId,
    taskId,
    tenantId
  } = query

  // TODO:: Need to implement query for context: landlordDashboard
  if (!context) throw new CustomError(400, 'Context is required.')

  const queryData = {
    $or: [
      { partnerId },
      { landlordPartnerId: partnerId },
      { tenantPartnerId: partnerId }
    ],
    context
  }

  if (context === 'account') {
    appHelper.checkRequiredFields(['accountId'], query)
    queryData.accountId = accountId
  } else if (context === 'property') {
    appHelper.checkRequiredFields(['propertyId'], query)
    queryData.propertyId = propertyId
  } else if (context === 'tenant') {
    appHelper.checkRequiredFields(['tenantId'], query)
    queryData.tenantId = tenantId
  } else if (context === 'task') {
    appHelper.checkRequiredFields(['taskId'], query)
    queryData.taskId = taskId
  }

  if (contractId) queryData.contractId = contractId

  return queryData
}

export const queryComments = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)
  const queryData = prepareQueryForComments(query)
  const commentsData = await getCommentsForQuery(queryData, options)
  const filteredDocuments = await countComments(queryData)
  const totalDocuments = await countComments({})
  return {
    data: commentsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const prepareAddCommentQuery = (body) => {
  const { accountId, contractId, propertyId, taskId, tenantId } = body
  let context = ''
  if (accountId) {
    appHelper.validateId({ accountId })
    context = 'account'
  }
  if (propertyId) {
    appHelper.validateId({ propertyId })
    context = 'property'
  }
  if (tenantId) {
    appHelper.validateId({ tenantId })
    context = 'tenant'
  }
  if (taskId) {
    appHelper.validateId({ taskId })
    context = 'task'
  }
  if (contractId) appHelper.validateId({ contractId })
  if (!context) throw new CustomError(400, 'Missing Required fields')
  body.context = context
  return body
}
