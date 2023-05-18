import { size } from 'lodash'
import { CustomError } from '../common'
import { AppRoleCollection } from '../models'
import { appHelper, branchHelper, userHelper } from '../helpers'

export const prepareAddRoleUserQuery = (body) => {
  const { _id, partnerId } = body
  return { _id, partnerId }
}

export const queryForRemoveRoleUser = (body) => {
  const { _id, partnerId, userId, roleUserId } = body
  const query = { _id, partnerId }
  if (userId === roleUserId) {
    query.type = { $ne: 'partner_admin' }
  }
  return query
}

export const prepareAddOrRemoveUserQuery = (body) => {
  const { roleId } = body
  return { _id: roleId, type: 'app_manager' }
}

export const prepareUpdateQuery = (body) => {
  const { updateType } = body
  const query = {
    addRoleUser: prepareAddRoleUserQuery,
    removeRoleUser: queryForRemoveRoleUser,
    addUserToAppManager: prepareAddOrRemoveUserQuery,
    removeRoleFromAppManager: prepareAddOrRemoveUserQuery
  }
  if (query[updateType]) {
    return query[updateType](body)
  }
  return {}
}

// Can not remove partner agent when a agent assigned to any branches
export const checkAssignedBranch = async (body) => {
  const { _id, partnerId, data } = body
  const { roleUserId } = data
  const role = await AppRoleCollection.findOne({ _id, partnerId })
  const branchQuery = { partnerId, agents: { $in: [roleUserId] } }
  const isAssignedToBranch = await branchHelper.countBranches(branchQuery)
  const { type } = role
  if (type === 'partner_agent' && isAssignedToBranch) {
    throw new CustomError(405, 'Can not remove! Agent is assigned to a branch')
  }
  return true
}

export const validatePutRequest = async (body) => {
  const { updateType } = body
  if (!updateType) {
    throw new CustomError(400, 'Bad request! updateType is missing')
  } else if (updateType === 'removeRoleUser') {
    await checkAssignedBranch(body)
  }
}

export const prepareUpdateData = (body) => {
  const { data, updateType } = body
  const { roleUserId } = data
  const updateData = {
    addRoleUser: { $addToSet: { users: roleUserId } },
    removeRoleUser: { $pull: { users: roleUserId } },
    addUserToAppManager: { $addToSet: { users: roleUserId } },
    removeRoleFromAppManager: { $pull: { users: roleUserId } }
  }
  if (updateData[updateType]) {
    return updateData[updateType]
  }
  return {}
}

export const getAppRole = async (query, session) => {
  const appRole = await AppRoleCollection.findOne(query).session(session)
  return appRole
}

export const getAppRoles = async (query, session) => {
  const appRoles = await AppRoleCollection.find(query).session(session)
  return appRoles
}

export const prepareAppRolesQueryBasedOnFilters = (query) => {
  const { type } = query
  if (type === 'allUsers') query.type = { $in: ['app_manager', 'app_admin'] }
  else if (type === 'appAdmin') query.type = 'app_admin'
  else if (type === 'managers') query.type = 'app_manager'
  return query
}

export const prepareAppRolesQueryForPartnersBasedOnFilters = (query) => {
  const { status = [], type } = query
  const roles = [
    'partner_accounting',
    'partner_admin',
    'partner_agent',
    'partner_janitor'
  ]
  if (
    status.includes('active') ||
    status.includes('inactive') ||
    status.includes('invited')
  ) {
    query.status = { $in: status }
  } else {
    query.status = { $in: ['active'] }
  }
  if (type) {
    query.type = type
  } else {
    query.type = {
      $in: roles
    }
  }
  return query
}

export const prepareQueryPipelineForAppRoles = (params = {}) => {
  const query = [
    { $match: params },
    {
      $unwind: '$users'
    },
    //To take only one user one time
    {
      $group: {
        _id: '$users',
        type: { $push: '$type' },
        userId: { $first: '$users' }
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        pipeline: [
          {
            $project: {
              _id: 1,
              createdAt: 1,
              emails: { $ifNull: ['$emails', []] },
              profile: 1,
              identity: 1,
              partners: 1
            }
          }
        ],
        as: 'users'
      }
    },
    {
      $unwind: '$users'
    }
  ]
  return query
}

export const getAppRolesForQuery = async (params, searchingData = {}) => {
  const { query, options } = params
  const { type, status, partnerId, context } = query
  const { limit, skip, sort } = options

  const preparedQuery = {}
  if (type) preparedQuery.type = type
  if (partnerId) preparedQuery.partnerId = partnerId

  const pipeline = []
  const pipelineQuery = prepareQueryPipelineForAppRoles(preparedQuery)
  pipeline.push(...pipelineQuery)
  if (size(status) && !size(searchingData) && context !== 'partnerRoles') {
    pipeline.push({
      $match: {
        'users.partners.status': status
      }
    })
  }
  if (size(searchingData)) {
    pipeline.push(searchingData)
  }
  pipeline.push({ $sort: sort }, { $skip: skip }, { $limit: limit })
  let appRoles = await AppRoleCollection.aggregate(pipeline)

  appRoles = appRoles.map((appRole) => {
    if (size(appRole.users.profile)) {
      appRole.users.profile.avatarKey = userHelper.getAvatar(appRole.users)
    }
    return appRole
  })

  return appRoles
}

export const countAppRoles = async (
  query = {},
  session,
  searchingData = {}
) => {
  const pipeline = []
  const { status = [], type = {}, partnerId } = query

  const preparedQuery = {}
  if (size(type)) {
    preparedQuery.type = type
  }
  if (partnerId) {
    preparedQuery.partnerId = partnerId
  }
  if (size(preparedQuery)) {
    pipeline.push({ $match: preparedQuery })
  }

  pipeline.push(
    {
      $unwind: '$users'
    },
    //To take only one user one time
    {
      $group: {
        _id: '$users',
        type: { $push: '$type' },
        userId: { $first: '$users' }
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        pipeline: [
          {
            $project: {
              _id: 1,
              createdAt: 1,
              emails: { $ifNull: ['$emails', []] },
              profile: 1,
              identity: 1,
              partners: 1
            }
          }
        ],
        as: 'users'
      }
    },
    {
      $unwind: '$users'
    }
  )

  if (!size(searchingData) && size(status)) {
    pipeline.push({
      $match: {
        'users.partners.status': status
      }
    })
  }
  if (size(searchingData)) {
    pipeline.push(searchingData)
  }
  pipeline.push({
    $group: {
      _id: null,
      count: { $sum: 1 }
    }
  })

  const numberOfAppRoles = await AppRoleCollection.aggregate(pipeline)
  return numberOfAppRoles[0]?.count || 0
}

export const prepareSearchQueryForAppRoles = (query) => {
  const { name, email, defaultSearchText } = query
  let searchingData = {}
  if (defaultSearchText) {
    searchingData = {
      $match: {
        $or: [
          {
            'users.emails.address': { $regex: defaultSearchText, $options: 'i' }
          },
          {
            'users.profile.name': { $regex: defaultSearchText, $options: 'i' }
          }
        ]
      }
    }
  } else if (name) {
    searchingData = {
      $match: {
        'users.profile.name': { $regex: name, $options: 'i' }
      }
    }
  } else if (email) {
    searchingData = {
      $match: {
        'users.emails.address': { $regex: email, $options: 'i' }
      }
    }
  }
  return searchingData
}

export const queryAppRoles = async (req) => {
  try {
    const { body } = req
    const { query, options } = body
    appHelper.validateSortForQuery(options.sort)
    const searchingData = prepareSearchQueryForAppRoles(query)
    body.query = prepareAppRolesQueryBasedOnFilters(query)
    const appRolesData = await getAppRolesForQuery(body, searchingData)
    const filteredDocuments = await countAppRoles(body.query, '', searchingData)
    const totalDocuments = await countAppRoles({})

    return {
      data: appRolesData,
      metaData: { filteredDocuments, totalDocuments }
    }
  } catch (e) {
    throw new CustomError(
      e.statusCode || 500,
      e.message || 'Internal Server Error'
    )
  }
}

export const appRolesQueryForPartnerApp = async (req) => {
  try {
    const { body, user = {} } = req
    const { query, options } = body
    const { partnerId } = user
    appHelper.checkRequiredFields(['userId', 'partnerId'], user)
    appHelper.validateSortForQuery(options.sort)

    body.query.partnerId = partnerId
    const searchingData = prepareSearchQueryForAppRoles(query)
    body.query = prepareAppRolesQueryForPartnersBasedOnFilters(query)
    const appRolesData = await getAppRolesForQuery(body, searchingData)
    const filteredDocuments = await countAppRoles(body.query, '', searchingData)
    const totalDocuments = await countAppRoles({ partnerId })
    if (size(appRolesData)) {
      appRolesData.forEach((item) => {
        item.imgUrl = userHelper.getAvatar(appRolesData) || ''
      })
    }
    return {
      data: appRolesData,
      metaData: { filteredDocuments, totalDocuments }
    }
  } catch (e) {
    throw new CustomError(
      e.statusCode || 500,
      e.message || 'Internal Server Error'
    )
  }
}

export const validateManagerData = (body) => {
  const { userId = '' } = body
  if (!userId) throw new CustomError(400, 'Required userId')
  appHelper.validateId({ userId })
}

export const prepareAppManagerRemovingQuery = (body) => {
  const { userId = '' } = body
  const query = {
    partnerId: { $exists: false },
    type: 'app_manager',
    users: { $in: [userId] }
  }

  return query
}

export const prepareDataForAppManagerRemove = (body) => {
  const { userId } = body
  const data = { $pull: { users: userId } }
  return data
}
