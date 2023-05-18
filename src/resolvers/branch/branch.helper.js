import { omit } from 'lodash'
import { BranchCollection } from '../models'
import { accountHelper, appHelper } from '../helpers'

export const isBranchSerialIdExists = async (query, session) => {
  const isAlreadyExists = !!(await BranchCollection.findOne(query).session(
    session
  ))
  return isAlreadyExists
}

export const prepareBranchDataForInsert = (branchData, user) => {
  const { adminId } = branchData
  const { userId } = user
  if (adminId) {
    branchData.agents = [adminId]
  }
  if (userId) {
    branchData.createdBy = userId
  }
  return branchData
}

export const getBranchById = async (id, session) => {
  const branch = await BranchCollection.findById(id).session(session)
  return branch
}

export const getABranch = async (query, session) => {
  const branch = await BranchCollection.findOne(query).session(session)
  return branch
}

export const getBranches = async (query, session) => {
  const branches = await BranchCollection.find(query).session(session)
  return branches
}

export const getBranchesForQuery = async (params) => {
  const { query, options } = params
  const { limit, skip, sort } = options
  const branches = await BranchCollection.find(query)
    .populate(['adminUser', 'partner', 'agentsInfo'])
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return branches
}

const lookupAgentUserInfo = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'agents',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getUserEmailPipeline(),
        {
          $project: {
            _id: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline(
              '$profile.avatarKey',
              'assets/default-image/user-primary.png'
            ),
            email: 1,
            name: '$profile.name'
          }
        }
      ],
      as: 'users'
    }
  },
  {
    $unwind: {
      path: '$users',
      preserveNullAndEmptyArrays: true
    }
  }
]

const groupAndFinalProjectByBranchName = () => [
  {
    $group: {
      _id: '$_id',
      adminId: {
        $first: '$adminId'
      },
      partnerId: {
        $first: '$partnerId'
      },
      branchSerialId: {
        $first: '$branchSerialId'
      },
      agents: {
        $push: '$users'
      },
      name: { $first: '$name' }
    }
  }
]

export const getBranchesAndUserRolesForQuery = async (partnerId) => {
  const pipeline = []
  pipeline.push(
    {
      $match: {
        partnerId
      }
    },
    {
      $unwind: {
        path: '$agents',
        preserveNullAndEmptyArrays: true
      }
    },
    ...lookupAgentUserInfo(),
    ...groupAndFinalProjectByBranchName()
  )

  const branchInfo = await BranchCollection.aggregate(pipeline)
  return branchInfo
}

const prepareQueryForBranchesDropdown = async (query) => {
  const { accountId, agentId, searchString } = query
  if (searchString) query.name = new RegExp('.*' + searchString + '.*', 'i')
  if (agentId) query.agents = agentId
  if (accountId) {
    const accountInfo = (await accountHelper.getAccountById(accountId)) || {}
    const { branchId } = accountInfo
    if (branchId) query._id = branchId
  }
  const preparedQuery = omit(query, ['accountId', 'agentId', 'searchString'])
  return preparedQuery
}

export const getBranchesDropdownForQuery = async (params) => {
  const { query, options, populate = [] } = params
  const { limit, skip } = options
  const branchesDropdownData = await BranchCollection.find(query, {
    _id: 1,
    name: 1,
    agents: 1
  })
    .populate(populate)
    .sort({ name: 1 })
    .skip(skip)
    .limit(limit)
  return branchesDropdownData
}

export const countBranches = async (query, session) => {
  const numberOfBranches = await BranchCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfBranches
}

export const queryBranches = async (req) => {
  const { body } = req
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const branchesData = await getBranchesForQuery(body)
  const filteredDocuments = await countBranches(query)
  const totalDocuments = await countBranches({})
  return {
    data: branchesData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryBranchesDropdown = async (req) => {
  const { body, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId'], user)
  const { query, options } = body
  let populate
  if (partnerId) {
    appHelper.validateId({ partnerId })
    query.partnerId = partnerId
    //For partner app populate is needed but not for admin app
    populate = 'agentsInfo'
  }
  const preparedQuery = await prepareQueryForBranchesDropdown(query)
  const branchesDropdownData = await getBranchesDropdownForQuery({
    query: preparedQuery,
    options,
    populate
  })

  // To count filter dropdown documents
  const filteredDocuments = await countBranches(preparedQuery)
  const totalDocuments = await countBranches(
    preparedQuery.partnerId ? { partnerId: preparedQuery.partnerId } : {}
  )

  return {
    data: branchesDropdownData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryForBranchAndUserRoles = async (params) => {
  const { user } = params
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const branchAndUserRole = await getBranchesAndUserRolesForQuery(partnerId)
  return {
    data: branchAndUserRole
  }
}
