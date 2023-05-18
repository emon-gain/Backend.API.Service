import { size, isEmpty } from 'lodash'
import { CustomError } from '../common'
import { AppRoleCollection, UserCollection } from '../models'
import { appRoleHelper, appHelper, branchHelper, userHelper } from '../helpers'
import { branchService, userService } from '../services'

export const updateARole = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for roles update')
  }

  const updatedRoleData = await AppRoleCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )

  if (!size(updatedRoleData)) {
    throw new CustomError(404, `Unable to update role`)
  }
  return updatedRoleData
}

export const removeAppManagerRole = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found to remove appManager')
  }
  const updatedRoles = await updateARole(query, data, session)
  return updatedRoles
}

export const updateRoles = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for roles update')
  }

  const updatedRoles = await AppRoleCollection.updateMany(query, data, {
    session
  })
  if (isEmpty(updatedRoles)) {
    throw new CustomError(404, `Unable to update roles`)
  }
  return updatedRoles
}

export const updateAppRole = async (req) => {
  const { body, session } = req
  await appRoleHelper.validatePutRequest(body)
  const query = appRoleHelper.prepareUpdateQuery(body)
  const updateData = appRoleHelper.prepareUpdateData(body)
  if (!size(query)) {
    throw new CustomError(404, 'Not found any resource to update')
  }
  const updateAppRole = await AppRoleCollection.findOneAndUpdate(
    query,
    updateData,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  return updateAppRole
}

export const createRoles = async (rolesArray, session) => {
  if (!size(rolesArray)) {
    throw new CustomError(404, 'Unable to create roles')
  }
  const createdRoles = await AppRoleCollection.create(rolesArray, { session })
  return createdRoles
}

export const addAppManager = async (req) => {
  const { body, session, user = {} } = req

  appHelper.checkUserId(user.userId)
  await appRoleHelper.validateManagerData(body)

  const query = { type: 'app_manager' }
  const data = { $addToSet: { users: body.userId } }

  const appManager = await updateARole(query, data, session)
  return appManager
}

export const addAppManagerForPartnerSite = async (req) => {
  const { body, user } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['type', 'userIds'], body)
  const { type, userIds } = body
  return await updateARole({ type, partnerId }, { $set: { users: userIds } })
}

export const addAgentUserRoleForPartnerApp = async (req) => {
  const { body, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['branchId', 'agentIds'], body)

  const { agentIds, branchId } = body
  const updatedBranch = await branchService.updateABranch(
    { _id: branchId, partnerId },
    { $set: { agents: agentIds } }
  )
  if (size(updatedBranch)) {
    const users = await UserCollection.aggregate([
      {
        $match: { _id: { $in: agentIds } }
      },
      {
        $project: {
          _id: 1,
          emails: 1,
          avatarKey: appHelper.getUserAvatarKeyPipeline(
            '$profile.avatarKey',
            'assets/default-image/user-primary.png'
          ),
          'profile.name': 1
        }
      },
      ...appHelper.getUserEmailPipeline(),
      {
        $project: {
          _id: 1,
          email: 1,
          name: '$profile.name',
          avatarKey: 1
        }
      }
    ])
    return users
  }
}

export const removeAppManager = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appRoleHelper.validateManagerData(body)

  const query = appRoleHelper.prepareAppManagerRemovingQuery(body)
  const data = appRoleHelper.prepareDataForAppManagerRemove(body)

  const appManager = await removeAppManagerRole(query, data, session)
  return appManager
}

export const removeUserForPartnerApp = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['type', 'roleUserId'], body)

  const { userId, partnerId } = user
  const { type, roleUserId } = body

  if (type === 'partner_agent') {
    const branch = await branchHelper.getABranch({
      partnerId,
      agents: roleUserId
    })
    if (size(branch)) {
      throw new CustomError(
        400,
        'Can not remove partner agent when an agent assigned to any branches'
      )
    }
  }
  if (type === 'partner_admin' && userId === roleUserId) {
    throw new CustomError(400, 'Partner admin user can not removed self')
  }

  const isExistUser = await appRoleHelper.getAppRole({
    type,
    partnerId,
    users: roleUserId
  })

  if (size(isExistUser)) {
    return await updateARole(
      { type, partnerId },
      { $pull: { users: roleUserId } }
    )
  } else {
    throw new CustomError(404, 'User does not exist')
  }
}

export const updatePartnerUserEmployeeIdForPartnerApp = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['partnerUserId', 'partnerEmployeeId'], body)

  const { partnerEmployeeId, partnerUserId } = body
  const isExistingEmployeeId = await userHelper.getAnUser({
    'partners.partnerId': partnerId,
    'partners.employeeId': partnerEmployeeId
  })

  if (size(isExistingEmployeeId))
    throw new CustomError(409, 'EmployeeId already exists')

  return await userService.updateAnUser(
    { _id: partnerUserId, 'partners.partnerId': partnerId },
    { $set: { 'partners.$.employeeId': partnerEmployeeId } },
    session
  )
}

export const removeAgentUserFromBranch = async (req) => {
  const { body, user = {} } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['agentId', 'branchId'], body)

  const { agentId, branchId } = body

  const query = {
    _id: branchId,
    partnerId
  }

  const data = {
    $pull: {
      agents: agentId
    }
  }

  const branch = await branchHelper.getABranch(query)

  if (!size(branch)) {
    throw new CustomError(404, 'Branch not found')
  }

  if (branch?.adminId !== agentId) {
    const updatedBranch = await branchService.updateABranch(query, data)
    return updatedBranch
  } else {
    throw new CustomError(400, 'Branch admin should not be removed')
  }
}
