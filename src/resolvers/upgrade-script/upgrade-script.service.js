import { isEmpty, size } from 'lodash'

import { userHelper } from '../helpers'

import { TenantCollection } from '../models'

import {
  appQueueService,
  branchService,
  partnerService,
  tenantService,
  userService
} from '../services'

import { CustomError } from '../common'

export const changeAppQueueStatus = async (
  queueIds = [],
  status = 'new',
  session
) => {
  console.log('Updating app queue for: ', queueIds)
  const query = {
    _id: { $in: queueIds }
  }
  const updateData = { status }
  if (status === 'completed') {
    updateData.isManuallyCompleted = true
  }
  const isUpdated = await appQueueService.updateAppQueueItems(
    query,
    { $set: updateData },
    session
  )
  console.log('is Updated app queues ', isUpdated)
  return isUpdated
}

export const addASelfServicePartnerForUniteLiving = async (session) => {
  const owner = await userHelper.getUserById('vsWWKJzwRDdhgjn6Y')
  if (!owner?._id) {
    throw new Error('Could not find owner!')
  }
  const { _id: ownerId } = owner || {}
  const partnerCreationData = {
    // Set partner creation data/info
    accountType: 'broker',
    country: 'NO',
    createdBy: ownerId,
    isActive: true,
    isSelfService: true,
    name: 'Unite Living Self Service Norway',
    ownerId,
    sms: false,
    subDomain: 'ss-no'
  }
  const [partner] = await partnerService.createAPartner(
    partnerCreationData,
    session
  )
  const [agent] = await userService.createAnUserWithNameAndEmail(
    { email: 'b2c@uniteliving.com', name: 'Unite Living' },
    session
  )
  const { _id: agentId } = agent || {}
  if (!agentId) throw new CustomError(404, 'Could not create agent')

  await partnerService.addRolesForPartner(
    { ...partner.toObject(), ownerId: agentId },
    session
  )
  const defaultBranchData = {
    name: 'Main Branch',
    agents: [agentId],
    adminId: ownerId,
    partnerId: partner._id,
    createdBy: ownerId
  }
  const createdBranch = await branchService.createABranch(
    defaultBranchData,
    session
  )
  if (isEmpty(createdBranch)) {
    throw new CustomError(404, 'Unable to create branch')
  }
  await partnerService.addDefaultPartnerSetting(partner.toObject(), session)
  const taxCodeIds = await partnerService.addTaxCodeForPartner(
    partner.toObject(),
    session
  )
  const ledgerAccountIds = await partnerService.addLedgerAccountForPartner(
    partner.toObject(),
    taxCodeIds,
    session
  )
  await partnerService.addAccountingForPartner(
    partner.toObject(),
    ledgerAccountIds,
    session
  )
  await partnerService.addAddonsForPartner(
    partner.toObject(),
    ledgerAccountIds,
    session
  )
  const updatedPartner = await partnerService.addSerialInPartner(
    partner.toObject(),
    session
  )
  await partnerService.addRoomItemForPartner(partner.toObject(), session)
  await partnerService.addApiKeyForPartner(partner.toObject(), session)
  await partnerService.sendUserInvitation(
    {
      senderUserId: ownerId,
      invitedUser: owner,
      partnerId: partner._id
    },
    session
  )
  await partnerService.sendUserInvitation(
    {
      senderUserId: agentId,
      invitedUser: agent,
      partnerId: partner._id
    },
    session
  )
  return updatedPartner
}

export const removePropertyObjectFromTenants = async (session) => {
  const tenants = await TenantCollection.aggregate([
    {
      $match: {
        properties: { $exists: true }
      }
    },
    {
      $unwind: {
        path: '$properties',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: {
          id: '$_id',
          contractId: '$properties.contractId'
        },
        contractId: { $first: '$properties.contractId' },
        contracts: {
          $push: {
            $cond: [
              {
                $in: ['$properties.status', ['active', 'closed', 'in_progress']]
              },
              '$properties.contractId',
              '$$REMOVE'
            ]
          }
        },
        properties: { $push: '$properties' }
      }
    },
    {
      $match: {
        $expr: {
          $gt: [{ $size: '$contracts' }, 1]
        }
      }
    }
  ]).session(session)

  if (!size(tenants)) {
    console.log('Tenants not found')
    return tenants
  }

  const promiseArray = []
  for (let i = 0; i < tenants.length; i++) {
    const tenant = tenants[i]
    console.log({ i, tenant })

    promiseArray.push(
      tenantService.updateATenant(
        {
          _id: tenant?._id?.id,
          properties: {
            $elemMatch: {
              contractId: tenant?.contractId,
              status: 'in_progress'
            }
          }
        },
        { $pull: { properties: { status: 'in_progress' } } },
        session
      )
    )
  }

  await Promise.all(promiseArray)
}
