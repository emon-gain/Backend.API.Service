import nid from 'nid'
import { assign, isEmpty, size } from 'lodash'

import { CustomError } from '../common'
import { ListingCollection, TenantCollection } from '../models'
import {
  appHelper,
  contractHelper,
  fileHelper,
  listingHelper,
  logHelper,
  partnerHelper,
  partnerSettingHelper,
  tenantHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  counterService,
  fileService,
  listingService,
  logService,
  notificationService,
  organizationService,
  partnerSettingService,
  propertyService,
  userService
} from '../services'

export const removeTenantInterestForm = async (partnerId, session) => {
  try {
    const partnerSettings = await partnerSettingHelper.getAPartnerSetting(
      { partnerId },
      session
    )
    const isEnableDeleteInterestForm =
      partnerSettings?.tenantSetting?.deleteInterestForm?.enabled
    const tenantInterestFormRetention =
      partnerSettings?.tenantSetting?.deleteInterestForm?.months
    const isEnableDeleteProspects =
      partnerSettings?.tenantSetting?.removeProspects?.enabled
    const tenantProspectRetention =
      partnerSettings?.tenantSetting?.removeProspects?.months
    const dateAfterInterestFromRetentionSubtract = (
      await appHelper.getActualDate(partnerSettings, true, new Date())
    )
      .subtract(tenantInterestFormRetention, 'months')
      .toDate()
    console.log(
      'Date after the interest form will be deleted',
      dateAfterInterestFromRetentionSubtract
    )
    console.log('Delete interest form of the partner', partnerId)
    const dateAfterProspectRetentionSubtract = (
      await appHelper.getActualDate(partnerSettings, true, new Date())
    )
      .subtract(tenantProspectRetention, 'months')
      .toDate()
    console.log(
      'Date after the prospect will be deleted',
      dateAfterProspectRetentionSubtract
    )
    console.log('Delete prospect of the partner', partnerId)
    const contractForFileIds = await getFileDataForDeleteInterestForm(
      dateAfterInterestFromRetentionSubtract,
      dateAfterProspectRetentionSubtract,
      partnerId
    )
    console.log('Contract data for deleting file', contractForFileIds)
    let hasProspect = true
    if (isEnableDeleteInterestForm) {
      await TenantCollection.updateMany(
        {
          partnerId,
          'properties.status': {
            $in: ['invited', 'rejected', 'interested', 'not_interested']
          },
          'properties.numberOfTenant': {
            $gt: 0
          },
          'properties.createdAt': {
            $lte: dateAfterInterestFromRetentionSubtract
          }
        },
        {
          $unset: {
            'properties.$[].numberOfTenant': 1,
            'properties.$[].interestFormMeta': 1,
            'properties.$[].fileIds': 1,
            'properties.$[].wantsRentFrom': 1
          }
        },
        {
          new: true,
          runValidators: true,
          session
        }
      )
      hasProspect = false
    }
    if (isEnableDeleteProspects) {
      await TenantCollection.updateMany(
        {
          partnerId,
          'properties.status': {
            $in: ['invited', 'rejected', 'interested', 'not_interested']
          },
          'properties.createdAt': {
            $lte: dateAfterProspectRetentionSubtract
          }
        },
        {
          $pull: {
            properties: {
              status: {
                $in: ['invited', 'rejected', 'interested', 'not_interested']
              },
              createdAt: {
                $lte: dateAfterProspectRetentionSubtract
              }
            }
          }
        },
        {
          new: true,
          session
        }
      )

      const userIdData = await TenantCollection.aggregate([
        {
          $match: {
            partnerId,
            'properties.0': {
              $exists: false
            }
          }
        },
        {
          $group: {
            _id: null,
            userIds: {
              $push: '$userId'
            }
          }
        }
      ]).session(session)
      await TenantCollection.deleteMany({
        partnerId,
        'properties.0': {
          $exists: false
        }
      }).session(session)
      const userIds = userIdData[0]?.userIds || []
      await userService.updateMultipleUser(
        {
          _id: {
            $in: userIds
          }
        },
        {
          $pull: {
            partners: {
              partnerId
            }
          }
        }
      )
      hasProspect = false
    }

    if (hasProspect === false) {
      console.log('contractForFileIds', contractForFileIds)
      const fileIds = contractForFileIds[0]?.fileIds || []
      const deletedFiles = await fileService.deleteMultipleFile(fileIds)
      console.log('deletedFiles', deletedFiles)
      const propertyId = contractForFileIds[0]?.propertyId || []
      await propertyService.updateMultipleProperty(
        {
          _id: {
            $in: propertyId
          }
        },
        {
          $set: {
            hasProspect: false
          }
        }
      )
    }
    return contractForFileIds[0]?.fileData || []
  } catch (e) {
    console.log('Tenant interest form error', e)
    throw new Error('Cannot delete interest form or prospect')
  }
}

const getFileDataForDeleteInterestForm = async (
  dateAfterInterestFromRetentionSubtract,
  dateAfterProspectRetentionSubtract,
  partnerId
) =>
  await TenantCollection.aggregate([
    {
      $match: {
        partnerId
      }
    },
    {
      $unwind: {
        path: '$properties',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$properties.fileIds',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'files',
        foreignField: '_id',
        localField: 'properties.fileIds',
        as: 'files'
      }
    },
    {
      $unwind: {
        path: '$files',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: null,
        propertyIdWithProspect: {
          $addToSet: {
            $cond: {
              if: {
                $and: [
                  {
                    $in: [
                      '$properties.status',
                      ['invited', 'rejected', 'interested', 'not_interested']
                    ]
                  },
                  {
                    $or: [
                      {
                        $gt: [
                          '$properties.createdAt',
                          dateAfterProspectRetentionSubtract
                        ]
                      },
                      {
                        $gt: [
                          '$properties.createdAt',
                          dateAfterInterestFromRetentionSubtract
                        ]
                      }
                    ]
                  }
                ]
              },
              then: '$properties.propertyId',
              else: '$$REMOVE'
            }
          }
        },
        propertyIds: {
          $addToSet: {
            $cond: {
              if: {
                $and: [
                  {
                    $in: [
                      '$properties.status',
                      ['invited', 'rejected', 'interested', 'not_interested']
                    ]
                  },
                  {
                    $gt: ['$properties.numberOfTenant', 0]
                  },
                  {
                    $or: [
                      {
                        $lte: [
                          '$properties.createdAt',
                          dateAfterProspectRetentionSubtract
                        ]
                      },
                      {
                        $lte: [
                          '$properties.createdAt',
                          dateAfterInterestFromRetentionSubtract
                        ]
                      }
                    ]
                  }
                ]
              },
              then: '$properties.propertyId',
              else: '$$REMOVE'
            }
          }
        },
        fileIds: {
          $addToSet: {
            $cond: {
              if: { $ifNull: ['$properties.fileIds', false] },
              then: '$properties.fileIds',
              else: '$$REMOVE'
            }
          }
        },
        fileData: {
          $addToSet: {
            $cond: {
              if: { $ifNull: ['$properties.fileIds', false] },
              then: '$files',
              else: '$$REMOVE'
            }
          }
        }
      }
    }
  ])

export const createATenant = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(404, `Unable to create tenant`)
  }
  const tenant = await TenantCollection.create([data], { session })
  return tenant
}

export const updateTenant = async (query, data, session) => {
  if (isEmpty(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedTenant = await TenantCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  if (isEmpty(updatedTenant)) {
    throw new CustomError(404, `Unable to update Tenant`)
  }
  return updatedTenant
}

export const tenantAfterInsertOperations = async (createdTenant, session) => {
  const { partnerId } = createdTenant
  if (createdTenant.userId) {
    createdTenant.type = 'tenant'
    await userService.addRelationBetweenUserAndPartner(createdTenant, session)
  }
  if (size(createdTenant.properties)) {
    // Todo :: Need to write test case for updatePropertiesForHasProspects
    const propertyInfo = createdTenant.properties[0]
    await updatePropertiesForHasProspects(partnerId, propertyInfo, session)
  }
  const ssn = await tenantHelper.hasNorwegianNationalIdentification(
    createdTenant,
    session
  )
  if (createdTenant.creditRatingTermsAcceptedOn && ssn) {
    // Todo :: Need to write test case for GenerateCreditRating
    const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
      partnerId
    )
    const automaticCreditRatingEnabled =
      partnerSettings?.tenantSetting?.automaticCreditRating?.enabled || false
    if (automaticCreditRatingEnabled) {
      await generateCreditRating(
        {
          tenantInfo: createdTenant,
          ssn
        },
        session
      )
    }
  }
}

export const tenantAfterUpdateOperations = async (params, session) => {
  const { oldTenant, updatedTenant, partnerId } = params
  if (updatedTenant.userId !== oldTenant.userId) {
    // AddRelationBetweenUserAndPartner
    await userService.addRelationBetweenUserAndPartner(updatedTenant, session)
  }
  // Todo :: Need to write test case for added after update hook operations
  // Todo :: Need to write test case for updatePropertiesForHasProspects
  // Todo :: Need to write test case for createLogForChangeTenantName
  const property = updatedTenant.properties.pop()
  if (size(property))
    await updatePropertiesForHasProspects(partnerId, property, session)

  if (updatedTenant.name !== oldTenant.name) {
    await createLogForChangeTenantName(params, session)
  }
}

export const updateTenants = async (query, data, session) => {
  if (isEmpty(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedTenants = await TenantCollection.updateMany(query, data, {
    session,
    runValidators: true
  })
  if (updatedTenants.nModified > 0) {
    return updatedTenants
  }
}

export const updateATenant = async (query, data, session, populate = []) => {
  const updatedTenant = await TenantCollection.findOneAndUpdate(query, data, {
    new: true,
    session
  }).populate(populate)
  return updatedTenant
}

export const updateOrCreateUserForTenant = async (data, session) => {
  const {
    userId,
    name,
    email,
    phoneNumber,
    norwegianNationalIdentification,
    organizationNumber
  } = data

  const existingUser = await userHelper.getUserByEmail(email, session)

  if (norwegianNationalIdentification && organizationNumber) {
    throw new CustomError(400, 'Please use SSN or organization number')
  }

  if (organizationNumber && organizationNumber.length !== 9) {
    throw new CustomError(400, 'Organization number must be 9 digits')
  }

  // TODO:: Later need to write test case for nid check.
  if (norwegianNationalIdentification) {
    const params = {
      norwegianNationalIdentification:
        existingUser?.profile?.norwegianNationalIdentification,
      currentNorwegianNationalId: norwegianNationalIdentification
    }
    existingUser
      ? await tenantHelper.checkNIDDuplication(params)
      : await appHelper.checkNIDDuplication(norwegianNationalIdentification)
  }

  if (size(existingUser)) {
    const personUserData = {}
    const oldPhone = existingUser?.profile?.phoneNumber
    phoneNumber && !oldPhone
      ? (personUserData['profile.phoneNumber'] = phoneNumber)
      : ''
    const oldNID = existingUser?.profile?.norwegianNationalIdentification
    const nid = norwegianNationalIdentification
      ? tenantHelper.nidValidationCheck(oldNID, norwegianNationalIdentification)
      : ''
    nid
      ? (personUserData['profile.norwegianNationalIdentification'] =
          norwegianNationalIdentification)
      : ''
    if (organizationNumber) {
      personUserData['profile.organizationNumber'] = organizationNumber
    }
    const query = { _id: existingUser._id }

    size(personUserData)
      ? await userService.updateAnUser(query, personUserData, session)
      : ''
    return existingUser
  } // User doesn't exists with the email, create a new user and return createdUserId
  if (!userId) {
    const userData = {
      email: email.toLowerCase(),
      profile: { name }
    }
    phoneNumber ? (userData.profile.phoneNumber = phoneNumber) : ''
    norwegianNationalIdentification
      ? (userData.profile.norwegianNationalIdentification =
          norwegianNationalIdentification)
      : ''
    if (organizationNumber) {
      userData.profile.organizationNumber = organizationNumber
    }
    const insertedUser = await userService.createAnUserWithNameAndEmail(
      userData,
      session
    )
    return insertedUser[0]
  }
  return { _id: userId }
}

export const createTenant = async (req) => {
  const { body, session, user = {} } = req
  const { partnerId, userId } = user
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  appHelper.checkRequiredFields(['data'], body)
  const { data } = body
  appHelper.checkRequiredFields(['email', 'propertyId'], data)
  let tenantData = {}

  data.createdBy = userId
  const { createdBy, propertyId, status } = data

  const contractId = await contractHelper.getUpcomingContractIdByPropertyId(
    partnerId,
    propertyId,
    session
  )

  const tenantUser = await updateOrCreateUserForTenant(data, session)
  data.userId = tenantUser._id

  const listingQuery = { _id: propertyId, partnerId }
  const propertyInfo = await listingHelper.getAListing(listingQuery, session)
  if (size(propertyInfo)) {
    const tenantPropertiesArrayData = {
      propertyInfo,
      contractId,
      status,
      createdBy
    }
    tenantData = tenantHelper.preparePropertiesArrayDataForTenant(
      tenantPropertiesArrayData
    )
    data.properties = [tenantData]
  }
  const tenantQuery = { userId: data.userId, partnerId }
  const tenantInfo = await tenantHelper.getATenant(tenantQuery, session)

  const userAvatarKey = userHelper.getAvatar(tenantUser)

  if (size(tenantInfo)) {
    if (tenantInfo.type === 'archived')
      throw new CustomError(400, 'Tenant is archived')
    const dataForActivePropertiesArray = {
      tenantInfo,
      tenantData,
      contractId
    }
    const activePropertiesArray =
      tenantHelper.prepareActivePropertiesArrayForTenant(
        dataForActivePropertiesArray
      )
    data.properties = activePropertiesArray
    data.lastUpdate = new Date()
    const query = { _id: tenantInfo._id }

    const updatedTenant = await updateTenant(query, { $set: data }, session)
    const tenantAfterUpdateData = {
      updatedTenant: updatedTenant.toObject(),
      oldTenant: tenantInfo,
      partnerId,
      userId
    }
    await tenantAfterUpdateOperations(tenantAfterUpdateData, session)
    updatedTenant.avatarKey = userAvatarKey

    return updatedTenant
  }
  data.type = 'active'
  data.partnerId = partnerId

  const [createdTenant] = await createATenant(data, session)
  await tenantAfterInsertOperations(createdTenant.toObject(), session) // Todo:: After insert hook of tenant
  createdTenant.avatarKey = userAvatarKey
  return createdTenant
}

export const updateTenantAbout = async (req) => {
  const { body, user, session } = req
  const requiredFields = ['tenantId', 'aboutText']
  appHelper.checkRequiredFields(requiredFields, body)
  const { tenantId, aboutText } = body
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  appHelper.validateId({ tenantId })
  const query = {
    _id: tenantId,
    partnerId
  }
  const updatedTenant = await updateTenant(query, { aboutText }, session)
  return updatedTenant
}

export const addTenantSerial = async (params, session) => {
  const { tenantId, partnerId } = params
  const newTenantSerial = await counterService.incrementCounter(
    `tenant-${partnerId}`,
    session
  )
  // Adding tenant serial
  const query = { _id: tenantId, partnerId }
  const data = { serial: newTenantSerial }
  await updateTenant(query, data, session)
}

export const updateTenantTypeStatus = async (req) => {
  const { body, session } = req
  appHelper.validatePartnerAppRequestData(req, ['tenantId', 'changeStatus'])
  const { changeStatus, partnerId, tenantId } = body

  const query = {
    _id: tenantId,
    partnerId
  }

  const tenantInfo = await tenantHelper.getATenant(query)
  if (!size(tenantInfo)) {
    throw new CustomError(404, 'Tenant not found')
  }
  if (tenantInfo.type !== changeStatus) {
    const updatedTenant = await updateTenant(
      query,
      { type: changeStatus },
      session
    )
    return {
      _id: updatedTenant._id,
      type: updatedTenant.type
    }
  }
  throw new CustomError(400, 'Tenant already updated')
}

export const updateTenantPropertyStatus = async (req) => {
  const { body = {}, session, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const dataNeedTobeMerge =
    await appHelper.validateSelfServicePartnerRequestAndUpdateBody(
      user,
      session
    )
  assign(body, dataNeedTobeMerge)
  body.partnerId = partnerId
  await tenantHelper.validateDataForUpdateTenantPropertyStatus(body)
  const { propertyId, status, tenantId } = body
  const query = {
    _id: tenantId,
    partnerId,
    properties: {
      $elemMatch: { propertyId }
    }
  }
  const data = { 'properties.$.status': status }
  await updateTenant(query, data)
  return {
    _id: tenantId,
    status
  }
}

export const createTenantUpdatedLog = async (action, options, session) => {
  const tenantLogData = await logHelper.prepareTenantUpdatedLogData(
    action,
    options,
    session
  )
  if (!size(tenantLogData)) {
    throw new CustomError(404, 'Could not create log for tenant update')
  }
  await logService.createLog(tenantLogData, session)
}

export const updateTenantInfo = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkUserId(user.userId)
  if (user?.roles && !user.roles.includes('lambda_manager')) {
    body.partnerId = user.partnerId
  }
  appHelper.checkRequiredFields(['tenantId', 'partnerId'], body)

  const { tenantId, partnerId } = body
  const query = { _id: tenantId, partnerId }

  const previousTenantInfo = await tenantHelper.getATenant(query, session, [
    'user'
  ])

  body.tenantInfo = previousTenantInfo

  let updatedTenant = {}
  let changesFields = []
  const { tenantUpdatingData, userUpdatingData } =
    await tenantHelper.prepareDataForTenantOrUserUpdate(body, session)

  if (!(size(tenantUpdatingData) || size(userUpdatingData)))
    throw new CustomError(404, 'Failed to update Tenants Data')

  if (size(userUpdatingData)) {
    const previousUserInfo = previousTenantInfo?.user || {}
    await userService.updateAnUser(
      { _id: previousUserInfo._id },
      userUpdatingData,
      session
    )
    const changesFieldsArray = tenantHelper.prepareChangesFieldsArrayOfUser(
      previousUserInfo,
      body
    )
    size(changesFieldsArray) ? (changesFields = changesFieldsArray) : ''
  }
  if (size(tenantUpdatingData)) {
    tenantUpdatingData.lastUpdate = new Date()
    const query = {
      _id: tenantId,
      partnerId
    }
    const { referenceNumber } = body
    if (referenceNumber)
      query['depositAccountMeta.kycForms'] = { $elemMatch: { referenceNumber } }
    updatedTenant = await updateTenant(query, tenantUpdatingData, session)
    if (!size(updatedTenant))
      throw new CustomError(404, 'Unable to update tenant')
    const params = {
      updatedTenant,
      previousTenantInfo
    }
    const changesFieldsArray =
      tenantHelper.prepareChangesFieldsArrayOfTenant(params)
    size(changesFieldsArray) ? changesFields.push(...changesFieldsArray) : ''
  }
  if (size(changesFields)) {
    // Creating a log for tenant.
    const options = {
      partnerId,
      collectionId: tenantId,
      collectionName: 'tenant',
      context: 'tenant',
      changesFields,
      previousDoc: previousTenantInfo
    }
    await createTenantUpdatedLog('updated_tenant', options, session)
  }
  return updatedTenant
}

export const updateTenantForPogo = async (req) => {
  const { body, session, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const query = await tenantHelper.prepareTenantsQuery(body, session)
  const { pullData, pushData, setData } =
    tenantHelper.prepareDataForTenantUpdateForPogo(body)
  await updateATenant(query, { $pull: pullData }, session)
  const updatedTenant = await updateATenant(
    query,
    { $addToSet: pushData, $set: setData },
    session,
    'user'
  )
  if (!size(updatedTenant)) {
    return {}
  }
  return tenantHelper.createTenantFieldNameForApi(updatedTenant)
}

export const changeTenantsProfileName = async (params = {}, session) => {
  if (!size(params)) {
    throw new CustomError(404, 'No data found for update')
  }
  const query = { userId: params._id }
  const data = { name: params.profile.name }
  await TenantCollection.updateMany(
    query,
    { $set: data },
    { session, runValidators: true }
  )
}

export const addInterestForm = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['listingId', 'userData'], body)
  const { userId } = user
  appHelper.validateId({ userId })
  const userInfo = await userHelper.getUserById(userId)
  if (!size(userInfo)) {
    throw new CustomError(401, 'Unauthorized')
  }

  const {
    listingId,
    userData,
    aboutYou,
    creditRatingTermsAcceptedOn,
    wantsRentFrom,
    numberOfTenant,
    preferredLengthOfLease
  } = body

  appHelper.validateId({ listingId })
  const propertyInfo = await listingHelper.getListingById(listingId)
  if (!size(propertyInfo)) {
    throw new CustomError(404, 'Listing not found')
  }

  const userUpdateData =
    await tenantHelper.prepareUserUpdateDataForInterestForm({
      userData,
      userInfo,
      aboutYou
    })
  if (size(userUpdateData)) {
    await userService.updateAnUser(
      {
        _id: userId
      },
      { $set: userUpdateData },
      session
    )
  }

  const partnerId = propertyInfo.partnerId
  if (partnerId) {
    const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
      partnerId
    )
    //Check for existing tenant
    const tenantInfo = await tenantHelper.getATenant({
      userId,
      partnerId
    })
    let tenantId = tenantInfo?._id
    if (!size(tenantInfo)) {
      const tenantCreateData =
        tenantHelper.prepareTenantCreateDataForInterestForm({
          propertyInfo,
          tenantInterestFormData: {
            aboutYou,
            creditRatingTermsAcceptedOn
          },
          tenantPropertiesData: {
            wantsRentFrom,
            numberOfTenant
          },
          userId,
          userData,
          userInfo
        })
      if (size(tenantCreateData)) {
        const [newTenantInfo] = await createATenant(tenantCreateData, session)
        if (size(newTenantInfo)) {
          tenantId = newTenantInfo._id
          // Creating queue for sending interest form invitation notification to tenant
          await sendEmailNotificationToInterestedTenant(
            {
              partnerId,
              tenantId: newTenantInfo._id,
              listingId
            },
            session
          )
        }
        await addInterestFormTenantInsertOperation(
          newTenantInfo,
          partnerSetting,
          session
        )
      }
    } else {
      const tenantUpdateData =
        tenantHelper.prepareTenantUpdateDataForInterestForm({
          creditRatingTermsAcceptedOn,
          listingInfo: propertyInfo,
          tenantInfo,
          tenantPropertiesData: {
            wantsRentFrom,
            numberOfTenant,
            preferredLengthOfLease
          },
          userId,
          userData
        })
      if (size(tenantUpdateData)) {
        const updatedTenant = await updateTenant(
          { _id: tenantInfo._id },
          { $set: tenantUpdateData },
          session
        )
        const tenantAfterUpdateData = {
          updatedTenant,
          partnerId,
          partnerSetting
        }
        await addInterestFormTenantUpdateOperation(
          tenantAfterUpdateData,
          session
        )
      }
    }
    await notificationService.insertANotification(
      {
        tenantId,
        propertyId: listingId,
        partnerId,
        type: 'interestedFormSubmitted'
      },
      session
    )
    //Send email to property owner when submit interest form
    const isSendEmail = partnerSetting?.notifications?.interestForm || false
    if (isSendEmail) {
      // Creating queue for sending interest form notification to owner
      await sendEmailNotificationToOwner(
        {
          partnerId,
          listingId,
          tenantId
        },
        session
      )
    }
  }
  return {
    result: true
  }
}

const sendEmailNotificationToInterestedTenant = async (
  params = {},
  session
) => {
  const { partnerId, tenantId, listingId } = params
  const appQueueData = {
    event: 'send_interest_form_invitation',
    action: 'send_notification',
    destination: 'notifier',
    params: {
      partnerId,
      collectionId: listingId,
      collectionNameStr: 'listings',
      options: {
        tenantId
      }
    },
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
}

const sendEmailNotificationToOwner = async (params = {}, session) => {
  const { listingId, partnerId, tenantId } = params
  const appQueueData = {
    action: 'send_notification',
    event: 'send_interest_form',
    destination: 'notifier',
    params: {
      partnerId,
      collectionId: listingId,
      collectionNameStr: 'listings',
      options: {
        tenantId
      }
    },
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
}

export const addFilesInTenants = async (req) => {
  const { body = {}, session, user } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(['fileData', 'propertyId'], body)
  const { userId } = user
  const { propertyId, fileData } = body
  const userInfo = await userHelper.getUserById(userId)
  if (!size(userInfo)) {
    throw new CustomError(401, 'Unauthorized')
  }
  const listingInfo = await listingHelper.getAListing({ _id: propertyId })
  if (!size(listingInfo)) {
    throw new CustomError(404, 'Listing not found')
  }
  const newFileIds = []
  const filesInfo = []
  for (const file of fileData) {
    const fileId = nid(17)
    newFileIds.push(fileId)
    filesInfo.push({
      ...file,
      _id: fileId,
      partnerId: listingInfo.partnerId,
      propertyId,
      accountId: listingInfo.accountId,
      directive: 'Files',
      context: 'interest_form',
      createdBy: user.userId
    })
  }
  await fileService.createFiles(filesInfo, session)
  const tenantInfo = await tenantHelper.getATenant({
    userId,
    partnerId: listingInfo.partnerId
  })
  if (size(tenantInfo)) {
    const updateData = prepareTenantUpdateDataForAddFilesInTenants({
      properties: tenantInfo.properties,
      newFileIds,
      userId,
      listingInfo
    })
    await updateATenant({ _id: tenantInfo }, updateData, session)
  } else {
    let { interestFormMeta = {} } = userInfo
    const { fileIds = [] } = interestFormMeta
    fileIds.push(...newFileIds)
    if (size(interestFormMeta)) interestFormMeta.fileIds = fileIds
    else interestFormMeta = { fileIds }
    await userService.updateAnUser(
      { _id: userId },
      { $set: { interestFormMeta } },
      session
    )
  }
  return filesInfo
}

const prepareTenantUpdateDataForAddFilesInTenants = (params = {}) => {
  const { properties = [], newFileIds, userId, listingInfo } = params
  let propertyInfo =
    properties.find((item) => item.propertyId === listingInfo._id) || {}
  const { fileIds = [] } = propertyInfo

  fileIds.push(...newFileIds)

  if (size(propertyInfo)) propertyInfo.fileIds = fileIds
  else {
    propertyInfo = {
      fileIds,
      status: 'interested',
      createdAt: new Date(),
      createdBy: userId,
      propertyId: listingInfo._id
    }
    if (listingInfo.accountId) propertyInfo.accountId = listingInfo.accountId
    if (listingInfo.branchId) propertyInfo.branchId = listingInfo.branchId
    if (listingInfo.agentId) propertyInfo.agentId = listingInfo.agentId
  }

  const updateProperties = properties.filter(
    (item) => item.propertyId !== propertyInfo.propertyId
  )
  updateProperties.push(propertyInfo)
  return { $set: { properties: updateProperties } }
}

export const askForCreditRating = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['tenantId'])
  const { body, session, user = {} } = req
  const { partnerId, tenantId } = body
  await appHelper.validateSelfServicePartnerRequestAndUpdateBody(user, session)
  const { enableCreditRating } =
    (await partnerHelper.getPartnerById(partnerId)) || {}
  if (!enableCreditRating)
    throw new CustomError(400, 'Partner credit rating is not enabled')
  const tenantInfo = await tenantHelper.getATenant({ _id: tenantId, partnerId })
  if (!size(tenantInfo)) throw new CustomError(404, 'Tenant not found')

  const token = nid(50)
  await updateTenant(
    { _id: tenantId },
    { $set: { tokenAskForCreditRatingUrl: token } },
    session
  )
  const appQueueData = {
    action: 'send_notification',
    destination: 'notifier',
    delaySeconds: 0,
    event: 'send_notification_ask_for_credit_rating',
    params: {
      partnerId,
      collectionId: tenantInfo._id,
      collectionNameStr: 'tenants',
      options: { tenantId, token }
    },
    priority: 'immediate',
    status: 'new'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
  return {
    result: true
  }
}

export const addTenantCreditRatingInfo = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['tenantId'])
  const { body, user, session } = req
  const { partnerId } = user
  if (partnerId) body.partnerId = partnerId
  const { appQueueData, tenantData } =
    await tenantHelper.validateAndPrepareDataForAddTenantCreditRatingInfo(body)
  let tenant = {}
  if (size(tenantData)) {
    const { data, query } = tenantData
    tenant = await updateTenant(query, data, session)
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
  return tenant
}

export const updateAndAddTenantKYCForm = async (req) => {
  const { body, session, user } = req
  const { userId } = user || {}
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(
    ['contractId', 'depositAmount', 'referenceNumber', 'tenantId'],
    body
  )

  const {
    contractId,
    depositAmount,
    referenceNumber,
    tenantId,
    appQueueParams
  } = body

  if (!(contractId && depositAmount && referenceNumber && tenantId)) {
    throw new CustomError(400, 'Missing required data')
  }

  const isCancelledPreviousKYCForm = size(
    await TenantCollection.findOneAndUpdate(
      {
        _id: tenantId,
        'depositAccountMeta.kycForms': {
          $elemMatch: { contractId, status: 'new' }
        }
      },
      { $set: { 'depositAccountMeta.kycForms.$.status': 'cancel' } },
      {
        runValidators: true,
        new: true,
        session
      }
    )
  )
  console.log(
    `====> Cancelled previous KYC form for tenantId: ${tenantId}, isCancelledPreviousKYCForm: ${isCancelledPreviousKYCForm} <====`
  )

  const updatedTenant = await updateATenant(
    { _id: tenantId },
    {
      $push: {
        'depositAccountMeta.kycForms': {
          contractId,
          depositAmount,
          referenceNumber,
          status: 'new'
        }
      }
    },
    session
  )
  if (updatedTenant) {
    const appQueueData = {
      event: 'handle_deposit_account_process',
      action: 'retrieve_tenant_deposit_account_pdf',
      priority: 'regular',
      destination: 'lease',
      params: appQueueParams,
      isSequential: true,
      sequentialCategory: `retrieve_tenant_deposit_account_pdf_${contractId}`
    }
    await appQueueService.createAnAppQueue(appQueueData, session)
  }
}

export const updatePropertyStatusInTenant = async (params, session) => {
  const { partnerId, tenantId, propertyId, contractId, status } = params
  if (status === 'active' || status === 'upcoming') {
    const tenantInfo = await tenantHelper.getATenant(
      { _id: tenantId, partnerId },
      session
    )
    if (size(tenantInfo) && !tenantInfo.serial) {
      await addTenantSerial(params, session)
    }
  }
  const query = {
    _id: tenantId,
    partnerId,
    properties: {
      $elemMatch: { propertyId, contractId }
    }
  }
  const data = { 'properties.$.status': status }
  console.log({ contractId, status })
  const updatedTenant = await updateATenant(query, data, session)
  return updatedTenant
}

export const downloadTenants = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.validatePartnerAppRequestData(req)

  const { userId, partnerId } = user
  const { query } = body

  query.partnerId = partnerId
  query.downloadProcessType = 'download_tenants'
  query.userId = userId
  const userInfo = await userHelper.getAnUser({ _id: userId })
  query.userLanguage = userInfo?.profile?.language
    ? userInfo.profile.language
    : 'en'

  const tenantQuery = await tenantHelper.prepareTenantsQueryForExcelCreator(
    query
  )

  await appHelper.isMoreOrLessThanTargetRows(ListingCollection, tenantQuery)

  const appQueueData = {
    action: 'download_email',
    event: 'download_email',
    destination: 'excel-manager',
    params: query,
    priority: 'immediate'
  }

  await appQueueService.createAnAppQueue(appQueueData)

  return {
    status: 202,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

export const tenantsAddSsnOrLandloardOrgId = async (req) => {
  await appHelper.validatePartnerAppRequestData(req)
  const { body = {}, session } = req
  const { partnerId, tenantsSsnOrLandlordInput } = body
  const errors = []
  if (size(tenantsSsnOrLandlordInput)) {
    for (const info of tenantsSsnOrLandlordInput) {
      if (!size(info)) {
        continue
      }
      if (
        info.ssn &&
        (await userHelper.getAnUser({
          'profile.norwegianNationalIdentification': info.ssn
        }))
      ) {
        errors.push({ userId: info.userId, msg: 'SSN already exists' })
      }
    }
    if (!size(errors)) {
      for (const info of tenantsSsnOrLandlordInput) {
        if (!size(info)) {
          continue
        }
        if (info.userId && info.ssn && !info.type) {
          await userService.updateAnUser(
            {
              _id: info.userId
            },
            {
              'profile.norwegianNationalIdentification': info.ssn
            },
            session
          )
        } else if (info.type && !info.userId && info.orgId) {
          if (info.type === 'partner' && info.orgId) {
            await partnerSettingService.updateAPartnerSetting(
              { partnerId },
              {
                'companyInfo.organizationId': info.orgId
              },
              session
            )
          } else if (
            info.type === 'account' &&
            info.organizationId &&
            info.orgId
          ) {
            await organizationService.updateAnOrganization(
              { _id: info.organizationId, partnerId },
              { orgId: info.orgId },
              session
            )
          }
        }
      }
    }
  } else {
    throw new CustomError(400, 'No data found')
  }
  return {
    errors
  }
}

export const uploadTenantProfileAvatarKey = async (req) => {
  await appHelper.validatePartnerAppRequestData(req, ['avatarKey', 'tenantId'])
  const { body = {} } = req
  const { partnerId, avatarKey, tenantId } = body

  const tenant = await tenantHelper.getATenant({ _id: tenantId, partnerId })
  if (!tenant) throw new CustomError(404, 'Tenant not found')

  const updatedUser = await userService.updateAnUser(
    {
      _id: tenant.userId
    },
    {
      'profile.avatarKey': avatarKey
    }
  )
  if (!updatedUser) {
    throw new CustomError(404, 'User not found')
  }
  return appHelper.getCDNDomain() + '/' + avatarKey
}

export const submitAskForCreditRating = async (req) => {
  const { body = {}, session } = req
  appHelper.checkRequiredFields(['tenantId', 'token'], body)
  const { isAskForCreditRating, tenantId, token, ssn } = body
  const tenantInfo = await tenantHelper.getATenant(
    {
      _id: tenantId
    },
    session,
    ['partnerSetting']
  )
  if (!size(tenantInfo)) throw new CustomError(404, 'Tenant not found')
  else if (!tenantInfo.tokenAskForCreditRatingUrl) {
    throw new CustomError(400, 'Ask for credit rating already submitted')
  } else if (tenantInfo.tokenAskForCreditRatingUrl !== token) {
    throw new CustomError(400, 'Invalid token')
  }
  if (ssn) {
    const existingSsn = await userHelper.getAnUser({
      _id: { $ne: tenantInfo.userId },
      'profile.norwegianNationalIdentification': ssn
    })
    if (size(existingSsn)) throw new CustomError(400, 'SSN already used')
    await userService.updateAnUser(
      {
        _id: tenantInfo.userId
      },
      { $set: { 'profile.norwegianNationalIdentification': ssn } },
      session
    )
  }
  await updateATenant(
    {
      _id: tenantId
    },
    {
      $set: {
        isAskForCreditRating: !!(isAskForCreditRating === 'yes'),
        creditRatingTermsAcceptedOn: new Date()
      },
      $unset: { tokenAskForCreditRatingUrl: '' }
    },
    session
  )
  if (
    ssn &&
    tenantInfo?.partnerSetting?.tenantSetting?.automaticCreditRating?.enabled
  ) {
    await generateCreditRating(
      {
        tenantInfo,
        ssn
      },
      session
    )
  }
  return {
    result: true
  }
}

export const generateCreditRating = async (params, session) => {
  const { tenantInfo, ssn } = params
  const appQueueData = {
    action: 'handle_credit_rating',
    destination: 'credit-rating',
    event: 'credit_rating',
    params: {
      partnerId: tenantInfo.partnerId,
      tenantId: tenantInfo._id,
      ssn,
      processType: 'add_credit_rating',
      createdBy: tenantInfo.userId
    },
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
}

export const deleteTenantInterestForm = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['propertyId', 'tenantId'])
  const { body, session, user = {} } = req
  const { partnerId, propertyId, tenantId } = body
  const dataNeedTobeMerge =
    await appHelper.validateSelfServicePartnerRequestAndUpdateBody(
      user,
      session
    )
  assign(body, dataNeedTobeMerge)

  const tenantInfo = await tenantHelper.getATenant({ _id: tenantId, partnerId })
  if (!size(tenantInfo)) throw new CustomError(404, 'Tenant not found')
  const { properties = [], userId } = tenantInfo
  const propertyInfo = properties.find(
    (property) => property.propertyId === propertyId
  )

  const query = {
    _id: tenantId,
    partnerId,
    properties: {
      $elemMatch: {
        propertyId,
        numberOfTenant: { $exists: true },
        status: { $in: ['interested', 'not_interested'] }
      }
    }
  }
  const updateData = {
    $unset: {
      'properties.$.numberOfTenant': 1,
      'properties.$.interestFormMeta': 1,
      'properties.$.fileIds': 1,
      'properties.$.wantsRentFrom': 1
    }
  }
  const updateTenant = await updateATenant(query, updateData, session)
  if (!updateTenant)
    throw new CustomError(400, 'Unable to delete interest form')
  await removeInterestFormFiles(propertyInfo, userId, session)
  await removeInterestFormInfoFromUser(userId, session)
  await updatePropertiesForHasProspects(partnerId, propertyInfo, session)
  await removeNotificationAfterDeleteInterestForm(body, session)
  return {
    _id: tenantId
  }
}

const removeInterestFormFiles = async (propertyInfo, tenantUserId, session) => {
  const { fileIds = [] } = propertyInfo
  if (size(fileIds)) {
    const removeFileQuery = {
      _id: { $in: fileIds }
    }
    const deletableFiles = await fileHelper.getFilesWithSelectedFields(
      removeFileQuery,
      ['type', 'partnerId', 'context', 'directive', 'name']
    )
    await appQueueService.createAppQueueForRemoveFilesFromS3(
      deletableFiles,
      session
    )
    await fileService.deleteFiles(removeFileQuery, session)
    await userService.updateUser(
      { _id: tenantUserId, 'interestFormMeta.fileIds': { $in: fileIds } },
      { $pull: { 'interestFormMeta.fileIds': { $in: fileIds } } },
      session
    )
  }
}

const removeInterestFormInfoFromUser = async (tenantUserId, session) => {
  const hasInterestForm = !!(await tenantHelper.getATenant(
    {
      userId: tenantUserId,
      properties: {
        $elemMatch: {
          status: {
            $in: ['invited', 'rejected', 'interested', 'not_interested']
          },
          numberOfTenant: { $gt: 0 }
        }
      }
    },
    session
  ))
  if (!hasInterestForm) {
    await userService.updateUser(
      { _id: tenantUserId, interestFormMeta: { $exists: true } },
      { $unset: { interestFormMeta: 1 } },
      session
    )
  }
}

const updatePropertiesForHasProspects = async (
  partnerId,
  propertyInfo,
  session
) => {
  const { contractId, propertyId } = propertyInfo
  if (contractId) {
    const contract = await contractHelper.getAContract({
      _id: contractId,
      partnerId,
      status: 'upcoming'
    })
    if (contract) {
      const tenants = await tenantHelper.getTenants(
        {
          partnerId,
          properties: {
            $elemMatch: {
              status: { $in: ['invited', 'interested'] },
              contractId
            }
          }
        },
        session
      )
      if (size(tenants)) {
        await listingService.updateAListing(
          {
            _id: propertyId,
            partnerId
          },
          { $set: { hasProspects: true } },
          session
        )
      }
    } else {
      await listingService.updateAListing(
        {
          _id: propertyId,
          partnerId
        },
        { $set: { hasProspects: false } },
        session
      )
    }
  }
}

export const createLogForChangeTenantName = async (params, session) => {
  const { updatedTenant, oldTenant, partnerId, userId } = params
  const previousName = oldTenant.name || ''
  const updatedName = updatedTenant.name || ''

  const logData = {
    partnerId,
    context: 'tenant',
    action: 'updated_tenant',
    tenantId: updatedTenant._id,
    isChangeLog: true,
    visibility: ['tenant'],
    changes: [
      {
        field: 'name',
        type: 'text',
        oldText: previousName,
        newText: updatedName
      }
    ],
    createdBy: userId
  }
  await logService.createLog(logData, session)
}

export const addInterestFormTenantInsertOperation = async (
  createdTenant,
  partnerSetting,
  session
) => {
  const { partnerId, userId } = createdTenant
  if (userId) {
    const params = {
      partnerId,
      userId,
      type: 'tenant'
    }
    await userService.addRelationBetweenUserAndPartner(params, session)
  }
  if (size(createdTenant.properties)) {
    // Todo :: Need to write test case for updatePropertiesForHasProspects
    const propertyInfo = createdTenant.properties[0]
    await updatePropertiesForHasProspects(partnerId, propertyInfo, session)
  }
  await generateCreditRatingAfterHookOperation(
    {
      tenantInfo: createdTenant,
      partnerSetting
    },
    session
  )
}

export const addInterestFormTenantUpdateOperation = async (params, session) => {
  const { updatedTenant, partnerId, partnerSetting } = params
  // Todo :: Need to write test case for updatePropertiesForHasProspects
  if (size(updatedTenant.properties)) {
    const property = updatedTenant.properties.pop()
    await updatePropertiesForHasProspects(partnerId, property, session)
  }
  await generateCreditRatingAfterHookOperation(
    {
      tenantInfo: updatedTenant,
      partnerSetting
    },
    session
  )
}

const generateCreditRatingAfterHookOperation = async (params, session) => {
  const { tenantInfo, partnerSettings } = params
  const ssn = await tenantHelper.hasNorwegianNationalIdentification(
    tenantInfo,
    session
  )
  if (tenantInfo.creditRatingTermsAcceptedOn && ssn) {
    // Todo :: Need to write test case for GenerateCreditRating
    const automaticCreditRatingEnabled =
      partnerSettings?.tenantSetting?.automaticCreditRating?.enabled || false
    if (automaticCreditRatingEnabled) {
      await generateCreditRating(
        {
          tenantInfo,
          ssn
        },
        session
      )
    }
  }
}

export const removeNotificationAfterDeleteInterestForm = async (
  params,
  session
) => {
  const { partnerId, propertyId, tenantId } = params
  const query = {
    partnerId,
    propertyId,
    tenantId,
    type: 'interestedFormSubmitted'
  }
  return await notificationService.removeNotifications(query, session)
}

export const removeATenant = async (query, session) => {
  if (!size(query))
    throw new CustomError(400, 'Query must be required while removing tenant')

  const response = await TenantCollection.findOneAndDelete(query, { session })
  console.log('=== Tenant Removed ===', response)
  return response
}
