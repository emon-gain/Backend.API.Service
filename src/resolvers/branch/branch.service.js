import { size, isEmpty } from 'lodash'
import { CustomError } from '../common'
import { BranchCollection } from '../models'
import { appHelper, branchHelper } from '../helpers'

export const validateDataToCreateBranch = async (body) => {
  const { adminId, partnerId, branchSerialId } = body
  appHelper.validateId({ adminId })
  appHelper.validateId({ partnerId })
  if (partnerId && branchSerialId) {
    const isBranchSerialIdExists = await branchHelper.isBranchSerialIdExists({
      partnerId,
      branchSerialId
    })
    if (isBranchSerialIdExists) {
      throw new CustomError(409, 'Branch serial id already exists')
    }
  }
  return true
}

export const createBranch = async (req) => {
  const { body, user = {} } = req
  const { partnerId = '' } = user
  appHelper.checkRequiredFields(['userId'], user)
  if (partnerId) {
    appHelper.validateId({ partnerId })
    body.partnerId = partnerId
  }
  appHelper.checkRequiredFields(['name', 'adminId', 'partnerId'], body)
  await validateDataToCreateBranch(body)
  const branchData = branchHelper.prepareBranchDataForInsert(body, user)

  return await createABranch(branchData)
}

export const createABranch = async (params, session) =>
  await BranchCollection.create([params], { session })

export const validateDataForUpdateBranch = async (params, body) => {
  const { branchId, branchSerialId, partnerId } = body
  if (branchSerialId) {
    const isBranchSerialIdExists = await branchHelper.isBranchSerialIdExists({
      _id: { $ne: branchId },
      partnerId,
      branchSerialId
    })
    if (isBranchSerialIdExists) {
      throw new CustomError(400, 'Branch branchSerialId already exists')
    }
  }
  return true
}

export const prepareUpdateData = (body) => {
  const setData = {}
  const updateData = {}
  const addToSetData = {}
  const { name, adminId, branchSerialId } = body
  if (name) {
    setData.name = name
  }
  if (adminId) {
    setData.adminId = adminId
    addToSetData.agents = adminId
  }
  if (branchSerialId) {
    setData.branchSerialId = branchSerialId
  }
  if (size(setData)) {
    updateData.$set = setData
  }
  if (size(addToSetData)) {
    updateData.$addToSet = addToSetData
  }
  return updateData
}

export const prepareQuery = (params, body) => {
  const { _id } = body
  return {
    _id
  }
}

export const updateBranch = async (req) => {
  const { body, user } = req
  const { partnerId } = user
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['branchId', 'name', 'adminId'], body)
  body.partnerId = partnerId
  await validateDataForUpdateBranch(user, body)

  const query = { _id: body.branchId, partnerId }
  const updateData = prepareUpdateData(body)
  return await updateABranch(query, updateData)
}

export const updateABranch = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedData = await BranchCollection.findOneAndUpdate(query, data, {
    session,
    new: true,
    runValidators: true
  })
  if (isEmpty(updatedData)) {
    throw new CustomError(404, `Unable to update branch`)
  }
  return updatedData
}

export const updateBranches = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedBranches = await BranchCollection.updateMany(query, data, {
    session
  })
  if (isEmpty(updatedBranches)) {
    throw new CustomError(404, `Unable to update branches`)
  }
  return updatedBranches
}
