import { size } from 'lodash'
import { CustomError } from '../common'
import { OrganizationCollection } from '../models'

export const createAnOrganization = async (data, session) => {
  const response = await OrganizationCollection.create([data], { session })
  if (!size(response)) {
    throw new CustomError(404, `Unable to create an Organization`)
  }
  return response
}

export const updateAnOrganization = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedOrganizationData = await OrganizationCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!size(updatedOrganizationData)) {
    throw new CustomError(404, `Unable to update Organization`)
  }
  console.log(
    `--- Organization has been updated for id: ${updatedOrganizationData._id} ---`
  )
  return updatedOrganizationData
}
