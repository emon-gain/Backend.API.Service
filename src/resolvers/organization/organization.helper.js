import { OrganizationCollection } from '../models'

export const getAnOrganization = async (query, session) => {
  const organizationInfo = await OrganizationCollection.findOne(query).session(
    session
  )
  return organizationInfo
}

export const getOrganizations = async (query, session) => {
  const organizations = await OrganizationCollection.find(query).session(
    session
  )
  return organizations
}
