import { each, size } from 'lodash'
import moment from 'moment-timezone'
import nid from 'nid'

import { partnerHelper, userHelper } from '../helpers'
import { userService } from '../services'
import { CustomError } from '../common'

export const getPartnerURLForV1 = async (partnerId) => {
  const partnerUrl = `${process.env.PARTNER_SITE_URL}` || ''
  if (!partnerUrl) throw new CustomError(404, 'Unable to find partnerSite')

  if (partnerId) {
    const { subDomain = '' } =
      (await partnerHelper.getAPartner({ _id: partnerId })) || {}
    if (subDomain) {
      const regex = /SUBDOMAIN/gi
      return partnerUrl.replace(regex, subDomain)
    }
    return ''
  }
  return ''
}

export const createVerificationToken = async (
  userId,
  partnerId,
  emailAddress
) => {
  if (!userId)
    throw new CustomError(
      404,
      'Missing required data to create verification token'
    )
  const user = await userHelper.getUserById(userId)
  if (size(user)) {
    const updateData = {}
    const partnersData = []
    const emailsData = []
    let token = nid(24)
    const expires = new Date(moment().add(7, 'days'))

    if (partnerId) {
      // Add token to specific partner
      each(user.partners, (partner) => {
        if (partner.partnerId === partnerId) {
          if (!partner.token) partner.token = token
          else token = partner.token // Don't replace token if already exist, just extend the expires date

          partner.expires = expires
          partner.status = 'invited'
        }

        partnersData.push(partner)
      })

      updateData.partners = partnersData // Set partners users token and others info
    } else if (size(user.emails) && emailAddress) {
      //add token to specific email address
      each(user.emails, (email) => {
        if (email.address === emailAddress) {
          if (!email.token) email.token = token
          else token = email.token // Don't replace token if already exist, just extend the expires date

          email.expires = expires
        }

        emailsData.push(email)
      })

      updateData.emails = emailsData
    }

    if (size(updateData))
      await userService.updateAnUser({ _id: userId }, { $set: updateData }) // Updated partners / emails

    return token
  } else throw new CustomError(404, "User doesn't exists")
}
