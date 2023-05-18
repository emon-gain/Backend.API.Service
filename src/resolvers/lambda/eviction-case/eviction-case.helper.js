import { size } from 'lodash'

import { CustomError } from '../../common'
import {
  appHelper,
  contractHelper,
  invoiceHelper,
  partnerSettingHelper
} from '../../helpers'

export const isEnabledTenantPaysAllDueDuringEvictionNotification = async (
  partnerId,
  session
) => {
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId,
    session
  )
  if (!size(partnerSetting)) return false

  const { tenantPaysAllDueDuringEviction = {} } = partnerSetting
  const { enabled = false } = tenantPaysAllDueDuringEviction || {}
  return enabled
}

export const checkRequiredFieldsAndDataForEvictionCase = async (
  body,
  session
) => {
  await appHelper.checkRequiredFields(
    ['contractId', 'invoiceId', 'partnerId'],
    body
  )

  const { contractId, invoiceId, partnerId } = body || {}
  appHelper.validateId({ contractId })
  appHelper.validateId({ invoiceId })
  appHelper.validateId({ partnerId })

  const isCreateEvictionPackage = await contractHelper.isCreateEvictionPackage(
    partnerId,
    session
  )
  if (!isCreateEvictionPackage)
    throw new CustomError(
      405,
      'Eviction process is not enabled for the partner'
    )
}

export const getContractWithInvoices = async (req = {}) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)

  const { query = {} } = body
  await appHelper.checkRequiredFields(['contractId', 'partnerId'], query)

  const { contractId, partnerId } = query || {}
  appHelper.validateId({ contractId })
  appHelper.validateId({ partnerId })

  const contract = await contractHelper.getAContract(
    { _id: contractId, partnerId },
    session
  )
  if (!size(contract)) throw new CustomError(404, 'Could not find contract')

  const invoices =
    (await invoiceHelper.getInvoices(
      {
        contractId,
        partnerId,
        invoiceType: 'invoice',
        $or: [{ evictionNoticeSent: true }, { evictionDueReminderSent: true }]
      },
      session
    )) || []

  return { contract, invoices }
}
