import { partnerSettingHelper } from '../helpers'

export const isEnabledVippsRegninger = async (partnerId) => {
  const partnerSetting = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const { invoiceSettings } = partnerSetting || {}
  const { enabledVippsRegninger } = invoiceSettings || {}
  return !!enabledVippsRegninger
}

export const hasAccessForVipps = async (invoice = {}) => {
  const { partnerId, enabledNotification } = invoice
  let access = false
  if (partnerId && enabledNotification) {
    access = await isEnabledVippsRegninger(partnerId)
  }
  return access
}
