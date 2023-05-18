import { partnerSettingService } from '../services'

export default {
  async addNewBankAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const bankAccount = await partnerSettingService.addNewBankAccount(req)
    return bankAccount
  },

  async deleteBankAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedBankAccount = await partnerSettingService.deleteBankAccount(
      req
    )
    return updatedBankAccount
  },

  async updateBankAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedBankAccount = await partnerSettingService.updateBankAccount(
      req
    )
    return updatedBankAccount
  },

  async updatePartnerSetting(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const { bankId } = req.body
    const { partnerId } = req.user
    let updatedSetting
    if (partnerId && !bankId) {
      req.body.partnerId = partnerId
      updatedSetting = await partnerSettingService.updatePartnerSetting(req)
    } else if (partnerId && bankId) {
      req.body.partnerId = partnerId
      updatedSetting =
        await partnerSettingService.updateBankAccountForPartnerSetting(req)
    } else {
      updatedSetting = await partnerSettingService.updateAppSetting(req)
    }
    return updatedSetting
  },

  async updateNotificationsSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerSetting =
      await partnerSettingService.updateNotificationsSetting(req)
    return updatedPartnerSetting
  },

  async updateGeneralSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerSetting =
      await partnerSettingService.updateGeneralSetting(req)
    return updatedPartnerSetting
  },

  async updateDomainSetting(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerSetting =
      await partnerSettingService.updateDomainSetting(req)
    return updatedPartnerSetting
  },

  async updateRentInvoiceSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerSetting =
      await partnerSettingService.updateRentInvoiceSetting(req)
    return updatedPartnerSetting
  },

  async updateLandlordInvoiceSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerSetting =
      await partnerSettingService.updateLandlordInvoiceSetting(req)
    return updatedPartnerSetting
  },

  async updatePayoutSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPartnerSetting =
      await partnerSettingService.updatePayoutSetting(req)
    return updatedPartnerSetting
  },

  async updateCompanyInfoSetting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedSetting = await partnerSettingService.updateCompanyInfo(req)
    return updatedSetting
  }
}
