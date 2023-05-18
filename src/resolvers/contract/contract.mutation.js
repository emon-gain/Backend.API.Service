import { contractService } from '../services'

export default {
  async updateContractEvictionCase(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.updateContractEvictionCase(req)
  },

  async removeEvictionCase(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.removeEvictionCase(req)
  },

  async dailyNaturalTerminationNoticeSend(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { contractId } = args
    return contractService.dailyNaturalTerminationNoticeSendService(
      contractId,
      req.session
    )
  },

  async dailyAssignmentESigningReminder(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { option } = args
    return contractService.dailyAssignmentEsigningReminderHelper(
      option,
      req.session
    )
  },

  async sendLeaseESigningReminder(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { option } = args
    return contractService.sendLeaseESigningReminderHelper(option, req.session)
  },

  async movingInSigningReminder(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { option } = args
    return contractService.movingInSigningReminderHelper(option, req.session)
  },

  async movingOutSigningReminder(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { option } = args
    return contractService.movingOutSigningReminderHelper(option, req.session)
  },

  async dailySoonEnding(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { contractId } = args
    return contractService.soonEndingService(contractId, req.session)
  },

  async updateContractStatusFromLambda(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { contractId, status } = args
    req.body = { contractId, status }
    return contractService.updateContractStatus(req)
  },

  // TODO: Need to write test case later.
  async produceEvictionDocuments(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.produceEvictionDocuments(req)
  },

  async updateAContractForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.updateAContract(req)
  },

  async updateContractAndAddLog(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return contractService.updateAContractAndAddALog(req)
  },

  async prepareContractESigningDataAndCreateAppQueue(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.prepareContractESigningDataAndCreateAppQueue(
      req
    )
  },

  async downloadJournal(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const result = await contractService.downloadJournal(req)
    return result
  },

  async createAnAssignment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.createAnAssignment(req)
  },

  async updateAssignment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.updateAssignment(req)
  },

  async regenerateContractEsigning(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.regenerateContractEsigning(req)
  },

  async terminateAssignment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.terminateAssignment(req)
  },

  async resetCpiFutureRentAmount(parent, args, context) {
    const { req } = context
    const { queryData } = args
    //req.session.startTransaction()
    req.body = {
      queryData
    }
    return contractService.resetContractForCpi(req)
  },

  async resetSingleCpiFutureRentAmount(parent, args, context) {
    const { req } = context
    const { queryData } = args
    //req.session.startTransaction()
    req.body = {
      queryData
    }
    return contractService.resetSingleContractForCpi(req)
  },

  async updateContractAddon(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.updateContractAddon(req)
  },

  async addAddonInContract(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.addAddonInContract(req)
  },

  async removeContractAddon(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.removeContractAddon(req)
  },

  async updateContractPayoutPauseStatus(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.updateContractPayoutPauseStatus(req)
  },

  async updateLeaseTerms(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.updateLeaseTerms(req)
  },

  async createALease(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.createALease(req)
  },

  async terminateLease(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.terminateLease(req)
  },

  async checkCommissionChangesAndAddHistory(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.checkCommissionChangesAndAddHistory(req)
  },

  async cancelLease(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.cancelLease(req)
  },

  async cancelLeaseForWrongSSN(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.cancelLeaseForWrongSSN(req)
  },

  // TODO: Need to write test case
  async updatePauseUnPausePayouts(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.updatePauseUnpauseOfPayouts(req)
  },

  async sendCPINotification(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.sendCPINotification(req)
  },

  async updateCpiContractRentAmount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.updateCpiContractRentAmount(req)
  },

  // This api will trigger manually from admin app
  async initiateMonthlyCreateInvoiceJob(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.initiateMonthlyCreateInvoiceJob(req)
  },

  async unsetContractDataForLambda(parent, args, context) {
    console.log('Request processing started')
    const { req } = context
    const { inputData = {} } = args
    console.log('Checking inputData', inputData)
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.unsetContractData(req)
  },

  async downloadAgedDebtorsReport(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await contractService.downloadAgedDebtorsReport(req)
  },

  async cancelLeaseTermination(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await contractService.cancelLeaseTermination(req)
  }
}
