import { finalSettlementService } from '../services'
import { finalSettlementHelper } from '../helpers'
export default {
  async generateFinalSettlement(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await finalSettlementService.generateFinalSettlement(req)
  },

  // Not used in generate final settlement
  async initializeFinalSettlementProcess(parent, args, context) {
    const { req } = context
    const { contractId, partnerId } = args
    //req.session.startTransaction()
    await finalSettlementHelper.initializeFinalSettlementProcessService(
      contractId,
      partnerId,
      req.session
    )
  },

  async createRentInvoiceForCorrections(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await finalSettlementService.createRentInvoiceForCorrections(req)
  },

  async adjustLandlordPayoutOrSendLandlordInvoiceForFinalSettlement(
    parent,
    args,
    context
  ) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await finalSettlementService.adjustLandlordPayoutOrSendLandlordInvoice(
      req
    )
  },

  async generateRefundPaymentToTenantOnLeaseTerminationForFinalSettlement(
    parent,
    args,
    context
  ) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await finalSettlementService.generateRefundPaymentToTenantOnLeaseTermination(
      req
    )
  },

  // Not used in generate final settlement
  async findUnbalancedLandlordInvoicesAndMakePayableForFinalSettlement(
    parent,
    args,
    context
  ) {
    const { req } = context
    const { contractId } = args
    //req.session.startTransaction()
    return finalSettlementService.findUnbalancedLandlordInvoicesAndMakePayable(
      contractId
    )
  },

  async overPaidInvoicesAmountForwardedToNonePaidInvoices(
    parent,
    args,
    context
  ) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await finalSettlementService.overPaidInvoicesAmountForwardedToNonePaidInvoices(
      req
    )
  },

  async cancelFinalSettlement(parent, args, context) {
    const { req } = context
    const { contractId, partnerId } = args
    //req.session.startTransaction()
    return finalSettlementService.cancelFinalSettlementService(
      {
        contractId,
        partnerId
      },
      req
    )
  },

  async checkDailyFinalSettlement(parent, args, context) {
    const { req } = context
    const { contractId } = args
    req.body = { contractId }
    //req.session.startTransaction()
    return finalSettlementService.checkDailyFinalSettlementsAndUpdateStatusToCompleted(
      req
    )
  }
}
