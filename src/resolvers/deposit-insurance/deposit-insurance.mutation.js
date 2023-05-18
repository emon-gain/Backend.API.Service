import { depositInsuranceService } from '../services'

export default {
  async updateDepositInsuranceCreationStatus(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedDepositInsurance =
      await depositInsuranceService.updateDepositInsuranceCreationStatus(req)
    return updatedDepositInsurance
  },
  async addDepositInsuranceDataForLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const addedDepositInsurance =
      await depositInsuranceService.addDepositInsuranceDataForLambda(req)
    return addedDepositInsurance
  },
  async createQForSendingDepositInsurancePaymentReminder(
    parent,
    args,
    context
  ) {
    const { req } = context
    const { queryData, optionData } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: JSON.parse(JSON.stringify(optionData))
    }
    //req.session.startTransaction()
    return depositInsuranceService.createQForSendingDepositInsurancePaymentReminder(
      req
    )
  }
}
