import { annualStatementService } from '../services'

export default {
  async downloadAnnualStatement(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const downloadStatement =
      await annualStatementService.downloadAnnualStatement(req)
    return downloadStatement
  },

  async createAnnualStatements(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const annualStatement =
      await annualStatementService.annualStatementCreateService(req)
    return annualStatement
  },

  async updateAnnualStatementFromLambda(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const annualStatement = await annualStatementService.updateAnnualStatement(
      req
    )
    return annualStatement
  }
}
