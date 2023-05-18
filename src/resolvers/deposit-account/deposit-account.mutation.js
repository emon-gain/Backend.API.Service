import { depositAccountService } from '../services'

export default {
  async createAppQueuesToRetrieveTenantDAPDF(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await depositAccountService.createAppQueuesToRetrieveTenantDepositAccountPDF(
      req
    )
  },
  async addFileAndUpdateContractForDepositAccount(parent, args, context) {
    const { req } = context
    const { input = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(input))
    console.log('Request body', req.body)
    return depositAccountService.addFileAndUpdateContract(req)
  },

  async submitKycForm(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await depositAccountService.submitKycForm(req)
  },

  async createADepositAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await depositAccountService.createADepositAccountForLambda(req)
  },

  async updateADepositAccountForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(inputData))
    return await depositAccountService.updateADepositAccountForLambda(req)
  },
  async uploadIdfySignedFileToS3(parent, args, context) {
    const { req } = context
    const { input = {} } = args
    //req.session.startTransaction()
    req.body = JSON.parse(JSON.stringify(input))
    return await depositAccountService.uploadIdfySignedFileToS3Service(req)
  },
  async createDepositAccountForTest(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await depositAccountService.createTestNotification(req)
  },
  async createLogForTestDepositAccount(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await depositAccountService.createLogForDepositAccount(req)
  }
}
