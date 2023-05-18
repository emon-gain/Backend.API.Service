import { accountingService } from '../services'

export default {
  async updateAccounting(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedAccounting = await accountingService.updateAccounting(req)
    return updatedAccounting
  }
}
