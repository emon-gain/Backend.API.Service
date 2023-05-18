import { phoneNumberService } from '../services'

export default {
  async addPhoneNumber(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdPhoneNumber = await phoneNumberService.createPhoneNumber(req)
    return createdPhoneNumber
  },

  async updatePhoneNumber(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPhoneNumber = await phoneNumberService.updatePhoneNumber(req)
    return updatedPhoneNumber
  },

  async removePhoneNumber(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedPhoneNumber = await phoneNumberService.removePhoneNumber(req)
    return removedPhoneNumber
  },
  // for lambda service
  async updateRemainingBalance(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedRemainingBalance =
      await phoneNumberService.updateRemainingBalance(req)
    return updatedRemainingBalance
  }
}
