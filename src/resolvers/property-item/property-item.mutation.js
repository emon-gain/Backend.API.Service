import { propertyItemService } from '../services'

export default {
  async updatePropertyItem(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPropertyItem = await propertyItemService.updatePropertyItem(
      req
    )
    return updatedPropertyItem
  },

  async updatePropertyItemAndProcessESigning(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPropertyItem =
      await propertyItemService.updatePropertyItemAndProcessESigning(req)
    return updatedPropertyItem
  },

  async updatePropertyItemFromPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPropertyItem =
      await propertyItemService.updatePropertyItemFromPartnerApp(req)
    return updatedPropertyItem
  },

  async addPropertyItem(parent, args, context) {
    console.log('Started addPropertyItem')
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return propertyItemService.addPropertyItem(req)
  },

  // TODO:: Later need to write test cases.
  async removePropertyItem(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyItemService.removePropertyItem(req)
  },

  async updateAPropertyItemForLambda(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPropertyItem =
      await propertyItemService.updateAPropertyItemForLambda(req)
    return updatedPropertyItem
  },

  // TODO:: Later need to write test cases.
  async goToMovingProtocol(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyItemService.goToMovingProtocol(req)
  },

  // TODO:: Later need to write test cases.
  async resetMovingProtocol(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyItemService.resetMovingProtocol(req)
  },

  async initiateMovingInOutProtocol(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyItemService.initiateMovingInOutProtocol(req)
  },

  // TODO:: Later need to write test cases.
  async cancelMovingInOutProtocol(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyItemService.cancelMovingInOutProtocol(req)
  }
}
