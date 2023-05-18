import { propertyRoomItemService } from '../services'

export default {
  async addPropertyRoomItem(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdPropertyRoomItem =
      await propertyRoomItemService.createRoomItem(req)
    return createdPropertyRoomItem
  },

  async updatePropertyRoomItem(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPropertyRoomItem =
      await propertyRoomItemService.updateRoomItem(req)
    return updatedPropertyRoomItem
  },

  async removePropertyRoomItem(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedPropertyRoomItem =
      await propertyRoomItemService.removeRoomItem(req)
    return removedPropertyRoomItem
  }
}
