import { propertyRoomService } from '../services'

export default {
  async addPropertyRoom(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdPropertyRoom = await propertyRoomService.createRooms(req)
    return createdPropertyRoom
  },

  async updatePropertyRoom(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedPropertyRoom = await propertyRoomService.updateRooms(req)
    return updatedPropertyRoom
  },

  async removePropertyRoom(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedPropertyRoom = await propertyRoomService.removeRoom(req)
    return removedPropertyRoom
  },

  async createPropertyIssue(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdPropertyIssue = await propertyRoomService.createPropertyIssue(
      req
    )
    return createdPropertyIssue
  }
}
