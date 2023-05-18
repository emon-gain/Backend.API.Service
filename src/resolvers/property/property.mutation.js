import { propertyService } from '../services'

export default {
  async addProperty(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const property = await propertyService.addProperty(req)
    return property
  },

  async updateProperty(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const property = await propertyService.updateProperty(req)
    return property
  },

  async shareAtFinn(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const property = await propertyService.shareAtFinn(req)
    return property
  },

  async removeFromFinn(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyService.removeFromFinn(req)
  },

  async cancelListingFinn(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyService.cancelListingFinn(req)
  },

  async updatePropertyStatus(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyService.updatePropertyStatus(req)
  },

  async updatePropertyOwner(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyService.updatePropertyOwner(req)
  },

  async updatePropertyAbout(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await propertyService.updatePropertyAbout(req)
  },

  async downloadProperty(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await propertyService.downloadProperty(req)
  },

  async updatePropertyJanitor(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await propertyService.updatePropertyJanitor(req)
  },

  async downloadRentRollReport(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await propertyService.downloadRentRollReport(req)
  }
}
