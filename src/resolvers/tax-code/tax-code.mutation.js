import { taxCodeService } from '../services'

export default {
  async addTaxCode(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdTaxCode = await taxCodeService.createTaxCode(req)
    return createdTaxCode
  },

  async updateTaxCode(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedTaxCode = await taxCodeService.updateTaxCode(req)
    return updatedTaxCode
  },

  async removeTaxCode(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedTaxCode = await taxCodeService.removeTaxCode(req)
    return removedTaxCode
  }
}
