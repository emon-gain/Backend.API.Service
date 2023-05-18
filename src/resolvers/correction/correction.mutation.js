import { correctionService } from '../services'

export default {
  async addCorrection(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdCorrection = await correctionService.createCorrection(req)
    return createdCorrection
  },

  async updateCorrection(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedCorrection = await correctionService.updateCorrection(req)
    return updatedCorrection
  },

  async removeCorrectionFiles(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedCorrection = await correctionService.removeCorrectionFiles(req)
    return removedCorrection
  },

  async cancelCorrection(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedCorrection = await correctionService.cancelCorrection(req)
    return updatedCorrection
  },

  async downloadCorrection(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const correctionQueue = await correctionService.downloadCorrection(req)
    return correctionQueue
  }
}
