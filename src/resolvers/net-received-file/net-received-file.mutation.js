import { netReceivedFileService } from '../services'

export default {
  async addNetReceivedFiles(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdNetReceivedFile =
      await netReceivedFileService.createNetReceivedFiles(req)
    return createdNetReceivedFile
  },

  async updateNetReceivedFile(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    const updatedNetReceivedFile =
      await netReceivedFileService.updateNetReceivedFile(req)
    return updatedNetReceivedFile
  }
}
