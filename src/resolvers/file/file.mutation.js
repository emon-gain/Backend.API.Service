import { fileService } from '../services'

export default {
  async addFile(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const createdFile = await fileService.addFile(req)
    return createdFile
  },

  async removeFile(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const deletedFile = await fileService.deleteFile(req)
    return deletedFile
  },

  async updateFile(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const file = await fileService.updateFileForLambda(req)
    return file
  },

  // Lambda Notification Attachments
  async addFileForNotificationAttachments(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const fileIdsWithAttachmentFileIds =
      await fileService.addFileForNotificationAttachments(req)
    return fileIdsWithAttachmentFileIds
  },

  async createFileAndAppQueueForPdfGeneration(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await fileService.createFileAndAppQueueForPdfGeneration(
      req
    )
    return response
  },

  async createProducedLogAndSendEvictionDueReminder(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response =
      await fileService.createProducedLogAndSendEvictionDueReminder(req)
    return response
  },

  async addFileFromUI(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await fileService.addFileFromUI(req)
  },

  async uploadFiles(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await fileService.uploadFiles(req)
  },

  // TODO:: Later need to write test cases.
  async removeFileFromUI(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await fileService.removeFileFromUI(req)
  },

  // TODO:: Later need to write test cases.
  async updateFileFromPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await fileService.updateFileFromPartnerApp(req)
  }
}
