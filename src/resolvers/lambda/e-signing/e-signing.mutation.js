import { eSigningService } from '../../services'

export default {
  async addESigningDocument(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await eSigningService.addESigningDocumentInfo(req)
    return response
  },

  async updateESigningDocument(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await eSigningService.updateESigningDocumentInfo(req)
    return response
  },

  async updateLeaseStatusAndCreateInvoice(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response = await eSigningService.updateLeaseStatusAndCreateInvoice(
      req
    )
    return response
  },

  async addDIAttachmentIdAndIDFYCreationProcess(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response =
      await eSigningService.addDIAttachmentIdAndIDFYCreationProcess(req)
    return response
  },

  async createQueuesToUploadDISignedFileFromIDFY(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const response =
      await eSigningService.createQueuesToUploadDISignedFileFromIDFY(req)
    return response
  }
}
