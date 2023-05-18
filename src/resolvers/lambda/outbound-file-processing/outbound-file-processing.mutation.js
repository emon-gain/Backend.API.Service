import { outBoundFileProcessingService } from '../../services'

export default {
  async processSftpReceivedFileInS3(parent, args, context) {
    console.log(' initiating processSftpReceivedFileInS3 ')
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await outBoundFileProcessingService.processSftpReceivedFileInS3(req)
  }
}
