import { fileHelper } from '../helpers'

export default {
  async getUploadPolicy(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const policy = await fileHelper.getUploadPolicy(req)
    return policy
  },

  async getFilesForPublicApp(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const files = await fileHelper.queryFilesForPublicApp(req)
    return files
  },

  async getImagesForPropertyDetailsUtility(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    return await fileHelper.queryImagesForPropertyDetailsUtility(req)
  },

  async getFileDownloadUrl(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await fileHelper.queryFileDownloadUrl(req)
  },

  async validateTokenAndGetFileDownloadUrl(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await fileHelper.validateTokenAndGetFileDownloadUrl(req)
  },

  async getImportErrorExcelFileUrl(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await fileHelper.queryImportErrorExcelFileUrl(req)
  },

  async getFilesForDetailsPage(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: -1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const files = await fileHelper.getFilesForDetailPage(req)
    return files
  },

  async getConversationFileDownloadUrl(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await fileHelper.queryConversationFileDownloadUrl(req)
  },

  async getAFileForLambda(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await fileHelper.getAFileForLambda(req)
  }
}
