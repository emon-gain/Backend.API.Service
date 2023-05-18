import { netReceivedFileHelper } from '../helpers'

export default {
  async netReceivedFiles(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const netReceivedFiles = await netReceivedFileHelper.queryNetReceivedFiles(
      req
    )
    return netReceivedFiles
  },
  async getNetReceivedFile(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = { query: JSON.parse(JSON.stringify(queryData)) }
    const netReceivedFile = await netReceivedFileHelper.queryNetReceivedFile(
      req
    )
    return netReceivedFile
  }
}
