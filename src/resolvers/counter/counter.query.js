import { counterHelper } from '../helpers'

export default {
  async getStartNumber(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = JSON.parse(JSON.stringify(queryData))
    return await counterHelper.QueryStartNumber(req)
  }
}
