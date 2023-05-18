import { eSigningHelper } from '../../helpers'

export default {
  async handleESigning(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const eSigningURL = await eSigningHelper.handleESigning(req)
    return eSigningURL
  },

  async handleMovingInOutESigning(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const eSigningURL = await eSigningHelper.handleMovingInOutESigning(req)
    return eSigningURL
  },

  async verifySignerSSN(parent, args, context) {
    const { req } = context
    const { queryData = {} } = args
    req.body = {
      query: JSON.parse(JSON.stringify(queryData))
    }
    const reponse = await eSigningHelper.verifySignerSSN(req)
    return reponse
  }
}
