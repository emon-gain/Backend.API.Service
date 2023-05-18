import { commentService } from '../services'

export default {
  async addComment(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const comment = await commentService.addComment(req)
    return comment
  }
}
