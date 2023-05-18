import { cpiDataSetService } from '../services'

export default {
  async addCpiDataSet(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const cpiDataSet = await cpiDataSetService.createCpiDataSet(req)
    return cpiDataSet
  }
}
