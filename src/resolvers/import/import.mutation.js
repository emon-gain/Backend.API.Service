import { importService } from '../services'

export default {
  async addAnExcelImport(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()

    return await importService.addAnExcelImport(req)
  },

  async addImportAndCollectionData(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()

    return await importService.addImportAndCollectionData(req)
  },

  async updateImport(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await importService.updateImport(req)
  }
}
