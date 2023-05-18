import { MethodList } from './upgrade-script.method'

export default {
  async executeUpgradeScript(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const { methodName } = args

    console.log(methodName)
    await MethodList[methodName](req)
    return {
      msg: 'Method Executed',
      code: 'SUCCESS'
    }
  }
}
