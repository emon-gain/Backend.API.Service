import { MethodList } from './upgrade-script.method'

export default {
  async getUpgradeScriptList() {
    const scriptList = Object.keys(MethodList)
    console.log('Scriptlist========>', scriptList)
    return scriptList
  }
}
