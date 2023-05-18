import { appRoleService } from '../services'

export default {
  async updateAppRole(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appRole = await appRoleService.updateAppRole(req)
    return appRole
  },

  async addManager(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appManager = await appRoleService.addAppManager(req)
    return appManager
  },

  async addRoleToUserForPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await appRoleService.addAppManagerForPartnerSite(req)
  },

  async addAgentUserForPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await appRoleService.addAgentUserRoleForPartnerApp(req)
  },

  async removeManager(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appManager = await appRoleService.removeAppManager(req)
    return appManager
  },

  async removeUserFromRoleForPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await appRoleService.removeUserForPartnerApp(req)
  },

  async updatePartnerUserEmployeeIdForPartnerApp(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    return await appRoleService.updatePartnerUserEmployeeIdForPartnerApp(req)
  },

  async removeAgentUserFromBranch(parent, args, context) {
    const { req } = context
    const { inputData = {} } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    return await appRoleService.removeAgentUserFromBranch(req)
  }
}
