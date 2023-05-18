import { creditRatingService } from '../../services'

export default {
  async setTenantCreditInfo(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedTenant = await creditRatingService.updateTenantCreditInfo(req)
    return updatedTenant
  },
  async removeExpiredCreditRatings(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const numberOfUpdatedTenants =
      await creditRatingService.removeExpiredCreditRatings(req)
    return numberOfUpdatedTenants
  }
}
