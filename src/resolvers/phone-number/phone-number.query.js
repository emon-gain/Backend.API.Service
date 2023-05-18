import { phoneNumberHelper } from '../helpers'

export default {
  async phoneNumbers(parent, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 50, skip = 0, sort = { createdAt: 1 } } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit, skip, sort }
    }
    const phoneNumbers = await phoneNumberHelper.queryPhoneNumbers(req)
    return phoneNumbers
  },
  async phoneNumbersForDropdown(parents, args, context) {
    const { req } = context
    const { queryData = {}, optionData = {} } = args
    const { limit = 30 } = optionData
    req.body = {
      query: JSON.parse(JSON.stringify(queryData)),
      options: { limit }
    }
    const phoneNumbers = await phoneNumberHelper.getPhoneNumbersForDropdown(req)
    return phoneNumbers
  }
}
