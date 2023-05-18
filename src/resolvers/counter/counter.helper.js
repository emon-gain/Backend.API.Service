import { CounterCollection } from '../models'
import { appHelper } from '../helpers'

export const getCounterDataById = async (id, session) => {
  const counterData = await CounterCollection.findById(id).session(session)
  return counterData
}

export const getACounter = async (query, session) => {
  const counterData = await CounterCollection.findOne(query).session(session)
  return counterData
}

export const getCounters = async (query, session) => {
  const counterData = await CounterCollection.find(query).session(session)
  return counterData
}

const prepareQueryAndResultObjForStartNumber = (query = {}) => {
  const { accountId, modules, partnerId } = query
  const preparedIds = []
  const resultObj = {}
  for (const module of modules) {
    if (['account', 'tenant', 'property'].includes(module)) {
      preparedIds.push(module + '-' + partnerId)
      resultObj[module + '-' + partnerId] = {
        key: module,
        value: 1
      }
    } else if (module === 'invoice') {
      preparedIds.push(partnerId)
      resultObj[partnerId] = {
        key: 'invoice',
        value: 1
      }
    } else if (module === 'finalSettlementInvoice') {
      preparedIds.push('final-settlement-invoice-' + partnerId)
      resultObj['final-settlement-invoice-' + partnerId] = {
        key: 'finalSettlementInvoice',
        value: 1
      }
    } else if (module === 'invoiceStartNumber') {
      appHelper.checkRequiredFields(['accountId'], query)
      appHelper.validateId({ accountId })
      preparedIds.push('invoice-start-number-' + accountId)
      resultObj['invoice-start-number-' + accountId] = {
        key: 'invoiceStartNumber',
        value: 1
      }
    }
  }
  const preparedQuery = {
    _id: { $in: preparedIds }
  }
  return { preparedQuery, resultObj }
}

export const QueryStartNumber = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  appHelper.checkRequiredFields(['modules'], body)
  const { partnerId } = user
  body.partnerId = partnerId
  const { preparedQuery, resultObj } =
    prepareQueryAndResultObjForStartNumber(body)
  const counters = await getCounters(preparedQuery)
  const result = {}
  for (const counter of counters) {
    resultObj[counter._id].value = 1 + (counter.next_val || 0)
  }
  Object.values(resultObj).forEach((item) => {
    result[item.key] = item.value
  })
  return result
}
