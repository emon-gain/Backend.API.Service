import { appHealthService } from '../services'
import {
  checkDailyCommissionTransactionsHelper,
  checkDailyCorrectionTransactionsHelper,
  checkDailyInvoiceTransactionsHelper,
  checkDailyPayoutTransactionsHelper,
  dailyPaymentTransactionHelper
} from './app-health.helper'

export default {
  async addAppHealths(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const appHealth = await appHealthService.createAppHealth(req)
    return appHealth
  },

  async reRunAppHealth(parent, args, context) {
    const { req } = context
    //req.session.startTransaction()
    const reRun = await appHealthService.reRunApphealth(req)
    if (reRun) {
      return {
        msg: 'App health restarted',
        code: 'success'
      }
    } else {
      throw new Error('App health restart failed')
    }
  },

  async updateAppHealth(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const updatedAppHealth = await appHealthService.updateAppHealth(req)
    return updatedAppHealth
  },

  async removeAppHealth(parent, args, context) {
    const { req } = context
    const { inputData } = args
    req.body = JSON.parse(JSON.stringify(inputData))
    //req.session.startTransaction()
    const removedAppHealth = await appHealthService.removeAppHealth(req)
    return removedAppHealth
  },

  async checkDailyInvoiceTransactions(parent, args, context) {
    const { req } = context
    const { partnerId, skip, appHealthId, appQueueId } = args
    req.body = {
      partnerId: JSON.parse(JSON.stringify(partnerId)),
      skip,
      appHealthId,
      appQueueId
    }
    //req.session.startTransaction()
    const response = await checkDailyInvoiceTransactionsHelper(req)
    return response
  },

  async checkDailyPaymentTransactions(parent, args, context) {
    const { req } = context
    const { partnerId } = args
    req.body = {
      partnerId: JSON.parse(JSON.stringify(partnerId))
    }
    const response = await dailyPaymentTransactionHelper(req)
    return response
  },

  async checkDailyCorrectionTransactions(parent, args, context) {
    const { req } = context
    const { partnerId } = args
    req.body = {
      partnerId: JSON.parse(JSON.stringify(partnerId))
    }
    const response = await checkDailyCorrectionTransactionsHelper(req)
    return response
  },

  async checkDailyCommissionTransactions(parent, args, context) {
    const { req } = context
    const { partnerId } = args
    req.body = {
      partnerId: JSON.parse(JSON.stringify(partnerId))
    }
    const response = await checkDailyCommissionTransactionsHelper(req)
    return response
  },

  async checkDailyPayoutTransactions(parent, args, context) {
    const { req } = context
    const { partnerId } = args
    req.body = {
      partnerId: JSON.parse(JSON.stringify(partnerId))
    }
    const response = await checkDailyPayoutTransactionsHelper(req)
    return response
  }
}
