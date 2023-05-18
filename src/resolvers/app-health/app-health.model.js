import mongoose from 'mongoose'
import { filter, size } from 'lodash'
import { AppHealthSchema } from '../models'
import { appHealthHelper } from '../helpers'

AppHealthSchema.virtual('invoice', {
  ref: 'invoices',
  localField: 'invoiceId',
  foreignField: '_id',
  justOne: true
})

AppHealthSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

AppHealthSchema.virtual('contract', {
  ref: 'contracts',
  localField: 'contractId',
  foreignField: '_id',
  justOne: true
})

AppHealthSchema.virtual('property', {
  ref: 'listings',
  localField: 'propertyId',
  foreignField: '_id',
  justOne: true
})

AppHealthSchema.methods = {
  getAppHealthTodayIssues(appHealthErrors) {
    const items = []

    if (size(appHealthErrors)) {
      const payoutTypeError = appHealthHelper.getAppHeathErrorsByContext(
        appHealthErrors,
        'payout'
      )
      if (size(payoutTypeError))
        items.push(
          appHealthHelper.getErrorObj('payout', size(payoutTypeError), true)
        )

      const invoiceTypeError = appHealthHelper.getAppHeathErrorsByContext(
        appHealthErrors,
        'invoice'
      )
      if (size(invoiceTypeError))
        items.push(
          appHealthHelper.getErrorObj('invoice', size(invoiceTypeError), true)
        )

      const sqsTypeError = filter(appHealthErrors, (appHealth) => {
        if (appHealth.type === 'sqs') return appHealth
      })
      if (size(sqsTypeError))
        items.push(appHealthHelper.getErrorObj('sqs', size(sqsTypeError), true))
    }

    return items
  },

  async getAppHealthNewIssues(appHealthErrors) {
    const yesterdayIssues = await appHealthHelper.getIssuesOfYesterday(
      appHealthErrors
    )
    const { newErrors = [] } = yesterdayIssues || {}
    return newErrors
  },

  async isAllIssuesSame(appHealthErrors) {
    const yesterdayIssues = await appHealthHelper.getIssuesOfYesterday(
      appHealthErrors
    )
    const { totalSameErrors = 0 } = yesterdayIssues || {}

    return totalSameErrors === size(appHealthErrors)
  },

  async isAppHealthAllIssuesNew(appHealthErrors) {
    const yesterdayIssues = await appHealthHelper.getIssuesOfYesterday(
      appHealthErrors
    )
    const { totalSameErrors = 0 } = yesterdayIssues || {}

    return !!totalSameErrors
  }
}

export const AppHealthCollection = mongoose.model(
  'app-health',
  AppHealthSchema,
  'app-health'
)
