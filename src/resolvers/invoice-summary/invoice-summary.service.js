import { size } from 'lodash'
import { CustomError } from '../common'
import { InvoiceSummaryCollection } from '../models'
import { appHelper, invoiceSummaryHelper } from '../helpers'

export const createInvoiceSummary = async (invoice, session) => {
  const summaryData = invoiceSummaryHelper.prepareInvoiceSummaryData(invoice)
  const invoiceSummary = await InvoiceSummaryCollection.create([summaryData], {
    session
  })
  if (size(invoiceSummary)) {
    console.log(
      `--- Invoice Summary Created for invoice id: ${summaryData.invoiceId} ---`
    )
  } else {
    throw new CustomError(
      500,
      `Could not create invoice summary for invoiceId: ${summaryData.invoiceId}`
    )
  }
  return invoiceSummary
}

export const updateInvoiceSummary = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedSummary = await InvoiceSummaryCollection.findOneAndUpdate(
    query,
    data,
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return updatedSummary
}

export const updateInvoiceInfoInInvoiceSummary = async (
  invoice = {},
  session
) => {
  const { _id, partnerId, invoiceTotal, feesMeta } = invoice
  if (_id && partnerId && invoiceTotal) {
    const updateData = { invoiceAmount: invoiceTotal }
    const feesAmount =
      invoiceSummaryHelper.getTotalFeeByInvoiceFeesMeta(feesMeta)
    updateData.feesAmount = await appHelper.convertTo2Decimal(feesAmount)
    const query = { invoiceId: _id, partnerId }
    const updatedSummary = await updateInvoiceSummary(
      query,
      updateData,
      session
    )
    return updatedSummary
  }
}

export const updateInvoiceSummaries = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedSummarys = await InvoiceSummaryCollection.updateMany(
    query,
    data,
    {
      session
    }
  )
  return updatedSummarys
}
