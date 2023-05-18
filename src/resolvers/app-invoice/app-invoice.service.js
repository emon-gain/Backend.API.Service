import { size } from 'lodash'

import { CustomError } from '../common'
import { depositInsuranceHelper, appInvoiceHelper } from '../helpers'
import { AppInvoiceCollection } from '../models'
import { appQueueService, depositInsuranceService } from '../services'

export const updateAppInvoice = async (query, data, session) => {
  const updatedAppInvoice = await AppInvoiceCollection.findOneAndUpdate(
    query,
    data,
    {
      runValidators: true,
      new: true,
      session
    }
  )
  return updatedAppInvoice
}

const updateDepositInsuranceStatusAndSendRequest = async (
  appInvoice,
  status,
  session
) => {
  if (!status) {
    console.log(
      `=== Status not found while updating DI And sendingRequest. appInvoiceId: ${appInvoice._id} ===`
    )
    return false
  }

  const { _id: appInvoiceId, depositInsuranceId } = appInvoice
  const depositInsurance = depositInsuranceId
    ? await depositInsuranceHelper.getADepositInsurance({
        _id: depositInsuranceId
      })
    : {}

  if (!(size(depositInsurance) || depositInsuranceId)) {
    depositInsuranceId
      ? console.log(
          `=== Deposit insurance doesn't exists. depositInsuranceId: ${depositInsuranceId} ===`
        )
      : console.log(
          `=== Deposit insuranceId doesn't exists in appInvoice. appInvoiceId: ${appInvoiceId} ===`
        )
    return false
  }

  const { contractId, status: depositStatus } = depositInsurance || {}

  if (depositStatus !== 'sent' && depositStatus !== 'registered') {
    const isDIUpdated = await depositInsuranceService.updateADepositInsurance(
      { _id: depositInsuranceId },
      { $set: { status } },
      session
    )

    if (
      size(isDIUpdated) &&
      (status === 'paid' || status === 'overpaid') &&
      contractId &&
      depositStatus
    ) {
      await appQueueService.createAppQueueToSendDepositInsuranceCreatingRequest(
        contractId,
        session
      )
    }
  }
}

export const initAfterUpdateProcessForAppInvoice = async (
  preDoc,
  newDoc,
  session
) => {
  if (!(size(preDoc) && size(newDoc)))
    throw new CustomError(
      400,
      'Required data missing to start after update process of an appInvoice'
    )
  const { totalPaid: preTotalPaid } = preDoc
  const { totalPaid: newTotalPaid, invoiceTotal } = newDoc
  let updatedAppInvoice = {}
  console.log('preTotalPaid', preTotalPaid)
  console.log('newTotalPaid', newTotalPaid)
  if (preTotalPaid !== newTotalPaid) {
    const updatingData = {}
    console.log(
      '=== Updating invoice status (paid, isPartiallyPaid, isOverPaid) based on invoiceTotal. ==='
    )

    if (newTotalPaid === 0 && newTotalPaid < invoiceTotal) {
      updatingData.status = 'created'
      updatingData.isPartiallyPaid = false
      updatingData.isOverPaid = false
    } else if (newTotalPaid !== 0 && newTotalPaid < invoiceTotal) {
      updatingData.status = 'created'
      updatingData.isPartiallyPaid = true
      updatingData.isOverPaid = false
    } else if (newTotalPaid === invoiceTotal) {
      updatingData.status = 'paid'
      updatingData.isPartiallyPaid = false
      updatingData.isOverPaid = false
    } else if (newTotalPaid > invoiceTotal) {
      updatingData.status = 'paid'
      updatingData.isOverPaid = true
      updatingData.isPartiallyPaid = false
    }

    updatedAppInvoice = size(updatingData)
      ? await updateAppInvoice(
          { _id: newDoc._id },
          { $set: updatingData },
          session
        )
      : {}

    if (!size(updatedAppInvoice))
      throw new CustomError(404, 'Unable to update app invoice')
  }

  if (
    preDoc.status !== updatedAppInvoice.status ||
    (!preDoc.isPartiallyPaid && updatedAppInvoice.isPartiallyPaid) ||
    (!preDoc.isOverPaid && updatedAppInvoice.isOverPaid)
  ) {
    let status

    if (updatedAppInvoice.status === 'paid') status = 'paid'
    if (updatedAppInvoice.status === 'paid' && updatedAppInvoice.isOverPaid)
      status = 'overpaid'
    if (
      updatedAppInvoice.status !== 'paid' &&
      updatedAppInvoice.isPartiallyPaid
    )
      status = 'partially_paid'

    if (status) {
      await updateDepositInsuranceStatusAndSendRequest(
        updatedAppInvoice,
        status,
        session
      )
    }
  }
}

const createAppInvoice = async (data, session) => {
  if (!size(data))
    throw new CustomError(400, 'No app invoice data found to insert')
  const addedAppInvoice = await AppInvoiceCollection.create([data], { session })
  return addedAppInvoice
}

export const createAppInvoiceForDepositInsurance = async (
  depositInsurance,
  contract,
  session
) => {
  const appInvoiceData = await appInvoiceHelper.prepareAppInvoiceData(
    depositInsurance,
    contract
  )
  const [addedAppInvoice] = await createAppInvoice(appInvoiceData, session)
  if (!size(addedAppInvoice))
    throw new CustomError(403, 'Could not add app invoice')
  await appQueueService.createAppQueueForAppInvoicePdf(addedAppInvoice, session)
  return addedAppInvoice
}
