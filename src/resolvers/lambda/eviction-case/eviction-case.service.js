import { assign, compact, find, head, includes, pick, size } from 'lodash'

import { CustomError } from '../../common'
import {
  appHelper,
  contractHelper,
  evictionCaseHelper,
  invoiceHelper,
  logHelper
} from '../../helpers'
import { appQueueService, contractService, logService } from '../../services'

const createAppQueueForTenantPaysAllOverdueDuringEvictionNotification = async (
  params = {},
  session
) => {
  const { contractId, partnerId } = params || {}
  const isEnabled =
    await evictionCaseHelper.isEnabledTenantPaysAllDueDuringEvictionNotification(
      partnerId,
      session
    )
  if (!isEnabled) return false

  const queueData = {
    action: 'send_notification',
    destination: 'notifier',
    event: 'send_notification_tenant_pays_all_due_during_eviction',
    params: {
      partnerId,
      collectionId: contractId,
      collectionNameStr: 'contracts'
    },
    priority: 'regular'
  }
  await appQueueService.insertInQueue(queueData, session)
}

export const createEvictionCaseRemoveLog = async (params = {}, session) => {
  const { contract, contractId, invoiceId } = params || {}
  const logCreationData = pick(contract, [
    'accountId',
    'partnerId',
    'propertyId',
    'leaseSerial'
  ])
  if (contract?.rentalMeta?.tenantId)
    logCreationData.tenantId = contract.rentalMeta.tenantId
  assign(logCreationData, { invoiceId, contractId })
  const logData = await logHelper.prepareLogDataForRemovedEvictionCase(
    logCreationData,
    session
  )
  return await logService.createLog(logData, session)
}

const createEvictionCaseForInvoice = async (
  contract = {},
  invoice = {},
  session
) => {
  const { _id: contractId, leaseSerial, rentalMeta = {} } = contract
  const { contractStartDate, contractEndDate, firstInvoiceDueDate, dueDate } =
    rentalMeta || {}
  const { _id: invoiceId, agentId, invoiceTotal, tenantId, tenants } = invoice
  const evictionCaseData = {
    agentId,
    amount: invoiceTotal,
    contractStartDate,
    contractEndDate,
    evictionInvoiceIds: [invoiceId],
    firstInvoiceDueDate,
    dueDate,
    invoiceId,
    leaseSerial,
    status: 'new',
    tenantId,
    tenants
  }

  const query = { _id: contractId }
  const updateData = { $push: { evictionCases: evictionCaseData } }
  const updatedContract = await contractService.updateContract(
    query,
    updateData,
    session
  )
  return updatedContract
}

const updateEvictionCaseForInvoice = async (
  invoice = {},
  evictionCase = {},
  session
) => {
  const { _id: invoiceId, contractId, evictionDueReminderSent } = invoice

  if (invoiceId && contractId && evictionDueReminderSent) {
    const evictionInvoiceIds = compact([
      ...(evictionCase.evictionInvoiceIds || []),
      evictionCase?.invoiceId
    ])
    const invoices = size(evictionInvoiceIds)
      ? await invoiceHelper.getInvoices(
          {
            _id: {
              $in: evictionInvoiceIds
            },
            evictionDueReminderSent: true,
            status: 'overdue'
          },
          session
        )
      : []
    invoices.push(invoice)

    let newEvictionInvoiceAmount = 0
    const newEvictionInvoiceIds = []
    for (const invoice of invoices) {
      const {
        _id: invoiceId,
        creditedAmount = 0,
        invoiceTotal = 0,
        lostMeta = {},
        totalPaid = 0
      } = invoice || {}
      const { amount: lostAmount = 0 } = lostMeta || {}
      const totalDue = invoiceTotal - totalPaid + creditedAmount - lostAmount

      if (totalDue > 0) {
        newEvictionInvoiceIds.push(invoiceId)
        newEvictionInvoiceAmount += invoiceTotal
        console.log(
          '====> Checking invoice total for eviction case:',
          { invoiceTotal, newEvictionInvoiceAmount },
          '<===='
        )
      }
    }
    const query = {
      _id: contractId,
      evictionCases: {
        $elemMatch: {
          invoiceId: evictionCase?.invoiceId,
          evictionInvoiceIds: { $ne: invoiceId },
          status: { $nin: ['canceled', 'completed'] }
        }
      }
    }
    const updateData = {
      $set: {
        'evictionCases.$.evictionInvoiceIds': newEvictionInvoiceIds,
        'evictionCases.$.amount': newEvictionInvoiceAmount
      }
    }
    console.log(
      '====> Contract updating data for eviction case:',
      updateData,
      '<===='
    )
    const updatedContract = await contractService.updateContract(
      query,
      updateData,
      session
    )
    return updatedContract
  }
}

const updateOrRemoveEvictionCaseForInvoice = async (
  contract = {},
  invoice = {},
  session
) => {
  const { _id: contractId, evictionCases = [], partnerId } = contract || {}
  const { _id: invoiceId } = invoice || {}
  const { evictionInvoiceIds = [], status: evictionStatus } =
    find(evictionCases, (eviction) => {
      const { evictionInvoiceIds = [], invoiceId: evictionInvoiceId } =
        eviction || {}

      return (
        evictionInvoiceId === invoiceId ||
        includes(evictionInvoiceIds, invoiceId)
      )
    }) || {}
  const invoices = size(evictionInvoiceIds)
    ? await invoiceHelper.getInvoices(
        {
          _id: { $in: [...evictionInvoiceIds, invoiceId] },
          evictionDueReminderSent: true,
          status: 'overdue'
        },
        session
      )
    : []

  let newEvictionInvoiceAmount = 0
  const newEvictionInvoiceIds = []
  for (const invoice of invoices) {
    const {
      _id: invoiceId,
      creditedAmount = 0,
      invoiceTotal = 0,
      lostMeta = {},
      totalPaid = 0
    } = invoice || {}
    const { amount: lostAmount = 0 } = lostMeta || {}
    const totalDue = invoiceTotal - totalPaid + creditedAmount - lostAmount

    if (totalDue > 0) {
      newEvictionInvoiceIds.push(invoiceId)
      newEvictionInvoiceAmount += invoiceTotal
      console.log(
        '====> Checking invoice total for eviction case:',
        { invoiceTotal, newEvictionInvoiceAmount },
        '<===='
      )
    }
  }
  const hasEvictionCases = !!(
    size(newEvictionInvoiceIds) && newEvictionInvoiceAmount
  )

  const evictionCaseUpdatingData = {}
  if (hasEvictionCases && evictionStatus === 'new') {
    evictionCaseUpdatingData['$set'] = {
      'evictionCases.$.invoiceId': head(newEvictionInvoiceIds),
      'evictionCases.$.evictionInvoiceIds': newEvictionInvoiceIds,
      'evictionCases.$.amount': newEvictionInvoiceAmount
    }
  } else if (!hasEvictionCases && evictionStatus === 'new') {
    evictionCaseUpdatingData['$pull'] = {
      evictionCases: { evictionInvoiceIds: invoiceId }
    }
  } else if (evictionStatus === 'in_progress') {
    evictionCaseUpdatingData['$set'] = {
      'evictionCases.$.evictionInvoiceIds': newEvictionInvoiceIds,
      'evictionCases.$.amount': newEvictionInvoiceAmount
    }
    if (!hasEvictionCases) {
      evictionCaseUpdatingData['$set']['evictionCases.$.hasPaid'] = true
    }
  }

  const updatedContract = size(evictionCaseUpdatingData)
    ? await contractService.updateContract(
        {
          _id: contractId,
          partnerId,
          'evictionCases.evictionInvoiceIds': invoiceId,
          'evictionCases.hasPaid': { $ne: true }
        },
        evictionCaseUpdatingData,
        session
      )
    : false
  // If eviction case removed then add activity log
  const isRemovedEvictionCase = size(updatedContract)
    ? !size(
        find(updatedContract.evictionCases, (eviction) => {
          const { evictionInvoiceIds = [], invoiceId: evictionInvoiceId } =
            eviction || {}

          return (
            evictionInvoiceId === invoiceId ||
            includes(evictionInvoiceIds, invoiceId)
          )
        })
      )
    : false
  if (isRemovedEvictionCase) {
    await createEvictionCaseRemoveLog(
      { contract: updatedContract, contractId, invoiceId },
      session
    )
  }

  // If all eviction case has paid then add app queue for sending tenant pays all overdue during eviction notification
  const hasPaidAllEvictionCases = size(updatedContract?.evictionCases)
    ? !size(
        find(updatedContract.evictionCases, (eviction) => !eviction.hasPaid)
      )
    : false
  if (hasPaidAllEvictionCases) {
    await createAppQueueForTenantPaysAllOverdueDuringEvictionNotification(
      { contractId, partnerId },
      session
    )
  }

  return updatedContract || (!size(evictionCaseUpdatingData) ? contract : {})
}

export const createOrUpdateEvictionCase = async (req = {}) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  await evictionCaseHelper.checkRequiredFieldsAndDataForEvictionCase(
    body,
    session
  )

  const { contractId, invoiceId, partnerId } = body || {}
  const contract = await contractHelper.getAContract(
    { _id: contractId, partnerId },
    session
  )
  if (!size(contract))
    throw new CustomError(404, 'Could not find contract data')

  const { evictionCases = [] } = contract || {}
  const evictionCaseWithStatusNew = find(
    evictionCases,
    (evictionCase) => !includes(['completed', 'canceled'], evictionCase.status)
  )
  const invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
  if (!size(invoice)) throw new CustomError(404, 'Could not find invoice data')

  let updatedContract
  if (evictionCaseWithStatusNew?.invoiceId) {
    updatedContract = await updateEvictionCaseForInvoice(
      invoice,
      evictionCaseWithStatusNew,
      session
    )
  } else {
    updatedContract = await createEvictionCaseForInvoice(
      contract,
      invoice,
      session
    )
  }

  return !!(size(updatedContract) && updatedContract._id)
}

export const updateOrRemoveEvictionCase = async (req = {}) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  await evictionCaseHelper.checkRequiredFieldsAndDataForEvictionCase(
    body,
    session
  )

  const { contractId, invoiceId, partnerId } = body || {}
  const contract = await contractHelper.getAContract(
    { _id: contractId, partnerId, evictionCases: { $exists: true } },
    session
  )
  if (!size(contract))
    throw new CustomError(404, 'Could not find contract data')

  const invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
  if (!size(invoice)) throw new CustomError(404, 'Could not find invoice data')

  const updatedContract = await updateOrRemoveEvictionCaseForInvoice(
    contract,
    invoice,
    session
  )

  return !!(size(updatedContract) && updatedContract._id)
}
