import {
  assign,
  cloneDeep,
  differenceBy,
  includes,
  intersection,
  isBoolean,
  isEmpty,
  isEqual,
  map,
  pick,
  size,
  union
} from 'lodash'
import moment from 'moment-timezone'
import nid from 'nid'

import { CustomError } from '../common'
import { AppQueueCollection, ContractCollection } from '../models'
import {
  accountHelper,
  addonHelper,
  appHelper,
  appHealthHelper,
  appQueueHelper,
  branchHelper,
  contractHelper,
  eSigningHelper,
  fileHelper,
  invoiceHelper,
  listingHelper,
  logHelper,
  partnerHelper,
  partnerSettingHelper,
  propertyItemHelper,
  userHelper
} from '../helpers'
import {
  accountService,
  appQueueService,
  commissionService,
  correctionService,
  counterService,
  evictionCaseService,
  fileService,
  invoicePaymentService,
  invoiceSummaryService,
  invoiceService,
  listingService,
  logService,
  partnerSettingService,
  partnerUsageService,
  payoutService,
  propertyItemService,
  tenantService
} from '../services'
import { getActualDate } from '../app/app.helper'

export const updateContractStatus = async (req) => {
  const { body, session } = req
  // Validating input data
  appHelper.compactObject(body)
  appHelper.checkRequiredFields(['contractId', 'status'], body)
  const { contractId, status } = body

  const previousContract = await contractHelper.getAContract({
    _id: contractId
  })

  if (!size(previousContract)) {
    throw new CustomError(404, 'Contract not found')
  }
  const { partnerId, propertyId } = previousContract

  const updatedContract = await updateContractStatusAndCreateRentInvoice(
    {
      contractId,
      contractInfo: previousContract,
      status,
      partnerId,
      propertyId
    },
    session
  )

  if (size(updatedContract)) {
    await contractStatusUpdateAfterHooksProcess(
      { previousContract, updatedContract },
      session
    )
  }
  return {
    msg: 'Contract updated successfully'
  }
}

export const resetSingleContractForCpi = async (req) => {
  const { body, session } = req
  const { queryData } = body
  const { contractId } = queryData
  const previousContract = await contractHelper.getAContract({
    _id: contractId
  })

  if (!size(previousContract)) {
    throw new CustomError(404, 'Contract not found')
  }

  const cpiDate = (
    await getActualDate(previousContract?.partnerId, true, new Date())
  )
    .add(31, 'days')
    .endOf('day')
    .toDate()
  console.log('Cpi date', cpiDate)
  console.log(queryData)
  const contract = await updateContract(
    {
      _id: contractId,
      status: { $in: ['active', 'upcoming'] },
      'rentalMeta.cpiEnabled': true,
      'rentalMeta.lastCPINotificationSentOn': { $exists: true },
      'rentalMeta.futureRentAmount': { $exists: true },
      'rentalMeta.nextCpiDate': { $exists: true, $gt: cpiDate }
    },
    {
      $unset: {
        'rentalMeta.futureRentAmount': 1,
        'rentalMeta.lastCPINotificationSentOn': 1
      }
    },
    session
  )
  return contract
}

export const resetContractForCpi = async (req) => {
  const { body, session } = req
  const { queryData } = body
  const { partnerId, cpiDate } = queryData
  console.log(queryData)
  const contract = await ContractCollection.updateMany(
    {
      partnerId,
      status: { $in: ['active', 'upcoming'] },
      'rentalMeta.cpiEnabled': true,
      'rentalMeta.lastCPINotificationSentOn': { $exists: true },
      'rentalMeta.futureRentAmount': { $exists: true },
      'rentalMeta.nextCpiDate': { $exists: true, $gt: cpiDate }
    },
    {
      $unset: {
        'rentalMeta.futureRentAmount': 1,
        'rentalMeta.nextCpiDate': 1
      }
    },
    {
      new: true,
      runValidators: true,
      session
    }
  )
  return contract
}

export const createContract = async (data, session) => {
  const [createdContract] = await ContractCollection.create([data], { session })
  if (!size(createdContract))
    throw new CustomError(400, 'Unable to create contract')
  return createdContract
}

export const updateContracts = async (query, data, session) => {
  if (isEmpty(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const updatedContracts = await ContractCollection.updateMany(query, data, {
    session
  })
  if (updatedContracts.nModified > 0) {
    return updatedContracts
  }
}

export const updateContract = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for update')
  }
  const response = await ContractCollection.findOneAndUpdate(query, data, {
    runValidators: true,
    new: true,
    session
  })
  return response
}

export const updateContractWithUpdateOne = async (query, data, session) => {
  const response = await ContractCollection.updateOne(query, data, {
    session
  })
  return response
}

export const dailyNaturalTerminationNoticeSendService = async (
  contractId,
  session
) => {
  const contract = await ContractCollection.aggregate()
    .match({ _id: contractId })
    .lookup({
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    })
    .unwind('$partnerSettings')
  console.log('Contract we are working on', contractId, contract)
  const today = (
    await appHelper.getActualDate(contract[0].partnerSetting, true, new Date())
  ).toDate()
  try {
    const contractUpdate = await ContractCollection.updateOne(
      { _id: contractId },
      { 'rentalMeta.naturalTerminatedNoticeSendDate': today },
      session
    )
    console.log('Contract update response', contractUpdate)
    const appQueueData = {
      event: 'send_natural_termination_notice',
      action: 'send_notification',
      destination: 'notifier',
      params: {
        partnerId: contract[0].partnerId,
        collectionId: contract[0]._id,
        collectionNameStr: 'contracts'
      },
      priority: 'immediate'
    }
    await appQueueService.createAnAppQueue(appQueueData, session)
    return {
      msg: 'Send natural termination notice queued'
    }
  } catch (e) {
    console.log('Contract termination notice cannot be sent', e)
    throw new Error(e.message)
  }
}

export const soonEndingService = async (contractId, session) => {
  const contract = await ContractCollection.aggregate()
    .match({ _id: contractId })
    .lookup({
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    })
  const today = (
    await appHelper.getActualDate(contract[0].partnerSetting, true, new Date())
  ).toDate()
  try {
    const contractUpdate = await ContractCollection.updateOne(
      { _id: contractId },
      { 'rentalMeta.soonTerminatedNoticeSendDate': today },
      session
    )
    console.log('Contract update response', contractUpdate)
    const appQueueData = {
      event: 'send_soon_ending_notice',
      action: 'send_notification',
      destination: 'notifier',
      params: {
        partnerId: contract[0].partnerId,
        collectionId: contract[0]._id,
        collectionNameStr: 'contracts'
      },
      priority: 'immediate'
    }
    await appQueueService.createAnAppQueue(appQueueData, session)
    return {
      msg: 'Soon ending notice queued'
    }
  } catch (e) {
    console.log('Contract soon ending notice cannot be sent', e)
    throw new Error(e.message)
  }
}

export const updateHistoryInContract = async (updateParams, session) => {
  const { contract, names, preparedHistory } = updateParams
  const { _id } = contract || {}
  let { history = [] } = contract || {}
  // Only commissions history can occur multiple times
  if (size(history) && !names.includes('commissions')) {
    history = history.filter(({ name }) => !names.includes(name))
  }
  history = union(history, preparedHistory)
  const updatedContract = await updateContract(
    { _id },
    { $set: { history } },
    session
  )
  return updatedContract
}

export const updateCommissionChangesInfoInContract = async (
  invoiceId,
  session
) => {
  const invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
  if (!size(invoice)) {
    throw new CustomError(404, 'Could not find invoice')
  }
  const { contractId } = invoice
  const contract = await contractHelper.getContractById(contractId, session)
  if (!size(contract)) {
    throw new CustomError(404, 'Could not find contract')
  }
  const oldCommissionTotal = await contractHelper.getOldCommissionTotal(
    contractId
  )
  const newCommissionTotal = await contractHelper.getNewCommissionTotal(
    contractId
  )
  if (oldCommissionTotal === newCommissionTotal) {
    return false
  }
  const params = { contract, oldCommissionTotal, newCommissionTotal }
  const { history, names } = await contractHelper.prepareHistoryAndNames(params)
  const updateParams = {
    contract,
    names,
    preparedHistory: history
  }
  const updatedContract = await updateHistoryInContract(updateParams, session)
  return updatedContract
}

export const addContractForDirectPartner = async (property, session) => {
  const contractAddData = await contractHelper.prepareContractAddData(
    property,
    session
  )
  const createdContract = await createContract(contractAddData, session)
  return createdContract
}

export const addDefaultedTagInContract = async (invoice, session) => {
  const { contractId, partnerId, propertyId, isDefaulted } = invoice
  if (isDefaulted) {
    const query = {
      _id: contractId,
      partnerId,
      propertyId
    }
    const data = { isDefaulted: true }
    const updatedContract = (await updateContract(query, data, session)) || {}
    return updatedContract.isDefaulted
  }
}

export const removeDefaultedTagFromContract = async (invoice, session) => {
  const { _id, contractId, partnerId, propertyId, isDefaulted } = invoice

  if (!isDefaulted) {
    let query = { _id: { $ne: _id }, contractId, isDefaulted: true }
    const isDefaultedInvoiceExist = !!(await invoiceHelper.getInvoice(
      query,
      session
    ))
    if (!isDefaultedInvoiceExist) {
      query = {
        _id: contractId,
        partnerId,
        propertyId
      }
      const data = { isDefaulted: false }
      const updatedContract = (await updateContract(query, data, session)) || {}
      return updatedContract.isDefaulted
    }
  }
}

export const createEvictionCase = async (invoice = {}, session) => {
  const { contractId, partnerId } = invoice
  const invoicesForEvictionCase =
    await contractHelper.getInvoicesForEvictionCase(invoice, session)
  const isCreateEvictionPackage = await contractHelper.isCreateEvictionPackage(
    partnerId,
    session
  )
  if (isCreateEvictionPackage && size(invoicesForEvictionCase) && contractId) {
    const params = { invoice, invoicesForEvictionCase }
    const evictionCaseData = await contractHelper.prepareEvictionCaseData(
      params,
      session
    )
    if (evictionCaseData) {
      const query = { _id: contractId }
      const updateData = { $push: { evictionCases: evictionCaseData } }
      const updatedContract = await updateContract(query, updateData, session)
      return updatedContract
    }
  }
}

export const updateEvictionCase = async (invoice = {}, session) => {
  const { contractId, partnerId, _id, invoiceTotal, evictionDueReminderSent } =
    invoice
  const isCreateEvictionPackage = await contractHelper.isCreateEvictionPackage(
    partnerId,
    session
  )
  if (
    isCreateEvictionPackage &&
    invoiceTotal &&
    _id &&
    contractId &&
    evictionDueReminderSent
  ) {
    const query = {
      _id: contractId,
      evictionCases: {
        $elemMatch: {
          status: { $nin: ['canceled', 'completed'] },
          evictionInvoiceIds: { $nin: [_id] }
        }
      }
    }
    const updateData = {
      $push: { 'evictionCases.$.evictionInvoiceIds': _id },
      $inc: { 'evictionCases.$.amount': invoiceTotal }
    }
    const updatedContract = await updateContract(query, updateData, session)
    return updatedContract
  }
}

export const createOrUpdateEvictionCase = async (data, session) => {
  const { invoiceId } = data
  const invoice = await invoiceHelper.getInvoiceById(invoiceId, session)
  const hasEvictionCase = await contractHelper.hasEvictionCase(invoice, session)
  let result
  if (!hasEvictionCase) {
    result = await createEvictionCase(invoice, session)
  } else {
    result = await updateEvictionCase(invoice, session)
  }
  return result
}

export const updateContractOwner = async (listing, session) => {
  const query = contractHelper.prepareQueryForActiveContract(listing)
  const { agentId } = listing
  const updatedContract = await updateContract(
    query,
    { $set: { agentId } },
    session
  )
  return updatedContract
}

export const addChangeLogHistoryToContract = async (
  updatedListing,
  previousListing,
  session
) => {
  const { _id } = updatedListing
  const contracts = await contractHelper.getContracts(
    { propertyId: _id },
    session
  )
  const params = { updatedListing, previousListing }
  const { history = [], names = [] } =
    await contractHelper.prepareHistoryAndNamesByListing(params)
  if (size(history)) {
    for (const contract of contracts) {
      const updateParams = {
        contract,
        names,
        preparedHistory: history
      }
      await updateHistoryInContract(updateParams, session)
    }
  }
}

export const createLogForRemovedEvictionCase = async (params, session) => {
  const { contractId, invoiceId, contract } = params
  let removedDoc = pick(contract, ['accountId', 'propertyId', 'leaseSerial'])
  if (contract.rentalMeta) {
    removedDoc.tenantId = contract.rentalMeta.tenantId
  }
  removedDoc = assign(removedDoc, { invoiceId, contractId })
  const logData = await logHelper.prepareLogDataForRemovedEvictionCase(
    removedDoc,
    session
  )
  await logService.createLog(logData, session)
}

export const updateOrRemoveContractEvictionCase = async (
  data = {},
  session
) => {
  const { partnerId, invoiceId, contractId, paidAmount, ignoreRemove } = data
  const isCreateEvictionPackage = await contractHelper.isCreateEvictionPackage(
    partnerId,
    session
  )
  const params = {
    contractId,
    invoiceId,
    paidAmount
  }
  const hasEvictionCases = await contractHelper.hasEvictionCases(
    params,
    session
  )
  if (hasEvictionCases && isCreateEvictionPackage) {
    let isRemoveEvictionCase = false
    if (!ignoreRemove) {
      isRemoveEvictionCase = await contractHelper.isRemoveEvictionCase(
        params,
        session
      )
    }
    const commonQuery = contractHelper.getCommonEvictionQuery(invoiceId)
    const evictionCaseQuery = contractHelper.getEvictionCaseQuery(
      contractId,
      commonQuery
    )
    params.isRemoveEvictionCase = isRemoveEvictionCase
    const updateData =
      await contractHelper.prepareEvictionCaseRemoveOrUpdateData(
        params,
        session
      )
    const updatedContract = await updateContract(
      evictionCaseQuery,
      updateData,
      session
    )
    if (isRemoveEvictionCase && updatedContract) {
      params.contract = updatedContract
      await createLogForRemovedEvictionCase(params, session)
    }
    return updatedContract
  }
}
export const createEvictionCaseUpdateLog = async (body, contract, session) => {
  const logData = await contractHelper.prepareEvictionCaseUpdateLogData(
    body,
    contract
  )
  return logService.createLog(logData, session)
}

export const removeEvictionCase = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'invoiceId'])
  const { body, session } = req
  const { contractId, invoiceId, partnerId } = body
  const query = {
    _id: contractId,
    partnerId,
    'evictionCases.invoiceId': invoiceId,
    'evictionCases.status': 'new'
  }
  const evictionCase = await contractHelper.getAContract(query, session)
  if (!size(evictionCase)) throw new CustomError(404, 'Eviction case not found')

  const updateData = {
    evictionCases: {
      invoiceId,
      status: 'new'
    }
  }
  const updatedContract = await updateContract(
    query,
    { $pull: updateData },
    session
  )
  const evictionRemovedLog =
    await evictionCaseService.createEvictionCaseRemoveLog(
      {
        contract: updatedContract,
        contractId,
        invoiceId
      },
      session
    )
  if (!(updatedContract?._id && evictionRemovedLog?._id)) {
    throw new CustomError(405, 'Could not complete the request')
  }
  return {
    success: !!(updatedContract._id && evictionRemovedLog._id)
  }
}

export const updateContractEvictionCase = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'contractId',
    'invoiceId',
    'status'
  ])
  const { body, session, user } = req
  const { contractId, invoiceId, status } = body
  const query = {
    _id: contractId,
    partnerId: user.partnerId,
    'evictionCases.invoiceId': invoiceId,
    'evictionCases.status': 'in_progress'
  }
  const updateData = { 'evictionCases.$.status': status }
  const updatedContract = await updateContract(
    query,
    { $set: updateData },
    session
  )
  if (!size(updatedContract)) {
    throw new CustomError(404, 'Eviction case not updated')
  } else {
    await createEvictionCaseUpdateLog(body, updatedContract, session)
  }
  return {
    success: true
  }
}

export const produceEvictionDocuments = async (req) => {
  const { body, session, user } = req
  if (user?.roles.includes('lambda_manager')) {
    user.partnerId = body.partnerId
    user.userId = body.userId
  }
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkRequiredFields(
    ['contractId', 'invoiceId', 'evictionPrevDoc'],
    body
  )
  appHelper.validateId({ partnerId })
  const { contractId, invoiceId, evictionPrevDoc } = body
  appHelper.validateId({ contractId })
  appHelper.validateId({ invoiceId })
  const contractQuery = {
    _id: contractId,
    partnerId,
    'evictionCases.invoiceId': invoiceId
  }

  const evictionInfo = await contractHelper.getAContract(contractQuery)
  if (!size(evictionInfo))
    throw new CustomError(404, 'Unable to find Eviction case')
  const evictionCase = evictionInfo.evictionCases.find(
    (eviction) => eviction.invoiceId === invoiceId
  )

  // produce eviction case
  if (evictionCase?.status === 'new') {
    const updatedContract = await updateContract(
      {
        ...contractQuery,
        'evictionCases.status': 'new'
      },
      {
        $set: { 'evictionCases.$.status': 'in_progress' }
      },
      session
    )
    if (!size(updatedContract))
      throw new CustomError(404, 'Unable to update Eviction case')
  }

  if (
    !(evictionCase?.status === 'new' || evictionCase?.status === 'in_progress')
  )
    throw new CustomError(404, 'Unable to update Eviction case')

  await fileService.removeFilesAndCreateLogs(
    {
      context: 'eviction_document',
      contractId,
      invoiceId,
      partnerId,
      type: 'eviction_document_pdf'
    },
    { userId },
    session
  )
  const queueData = {
    event: 'produce_eviction_document_and_upload_to_s3',
    action: 'produce_eviction_document',
    destination: 'lease',
    priority: 'immediate',
    params: {
      contractId,
      partnerId,
      invoiceId,
      type: 'eviction_document',
      evictionPrevDoc
    }
  }
  await appQueueService.insertInQueue(queueData, session)
  return {
    success: true
  }
}

export const updateAContractAndAddALog = async (req) => {
  const { body, session } = req
  const { data, inputData, contractId } = body
  const previousContract = await contractHelper.getAContract(
    { _id: contractId },
    session
  )
  if (!size(previousContract)) throw new CustomError(404, 'Contract not found')
  try {
    const updatingData = getPreparedContractUpdatingData(data)
    const updatedContractInfo = await updateContract(
      { _id: contractId },
      { $set: updatingData },
      session
    )
    console.log(inputData)
    if (updatedContractInfo) {
      const logData = await logHelper.prepareLogDataForUpdateLease(
        updatedContractInfo,
        inputData
      )
      if (logData) {
        console.log('Log data found', logData)
        await logService.createLog(logData, session)
      }
      // Contract after update hooks process
      if (data?.monthlyRentAmount && data?.nextCpiDate)
        await createContractUpdateChangeLog(
          {
            action: 'updated_lease',
            context: 'property',
            previousContract,
            updatedContract: updatedContractInfo
          },
          session
        )
    }
    return {
      msg: 'Contract updated and log added'
    }
  } catch (e) {
    console.log('Error when updating lease and add log', e)
    throw new Error(e)
  }
}

const createAssignmentSignedLog = async (contract, action, session) => {
  if (!(size(contract) && action))
    throw new CustomError(
      404,
      'Required parameter not found while creating assignment signed log'
    )
  if (!(action === 'agent' || action === 'landlord'))
    throw new CustomError(
      400,
      'Invalid action found while creating assignment signed log'
    )

  const {
    _id: contractId,
    accountId,
    assignmentSerial,
    agentId,
    branchId,
    partnerId,
    propertyId
  } = contract

  const logData = {
    accountId,
    action:
      action === 'agent'
        ? 'agent_signed_assignment_contract'
        : 'landlord_signed_assignment_contract',
    agentId,
    branchId,
    context: 'property',
    contractId,
    isChangeLog: false,
    partnerId,
    propertyId,
    visibility: ['property', 'account']
  }

  if (assignmentSerial)
    logData.metaData = [{ field: 'assignmentSerial', value: assignmentSerial }]

  const log = await logService.createLog(logData, session)
  size(log)
    ? console.log(`=== Created log for ${log.action}. logId: ${log._id} ===`)
    : console.log(`=== Unable to create log for ${log.action} ===`)
}

const createLeaseSignedLog = async (params, session) => {
  const { action, contract, tenantId } = params

  if (!(size(contract) && action && tenantId))
    throw new CustomError(
      404,
      'Required parameter not found while creating lease signed log'
    )
  if (!(action === 'tenant' || action === 'landlord'))
    throw new CustomError(
      400,
      'Invalid action found while creating lease signed log'
    )

  const {
    _id: contractId,
    accountId,
    assignmentSerial,
    agentId,
    branchId,
    leaseSerial,
    partnerId,
    propertyId
  } = contract

  const logData = {
    accountId,
    action:
      action === 'tenant'
        ? 'tenant_signed_lease_contract'
        : 'landlord_signed_lease_contract',
    agentId,
    branchId,
    context: 'property',
    contractId,
    isChangeLog: false,
    partnerId,
    propertyId,
    tenantId,
    visibility: ['property', 'account', 'tenant']
  }

  const metaData = []

  if (leaseSerial) metaData.push({ field: 'leaseSerial', value: leaseSerial })
  if (assignmentSerial)
    metaData.push({ field: 'assignmentSerial', value: assignmentSerial })

  if (size(metaData)) logData.meta = metaData

  const log = await logService.createLog(logData, session)
  size(log)
    ? console.log(`=== Created log for ${log.action}. logId: ${log._id} ===`)
    : console.log(`=== Unable to create log for ${log.action} ===`)
}

export const createPartnerUsagesAndLogForNewlySignedUsersOfAssignment = async (
  oldContract,
  updatedContract,
  session
) => {
  if (!(size(oldContract) && size(updatedContract))) return false

  const { branchId, partnerId } = updatedContract || {}
  const partnerUsagesCreationData = { partnerId, type: 'esign', total: 1 }
  if (branchId) partnerUsagesCreationData.branchId = branchId

  if (
    oldContract.landlordAssignmentSigningStatus &&
    oldContract.landlordAssignmentSigningStatus.signed !== true &&
    updatedContract.landlordAssignmentSigningStatus &&
    updatedContract.landlordAssignmentSigningStatus.signed === true
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for landlordAssignmentSigningStatus. partnerUsageId: ${partnerUsageId} ===`
    )
    await createAssignmentSignedLog(updatedContract, 'landlord', session)
  }

  if (
    oldContract.agentAssignmentSigningStatus &&
    oldContract.agentAssignmentSigningStatus.signed !== true &&
    updatedContract.agentAssignmentSigningStatus &&
    updatedContract.agentAssignmentSigningStatus.signed === true
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for agentAssignmentSigningStatus. partnerUsageId: ${partnerUsageId} ===`
    )
    await createAssignmentSignedLog(updatedContract, 'agent', session)
  }
}

const getSignersInfo = (previous, doc) => {
  const currentRentalMeta = doc?.rentalMeta || null
  const prevRentalMeta = previous?.rentalMeta || null
  const currentLeaseSigningMeta =
    size(currentRentalMeta) && size(currentRentalMeta.leaseSigningMeta)
      ? currentRentalMeta.leaseSigningMeta
      : null
  const prevLeaseSigningMeta =
    size(prevRentalMeta) && size(prevRentalMeta.leaseSigningMeta)
      ? prevRentalMeta.leaseSigningMeta
      : null
  const currentSigners =
    size(currentLeaseSigningMeta) && size(currentLeaseSigningMeta.signers)
      ? currentLeaseSigningMeta.signers
      : []
  const prevSigners =
    size(prevLeaseSigningMeta) && size(prevLeaseSigningMeta.signers)
      ? prevLeaseSigningMeta.signers
      : []

  let newSigners = []

  if (
    (!size(prevSigners) && !size(currentSigners)) ||
    size(prevSigners) === size(currentSigners)
  )
    return null
  else
    newSigners = differenceBy(currentSigners, prevSigners, 'externalSignerId')

  return size(newSigners) ? newSigners : ''
}

export const createPartnerUsagesAndLogForNewlySignedUsersOfLease = async (
  oldContract,
  updatedContract,
  session
) => {
  if (!(size(oldContract) && size(updatedContract))) return false

  const {
    branchId,
    partnerId,
    rentalMeta: updatedRentalMata
  } = updatedContract || {}

  const { rentalMeta: oldRentalMata } = oldContract

  if (!(size(oldRentalMata) && size(updatedRentalMata))) return false

  const preLandlordLeasSigningStatus = oldRentalMata.landlordLeaseSigningStatus
  const currentLandlordLeaseSigningStatus =
    updatedRentalMata.landlordLeaseSigningStatus
  const preTenantLeaseSigningStatus = oldRentalMata.tenantLeaseSigningStatus
  const currentTenantLeaseSigningStatus =
    updatedRentalMata.tenantLeaseSigningStatus

  const partnerUsagesCreationData = { partnerId, type: 'esign', total: 1 }
  if (branchId) partnerUsagesCreationData.branchId = branchId

  if (
    size(preLandlordLeasSigningStatus) &&
    preLandlordLeasSigningStatus.signed !== true &&
    size(currentLandlordLeaseSigningStatus) &&
    currentLandlordLeaseSigningStatus.signed === true
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for landlordLeaseSigningStatus. partnerUsageId: ${partnerUsageId} ===`
    )
    await createLeaseSignedLog(
      {
        action: 'landlord',
        contract: updatedContract,
        tenantId: updatedRentalMata.tenantId
      },
      session
    )
  }

  if (
    size(preTenantLeaseSigningStatus) &&
    size(currentTenantLeaseSigningStatus) &&
    appHelper.isAnyTenantSigned(
      preTenantLeaseSigningStatus,
      currentTenantLeaseSigningStatus
    )
  ) {
    const [{ _id: partnerUsageId }] =
      await partnerUsageService.createAPartnerUsage(
        partnerUsagesCreationData,
        session
      )
    console.log(
      `=== Created partnerUsage for tenantLeaseSigningStatus. partnerUsageId: ${partnerUsageId} ===`
    )
    const signers = getSignersInfo(oldContract, updatedContract)

    if (size(signers)) {
      console.log(
        `=== New tenant signers found. singers: ${JSON.stringify(signers)} ===`
      )
      for (const signerInfo of signers) {
        await createLeaseSignedLog(
          {
            action: 'tenant',
            contract: updatedContract,
            tenantId: signerInfo.externalSignerId || updatedRentalMata.tenantId
          },
          session
        )
      }
    }

    // Send ESiging notice to account when all tenant signed
    if (contractHelper.isAllTenantSignCompleted(updatedContract)) {
      const appQueueData = {
        action: 'send_notification',
        destination: 'notifier',
        event: 'send_landlord_lease_esigning',
        params: {
          partnerId,
          collectionId: updatedContract._id,
          collectionNameStr: 'contracts'
        },
        priority: 'immediate',
        status: 'new'
      }
      const [createdQueue] = await appQueueService.createAnAppQueue(
        appQueueData,
        session
      )
      if (size(createdQueue)) {
        console.log(
          `## Creating an appQueue to send emails to the landlord with document E-SigningUrl for lease. CreatedQueueId:
        ${createdQueue._id}`
        )
      }
    }
  }
}

export const initAfterUpdateProcessForContractESigning = async (
  oldContract,
  updatedContract,
  session
) => {
  if (!(size(oldContract) && size(updatedContract)))
    throw new CustomError(
      404,
      'Required params missing, while initialing contract after update esigning process'
    )
  // For Assignment (Agent and Landlord)
  await createPartnerUsagesAndLogForNewlySignedUsersOfAssignment(
    oldContract,
    updatedContract,
    session
  )

  // For Lease (Landlord and tenant or tenants)
  await createPartnerUsagesAndLogForNewlySignedUsersOfLease(
    oldContract,
    updatedContract,
    session
  )
}

export const updateAContract = async (req) => {
  const { body, session, user } = req

  // Validating userId
  appHelper.checkRequiredFields(['userId'], user)
  const { userId = '' } = user || {}
  appHelper.checkUserId(userId)

  // Validating input data
  appHelper.checkRequiredFields(['contractId', 'data'], body)
  // if (!body.data || !body.unsetData) throw new CustomError(400, `Missing data`)

  // Extracting data from body
  const { contractId = '', data = {}, queueId, unsetData = {} } = body
  appHelper.validateId({ contractId })

  const contractInfo = await contractHelper.getAContract(
    { _id: contractId },
    session,
    ['partner']
  )
  if (!size(contractInfo)) throw new CustomError(404, 'Contract not found')

  console.log('Data that will process to update', data)
  const nonESigningPdfContext = ['assignmentPdfGenerated', 'leasePdfGenerated']
  if (data.pdfContext && nonESigningPdfContext.includes(data.pdfContext)) {
    const fileQuery = { contractId }
    if (data.pdfContext === 'assignmentPdfGenerated')
      fileQuery.type = 'assignment_pdf'
    else if (data.pdfContext === 'leasePdfGenerated')
      fileQuery.type = 'lease_pdf'
    console.log('===> fileQuery', fileQuery)
    await fileService.addORRemoveFileInUseTag(
      fileQuery,
      { isFileInUse: false },
      session
    )
  }
  if (
    data?.type !== 'deposit_account_contract_pdf' &&
    data.hasSignersAttachmentPadesFile
  ) {
    delete data.hasSignersAttachmentPadesFile
  }
  // Preparing updating data
  const updatingData = getPreparedContractUpdatingData(data)
  console.log('Data after processed', updatingData)
  if (!size(updatingData)) throw new CustomError(400, 'Invalid updating data!')

  let query = { _id: contractId }
  if (data?.tenantId && data?.isSendToBank) {
    query = {
      ...query,
      'rentalMeta.tenantLeaseSigningStatus.tenantId': data.tenantId
    }
    // Removing fileInUse tag from the files
    await fileService.addORRemoveFileInUseTag(
      {
        contractId,
        $or: [{ tenantId: data.tenantId }, { tenantId: { $exists: false } }],
        type: {
          $in: ['esigning_lease_pdf', 'deposit_account_contract_pdf']
        },
        isFileInUse: true
      },

      { $set: { isFileInUse: false } },
      session
    )
  }

  if (data?.tenantId && data?.attachmentFileId) {
    query = {
      ...query,
      'rentalMeta.tenantLeaseSigningStatus.tenantId': data.tenantId,
      'rentalMeta.tenantLeaseSigningStatus.attachmentFileId':
        data.attachmentFileId
    }
  }
  const updateData = { $set: updatingData }
  console.log('== Checking unsetData: ', unsetData)
  if (size(unsetData)) {
    const {
      futureRentAmount = false,
      lastCPINotificationSentOn = false,
      unsetDepositAccountCreationTestProcessing = false,
      unsetIncomingPaymentTestProcessing = false
    } = unsetData
    const preparedUnsetData = {}
    if (futureRentAmount) preparedUnsetData['rentalMeta.futureRentAmount'] = 1
    if (lastCPINotificationSentOn)
      preparedUnsetData['rentalMeta.lastCPINotificationSentOn'] = 1
    if (unsetDepositAccountCreationTestProcessing) {
      preparedUnsetData['rentalMeta.isDepositAccountCreationTestProcessing'] = 1
    }
    if (unsetIncomingPaymentTestProcessing) {
      preparedUnsetData['rentalMeta.isDepositAccountPaymentTestProcessing'] = 1
    }
    console.log('== Checking preparedUnsetData: ', preparedUnsetData)
    if (size(preparedUnsetData)) {
      updateData.$unset = preparedUnsetData
    }
  }
  console.log('== Checking updateData: ', updateData)
  // Updating contract
  const updatedContractInfo = await updateContract(query, updateData, session)

  if (!size(updatedContractInfo))
    throw new CustomError(400, 'Wrong contractId!')

  await initAfterUpdateProcessForContractESigning(
    contractInfo.toObject(),
    updatedContractInfo.toObject(),
    session
  )

  await contractUpdateAfterUpdateHooksProcessForLambda(
    {
      data,
      previousContract: contractInfo.toObject(),
      updatedContract: updatedContractInfo.toObject(),
      userId
    },
    session
  )

  if (queueId) {
    // Updating lambda appQueue to completed
    console.log(`=== Updating AppQueue to completed ===`)
    await appQueueService.updateAppQueueToCompleted(queueId, session)
  }
  updatedContractInfo.partner = contractInfo?.partner
  return updatedContractInfo
}

const contractUpdateAfterUpdateHooksProcessForLambda = async (
  params = {},
  session
) => {
  const { data, previousContract, updatedContract, userId } = params

  // Send assignment landlord welcome email
  const { partner, partnerId, status } = previousContract
  if (
    (updatedContract.status === 'upcoming' &&
      status === 'in_progress' &&
      updatedContract.agentAssignmentSigningStatus?.signed === true &&
      updatedContract.landlordAssignmentSigningStatus?.signed === true) ||
    (partner?.accountType === 'broker' &&
      updatedContract.isSendAssignmentPdf &&
      updatedContract.assignmentPdfGenerated &&
      !previousContract.assignmentPdfGenerated)
  ) {
    const partnerSettings = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })

    if (!size(partnerSettings)) {
      throw new CustomError(404, 'Could not find partner setting')
    }

    const isSendAssignmentEmail = partnerSettings.notifications?.sentAssignment

    if (isSendAssignmentEmail)
      await createAppQueueToSendAssignmentNotification(
        previousContract._id,
        partnerId,
        session
      )
  }
  const { lastCPINotificationSentOn, nextCpiDate } = data

  if (lastCPINotificationSentOn || nextCpiDate) {
    await createContractUpdateChangeLog(
      {
        action: 'updated_lease',
        context: 'property',
        previousContract,
        updatedContract,
        userId
      },
      session
    )
  }
}

const getPreparedContractUpdatingData = (params = {}) => {
  const {
    agentAssignmentSigningStatus,
    assignmentPadesFileCreatedAt = undefined,
    assignmentSignerXmlFileInS3At = undefined,
    assignmentSigningMeta,
    attachmentPadesFileCreatedAt,
    cpiFromMonth,
    cpiInMonth,
    cpiNotificationSentHistory,
    draftAssignmentDoc = undefined,
    draftLeaseDoc = undefined,
    futureRentAmount,
    hasAssignmentPadesFile = undefined,
    hasAttachmentPadesFile,
    hasAssignmentSignerXmlFileInS3 = undefined,
    hasLeaseSignerXmlFileInS3 = undefined,
    hasSignersAttachmentPadesFile,
    idfyLeaseDocId,
    landlordAssignmentSigningStatus,
    lastCpiDate,
    lastCPINotificationSentOn,
    landlordLeaseSigningStatus = {},
    leaseSigningComplete = undefined,
    leaseSignerXmlInS3At,
    leaseSigningMeta = {},
    monthlyRentAmount,
    nextCpiDate,
    pdfContext = '',
    signedAt,
    signDate = undefined,
    status = '',
    isSendToBank = false,
    tenantLeaseSigningStatus = []
  } = params || {}

  const updatingData = {}

  if (idfyLeaseDocId) updatingData.idfyLeaseDocId = idfyLeaseDocId
  if (assignmentPadesFileCreatedAt)
    updatingData.assignmentPadesFileCreatedAt = assignmentPadesFileCreatedAt
  if (assignmentSignerXmlFileInS3At)
    updatingData.assignmentSignerXmlFileInS3At = assignmentSignerXmlFileInS3At
  if (isBoolean(draftAssignmentDoc))
    updatingData.draftAssignmentDoc = draftAssignmentDoc
  if (isBoolean(draftLeaseDoc)) updatingData.draftLeaseDoc = draftLeaseDoc
  if (isBoolean(hasAssignmentPadesFile))
    updatingData.hasAssignmentPadesFile = hasAssignmentPadesFile
  if (isBoolean(hasAssignmentSignerXmlFileInS3))
    updatingData.hasAssignmentSignerXmlFileInS3 = hasAssignmentSignerXmlFileInS3

  if (status) updatingData.status = status
  if (size(agentAssignmentSigningStatus))
    updatingData.agentAssignmentSigningStatus = agentAssignmentSigningStatus
  if (size(landlordAssignmentSigningStatus))
    updatingData.landlordAssignmentSigningStatus =
      landlordAssignmentSigningStatus
  if (size(assignmentSigningMeta))
    updatingData.assignmentSigningMeta = assignmentSigningMeta
  if (signDate) updatingData.signDate = signDate

  if (
    includes(
      [
        'assignmentContractPdfGenerated',
        'assignmentPdfGenerated',
        'leaseContractPdfGenerated'
      ],
      pdfContext
    )
  )
    updatingData[pdfContext] = true

  if (pdfContext === 'leasePdfGenerated')
    updatingData['rentalMeta.leasePdfGenerated'] = true
  if (lastCPINotificationSentOn)
    updatingData['rentalMeta.lastCPINotificationSentOn'] =
      lastCPINotificationSentOn
  if (futureRentAmount)
    updatingData['rentalMeta.futureRentAmount'] = futureRentAmount
  if (cpiNotificationSentHistory)
    updatingData['rentalMeta.cpiNotificationSentHistory'] =
      cpiNotificationSentHistory
  if (cpiFromMonth) updatingData['rentalMeta.cpiFromMonth'] = cpiFromMonth
  if (cpiInMonth) updatingData['rentalMeta.cpiInMonth'] = cpiInMonth
  if (nextCpiDate) updatingData['rentalMeta.nextCpiDate'] = nextCpiDate
  if (lastCpiDate) updatingData['rentalMeta.lastCpiDate'] = lastCpiDate
  if (monthlyRentAmount)
    updatingData['rentalMeta.monthlyRentAmount'] = monthlyRentAmount
  if (isBoolean(hasLeaseSignerXmlFileInS3))
    updatingData['rentalMeta.hasLeaseSignerXmlFileInS3'] =
      hasLeaseSignerXmlFileInS3
  if (size(landlordLeaseSigningStatus))
    updatingData['rentalMeta.landlordLeaseSigningStatus'] =
      landlordLeaseSigningStatus
  if (size(tenantLeaseSigningStatus))
    updatingData['rentalMeta.tenantLeaseSigningStatus'] =
      tenantLeaseSigningStatus
  if (size(leaseSigningMeta))
    updatingData['rentalMeta.leaseSigningMeta'] = leaseSigningMeta
  if (isBoolean(leaseSigningComplete))
    updatingData['rentalMeta.leaseSigningComplete'] = leaseSigningComplete
  if (leaseSignerXmlInS3At)
    updatingData['rentalMeta.leaseSignerXmlInS3At'] = leaseSignerXmlInS3At
  if (signedAt) updatingData['rentalMeta.signedAt'] = signedAt
  if (isSendToBank)
    updatingData[
      'rentalMeta.tenantLeaseSigningStatus.$.isSentDepositDataToBank'
    ] = isSendToBank
  if (hasAttachmentPadesFile) {
    updatingData[
      'rentalMeta.tenantLeaseSigningStatus.$.hasAttachmentPadesFile'
    ] = hasAttachmentPadesFile
  }
  if (attachmentPadesFileCreatedAt) {
    updatingData[
      'rentalMeta.tenantLeaseSigningStatus.$.attachmentPadesFileCreatedAt'
    ] = attachmentPadesFileCreatedAt
  }
  if (hasSignersAttachmentPadesFile) {
    updatingData['rentalMeta.hasSignersAttachmentPadesFile'] =
      hasSignersAttachmentPadesFile
  }

  return updatingData
}

export const prepareContractESigningDataAndCreateAppQueue = async (req) => {
  const { body, session, user } = req

  // Validating userId
  appHelper.checkRequiredFields(['userId'], user)
  const { userId = '' } = user || {}
  appHelper.checkUserId(userId)

  // Validating input data
  appHelper.checkRequiredFields(
    [
      'callBackAction',
      'callBackEvent',
      'contractId',
      'eSigningType',
      'fileType'
    ],
    body
  )

  // Extracting data from body
  const {
    callBackAction = '',
    callBackEvent = '',
    contractId = '',
    eSigningType = '',
    fileType = ''
  } = body
  appHelper.validateId({ contractId })

  if (
    !(
      includes(['assignment', 'lease'], eSigningType) &&
      includes(['esigning_assignment_pdf', 'esigning_lease_pdf'], fileType)
    )
  )
    throw new CustomError(400, 'Invalid e-signing request!')

  const contractInfo =
    (await contractHelper.getAContract({ _id: contractId }, null, [
      { path: 'account', populate: ['person'] },
      { path: 'partner', populate: ['owner', 'partnerSetting'] },
      { path: 'tenant' }
    ])) || {}
  if (!size(contractInfo)) throw new CustomError(404, 'Wrong contractId!')

  const isAssignmentESigning = eSigningType === 'assignment'
  const eSigningFileInfo =
    (await fileHelper.getAFileWithSort(
      { contractId, type: fileType },
      { createdAt: -1 },
      null
    )) || {}
  if (!size(eSigningFileInfo))
    throw new CustomError(404, 'E-signing PDF file not found!')

  const fileKey = (await fileHelper.getFileKey(eSigningFileInfo)) || ''
  if (!size(fileKey))
    throw new CustomError(404, 'E-signing PDF file key not found!')
  // Getting data for IDFY document
  const dataForIdfy = await eSigningHelper.getContractESigningDataForIdfy({
    contractId,
    contractInfo,
    eSigningType,
    isAssignmentESigning
  })
  // Generating app queue
  const appQueueData = size(dataForIdfy)
    ? appQueueHelper.getAppQueueDataForESigningGeneration({
        docId: contractId,
        callBackParams: {
          callBackAction,
          callBackDestination: 'lease',
          callBackEvent
        },
        dataForIdfy,
        eSignType: eSigningType,
        fileType,
        fileKey,
        partnerId: contractInfo.partnerId
      })
    : {}
  const [appQueueInfo] = size(appQueueData)
    ? await appQueueService.createAnAppQueue(appQueueData, session)
    : []
  if (!size(appQueueInfo))
    throw new CustomError(400, 'Could not create app queue collection data!')

  const { _id: queueId } = appQueueInfo || {}
  return { queueId }
}

export const downloadJournal = async (req) => {
  const { body, session, user } = req
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  appHelper.checkRequiredFields(['type'], body)

  const {
    accountId,
    agentId,
    branchId,
    propertyId,
    assignmentStatus,
    leaseStatus,
    assignmentDateRange,
    leaseDateRange,
    type,
    sort = { createdAt: -1 }
  } = body

  appHelper.validateSortForQuery(sort)
  const params = {}

  if (accountId) {
    appHelper.validateId({ accountId })
    params.accountId = accountId
  }
  if (agentId) {
    appHelper.validateId({ agentId })
    params.agentId = agentId
  }
  if (branchId) {
    appHelper.validateId({ branchId })
    params.branchId = branchId
  }
  if (propertyId) {
    appHelper.validateId({ propertyId })
    params.propertyId = propertyId
  }

  params.partnerId = partnerId
  params.userId = userId
  params.sort = sort
  if (type === 'assignment_journals') {
    if (assignmentStatus) params.assignmentStatus = assignmentStatus
    if (size(assignmentDateRange)) {
      const { startDate, endDate } = assignmentDateRange
      params.assignmentDateRange = {
        startDate: new Date(startDate),
        endDate: new Date(endDate)
      }
    }
    params.type = 'assignment'
    params.downloadProcessType = 'download_assignment_journals'
  } else {
    if (leaseStatus) params.leaseStatus = leaseStatus
    if (size(leaseDateRange)) {
      const { startDate, endDate } = leaseDateRange
      params.leaseDateRange = {
        startDate: new Date(startDate),
        endDate: new Date(endDate)
      }
    }
    if (size(assignmentDateRange)) {
      const { startDate, endDate } = assignmentDateRange
      params.assignmentDateRange = {
        startDate: new Date(startDate),
        endDate: new Date(endDate)
      }
    }
    params.type = 'lease'
    params.downloadProcessType = 'download_turnover_journals'
  }
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'

  const prepareQueueData = {
    action: 'download_email',
    event: 'excel manager',
    priority: 'immediate',
    destination: 'excel-manager',
    status: 'new',
    params
  }

  const annualStatementQueue = await appQueueService.createAnAppQueue(
    prepareQueueData,
    session
  )
  if (size(annualStatementQueue)) {
    return {
      status: 200,
      message:
        'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
    }
  } else {
    throw new CustomError(404, `Unable to download journal`)
  }
}

export const createAnAssignment = async (req) => {
  const { body = {}, session, user = {} } = req
  const { roles = [] } = user
  if (!roles.includes('lambda_manager') && user.partnerId) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }
  await contractHelper.validateDataToCreateAnAssignment(body)
  const { partnerId, userId } = body
  const {
    actionType,
    agentId,
    esigningPdfContent,
    files,
    fileVisibleToLandlord,
    listingInfo,
    payoutTo,
    propertyId
  } = body
  const partnerSetting = await partnerSettingHelper.getAPartnerSetting({
    partnerId
  })
  if (!size(partnerSetting))
    throw new CustomError(404, 'Partner setting not found')
  const enabledEsigning =
    partnerSetting.assignmentSettings?.enableEsignAssignment || false
  const enabledShowAssignmentFilesToLandlord =
    partnerSetting.assignmentSettings?.enabledShowAssignmentFilesToLandlord ||
    false
  const isSendAssignmentEmail =
    partnerSetting.notifications?.sentAssignment || false
  body.enabledEsigning = enabledEsigning
  const property = await listingHelper.getAListing({
    _id: propertyId,
    partnerId
  })
  if (!size(property)) throw new CustomError(404, 'Property not found')
  if (agentId) {
    const branchInfo = await branchHelper.getABranch({
      _id: property.branchId,
      agents: agentId
    })
    if (!branchInfo)
      throw new CustomError(
        404,
        "Agent has been removed from this property's branch. Please change the agent and try again"
      )
  }
  const existingAssignment = await contractHelper.getAContract({
    partnerId,
    propertyId,
    status: 'upcoming'
  })
  if (size(existingAssignment))
    throw new CustomError(400, 'Assignment already created')
  const preparedData = contractHelper.prepareDataToCreateAnAssignment({
    ...body,
    property
  })
  preparedData.assignmentSerial = await counterService.incrementCounter(
    'assignment-' + propertyId,
    session
  )
  const contractInfo = await createContract(preparedData, session)
  if (size(files)) {
    await insertAssignmentFiles({
      accountId: property.accountId,
      assignmentSerial: contractInfo.assignmentSerial,
      contractId: contractInfo._id,
      enabledShowAssignmentFilesToLandlord,
      files,
      fileVisibleToLandlord,
      partnerId,
      propertyId,
      userId,
      session
    })
  }
  // Add to bank account number in accounts
  if (payoutTo) {
    await addBankAccountNumberInAccount({
      accountId: property.accountId,
      partnerId,
      payoutTo,
      session,
      userId
    })
  }
  if (contractInfo.status === 'new' || contractInfo.status === 'upcoming') {
    if (esigningPdfContent) {
      await createAQueueToUploadContractPdfToS3AndRemoveOldPdf(
        {
          actionType,
          contractId: contractInfo._id,
          esigningPdfContent,
          partnerId,
          userId
        },
        session
      )
    }
    // To create assignment log (Also part of after insertion work)
    await createAssignmentLog(contractInfo, session)
    // To update all invited and interested prospects of this property
    await tenantService.updateTenants(
      {
        partnerId,
        properties: {
          $elemMatch: {
            propertyId,
            status: { $in: ['invited', 'interested'] }
          }
        }
      },
      {
        $set: { 'properties.$.contractId': contractInfo._id }
      },
      session
    )
  }
  let hasAssignment
  if (size(listingInfo)) {
    if (
      contractInfo.status !== 'in_progress' &&
      !contractInfo.enabledEsigning
    ) {
      hasAssignment = true
      listingInfo.hasAssignment = true
    }
    const updatedProperty = await listingService.updateAListing(
      {
        _id: propertyId,
        partnerId
      },
      {
        $set: listingInfo
      },
      session
    )
    await propertyAfterUpdateProcess(updatedProperty, session)
  }
  // Set customerId to the account
  if (contractInfo.accountId) {
    await updateAccountForCreatingAssignment({
      contractInfo,
      hasAssignment,
      previousAssignment: property.hasAssignment,
      session
    })
  }
  if (
    !contractInfo.enabledEsigning &&
    isSendAssignmentEmail &&
    !contractInfo.isSendAssignmentPdf
  )
    await createAppQueueToSendAssignmentNotification(
      contractInfo._id,
      partnerId,
      session
    )
  return await contractHelper.getNewlyCreatedAssignmentData(contractInfo)
}

const updateListingForAssignmentUpdate = async (params = {}, session) => {
  const updateData = {}
  const query = { _id: params.propertyId, partnerId: params.partnerId }
  const listing = await listingHelper.getAListing(query)
  if (!size(listing)) {
    throw new CustomError(404, 'Listing not found')
  }

  const { depositAmount, monthlyRentAmount } = params
  if (depositAmount) updateData.depositAmount = depositAmount
  if (monthlyRentAmount) updateData.monthlyRentAmount = monthlyRentAmount

  if (size(updateData)) {
    const updatedListingInfo = await listingService.updateAListing(
      query,
      { $set: updateData },
      session
    )

    // Implement after update hook for listing
    if (updateData.monthlyRentAmount) {
      await listingService.insertInQueueForListingBasePrice(
        updatedListingInfo._id,
        session,
        true
      )
    }
  }
}

const action_change_log = async (query = {}, logData = {}, options = {}) => {
  const { collectionId = '', fieldName = '', previousDoc = {} } = options
  let newText = '',
    oldText = ''

  const changesArray = []
  const metaData = []
  const collectionData = await contractHelper.getAContract(query)
  const newLogData = {
    accountId: collectionData.accountId,
    agentId: collectionData.agentId,
    branchId: collectionData.branchId,
    propertyId: collectionData.propertyId,
    tenantId: collectionData.tenantId
  }
  logData.isChangeLog = true
  Object.assign(logData, newLogData)

  logData.visibility = logHelper.getLogVisibility(options, collectionData)
  logData.contractId = collectionId

  if (collectionData.rentalMeta?.tenantId)
    logData.tenantId = collectionData.rentalMeta.tenantId

  const allowedDateField = [
    'availabilityStartDate',
    'availabilityEndDate',
    'assignmentFrom',
    'assignmentTo'
  ]
  const allowedListingField = [
    'monthlyRentAmount',
    'depositAmount',
    'availabilityStartDate',
    'availabilityEndDate',
    'minimumStay'
  ]
  const allowedForeignKeyField = ['agentId', 'representativeId']

  let type = 'text',
    oldDate = '',
    newDate = ''
  const changeData = {}

  if (allowedDateField.includes(fieldName)) type = 'date'
  if (allowedForeignKeyField.includes(fieldName)) type = 'foreignKey'

  // update changeData object
  if (allowedListingField.includes(fieldName)) {
    newText = collectionData.listingInfo[fieldName] || '0'
    oldText = previousDoc.listingInfo[fieldName] || '0'
  } else {
    newText = collectionData[fieldName] || ''
    oldText = previousDoc[fieldName] || ''
    if (fieldName === 'hasBrokeringContract' && !newText) newText = 'false'
    if (fieldName === 'hasBrokeringContract' && !oldText) oldText = 'false'

    //Set undefined amount value
    if (
      (fieldName === 'rentalManagementCommissionAmount' ||
        fieldName === 'brokeringCommissionAmount') &&
      !newText
    )
      newText = '0'
    if (
      (fieldName === 'rentalManagementCommissionAmount' ||
        fieldName === 'brokeringCommissionAmount') &&
      !oldText
    )
      oldText = '0'
  }

  changeData.field = fieldName
  changeData.type = type

  if (allowedDateField.includes(fieldName)) {
    oldDate = oldText
    if (oldDate) changeData.oldDate = oldDate

    newDate = newText
    if (newDate) changeData.newDate = newDate
  } else {
    if (oldText) changeData.oldText = oldText
    if (newText) changeData.newText = newText
  }
  // updated changeData object

  if (
    collectionData.assignmentSerial &&
    (logData.action === 'updated_contract' ||
      logData.action === 'updated_assignment_addon')
  ) {
    metaData.push({
      field: 'assignmentSerial',
      value: collectionData.assignmentSerial
    })
  }

  if (collectionData.leaseSerial) {
    metaData.push({ field: 'leaseSerial', value: collectionData.leaseSerial })
  }
  if (
    (fieldName === 'brokeringCommissionAmount' &&
      collectionData.brokeringCommissionType === 'percent') ||
    (fieldName === 'rentalManagementCommissionAmount' &&
      collectionData.rentalManagementCommissionType === 'percent')
  ) {
    metaData.push({ field: 'commissionType', value: 'percent' })
  }

  if (size(changeData)) changesArray.push(changeData)

  if (size(metaData)) logData.meta = metaData
  if (size(changesArray)) logData.changes = changesArray

  return logData
}

export const createAssignmentUpdateLog = async (
  action = '',
  options = {},
  session
) => {
  const { collectionId, context, createdBy, partnerId } = options
  let logData = { action, context, partnerId }
  const query = { _id: collectionId, partnerId }

  logData = await action_change_log(query, logData, options)
  if (createdBy) logData.createdBy = createdBy
  const log = await logService.createLog(logData, session)
  return log
}

const addHistoryToContractForChangeLog = async (
  fieldNames,
  params,
  session
) => {
  const preparedData =
    await contractHelper.prepareContractAddHistoryChangeLogData(
      params,
      fieldNames
    )
  if (!preparedData) return false

  const { names, history } = preparedData
  const { updatedContract } = params
  await updateContractForChangeLog(
    {
      contract: updatedContract,
      history,
      names
    },
    session
  )
}

export const updateAssignment = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'propertyId'])
  const { body = {}, session } = req
  appHelper.compactObject(body, true)
  const updateData = await contractHelper.prepareUpdateAssignmentData(body)
  if (!size(updateData)) throw new CustomError(400, 'No data found for update')

  const { contractId, partnerId, previousContract, payoutTo, userId } = body
  const updatedContract = await updateContract(
    { _id: contractId, partnerId },
    { $set: updateData }
  )

  if (!size(updateData)) throw new CustomError(400, 'No data found for update')
  body.updatedContract = updatedContract
  await updateListingForAssignmentUpdate(body, session)

  // Started after update hook process
  if (payoutTo && previousContract.payoutTo !== payoutTo) {
    const accountInfo = await accountHelper.getAnAccount({
      _id: updatedContract.accountId
    })
    if (!size(accountInfo)) throw new CustomError(404, 'Account not found')
    const bankAccountNumbers = union(accountInfo.bankAccountNumbers || [], [
      payoutTo
    ])
    await accountService.updateAnAccount(
      { _id: updatedContract.accountId },
      { $set: { bankAccountNumbers } },
      session
    )
  }

  const fieldNamesForCheckingChanges = [
    'agentId',
    'assignmentFrom',
    'assignmentTo',
    'listingInfo.monthlyRentAmount',
    'representativeId'
  ]

  const updatedFieldNames = Object.keys(updateData) || []
  const isRequiredFieldUpdated = intersection(
    fieldNamesForCheckingChanges,
    updatedFieldNames
  )
  if (size(isRequiredFieldUpdated)) {
    await addHistoryToContractForChangeLog(updatedFieldNames, body, session)
  }
  const params = {
    collectionId: contractId,
    collectionName: 'contract',
    context: 'property',
    createdBy: userId,
    fieldName: updatedFieldNames[0],
    partnerId,
    previousDoc: previousContract
  }
  await createAssignmentUpdateLog('updated_contract', params, session)
  return updatedContract
}

const updateAccountForCreatingAssignment = async ({
  contractInfo,
  hasAssignment,
  previousAssignment,
  session
}) => {
  const { accountId, partnerId } = contractInfo
  const accountUpdateData = {}
  const setData = {}
  const incData = {}
  const accountInfo = await accountHelper.getAnAccount({
    _id: accountId,
    partnerId
  })
  if (!size(accountInfo)) throw new CustomError(404, 'Account not found')
  if (!accountInfo.serial)
    setData.serial = await counterService.incrementCounter(
      'tenant-' + partnerId,
      session
    )
  if (hasAssignment && !previousAssignment) {
    incData.totalActiveProperties = 1
    const inProgressLease = !!(await contractHelper.getAContract({
      accountId,
      status: 'in_progress'
    }))
    if (inProgressLease) setData.status = 'in_progress'
    else setData.status = 'active'
  }
  if (size(setData)) accountUpdateData.$set = setData
  if (size(incData)) accountUpdateData.$inc = incData
  if (size(accountUpdateData))
    await accountService.updateAnAccount(
      {
        _id: accountId,
        partnerId
      },
      accountUpdateData,
      session
    )
}

const createAppQueueToSendAssignmentNotification = async (
  contractId,
  partnerId,
  session
) => {
  const appQueueData = {
    action: 'send_notification',
    destination: 'notifier',
    event: 'send_assignment_email',
    params: {
      collectionId: contractId,
      collectionNameStr: 'contracts',
      partnerId
    },
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(appQueueData, session)
}

const createAssignmentLog = async (contractInfo, session) => {
  const visibility = ['property']
  if (contractInfo.accountId) visibility.push('account')
  const logData = {
    accountId: contractInfo.accountId,
    action: 'created_new_assignment',
    context: 'property',
    contractId: contractInfo._id,
    createdBy: contractInfo.createdBy,
    meta: [{ field: 'assignmentSerial', value: contractInfo.assignmentSerial }],
    partnerId: contractInfo.partnerId,
    propertyId: contractInfo.propertyId,
    visibility
  }
  await logService.createLog(logData, session)
}

const createAQueueToUploadContractPdfToS3AndRemoveOldPdf = async (
  params = {},
  session
) => {
  const {
    actionType,
    contractData,
    contractId,
    contractType = 'assignment',
    esigningPdfContent,
    partnerId,
    userId
  } = params
  console.log('===> Uploaded contract pdf params', {
    actionType,
    contractData,
    contractId,
    contractType,
    partnerId,
    userId
  })
  const queueParams = {
    contractId,
    context: contractType,
    eSigningPdfContent: esigningPdfContent,
    isESigningEnabled: actionType === 'esigning',
    partnerId
  }
  let action = 'handle_assignment_pdf_generation'
  if (contractType === 'lease') action = 'handle_lease_pdf_generation'
  const queueData = {
    action,
    destination: 'lease',
    event: 'handle_pdf_generation',
    params: queueParams,
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(queueData, session)

  /**  File remove */
  if (queueParams.isESigningEnabled) {
    const fileRemoveQuery = { contractId, partnerId }
    if (
      contractType === 'lease' &&
      (contractData?.rentalMeta?.depositType === 'deposit_insurance' ||
        contractData?.rentalMeta?.depositType === 'deposit_account') &&
      contractData?.rentalMeta?.tenantId
    ) {
      const filetype =
        contractData.rentalMeta.depositType === 'deposit_insurance'
          ? 'esigning_deposit_insurance_pdf'
          : 'deposit_account_contract_pdf'

      fileRemoveQuery.type = {
        $in: [`${filetype}`, 'esigning_lease_pdf']
      }
      fileRemoveQuery['$or'] = [
        { tenantId: contractData.rentalMeta.tenantId },
        { tenantId: { $exists: false } }
      ]
    } else if (contractType === 'assignment' || contractType === 'lease') {
      fileRemoveQuery.type =
        contractType === 'lease'
          ? 'esigning_lease_pdf'
          : 'esigning_assignment_pdf'
    }
    console.log('=== fileRemoveQuery ===', fileRemoveQuery)
    if (fileRemoveQuery.type) {
      await fileService.removeFilesAndCreateLogs(
        fileRemoveQuery,
        { userId },
        session
      )
    }
  }
}

const addBankAccountNumberInAccount = async ({
  accountId,
  partnerId,
  payoutTo,
  session,
  userId
}) => {
  const accountInfo = await accountHelper.getAnAccount({
    _id: accountId,
    partnerId
  })
  if (!size(accountInfo)) throw new CustomError(404, 'Account not found')
  const bankAccountNumbers = accountInfo.bankAccountNumbers || []
  if (!bankAccountNumbers.includes(payoutTo)) {
    bankAccountNumbers.push(payoutTo)
    const updatedAccount = await accountService.updateAnAccount(
      {
        _id: accountId,
        partnerId
      },
      {
        $set: {
          bankAccountNumbers
        }
      },
      session
    )
    const logData = {
      accountId: updatedAccount._id,
      action: 'updated_account',
      agentId: updatedAccount.agentId,
      branchId: updatedAccount.branchId,
      changes: [
        {
          field: 'bankAccountNumbers',
          newText: payoutTo,
          type: 'text'
        }
      ],
      context: 'account',
      createdBy: userId,
      isChangeLog: true,
      partnerId,
      visibility: ['account']
    }
    await logService.createLog(logData, session)
  }
}

const insertAssignmentFiles = async ({
  accountId,
  assignmentSerial,
  contractId,
  enabledShowAssignmentFilesToLandlord,
  files,
  fileVisibleToLandlord,
  partnerId,
  propertyId,
  userId,
  session
}) => {
  const filesData = []
  const logsData = []
  const contractFiles = []
  const isVisibleToLandlord =
    enabledShowAssignmentFilesToLandlord && fileVisibleToLandlord
  for (const file of files) {
    const fileId = nid(17)
    const fileData = {
      _id: fileId,
      accountId,
      context: 'contract',
      contractId,
      createdBy: userId,
      directive: 'Files',
      isVisibleToLandlord,
      name: file.name,
      partnerId,
      propertyId,
      size: file.size,
      title: file.title,
      createdAt: new Date()
    }
    filesData.push(fileData)
    contractFiles.push({
      fileId,
      context: 'assignment'
    })
    const logData = {
      accountId,
      action: 'uploaded_file',
      contractId,
      context: 'contract',
      fileId,
      meta: [
        {
          field: 'assignmentSerial',
          value: assignmentSerial
        },
        {
          field: 'fileName',
          value: file.title
        }
      ],
      partnerId,
      propertyId,
      createdBy: userId,
      createdAt: new Date()
    }
    logsData.push(logData)
  }
  await fileService.createFiles(filesData, session)
  await logService.createLogs(logsData, session)
  const updatedContract = await updateContract(
    {
      _id: contractId,
      partnerId
    },
    {
      $set: {
        files: contractFiles
      }
    },
    session
  )
  if (!size(updatedContract))
    throw new CustomError(404, 'Unable to update contract')
}

export const regenerateContractEsigning = async (req) => {
  const { body = {}, session, user = {} } = req
  const { roles = [] } = user
  if (roles.includes('lambda_manager')) {
    user.partnerId = body.partnerId
    user.userId = body.userId
  }
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  appHelper.checkRequiredFields(['contractId', 'contractType'], body)
  const { contractId, contractType, esigningPdfContent } = body
  const contractInfo = await contractHelper.getAContract({
    _id: contractId,
    partnerId
  })
  if (!size(contractInfo)) throw new CustomError(404, 'Contract not found')
  await contractHelper.validateDataForRegenerateContractEsigning(
    contractType,
    contractInfo
  )
  const { event, responseData, updateData } =
    contractHelper.prepareUpdateDataForRegenerateContractEsigning(
      body,
      contractInfo
    )
  await updateContract(
    {
      _id: contractId
    },
    updateData,
    session
  )
  await updateQueueStatusForRegeneratingContract(contractId, event, session)
  if (esigningPdfContent) {
    await createAQueueToUploadContractPdfToS3AndRemoveOldPdf(
      {
        actionType: 'esigning',
        contractData: contractInfo,
        contractId,
        contractType,
        esigningPdfContent,
        partnerId,
        userId
      },
      session
    )
    await createRegenerateSigningLog(
      {
        contractInfo,
        contractType,
        partnerId,
        userId
      },
      session
    )
  }
  return responseData
}

const createRegenerateSigningLog = async (
  { contractInfo, contractType, partnerId, userId },
  session
) => {
  const visibility = ['property', 'account']
  if (contractInfo.rentalMeta?.tenantId) visibility.push('tenant')
  const logData = {
    accountId: contractInfo.accountId,
    action:
      contractType === 'assignment'
        ? 'regenerate_assignment_signing'
        : 'regenerate_lease_signing',
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    context: 'property',
    contractId: contractInfo._id,
    createdBy: userId,
    partnerId,
    propertyId: contractInfo.propertyId,
    tenantId: contractInfo.rentalMeta?.tenantId,
    visibility
  }
  await logService.createLog(logData, session)
}

const updateQueueStatusForRegeneratingContract = async (
  contractId,
  event,
  session
) => {
  const queueQuery = {
    event,
    action: 'send_notification',
    'params.collectionId': contractId,
    'params.collectionNameStr': 'contracts',
    status: 'new'
  }
  await AppQueueCollection.findOneAndUpdate(
    queueQuery,
    {
      $set: { status: 'completed' }
    },
    {
      new: true,
      runValidators: true,
      session
    }
  )
}
export const addLeaseDataInContractAndCreateRentInvoice = async (
  params = {},
  rentalMetaData = {},
  session
) => {
  const {
    addons = [],
    contractId,
    contractInfo,
    partnerId,
    propertyId,
    userId
  } = params

  const updateData = {
    hasRentalContract: true,
    rentalMeta: rentalMetaData
  }

  if (rentalMetaData.status !== 'in_progress') {
    updateData.status = rentalMetaData.status
  }

  if (size(addons)) {
    const oldAddons = contractInfo.addons ? contractInfo.addons : []
    updateData.addons = [...oldAddons, ...addons]
  }

  if (size(params.fileIds)) {
    updateData.files = params.fileIds.map((fileId) => ({
      context: 'lease',
      fileId
    }))
  }
  if (!contractInfo?.leaseSerial) {
    updateData.leaseSerial = await counterService.incrementCounter(
      'lease-' + propertyId,
      session
    )
  }
  console.log('Creating lease, updateData:', updateData)
  const updatedContract = await updateContract(
    {
      _id: contractId,
      partnerId,
      propertyId,
      $or: [{ 'rentalMeta.status': 'new' }, { rentalMeta: { $exists: false } }]
    },
    { $set: updateData },
    session
  )
  if (!size(updatedContract)) {
    throw new CustomError(404, 'Contract not available for lease create')
  }
  const updatedRentalMeta = updatedContract?.rentalMeta

  if (
    updatedRentalMeta &&
    (!updatedRentalMeta.enabledLeaseEsigning ||
      (updatedRentalMeta.enabledLeaseEsigning &&
        updatedContract.isAllSignCompleted()))
  ) {
    await appQueueService.createAppQueueForCreateRentInvoice(
      {
        contractId: updatedContract._id,
        enabledNotification: updatedContract.rentalMeta?.enabledNotification,
        partnerId: updatedContract.partnerId,
        today: new Date(),
        userId
      },
      session
    )
  }
  return updatedContract
}

const createAssignmentForDirectPartner = async (params, session) => {
  const { partnerId, propertyId, userId } = params
  let { contractInfo = {} } = params

  if (!size(contractInfo))
    contractInfo =
      (await contractHelper.getAContract({
        partnerId,
        propertyId
      })) || {}

  const contractData = {
    accountId: contractInfo.accountId,
    assignmentSerial: await counterService.incrementCounter(
      'assignment-' + propertyId,
      session
    ),
    agentId: contractInfo.agentId,
    branchId: contractInfo.branchId,
    createdBy: userId,
    hasBrokeringContract: false,
    hasRentalContract: false,
    hasRentalManagementContract: false,
    partnerId,
    propertyId,
    status: 'upcoming',
    rentalMeta: {
      status: 'new'
    }
  }

  const contract = await createContract(contractData, session)
  return contract
}

const createLogForTerminateAnAssignment = async (body, contract, session) => {
  const logData = await contractHelper.prepareLogDataForTerminateAnAssignment(
    body,
    contract
  )
  const log = await logService.createLog(logData, session)
  return log
}

const updatePropertyInfoForTerminateAssignment = async (contract, session) => {
  const { partnerId, propertyId } = contract

  const listing = await listingHelper.getAListing({ _id: propertyId }, session)
  if (listing && !(await listing.getActiveOrUpcomingContract())) {
    // Update property information
    const updateData = { $set: { hasAssignment: false } }
    const updatedProperty = await listingService.updateAListing(
      { _id: propertyId, partnerId },
      updateData,
      session
    )
    await propertyAfterUpdateProcess(updatedProperty, session)
  }
  return listing
}

export const terminateAssignment = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'propertyId'])
  const { body, session } = req
  const assignmentUpdateData =
    await contractHelper.prepareTerminateAssignmentUpdateData(body)

  const { contractId, partnerId, propertyId } = body
  const updatedAssignment = await updateContract(
    { _id: contractId, partnerId, propertyId },
    assignmentUpdateData,
    session
  )
  // Create logs
  await createLogForTerminateAnAssignment(body, updatedAssignment, session)
  await updatePropertyInfoForTerminateAssignment(updatedAssignment, session)
  return updatedAssignment
}

export const updateContractAddon = async (req) => {
  const { body = {}, session } = req
  appHelper.validatePartnerAppRequestData(req, [
    'addonId',
    'contractId',
    'fieldName'
  ])
  const { addonId, contractId } = body
  const previousContract = await contractHelper.getContractById(contractId)
  if (!size(previousContract)) throw new CustomError(404, 'Contract not found')

  const previousAddon = previousContract.addons.find(
    (addon) => addon.addonId === addonId
  )
  if (!previousAddon) throw new CustomError(404, 'Addon not found')
  const addonInfo = await addonHelper.getAddonById(addonId)
  if (!size(addonInfo)) throw new CustomError(404, 'Addon not found')
  body.addonInfo = addonInfo
  body.previousAddon = previousAddon
  body.contractType = previousContract.hasRentalContract
    ? 'lease'
    : 'assignment'

  const { data, query } =
    await contractHelper.prepareQueryAndDataForUpdateLeaseAddon(
      body,
      previousContract
    )
  const updatedContract = await updateContract(query, { $set: data }, session)
  if (size(updatedContract)) {
    body.updatedContract = updatedContract
    const logData = contractHelper.prepareLogDataForUpdateLeaseAddon(body)
    await logService.createLog(logData, session)
    return updatedContract.addons.find((addon) => addon.addonId === addonId)
  } else throw new CustomError(400, 'Addon not updated')
}

export const addAddonInContract = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['addonId', 'contractId'])
  const { body = {}, session } = req
  const { contractId } = body
  const { data, newAddonData } =
    await contractHelper.validateAndPrepareDataForAddAddonInContract(body)
  const updatedContract = await updateContract(
    { _id: contractId },
    data,
    session
  )
  if (!size(updatedContract)) throw new CustomError(400, 'Failed to add Addon')
  const logData = contractHelper.prepareLogDataForAddAddonInContract(
    body,
    updatedContract
  )
  await logService.createLog(logData, session)
  return newAddonData
}

export const addHistoryToContractForCommissionChanges = async (
  params,
  session
) => {
  const { contract, newCommission, oldCommission, partnerSetting } = params
  const history = []
  const names = []
  const name = 'commissions'
  const previouslyUpdatedAt = contractHelper.getPreviouslyUpdatedDate(
    contract,
    name
  )
  const commissionChangeLog = {
    name,
    oldValue: oldCommission || 0,
    oldUpdatedAt: previouslyUpdatedAt
      ? previouslyUpdatedAt
      : await appHelper.getActualDate(
          partnerSetting,
          false,
          contract.createdAt
        ),
    newValue: newCommission || 0,
    newUpdatedAt: await appHelper.getActualDate(partnerSetting, false)
  }

  names.push(name)
  history.push(commissionChangeLog)

  const totalIncomeChangeLog = await contractHelper.prepareTotalIncomeChangeLog(
    contract,
    {
      oldCommissionTotal: oldCommission,
      newCommissionTotal: newCommission
    },
    partnerSetting
  )

  if (size(totalIncomeChangeLog)) {
    history.push(totalIncomeChangeLog)
    names.push(totalIncomeChangeLog.name)
  }

  await updateContractForChangeLog(
    {
      contract,
      history,
      names
    },
    session
  )
}

const updateContractForChangeLog = async (params, session) => {
  const { contract, names } = params
  let { history = [] } = params
  if (contract && contract._id && size(contract.history)) {
    if (!names.includes('commissions')) {
      const oldHistory = contract.history.filter(
        (item) => !names.includes(item.name)
      )
      history = [...oldHistory, ...history]
    }
  }
  if (contract?._id) {
    await updateContract({ _id: contract._id }, { $set: { history } }, session)
  }
}

export const dailyAssignmentEsigningReminderHelper = async (
  option,
  session
) => {
  const { skip = 0, limit = 100 } = option
  const pipeline = pipelineForAssignmentEsigningReminder(skip, limit)
  const contractData = await ContractCollection.aggregate(pipeline)
  console.log('Contract data found to send e signing reminder', contractData)
  try {
    const contractIds = contractData[0]?.contractIds || []
    const queueInfo = contractData[0]?.queueData || []
    const today = (await appHelper.getActualDate('', true, new Date())).toDate()
    if (contractIds.length > 0) {
      const updateData = await updateContracts(
        {
          _id: {
            $in: contractIds
          }
        },
        {
          $set: {
            assignmentESigningReminderToLandlordSentAt: today,
            newField: true
          }
        },
        session
      )
      console.log('Log after update', updateData)
      const appQueues = []
      for (let i = 0; i < queueInfo.length; i++) {
        const singleAppQueue = {
          event: 'send_assignment_esigning_reminder_notice_to_landlord',
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: queueInfo[i].partnerId,
            collectionId: queueInfo[i].contractId,
            collectionNameStr: 'contracts'
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
      }
      await appQueueService.insertAppQueueItems(appQueues, session)
      return {
        msg: 'Assignment e-signing reminder send success'
      }
    } else {
      return {
        msg: 'No Contract Found'
      }
    }
  } catch (e) {
    console.log('Assignment e-signing reminder send failed', e)
    throw new Error('Assignment e-signing reminder send failed')
  }
}

export const pipelineForAssignmentEsigningReminder = (skip, limit) => [
  {
    $match: {
      enabledEsigning: true,
      status: { $ne: 'closed' },
      landlordAssignmentSigningStatus: { $exists: true },
      'landlordAssignmentSigningStatus.signed': false
    }
  },
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: '$partnerSettings'
  },
  {
    $addFields: {
      esignReminderNoticeDays: {
        $ifNull: [
          '$partnerSettings.assignmentSettings.esignReminderNoticeDays',
          1
        ]
      },
      today: {
        $toDate: {
          $dateToString: {
            date: new Date(),
            timezone: '$partnerSettings.dateTimeSettings.timezone'
          }
        }
      }
    }
  },
  {
    $match: {
      esignReminderNoticeDays: { $gte: 1, $lte: 45 },
      'partnerSettings.assignmentSettings.enabledAssignmentEsignReminder': true
    }
  },
  {
    $addFields: {
      actualDate: {
        $dateSubtract: {
          startDate: '$today',
          unit: 'day',
          amount: '$esignReminderNoticeDays',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isActualaDateBefore: {
        $cond: {
          if: { $lt: ['$createdAt', '$actualDate'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: {
      isActualaDateBefore: true
    }
  },
  {
    $addFields: {
      lastReminderToLandlord: {
        $cond: {
          if: {
            $ifNull: ['$assignmentESigningReminderToLandlordSentAt', false]
          },
          then: '$assignmentESigningReminderToLandlordSentAt',
          else: '$createdAt'
        }
      }
    }
  },
  {
    $addFields: {
      nextReminderDate: {
        $dateAdd: {
          startDate: '$lastReminderToLandlord',
          unit: 'day',
          amount: '$esignReminderNoticeDays'
          //timezone: "+08:00"
        }
      }
    }
  },
  {
    $skip: skip
  },
  {
    $limit: limit
  },
  {
    $group: {
      _id: null,
      queueData: {
        $push: {
          $cond: {
            if: { $lte: ['$nextReminderDate', '$today'] },
            then: {
              contractId: '$_id',
              partnerId: '$partnerId'
            },
            else: '$$REMOVE'
          }
        }
      },
      contractIds: {
        $push: {
          $cond: {
            if: { $lte: ['$nextReminderDate', '$today'] },
            then: '$_id',
            else: '$$REMOVE'
          }
        }
      }
    }
  }
]

export const movingInSigningReminderHelper = async (option, session) => {
  const { skip = 0, limit = 100 } = option

  const pipeline = pipelineForMovingInSigningReminder(skip, limit)
  const propertyItemData = await propertyItemHelper.propertyItemAggregateHelper(
    pipeline
  )
  console.log(
    `Valid property Items data found to send lease moving in e signing reminder. #${size(
      propertyItemData
    )}`,
    propertyItemData
  )

  if (propertyItemData.length < 1) return { msg: 'No moving in items found' }

  const appQueues = []

  for (const elem of propertyItemData) {
    console.log(`=== elem`, elem)
    if (size(elem.contract)) {
      console.log(`=== MovingInId for reminder`, elem._id)
      const today = moment()
      const nextMoveInToTenantReminderDate = moment(
        elem.nextMoveInToTenantReminderDate
      )

      const nextMoveInToAgentReminderDate = elem.nextMoveInToAgentReminderDate
        ? moment(elem.nextMoveInToAgentReminderDate)
        : null
      const nextMoveInToLandlordReminderDate =
        elem.nextMoveInToLandlordReminderDate
          ? moment(elem.nextMoveInToLandlordReminderDate)
          : null
      console.log(
        `Today: ${today}, nextMoveInTenantReminderDate: ${nextMoveInToTenantReminderDate}, nextMoveInAgentReminderDate: ${nextMoveInToAgentReminderDate}, nextMoveInLandlordReminderDate: ${nextMoveInToLandlordReminderDate}`
      )

      const updatingData = {}

      if (
        !elem.isTenantSignedMovingIn &&
        nextMoveInToTenantReminderDate.isSameOrBefore(today)
      ) {
        console.log('=== Creating Q to send tenant moving in reminder ===')
        const event = 'send_move_in_esigning_reminder_notice_to_tenant'

        const singleAppQueue = {
          event,
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: elem.partnerId,
            collectionId: elem.contractId,
            collectionNameStr: 'contracts',
            options: { movingId: elem._id }
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
        console.log(
          `Creating Q to send moving in reminder for ${event}, movingId ${elem._id}`
        )
        updatingData.eSignReminderToTenantForMoveInSentAt = today
      }

      let event
      if (
        elem.isBrokerPartner &&
        elem.isAgentSignedMovingIn === false &&
        nextMoveInToAgentReminderDate.isSameOrBefore(today)
      ) {
        console.log('=== Creating Q to send agent moving in reminder  ===')
        event = 'send_move_in_esigning_reminder_notice_to_agent'
        updatingData.eSignReminderToAgentForMoveInSentAt = today
      } else if (
        elem.isDirectPartner &&
        elem.isLandlordSignedMovingIn === false &&
        nextMoveInToLandlordReminderDate.isSameOrBefore(today)
      ) {
        console.log('=== Creating Q to send landlord moving in reminder  ===')
        event = 'send_move_in_esigning_reminder_notice_to_landlord'
        updatingData.eSignReminderToLandlordForMoveInSentAt = today
      }
      if (event) {
        const singleAppQueue = {
          event,
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: elem.partnerId,
            collectionId: elem.contractId,
            collectionNameStr: 'contracts',
            options: { movingId: elem._id }
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
        console.log(
          `Creating Q to send moving in reminder for ${event}, movingId ${elem._id}`
        )
      }
      console.log(
        '=== Updating Data for movingInId: ',
        elem._id,
        'Data: ',
        updatingData
      )
      if (size(updatingData)) {
        const updatedRes = await propertyItemService.updateAPropertyItem(
          { _id: elem._id },
          { $set: updatingData },
          session
        )
        console.log('Updated propertyItem', updatedRes._id)
      }
    }
  }

  try {
    const createdQRes = await appQueueService.insertAppQueueItems(
      appQueues,
      session
    )
    console.log(`=== # ${size(createdQRes)} createdQRes: `, createdQRes)

    return { msg: `Moving in reminder send success #${size(appQueues)}` }
  } catch (err) {
    console.log('Error while sending reminder for moving in', err)
    throw new Error('Error while sending reminder for moving in')
  }
}

export const movingOutSigningReminderHelper = async (option, session) => {
  const { skip = 0, limit = 100 } = option

  const pipeline = pipelineForMovingOutSigningReminder(skip, limit)
  const propertyItemData = await propertyItemHelper.propertyItemAggregateHelper(
    pipeline
  )
  console.log(
    `Valid property Items data found to send lease moving out e signing reminder. #${size(
      propertyItemData
    )}`,
    propertyItemData
  )
  if (propertyItemData.length < 1) return { msg: 'No moving out items found' }

  const appQueues = []

  for (const elem of propertyItemData) {
    console.log(`=== elem`, elem)
    if (size(elem.contract)) {
      console.log(`=== MovingOutId for reminder`, elem._id)
      const today = moment()

      const nextMoveOutToTenantReminderDate = moment(
        elem.nextMoveOutToTenantReminderDate
      )
      const nextMoveOutToAgentReminderDate = elem.nextMoveOutToAgentReminderDate
        ? moment(elem.nextMoveOutToAgentReminderDate)
        : null
      const nextMoveOutToLandlordReminderDate =
        elem.nextMoveOutToLandlordReminderDate
          ? moment(elem.nextMoveOutToLandlordReminderDate)
          : null
      console.log(
        `Today: ${today}, nextMoveOutToTenantReminderDate: ${nextMoveOutToTenantReminderDate}, nextMoveOutToAgentReminderDate: ${nextMoveOutToAgentReminderDate}, nextMoveOutToLandlordReminderDate: ${nextMoveOutToLandlordReminderDate}`
      )

      const updatingData = {}

      if (
        !elem.isTenantSignedMovingOut &&
        nextMoveOutToTenantReminderDate.isSameOrBefore(today)
      ) {
        console.log('=== Creating Q to send tenant moving out reminder ===')
        const event = 'send_move_out_esigning_reminder_notice_to_tenant'

        const singleAppQueue = {
          event,
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: elem.partnerId,
            collectionId: elem.contractId,
            collectionNameStr: 'contracts',
            options: { movingId: elem._id }
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
        console.log(
          `Creating Q to send moving out reminder for ${event}, movingId ${elem._id}`
        )
        updatingData.eSignReminderToTenantForMoveOutSentAt = today
      }

      let event
      if (
        elem.isBrokerPartner &&
        elem.isAgentSignedMovingOut === false &&
        nextMoveOutToAgentReminderDate.isSameOrBefore(today)
      ) {
        console.log('=== Creating Q to send agent moving out reminder  ===')
        event = 'send_move_out_esigning_reminder_notice_to_agent'
        updatingData.eSignReminderToAgentForMoveOutSentAt = today
      } else if (
        elem.isDirectPartner &&
        elem.isLandlordSignedMovingOut === false &&
        nextMoveOutToLandlordReminderDate.isSameOrBefore(today)
      ) {
        console.log('=== Creating Q to send landlord moving out reminder  ===')
        event = 'send_move_out_esigning_reminder_notice_to_landlord'
        updatingData.eSignReminderToLandlordForMoveOutSentAt = today
      }
      if (event) {
        const singleAppQueue = {
          event,
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: elem.partnerId,
            collectionId: elem.contractId,
            collectionNameStr: 'contracts',
            options: { movingId: elem._id }
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
        console.log(
          `Creating Q to send moving out reminder for ${event}, movingId ${elem._id}`
        )
      }

      console.log(
        '=== Updating Data for movingOutId: ',
        elem._id,
        'Data: ',
        updatingData
      )
      if (size(updatingData)) {
        const updatedRes = await propertyItemService.updateAPropertyItem(
          { _id: elem._id },
          { $set: updatingData },
          session
        )
        console.log('Updated propertyItem', updatedRes._id)
      }
    }
  }

  try {
    const createdQRes = await appQueueService.insertAppQueueItems(
      appQueues,
      session
    )
    console.log(`=== # ${size(createdQRes)} createdQRes: `, createdQRes)

    return { msg: `Moving out reminder send success #${size(appQueues)}` }
  } catch (e) {
    console.log('Error while sending reminder for moving out', e)
    throw new Error('Error while sending reminder for moving out')
  }
}

const pipelineForMovingInSigningReminder = (skip, limit) => [
  {
    $match: {
      type: 'in',
      isEsigningInitiate: true,
      contractId: { $exists: true },
      $or: [
        {
          tenantSigningStatus: { $exists: true, $elemMatch: { signed: false } }
        },
        {
          agentSigningStatus: { $exists: true },
          'agentSigningStatus.signed': false
        },
        {
          landlordSigningStatus: { $exists: true },
          'landlordSigningStatus.signed': false
        }
      ]
    }
  },
  {
    $sort: { createdAt: 1 }
  },
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: { path: '$partner', preserveNullAndEmptyArrays: true }
  },
  {
    $match: { 'partner.isActive': true }
  },
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'contract'
    }
  },
  {
    $unwind: { path: '$contract', preserveNullAndEmptyArrays: true }
  },
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  },
  {
    $addFields: {
      enabledMoveInEsignReminder: {
        $ifNull: [
          '$partnerSettings.propertySettings.enabledMoveInEsignReminder',
          false
        ]
      },
      esignReminderNoticeDaysTenant: {
        $ifNull: [
          '$partnerSettings.propertySettings.esignReminderNoticeDaysForMoveIn',
          1
        ]
      },
      today: {
        $dateToString: {
          date: new Date(),
          format: '%Y-%m-%dT%H:%M:%S',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      actualDate: {
        $dateSubtract: {
          startDate: new Date(),
          unit: 'day',
          amount: '$esignReminderNoticeDaysTenant',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isActualDateBefore: {
        $cond: [{ $lt: ['$esigningInitiatedAt', '$actualDate'] }, true, false]
      }
    }
  },
  {
    $match: {
      'contract.status': { $ne: 'closed' },
      esignReminderNoticeDaysTenant: { $gte: 1, $lte: 45 },
      enabledMoveInEsignReminder: true,
      isActualDateBefore: true
    }
  },
  {
    $skip: skip
  },
  {
    $limit: limit
  },
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: '$partner'
  },
  {
    $addFields: {
      isBrokerPartner: {
        $cond: {
          if: { $eq: ['$partner.accountType', 'broker'] },
          then: true,
          else: false
        }
      },
      isDirectPartner: {
        $cond: {
          if: { $eq: ['$partner.accountType', 'direct'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $addFields: {
      isAgentSignedMovingIn: {
        $ifNull: ['$agentSigningStatus.signed', false]
      },
      isLandlordSignedMovingIn: {
        $ifNull: ['$landlordSigningStatus.signed', false]
      },
      signedTenant: {
        $filter: {
          input: '$tenantSigningStatus',
          as: 'item',
          cond: { $eq: ['$$item.signed', false] }
        }
      }
    }
  },
  {
    $addFields: {
      signedTenant: '$$REMOVE',
      isTenantSignedMovingIn: {
        $cond: {
          if: {
            $gt: [
              {
                $size: '$signedTenant'
              },
              0
            ]
          },
          then: false,
          else: true
        }
      },
      lastMoveInToTenantReminderDate: {
        $ifNull: [
          '$eSignReminderToTenantForMoveInSentAt',
          '$esigningInitiatedAt'
        ]
      },
      lastMoveInToAgentReminderDate: {
        $cond: [
          '$isBrokerPartner',
          {
            $ifNull: [
              '$eSignReminderToAgentForMoveInSentAt',
              '$esigningInitiatedAt'
            ]
          },
          false
        ]
      },
      lastMoveInToLandlordReminderDate: {
        $cond: [
          '$isDirectPartner',
          {
            $ifNull: [
              '$eSignReminderToLandlordForMoveInSentAt',
              '$esigningInitiatedAt'
            ]
          },
          false
        ]
      }
    }
  },
  {
    $addFields: {
      nextMoveInToTenantReminderDate: {
        $dateAdd: {
          startDate: '$lastMoveInToTenantReminderDate',
          unit: 'day',
          amount: '$esignReminderNoticeDaysTenant'
        }
      },
      nextMoveInToAgentReminderDate: {
        $cond: [
          '$lastMoveInToAgentReminderDate',
          {
            $dateAdd: {
              startDate: '$lastMoveInToAgentReminderDate',
              unit: 'day',
              amount: '$esignReminderNoticeDaysTenant'
            }
          },
          false
        ]
      },
      nextMoveInToLandlordReminderDate: {
        $cond: [
          '$lastMoveInToLandlordReminderDate',
          {
            $dateAdd: {
              startDate: '$lastMoveInToLandlordReminderDate',
              unit: 'day',
              amount: '$esignReminderNoticeDaysTenant'
            }
          },
          false
        ]
      }
    }
  },

  {
    $addFields: {
      isNextMoveInTenantReminderDateBeforeToday: {
        $cond: {
          if: { $lte: ['$nextMoveInToTenantReminderDate', new Date()] },
          then: true,
          else: false
        }
      },
      isNextMoveInToAgentReminderDateBeforeToday: {
        $cond: {
          if: { $lte: ['$nextMoveInToAgentReminderDate', new Date()] },
          then: true,
          else: false
        }
      },
      isNextMoveInToLandlordReminderDateBeforeToday: {
        $cond: {
          if: { $lte: ['$nextMoveInToLandlordReminderDate', new Date()] },
          then: true,
          else: false
        }
      }
    }
  },

  {
    $project: {
      tenantSigningStatus: 1,
      agentSigningStatus: 1,
      landlordSigningStatus: 1,
      contractId: 1,
      partnerId: 1,
      partner: 1,
      contract: 1,
      eSignReminderToTenantForMoveInSentAt: 1,
      eSignReminderToAgentForMoveInSentAt: 1,
      eSignReminderToLandlordForMoveInSentAt: 1,
      esigningInitiatedAt: 1,
      esignReminderNoticeDaysTenant: 1,
      today: 1,
      isActualDateBefore: 1,
      actualDate: 1,
      isAgentSignedMovingIn: 1,
      isLandlordSignedMovingIn: 1,
      isTenantSignedMovingIn: 1,
      lastMoveInToTenantReminderDate: 1,
      lastMoveInToAgentReminderDate: 1,
      lastMoveInToLandlordReminderDate: 1,
      nextMoveInToTenantReminderDate: 1,
      nextMoveInToAgentReminderDate: 1,
      nextMoveInToLandlordReminderDate: 1,
      isNextMoveInTenantReminderDateBeforeToday: 1,
      isNextMoveInToAgentReminderDateBeforeToday: 1,
      isNextMoveInToLandlordReminderDateBeforeToday: 1,
      isBrokerPartner: 1,
      isDirectPartner: 1
    }
  }
]

const pipelineForMovingOutSigningReminder = (skip, limit) => [
  {
    $match: {
      type: 'out',
      isEsigningInitiate: true,
      contractId: { $exists: true },
      $or: [
        {
          tenantSigningStatus: { $exists: true, $elemMatch: { signed: false } }
        },
        {
          agentSigningStatus: { $exists: true },
          'agentSigningStatus.signed': false
        },
        {
          landlordSigningStatus: { $exists: true },
          'landlordSigningStatus.signed': false
        }
      ]
    }
  },
  {
    $sort: { createdAt: 1 }
  },
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: { path: '$partner', preserveNullAndEmptyArrays: true }
  },
  {
    $match: { 'partner.isActive': true }
  },
  {
    $lookup: {
      from: 'contracts',
      localField: 'contractId',
      foreignField: '_id',
      as: 'contract'
    }
  },
  {
    $unwind: { path: '$contract', preserveNullAndEmptyArrays: true }
  },
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  },
  {
    $addFields: {
      enabledMoveOutEsignReminder: {
        $ifNull: [
          '$partnerSettings.propertySettings.enabledMoveOutEsignReminder',
          false
        ]
      },
      esignReminderNoticeDaysTenant: {
        $ifNull: [
          '$partnerSettings.propertySettings.esignReminderNoticeDaysForMoveOut',
          1
        ]
      },
      today: {
        $dateToString: {
          date: new Date(),
          format: '%Y-%m-%dT%H:%M:%S',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      actualDate: {
        $dateSubtract: {
          startDate: new Date(),
          unit: 'day',
          amount: '$esignReminderNoticeDaysTenant',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isActualDateBefore: {
        $cond: [{ $lt: ['$esigningInitiatedAt', '$actualDate'] }, true, false]
      }
    }
  },
  {
    $match: {
      esignReminderNoticeDaysTenant: { $gte: 1, $lte: 45 },
      enabledMoveOutEsignReminder: true,
      isActualDateBefore: true
    }
  },
  {
    $skip: skip
  },
  {
    $limit: limit
  },
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: '$partner'
  },
  {
    $addFields: {
      isBrokerPartner: {
        $cond: {
          if: { $eq: ['$partner.accountType', 'broker'] },
          then: true,
          else: false
        }
      },
      isDirectPartner: {
        $cond: {
          if: { $eq: ['$partner.accountType', 'direct'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $addFields: {
      isAgentSignedMovingOut: {
        $ifNull: ['$agentSigningStatus.signed', false]
      },
      isLandlordSignedMovingOut: {
        $ifNull: ['$landlordSigningStatus.signed', false]
      },
      signedTenant: {
        $filter: {
          input: '$tenantSigningStatus',
          as: 'item',
          cond: { $eq: ['$$item.signed', false] }
        }
      }
    }
  },
  {
    $addFields: {
      signedTenant: '$$REMOVE',
      isTenantSignedMovingOut: {
        $cond: {
          if: {
            $gt: [
              {
                $size: '$signedTenant'
              },
              0
            ]
          },
          then: false,
          else: true
        }
      },
      lastMoveOutToTenantReminderDate: {
        $ifNull: [
          '$eSignReminderToTenantForMoveOutSentAt',
          '$esigningInitiatedAt'
        ]
      },
      lastMoveOutToAgentReminderDate: {
        $cond: [
          '$isBrokerPartner',
          {
            $ifNull: [
              '$eSignReminderToAgentForMoveOutSentAt',
              '$esigningInitiatedAt'
            ]
          },
          false
        ]
      },
      lastMoveOutToLandlordReminderDate: {
        $cond: [
          '$isDirectPartner',
          {
            $ifNull: [
              '$eSignReminderToLandlordForMoveOutSentAt',
              '$esigningInitiatedAt'
            ]
          },
          false
        ]
      }
    }
  },
  {
    $addFields: {
      nextMoveOutToTenantReminderDate: {
        $dateAdd: {
          startDate: '$lastMoveOutToTenantReminderDate',
          unit: 'day',
          amount: '$esignReminderNoticeDaysTenant'
        }
      },
      nextMoveOutToAgentReminderDate: {
        $cond: [
          '$lastMoveOutToAgentReminderDate',
          {
            $dateAdd: {
              startDate: '$lastMoveOutToAgentReminderDate',
              unit: 'day',
              amount: '$esignReminderNoticeDaysTenant'
            }
          },
          false
        ]
      },
      nextMoveOutToLandlordReminderDate: {
        $cond: [
          '$lastMoveOutToLandlordReminderDate',
          {
            $dateAdd: {
              startDate: '$lastMoveOutToLandlordReminderDate',
              unit: 'day',
              amount: '$esignReminderNoticeDaysTenant'
            }
          },
          false
        ]
      }
    }
  },
  {
    $addFields: {
      isNextMoveOutTenantReminderDateBeforeToday: {
        $cond: {
          if: { $lte: ['$nextMoveOutToTenantReminderDate', new Date()] },
          then: true,
          else: false
        }
      },
      isNextMoveOutToAgentReminderDateBeforeToday: {
        $cond: {
          if: { $lte: ['$nextMoveOutToAgentReminderDate', new Date()] },
          then: true,
          else: false
        }
      },
      isNextMoveOutToLandlordReminderDateBeforeToday: {
        $cond: {
          if: { $lte: ['$nextMoveOutToLandlordReminderDate', new Date()] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $project: {
      tenantSigningStatus: 1,
      agentSigningStatus: 1,
      landlordSigningStatus: 1,
      contractId: 1,
      partnerId: 1,
      partner: 1,
      contract: 1,
      eSignReminderToTenantForMoveOutSentAt: 1,
      eSignReminderToAgentForMoveOutSentAt: 1,
      eSignReminderToLandlordForMoveOutSentAt: 1,
      esigningInitiatedAt: 1,
      esignReminderNoticeDaysTenant: 1,
      today: 1,
      isActualDateBefore: 1,
      actualDate: 1,
      isAgentSignedMovingOut: 1,
      isLandlordSignedMovingOut: 1,
      isTenantSignedMovingOut: 1,
      lastMoveOutToTenantReminderDate: 1,
      lastMoveOutToAgentReminderDate: 1,
      lastMoveOutToLandlordReminderDate: 1,
      nextMoveOutToTenantReminderDate: 1,
      nextMoveOutToAgentReminderDate: 1,
      nextMoveOutToLandlordReminderDate: 1,
      isNextMoveOutTenantReminderDateBeforeToday: 1,
      isNextMoveOutToAgentReminderDateBeforeToday: 1,
      isNextMoveOutToLandlordReminderDateBeforeToday: 1,
      isBrokerPartner: 1,
      isDirectPartner: 1
    }
  }
]

export const sendLeaseESigningReminderHelper = async (option, session) => {
  const { skip = 0, limit = 100 } = option
  const pipeline = pipelineForSendingLeaseESigningReminderPipeline(skip, limit)
  const contractData = (await ContractCollection.aggregate(pipeline)) || []
  console.log(
    `Valid contract data found to send lease e signing reminder. #${size(
      contractData
    )}`,
    contractData
  )

  const appQueues = []

  if (contractData.length < 1) return { msg: 'No Contract Found' }

  try {
    for (const elem of contractData) {
      console.log(`=== elem`, elem)
      console.log(`=== ContractId for lease reminder`, elem._id)

      const today = new Date()
      const nextLeaseReminderToTenant = moment(elem.nextLeaseReminderToTenant)
      const nextLeaseReminderToLandlord = moment(
        elem.nextLeaseReminderToLandlord
      )
      console.log(
        `Today: ${today}, nextLeaseReminderToTenant: ${nextLeaseReminderToTenant}, nextLeaseReminderToLandlord: ${nextLeaseReminderToLandlord}`
      )

      const updatingData = {}
      if (
        elem.isAllTenantLeaseSigned === false &&
        nextLeaseReminderToTenant.isSameOrBefore(today)
      ) {
        console.log(
          '=== Creating Q to send tenant lease reminder. contractId',
          elem._id
        )
        const singleAppQueue = {
          event: 'send_lease_esigning_reminder_notice_to_tenant',
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: elem.partnerId,
            collectionId: elem._id,
            collectionNameStr: 'contracts'
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
        updatingData['rentalMeta.eSignReminderToTenantForLeaseSendAt'] = today
      }
      if (
        elem.isAllTenantLeaseSigned === true &&
        elem.isLandlordLeaseSigned === false &&
        nextLeaseReminderToLandlord.isSameOrBefore(today)
      ) {
        console.log(
          '=== Creating Q to send landlord lease reminder. contractId',
          elem._id
        )
        const singleAppQueue = {
          event: 'send_lease_esigning_reminder_notice_to_landlord',
          action: 'send_notification',
          destination: 'notifier',
          params: {
            partnerId: elem.partnerId,
            collectionId: elem._id,
            collectionNameStr: 'contracts'
          },
          priority: 'regular'
        }
        appQueues.push(singleAppQueue)
        updatingData['rentalMeta.eSignReminderToLandlordForLeaseSendAt'] = today
      }
      console.log('=== Contract updating Data', updatingData)
      if (size(updatingData)) {
        const response = await updateContract(
          { _id: elem._id },
          {
            $set: updatingData
          },
          session
        )
        console.log(
          '=== Updated contract successfully. contractId: ',
          response._id
        )
      }
    }

    const createdQRes = await appQueueService.insertAppQueueItems(
      appQueues,
      session
    )
    console.log(`=== #${createdQRes} createdQRes: `, createdQRes)
    return {
      msg: `Lease e-signing reminder send success #${size(createdQRes)}`
    }
  } catch (err) {
    console.log('Error while sending reminder for lease e signing', err)
    throw new Error('Error while sending reminder for lease e signing')
  }
}

const pipelineForSendingLeaseESigningReminderPipeline = (skip, limit) => [
  {
    $match: {
      'rentalMeta.enabledLeaseEsigning': true,
      status: { $ne: 'closed' },
      $or: [
        {
          'rentalMeta.landlordLeaseSigningStatus': { $exists: true },
          'rentalMeta.landlordLeaseSigningStatus.signed': false
        },
        {
          'rentalMeta.tenantLeaseSigningStatus': { $exists: true },
          'rentalMeta.tenantLeaseSigningStatus.signed': false
        }
      ]
    }
  },
  { $sort: { createdAt: 1 } },
  {
    $lookup: {
      from: 'partners',
      localField: 'partnerId',
      foreignField: '_id',
      as: 'partner'
    }
  },
  {
    $unwind: { path: '$partner', preserveNullAndEmptyArrays: true }
  },
  {
    $match: { 'partner.isActive': true }
  },
  {
    $lookup: {
      from: 'partner_settings',
      localField: 'partnerId',
      foreignField: 'partnerId',
      as: 'partnerSettings'
    }
  },
  {
    $unwind: { path: '$partnerSettings', preserveNullAndEmptyArrays: true }
  },
  {
    $addFields: {
      enabledLeaseEsignReminder: {
        $ifNull: [
          '$partnerSettings.leaseSetting.enabledLeaseESigningReminder',
          false
        ]
      },
      esignReminderNoticeDays: {
        $ifNull: ['$partnerSettings.leaseSetting.esignReminderNoticeDays', 1]
      },
      today: {
        $dateToString: {
          date: new Date(),
          format: '%Y-%m-%dT%H:%M:%S',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $match: {
      enabledLeaseEsignReminder: true,
      esignReminderNoticeDays: { $gte: 1, $lte: 45 }
    }
  },

  {
    $addFields: {
      actualDate: {
        $dateSubtract: {
          startDate: new Date(),
          unit: 'day',
          amount: '$esignReminderNoticeDays',
          timezone: '$partnerSettings.dateTimeSettings.timezone'
        }
      }
    }
  },
  {
    $addFields: {
      isActualDateBefore: {
        $cond: {
          if: { $lte: ['$rentalMeta.createdAt', '$actualDate'] },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $match: { isActualDateBefore: true }
  },
  {
    $addFields: {
      tenantLeaseSigningStatus: {
        $cond: {
          if: {
            $ifNull: ['$rentalMeta.tenantLeaseSigningStatus', false]
          },
          then: '$rentalMeta.tenantLeaseSigningStatus',
          else: []
        }
      },
      landlordLeaseSigningStatus: {
        $cond: {
          if: {
            $ifNull: ['$rentalMeta.landlordLeaseSigningStatus', false]
          },
          then: '$rentalMeta.landlordLeaseSigningStatus',
          else: {}
        }
      },
      isLandlordLeaseSigned: {
        $cond: {
          if: {
            $ifNull: ['$rentalMeta.landlordLeaseSigningStatus.signed', false]
          },
          then: true,
          else: false
        }
      }
    }
  },
  {
    $addFields: {
      isNeedTenantLeaseSigning: {
        $filter: {
          input: '$tenantLeaseSigningStatus',
          as: 'item',
          cond: { $ne: ['$$item.signed', true] }
        }
      }
    }
  },
  {
    $addFields: {
      isAllTenantLeaseSigned: {
        $cond: {
          if: {
            $gt: [
              {
                $size: '$isNeedTenantLeaseSigning'
              },
              0
            ]
          },
          then: false,
          else: true
        }
      },
      lastLeaseReminderToTenant: {
        $cond: {
          if: {
            $ifNull: ['$rentalMeta.eSignReminderToTenantForLeaseSendAt', false]
          },
          then: '$rentalMeta.eSignReminderToTenantForLeaseSendAt',
          else: '$rentalMeta.createdAt'
        }
      },
      lastLeaseReminderToLandlord: {
        $cond: {
          if: {
            $ifNull: [
              '$rentalMeta.eSignReminderToLandlordForLeaseSendAt',
              false
            ]
          },
          then: '$rentalMeta.eSignReminderToLandlordForLeaseSendAt',
          else: '$rentalMeta.createdAt'
        }
      }
    }
  },
  {
    $addFields: {
      nextLeaseReminderToTenant: {
        $dateAdd: {
          startDate: '$lastLeaseReminderToTenant',
          unit: 'day',
          amount: '$esignReminderNoticeDays'
        }
      },
      nextLeaseReminderToLandlord: {
        $dateAdd: {
          startDate: '$lastLeaseReminderToLandlord',
          unit: 'day',
          amount: '$esignReminderNoticeDays'
        }
      }
    }
  },
  {
    $addFields: {
      isTodayIsAfterNextLeaseReminderToTenant: {
        $cond: {
          if: {
            $lte: ['$nextLeaseReminderToTenant', new Date()]
          },
          then: true,
          else: false
        }
      },
      isTodayIsAfterNextLeaseReminderToLandlord: {
        $cond: {
          if: {
            $lte: ['$nextLeaseReminderToLandlord', new Date()]
          },
          then: true,
          else: false
        }
      }
    }
  },
  { $skip: skip },
  { $limit: limit },
  {
    $project: {
      createdAt: 1,
      partnerId: 1,
      esignReminderNoticeDays: 1,
      today: 1,
      actualDate: 1,
      isActualDateBefore: 1,
      tenantLeaseSigningStatus: 1,
      landlordLeaseSigningStatus: 1,
      isLandlordLeaseSigned: 1,
      isNeedTenantLeaseSigning: 1,
      isAllTenantLeaseSigned: 1,
      nextLeaseReminderToTenant: 1,
      lastLeaseReminderToTenant: 1,
      lastLeaseReminderToLandlord: 1,
      nextLeaseReminderToLandlord: 1,
      isTodayIsAfterNextLeaseReminderToTenant: 1,
      isTodayIsAfterNextLeaseReminderToLandlord: 1
    }
  }
]

export const updateContractPayoutPauseStatus = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['holdPayout', 'contractId'])
  const { body, session } = req
  await contractHelper.validateUpdateContractPayoutPauseStatus(body)
  const {
    contractId,
    hasHoldPayout,
    holdPayout = false,
    partnerId,
    payoutId,
    unpauseAllPayouts
  } = body
  const contract = await updateContract(
    { _id: contractId },
    { $set: { holdPayout } },
    session
  )

  if (hasHoldPayout && unpauseAllPayouts) {
    await payoutService.updatePayouts(
      { _id: payoutId, partnerId },
      { $set: { holdPayout } },
      session
    )
  }
  return contract
}

export const addContractIdInTenantProperties = async (
  contractData = {},
  session
) => {
  const tenantUpdateQuery = {
    _id: contractData.rentalMeta?.tenantId,
    partnerId: contractData.partnerId,
    properties: {
      $elemMatch: {
        propertyId: contractData.propertyId,
        contractId: { $exists: false }
      }
    }
  }
  const updatedTenant = await tenantService.updateATenant(
    tenantUpdateQuery,
    { $set: { 'properties.$.contractId': contractData._id } },
    session
  )
  return updatedTenant
}

export const updateTenantsPropertyStatusForUpdateContract = async (
  contractData = {},
  session
) => {
  const { _id, rentalMeta, partnerId, propertyId } = contractData
  const tenants = size(rentalMeta?.tenants)
    ? rentalMeta.tenants
    : [{ tenantId: rentalMeta?.tenantId }]
  console.log('Updating property status', _id, tenants)
  const updateTenantPromise = []
  for (const tenant of tenants) {
    if (tenant?.tenantId) {
      const params = {
        contractId: _id,
        partnerId,
        propertyId,
        status: rentalMeta?.status,
        tenantId: tenant.tenantId
      }
      updateTenantPromise.push(
        tenantService.updatePropertyStatusInTenant(params, session)
      )
    }
  }
  if (size(updateTenantPromise)) {
    await Promise.all(updateTenantPromise)
  }
}

export const createJointlyLiableChangeLog = async (params, session) => {
  const logData = await contractHelper.prepareJointlyLiableChangeLogData(params)
  if (!size(logData)) {
    throw new CustomError(404, 'Could not create jointly liable change log')
  }
  await logService.createLog(logData, session)
}

export const createContractUpdateChangeLog = async (params, session) => {
  if (!params.fieldName) {
    const fieldName = contractHelper.getLeaseUpdateFieldName(params)
    if (!fieldName) return false
    params.fieldName = fieldName
  }
  const logData = contractHelper.prepareLeaseUpdateChangeLogData(params)
  if (!size(logData)) {
    throw new CustomError(404, 'Could not create lease change log')
  }
  await logService.createLog(logData, session)
  return logData
}

const addContractInfoInTenantProperties = async (params = {}, session) => {
  const { updatedContract, userId, partnerSettings } = params
  const tenants = updatedContract.rentalMeta?.tenants

  if (size(tenants)) {
    const tenantIds = map(tenants, 'tenantId')
    const updateData = pick(updatedContract, [
      'propertyId',
      'accountId',
      'branchId',
      'agentId',
      'status'
    ])
    const partnerIdOrPartnerSettings = size(partnerSettings)
      ? partnerSettings
      : updatedContract?.partnerId
    updateData.contractId = updatedContract._id
    updateData.createdBy = userId
    updateData.createdAt = await appHelper.getActualDate(
      partnerIdOrPartnerSettings,
      true,
      new Date()
    )
    if (updatedContract.rentalMeta?.status) {
      updateData.status = updatedContract?.rentalMeta?.status
    }

    const query = {
      _id: { $in: tenantIds },
      partnerId: updatedContract.partnerId,
      properties: {
        $not: {
          $elemMatch: {
            propertyId: updatedContract.propertyId,
            contractId: updatedContract._id
          }
        }
      }
    }

    await tenantService.updateTenants(
      query,
      { $push: { properties: updateData } },
      session
    )
  }
}

const updateMultiTenantsForUpdateContract = async (
  params,
  options,
  session
) => {
  const { updatedContract } = params
  const invoiceIds = await invoiceHelper.getUniqueFieldValue('_id', {
    contractId: updatedContract._id,
    partnerId: updatedContract.partnerId,
    invoiceType: { $in: ['invoice', 'credit_note'] }
  })

  if (options.action === 'added_lease_tenant') {
    await addContractInfoInTenantProperties(params, session)
  }

  if (options.action === 'removed_lease_tenant') {
    const query = {
      _id: options.tenantId,
      properties: {
        $elemMatch: {
          propertyId: updatedContract.propertyId,
          contractId: updatedContract._id
        }
      }
    }
    await tenantService.updateTenant(
      query,
      { $set: { 'properties.$.status': 'closed' } },
      session
    )
  }

  if (options.mainTenantId && options.action === 'updated_main_tenant') {
    await invoiceService.updateInvoice(
      { _id: { $in: invoiceIds } },
      { $set: { newTenantId: options.mainTenantId } },
      session
    )
  } else {
    const updateData = {}
    if (options.tenants) updateData.tenants = options.tenants

    await invoiceService.updateInvoice(
      {
        _id: { $in: invoiceIds },
        contractId: updatedContract._id,
        partnerId: updatedContract.partnerId
      },
      { $set: updateData },
      session
    )
    await invoiceSummaryService.updateInvoiceSummaries(
      { invoiceId: { $in: invoiceIds }, partnerId: updatedContract.partnerId },
      { $set: updateData },
      session
    )
    await correctionService.updateCorrections(
      { contractId: updatedContract._id, partnerId: updatedContract.partnerId },
      { $set: updateData },
      session
    )
    await invoicePaymentService.updateInvoicePayments(
      {
        contractId: updatedContract._id,
        invoiceId: { $in: invoiceIds },
        partnerId: updatedContract.partnerId
      },
      { $set: updateData },
      session
    )
    await payoutService.updatePayouts(
      {
        contractId: updatedContract._id,
        invoiceId: { $in: invoiceIds },
        partnerId: updatedContract.partnerId
      },
      { $set: updateData },
      session
    )
    await commissionService.updateCommissions(
      { invoiceId: { $in: invoiceIds }, partnerId: updatedContract.partnerId },
      { $set: updateData },
      session
    )
  }
}

const createLeaseTenantsUpdateLog = async (body, params, session) => {
  const logData = contractHelper.prepareLeaseTenantsUpdateLogData(
    body,
    params,
    session
  )
  if (!size(logData)) {
    throw new CustomError(404, 'Could not create lease tenant change log')
  }
  await logService.createLog(logData, session)
}

export const updateLeaseTermsAfterUpdateProcess = async (body, session) => {
  const { previousContract, updatedContract, userId, partner, updateData } =
    body
  const isDirectPartner = partner?.accountType === 'direct'
  const prevRentalMeta = previousContract.rentalMeta || {}
  const updatedRentalMeta = updatedContract.rentalMeta || {}
  const prevTenants = prevRentalMeta.tenants || []
  const updatedTenants = updatedRentalMeta.tenants || []

  if (
    isDirectPartner &&
    previousContract.status === 'upcoming' &&
    updatedRentalMeta.tenantId &&
    prevRentalMeta.tenantId !== updatedRentalMeta.tenantId
  ) {
    await addContractIdInTenantProperties(updatedContract, session)
  }

  const changeTenants =
    size(updatedTenants) < size(prevTenants)
      ? prevTenants.filter(
          ({ tenantId: oldTenantId }) =>
            !updatedTenants.some(({ tenantId }) => oldTenantId === tenantId)
        )
      : updatedTenants.filter(
          ({ tenantId: newTenantId }) =>
            !prevTenants.some(({ tenantId }) => newTenantId === tenantId)
        )

  if (
    (previousContract.status === 'upcoming' &&
      updatedRentalMeta.tenantId &&
      !(prevRentalMeta && prevRentalMeta.tenantId)) ||
    (size(changeTenants) && changeTenants[0].tenantId)
  ) {
    await updateTenantsPropertyStatusForUpdateContract(updatedContract, session)
  }

  if (
    size(prevRentalMeta) &&
    size(updatedRentalMeta) &&
    prevRentalMeta.tenantId &&
    (updatedRentalMeta.tenantId !== prevRentalMeta.tenantId ||
      (size(changeTenants) && changeTenants[0].tenantId))
  ) {
    const changes = contractHelper.getLeaseTenantsUpdateChanges(body)
    if (size(changes)) {
      await updateMultiTenantsForUpdateContract(body, changes, session)
      await createLeaseTenantsUpdateLog(body, changes, session)
    }
  }

  if (
    size(updatedRentalMeta) &&
    size(prevRentalMeta) &&
    prevRentalMeta.enabledJointlyLiable !== null &&
    updatedRentalMeta.enabledJointlyLiable !==
      prevRentalMeta.enabledJointlyLiable
  ) {
    await createJointlyLiableChangeLog(
      {
        previousContract,
        updatedContract,
        userId
      },
      session
    )
  }

  if (body.extendContractEndDate) {
    body.fieldName = 'contractEndDate'
    if (
      updatedRentalMeta &&
      (!updatedRentalMeta.enabledLeaseEsigning ||
        (updatedRentalMeta.enabledLeaseEsigning &&
          updatedContract.isAllSignCompleted()))
    ) {
      await appQueueService.createAppQueueForCreateRentInvoice(
        {
          contractId: updatedContract._id,
          enabledNotification: updatedContract.rentalMeta?.enabledNotification,
          partnerId: updatedContract.partnerId,
          today: new Date(),
          userId
        },
        session
      )
    }
  }
  await createContractUpdateChangeLog(body, session)

  const fieldNamesForCheckingChanges = [
    'rentalMeta',
    'rentalMeta.tenants',
    'rentalMeta.monthlyRentAmount'
  ]

  const updatedFieldNames = Object.keys(updateData) || []
  const isRequiredFieldUpdated = intersection(
    fieldNamesForCheckingChanges,
    updatedFieldNames
  )
  if (size(isRequiredFieldUpdated)) {
    await addHistoryToContractForChangeLog(updatedFieldNames, body, session)
  }
  //
  // if (size(updatedFieldNames[0])) {
  //   const params = {
  //     collectionId: contractId,
  //     collectionName: 'contract',
  //     context: 'property',
  //     createdBy: userId,
  //     fieldName: updatedFieldNames[0],
  //     partnerId,
  //     previousDoc: previousContract
  //   }
  //   await createAssignmentUpdateLog('updated_contract', params, session)
  // }
}

export const updateLeaseTerms = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'contractId',
    'propertyId',
    'leaseSerial'
  ])
  const { body, session } = req
  appHelper.compactObject(body, true)
  await contractHelper.validateUpdateLeaseTermsData(body)
  const updateData = await contractHelper.prepareUpdateLeaseTermsData(body)
  if (!size(updateData)) throw new CustomError(400, 'No data found for update')

  const { contractId, partnerId } = body
  const updatedContract = await updateContract(
    { _id: contractId, partnerId },
    { $set: updateData },
    session
  )

  if (!size(updatedContract)) {
    throw new CustomError(400, 'No data found for update')
  }
  body.updatedContract = updatedContract
  body.updateData = updateData
  await updateListingForAssignmentUpdate(body, session)
  await updateLeaseTermsAfterUpdateProcess(body, session)
  return updatedContract
}

export const createLogForNewLease = async (params, session) => {
  const logData = contractHelper.prepareLogDataForNewLease(params, session)
  if (!size(logData)) {
    throw new CustomError(404, 'Could not create new lease log')
  }

  const log = await logService.createLog(logData, session)
  return log
}

export const createNewAddonLogForNewLease = async (params, session) => {
  const logData = contractHelper.prepareLogDataForAddAddonInNewLease(params)

  if (!size(logData)) {
    throw new CustomError(404, 'Could not create new lease addon log')
  }
  const log = await logService.createLog(logData, session)
  return log
}

export const createLeaseChangeLog = async (params, session) => {
  const logData = contractHelper.prepareLogDataForUpdateLease(params)

  if (!size(logData)) {
    throw new CustomError(404, 'Could not create update lease log')
  }
  const log = await logService.createLog(logData, session)
  return log
}

export const createJointDepositAccountChangeLog = async (params, session) => {
  const logData =
    contractHelper.prepareLogDataForJointDepositAccountChangeLog(params)

  if (!size(logData)) {
    throw new CustomError(404, 'Could not create joint deposit account')
  }
  const log = await logService.createLog(logData, session)
  return log
}

export const leaseInsertAfterHooksProcess = async (params, session) => {
  const {
    isDirectPartner,
    partnerId,
    previousContract,
    updatedContract,
    userId
  } = params
  if (!size(previousContract) || !size(updatedContract)) {
    throw new CustomError(400, 'Contract data not found')
  }
  const prevRentalMeta = previousContract?.rentalMeta || {}
  const updatedRentalMeta = updatedContract?.rentalMeta || {}

  let propertyUpdateData = {}

  if (
    updatedContract.status === 'active' &&
    previousContract.status !== 'active'
  ) {
    propertyUpdateData = {
      hasActiveLease: true,
      hasUpcomingLease: false,
      hasInProgressLease: false
    }
    if (size(updatedRentalMeta)) {
      propertyUpdateData.leaseStartDate = updatedRentalMeta.leaseStartDate
      propertyUpdateData.leaseEndDate = updatedRentalMeta.leaseEndDate
    }
    await addContractInfoInTenantProperties(
      { updatedContract, userId },
      session
    )
    await updateTenantsPropertyStatusForUpdateContract(updatedContract, session)
    await createLogForNewLease({ updatedContract, userId }, session)
  }

  if (updatedContract.status === 'upcoming') {
    if (isDirectPartner) {
      await addContractIdInTenantProperties(updatedContract, session)
    }
    // propertyUpdateData.hasActiveLease = false
    await addContractInfoInTenantProperties(
      { updatedContract, userId },
      session
    )
    propertyUpdateData.hasUpcomingLease = true
    if (prevRentalMeta.status === 'in_progress') {
      propertyUpdateData.hasInProgressLease = false
    }
    await updateTenantsPropertyStatusForUpdateContract(updatedContract, session)

    if (
      updatedContract.hasRentalContract &&
      !previousContract.hasRentalContract
    )
      propertyUpdateData.hasUpcomingLease = true
    await createLogForNewLease({ updatedContract, userId }, session)
  }

  if (updatedRentalMeta.status === 'in_progress') {
    propertyUpdateData.hasUpcomingLease = false
    propertyUpdateData.hasActiveLease = false
    propertyUpdateData.hasInProgressLease = true
    const upcomingOrActiveLease = await contractHelper.getUniqueFieldValue(
      'rentalMeta.status',
      {
        partnerId,
        propertyId: updatedContract.propertyId,
        'rentalMeta.status': { $in: ['active', 'upcoming'] }
      }
    )
    if (size(upcomingOrActiveLease)) {
      if (upcomingOrActiveLease.includes('active'))
        propertyUpdateData.hasActiveLease = true
      if (upcomingOrActiveLease.includes('upcoming'))
        propertyUpdateData.hasUpcomingLease = true
    }
  }

  if (size(updatedContract.addons)) {
    const leaseAddons = updatedContract.addons.filter(
      (addon) => addon.type === 'lease'
    )
    console.log('Creating lease addons', leaseAddons)
    if (size(leaseAddons)) {
      await createNewAddonLogForNewLease(
        { addons: leaseAddons, userId, contractInfo: updatedContract },
        session
      )
    }
  }
  console.log('Updating lease property data', propertyUpdateData)
  if (size(propertyUpdateData)) {
    const updatedProperty = await listingService.updateAListing(
      { _id: updatedContract.propertyId, partnerId },
      { $set: propertyUpdateData },
      session
    )
    await propertyAfterUpdateProcess(updatedProperty, session)
  }

  await createContractUpdateChangeLog(
    {
      updatedContract,
      previousContract,
      userId,
      context: 'property',
      action: 'updated_lease'
    },
    session
  )

  if (
    previousContract.status !== 'upcoming' &&
    updatedContract.status === 'upcoming'
  ) {
    await createContractUpdateChangeLog(
      {
        updatedContract,
        previousContract,
        userId,
        context: 'property',
        fieldName: 'status',
        action: 'updated_contract'
      },
      session
    )
  }

  if (
    size(prevRentalMeta) &&
    size(updatedRentalMeta) &&
    prevRentalMeta.tenantId &&
    (updatedRentalMeta.tenantId !== prevRentalMeta.tenantId ||
      !isEqual(updatedRentalMeta.tenants, prevRentalMeta.tenants))
  ) {
    const changes = contractHelper.getLeaseTenantsUpdateChanges(params)
    if (size(changes)) {
      await updateMultiTenantsForUpdateContract(params, changes, session)
      await createLeaseTenantsUpdateLog(params, changes, session)
    }
  }

  if (
    size(updatedRentalMeta) &&
    size(prevRentalMeta) &&
    prevRentalMeta.enabledJointlyLiable !== null &&
    updatedRentalMeta.enabledJointlyLiable !==
      prevRentalMeta.enabledJointlyLiable
  ) {
    await createJointlyLiableChangeLog(
      {
        previousContract,
        updatedContract,
        userId
      },
      session
    )
  }

  if (
    size(updatedRentalMeta) &&
    size(prevRentalMeta) &&
    prevRentalMeta.enabledJointDepositAccount !== null &&
    updatedRentalMeta.enabledJointDepositAccount !==
      prevRentalMeta.enabledJointDepositAccount
  ) {
    const jointLogParams = {
      contractInfo: updatedContract,
      previousContract,
      userId
    }
    await createJointDepositAccountChangeLog(jointLogParams, session)
  }

  const { leaseWelcomeEmailSentInProgress, leaseWelcomeEmailSentAt } =
    updatedRentalMeta

  if (
    !leaseWelcomeEmailSentAt &&
    leaseWelcomeEmailSentInProgress &&
    updatedRentalMeta.status !== 'in_progress'
  ) {
    await createAppQueueForSendLeaseNotificationToTenant(
      updatedContract,
      session
    )
  }

  return updatedContract
}

export const createALease = async (req) => {
  const { body, session, user } = req
  const { roles = [] } = user
  if (!roles.includes('lambda_manager') && user.partnerId) {
    body.partnerId = user.partnerId
    body.userId = user.userId
  }
  appHelper.checkRequiredFields(
    [
      'contractStartDate',
      'depositType',
      'dueDate',
      'firstInvoiceDueDate',
      'invoiceFrequency',
      'invoiceStartFrom',
      'isMovedIn',
      'minimumStay',
      'noticeInEffect',
      'propertyId',
      'partnerId',
      'tenantId',
      'userId'
    ],
    body
  )

  appHelper.compactObject(body)
  console.log('Checking validations for create a lease')
  await contractHelper.getRequiredDataAndValidateLeaseCreateData(body)
  console.log('Validations passed')
  const { partnerId, isDirectPartner, userId } = body
  const invalidTenantsOrBusinessLandlord =
    await contractHelper.validateTenantsBusinessLandlord(body)
  console.log(
    'invalidTenantsOrBusinessLandlord',
    invalidTenantsOrBusinessLandlord
  )
  if (size(invalidTenantsOrBusinessLandlord)) {
    return { invalidTenantsOrBusinessLandlord }
  }

  const { invoiceAccountNumber, leaseEsigningPdfContent, leaseType } = body
  console.log('isDirectPartner', isDirectPartner)
  console.log('invoiceAccountNumber', invoiceAccountNumber)
  if (isDirectPartner) {
    if (!invoiceAccountNumber) {
      throw new CustomError(400, 'Invoice account number is required')
    }
    if (invoiceAccountNumber.length !== 11 || isNaN(invoiceAccountNumber)) {
      throw new CustomError(400, 'Please provide valid invoice account number')
    }
    await partnerSettingService.createABankAccount(
      { partnerId },
      {
        accountNumber: invoiceAccountNumber,
        canUsePartnerAccountNumber: true
      },
      session
    )
    console.log('BankAccount created')
  }
  // To create assignment for direct partner
  console.log('ContractId', body.contractId)
  if (isDirectPartner && !size(body.upcomingContract)) {
    const contractInfo = await createAssignmentForDirectPartner(body, session)
    if (!size(contractInfo))
      throw new CustomError(400, 'Contract is not available for create lease')
    body.contractId = contractInfo._id
    body.contractInfo = contractInfo
  }

  await createAQueueToUploadContractPdfToS3AndRemoveOldPdf(
    {
      actionType: leaseType,
      contractId: body.contractId,
      contractType: 'lease',
      esigningPdfContent: leaseEsigningPdfContent,
      partnerId,
      userId
    },
    session
  )

  console.log('preparing rentalMetaData ')
  const rentalMetaData = await contractHelper.prepareRentalMetaDataForAddLease(
    body
  )
  console.log('prepared rentalMetaData ', rentalMetaData)
  const updatedContract = await addLeaseDataInContractAndCreateRentInvoice(
    body,
    rentalMetaData,
    session
  )
  console.log('Lease created ', updatedContract)
  const params = {
    isDirectPartner,
    partnerId,
    previousContract: body.contractInfo,
    userId,
    updatedContract
  }
  console.log('params preapred for lease hook ', params)
  await leaseInsertAfterHooksProcess(params, session)
  return updatedContract
}
export const updateContractStatusAndCreateRentInvoice = async (
  params,
  session
) => {
  console.log(
    'Updating contract status and create rent invoice, params:',
    params?.contractId
  )
  const { contractId, files, status, userId = 'SYSTEM' } = params
  const { updateData, isCreateInvoice } =
    await contractHelper.prepareContractDataForUpdateStatus(params)
  if (!size(updateData)) return false

  const contractUpdateData = {
    $set: updateData
  }

  if (size(files)) {
    contractUpdateData.$push = {
      files: { $each: files }
    }
  }
  console.log('Update contract data:', contractUpdateData)
  const updatedContract = await updateContract(
    { _id: contractId },
    contractUpdateData,
    session
  )
  if (
    size(updatedContract) &&
    (status === 'active' || status === 'upcoming' || isCreateInvoice)
  ) {
    const appQueueParams = {
      contractId: updatedContract._id,
      enabledNotification: updatedContract.rentalMeta?.enabledNotification,
      partnerId: updatedContract.partnerId,
      today: new Date(),
      userId
    }
    await appQueueService.createAppQueueForCreateRentInvoice(
      appQueueParams,
      session,
      'regular'
    )
  }
  return updatedContract
}

export const createLogForTerminateLease = async (params, session) => {
  const logData = contractHelper.prepareLogDataForTerminateLease(params)
  if (!size(logData)) {
    throw new CustomError(404, 'Could not create lease terminate log')
  }
  await logService.createLog(logData, session)
  return logData
}

export const sendLeaseTerminationNotificationEmail = async (
  params,
  isScheduleNotification = false,
  session
) => {
  const { contractInfo, partnerSetting, userId } = params
  const {
    leaseScheduleTerminatedByLandlord,
    leaseScheduleTerminatedByTenant,
    leaseTerminatedByLandlord,
    leaseTerminatedByTenant
  } = partnerSetting?.notifications

  const terminatedBy = contractInfo.rentalMeta?.terminatedBy
  let event = ''

  if (isScheduleNotification) {
    if (terminatedBy === 'tenant' && leaseScheduleTerminatedByTenant) {
      event = 'send_schedule_termination_notice_by_tenant'
    }
    if (terminatedBy === 'landlord' && leaseScheduleTerminatedByLandlord) {
      event = 'send_schedule_termination_notice_by_landlord'
    }
  } else {
    if (terminatedBy === 'tenant' && leaseTerminatedByTenant) {
      event = 'send_termination_notice_by_tenant'
    }
    if (terminatedBy === 'landlord' && leaseTerminatedByLandlord) {
      event = 'send_termination_notice_by_landlord'
    }
  }
  console.log('Lease termination notification', {
    event,
    terminatedBy
  })
  if (event) {
    const appQueueData = {
      destination: 'notifier',
      params: {
        partnerId: contractInfo.partnerId,
        collectionId: contractInfo._id,
        collectionNameStr: 'contracts'
      },
      priority: 'immediate',
      action: 'send_notification',
      status: 'new',
      event,
      createdBy: userId
    }
    await appQueueService.createAnAppQueue(appQueueData, session)
  }
}

export const createContractForDirectPartner = async (params = {}, session) => {
  const contractAddData = contractHelper.prepareContractCreateData(params)
  contractAddData.assignmentSerial = await counterService.incrementCounter(
    `assignment-${params.propertyId}`,
    session
  )
  const createdContract = await createContract(contractAddData, session)
  return createdContract
}

export const leaseTerminateAfterHooksProcess = async (params, session) => {
  const {
    creditWholeInvoice,
    partnerSetting,
    previousContract,
    upcomingContract,
    updatedContract,
    userId
  } = params
  const propertyUpdateData = {}
  const unsetPropertyData = {}

  const isCreateCreditNoteInvoice =
    await contractHelper.isCreateCreditNoteInvoice(
      updatedContract,
      previousContract,
      partnerSetting
    )

  if (
    (updatedContract.status === 'closed' &&
      previousContract.status !== 'closed') ||
    isCreateCreditNoteInvoice
  ) {
    // Since it's termination process
    await invoiceService.createCreditNoteInvoices(
      {
        contractId: updatedContract._id,
        creditWholeInvoice,
        enabledNotification: updatedContract.rentalMeta?.enabledNotification,
        partnerId: updatedContract.partnerId,
        partnerSetting,
        terminationDate: updatedContract.rentalMeta?.contractEndDate,
        userId
      },
      session
    )
  }

  if (
    updatedContract.status === 'closed' &&
    previousContract.status !== 'closed'
  ) {
    const activeContract = await contractHelper.getAContract(
      {
        propertyId: updatedContract.propertyId,
        partnerId: updatedContract.partnerId,
        status: 'active'
      },
      session
    )

    if (
      !contractHelper.hasActiveOrUpcomingContract(
        activeContract,
        upcomingContract
      )
    ) {
      propertyUpdateData.hasAssignment = false
    }

    if (updatedContract.hasRentalContract) {
      unsetPropertyData.leaseStartDate = 1
      unsetPropertyData.leaseEndDate = 1

      const partnerInfo = await partnerHelper.getAPartner({
        _id: updatedContract.partnerId
      })
      const isDirectPartner = partnerInfo?.accountType === 'direct'

      if (!upcomingContract && isDirectPartner) {
        propertyUpdateData.hasUpcomingLease = false
      }

      if (!activeContract) {
        propertyUpdateData.hasActiveLease = false
        if (!upcomingContract && isDirectPartner) {
          await createContractForDirectPartner(updatedContract, session)
        }
      }

      await updateTenantsPropertyStatusForUpdateContract(
        updatedContract,
        session
      )
    }
  }

  if (size(propertyUpdateData)) {
    const updateData = { $set: propertyUpdateData }
    if (unsetPropertyData) {
      updateData['$unset'] = unsetPropertyData
    }

    const updatedProperty = await listingService.updateAListing(
      { _id: updatedContract.propertyId, partnerId: updatedContract.partnerId },
      updateData,
      session
    )
    await propertyAfterUpdateProcess(updatedProperty, session)
  }

  if (
    previousContract.rentalMeta?.status !== updatedContract.rentalMeta?.status
  ) {
    await createLeaseChangeLog(
      {
        fieldName: 'status',
        contractInfo: updatedContract,
        previousContract,
        userId
      },
      session
    )
  }
  return updatedContract
}

const createFileForTerminateLease = async (params = {}, session) => {
  const { contract = {}, files = [], userId } = params
  const { _id: contractId, leaseSerial, partnerId, propertyId } = contract

  const propertyInfo =
    (await listingHelper.getAListing({ _id: propertyId })) || {}
  const accountId = propertyInfo?.accountId || ''

  const fileData = {
    accountId,
    context: 'contract',
    contractId,
    directive: 'Files',
    leaseSerial,
    partnerId,
    propertyId
  }
  const filesArray = []
  const fileIds = []
  const promiseArr = []
  for (const file of files) {
    const fieldId = nid(17)
    fileData._id = fieldId
    fileData.name = file.name
    fileData.size = file.size
    fileData.title = file.title
    fileData.createdBy = userId

    filesArray.push(fileData)
    fileIds.push(fieldId)

    if (leaseSerial) {
      const options = {
        accountId,
        action: 'uploaded_file',
        context: 'contract',
        contractId,
        createdBy: userId,
        fileId: fieldId,
        fileTitle: fileData.title,
        leaseSerial,
        partnerId,
        propertyId
      }
      promiseArr.push(fileService.createLogForUploadedFile(options, session))
    }
  }

  if (size(filesArray)) {
    await fileService.createFiles(filesArray, session)
    if (size(promiseArr)) await Promise.all(promiseArr)
  }

  return fileIds
}

export const terminateLease = async (req) => {
  appHelper.validatePartnerAppRequestData(req, [
    'contractEndDate',
    'contractId',
    'creditWholeInvoice',
    'enabledNotification',
    'propertyId',
    'terminatedBy'
  ])

  const { body = {}, session } = req
  const { creditWholeInvoice, files, userId } = body
  const preparedData = await contractHelper.prepareLeaseTerminateData(body)

  let filesArr = []
  if (size(files)) {
    const fileCreateParams = {
      contract: preparedData.contractInfo,
      files,
      userId
    }
    const filesInfo = await createFileForTerminateLease(
      fileCreateParams,
      session
    )
    filesArr = filesInfo.map((fileId) => ({ context: 'contract', fileId }))
  }
  preparedData.files = filesArr

  const updatedLease = await updateContractStatusAndCreateRentInvoice(
    preparedData,
    session
  )
  if (updatedLease) {
    const { contractInfo, partnerSetting, todayDate, upcomingContract } =
      preparedData
    await leaseTerminateAfterHooksProcess(
      {
        creditWholeInvoice,
        partnerSetting,
        previousContract: contractInfo,
        updatedContract: updatedLease,
        upcomingContract,
        userId
      },
      session
    )
    updatedLease.userId = userId
    await createLogForTerminateLease(updatedLease, session)

    if (body.enabledNotification) {
      const query = {
        contractInfo: updatedLease,
        partnerSetting,
        userId
      }
      const contractEndDate = await appHelper.getActualDate(
        partnerSetting,
        true,
        preparedData?.contractEndDate
      )
      console.log('Lease termination notification date', {
        todayDate,
        contractEndDate
      })
      if (contractEndDate > todayDate) {
        await sendLeaseTerminationNotificationEmail(query, true, session)
      } else {
        await sendLeaseTerminationNotificationEmail(query, false, session)
      }
    }
  }
  return updatedLease
}

export const checkCommissionChangesAndAddHistory = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['contractId'], body)
  const { contractId } = body
  const contract = await contractHelper.getAContract(
    {
      _id: contractId
    },
    undefined,
    ['partnerSetting']
  )
  if (!size(contract) || !size(contract.partnerSetting)) {
    throw new CustomError(404, 'Please provide valid contract')
  }
  await invoiceService.findCreditNoteInvoiceAndCheckCommissionChanges({
    contract,
    partnerSetting: contract.partnerSetting
  })
  return {
    result: true
  }
}

export const createLogForCancelLease = async (params, session) => {
  const logData = contractHelper.prepareLogDataForCancelLease(params)
  if (!size(logData)) {
    throw new CustomError(404, 'Could not create lease cancel log')
  }
  await logService.createLog(logData, session)
  return logData
}

export const createAppQueueForSendTenantLeaseESigningNotification = async (
  updatedContract,
  createdBy = 'SYSTEM',
  session
) => {
  const appQueueData = {
    action: 'send_notification',
    createdBy,
    destination: 'notifier',
    event: 'send_tenant_lease_esigning',
    params: {
      collectionNameStr: 'contracts',
      collectionId: updatedContract._id,
      partnerId: updatedContract._id
    },
    priority: 'immediate',
    status: 'new'
  }

  const createdQueue = await appQueueService.createAnAppQueue(
    appQueueData,
    session
  )
  return createdQueue
}

export const sendLeaseEsigingNotification = async (params, session) => {
  const { previousContract = {}, updatedContract = {}, userId } = params

  const isEnabledEsiging = await contractHelper.isEnabledSendEsignNotification(
    previousContract,
    updatedContract
  )
  if (!isEnabledEsiging) return false

  const createdQueue =
    await createAppQueueForSendTenantLeaseESigningNotification(
      updatedContract,
      userId,
      session
    )

  if (createdQueue && isEnabledEsiging?.enabledSendEsignNotification) {
    await updateContract(
      { _id: updatedContract._id, partnerId: updatedContract.partnerId },
      { $set: { 'rentalMeta.isSendEsignNotify': true } },
      session
    )
  }
  return true
}

export const sendTenantLeaseESigningNotificationAndAddSendESigningTagInContract =
  async (contract, session) => {
    if (!size(contract))
      throw new CustomError(
        404,
        'Contract not found while sending tenant lease ESiging notification'
      )
    const appQueueData = {
      action: 'send_notification',
      destination: 'notifier',
      event: 'send_tenant_lease_esigning',
      params: {
        collectionId: contract._id,
        collectionNameStr: 'contracts',
        partnerId: contract.partnerId
      },
      priority: 'immediate',
      status: 'new'
    }
    const [createdQueue] = await appQueueService.createAnAppQueue(
      appQueueData,
      session
    )
    if (size(createdQueue)) {
      console.log(
        `## Creating an appQueue to send emails to the tenant or tenants with document E-SigningUrl for lease. CreatedQueueId:
        ${createdQueue._id}`
      )
      const updatedContract = await updateContract(
        { _id: contract._id },
        { $set: { 'rentalMeta.isSendEsignNotify': true } },
        session
      )
      console.log(
        `=== Added isSendEsignNotify tag in contract. contractId: ${updatedContract._id} ===`
      )
    }
  }

const updatePendingAppQueueStatuses = async (contract, session) => {
  const { idfyLeaseDocId = '' } = contract
  console.log('Found idfyLeaseDocId ', idfyLeaseDocId)
  if (!idfyLeaseDocId) return true
  const query = {
    'params.payload.documentId': idfyLeaseDocId,
    action: 'handle_idfy_response',
    status: { $ne: 'completed' }
  }
  const updateData = {
    $set: { status: 'completed', isManuallyCompleted: true }
  }
  console.log('Prepared query ', query)
  console.log('Prepared updateData ', updateData)
  const appQueues = await appQueueService.updateAppQueueItems(
    query,
    updateData,
    session
  )
  console.log('Updated appQueues ', appQueues)
}

export const cancelLeaseAfterHooksProcess = async (params, session) => {
  console.log('Started cancel lease after process')
  const {
    isBrokerPartner,
    partnerSetting,
    previousContract,
    updatedContract,
    userId
  } = params
  const unsetPropertyData = {}
  const propertyUpdateData = { hasInProgressLease: false }

  const activeContract = await contractHelper.getAContract(
    {
      propertyId: updatedContract.propertyId,
      partnerId: updatedContract.partnerId,
      status: 'active'
    },
    session
  )
  const upcomingContract = await contractHelper.getAContract(
    {
      propertyId: updatedContract.propertyId,
      partnerId: updatedContract.partnerId,
      status: 'upcoming'
    },
    session
  )

  updatedContract.rentalMeta.status = 'closed'

  if (isBrokerPartner) {
    propertyUpdateData.hasUpcomingLease = false
    propertyUpdateData.hasProspects = false
    await invoiceService.updateInvoices(
      {
        partnerId: updatedContract.partnerId,
        contractId: updatedContract._id
      },
      {
        $set: { leaseCancelled: true }
      },
      session
    )
  }

  // Since it's lease cancel process,So we need to create credit note of invoices
  await invoiceService.createCreditNoteInvoices(
    {
      contractId: updatedContract._id,
      partnerId: updatedContract.partnerId,
      partnerSetting,
      userId
    },
    session
  )

  if (
    !contractHelper.hasActiveOrUpcomingContract(
      activeContract,
      upcomingContract
    )
  ) {
    propertyUpdateData.hasAssignment = false
  }

  if (updatedContract.hasRentalContract) {
    unsetPropertyData.leaseStartDate = 1
    unsetPropertyData.leaseEndDate = 1

    const partnerInfo = await partnerHelper.getAPartner({
      _id: updatedContract.partnerId
    })
    const isDirectPartner = partnerInfo?.accountType === 'direct'

    if (!upcomingContract && isDirectPartner) {
      propertyUpdateData.hasUpcomingLease = false
    }

    if (!activeContract) {
      propertyUpdateData.hasActiveLease = false
      if (!upcomingContract && isDirectPartner) {
        await createContractForDirectPartner(updatedContract, session)
      }
    }
  }

  console.log(
    'Started updating property status',
    updatedContract.hasRentalContract,
    isBrokerPartner
  )

  const tenantParams = {
    _id: updatedContract._id,
    rentalMeta: {
      status: 'closed',
      tenantId: previousContract?.rentalMeta?.tenantId,
      tenants: previousContract?.rentalMeta?.tenants
    },
    partnerId: updatedContract.partnerId,
    propertyId: updatedContract.propertyId
  }
  await updateTenantsPropertyStatusForUpdateContract(tenantParams, session)

  await updatePendingAppQueueStatuses(updatedContract, session)

  if (size(propertyUpdateData)) {
    const updateData = { $set: propertyUpdateData }
    if (size(unsetPropertyData)) {
      updateData['$unset'] = unsetPropertyData
    }

    const updatedProperty = await listingService.updateAListing(
      { _id: updatedContract.propertyId, partnerId: updatedContract.partnerId },
      updateData,
      session
    )
    await propertyAfterUpdateProcess(updatedProperty, session)
  }

  await createLeaseChangeLog(
    {
      contractInfo: updatedContract,
      fieldName: 'status',
      previousContract,
      userId
    },
    session
  )
  return updatedContract
}

export const cancelLeaseAndAfterHooksProcess = async (params, session) => {
  const {
    contractInfo,
    isBrokerPartner,
    partnerSetting,
    signerId,
    signerType,
    userId
  } = params
  const updateData = await contractHelper.prepareCancelLeaseUpdateData(params)

  if (!size(updateData)) {
    throw new CustomError(404, 'Could not cancel lease')
  }

  if (isBrokerPartner) {
    updateData.$set.leaseSerial = await counterService.incrementCounter(
      `lease-${contractInfo.propertyId}`,
      session
    )
  }
  console.log('Started cancel lease', JSON.parse(JSON.stringify(updateData)))
  const updatedContract = await updateContract(
    { _id: contractInfo._id, propertyId: contractInfo.propertyId },
    updateData,
    session
  )
  console.log(
    'Successfully canceled contract info',
    JSON.parse(JSON.stringify(updatedContract))
  )
  if (!size(updatedContract)) {
    throw new CustomError(404, 'Could not cancel lease')
  }

  const logParams = {
    contractInfo,
    signerId,
    signerType,
    userId
  }

  await createLogForCancelLease(logParams, session)
  const updateParams = {
    isBrokerPartner,
    partnerSetting,
    previousContract: contractInfo,
    updatedContract: cloneDeep(updatedContract),
    userId
  }

  await cancelLeaseAfterHooksProcess(updateParams, session)
  return updatedContract
}

export const cancelLease = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'propertyId'])
  const { body, session } = req

  const preparedData = await contractHelper.getRequiredDataForCancelLease(body)
  const updatedContract = await cancelLeaseAndAfterHooksProcess(
    preparedData,
    session
  )
  return updatedContract
}

export const createAppQueueForSendLeaseNotificationToTenant = async (
  params,
  session
) => {
  const appQueueData = {
    action: 'send_notification',
    destination: 'notifier',
    event: 'send_welcome_lease',
    params: {
      collectionId: params._id,
      collectionNameStr: 'contracts',
      partnerId: params.partnerId
    },
    status: 'new',
    priority: 'immediate'
  }

  await appQueueService.createAnAppQueue(appQueueData, session)
}

export const contractStatusUpdateAfterHooksProcess = async (
  params = {},
  session
) => {
  let { isBrokerPartner, partnerSettings } = params
  const { previousContract = {}, updatedContract = {}, userId } = params
  console.log('Started contract update after hooks process, params', params)
  if (!size(previousContract) || !size(updatedContract)) {
    throw new CustomError(404, 'Required contract info')
  }

  /** @Desc preparing required data */

  if (!size(partnerSettings) || isBrokerPartner === undefined) {
    const partner = await partnerHelper.getAPartner(
      { _id: updatedContract?.partnerId },
      null,
      ['partnerSetting']
    )

    if (!size(partner)) throw new CustomError(404, 'Partner not found')

    const { accountType, partnerSetting } = partner
    if (!size(partnerSetting)) {
      throw new CustomError(404, 'Partner setting not found')
    }
    isBrokerPartner = accountType === 'broker'
    partnerSettings = partnerSetting
  }

  const prevRentalMeta = previousContract.rentalMeta || {}
  const updatedRentalMeta = updatedContract.rentalMeta || {}

  const unsetPropertyData = {}
  let propertyUpdateData = {}
  const updateContractData = {}

  /** @For {status: 'active'} */

  if (
    updatedContract.status === 'active' &&
    previousContract.status !== 'active'
  ) {
    propertyUpdateData = {
      hasActiveLease: true,
      hasUpcomingLease: false,
      hasInProgressLease: false,
      leaseStartDate: updatedRentalMeta.leaseStartDate,
      leaseEndDate: updatedRentalMeta.leaseEndDate
    }
    await addContractIdInTenantProperties(updatedContract, session)
    await updateTenantsPropertyStatusForUpdateContract(updatedContract, session)
    // await createLogForNewLease({ updatedContract, userId }, session)
    if (!updatedContract.leaseSerial) {
      updateContractData.leaseSerial = await counterService.incrementCounter(
        `lease-${updatedContract.propertyId}`,
        session
      )
    }

    if (prevRentalMeta.status !== 'upcoming') {
      const {
        enabledNotification,
        leaseWelcomeEmailSentInProgress,
        leaseWelcomeEmailSentAt
      } = updatedRentalMeta
      const isSendWelcomeLease =
        partnerSettings?.notifications?.sentWelcomeLease
      if (
        isSendWelcomeLease &&
        enabledNotification &&
        !leaseWelcomeEmailSentAt &&
        !leaseWelcomeEmailSentInProgress
      ) {
        updateContractData['rentalMeta.leaseWelcomeEmailSentInProgress'] = true
        await createAppQueueForSendLeaseNotificationToTenant(
          updatedContract,
          session
        )
      }
    }
  }

  /** @For {status: 'upcomming'} */

  if (
    updatedRentalMeta.status === 'upcoming' &&
    prevRentalMeta.status !== 'upcoming'
  ) {
    propertyUpdateData.hasUpcomingLease = true
    propertyUpdateData.leaseStartDate = updatedRentalMeta.leaseStartDate

    await addContractIdInTenantProperties(updatedContract, session)

    if (prevRentalMeta.status === 'in_progress') {
      propertyUpdateData.hasInProgressLease = false
      await updateTenantsPropertyStatusForUpdateContract(
        updatedContract,
        session
      )
    }

    // await createLogForNewLease({ updatedContract, userId }, session)

    if (!updatedContract.leaseSerial && updatedContract.hasRentalContract) {
      updateContractData.leaseSerial = await counterService.incrementCounter(
        `lease-${updatedContract.propertyId}`,
        session
      )
    }

    if (prevRentalMeta.status === 'in_progress') {
      const accountInfo = accountHelper.getAnAccount({
        _id: updatedContract?.accountId
      })
      if (size(accountInfo) && accountInfo.status !== 'in_progress') {
        await accountService.updateAnAccount(
          { _id: updatedContract?.accountId },
          { $set: { status: 'active' } },
          session
        )
      }
    }

    const {
      enabledNotification,
      leaseWelcomeEmailSentInProgress,
      leaseWelcomeEmailSentAt
    } = updatedRentalMeta
    const isSendWelcomeLease = partnerSettings?.notifications?.sentWelcomeLease
    if (
      isSendWelcomeLease &&
      enabledNotification &&
      !leaseWelcomeEmailSentAt &&
      !leaseWelcomeEmailSentInProgress
    ) {
      updateContractData['rentalMeta.leaseWelcomeEmailSentInProgress'] = true
      await createAppQueueForSendLeaseNotificationToTenant(
        updatedContract,
        session
      )
    }
  }

  /** @For {status: 'closed'} */

  if (
    updatedContract.status === 'closed' &&
    previousContract.status !== 'closed'
  ) {
    const isCreateCreditNoteInvoice =
      await contractHelper.isCreateCreditNoteInvoice(
        updatedContract,
        previousContract,
        partnerSettings
      )

    if (isCreateCreditNoteInvoice) {
      await invoiceService.createCreditNoteInvoices(
        {
          contractId: updatedContract._id,
          enabledNotification: updatedRentalMeta?.enabledNotification || false,
          partnerId: updatedContract.partnerId,
          partnerSetting: partnerSettings,
          terminationDate: updatedRentalMeta?.contractEndDate,
          userId
        },
        session
      )
    }
    const activeContract = await contractHelper.getAContract(
      {
        status: 'active',
        partnerId: updatedContract.partnerId,
        propertyId: updatedContract.propertyId
      },
      session
    )

    const upcomingContract = await contractHelper.getAContract(
      {
        status: 'upcoming',
        partnerId: updatedContract.partnerId,
        propertyId: updatedContract.propertyId
      },
      session
    )

    if (
      !contractHelper.hasActiveOrUpcomingContract(
        activeContract,
        upcomingContract
      )
    ) {
      propertyUpdateData.hasAssignment = false
    }

    if (updatedContract.hasRentalContract) {
      unsetPropertyData.leaseStartDate = 1
      unsetPropertyData.leaseEndDate = 1

      if (!upcomingContract && !isBrokerPartner) {
        propertyUpdateData.hasUpcomingLease = false
      }

      if (!activeContract) {
        propertyUpdateData.hasActiveLease = false
        if (!upcomingContract && !isBrokerPartner) {
          await createContractForDirectPartner(updatedContract, session)
        }
      }
    }

    if (updatedContract.hasRentalContract || isBrokerPartner) {
      await updateTenantsPropertyStatusForUpdateContract(
        updatedContract,
        session
      )
    }

    if (
      isBrokerPartner &&
      (prevRentalMeta.status === 'upcoming' ||
        prevRentalMeta.status === 'in_progress')
    ) {
      propertyUpdateData.hasUpcomingLease = false
      propertyUpdateData.hasProspects = false
      await invoiceService.updateInvoices(
        {
          partnerId: updatedContract.partnerId,
          contractId: updatedContract._id
        },
        {
          $set: { leaseCancelled: true }
        },
        session
      )
    }
  }
  if (
    updatedContract.hasRentalContract &&
    (prevRentalMeta.status === 'upcoming' ||
      prevRentalMeta.status === 'in_progress') &&
    updatedRentalMeta.status === 'closed'
  ) {
    propertyUpdateData.hasInProgressLease = false
  }

  /** @For {status: 'in_progress'} */

  if (
    updatedRentalMeta.status === 'in_progress' &&
    prevRentalMeta.status !== 'in_progress'
  ) {
    propertyUpdateData.hasUpcomingLease = false
    propertyUpdateData.hasInProgressLease = true
  }

  if (size(updateContractData)) {
    await updateContract(
      { _id: updatedContract._id },
      { $set: updateContractData },
      session
    )
  }
  console.log('Update property data', propertyUpdateData)

  if (size(propertyUpdateData)) {
    const updateData = { $set: propertyUpdateData }
    if (unsetPropertyData) {
      updateData['$unset'] = unsetPropertyData
    }

    const updatedProperty = await listingService.updateAListing(
      { _id: updatedContract.propertyId, partnerId: updatedContract.partnerId },
      updateData,
      session
    )
    await propertyAfterUpdateProcess(updatedProperty, session)
  }

  if (prevRentalMeta?.status !== updatedRentalMeta?.status) {
    await createLeaseChangeLog(
      {
        fieldName: 'status',
        contractInfo: updatedContract,
        previousContract,
        userId
      },
      session
    )
  }
  return updatedContract
}

export const createAppQueueForCancelLeaseAndSendNotification = async (
  contractInfo,
  session
) => {
  const appQueueData = {
    action: 'send_notification',
    event: 'send_wrong_ssn_notification',
    destination: 'notifier',
    params: {
      partnerId: contractInfo.partnerId,
      collectionId: contractInfo._id,
      collectionNameStr: 'contracts'
    },
    priority: 'immediate',
    status: 'new'
  }

  await appQueueService.createAnAppQueue(appQueueData, session)
}

export const cancelLeaseForWrongSSN = async (req) => {
  const { body, user = {}, session } = req
  appHelper.checkRequiredFields(['userId'], user)
  appHelper.checkRequiredFields(
    ['contractId', 'signerType', 'signerId', 'partnerId', 'propertyId'],
    body
  )
  body.userId = user.userId

  const preparedData = await contractHelper.getRequiredDataForCancelLease(body)
  const { contractInfo, partnerSetting } = preparedData

  const isEnabledNotification =
    partnerSetting?.notifications?.wrongSSNNotification

  if (isEnabledNotification) {
    await createAppQueueForCancelLeaseAndSendNotification(contractInfo, session)
  }

  const updatedContract = await cancelLeaseAndAfterHooksProcess(
    preparedData,
    session
  )
  return updatedContract
}

const validateRemoveAddonData = async (params) => {
  const { addonId, contractId, partnerId } = params

  const query = {
    _id: contractId,
    partnerId,
    addons: {
      $elemMatch: {
        addonId
      }
    }
  }
  const prevContract = await contractHelper.getAContract(query)
  if (!size(prevContract)) throw new CustomError(404, 'Contract not found')
  const removeFrom = prevContract.hasRentalContract ? 'lease' : 'assignment'
  const matchedType = prevContract.addons.find(
    (addon) => addon.type === removeFrom && addon.addonId === addonId
  )
  if (!size(matchedType)) throw new CustomError(404, 'Addon not found')
}

const createLogAfterRemoveAddon = async (params, session) => {
  const { body, removedAddonContract } = params
  const { addonId, partnerId, userId } = body
  const {
    accountId,
    agentId,
    assignmentSerial,
    branchId,
    hasRentalContract,
    leaseSerial,
    propertyId,
    rentalMeta
  } = removedAddonContract
  const logData = {
    accountId,
    agentId,
    branchId,
    context: 'property',
    contractId: removedAddonContract._id,
    createdBy: userId,
    partnerId,
    propertyId,
    tenantId: rentalMeta.tenantId
  }

  const contractType = hasRentalContract ? 'lease' : 'assignment'

  let action = ''
  if (contractType === 'lease') action = 'removed_lease_addon'
  else action = 'removed_assignment_addon'

  logData.action = action

  const visibility = ['property']
  if (accountId) visibility.push('account')
  if (logData.tenantId) visibility.push('tenant')
  logData.visibility = visibility

  let serialFieldName = ''
  let serial = ''

  if (action === 'removed_lease_addon') {
    serialFieldName = 'leaseSerial'
    serial = leaseSerial
  } else {
    serialFieldName = 'assignmentSerial'
    serial = assignmentSerial
  }

  logData.meta = [
    { field: 'addonId', value: addonId },
    {
      field: serialFieldName,
      value: serial
    }
  ]

  await logService.createLog(logData, session)
}

export const removeContractAddon = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['addonId', 'contractId'])

  const { body = {}, session } = req
  const { addonId, contractId, partnerId } = body
  await validateRemoveAddonData(body)
  const removedAddonContract = await updateContract(
    {
      _id: contractId,
      partnerId,
      addons: {
        $elemMatch: {
          addonId
        }
      }
    },
    {
      $pull: { addons: { addonId } }
    },
    session
  )
  const params = {
    body,
    removedAddonContract
  }
  await createLogAfterRemoveAddon(params, session)
  return {
    message: 'Addon removed successfully!'
  }
}

export const updatePauseUnpauseOfPayouts = async (req) => {
  const { session } = req
  appHelper.validatePartnerAppRequestData(req, ['contractId', 'holdPayout'])
  const { partnerId, contractId, holdPayout, isAnyPayoutHold } = req.body

  await updateContract(
    {
      _id: contractId,
      partnerId
    },
    {
      holdPayout
    },
    session
  )

  if (isAnyPayoutHold) {
    await payoutService.updatePayouts(
      {
        contractId,
        partnerId
      },
      {
        holdPayout
      },
      session
    )
  }

  return {
    result: true
  }
}

export const sendCPINotification = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['contractId', 'createdAt'], body)
  const { contractId, createdAt } = body

  const contract = await contractHelper.getAContract(
    {
      _id: contractId,
      status: { $in: ['active', 'upcoming'] }
    },
    session,
    ['partnerSetting']
  )

  if (!size(contract) || !size(contract.partnerSetting)) {
    throw new CustomError(404, 'Please provide valid contract')
  }

  const { partnerSetting } = contract

  const cpiDate = (
    await appHelper.getActualDate(partnerSetting, true, createdAt)
  )
    .add(1, 'months')
    .endOf('day')
    .toDate()
  console.log('===> CPI date', cpiDate, new Date())
  const cpi_months_before_index = partnerSetting?.CPISettlement?.months
  const isStopCPIRegulation = partnerSetting?.stopCPIRegulation || false

  if (isStopCPIRegulation) {
    console.log('===> CPI Regulation is stopped for partner: ', contractId)
  }

  const isFutureMonthlyRentAvailable =
    await invoiceHelper.isAddFutureMonthlyRent(contract, partnerSetting)

  console.log('isFutureMonthlyRentAvailable', isFutureMonthlyRentAvailable)
  let updatedContract = false
  if (
    !contract.rentalMeta?.lastCPINotificationSentOn &&
    (!contract.rentalMeta.contractEndDate ||
      contract.rentalMeta.contractEndDate > contract.rentalMeta.nextCpiDate) &&
    isFutureMonthlyRentAvailable
  ) {
    console.log('+++ Preparing data for app Queue', contractId)
    const cpiNotificationSentHistory = [
      ...(contract?.rentalMeta?.cpiNotificationSentHistory || []),
      createdAt
    ]
    console.log('+++ cpiNotificationSentHistory', cpiNotificationSentHistory)

    const sendNotificationDate = contract.rentalMeta?.lastCpiDate
      ? (
          await appHelper.getActualDate(
            partnerSetting,
            true,
            contract.rentalMeta.lastCpiDate
          )
        )
          .subtract(cpi_months_before_index, 'months')
          .toDate()
      : ''

    console.log('+++ sendNotificationDate', sendNotificationDate)

    const getCPIInMonth = await contractHelper.getCPIInMonthDate({
      contractInfo: contract,
      partnerSettings: partnerSetting
    })
    const futureMonthlyRentAmount = await contract.getCPINextMonthlyRentAmount()
    console.log('+++ getCPIInMonth', getCPIInMonth)
    const updateData = {
      $set: {
        'rentalMeta.lastCPINotificationSentOn': cpiDate,
        'rentalMeta.futureRentAmount': futureMonthlyRentAmount,
        'rentalMeta.cpiNotificationSentHistory': cpiNotificationSentHistory,
        'rentalMeta.cpiFromMonth': sendNotificationDate,
        'rentalMeta.cpiInMonth': getCPIInMonth
      }
    }

    updatedContract = await updateContract(
      {
        _id: contractId,
        partnerId: contract.partnerId,
        status: { $in: ['active', 'upcoming'] },
        'rentalMeta.cpiEnabled': true,
        'rentalMeta.lastCPINotificationSentOn': { $exists: false },
        'rentalMeta.nextCpiDate': { $lte: cpiDate }
      },
      updateData,
      session
    )

    const appQueuesData = {
      destination: 'notifier',
      event: 'send_CPI_settlement_notice',
      action: 'send_notification',
      params: {
        partnerId: contract.partnerId,
        collectionId: contractId,
        collectionNameStr: 'contracts'
      },
      priority: 'immediate'
    }
    if (updatedContract) {
      await appQueueService.createAnAppQueue(appQueuesData, session)
    }
  }

  if (
    !contract.rentalMeta?.lastCPINotificationSentOn &&
    !updatedContract &&
    !isFutureMonthlyRentAvailable
  ) {
    console.log(
      '+++ Preparing data for update contract next CPI date',
      contractId
    )
    const nextCpiDate = contract.rentalMeta.nextCpiDate
    console.log('+++ Preparing nextCpiDate ', nextCpiDate)
    const updatedNextCpiDate = (
      await appHelper.getActualDate(partnerSetting, true)
    )
      .add(12, 'months')
      .endOf('day')
      .toDate()

    updatedContract = await updateContract(
      {
        _id: contractId,
        status: { $in: ['active', 'upcoming'] },
        'rentalMeta.cpiEnabled': true,
        'rentalMeta.lastCPINotificationSentOn': { $exists: false },
        'rentalMeta.nextCpiDate': { $lte: cpiDate }
      },
      { $set: { nextCpiDate: updatedNextCpiDate } },
      session
    )
    console.log('Updated contract next cpi date', !!updatedContract)
    if (updatedContract) {
      await createContractUpdateChangeLog(
        {
          action: 'updated_lease',
          context: 'property',
          previousContract: contract,
          updatedContract
        },
        session
      )
    }
  }

  if (
    contract.rentalMeta?.lastCPINotificationSentOn
    // updatedContract?.rentalMeta?.lastCPINotificationSentOn
  ) {
    console.log('Removing last cpi notification send on')
    const result = await updateContract(
      {
        _id: contractId,
        status: { $in: ['active', 'upcoming'] },
        'rentalMeta.cpiEnabled': true,
        'rentalMeta.lastCPINotificationSentOn': { $exists: true },
        'rentalMeta.futureRentAmount': { $exists: true },
        'rentalMeta.nextCpiDate': { $exists: true, $gte: cpiDate }
      },
      {
        $unset: {
          'rentalMeta.futureRentAmount': 1,
          'rentalMeta.lastCPINotificationSentOn': 1
        }
      },
      session
    )
    if (result?._id) {
      console.log('Creating app queue for cpi notice')
      const appQueuesData = {
        destination: 'notifier',
        event: 'send_CPI_settlement_notice',
        action: 'send_notification',
        params: {
          partnerId: contract.partnerId,
          collectionId: contractId,
          collectionNameStr: 'contracts'
        },
        priority: 'immediate'
      }
      await appQueueService.createAnAppQueue(appQueuesData, session)
    }
  }

  return {
    code: 200,
    msg: 'Successfully updated next cpi date'
  }
}

export const updateCpiContractRentAmount = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(['contractId'], body)
  const { contractId } = body

  const contract = await contractHelper.getAContract(
    {
      _id: contractId,
      status: { $in: ['active', 'upcoming'] }
    },
    session,
    ['partnerSetting']
  )

  if (!size(contract) || !size(contract.partnerSetting)) {
    throw new CustomError(404, 'Please provide valid contract')
  }

  const { partnerSetting } = contract

  const isFutureMonthlyRentAvailable =
    await invoiceHelper.isAddFutureMonthlyRent(contract, partnerSetting)

  console.log('isFutureMonthlyRentAvailable', isFutureMonthlyRentAvailable)

  if (
    contract.rentalMeta.futureRentAmount &&
    contract.rentalMeta.lastCPINotificationSentOn &&
    isFutureMonthlyRentAvailable
  ) {
    const nextCpiDate = (await appHelper.getActualDate(partnerSetting, true))
      .add(12, 'months')
      .endOf('day')
      .toDate()

    const lastCpiDate = (await appHelper.getActualDate(partnerSetting, true))
      .endOf('day')
      .toDate()

    const updateData = {
      $set: {
        'rentalMeta.monthlyRentAmount': contract.rentalMeta.futureRentAmount
          ? contract.rentalMeta.futureRentAmount
          : contract.rentalMeta.monthlyRentAmount,
        'rentalMeta.lastCpiDate': lastCpiDate,
        'rentalMeta.nextCpiDate': nextCpiDate
      },
      $unset: {
        'rentalMeta.futureRentAmount': 1,
        'rentalMeta.lastCPINotificationSentOn': 1
      }
    }

    const updatedContract = await updateContract(
      {
        _id: contractId,
        partnerId: contract.partnerId
      },
      updateData,
      session
    )

    console.log('Updated contract next cpi amount', !!updatedContract)
    if (updatedContract) {
      await createContractUpdateChangeLog(
        {
          action: 'updated_lease',
          context: 'property',
          previousContract: contract,
          updatedContract
        },
        session
      )

      await createContractUpdateChangeLog(
        {
          action: 'updated_lease',
          context: 'property',
          previousContract: contract,
          updatedContract,
          fieldName: 'monthlyRentAmount',
          CPIBasedIncrement: true
        },
        session
      )
    }
  }

  return {
    code: 200,
    msg: 'Successfully updated next cpi amount'
  }
}

export const initiateMonthlyCreateInvoiceJob = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['appHealthId'], body)
  const { appHealthId } = body
  const appHealthInfo = await appHealthHelper.getAnAppHeath({
    _id: appHealthId
  })
  if (!size(appHealthInfo?.partnerId))
    throw new CustomError(404, 'App health info not found')
  const { partnerId } = appHealthInfo

  const appQueueData = {
    action: 'initial_daily_rent_invoice_create',
    destination: 'invoice',
    event: 'initial_daily_rent_invoice_create',
    params: { dataToSkip: 0, partnerId },
    priority: 'regular'
  }
  await appQueueService.createAnAppQueue(appQueueData)

  return {
    code: 202,
    msg: 'Request has been accepted for processing, please wait for a while'
  }
}

export const propertyAfterUpdateProcess = async (property = {}, session) => {
  const previousProperty = await listingHelper.getAListing({
    _id: property?._id
  })

  if (
    size(previousProperty) &&
    (property.hasActiveLease !== previousProperty.hasActiveLease ||
      property.hasAssignment !== previousProperty.hasAssignment)
  ) {
    const totalActiveProperties = await listingHelper.countListings(
      {
        partnerId: property.partnerId,
        accountId: property.accountId,
        $or: [{ hasActiveLease: true }, { hasAssignment: true }]
      },
      session
    )

    const hasInProgressLease = await contractHelper.countContracts(
      {
        accountId: property.accountId,
        status: 'in_progress'
      },
      session
    )

    const accountsData = {
      status: 'in_progress',
      totalActiveProperties
    }

    if (totalActiveProperties && !hasInProgressLease)
      accountsData.status = 'active'

    const updatedAccounts = await accountService.updateAccounts(
      {
        _id: property.accountId,
        partnerId: property.partnerId
      },
      {
        $set: accountsData
      },
      session
    )
    return updatedAccounts
  }
}

const prepareUnsetData = (body) => {
  const { unsetData } = body
  const {
    unsetDepositAccountCreationTestProcessing,
    unsetIncomingPaymentTestProcessing
  } = unsetData
  const preparedUnsetData = {}
  if (unsetDepositAccountCreationTestProcessing) {
    preparedUnsetData['rentalMeta.isDepositAccountCreationTestProcessing'] = 1
  }
  if (unsetIncomingPaymentTestProcessing) {
    preparedUnsetData['rentalMeta.isDepositAccountPaymentTestProcessing'] = 1
  }
  console.log('== Checking preparedUnsetData: ', preparedUnsetData)
  if (!size(preparedUnsetData)) return {}
  return { $unset: preparedUnsetData }
}

export const unsetContractData = async (req) => {
  const { body, session } = req
  const updateData = prepareUnsetData(body)
  console.log('== Checking updateData: ', updateData)
  const { contractId } = body
  console.log('== Checking contractId: ', contractId)
  const updated = await updateContract({ _id: contractId }, updateData, session)
  return updated
}

export const downloadAgedDebtorsReport = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const {
    accountId,
    agentId,
    branchId,
    createdAtDateRange,
    due,
    partnerId,
    propertyId,
    sort = {},
    tenantId,
    userId
  } = body
  const params = {
    accountId,
    agentId,
    branchId,
    downloadProcessType: 'download_aged_debtors_report',
    due,
    partnerId,
    propertyId,
    tenantId,
    userId
  }
  if (size(sort)) {
    appHelper.validateSortForQuery(sort)
    if (sort['propertyInfo.location.name']) {
      sort.propertyInfo_location_name = sort['propertyInfo.location.name']
      delete sort['propertyInfo.location.name']
    }
    params.sort = sort
  } else {
    params.sort = {
      propertyInfo_location_name: 1
    }
  }
  if (size(createdAtDateRange)) {
    const { startDate, endDate } = createdAtDateRange
    params.createdAtDateRange = {
      startDate: new Date(startDate),
      endDate: new Date(endDate)
    }
  }
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'
  const queueData = {
    action: 'download_email',
    event: 'download_email',
    destination: 'excel-manager',
    params,
    priority: 'immediate',
    status: 'new'
  }
  await appQueueService.createAnAppQueue(queueData)
  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}

export const cancelLeaseTermination = async (req) => {
  appHelper.validatePartnerAppRequestData(req, ['contractId'])
  const { body = {}, session } = req
  const preparedData = await contractHelper.prepareContractCancelTermination(
    body,
    session
  )
  if (!preparedData?.updateData) {
    throw new CustomError(400, 'Lease update data not found')
  }

  const { contractId } = body
  const contract = await contractHelper.getAContract(
    { _id: contractId },
    session
  )
  if (!contract) {
    throw new CustomError(404, 'Lease not found')
  }

  if (
    contract?.rentalMeta?.status !== 'active' ||
    !contract?.rentalMeta?.terminatedBy
  ) {
    throw new CustomError(400, 'Lease is not available for cancel termination')
  }

  const updatedContract = await updateContract(
    { _id: contractId },
    preparedData.updateData,
    session
  )

  await createLogForCancelTermination(
    { userId: body.userId, contract },
    session
  )

  if (updatedContract && preparedData?.isChangedContractEndDate) {
    await appQueueService.createAppQueueForCreateRentInvoice(
      {
        contractId: updatedContract._id,
        enabledNotification: updatedContract.rentalMeta?.enabledNotification,
        partnerId: updatedContract.partnerId,
        today: new Date(),
        userId: body.userId
      },
      session,
      'regular'
    )

    await createContractUpdateChangeLog(
      {
        action: 'updated_lease',
        context: 'property',
        fieldName: 'contractEndDate',
        updatedContract,
        userId: body.userId,
        previousContract: preparedData.previousContract
      },
      session
    )
  }

  return {
    msg: 'Successfully canceled lease termination',
    code: 200
  }
}

export const createLogForCancelTermination = async (params, session) => {
  const logData = contractHelper.prepareLogDataForCancelTermination(
    params,
    session
  )

  if (!size(params)) {
    throw new CustomError(
      404,
      'Log data not found for cancel lease termination'
    )
  }
  await logService.createLog(logData, session)
}
