import { size } from 'lodash'
import nid from 'nid'
import { CustomError } from '../common'
import { CommissionCollection } from '../models'
import {
  appHelper,
  commissionHelper,
  invoiceHelper,
  userHelper
} from '../helpers'
import {
  appQueueService,
  invoiceService,
  logService,
  transactionService
} from '../services'

export const createCommission = async (commissionData, session) => {
  const commission = await CommissionCollection.create([commissionData], {
    session
  })
  return commission
}

export const createCommissions = async (commissions = [], session) => {
  const response = await CommissionCollection.insertMany(commissions, {
    session
  })
  return response
}

export const updateCommission = async (query, data, session) => {
  const updatedCommission = await CommissionCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  return updatedCommission
}

export const updateCommissions = async (query, data, session) => {
  const response = await CommissionCollection.updateMany(query, data, {
    session
  })
  return response
}

export const addAddonCommission = async (
  invoiceData,
  propertyContractInfo,
  session
) => {
  let invoiceCommissionableTotal = invoiceData.commissionableTotal
  const { addonsMeta } = invoiceData
  if (!(size(invoiceData) && size(addonsMeta))) {
    return invoiceCommissionableTotal
  }
  const params = {
    addonsMeta,
    invoiceData,
    propertyContractInfo,
    invoiceCommissionableTotal
  }
  const { commissionsData, commissionTotal } =
    await commissionHelper.prepareAddonCommissionData(params)
  invoiceCommissionableTotal = commissionTotal
  if (size(commissionsData)) {
    await addCommissionsToCollection(commissionsData, session)
  }
  return invoiceCommissionableTotal
}

export const addBrokeringCommission = async (
  invoiceData,
  propertyContractInfo = {},
  session
) => {
  if (
    propertyContractInfo.hasBrokeringContract &&
    propertyContractInfo.brokeringCommissionAmount
  ) {
    const monthlyRentAmount = commissionHelper.getMonthlyRentTotal(
      invoiceData.invoiceContent
    )
    const brokeringCommissionParams = {
      invoiceData,
      monthlyRentAmount,
      propertyContractInfo,
      isEstimatedPayouts: false
    }
    const brokeringCommissionData =
      await commissionHelper.prepareBrokeringCommissionData(
        brokeringCommissionParams
      )
    if (brokeringCommissionData.amount > 0) {
      const insertedCommission = await addCommissionToCollection(
        brokeringCommissionData,
        session
      )
      return insertedCommission
    }
  }
}

export const addManagementCommission = async (params, session) => {
  const { invoiceData, invoiceCommissionableTotal, propertyContractInfo } =
    params
  const managementAddData = await commissionHelper.prepareManagementAddData({
    invoiceData,
    invoiceCommissionableTotal,
    propertyContractInfo
  })
  if (managementAddData.amount > 0) {
    await addCommissionToCollection(managementAddData, session)
  }
}

export const addAssignmentAddonIncome = async (
  invoiceData,
  propertyContractInfo,
  session
) => {
  const contractAddons =
    propertyContractInfo && size(propertyContractInfo.addons)
      ? propertyContractInfo.addons
      : []
  if (!(invoiceData.isFirstInvoice && size(contractAddons))) {
    return
  }
  const assignmentIncomeData =
    await commissionHelper.prepareAssignmentIncomeData(
      invoiceData,
      contractAddons,
      session
    )
  if (size(assignmentIncomeData)) {
    await addCommissionsToCollection(assignmentIncomeData, session)
  }
}

export const addInvoiceCommissions = async (params = {}, session) => {
  const {
    adjustmentNotNeeded,
    invoiceData,
    partner,
    partnerSetting,
    propertyContractInfo,
    userId
  } = params
  const parsedInvoiceData = JSON.parse(JSON.stringify(invoiceData))
  if (size(propertyContractInfo)) {
    if (invoiceData && invoiceData.isFirstInvoice) {
      await addBrokeringCommission(
        parsedInvoiceData,
        propertyContractInfo,
        session
      )
    }
    const updatedCommissionTotal = await addAddonCommission(
      parsedInvoiceData,
      propertyContractInfo,
      session
    )
    await addManagementCommission(
      {
        invoiceData: parsedInvoiceData,
        invoiceCommissionableTotal: updatedCommissionTotal,
        propertyContractInfo
      },
      session
    )
    await addAssignmentAddonIncome(
      parsedInvoiceData,
      propertyContractInfo,
      session
    )
    await appQueueService.createAppQueueForAddingSerialId(
      'commissions',
      {
        partnerId: partner._id
      },
      session
    )
    if (invoiceData.invoiceType === 'invoice') {
      const commissionParams = {
        adjustmentNotNeeded,
        contract: propertyContractInfo,
        dueDate: invoiceData.dueDate,
        invoiceEndOn: invoiceData.invoiceEndOn,
        invoiceId: invoiceData._id,
        invoiceStartOn: invoiceData.invoiceStartOn,
        partner,
        partnerId: invoiceData.partnerId,
        partnerSetting,
        userId
      }
      await invoiceService.addLandlordInvoicesForCommission(
        commissionParams,
        session
      )
    }
  }
}

export const createLog = async (commission, session) => {
  const logData = commissionHelper.prepareLogData(commission)
  const insertedLog = await logService.createLog(logData, session)
  return insertedLog
}

export const initAfterInsertProcess = async (commission, session) => {
  await createLog(commission, session)
  const { commissionId, _id, amount } = commission
  if (commissionId && _id) {
    const query = { _id: commissionId }
    const data = {
      refundCommissionId: _id,
      refundCommissionAmount: amount
    }
    await updateCommission(query, data, session)
  }
}

export const addCommissionToCollection = async (commissionData, session) => {
  const [insertedCommission] = await CommissionCollection.create(
    [commissionData],
    {
      session
    }
  )
  if (!size(insertedCommission)) {
    throw new CustomError(400, 'Unable to create commission')
  }
  await initAfterInsertProcess(insertedCommission, session)
  return insertedCommission
}

export const addCommissionsToCollection = async (commissionsData, session) => {
  const commissions = []
  const promiseArr = []
  const commissionLogs = []
  for (const commission of commissionsData) {
    commission._id = nid(17)
    commissions.push(commission)
    const { amount } = commission
    if (commission.commissionId) {
      const query = { _id: commission.commissionId }
      const data = {
        refundCommissionId: commission._id,
        refundCommissionAmount: amount
      }
      promiseArr.push(updateCommission(query, data, session))
    }
    const logData = commissionHelper.prepareLogData(commission)
    commissionLogs.push({
      ...logData,
      _id: nid(17)
    })
  }
  if (size(commissions)) {
    await createCommissions(commissions, session)
  }
  if (size(promiseArr)) await Promise.all(promiseArr)
  if (size(commissionLogs)) {
    await logService.createLogs(commissionLogs, session)
  }
  return commissions
}

export const addCommissionTransaction = async (
  commission,
  transactionEvent,
  session
) => {
  const { type = '' } = commission
  const accountingType = await commissionHelper.getAccountingType(type)
  const isTransactionExists =
    await commissionHelper.isCommissionTransactionExists(
      commission,
      accountingType,
      session
    )
  if (isTransactionExists) {
    return false
  }
  const transactionData = await commissionHelper.prepareTransactionData(
    commission,
    accountingType,
    transactionEvent
  )
  const transaction = await transactionService.createTransaction(
    transactionData,
    session
  )
  return transaction
}

export const downloadCommission = async (req) => {
  const { body, session, user } = req
  const { partnerId, userId } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  body.partnerId = partnerId

  const commissionQuery = await commissionHelper.prepareCommissionsQuery(body)

  await appHelper.isMoreOrLessThanTargetRows(
    CommissionCollection,
    commissionQuery,
    {
      moduleName: 'Commissions'
    }
  )

  const {
    accountId,
    agentId,
    branchId,
    dateRange,
    propertyId,
    sort = { createdAt: -1 },
    tenantId,
    type
  } = body

  appHelper.validateSortForQuery(sort)
  const params = {}

  if (type) params.type = type

  if (size(dateRange)) {
    params.dateRange = {
      startDate: new Date(dateRange.startDate),
      endDate: new Date(dateRange.endDate)
    }
  }
  if (tenantId) {
    appHelper.validateId({ tenantId })
    params.tenantId = tenantId
  }
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
  params.downloadProcessType = 'download_commissions'
  const userInfo = await userHelper.getAnUser({ _id: userId })
  params.userLanguage = userInfo?.profile?.language || 'en'

  const queueData = {
    action: 'download_email',
    event: 'download_email',
    priority: 'immediate',
    destination: 'excel-manager',
    status: 'new',
    params
  }

  const payoutQueue = await appQueueService.createAnAppQueue(queueData, session)
  if (size(payoutQueue)) {
    return {
      status: 200,
      message:
        'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
    }
  } else {
    throw new CustomError(404, `Unable to download payout`)
  }
}

export const addInvoiceCommissionsService = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['invoiceId'], body)
  const { adjustmentNotNeeded, invoiceId } = body
  const invoice = await invoiceHelper.getInvoice(
    {
      _id: invoiceId
    },
    undefined,
    ['contract', 'partner', 'partnerSetting']
  )
  if (!size(invoice)) {
    throw new CustomError(404, 'Invoice not found')
  } else if (!size(invoice.contract)) {
    throw new CustomError(404, 'Invoice contract not found')
  } else if (!size(invoice.partnerSetting)) {
    throw new CustomError(404, 'Partner setting not found')
  } else if (!size(invoice.partner)) {
    throw new CustomError(404, 'Partner not found')
  }
  const params = {
    adjustmentNotNeeded,
    invoiceData: invoice,
    partner: invoice.partner,
    partnerSetting: invoice.partnerSetting,
    propertyContractInfo: invoice.contract,
    userId: invoice.createdBy
  }
  await addInvoiceCommissions(params, session)
  return {
    result: true
  }
}
