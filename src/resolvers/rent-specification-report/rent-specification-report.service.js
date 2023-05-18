import nid from 'nid'
import moment from 'moment-timezone'
import { compact, find, isEmpty, map, size, sum } from 'lodash'
import { CustomError } from '../common'
import { RentSpecificationReportCollection } from '../models'
import {
  addonHelper,
  appHelper,
  appQueueHelper,
  contractHelper,
  invoiceHelper,
  rentSpecificationReportHelper,
  transactionHelper,
  userHelper
} from '../helpers'
import { appQueueService } from '../services'

export const addRentSpecificationReport = async (data, session) => {
  if (!size(data)) {
    throw new CustomError(
      404,
      'No data found for rent specification report creation'
    )
  }
  const createdRentSpecificationReport =
    await RentSpecificationReportCollection.create([data], {
      session
    })
  if (isEmpty(createdRentSpecificationReport)) {
    throw new CustomError(404, 'Unable to create rent specification report')
  }
  return createdRentSpecificationReport
}

export const addMultipleRentSpecificationReports = async (data, session) => {
  if (!(size(data) && Array.isArray(data))) {
    throw new CustomError(
      400,
      'Rent specification reports creation data not found!'
    )
  }
  const createdRentSpecificationReports =
    await RentSpecificationReportCollection.insertMany(data, {
      session,
      runValidators: true
    })
  if (isEmpty(createdRentSpecificationReports)) {
    throw new CustomError(404, 'Unable to create rent specification reports!')
  }
  return createdRentSpecificationReports
}

export const addRentSpecificationReports = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  appHelper.checkRequiredFields(
    ['accountingEndDate', 'accountingStartDate', 'contractId', 'partnerId'],
    body
  )

  appHelper.validateId({ contractId: body.contractId })
  appHelper.validateId({ partnerId: body.partnerId })

  const addingResult = await prepareAndAddRentSpecificationReports(
    body,
    session
  )
  return addingResult
}

const prepareAndAddRentSpecificationReports = async (params = {}, session) => {
  const { accountingEndDate, accountingStartDate, contractId, partnerId } =
    params || {}
  const contractInfo = await contractHelper.getAContract(
    { _id: contractId, partnerId },
    session,
    [{ path: 'partner', populate: ['partnerSetting'] }]
  )
  console.log(
    '====> Checking contract info:',
    contractInfo?._id,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  if (isEmpty(contractInfo)) {
    throw new CustomError(
      400,
      'Could not find contract info with the contractId!'
    )
  }

  const {
    addons = [],
    accountId,
    branchId,
    agentId,
    partner: partnerInfo,
    propertyId,
    rentalMeta
  } = contractInfo
  const { partnerSetting = {} } = partnerInfo || {}
  const {
    contractStartDate,
    contractEndDate,
    isVatEnable = false,
    monthlyRentAmount = 0,
    tenantId
  } = rentalMeta
  const rentType = isVatEnable ? 'rent_with_vat' : 'rent'

  const paramsForPreparingReports = {
    accountId,
    accountingStartDate,
    accountingEndDate,
    addonsFromLease: addons,
    agentId,
    branchId,
    contractId,
    contractStartDate,
    contractEndDate,
    isVatEnable,
    monthlyRentAmount,
    partnerId,
    partnerSetting,
    propertyId,
    rentalMeta,
    rentType,
    tenantId
  }
  const rentSpecificationReports = await prepareRentSpecificationReports(
    paramsForPreparingReports,
    session
  )
  const addedRentSpecificationReports =
    await addMultipleRentSpecificationReports(rentSpecificationReports)
  return addedRentSpecificationReports
}

export const deleteRentSpecificationReportsOfContracts = async (
  contractIds = [],
  session
) => {
  if (!size(contractIds)) return false

  const { deletedCount } = await RentSpecificationReportCollection.deleteMany(
    {
      contractId: { $in: contractIds }
    },
    { session }
  )
  return deletedCount
}

const getAllTransactionsForContract = async (params, session) => {
  const { contractId, partnerId, rentType } = params
  const queryForTransactions = {
    contractId,
    partnerId,
    type: { $in: ['invoice', 'credit_note'] }
  }
  const queryForRentTransactions = {
    ...queryForTransactions,
    subType: { $in: [rentType, 'loss_recognition', 'rounded_amount'] }
  }
  const allRentTransactionsOfContract = await transactionHelper.getTransactions(
    queryForRentTransactions,
    session
  )
  const queryForAddonTransactions = {
    ...queryForTransactions,
    subType: 'addon'
  }
  const allAddonTransactionsOfContract =
    await transactionHelper.getTransactions(queryForAddonTransactions, session)
  const subTypesOfFees = [
    'invoice_fee',
    'invoice_reminder_fee',
    'collection_notice_fee',
    'eviction_notice_fee',
    'administration_eviction_notice_fee',
    'reminder_fee_move_to',
    'collection_notice_fee_move_to',
    'eviction_notice_fee_move_to',
    'administration_eviction_notice_fee_move_to',
    'unpaid_reminder',
    'unpaid_collection_notice',
    'unpaid_eviction_notice',
    'unpaid_administration_eviction_notice'
  ]
  const queryForFeesTransactions = {
    ...queryForTransactions,
    subType: { $in: subTypesOfFees }
  }
  const allFeesTransactionsOfContract = await transactionHelper.getTransactions(
    queryForFeesTransactions,
    session
  )
  const queryForCorrectionTransactions = {
    ...queryForTransactions,
    type: 'correction',
    subType: { $in: ['addon', 'rounded_amount'] }
  }
  const allCorrectionsOfContract = await transactionHelper.getTransactions(
    queryForCorrectionTransactions,
    session
  )
  return {
    allAddonTransactionsOfContract,
    allCorrectionsOfContract,
    allFeesTransactionsOfContract,
    allRentTransactionsOfContract
  }
}

const prepareRentSpecificationReports = async (params, session) => {
  const {
    accountId,
    accountingStartDate,
    accountingEndDate,
    addonsFromLease,
    agentId,
    branchId,
    contractId,
    contractStartDate,
    contractEndDate,
    isVatEnable,
    monthlyRentAmount,
    partnerId,
    partnerSetting,
    propertyId,
    tenantId,
    rentType
  } = params
  const paramsForGettingTransactions = {
    contractId,
    partnerId,
    partnerSetting,
    rentType
  }
  const transactionsList = await getAllTransactionsForContract(
    paramsForGettingTransactions,
    session
  )
  const {
    allAddonTransactionsOfContract,
    allCorrectionsOfContract,
    allFeesTransactionsOfContract,
    allRentTransactionsOfContract
  } = transactionsList || {}
  console.log(
    '====> Checking allAddonTransactionsOfContract:',
    size(allAddonTransactionsOfContract),
    ', allCorrectionsOfContract:',
    size(allCorrectionsOfContract),
    ', allFeesTransactionsOfContract:',
    size(allFeesTransactionsOfContract),
    ', allRentTransactionsOfContract:',
    size(allRentTransactionsOfContract),
    'for contractId:',
    contractId,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  const addonIds = size(addonsFromLease) ? map(addonsFromLease, 'addonId') : []
  console.log(
    '====> Checking addonIds:',
    size(addonIds),
    'for contractId:',
    contractId,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  const addonsList = await addonHelper.getAddons(
    { _id: { $in: addonIds } },
    session
  )
  console.log(
    '====> Checking addonsList:',
    size(addonsList),
    'for contractId:',
    contractId,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  const accountingPeriods =
    await transactionHelper.getAccountingPeriodsForQuery({
      contractId,
      createdAt: {
        $gte: new Date(accountingStartDate),
        $lte: new Date(accountingEndDate)
      }
    })
  console.log(
    '====> Checking accountingPeriods:',
    accountingPeriods,
    'for contractId:',
    contractId,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  const listsOfMonths =
    accountingStartDate && accountingEndDate
      ? await invoiceHelper.getListOfMonths({
          endMonthDate: accountingEndDate,
          partnerSetting,
          startMonthDate: accountingStartDate
        })
      : []
  console.log(
    '====> Checking listsOfMonths:',
    listsOfMonths,
    'for contractId:',
    contractId,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  const lengthOfListOfMonths = size(listsOfMonths)
  const rentSpecificationReports = []
  if (lengthOfListOfMonths) {
    for (const [index, month] of listsOfMonths.entries()) {
      console.log(
        '====> Started rent specification data generating for month:',
        month,
        'for contractId:',
        contractId,
        'for partnerId:',
        partnerId,
        'for generating rent specification reports <===='
      )
      const transactionPeriod = (
        await appHelper.getActualDate(partnerSetting, true, month)
      )
        .date(2)
        .toDate()
      const periodStartDate =
        index === 0
          ? await appHelper.getActualDate(
              partnerSetting,
              false,
              accountingStartDate
            )
          : (
              await appHelper.getActualDate(
                partnerSetting,
                true,
                transactionPeriod
              )
            )
              .startOf('month')
              .toDate()
      const periodEndDate =
        lengthOfListOfMonths - index === 1
          ? await appHelper.getActualDate(
              partnerSetting,
              false,
              accountingEndDate
            )
          : (
              await appHelper.getActualDate(
                partnerSetting,
                true,
                transactionPeriod
              )
            )
              .endOf('month')
              .toDate()
      const rent = isVatEnable ? 0 : monthlyRentAmount
      const rentWithVat = isVatEnable ? monthlyRentAmount : 0
      const { estimatedAddonsMeta, estimatedAddonsTotal } =
        (await getEstimatedAddonsInfo(addonsFromLease, addonsList)) || {}
      const totalMonthly = monthlyRentAmount + estimatedAddonsTotal
      const durationOfPeriod =
        (await getDurationOfPeriodForContract(
          partnerSetting,
          periodStartDate,
          periodEndDate
        )) || {}
      const estimatedTotalPeriod = await getTotalPeriod({
        totalCorrections: 0,
        durationOfPeriod,
        totalFees: 0,
        partnerSetting,
        periodStartDate,
        totalMonthly
      })
      console.log(
        '====> Started checking has transactions for contractId:',
        contractId,
        'for partnerId:',
        partnerId,
        'for generating rent specification reports <===='
      )
      const hasTransactions =
        getTransactionStatusForPeriod({
          accountingPeriods,
          periodStartDate,
          periodEndDate
        }) || false
      console.log(
        '====> Ended checking has transactions:',
        hasTransactions,
        'for contractId:',
        contractId,
        'for partnerId:',
        partnerId,
        'for generating rent specification reports <===='
      )
      const rentSpecBasicData = {
        partnerId,
        accountId,
        agentId,
        branchId,
        propertyId,
        contractId,
        tenantId,
        contractStartDate,
        contractEndDate,
        transactionPeriod,
        rent,
        rentWithVat,
        estimatedAddonsMeta,
        totalEstimatedAddons: estimatedAddonsTotal,
        totalMonthly,
        months: durationOfPeriod.months,
        days: durationOfPeriod.days,
        estimatedTotalPeriod
      }

      if (hasTransactions) {
        const periodStrings = getAccountingPeriodStrings({
          accountingPeriods,
          periodStartDate,
          periodEndDate
        })
        console.log(
          '====> Checking periodStrings:',
          periodStrings,
          'for contractId:',
          contractId,
          'for partnerId:',
          partnerId,
          'for generating rent specification reports <===='
        )
        if (size(periodStrings)) {
          for (const [periodIndex, period] of periodStrings.entries()) {
            const paramsForGettingTotalRent = {
              durationOfPeriod,
              hasTransactions,
              partnerSetting,
              period,
              periodStartDate,
              periodEndDate,
              totalMonthly: rent ? rent : rentWithVat,
              transactionsList: allRentTransactionsOfContract
            }
            const actualRentTotal = await getActualTotalFromTransactionsOrLease(
              paramsForGettingTotalRent
            )
            const totalRent = rent ? actualRentTotal : 0
            const totalRentWithVat = rentWithVat ? actualRentTotal : 0
            const paramsForGettingTotalAddons = {
              addonsFromLease: estimatedAddonsMeta,
              addonsList,
              allAddonTransactionsOfContract,
              durationOfPeriod,
              hasTransactions,
              partnerSetting,
              period,
              periodStartDate,
              periodEndDate
            }
            const { actualAddonsMeta, actualAddonsTotal } =
              await getActualAddonsInfo(paramsForGettingTotalAddons)
            const totalFees =
              (await getActualFeesTotal({
                allFeesTransactionsOfContract,
                period,
                periodStartDate,
                periodEndDate
              })) || 0
            const totalCorrections =
              (await getActualCorrectionsTotal({
                allCorrectionsOfContract,
                period,
                periodStartDate,
                periodEndDate
              })) || 0
            const actualTotalAmount =
              totalRent +
              totalRentWithVat +
              actualAddonsTotal +
              totalFees +
              totalCorrections
            const actualTotalPeriod = await appHelper.getRoundedAmount(
              actualTotalAmount,
              partnerSetting
            )
            const accountingPeriod = (
              await appHelper.getActualDate(
                partnerSetting,
                true,
                moment(period, 'YYYY-MM')
              )
            )
              .set('date', 2)
              .toDate()

            const rentSpecificationReport = {
              _id: nid(17),
              ...rentSpecBasicData,
              months: periodIndex === 0 ? durationOfPeriod.months : 0,
              days: periodIndex === 0 ? durationOfPeriod.days : 0,
              accountingPeriod,
              totalRent,
              totalRentWithVat,
              actualAddonsMeta,
              totalActualAddons: actualAddonsTotal,
              totalFees,
              totalCorrections,
              actualTotalPeriod,
              hasTransactions
            }
            rentSpecificationReports.push(rentSpecificationReport)
          }
        }
      } else {
        rentSpecificationReports.push({
          _id: nid(17),
          ...rentSpecBasicData,
          accountingPeriod: transactionPeriod,
          totalRent: 0,
          totalRentWithVat: 0,
          actualAddonsMeta: [],
          totalActualAddons: 0,
          totalFees: 0,
          totalCorrections: 0,
          actualTotalPeriod: 0,
          hasTransactions
        })
      }
      console.log(
        '====> Ended rent specification data generating for month:',
        month,
        'for contractId:',
        contractId,
        'for partnerId:',
        partnerId,
        'for generating rent specification reports <===='
      )
    }
  } else throw new CustomError(400, 'Invalid accounting period!')
  console.log(
    '====> Checking rentSpecificationReports:',
    size(rentSpecificationReports),
    'for contractId:',
    contractId,
    'for partnerId:',
    partnerId,
    'for generating rent specification reports <===='
  )
  return rentSpecificationReports
}

const getTransactionStatusForPeriod = (params) => {
  const { accountingPeriods, periodStartDate, periodEndDate } = params
  console.log('accountingPeriods =====>', accountingPeriods)
  const hasTransactions = find(accountingPeriods, (period) => {
    const { createdAtDates: dates = [] } = period
    const hasCreatedAt = find(
      dates,
      (date) => date >= periodStartDate && date <= periodEndDate
    )
    if (hasCreatedAt) return period
  })
  return !!size(hasTransactions)
}

const getAccountingPeriodStrings = (params) => {
  const { accountingPeriods, periodStartDate, periodEndDate } = params
  const periodStrings = map(accountingPeriods, (period) => {
    const { createdAtDates: dates = [], period: periodString } = period
    const hasCreatedAt = find(
      dates,
      (date) => date >= periodStartDate && date <= periodEndDate
    )
    if (hasCreatedAt) return periodString
  })
  return compact(periodStrings)
}

const getActualTotalFromTransactionsOrLease = async (params) => {
  const {
    durationOfPeriod,
    hasTransactions,
    partnerSetting,
    period,
    periodStartDate,
    periodEndDate,
    totalMonthly,
    transactionsList
  } = params
  let totalAmount = 0
  if (hasTransactions) {
    const amountsArray = map(transactionsList, (transaction) => {
      if (
        transaction?.createdAt >= periodStartDate &&
        transaction?.createdAt <= periodEndDate &&
        transaction?.period === period
      ) {
        if (transaction?.subType === 'loss_recognition')
          return transaction.amount * -1
        else return transaction.amount
      }
      return 0
    })
    totalAmount = await appHelper.getRoundedAmount(sum(amountsArray))
  } else {
    totalAmount = await getTotalPeriod({
      durationOfPeriod,
      partnerSetting,
      periodStartDate,
      totalMonthly
    })
  }
  return totalAmount
}

const getEstimatedAddonsInfo = async (addonsFromLease, addonsList) => {
  const estimatedAddonsMeta = []
  let estimatedAddonsTotal = 0
  if (size(addonsFromLease)) {
    for (const addon of addonsFromLease) {
      const { addonId, type, total } = addon
      const { name: addonName } =
        find(addonsList, (addonInfo) => addonInfo?._id === addonId) || {}

      if (type === 'lease' && addonId && addonName) {
        estimatedAddonsMeta.push({
          addonId,
          addonName,
          addonTotal: total
        })
        estimatedAddonsTotal += total
      }
    }
  }
  return { estimatedAddonsMeta, estimatedAddonsTotal }
}

const getActualAddonsInfo = async (params) => {
  const {
    addonsFromLease,
    addonsList,
    allAddonTransactionsOfContract,
    durationOfPeriod,
    hasTransactions,
    partnerSetting,
    period,
    periodStartDate,
    periodEndDate
  } = params
  const actualAddonsMeta = []
  let actualAddonsTotal = 0
  if (size(addonsFromLease)) {
    for (const addon of addonsFromLease) {
      const { addonId, addonName, addonTotal } = addon
      const { isRecurring = false } =
        find(addonsList, (addonInfo) => addonInfo?._id === addonId) || {}

      let addonAmount
      if (hasTransactions) {
        const addonAmountsArray = map(
          allAddonTransactionsOfContract,
          (transaction) => {
            if (
              transaction?.addonId === addonId &&
              transaction?.createdAt >= periodStartDate &&
              transaction?.createdAt <= periodEndDate &&
              transaction?.period === period
            ) {
              return transaction?.amount
            }
            return 0
          }
        )
        addonAmount = await appHelper.getRoundedAmount(sum(addonAmountsArray))
      } else {
        if (isRecurring) {
          addonAmount = await getTotalPeriod({
            durationOfPeriod,
            partnerSetting,
            periodStartDate,
            totalMonthly: addonTotal
          })
        }
      }
      addonAmount = Number(addonAmount)
      if (addonAmount && addonId && addonName) {
        actualAddonsMeta.push({
          addonId,
          addonName,
          addonTotal: addonAmount
        })
        actualAddonsTotal += addonAmount
      }
    }
  }
  return { actualAddonsMeta, actualAddonsTotal }
}

const getActualFeesTotal = async (params) => {
  const {
    allFeesTransactionsOfContract,
    period,
    periodStartDate,
    periodEndDate
  } = params
  const feesTransactionsForPeriod = map(
    allFeesTransactionsOfContract,
    (transaction) => {
      if (
        transaction?.createdAt >= periodStartDate &&
        transaction?.createdAt <= periodEndDate &&
        transaction?.period === period
      ) {
        return transaction?.amount
      } else {
        return 0
      }
    }
  )
  const feesTotal = await appHelper.getRoundedAmount(
    sum(feesTransactionsForPeriod)
  )
  return feesTotal
}

const getActualCorrectionsTotal = async (params) => {
  const { allCorrectionsOfContract, period, periodStartDate, periodEndDate } =
    params
  const correctionTransactionsForPeriod = map(
    allCorrectionsOfContract,
    (transaction) => {
      if (
        transaction?.createdAt >= periodStartDate &&
        transaction?.createdAt <= periodEndDate &&
        transaction?.period === period
      ) {
        return transaction?.amount
      } else {
        return 0
      }
    }
  )
  const correctionsTotal = await appHelper.getRoundedAmount(
    sum(correctionTransactionsForPeriod)
  )
  return correctionsTotal
}

const getDurationOfPeriodForContract = async (
  partnerSetting,
  periodStartDate,
  periodEndDate
) => {
  let months = 0
  let days = 0
  const periodStartDateMoment = await appHelper.getActualDate(
    partnerSetting,
    true,
    periodStartDate
  )
  const periodEndDateMoment = await appHelper.getActualDate(
    partnerSetting,
    true,
    periodEndDate
  )
  const periodStartDay = parseInt(periodStartDateMoment.format('D'))
  const periodEndDay = parseInt(periodEndDateMoment.format('D'))
  const totalDaysInMonth = parseInt(periodStartDateMoment.daysInMonth())
  const actualDaysInMonth =
    periodEndDateMoment.diff(periodStartDateMoment, 'days') + 1

  if (periodStartDay === 1 && periodEndDay === totalDaysInMonth) {
    months = 1
    days = 0
  } else {
    months = 0
    days = actualDaysInMonth
  }

  return { months, days }
}

const getTotalPeriod = async ({
  totalCorrections = 0,
  durationOfPeriod = {},
  totalFees = 0,
  partnerSetting = {},
  periodStartDate,
  totalMonthly = 0
}) => {
  let totalPeriodAmount = 0
  const { months = 0, days = 0 } = durationOfPeriod
  const totalDaysInMonth = (
    await appHelper.getActualDate(partnerSetting, true, periodStartDate)
  ).daysInMonth()
  const perDayRentAmount = totalMonthly / totalDaysInMonth
  const totalMonthlyForDays = days ? perDayRentAmount * days : 0

  if (months) totalPeriodAmount += months * totalMonthly
  if (totalMonthlyForDays) totalPeriodAmount += totalMonthlyForDays
  if (totalFees) totalPeriodAmount += totalFees
  if (totalCorrections) totalPeriodAmount += totalCorrections
  totalPeriodAmount = await appHelper.getRoundedAmount(totalPeriodAmount)

  return totalPeriodAmount
}

export const resetRentSpecificationReports = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { session, user = {} } = req
  const { partnerId } = user

  const pendingQueueQuery = {
    action: 'get_contract_ids_and_create_app_queues',
    event: 'rent_specification_reports_generation',
    'params.partnerId': partnerId,
    'params.isForReset': true,
    status: { $in: ['new', 'on_flight', 'sent', 'processing', 'failed'] }
  }
  const pendingAppQueue = await appQueueHelper.countAppQueues(pendingQueueQuery)

  if (pendingAppQueue) {
    throw new CustomError(405, 'Pending previous reset operation')
  }

  const appQueuesData = {
    destination: 'reports',
    params: {
      partnerId,
      isForReset: true,
      dataToSkip: 0
    },
    priority: 'immediate',
    action: 'get_contract_ids_and_create_app_queues',
    event: 'rent_specification_reports_generation',
    status: 'new',
    delaySeconds: 0
  }

  const appQueueId = await appQueueService.createAnAppQueue(
    appQueuesData,
    session
  )

  if (!appQueueId) {
    throw new CustomError(404, 'Unable to create appQueue')
  }

  return {
    message:
      'Rent specification reports will reset soon. Please check wait for a while'
  }
}

export const downloadRentSpecificationReports = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const preparedQuery =
    rentSpecificationReportHelper.prepareRentSpecificationReportQuery(body)
  const totalCount =
    await rentSpecificationReportHelper.countTotalRentSpecificationReports(
      preparedQuery
    )
  if (totalCount > 50000) {
    throw new CustomError(
      400,
      'Too many rent specification report entries. Please change the filter.'
    )
  }
  const { accountingPeriod, sort, transactionPeriod, userId } = body
  if (
    size(accountingPeriod) &&
    size(accountingPeriod.startDate) &&
    size(accountingPeriod.endDate)
  ) {
    body.accountingPeriod = {
      startDate: new Date(accountingPeriod.startDate),
      endDate: new Date(accountingPeriod.endDate)
    }
  }
  if (
    size(transactionPeriod) &&
    size(transactionPeriod.startDate) &&
    size(transactionPeriod.endDate)
  ) {
    body.transactionPeriod = {
      startDate: new Date(transactionPeriod.startDate),
      endDate: new Date(transactionPeriod.endDate)
    }
  }
  if (size(sort)) {
    appHelper.validateSortForQuery(sort)
    if (sort['propertyInfo.location.name']) {
      sort.propertyInfo_location_name = sort['propertyInfo.location.name']
      delete sort['propertyInfo.location.name']
    }
    body.sort = sort
  }
  body.downloadProcessType = 'download_rent_specification_reports'
  const userInfo = (await userHelper.getAnUser({ _id: userId })) || {}
  body.userLanguage = userInfo.profile?.language || 'no'
  const appQueueData = {
    action: 'download_email',
    event: 'download_email',
    destination: 'excel-manager',
    params: body,
    priority: 'immediate'
  }
  await appQueueService.createAnAppQueue(appQueueData)
  return {
    status: 200,
    message:
      'Your download request is in progress, When your download is ready, we will send you an email with download link. It could take couple of minutes.'
  }
}
