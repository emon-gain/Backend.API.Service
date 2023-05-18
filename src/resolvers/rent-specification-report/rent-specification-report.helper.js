import { map, size } from 'lodash'
import { CustomError } from '../common'

import {
  ContractCollection,
  RentSpecificationReportCollection
} from '../models'

import { appHelper, appQueueHelper, partnerHelper } from '../helpers'
import { rentSpecificationReportService } from '../services'

export const prepareRentSpecificationReportQuery = (params = {}) => {
  const {
    accountId = '',
    accountingPeriod,
    agentId = '',
    branchId = '',
    contractId = '',
    partnerId = '',
    propertyId = '',
    tenantId = '',
    transactionPeriod,
    type = ''
  } = params
  const query = {}

  if (accountId) query.accountId = accountId
  if (
    size(accountingPeriod) &&
    accountingPeriod.startDate &&
    accountingPeriod.endDate
  ) {
    query.accountingPeriod = {
      $gte: new Date(accountingPeriod.startDate),
      $lte: new Date(accountingPeriod.endDate)
    }
  }
  if (
    size(transactionPeriod) &&
    transactionPeriod.startDate &&
    transactionPeriod.endDate
  ) {
    query.transactionPeriod = {
      $gte: new Date(transactionPeriod.startDate),
      $lte: new Date(transactionPeriod.endDate)
    }
  }
  if (agentId) query.agentId = agentId
  if (branchId) query.branchId = branchId
  if (contractId) query.contractId = contractId
  if (partnerId) query.partnerId = partnerId
  if (propertyId) query.propertyId = propertyId
  if (tenantId) query.tenantId = tenantId
  if (type === 'transaction') query.hasTransactions = true
  if (type === 'budgeting') query.hasTransactions = false

  return query
}

const getRentSpecificationReportsForAggregationQuery = async (
  aggregationPipeLines = []
) => {
  const rentSpecificationReports =
    await RentSpecificationReportCollection.aggregate(aggregationPipeLines)
  return rentSpecificationReports
}

const contractGroupPipeline = {
  $group: {
    _id: '$contractId',
    createdAt: { $first: '$createdAt' },
    partnerId: { $first: '$partnerId' },
    accountId: { $first: '$accountId' },
    agentId: { $first: '$agentId' },
    branchId: { $first: '$branchId' },
    propertyId: { $first: '$propertyId' },
    tenantId: { $first: '$tenantId' },
    contractStartDate: { $first: '$contractStartDate' },
    contractEndDate: { $first: '$contractEndDate' },
    rent: { $first: '$rent' },
    rentWithVat: { $first: '$rentWithVat' },
    estimatedAddonsMeta: { $first: '$estimatedAddonsMeta' },
    totalEstimatedAddons: { $first: '$totalEstimatedAddons' },
    totalMonthly: { $avg: '$totalMonthly' },
    months: { $sum: '$months' },
    days: { $sum: '$days' },
    totalRent: { $sum: '$totalRent' },
    totalRentWithVat: { $sum: '$totalRentWithVat' },
    actualAddonsMeta: { $push: '$actualAddonsMeta' },
    totalActualAddons: { $sum: '$totalActualAddons' },
    totalFees: { $sum: '$totalFees' },
    totalCorrections: { $sum: '$totalCorrections' }
  }
}

const addonGroupPipeline = {
  $group: {
    _id: {
      contractId: '$_id',
      addonId: '$actualAddonsMeta.addonId',
      addonName: '$actualAddonsMeta.addonName'
    },
    contractId: { $first: '$_id' },
    createdAt: { $first: '$createdAt' },
    actualAddonTotal: { $sum: '$actualAddonsMeta.addonTotal' },
    partnerId: { $first: '$partnerId' },
    accountId: { $first: '$accountId' },
    agentId: { $first: '$agentId' },
    branchId: { $first: '$branchId' },
    propertyId: { $first: '$propertyId' },
    tenantId: { $first: '$tenantId' },
    contractStartDate: { $first: '$contractStartDate' },
    contractEndDate: { $first: '$contractEndDate' },
    rent: { $first: '$rent' },
    rentWithVat: { $first: '$rentWithVat' },
    estimatedAddonsMeta: { $first: '$estimatedAddonsMeta' },
    totalEstimatedAddons: { $first: '$totalEstimatedAddons' },
    totalMonthly: { $first: '$totalMonthly' },
    months: { $first: '$months' },
    days: { $first: '$days' },
    totalRent: { $first: '$totalRent' },
    totalRentWithVat: { $first: '$totalRentWithVat' },
    totalActualAddons: { $first: '$totalActualAddons' },
    totalFees: { $first: '$totalFees' },
    totalCorrections: { $first: '$totalCorrections' }
  }
}

const secondContractGroupPipeline = {
  $group: {
    _id: '$contractId',
    createdAt: { $first: '$createdAt' },
    partnerId: { $first: '$partnerId' },
    accountId: { $first: '$accountId' },
    agentId: { $first: '$agentId' },
    branchId: { $first: '$branchId' },
    propertyId: { $first: '$propertyId' },
    tenantId: { $first: '$tenantId' },
    contractStartDate: { $first: '$contractStartDate' },
    contractEndDate: { $first: '$contractEndDate' },
    rent: { $first: '$rent' },
    rentWithVat: { $first: '$rentWithVat' },
    estimatedAddonsMeta: { $first: '$estimatedAddonsMeta' },
    totalEstimatedAddons: { $first: '$totalEstimatedAddons' },
    totalMonthly: { $first: '$totalMonthly' },
    months: { $first: '$months' },
    days: { $first: '$days' },
    totalRent: { $first: '$totalRent' },
    totalRentWithVat: { $first: '$totalRentWithVat' },
    actualAddonsMeta: {
      $push: {
        addonId: '$_id.addonId',
        addonName: '$_id.addonName',
        addonTotal: { $sum: '$actualAddonTotal' }
      }
    },
    totalActualAddons: { $first: '$totalActualAddons' },
    totalFees: { $first: '$totalFees' },
    totalCorrections: { $first: '$totalCorrections' }
  }
}

const getProjectPipeLine = async (params = {}) => {
  const { partnerId } = params
  const partnerSettings = await partnerHelper.getPartnerById(partnerId)
  const dateFormat =
    partnerSettings?.dateTimeSettings?.dateFormat === 'DD.MM.YYYY'
      ? '%d.%m.%Y'
      : '%Y.%m.%d'
  const timezone = partnerSettings?.dateTimeSettings?.timezone || 'Europe/Oslo'
  const projectPipeLine = {
    $project: {
      createdAt: 1,
      partnerId: 1,
      accountId: 1,
      agentId: 1,
      branchId: 1,
      propertyId: 1,
      propertyLocation: '$property.location.name',
      propertyApartmentId: '$property.apartmentId',
      propertyImages: '$property.images',
      tenantId: 1,
      tenantName: '$tenant.name',
      tenantUserInfo: '$tenantUserInfo.profile',
      contractStartDate: {
        $dateToString: {
          format: dateFormat,
          date: '$contractStartDate',
          timezone
        }
      },
      contractEndDate: {
        $dateToString: {
          format: dateFormat,
          date: '$contractEndDate',
          timezone
        }
      },
      rent: { $round: ['$rent', 2] },
      rentWithVat: { $round: ['$rentWithVat', 2] },
      estimatedAddonsMeta: 1,
      totalEstimatedAddons: 1,
      totalMonthly: { $round: ['$totalMonthly', 2] },
      months: 1,
      days: 1,
      estimatedTotalPeriod: {
        $round: [
          {
            $add: [
              { $multiply: ['$totalMonthly', '$months'] },
              { $multiply: ['$totalMonthly', { $divide: ['$days', 30] }] }
            ]
          },
          2
        ]
      },
      totalRent: { $round: ['$totalRent', 2] },
      totalRentWithVat: { $round: ['$totalRentWithVat', 2] },
      actualAddonsMeta: {
        $filter: {
          input: '$actualAddonsMeta',
          as: 'addon',
          cond: { $ifNull: ['$$addon.addonId', false] }
        }
      },
      totalActualAddons: 1,
      totalFees: 1,
      totalCorrections: 1,
      actualTotalPeriod: {
        $round: [
          {
            $add: [
              '$totalActualAddons',
              '$totalRent',
              '$totalRentWithVat',
              '$totalFees',
              '$totalCorrections'
            ]
          },
          2
        ]
      }
    }
  }
  return projectPipeLine
}

const getAggregationPipeLinesForRSRs = async (params) => {
  const { query, options } = params
  const { sort, skip, limit } = options
  const projectPipeline = await getProjectPipeLine(query)
  const aggregationPipeLines = [
    { $match: query },
    contractGroupPipeline,
    {
      $unwind: {
        path: '$actualAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$actualAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    addonGroupPipeline,
    secondContractGroupPipeline,
    {
      $lookup: {
        from: 'listings',
        localField: 'propertyId',
        foreignField: '_id',
        as: 'property'
      }
    },
    {
      $unwind: {
        path: '$property',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'tenants',
        localField: 'tenantId',
        foreignField: '_id',
        as: 'tenant'
      }
    },
    {
      $unwind: {
        path: '$tenant',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'tenant.userId',
        foreignField: '_id',
        as: 'tenantUserInfo'
      }
    },
    {
      $unwind: {
        path: '$tenantUserInfo',
        preserveNullAndEmptyArrays: true
      }
    },
    projectPipeline
  ]
  if (size(sort))
    aggregationPipeLines.push({
      $sort: sort
    })
  if (skip)
    aggregationPipeLines.push({
      $skip: skip
    })
  if (limit)
    aggregationPipeLines.push({
      $limit: limit
    })
  return aggregationPipeLines
}

const getAddonNamesArray = async (query = {}) => {
  const aggregationPipelines = [
    {
      $match: query
    },
    {
      $unwind: {
        path: '$estimatedAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $sort: { 'estimatedAddonsMeta.addonName': 1 }
    },
    {
      $group: {
        _id: null,
        addonNamesArray: {
          $addToSet: '$estimatedAddonsMeta.addonName'
        }
      }
    }
  ]
  const [addonsList] =
    (await RentSpecificationReportCollection.aggregate(aggregationPipelines)) ||
    []
  const { addonNamesArray = [] } = addonsList || {}
  return addonNamesArray
}

const projectPipelineForSummaryData = {
  $project: {
    rent: 1,
    rentWithVat: 1,
    estimatedAddonsMeta: {
      $ifNull: ['$estimatedAddonsMeta', '$$REMOVE']
    },
    totalEstimatedAddons: 1,
    totalMonthly: 1,
    estimatedTotalPeriod: {
      $round: [
        {
          $add: [
            { $multiply: ['$totalMonthly', '$months'] },
            { $multiply: ['$totalMonthly', { $divide: ['$days', 30] }] }
          ]
        },
        2
      ]
    },
    totalRent: 1,
    totalRentWithVat: 1,
    actualAddonsMeta: 1,
    totalActualAddons: 1,
    totalFees: 1,
    totalCorrections: 1,
    actualTotalPeriod: {
      $round: [
        {
          $add: [
            '$totalActualAddons',
            '$totalRent',
            '$totalRentWithVat',
            '$totalFees',
            '$totalCorrections'
          ]
        },
        2
      ]
    }
  }
}

const groupPipelineForSummaryData = {
  $group: {
    _id: null,
    rent: { $sum: '$rent' },
    rentWithVat: { $sum: '$rentWithVat' },
    estimatedAddonsMeta: { $push: '$estimatedAddonsMeta' },
    totalEstimatedAddons: { $sum: '$totalEstimatedAddons' },
    totalMonthly: { $sum: '$totalMonthly' },
    estimatedTotalPeriod: { $sum: '$estimatedTotalPeriod' },
    totalRent: { $sum: '$totalRent' },
    totalRentWithVat: { $sum: '$totalRentWithVat' },
    actualAddonsMeta: { $push: '$actualAddonsMeta' },
    totalActualAddons: { $sum: '$totalActualAddons' },
    totalFees: { $sum: '$totalFees' },
    totalCorrections: { $sum: '$totalCorrections' },
    actualTotalPeriod: { $sum: '$actualTotalPeriod' }
  }
}

const groupPipeLineForFilteredAddonOne = {
  $group: {
    _id: {
      addonId: '$actualAddonsMeta.addonId',
      addonName: '$actualAddonsMeta.addonName'
    },
    actualAddonTotal: {
      $sum: '$actualAddonsMeta.addonTotal'
    },
    rent: {
      $first: '$rent'
    },
    rentWithVat: {
      $first: '$rentWithVat'
    },
    estimatedAddonsMeta: {
      $first: '$estimatedAddonsMeta'
    },
    totalMonthly: {
      $first: '$totalMonthly'
    },
    estimatedTotalPeriod: {
      $first: '$estimatedTotalPeriod'
    },
    totalRent: {
      $first: '$totalRent'
    },
    totalRentWithVat: {
      $first: '$totalRentWithVat'
    },
    totalActualAddons: {
      $first: '$totalActualAddons'
    },
    totalEstimatedAddons: {
      $first: '$totalEstimatedAddons'
    },
    totalFees: {
      $first: '$totalFees'
    },
    totalCorrections: {
      $first: '$totalCorrections'
    },
    actualTotalPeriod: {
      $first: '$actualTotalPeriod'
    }
  }
}

const groupPipeLineForFilteredAddonTwo = {
  $group: {
    _id: null,
    actualAddonsMeta: {
      $push: {
        addonId: '$_id.addonId',
        addonName: '$_id.addonName',
        addonTotal: {
          $sum: '$actualAddonTotal'
        }
      }
    },
    rent: {
      $first: '$rent'
    },
    rentWithVat: {
      $first: '$rentWithVat'
    },
    estimatedAddonsMeta: {
      $first: '$estimatedAddonsMeta'
    },
    totalMonthly: {
      $first: '$totalMonthly'
    },
    estimatedTotalPeriod: {
      $first: '$estimatedTotalPeriod'
    },
    totalRent: {
      $first: '$totalRent'
    },
    totalRentWithVat: {
      $first: '$totalRentWithVat'
    },
    totalActualAddons: {
      $first: '$totalActualAddons'
    },
    totalEstimatedAddons: {
      $first: '$totalEstimatedAddons'
    },
    totalFees: {
      $first: '$totalFees'
    },
    totalCorrections: {
      $first: '$totalCorrections'
    },
    actualTotalPeriod: {
      $first: '$actualTotalPeriod'
    }
  }
}

const groupPipeLineForFilteredAddonThree = {
  $group: {
    _id: {
      addonId: '$estimatedAddonsMeta.addonId',
      addonName: '$estimatedAddonsMeta.addonName'
    },
    estimatedAddonTotal: {
      $sum: '$estimatedAddonsMeta.addonTotal'
    },
    rent: {
      $first: '$rent'
    },
    rentWithVat: {
      $first: '$rentWithVat'
    },
    totalMonthly: {
      $first: '$totalMonthly'
    },
    estimatedTotalPeriod: {
      $first: '$estimatedTotalPeriod'
    },
    totalRent: {
      $first: '$totalRent'
    },
    totalRentWithVat: {
      $first: '$totalRentWithVat'
    },
    actualAddonsMeta: {
      $first: '$actualAddonsMeta'
    },
    totalActualAddons: {
      $first: '$totalActualAddons'
    },
    totalEstimatedAddons: {
      $first: '$totalEstimatedAddons'
    },
    totalFees: {
      $first: '$totalFees'
    },
    totalCorrections: {
      $first: '$totalCorrections'
    },
    actualTotalPeriod: {
      $first: '$actualTotalPeriod'
    }
  }
}

const groupPipeLineForFilteredAddonFour = {
  $group: {
    _id: null,
    estimatedAddonsMeta: {
      $push: {
        addonId: '$_id.addonId',
        addonName: '$_id.addonName',
        addonTotal: {
          $sum: '$estimatedAddonTotal'
        }
      }
    },
    rent: {
      $first: '$rent'
    },
    rentWithVat: {
      $first: '$rentWithVat'
    },
    totalMonthly: {
      $first: '$totalMonthly'
    },
    estimatedTotalPeriod: {
      $first: '$estimatedTotalPeriod'
    },
    totalRent: {
      $first: '$totalRent'
    },
    totalRentWithVat: {
      $first: '$totalRentWithVat'
    },
    actualAddonsMeta: {
      $first: '$actualAddonsMeta'
    },
    totalActualAddons: {
      $first: '$totalActualAddons'
    },
    totalEstimatedAddons: {
      $first: '$totalEstimatedAddons'
    },
    totalFees: {
      $first: '$totalFees'
    },
    totalCorrections: {
      $first: '$totalCorrections'
    },
    actualTotalPeriod: {
      $first: '$actualTotalPeriod'
    }
  }
}

const getAggregationPipeLinesForRSRsSummaryData = (query = {}) => {
  const aggregationPipeLines = [
    { $match: query },
    contractGroupPipeline,
    {
      $unwind: {
        path: '$actualAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$actualAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    addonGroupPipeline,
    secondContractGroupPipeline,
    projectPipelineForSummaryData,
    groupPipelineForSummaryData,
    {
      $unwind: {
        path: '$actualAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$actualAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    groupPipeLineForFilteredAddonOne,
    groupPipeLineForFilteredAddonTwo,
    {
      $unwind: {
        path: '$estimatedAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $unwind: {
        path: '$estimatedAddonsMeta',
        preserveNullAndEmptyArrays: true
      }
    },
    groupPipeLineForFilteredAddonThree,
    groupPipeLineForFilteredAddonFour
  ]
  return aggregationPipeLines
}

const getRentSpecReportsSummaryData = async (query = {}) => {
  const aggregationPipeLines = getAggregationPipeLinesForRSRsSummaryData(query)
  const [summaryData] =
    (await getRentSpecificationReportsForAggregationQuery(
      aggregationPipeLines
    )) || []

  return summaryData
}

export const countTotalRentSpecificationReports = async (query = {}) => {
  const aggregationPipeLines = [
    { $match: query },
    { $group: { _id: '$contractId' } }
  ]
  const rentSpecificationReports =
    await getRentSpecificationReportsForAggregationQuery(aggregationPipeLines)
  return size(rentSpecificationReports)
}

const getRentSpecificationReportsOtherInfo = async (
  query = {},
  isForExcelDownload
) => {
  const addonNamesArray = await getAddonNamesArray(query)
  const summaryData = !isForExcelDownload
    ? await getRentSpecReportsSummaryData(query)
    : {}
  const filterDocument = await countTotalRentSpecificationReports(query)
  const totalDocument = !isForExcelDownload
    ? await countTotalRentSpecificationReports({
        partnerId: query?.partnerId
      })
    : 0
  return { addonNamesArray, filterDocument, summaryData, totalDocument }
}

export const getRentSpecificationReportsForExcel = async (req) => {
  const { body, user = {} } = req
  const { userId = '' } = user
  appHelper.checkUserId(userId)
  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  options.sort = prepareSortForRentSpecificationReport(options.sort)
  appHelper.checkRequiredFields(['queueId'], query)
  const { queueId } = query
  appHelper.validateId({ queueId })

  const { params = {} } = (await appQueueHelper.getQueueItemById(queueId)) || {}
  const { downloadProcessType = '', partnerId = '' } = params
  if (!size(downloadProcessType)) {
    throw new CustomError(400, 'Download process type is missing')
  }
  if (downloadProcessType === 'download_rent_specification_reports') {
    if (!size(partnerId)) {
      throw new CustomError(400, 'Missing partnerId')
    }
    const rentSpecificationReportsQuery =
      prepareRentSpecificationReportQuery(params)
    const aggregationPipeLines = await getAggregationPipeLinesForRSRs({
      query: rentSpecificationReportsQuery,
      options
    })
    const rentSpecificationReports =
      await getRentSpecificationReportsForAggregationQuery(aggregationPipeLines)
    const { addonNamesArray, filterDocument } =
      await getRentSpecificationReportsOtherInfo(
        rentSpecificationReportsQuery,
        true
      )

    return {
      addons: addonNamesArray,
      data: rentSpecificationReports,
      total: filterDocument
    }
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const getContractDataForRentSpecificationReports = async (req) => {
  const { body, session, user = {} } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  const { isForReset } = query

  delete query.isForReset
  const contractData = await getContractDataForQueryAndOptions(
    query,
    options,
    isForReset
  )
  // Deleting rent specification reports of contracts
  await rentSpecificationReportService.deleteRentSpecificationReportsOfContracts(
    map(contractData, 'contractId'),
    session
  )
  return contractData
}

const getContractDataForQueryAndOptions = async (
  query = {},
  options = {},
  isForReset = false
) => {
  const { limit = 100, skip = 0 } = options
  const aggregationPipeLine = [
    {
      $match: {
        ...query,
        'rentalMeta.contractStartDate': { $exists: true },
        'rentalMeta.status': { $ne: 'new' },
        'rentalMeta.tenantId': { $exists: true }
      }
    }
  ]
  if (!isForReset) {
    aggregationPipeLine.push(
      ...[
        {
          $lookup: {
            from: 'rent_specification_reports',
            localField: '_id',
            foreignField: 'contractId',
            as: 'rentSpecificationReport'
          }
        },
        {
          $match: {
            $or: [
              { 'rentSpecificationReport.createdAt': { $exists: false } },
              { updatedAt: { $gt: '$rentSpecificationReport.createdAt' } }
            ]
          }
        }
      ]
    )
  }
  aggregationPipeLine.push(
    ...[
      {
        $lookup: {
          from: 'partner_settings',
          localField: 'partnerId',
          foreignField: 'partnerId',
          as: 'partnerSetting'
        }
      },
      {
        $unwind: {
          path: '$partnerSetting',
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $sort: { createdAt: 1 }
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
          contractData: {
            $addToSet: {
              contractId: '$_id',
              contractStartDate: '$rentalMeta.contractStartDate',
              contractInvoicedAsOn: '$rentalMeta.invoicedAsOn',
              partnerId: '$partnerId',
              partnerTimeZone: '$partnerSetting.dateTimeSettings.timezone'
            }
          }
        }
      }
    ]
  )
  const [contractsList] =
    (await ContractCollection.aggregate(aggregationPipeLine)) || []
  const { contractData = [] } = contractsList || {}
  return contractData
}

const getFirstContractGroupPipeline = () => ({
  $group: {
    _id: '$contractId',
    createdAt: { $first: '$createdAt' },
    partnerId: { $first: '$partnerId' },
    accountId: { $first: '$accountId' },
    agentId: { $first: '$agentId' },
    branchId: { $first: '$branchId' },
    propertyId: { $first: '$propertyId' },
    tenantId: { $first: '$tenantId' },
    contractStartDate: { $first: '$contractStartDate' },
    contractEndDate: { $first: '$contractEndDate' },
    rent: { $first: '$rent' },
    rentWithVat: { $first: '$rentWithVat' },
    totalEstimatedAddons: { $first: '$totalEstimatedAddons' },
    totalMonthly: { $avg: '$totalMonthly' },
    months: { $sum: '$months' },
    days: { $sum: '$days' },
    totalRent: { $sum: '$totalRent' },
    totalRentWithVat: { $sum: '$totalRentWithVat' },
    actualAddonsMeta: { $push: '$actualAddonsMeta' },
    estimatedAddonsMeta: { $first: '$estimatedAddonsMeta' },
    totalActualAddons: { $sum: '$totalActualAddons' },
    totalFees: { $sum: '$totalFees' },
    totalCorrections: { $sum: '$totalCorrections' }
  }
})

const getAddonPipeline = () => ({
  $addFields: {
    actualAddonsMeta: {
      $reduce: {
        input: '$actualAddonsMeta',
        initialValue: [],
        in: { $concatArrays: ['$$value', '$$this'] }
      }
    }
  }
})

const getRentSpecificationReportForQuery = async (params = {}) => {
  const { options = {}, query } = params
  const { limit, skip, sort } = options
  const pipeline = [
    { $match: query },
    getFirstContractGroupPipeline(),
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    getAddonPipeline(),
    ...appHelper.getCommonPropertyInfoPipeline(),
    ...appHelper.getCommonTenantInfoPipeline(),
    {
      $project: {
        _id: 1,
        propertyInfo: {
          _id: 1,
          imageUrl: 1,
          location: {
            name: 1,
            city: 1,
            country: 1,
            postalCode: 1
          },
          serial: 1,
          propertyTypeId: 1,
          listingTypeId: 1,
          floor: 1,
          apartmentId: 1,
          placeId: 1
        },
        tenantInfo: {
          _id: 1,
          name: 1,
          avatarKey: 1
        },
        contractStartDate: 1,
        contractEndDate: 1,
        rent: 1,
        rentWithVat: 1,
        months: 1,
        days: 1,
        estimatedTotalPeriod: {
          $add: [
            { $multiply: ['$totalMonthly', '$months'] },
            { $multiply: ['$totalMonthly', { $divide: ['$days', 30] }] }
          ]
        },
        totalMonthly: 1,
        totalRent: 1,
        totalRentWithVat: 1,
        totalFees: 1,
        totalCorrections: 1,
        actualTotalPeriod: {
          $add: [
            '$totalActualAddons',
            '$totalRent',
            '$totalRentWithVat',
            '$totalFees',
            '$totalCorrections'
          ]
        },
        totalEstimatedAddons: 1,
        actualAddonsMeta: {
          $filter: {
            input: '$actualAddonsMeta',
            as: 'addon',
            cond: { $ifNull: ['$$addon.addonId', false] }
          }
        },
        totalActualAddons: {
          $sum: '$actualAddonsMeta.addonTotal'
        },
        estimatedAddonsMeta: 1
      }
    }
  ]
  const reports =
    (await RentSpecificationReportCollection.aggregate(pipeline)) || []
  return reports
}

export const getRentSpecificationReport = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId = '', partnerId = '' } = user
  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })
  const { query, options } = body
  query.partnerId = partnerId
  appHelper.validateSortForQuery(options.sort)

  const preparedQuery = prepareRentSpecificationReportQuery(query)
  const rentSpecificationReports = await getRentSpecificationReportForQuery({
    query: preparedQuery,
    options
  })
  const addonsName = await getAddonNamesArray(preparedQuery)
  const filteredDocuments = await countTotalRentSpecificationReports(
    preparedQuery
  )
  const totalDocuments = await countTotalRentSpecificationReports({
    partnerId
  })
  return {
    data: rentSpecificationReports,
    metaData: {
      addons: addonsName,
      filteredDocuments,
      totalDocuments
    }
  }
}

export const getRentSpecificationReportSummary = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId = '' } = user
  body.partnerId = partnerId
  const preparedQuery = prepareRentSpecificationReportQuery(body)
  const rentSpecificationReportSummary = await getRentSpecReportsSummaryData(
    preparedQuery
  )
  const addonsName = await getAddonNamesArray(preparedQuery)
  return {
    data: rentSpecificationReportSummary,
    metaData: {
      addons: addonsName
    }
  }
}

export const prepareSortForRentSpecificationReport = (sort) => {
  if (sort.propertyInfo_location_name) {
    sort['property.location.name'] = sort.propertyInfo_location_name
    delete sort.propertyInfo_location_name
  }
  return sort
}
