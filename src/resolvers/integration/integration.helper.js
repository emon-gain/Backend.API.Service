import axios from 'axios'
import { each, size } from 'lodash'

import { appHelper, integrationHelper, partnerHelper } from '../helpers'
import { integrationService } from '../services'
import {
  IntegrationCollection,
  LedgerAccountCollection,
  TransactionCollection
} from '../models'
import { CustomError } from '../common'

const { getDifference, getUnion, getXor } = appHelper

export const getIntegrations = async (params = {}, session) => {
  const { query, options = {}, populate } = params
  const { limit, sort, skip } = options
  const integrations = await IntegrationCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
    .session(session)
  return integrations
}

export const getUniqueFieldValues = async (field, query) =>
  await IntegrationCollection.distinct(field, query)

export const getIntegratedPartnersToStartSyncProcess = async (
  query = {},
  options = {}
) => {
  const { limit, skip, sort } = options
  const pipeline = [
    {
      $match: query
    },
    {
      $group: {
        _id: '$partnerId',
        createdAt: {
          $first: '$createdAt'
        }
      }
    },
    {
      $sort: sort
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
        localField: '_id',
        foreignField: '_id',
        as: 'partner'
      }
    },
    appHelper.getUnwindPipeline('partner'),
    {
      $project: {
        partnerId: '$_id',
        partner: {
          _id: 1,
          accountType: 1
        }
      }
    }
  ]
  return (await IntegrationCollection.aggregate(pipeline)) || []
}

export const getAnIntegration = async (query, session) => {
  const integration = await IntegrationCollection.findOne(query).session(
    session
  )
  return integration
}

export const getIntegrationAccountTenantType = (integrationInfo = {}) => {
  const tenantAccountType = integrationInfo.tenantAccountType || ''
  const typeArray = tenantAccountType ? tenantAccountType.split('_') : []
  const tenantType = size(typeArray) && typeArray[0] ? typeArray[0] : 'customer'
  const accountType =
    size(typeArray) && typeArray[1] ? typeArray[1] : 'supplier'
  return {
    accountType,
    tenantType
  }
}

const lookupUlAccountBranchGroupInfo = () => [
  {
    $lookup: {
      from: 'ledger_accounts',
      let: { mapAccounts: '$mapAccounts' },
      localField: 'partnerId',
      foreignField: 'partnerId',
      pipeline: [
        {
          $match: {
            $expr: {
              $not: {
                $in: ['$_id', { $ifNull: ['$$mapAccounts.accountingId', []] }]
              }
            }
          }
        }
      ],
      as: 'ul_accounts'
    }
  },
  {
    $lookup: {
      from: 'branches',
      let: { mapBranches: '$mapBranches' },
      localField: 'partnerId',
      foreignField: 'partnerId',
      pipeline: [
        {
          $match: {
            branchSerialId: { $exists: true },
            $expr: {
              $and: [
                {
                  $not: {
                    $in: [
                      '$branchSerialId',
                      { $ifNull: ['$$mapBranches.branchSerialId', []] }
                    ]
                  }
                }
              ]
            }
          }
        }
      ],
      as: 'ul_branches'
    }
  },
  {
    $lookup: {
      from: 'listings',
      let: { mapGroups: '$mapGroups' },
      localField: 'partnerId',
      foreignField: 'partnerId',
      pipeline: [
        {
          $match: {
            groupId: { $exists: true },
            $expr: {
              $and: [
                {
                  $not: {
                    $in: [
                      '$groupId',
                      { $ifNull: ['$$mapGroups.propertyGroupId', []] }
                    ]
                  }
                }
              ]
            }
          }
        }
      ],
      as: 'ul_groups'
    }
  }
]

const getIntegrationDetails = async (query) => {
  const pipeline = [
    {
      $match: query
    },
    ...lookupUlAccountBranchGroupInfo(),
    {
      $project: {
        _id: 1,
        accountId: 1,
        accountSubledgerSeries: 1,
        applicationKey: 1,
        clientKey: 1,
        createdAt: 1,
        enabledPowerOfficeIntegration: 1,
        fromDate: 1,
        isGlobal: 1,
        mapAccounts: 1,
        mapBranches: 1,
        mapGroups: 1,
        projectDepartmentType: 1,
        status: 1,
        tenantAccountType: 1,
        tenantSubledgerSeries: 1,
        ul_accounts: {
          _id: 1,
          accountNumber: 1,
          accountName: 1,
          taxCodeId: 1
        },
        ul_branches: {
          _id: 1,
          name: 1,
          branchSerialId: 1
        },
        ul_groups: {
          _id: 1,
          groupId: 1
        },
        isStatusChecking: 1,
        errorsMeta: 1
      }
    }
  ]

  const [integration = {}] =
    (await IntegrationCollection.aggregate(pipeline)) || []

  return integration
}

const getIntegrationInfoForPartnerApp = async (params) => {
  const { accountId, partnerId } = params
  const integrationQuery = { partnerId, type: 'power_office_go' }
  const partnerInfo = await partnerHelper.getAPartner({ _id: partnerId })
  const isDirectPartner = partnerInfo?.accountType === 'direct'

  let globalMappingIntegration = {}

  if (isDirectPartner) {
    appHelper.checkRequiredFields(['accountId'], params)
    integrationQuery.accountId = accountId
    globalMappingIntegration = await getAnIntegration({
      partnerId,
      type: 'power_office_go',
      isGlobal: true
    })
  }
  // aggregate query
  const integrationInfo = await getIntegrationDetails(integrationQuery)
  if (!size(integrationInfo)) return {}

  integrationInfo.mapAccounts = size(integrationInfo.mapAccounts)
    ? integrationInfo.mapAccounts
    : size(globalMappingIntegration?.mapAccounts)
    ? globalMappingIntegration.mapAccounts
    : []

  integrationInfo.mapBranches = size(integrationInfo.mapBranches)
    ? integrationInfo.mapBranches
    : size(globalMappingIntegration?.mapBranches)
    ? globalMappingIntegration.mapBranches
    : []

  integrationInfo.mapGroups = size(integrationInfo.mapGroups)
    ? integrationInfo.mapGroups
    : size(globalMappingIntegration?.mapGroups)
    ? globalMappingIntegration.mapGroups
    : []

  return integrationInfo
}

const getIntegrationInfo = async (integrationsQuery) => {
  const integrationInfo = (await getAnIntegration(integrationsQuery)) || {}
  let globalIntegrationInfo = {}
  if (size(integrationsQuery.accountId) && !integrationInfo.isGlobal) {
    integrationsQuery.isGlobal = true
    delete integrationsQuery.accountId

    globalIntegrationInfo = await getAnIntegration(integrationsQuery)
    if (
      size(globalIntegrationInfo) &&
      size(globalIntegrationInfo.mapAccounts)
    ) {
      integrationInfo.mapAccounts = globalIntegrationInfo.mapAccounts
    }
    if (
      size(globalIntegrationInfo) &&
      size(globalIntegrationInfo.mapBranches)
    ) {
      integrationInfo.mapBranches = globalIntegrationInfo.mapBranches
    }
    if (size(globalIntegrationInfo) && size(globalIntegrationInfo.mapGroups)) {
      integrationInfo.mapGroups = globalIntegrationInfo.mapGroups
    }
  }
  return integrationInfo
}

export const createIntegrationFieldNameForApi = (integration) => {
  const integrationData = {
    _id: integration._id,
    type: integration.type,
    status: integration.status,
    applicationKey: integration.applicationKey,
    clientKey: integration.clientKey,
    enabledPowerOfficeIntegration: integration.enabledPowerOfficeIntegration,
    tenantAccountType: integration.tenantAccountType,
    accountSubledgerSeries: integration.accountSubledgerSeries,
    tenantSubledgerSeries: integration.tenantSubledgerSeries,
    fromDate: integration.fromDate,
    mapAccounts: integration.mapAccounts,
    projectDepartmentType: integration.projectDepartmentType,
    mapBranches: integration.mapBranches,
    mapGroups: integration.mapGroups,
    companyDbId: integration.companyDbId,
    ownerDbId: integration.ownerDbId
  }

  return integrationData
}

//For lambda accounting bridge pogo #10175
export const queryIntegration = async (req) => {
  const { body, user } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)

  const integration = await getIntegrationInfo(body)

  const response = createIntegrationFieldNameForApi(integration)
  return response
}

//for integration for partner app
export const queryIntegrationForPartnerApp = async (req) => {
  const { body, user } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  return await getIntegrationInfoForPartnerApp(body)
}

const getPogoConfig = async (params) => {
  const { accountId, partnerId } = params
  let { applicationKey, clientKey } = params
  const baseUrl = process.env.POGO_URL
  if (!baseUrl) throw new CustomError(404, 'Please add power office base url')
  const integrationQuery = { partnerId, type: 'power_office_go' }
  if (accountId) integrationQuery.accountId = accountId

  if (!applicationKey || !clientKey) {
    const integrationInfo = await getAnIntegration(integrationQuery)
    applicationKey = integrationInfo.applicationKey
    clientKey = integrationInfo.clientKey
  }

  if (!applicationKey || !clientKey)
    throw new CustomError(404, 'Please add applicationKey and clientKey')

  return {
    applicationKey,
    baseUrl,
    clientKey
  }
}

export const getAuthorizationToken = async (params) => {
  const { applicationKey, baseUrl, clientKey } = await getPogoConfig(params)
  const type = { grant_type: 'client_credentials' }
  const formBody = Object.keys(type)
    .map((key) => `${encodeURIComponent(key)}=${encodeURIComponent(type[key])}`)
    .join('&')
  let accessTokenData = {}

  try {
    accessTokenData = await axios({
      method: 'post',
      url: `${baseUrl}OAuth/Token`,
      headers: {
        Authorization: `Basic ${Buffer.from(
          `${applicationKey}:${clientKey}`,
          'utf8'
        ).toString('base64')}`,
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
      },
      data: formBody
    })
  } catch (err) {
    throw new CustomError(400, 'Invalid application key or client key')
  }

  return accessTokenData?.data?.access_token
}

const getListDataForPogo = async (context, accessToken) =>
  await axios.get(`${process.env.POGO_URL}${context}/`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json; charset=utf-8'
    }
  })

// Getting pogo account list
export const queryPogoIntegrationAccountList = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) appHelper.checkRequiredFields(['accountId'], body)
  body.partnerId = partnerId
  const { accountId } = body

  const accessToken = await getAuthorizationToken({ accountId, partnerId })
  const accountListData = await getListDataForPogo(
    'GeneralLedgerAccount',
    accessToken
  )

  return {
    data: accountListData?.data?.data || []
  }
}

// Getting pogo subledger series list
export const queryPogoIntegrationSubledgerList = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) appHelper.checkRequiredFields(['accountId'], body)
  body.partnerId = partnerId

  const { accountId } = body

  const accessToken = await getAuthorizationToken({ accountId, partnerId })
  const subledgerListData = await getListDataForPogo(
    'SubledgerNumberSeries',
    accessToken
  )

  return {
    data: subledgerListData?.data?.data || []
  }
}

// Getting pogo branch list
export const queryPogoIntegrationBranchList = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) appHelper.checkRequiredFields(['accountId'], body)
  body.partnerId = partnerId

  const { accountId } = body

  const accessToken = await getAuthorizationToken({ accountId, partnerId })
  const branchListData = await getListDataForPogo('Department', accessToken)

  return {
    data: branchListData?.data?.data || []
  }
}

// Getting pogo group list
export const queryPogoIntegrationGroupList = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) appHelper.checkRequiredFields(['accountId'], body)
  body.partnerId = partnerId

  const { accountId } = body

  const accessToken = await getAuthorizationToken({ accountId, partnerId })
  const groupListData = await getListDataForPogo('Project', accessToken)

  return {
    data: groupListData?.data?.data || []
  }
}

export const getTransactionIds = async (
  fromDate,
  isDirectPartner,
  partnerId
) => {
  if (isDirectPartner) return {}
  const allowedSubTypeForTenantDebit = [
    'invoice_fee',
    'rent',
    'collection_notice_fee',
    'invoice_reminder_fee',
    'eviction_notice_fee',
    'administration_eviction_notice_fee',
    'rent_with_vat',
    'rounded_amount'
  ]
  const allowedSubTypeForTenantCredit = ['rent_payment', 'loss_recognition']

  const allowedSubTypeForAccountDebit = [
    'brokering_commission',
    'addon_commission',
    'management_commission',
    'payout_to_landlords',
    'payout_addon'
  ]
  const allowedSubTypeForAccountCredit = [
    'rent',
    'rent_with_vat',
    'final_settlement_payment'
  ]

  const todayDate = await appHelper.getActualDate(partnerId, false, null)

  const transactionQuery = {
    partnerId,
    powerOffice: { $exists: false }
  }

  let from
  if (fromDate) {
    from = new Date(fromDate)
    transactionQuery.createdAt = { $gte: from, $lte: todayDate }
  } else {
    transactionQuery.createdAt = { $lte: todayDate }
  }

  const transactionPipeline = [
    {
      $match: transactionQuery
    },
    {
      $project: {
        _id: 1,
        subType: 1,
        type: 1
      }
    },
    {
      $addFields: {
        tenantDebitTransaction: {
          $cond: [
            {
              $or: [
                { $in: ['$subType', allowedSubTypeForTenantDebit] },
                {
                  $and: [
                    { $eq: ['$subType', 'addon'] },
                    { $in: ['$type', ['invoice', 'correction']] }
                  ]
                }
              ]
            },
            true,
            false
          ]
        },
        tenantCreditTransaction: {
          $cond: [
            { $in: ['$subType', allowedSubTypeForTenantCredit] },
            true,
            false
          ]
        },
        accountDebitTransaction: {
          $cond: [
            {
              $or: [
                { $in: ['$subType', allowedSubTypeForAccountDebit] },
                {
                  $and: [
                    { $eq: ['$subType', 'addon'] },
                    { $eq: ['$type', 'commission'] }
                  ]
                }
              ]
            },
            true,
            false
          ]
        },
        accountCreditTransaction: {
          $cond: [
            {
              $or: [
                { $in: ['$subType', allowedSubTypeForAccountCredit] },
                {
                  $and: [
                    { $eq: ['$subType', 'addon'] },
                    { $in: ['$type', ['invoice', 'correction']] }
                  ]
                }
              ]
            },
            true,
            false
          ]
        }
      }
    },
    {
      $group: {
        _id: null,
        tenantDebitTransactionIds: {
          $push: {
            $cond: [
              { $eq: ['$tenantDebitTransaction', true] },
              '$_id',
              '$$REMOVE'
            ]
          }
        },
        tenantCreditTransactionIds: {
          $push: {
            $cond: [
              { $eq: ['$tenantCreditTransaction', true] },
              '$_id',
              '$$REMOVE'
            ]
          }
        },
        accountDebitTransactionIds: {
          $push: {
            $cond: [
              { $eq: ['$accountDebitTransaction', true] },
              '$_id',
              '$$REMOVE'
            ]
          }
        },
        accountCreditTransactionIds: {
          $push: {
            $cond: [
              { $eq: ['$accountCreditTransaction', true] },
              '$_id',
              '$$REMOVE'
            ]
          }
        }
      }
    },
    {
      $addFields: {
        skipTransactionIdsForTenantDebitAccountCredit: {
          $setIntersection: [
            '$tenantDebitTransactionIds',
            '$accountCreditTransactionIds'
          ]
        },
        skipTransactionIdsForAccountDebitTenantCredit: {
          $setIntersection: [
            '$accountDebitTransactionIds',
            '$tenantCreditTransactionIds'
          ]
        }
      }
    }
  ]

  const [transactionIds = {}] =
    (await TransactionCollection.aggregate(transactionPipeline)) || []

  const {
    accountDebitTransactionIds,
    accountCreditTransactionIds,
    skipTransactionIdsForTenantDebitAccountCredit,
    skipTransactionIdsForAccountDebitTenantCredit,
    tenantDebitTransactionIds,
    tenantCreditTransactionIds
  } = transactionIds

  const totalSkippingTransactionIds = getUnion(
    skipTransactionIdsForTenantDebitAccountCredit,
    skipTransactionIdsForAccountDebitTenantCredit
  )
  const tenantDebitDiffIds = getDifference(
    tenantDebitTransactionIds,
    totalSkippingTransactionIds
  )
  const accountDebitDiffIds = getDifference(
    accountDebitTransactionIds,
    totalSkippingTransactionIds
  )
  const totalDebitPartSkippingTransactionIds = getUnion(
    tenantDebitDiffIds,
    accountDebitDiffIds
  )
  const tenantCreditDiffIds = getDifference(
    tenantCreditTransactionIds,
    totalSkippingTransactionIds
  )
  const accountCreditDiffIds = getDifference(
    accountCreditTransactionIds,
    totalSkippingTransactionIds
  )
  const totalCreditPartSkippingTransactionIds = getUnion(
    tenantCreditDiffIds,
    accountCreditDiffIds
  )

  return {
    totalCreditPartSkippingTransactionIds,
    totalDebitPartSkippingTransactionIds,
    totalSkippingTransactionIds
  }
}

export const getAccountCodeListByPartnerType = async (
  partnerId,
  partnerType,
  transactionData = {}
) => {
  const {
    fromDate,
    totalSkippingTransactionIds,
    totalDebitPartSkippingTransactionIds,
    totalCreditPartSkippingTransactionIds
  } = transactionData
  let accountCodes = []
  let date = null
  if (fromDate) date = new Date(fromDate)

  const transactionPipelineForBroker = [
    {
      $match: {
        partnerId,
        $expr: {
          $cond: [date, { $gte: ['$createdAt', date] }, true]
        }
      }
    },
    {
      $addFields: {
        isDebited: {
          $cond: {
            if: {
              $and: [
                {
                  $not: { $in: ['$_id', totalDebitPartSkippingTransactionIds] }
                },
                {
                  $not: { $in: ['$_id', totalSkippingTransactionIds] }
                }
              ]
            },
            then: true,
            else: false
          }
        },
        isCredited: {
          $cond: {
            if: {
              $and: [
                {
                  $not: { $in: ['$_id', totalCreditPartSkippingTransactionIds] }
                },
                { $not: { $in: ['$_id', totalSkippingTransactionIds] } }
              ]
            },
            then: true,
            else: false
          }
        }
      }
    },
    {
      $group: {
        _id: null,
        debitAccountCodes: {
          $push: {
            $cond: ['$isDebited', '$debitAccountCode', '$$REMOVE']
          }
        },
        creditAccountCodes: {
          $push: {
            $cond: ['$isCredited', '$creditAccountCode', '$$REMOVE']
          }
        }
      }
    }
  ]

  const transactionPipelineForDirect = [
    {
      $match: {
        partnerId,
        $expr: {
          $cond: [date, { $gte: ['$createdAt', date] }, true]
        },
        $or: [
          { debitAccountCode: { $exists: true } },
          { creditAccountCode: { $exists: true } }
        ]
      }
    },
    {
      $group: {
        _id: null,
        debitAccountCodes: {
          $push: '$debitAccountCode'
        },
        creditAccountCodes: {
          $push: '$creditAccountCode'
        }
      }
    }
  ]

  if (partnerType === 'broker') {
    const [accountCodeLists = {}] =
      (await TransactionCollection.aggregate(transactionPipelineForBroker)) ||
      []

    accountCodes = getUnion(
      accountCodeLists.debitAccountCodes,
      accountCodeLists.creditAccountCodes
    )
  } else {
    const [accountCodeLists = {}] =
      (await TransactionCollection.aggregate(transactionPipelineForDirect)) ||
      []

    accountCodes = getUnion(
      accountCodeLists.debitAccountCodes,
      accountCodeLists.creditAccountCodes
    )
    const pullingItems = [1500, 1501]
    accountCodes = accountCodes.filter((item) => !pullingItems.includes(item))
  }

  return accountCodes
}

export const getLedgerAccountInfo = async (accountCodes, partnerId) => {
  const ledgerAccountsPipeline = [
    {
      $match: {
        partnerId,
        accountNumber: { $in: accountCodes }
      }
    },
    {
      $lookup: {
        from: 'tax_codes',
        localField: 'taxCodeId',
        foreignField: '_id',
        as: 'taxCodeInfo'
      }
    },
    { $unwind: '$accountNumber' },
    { $unwind: '$taxCodeInfo' },
    {
      $group: {
        _id: null,
        ledgerAccountInfo: {
          $push: {
            accountNumber: '$accountNumber',
            vatCode: '$taxCodeInfo.taxCode'
          }
        },
        accountCodes: { $push: '$accountNumber' }
      }
    }
  ]
  const [ledgerInfo = {}] =
    (await LedgerAccountCollection.aggregate(ledgerAccountsPipeline)) || []

  return {
    ledgerAccountInfo: ledgerInfo?.ledgerAccountInfo || [],
    ledgerAccountCodes: ledgerInfo?.accountCodes || []
  }
}

const getLedgerAccountQuery = (accountCodes, type) => {
  if (accountCodes?.length === 0) return false

  const totalCode = accountCodes.length
  let query = '('

  if (type === 'vat_code') {
    each(accountCodes, (account, index) => {
      query =
        query +
        `Code eq ${account.accountNumber} and VatCode eq '${account.vatCode}'`

      if (totalCode !== index + 1) query += ' or '
      else query += ')'
    })
  } else {
    each(accountCodes, (code, index) => {
      query = query + `Code eq ${code}`

      if (totalCode !== index + 1) query += ' or '
      else query += ')'
    })
  }

  return query
}

const gettingExistingAccountCode = async (
  accessToken = '',
  ledgerAccountCodes = [],
  type = ''
) => {
  const query = getLedgerAccountQuery(ledgerAccountCodes, type)

  if (accessToken) {
    const res = await axios.get(
      `${process.env.POGO_URL}GeneralLedgerAccount/`,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json; charset=utf-8'
        },
        params: { $filter: query }
      }
    )
    let accountCode = []
    if (res?.data?.data.length > 0) {
      accountCode = res.data.data.reduce(
        (prev, next) => [...prev, next.code],
        []
      )
    }

    return accountCode
  }
}

export const queryPogoIntegrationStatus = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) appHelper.checkRequiredFields(['accountId'], body)
  body.partnerId = partnerId

  const partnerType = isDirectPartner ? 'direct' : 'broker'
  const { accountId } = body

  const integrationQuery = { partnerId, type: 'power_office_go' }
  if (isDirectPartner) integrationQuery.accountId = accountId
  const integrationInfo = await getAnIntegration(integrationQuery)
  if (!size(integrationInfo))
    throw new CustomError(400, 'Please add integration first')

  const {
    enabledPowerOfficeIntegration,
    fromDate,
    mapAccounts,
    mapBranches,
    mapGroups,
    projectDepartmentType
  } = integrationInfo

  if (!enabledPowerOfficeIntegration)
    throw new CustomError(400, 'Please enable power office go')

  const accessToken = await getAuthorizationToken({ accountId, partnerId })

  const {
    totalCreditPartSkippingTransactionIds,
    totalDebitPartSkippingTransactionIds,
    totalSkippingTransactionIds
  } = await getTransactionIds(fromDate, isDirectPartner, partnerId)

  const transactionData = {
    fromDate,
    totalSkippingTransactionIds,
    totalDebitPartSkippingTransactionIds,
    totalCreditPartSkippingTransactionIds
  }
  const accountCodes = await getAccountCodeListByPartnerType(
    partnerId,
    partnerType,
    transactionData
  )
  const { ledgerAccountCodes, ledgerAccountInfo } = await getLedgerAccountInfo(
    accountCodes,
    partnerId
  )

  let hasError = false,
    vatCodeMismatchAccountCode = [],
    digitErrorAccountCode = [],
    missingAccountCode = []

  if (ledgerAccountInfo.length > 0) {
    const getMatchingVatCodeAccount = await gettingExistingAccountCode(
      accessToken,
      ledgerAccountInfo,
      'vat_code'
    )
    const getExistingAccountCode = await gettingExistingAccountCode(
      accessToken,
      ledgerAccountCodes,
      ''
    )

    let mappedAccountCodes = []

    if (mapAccounts?.length) {
      mappedAccountCodes = mapAccounts.reduce(
        (prev, next) => [...prev, next.accountNumber],
        []
      )
    }

    missingAccountCode = getDifference(
      getXor(ledgerAccountCodes, getExistingAccountCode),
      mappedAccountCodes
    )
    vatCodeMismatchAccountCode = getDifference(
      getXor(
        getXor(ledgerAccountCodes, getMatchingVatCodeAccount),
        missingAccountCode
      ),
      mappedAccountCodes
    )
    for (const ledgerAccount of ledgerAccountInfo) {
      const parseStr = `${ledgerAccount.accountNumber}`
      if (parseStr.toString().length !== 4)
        digitErrorAccountCode.push(ledgerAccount.accountNumber)
    }

    digitErrorAccountCode = getDifference(
      digitErrorAccountCode,
      mappedAccountCodes
    )
  }

  const branchErrorCode = []
  const groupErrorCode = []

  if (mapBranches?.length > 0 || mapGroups?.length > 0) {
    //  Get pogo department list and project list
    const branchListData = await getListDataForPogo('Department', accessToken)
    const groupListData = await getListDataForPogo('Project', accessToken)

    const pogoDepartments = branchListData?.data?.data || []
    const pogoProjects = groupListData?.data?.data || []

    if (mapBranches?.length > 0) {
      let getPogoBranches = []
      if (projectDepartmentType === 'branch_department_and_group_project') {
        getPogoBranches = [...pogoDepartments]
      } else {
        getPogoBranches = [...pogoProjects]
      }

      if (getPogoBranches?.length > 0) {
        const map = {}
        for (const pogoBranch of getPogoBranches) {
          if (map[pogoBranch.code] === undefined) map[pogoBranch.code] = true
        }
        for (const branch of mapBranches) {
          if (map[branch.pogoBranchSerialId] === undefined)
            branchErrorCode.push(branch.pogoBranchSerialId)
        }
      }
    }

    if (mapGroups?.length > 0) {
      let getPogoGroups = []

      if (projectDepartmentType === 'branch_department_and_group_project') {
        getPogoGroups = [...pogoProjects]
      } else {
        getPogoGroups = [...pogoDepartments]
      }

      if (getPogoGroups?.length > 0) {
        const map = {}
        for (const pogoGroup of getPogoGroups) {
          if (map[pogoGroup.code] === undefined) map[pogoGroup.code] = true
        }
        for (const group of mapGroups) {
          if (map[group.pogoPropertyGroupId] === undefined)
            groupErrorCode.push(group.pogoPropertyGroupId)
        }
      }
    }
  }

  let updatedIntegrationInfo = {}
  let status = 'pending'

  if (
    !(
      vatCodeMismatchAccountCode.length > 0 ||
      digitErrorAccountCode.length > 0 ||
      missingAccountCode?.length > 0 ||
      groupErrorCode.length > 0 ||
      branchErrorCode.length > 0
    )
  ) {
    updatedIntegrationInfo = await integrationService.updateAnIntegration(
      { _id: integrationInfo._id },
      {
        $set: { status: 'integrated' }
      }
    )
    status = updatedIntegrationInfo.status
  } else {
    hasError = true

    //  DebitOrCreditAccountMissingTrIds, missingAccountCodeErrorTrIds and vatCodeMismatchAccountCodeErrorTrIds removed from v2
  }

  return {
    branchErrorCode,
    digitErrorAccountCode: digitErrorAccountCode.join(',') || '',
    groupErrorCode,
    hasError,
    missingAccountCode: missingAccountCode.join(',') || '',
    status,
    vatCodeMismatchAccountCode: vatCodeMismatchAccountCode.join(',') || ''
  }
}

export const prepareDataToUpdateIntegration = (body) => {
  const { errorsMeta, isStatusChecking, status, unsetErrors } = body
  const setData = {}
  const unsetData = {}
  if (unsetErrors) unsetData.errorsMeta = ''
  else if (size(errorsMeta)) setData.errorsMeta = errorsMeta
  if (status) setData.status = status
  if (body.hasOwnProperty('isStatusChecking'))
    setData.isStatusChecking = isStatusChecking
  const updateData = {}
  if (size(setData)) {
    updateData.$set = setData
  }
  if (size(unsetData)) {
    updateData.$unset = unsetData
  }
  return updateData
}

export const checkXledgerTokenValidity = async (token) => {
  const response = await axios({
    url: (process.env.XLEDGER_URL || 'https://demo.xledger.net') + '/graphql',
    method: 'post',
    headers: {
      Accept: 'application/json',
      Authorization: `token ${token}`
    },
    data: {
      query: `
          query {
            viewer {
              entity {
                company {
                  dbId
                }
                ownerDbId
              }
            }
          }
          `
    }
  })
  const { data = {}, errors } = response.data
  if (size(errors)) {
    throw new CustomError(401, 'Xledger token not valid')
  }
  const companyDbId = data.viewer?.entity?.company?.dbId
  const ownerDbId = data.viewer?.entity?.ownerDbId
  if (!companyDbId) throw new CustomError(404, 'Xledger company not found')
  if (!ownerDbId) throw new CustomError(404, 'Xledger owner not found')
  return { companyDbId, ownerDbId }
}

export const prepareInsertDataForXledger = async (body = {}, isDirect) => {
  const {
    accountId,
    clientKey,
    companyDbId,
    enabledPeriodSync,
    fromDate,
    mapXledgerGlObjects,
    ownerDbId,
    partnerId,
    tenantAccountType
  } = body

  const updateData = {
    clientKey,
    companyDbId,
    enabledPeriodSync,
    fromDate,
    ownerDbId,
    status: 'pending',
    tenantAccountType
  }
  if (size(mapXledgerGlObjects))
    updateData.mapXledgerGlObjects = mapXledgerGlObjects

  const insertData = {
    ...updateData,
    partnerId,
    type: 'xledger',
    enabledIntegration: true
  }

  const query = { partnerId, type: 'xledger' }

  if (accountId && isDirect) {
    insertData.accountId = accountId
    query.accountId = accountId

    const isGlobalIntegration = await integrationHelper.getAnIntegration({
      partnerId,
      isGlobal: true
    })

    if (!isGlobalIntegration) insertData.isGlobal = true
  }

  return {
    insertData,
    query,
    updateData
  }
}

export const queryXledgerIntegrationInfos = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], user)
  const { partnerId } = user
  const { query, options } = body
  appHelper.checkRequiredFields(['context'], query)
  const { context } = query
  const xledgerIntigation = await getAnIntegration({
    type: 'xledger',
    partnerId
  })
  const token = xledgerIntigation.clientKey
  let requestBody

  const xledgerQuery = preparedQueryForXledger(options)
  let apiName = ''
  if (context === 'projects') {
    requestBody = getApiRequestBodyForXledgerProjects(xledgerQuery)
    apiName = 'projects'
  } else if (context === 'accounts') {
    requestBody = getApiRequestBodyForXledgerAccounts(xledgerQuery)
    apiName = 'accounts'
  } else if (context === 'taxRules') {
    requestBody = getApiRequestBodyForXledgerTaxRules({
      ...xledgerQuery,
      filter: {
        objectKindDbId: 14
      }
    })
    apiName = 'objectValues'
  } else if (context === 'companies') {
    requestBody = getApiRequestBodyForXledgerCompanies(xledgerQuery)
    apiName = 'companies'
  } else if (context === 'owners') {
    requestBody = getApiRequestBodyForXledgerOwners(xledgerQuery)
    apiName = 'entities'
  } else if (context === 'glObjects') {
    appHelper.checkRequiredFields(['mappingContext'], query)
    const mappedObjectKind = (
      xledgerIntigation.mapXledgerObjectKinds || []
    ).find((item) => item.field === query.mappingContext)
    const filter = {
      objectKindDbId: mappedObjectKind
        ? parseInt(mappedObjectKind.objectKindDbId)
        : null
    }
    requestBody = getApiRequestBodyForXledgerGlObjects({
      ...xledgerQuery,
      filter
    })
    apiName = 'objectValues'
  } else if (context === 'objectKinds') {
    requestBody = getApiRequestBodyForXledgerObjectKinds(xledgerQuery)
    apiName = 'viewer'
  }

  const xledgerData = await apiRequestInXledger(token, requestBody, apiName)
  const filteredDocuments = xledgerData.pageInfo?.hasNextPage ? 100 : 0
  if (context === 'objectKinds')
    xledgerData.edges = xledgerData.entity?.glSetupEntries || []
  const data = xledgerData.edges || []
  return {
    data,
    metaData: {
      filteredDocuments,
      lastCursor: data[data.length - 1]?.cursor
    }
  }
}

export const apiRequestInXledger = async (token, body, apiName) => {
  try {
    const response = await axios({
      url: (process.env.XLEDGER_URL || 'https://demo.xledger.net') + '/graphql',
      method: 'post',
      headers: {
        Accept: 'application/json',
        Authorization: `token ${token}`
      },
      data: body
    })

    const { data = {}, errors } = response.data

    if (size(errors)) {
      const [error] = errors
      const { message } = error || {}
      throw new Error(message)
    } else return data[`${apiName}`]
  } catch (error) {
    throw new Error(error)
  }
}

export const getApiRequestBodyForXledgerProjects = (queryData) => {
  const body = {
    query: `
          query($first: Int, $after: String) {
              projects(first: $first, orderBy: {field: CREATED_AT, direction: ASC}, after: $after) {
                edges {
                  node {
                    description
                    dbId
                    code
                  }
                  cursor
                }
                pageInfo {
                  hasNextPage
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const getApiRequestBodyForXledgerAccounts = (queryData) => {
  const body = {
    query: `
          query($first: Int, $after: String) {
              accounts(first: $first, orderBy: {field: CREATED_AT, direction: ASC}, after: $after) {
                edges {
                  node {
                    description
                    dbId
                    code
                  }
                  cursor
                }
                pageInfo {
                  hasNextPage
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const getApiRequestBodyForXledgerTaxRules = (queryData) => {
  const body = {
    query: `
          query($first: Int, $after: String, $filter: ObjectValue_Filter) {
              objectValues(first: $first, orderBy: {field: CREATED_AT, direction: ASC}, after: $after, filter: $filter) {
                edges {
                  node {
                    description
                    dbId
                    code
                  }
                  cursor
                }
                pageInfo {
                  hasNextPage
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const getApiRequestBodyForXledgerCompanies = (queryData) => {
  const body = {
    query: `
          query($first: Int, $after: String, $filter: Company_Filter) {
              companies(first: $first, orderBy: {field: CREATED_AT, direction: ASC}, after: $after, filter: $filter) {
                edges {
                  node {
                    companyNumber
                    description
                    dbId
                    ownerDbId
                  }
                  cursor
                }
                pageInfo {
                  hasNextPage
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const getApiRequestBodyForXledgerOwners = (queryData) => {
  const body = {
    query: `
          query($first: Int, $after: String) {
              entities(first: $first, orderBy: {field: CREATED_AT, direction: ASC}, after: $after) {
                edges {
                  node {
                    owner {
                      dbId
                      description
                    }
                  }
                  cursor
                }
                pageInfo {
                  hasNextPage
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const getApiRequestBodyForXledgerGlObjects = (queryData) => {
  const body = {
    query: `
          query($first: Int, $after: String, $filter: ObjectValue_Filter) {
              objectValues(first: $first, orderBy: {field: CREATED_AT, direction: ASC}, after: $after, filter: $filter) {
                edges {
                  node {
                    code
                    dbId
                    description
                  }
                  cursor
                }
                pageInfo {
                  hasNextPage
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const getApiRequestBodyForXledgerObjectKinds = (queryData) => {
  const body = {
    query: `
          query {
              viewer {
                entity {
                  glSetupEntries {
                    fieldObject {
                      objectKind {
                        dbId
                        name
                      }
                    }
                  }
                }
              }
            }
          `,
    variables: queryData
  }

  return body
}

export const preparedQueryForXledger = (options) => {
  const { limit, cursor } = options
  const queryData = {}

  if (limit) queryData.first = limit
  if (cursor) queryData.after = cursor

  return queryData
}

export const queryIntegrationData = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], user)
  const { partnerId } = user
  body.partnerId = partnerId
  const requiredFields = ['type']
  const isDirectPartner = await partnerHelper.isDirectPartner(partnerId)
  if (isDirectPartner) requiredFields.push('accountId')
  appHelper.checkRequiredFields(requiredFields, body)
  const integrationQuery = preparedQueryForIntegration(body)
  const { type } = body
  let pipeline = []
  if (type === 'xledger') {
    pipeline = getIntegrationPipelineForXledger()
  }
  const finalPipeline = [
    {
      $match: integrationQuery
    },
    ...pipeline
  ]

  const integration = await getIntegrationDetailsByAggregate(finalPipeline)

  return integration
}

const getUlBranchPipelineForXledger = () => [
  {
    $lookup: {
      from: 'branches',
      localField: 'partnerId',
      foreignField: 'partnerId',
      let: { mapXledgerBranches: '$mapXledgerBranches' },
      pipeline: [
        {
          $match: {
            $expr: {
              $not: {
                $in: [
                  '$_id',
                  { $ifNull: ['$$mapXledgerBranches.branchId', []] }
                ]
              }
            }
          }
        }
      ],
      as: 'ul_branches'
    }
  }
]

const getUlGroupPipelineForXledger = () => [
  {
    $lookup: {
      from: 'listings',
      let: { mapGroups: '$mapXledgerGroups' },
      localField: 'partnerId',
      foreignField: 'partnerId',
      pipeline: [
        {
          $match: {
            groupId: { $exists: true },
            $expr: {
              $not: {
                $in: [
                  { $toString: '$groupId' },
                  { $ifNull: ['$$mapGroups.propertyGroupId', []] }
                ]
              }
            }
          }
        },
        {
          $group: {
            _id: '$groupId'
          }
        },
        {
          $project: {
            groupId: '$_id'
          }
        }
      ],
      as: 'ul_groups'
    }
  }
]

const getIntegrationPipelineForXledger = () => [
  {
    $lookup: {
      from: 'ledger_accounts',
      let: { mapXledgerAccounts: '$mapXledgerAccounts' },
      localField: 'partnerId',
      foreignField: 'partnerId',
      pipeline: [
        {
          $match: {
            $expr: {
              $not: {
                $in: [
                  '$_id',
                  { $ifNull: ['$$mapXledgerAccounts.accountingId', []] }
                ]
              }
            }
          }
        }
      ],
      as: 'ul_accounts'
    }
  },
  {
    $lookup: {
      from: 'tax_codes',
      localField: 'partnerId',
      foreignField: 'partnerId',
      let: { mapXledgerTaxCodes: '$mapXledgerTaxCodes' },
      pipeline: [
        {
          $match: {
            $expr: {
              $not: {
                $in: [
                  '$_id',
                  { $ifNull: ['$$mapXledgerTaxCodes.taxCodeId', []] }
                ]
              }
            }
          }
        }
      ],
      as: 'ul_taxCodes'
    }
  },
  ...getUlBranchPipelineForXledger(),
  ...getUlGroupPipelineForXledger(),
  {
    $project: {
      _id: 1,
      clientKey: 1,
      createdAt: 1,
      createdBy: 1,
      enabledPeriodSync: 1,
      fromDate: 1,
      mapXledgerAccounts: 1,
      mapXledgerTaxCodes: 1,
      partnerId: 1,
      status: 1,
      tenantAccountType: 1,
      type: 1,
      ul_accounts: 1,
      ul_branches: {
        _id: 1,
        branchSerialId: 1,
        name: 1
      },
      ul_groups: {
        groupId: 1
      },
      ul_taxCodes: 1,
      errorsMeta: 1,
      mapXledgerGlObjects: 1,
      mapXledgerBranches: 1,
      mapXledgerGroups: 1,
      mapXledgerInternalAssignmentIds: 1,
      mapXledgerInternalLeaseIds: 1,
      mapXledgerEmployeeIds: 1,
      mapXledgerTransactionText: 1,
      mapXledgerObjectKinds: 1,
      isGlobal: 1
    }
  }
]

export const preparedQueryForIntegration = (body) => {
  const { accountId, partnerId, type } = body
  const queryData = {
    partnerId
  }

  if (type) queryData.type = type
  if (accountId) queryData.accountId = accountId

  return queryData
}

const getIntegrationDetailsByAggregate = async (pipeline = []) => {
  const [integration = {}] =
    (await IntegrationCollection.aggregate(pipeline)) || []

  return integration
}

export const prepareUpdateDataForUpdateIntegrationItem = (
  params,
  integrationInfo
) => {
  const { updateType, data } = params
  data.integrationInfo = integrationInfo
  const modifiersObj = {
    addXledgerMapAccount: prepareUpdateDataForAddMapXledgerAccount,
    removeXledgerMapAccount: prepareUpdateDataForRemoveXledgerMapAccount,
    addXledgerMapTaxCode: prepareUpdateDataForAddXledgerMapTaxCode,
    removeXledgerMapTaxCode: prepareUpdateDataForRemoveXledgerMapTaxCode,
    addXledgerMapGlObjects: prepareUpdateDataForMapXledgerGlObjects,
    addXledgerMapBranches: prepareUpdateDataForMapXledgerBranches,
    removeXledgerMapBranch: prepareUpdateDataForRemoveXledgerMapBranch,
    addXledgerMapGroups: prepareUpdateDataForMapXledgerGroups,
    removeXledgerMapGroup: prepareUpdateDataForRemoveXledgerMapGroup,
    addXledgerMapInternalAssignmentId:
      prepareUpdateDataForMapXledgerInternalAssignmentId,
    removeXledgerMapInternalAssignmentId:
      prepareUpdateDataForRemoveXledgerMapInternalAssignmentId,
    addXledgerMapInternalLeaseId: prepareUpdateDataForMapXledgerInternalLeaseId,
    removeXledgerMapInternalLeaseId:
      prepareUpdateDataForRemoveXledgerMapInternalLeaseId,
    addXledgerMapEmployeeId: prepareUpdateDataForMapXledgerEmployeeId,
    removeXledgerMapEmployeeId: prepareUpdateDataForRemoveXledgerMapEmployeeId,
    addXledgerMapTransactionText: prepareUpdateDataForMapXledgerTransactionText,
    addXledgerMapObjectKind: prepareUpdateDataForMapXledgerObjectKind,
    removeXledgerMapObjectKind: prepareUpdateDataForRemoveXledgerMapObjectKind
  }
  if (modifiersObj[updateType]) return modifiersObj[updateType](data)

  return {}
}

const prepareUpdateDataForAddXledgerMapTaxCode = (params) => {
  const { integrationInfo, mapXledgerTaxCode } = params
  if (!size(mapXledgerTaxCode))
    throw new CustomError(401, 'Could not find resource to update')
  appHelper.checkRequiredFields(
    ['taxCodeId', 'taxCodeName', 'taxCode', 'xledgerTaxCodeName', 'xledgerId'],
    mapXledgerTaxCode
  )
  const existingMap = (integrationInfo.mapXledgerTaxCodes || []).find(
    (item) => item.taxCodeId === mapXledgerTaxCode.taxCodeId
  )
  if (existingMap) throw new CustomError(409, 'TaxCode mapping already exists')
  return { $addToSet: { mapXledgerTaxCodes: mapXledgerTaxCode } }
}

const prepareUpdateDataForAddMapXledgerAccount = (params) => {
  const { integrationInfo, mapXledgerAccount } = params
  if (!size(mapXledgerAccount))
    throw new CustomError(401, 'Could not find resource to update')
  appHelper.checkRequiredFields(
    [
      'accountingId',
      'accountName',
      'accountNumber',
      'xledgerAccountName',
      'xledgerId'
    ],
    mapXledgerAccount
  )
  const existingMap = (integrationInfo.mapXledgerAccounts || []).find(
    (item) => item.accountingId === mapXledgerAccount.accountingId
  )
  if (existingMap) throw new CustomError(409, 'Account mapping already exists')

  return { $addToSet: { mapXledgerAccounts: mapXledgerAccount } }
}

const prepareUpdateDataForMapXledgerBranches = (params) => {
  const { integrationInfo, mapXledgerBranch } = params
  if (!size(mapXledgerBranch))
    throw new CustomError(401, 'Could not find resource to update')
  const requiredFields = [
    'branchId',
    'branchName',
    'glObjectDbId',
    'glObjectName'
  ]
  appHelper.checkRequiredFields(requiredFields, mapXledgerBranch)
  const existMapping = (integrationInfo.mapXledgerBranches || []).find(
    (item) => item.branchId === mapXledgerBranch.branchId
  )
  if (existMapping) throw new CustomError(409, 'Branch mapping already exists')
  return { $addToSet: { mapXledgerBranches: mapXledgerBranch } }
}

const prepareUpdateDataForMapXledgerGroups = (params) => {
  const { integrationInfo, mapXledgerGroup } = params
  if (!size(mapXledgerGroup))
    throw new CustomError(401, 'Could not find resource to update')
  const requiredFields = ['propertyGroupId', 'glObjectDbId', 'glObjectName']
  appHelper.checkRequiredFields(requiredFields, mapXledgerGroup)
  const existMapping = (integrationInfo.mapXledgerGroups || []).find(
    (item) => item.propertyGroupId === mapXledgerGroup.propertyGroupId
  )
  if (existMapping)
    throw new CustomError(409, 'Property group mapping already exists')
  return { $addToSet: { mapXledgerGroups: mapXledgerGroup } }
}

const prepareUpdateDataForMapXledgerInternalAssignmentId = (params) => {
  const { integrationInfo, mapXledgerInternalAssignmentId } = params
  if (!size(mapXledgerInternalAssignmentId))
    throw new CustomError(401, 'Could not find resource to update')
  const requiredFields = [
    'internalAssignmentId',
    'glObjectDbId',
    'glObjectName'
  ]
  appHelper.checkRequiredFields(requiredFields, mapXledgerInternalAssignmentId)
  const existMapping = (
    integrationInfo.mapXledgerInternalAssignmentIds || []
  ).find(
    (item) =>
      item.internalAssignmentId ===
      mapXledgerInternalAssignmentId.internalAssignmentId
  )
  if (existMapping)
    throw new CustomError(409, 'Internal assignment id mapping already exists')
  return {
    $addToSet: {
      mapXledgerInternalAssignmentIds: mapXledgerInternalAssignmentId
    }
  }
}

const prepareUpdateDataForMapXledgerInternalLeaseId = (params) => {
  const { integrationInfo, mapXledgerInternalLeaseId } = params
  if (!size(mapXledgerInternalLeaseId))
    throw new CustomError(401, 'Could not find resource to update')
  const requiredFields = ['internalLeaseId', 'glObjectDbId', 'glObjectName']
  appHelper.checkRequiredFields(requiredFields, mapXledgerInternalLeaseId)
  const existMapping = (integrationInfo.mapXledgerInternalLeaseIds || []).find(
    (item) => item.internalLeaseId === mapXledgerInternalLeaseId.internalLeaseId
  )
  if (existMapping)
    throw new CustomError(409, 'Internal lease id mapping already exists')
  return {
    $addToSet: {
      mapXledgerInternalLeaseIds: mapXledgerInternalLeaseId
    }
  }
}

const prepareUpdateDataForMapXledgerEmployeeId = (params) => {
  const { integrationInfo, mapXledgerEmployeeId } = params
  if (!size(mapXledgerEmployeeId))
    throw new CustomError(401, 'Could not find resource to update')
  const requiredFields = ['employeeId', 'glObjectDbId', 'glObjectName']
  appHelper.checkRequiredFields(requiredFields, mapXledgerEmployeeId)
  const existMapping = (integrationInfo.mapXledgerEmployeeIds || []).find(
    (item) => item.employeeId === mapXledgerEmployeeId.employeeId
  )
  if (existMapping)
    throw new CustomError(409, 'Employee id mapping already exists')
  return {
    $addToSet: {
      mapXledgerEmployeeIds: mapXledgerEmployeeId
    }
  }
}

const prepareUpdateDataForMapXledgerTransactionText = (params) => {
  const { mapXledgerTransactionText } = params
  if (!params.hasOwnProperty('mapXledgerTransactionText'))
    throw new CustomError(401, 'Could not find resource to update')
  return {
    $set: {
      mapXledgerTransactionText
    }
  }
}

const prepareUpdateDataForMapXledgerObjectKind = (params) => {
  const { integrationInfo, mapXledgerObjectKind } = params
  if (!size(mapXledgerObjectKind))
    throw new CustomError(401, 'Could not find resource to update')
  appHelper.checkRequiredFields(
    ['field', 'objectKindDbId', 'objectKindName'],
    mapXledgerObjectKind
  )
  const existMapping = (integrationInfo.mapXledgerObjectKinds || []).find(
    (item) => item.field === mapXledgerObjectKind.field
  )
  if (existMapping)
    throw new CustomError(409, 'Object kind mapping already exists')
  return {
    $addToSet: {
      mapXledgerObjectKinds: mapXledgerObjectKind
    }
  }
}

const prepareUpdateDataForMapXledgerGlObjects = (params) => {
  const { mapXledgerGlObjects = {} } = params
  if (!size(mapXledgerGlObjects)) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  const updateData = {}
  for (const [key, value] of Object.entries(mapXledgerGlObjects)) {
    updateData['mapXledgerGlObjects.' + key] = value
  }
  return { $set: updateData }
}

const prepareUpdateDataForRemoveXledgerMapAccount = (data) => {
  const { ledgerAccountId } = data
  if (!ledgerAccountId)
    throw new CustomError(401, 'Could not find resource to update')

  return { $pull: { mapXledgerAccounts: { accountingId: ledgerAccountId } } }
}

const prepareUpdateDataForRemoveXledgerMapBranch = (data) => {
  const { branchId } = data
  if (!branchId) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  return { $pull: { mapXledgerBranches: { branchId } } }
}

const prepareUpdateDataForRemoveXledgerMapGroup = (data) => {
  const { propertyGroupId } = data
  if (!propertyGroupId) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  return { $pull: { mapXledgerGroups: { propertyGroupId } } }
}

const prepareUpdateDataForRemoveXledgerMapInternalAssignmentId = (data) => {
  const { internalAssignmentId } = data
  if (!internalAssignmentId) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  return {
    $pull: { mapXledgerInternalAssignmentIds: { internalAssignmentId } }
  }
}

const prepareUpdateDataForRemoveXledgerMapInternalLeaseId = (data) => {
  const { internalLeaseId } = data
  if (!internalLeaseId) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  return {
    $pull: { mapXledgerInternalLeaseIds: { internalLeaseId } }
  }
}

const prepareUpdateDataForRemoveXledgerMapEmployeeId = (data) => {
  const { employeeId } = data
  if (!employeeId) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  return {
    $pull: { mapXledgerEmployeeIds: { employeeId } }
  }
}

const prepareUpdateDataForRemoveXledgerMapObjectKind = (data) => {
  const { objectKindField } = data
  if (!objectKindField) {
    throw new CustomError(401, 'Could not find resource to update')
  }
  return {
    $pull: { mapXledgerObjectKinds: { field: objectKindField } }
  }
}

const prepareUpdateDataForRemoveXledgerMapTaxCode = (data) => {
  const { taxCodeId } = data
  if (!taxCodeId)
    throw new CustomError(401, 'Could not find resource to update')

  return { $pull: { mapXledgerTaxCodes: { taxCodeId } } }
}
