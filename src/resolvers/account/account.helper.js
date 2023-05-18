import { compact, indexOf, map, omit, size, uniq } from 'lodash'
import moment from 'moment-timezone'
import { validateNorwegianIdNumber } from 'norwegian-national-id-validator'
import { AccountCollection, IntegrationCollection } from '../models'
import { CustomError } from '../common'
import {
  accountHelper,
  appHelper,
  appQueueHelper,
  contractHelper,
  counterHelper,
  integrationHelper,
  invoiceHelper,
  listingHelper,
  partnerHelper,
  partnerSettingHelper,
  tenantHelper,
  userHelper
} from '../helpers'

export const getAccountIdsByQuery = async (query = {}) => {
  const accountIds = await AccountCollection.distinct('_id', query)
  return accountIds || []
}

export const getAccountById = async (id, session) => {
  const account = await AccountCollection.findById(id)
    .session(session)
    .populate(['agent', 'branch', 'person', 'partner', 'organization'])
    .exec()
  return account
}

export const getAnAccount = async (query, session, populate = []) => {
  const account = await AccountCollection.findOne(query)
    .session(session)
    .populate(populate)
  return account
}

export const getAccounts = async (query, session) => {
  const accounts = await AccountCollection.find(query).session(session)
  return accounts
}

export const getAccountsWithProjection = async (params = {}, session) => {
  const { query, options = {}, projection = '' } = params
  const { sort = {} } = options
  const accounts = await AccountCollection.find(query, projection)
    .populate('person', '_id profile')
    .sort(sort)
    .session(session)
  return accounts
}

export const getAccountsWithSelect = async (query, select = []) => {
  const accounts = await AccountCollection.find(query).select(select)
  return accounts
}

export const createAccountFieldNameForApi = (account) => {
  const accountData = {
    _id: account._id,
    name: account.name,
    type: account.type,
    partnerId: account.partnerId,
    status: account.status,
    id: account.serial,
    address: account.address,
    zipCode: account.zipCode,
    city: account.city,
    country: account.country,
    bankAccountNumbers: account.bankAccountNumbers,
    powerOffice: account.powerOffice
  }

  const userInfo = size(account) && size(account.person) ? account.person : {}
  if (size(userInfo)) {
    accountData.userInfo = {
      name: userInfo.getName() || '',
      email: userInfo.getEmail() || '',
      phoneNumber: userInfo.getPhone() || '',
      address: userInfo.getHometown() || '',
      zipCode: userInfo.getZipCode() || '',
      city: userInfo.getCity() || '',
      country: userInfo.getCountry() || ''
    }
  }
  return accountData
}

const prepareAccountPipeline = (query) => [
  {
    $match: query
  }
]

export const prepareUnwindObj = (collectionName) => ({
  $unwind: {
    path: `$${collectionName}`,
    preserveNullAndEmptyArrays: true
  }
})

const prepareAgentsPipeline = () => {
  const unwindAgent = prepareUnwindObj('agent')
  return [
    {
      $lookup: {
        from: 'users',
        as: 'agent',
        localField: 'agentId',
        foreignField: '_id',
        pipeline: [
          {
            $project: {
              _id: 1,
              name: '$profile.name',
              avatarKey:
                appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
            }
          }
        ]
      }
    },
    unwindAgent
  ]
}

const prepareUserPipeline = () => {
  const unwindPerson = prepareUnwindObj('person')
  return [
    {
      $lookup: {
        from: 'users',
        as: 'person',
        localField: 'personId',
        foreignField: '_id',
        pipeline: [
          ...appHelper.getUserEmailPipeline(),
          {
            $project: {
              _id: 1,
              name: '$profile.name',
              'profile.country': 1,
              'profile.hometown': 1,
              'profile.zipCode': 1,
              'profile.city': 1,
              avatarKey:
                appHelper.getUserAvatarKeyPipeline('$profile.avatarKey'),
              email: 1,
              phoneNumber: '$profile.phoneNumber'
            }
          }
        ]
      }
    },
    unwindPerson
  ]
}

const prepareListingPipeline = () => [
  {
    $lookup: {
      from: 'listings',
      as: 'listing',
      localField: '_id',
      foreignField: 'accountId',
      let: { partnerId: '$partnerId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$partnerId', '$$partnerId']
            }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      totalProperties: { $size: { $ifNull: ['$listing', []] } }
    }
  }
]

const prepareInvoicePipeline = () => {
  const unwindInvoices = prepareUnwindObj('invoices')
  return [
    {
      $lookup: {
        from: 'invoices',
        as: 'invoices',
        localField: '_id',
        foreignField: 'accountId',
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  {
                    $eq: ['$status', 'overdue']
                  },
                  {
                    $eq: ['$invoiceType', 'invoice']
                  }
                ]
              }
            }
          }
        ]
      }
    },
    unwindInvoices
  ]
}

const prepareFinalGroupPipeline = () => ({
  $group: {
    _id: '$_id',
    invoiceTotal: { $sum: '$invoices.invoiceTotal' },
    paidTotal: { $sum: '$invoices.totalPaid' },
    creditedTotal: { $sum: '$invoices.creditedAmount' },
    accountId: { $first: '$_id' },
    totalActiveProperties: { $first: '$totalActiveProperties' },
    totalProperties: { $first: '$totalProperties' },
    agent: { $first: '$agent' },
    agentId: { $first: '$agentId' },
    person: { $first: '$person' },
    name: { $first: '$name' },
    type: { $first: '$type' },
    status: { $first: '$status' },
    partnerId: { $first: '$partnerId' },
    organization: { $first: '$organization' },
    organizationId: { $first: '$organizationId' },
    personId: { $first: '$personId' },
    createdAt: { $first: '$createdAt' },
    serial: { $first: '$serial' },
    address: { $first: '$address' },
    zipCode: { $first: '$zipCode' },
    city: { $first: '$city' },
    country: { $first: '$country' },
    branch: { $first: '$branch' },
    bankAccountNumbers: {
      $first: '$bankAccountNumbers'
    },
    invoiceAccountNumber: {
      $first: '$invoiceAccountNumber'
    }
  }
})

const prepareFinalProjectPipeline = () => ({
  $project: {
    _id: '$accountId',
    totalOverDue: {
      $subtract: [{ $add: ['$invoiceTotal', '$creditedTotal'] }, '$paidTotal']
    },
    totalActiveProperties: 1,
    totalProperties: 1,
    name: 1,
    type: 1,
    status: 1,
    serial: 1,
    agentId: 1,
    agent: 1,
    person: 1,
    personId: 1,
    organizationId: 1,
    partnerId: 1,
    organization: 1,
    createdAt: 1,
    branch: {
      _id: 1,
      name: 1
    },
    bankAccountNumbers: 1,
    invoiceAccountNumber: 1,
    address: {
      $cond: [
        { $eq: ['$type', 'person'] },
        '$person.profile.hometown',
        '$address'
      ]
    },
    zipCode: {
      $cond: [
        { $eq: ['$type', 'person'] },
        '$person.profile.zipCode',
        '$zipCode'
      ]
    },
    city: {
      $cond: [{ $eq: ['$type', 'person'] }, '$person.profile.city', '$city']
    },
    country: {
      $cond: [
        { $eq: ['$type', 'person'] },
        '$person.profile.country',
        '$country'
      ]
    }
  }
})

const prepareOrganizationPipeline = () => {
  const unwindOrganization = prepareUnwindObj('organization')
  return [
    {
      $lookup: {
        from: 'organizations',
        as: 'organization',
        localField: 'organizationId',
        foreignField: '_id',
        let: { partnerId: '$partnerId' },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ['$partnerId', '$$partnerId']
              }
            }
          },
          {
            $project: {
              _id: 1,
              name: 1,
              imageUrl: appHelper.getOrganizationLogoPipeline('$image')
            }
          }
        ]
      }
    },
    unwindOrganization
  ]
}

const prepareSortLimitOptionPipeline = (options) => {
  const { limit, skip, sort } = options
  return [
    {
      $sort: sort
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    }
  ]
}

const prepareBranchPipeline = () => [
  {
    $lookup: {
      from: 'branches',
      localField: 'branchId',
      foreignField: '_id',
      as: 'branch'
    }
  },
  prepareUnwindObj('branch')
]

const prepareAggregatePipeline = (query, options) => {
  const accountPipeline = prepareAccountPipeline(query)
  const agentsPipeline = prepareAgentsPipeline()
  const userPipeline = prepareUserPipeline()
  const listingPipeline = prepareListingPipeline()
  const invoicePipeline = prepareInvoicePipeline()
  const organizationPipeline = prepareOrganizationPipeline()
  const optionsPipeline = prepareSortLimitOptionPipeline(options) // Skip, limit, sort
  const branchPipeline = prepareBranchPipeline()
  const pipeline = [
    ...accountPipeline,
    ...optionsPipeline,
    ...listingPipeline,
    ...agentsPipeline,
    ...userPipeline,
    ...organizationPipeline,
    ...branchPipeline,
    ...invoicePipeline
  ]
  const finalGroup = prepareFinalGroupPipeline() // Returns object
  const finalProject = prepareFinalProjectPipeline() // Returns object
  pipeline.push(finalGroup, finalProject)
  pipeline.push({ $sort: options.sort }) // Merged in one array
  return pipeline
}

export const getAccountsDropdownForQuery = async (params = {}) => {
  const { query, options = {} } = params
  const { limit, skip } = options

  const accountsData = await AccountCollection.aggregate([
    {
      $match: query
    },
    {
      $sort: { name: 1 }
    },
    {
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $lookup: {
        from: 'organizations',
        foreignField: '_id',
        localField: 'organizationId',
        as: 'organization'
      }
    },
    appHelper.getUnwindPipeline('organization'),
    {
      $lookup: {
        from: 'users',
        foreignField: '_id',
        localField: 'personId',
        as: 'person'
      }
    },
    appHelper.getUnwindPipeline('person'),
    {
      $project: {
        _id: 1,
        name: 1,
        avatarKey: {
          $cond: [
            { $eq: ['$type', 'person'] },
            appHelper.getUserAvatarKeyPipeline('$person.profile.avatarKey'),
            appHelper.getOrganizationLogoPipeline('$organization.image')
          ]
        }
      }
    }
  ])

  return accountsData
}

export const getAccountsForQuery = async (params, session) => {
  const { query, options } = params
  const { limit, sort, skip } = options
  const accounts = []
  if (query.dataType) {
    delete query.dataType
    const accountsInfo = await AccountCollection.find(query)
      .populate('person')
      .sort(sort)
      .limit(limit)
      .session(session)
    for (const account of accountsInfo) {
      const preparedAccount = createAccountFieldNameForApi(account)
      accounts.push(preparedAccount)
    }
  } else {
    const accountInformation = await AccountCollection.find(query)
      .populate(['person', 'organization', 'agent', 'branch'])
      .sort(sort)
      .skip(skip)
      .limit(limit)
    accounts.push(...accountInformation)
  }
  return accounts
}

export const countAccounts = async (query, session) => {
  const numberOfAccounts = await AccountCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfAccounts
}

export const prepareAccountsQuery = async (query, session) => {
  const preparedQuery = {}
  if (size(query._id)) preparedQuery._id = query._id
  if (size(query.partnerId)) preparedQuery.partnerId = query.partnerId

  if (size(query.dataType)) {
    appHelper.checkRequiredFields(['partnerId'], query)
    preparedQuery.dataType = query.dataType
    // Its only for direct partners
    if (query.dataType === 'integrated_accounts' && size(query.partnerId)) {
      const integrationQuery = {
        partnerId: query.partnerId,
        status: 'integrated',
        type: 'power_office_go'
      }
      const integratedAccountIds = await IntegrationCollection.distinct(
        'accountId',
        integrationQuery
      ).session(session)

      if (size(integratedAccountIds)) {
        preparedQuery._id = { $in: integratedAccountIds }
      }
    } else if (query.dataType === 'get_account_for_pogo') {
      preparedQuery.powerOffice = { $exists: false }
      preparedQuery.serial = { $exists: true }
    } else if (query.dataType === 'get_updated_account') {
      preparedQuery.powerOffice = { $exists: true }
      preparedQuery['powerOffice.syncedAt'] = { $exists: true }
      preparedQuery.lastUpdate = { $exists: true }
      preparedQuery.serial = { $exists: true }
      preparedQuery['$expr'] = {
        $gte: ['$lastUpdate', '$powerOffice.syncedAt']
      }
    }
  }
  return preparedQuery
}

export const prepareAccountsQueryForUpdateAccountForPogo = (query) => {
  const preparedQuery = {}
  if (size(query._id)) preparedQuery._id = query._id
  if (size(query.partnerId)) preparedQuery.partnerId = query.partnerId
  return preparedQuery
}

export const getAccountUpdateData = (data) => {
  const updateData = {}

  if (size(data) && size(data.powerOffice))
    updateData.powerOffice = data.powerOffice

  return updateData
}

const prepareAccountsOptions = (body) => {
  const { query, options } = body
  if (size(query.dataType)) {
    const limit =
      indexOf(
        ['get_account_for_pogo', 'get_updated_account'],
        query.dataType
      ) !== -1
        ? 1
        : 0
    if (limit) options.limit = limit
    options.sort = { serial: 1 }
  }
  return options
}

const getAccountsForPartnerApp = async (query, options) => {
  const pipeline = prepareAggregatePipeline(query, options)
  const accounts = await AccountCollection.aggregate(pipeline)
  return accounts
}

export const queryAccountsForPartnerApp = async (req) => {
  const { body, user } = req
  const { options, query } = body
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { partnerId } = user
  appHelper.validateId({ partnerId })
  query.partnerId = partnerId
  const preparedQuery = await prepareQueryForAccounts(query)
  const accountsData = await getAccountsForPartnerApp(preparedQuery, options)
  const totalDocuments = await countAccounts({
    partnerId
  })
  const filteredDocuments = await countAccounts(preparedQuery)
  return {
    data: accountsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const queryAccounts = async (req) => {
  const { body, session, user } = req
  let { query, options } = body
  console.log(user)
  appHelper.checkPartnerId(user, query)
  const preparedQuery = await prepareAccountsQuery(query, session)
  const preparedOptions = prepareAccountsOptions(body)
  query = preparedQuery
  options = preparedOptions
  appHelper.validateSortForQuery(options.sort)
  const accountsData = await getAccountsForQuery(
    {
      query: { ...query },
      options
    },
    session
  )
  if (size(query.dataType)) {
    delete query.dataType
  }
  const totalDocuments = await countAccounts(
    { partnerId: query.partnerId },
    session
  )
  const filteredDocuments = await countAccounts(query, session)
  return {
    data: accountsData,
    metaData: { filteredDocuments, totalDocuments },
    req
  }
}

const prepareQueryForAccountsDropdown = (query = {}) => {
  const { agentId, branchId, partnerId, searchString } = query
  const preparedQuery = {
    partnerId,
    status: { $ne: 'archived' }
  }
  if (branchId) preparedQuery['branchId'] = branchId
  if (agentId) preparedQuery['agentId'] = agentId

  if (searchString) preparedQuery.name = new RegExp(searchString, 'i')
  return preparedQuery
}

export const queryAccountsDropdown = async (req) => {
  const { body, user = {} } = req
  appHelper.checkRequiredFields(['userId', 'partnerId'], user)
  const { userId, partnerId } = user

  appHelper.checkUserId(userId)
  appHelper.validateId({ partnerId })

  const { query, options } = body
  query.partnerId = partnerId
  const preparedQuery = prepareQueryForAccountsDropdown(query)
  const accountsDropdownData = await getAccountsDropdownForQuery({
    query: preparedQuery,
    options
  })
  const filteredDocuments = await countAccounts(preparedQuery)
  const totalDocuments = await countAccounts({ partnerId })

  return {
    data: accountsDropdownData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

const lookupInvoiceForAccountDetails = (startOfMonth, endOfMonth) => [
  {
    $lookup: {
      from: 'invoices',
      localField: '_id',
      foreignField: 'accountId',
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$invoiceType', 'invoice']
            }
          }
        },
        {
          $group: {
            _id: null,
            invoiceTotalAmount: {
              $sum: {
                $cond: [{ $eq: ['$status', 'overdue'] }, '$invoiceTotal', 0]
              }
            },
            invoiceTotalPaidAmount: {
              $sum: {
                $cond: [{ $eq: ['$status', 'overdue'] }, '$totalPaid', 0]
              }
            },
            totalCreditedAmount: {
              $sum: {
                $cond: [{ $eq: ['$status', 'overdue'] }, '$creditedAmount', 0]
              }
            },
            invoicedThisMonthTotalAmount: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      {
                        $gte: ['$dueDate', startOfMonth]
                      },
                      { $lte: ['$dueDate', endOfMonth] }
                    ]
                  },
                  '$invoiceTotal',
                  0
                ]
              }
            },
            invoicedThisMonthTotalCreditedAmount: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      {
                        $gte: ['$dueDate', moment().startOf('month').toDate()]
                      },
                      { $lte: ['$dueDate', moment().endOf('month').toDate()] }
                    ]
                  },
                  '$creditedAmount',
                  0
                ]
              }
            }
          }
        },
        {
          $project: {
            totalOverDue: {
              $subtract: [
                { $add: ['$invoiceTotalAmount', '$totalCreditedAmount'] },
                '$invoiceTotalPaidAmount'
              ]
            },
            invoiceThisMonth: {
              $add: [
                '$invoicedThisMonthTotalAmount',
                '$invoicedThisMonthTotalCreditedAmount'
              ]
            }
          }
        }
      ],
      as: 'invoiceInfo'
    }
  },
  appHelper.getUnwindPipeline('invoiceInfo')
]

const lookupDueTaskForAccountDetails = () => [
  {
    $lookup: {
      from: 'tasks',
      localField: '_id',
      foreignField: 'accountId',
      let: { partnerId: '$partnerId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ['$partnerId', '$$partnerId'] },
                { $lte: ['$dueDate', new Date()] }
              ]
            }
          }
        }
      ],
      as: 'taskInfo'
    }
  }
]

const lookupListingForAccountDetails = () => [
  {
    $lookup: {
      from: 'listings',
      localField: '_id',
      foreignField: 'accountId',
      let: { partnerId: '$partnerId' },
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ['$partnerId', '$$partnerId']
            }
          }
        },
        {
          $group: {
            _id: null,
            count: { $sum: 1 },
            totalHasActiveLease: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $ne: ['$propertyStatus', 'archived'] },
                      { $eq: ['$hasActiveLease', true] }
                    ]
                  },
                  1,
                  0
                ]
              }
            },
            totalProperty: {
              $sum: {
                $cond: [{ $ne: ['$propertyStatus', 'archived'] }, 1, 0]
              }
            }
          }
        }
      ],
      as: 'listingInfo'
    }
  },
  appHelper.getUnwindPipeline('listingInfo')
]

const lookupUserForAccountDetails = () => [
  {
    $lookup: {
      from: 'users',
      localField: 'personId',
      foreignField: '_id',
      pipeline: [
        ...appHelper.getUserEmailPipeline(),
        {
          $project: {
            _id: 1,
            profile: 1,
            email: 1,
            status: 1,
            avatarKey: appHelper.getUserAvatarKeyPipeline('$profile.avatarKey')
          }
        }
      ],
      as: 'userInfo'
    }
  },
  appHelper.getUnwindPipeline('userInfo')
]

const lookupOrganizationForAccountDetails = () => [
  {
    $lookup: {
      from: 'organizations',
      localField: 'organizationId',
      foreignField: '_id',
      as: 'organizationInfo'
    }
  },
  appHelper.getUnwindPipeline('organizationInfo')
]

const getFinalProjectForAccountDetails = () => [
  {
    $project: {
      _id: 1,
      dueTask: {
        $size: { $ifNull: ['$taskInfo', []] }
      },
      name: {
        $cond: [
          { $eq: ['$type', 'organization'] },
          '$organizationInfo.name',
          '$name'
        ]
      },
      orgId: {
        $cond: [
          { $eq: ['$type', 'organization'] },
          '$organizationInfo.orgId',
          null
        ]
      },
      type: 1,
      status: 1,
      invoiceAccountNumber: 1,
      totalProperties: '$listingInfo.count',
      totalRentCoveragePercentage: {
        $cond: [
          { $eq: ['$listingInfo.totalProperty', 0] },
          0,
          {
            $divide: [
              { $multiply: ['$listingInfo.totalHasActiveLease', 100] },
              '$listingInfo.totalProperty'
            ]
          }
        ]
      },
      totalActiveProperties: 1,
      serial: 1,
      aboutText: 1,
      address: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$userInfo.profile.hometown',
          '$address'
        ]
      },
      zipCode: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$userInfo.profile.zipCode',
          '$zipCode'
        ]
      },
      city: {
        $cond: [{ $eq: ['$type', 'person'] }, '$userInfo.profile.city', '$city']
      },
      country: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$userInfo.profile.country',
          '$country'
        ]
      },
      userInfo: {
        _id: 1,
        email: 1,
        name: '$userInfo.profile.name',
        phoneNumber: '$userInfo.profile.phoneNumber',
        norwegianNationalIdentification:
          '$userInfo.profile.norwegianNationalIdentification',
        hometown: '$userInfo.profile.hometown',
        zipCode: '$userInfo.profile.zipCode',
        city: '$userInfo.profile.city',
        country: '$userInfo.profile.country',
        avatarKey: 1
      },
      branchInfo: 1,
      agentInfo: 1,
      totalOverDue: '$invoiceInfo.totalOverDue',
      invoiceThisMonth: '$invoiceInfo.invoiceThisMonth',
      avatarKey: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$userInfo.avatarKey',
          appHelper.getOrganizationLogoPipeline('$organizationInfo.image')
        ]
      },
      accountNumber: '$invoiceAccountNumber',
      bankAccountNumbers: 1,
      vatRegistered: 1
    }
  },
  {
    $addFields: {
      totalRentCoveragePercentage: {
        $cond: [
          {
            $gte: [
              {
                $subtract: [
                  '$totalRentCoveragePercentage',
                  { $floor: '$totalRentCoveragePercentage' }
                ]
              },
              0.5
            ]
          },
          { $ceil: '$totalRentCoveragePercentage' },
          { $floor: '$totalRentCoveragePercentage' }
        ]
      }
    }
  }
]

// userInfo === personInfo

const getAccountDetails = async (params) => {
  const { _id, partnerId } = params
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const startOfMonth = (await appHelper.getActualDate(partnerSettings, true))
    .startOf('month')
    .toDate()
  const endOfMonth = (await appHelper.getActualDate(partnerSettings, true))
    .endOf('month')
    .toDate()
  const pipeline = [
    {
      $match: {
        _id,
        partnerId
      }
    },
    ...lookupInvoiceForAccountDetails(startOfMonth, endOfMonth),
    ...lookupDueTaskForAccountDetails(),
    ...lookupListingForAccountDetails(),
    ...lookupUserForAccountDetails(),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...appHelper.getCommonAgentInfoPipeline(),
    ...lookupOrganizationForAccountDetails(),
    ...getFinalProjectForAccountDetails()
  ]
  const [accountDetailsInfo] =
    (await AccountCollection.aggregate(pipeline)) || []
  if (!size(accountDetailsInfo))
    throw new CustomError(404, "Doesn't found Account details data")
  return accountDetailsInfo
}

export const queryAccountDetails = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkRequiredFields(['partnerId', 'userId'], user)
  const { partnerId } = user
  appHelper.checkRequiredFields(['accountId'], body)
  body.partnerId = partnerId
  const { accountId } = body
  body._id = accountId
  delete body.accountId

  return await getAccountDetails(body)
}

export const queryAccountIdsForLambda = async (req) => {
  const { body = {}, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId', 'requestFor'], body)
  const { partnerId, requestFor } = body
  let accountIds = []
  if (requestFor === 'xledger') {
    accountIds = await integrationHelper.getUniqueFieldValues('accountId', {
      partnerId,
      type: 'xledger',
      status: 'integrated'
    })
  } else {
    accountIds = await getAccountIdsByQuery({ partnerId })
  }
  return accountIds
}

export const getNotSignedAccountIds = async (partnerId) => {
  const query = {
    enabledEsigning: true,
    status: 'in_progress',
    'landlordAssignmentSigningStatus.signed': { $ne: true }
  }
  if (partnerId) query.partnerId = partnerId
  return await contractHelper.getUniqueFieldValue('accountId', query)
}

export const prepareQueryForAccounts = async (query) => {
  console.log('=== accountsQuery', query)
  const { createdAt = {}, eSignStatus = '', partnerId, status = [] } = query
  let {
    name = '',
    email = '',
    phoneNumber = '',
    ssn = '',
    searchKeyword = ''
  } = query
  query.status = { $ne: 'archived' }
  if (size(status)) {
    query.status = { $in: status }
  }
  const { startDate, endDate } = createdAt
  if (startDate && endDate) {
    query.createdAt = {
      $gte: new Date(startDate),
      $lte: new Date(endDate)
    }
  }
  const isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  if (!isBrokerPartner && query.type) {
    delete query.type
  }
  if (eSignStatus === 'sentToAccount') {
    const accountIds = await getNotSignedAccountIds(partnerId)
    if (size(accountIds)) {
      query._id = { $in: accountIds }
    } else {
      query._id = 'nothing'
    }
  }

  if (name || email || phoneNumber || ssn || searchKeyword) {
    let userIds = []
    if (searchKeyword) {
      searchKeyword = searchKeyword.trim()
      userIds = await userHelper.getUserIdsByQuery({
        $or: [
          { 'emails.address': { $regex: searchKeyword, $options: 'i' } },
          {
            'profile.phoneNumber': { $regex: searchKeyword, $options: 'i' }
          },
          {
            'profile.norwegianNationalIdentification': {
              $regex: searchKeyword,
              $options: 'i'
            }
          }
        ]
      })
      console.log('=== UserId with searchKeyword', userIds)
    } else if (name) {
      name = name.trim()
      query.name = { $regex: name, $options: 'i' }
    } else if (email) {
      email = email.trim()
      userIds = await userHelper.getUserIdsByQuery({
        'emails.address': { $regex: email, $options: 'i' }
      })
    } else if (phoneNumber) {
      phoneNumber = phoneNumber.trim()
      userIds = await userHelper.getUserIdsByQuery({
        'profile.phoneNumber': { $regex: phoneNumber, $options: 'i' }
      })
    } else if (ssn) {
      ssn = ssn.trim()
      userIds = await userHelper.getUserIdsByQuery({
        'profile.norwegianNationalIdentification': {
          $regex: ssn,
          $options: 'i'
        }
      })
    }

    if (!name) {
      query['$or'] = [{ personId: { $in: userIds } }]
      if (searchKeyword) {
        query['$or'].push({ name: { $regex: searchKeyword, $options: 'i' } })
        if (!isNaN(searchKeyword)) {
          query['$or'].push({ serial: parseInt(searchKeyword) })
        }
      }
    }
  }

  const updatedQuery = omit(query, [
    'eSignStatus',
    'email',
    'phoneNumber',
    'searchKeyword',
    'sort',
    'ssn'
  ])
  return updatedQuery
}

export const getAccountsByAggregate = async (preparedQuery = {}) => {
  if (!size(preparedQuery)) {
    throw new CustomError(404, 'Query not found to get accounts')
  }
  const accounts =
    (await AccountCollection.aggregate([
      {
        $match: preparedQuery
      },
      {
        $lookup: {
          from: 'users',
          as: 'user',
          localField: 'personId',
          foreignField: '_id'
        }
      },
      {
        $unwind: '$user'
      },
      {
        $group: {
          _id: null,
          accountIdsWithPhoneNumbers: {
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.phoneNumber', ''] },
                    { $ifNull: ['$user.profile.phoneNumber', false] }
                  ]
                },
                then: '$_id',
                else: '$$REMOVE'
              }
            }
          },
          accountNamesWithoutPhoneNumbers: {
            // if account user has NO phone number, add user's name in array
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.phoneNumber', ''] },
                    { $ifNull: ['$user.profile.phoneNumber', false] }
                  ]
                },
                then: '$$REMOVE',
                else: '$name'
              }
            }
          },
          accountNamesWithPhoneNumbers: {
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.phoneNumber', ''] },
                    { $ifNull: ['$user.profile.phoneNumber', false] }
                  ]
                },
                then: '$name',
                else: '$$REMOVE'
              }
            }
          }
        }
      }
    ])) || []
  return accounts
}

export const checkRequiredAndDuplicatedData = async (body = {}) => {
  appHelper.checkRequiredFields(
    ['agentId', 'branchId', 'name', 'partnerId'],
    body
  )
  const {
    agentId,
    branchId,
    contactPersonEmail,
    email,
    norwegianNationalIdentification,
    partnerId
  } = body
  appHelper.validateId({ agentId })
  appHelper.validateId({ branchId })
  // It is not allowed to create duplicate account with the same email.
  if (email) {
    body.email = email.toLowerCase()
    const user = await userHelper.getUserByEmail(body.email)
    if (size(user)) {
      const query = {
        personId: user._id,
        partnerId,
        type: 'person'
      }
      const existingAccount = await getAnAccount(query)
      if (size(existingAccount)) {
        throw new CustomError(405, `Email already exists`)
      }
    }
  }

  if (norwegianNationalIdentification) {
    const isValidNewSSN = validateNorwegianIdNumber(
      norwegianNationalIdentification
    )
    if (!isValidNewSSN) {
      throw new CustomError(405, 'Invalid NID Number')
    }
  }

  if (contactPersonEmail) {
    body.contactPersonEmail = contactPersonEmail.toLowerCase()
  }
}

export const checkingSerialNumberForAccount = async (
  accountId = '',
  params = {}
) => {
  const { isDirectPartner, partnerId, serial } = params
  let isExistSerialIdInTenants = {}
  const accountQuery = {
    _id: { $ne: accountId },
    partnerId,
    serial
  }
  const isExistSerialIdInAccounts = await getAnAccount(accountQuery)
  if (!size(isExistSerialIdInAccounts) && !isDirectPartner) {
    isExistSerialIdInTenants = await tenantHelper.getATenant({
      partnerId,
      serial
    })
  }
  if (size(isExistSerialIdInAccounts) || size(isExistSerialIdInTenants)) {
    throw new CustomError(405, 'Account serial already exists')
  }
  const query = { next_val: { $exists: true } }
  query._id = !isDirectPartner ? `tenant-${partnerId}` : `account-${partnerId}`
  const counter = await counterHelper.getACounter(query)
  const nextValue = counter && counter.next_val ? counter.next_val + 1 : 1
  // We only allow unused serial not new serial
  if (serial >= nextValue)
    throw new CustomError(405, `Serial id should be lower than ${nextValue}`)
  return serial
}

export const getAccountIdsByUserId = async (userId, partnerId) => {
  const user = await userHelper.getUserById(userId)
  if (!size(user)) {
    return []
  }
  const accounts = await getAccounts({
    personId: userId,
    partnerId
  })
  const accountIds = uniq(map(accounts, '_id'))
  return accountIds
}

export const countPropertiesForAccount = async (partnerId, accountId) => {
  const property = await listingHelper.countListings({ partnerId, accountId })
  return property
}

export const prepareActivePropertiesQuery = (params) => {
  const { accountId, partnerId } = params

  return {
    accountId,
    partnerId,
    $or: [{ hasActiveLease: true }, { hasAssignment: true }]
  }
}

export const prepareInProgressPropertiesQuery = (params) => {
  const { accountId } = params
  return {
    accountId,
    status: 'in_progress'
  }
}

export const prepareUpdateDataForTotalActiveProperties = async (
  params,
  session
) => {
  const activePropertiesQuery = prepareActivePropertiesQuery(params)
  const totalActiveProperties = await listingHelper.countListings(
    activePropertiesQuery,
    session
  )
  const inProgressContractQuery = prepareInProgressPropertiesQuery(params)
  const totalInProgressContracts = await contractHelper.countContracts(
    inProgressContractQuery,
    session
  )
  const updateData = { totalActiveProperties }
  if (totalActiveProperties > 0 && totalInProgressContracts === 0) {
    updateData.status = 'active'
  } else {
    updateData.status = 'in_progress'
  }
  return { $set: updateData }
}

export const prepareAccountsQueryForExcelCreator = async (params) => {
  const query = {}

  if (size(params) && params.partnerId) {
    let { email, name, phoneNumber, searchKeyword, ssn } = params
    const {
      address,
      agentId,
      branchId,
      createdAt,
      partnerId,
      invoiceAccountNumber,
      organizationId,
      personId,
      serial,
      type
    } = params

    query.partnerId = partnerId
    if (branchId) query.branchId = branchId
    if (agentId) query.agentId = agentId
    if (invoiceAccountNumber) query.invoiceAccountNumber = invoiceAccountNumber
    if (organizationId) query.organizationId = organizationId
    if (personId) query.personId = personId
    if (serial) query.serial = serial
    if (type) query.type = type
    if (address) {
      address.trim()
      query.address = { $regex: address, $options: 'i' }
    }
    // Set createdAt filters in query
    if (size(createdAt)) {
      const { startDate, endDate } = createdAt
      if (startDate && endDate) {
        query.createdAt = {
          $gte: new Date(startDate),
          $lte: new Date(endDate)
        }
      }
    }
    query.status = { $ne: 'archived' }
    const accountStatus = size(params.accountStatus)
      ? compact(params.accountStatus)
      : []
    // Set account status filters in query
    if (size(accountStatus)) query['status'] = { $in: accountStatus }
    const partnerInfo = await partnerHelper.getAPartner({
      _id: partnerId
    })
    if (partnerInfo?.accountType === 'broker') {
      //Set account type filters in query
      if (params.accountType && params.accountType !== 'all')
        query['type'] = params.accountType
    }

    // Set accounts emails, phone, ssn filters.
    if (name || email || phoneNumber || ssn || searchKeyword) {
      let userIds = []
      searchKeyword = searchKeyword ? searchKeyword.trim() : ''
      if (searchKeyword) {
        const keyword = new RegExp(searchKeyword, 'i')
        userIds = await userHelper.getUserIdsByQuery({
          $or: [
            { 'emails.address': keyword },
            {
              'profile.phoneNumber': keyword
            },
            {
              'profile.norwegianNationalIdentification': keyword
            }
          ]
        })

        query['$or'] = [
          { serial: parseInt(searchKeyword) || undefined },
          { name: keyword }
        ]
      } else if (name) {
        name = name.trim()
        query.name = { $regex: name, $options: 'i' }
      } else if (email) {
        email = email.trim()
        userIds = await userHelper.getUserIdsByQuery({
          'emails.address': { $regex: email, $options: 'i' }
        })
      } else if (phoneNumber) {
        phoneNumber = phoneNumber.trim()
        userIds = await userHelper.getUserIdsByQuery({
          'profile.phoneNumber': { $regex: phoneNumber, $options: 'i' }
        })
      } else if (ssn) {
        ssn = ssn.trim()
        userIds = await userHelper.getUserIdsByQuery({
          'profile.norwegianNationalIdentification': {
            $regex: ssn,
            $options: 'i'
          }
        })
      }

      if (size(userIds)) {
        const uniqUserIds = uniq(userIds)
        const searchQuery = query['$or'] || []
        query['$or'] = [...searchQuery, { personId: { $in: uniqUserIds } }]
      }
    }

    if (params.eSignStatus && params.eSignStatus === 'sentToAccount') {
      const notSignedAccountIds = await getNotSignedAccountIds()
      if (size(notSignedAccountIds)) query['_id'] = { $in: notSignedAccountIds }
    }
  }
  return query
}

export const getOverDueWithDecimalRound = async (query, partnerId) => {
  let overDue = 0
  const totalOverDue = (await invoiceHelper.getTotalOverDue(query)) || 0
  if (totalOverDue === 0) {
    return overDue
  }
  overDue = await appHelper.getRoundedAmount(totalOverDue, partnerId)
  return overDue
}

export const prepareResponseForAccountsQuery = async (
  accounts,
  userLanguage,
  partnerId
) => {
  const rowData = []

  if (size(accounts)) {
    const accountArrayLength = accounts.length
    for (let i = 0; i < accountArrayLength; i++) {
      const person = accounts[i].person || {}
      const contactPerson = size(person) ? person.profile?.name : ''
      const { emails = [], profile = {} } = person || {}
      const email = size(emails) ? emails[0]?.address : ''
      const phone = profile?.phoneNumber || ''
      const agent = size(accounts[i].agent)
        ? size(accounts[i].agent.profile)
          ? accounts[i].agent.profile.name
          : ''
        : ''
      const itemObj = {
        name: accounts[i]?.name,
        id: accounts[i]?.serial,
        organizationId:
          accounts[i]?.type === 'organization'
            ? accounts[i]?.organization?.orgId
            : '',
        contactPerson,
        accountType: appHelper.translateToUserLng(
          'accounts.' + accounts[i]?.type,
          userLanguage
        ),
        address: accounts[i]?.getFullAddress(),
        status: appHelper.translateToUserLng(
          'common.' + accounts[i].status,
          userLanguage
        ),
        email,
        phone,
        branch: accounts[i]?.branch?.name,
        agent,
        overdue: await getOverDueWithDecimalRound(
          { accountId: accounts[i]?._id },
          partnerId
        ),
        properties:
          accounts[i]?.totalActiveProperties +
          '/' +
          (await countPropertiesForAccount(
            accounts[i]?.partnerId,
            accounts[i]?._id
          ))
      }
      if (size(itemObj)) rowData.push(itemObj)
    }
  }
  return rowData
}

export const getAccountsData = async (params, options) => {
  const { partnerId = {}, userId = {} } = params
  appHelper.validateId({ partnerId })
  appHelper.validateId({ userId })

  const userInfo = await userHelper.getAnUser({ _id: userId })
  const accountsQuery = await prepareAccountsQueryForExcelCreator(params)
  const userLanguage = userInfo?.getLanguage()
  const dataCount = await countAccounts(accountsQuery)
  const query = accountsQuery
  const accounts = await getAccountsForQuery({ query, options })
  const accountsData = await prepareResponseForAccountsQuery(
    accounts,
    userLanguage,
    partnerId
  )
  return {
    data: accountsData,
    total: dataCount
  }
}

export const queryForAccountExcelCreator = async (req) => {
  const { body, user = {} } = req

  const { userId = '' } = user
  appHelper.checkUserId(userId)

  const { query, options } = body
  appHelper.validateSortForQuery(options.sort)
  const { queueId } = query
  appHelper.validateId({ queueId })

  const queueInfo = (await appQueueHelper.getQueueItemById(queueId)) || {}
  if (queueInfo?.params?.downloadProcessType === 'download_accounts') {
    const accountsData = await getAccountsData(queueInfo.params, options)
    return accountsData
  } else {
    throw new CustomError(400, 'Invalid download type')
  }
}

export const queryAccountForPaymentXml = async (req) => {
  const { body, user } = req
  const { userId } = user
  appHelper.checkUserId(userId)
  const { query } = body
  const { accountId } = query
  appHelper.validateId({ accountId })
  const accountDetails = await getAnAccount({ _id: accountId })
  return accountDetails
}

export const getAccountCreationReturnData = async (params = {}, session) => {
  const { contactPersonEmail, createdAccount, email, partnerType } = params
  const pipeline = [
    {
      $match: {
        _id: createdAccount._id
      }
    },
    ...appHelper.getCommonAgentInfoPipeline(),
    {
      $lookup: {
        from: 'users',
        localField: 'personId',
        foreignField: '_id',
        as: 'person'
      }
    },
    appHelper.getUnwindPipeline('person'),
    {
      $lookup: {
        from: 'organizations',
        localField: 'organizationId',
        foreignField: '_id',
        as: 'organization'
      }
    },
    appHelper.getUnwindPipeline('organization'),
    ...appHelper.getCommonBranchInfoPipeline(),
    ...getProjectPipelineForAccountCreation(
      contactPersonEmail,
      email,
      partnerType
    )
  ]
  const [accountInfo] =
    (await AccountCollection.aggregate(pipeline).session(session)) || []
  return accountInfo
}

const getProjectPipelineForAccountCreation = (
  contactPersonEmail,
  email,
  partnerType
) => [
  {
    $project: {
      _id: 1,
      totalOverDue: 1,
      totalActiveProperties: 1,
      totalProperties: 1,
      name: 1,
      type: 1,
      status: 1,
      serial: 1,
      agent: '$agentInfo',
      person: {
        _id: 1,
        name: '$person.profile.name',
        avatarKey: appHelper.getUserAvatarKeyPipeline(
          '$person.profile.avatarKey',
          undefined,
          'person'
        ),
        phoneNumber: '$person.profile.phoneNumber',
        email: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    { $eq: [partnerType, 'broker'] },
                    { $eq: ['$type', 'organization'] }
                  ]
                },
                then: contactPersonEmail
              },
              {
                case: {
                  $and: [
                    { $eq: [partnerType, 'broker'] },
                    { $eq: ['$type', 'person'] }
                  ]
                },
                then: email
              }
            ],
            default: '$$REMOVE'
          }
        }
      },
      organization: {
        _id: 1,
        name: 1,
        imageUrl: appHelper.getOrganizationLogoPipeline('$image')
      },
      createdAt: 1,
      branch: '$branchInfo',
      address: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$person.profile.hometown',
          '$address'
        ]
      },
      zipCode: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$person.profile.zipCode',
          '$zipCode'
        ]
      },
      city: {
        $cond: [{ $eq: ['$type', 'person'] }, '$person.profile.city', '$city']
      },
      country: {
        $cond: [
          { $eq: ['$type', 'person'] },
          '$person.profile.country',
          '$country'
        ]
      },
      agentId: 1,
      personId: 1,
      organizationId: 1,
      partnerId: 1,
      bankAccountNumbers: 1
    }
  }
]

export const validateDataForAddBankAccount = (body) => {
  const { accountId, bankAccountNumber } = body
  appHelper.validateId({ accountId })
  if (!(!isNaN(bankAccountNumber) && bankAccountNumber.length === 11))
    throw new CustomError(400, 'Invalid bankAccountNumber')
}

export const queryForAccountXledger = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  appHelper.checkRequiredFields(['partnerId'], body)
  const { partnerId } = body
  appHelper.validateId({ partnerId })
  const preparedQuery = prepareQueryForAccountXledger(body)
  const accountData = await accountHelper.getAnAccount(preparedQuery, null, [
    'person'
  ])
  let xledgerInfo
  if (size(accountData)) {
    const {
      address,
      city,
      country,
      name,
      person,
      serial,
      xledger = {},
      zipCode
    } = accountData

    if (!size(person)) throw new CustomError(404, 'Account user not found')

    xledgerInfo = {
      _id: accountData._id,
      code: serial,
      country,
      dbId: xledger.id,
      description: name,
      email: person?.getEmail(),
      phone: person?.getPhone(),
      place: city,
      streetAddress: address,
      zipCode
    }
  }

  return xledgerInfo
}

const prepareQueryForAccountXledger = (body = {}) => {
  const { dataType, partnerId } = body
  const query = {
    partnerId,
    serial: {
      $exists: true
    }
  }
  if (dataType === 'get_update_account') {
    query.xledger = {
      $exists: true
    }
    query['xledger.hasUpdateError'] = {
      $exists: false
    }
    query.lastUpdate = {
      $exists: true
    }
    query.$expr = {
      $gte: ['$lastUpdate', '$xledger.syncedAt']
    }
  } else {
    query.xledger = {
      $exists: false
    }
  }
  return query
}

export const getInvoiceAccountNumbers = async (req) => {
  appHelper.validatePartnerAppRequestData(req)
  const { body } = req
  const { partnerId, query } = body
  const { accountId } = query
  let bankAccounts = []

  if (accountId) {
    const accountInfo = await accountHelper.getAnAccount({
      _id: accountId,
      partnerId
    })
    bankAccounts = accountInfo?.bankAccountNumbers || []
  } else {
    const partnerSettingsInfo =
      await partnerSettingHelper.getSettingByPartnerId(partnerId)
    const partnerBankAccountNumbers = partnerSettingsInfo?.bankAccounts || []
    if (size(partnerBankAccountNumbers)) {
      partnerBankAccountNumbers.forEach((bankAccount) => {
        bankAccounts.push(bankAccount.accountNumber)
      })
    }
  }
  return bankAccounts
}

export const getAccountNamesByAggregate = async (query = {}) => {
  if (!size(query)) {
    throw new CustomError(404, 'Query not found to get accounts')
  }
  const accountsInfo =
    (await AccountCollection.aggregate([
      { $match: query },
      {
        $lookup: {
          from: 'users',
          as: 'user',
          localField: 'personId',
          foreignField: '_id'
        }
      },
      { $unwind: '$user' },
      {
        $group: {
          _id: null,
          accountNames: {
            $push: {
              $cond: {
                if: {
                  $and: [
                    { $ne: ['$user.profile.name', ''] },
                    { $ifNull: ['$user.profile.name', false] }
                  ]
                },
                then: '$name',
                else: '$$REMOVE'
              }
            }
          }
        }
      }
    ])) || []
  return accountsInfo
}
