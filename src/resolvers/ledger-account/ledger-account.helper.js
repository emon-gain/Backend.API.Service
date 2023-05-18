import { isBoolean, isInteger, isString, omit, size } from 'lodash'
import {
  accountingHelper,
  addonHelper,
  appHelper,
  partnerSettingHelper,
  transactionHelper
} from '../helpers'
import { LedgerAccountCollection } from '../models'
import { CustomError } from '../common'

export const getLedgerAccById = async (id, session) => {
  const account = await LedgerAccountCollection.findById(id).session(session)
  return account
}

export const getLedgerAccount = async (query, session) => {
  const account = await LedgerAccountCollection.findOne(query).session(session)
  return account
}

export const getLedgerAccounts = async (query, session, populate = []) => {
  const accounts = await LedgerAccountCollection.find(query)
    .populate(populate)
    .session(session)
  return accounts
}

export const prepareLedgerAccountsQueryBasedOnFilters = (query) => {
  const { appAdmin, dataType } = query
  if (appAdmin) query.partnerId = { $exists: false }
  if (size(dataType) && dataType === 'get_ledger_accounts_for_pogo')
    appHelper.checkRequiredFields(['partnerId'], query)
  const ledgerAccountsQuery = omit(query, ['appAdmin'])
  return ledgerAccountsQuery
}

export const getLedgerAccountsForQuery = async (params) => {
  const { query, options, populate = [] } = params
  const { limit, skip, sort } = options
  const ledgerAccounts = await LedgerAccountCollection.find(query)
    .populate(populate)
    .limit(limit)
    .skip(skip)
    .sort(sort)
  return ledgerAccounts
}

export const getLedgerAccountsByAggregation = async (params) => {
  const { query, options, populate = [] } = params
  const { limit, skip, sort } = options
  const pipelines = [{ $match: query }]
  for (let i = 0; i < size(populate); i++) {
    if (populate[i]) {
      pipelines.push(
        ...[
          {
            $lookup: populate[i]
          },
          { $addFields: { [populate[i].as]: { $first: `$${populate[i].as}` } } }
        ]
      )
    }
  }
  pipelines.push(
    ...[
      {
        $lookup: {
          from: 'transactions',
          localField: 'partnerId',
          foreignField: 'partnerId',
          let: { ledgerAccountId: '$_id' },
          pipeline: [
            {
              $match: {
                $expr: {
                  $or: [
                    { $eq: ['$debitAccountId', '$$ledgerAccountId'] },
                    { $eq: ['$creditAccountId', '$$ledgerAccountId'] }
                  ]
                }
              }
            },
            { $limit: 1 }
          ],
          as: 'transactions'
        }
      },
      {
        $addFields: {
          hasTransactions: {
            $cond: [
              { $ifNull: [{ $first: '$transactions._id' }, false] },
              true,
              false
            ]
          },
          transactions: '$$REMOVE'
        }
      }
    ]
  )
  if (sort) {
    pipelines.push({ $sort: sort })
  }
  if (limit) {
    pipelines.push({ $limit: limit })
  }
  if (skip) {
    pipelines.push({ $skip: skip })
  }
  return LedgerAccountCollection.aggregate(pipelines)
}

export const countLedgerAccounts = async (query, session) => {
  const numberOfLedgerAccounts = await LedgerAccountCollection.find(query)
    .session(session)
    .countDocuments()
  return numberOfLedgerAccounts
}

export const getLedgerAccountsInfo = async (body) => {
  const { query = {} } = body
  let ledgerAccountsData = []
  if (
    size(query.dataType) &&
    query.dataType === 'get_ledger_accounts_for_pogo'
  ) {
    //When requesting from lambda accountingBridgePogo #10175
    body.populate = 'taxCodeInfo'
    delete query.dataType
    const ledgerAccounts = await getLedgerAccountsForQuery(body)
    ledgerAccounts.forEach((ledgerAccount) => {
      const ledgerAccountData = createAccountFieldNameForApi(ledgerAccount)
      ledgerAccountsData.push(ledgerAccountData)
    })
  } else {
    body.populate = [
      {
        from: 'partners',
        localField: 'partnerId',
        foreignField: '_id',
        as: 'partner'
      },
      {
        from: 'tax_codes',
        localField: 'taxCodeId',
        foreignField: '_id',
        as: 'taxCode'
      }
    ]
    ledgerAccountsData = await getLedgerAccountsByAggregation(body)
  }
  return ledgerAccountsData
}

export const queryLedgerAccounts = async (req) => {
  const { body, user = {} } = req
  const { query, options } = body
  appHelper.checkUserId(user.userId)
  const { partnerId, roles = [] } = user
  if (partnerId && !roles.includes('lambda_manager')) {
    // This is applied only for partner app
    appHelper.validateId({ partnerId })
    body.query.partnerId = partnerId
    delete body.query.dataType
    delete body.query.appAdmin
  } else {
    body.query = prepareLedgerAccountsQueryBasedOnFilters(query)
  }
  appHelper.validateSortForQuery(options.sort)
  const ledgerAccountsData = await getLedgerAccountsInfo(body)
  const filteredDocuments = await countLedgerAccounts(body.query)
  const totalDocumentsQuery = body?.query?.appAdmin
    ? { partnerId: { $exists: false } }
    : roles.includes('lambda_manager')
    ? {}
    : partnerId
    ? { partnerId }
    : {}
  const totalDocuments = await countLedgerAccounts(totalDocumentsQuery)
  return {
    data: ledgerAccountsData,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const validateLedgerAccountCreationData = (data = {}) => {
  const requiredFields = ['accountName', 'accountNumber', 'taxCodeId']
  appHelper.checkRequiredFields(requiredFields, data)
  const { accountName, accountNumber, taxCodeId } = data
  if (!isString(accountName) || !accountName) {
    throw new CustomError(400, 'Account name must be a valid string')
  }
  if (
    !isInteger(accountNumber) ||
    accountNumber < 1000 ||
    accountNumber > 999999
  ) {
    throw new CustomError(
      400,
      'Account number must be an integer of four to six digits'
    )
  }
  appHelper.validateId({ taxCodeId })
}

export const prepareDataForUpdatingLedgerAccount = async (body, session) => {
  const { data, _id, partnerId } = body
  const { name, valueBoolean, valueInt, valueString } = data
  const updateData = {}
  if (name === 'accountNumber') {
    if (!isInteger(valueInt) || valueInt < 1000 || valueInt > 999999) {
      throw new CustomError(
        400,
        'Value must be an integer of four to six digits'
      )
    }
    updateData.accountNumber = valueInt
    const query = {
      partnerId: partnerId ? partnerId : { $exists: false },
      accountNumber: valueInt
    }
    const transactionQuery = {
      partnerId: partnerId ? partnerId : { $exists: false },
      $or: [{ debitAccountId: _id }, { creditAccountId: _id }]
    }
    const transaction = await transactionHelper.getTransaction(
      transactionQuery,
      session
    )
    const ledgerAccount = await getLedgerAccount(query, session)
    if (size(ledgerAccount) || size(transaction)) {
      throw new CustomError(
        405,
        'Could not edit account number, it is being used'
      )
    }
  } else if (name === 'accountName') {
    if (!isString(valueString) || !valueString) {
      throw new CustomError(400, 'Value must be a valid string')
    }
    updateData.accountName = valueString
  } else if (name === 'taxCodeId') {
    appHelper.validateId({ taxCodeId: valueString })
    updateData.taxCodeId = valueString
    const transactionQuery = {
      partnerId: partnerId ? partnerId : { $exists: false },
      $or: [{ debitAccountId: _id }, { creditAccountId: _id }]
    }
    const transaction = await transactionHelper.getTransaction(
      transactionQuery,
      session
    )
    if (size(transaction)) {
      throw new CustomError(405, `Could not edit taxCodeId, it is being used`)
    }
  } else if (name === 'enable') {
    if (!isBoolean(valueBoolean)) {
      throw new CustomError(400, 'Value must be a boolean')
    }
    updateData.enable = valueBoolean
  } else {
    throw new CustomError(400, `Invalid name`)
  }
  return updateData
}

export const validateRemovingLedgerAccount = async (
  ledgerAccountId,
  partnerId,
  session
) => {
  const query = {
    $or: [
      { debitAccountId: ledgerAccountId },
      { creditAccountId: ledgerAccountId }
    ]
  }
  query.partnerId = partnerId ? partnerId : { $exists: false }
  const accounting = await accountingHelper.getAccounting(query, session)
  const addon = await addonHelper.getAddon(query, session)
  const transaction = await transactionHelper.getTransaction(query, session)
  const partnerSetting = partnerSettingHelper.getAPartnerSetting({
    bankAccounts: { $elemMatch: { ledgerAccountId } },
    session
  })
  if (
    size(accounting) ||
    size(addon) ||
    size(partnerSetting) ||
    size(transaction)
  ) {
    throw new CustomError(
      405,
      'Could not remove ledger account, it is being used'
    )
  }
}

//For lambda accountingBridgePogo #10175
const createAccountFieldNameForApi = async (ledgerAccount) => {
  const accountData = {
    _id: ledgerAccount._id,
    accountNumber: ledgerAccount.accountNumber,
    accountName: ledgerAccount.accountName,
    taxCodeId: ledgerAccount.taxCodeId,
    enable: ledgerAccount.enable,
    mapAccounts: ledgerAccount.mapAccounts
  }
  if (size(ledgerAccount.taxCodeInfo) && ledgerAccount.taxCodeInfo.taxCode) {
    accountData.taxCodePogo = ledgerAccount.taxCodeInfo.taxCode
  }

  return accountData
}
