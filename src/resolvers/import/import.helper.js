import { size } from 'lodash'
import { ImportCollection } from '../models'
import { appHelper } from '../helpers'

export const getAnImport = async (query, session) => {
  const importInfo = await ImportCollection.findOne(query)
    .populate(['partner'])
    .session(session)
  return importInfo
}

export const getMultipleImports = async (query, session) => {
  const importsList = await ImportCollection.find(query).session(session)
  return importsList
}

export const getImportsListForQuery = async (query = {}, options) => {
  const matchQuery = { $match: { ...query, importRefId: { $exists: false } } }
  const sortPipeLineForImport = getSortPipeLineForImport(options?.sort)
  const skipPipeLineForImport = getSkipPipeLineForImport(options?.skip)
  const limitPipeLineForImport = getLimitPipeLineForImport(options?.limit)
  const importsList = await ImportCollection.aggregate([
    matchQuery,
    {
      $addFields: {
        hasError: { $ifNull: ['$hasError', false] }
      }
    },
    lookupForChildrenImports,
    unwindForChildrenImports,
    groupPipeLineForImport,
    lookupPipeLineForImport,
    addFieldsPipeLineForImport,
    lookupPipeLineForCreatedBy,
    unwindForForCreatedBy,
    projectPipeLineForImport,
    lookupForPartner,
    unwindForPartner,
    sortPipeLineForImport,
    skipPipeLineForImport,
    limitPipeLineForImport
  ])
  return importsList
}

export const getErrorImportsData = async (query) => {
  const importsList = await ImportCollection.aggregate([
    {
      $match: query
    },
    {
      $replaceRoot: {
        newRoot: {
          $mergeObjects: [
            '$jsonData',
            {
              importId: '$_id',
              collectionId: '$collectionId',
              collectionName: '$collectionName',
              errorMessage: '$errorMessage',
              hasError: '$hasError',
              importRefId: '$importRefId',
              partnerId: '$partnerId'
            }
          ]
        }
      }
    }
  ])
  return importsList
}

export const getImports = async (req) => {
  const { body, user = {} } = req
  const { query, options } = body
  appHelper.checkUserId(user.userId)
  appHelper.validateSortForQuery(options.sort)
  body.query = prepareImportsQueryBasedOnFilters(query)
  const importsList = await getImportsListForQuery(query, options)
  const filteredDocuments = await countImports(body.query)
  const totalDocuments = await countImports({})
  return {
    data: importsList,
    metaData: { filteredDocuments, totalDocuments }
  }
}

export const getErrorImports = async (req) => {
  const { body, session } = req
  const { query } = body
  const importsList = await getErrorImportsData(query, session)
  return { data: importsList }
}

const prepareImportsQueryBasedOnFilters = (params) => {
  const query = {}

  if (params.partnerId) {
    query.partnerId = params.partnerId
  }

  return query
}

const countImports = async (query) => {
  const importsList = await getImportsListForQuery(query)
  return size(importsList)
}

const lookupForChildrenImports = {
  $lookup: {
    from: 'imports',
    localField: '_id',
    foreignField: 'importRefId',
    as: 'import'
  }
}

const unwindForChildrenImports = {
  $unwind: {
    path: '$import',
    preserveNullAndEmptyArrays: true
  }
}

const groupPipeLineForImport = {
  $group: {
    _id: '$_id',
    createdAt: { $first: '$createdAt' },
    createdBy: { $first: '$createdBy' },
    fileKey: { $first: '$fileKey' },
    fileBucket: { $first: '$fileBucket' },
    partnerId: { $first: '$partnerId' },
    errorMessage: { $first: '$errorMessage' },
    hasError: { $first: '$hasError' },
    totalBranchCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'branch'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalBranchSuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'branch'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalUserCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'user'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalUserSuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'user'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalAccountCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'account'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalAccountSuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'account'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalRoomCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'room'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalRoomSuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'room'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalInventoryCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'inventory'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalInventorySuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'inventory'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalAddonCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'addon'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalAddonSuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'addon'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalPropertyCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'property'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalPropertySuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'property'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalTenantCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'tenant'] },
              { $ne: ['$import.isImportingFromError', true] }
            ]
          },
          1,
          0
        ]
      }
    },
    totalTenantSuccessCount: {
      $sum: {
        $cond: [
          {
            $and: [
              { $eq: ['$import.collectionName', 'tenant'] },
              { $ne: ['$import.isImportingFromError', true] },
              { $eq: ['$import.hasError', false] }
            ]
          },
          1,
          0
        ]
      }
    }
  }
}

const lookupPipeLineForImport = {
  $lookup: {
    from: 'app_queues',
    localField: '_id',
    foreignField: 'params.importRefId',
    as: 'app_queues'
  }
}

const addFieldsPipeLineForImport = {
  $addFields: {
    completedAppQueues: {
      $filter: {
        input: '$app_queues',
        as: 'app_queue',
        cond: { $eq: ['$$app_queue.status', 'completed'] }
      }
    },
    failedAppQueues: {
      $filter: {
        input: '$app_queues',
        as: 'app_queue',
        cond: { $eq: ['$$app_queue.status', 'failed'] }
      }
    },
    processingAppQueues: {
      $filter: {
        input: '$app_queues',
        as: 'app_queue',
        cond: {
          $in: [
            '$$app_queue.status',
            ['new', 'on_flight', 'sent', 'processing']
          ]
        }
      }
    }
  }
}

const lookupPipeLineForCreatedBy = {
  $lookup: {
    from: 'users',
    localField: 'createdBy',
    foreignField: '_id',
    as: 'user'
  }
}

const unwindForForCreatedBy = {
  $unwind: {
    path: '$user',
    preserveNullAndEmptyArrays: true
  }
}

const projectPipeLineForImport = {
  $project: {
    createdAt: 1,
    createdBy: {
      $cond: [{ $eq: ['$createdBy', 'SYSTEM'] }, 'SYSTEM', '$user.profile.name']
    },
    fileKey: 1,
    fileBucket: 1,
    partnerId: 1,
    errorMessage: 1,
    status: {
      $switch: {
        branches: [
          {
            case: { $eq: ['$hasError', true] },
            then: 'failed'
          },
          {
            case: { $gt: [{ $size: '$processingAppQueues' }, 0] },
            then: 'processing'
          },
          {
            case: {
              $or: [
                { $gt: [{ $size: '$failedAppQueues' }, 0] },
                { $ne: ['$totalBranchCount', '$totalBranchSuccessCount'] },
                { $ne: ['$totalUserCount', '$totalUserSuccessCount'] },
                { $ne: ['$totalAccountCount', '$totalAccountSuccessCount'] },
                { $ne: ['$totalAccountCount', '$totalAccountSuccessCount'] },
                { $ne: ['$totalRoomCount', '$totalRoomSuccessCount'] },
                {
                  $ne: ['$totalInventoryCount', '$totalInventorySuccessCount']
                },
                {
                  $ne: ['$totalInventoryCount', '$totalInventorySuccessCount']
                },
                { $ne: ['$totalAddonCount', '$totalAddonSuccessCount'] },
                { $ne: ['$totalPropertyCount', '$totalPropertySuccessCount'] },
                { $ne: ['$totalTenantCount', '$totalTenantSuccessCount'] }
              ]
            },
            then: 'failed'
          },
          {
            case: { $gt: [{ $size: '$completedAppQueues' }, 0] },
            then: 'completed'
          }
        ],
        default: 'new'
      }
    },
    totalBranchCount: 1,
    totalBranchSuccessCount: 1,
    totalUserCount: 1,
    totalUserSuccessCount: 1,
    totalAccountCount: 1,
    totalAccountSuccessCount: 1,
    totalRoomCount: 1,
    totalRoomSuccessCount: 1,
    totalInventoryCount: 1,
    totalInventorySuccessCount: 1,
    totalAddonCount: 1,
    totalAddonSuccessCount: 1,
    totalPropertyCount: 1,
    totalPropertySuccessCount: 1,
    totalTenantCount: 1,
    totalTenantSuccessCount: 1
  }
}

const lookupForPartner = {
  $lookup: {
    from: 'partners',
    localField: 'partnerId',
    foreignField: '_id',
    as: 'partner'
  }
}

const unwindForPartner = {
  $unwind: {
    path: '$partner',
    preserveNullAndEmptyArrays: true
  }
}

const getSortPipeLineForImport = (sort) => {
  const sortPipeLine = {
    $sort: sort || { createdAt: 1 }
  }
  return sortPipeLine
}

const getSkipPipeLineForImport = (skip) => {
  const skipPipeLine = {
    $skip: skip || 0
  }
  return skipPipeLine
}

const getLimitPipeLineForImport = (limit) => {
  const limitPipeLine = {
    $limit: limit || 50
  }
  return limitPipeLine
}

export const prepareImportUpdateData = (data) => {
  const { hasError, errorMessage } = data
  const updateData = {}
  if (errorMessage) updateData.errorMessage = errorMessage
  if (data.hasOwnProperty('hasError')) updateData.hasError = hasError
  return updateData
}
