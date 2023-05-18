import { size } from 'lodash'

import { NetReceivedFileCollection } from '../models'

import { appHelper, netReceivedFileHelper } from '../helpers'
import { appQueueService } from '../services'

export const insertNetReceivedFiles = async (data, session) => {
  const files = await NetReceivedFileCollection.create(data, session)
  return files
}

export const updateANetReceivedFile = async (query, data, session) => {
  const netReceivedFile = await NetReceivedFileCollection.findOneAndUpdate(
    query,
    data,
    {
      session,
      new: true,
      runValidators: true
    }
  )
  return netReceivedFile
}

export const createNetReceivedFiles = async (req) => {
  const { body, session } = req
  const createdNetReceivedFiles = await insertNetReceivedFiles(body, session)
  const queueData = []
  for (const netFile of createdNetReceivedFiles) {
    queueData.push({
      action: 'process_nets_received_file',
      destination: 'payments',
      event: 'process_nets_received_file',
      isSequential: true,
      params: {
        netsReceiveFileId: netFile._id,
        receivedFileName: netFile.receivedFileName,
        receivedFileKey: netFile.receivedFileKey
      },
      priority: 'regular',
      sequentialCategory: 'process_nets_received_file'
    })
  }
  await appQueueService.addSequentialAppQueues(queueData, session)
  return createdNetReceivedFiles
}

export const updateNetReceivedFile = async (req) => {
  const { body, session, user } = req
  const { userId } = user
  appHelper.checkUserId(userId)

  const query = netReceivedFileHelper.prepareQueryToUpdateNetReceivedFile(body)
  const updateData =
    await netReceivedFileHelper.prepareDataToUpdateNetReceivedFile(body)
  const updatedNetReceivedFile = size(updateData)
    ? await updateANetReceivedFile(query, updateData, session)
    : {}
  return updatedNetReceivedFile
}
