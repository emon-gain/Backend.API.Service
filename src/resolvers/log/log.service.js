import { size } from 'lodash'
import { CustomError } from '../common'
import { LogCollection } from '../models'

export const createLog = async (logData, session) => {
  if (!size(logData)) {
    throw new CustomError(405, `Can not create log without data`)
  }
  const [log] = await LogCollection.create([logData], { session })
  return log
}

export const createLogs = async (logsData, session) => {
  if (!size(logsData)) {
    throw new CustomError(405, `Can not create logs without data`)
  }

  const logs = await LogCollection.insertMany(logsData, {
    runValidators: true,
    session
  })
  if (!size(logs)) {
    throw new CustomError(400, 'Unable to create logs')
  }
  return logs
}
