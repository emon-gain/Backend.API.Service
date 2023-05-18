import { size } from 'lodash'
import { CpiDataSetCollection } from '../models'

export const getLastCPIDataSet = async (query = {}) => {
  const lastCPIDataSet = await CpiDataSetCollection.find(query).sort({
    createdAt: -1
  })

  return size(lastCPIDataSet) ? lastCPIDataSet[0] : false
}
