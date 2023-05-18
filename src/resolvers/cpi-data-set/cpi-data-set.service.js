import { size } from 'lodash'
import { CustomError } from '../common'
import { CpiDataSetCollection, SettingCollection } from '../models'
import { appHelper, cpiDataSetHelper } from '../helpers'

export const createACpiDataSet = async (data, session) => {
  const response = await CpiDataSetCollection.create([data], { session })
  return response
}

export const updateSettingAndCreateACpiDataSet = async (body, session) => {
  const { CPIData } = body
  const setting = await SettingCollection.findOne({})
  if (!(size(CPIData) && size(setting) && setting._id)) {
    throw new CustomError(404, `Missing CPIData or Setting`)
  }
  const cpiData = {
    cpiDataSet: CPIData
  }
  const updatedSetting = await SettingCollection.findOneAndUpdate(
    { _id: setting._id },
    { $set: cpiData },
    {
      runValidators: true,
      new: true,
      session
    }
  )
  if (!size(updatedSetting)) {
    throw new CustomError(404, `Could not update Setting`)
  }
  console.log(`--- Updated Setting for Id: ${updatedSetting._id} ---`)
  const lastCpiDataSet = await cpiDataSetHelper.getLastCPIDataSet({})
  if (
    !lastCpiDataSet ||
    JSON.stringify(lastCpiDataSet.cpiDataSet) !== JSON.stringify(CPIData)
  ) {
    const insertedCpiDataSet = await createACpiDataSet(cpiData, session)
    if (!size(insertedCpiDataSet)) {
      throw new CustomError(404, `Could not insert Cpi Data Set`)
    }
    console.log(
      `--- Created Cpi Data Set for Id: ${insertedCpiDataSet[0]._id} ---`
    )
    return insertedCpiDataSet
  }
  return [lastCpiDataSet]
}

export const createCpiDataSet = async (req) => {
  const { body, session } = req
  const requiredFields = ['CPIData']
  appHelper.checkRequiredFields(requiredFields, body)
  const result = await updateSettingAndCreateACpiDataSet(body, session)
  return result
}
