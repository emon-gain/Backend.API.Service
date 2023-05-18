import { isEmpty, size } from 'lodash'
import { CustomError } from '../common'
import { CounterCollection } from '../models'
import {
  counterHelper,
  listingHelper,
  tenantHelper,
  invoiceHelper
} from '../helpers'

export const incrementCounter = async (id, session) => {
  const amount = 1
  const filter = { _id: id }
  let resultData = await CounterCollection.findOneAndUpdate(
    filter,
    { $inc: { next_val: amount } },
    {
      session,
      new: true,
      runValidators: true
    }
  )
  if (!resultData) {
    resultData = await CounterCollection.create(
      [{ _id: id, next_val: amount }],
      { session }
    )
    return resultData[0].next_val
  }
  return resultData.next_val
}

export const createACounter = async (data, session) => {
  const counterData = await CounterCollection.create([data], { session })
  if (isEmpty(counterData)) {
    throw new CustomError(404, 'Unable to create a counter')
  }
  console.log(`--- Counter has been created for id: ${counterData[0]._id} ---`)
  return counterData
}

export const updateACounter = async (query, data, session) => {
  if (!size(data)) {
    throw new CustomError(404, 'No data found for counter update')
  }
  const updatedCounter = await CounterCollection.findOneAndUpdate(query, data, {
    session,
    new: true,
    runValidators: true
  })
  if (!size(updatedCounter)) {
    throw new CustomError(404, `Unable to update Counter`)
  }
  return updatedCounter
}

export const createOrUpdateACounter = async (id, value, session) => {
  const counter = await counterHelper.getACounter({
    _id: id,
    next_val: { $exists: true }
  })
  let newValue = value - 1
  newValue = newValue < 1 ? 1 : newValue
  if (size(counter))
    await updateACounter({ _id: id }, { next_val: newValue }, session)
  else await createACounter({ _id: id, next_val: newValue }, session)
}

export const updatePropertyStartNumber = async (params) => {
  const { partnerId, value, session } = params
  const counterId = `property-${partnerId}`
  const maxPropertySerial = await listingHelper.getMaxPropertySerial(partnerId)
  const maxPropertySerialId = size(maxPropertySerial)
    ? maxPropertySerial[0].maxSerial + 1
    : 1
  if (value < maxPropertySerialId) {
    throw new CustomError(
      405,
      `Property start number can not be less than ${maxPropertySerialId}`
    )
  }
  await createOrUpdateACounter(counterId, value, session)
}

export const updateTenantStartNumber = async (params) => {
  const { partnerId, value, session } = params
  const counterId = `tenant-${partnerId}`
  const maxTenantSerial = await tenantHelper.getMaxTenantSerial(partnerId)
  const maxTenantSerialId = size(maxTenantSerial)
    ? maxTenantSerial[0].maxSerial + 1
    : 1
  if (value < maxTenantSerialId) {
    throw new CustomError(
      405,
      `Tenant start number can not be less than ${maxTenantSerialId}`
    )
  }
  await createOrUpdateACounter(counterId, value, session)
}

export const updateInvoiceStartNumber = async (params) => {
  const { partnerId, value, session } = params
  const maxInvoiceSerial = await invoiceHelper.getMaxInvoiceSerial(partnerId)
  const maxInvoiceSerialId = size(maxInvoiceSerial)
    ? maxInvoiceSerial[0].invoiceSerialId + 1
    : 1
  if (value < maxInvoiceSerialId) {
    throw new CustomError(
      405,
      `Invoice start number can not be less than ${maxInvoiceSerialId}`
    )
  }
  await createOrUpdateACounter(partnerId, value, session)
}

export const updateFinalSettlementInvoiceStartNumber = async (params) => {
  const { partnerId, value, session } = params
  const counterId = `final-settlement-invoice-${partnerId}`
  const maxFinalSettlementInvoiceSerial =
    await invoiceHelper.getMaxFinalSettlementInvoiceSerial(partnerId)
  const maxFinalSettlementInvoiceSerialId = size(
    maxFinalSettlementInvoiceSerial
  )
    ? maxFinalSettlementInvoiceSerial[0].invoiceSerialId + 1
    : 1
  if (value < maxFinalSettlementInvoiceSerialId) {
    throw new CustomError(
      405,
      `Final settlement invoice start number can not be less than ${maxFinalSettlementInvoiceSerialId}`
    )
  }
  await createOrUpdateACounter(counterId, value, session)
}
