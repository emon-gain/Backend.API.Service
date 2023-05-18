import mongoose from 'mongoose'
import { CounterSchema } from '../models'

CounterSchema.statics.incrementCounter = async function (id, session) {
  const amount = 1
  let resultData = await this.findById(id).session(session)
  if (resultData) {
    resultData.next_val += 1
    await resultData.save()
    return resultData.next_val
  }
  ;[resultData] = await this.create([{ _id: id, next_val: amount }], {
    session
  })
  return resultData.next_val
}

export const CounterCollection = mongoose.model('counters', CounterSchema)
