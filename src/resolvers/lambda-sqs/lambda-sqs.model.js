import mongoose from 'mongoose'
import { LambdaSqsSchema } from '../models'

LambdaSqsSchema.virtual('partner', {
  ref: 'partners',
  localField: 'partnerId',
  foreignField: '_id',
  justOne: true
})

export const LambdaSqsCollection = mongoose.model('lambda_sqs', LambdaSqsSchema)
