import mongoose from 'mongoose'
import { AppQueueSchema } from '../models'

AppQueueSchema.index({ isSequential: 1 })
AppQueueSchema.index({ sequentialCategory: 1 })
AppQueueSchema.index({ isSequential: 1, sequentialCategory: 1 })
AppQueueSchema.index({ 'params.contractId': 1 })
AppQueueSchema.index({ 'params.collectionId': 1 })
AppQueueSchema.index({ 'params.docId': 1 })
AppQueueSchema.index({ 'params.importRefId': 1 })
AppQueueSchema.index({ 'params.invoiceId': 1 })
AppQueueSchema.index({ event: 1, 'params.invoiceId': 1, status: 1 })
AppQueueSchema.index({ 'params.partnerId': 1 })

export const AppQueueCollection = mongoose.model('app_queue', AppQueueSchema)
