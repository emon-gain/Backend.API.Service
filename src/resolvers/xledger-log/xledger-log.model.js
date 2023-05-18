import mongoose from 'mongoose'
import { XledgerLogSchema } from '../models'

export const XledgerLogCollection = mongoose.model(
  'xledger-log',
  XledgerLogSchema,
  'xledger-log'
)
