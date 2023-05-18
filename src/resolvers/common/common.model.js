import mongoose from 'mongoose'
import nid from 'nid'

export const Id = new mongoose.Schema({
  _id: {
    type: String,
    immutable: true
  }
})

Id.pre('save', function (next) {
  // For Seed:
  if (process.env.NODE_ENV === 'test') {
    this._id = this._id ? this._id : nid(17)
  } else {
    this._id = nid(17)
  }
  next()
})

export const CreatedAtSchemas = new mongoose.Schema({
  createdAt: {
    type: Date,
    index: true,
    immutable: true,
    default: new Date()
  }
})

export const CreatedBySchemas = new mongoose.Schema({
  createdBy: {
    type: String,
    index: true,
    immutable: true,
    default: 'SYSTEM'
  }
})

export const CreatedAtCustomSchemas = new mongoose.Schema({
  createdAt: {
    type: Date,
    index: true,
    immutable: true,
    default: new Date()
  }
})

export const TenantsIdSchemas = new mongoose.Schema(
  {
    tenantId: {
      type: String,
      required: true
    }
  },
  { _id: false }
)
