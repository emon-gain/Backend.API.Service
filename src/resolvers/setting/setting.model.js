import { find } from 'lodash'
import mongoose from 'mongoose'
import { SettingSchema } from '../models'

SettingSchema.methods = {
  getIsDefaultPartyHabits() {
    if (this && this.partyHabits)
      return find(this.partyHabits, function (partyHabitInfo) {
        return partyHabitInfo && partyHabitInfo.isDefault === true
      })
  },

  getIsDefaultKeepingSpaceHabits() {
    if (this && this.keepingSpace)
      return find(this.keepingSpace, function (keepingSpaceHabitInfo) {
        return keepingSpaceHabitInfo && keepingSpaceHabitInfo.isDefault === true
      })
  }
}
export const SettingCollection = mongoose.model('settings', SettingSchema)
