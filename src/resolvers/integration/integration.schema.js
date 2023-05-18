import mongoose from 'mongoose'
import { CreatedAtSchemas, CreatedBySchemas, Id, Message } from '../common'

const mapAccountsSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      accountingId: {
        type: String
      },
      accountName: {
        type: String
      },
      accountNumber: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      pogoId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      pogoAccountNumber: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      pogoAccountName: {
        type: String
      },
      pogoVatCode: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerAccountsSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      accountingId: {
        type: String
      },
      accountName: {
        type: String
      },
      accountNumber: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      xledgerAccountName: {
        type: String
      },
      xledgerAccountNumber: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      xledgerId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      }
    }
  ],
  { _id: false }
)

const subledgerSeriesSchema = new mongoose.Schema(
  [
    {
      subledgerSeriesId: {
        type: String
      },
      toInclusive: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      fromInclusive: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      subLedgerType: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      }
    }
  ],
  { _id: false }
)

const mapBranchesSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      branchName: {
        type: String
      },
      branchSerialId: {
        type: String
      },
      pogoId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      pogoBranchName: {
        type: String
      },
      pogoBranchSerialId: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerBranchesSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      branchId: {
        type: String
      },
      branchName: {
        type: String
      },
      branchSerialId: {
        type: String
      },
      glObjectDbId: {
        type: String
      },
      glObjectName: {
        type: String
      },
      glObjectCode: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapGroupsSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      propertyGroupId: {
        type: String
      },
      pogoId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      pogoGroupName: {
        type: String
      },
      pogoPropertyGroupId: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerGroupsSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      propertyGroupId: {
        type: String
      },
      glObjectDbId: {
        type: String
      },
      glObjectName: {
        type: String
      },
      glObjectCode: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerInternalAssignmentIdSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      internalAssignmentId: {
        type: String
      },
      glObjectDbId: {
        type: String
      },
      glObjectName: {
        type: String
      },
      glObjectCode: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerInternalLeaseIdSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      internalLeaseId: {
        type: String
      },
      glObjectDbId: {
        type: String
      },
      glObjectName: {
        type: String
      },
      glObjectCode: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerEmployeeIdSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      employeeId: {
        type: String
      },
      glObjectDbId: {
        type: String
      },
      glObjectName: {
        type: String
      },
      glObjectCode: {
        type: String
      }
    }
  ],
  { _id: false }
)

const mapXledgerTaxCodesSchema = new mongoose.Schema(
  [
    CreatedAtSchemas,
    {
      taxCodeId: {
        type: String
      },
      taxCodeName: {
        type: String
      },
      taxCode: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      },
      xledgerTaxCodeName: {
        type: String
      },
      xledgerTaxCode: {
        type: String
      },
      xledgerId: {
        type: Number,
        validate: {
          validator: Number.isInteger,
          message: Message.integerError
        }
      }
    }
  ],
  { _id: false }
)

const glObjectValueTypes = [
  'branch',
  'group',
  'internalAssignmentId',
  'internalLeaseId',
  'agentEmployeeId'
]

const mapXledgerGlObjectsSchema = new mongoose.Schema(
  [
    {
      glObject1: {
        type: String,
        enum: glObjectValueTypes
      },
      glObject2: {
        type: String,
        enum: glObjectValueTypes
      },
      glObject3: {
        type: String,
        enum: glObjectValueTypes
      },
      glObject4: {
        type: String,
        enum: glObjectValueTypes
      },
      glObject5: {
        type: String,
        enum: glObjectValueTypes
      }
    }
  ],
  { _id: false }
)

const mapXledgerTransactionTextSchema = new mongoose.Schema(
  [
    {
      type: {
        type: 'String',
        enum: ['field', 'text']
      },
      value: {
        type: String
      }
    }
  ],
  { _id: false }
)

const MapXledgerObjectKindsSchema = new mongoose.Schema(
  [
    {
      field: {
        type: 'String',
        enum: glObjectValueTypes
      },
      objectKindDbId: {
        type: String
      },
      objectKindName: {
        type: String
      }
    }
  ],
  { _id: false }
)

export const IntegrationSchema = new mongoose.Schema(
  [
    CreatedBySchemas,
    Id,
    {
      type: {
        type: String
      },
      status: {
        type: String,
        enum: ['pending', 'disabled', 'integrated']
      },
      partnerId: {
        type: String
      },
      accountId: {
        type: String
      },
      applicationKey: {
        type: String
      },
      clientKey: {
        type: String
      },
      enabledPowerOfficeIntegration: {
        type: Boolean
      },
      enabledIntegration: {
        type: Boolean
      },
      enabledPeriodSync: {
        type: Boolean
      },
      tenantAccountType: {
        type: String
      },
      fromDate: {
        type: Date
      },
      mapAccounts: {
        type: [mapAccountsSchema],
        default: undefined
      },
      mapXledgerAccounts: {
        type: [mapXledgerAccountsSchema],
        default: undefined
      },
      mapXledgerTaxCodes: {
        type: [mapXledgerTaxCodesSchema],
        default: undefined
      },
      mapXledgerGlObjects: {
        type: mapXledgerGlObjectsSchema,
        default: undefined
      },
      isGlobal: {
        type: Boolean
      },
      tenantSubledgerSeries: {
        type: subledgerSeriesSchema
      },
      accountSubledgerSeries: {
        type: subledgerSeriesSchema
      },
      projectDepartmentType: {
        type: String,
        enum: [
          'branch_department_and_group_project',
          'branch_project_and_group_department'
        ],
        default: 'branch_department_and_group_project'
      },
      mapBranches: {
        type: [mapBranchesSchema]
      },
      mapXledgerBranches: {
        type: [mapXledgerBranchesSchema],
        default: undefined
      },
      mapGroups: {
        type: [mapGroupsSchema]
      },
      mapXledgerGroups: {
        type: [mapXledgerGroupsSchema],
        default: undefined
      },
      mapXledgerInternalAssignmentIds: {
        type: [mapXledgerInternalAssignmentIdSchema],
        default: undefined
      },
      mapXledgerInternalLeaseIds: {
        type: [mapXledgerInternalLeaseIdSchema],
        default: undefined
      },
      mapXledgerEmployeeIds: {
        type: [mapXledgerEmployeeIdSchema],
        default: undefined
      },
      mapXledgerTransactionText: {
        type: [mapXledgerTransactionTextSchema],
        default: undefined
      },
      mapXledgerObjectKinds: {
        type: [MapXledgerObjectKindsSchema],
        default: undefined
      },
      companyDbId: {
        type: String,
        default: undefined
      },
      ownerDbId: {
        type: String,
        default: undefined
      },
      isStatusChecking: {
        type: Boolean
      },
      errorsMeta: {
        type: Object
      }
    }
  ],
  {
    timestamps: true,
    versionKey: false,
    toJSON: { virtuals: true }
  }
)

IntegrationSchema.index({ createdAt: 1 })
IntegrationSchema.index({ 'mapAccounts.createdAt': 1 })
IntegrationSchema.index({ 'mapBranches.createdAt': 1 })
IntegrationSchema.index({ 'mapGroups.createdAt': 1 })
