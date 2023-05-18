import { find, indexOf, size } from 'lodash'
import {
  accountHelper,
  appRoleHelper,
  branchHelper,
  commissionHelper,
  correctionHelper,
  invoiceHelper,
  paymentHelper,
  payoutHelper,
  partnerHelper,
  partnerSettingHelper,
  userHelper
} from '../helpers'

export const isAvailableBranchOfAgent = async (userId, partnerId) => {
  const query = { partnerId, agents: { $in: [userId] } }
  const branchInfo = await branchHelper.getABranch(query)
  return !!branchInfo
}

export const isPartnerUser = async (userId, partnerId) => {
  const isPartnersActiveUser = await userHelper.isPartnersActiveUser(
    userId,
    partnerId
  )
  return isPartnersActiveUser
}

export const isAppAdmin = async (userId) => {
  const appAdminQuery = { type: 'app_admin', users: { $in: [userId] } }
  const appAdmin = await appRoleHelper.getAppRole(appAdminQuery)
  return !!appAdmin
}

export const isPartnerAdmin = async (userId, partnerId) => {
  const appAdmin = await isAppAdmin(userId)

  const partnerAdminQuery = {
    type: 'partner_admin',
    partnerId,
    users: { $in: [userId] }
  }
  const partnerAdmin = await appRoleHelper.getAppRole(partnerAdminQuery)

  const isPartnerAdmin = !!appAdmin || !!partnerAdmin
  console.log(`isPartnerAdmin: ${isPartnerAdmin}`)
  return isPartnerAdmin
}

export const isPartnerAccounting = async (userId, partnerId) => {
  const partnerAccountingQuery = {
    type: 'partner_accounting',
    partnerId,
    users: { $in: [userId] }
  }
  const partnerAccounting = await appRoleHelper.getAppRole(
    partnerAccountingQuery
  )
  console.log(`isPartnerAccounting: ${partnerAccounting}`)
  return !!partnerAccounting
}

export const isPartnerAgent = async (userId, partnerId) => {
  const partnerAgentQuery = {
    type: 'partner_agent',
    partnerId,
    users: { $in: [userId] }
  }
  const partnerAgent = await appRoleHelper.getAppRole(partnerAgentQuery)
  return !!partnerAgent
}

export const isPartnerLandlord = async (userId, partnerId) => {
  const user = await userHelper.getUserById(userId)
  const { partners = [] } = user
  if (size(partners)) {
    return !!find(
      partners,
      (partner) => partner.partnerId === partnerId && partner.type === 'account'
    )
  }
  return false
}

export const isPartnerTenant = async (userId, partnerId) => {
  const user = await userHelper.getUserById(userId)
  const { partners = [] } = user
  if (size(partners)) {
    return !!find(
      partners,
      (partner) => partner.partnerId === partnerId && partner.type === 'tenant'
    )
  }
  return false
}

export const isPartnerAlsoLandlord = async (userId, partnerId) => {
  const isPartnersActiveUser = await userHelper.isPartnersActiveUser(
    userId,
    partnerId
  )
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  return isPartnersActiveUser && _isPartnerLandlord
}

export const isPartnerAlsoTenant = async (userId, partnerId) => {
  const isPartnersActiveUser = await userHelper.isPartnersActiveUser(
    userId,
    partnerId
  )
  const _isPartnerTenant = await isPartnerTenant(userId, partnerId)
  return isPartnersActiveUser && _isPartnerTenant
}

export const canManageDTMSPartnerRoles = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isPartnerAdmin
}

export const canManageDTMSPartnerSettings = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isPartnerAdmin
}

export const canManageDTMSPartnerAccounts = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isPartnerAdmin
}

export const canViewPartnerAccounts = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser // User will be active partner user and user can view partner accounts
}

export const canCreatePartnerAccounts = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  const _isAvailableBranchOfAgent = await isAvailableBranchOfAgent(
    userId,
    partnerId
  )
  return (
    _isPartnerUser &&
    (_isPartnerAdmin || (_isPartnerAgent && _isAvailableBranchOfAgent))
  )
}

export const canEditPartnerAccounts = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewComments = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canCreateComments = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewFiles = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canUploadFiles = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerUser && (_isPartnerAdmin || _isPartnerAgent)
}

export const canViewPartnerTenants = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canCreatePartnerTenants = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerUser && (_isPartnerAdmin || _isPartnerAgent)
}

export const canCreatePartnerProperties = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  const _isAvailableBranchOfAgent = await isAvailableBranchOfAgent(
    userId,
    partnerId
  )
  return (
    _isPartnerUser &&
    (_isPartnerAdmin || (_isPartnerAgent && _isAvailableBranchOfAgent))
  ) // User will be active partner user and user can create partner properties, if user has admin role or agent role
}

export const canEditPartnerProperties = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerUser && (_isPartnerAdmin || _isPartnerAgent) // User will be active partner user and user can edit partner properties, if user has admin role or agent role
}

export const canViewActivities = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canSendMessages = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canAccessPartnerConversation = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canCreatePartnerRules = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isPartnerUser && _isPartnerAdmin
}

export const canViewPartnerRules = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isPartnerUser && _isPartnerAdmin
}

export const canCreateAdminRules = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  return _isAppAdmin
}

export const canViewAdminRules = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  return _isAppAdmin
}

export const canCreateRules = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isAppAdmin || (_isPartnerAdmin && _isPartnerUser)
}

export const canViewRules = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isAppAdmin || (_isPartnerAdmin && _isPartnerUser)
}

export const canCreateNotificationTemplates = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isAppAdmin || _isPartnerAdmin
}

export const canRemoveNotificationTemplates = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isAppAdmin || _isPartnerAdmin
}

export const canRemoveCorrectionFiles = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isAppAdmin || _isPartnerAdmin
}

export const canViewNotificationTemplates = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isAppAdmin || _isPartnerAdmin || _isPartnerAgent
}

export const canCreatePartnerInvoices = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerInvoices = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canDeletePartnerLossRecognition = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAgent
}

export const canCreatePartnerInvoicePayments = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAccounting = await isPartnerAccounting(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAccounting
}

export const canRemovePartnerInvoicePayments = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAccounting = await isPartnerAccounting(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAccounting
}

export const canViewPartnerInvoicePayments = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerInterestForms = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canCreatePartnerCorrections = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerCorrections = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerCommissions = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerPayouts = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewDirectRemittances = async (userId, partnerId) => {
  const _isPartnerAccounting = await isPartnerAccounting(userId, partnerId)
  const partnerSettings = await partnerSettingHelper.getSettingByPartnerId(
    partnerId
  )
  const { directRemittanceApproval = {} } = partnerSettings
  const { persons = [] } = directRemittanceApproval
  const hasUserRemittancePermission = persons.includes(userId)
  return hasUserRemittancePermission && _isPartnerAccounting
}

export const canEditPartnerPayouts = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerDashboard = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canCreateTask = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewTaskDetails = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewLandlordTaskDetails = async (userId, landlordPartnerId) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, landlordPartnerId)
  return _isPartnerLandlord
}

export const canViewTenantTaskDetails = async (userId, tenantPartnerId) => {
  const _isPartnerTenant = await isPartnerTenant(userId, tenantPartnerId)
  return _isPartnerTenant
}

export const canEditTask = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canCreatePartnerAddons = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAgent
}

export const canViewPartnerAddons = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canEditPartnerAddons = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAgent
}

export const canDeletePartnerAddons = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAgent
}

export const canRemoveInvoiceFees = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAgent
}

export const canViewAccounting = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerTransactions = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAccounting = await isPartnerAccounting(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAccounting
}

export const canCreditInvoice = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isAppAdmin || _isPartnerUser
}

export const canViewPartnerAssignments = async (userId, partnerId) => {
  const _isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser && _isBrokerPartner
}

export const canEnablePartnerEsignAssignment = async (userId, partnerId) => {
  const _isBrokerPartner = await partnerHelper.isBrokerPartner(partnerId)
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser && _isBrokerPartner
}

export const canViewPartnerLeases = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canEditPartnerLeases = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPdfStressTest = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  return _isAppAdmin && process.env.DO_PDF_STRESS_TEST === '1'
}

export const canShareAtFinn = async (userId, partnerId) => {
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isAppAdmin || _isPartnerUser
}

export const canEditInvoiceDelayDate = async (userId, partnerId, branchId) => {
  const branchQuery = {
    _id: branchId,
    adminId: userId,
    partnerId
  }
  const branchAdminUser = await branchHelper.getABranch(branchQuery)
  const _isAppAdmin = await isAppAdmin(userId, partnerId)
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  return _isAppAdmin || _isPartnerAdmin || branchAdminUser
}

export const canViewLandlordInvoice = async (userId, partnerId, invoiceId) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const invoiceInfo = await invoiceHelper.getInvoice({
    _id: invoiceId,
    partnerId
  })
  const { accountId = '' } = invoiceInfo
  if (!accountId) {
    return false
  }
  const accountIds = await accountHelper.getAccountIdsByUserId(
    userId,
    partnerId
  )
  if (size(accountIds) && indexOf(accountIds, accountId) !== -1) {
    return true
  }
  return false
}

export const canViewLandlordPayout = async (userId, partnerId, payoutId) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const payoutInfo = await payoutHelper.getPayout({ _id: payoutId, partnerId })
  const { accountId = '' } = payoutInfo
  if (!accountId) {
    return false
  }
  const accountIds = await accountHelper.getAccountIdsByUserId(
    userId,
    partnerId
  )
  if (size(accountIds) && indexOf(accountIds, accountId) !== -1) {
    return true
  }
  return false
}

export const canViewLandlordPayment = async (userId, partnerId, paymentId) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const paymentInfo = await paymentHelper.getPayment({
    _id: paymentId,
    partnerId
  })
  const isAccountIdOfThisPartner = await isAccountIdOfThisPartner(
    userId,
    partnerId,
    paymentInfo
  )
  return isAccountIdOfThisPartner
}

export const canViewLandlordCorrection = async (
  userId,
  partnerId,
  correctionId
) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const correctionInfo = await correctionHelper.getCorrection({
    _id: correctionId,
    partnerId
  })
  const _isAccountIdOfThisPartner = await isAccountIdOfThisPartner(
    userId,
    partnerId,
    correctionInfo
  )
  return _isAccountIdOfThisPartner
}

export const canViewLandlordCorrectionSummary = async (
  userId,
  partnerId,
  invoiceSummaryId
) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const correctionInfo = await correctionHelper.getCorrection({
    invoiceSummaryId,
    partnerId
  })
  const _isAccountIdOfThisPartner = await isAccountIdOfThisPartner(
    userId,
    partnerId,
    correctionInfo
  )
  return _isAccountIdOfThisPartner
}

export const canViewLandlordCommission = async (
  userId,
  partnerId,
  commissionId
) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const commissionInfo = await commissionHelper.getCommission({
    _id: commissionId,
    partnerId
  })
  const _isAccountIdOfThisPartner = await isAccountIdOfThisPartner(
    userId,
    partnerId,
    commissionInfo
  )
  return _isAccountIdOfThisPartner
}

export const canViewLandlordCommissionSummary = async (
  userId,
  partnerId,
  invoiceId
) => {
  const _isPartnerLandlord = await isPartnerLandlord(userId, partnerId)
  if (!_isPartnerLandlord) {
    return false
  }
  const commissionInfo = await commissionHelper.getCommission({
    invoiceId,
    partnerId
  })
  const _isAccountIdOfThisPartner = await isAccountIdOfThisPartner(
    userId,
    partnerId,
    commissionInfo
  )
  return _isAccountIdOfThisPartner
}

export const isAccountIdOfThisPartner = async (
  userId,
  partnerId,
  collectionInfo
) => {
  const { accountId = '' } = collectionInfo
  if (!accountId) {
    return false
  }
  const accountIds = await accountHelper.getAccountIdsByUserId(
    userId,
    partnerId
  )
  if (size(accountIds) && indexOf(accountIds, accountId) !== -1) {
    return true
  }
  return false
}

export const canViewNotifications = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewDepositAccounts = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canViewPartnerAnnualStatement = async (userId, partnerId) => {
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser
}

export const canManagePartnerMoveInOutProtocol = async (userId, partnerId) => {
  const _isPartnerAdmin = await isPartnerAdmin(userId, partnerId)
  const _isPartnerAgent = await isPartnerAgent(userId, partnerId)
  return _isPartnerAdmin || _isPartnerAgent
}

export const canDownloadReportForSkatteetaten = async (userId, partnerId) => {
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  const { enableAnnualStatement } = partnerInfo
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser && enableAnnualStatement
}

export const canViewBrokerJournals = async (userId, partnerId) => {
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  const { enableBrokerJournals } = partnerInfo
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser && enableBrokerJournals
}

export const canDownloadBrokerJournals = async (userId, partnerId) => {
  const partnerInfo = await partnerHelper.getPartnerById(partnerId)
  const { enableBrokerJournals } = partnerInfo
  const _isPartnerUser = await isPartnerUser(userId, partnerId)
  return _isPartnerUser && enableBrokerJournals
}
