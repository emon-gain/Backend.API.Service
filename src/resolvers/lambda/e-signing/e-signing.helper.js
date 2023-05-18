import { cloneDeep, find, includes, map, size, split } from 'lodash'
import nid from 'nid'

import { CustomError } from '../../common'
import {
  accountHelper,
  appHelper,
  contractHelper,
  depositAccountHelper,
  depositInsuranceHelper,
  propertyItemHelper,
  tenantHelper,
  userHelper
} from '../../helpers'

export const handleESigning = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query = {} } = body
  await appHelper.checkRequiredFields(['collectionId', 'key', 'type'], query)

  const { collectionId, key, type, version = 'v1' } = query
  const contractInfo = await contractHelper.getContractById(
    collectionId,
    session
  )
  if (!size(contractInfo)) {
    throw new CustomError(404, 'Contract info not found!')
  }

  const {
    _id: contractId,
    agentAssignmentSigningStatus = {},
    landlordAssignmentSigningStatus = {},
    partnerId = '',
    rentalMeta = {},
    status = ''
  } = contractInfo
  const {
    landlordLeaseSigningStatus = {},
    status: leaseStatus,
    tenantLeaseSigningStatus = []
  } = rentalMeta

  let eSigningURL = null
  if (
    status === 'closed' &&
    (type === 'landlord_assignment' || type === 'agent_assignment')
  ) {
    throw new CustomError(400, 'This assignment has already been closed!')
  } else if (type === 'landlord_assignment') {
    const { internalUrl = '', signingUrl = '' } =
      landlordAssignmentSigningStatus

    if (!(internalUrl === key && size(signingUrl))) {
      throw new CustomError(404, 'Landlord assignment e-signing URL not found!')
    }

    eSigningURL = signingUrl
  } else if (type === 'agent_assignment') {
    const { internalUrl = '', signingUrl = '' } = agentAssignmentSigningStatus

    if (!(internalUrl === key && size(signingUrl))) {
      throw new CustomError(404, 'Agent assignment e-signing URL not found!')
    }

    eSigningURL = signingUrl
  } else if (
    leaseStatus === 'closed' &&
    (type === 'tenant_lease' || type === 'landlord_lease')
  ) {
    throw new CustomError(400, 'This lease has already been closed!')
  } else if (type === 'tenant_lease') {
    // Find only tenant whose internal URL matches with key
    const { signingUrl = '', tenantId = '' } =
      find(tenantLeaseSigningStatus, {
        internalUrl: key
      }) || {}

    const isEnabledDepositAccountProcess =
      await depositAccountHelper.isEnabledDepositAccountProcess(
        {
          partnerInfoOrId: partnerId,
          contractInfoOrId: contractInfo
        },
        session
      )

    let kycFormURLOrSigningURL = signingUrl
    if (isEnabledDepositAccountProcess) {
      const paramsForKycData = { partnerId, tenantId, contractId }
      const { referenceNumber = '', isSubmitted = false } =
        (await depositAccountHelper.getTenantDepositKycData(
          paramsForKycData
        )) || {}

      if (size(referenceNumber) && !isSubmitted) {
        const isV1Link = version === 'v2' ? false : true
        let partnerBaseURL = await appHelper.getPartnerURL(partnerId, isV1Link)
        if (!isV1Link) partnerBaseURL = partnerBaseURL.replace('.app', '') // For v2: redirect to partner public
        console.log('++ Checking partnerBaseURL: ', partnerBaseURL)
        kycFormURLOrSigningURL = `${partnerBaseURL}/deposit/kyc_form/${referenceNumber}`
      }
    }

    if (!size(kycFormURLOrSigningURL)) {
      throw new CustomError(404, 'Tenant lease e-signing URL not found!')
    }

    eSigningURL = kycFormURLOrSigningURL
  } else if (type === 'landlord_lease') {
    const { internalUrl = '', signingUrl = '' } = landlordLeaseSigningStatus

    if (!(internalUrl === key && size(signingUrl))) {
      throw new CustomError(404, 'Landlord lease e-signing URL not found!')
    }

    eSigningURL = signingUrl
  }

  if (!size(eSigningURL)) {
    throw new CustomError(
      404,
      'Assignment-lease e-signing URL could not found!'
    )
  }

  return eSigningURL
}

export const handleMovingInOutESigning = async (req) => {
  const { body, session, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query = {} } = body
  await appHelper.checkRequiredFields(['collectionId', 'key', 'type'], query)

  const { collectionId = '', key = '', type = '' } = query
  const movingInOutInfo = await propertyItemHelper.getAPropertyItem(
    {
      _id: collectionId
    },
    session
  )
  if (!size(movingInOutInfo)) {
    throw new CustomError(404, 'Moving in out info not found!')
  }
  const {
    agentSigningStatus = {},
    landlordSigningStatus = {},
    tenantSigningStatus = []
  } = movingInOutInfo

  let eSigningURL = null
  if (type === 'tenant_moving_in' || type === 'tenant_moving_out') {
    // Find only tenant whose internal URL matches with key
    const { signingUrl } = find(tenantSigningStatus, { internalUrl: key }) || {}

    if (!size(signingUrl)) {
      throw new CustomError(
        404,
        'Tenant moving in out e-signing URL not found!'
      )
    }

    eSigningURL = signingUrl
  } else if (type === 'landlord_moving_in' || type === 'landlord_moving_out') {
    const { signingUrl } = landlordSigningStatus

    if (!size(signingUrl)) {
      throw new CustomError(
        404,
        'Landlord moving in out e-signing URL not found!'
      )
    }

    eSigningURL = signingUrl
  } else if (type === 'agent_moving_in' || type === 'agent_moving_out') {
    const { signingUrl } = agentSigningStatus

    if (!size(signingUrl)) {
      throw new CustomError(404, 'Agent moving in out e-signing URL not found!')
    }

    eSigningURL = signingUrl
  }

  if (!size(eSigningURL)) {
    throw new CustomError(404, 'Moving in out e-signing URL could not found!')
  }

  return eSigningURL
}

export const getContractESigningDataForIdfy = async (params = {}) => {
  // Extracting data from params
  const {
    contractId = '',
    contractInfo = {},
    eSigningType = '',
    isAssignmentESigning = false
  } = params || {}

  if (!size(contractInfo))
    throw new CustomError(400, 'Contract info is required!')

  // Extracting contract infos
  const {
    partnerId,
    partner: partnerInfo,
    rentalMeta: contractRentalMeta,
    tenant: tenantInfo
  } = contractInfo || {}

  const { depositType = 'no_deposit' } = contractRentalMeta || {}

  // Extracting partner owner and setting info
  const { owner: partnerOwnerInfo } = partnerInfo || {}

  // Preparing e-signing data
  const contractTitle = isAssignmentESigning
    ? 'assignment_signing'
    : 'lease_signing'
  const contractDescription = isAssignmentESigning
    ? 'assignment_to_be_signed'
    : 'lease_to_be_signed'
  const externalId = isAssignmentESigning
    ? 'assignment-' + contractId
    : 'lease-' + contractId
  const userLang = partnerOwnerInfo?.getLanguage() || 'no'
  const contactEmail =
    process.env.STAGE === 'production'
      ? 'contact-us@uniteliving.com'
      : `contact-us.${process.env.STAGE}@uniteliving.com`

  // Checking if deposit account enabled for lease
  const isEnabledDepositAccount =
    depositType === 'deposit_account'
      ? await depositAccountHelper.isEnabledDepositAccountProcess({
          partnerInfoOrId: partnerId,
          contractInfoOrId: contractInfo
        })
      : false
  // Checking if deposit insurance enabled for lease and Credit Rating is available  for partner and tenant
  const isEnabledDepositInsurance =
    depositType === 'deposit_insurance'
      ? await depositInsuranceHelper.isEnabledDepositInsuranceForContract(
          partnerInfo,
          tenantInfo
        )
      : false
  // Getting attachments count for e-signing if there is deposit account or deposit insurance
  let advanceOptionOfDocument = {}
  if (isEnabledDepositAccount) {
    const {
      enabledJointlyLiable,
      enabledJointDepositAccount,
      tenantId,
      tenants
    } = contractRentalMeta || {}

    let tenantIds = tenantId ? [tenantId] : []
    if (enabledJointlyLiable && !enabledJointDepositAccount)
      tenantIds = size(tenants) ? map(tenants, 'tenantId') : []

    advanceOptionOfDocument = { attachments: size(tenantIds) }
  } else if (isEnabledDepositInsurance) {
    advanceOptionOfDocument = { attachments: 1 }
  }

  const signers = await getContractSignersInfo(contractInfo, eSigningType)
  console.log('=== signers', signers)
  const dataForIdfy = {
    title: appHelper.translateToUserLng('contract.' + contractTitle, userLang),
    description: appHelper.translateToUserLng(
      'contract.' + contractDescription,
      userLang
    ),
    contactDetails: { email: contactEmail },
    dataToSign: { fileName: 'contract.pdf' },
    signers,
    externalId,
    advanced: {
      ...advanceOptionOfDocument,
      getSocialSecurityNumber: true,
      timeToLive: { deleteAfterHours: 744 }
    }
  }

  return dataForIdfy
}

const getContractSignersInfo = async (contractInfo, eSigningType) => {
  if (!(size(contractInfo) && eSigningType))
    throw new CustomError(
      400,
      'Required data missing for getting e-signers data!'
    )

  const {
    accountId,
    agentId,
    assignmentSignatureMechanism,
    partnerId,
    propertyId,
    rentalMeta,
    _id: contractId
  } = contractInfo || {}
  const {
    depositType,
    enabledJointlyLiable,
    leaseSignatureMechanism,
    tenantId,
    tenants
  } = rentalMeta || {}

  const redirectUrl = await appHelper.getPartnerURL(partnerId, true)
  const signersMeta = { ui: { language: 'en' } }

  let signatureMechanism = ''
  if (eSigningType === 'assignment' && assignmentSignatureMechanism)
    signatureMechanism = assignmentSignatureMechanism
  else if (eSigningType === 'lease' && leaseSignatureMechanism)
    signatureMechanism = leaseSignatureMechanism

  let signatureType = ''
  if (signatureMechanism === 'hand_written')
    signatureType = { signatureMethods: [], mechanism: 'handwritten' }
  else
    signatureType = {
      signatureMethods: ['NO_BANKID'],
      mechanism: 'pkisignature'
    }
  signersMeta.signatureType = signatureType
  const signers = []
  const accountSignerInfo = await getAccountSignerInfo({
    accountId,
    redirectUrl,
    signersMeta,
    partnerId,
    contractId
  })
  signers.push(accountSignerInfo)
  console.log('eSigningType', eSigningType)
  if (eSigningType === 'lease') {
    let tenantIds = tenantId ? [tenantId] : []
    if (enabledJointlyLiable || depositType === 'deposit_insurance')
      tenantIds = size(tenants) ? map(tenants, 'tenantId') : []

    const tenantsSignerInfo = await getTenantsSignerInfo({
      redirectUrl,
      signersMeta,
      tenantIds,
      partnerId,
      contractId
    })
    signers.push(...tenantsSignerInfo)
  } else {
    const agentSignerInfo = await getAgentSignerInfo({
      agentId,
      contractId,
      partnerId,
      propertyId,
      redirectUrl,
      signersMeta
    })
    console.log('=== agentSignerInfo', agentSignerInfo)
    signers.push(agentSignerInfo)
  }

  return signers
}

export const getAccountSignerInfo = async (params) => {
  const {
    accountId,
    redirectUrl: v1RedirectUrl,
    signersMeta,
    partnerId,
    contractId
  } = params || {}
  if (!(accountId && size(signersMeta)))
    throw new CustomError(
      400,
      'Required data missing for getting account signer info!'
    )

  // const accountRedirectUrl = redirectUrl + '/login/'
  console.log('Checking partnerId: ', partnerId, ' contractId', contractId)
  const v2SubDomain = await appHelper.getPartnerPublicURL(partnerId)
  console.log('Checking v2SubDomain: ', v2SubDomain)
  const v2_url = `${v2SubDomain}/lease/${contractId}?redirectFrom=idfy`
  console.log('Checking v2_url: ', v2_url)
  const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${v1RedirectUrl}/login/`
  console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
  const accountRedirectUrl =
    appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`

  const signersMetaForAccount = cloneDeep(signersMeta)
  signersMetaForAccount['redirectSettings'] = {
    redirectMode: 'redirect',
    success: accountRedirectUrl,
    cancel: accountRedirectUrl + '&signingStatus=cancel',
    error: accountRedirectUrl + '&signingStatus=error'
  }
  signersMetaForAccount['externalSignerId'] = accountId
  signersMetaForAccount['tags'] = ['account']
  signersMetaForAccount['signerInfo'] = await getSignerInfo(
    accountId,
    'account'
  )

  return signersMetaForAccount
}

export const getAgentSignerInfo = async (params) => {
  const {
    agentId,
    propertyId,
    redirectUrl,
    signersMeta,
    partnerId,
    contractId
  } = params || {}
  if (!(agentId && propertyId && size(signersMeta)))
    throw new CustomError(
      400,
      'Required data missing for getting agent signer info!'
    )

  // const agentRedirectUrl = `${redirectUrl}/dtms/properties/${propertyId}`

  console.log('Checking partnerId: ', partnerId, ' contractId', contractId)
  const v2SubDomain = await appHelper.getPartnerURL(partnerId)
  console.log('Checking v2SubDomain: ', v2SubDomain)
  const v2_url = `${v2SubDomain}/property/properties/${propertyId}?redirectFrom=idfy`
  console.log('Checking v2_url: ', v2_url)
  const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${redirectUrl}/dtms/properties/${propertyId}`
  console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
  const agentRedirectUrl = appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`

  const signersMetaForAgent = cloneDeep(signersMeta)
  signersMetaForAgent['redirectSettings'] = {
    redirectMode: 'redirect',
    success: agentRedirectUrl,
    cancel: agentRedirectUrl + '&signingStatus=cancel',
    error: agentRedirectUrl + '&signingStatus=error'
  }
  signersMetaForAgent['externalSignerId'] = agentId
  signersMetaForAgent['tags'] = ['agent']

  return signersMetaForAgent
}

export const getTenantsSignerInfo = async (params) => {
  const {
    redirectUrl: v1RedirectUrl,
    signersMeta,
    tenantIds,
    partnerId,
    contractId
  } = params || {}
  if (!(size(signersMeta) && size(tenantIds)))
    throw new CustomError(
      400,
      'Required data is missing for getting tenant signer info!'
    )

  // const tenantRedirectUrl = `${redirectUrl}/login/`
  console.log('Checking partnerId: ', partnerId, ' contractId', contractId)
  const v2SubDomain = await appHelper.getPartnerPublicURL(partnerId)
  console.log('Checking v2SubDomain: ', v2SubDomain)
  const v2_url = `${v2SubDomain}/lease/${contractId}?redirectFrom=idfy`
  console.log('Checking v2_url: ', v2_url)
  const linkForV1AndV2 = `redirect?v2_url=${v2_url}&v1_url=${v1RedirectUrl}/login/`
  console.log('Checking linkForV1AndV2: ', linkForV1AndV2)
  const tenantRedirectUrl = appHelper.getLinkServiceURL() + `/${linkForV1AndV2}`

  const cancelRedirectUrl = await prepareV2RedirectUrl(
    partnerId,
    contractId,
    'cancel',
    v1RedirectUrl
  )
  console.log('Checking cancelRedirectUrl: ', cancelRedirectUrl)
  const errorRedirectUrl = await prepareV2RedirectUrl(
    partnerId,
    contractId,
    'error',
    v1RedirectUrl
  )
  console.log('Checking errorRedirectUrl: ', errorRedirectUrl)
  const signersInfo = []
  for (const tenantId of tenantIds) {
    const signersMetaForTenant = cloneDeep(signersMeta)

    signersMetaForTenant['redirectSettings'] = {
      redirectMode: 'redirect',
      success: tenantRedirectUrl,
      cancel: cancelRedirectUrl,
      error: errorRedirectUrl
    }
    console.log('signersMetaForTenant: ', signersMetaForTenant)
    signersMetaForTenant['externalSignerId'] = tenantId
    signersMetaForTenant['signerInfo'] = await getSignerInfo(tenantId, 'tenant')
    signersMetaForTenant['tags'] = ['tenant']

    signersInfo.push(signersMetaForTenant)
  }

  return signersInfo
}

const prepareV2RedirectUrl = async (
  partnerId,
  contractId,
  signingStatus,
  v1RedirectUrl
) => {
  const v2SubDomain = await appHelper.getPartnerPublicURL(partnerId)
  console.log('Checking v2SubDomain on prepareV2RedirectUrl: ', v2SubDomain)
  let v2_url = `${v2SubDomain}/lease/${contractId}?redirectFrom=idfy`
  if (signingStatus === 'cancel' || signingStatus === 'error')
    v2_url = `${v2_url}&signingStatus=${signingStatus}`
  console.log('Checking v2_url prepareV2RedirectUrl: ', v2_url)
  return (
    appHelper.getLinkServiceURL() +
    `/redirect?v2_url=${v2_url}&v1_url=${v1RedirectUrl}/login/`
  )
}

const getSignerInfo = async (collectionId, type) => {
  if (!size(collectionId)) return false
  console.log(
    `== gettingSignerInfo: type: ${type}, collectionId: ${collectionId}`
  )
  let socialSecurityNumber
  if (type === 'account') {
    const { person: userInfo } =
      (await accountHelper.getAnAccount({ _id: collectionId }, null, [
        'person'
      ])) || {}
    socialSecurityNumber =
      userInfo?.getNorwegianNationalIdentification() || null
  } else if (type === 'agent') {
    const userInfo = (await userHelper.getAnUser({ _id: collectionId })) || {}
    socialSecurityNumber = userInfo.getNorwegianNationalIdentification()
  } else if (type === 'tenant') {
    const { user: userInfo } =
      (await tenantHelper.getATenant({ _id: collectionId }, null, ['user'])) ||
      {}
    socialSecurityNumber = userInfo.getNorwegianNationalIdentification()
  }

  return { socialSecurityNumber }
}

export const checkRequiredDataBeforeStartESigningProgress = (body) => {
  appHelper.checkRequiredFields(['queueId', 'idfyResData'], body)
  const { idfyResData = {}, queueId = '' } = body || {}
  if (!(queueId && size(idfyResData)))
    throw new CustomError(400, 'Missing required data')
}

export const prepareDataForAssignmentOrLeaseSigningStatusInitialization =
  async (idfyResponse, fileType) => {
    if (!(size(idfyResponse) && fileType))
      throw new CustomError(
        404,
        'Missing required data to initialize esigning of assignment or lease'
      )

    let updatingData = {}
    let accountSigingStatusFieldKey = ''
    let agentSigningStatusFieldKey = ''
    let tenantSigningStatusFieldKey = ''

    const _id = split(idfyResponse.externalId, '-')[1] // contractId

    if (fileType === 'esigning_lease_pdf') {
      accountSigingStatusFieldKey = 'rentalMeta.landlordLeaseSigningStatus'
      tenantSigningStatusFieldKey = 'rentalMeta.tenantLeaseSigningStatus'
      updatingData = {
        idfyLeaseDocId: idfyResponse.documentId,
        draftLeaseDoc: true
      }
    } else if (fileType === 'esigning_assignment_pdf') {
      accountSigingStatusFieldKey = 'landlordAssignmentSigningStatus'
      agentSigningStatusFieldKey = 'agentAssignmentSigningStatus'
      updatingData = {
        idfyAssignmentDocId: idfyResponse.documentId,
        draftAssignmentDoc: true,
        status: 'in_progress'
      }
    }

    const tenantsSigningStatus = []

    idfyResponse.signers.forEach((signer) => {
      if (includes(signer.tags, 'account')) {
        updatingData[accountSigingStatusFieldKey] = {
          idfySignerId: signer.id,
          signingUrl: signer.url,
          landlordId: signer.externalSignerId,
          internalUrl: nid(17),
          signed: false
        }
      } else if (includes(signer.tags, 'agent')) {
        updatingData[agentSigningStatusFieldKey] = {
          idfySignerId: signer.id,
          signingUrl: signer.url,
          agentId: signer.externalSignerId,
          internalUrl: nid(17),
          signed: false
        }
      } else if (includes(signer.tags, 'tenant')) {
        tenantsSigningStatus.push({
          idfySignerId: signer.id,
          signingUrl: signer.url,
          tenantId: signer.externalSignerId,
          internalUrl: nid(17),
          signed: false
        })
      }

      if (size(tenantsSigningStatus))
        updatingData[tenantSigningStatusFieldKey] = tenantsSigningStatus
    })

    return { queryData: { _id }, updatingData }
  }

export const prepareDataForMovingSigningStatusInitialization = async (
  idfyResponse,
  fileType
) => {
  if (!(size(idfyResponse) && fileType))
    throw new CustomError(
      404,
      'Missing required data to initialize esigning of assignment or lease'
    )

  const updatingData = { idfyMovingInDocId: idfyResponse.documentId }

  const _id = split(idfyResponse.externalId, '-')[1] // propertyItemId

  if (fileType === 'esigning_moving_in_pdf')
    updatingData.draftMovingInDoc = true
  else if (fileType === 'esigning_moving_out_pdf')
    updatingData.draftMovingOutDoc = true
  else throw new CustomError(404, 'Invalid filetype found for moving protocol')

  const tenantsSigningStatus = []

  idfyResponse.signers.forEach((signer) => {
    if (includes(signer.tags, 'account')) {
      updatingData['landlordSigningStatus'] = {
        idfySignerId: signer.id,
        signingUrl: signer.url,
        landlordId: signer.externalSignerId,
        internalUrl: nid(17),
        signed: false
      }
    } else if (includes(signer.tags, 'agent')) {
      updatingData['agentSigningStatus'] = {
        idfySignerId: signer.id,
        signingUrl: signer.url,
        agentId: signer.externalSignerId,
        internalUrl: nid(17),
        signed: false
      }
    } else if (includes(signer.tags, 'tenant')) {
      tenantsSigningStatus.push({
        idfySignerId: signer.id,
        signingUrl: signer.url,
        tenantId: signer.externalSignerId,
        internalUrl: nid(17),
        signed: false
      })
    }

    if (size(tenantsSigningStatus))
      updatingData['tenantSigningStatus'] = tenantsSigningStatus
  })

  return { queryData: { _id }, updatingData }
}

export const verifySignerSSN = async (req) => {
  const { body, user = {} } = req
  appHelper.checkUserId(user.userId)
  const { query = {} } = body
  await appHelper.checkRequiredFields(
    ['collectionId', 'signerSSN', 'signerType'],
    query
  )

  const { collectionId, signerSSN, signerType } = query

  if (!(collectionId && signerSSN && signerType))
    throw new CustomError(400, 'Missing required data')

  let ssn = ''

  if (signerType === 'account') {
    const account = await accountHelper.getAnAccount(
      { _id: collectionId },
      null,
      ['person']
    )
    if (!size(account)) throw new CustomError(404, 'Account does not exist')

    const { person } = account
    if (size(person))
      ssn = person?.profile?.norwegianNationalIdentification || ''
  } else if (signerType === 'tenant') {
    const tenant = await tenantHelper.getATenant({ _id: collectionId }, null, [
      'user'
    ])
    if (!size(tenant)) throw new CustomError(404, 'Tenant does not exist')

    const { user } = tenant
    if (size(user)) ssn = user?.profile?.norwegianNationalIdentification || ''
  }

  if (!ssn) return { result: true } // No ssn then return true

  return { result: signerSSN === ssn }
}
