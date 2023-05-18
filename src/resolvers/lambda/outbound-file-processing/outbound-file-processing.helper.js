import moment from 'moment-timezone'
import { assign, clone, indexOf, find, pick, size, last } from 'lodash'
import {
  appHelper,
  invoiceHelper,
  partnerHelper,
  partnerSettingHelper,
  payoutProcessHelper
} from '../../helpers'

export const getFeedbackHistory = (
  creditTransInfo,
  xmlReceivedFileName,
  feedbackCreatedAt
) => {
  const newFeedbackHistory = {
    createdAt: feedbackCreatedAt,
    status: creditTransInfo.status,
    receivedFileName: xmlReceivedFileName,
    reason: creditTransInfo.reason
  }

  if (size(creditTransInfo.payoutId))
    newFeedbackHistory.payoutId = creditTransInfo.payoutId

  if (size(creditTransInfo.paymentId))
    newFeedbackHistory.paymentId = creditTransInfo.paymentId

  return newFeedbackHistory
}

export const getReasonCode = (findTransactionInfo) => {
  const statusReasonInfo =
    size(findTransactionInfo) && size(findTransactionInfo.StsRsnInf)
      ? findTransactionInfo.StsRsnInf[0]
      : []
  const reasonInfo =
    size(statusReasonInfo) && size(statusReasonInfo.Rsn)
      ? statusReasonInfo.Rsn[0]
      : []
  const reasonCode =
    size(reasonInfo) && size(reasonInfo.Cd) ? reasonInfo.Cd[0] : ''
  return reasonCode
}

export const prepareDataToAddPayment = async (paymentData) => {
  if (!size(paymentData)) return false

  //Set data for invoiceId
  const invoiceInfo = await invoiceHelper.getInvoiceById(paymentData.invoiceId)

  if (size(invoiceInfo) && size(paymentData)) {
    const invoiceData = pick(invoiceInfo, [
      'accountId',
      'agentId',
      'branchId',
      'contractId',
      'propertyId',
      'tenantId',
      'tenants'
    ])
    paymentData = assign(paymentData, invoiceData)
  }

  paymentData.type = 'payment'

  if (size(paymentData.paymentToAccountNumber)) {
    paymentData.meta = {}
    const partnerId = paymentData.partnerId
    const partnerInfo = await partnerHelper.getPartnerById(partnerId)
    let bankAccount = {}
    const paymentMeta = {
      cdTrAccountNumber: paymentData.paymentToAccountNumber
    }
    const partnerSettingsInfo = await partnerSettingHelper.getAPartnerSetting({
      partnerId
    })
    const isDirectPartner = !!(
      size(partnerInfo) && partnerInfo.accountType === 'direct'
    )

    if (isDirectPartner) {
      if (
        size(partnerSettingsInfo) &&
        size(partnerSettingsInfo.companyInfo) &&
        size(partnerSettingsInfo.companyInfo.companyName)
      )
        paymentMeta.cdTrName = partnerSettingsInfo.companyInfo.companyName
      if (
        size(partnerSettingsInfo) &&
        size(partnerSettingsInfo.companyInfo) &&
        size(partnerSettingsInfo.companyInfo.officeAddress)
      )
        paymentMeta.cdTrAddress = partnerSettingsInfo.companyInfo.officeAddress
    } else {
      if (size(partnerSettingsInfo) && size(partnerSettingsInfo.bankAccounts))
        bankAccount = find(partnerSettingsInfo.bankAccounts, {
          accountNumber: paymentData.paymentToAccountNumber
        })

      if (size(bankAccount) && size(bankAccount.orgName))
        paymentMeta.cdTrName = bankAccount.orgName
      if (size(bankAccount) && size(bankAccount.orgAddress))
        paymentMeta.cdTrAddress = bankAccount.orgAddress
    }

    paymentData.meta = paymentMeta
  }

  return paymentData
}

export const getBankToCustomerDbtTrnListData = async (params) => {
  let { bankToCustomerDbtTrnList } = params
  const {
    payoutProcessIds,
    endToEndId,
    bookingDate,
    createdAt,
    referenceCode,
    bankRef
  } = params
  const payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
    creditTransferInfo: { $elemMatch: { paymentEndToEndId: endToEndId } }
  })

  if (!size(bankToCustomerDbtTrnList)) bankToCustomerDbtTrnList = []

  if (payoutProcessInfo) {
    const bankToCustomerDbtTrnData = []

    if (indexOf(payoutProcessIds, payoutProcessInfo._id) !== -1) {
      for (const bankToCustomerDbtTrn of bankToCustomerDbtTrnList) {
        if (bankToCustomerDbtTrn.payoutProcessId === payoutProcessInfo._id) {
          const dbtTrnData =
            clone(bankToCustomerDbtTrn.bankToCustomerDbtTrnData) || []
          dbtTrnData.push({
            endToEndId,
            bookingDate,
            createdAt,
            referenceCode,
            bankRef
          })
          bankToCustomerDbtTrn.bankToCustomerDbtTrnData = clone(dbtTrnData)
        }
      }
    } else {
      payoutProcessIds.push(payoutProcessInfo._id)
      bankToCustomerDbtTrnData.push({
        endToEndId,
        bookingDate,
        createdAt,
        referenceCode,
        bankRef
      })
      bankToCustomerDbtTrnList.push({
        payoutProcessId: payoutProcessInfo._id,
        bankToCustomerDbtTrnData
      })
    }
  }

  return { bankToCustomerDbtTrnList, payoutProcessIds }
}

export const prepareDataToUpdateBookingDate = async (params) => {
  const {
    payoutProcessInfo,
    creditTransferData,
    xmlReceivedFileName,
    netsReceivedFileId
  } = params
  let payoutId = null
  const creditTransferInfo = []
  let paymentId = null
  let feedbackCreatedAt = ''
  let feedbackStatusLog = []
  feedbackStatusLog = payoutProcessInfo.feedbackStatusLog || []

  for (const creditTransfer of payoutProcessInfo.creditTransferInfo) {
    if (size(creditTransfer) && size(creditTransfer.paymentEndToEndId)) {
      const getCreditTransferData = creditTransferData.find(
        (newCreditTransferInfo) =>
          size(newCreditTransferInfo) &&
          newCreditTransferInfo.endToEndId === creditTransfer.paymentEndToEndId
      )

      //check payout id, paymentEndToEndId and set booking date and status
      if (
        size(getCreditTransferData) &&
        getCreditTransferData.bookingDate &&
        (creditTransfer.payoutId || creditTransfer.paymentId)
      ) {
        creditTransfer.bookingDate = await appHelper.getActualDate(
          payoutProcessInfo.partnerId,
          false,
          getCreditTransferData.bookingDate
        )
        creditTransfer.status = 'booked'

        if (size(getCreditTransferData.bankRef))
          creditTransfer.bankRef = getCreditTransferData.bankRef

        feedbackCreatedAt = getCreditTransferData.createdAt || ''

        if (size(creditTransfer.payoutId)) payoutId = creditTransfer.payoutId

        if (size(creditTransfer.paymentId)) paymentId = creditTransfer.paymentId
      }
    }
    creditTransferInfo.push(creditTransfer)
    return creditTransfer
  }

  const findFeedbackStatusLog = feedbackStatusLog.find(
    (feedbackStatusLogInfo) =>
      feedbackStatusLogInfo &&
      feedbackStatusLogInfo.receivedFileName === xmlReceivedFileName
  )

  if (!size(findFeedbackStatusLog)) {
    feedbackStatusLog.push({
      createdAt: feedbackCreatedAt
        ? moment(feedbackCreatedAt).toDate()
        : moment().toDate(),
      status: 'bank_feedback',
      receivedFileName: xmlReceivedFileName,
      netsReceivedFileId
    })
  }
  return { feedbackStatusLog, creditTransferInfo, payoutId, paymentId }
}

export const getPayoutProcessFromOutboundXmlData = async (
  groupHeaderMsgId,
  status
) => {
  let payoutProcessInfo = {}
  if (
    size(groupHeaderMsgId) &&
    groupHeaderMsgId.includes('AML.PAYMENT') &&
    !status
  ) {
    const payoutProcessId = last(groupHeaderMsgId.split('.'))
    console.log('payoutProcessId ', payoutProcessId)
    payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
      _id: payoutProcessId
    })
  } else if (
    size(groupHeaderMsgId) &&
    groupHeaderMsgId.includes('ApprovalData') &&
    !status
  ) {
    const payoutProcessId = groupHeaderMsgId.split('.')[1]
    payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
      _id: payoutProcessId
    })
  } else if (
    size(groupHeaderMsgId) &&
    groupHeaderMsgId.includes('ISO.PAIN001') &&
    !status
  ) {
    const payoutProcessId = groupHeaderMsgId.split('.')[2]
    payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
      _id: payoutProcessId
    })
  } else if (size(groupHeaderMsgId) && status) {
    payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
      groupHeaderMsgId
    })
  }
  return payoutProcessInfo
}

export const getPaymentsDataFromRmtInf = (RmtInf) => {
  const payments = []
  let paymentAddedFromKid = false
  if (size(RmtInf) && size(RmtInf.Strd)) {
    for (const Strd of RmtInf.Strd) {
      if (size(Strd.CdtrRefInf) && size(Strd.CdtrRefInf[0])) {
        const CdtrRefInf = Strd.CdtrRefInf[0]
        let hasKID = false

        if (
          size(CdtrRefInf) &&
          size(CdtrRefInf.Tp[0]) &&
          size(CdtrRefInf.Tp[0].CdOrPrtry) &&
          size(CdtrRefInf.Tp[0].CdOrPrtry[0]) &&
          size(CdtrRefInf.Tp[0].CdOrPrtry[0].Cd) &&
          CdtrRefInf.Tp[0].CdOrPrtry[0].Cd[0] === 'SCOR'
        ) {
          hasKID = true
        }

        if (hasKID) {
          const KID = CdtrRefInf.Ref[0]
          if (
            size(Strd.RfrdDocAmt) &&
            size(Strd.RfrdDocAmt[0]) &&
            size(Strd.RfrdDocAmt[0].RmtdAmt) &&
            size(Strd.RfrdDocAmt[0].RmtdAmt[0])
          ) {
            const amount = Strd.RfrdDocAmt[0].RmtdAmt[0]._
            paymentAddedFromKid = true //this transaction will be added KID wise

            payments.push({ amount, kidNumber: KID })
          }
        }
      }
    }
  }
  return {
    paymentsRmtInf: payments,
    paymentAddedFromKidRmtInf: paymentAddedFromKid
  }
}

export const getMetaDataFromRltdPties = (RltdPties) => {
  const metaData = {}
  if (size(RltdPties.Dbtr) && size(RltdPties.Dbtr[0])) {
    const Dbtr = RltdPties.Dbtr[0]
    if (size(Dbtr)) {
      let dbTrPostalAddress = ''
      const dbTrName = size(Dbtr.Nm) ? Dbtr.Nm[0] : ''
      const dbTrPstlAdr = size(Dbtr.PstlAdr) ? Dbtr.PstlAdr[0] : ''

      if (size(dbTrPstlAdr)) {
        if (size(dbTrPstlAdr.AdrLine) && size(dbTrPstlAdr.AdrLine[0]))
          dbTrPostalAddress += dbTrPstlAdr.AdrLine[0]
        if (size(dbTrPstlAdr.TwnNm) && size(dbTrPstlAdr.TwnNm[0]))
          dbTrPostalAddress += ', ' + dbTrPstlAdr.TwnNm[0]
        if (size(dbTrPstlAdr.PstCd) && size(dbTrPstlAdr.PstCd[0]))
          dbTrPostalAddress += ', ' + dbTrPstlAdr.PstCd[0]
      }

      metaData.dbTrName = dbTrName
      metaData.dbTrAddress = dbTrPostalAddress
    }
  }

  if (size(RltdPties.DbtrAcct) && size(RltdPties.DbtrAcct[0])) {
    const DbtrAcct = RltdPties.DbtrAcct[0]

    if (
      size(DbtrAcct) &&
      size(DbtrAcct.Id) &&
      size(DbtrAcct.Id[0]) &&
      size(DbtrAcct.Id[0].Othr) &&
      size(DbtrAcct.Id[0].Othr[0]) &&
      size(DbtrAcct.Id[0].Othr[0].Id) &&
      size(DbtrAcct.Id[0].Othr[0].Id[0])
    ) {
      metaData.dbTrAccountNumber = DbtrAcct.Id[0].Othr[0].Id[0]
    }
  }

  if (size(RltdPties.Cdtr) && size(RltdPties.Cdtr[0])) {
    const Cdtr = RltdPties.Cdtr[0]
    if (size(Cdtr)) {
      let CdtrPostalAddress = ''
      const CdtrName = size(Cdtr.Nm) ? Cdtr.Nm[0] : ''
      const CdtrPstlAdr = size(Cdtr.PstlAdr) ? Cdtr.PstlAdr[0] : ''

      if (size(CdtrName)) {
        if (CdtrPstlAdr.AdrLine && CdtrPstlAdr.AdrLine[0])
          CdtrPostalAddress += CdtrPstlAdr.AdrLine[0]
        if (CdtrPstlAdr.TwnNm && CdtrPstlAdr.TwnNm[0])
          CdtrPostalAddress += ', ' + CdtrPstlAdr.TwnNm[0]
        if (CdtrPstlAdr.PstCd && CdtrPstlAdr.PstCd[0])
          CdtrPostalAddress += ', ' + CdtrPstlAdr.PstCd[0]
      }

      metaData.cdTrName = CdtrName
      metaData.cdTrAddress = CdtrPostalAddress
    }
  }

  if (size(RltdPties.CdtrAcct) && size(RltdPties.CdtrAcct[0])) {
    const CdtrAcct = RltdPties.CdtrAcct[0]

    if (
      size(CdtrAcct) &&
      size(CdtrAcct.Id) &&
      size(CdtrAcct.Id[0]) &&
      size(CdtrAcct.Id[0].Othr) &&
      size(CdtrAcct.Id[0].Othr[0]) &&
      size(CdtrAcct.Id[0].Othr[0].Id) &&
      size(CdtrAcct.Id[0].Othr[0].Id[0])
    ) {
      metaData.cdTrAccountNumber = CdtrAcct.Id[0].Othr[0].Id[0]
    }
  }

  return metaData
}
