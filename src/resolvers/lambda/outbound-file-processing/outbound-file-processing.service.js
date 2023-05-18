import { clone, find, head, indexOf, map, size } from 'lodash'
import moment from 'moment-timezone'
import {
  appHelper,
  invoicePaymentHelper,
  outBoundFileProcessingHelper,
  payoutProcessHelper
} from '../../helpers'
import {
  invoicePaymentService,
  netReceivedFileService,
  payoutProcessService,
  payoutService
} from '../../services'
import { CustomError } from '../../common'

export const processSftpReceivedFileInS3 = async (req) => {
  const { body, session } = req
  appHelper.checkRequiredFields(
    ['outboundXmlData', 'xmlReceivedFileName'],
    body
  )
  const { outboundXmlData, xmlReceivedFileName, netsReceivedFileId } = body
  const xmlJsonObject = outboundXmlData.Document
  console.log('+++ Checking xmlReceivedFileName ', xmlReceivedFileName)
  console.log('+++ Checking netsReceivedFileId ', netsReceivedFileId)
  console.log('+++ Checking xmlJsonObject ', xmlJsonObject)
  if (
    size(xmlJsonObject) &&
    size(xmlJsonObject.CstmrPmtStsRpt) &&
    size(xmlJsonObject.CstmrPmtStsRpt[0])
  ) {
    //update nets received file file type to CstmrPmtStsRpt
    await netReceivedFileService.updateANetReceivedFile(
      {
        _id: netsReceivedFileId
      },
      { $set: { fileType: 'CstmrPmtStsRpt' } }
    )
    return updatePayoutProcessDataFromOutboundXmlData(
      xmlJsonObject.CstmrPmtStsRpt[0],
      xmlReceivedFileName,
      session
    )
  } else if (
    size(xmlJsonObject) &&
    size(xmlJsonObject.BkToCstmrDbtCdtNtfctn) &&
    size(xmlJsonObject.BkToCstmrDbtCdtNtfctn[0])
  ) {
    console.log('BkToCstmrDbtCdtNtfctn ')
    //update nets received file file type to BkToCstmrDbtCdtNtfctn
    await netReceivedFileService.updateANetReceivedFile(
      {
        _id: netsReceivedFileId
      },
      { $set: { fileType: 'BkToCstmrDbtCdtNtfctn' } }
    )
    //TODO: #10482 Test cases for this function
    const params = {
      doc: xmlJsonObject.BkToCstmrDbtCdtNtfctn[0],
      xmlReceivedFileName,
      netsReceivedFileId
    }
    return await processPaymentReceivedXML(params, session)
  } else {
    console.log(
      "We've found an unknown file from S3. Unable to process.",
      xmlReceivedFileName
    )
    throw new CustomError('Unknown file from S3')
  }
}

const updatePayoutProcessDataFromOutboundXmlData = async (
  customerPaymentStatusReport,
  xmlReceivedFileName,
  session
) => {
  if (size(customerPaymentStatusReport) && size(xmlReceivedFileName)) {
    if (xmlReceivedFileName.includes('AML.RECEIPT')) {
      const groupHeaderInfo = head(customerPaymentStatusReport.GrpHdr)
      const groupHeaderMsgId =
        size(groupHeaderInfo) && size(groupHeaderInfo.MsgId)
          ? head(groupHeaderInfo.MsgId)
          : ''
      const originalGroupInfoStatus = head(
        customerPaymentStatusReport.OrgnlGrpInfAndSts
      )
      const statusReasonInfo =
        size(originalGroupInfoStatus) && size(originalGroupInfoStatus.StsRsnInf)
          ? head(originalGroupInfoStatus.StsRsnInf)
          : {}
      const statusReasonReason =
        size(statusReasonInfo) && size(statusReasonInfo.Rsn)
          ? head(statusReasonInfo.Rsn)
          : {}
      const statusReasonReasonCode =
        size(statusReasonReason) && size(statusReasonReason.Cd)
          ? head(statusReasonReason.Cd)
          : ''
      const status = !!(
        size(statusReasonReasonCode) && statusReasonReasonCode === 'OK'
      )
      const feedbackStatus = status ? 'ASICE_OK' : 'RJCT'
      let feedbackCreatedAt =
        size(groupHeaderInfo) && size(groupHeaderInfo.CreDtTm)
          ? head(groupHeaderInfo.CreDtTm)
          : ''
      const payoutFeedbackStatusHistory = []

      feedbackCreatedAt = moment(feedbackCreatedAt).toDate()
      const payoutProcessInfo =
        await outBoundFileProcessingHelper.getPayoutProcessFromOutboundXmlData(
          groupHeaderMsgId,
          status
        )
      if (!(size(groupHeaderMsgId) && status) && !size(payoutProcessInfo)) {
        console.log('Got error in AML file from NETS', xmlReceivedFileName)
        return { invalidFile: true, success: false }
      }
      if (size(payoutProcessInfo)) {
        const feedbackStatusHistory = size(payoutProcessInfo.feedbackStatusLog)
          ? payoutProcessInfo.feedbackStatusLog
          : []
        //set feedback createdAt and status in feedback status log
        feedbackStatusHistory.push({
          createdAt: feedbackCreatedAt,
          status: feedbackStatus,
          receivedFileName: xmlReceivedFileName
        })

        const updatedData = {
          feedbackStatusLog: feedbackStatusHistory
        }
        if (size(feedbackStatus) && feedbackStatus === 'RJCT') {
          const creditTransferData = map(
            payoutProcessInfo.creditTransferInfo,
            (creditTransInfo) => {
              creditTransInfo.status = feedbackStatus
              creditTransInfo.reason = statusReasonReasonCode

              const feedbackStatusLogHistory =
                outBoundFileProcessingHelper.getFeedbackHistory(
                  creditTransInfo,
                  xmlReceivedFileName,
                  feedbackCreatedAt
                )

              payoutFeedbackStatusHistory.push(feedbackStatusLogHistory)

              return creditTransInfo
            }
          )

          updatedData.creditTransferInfo = creditTransferData
          //Payout feedback history
          if (size(payoutFeedbackStatusHistory)) {
            for (const feedbackInfo of payoutFeedbackStatusHistory) {
              if (size(feedbackInfo.paymentId)) {
                const paymentId = feedbackInfo.paymentId
                delete feedbackInfo.paymentId
                const updatedRefundPayment =
                  await invoicePaymentService.updateAnInvoicePayment(
                    {
                      _id: paymentId
                    },
                    {
                      $push: { feedbackStatusLog: feedbackInfo }
                    },
                    session
                  )

                if (updatedRefundPayment?._id)
                  await invoicePaymentService.createRefundPaymentUpdatedLog(
                    updatedRefundPayment,
                    session,
                    feedbackInfo
                  )
              } else {
                const payoutId = feedbackInfo.payoutId
                delete feedbackInfo.payoutId
                const updatedPayout = await payoutService.updateAPayout(
                  {
                    _id: payoutId
                  },
                  {
                    $push: { feedbackStatusLog: feedbackInfo }
                  },
                  session
                )

                if (updatedPayout?._id)
                  await payoutService.createLogForUpdatedPayout(
                    updatedPayout,
                    session,
                    {
                      payoutFeedbackHistory: feedbackInfo
                    }
                  )
              }
            }
          }
        }

        const updatedPayoutProcess =
          await payoutProcessService.updateAPayoutProcessWithAfterUpdate(
            {
              _id: payoutProcessInfo._id,
              partnerId: payoutProcessInfo.partnerId
            },
            {
              $set: {
                ...updatedData
              }
            },
            session
          )
        if (size(updatedPayoutProcess)) {
          // return payoutProcessInfo.partnerId
          return { success: true }
        } else {
          console.log("Payout Process data couldn't updated successfully")
          return { success: false }
        }
      } else {
        console.log(
          'We got an unknown AML file in S3 NETS/Received directory',
          xmlReceivedFileName
        )
        return { success: false }
      }
    } else {
      //partner to landlord credit transfer information status
      //get customer payment status from xml file data
      const groupHeaderInfo =
        size(customerPaymentStatusReport) &&
        size(customerPaymentStatusReport.GrpHdr)
          ? customerPaymentStatusReport.GrpHdr[0]
          : {}
      const originalGroupInfoStatus =
        size(customerPaymentStatusReport) &&
        size(customerPaymentStatusReport.OrgnlGrpInfAndSts)
          ? customerPaymentStatusReport.OrgnlGrpInfAndSts[0]
          : {}
      const originalPaymentInfoStatus =
        size(customerPaymentStatusReport) &&
        size(customerPaymentStatusReport.OrgnlPmtInfAndSts)
          ? customerPaymentStatusReport.OrgnlPmtInfAndSts[0]
          : {}
      const transactionInfoStatus = size(originalPaymentInfoStatus.TxInfAndSts)
        ? originalPaymentInfoStatus.TxInfAndSts
        : []
      let feedbackCreatedAt =
        size(groupHeaderInfo) && size(groupHeaderInfo.CreDtTm)
          ? groupHeaderInfo.CreDtTm[0]
          : ''
      const groupHeaderMsgId = size(originalGroupInfoStatus.OrgnlMsgId)
        ? originalGroupInfoStatus.OrgnlMsgId[0]
        : ''
      const feedbackStatus = size(originalGroupInfoStatus.GrpSts)
        ? originalGroupInfoStatus.GrpSts[0]
        : ''

      if (!size(groupHeaderMsgId)) {
        console.log('Got invalid file from NETS', xmlReceivedFileName)
        //we can't do farther processing of the file without the groupHeaderMsgId
        //we'll mark the file as processed with invalidFile flag.

        return { invalidFile: true, success: false }
      }

      feedbackCreatedAt = moment(feedbackCreatedAt).toDate()

      const payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
        groupHeaderMsgId
      })
      if (size(payoutProcessInfo)) {
        const processUpdateData = {}
        const feedbackStatusHistory =
          size(payoutProcessInfo) && size(payoutProcessInfo.feedbackStatusLog)
            ? payoutProcessInfo.feedbackStatusLog
            : []
        const payoutFeedbackStatusHistory = []

        //set feedback createdAt and status in feedback status log
        feedbackStatusHistory.push({
          createdAt: feedbackCreatedAt,
          status: feedbackStatus,
          receivedFileName: xmlReceivedFileName
        })
        processUpdateData.feedbackStatusLog = feedbackStatusHistory

        if (
          size(transactionInfoStatus) ||
          (size(feedbackStatus) &&
            indexOf(['ACCP', 'RJCT'], feedbackStatus) !== -1)
        ) {
          const creditTransferData = map(
            payoutProcessInfo.creditTransferInfo,
            function (creditTransInfo) {
              const findTransactionInfo = find(
                transactionInfoStatus,
                function (transactionInfo) {
                  return (
                    transactionInfo.OrgnlInstrId[0] ===
                      creditTransInfo.paymentInstrId &&
                    transactionInfo.OrgnlEndToEndId[0] ===
                      creditTransInfo.paymentEndToEndId
                  )
                }
              )
              const reasonCode = outBoundFileProcessingHelper.getReasonCode(
                findTransactionInfo || originalGroupInfoStatus
              )
              let feedbackStatusLogHistory = {}

              if (
                size(feedbackStatus) &&
                indexOf(['ACCP', 'RJCT'], feedbackStatus) !== -1
              ) {
                creditTransInfo.status = feedbackStatus

                if (reasonCode) {
                  creditTransInfo.reason = reasonCode
                  //Payout update for feedback log history
                  if (feedbackStatus === 'RJCT') {
                    feedbackStatusLogHistory =
                      outBoundFileProcessingHelper.getFeedbackHistory(
                        creditTransInfo,
                        xmlReceivedFileName,
                        feedbackCreatedAt
                      )
                    payoutFeedbackStatusHistory.push(feedbackStatusLogHistory)
                  }
                }
              } else {
                if (size(findTransactionInfo)) {
                  creditTransInfo.status = findTransactionInfo.TxSts[0]
                  creditTransInfo.reason = reasonCode || ''

                  //Payout update for feedback log history
                  if (creditTransInfo.status === 'PDNG') {
                    feedbackStatusLogHistory =
                      outBoundFileProcessingHelper.getFeedbackHistory(
                        creditTransInfo,
                        xmlReceivedFileName,
                        feedbackCreatedAt
                      )
                    payoutFeedbackStatusHistory.push(feedbackStatusLogHistory)
                  }
                }
              }
              return creditTransInfo
            }
          )

          processUpdateData.creditTransferInfo = creditTransferData
        }

        //Payout feedback history
        if (size(payoutFeedbackStatusHistory)) {
          for (const feedbackInfo of payoutFeedbackStatusHistory) {
            if (size(feedbackInfo.paymentId)) {
              const paymentId = feedbackInfo.paymentId
              delete feedbackInfo.paymentId
              const updatedRefundPayment =
                await invoicePaymentService.updateAnInvoicePayment(
                  {
                    _id: paymentId
                  },
                  {
                    $push: { feedbackStatusLog: feedbackInfo }
                  },
                  session
                )
              if (updatedRefundPayment?._id)
                await invoicePaymentService.createRefundPaymentUpdatedLog(
                  updatedRefundPayment,
                  session,
                  feedbackInfo
                )
            } else {
              const payoutId = feedbackInfo.payoutId

              delete feedbackInfo.payoutId
              const updatedPayout = await payoutService.updateAPayout(
                {
                  _id: payoutId
                },
                { $push: { feedbackStatusLog: feedbackInfo } }
              )

              if (updatedPayout?._id)
                await payoutService.createLogForUpdatedPayout(
                  updatedPayout,
                  session,
                  {
                    payoutFeedbackHistory: feedbackInfo
                  }
                )
            }
          }
        }

        const updatedPayoutProcess =
          await payoutProcessService.updateAPayoutProcessWithAfterUpdate(
            {
              _id: payoutProcessInfo._id,
              partnerId: payoutProcessInfo.partnerId
            },
            { $set: { ...processUpdateData } },
            session
          )

        if (size(updatedPayoutProcess)) {
          // return payoutProcessInfo.partnerId
          return { success: true }
        } else {
          console.log("Payout Process data couldn't be updated successfully")
          return { success: false }
        }
      } else {
        console.log(
          'We got an unknown file in S3 NETS/Received directory',
          xmlReceivedFileName
        )
        return { success: false }
      }
    }
  } else return { success: false }
}

const processPaymentReceivedXML = async (params, session) => {
  console.log(' ==== Starting processPaymentReceivedXML ==== ')
  const { doc, xmlReceivedFileName, netsReceivedFileId } = params
  let hasError = false
  console.log(
    'doc xmlReceivedFileName netsReceivedFileId ',
    size(doc) && size(xmlReceivedFileName) && size(netsReceivedFileId)
  )
  if (!(size(doc) && size(xmlReceivedFileName) && size(netsReceivedFileId))) {
    return { success: false }
  }
  const groupHeader = doc.GrpHdr[0]
  const createdAt = groupHeader.CreDtTm[0]
  let nodeIndex = 0
  let payoutProcessIds = []
  let bankToCustomerDbtTrnList = [] //Bank to customer debit transactions list for update payout process credit transfer info
  console.log('+++ Checking notification of doc: ', doc.Ntfctn)
  if (!size(doc.Ntfctn)) {
    console.log('NO notification found for: ', xmlReceivedFileName)
    return { success: false }
  }
  for (const Ntfctn of doc.Ntfctn) {
    //check the entries
    const entries = Ntfctn.Ntry
    console.log('Found entries: ', entries)
    if (size(entries)) {
      for (const entry of entries) {
        let bookingDate = createdAt
        console.log('Found entry.CdtDbtInd[0]: ', entry.CdtDbtInd[0])
        if (size(entry.CdtDbtInd) && entry.CdtDbtInd[0] === 'CRDT') {
          await netReceivedFileService.updateANetReceivedFile(
            {
              _id: netsReceivedFileId
            },
            { $set: { isCreditTransaction: true }, session }
          )

          let ntryRef = ''
          if (size(entry.NtryRef) && size(entry.NtryRef[0]))
            ntryRef = entry.NtryRef[0]

          if (size(entry.BookgDt) && size(entry.BookgDt[0].Dt)) {
            bookingDate = entry.BookgDt[0].Dt[0]
          }
          const entryDetails = entry.NtryDtls[0]
          console.log('entryDetails entryDetails: ', entryDetails)
          if (size(entryDetails)) {
            const TxDtls = entryDetails.TxDtls

            if (size(TxDtls)) {
              for (const txInfo of TxDtls) {
                let paymentAddedFromKid = false
                let payments = []
                let metaData = { bookingDate }

                if (
                  size(txInfo.AmtDtls) &&
                  size(txInfo.AmtDtls[0]) &&
                  size(txInfo.AmtDtls[0].TxAmt) &&
                  size(txInfo.AmtDtls[0].TxAmt[0]) &&
                  size(txInfo.AmtDtls[0].TxAmt[0].Amt)
                ) {
                  metaData.transactionTotal =
                    txInfo.AmtDtls[0].TxAmt[0].Amt[0]._
                }

                if (
                  size(txInfo.RltdDts) &&
                  size(txInfo.RltdDts[0]) &&
                  size(txInfo.RltdDts[0].IntrBkSttlmDt)
                ) {
                  metaData.settlementDate = txInfo.RltdDts[0].IntrBkSttlmDt[0]
                }

                //find the payments by kid numbers
                //if we don't found the kid number, then we'll add the payment based on the RltdPties
                //sometime there could be same entry twice. both in RltdPties and RmtInf
                //if we found the entry in RmtInf, we'll ignore the RltdPties
                //although we'll save all meta data.

                if (size(txInfo.RmtInf) && size(txInfo.RmtInf[0])) {
                  const RmtInf = txInfo.RmtInf[0]
                  const { paymentsRmtInf, paymentAddedFromKidRmtInf } =
                    outBoundFileProcessingHelper.getPaymentsDataFromRmtInf(
                      RmtInf
                    )
                  payments = [...payments, ...paymentsRmtInf]
                  paymentAddedFromKid = paymentAddedFromKidRmtInf
                }

                if (size(txInfo.RltdPties) && size(txInfo.RltdPties[0])) {
                  const RltdPties = txInfo.RltdPties[0]
                  const metaDataRltdPties =
                    outBoundFileProcessingHelper.getMetaDataFromRltdPties(
                      RltdPties
                    )
                  metaData = { ...metaData, ...metaDataRltdPties }
                }

                if (!paymentAddedFromKid) {
                  payments.push({ amount: metaData.transactionTotal })
                }
                console.log('prepared payments: ', payments)
                //insert the payments collection
                for (const paymentInfo of payments) {
                  const paymentData = {
                    paymentDate: moment(
                      metaData.bookingDate,
                      'YYYY-MM-DD'
                    ).toDate(),
                    amount: parseFloat(paymentInfo.amount),
                    paymentType: 'bank',
                    status: 'new',
                    meta: {
                      kidNumber: paymentInfo.kidNumber,
                      dbTrName: metaData.dbTrName,
                      dbTrAddress: metaData.dbTrAddress,
                      dbTrAccountNumber: metaData.dbTrAccountNumber,
                      cdTrName: metaData.cdTrName,
                      cdTrAddress: metaData.cdTrAddress,
                      cdTrAccountNumber: metaData.cdTrAccountNumber,
                      settlementDate: metaData.settlementDate,
                      bankRef: ntryRef
                    },
                    receivedFileName: xmlReceivedFileName,
                    netsReceivedFileId,
                    nodeIndex
                  }

                  try {
                    const exists = await invoicePaymentHelper.getInvoicePayment(
                      {
                        receivedFileName: xmlReceivedFileName,
                        nodeIndex: paymentData.nodeIndex
                      }
                    )
                    console.log('Is exists payments: ', exists)
                    if (size(exists))
                      console.log(
                        'Skip this payment since already exists in database. File name:' +
                          xmlReceivedFileName,
                        paymentData
                      )
                    else {
                      const createData =
                        await outBoundFileProcessingHelper.prepareDataToAddPayment(
                          paymentData
                        )
                      console.log('createData payments: ', createData)
                      await invoicePaymentService.insertAnInvoicePayment(
                        createData,
                        session
                      )
                    }
                  } catch (e) {
                    hasError = true
                    console.log(
                      'Error occurred during saving the payment data',
                      paymentData,
                      e
                    )
                  }
                  nodeIndex++
                }
              }
            }
          }
        } else if (size(entry.CdtDbtInd) && entry.CdtDbtInd[0] === 'DBIT') {
          const referenceCode = ''
          let bankRef = ''

          if (
            size(entry.BookgDt) &&
            size(entry.BookgDt[0]) &&
            size(entry.BookgDt[0].Dt) &&
            entry.BookgDt[0].Dt[0]
          ) {
            bookingDate = entry.BookgDt[0].Dt[0]
          }
          if (size(entry.NtryRef) && size(entry.NtryRef[0]))
            bankRef = entry.NtryRef[0]
          const entryDetails = entry.NtryDtls[0]

          if (size(entryDetails)) {
            const TxDtls = entryDetails.TxDtls

            if (size(TxDtls)) {
              for (const txInfo of TxDtls) {
                let endToEndId = ''

                if (
                  size(txInfo.Refs) &&
                  size(txInfo.Refs[0]) &&
                  size(txInfo.Refs[0].EndToEndId) &&
                  size(txInfo.Refs[0].EndToEndId[0])
                ) {
                  endToEndId = txInfo.Refs[0].EndToEndId[0]
                  const params = {
                    bankToCustomerDbtTrnList: clone(bankToCustomerDbtTrnList),
                    payoutProcessIds: clone(payoutProcessIds),
                    endToEndId: clone(endToEndId),
                    bookingDate: clone(bookingDate),
                    createdAt: clone(createdAt),
                    referenceCode: clone(referenceCode),
                    bankRef: clone(bankRef)
                  }
                  const newBankToCustomerDbtTrnList =
                    await outBoundFileProcessingHelper.getBankToCustomerDbtTrnListData(
                      params
                    )

                  bankToCustomerDbtTrnList =
                    size(newBankToCustomerDbtTrnList) &&
                    size(newBankToCustomerDbtTrnList.bankToCustomerDbtTrnList)
                      ? newBankToCustomerDbtTrnList.bankToCustomerDbtTrnList
                      : []
                  payoutProcessIds =
                    size(newBankToCustomerDbtTrnList) &&
                    size(newBankToCustomerDbtTrnList.payoutProcessIds)
                      ? newBankToCustomerDbtTrnList.payoutProcessIds
                      : []
                }
              }
            }
          }
        }
      }
    }
  }

  if (size(bankToCustomerDbtTrnList)) {
    for (const bankToCustomerDbtTrnInfo of bankToCustomerDbtTrnList) {
      const payoutProcessParams = {
        payoutProcessId: bankToCustomerDbtTrnInfo.payoutProcessId,
        creditTransferData: bankToCustomerDbtTrnInfo.bankToCustomerDbtTrnData,
        xmlReceivedFileName,
        netsReceivedFileId
      }
      hasError = await updateBookingDate(payoutProcessParams, session)
    }

    await netReceivedFileService.updateANetReceivedFile(
      {
        _id: netsReceivedFileId
      },
      { $set: { isDebitTransaction: true } },
      session
    )
  }

  return { success: !hasError }
}

const updateBookingDate = async (params, session) => {
  const {
    payoutProcessId,
    creditTransferData,
    xmlReceivedFileName,
    netsReceivedFileId
  } = params
  let hasError = false

  if (
    size(payoutProcessId) &&
    size(xmlReceivedFileName) &&
    size(creditTransferData)
  ) {
    const payoutProcessInfo = await payoutProcessHelper.getPayoutProcess({
      _id: payoutProcessId
    })
    if (size(payoutProcessInfo)) {
      const params = {
        payoutProcessInfo,
        creditTransferData,
        xmlReceivedFileName,
        netsReceivedFileId
      }
      const { paymentId, payoutId, creditTransferInfo, feedbackStatusLog } =
        await outBoundFileProcessingHelper.prepareDataToUpdateBookingDate(
          params
        )
      if (size(payoutId) || size(paymentId)) {
        try {
          await payoutProcessService.updateAPayoutProcess(
            { _id: payoutProcessInfo._id },
            {
              $set: {
                creditTransferInfo,
                feedbackStatusLog
              }
            },
            session
          )
        } catch (e) {
          hasError = true
          console.log(
            'Error occurred during updating the payout process ' +
              payoutProcessInfo._id,
            e
          )
        }
      }
    }
  } else
    console.log(
      'Not update payout process credit transfer info for payout process id: ',
      payoutProcessId,
      ' and received file name: ',
      xmlReceivedFileName
    )

  if (hasError) return hasError
}
