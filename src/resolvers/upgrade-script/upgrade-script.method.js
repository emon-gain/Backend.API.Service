import { size } from 'lodash'

import {} from '../helpers'
import {
  appQueueHelper,
  invoiceHelper,
  tenantHelper,
  transactionHelper,
  upgradeScriptHelper
} from '../helpers'

import {
  AppQueueCollection,
  DepositAccountCollection,
  TenantCollection,
  UserCollection
} from '../models'

import {
  appQueueService,
  invoiceService,
  propertyItemService,
  transactionService,
  upgradeScriptService,
  userService
} from '../services'

const MethodList = {}

MethodList.removeEmailFromAnUser_13777 = async (req) => {
  try {
    console.log('=== Started upgrade script to remove an user email #13777')
    const { session } = req
    const userId = 'Y2iHdvbjmJeW2biKB'
    const user = await UserCollection.findOne({ _id: userId })

    console.log('=== User emails was', JSON.stringify(user?.emails))
    const updatedUser = await UserCollection.findOneAndUpdate(
      { _id: userId },
      {
        $pull: {
          emails: { address: 'tom.henry@sjolystregnskap.no', verified: false }
        }
      },
      {
        runValidators: true,
        new: true,
        session
      }
    )
    console.log('=== User emails now', JSON.stringify(updatedUser?.emails))
    console.log('=== Ended upgrade script to remove an user email #13777')
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.removeEmailFromAnUser_13777, error:',
      err
    )
    throw new Error(err?.message)
  }
}

MethodList.insertAnTransactionForMakeBalanceAmount = async (req) => {
  try {
    console.log(
      'Started intering transaction for making balance transactions ====>>>'
    )
    const { session } = req

    const invoiceInfo = await invoiceHelper.getInvoice({
      _id: 'W22vAAgJJXmZDSi6A'
    })
    console.log('invoiceInfo  =======> ', invoiceInfo)
    const queue = await appQueueHelper.getAnAppQueue({
      _id: 'wy991n4ho9i8dylor'
    })
    const { lostMeta = {} } = queue.params || {}

    console.log('lostmeta from queue ====> ', lostMeta)
    lostMeta.amount = lostMeta.amount * -1
    const transactionData =
      await invoiceHelper.prepareLossRecognitionTransactionData(
        invoiceInfo,
        lostMeta
      )

    await transactionService.createTransaction(transactionData, session)

    await appQueueService.updateAnAppQueue(
      { _id: 'wy991n4ho9i8dylor' },
      { status: 'completed' },
      session
    )
  } catch (err) {
    console.log(
      'Error happened when MethodList.insertAnTransactionForMakeBalanceAmount worked =====>>>>',
      err,
      '<============='
    )
    throw new Error(err?.message)
  }
}

MethodList.addASelfServicePartnerForUniteLiving = async (req) => {
  try {
    console.log(
      '=====> Started upgrade script to add a self service partner for Unite Living <===='
    )

    console.log(
      '+++ Checking adding result:',
      await upgradeScriptService.addASelfServicePartnerForUniteLiving(
        req.session
      ),
      '+++'
    )

    console.log(
      '=====> Ended upgrade script to add a self service partner for Unite Living <===='
    )
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.addASelfServicePartnerForUniteLiving, error:',
      err
    )
    throw new Error(err)
  }
}

MethodList.addTransactionForMakeAmountBalanced = async (req) => {
  try {
    console.log(
      '=====> Started upgrade script to add transaction for make amount balance for this invoiceId: kkc1xddmj63wr5vxk and create app queue <===='
    )
    const { session } = req
    const invoice = await invoiceHelper.getInvoice({ _id: 'kkc1xddmj63wr5vxk' })

    const transactionData = {
      creditAccountId: 'KJ4jS8heemJgvM7nt',
      creditAccountCode: 3600,
      creditTaxCodeId: 'ugWpPGm95fK6wrXLa',
      creditTaxCode: 0,
      creditTaxPercentage: 0,
      debitAccountId: 'GXxWxvSk5grvHbSZ2',
      debitAccountCode: 1500,
      debitTaxCodeId: 'ugWpPGm95fK6wrXLa',
      debitTaxCode: 0,
      debitTaxPercentage: 0,
      partnerId: 'uNm5XuswTj3odjn5X',
      contractId: 'w3S9ogrMRPJHywhRQ',
      agentId: 'hdSWAmBtenY5RiakR',
      branchId: 'po5cTt5eoJbH8Rye8',
      accountId: 'rEpuAi2dJAQ9KR9dG',
      propertyId: 'yB37W9znQjCoxSWB6',
      tenantId: 'hHipGXMoaACs2Q8Wc',
      type: 'credit_note',
      invoiceId: 'kkc1xddmj63wr5vxk',
      period: '2023-01',
      agentName: 'Morten Jørgensen',
      accountName: 'MMJ Eiendom AS - Morten Jørgensen',
      accountSerialId: '2',
      accountAddress: 'Åsas vei 20',
      accountZipCode: '4633',
      accountCity: 'Kristiansand',
      accountCountry: 'Norge',
      assignmentNumber: '009100058001',
      internalAssignmentId: '',
      internalLeaseId: '',
      tenantName: 'Ingrid Marie Drange Aleksandersen',
      tenantSerialId: '85',
      tenantPhoneNumber: '+4746813805',
      tenantEmailAddress: 'ingridselmao@icloud.com',
      locationName: 'Kvernveien 26C, 4630, Kristiansand, Norge',
      propertySerialId: '58',
      apartmentId: '4',
      locationZipCode: '4630',
      locationCity: 'Kristiansand',
      locationCountry: 'Norge',
      tenantAddress: 'Kvernveien 26C, 4630, Kristiansand, Norge',
      tenantZipCode: '4630',
      tenantCity: 'Kristiansand',
      tenantCountry: 'Norge',
      bankAccountNumber: '30005688155',
      invoiceSerialId: '20298',
      kidNumber: '0091000580010',
      invoiceDueDate: invoice.dueDate,
      createdAt: invoice.createdAt
    }

    for (let i = 0; i < 2; i++) {
      await transactionService.createTransaction(
        {
          subType: 'rent',
          amount: i === 0 ? 5100 : -132,
          ...transactionData
        },
        session
      )
      await transactionService.createTransaction(
        {
          subType: 'addon',
          amount: i === 0 ? 400 : -38.71,
          addonId: 'df7c7RFMxkjdmL8GA',
          addonName: 'Strøm',
          ...transactionData
        },
        session
      )
    }

    await invoiceService.updateInvoice(
      {
        _id: 'kkc1xddmj63wr5vxk'
      },
      {
        $set: {
          addonsMeta: [
            {
              type: 'addon',
              description: 'Strøm',
              qty: -1,
              taxPercentage: 0,
              price: 38.71,
              total: -38.71,
              addonId: 'df7c7RFMxkjdmL8GA'
            }
          ],
          invoiceContent: [
            {
              type: 'monthly_rent',
              qty: -1,
              price: 132,
              total: -132,
              taxPercentage: 0
            }
          ]
        }
      },
      session
    )
    const appQueueData = await transactionHelper.prepareSerialIdAppQueue(
      'uNm5XuswTj3odjn5X',
      session
    )
    console.log(appQueueData, '<---===== Should get app queue data')

    await appQueueService.createAnAppQueue(appQueueData, session)
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.addTransactionForMakeAmountBalanced, error:',
      err
    )
    throw new Error(err)
  }
}

MethodList.removePropertyObjectFromTenants = async (req) => {
  try {
    console.log('Started upgrade script for removePropertyObjectFromTenants')

    const { session } = req
    await upgradeScriptService.removePropertyObjectFromTenants(session)

    console.log('Ended upgrade script for update failed queues')
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.removePropertyObjectFromTenants, error:',
      err
    )
    throw new Error(err?.message)
  }
}

MethodList.updateQToCompleted_13789 = async (req) => {
  try {
    console.log('=== Started upgrade script to update a Q to completed #13789')
    const { session } = req
    const queueId = '5hciebm735rv1koh3'
    console.log('=== QueueId is: ', queueId)
    const updatedQ = await AppQueueCollection.findOneAndUpdate(
      { _id: queueId },
      {
        $set: {
          status: 'completed',
          isManuallyCompleted: true,
          errorDetails: {
            message: 'Manually completed by #13789'
          }
        }
      },
      {
        runValidators: true,
        new: true,
        session
      }
    )
    console.log('=== Updated Q response', updatedQ)
    console.log('=== Ended upgrade script to update a Q to completed #13789')
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.updateQToCompleted_13789, error:',
      err
    )
    throw new Error(err?.message)
  }
}

MethodList.updateFailedAppQueuesOfDifferentTypesToCompleted = async (req) => {
  try {
    console.log(
      '====> Started upgrade script to update failed app queues of different types to completed <===='
    )

    await upgradeScriptService.changeAppQueueStatus(
      [
        '8yqs6v0b6vi9u68yi', // #13740
        '7n6ixv3hrt7044wap', // #13740
        '3w23wybq8dbx0495x',
        'zuc537gvr43el7tpw',
        'oovs66xapo4b1m0kl',
        'sjq8ya9loutazcwa9',
        'emby7j5lmamw07mcj'
      ],
      'completed',
      req.session
    )

    console.log(
      '====> Ended upgrade script to update failed app queues of different types to completed <===='
    )
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.updateFailedAppQueuesOfDifferentTypesToCompleted, error:',
      err
    )
    throw new Error(err?.message)
  }
}

MethodList.updateMoveInDocument2 = async (req) => {
  console.log('Started upgrade script for updateMoveInDocument')
  try {
    const { session } = req
    await propertyItemService.updatePropertyItems(
      { _id: { $in: ['2t3by7ymy2lw9zs6k', '5d79t1v94bzu1n7d0'] } },
      { moveInCompleted: true, movingInSigningComplete: true },
      session
    )
    console.log('Completed updated property item for 2t3by7ymy2lw9zs6k')
  } catch (e) {
    console.log('ERROR occurred on updating property item: ', e)
  }
}

MethodList.removePaymentsFromDA_13784 = async (req) => {
  try {
    console.log(
      '=== Started upgrade script to remove payments from deposit account #13784'
    )
    const { session } = req

    const depositQuires = [
      {
        _id: 'xaglv8clsfog70gn9',
        contractId: '6aftr2rpqmh89naig',
        partnerId: 'uEoy3m8cnav2LF2ty',
        tenantId: '3rbicr9417z449eoi'
      },
      {
        _id: 'm6byxj5i6f7u55h67',
        contractId: '6aftr2rpqmh89naig',
        partnerId: 'uEoy3m8cnav2LF2ty',
        tenantId: '0of2vc5bm79vje3kb'
      }
    ]

    for (const query of depositQuires) {
      console.log('=== Query', query)
      const depositAccount = await DepositAccountCollection.findOne(query)
      if (size(depositAccount)) {
        const { totalPaymentAmount, depositAmount, payments } = depositAccount
        console.log(
          '=== TotalPaymentAmount',
          totalPaymentAmount,
          'depositAmount',
          depositAmount,
          'payments',
          JSON.stringify(payments)
        )
        const newPaymentsArray = [payments[0]]
        console.log('=== newPaymentsArray', newPaymentsArray)

        const updatedDA = await DepositAccountCollection.findOneAndUpdate(
          query,
          {
            $set: {
              payments: newPaymentsArray,
              totalPaymentAmount: depositAmount
            }
          },
          { runValidators: true, new: true, session }
        )
        if (!size(updatedDA)) throw new Error('Unable to update DA')
        console.log(
          '=== Updated DA ===',
          updatedDA.totalPaymentAmount,
          updatedDA.payments
        )
      } else {
        console.log('=== No deposit account found with query', query)
        throw new Error('No deposit account')
      }
    }

    console.log(
      '=== Ended upgrade script to remove payments from deposit account #13784'
    )
  } catch (err) {
    console.log(
      '=====> Error happened in MethodList.removePaymentsFromDA_13784, error:',
      err
    )
    throw new Error(err?.message)
  }
}

MethodList.addMissingTenantData = async (req) => {
  console.log('Started upgrade script for addMissingTenantData')
  try {
    const { session } = req
    const { tenant, partners } = upgradeScriptHelper.missingTenantsData || {}
    const existsTenant = await tenantHelper.getATenant(
      { _id: tenant?._id },
      session
    )

    if (size(existsTenant)) {
      console.log('Tenant already exists')
      return false
    }

    const [tenantInfo] = await TenantCollection.insertMany([tenant], {
      session
    })

    await userService.updateAnUser(
      { _id: tenantInfo?.userId },
      {
        $set: { partners }
      },
      session
    )
    console.log('Ended upgrade script for addMissingTenantData')
  } catch (e) {
    console.log('Error happened in MethodList.addMissingTenantData: ', e)
    throw new Error(e)
  }
}

export { MethodList }
