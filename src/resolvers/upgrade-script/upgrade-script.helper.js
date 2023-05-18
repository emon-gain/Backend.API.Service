import {} from 'lodash'

import {} from '../helpers'

import {} from '../models'

import {} from '../services'

export const missingTenantsData = {
  partners: [
    {
      partnerId: 'Kofdgk654qqK3CT5s',
      type: 'tenant',
      status: 'invited',
      token: '0tx6p8if4rpbts5b3nyetw1d',
      expires: '2022-07-02T18:10:31.477Z'
    }
  ],
  tenant: {
    _id: 'zEoNFAs4qJvcuK2KB',
    name: 'Daniel Nilsen',
    partnerId: 'Kofdgk654qqK3CT5s',
    billingAddress: 'Trondheimsveien 82',
    zipCode: '0565',
    city: 'Oslo',
    country: 'Norge',
    userId: 'L99BYMCKoCAE6SbFe',
    properties: [
      {
        propertyId: 'i3tYbT7KQHZdujwpY',
        accountId: 'dfib8rGQLWN8MQMsd',
        branchId: 'H88uM2ifadD8KfQ76',
        agentId: 'bBhaCEogksedpEykq',
        status: 'closed',
        createdAt: '2022-06-24T08:09:05.878Z',
        createdBy: 'bBhaCEogksedpEykq',
        contractId: 'bgw8PX66RMc3eEMbe'
      },
      {
        propertyId: 'i3tYbT7KQHZdujwpY',
        accountId: 'dfib8rGQLWN8MQMsd',
        branchId: 'H88uM2ifadD8KfQ76',
        agentId: 'bBhaCEogksedpEykq',
        status: 'active',
        contractId: 'fpTchhXQhexRkT8m6',
        createdAt: '2022-06-27T10:44:37.245Z',
        createdBy: 'bBhaCEogksedpEykq'
      }
    ],
    type: 'active',
    createdAt: '2022-06-24T08:09:05.881Z',
    createdBy: 'bBhaCEogksedpEykq',
    depositAccountMeta: {
      kycForms: [
        {
          contractId: 'bgw8PX66RMc3eEMbe',
          referenceNumber: 'Rwn8Pdi57He3voj2f',
          depositAmount: 61500,
          status: 'new',
          formData: {
            nationalIdentityNumber: '28029837769',
            referenceNumber: 'Rwn8Pdi57He3voj2f',
            irregularIncome: {
              hasIrregularIncome: true,
              incomes: [
                {
                  type: 'SALARY_PENSION_SOCIAL_SECURITY',
                  amount: 500000,
                  comment:
                    'Summen er antatt årslønn i tillegg til allerede inneværende sum, summen er ikke nødvendigvis helt nøyaktig. Får ikke sjekket kontrakt for øyeblikket, og venter enda svar fra sjef. '
                }
              ]
            },
            politicallyExposedPerson: {
              isPoliticallyExposedPerson: false
            },
            taxableAbroad: {
              isTaxableAbroad: false
            },
            taxResidentOrResidentOfUsa: {
              isTaxResidentOrResidentOfUsa: false
            }
          },
          isSubmitted: true,
          createdAt: '2022-06-24T22:00:00.000Z'
        }
      ]
    },
    serial: 1000650
  }
}
