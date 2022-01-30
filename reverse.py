from transactions import reverseAllTransactions
from mambu import changeLoanStatus, api
from migrate import VlsLoan

def reverse(loan):

    for loan in loans:
        
        vlsConNum = loan[0]
        mamLoanId = loan[1]

        vlsLoan = VlsLoan(vlsConNum)

        logData = {
            'requestType': 'getLoanToReverse'
        }

        res = api(2, 'GET', '/loans/' + mamLoanId, {}, {}, logData, vlsLoan)
        
        vlsLoan.mamLoan = res.json()
        
        reverseAllTransactions(vlsLoan)
        changeLoanStatus(vlsLoan, 'DISBURSMENT_ADJUSTMENT')
        changeLoanStatus(vlsLoan, 'UNDO_APPROVAL')
        changeLoanStatus(vlsLoan, 'WITHDRAW')
    

if __name__ == '__main__':
    
    # mamLoanIds = [
    #     'KoraJPPD',
    #     'cZGImCRB',
    #     'iQKcAAHC',
    #     'yQhIuJZM',
    #     'obUmflXA',
    #     ]

    # ['vlsConNum', 'mamLoanId']
    loans = [
        ['A000114114','qHSrWnYK'],
        ['A000114108','mxNRTfHa'],
        ['A000114084','ppFTdzlr'],
        ['A000114062','kLMNwDvb'],
        ['A000114059','DRkHDGcS'],
    ]

    reverse(loans)