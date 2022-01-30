import util, mambu


def getRepayments(vlsLoan):
    
    mamLoanId = vlsLoan.mamLoan['id']
    
    reqParams = {
        "limit": "1000",
    }
    
    logData = {
        "mamLoanId": mamLoanId,
        "requestType": "getRepayments",
    }

    req = mambu.api(1, 'GET', '/loans/' + mamLoanId + '/repayments', reqParams, {}, logData, vlsLoan)

    repayments = []

    for r in req.json():
        
        repayment = {
                "encodedKey": r["encodedKey"],
                "principalDue": r["principalDue"],
                "dueDate": r["dueDate"][:10],
                "interestDue": r["interestDue"],
                "parentAccountKey": r["parentAccountKey"],
            }
        
        repayments.append(repayment)
    
    return repayments
