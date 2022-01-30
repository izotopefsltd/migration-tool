import json, requests
import util, mambu

def procTransactions(req, transactionTypes):
    
    transactions = []
    for r in req.json():
        
        transaction = {}
        if r["type"] in transactionTypes:
            transaction = {
                "encodedKey": r["encodedKey"],
                "transactionId": r["id"],
                "type": r["type"],
                "valueDate": r["valueDate"],
                "amount": r["amount"],
            }

            try:
                transaction["notes"] = r["notes"]
            except:
                transaction["notes"] = ""

            try:
                transaction["adjustmentTransactionKey"] = r["adjustmentTransactionKey"]
            except:
                transaction["adjustmentTransactionKey"] = ""

            transactions.append(transaction)
    
    return transactions


def getTransactions(vlsLoan, transactionTypes):

    mamLoanId = vlsLoan.mamLoan['id']
    
    logData = {
        "mamLoanId": mamLoanId,
        "requestType": "getTransactions",
    }

    reqParams = {
        "offset": "0",
        "limit": "1000",
        "paginationDetails": "ON",
        # "detailsLevel": "FULL",
    }

    req = mambu.api(2, 'GET', '/loans/' + mamLoanId + '/transactions', reqParams, {}, logData, vlsLoan)

    return procTransactions(req, transactionTypes)


def reverseAllTransactions(vlsLoan):
    
    mamLoanId = vlsLoan.mamLoan['id']
    
    transactionType = ['REPAYMENT', 'INTEREST_APPLIED', 'FEE_APPLIED']
    txs = getTransactions(vlsLoan, transactionType)

    for tx in txs:
        
        if tx["adjustmentTransactionKey"] == '':
        
            logData = {
                "requestType": "reverseAllTransactions",
            }

            reqData = {
                "notes": tx["notes"],
                "type": "FEE_ADJUSTMENT",
                "originalTransactionId": tx["transactionId"],
            }

            if tx["type"] == 'REPAYMENT':
                reqData["type"] = 'REPAYMENT_ADJUSTMENT'
            elif tx["type"] == 'INTEREST_APPLIED':
                reqData["type"] = 'INTEREST_APPLIED_ADJUSTMENT'
            elif tx["type"] == 'FEE_APPLIED':
                reqData["type"] = 'FEE_ADJUSTMENT'

            mambu.api(1, 'POST', '/loans/' + mamLoanId + '/transactions', {}, reqData, logData, vlsLoan)