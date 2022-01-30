from requests import get, post, patch
from mambu import auth, base_url, api
from json import dumps, loads
from sql import executeSql, logMigrationStatus
from schedUpdates import schedUpdates
from migrate import VlsLoan, MigrationStatus, fatal_error_details

import mambu, util, transactions

def getMamTrans(mamLoanId):
    
    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/vnd.mambu.v2+json',
    }
    url = base_url + '/loans/' + mamLoanId + '/transactions?detailsLevel=FULL&limit=1000'

    req = get(url=url, auth=auth, headers=headers)
    
    return req.json()


def revMamTrans(mamLoanId, trans):

    headers = {'Content-Type': 'application/json'}
    url = base_url + '/loans/' + mamLoanId + '/transactions'
    
    tranTypes = ['REPAYMENT', 'INTEREST_APPLIED', 'FEE_APPLIED']

    for tran in trans:
        
        if tran.get('type') in tranTypes and tran.get('adjustmentTransactionKey', '') == '':

            reqData = {
              'originalTransactionId': tran['id'],
              'notes': tran.get('notes', ''),
            }

            if tran.get('type') == 'REPAYMENT':
                reqData['type'] = 'REPAYMENT_ADJUSTMENT'
            elif tran.get('type') == 'INTEREST_APPLIED':
                reqData['type'] = 'INTEREST_APPLIED_ADJUSTMENT'
            elif tran.get('type') == 'FEE_APPLIED':
                reqData['type'] = 'FEE_ADJUSTMENT'
            
            req = post(url=url, headers=headers, data=dumps(reqData), auth=auth)
            print(req.status_code, reqData)


def changeStatus(mamLoanId, status):

    headers = {'Content-Type': 'application/json'}
    url = base_url + '/loans/' + mamLoanId + '/transactions'
    reqData = {'type': status}

    req = post(url=url, headers=headers, data=dumps(reqData), auth=auth)
    print(req.status_code, reqData)


def unapprove(mamLoanId):

    headers = {'Content-Type': 'application/json'}
    url = base_url + '/loans/' + mamLoanId + '/transactions'
    reqData = {'type': 'UNDO_APPROVAL'}

    req = post(url=url, headers=headers, data=dumps(reqData), auth=auth)
    print(req.status_code, reqData)


def getScheds(mamLoanId):
    
    headers = {'Content-Type': 'application/json'}
    url = base_url + '/loans/' + mamLoanId + '/transactions'
    reqParams = {'limit': '1000'}

    offset = 0
    results = []
    url = base_url + '/loans/' + mamLoanId + '/repayments'

    while True:
        reqParams['offset'] = str(offset)
        req = get(url=url, headers=headers, params=reqParams, auth=auth)
        if req.status_code not in [200, 201]:
            print(mamLoanId, req.text)
            quit()

        if len(req.json()) > 0:
            for result in req.json():
                results.append(result)
            offset += int(reqParams['limit'])
            continue
        else:
            break

    repayments = []
    for r in results:
        repayment = {
                'encodedKey': r['encodedKey'],
                'principalDue': r['principalDue'],
                'dueDate': r['dueDate'][:10],
                'interestDue': r['interestDue'],
                'parentAccountKey': r['parentAccountKey'],
            }
        repayments.append(repayment)
    
    return repayments


def update(mamLoanId, data, sched):

    for sched in sched:
        if sched['dueDate'] == data['oldDate']:
            sched['dueDate'] = data['newDate']

    return scheds


def patchScheds(mamLoanId, scheds):

    headers = {'Content-Type': 'application/json'}
    url = base_url + '/loans/' + mamLoanId + '/repayments'
    reqData = {'repayments': scheds}

    req = patch(url=url, headers=headers, data=dumps(reqData), auth=auth)
    if req.status_code not in [200, 201]:
        print(mamLoanId, req.text)
        quit()


def getMamClient(mamClientId):

    headers = {'Content-Type': 'application/json'}
    url = base_url + '/clients/' + mamClientId

    req = get(url, auth=auth, headers=headers)

    return req.json()


def getMamLoan(mamLoanId):

    headers = {'Content-Type': 'application/json'}
    url = base_url + '/loans/' + mamLoanId

    req = get(url, auth=auth, headers=headers)

    return req.json()


def adjTrans(trans):

    for tran in trans:
        if tran['date'][:10] == '2021-03-15':
            tran['date'] = '2021-03-25'
            print(trans)

    return trans


def postTransactions(vlsLoan):

    mamLoan = vlsLoan.mamLoan
    mamLoanId = mamLoan["id"]
    vlsConNum = vlsLoan.vlsLoanDetails['vlsConNum']
    vlsTransactions = vlsLoan.vlsTransactions
    vlsLoanDetails = vlsLoan.vlsLoanDetails
    
    totalBalance = float(mamLoan["loanAmount"])

    # iterate through the VLS transactions, executing the necessary Mambu API calls
    installments = {}
    installmentNumber = vlsLoan.vlsLoanDetails['gracePeriod']               # {"errors": [{"errorCode": 266, "errorReason": "PURE_GRACE_INSTALLMENT_ARE_NOT_EDITABLE"}]}
    postedTransactions = []
    unpostedFees = 0.0     # keeps track of any VLS fees not applied in Mambu because of: {'errors': [{'errorCode': 3753, 'errorReason': 'CANNOT_APPLY_FEE_ON_PAID_INSTALLMENT'}]}

    err = None
    for i in range(len(vlsTransactions)):

        thisTransaction = {
            "mamTranType": vlsTransactions[i]["mamTranType"],
            "mamChannel": vlsTransactions[i]["mamChannel"],
            "vlsAccCode": vlsTransactions[i]["vlsAccCode"],
            "vlsTranType": vlsTransactions[i]["vlsTranType"],
            "vlsTranNo": vlsTransactions[i]["vlsTranNo"],
            "vlsDate": vlsTransactions[i]["vlsDate"],
            "vlsAmount":  vlsTransactions[i]["vlsAmount"],
            "ChunkNo": 1,
            "ChunkAmt": None,
        }

        customFields = {
            "vlsAccCode_Transactions": vlsTransactions[i]["vlsAccCode"],
            "vlsTranType_Transactions": vlsTransactions[i]["vlsTranType"],
            "vlsTranNo_Transactions": vlsTransactions[i]["vlsTranNo"],
            "vlsDate_Transactions": vlsTransactions[i]["vlsDate"],
            "vlsAmount_Transactions": vlsTransactions[i]["vlsAmount"],
        }
        
        logData = {
            "mamLoanId": mamLoanId,
            "vlsConNum": vlsConNum,
            "transaction": thisTransaction,
            "originalTransactionId": "",
            "installment": {
                "number": "",
                "date": "",
                "amount": "",
                "adjusted": False,
            },
            "chunk": {
                "number": "",
                "amount": "",
            },
        }

        
        # VLS Contractual Instalment Due - requires no API call, but indicates that we need to increment our installmentNumber counter
        if thisTransaction["mamTranType"] == 'CONTRACTUAL_INSTALMENT_DUE':
            
            installmentNumber += 1

            installment = {
                "number": installmentNumber,
                "date": thisTransaction["vlsDate"],
                "amount": float(thisTransaction["vlsAmount"]),
                "adjusted": False,
            }

            installments[installmentNumber] = installment

            logData["installment"] = installment
            logData["requestType"] = None

            util.logTransaction(None, {}, {}, logData)
        
        
        # a customer payment, be it by DD, Cash, Card, or Transfer
        # some checks are required when a proposed REPAYMENTs
        if thisTransaction["mamTranType"] == 'REPAYMENT':
            
            transactionAmount = -1.0 * float(thisTransaction["vlsAmount"])
            
            # check if this REPAYMENT is greater than the totalBalance outstanding
            if transactionAmount > totalBalance:

                # POST the OVERPAYMENT to Mambu as a normal REPAYMENT (flagged with channel =  OVERPAYMENT)
                overpayment = round(transactionAmount - totalBalance, 2)

                logData["requestType"] = 'postOverpayment'

                reqData = {
                    "type": 'REPAYMENT',
                    "valueDate": util.sqlDateToISO8601(thisTransaction["vlsDate"]),
                    "amount": overpayment,
                    "transactionDetails": {
                        "transactionChannelId": 'Overpayment',
                    },
                    "_Custom_Fields_Transactions": customFields,
                }

                reqData["notes"] = dumps(thisTransaction)

                ##############################
                req = mambu.api(2, 'POST', '/loans/' + mamLoanId + '/repayment-transactions', {}, reqData, logData, vlsLoan)
                    
                if req.status_code not in [200, 201]:
                    # mambu.postTransactionsErrorHandler(req, mamLoanId)            # turned this off for now
                    return req.json(), None
                
                resData = req.json()

                totalBalance = float(resData["accountBalances"]["totalBalance"])
                originalTransactionId = resData["id"]

                util.logTransaction(req, reqData, resData, logData)
                ##############################


                # POST a REPAYMENT_ADJUSTMENT to Mambu to reverse out the OVERPAYMENT
                logData["requestType"] = 'postOverpaymentAdjustment'

                reqData = {
                    "type": 'REPAYMENT_ADJUSTMENT',
                    "originalTransactionId": originalTransactionId,
                }

                ##############################
                req = mambu.api(1, 'POST', '/loans/' + mamLoanId + '/transactions', {}, reqData, logData, vlsLoan)

                if req.status_code not in [200, 201]:
                    # mambu.postTransactionsErrorHandler(req, mamLoanId)            # turned this off for now
                    return req.json(), None

                resData = req.json()

                totalBalance = float(resData["balance"])

                util.logTransaction(req, reqData, resData, logData)
                ##############################


                # POST a REPAYMENT to Mambu to clear the totalBalance to zero
                logData["requestType"] = 'postSettlementRepayment'

                reqData = {
                    "type": 'REPAYMENT',
                    "valueDate": util.sqlDateToISO8601(thisTransaction["vlsDate"]),
                    "amount": totalBalance,
                    "transactionDetails": {
                        "transactionChannelId": thisTransaction["mamChannel"],
                    },
                    "_Custom_Fields_Transactions": customFields,
                }

                reqData["notes"] = dumps(thisTransaction)

                ##############################
                req = mambu.api(2, 'POST', '/loans/' + mamLoanId + '/repayment-transactions', {}, reqData, logData, vlsLoan)
                    
                if req.status_code not in [200, 201]:
                    # mambu.postTransactionsErrorHandler(req, mamLoanId)            # turned this off for now
                    return req.json(), None
                
                resData = req.json()

                totalBalance = float(resData["accountBalances"]["totalBalance"])
                originalTransactionId = resData["id"]

                util.logTransaction(req, reqData, resData, logData)
                ##############################


                # create an item in the action_log.csv to notify Operations of the OVERPAYMENT 
                util.logAction(req, reqData, resData, logData)

                # ignore all subsequent transactions
                return None, None

            else:
                
                
                logData["requestType"] = 'postRepayment'

                reqData = {
                    "type": 'REPAYMENT',
                    "valueDate": util.sqlDateToISO8601(thisTransaction["vlsDate"]),
                    "transactionDetails": {
                        "transactionChannelId": thisTransaction["mamChannel"],
                    },
                    "_Custom_Fields_Transactions": customFields,
                }

                reqData["notes"] = dumps(thisTransaction)
                
                # iteratively post REPAYMENTS in chunks of installmentAmount, beginning with the current installment and iterating backwards if necessary, until the whole payment has been posted
                chunks = []
                thisInstallmentNumber = installmentNumber
                while transactionAmount > 0:
                    
                    if thisInstallmentNumber > vlsLoan.vlsLoanDetails['gracePeriod']:
                        
                        # we try to chunk up the REPAYMENT into chunks determined by the past installments
                        postingAmount = min(installments[thisInstallmentNumber]["amount"], transactionAmount)
                        thisInstallmentNumber -= 1

                    else:
                        
                        if installmentNumber > vlsLoan.vlsLoanDetails['gracePeriod']:
                            # rarely the REPAYMENT will include payment for an installment due in the future (A000028116) so as an expect we use the current instalment amount as a proxy for the next installment amount
                            postingAmount = min(installments[installmentNumber]["amount"], transactionAmount)
                        
                        else:
                            
                            # thisInstallmentNumber <= gracePeriod and installmentNumber <= gracePeriod
                            # meaning we have allocated repayments to all available historic instalments
                            # so there is no installment available to chunk the repayment against (probably a fee paid along with the first (bounced) DD)
                            # eg. A000060396 
                            # also early settlements may occur before the first installment is due, so we have no installment to associate the payment with
                            postingAmount = transactionAmount
                    
                    chunk = {
                        "number": len(chunks) + 1,
                        "amount": postingAmount
                    }
                    
                    chunks.append(chunk)
                    
                    transactionAmount = round(transactionAmount - postingAmount, 2)

                # reverse through the chunks so that smallest is posted first
                for chunk in reversed(chunks):
                
                    reqData["amount"] = chunk["amount"]
                    logData["chunk"] = chunk

                    thisTransaction['ChunkNo'] = chunk["number"]
                    thisTransaction['ChunkAmt'] = chunk["amount"]
                    reqData["notes"] = dumps(thisTransaction)

                    req = mambu.api(2, 'POST', '/loans/' + mamLoanId + '/repayment-transactions', {}, reqData, logData, vlsLoan)
                        
                    if req.status_code not in [200, 201]:
                        # mambu.postTransactionsErrorHandler(req, mamLoanId)            # turned this off for now
                        return req.json(), None
                    
                    resData = req.json()

                    totalBalance = float(resData["accountBalances"]["totalBalance"])

                    util.logTransaction(req, reqData, resData, logData)

        
        # a bounced payment
        if thisTransaction["mamTranType"] == 'REPAYMENT_ADJUSTMENT':

            logData["requestType"] = 'postRepaymentAdjustment'

            reqData = {
                "type": 'REPAYMENT_ADJUSTMENT',
                "originalTransactionId": None,
            }

            # chunk up the REPAYMENT_ADJUSTMENT into chunks determined by historic CONTRACTUAL_INSTALMENT_DUEs
            transactionAmount = float(thisTransaction["vlsAmount"])

            chunks = []
            thisInstallmentNumber = installmentNumber
            while transactionAmount > 0:

                if thisInstallmentNumber > vlsLoan.vlsLoanDetails['gracePeriod']:

                    postingAmount = min(installments[thisInstallmentNumber]["amount"], transactionAmount)
                    thisInstallmentNumber -= 1
                
                else:

                    # eg. A000069911 - bounced DD that can't be chunked
                    postingAmount = transactionAmount
                
                chunk = {
                    "number": len(chunks) + 1,
                    "amount": postingAmount,
                }
                
                chunks.append(chunk)
                
                transactionAmount = round(transactionAmount - postingAmount, 2)
            
            # print(chunks)
            
            # reverse through the chunks so that smallest is posted first
            for chunk in reversed(chunks):
 
                # get all the Transactions already posted to Mambu
                transactionTypes = ['REPAYMENT', 'FEE']
                postedTransactions = transactions.getTransactions(vlsLoan, transactionTypes)
                
                # iterate (backwards in time) through the posted Transactions, and match the chunk to a historic 'Electronic Funds Transfer Receipt' or 'Direct Debit Received'
                originalTransactionId = ''
                for postedTransaction in postedTransactions:
                    if postedTransaction["type"] == 'REPAYMENT' \
                        and loads(postedTransaction["notes"])["vlsTranType"] in ('CPEFTREC', 'DD-REC') \
                        and postedTransaction["adjustmentTransactionKey"] == '' \
                        and float(postedTransaction["amount"]) == chunk["amount"]:
                        
                        originalTransactionId = postedTransaction["transactionId"]
                        break

                reqData["originalTransactionId"] = originalTransactionId
                logData["originalTransactionId"] = originalTransactionId

                thisTransaction['ChunkNo'] = chunk["number"]
                thisTransaction['ChunkAmt'] = chunk["amount"]
                reqData["notes"] = dumps(thisTransaction)

                logData["chunk"] = chunk

                # if match was unsuccesful continue. this might be because eg. A000109271 the repayment was made after the balance was zero (hence wasn't migrated to Mambu)
                if originalTransactionId == '':
                    # print("couldn't match adjustment to repayment:")
                    print(dumps(reqData))
                    continue
                
                req = mambu.api(1, 'POST', '/loans/' + mamLoanId + '/transactions', {}, reqData, logData, vlsLoan)

                if req.status_code not in [200, 201]:
                    # mambu.postTransactionsErrorHandler(req, mamLoanId)            # turned this off for now
                    return req.json(), None

                resData = req.json()

                totalBalance = float(resData["balance"])

                util.logTransaction(req, reqData, resData, logData)
        
        
        # a fee applied to the account
        if thisTransaction["mamTranType"] == 'FEE':
            
            # skip Transfer To Collection Ledger transaction if the termination interest is zero
            if thisTransaction['vlsTranType'] == 'ACCOLTER' and vlsLoanDetails['terminationInterestAmount'] == None:
                continue
            
            logData["requestType"] = 'postFee'
            
            reqData = {
                "valueDate": util.sqlDateToISO8601(thisTransaction["vlsDate"]),
                "installmentNumber": installmentNumber,
            }
            
            product = {}
            if vlsLoan.vlsLoanDetails["gracePeriod"] == 0:
                product = util.config["BBIB"]
            else:
                product = util.config["BNPL"]
            
            if thisTransaction['vlsTranType'] == 'ACCOLTER':
                reqData['predefinedFeeKey'] = product['terminationInterestEncodedKey']
                reqData['amount'] = vlsLoanDetails['terminationInterestAmount']
                reqData['installmentNumber'] = reqData['installmentNumber'] + 1
            elif float(thisTransaction["vlsAmount"]) == 12:
                reqData['predefinedFeeKey'] = product['arrearsFeeEncodedKey']
            elif float(thisTransaction["vlsAmount"]) == 25:
                reqData['predefinedFeeKey'] = product['terminationFeeEncodedKey']
            else:
                reqData['amount'] = thisTransaction["vlsAmount"]
            
            req = mambu.api(2, 'POST', '/loans/' + mamLoanId + '/fee-transactions', {}, reqData, logData, vlsLoan)

            if req.status_code not in [200, 201]:
                err, req = mambu.postFeeErrorHandler(req, vlsLoan)
                if err != None:
                    return err, None
                else:
                    # we have a fee that couldn't be posted, so make a note of it and move on to next transaction
                    unpostedFees += float(thisTransaction["vlsAmount"])
                    continue

            resData = req.json()

            totalBalance = float(resData["accountBalances"]["totalBalance"])

            util.logTransaction(req, reqData, resData, logData)

            # prevFee = resData
        
        
        # undo fee applied to the account
        if thisTransaction["mamTranType"] == 'FEE_ADJUSTMENT':
            
            # search through posted fees for the most recent one that hasn't been adjusted
            originalTransactionId = None
            fees = transactions.getTransactions(vlsLoan, ['FEE_APPLIED'])
            for fee in fees:
                if fee['adjustmentTransactionKey'] == '':
                    originalTransactionId = fee['transactionId']
                    break
            
            # handle the case where there are no unadjusted fees left
            if originalTransactionId == None:
                # keeps track of any VLS fees not applied in Mambu because of: {'errors': [{'errorCode': 3753, 'errorReason': 'CANNOT_APPLY_FEE_ON_PAID_INSTALLMENT'}]}
                unpostedFees += float(thisTransaction["vlsAmount"])
                continue

            logData["requestType"] = 'postFeeAdjustment'
            logData["originalTransactionId"] = originalTransactionId

            reqData = {
                "type": 'FEE_ADJUSTMENT',
                "originalTransactionId": originalTransactionId,
            }

            req = mambu.api(1, 'POST', '/loans/' + mamLoanId + '/transactions', {}, reqData, logData, vlsLoan)

            if req.status_code not in [200, 201]:
                # mambu.postTransactionsErrorHandler(req, mamLoanId)            # turned this off for now
                return req.json(), None

            resData = req.json()

            totalBalance = float(resData["balance"])

            util.logTransaction(req, reqData, resData, logData)

    return None, None


if __name__ == '__main__':
    
    for mamLoanId, data in schedUpdates.items():
        
        mamTrans = getMamTrans(mamLoanId)
        revMamTrans(mamLoanId, mamTrans)

        changeStatus(mamLoanId, 'DISBURSMENT_ADJUSTMENT')
        changeStatus(mamLoanId, 'UNDO_APPROVAL')
        
        scheds = getScheds(mamLoanId)
        scheds = update(mamLoanId, data, scheds)

        patchScheds(mamLoanId, scheds)

        changeStatus(mamLoanId, 'APPROVAL')
        changeStatus(mamLoanId, 'DISBURSEMENT')

        vlsLoan = VlsLoan(data['vlsConNum'])
        vlsLoan.mamClient = getMamClient(data['mamClientId'])
        vlsLoan.mamLoan = getMamLoan(mamLoanId)

        # postTransactions
        try:
            err, _ = postTransactions(vlsLoan)
        except Exception as e:
            logMigrationStatus(MigrationStatus(data['vlsConNum'], vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'],  'fatal', 'postTransactions', fatal_error_details(e), vlsLoan))
            # return

        if err != None:
            logMigrationStatus(MigrationStatus(data['vlsConNum'], vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'],  'error', 'postTransactions', err, vlsLoan))
            # return