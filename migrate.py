
import util, mambu, sql, clients, products, transactions, repayments
import csv, json, pyodbc
from datetime import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
import uuid

class VlsLoan(object):

    def __init__(self, vlsConNum):
        
        if util.config['mock']:
            vlsLoanDetails = util.read_mock_line('data/in/mock/vlsLoanDetails.csv')
            self.vlsLoanDetails = vlsLoanDetails

            vlsSchedules = util.read_mock_list('data/in/mock/vlsSchedules.csv')
            self.vlsSchedules = vlsSchedules

            vlsTransactions = util.read_mock_list('data/in/mock/vlsTransactions.csv')
            self.vlsTransactions = vlsTransactions

            vlsPayHols = util.read_mock_list('data/in/mock/vlsPayHols.csv')
            self.vlsPayHols = vlsPayHols
        
        else:
            
            schema = util.config['sql']['schema']
            
            vlsLoanDetails = sql.executeSql('exec [' + schema + '].[mam_LoanDetails] \'' + vlsConNum + '\'')[0]
            self.vlsLoanDetails = vlsLoanDetails

            vlsSchedules = sql.executeSql('exec [' + schema + '].[mam_Schedules] \'' + vlsConNum + '\'')
            self.vlsSchedules = vlsSchedules

            vlsTransactions = sql.executeSql('exec [' + schema + '].[mam_Transactions] \'' + vlsConNum + '\'')
            self.vlsTransactions = vlsTransactions

            vlsPayHols = sql.executeSql('exec [' + schema + '].[mam_PayHols] \'' + vlsConNum + '\'')
            self.vlsPayHols = vlsPayHols

        # placeholder - gets populated with succesful postClient response
        self.mamClient = None

        # placeholder - gets populated with succesful postLoan response
        self.mamLoan = None


def postClient(vlsLoan):

    # on success returns a mamClient
    
    vlsLoanDetails = vlsLoan.vlsLoanDetails

    mamClientId = vlsLoanDetails["sfPRN"]

    if util.config.get('environment') == 'test':
        firstName = vlsLoanDetails['vlsConNum']
        lastName = vlsLoanDetails['vlsConNum']
    else:
        firstName = vlsLoanDetails["sfFirstName"]
        lastName = vlsLoanDetails["sfLastName"]
    
    reqData = {

        "id": mamClientId,
        "firstName": firstName,
        "lastName": lastName,
        "assignedBranchKey": util.config["assignedBranchKey"],
        "notes": vlsLoanDetails.get("notes", None),
        "_Custom_Fields_Clients": {
            "SFAccountID_Clients": vlsLoanDetails['sfAccountId'],
            "Migration_Identifier_Clients": json.load(open('config/run_id.json'))['RUN_ID'],
        },
    }

    logData = {
        "mamClientId": mamClientId,
        "requestType": "postClient",
    }

    req = mambu.api(2, 'POST', '/clients', {}, reqData, logData, vlsLoan)

    err = None
    res = req.json()
    if req.status_code != 201:
        err, res = mambu.postClientErrorHandler(req, vlsLoan, mamClientId)

    return err, res


def postLoan(vlsLoan):

    # on success returns a mamLoan
    
    vlsLoanDetails = vlsLoan.vlsLoanDetails
    mamClient = vlsLoan.mamClient

    mamLoanId = util.genRandomId(vlsLoanDetails["sfLRN"])

    repaymentInstallments = len(vlsLoan.vlsSchedules)

    product = {}
    if vlsLoanDetails["gracePeriod"] == 0:
        product = util.config["BBIB"]
    else:
        product = util.config["BNPL"]

    assignedBranchKey = util.config["assignedBranchKey"]

    customFields = {
        "SFLoanID_Loan_Accounts": vlsLoanDetails["sfLoanId"],
        "SFLRN_Loan_Accounts": vlsLoanDetails["sfLRN"],
        "SFClientLoanNumber_Loan_Accounts": vlsLoanDetails["vlsConNum"],
        "Migration_Identifier": json.load(open('config/run_id.json'))['RUN_ID'],
    }

    # generic loan request body
    reqData = {
        "accountHolderKey": mamClient["encodedKey"],
        "accountHolderType": "CLIENT",
        "assignedBranchKey": assignedBranchKey,
        "disbursementDetails": {
            "expectedDisbursementDate": util.sqlDateToISO8601(vlsLoanDetails["expectedDisbursementDate"]),
            "fees": [],
        },
        "id": mamLoanId,
        "interestSettings": {
            "interestRate": vlsLoanDetails["interestRate"],
        },
        "loanAmount": vlsLoanDetails["loanAmount"],
        # "loanName": vlsLoanDetails['vlsConNum'],
        "loanName": mamLoanId,
        "notes": vlsLoanDetails.get("notes", None),
        "productTypeKey": product["productTypeKey"],
        "scheduleSettings": {
            "gracePeriod": vlsLoanDetails["gracePeriod"],
            "repaymentInstallments": repaymentInstallments,
        },
        "_Custom_Fields_Loan_Accounts": customFields,
    }


    # add fees to request body as necessary
    
    if vlsLoanDetails["subsidyAmount"] != None:
        reqData["disbursementDetails"]["fees"].append(
            {
                "amount": vlsLoanDetails["subsidyAmount"],
                "predefinedFeeEncodedKey": product["subsidyEncodedKey"],
            },
        )
        
    if vlsLoanDetails["deferredInterestAmount"] != None:
        reqData["disbursementDetails"]["fees"].append(
            {
                "amount": vlsLoanDetails["deferredInterestAmount"],
                "predefinedFeeEncodedKey": product["deferredInterestEncodedKey"],
            },
        )

    logData = {
        "mamLoanId": mamLoanId,
        "requestType": "postLoan",
    }

    req = mambu.api(2, 'POST', '/loans', {}, reqData, logData, vlsLoan)

    err = None
    res = req.json()
    if req.status_code != 201:
        err, res = mambu.postLoanErrorHandler(req, vlsLoan)
    
    return err, res


def patchSchedule(vlsLoan):

    # now that we have created the Loan Account in Mambu, we GET the schedule, edit it with VLS data, and then PATCH it, and then apply Payment Holidays
    vlsConNum = vlsLoan.vlsLoanDetails['vlsConNum']
    mamLoan = vlsLoan.mamLoan
    mamLoanId = mamLoan["id"]

    # GET the repayment schedule that Mambu created on postLoan
    mamSchedules = repayments.getRepayments(vlsLoan)

    # load the correct schedule (per VLS)
    vlsSchedules = vlsLoan.vlsSchedules

    # load the payment holidays
    vlsPayHols = vlsLoan.vlsPayHols
    
    # set generic log fields
    logData = {
        "vlsConNum": vlsConNum,
        "mamLoanId": mamLoanId,
    }
    
    # update the Mambu schedule with the correct (per VLS) dates and amount
    for i in range(len(mamSchedules)):
        mamSchedules[i]["principalDue"] = vlsSchedules[i]["principalDue"]
        if vlsSchedules[i]["principalDue"] != '0.00':
            mamSchedules[i]["dueDate"] =  vlsSchedules[i]["dueDate"]
        mamSchedules[i]["interestDue"] = vlsSchedules[i]["interestDue"]
    
    # PATCH the updated schedule into Mambu
    reqData = {"repayments": mamSchedules}

    logData['requestType'] = 'patchRepayments'

    req = mambu.api(1, 'PATCH', '/loans/' + mamLoanId + '/repayments', {}, reqData, logData, vlsLoan)

    if req.status_code != 200:
        return req.json(), None

    res = req.json()
    # apply payment holidays
    for vlsPayHol in vlsPayHols:

        start = datetime.strptime(vlsPayHol['Start'], '%Y-%m-%d')
        number = int(vlsPayHol['Number'])

        for n in range(number):
            
            # GET the PATCHed schedule
            mamSchedules = repayments.getRepayments(vlsLoan)
            # print('mamSchedules', mamSchedules)
            
            # find the mamSchedule dueDate immediately on or after dateadd(month, start, number)
            encodedKey = ''
            parentAccountKey = ''
            for i in range(len(mamSchedules)):
                
                if datetime.strptime(mamSchedules[i]['dueDate'], '%Y-%m-%d') < start + relativedelta(months=+n):
                    encodedKey = mamSchedules[i + 1]['encodedKey']
                    parentAccountKey = mamSchedules[i + 1]['parentAccountKey']
                    # print('found dueDate', datetime.strptime(mamSchedules[i + 1]['dueDate'], '%Y-%m-%d'))
                else:
                    break
            
            # print(encodedKey, parentAccountKey, dueDate)

            # PATCH a payment holiday to the mamSchedule of encodedKey, parentAccountKey
            logData['requestType'] = 'patchPaymentHolidays'

            reqData = {
                'repayments': [
                    {
                        'encodedKey': encodedKey,
                        'parentAccountKey': parentAccountKey,
                        'isPaymentHoliday': True
                    }
                ]
            }

            # print('reqData', reqData)

            req = mambu.api(1, 'PATCH', '/loans/' + mamLoanId + '/repayments', {}, reqData, logData, vlsLoan)
            
            if req.status_code != 200:
                
                if 'returnCode' in req.json():
                    # {'returnCode': 2436, 'returnStatus': 'PAYMENT_HOLIDAYS_ARE_NOT_ALLOWED_FOR_INSTALLMENTS_THAT_ALREADY_HAVE_PAYMENT_HOLIDAYS'}
                    # this happend if there are multiple payment holidays and they overlap
                    if req.json()['returnCode'] == 2436:
                        continue
                else:
                    return req.json(), None
            
            res = req.json()

    return None, res


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

                reqData["notes"] = json.dumps(thisTransaction)

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

                reqData["notes"] = json.dumps(thisTransaction)

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

                reqData["notes"] = json.dumps(thisTransaction)
                
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
                    reqData["notes"] = json.dumps(thisTransaction)

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
                        and json.loads(postedTransaction["notes"])["vlsTranType"] in ('CPEFTREC', 'DD-REC') \
                        and postedTransaction["adjustmentTransactionKey"] == '' \
                        and float(postedTransaction["amount"]) == chunk["amount"]:
                        
                        originalTransactionId = postedTransaction["transactionId"]
                        break

                reqData["originalTransactionId"] = originalTransactionId
                logData["originalTransactionId"] = originalTransactionId

                thisTransaction['ChunkNo'] = chunk["number"]
                thisTransaction['ChunkAmt'] = chunk["amount"]
                reqData["notes"] = json.dumps(thisTransaction)

                logData["chunk"] = chunk

                # if match was unsuccesful continue. this might be because eg. A000109271 the repayment was made after the balance was zero (hence wasn't migrated to Mambu)
                if originalTransactionId == '':
                    print("couldn't match adjustment to repayment:")
                    print(json.dumps(reqData))
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
                "notes": json.dumps(thisTransaction),
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


class MigrationStatus():

     def __init__(self, vlsConNum, mamClientId, mamLoanId, status, substatus, error_response, vlsLoan):
        self.vlsConNum = vlsConNum
        self.mamClientId = mamClientId
        self.mamLoanId = mamLoanId
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.status = status
        self.substatus = substatus
        self.error_response = error_response
        self.vlsLoan = vlsLoan


def fatal_error_details(e):
    msg = {'error_type': e.__class__.__name__, 'error_text': str(e)}
    print(msg)
    return msg


def migrate(vlsConNum):
        
        # get loan vls loan details from sql dw - includes high level loan details, schedule, and transactions
        # vlsLoan
        # -sfLoan (vlsConNum)
        # -vlsLoanDetails (vlsConNum, sfAccountId, sfFirstName, sfLastName, sfLoanId, sfLRN, ...loan details)
        # -vlsSchedules
        # -vlsTransactions
        try:
            vlsLoan = VlsLoan(vlsConNum)
        except Exception as e:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, None, None, 'fatal', 'initVlsLoan', fatal_error_details(e), None))
            return
        
        err = None

        # create client in mambu
        # note - if the client id already exists in mambu then instead of POSTing we GET its details
        try:
            err, res = postClient(vlsLoan)      # success returns a mamClient
        except Exception as e:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, None, None, 'fatal', 'postClient', fatal_error_details(e), vlsLoan))
            return
        
        if err != None:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, None, None, 'error', 'postClient', err, vlsLoan))
            return
        else:
            vlsLoan.mamClient = res

        # create loan account in mambu
        # note - if the loan id already exists in mambu then instead of POSTing we GET its details
        try:
            err, res = postLoan(vlsLoan)      # success returns a mamLoan
        except Exception as e:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], None, 'fatal', 'postLoan', fatal_error_details(e), vlsLoan))
            return
        
        if err != None:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], None,  'error', 'postLoan', err, vlsLoan))
            return
        else:
            vlsLoan.mamLoan = res
        
        # patch correct loan schedule to mambu
        try:
            err, _ = patchSchedule(vlsLoan)
        except Exception as e:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'],  'fatal', 'patchSchedule', fatal_error_details(e), vlsLoan))
            return

        if err != None:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'], 'error', 'patchSchedule', err, vlsLoan))
            return

        # approve and disburse the loan
        mambu.changeLoanStatus(vlsLoan, 'APPROVAL').json()
        mambu.changeLoanStatus(vlsLoan, 'DISBURSEMENT').json()

        # post transactions to mambu
        # postTransactions(vlsLoan)
        try:
            err, _ = postTransactions(vlsLoan)
        except Exception as e:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'],  'fatal', 'postTransactions', fatal_error_details(e), vlsLoan))
            return

        if err != None:
            sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'],  'error', 'postTransactions', err, vlsLoan))
            return

        # success
        sql.logMigrationStatus(MigrationStatus(vlsConNum, vlsLoan.mamClient['id'], vlsLoan.mamLoan['id'],  'success', None, None, vlsLoan))
        return


def generate_run_id():
    
    with open('config/run_id.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps({
            'RUN_ID': str(uuid.uuid4())
        }, indent='  '))


def migrate_concurrently(num_threads):
    
    generate_run_id()
    
    with open('data/log/apiLog.csv', 'a', encoding='utf-8') as f:
        f.write('')
    util.initTransactionLog()
    
    vlsConNums = []
    if util.config['mock']:
        vlsConNums = ['mock']
    else:
        from vlsConNums import goof as vlsConNums

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:

        future_to_url = {executor.submit(migrate, vlsConNum): vlsConNum for vlsConNum in vlsConNums}

        for future in concurrent.futures.as_completed(future_to_url):
            future.result()


if __name__ == "__main__":
    migrate_concurrently(128)