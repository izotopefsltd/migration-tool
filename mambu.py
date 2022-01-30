import util, sql, transactions
import requests, json, sys, time

auth = util.auth()
base_url = util.base_url()

def api(version, method, endpoint, reqParams, reqData, logData, vlsLoan):

    headers = {"Content-Type": "application/json"}

    if version == 2:
        headers["Accept"] = "application/vnd.mambu.v2+json"

    req = None
    if method in ['POST', 'PATCH']:
        req = requests.request(method=method, url=base_url + endpoint, auth=auth, headers=headers, data=json.dumps(reqData))
    elif method in ['GET']:
        req = requests.request(method=method, url=base_url + endpoint, auth=auth, headers=headers, params=reqParams)
    elif method in ['DELETE']:
        req = requests.request(method=method, url=base_url + endpoint, auth=auth, headers=headers)

    time.sleep(0.005)

    resData = {}
    try:
        resData = req.json()
    except:
        resData = {"req.text": req.text}

    
    if util.config['logRequest_to']['file']:
        util.logRequest(req, reqData, resData, logData, vlsLoan)
    
    if util.config['logRequest_to']['dw']:
        sql.logRequest(req, reqData, resData, logData, vlsLoan)

    return req


def getUrl(version, endpoint):

    headers = {"Content-Type": "application/json"}

    if version == 2:
        headers["Accept"] = "application/vnd.mambu.v2+json"

    req = requests.request(method='GET', url=base_url + endpoint, auth=auth, headers=headers)

    return req


def postClientErrorHandler(req, vlsLoan, mamClientId):
    
    errs = {}
    try:
        errs = req.json()["errors"]
    except:
        return req.json(), None

    for err in errs:
        if err["errorCode"] == 316:
            # {"errors": [{"errorCode": 316, "errorReason": "CLIENT_ID_ALREADY_IN_USE"}]}
            # if client already exists, then instead of POSTing it, we GET it
            logData = {
                "requestType": "getClient",
            }
            req2 = api(2, 'GET', '/clients/' + mamClientId, {}, {}, logData, vlsLoan)
            return None, req2.json()
        else:
            # return error, success
            return req.json(), None


def postLoanErrorHandler(req, vlsLoan):
    
    mamLoanId = vlsLoan.mamLoan['id']
    
    errs = {}
    try:
        errs = req.json()["errors"]
    except:
        return req.json(), None

    for err in errs:
        if err["errorCode"] == 411:
            # {"errors": [{"errorCode": 411, "errorReason": "ACCOUNT_ID_ALREADY_IN_USE"}]}
            # if loan already exists, then instead of POSTing it, we GET it
            logData = {
                "requestType": "getLoan",
            }
            req = api(2, 'GET', '/loans/' + mamLoanId, {}, {}, logData, vlsLoan)

            # check the existing loan's status. if it's WITHDRAWN or REJECTED then undo
            mamLoan = req.json()

            if mamLoan["accountState"] == 'CLOSED':
                if mamLoan["accountSubState"] == 'WITHDRAWN':
                    req = changeLoanStatus(mamLoan["id"], 'UNDO_WITHDRAWN')

            if mamLoan["accountState"] == 'CLOSED_REJECTED':
                req = changeLoanStatus(mamLoan["id"], 'UNDO_REJECT')

            return None, req.json()
        else:
            return req.json(), None


def postFeeErrorHandler(req, vlsLoan):
    
    errs = {}
    try:
        errs = req.json()["errors"]
    except:
        return req.json(), None

    for err in errs:
        if err["errorCode"] == 3753:
            # {'errors': [{'errorCode': 3753, 'errorReason': 'CANNOT_APPLY_FEE_ON_PAID_INSTALLMENT'}]}
            # ignore this error as it's dealt with by the calling funtion (postFee routine)
            return None, req.json()
        else:
            return req.json(), None


def postTransactionsErrorHandler(req, vlsLoan):

    # '{"errors": [{"errorCode": 3620, "errorReason": "CANNOT_APPLY_REPAYMENT_ON_ZERO_BALANCE_ACCOUNT"}]}'
            
    # on error we reverse (adjust) all the transactions, undisburse and unapprove
    
    transactions.reverseAllTransactions(vlsLoan)
    changeLoanStatus(vlsLoan, 'DISBURSMENT_ADJUSTMENT')
    changeLoanStatus(vlsLoan, 'UNDO_APPROVAL')
    changeLoanStatus(vlsLoan, 'WITHDRAW')


def changeLoanStatus(vlsLoan, loanStatus):

    mamLoanId = vlsLoan.mamLoan['id']
    
    if loanStatus == 'DISBURSMENT_ADJUSTMENT':
        logData = {"requestType": "undisburseLoan"}
    elif loanStatus == 'UNDO_APPROVAL':
        logData = {"requestType": "unapproveLoan"}
    elif loanStatus == 'WITHDRAW':
        logData = {"requestType": "withdrawLoan"}
    elif loanStatus == 'UNDO_WITHDRAWN':
        logData = {"requestType": "undoWithdraw"}
    elif loanStatus == 'UNDO_REJECT':
        logData = {"requestType": "undoReject"}
    elif loanStatus == 'APPROVAL':
        logData = {"requestType": "approveLoan"}
    elif loanStatus == 'DISBURSEMENT':
        logData = {"requestType": "disburseLoan"}
    elif loanStatus == 'REJECT ':
        logData = {"requestType": "rejectLoan"}
    elif loanStatus == 'WITHDRAW':
        logData = {"requestType": "withdrawLoan"}
    
    reqData = {"type": loanStatus}
    return api(1, 'POST', '/loans/' + mamLoanId + '/transactions', {}, reqData, logData, vlsLoan)
