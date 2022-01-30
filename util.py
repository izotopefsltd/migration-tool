import csv, json, random, string
from datetime import datetime
from requests.auth import HTTPBasicAuth

config = json.load(open('config/config.json'))

def auth():
    return HTTPBasicAuth(config['user_name'], config['password'])

def base_url():
    return config['base_url']


def logAction(req, reqData, resData, logData):
    
    with open('data/log/actionLog.csv', 'a', newline='') as f:
        c = csv.writer(f, quoting=csv.QUOTE_ALL, quotechar='\'', escapechar='\\')

        out = [
            str(datetime.now()),
            req.status_code,
            logData["requestType"],
            req.url,
            json.dumps(logData),        # , indent=' '
            json.dumps(reqData),
            json.dumps(resData),
        ]

        try:
            c.writerow(out)
        except:
                print('coudn not write to csv')
                print(req.status_code)
                print(req.content)
                print(reqData)
                print('error:\t', 'logRequest')


def logRequest(req, reqData, resData, logData, vlsLoan):
    
    with open('data/log/apiLog.csv', 'a', newline='') as f:
        c = csv.writer(f, quoting=csv.QUOTE_ALL, quotechar='\'', escapechar='\\')

        if vlsLoan.mamLoan == None:
            vlsLoan.mamLoan = {'id': None}
        
        out = [
            str(datetime.now()),
            vlsLoan.vlsLoanDetails['vlsConNum'],
            vlsLoan.mamLoan['id'],
            req.status_code,
            logData["requestType"],
            req.url,
            json.dumps(logData),        # , indent=' '
            json.dumps(reqData),
            json.dumps(resData),
        ]

        try:
            c.writerow(out)
        except:
                print('coudn not write to csv')
                print(req.status_code)
                print(req.content)
                print(reqData)
                print('error:\t', 'logRequest')


def logTransaction(req, reqData, resData, logData):
    
    if config['logTransactions']:
    
        with open('data/log/transactionLog.csv', 'a', newline='') as f:
            c = csv.writer(f, quoting=csv.QUOTE_ALL, quotechar='\'', escapechar='\\')

            out = {
                "timestamp": str(datetime.now()),
                
                "vlsConNum": logData["vlsConNum"],
                
                "mamLoanId": logData["mamLoanId"],
                
                "mamTranType": logData["transaction"]["mamTranType"],
                "vlsTranType": logData["transaction"]["vlsTranType"],
                "vlsDate": logData["transaction"]["vlsDate"],
                "vlsAmount": logData["transaction"]["vlsAmount"],

                "chunkAmount": logData["chunk"]["amount"],
                "chunkNumber": logData["chunk"]["number"],

                "installmentNumber": logData["installment"]["number"],

                "transactionId": "",
                "originalTransactionId": logData["originalTransactionId"],
                "valueDate": "",
                "amount": "",
                "principalAmount": "",
                "interestAmount": "",
                "feesAmount": "",
                "arrearsPosition": "",
                "totalBalance": "",
                "principalBalance": "",

                # "logData": "",
                "reqURL": "",
                "reqData": "",
                "resData": "",
            }

            if logData["transaction"]["mamTranType"] in ['REPAYMENT', 'REPAYMENT_ADJUSTMENT', 'FEE', 'FEE_ADJUSTMENT']:

                out["valueDate"] = resData["valueDate"][:10]
                out["amount"] = resData["amount"]

                try:        # api v2
                    out["transactionId"] = resData["id"]
                    out["principalAmount"] = resData["affectedAmounts"]["principalAmount"]
                    out["interestAmount"] = resData["affectedAmounts"]["interestAmount"]
                    out["feesAmount"] = resData["affectedAmounts"]["feesAmount"]
                    out["arrearsPosition"] = resData["accountBalances"]["arrearsPosition"]
                    out["totalBalance"] = resData["accountBalances"]["totalBalance"]
                    out["principalBalance"] = resData["accountBalances"]["principalBalance"]
                except:     # api v1
                    out["transactionId"] = resData["transactionId"]
                    out["principalAmount"] = resData["principalPaid"]
                    out["interestAmount"] = resData["interestPaid"]
                    out["feesAmount"] = resData["feesPaid"]
                    out["arrearsPosition"] = resData["arrearsPosition"]
                    out["totalBalance"] = resData["balance"]
                    out["principalBalance"] = resData["principalBalance"]

                # out["logData"] = json.dumps(logData)
                out["reqURL"] = req.url.replace(base_url(),'')
                out["reqData"] = json.dumps(reqData)
                out["resData"] = json.dumps(resData)

            foo = []
            for value in out.values():
                foo.append(value)

            try:
                c.writerow(foo)
            except:
                    print('something went wrong')
                    print(reqData)
                    print('error:\t', 'logRequest')


# def randomWord(minLength, maxLength):

#     r = RandomWords()
#     return r.get_random_word(minLength=minLength, maxLength=maxLength)

def randomString(length):

    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def genRandomId(Id):

    if json.load(open('config/config.json'))["environment"] != 'prod':
        return randomString(8)
        # return randomWord(6, 12)
    else:
        return Id

def sqlDateToISO8601(sqlDate):
    y = sqlDate[:4]
    m = sqlDate[5:7]
    d = sqlDate[8:]

    # print(y, m, d)

    return datetime(int(y), int(m), int(d)).astimezone().isoformat()


def initTransactionLog():

    with open('data/log/transactionLog.csv', 'w', encoding='utf-8', newline='') as f:

        c = csv.writer(f, quoting=csv.QUOTE_ALL, quotechar='\'', escapechar='\\')
        
        headers = [
            'timestamp',
            'vlsConNum',
            'mamLoanId',
            'mamTranType',
            'vlsTranType',
            'vlsDate',
            'vlsAmount',
            'chunkAmount',
            'chunkNumber',
            'installmentNumber',
            'transactionId',
            'originalTransactionId',
            'valueDate',
            'amount',
            'principalAmount',
            'interestAmount',
            'feesAmount',
            'arrearsPosition',
            'totalBalance',
            'principalBalance',
            # 'logData',
            'reqURL',
            'reqData',
            'resData',
        ]
        
        c.writerow(headers)


def read_mock_line(file_path):
    
    d = {}
    with open(file_path) as f:
        
        from csv import DictReader
        c = DictReader(f)

        for m in map(dict, c):
            d = m

    for k, v in d.items():
        if v == 'NULL':
            d[k] = None

    return d
        

def read_mock_list(file_path):
    
    with open(file_path) as f:
        
        from csv import DictReader
        c = DictReader(f)

        l = []
        for m in map(dict, c):
            l.append(m)

    
    return l