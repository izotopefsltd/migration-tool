from util import config
import json, csv, pyodbc
from datetime import datetime

schema = config['sql']['schema']
server = config['sql']['server']
database = config['sql']['database']
username = config['sql']['username']
password = config['sql']['password']
driver= config['sql']['driver']

# pyodbc.OperationalError: ('08S01', '[08S01] [Microsoft][ODBC Driver 13 for SQL Server]TCP Provider: A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond.\r\n (10060) (SQLSetConnectAttr(SQL_ATTR_AUTOCOMMIT)); [08S01] [Microsoft][ODBC Driver 13 for SQL Server]Communication link failure (10060)')

def executeSql(qry):
    
    conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor().execute(qry)

    columns = [column[0] for column in cursor.description]

    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    
    return results


def logMigrationStatus(s):
    
    vlsConNum = s.vlsConNum
    mamClientId = s.mamClientId
    mamLoanId = s.mamLoanId
    timestamp = s.timestamp
    status = s.status
    substatus = s.substatus

    error_response = json.dumps(s.error_response, default=str).replace("'", "''")
    
    vlsLoanDetails = None
    vlsSchedules = None
    vlsTransactions = None
    mamClient = None
    mamLoan = None

    env = config['environment'].capitalize()

    if status != 'success':
        try:
            vlsLoanDetails = json.dumps(s.vlsLoan.vlsLoanDetails, default=str).replace("'", "''")
        except AttributeError:
            vlsLoanDetails = None
        
        try:
            vlsSchedules = json.dumps(s.vlsLoan.vlsSchedules, default=str).replace("'", "''")
        except AttributeError:
            vlsSchedules = None

        try:
            vlsTransactions = json.dumps(s.vlsLoan.vlsTransactions, default=str).replace("'", "''")
        except AttributeError:
            vlsTransactions = None

        try:
            mamClient = json.dumps(s.vlsLoan.mamClient, default=str).replace("'", "''")
        except AttributeError:
            mamClient = None

        try:
            mamLoan = json.dumps(s.vlsLoan.mamLoan, default=str).replace("'", "''")
        except AttributeError:
            mamLoan = None


    RUN_ID = json.load(open('config/run_id.json'))['RUN_ID']
    
    qry = f"insert into [{schema}].[w_mam_MigrationStatus] values ('{timestamp}', '{vlsConNum}', '{mamClientId}', '{mamLoanId}', '{status}', '{substatus}', '{error_response}', '{vlsLoanDetails}', '{vlsSchedules}', '{vlsTransactions}', '{mamClient}', '{mamLoan}', '{RUN_ID}', '{env}')"


    with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        # print(qry)
        c = conn.cursor()
        try:
            c.execute(qry)
        except Exception as e:
            with open('data/log/migrationStatusErrorLog.txt', 'a', encoding='utf-8') as f:
                f.write('could not execute query in logMigrationStatus:\t', qry) 
                f.write(str(e))
            
            print('******************************************************')
            print('could not execute query in logMigrationStatus:\t', qry)
            print(str(e))
            print('******************************************************')
            


def logRequest(req, reqData, resData, logData, vlsLoan):

    if vlsLoan.mamClient == None:
        vlsLoan.mamClient = {'id': None}

    if vlsLoan.mamLoan == None:
        vlsLoan.mamLoan = {'id': None}
    
    timestamp = str(datetime.now())
    vlsConNum = vlsLoan.vlsLoanDetails['vlsConNum']
    mamClientId = vlsLoan.mamClient['id']
    mamLoanId = vlsLoan.mamLoan['id']
    statusCode = req.status_code
    requestType = logData["requestType"]
    url = req.url
    logData = json.dumps(logData).replace("'", "''")        # , indent=' '
    reqData = json.dumps(reqData).replace("'", "''")
    resData = json.dumps(resData).replace("'", "''")
    env = config['environment'].capitalize()

    RUN_ID = json.load(open('config/run_id.json'))['RUN_ID']

    qry = f"insert into [{schema}].[w_mam_ApiLog] values ('{RUN_ID}', '{timestamp}', '{vlsConNum}', '{mamClientId}', '{mamLoanId}', '{statusCode}', '{requestType}', '{url}', '{logData}', '{reqData}', '{resData}', '{env}')"

    with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        # print(qry)
        c = conn.cursor()
        try:
            c.execute(qry)
        except Exception as e:
            print(str(e))
            print('could not execute query in logRequest:\t', qry)