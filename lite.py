import sqlite3

LITE_DB_FILE = 'data/log/actionLog.db'

def createLogTable():
  
  with sqlite3.connect(LITE_DB_FILE) as conn:
    c = conn.cursor()

    qry = "drop table if exists api_log;"
    c.execute(qry)

    qry = "create table api_log (runId text, timestamp text, vlsConNum text, mamLoanId text, status_code integer, requestType text, url text, logData blob, reqData blob, resData blob);"
    c.execute(qry)

    qry = "create index idx_vlsConNum on api_log (vlsConNum);"
    c.execute(qry)


def selectAll():

  with sqlite3.connect(LITE_DB_FILE) as conn:
      c = conn.cursor()

      qry = "select * from api_log;"
      c.execute(qry)

      rows = c.fetchall()
      for row in rows:
        print(row)


if __name__ == '__main__':
  # createLogTable()
  selectAll()