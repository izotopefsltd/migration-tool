from util import auth, base_url
from requests import get
from json import dumps, loads


def get_loans():

  headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/vnd.mambu.v2+json',
  }
  
  offset = 0
  limit = 1000
  params = {
    'offset': offset,
    'limit': limit,
    'detailsLevel': 'FULL'
  }

  with open('data/get/getLoans.json', 'w', encoding='utf-8') as f:
    f.write('')
  
  loans = []
  while True:
    
    r = get(auth=auth(), url=base_url() + '/loans', headers=headers, params=params)

    if len(r.json()) > 0:
      
      for loan in r.json():
        loans.append(loan)
      
      offset += limit
      params['offset'] = offset

    else:

      break

  with open('data/get/getLoans.json', 'a', encoding='utf-8') as f:
      f.write(dumps(loans, indent=' '))


def read_loans():

  with open('data/get/getLoans.json', 'r', encoding='utf-8') as f:
    
    s = f.read()

    return loads(s)


if __name__ == '__main__':
  
  # get_loans()

  loans = read_loans()

  with open('data/get/getLoans.csv', 'w', encoding='utf-8') as f:
    for loan in loans:

      SFClientLoanNumber_Loan_Accounts = ''
      try:
        SFClientLoanNumber_Loan_Accounts = loan['_Custom_Fields_Loan_Accounts']['SFClientLoanNumber_Loan_Accounts']
      except:
        pass
      
      f.writelines(
        ','.join(
          [
            loan.get('encodedKey', ''),
            loan.get('id', ''),
            loan.get('accountHolderKey', ''),
            SFClientLoanNumber_Loan_Accounts,
          ]
        ) + '\r'
      )