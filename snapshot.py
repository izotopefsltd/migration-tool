from requests import get
from json import dumps, loads
from util import auth
from csv import DictReader

# this module generates a snapshot of the mambu env
# get all clients
# get all loans

def loadFromFile(filename):

    result = []
    with open(f'data/get/get{filename}.json', 'r', encoding='utf-8') as f:
        result = loads(f.read())

    return result


def pagininatedGet(endpoint):
    
    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/vnd.mambu.v2+json',
    }
    
    reqParams = {
        "limit": "1000",
        "paginationDetails": "ON",
        "detailsLevel": "FULL",
    }
    
    offset = 0
    results = []
    while True:
        reqParams['offset'] = str(offset)
        r = get(f'https://duologi.sandbox.mambu.com/api/{endpoint}/', headers=headers, params=reqParams, auth=auth())
        # print(offset, r.status_code)
        if len(r.json()) > 0:
            for result in r.json():
                results.append(result)
            offset += 1000
            continue
        else:
            break

    with open(f'data/get/get{endpoint}.json', 'w', encoding='utf-8') as foo:
        foo.write(dumps(results, indent=' '))
    
    return results
    

def getLoansAndClients():
    clients = pagininatedGet('clients')
    print(len(clients))
    
    loans = pagininatedGet('loans')
    print(len(loans))
    

def getistOfVlsConNums():

    with open('data/get/getloans.json') as f:
        c = DictReader(f.readlines)

        for line in c:
            print(line[''])


if __name__ == '__main__':
    # print(len(loadFromFile('loans')))
    # pass

    # getLoansAndClients()

    loans = loadFromFile('data/get/get;oans.json')
