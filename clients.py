import util, mambu
import json, csv


def getClient(clientId):

    req = mambu.api(1, 'GET', '/clients/' + clientId, {}, {}, {})

    return req.json()



def getClients():
    
    reqParams = {
        "offset": "0",
        "limit": "1000",
        "paginationDetails": "ON",
        "detailsLevel": "FULL",
    }
    
    req = mambu.api(2, 'GET', '/clients', reqParams, {}, {})

    resData = req.json()

    open('data/get/getClients.json', 'w', encoding='utf-8').write(json.dumps(resData, indent=4, sort_keys=True))
