import util, mambu
import sys, requests, json, csv


def getProduct(productId):
    
    req = mambu.api(1, 'GET', '/loanproducts/' + productId, {}, {}, {})

    return req.json()