#!/bin/python
# -*- coding: utf-8 -*-
import sys
import odtw
import map2ckan
import re
import os
import requests

def findOldPackageRes(pid):
    oldDataBasePath = "/opt/ipgod_production/data_download/"
    od = odtw.od()
    oldDataPath = oldDataBasePath+"/"+pid+"/"+pid+".json"
    
    if os.path.isfile(oldDataPath) == False:
        return ''
    jsonfile = oldDataPath
    data = od.read(jsonfile)
    ckmap = map2ckan.mapod2ckan()
    package = ckmap.map(data)
    resdatas = package['resources']
    return resdatas

def getRIDfromURL(res, url):
    url = re.sub('.*:', '', url, 1)
    resdatas = res
    for resdata in resdatas:
            print resdata.get('extras').get('downloadURL')
            extras = resdata['extras']
            testurl = re.sub('.*:', '', extras['downloadURL'], 1)
            #print resdata['resourceid']
            if testurl == url:
                return resdata['resourceid']
                
    return ''

# https://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
def downloadFile(url, fn):
    #local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(fn, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return True

if __name__ == '__main__':
    pid = "A62000000G-000076"
    testurl = "https://www.ftc.gov.tw/upload/10401opm.pdf"
    rid = getRIDfromURL(pid, testurl)
    print rid
