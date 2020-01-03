#!/bin/python
# -*- coding: utf-8 -*-
import os
import logging
import ConfigParser
import sys
import od2ckan

from ckanapi import RemoteCKAN, NotAuthorized, NotFound, ValidationError, CKANAPIError, ServerIncompatibleError

LOGGING_FILE = 'ipgod-od2ckan.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d  : %(message)s')
logger = logging.getLogger('root') 

config = ConfigParser.ConfigParser()
config.read('config.ini')

def scan_lass_data(package):
    files = os.listdir(package['basepath'])
    for file in files:
        print file
        resource={}
        resource['resourceid'] = file
        resource['format'] = 'txt'
        resource['resourcedescription'] = u'日期：'+file
        resource['resourcemodified'] = ''
        resource['file'] = package['basepath']+'/'+file
        package['resources'].append(resource)

if __name__ == '__main__': 
    package={'resources':[]}
    lass_data_path = '/home/thomas/lass2ckan'
    package['name'] = 'lass_data'
    package['title'] = 'LASS環境感測網路系統量測資料 '
    package['owner_org'] = 'lass'
    package['notes'] = "\nLASS希望由民間社群自發地從Bottom up建置一套環境感測網路系統，任何人都可以自己輕易地架設起來，再把所得資訊分享出去；任何想知道某地環境狀況的人，都可以在地圖上看的到，讓我們的生活品質更美好。\n\nLASS是一套完整的開源環境感測網路系統，涵蓋感測裝置、網路系統架構、大數據分析及展示介面等等，所有資料皆可公開取得。\n"
    package['type'] = ''
    package['last_modified'] = ''
    package['author'] = 'thomas'
    package['author_email'] = 'thomas@nchc.org.tw'
    package['tag'] = [{'name':u'空污'},{'name':u'空氣盒子'}, {'name':u'環保'}, {'name':u'lass'}, {'name':u'空汙'}, {'name':u'Sensor'}]
    package['extras'] = [{'key':'official-web-site', 'value':'http://lass-net.org/'}]
    package['org']={}
    package['org']['name'] = 'lass'
    package['org']['title'] = 'lass'
    package['org']['extras'] = []
    package['basepath'] = lass_data_path
    scan_lass_data(package)
    print package
    put2ckan = od2ckan.import2ckan()
    res = put2ckan.commit(package)
    print res

