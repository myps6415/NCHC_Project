#!/usr/bin/python3
# -*- coding: utf-8 -*-
# encoding=utf8  

import os, sys, time
import json
import Geohash
import csv
from dateutil.parser import parse
from influxdb import InfluxDBClient
from geolite2 import geolite2
from ckanapi import RemoteCKAN
import datetime

url = "https://scidm.nchc.org.tw"
ua = 'ckanapiexample/1.0 (+http://example.com/my/website)'
ckan_key = ""
ckan = RemoteCKAN(url, apikey=ckan_key, user_agent=ua)

reader = geolite2.reader()

#influxdb
host = ""
port = 
user = ""
password = ""
database = ""
client = InfluxDBClient(host=host, port=port, username=user, password=password, database=database, timeout=300)  # 初始化

cache_dataset_name = {}
cache_org_name = {}

def data2influxdb(dataset_title, organization_title, dateTime, ip):
    dataset = dataset_title
    organization = organization_title
    geoIPdata = reader.get(ip)
    
    try:
        lat = float(geoIPdata['location']['latitude'])
        lon = float(geoIPdata['location']['longitude'])
        geohash_data = Geohash.encode(float(lat), float(lon))
    except:
        geohash_data = float(0)

    try:
        country = geoIPdata['registered_country']['iso_code']
    except:
        country = 'null'
    try:    
        city = geoIPdata['city']['names']['en']
    except:
        city = 'null'

    access_log = [{
        'measurement' : 'access',
        'tags': { 'dataset': dataset,
                 'organization' : organization,
                 'geohash' : geohash_data,
                 'country' : country,
                 'city' : city
               },
        'fields' : {'value' : 1},
        'time' : dateTime
    }]
    print(access_log)
    client.write_points(access_log)

def get_dataset_title(dataset):
    
    if dataset in cache_dataset_name:
        return cache_dataset_name[dataset]
    else:
        print("retrive {}".format(dataset))
        try:
            pkg_data = ckan.action.package_show(id=dataset)
            org_data = pkg_data['organization']
            cache_dataset_name[dataset] = pkg_data['title']
            cache_org_name[dataset] =  org_data['title']
            print("add cache {} {}".format(pkg_data['title'], org_data['title']))
        except:
            cache_dataset_name[dataset] = 'False'
            cache_org_name[dataset] = 'False'
        return cache_dataset_name[dataset]

def get_org_title(dataset):

    if dataset in cache_org_name:
        return cache_org_name[dataset]
    else:
        print("retrive org {}".format(dataset))
        try:
            pkg_data = ckan.action.package_show(id=dataset)
            org_data = pkg_data['organization']
            cache_dataset_name[dataset] = pkg_data['title']
            cache_org_name[dataset] =  org_data['title']
            print("add org cache {} {}".format(pkg_data['title'], org_data['title']))
        except:
            cache_dataset_name[dataset] = 'False'
            cache_org_name[dataset] = 'False'

        return cache_org_name[dataset]

def update_format(date):
    x = parse(date[:11] + " " + date[12:])
    return x.strftime('%Y-%m-%dT%H:%M:%SZ')

def main(ckanLogFile):
    with open('/log/log2db/scidm.name.log', newline='') as namefile:
        rows = csv.reader(namefile, delimiter=',')
        for row in rows:
            datasetID = row[0]
            orgTitle = row[1]
            datasetTitle = row[2]
            cache_dataset_name[datasetID] = datasetTitle
            cache_org_name[datasetID] = orgTitle
    print("read cache done")

    with open(ckanLogFile, newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=',')

        for row in rows:
            print("read {}".format(row[2]))
            org = row[0]
            ip = row[1]
            dataset = row[2]
            download = row[3]
            date = row[4]

            if dataset == 'False':
                continue
            dataset_title = get_dataset_title(dataset)
            if dataset_title == 'False':
                print("skip dataset {}".format(dataset_title))
                continue
            organization_title = get_org_title(dataset)
            if organization_title == 'False':
                print("skip org {}".format(organization_title))
                continue
            datestr = update_format(date)
            try:
                data2influxdb(dataset_title, organization_title, datestr, ip)
            except:
                with open('/log/log2db/error.log.csv', "a", newline='') as errorfile:
                    errwriter = csv.writer(errorfile)
                    errwriter.writerow(row)
                    
if __name__ == '__main__': 
    if len(sys.argv) > 1:
        ckanlogfile = sys.argv[1]
        main(ckanlogfile)
    else:
        print("please input ckan log file")

