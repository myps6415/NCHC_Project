#!//usr/bin/python

#####
# Goal: To upload a local file to CAKN
# Author : Ceasar Sun
# Power by IPGOD project
####

from ckanapi import RemoteCKAN
from os import listdir
from os.path import isfile, join
import os
import argparse
import json

default_site = 'http://ipgod.nchc.org.tw'
default_ua = 'ckanapiexample/1.0 + site_url'

def parse_arg():
    parser = argparse.ArgumentParser(description='Upload single file to CKAN', epilog="Author : Ceasar Sun. Power by IPGOD project")
    parser.add_argument('--api_key', help='API key')
    parser.add_argument('--site',    default=default_site, help='CKAN site. Default: [http://ipgod.nchc.org.tw]')
    parser.add_argument('--ua',      default=default_ua, help='Used agent')
    parser.add_argument('--package', help='Package/Dataset ID')
    parser.add_argument('--name',    help='Show name')
    parser.add_argument('--file',    help='Upload file with path')
    return parser.parse_args()

def uploadfile(site_url, ua, api_key, packageid, filepath, showname):

    #print "arg: '%s' '%s' '%s' '%s' '%s' " % (args.api_key, args.site, args.package, args.name, args.file ) 

    mysite = RemoteCKAN(site_url, apikey=api_key, user_agent=ua)
    resource_obj = mysite.action.resource_create(
      package_id = packageid,
      upload = open( filepath, 'rb'),
      name = showname)
    #print  type(resource_obj)
    return resource_obj

args = parse_arg()

resource_obj = uploadfile( args.site, args.ua, args.api_key, args.package , args.file, args.name)

print json.dumps(resource_obj, sort_keys=True, indent=4, separators=(',', ': '))
