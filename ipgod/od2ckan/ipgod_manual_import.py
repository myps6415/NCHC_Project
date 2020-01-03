#!/bin/python
# -*- coding: utf-8 -*-
import odtw
import map2ckan
#import tockan
import od2ckan
import os
import logging
import time
import datetime
import ipgodDB
import ConfigParser
import sys

LOGGING_FILE = 'ipgod-od2ckan.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d  : %(message)s')
logger = logging.getLogger('root') 

config = ConfigParser.ConfigParser()
config.read('config.ini')


if __name__ == '__main__':
    if len(sys.argv) > 0:
        pkg = sys.argv[1]
    idb = ipgodDB.ipgoddb()
    rootpath=config.get('od2ckan', 'root_path')
    jsonfile = rootpath+"/"+pkg+"/"+pkg+".json"
    if os.path.isfile(jsonfile) != True:
	error = "jsonfile %s error" % jsonfile
	logger.warn("%s" % error)
        idb.log_package(pkg, 'lost json file: %s', jsonfile)
	print "load file %s fail\n" % jsonfile
	sys.exit()
    odtwdata = odtw.od()
    data = odtwdata.read(jsonfile)

    ckmap = map2ckan.mapod2ckan()
    package = ckmap.map(data)
    od_data_path = os.path.dirname(os.path.realpath(jsonfile))
    package['basepath'] = od_data_path
    put2ckan = od2ckan.import2ckan()
    res = {'package':{pkg:False}, 'resources':False}
    res = put2ckan.commit(package)

    print res
    if res['package'][pkg] == True:
        idb.update_pkg(pkg, 'metadata', 1)
	res_data = res['resources']
	for fileid, status in res_data.items():
	    rids = fileid.split('-')
	    rid = rids[-1]
	    print "%s %s %s" % (pkg, rid, status)
	    if status == True:
		idb.import_done(pkg, rid)
    		idb.update_pkg(pkg, rid, 1)

