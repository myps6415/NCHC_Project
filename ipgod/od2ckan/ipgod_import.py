#!/bin/python
# -*- coding: utf-8 -*-
import sys
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
import re

LOGGING_FILE = 'ipgod-od2ckan.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d  : %(message)s')
logger = logging.getLogger('root') 

config = ConfigParser.ConfigParser()
config.read('config.ini')
skip_wall=-1

def run(pkgs, skip) :
        error=''
        print "cout of package %i" % len(pkgs)
        for pkg in pkgs:
            pstatus = 0
            pstatus = idb.get_status(pkg, 'metadata')
            if pstatus <= skip_wall and skip == 1:
		idb.skip_package(pkg)
                continue
	    idb.import_pkg(pkg, 'metadata', 0)
            jsonfile = rootpath+"/"+pkg+"/"+pkg+".json"
	    if os.path.isfile(jsonfile) != True:
		error = "jsonfile %s error" % jsonfile
		logger.warn("%s" % error)
                #idb.remove_pkg(pkg)
		idb.skip_package(pkg)
		continue
            odtwdata = odtw.od()
            data = odtwdata.read(jsonfile)

            ckmap = map2ckan.mapod2ckan()
            package = ckmap.map(data)
            od_data_path = os.path.dirname(os.path.realpath(jsonfile))
            package['basepath'] = od_data_path
            put2ckan = od2ckan.import2ckan()
	    res = {'package':{pkg:False}, 'resources':False}
            try:
		res = put2ckan.commit(package)
	    except:
                error = "unknow error"
                pstatus = int(pstatus)
                pstatus = pstatus-1
                idb.update_pkg(pkg, 'metadata', pstatus)
                idb.log_package(pkg, 'metadata', error)
		continue
	    print res
	    if res['package'][pkg] == True:
                idb.update_pkg(pkg, 'metadata', 1)
		res_data = res['resources']
		for resourceid, status in res_data.items():
                    #skippkgkw = re.compile('_')
                    #if skippkgkw.search(resourceid) != '':
                    #    idb.skip_package(pkg)
                    #    print "skip , but ... should be fix! %s" % resourceid
		    #rids = resourceid.split('-')
		    #rid = rids[-1]
                    rstatus = idb.get_status(pkg, resourceid)
                    if pstatus <= skip_wall and skip == 1:
			idb.skip_package(pkg, resourceid)
                        continue
		    print "status: %s %s %s" % (pkg, resourceid, status)
	    	    idb.import_pkg(pkg, resourceid, 0)
		    if status == True:
			idb.import_done(pkg, resourceid)
	    		idb.update_pkg(pkg, resourceid, 1)
                    else:
			rstatus = int(rstatus)
			rstatus = rstatus-1
	    		idb.update_pkg(pkg, resourceid, rstatus)
	    else:
                pstatus = int(pstatus)
                pstatus = pstatus-1
		idb.update_pkg(pkg, 'metadata', pstatus)



if __name__ == '__main__':
    idb = ipgodDB.ipgoddb()
    rootpath=config.get('od2ckan', 'root_path')

    if len(sys.argv) == 2:
	mpkgs = []
        mpkgs.append(sys.argv[1])
        print mpkgs
        run(mpkgs, 0)
    else:
    	while True:
            db_pkgs = idb.get_pkgs()
	    run(db_pkgs, 1)
	    time.sleep(8)

