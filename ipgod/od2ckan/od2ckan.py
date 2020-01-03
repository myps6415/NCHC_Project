#!/bin/python
# -*- coding: utf-8 -*-
# encoding=utf8  
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')
import odtw
import map2ckan
import os
import logging
import ConfigParser
import time
import hashlib
from migrate_old_odformat import getRIDfromURL, downloadFile, findOldPackageRes

from ckanapi import RemoteCKAN, NotAuthorized, NotFound, ValidationError, CKANAPIError, ServerIncompatibleError

LOGGING_FILE = 'ipgod-od2ckan.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d  : %(message)s')
logger = logging.getLogger('root') 

config = ConfigParser.ConfigParser()
config.read('config.ini')

class import2ckan():
    def __init__(self):
        global config
	url = config.get('od2ckan', 'ckanurl')
	ckan_key = config.get('od2ckan', 'ckan_key')
	ua = 'ckanapiexample/1.0 (+http://example.com/my/website)'
	self.ckan = RemoteCKAN(url, apikey=ckan_key, user_agent=ua)
	self.package = {}
	self.package_exist = 0
	self.resources = []

    def check_package(self):
	pkgs = self.ckan.action.package_autocomplete(q=self.package['name'].lower())
	for pkg in pkgs: 
	    if self.package['name'].lower() == pkg['name']:
		package_resources = self.ckan.action.package_show(id=pkg['name'])
		self.resources = package_resources['resources']
		return True
	return False

    def check_resource(self, testresid):
	for res in self.resources:
	    if res['name'] == testresid:
                logger.info("resource check: name = resid")
		return True
            if testresid in res['url']:
                logger.info("resource check: resid in url")
                return True
            
	return False

    def check_organization(self):
	org = self.ckan.action.organization_list(organizations=[self.package['owner_org']])
	if len(org) == 0:
	    return False
	else:
	    return True

    def check_tag(self):
	return

    def add_package(self):
	try:
            self.ckan.action.package_create(
		name = self.package['name'].lower(),
		title = self.package['title'],
		owner_org = self.package['owner_org'],
		notes = self.package['notes'],
		type = "dataset",
		last_modified = self.package['last_modified'],
		#license_id = self.package['license_id'],
		author = self.package['author'],
		author_email = self.package['author_email'],
		tags = self.package['tag'],
		extras = self.package['extras']
            )
	    res = {self.package['name']:True}
	except CKANAPIError as e:
	    res = {self.package['name']:False}
            print "CKANAPIError %s\n" % e
            logger.info("add package %s fail: (%s)" % (self.package['name'], e))
	except ValidationError as e:
	    res = {self.package['name']:False}
	    print "ValidationError %s\n" % e
	    logger.info("add package %s fail: (%s)" % (self.package['name'], e))
	except:
	    res = {self.package['name']:False}
	    logger.info("add package %s fail: (%s)" % (self.package['name'], 'no idea'))
	return res

    def add_resource(self):

	rres = {}
        testpid = self.package['name']
        oldRes = findOldPackageRes(testpid)
	for res in self.package['resources']:

            testurl = res.get('extras').get('downloadURL')
            resourceid = getRIDfromURL(oldRes, testurl)
            res['resourceid'] = resourceid
            logger.info("get old rid %s" % resourceid) 
            if res['resourceid'] == '':
	        logger.info("dangerous resourceid or new format %s" % (res['resourceid']))
                xurl = testurl
		m = hashlib.md5()
		m.update(xurl.encode('utf-8'))
		resourceid = m.hexdigest()
                res['resourceid'] = resourceid
		rid = res['resourceid']
		rres[rid] = False
	        #continue
            if 'file' in res:
                rfile = res['file']
            else:
	        rfile = self.package['basepath']+'/'+res['resourceid']+'.'+res['format'].lower()

	    if self.check_resource(res['resourceid'].lower()) == True:
                logger.info("resource %s exist" % res['resourceid'])
                continue

	    time.sleep(1)

	    rid = res['resourceid']
	    rres[rid] = False
            if os.path.isfile(rfile) == True:
	        logger.info("adding resource %s and upload file %s" % (res['resourceid'], rfile))
            else:
	        logger.info("file %s not exist or some file error" % rfile)
	        logger.info("file %s not exist, try to download again" % testurl)
                #local_filename = testurl.split('/')[-1]
                #rfile = self.package['basepath']+'/'+local_filename
                downloadFile(testurl, rfile)                

	    if self.check_resource(res['resourceid'].lower()) == True:
                logger.info("resource %s exist" % res['resourceid'])
		rres[rid] = True
	    else:
      
                if os.path.isfile(rfile) == False:
                    rfile=''
	        #print "upload file %s" % rfile
                ndesc = "資源描述：\n\n"
                rextras = res['extras']
                for k, v in rextras.items():
                    ndesc = ndesc+k.encode('utf-8')+":"+v.encode('utf-8')+"\n\n"
                #print ndesc
	        #resc = self.ckan.action.resource_create(
	        #    package_id=self.package['name'].lower(),
	        #    url=testurl,
	        #    description=ndesc,
	        #    format=res['format'].lower(),
	        #    name=res['resourcedescription'],
	        #    last_modified=res['resourcemodified'],
	        #    upload=open(rfile, 'rb')
                #)
                #print resc
		#rres[rid] = True
	    	try:
                    if rfile == '':
		        resc = self.ckan.action.resource_create(
		            package_id=self.package['name'].lower(),
		            url=testurl,
		            description=ndesc,
		            format=res['format'].lower(),
		            name=res['resourcedescription'],
		            last_modified=res['resourcemodified'],
		        )

                    else:
		        resc = self.ckan.action.resource_create(
		            package_id=self.package['name'].lower(),
		            url=testurl,
		            description=ndesc,
		            format=res['format'].lower(),
		            name=res['resourcedescription'],
		            last_modified=res['resourcemodified'],
		            upload=open(rfile, 'rb'),
		        )
                    #print resc
		    logger.info("resource added %s" % res['resourceid'])
		    rres[rid] = True
	    	except NotFound:
	            logger.info("add resource %s fail (name/id not found)" % res['resourceid'])
	        except:
	            logger.info("add resource %s fail" % res['resourceid'])
	return rres

    def add_organization(self):
    
	self.ckan.action.organization_create(
		name=self.package['owner_org'],
		title=self.package['org']['title'],
		extras=self.package['org']['extras'],
		users=[{"name":"admin"}]
		)
	return

    def add_tag(self):
	return

    def update_organization(self):
	self.ckan.action.organization_update(
		id=self.package['owner_org'],
		name=self.package['owner_org'],
		title=self.package['org']['title'],
		extras=self.package['org']['extras'],
		users=[{"name":"admin"}]
		)
	return

    def update_package(self):
	try:
	    self.ckan.action.package_patch(
		id = self.package['name'].lower(),
		name = self.package['name'].lower(),
		title = self.package['title'],
		owner_org = self.package['owner_org'],
		notes = self.package['notes'],
		type = "dataset",
		last_modified = self.package['last_modified'],
		#license_id = self.package['license_id'],
		author = self.package['author'],
		author_email = self.package['author_email'],
		tags = self.package['tag'],
		extras = self.package['extras']
	    )
	    res = {self.package['name']:True}
	except:
	    res = {self.package['name']:False}
	return res

    def commit(self, data):
	self.package = data
	ckan_res = {}
        #print data
	if self.check_organization() == False:
	    logger.info("add organization " + self.package.get('org').get('name'))
	    self.add_organization()
	else:
	    logger.info("update organization " + self.package['org']['name'])
	    self.update_organization()
	
	if self.check_package() == True:
	    logger.info("update package and add resources " + self.package['name'])
	    pres = self.update_package()
	    rres = self.add_resource()
	else:
	    logger.info("add package " + self.package['name'])
	    pres = self.add_package()
	    logger.info("add resources from package" + self.package['name'])
	    rres = self.add_resource()
	ckan_res = {'package':pres, 'resources':rres}
	return ckan_res

if __name__ == '__main__': 
    #jsonfile = 'testdata/A41000000G-000001/A41000000G-000001.json'
    jsonfile = '/opt/ipgod_production/data_download/313000000G-A00123/313000000G-A00123.json'
    if len(sys.argv) > 1:
        jsonfile = sys.argv[1]
    odtw = odtw.od()
    data = odtw.read(jsonfile)

    ckmap = map2ckan.mapod2ckan()
    package = ckmap.map(data)
    #print package
    od_data_path = os.path.dirname(os.path.realpath(jsonfile))
    package['basepath'] = od_data_path
    put2ckan = import2ckan()
    res = put2ckan.commit(package)
    print res

