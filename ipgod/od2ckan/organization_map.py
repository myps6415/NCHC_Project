#!/bin/python
# -*- coding: utf-8 -*-
import unicodecsv
import logging
import os.path
LOGGING_FILE = 'ipgod-od2ckan.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d  : %(message)s')
logger = logging.getLogger('root') 

class organization_name():
    def __init__(self):
	self.mapfile = "agencies_name_utf8.csv"

    def search(self, keyword):
        if os.path.isfile(self.mapfile) == True:
	    with open(self.mapfile, 'r') as govfile:
	        spamreader = unicodecsv.reader(govfile, encoding='utf-8')
	        for row in spamreader:
		    org_data = row[1].encode('utf8')
		    if org_data == keyword:
                        logger.info("organization map successfully")
		        en = row[2].lower()
		        en = en.replace(" ", "_")
		        en = en.replace(".", "_")
		        en = en.replace(",", "")
		        en = en.replace(")", "")
		        en = en.replace("(", " ")
		        en = en.replace("  ", " ")
		        en = en.replace(" ", "_")
		        en = en.replace("__", "_")
		        govfile.close()
		        return en
        else:
            logger.warn("agencies_name NOT exist")
        logger.info("organization map(%s) fail" % keyword)

if __name__ == '__main__': 
    org=organization_name()
    print org.search("國家發展委員會")

