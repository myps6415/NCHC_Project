#!/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import time
import datetime
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('config.ini')
skip_wall=-1
class ipgoddb():
    def __init__(self):
        self.conn = 0
	db = config.get('db', 'database')
	user = config.get('db', 'user')
	server = config.get('db', 'host')
	password = config.get('db', 'password')

        try:
            self.conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user, server, password))
        except:
            logger.warn("connect db error")
        self.cur = self.conn.cursor()

    def update(self):
        
        pkgs = []
	self.cur.execute("SELECT package_name, file_id from import  where file_id != 'metadata' and file_id != ''")
	rows = self.cur.fetchall()
        
        for row in rows:
	    pkg = row[0].rstrip()
            resourceid = row[1].rstrip()
            nresid = "{0}-{1}".format(pkg, resourceid)
            print "update pkg {0} and resid {1} to {2}".format(pkg, resourceid, nresid)
            
            self.cur.execute("UPDATE import SET file_id='{0}' where package_name='{1}' and file_id='{2}'".format(nresid, pkg, resourceid))
            self.conn.commit()


if __name__ == '__main__':
    print "start fix"
    xdb = ipgoddb()
    xdb.update()
