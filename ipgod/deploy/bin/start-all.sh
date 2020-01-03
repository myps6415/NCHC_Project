#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
cd $bin;

date=$(date +%Y%m%d%H%M%S)

check_for_root() {
  if [ $(whoami) == "ipgod" ] ; then
    echo "welcome : $(id -un)!"
  else
    echo 'Error: ipgod required'
    echo
    exit 1
  fi

}



#from=$(date +%s)

check_for_root

#mkdir /opt/ipgod/var/

cd /opt/ipgod/crawler/src
nohup python3 crawler.py >  /opt/ipgod/var/crawler_std.log 2>&1  & 
PID=$!
#if [ $RET -eq 0 ];then
    echo $PID > /opt/ipgod/var/crawler.pid
#else
#    echo "crawler error , stop";
#    exit 1;
#fi

cd /opt/ipgod/od2ckan/
nohup python2 ipgod_import.py >  /opt/ipgod/var/ipgod_import_std.log 2>&1  &
PID=$!

#if [ $RET -eq 0 ];then
    echo $PID > /opt/ipgod/var/import.pid
#else
#    echo "od2import error , stop";
#    exit 1;
#fi
#



