#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
cd $bin;

date=$(date +%Y%m%d%H%M%S)

check_for_ipgod() {
  if [ $(whoami) == "ipgod" ] ; then
    echo "welcome : $(id -un)!"
  else
    echo 'Error: ipgod required'
    echo
    exit 1
  fi

}



#from=$(date +%s)

check_for_ipgod

#mkdir /opt/ipgod/var/


echo "  PID TTY      STAT   TIME COMMAND"
pid=$(cat /opt/ipgod/var/crawler.pid)
ps $pid |grep python


pid=$(cat /opt/ipgod/var/import.pid)

ps $pid |grep python


