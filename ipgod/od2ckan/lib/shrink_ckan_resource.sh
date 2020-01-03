#!/bin/bash 

### 
# Author : Ceasar Sun , Power by IPGOD project
# 20170927: version 1
# Goal: To shrink quantity of resource in dataset , by .last_modified
# Usage: $0 -p [package-id] -a [api-key] -r [ckan-site] -n [quantity] 
#
###

package_id=""
api_key=""
ckan_site=""

reserve_number=30

LOCK_FILE=/tmp/$(basename $0)

# Main

[ -z "$(which jq)" ] && echo "'jq' command not found ? exit ..." && exit ;

# get parameter form command
while getopts p:a:r:n: option
do
 case "${option}"
 in
 p) package_id=${OPTARG};;
 a) api_key=${OPTARG};;
 r) ckan_site=${OPTARG};;
 n) reserve_number=$OPTARG;;
 esac
done

# Check required parameters
[ -z "$package_id" -o -z "$api_key" -o -z "$ckan_site" ] && echo "No '-p [package_id]' or '-a [api_key]' or '-r [ckan_site]' , exit !!" && exit;

LOCK_FILE=$LOCK_FILE.$package_id.lock
[ -e "$LOCK_FILE" ] && echo "Locked:$LOCK_FILE ! Try later ..." && exit || touch $LOCK_FILE
#read

while read index created id ; do
    echo "Delete : $index - $created - $id "
    ckanapi action resource_delete -a $api_key -r $ckan_site id=$id
done < <( ckanapi action package_show -a $api_key -r $ckan_site id=$package_id  | jq -r '.resources[]|"\(.last_modified), \(.id)"' | sort -n -r | cat -n  | sed -e "1,${reserve_number}d" | sort -n -r )

rm -rf $LOCK_FILE 2>/dev/null


exit

