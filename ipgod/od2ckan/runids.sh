#!/bin/bash
iddata="./ids"
for id in `cat $iddata`;do

echo $id
python ./od2ckan.py /home/thomas/data_download/$id/$id.json >> runid.log

sleep 5

done
