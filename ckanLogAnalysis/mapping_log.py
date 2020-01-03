import time
import re
import ckanapi
import csv

from ckanapi import RemoteCKAN
url="https://scidm.nchc.org.tw"
ua = 'ckanapiexample/1.0 (+http://example.com/my/website)'
scidm = RemoteCKAN(url, user_agent=ua)

# cache dict
ckanDataset = {}
ckanDatasetName = {}
ckanDatasetNameFalse = ['xxx']


# parse apache2 log
HOST = r'^(?P<host>.*?)'
SPACE = r'\s'
IDENTITY = r'\S+'
USER = r'\S+'
TIME = r'(?P<time>\[.*?\])'
REQUEST = r'\"(?P<request>.*?)\"'
STATUS = r'(?P<status>\d{3})'
SIZE = r'(?P<size>\S+)'

REGEX = HOST+SPACE+IDENTITY+SPACE+USER+SPACE+TIME+SPACE+REQUEST+SPACE+STATUS+SPACE+SIZE+SPACE

def logParser(log_line):
    match = re.search(REGEX,log_line)
    return ( (match.group('host'),
            match.group('time'), 
            match.group('request') , 
            match.group('status') ,
            match.group('size')
            )
          )

def get_ckan_data(name):
    print('get ckanapi...',name)
    global ckanDataset, ckanDatasetName, ckanDatasetNameFalse
    ckanData = {}
    if name in ckanDatasetNameFalse:
        return False
     
    if len(ckanDataset) > 0:
        if name in ckanDataset.keys():
            dId = name
            return ckanDataset[dId]
   
    if len(ckanDatasetName) > 0:
        if name in ckanDatasetName.keys():
            dId = ckanDatasetName[name]
            return ckanDataset[dId]
    try:
        print('run ckanapi...',name)
        pkg_data = scidm.action.package_show(id=name)
        datasetId = pkg_data['id']
        datasetName = pkg_data['name'] 
        org = pkg_data['organization']
        orgName = org['name']
        groupName = []
        datasetGroup = pkg_data['groups']
        for g in datasetGroup:
            groupName.append(g['name'])
        ckanData = {'id':datasetId, 'name':datasetName, 'org':orgName, 'group':groupName}
        ckanDataset[datasetId] = ckanData
        ckanDatasetName[datasetName] = datasetId
        return ckanData
    except:
        ckanDatasetNameFalse.append(name)
        return False


def find_dataset_id(name):
    #DATASETUUID = r'.*-w4-w4-w4-.*'
    #match = re.search(DATASETUUID, name)
    #if match != None:
    #    return name
    data = get_ckan_data(name)
    if data == False:
        return False
    return data['id']


def find_dataset_org(name):
    
    data = get_ckan_data(name)
    if data == False:
        return False
    orgName = data['org']
    return orgName

def find_dataset_group(name):
    groupName = []
    data = get_ckan_data(name)
    if data == False:
        return False
    
    datasetGroup = data['group']
    #print(datasetGroup)
    #for g in datasetGroup:
    #    print(g)
    return datasetGroup

# parse apache2 request log to dataset

def datasetRequest(data):
    method, uri, proto = data.split(' ')
    print(uri)
    uri = uri+'/'
    DATASET = r'^/dataset/(?P<dataset>.*?)/.*$'
    match = re.search(DATASET, uri)
    if match != None:
        dataset_name = match.group('dataset')
        if dataset_name != '':
            datasetID = find_dataset_id(dataset_name)
            if datasetID != '':
                return datasetID
            else:
                return False
        return False
    return False

# parse apache2 request log to download or not
def datasetDownloadandViewRequest(data):
    DOWNLOAD = r'^GET\s/dataset.*/download'
    VIEW = r'^GET\s/dataset.*/resource.*/view'
    match_download = re.search(DOWNLOAD, data)
    match_view = re.search(VIEW, data)
    if match_download != None:
        return True
    if match_view != None:
        return True
    return False

# test request
#testStr = """207.46.13.146 - - [13/Aug/2018:06:25:25 +0800] "GET /dataset/369000000h-000184 HTTP/1.0" 200 5263 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"""
#downloadStr = """127.0.0.1 - - [02/Aug/2018:08:43:51 +0800] "GET /dataset/73a12ed3-9088-4d5e-98b8-8f720c7004d9/resource/358f60b7-377c-4ae8-b178-af3b0aafca4f/download/201612-v2.2.xlsx HTTP/1.0" 200 61111 "-" "Python-urllib/2.7"""
#viewStr = """144.82.8.206 - - [02/Aug/2018:19:05:34 +0800] "GET /dataset/lass-2017q1/resource/9b5b3907-a08a-478c-8f7e-b6af1574c05d/view/58840689-9f2d-4dd8-85b5-48437d2352b4 HTTP/1.0" 200 1626 "https://scidm.nchc.org.tw/dataset/lass-2017q1/resource/9b5b3907-a08a-478c-8f7e-b6af1574c05d" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"""
#host, time, request, status, size = logParser(viewStr)
#host, time, request, status, size = logParser(testStr)
#print(request)
#print("dataste?",datasetRequest(request))
#print("view or download?",datasetDownloadandViewRequest(request))

# open apache2 log, find request include ckan '''host, dataste, download''' and write to file 'dataset.log'
dataset_file = open('dataset.csv', 'w')
csv_dataset_file = csv.writer(dataset_file,quoting=csv.QUOTE_ALL)
group_file = open('dataset-group.csv', 'w')
csv_group_file = csv.writer(group_file,quoting=csv.QUOTE_ALL)
org_file = open('dataset-organization.csv', 'w')
csv_org_file = csv.writer(org_file, quoting=csv.QUOTE_ALL)

f = open('f10.log', 'r')
#f = open('allCkanCustom.log', 'r')
for line in f: # read all lines already in the file
    #print(line.strip())

    if line: # if you got something...
        #print('got data:', line.strip())
        host, time, request, status, size = logParser(line)
        if host == '127.0.0.1':
            continue
        dataset = datasetRequest(request)
        download = datasetDownloadandViewRequest(request)
        #print(host, dataset, download) # print IP_or_Host, dataste id or name, download true or false
        #dataset_log = "'{0}', '{1}', '{2}', '{3}'\r\n".format(host, dataset, download, request)
        #dataset_file.write(dataset_log)
        csv_dataset_file.writerow([host, dataset, download, request])
        #print(dataset_log)
        orgName = find_dataset_org(dataset)
        #dataset_org_log = "'{0}', '{1}', '{2}', '{3}', '{4}'\r\n".format(orgName, host, dataset, download, request)
        #org_file.write(dataset_org_log)
        csv_org_file.writerow([orgName, host, dataset, download, request])
        dGroups = find_dataset_group(dataset)
        if dGroups != False:
            for dg in dGroups:
                #dataset_group_log = "'{0}', '{1}', '{2}', '{3}', '{4}'\r\n".format(dg, host, dataset, download, request)
                #group_file.write(dataset_group_log)
                csv_group_file.writerow([dg, host, dataset, download, request])

dataset_file.close()
org_file.close()
group_file.close()
#print(ckanDatasetName)
#print(ckanDataset)
#print(ckanDatasetNameFalse)
