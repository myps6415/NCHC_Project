# ipgod

### Create database and table
```SQL
CREATE TABLE ckan_download (
	package_name    text  NOT NULL,
	resource_id     text  NOT NULL, 
	download_time   TIMESTAMP WITH TIME ZONE,
	status          SMALLINT,
	processed       boolean, 
	skip            boolean
);

CREATE TABLE resource_metadata(
    id              serial, 
    package_name    text  NOT NULL,
    resource_id     text  NOT NULL,
    url             text,             
    format          text, 
    processed       boolean
);

CREATE TABLE dataset(
    package_name    text  NOT NULL,
    processed       boolean
);
```

### Setup Python environment.
> Install pip for Python3
```bash
$ sudo apt-get remove python3-pip
$ sudo apt-get install python3-setuptools
$ sudo easy_install3 pip
```
> Install required modules
```bash
$ sudo pip3 install schedule requests PyGreSQL
```

### Change configuration in config.py
```python
FetchHistory=True           # start a thread to get history data since last update

db_ip = "localhost"         # IP of Database which having download status 
db_port=5432                # Port of Database which having download status 
db_user="ckan_default"      # username of Database which having download status 
db_password="ckan_passwd"   # password of Database which having download status
 
DOWNLOAD_PATH = '/tmp'      # Where to place download open data resource

update_interval_sec = 60    # Frequency of fetching new data, in second  

logging_configure_file = "logging.ini"  # LOG file onfiguration file

downloader_num = 4          # Number of Downloaders

```

### Use *Python3* to launch Crawler 
```bash
$ python3 crawler.py
```

