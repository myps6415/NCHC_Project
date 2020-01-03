# waue ; ckan plugin , theme , ...


crawler 安裝啟動方式
od2ckan 安裝方式
ipgod ckan plugin theme 安裝方式


## 注意

	* 尚未開發 deploy 程式，需手動
	* 其中 od2ckan , crawler 皆可直接換
	* 但 ui 關係到 ckan 架構，直接將 github 上的 ckanext-iauthfunctions 直接複製到 /usr/lib/ckan/default/src/ 沒有用
	* login as  :   ipgod@ckan-dev


### 


===    0. download  github ===   
* 0.A 新增
sudo chown ipgod /opt/
cd /opt

git clone https://github.com/ogre0403/ipgod.git
mv ipgod ipgod_github

##  製作ipgod_home ##
mkdir /opt/ipgod_production
ln -sf /opt/ipgod_production /opt/ipgod
cp -rf /opt/ipgod_github/ipgod/crawler/ ./
cp -rf /opt/ipgod_github/ipgod/od2ckan/ ./
cp -rf /opt/ipgod_github/ipgod/ui/


* 0.B 維護
cd /opt/ipgod_github

git pull 

cd /opt/ipgod_production
mkdir backup-$(date +%Y%m%d)

cp -rf /opt/ipgod_github/crawler/ ./
cp -rf /opt/ipgod_github/od2ckan/ ./


===    1. pip3 的安裝方法=== 
sudo apt-get -y remove python3-pip
sudo apt-get -y install python3-setuptools python3-all-dev
sudo easy_install3 pip
sudo apt-get install -y python-unicodecsv python-psycopg2



===    2. 初始資料庫=== 

** 須先建立 ipgod db **

sudo su - postgres
psql

> \password postgres
ipgod@nchc
ipgod@nchc

createdb ipgod




psql -h localhost -p 5432 -d ipgod -U postgres -W
ipgod@nchc

---------------

ALTER DATABASE ipgod OWNER TO ckan_default;
\q
-------------

psql -h localhost -p 5432 -d ipgod -U ckan_default -W
ckan_passwd

-------------
CREATE TABLE import (
    id bigint NOT NULL,
    package_name text,
    datetime timestamp with time zone,
    comment text,
    status smallint DEFAULT 0 NOT NULL,
    file_id character(12)
);
ALTER TABLE import OWNER TO ckan_default;
CREATE SEQUENCE import_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE import_id_seq OWNER TO ckan_default;
ALTER SEQUENCE import_id_seq OWNED BY import.id;
ALTER TABLE ONLY import ALTER COLUMN id SET DEFAULT nextval('import_id_seq'::regclass);
ALTER TABLE ONLY import
    ADD CONSTRAINT import_pkey PRIMARY KEY (id);

CREATE TABLE ckan_download (
        package_name    CHAR(17) NOT NULL,
        file_id         CHAR(3)  NOT NULL,
        download_time   TIMESTAMP WITH TIME ZONE,
        status          SMALLINT,
        processed       boolean,
        PRIMARY KEY(package_name, file_id)
);
ALTER TABLE ckan_download OWNER TO ckan_default;

\q
-----------------------------


===    3. 安裝相依套件 ===   
sudo  pip3 install schedule requests PyGreSQL
sudo  pip2 install ckanapi

===     4. 設定參數 ===   

vim /opt/ipgod/crawler/src/const.py
----------
DB_DATABASE = "ipgod"
DB_TABLE = "ckan_download"
----------

vim /opt/ipgod/crawler/src/config.py
------------
FetchHistory = True

db_ip = "localhost"
db_port = 5432
db_user = "ckan_default"
db_password = "ckan_passwd"

DOWNLOAD_PATH = '/opt/ipgod_production/data_download'
------------

vim /opt/ipgod_production/od2ckan/config.ini
-------------
[od2ckan]
ckanurl=http://ckan-dev.nchc.org.tw
ckan_key=02285f49-a9a7-4809-a42c-a568547511ec
root_path=/opt/ipgod_production/data_download
[db]
database=ipgod
user=ckan_default
password=ckan_passwd
host=127.0.0.1
------------




===    5. 啟動 ===   

mkdir /opt/ipgod_production/data_download

cd /opt/ipgod_production/crawler/src
python3 crawler.py

cd /opt/ipgod_production/od2ckan/
python ipgod_import.py


##  ps ipgod_import.py  中的 import tockan 要註解掉



===  A. extra ===

	1. 修改phppgadmin


vim /etc/phppgadmin/config.inc.php
------
 $conf['extra_login_security'] = false;
-------

vim /etc/apache2/conf-available/phppgadmin.conf
------
#Require local
Allow from all
------

sudo service apache2 restart





