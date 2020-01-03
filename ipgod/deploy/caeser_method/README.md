
cd /usr/lib/ckan/default/src/

. /usr/lib/ckan/default/bin/activate

paster --plugin=ckan create -t ckanext ckanext-caeser_method


cd /opt/ipgod_github
git pull 
cd /opt/ipgod_github/deploy/caeser_method
cp plugin.py /usr/lib/ckan/default/src/ckanext-caeser_method/ckanext/caeser_method/plugin.py
cp setup.py /usr/lib/ckan/default/src/ckanext-caeser_method/setup.py

cd /usr/lib/ckan/default/src/ckanext-caeser_method/
python setup.py develop

cd /opt/ipgod_github/deploy/caeser_method
cp routing.py /usr/lib/ckan/default/src/ckan/ckan/config/routing.py
cp user.py /usr/lib/ckan/default/src/ckan/ckan/controllers/user.py

cp db_search.html /usr/lib/ckan/default/src/ckan/ckan/templates/user/db_search.html
cp resource_read.html  /usr/lib/ckan/default/src/ckan/ckan/templates/package/resource_read.html
cp suggest.html  /usr/lib/ckan/default/src/ckan/ckan/templates/home/suggest.html 
cp package.py /usr/lib/ckan/default/src/ckan/ckan/controllers/package.py
cp add_button.html  /usr/lib/ckan/default/src/ckan/ckan/templates/snippets/add_button.html
cp read_base.html /usr/lib/ckan/default/src/ckan/ckan/templates/user/read_base.html
cp header.html /usr/lib/ckan/default/src/ckan/ckan/templates/header.html
cp home.py  /usr/lib/ckan/default/src/ckan/ckan/controllers/home.py


cd /usr/lib/ckan/default/src/
pip install -e git+https://github.com/okfn/ckanext-disqus#egg=ckanext-disqus


vim /etc/ckan/default/development.ini
======
ckan.plugins = stats text_view image_view recline_view datastore datapusher pdf_view resource_proxy recline_grid_view geojson_view recline_graph_view recline_map_view geo_view iauthfunctions caeser_method disqus

disqus.name = caeser

======

service apache2 restart
service nginx restart

