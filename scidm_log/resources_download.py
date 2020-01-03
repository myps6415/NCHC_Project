import sys
import requests
from ckanapi import RemoteCKAN
import pandas as pd


class user_information:
    def __init__(self, token, package_id, download_location):
        self.token = token #sys.argv[1]
        self.package_id = package_id #sys.argv[2]
        self.download_location = download_location #sys.argv[3]


def scidm_resources_download(token, package_id):
    scidm = RemoteCKAN('https://scidm.nchc.org.tw', apikey=token)

    # 遍尋包含的資料
    name_list = []
    url_list = []
    for data in scidm.call_action('package_show', {'id': package_id})['resources']:
        name_list.append(data['name'])
        url_list.append(data['url'])

    download_data = pd.DataFrame({'name': name_list, 'url': url_list})

    return download_data


def login():
    s = requests.Session()
    url = 'https://scidm.nchc.org.tw' + '/login_generic'
    r = s.post(url)
    if 'field-login' in r.text:
        # Response still contains login form
        raise RuntimeError('Login failed.')
    return s


def download_resource_data(session, download_data, download_location):
    for name, url in zip(download_data['name'], download_data['url']):
        if '/' in name:
            name = name.replace('/','_')
        data = session.get(url)
        with open(download_location+'/{}'.format(name), 'wb') as f:
            f.write(data.content)
            print(name + ' download finish')


if __name__ == '__main__':
    information = user_information(sys.argv[1], sys.argv[2], sys.argv[3])
    download_data = scidm_resources_download(information.token, information.package_id)
    sesson = login()
    download_resource_data(sesson, download_data, information.download_location)
