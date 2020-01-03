import pandas as pd
import datetime
from ckanapi import RemoteCKAN

# 定義 scidm 位置
scidm = RemoteCKAN('https://scidm.nchc.org.tw', apikey='API_KEY')

# 定義日期
now = datetime.datetime.now()
today = now.strftime('%Y%m%d')

# 讀取 dataset，處理統計資料
datasetdata = pd.read_csv('~/dataset.csv', header=None, names=['ip', 'dataset','download', 'date', 'size', 'request', 'user-agent'])
datasetdata2 = datasetdata[datasetdata['download'] == True]


def dataset_static(datasetdata, datasetdata2):
    # 將屬於資策會的資料集 id 存入 data_id list
    data_id = []

    for result in scidm.call_action('package_search', {'q': 'organization:i-scipark', 'rows':100})['results']:
        datasets_id = result['id']
        data_id.append(datasets_id)

    datasets = []
    counts = []

    for id_ in data_id:
        datasets.append(scidm.call_action('package_show', {'id': id_})['title'] + '_連線數')
        counts.append(datasetdata[datasetdata['dataset'] == id_]['dataset'].count())
        datasets.append(scidm.call_action('package_show', {'id': id_})['title'] + '_下載數')
        counts.append(datasetdata2[datasetdata2['dataset'] == id_]['dataset'].count())

    iii_data = pd.DataFrame({'datasets': datasets, 'count': counts})
    iii_data.index += 1
    iii_data.columns.names = ['id']
    iii_data.to_csv('~/iii_data.csv', index_label='id')
    print('iii_data.csv created done!')


def dataset_upload_scidm():
    scidm.action.resource_create(
        package_id = 'i-scipark-static',
        description = '{}_iii_data.csv'.format(today),
        format = 'csv',
        name = '{}_iii_data.csv'.format(today),
        last_modified = today,
        upload=open('~/iii_data.csv', 'rb')
    )
    print('iii_data.csv upload done!')


if __name__ == '__main__':
    dataset_static(datasetdata, datasetdata2)
    dataset_upload_scidm()