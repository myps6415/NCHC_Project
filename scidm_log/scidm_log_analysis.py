import pandas as pd
import datetime
import time
import random
import os, shutil
import zipfile
from ipwhois import IPWhois
from ckanapi import RemoteCKAN

dataset = pd.read_csv('/mnt/dm-storage/daas/log/dataset.csv', header=None, names=['ip', 'dataset','download', 'date', 'size', 'request', 'user-agent'])
dataset_org = pd.read_csv('/mnt/dm-storage/daas/log/dataset-organization.csv', header=None, names = ['org', 'ip', 'dataset', 'download', 'date', 'size', 'requests', 'user-agent'])
dataset_group = pd.read_csv('/mnt/dm-storage/daas/log/dataset-group.csv', header=None, names=['group', 'ip', 'dataset','download', 'date', 'size', 'request', 'user-agent'])

#檔案存放路徑
path = '/mnt/dm-storage/daas/results/scidm_usage_statistic/'
nspo_path = '/mnt/dm-storage/daas/results/'

scidm = RemoteCKAN('https://scidm.nchc.org.tw', apikey='APIKEY')

now = datetime.datetime.now()
today = now.strftime('%Y%m%d')

def clean_file():
    for the_file in os.listdir(path):
        file_path = os.path.join(path, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)

def nspo_data(dataset_org):
    nspo_data = dataset_org[dataset_org['org'] == 'narl-nspo']
    nspo_count = pd.DataFrame({'count': nspo_data.groupby(['dataset']).size()}).reset_index().sort_values('count', ascending=False)
    nspo_dataset_name = []
    for dataset in nspo_count['dataset']:
        name = scidm.call_action('package_show', {'id': dataset})['title']
        nspo_dataset_name.append(name)

    nspo_count['dataset'] = nspo_dataset_name
    nspo_count.to_csv('{}/NSPO_view_count.csv'.format(nspo_path), index= False)
    scidm.action.resource_create(
        package_id='nspo-usage-statistic',
        description='{}_NSPO資料瀏覽次數.csv'.format(today),
        format='csv',
        name='{}_NSPO資料瀏覽次數.csv'.format(today),
        last_modified=today,
        upload=open('{}/NSPO_view_count.csv'.format(nspo_path), 'rb')
    )
    print('Finish {}_NSPO資料瀏覽次數.csv upload'.format(today))

def nspo_download_data(dataset_org):
    nspo_data = dataset_org[dataset_org['org'] == 'narl-nspo']
    nspo_download_data = nspo_data[nspo_data['download'] == True]
    nspo_download_count = pd.DataFrame({'count': nspo_download_data.groupby(['dataset']).size()}).reset_index().sort_values('count', ascending=False)
    nspo_dataset_name = []
    for dataset in nspo_download_count['dataset']:
        name = scidm.call_action('package_show', {'id': dataset})['title']
        nspo_dataset_name.append(name)

    nspo_download_count['dataset'] = nspo_dataset_name
    nspo_download_count.to_csv('{}/NSPO_download_count.csv'.format(nspo_path), index= False)
    scidm.action.resource_create(
        package_id='nspo-usage-statistic',
        description='{}_NSPO被下載資料.csv'.format(today),
        format='csv',
        name='{}_NSPO被下載資料.csv'.format(today),
        last_modified=today,
        upload=open('{}/NSPO_download_count.csv'.format(nspo_path), 'rb')
    )
    print('Finish {}_NSPO被下載資料.csv upload'.format(today))

def browse_download(dataset):
    browser_download = {'類別': ['瀏覽總數', '下載總數'],
                        '數量': [len(dataset['download']), len(dataset[dataset['download'] == True])]}
    browser_download_table = pd.DataFrame.from_dict(browser_download)
    browser_download_table.to_csv('{}/browser_download_table.csv'.format(path), index=False, header=True)
    print('Finish {}_瀏覽及下載總數.csv create'.format(today))

def top_10_ip(dataset):
    ip_count = pd.DataFrame({'count' : dataset.groupby(['ip']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    ip_count.to_csv('{}/top10_ip_count.csv'.format(path), index= False)
    print('Finish {}_前10大被瀏覽ip.csv create'.format(today))

def top_10_download_ip(dataset):
    download_ip_data = dataset[dataset['download'] == True]
    download_ip_count = pd.DataFrame({'count' : download_ip_data.groupby(['ip']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    download_ip_count.to_csv('{}/top10_download_ip_count.csv'.format(path), index=False)
    print('Finish {}_前10大有下載動作ip.csv create'.format(today))

def top_10_dataset(dataset):
    datasetdata = dataset[~dataset['dataset'].isin(['False'])]
    dataset_count = pd.DataFrame({'count': datasetdata.groupby(['dataset']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    # 將原本 dataset id 單獨拉出來成一個 list
    dataset_list = list(dataset_count['dataset'])

    #建立空 list 存 for 迴圈向 scidm 取回的資料集名稱
    dataset_name = []
    for dts in dataset_list:
        try:
            name = scidm.call_action('package_show', {'id': dts})['title']
        except:
            name="N/A"
        dataset_name.append(name)

    # 將名稱回填到 DataFrame 的 dataset 欄位中
    dataset_count['dataset'] = dataset_name
    dataset_count.to_csv('{}/top10_dataset.csv'.format(path), index=False)
    print('Finish {}_前10大被瀏覽資料集.csv create'.format(today))

def top_10_download_dataset(dataset):
    select_data = dataset[(~dataset['dataset'].isin(['False'])) & (dataset['download'].isin([True]))]
    download_dataset = pd.DataFrame({'count': select_data.groupby(['dataset']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    dataset_list = list(download_dataset['dataset'])
    dataset_name = []
    for dts in dataset_list:
        try:
            name = scidm.call_action('package_show', {'id': dts})['title']
        except:
            name="N/A"
        dataset_name.append(name)

    download_dataset['dataset'] = dataset_name
    download_dataset.to_csv('{}/top10_download_dataset.csv'.format(path), index=False)
    print('Finish {}_前10大被下載資料集.csv create'.format(today))

def top_10_group(dataset_group):
    group_count = pd.DataFrame({'count': dataset_group.groupby(['group']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    group_name = []
    for group in group_count['group']:
        try:
            name = scidm.call_action('group_show', {'id': group})['title']
        except:
            name="N/A"
        group_name.append(name)

    group_count['group'] = group_name
    group_count.to_csv('{}/top10_group.csv'.format(path), index= False)
    print('Finish {}_前10大瀏覽分類.csv create'.format(today))

def top_10_download_group(dataset_group):
    download_group_data = dataset_group[dataset_group['download'] == True]
    group_count = pd.DataFrame({'count': download_group_data.groupby(['group']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    group_name = []
    for group in group_count['group']:
        try:
            name = scidm.call_action('group_show', {'id': group})['title']
        except:
            name="N/A"
        group_name.append(name)

    group_count['group'] = group_name
    group_count.to_csv('{}/top10_download_group.csv'.format(path), index= False)
    print('Finish {}_前10大被下載分類.csv create'.format(today))

def top_10_org(dataset_org):
    organization_data = dataset_org[dataset_org['org'] != 'False']
    org_count = pd.DataFrame({'count': organization_data.groupby(['org']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    org_name = []
    for org in org_count['org']:
        try:
            name = scidm.call_action('organization_show', {'id': org})['title']
        except:
            name="N/A"
        org_name.append(name)

    org_count['org'] = org_name
    org_count.to_csv('{}/top10_org.csv'.format(path), index= False)
    print('Finish {}_前10大瀏覽組織.csv create'.format(today))

def top_10_download_org(dataset_org):
    organization_data = dataset_org[(dataset_org['org'] != 'False') & dataset_org['download'] == True]
    org_count = pd.DataFrame({'count': organization_data.groupby(['org']).size()}).reset_index().sort_values('count', ascending=False).head(10)
    org_name = []
    for org in org_count['org']:
        try:
            name = scidm.call_action('organization_show', {'id': org})['title']
        except:
            name="N/A"
        org_name.append(name)

    org_count['org'] = org_name
    org_count.to_csv('{}/top10_download_org.csv'.format(path), index= False)
    print('Finish {}_前10大被下載組織.csv create'.format(today))

def top_10_dataset_ip_source(dataset):
    datasetdata = dataset[~dataset['dataset'].isin(['False'])]
    dataset_count = pd.DataFrame({'count': datasetdata.groupby(['dataset']).size()}).reset_index().sort_values('count', ascending=False).head(10)

    for datasetID in list(dataset_count['dataset']):
        ip_dataframe = pd.DataFrame(dataset[dataset['dataset'] == datasetID]['ip'])
        ip_count = pd.DataFrame({'count': ip_dataframe.groupby(['ip']).size()}).reset_index().sort_values('count',
                                                                                                          ascending=False)

        save_source = []
        for IP in ip_count['ip']:
            obj = IPWhois(IP.strip())
            while True:
                try:
                    results = obj.lookup_rdap(depth=1)
                except Exception as e:
                    if 'HTTP lookup failed' in str(e):
                        results['asn_country_code'] = 'error'
                        source = str(e)
                        break
                    else:
                        continue
                break

            # 判斷 ip 來源
            if results['asn_country_code'] == 'error':
                save_source.append(source)
            elif results['asn_country_code'] == 'TW':
                if 'TANET' in results['network']['name']:
                    source = '學術單位'
                    save_source.append(source)
                elif results['network']['name'] == 'GSN-NET':
                    source = '政府機關'
                    save_source.append(source)
                else:
                    source = '企業民眾'
                    save_source.append(source)
            elif results['asn_country_code'] != 'TW':
                source = '海外民眾'
                save_source.append(source)

        ip_count['source'] = save_source
        dataset_name = scidm.call_action('package_show', {'id': datasetID})['title']
        ip_count.to_csv('{}/{}_ip_source.csv'.format(path, datasetID), index=False)
        print('Finish {}_{}_ip_source.csv create'.format(today, dataset_name))
        time.sleep(random.randint(1, 5))

def zipdir(path, dst):
    zf = zipfile.ZipFile("%s.zip" % (dst), "w", zipfile.ZIP_DEFLATED)
    abs_src = os.path.abspath(path)
    for dirname, subdirs, files in os.walk(path):
        for filename in files:
            absname = os.path.abspath(os.path.join(dirname, filename))
            arcname = absname[len(abs_src) + 1:]
            print('zipping %s as %s' % (os.path.join(dirname, filename), arcname))
            zf.write(absname, arcname)
    zf.close()


if __name__ == '__main__':
    # 清空資料夾
    clean_file()

    # 先處理並上傳 NSPO 的資料
    nspo_data(dataset_org)
    nspo_download_data(dataset_org)

    # 建立分析資料
    browse_download(dataset)
    top_10_ip(dataset)
    top_10_download_ip(dataset)
    top_10_dataset(dataset)
    top_10_download_dataset(dataset)
    top_10_group(dataset_group)
    top_10_download_group(dataset_group)
    top_10_org(dataset_org)
    top_10_download_org(dataset_org)
    top_10_dataset_ip_source(dataset)

    # 將上列檔案壓縮成 zip 檔
    zipdir(path, nspo_path + 'analysis_result')

    # 將壓縮檔上傳
    scidm.action.resource_create(
        package_id='scidm-usage-statistic',
        description='{}_分析結果.zip'.format(today),
        format='zip',
        name='{}_分析結果.zip'.format(today),
        last_modified=today,
        upload=open('{}analysis_result.zip'.format(nspo_path), 'rb')
    )
    print('Finish {}_分析結果.zip upload'.format(today))
