# scidm_log 說明
## 主要程式
- mapping_log.py
- scidm_log_analysis.py
- iii_data.py
- scidm_upload_check.py

## 下載及刪除 resource 程式
- find_resource_and_delete.ipynb

## 程式開發初期測試撰寫邏輯 程式
- scidm_log_pandas.ipynb
- scidm_log_upload.ipynb

## 程式功能描述
1. mapping_log.py：將 Apache log 轉為 csv 檔
2. scidm_log_analysis.py：讀取 mapping_log.py 產出的 csv 檔，分析『資料集平台』和『太空中心資料』的使用情況，於每週一執行一次，產出的檔案分別為：
    - 資料集平台
        - 瀏覽及下載總數.csv
        - 前10大被瀏覽ip.csv
        - 前10大有下載動作ip.csv
        - 前10大被瀏覽資料集.csv
        - 前10大被下載資料集.csv
        - 前10大瀏覽分類.csv
        - 前10大被下載分類.csv
        - 前10大瀏覽組織.csv
        - 前10大被下載組織.csv
        >詳見 (帳號需有權限)：[SCIDM網站資料平台使用統計資訊頁](https://scidm.nchc.org.tw/dataset/scidm-usage-statistic?__no_cache__=True)
        
        >2019/05/03 更新描述：上述檔案改為每週以一包壓縮檔上傳 SCIDM，內包含前十大被瀏覽資料集的瀏覽 ip 分析 csv 檔

    - 太空中心資料
        - NSPO資料瀏覽次數.csv
        - NSPO被下載資料.csv
        >詳見 (帳號需有權限)：[NSPO 資料使用統計](https://scidm.nchc.org.tw/dataset/nspo-usage-statistic)

3. iii_data.py：讀取 mapping_log.py 產出的 csv 檔，分析『智慧科學園區』的使用情況，於每週一執行一次，產出的檔案為：
     - iii_data.csv
        >詳見 (帳號需有權限)：[智慧科學園區資料使用統計頁](https://scidm.nchc.org.tw/dataset/i-scipark-static)
