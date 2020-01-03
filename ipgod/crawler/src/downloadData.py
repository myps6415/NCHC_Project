import logging
import config
import requests
import datetime
import DBUtil

logging.getLogger("download").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

class downloadData:
    def __init__(self, URL, format, dataSetID, resourceID):
        self.URL = URL
        self.format = format
        self.dataSetID = dataSetID
        self.resourceID = resourceID

    def download(self):
        # write download result to DB
        conn = DBUtil.createConnection()
        now = datetime.datetime.now()
        timeStr = (now - datetime.timedelta(seconds=config.update_interval_sec)).strftime('%Y-%m-%d %H:%M:%S')
        download_flag = self.writeData()
        DBUtil.insertDownloadResult(conn, self.dataSetID, self.resourceID, timeStr, download_flag)
        logging.info("set "+self.resourceID+" processed to DB")
        DBUtil.UpdateResourceToProcessed(conn, self.resourceID)
        DBUtil.UpdateDataSetToProcessed(conn, self.dataSetID)
        DBUtil.closeConnection(conn)

    def writeData(self):
        URL = self.URL
        logger.info("download from " + URL)

        name = self.resourceID.replace("/", "")
        dir_name = self.dataSetID + "/"

        abspath = config.DOWNLOAD_PATH + "/" + dir_name

        # to avoid the bad connection and invalid URL
        try:
            response = requests.get(URL, stream=True, verify=False, headers={'Connection': 'close'})

            file_name = abspath + name + "." + self.format.strip(";\"").lower()

            with open(file_name, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            logger.info("Download completed. File path: " + file_name)
        except:
            logger.debug("Request error at " + URL)
            return -1
        return response.status_code

    def getResourceID(self):
        return self.resourceID
    def getDataSetID(self):
        return self.dataSetID
    def getURL(self):
        return self.URL
    def getFormat(self):
        return self.format