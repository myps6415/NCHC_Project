# from metadata import Metadata
import threading
import logging.config
import DBUtil, config
import datetime, requests, time
import logging

logging.getLogger("schedule").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class Downloader(threading.Thread):
    def __init__(self, queue=None):
        """
        queue: shared queue between Fetcher and Downloader
        """
        super(Downloader, self).__init__()
        self.queue = queue
        self.count = 0

    def run(self):
        # Download info from shared queue
        while True:
            if not self.queue.empty():
                # Get metadata item from queue, and execute download logic
                item = self.queue.get()

                # Get the flag to check whether the download task is OK or not
                self.count = self.count + 1
                logger.info("Thread {" + str(threading.get_ident()) + "} has processed " + str(self.count) + " metadata")
                try:
                    logger.info("Thread {" + str(threading.get_ident()) + "} start download " + item.getResourceID())
                    self.download_flag = item.download()

                except Exception as e:
                    logging.exception(str(threading.get_ident()) + " download " + item.getResourceID() + " ERROR!!!")
            else:
                logger.debug("Queue empty, sleep 60 sec...")
                time.sleep(60)

    def getResourceFromDB(self, conn):

        qs = "select * from resource_metadata WHERE processed = FALSE limit 1"
        try:
            q = conn.query(qs)
            return q
        except:
            logging.exception("Select error !!")
        return None

    # Implementation of Issue #22
    def process(self, conn, row):

        rlist = row.namedresult()
        r = rlist[0]

        # update processed to True
        update_str = "UPDATE resource_metadata set processed=TRUE WHERE id = '{}'".format(r.id)
        try:
            conn.query(update_str)
        except:
            logging.exception("Update error !!")

        # Download data
        status = self.download(r)

        # if download complete, remove from DB
        if status == 200:
            del_str = "Delete From resource_metadata WHERE id = {}".format(r.id)
            try:
                conn.query(del_str)
            except:
                logging.exception("Delete error !!")

        # write result to DB
        now = datetime.datetime.now()
        timeStr = (now - datetime.timedelta(seconds=config.update_interval_sec)).strftime('%Y-%m-%d %H:%M:%S')
        DBUtil.insertDownloadResult(conn, r.package_name, r.resource_id, timeStr, status)

    def download(self, nameresult):
        URL = nameresult.url
        logger.info("download from " + URL)

        name = nameresult.resource_id.replace("/", "")
        dir_name = nameresult.package_name + "/"

        abspath = config.DOWNLOAD_PATH + "/" + dir_name

        # to avoid the bad connection and invalid URL
        try:
            response = requests.get(URL, stream=True, verify=False, headers={'Connection': 'close'})
        except:
            logger.exception("Request error at "+URL)
            return -1

        file_name = abspath + name + "." + nameresult.format.strip(";\"").lower()

        with open(file_name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logger.info("Download completed. File path: " + file_name)

        return response.status_code