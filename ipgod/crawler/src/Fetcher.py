from downloadData import downloadData
from metadata import Metadata
import schedule
import time
import threading
import const
import json
import requests
import datetime
import DBUtil
import logging
import config


logging.getLogger("schedule").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


class Fetcher(threading.Thread):
    """
    Fetcher thread get metadata of newly updated opendata.
    """

    dataid = None

    fetcher_id = -1

    def __init__(self, queue, id , dataid=None, sec=-1):
        """
        queue: shared queue between Fetcher and Downloader
        sec: interval to fetch updated metadata, default is -1,
            means not get new data periodically.
        """
        super(Fetcher, self).__init__()
        self.queue = queue
        self.updateInterval = sec
        self.fetcher_id = id
        self.dataid = dataid

        if self.updateInterval > 0:
            schedule.every(self.updateInterval).seconds.do(self.fetchNewMetadata)

    def dummy(self):
        """
        dummy function for test
        """
        logger.info(str(threading.get_ident()) + " do repeat")

    def process_history(self):
        """
        process history data since last fetch
        """
        logger.info(str(threading.get_ident()) + " process hisotry")
        conn = DBUtil.createConnection()
        latestTime = DBUtil.getLastUpdateEpoch(conn)
        DBUtil.closeConnection(conn)
        dataid = self.findUpdateDataID(latestTime)
        logger.debug(len(dataid))
        dataid_count = 0
        meta_count = 0
        for s in dataid:
            dataid_count = dataid_count +1
            logger.info("data count = " + str(dataid_count))
            meta = Metadata.getMetaData(s, self.timeStr)
            for md in meta:
                meta_count = meta_count + 1
                logger.info("meta_count = "+ str(meta_count))
                logger.debug(md.getResourceID() + " put to queue")
                self.queue.put(md)

    def first_run_history2(self):
        logger.debug(str(self.fetcher_id) + " _ "+str(len(self.dataid)))

        self.timeStr = self.buildQueryTimeStr()
        data_count = 0
        conn = None
        for index in range(self.fetcher_id, len(self.dataid), config.fetcher_num):
            meta = Metadata.getMetaData(self.dataid[index], self.timeStr)

            conn = DBUtil.createConnection()

            # To solve the restart problem
            # Solution: all processed flag will be set after download
            # DBUtil.UpdateDataSetToProcessed(conn, self.dataid[index])

            data_count = data_count + 1
            # logger.debug("Fetcher {" + str(self.fetcher_id)+"} query {" + str(data_count) + "} data set "
            #              + self.dataid[index] +
            #              " has {" + str(len(meta)) + "} resource")
            logger.debug("Fetcher [{}] query [{}] dataset [{}] @ dataid[{}] + has [{}] resource"
                         .format(str(self.fetcher_id), str(data_count), self.dataid[index], index, str(len(meta))))
            for m in meta:
                if DBUtil.isResourceURLExist(conn,m.getDataSetID(),m.getResourceID() ,m.getDownloadURL(), m.getFormat()) is False:
                    DBUtil.InsertResourceURL(conn, m.getDataSetID(),m.getResourceID() ,m.getDownloadURL(), m.getFormat())

                # building a downloadData and using queue to get the downloadData
                row = downloadData(m.getDownloadURL(),m.getFormat(),m.getDataSetID(),m.getResourceID())
                self.queue.put(row)


                logger.debug( "Fetcher {" + str(self.fetcher_id) + "} " +
                m.getDownloadURL() + " " +
                m.getFormat() + " " +
                m.getDataSetID() + " " +
                m.getResourceID() )

            DBUtil.closeConnection(conn)

    def run(self):
        if self.dataid is None:
            self.process_history()
        elif self.dataid is not None:
            self.first_run_history2()
            return

        while True:
            schedule.run_pending()
            time.sleep(1)

    def fetchNewMetadata(self):
        dataid = self.findUpdateDataID()
        for s in dataid:
            meta = Metadata.getMetaData(s, self.timeStr)
            logger.debug(dataid)
            for md in meta:
                logger.debug(md.getResourceID() + " put to queue")
                self.queue.put(md)

    def findUpdateDataID(self, historyTime= None):
        """
        find all data set which have been updated since self.updateInterval
        return array contains dataset id
        """

        if historyTime is None:
            # fetcher thread get new data periodically
            self.timeStr = self.buildQueryTimeStr()
            logger.info("Fetch new metadata since " + self.timeStr)
            r = requests.get(const.MODIFIED_URL_PREFIX + self.timeStr)
        else:
            # fetcher thread handle history data
            # Case 1: First run
            # Case 2: recovery from failure
            self.timeStr = self.buildQueryTimeStr()
            if historyTime is "NA":
                # Case 1: The first run scenario, handle all past data
                logger.info("Fetch new history metadata from scratch")
                r = requests.get(const.METADATA_URL_PREFIX)
            else:
                # Case 2: Recovery from failure, handle data between failure time and current
                logger.info("Fetch new history metadata since "+historyTime)
                r = requests.get(const.MODIFIED_URL_PREFIX + historyTime)

        x = json.loads(r.text)
        return x['result']


    def buildQueryTimeStr(self):
        """
        build query time string with format yyyy-mm-dd hh:mm:ss
        datetime object value is equal to self.updateInterval ago
        """
        now = datetime.datetime.now()
        return (now - datetime.timedelta(seconds=self.updateInterval)).strftime('%Y-%m-%d %H:%M:%S')
        # Use a fixed time String for testing
        # return "2016-12-01 11:30:00"