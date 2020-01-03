import threading
import schedule
import time
import config
import os
import requests
import logging

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("schedule").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class Downloader(threading.Thread):
    def __init__(self, item):
        super(Downloader, self).__init__()
        self.item = item
        schedule.every(600).seconds.do(self.download)

    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def download(self):
        ts = int(time.time())
        for urls in self.item[1]:
            for fmt in urls:
                self.do_download(self.item[0], urls[fmt], fmt, ts)


    def do_download(self, path, url, extension,ts):
        file_name = path+"-"+str(ts) + "." + extension
        final_path = os.path.join(config.DOWNLOAD_PATH, path.lower(), file_name.lower())
        logger.debug(final_path)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        try:
            response = requests.get(url, stream=True, verify=False, headers={'Connection': 'close'})
        except:
            logging.exception("Request error at "+ url)
            return

        with open(final_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        logging.info("Download completed. File path: " + final_path)

