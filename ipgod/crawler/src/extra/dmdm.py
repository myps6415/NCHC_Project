from period_download import Downloader
import yaml
import logging

logger = logging.getLogger(__name__)

def main():
    # dump download info in yaml to queue
    workers = dump_to_queue("resource.yaml")

    # start thread to download data periodically
    for i in range(len(workers)):
        workers[i].start()
        workers[i].join()


def dump_to_queue(file):
    downloaders = []
    stream = open(file, 'r')
    data = yaml.load(stream)
    for item in data.items():
        downloaders.append(Downloader(item))
    return downloaders


if __name__ == "__main__":
    import logging.config
    logging.config.fileConfig("logging.ini",
                              defaults=None,
                              disable_existing_loggers=False)
    main()
