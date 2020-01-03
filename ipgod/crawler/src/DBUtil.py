from pg import DB
import const
import config
import logging

q_string1_template = "select max(download_time at time zone 'Asia/Taipei') as last from {} "
q_string2_template = "INSERT INTO {} VALUES ( '{}', '{}',TIMESTAMP '{}' at time zone 'Asia/Taipei', {}, False, False)"

insert_dataset = "INSERT INTO {} VALUES ('{}' , TRUE )"
insert_dataset_template = "INSERT INTO {} VALUES ('{}' , FALSE )"
query_not_porcessed_dataset = "SELECT package_name from dataset where processed = FALSE"
update_dataset = "UPDATE dataset set processed=TRUE WHERE package_name = '{}' "
update_resource = "UPDATE resource_metadata set processed=TRUE WHERE resource_id = '{}' "

insert_resource_template = "INSERT INTO {} VALUES (nextval('resource_metadata_id_seq'),'{}', '{}','{}','{}', FALSE)"

query_dataset = "SELECT package_name from dataset"
query_dataset_done = "SELECT package_name from dataset WHERE package_name = 'Done'"
query_resource_count = "SELECT COUNT(*) from resource_metadata WHERE package_name = '{}' AND resource_id = '{}' AND url = '{}' AND format = '{}'"

logger = logging.getLogger(__name__)

def createConnection():
    """
    Create PostgreSQL connection with configured db/port/user/password
    :return: PostgreSQL connection to const.DB_DATABASE
    """
    try:
        conn = DB(dbname=const.DB_DATABASE,
                  host=config.db_ip,
                  port=config.db_port,
                  user=config.db_user,
                  passwd=config.db_password)
        return conn
    except Exception as e:
        logging.exception("Create DB connection error!!")


def getLastUpdateEpoch(conn):
    """
    Find out last update timestamp from PostgreSQL with format 2016-11-21 12:00:02
    :param conn: previous created PostgreSQL connection
    :return: Last update timestamp or None
    """
    qs = q_string1_template.format(const.DB_TABLE)
    try:
        q = conn.query(qs)
        """
        To aviod empty DB
        """
        if q.namedresult()[0].last is None:
            return "NA"
        else:
            return q.namedresult()[0].last.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logging.exception("Query DB for last fetch error!!")


def insertDownloadResult(conn, package_name, file_id, timeStr, status):
    """
    Write download result status into database.
    :param conn: PostgreSQL connection
    :param resource_id: String, with format A59000000N-000229, which has fixed length 17
    :param file_id: String, with format 002, which has fixed length 3
    :param timeStr: String, with 2016-11-22 12:10:00 format
    :param status: small int
    :return:
    """
    logger.debug(timeStr)
    qs = q_string2_template.format(const.DB_TABLE, package_name, file_id, timeStr, status)
    try:
        conn.query(qs)
    except Exception as e:
        logging.exception("Insert new record error !!")

def insertDataSetID(conn, package_name):
    qs = insert_dataset_template.format("dataset", package_name)
    try:
        conn.query(qs)
    except Exception as e:
        logging.exception("Insert dataset id error !!")

def insertDataSetDoneFlag(conn):
    qs = insert_dataset.format("dataset","Done")
    try:
        conn.query(qs)
    except:
        logging.exception("Insert Done flag error !!")

def isResourceURLExist(conn,package_name, resource_id, url, format):
    qs = query_resource_count.format(package_name, resource_id, url, format)
    try:
        res = conn.query(qs)
        if res.namedresult()[0][0] is 0:
            return False
        else:
            return True
    except:
        logging.exception("Select resource_metadata error !!")
        return False

def isDatasetEmpty(conn):
    qs = query_dataset
    qs2 = query_dataset_done
    try:
        res = conn.query(qs)
        res2 = conn.query(qs2)
        if len(res.namedresult()) is 0:
            return True
        if len(res2.namedresult()) is 1:
            return False
        else:
            return True
    except:
        logging.exception("Select dataset error!!")
        return False


def UpdateDataSetToProcessed(conn, package_name):
    qs = update_dataset.format(package_name)
    try:
        conn.query(qs)
    except :
        logging.exception("Update error !!")

def UpdateResourceToProcessed(conn, package_name):
    qs = update_resource.format(package_name)
    try:
        conn.query(qs)
    except:
        logging.exception("Update error !!")

def InsertResourceURL(conn, package_name, resource_id, url, format):
    qs = insert_resource_template.format("resource_metadata", package_name, resource_id, url, format)
    try:
        conn.query(qs)
    except:
        logging.exception("Insert error !!")


def getNotProcessedDataSet(conn):
    """
    Get Array of non-porcessed dataset id
    """
    result =[]
    try:
        q = conn.query(query_not_porcessed_dataset)
        for item in q.namedresult():
            result.append(item.package_name)
        return result
    except Exception as e:
        logging.exception("Query not processed  id error !!")

def closeConnection(conn):
    """
    Close PostgreSQL connection
    :param conn: previous created PostgreSQL connection
    """
    try:
        conn.close()
    except Exception as e:
        logging.exception("Close DB connection error!!")