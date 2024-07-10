#!/usr/bin/python

import logging
import pymysql
import socket


# Global connection per execution of the script. Closed in main()
_log_db_mysql_conn = None
_log = logging.getLogger()


def _init(config, key_prefix):
    global _log_db_mysql_conn
    _log_db_mysql_conn = pymysql.connect(host=config[key_prefix + '_HOST'], user=config[key_prefix + '_USERNAME'], port=3312,
                                         passwd=config[key_prefix + '_PASSWORD'], db=config[key_prefix + '_DATABASE'],
                                         autocommit=True,read_timeout=10, write_timeout=10, connect_timeout=10)
    print _log_db_mysql_conn.host


def _get_prev_job_status(job_id, time_interval=None):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    if time_interval and isinstance(time_interval,int) and time_interval>=0:
        sql = "select status from run_log where job_id=%s and (date(run_date) >= DATE_SUB(run_date, INTERVAL %s DAY)) " \
              "order by run_id desc limit 1;" % (job_id, time_interval)
    elif time_interval == -1:
        sql = "select status from run_log where job_id=%s order by run_id desc limit 1;" % job_id
    else:
        sql = "select status from run_log where job_id=%s and date(run_date)=current_date " \
              "order by run_id desc limit 1;" % job_id
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'completed'
    status = row[0].strip()
    cursor.close()
    return str(status)


def _get_last_completed_date(job_id):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select date(max(run_date))  from log.run_log where job_id= %s and status='completed' ; " % job_id
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'
    date = row[0]
    cursor.close()
    return str(date)


def _get_last_aggregated_hour(job_id,spark_job_id):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select min(processed_utc_hour)  from log.aggregate_run_log where status in ('COMPLETED','RUNNING')  and " \
          "job_id = %s and processed_utc_hour > (select COALESCE(max(processed_utc_hour),0) from " \
          "log.aggregate_run_log where job_id= %s and status='COMPLETED')  ; "% (spark_job_id,job_id)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()
    if  processed_utc_hour == None:
        return 'none'

    return processed_utc_hour


def _get_next_transfer_hour(database_name,table_name,spark_job_id):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select min(processed_utc_hour) from log.aggregate_run_log  where status in ('COMPLETED','RUNNING')  and " \
          "job_id = %s and processed_utc_hour > (select COALESCE(max(processed_utc_hour)," \
          "max_prev_spark_agg_hr) max_trf_hr from log.trf_load_to_db_log," \
          "(select max(processed_utc_hour) -1   as max_prev_spark_agg_hr from log.aggregate_run_log  " \
          "where status in ('COMPLETED','RUNNING')  and job_id = %s ) vw_max_prev_spark_agg where process_name = 'TRANSFER' and" \
          " table_name = '%s'  and status='COMPLETED' and database_name = '%s'); " \
          % (spark_job_id,spark_job_id,table_name,database_name)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if  processed_utc_hour == None:
        return 'none'
    return processed_utc_hour


def _is_beacon_processing_for_hour(current_sync_hour):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "SELECT COUNT(1) FROM log.beacons_pagg_upload WHERE ( status = 'PROCESSING'  AND is_forced = 0)" \
          " AND load_hour= '%s';" % (current_sync_hour)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    number_of_beacons_processing = row[0]
    cursor.close()
    return number_of_beacons_processing


def _get_next_load_hour(database_name,table_name):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select COALESCE(min(processed_utc_hour),0)  from log.trf_load_to_db_log where status in ('COMPLETED') " \
          "and database_name = '%s' and table_name = '%s' and process_name='TRANSFER'  and processed_utc_hour > " \
          "  (select COALESCE(max(processed_utc_hour),0) from log.trf_load_to_db_log where process_name = 'LOAD' " \
          "and  table_name = '%s' and status='COMPLETED' and database_name = '%s'); " % \
          (database_name,table_name,table_name,database_name)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if  processed_utc_hour == None:
        return 'none'
    return processed_utc_hour


def _get_next_transfer_real_time_hour(database_name,table_name):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select coalesce(min(trf.processed_utc_hour),0) from log.trf_load_to_db_log trf where trf.status in ( 'COMPLETED','STARTED') " \
          "and trf.table_name = '%s' and database_name = '%s' and trf.process_name = 'S3Sync' and processed_utc_hour > " \
          "( select  max(run.processed_utc_hour)   from log.trf_load_to_db_log run  " \
          "where run.table_name = '%s' and run.process_name = 'TRANSFER'   and run.status = 'COMPLETED' );"\
          % (table_name, database_name, table_name)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if  processed_utc_hour == None:
        return 'none'
    return processed_utc_hour


def _is_sync_real_time_completed_for_hour(database_name,table_name,utc_hour):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select  case when coalesce(min(trf.processed_utc_hour - %d  ),-1) =  0 then  true  else  false  end " \
          "from log.trf_load_to_db_log trf  where trf.status in ( 'COMPLETED') " \
          "and trf.table_name = '%s' and trf.process_name = 'S3Sync' " \
          "and trf.database_name = '%s' " \
          "and processed_utc_hour = %d ;"\
          % (utc_hour, table_name, database_name, utc_hour)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    sync_status = row[0]

    cursor.close()
    return sync_status


def get_last_load_hour(database_name,table_name):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select COALESCE(max(processed_utc_hour),0) from log.trf_load_to_db_log where process_name = 'LOAD' " \
          "and table_name = '%s' and status='COMPLETED' and database_name = '%s'; "% (table_name,database_name)

    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if processed_utc_hour is None:
        return 'none'
    return processed_utc_hour


def get_last_load_minute(database_name,table_name, utc_hour):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select COALESCE(max(process_minute),0) from log.trf_load_to_db_log where process_name = 'LOAD' and " \
          "table_name = '%s' and status='COMPLETED' and database_name = '%s' and " \
          "processed_utc_hour = %s; " % (table_name, database_name, utc_hour)

    cursor.execute(sql)
    row = cursor.fetchone()

    if (row is None) or (row[0] == 0):
        return None

    date_to_minute = row[0]

    cursor.close()

    if date_to_minute is None:
        return 'none'
    return date_to_minute


def _get_current_processing_sync_hour(table_name):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "SELECT processed_utc_hour FROM log.trf_load_to_db_log WHERE table_name = '%s' and process_name = 'S3Sync'" \
          " AND status = 'STARTED'"% (table_name)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if  processed_utc_hour == None:
        return 'none'
    return processed_utc_hour


def _get_hour_to_be_synced(current_sync_hour):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "SELECT MIN(load_hour) next_hr_to_sync FROM log.beacons_pagg_upload WHERE " \
          " ( status = 'COMPLETED'  OR is_forced = 1) AND load_hour < (SELECT MIN(m.load_hour) next_hr_to_sync" \
          " FROM log.beacons_pagg_upload m" \
          " WHERE m.status = 'PROCESSING' AND m.is_forced = 0) AND load_hour > '%s'" % current_sync_hour
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if  processed_utc_hour == None:
        return 'none'
    return processed_utc_hour


def _get_latest_beacon_processing_hour():
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "SELECT MAX(load_hour) next_hr_to_sync FROM log.beacons_pagg_upload WHERE status = 'PROCESSING';"
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    return processed_utc_hour


def _get_hour_to_be_transferred(current_sync_hour):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "SELECT MIN(load_hour) next_hr_to_sync FROM log.beacons_pagg_upload WHERE " \
          " ( status = 'COMPLETED'  OR is_forced = 1) AND load_hour < (SELECT MIN(m.load_hour) next_hr_to_sync" \
          " FROM log.beacons_pagg_upload m" \
          " WHERE m.status = 'PROCESSING' AND m.is_forced = 0) AND load_hour > '%s'" % current_sync_hour
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        return 'none'

    processed_utc_hour = row[0]

    cursor.close()

    if  processed_utc_hour == None:
        return 'none'
    return processed_utc_hour


def _get_list_of_hours_to_be_synched(table_name, database_name):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    table_database_string  = "sync.table_name = '%s' and sync.database_name = '%s'" % (table_name, database_name)
    sql = "select distinct TIMESTAMPDIFF(HOUR, str_to_date('1970-01-01','%Y-%m-%d'), bcn.load_hour)  processed_utc_hour" \
          " from log.beacons_pagg_upload bcn where" \
          " bcn.load_hour > (select date_sub(max(b.load_hour), INTERVAL + 1 HOUR) from log.beacons_pagg_upload b )" \
          " and not exists " \
          "( select  1 from log.trf_load_to_db_log sync where bcn.load_hour = " \
          "date_sub(str_to_date('1970-01-01','%Y-%m-%d') , " \
          "INTERVAL - sync.processed_utc_hour  HOUR) and sync.process_name = 'S3Sync' " \
          "and sync.status = 'COMPLETED' and " + table_database_string + " ) ;"
    cursor.execute(sql)
    hours_to_be_processed = cursor.fetchall()
    if hours_to_be_processed is None:
        return 'none'
    cursor.close()
    return hours_to_be_processed


def _get_list_of_hours_to_be_transferred(table_name, database_name):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "select max(sync.processed_utc_hour) from log.trf_load_to_db_log sync" \
          " where sync.table_name = '%s' and sync.process_name = 'S3Sync' " \
          "and sync.database_name = '%s' ;" % (table_name, database_name)
    cursor.execute(sql)
    hours_to_be_processed = cursor.fetchall()
    if hours_to_be_processed is None:
        return 'none'
    cursor.close()
    return hours_to_be_processed


def _log_trf_loaded_status(database_server,process_name,table_name,process_utc_hour,status, platform,
                           process_minute=None):

    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    if process_minute is None:
        query = "INSERT INTO log.trf_load_to_db_log ( database_name,table_name,process_name,processed_utc_hour," \
                "status, platform) " \
                "values ('%s','%s','%s',%d,'%s','%s');" \
                % (database_server,table_name,process_name,process_utc_hour,status,platform)
    else:
        query = "INSERT INTO log.trf_load_to_db_log ( database_name,table_name,process_name,processed_utc_hour," \
                "status, platform, process_minute) " \
                "values ('%s','%s','%s',%d,'%s','%s','%s');" \
                % (database_server, table_name, process_name, process_utc_hour, status, platform, process_minute)
    data = cursor.execute(query)
    cursor.close()
    return str(data)


def log_job_status(host_name, job_id, status, process_hr, platform='NULL'):
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()

    query = "INSERT INTO log.aggregate_run_log " \
            "(host_name,job_id,processed_utc_hour,status,inserted_at,updated_at, platform) " \
            "VALUES ('%s',%d,%d,'%s',now(),now());" % (host_name,job_id, process_hr, status, platform)

    data = cursor.execute(query)
    cursor.close()
    return str(data)


def _insert_run_log(process_id, status, job_id, error_msg=""):
    _log.info("Job [id=%s] with status %s" % (job_id, status))
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "insert into run_log(JOB_ID, PROCESS_ID, CREATE_DATE, RUN_DATE, SERVER_NAME, STATUS, ERROR_MESSAGE) " \
          "values (%d, %d, current_timestamp, current_timestamp, '%s', '%s', '%s');" % \
          (int(job_id), int(process_id), socket.gethostname(), status, error_msg)
    data = cursor.execute(sql)
    cursor.close()
    return str(data)


def _insert_run_log_with_rowcount(process_id, status, job_id, row_count=0, error_msg="", timestamp="current_timestamp"):
    _log.info("Job [id=%s] with status %s" % (job_id, status))
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "insert into run_log(JOB_ID, PROCESS_ID, CREATE_DATE, RUN_DATE, SERVER_NAME, STATUS, " \
          "ERROR_MESSAGE, LOADED_RECORDS) values (%d, %d, current_timestamp, %s, '%s', '%s', '%s', %d);" % \
          (int(job_id), int(process_id), timestamp, socket.gethostname(), status, error_msg, row_count)
    data = cursor.execute(sql)
    cursor.close()
    return str(data)


def _insert_run_log_with_filename(process_id, status, job_id, error_msg="", filename="", lines_in_file=0):
    _log.info("Job [id=%s] with status %s" % (job_id, status))
    _ping_connection()
    cursor = _log_db_mysql_conn.cursor()
    sql = "insert into run_log(JOB_ID, PROCESS_ID, CREATE_DATE, RUN_DATE, FILE_NAME, SERVER_NAME, STATUS, " \
          "ERROR_MESSAGE, LINES_IN_FILE) " \
          "values (%d, %d, current_timestamp, current_timestamp, '%s', '%s', '%s', '%s', '%d');" % \
          (int(job_id), int(process_id), filename, socket.gethostname(), status, error_msg, lines_in_file)
    data = cursor.execute(sql)
    cursor.close()
    return str(data)


def _ping_connection():
    try:
        _log_db_mysql_conn.ping()
    except:
        _log_db_mysql_conn._connect()


def _cleanup():
    if _log_db_mysql_conn is not None:
        _log_db_mysql_conn.close()


def main():
    config = {}
    config['LOCAL_HOST'] = "localhost"
    config['LOCAL_USERNAME'] = "flixtrix"
    config['LOCAL_PASSWORD'] = "local"
    config['LOCAL_DATABASE'] = "log"
    _init(config, 'LOCAL')
    _insert_run_log(1, "started", 1, "none")
    status = _get_prev_job_status(1)
    print status
    _cleanup()

if __name__ == '__main__':
    main()
