#!/usr/bin/python

import os
import socket
import sys
import optparse
import time
import logging
import smtplib
<<<<<<< HEAD
import dw_utils as saf
=======
import dim_utils as saf
>>>>>>> 9376cfc (Update readme)
import run_log_writer
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import pytz
import pymysql
import ConfigParser
from datadog import initialize, api
import parse_schema as keys_metrics
import subprocess


SERVER_NAME = socket.gethostname()
PROCESS_ID = int(time.time())

config = {}


def init():

    global config
    global log

    # Initialize configure and working directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Load config from load_config.ini
    config_parse = ConfigParser.RawConfigParser()
    config_parse.optionxform = str
    config_parse.read('%s/config/%s/load_config_%s.ini' % (script_dir, platform, platform))

    # Dumps load_config.ini file into config dictionary
    config = dict(config_parse.items(env))

    config['PROCESS_NAME'] = config_parse.get('general', 'PROCESS_NAME')
    config['RUN_DIR'] = os.path.join(script_dir, '..', 'var', 'run', config['PROCESS_NAME'])
    config['RUN_DIR_LOCK_HOUR'] = os.path.join(script_dir, '..', 'var', 'run', config['PROCESS_NAME'], table_name)
    config['LOG_DIR'] = os.path.join(script_dir, '..', 'var', 'log', config['PROCESS_NAME'])
    config['CONF_DIR'] = os.path.join(script_dir, '..', 'etc', config['PROCESS_NAME'])

    config['RUN_SCHEMA'] = os.path.join(script_dir, config['LOCAL_SCHEMA_DIR'])

    saf.mkdir(config['RUN_DIR'])
    saf.mkdir(config['RUN_DIR'] + '/' + options.VERTICA_DOMAIN_NAME + '/lock_files/%s' % table_name)
    saf.mkdir(config['RUN_DIR_LOCK_HOUR'])
    saf.mkdir(config['LOG_DIR'])
    saf.mkdir(config['CONF_DIR'])

    STATS_SUMMARY_METADATA_PASSWORD_FILE = os.path.join(config['CONF_DIR'], config['SSMETA_PW_FILE'])
    LOG_PASSWORD_FILE = os.path.join(config['CONF_DIR'], config['LOG_PW_FILE'])
    VERTICA_PASSWORD_FILE = os.path.join(config['CONF_DIR'], config['VERTICA_PW_FILE'])

    if config['OUTPUT_LOCATION'] == 'HDFS':
        config['LOG_PASSWORD'] = subprocess.Popen(config['LOG_PASSWORD'], stdout=subprocess.PIPE, shell=True).stdout.read()
    else:
        config['LOG_PASSWORD'] = saf.get_content(LOG_PASSWORD_FILE)


    config['SSMETA_PASSWORD'] = saf.get_content(STATS_SUMMARY_METADATA_PASSWORD_FILE)
    config['VERTICA_PSWD'] = saf.get_content(VERTICA_PASSWORD_FILE)
    config['DD_API_KEY'] = saf.get_content(config['CONF_DIR'] + '/' + 'dd_api.key')
    config['DD_APP_KEY'] = saf.get_content(config['CONF_DIR'] + '/' + 'dd_app.key')
    config['AWS_ID'] = saf.get_content(config['CONF_DIR'] + '/' + 's3_id.pass')
    config['AWS_SECRET'] = saf.get_content(config['CONF_DIR'] + '/' + 's3_secret.pass')

    # Initialize logging
    FORMAT = "%(asctime)-15s |%(levelname)s| %(message)s"
    logfile = os.path.join(config['LOG_DIR'], "%s_%s_%s.log" % (config['PROCESS_NAME'], table_name,
                                                                time.strftime("%Y-%m-%d")))

    config['USE_DATADOG'] = config_parse.getboolean(env, 'USE_DATADOG')

    if config['USE_DATADOG'] is True:
        options_dd = {
            "api_key": config['DD_API_KEY'],
            "app_key": config['DD_APP_KEY'],
            "api_host": "https://datadog.monitoring.yahoo.com"
        }

        initialize(**options_dd)

    logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename=logfile)

    ##########################################
    # This will log to both console and file #
    ##########################################
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)-15s |%(levelname)s| %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    log = logging.getLogger()

    run_log_writer._init(config, 'LOG')


def notify_error(error_msg):

    smtp = smtplib.SMTP('localhost')
    email_message = "Subject: [%s] Error on %s(%s)\n%s" % (os.path.basename(__file__), socket.gethostname(),
                                                           env, error_msg)

    try:
        smtp.sendmail(config['ERROR_EMAIL_FROM_ADDRESS'], config['ERROR_EMAIL_TO_ADDRESSES'], email_message)
    except:
        log.error("Error sending email:", sys.exc_info()[0])
    finally:
        smtp.quit()


def usage():

    print """
            Usage: python %s [options] -h
          """ % (__file__)


def execute_update_statement(update_statement):
    stats_summary_metadata_conn = pymysql.connect(host=config['SSMETA_HOST'],
                                                  user=config['SSMETA_USERNAME'],
                                                  passwd=config['SSMETA_PASSWORD'],
                                                  db=config['SSMETA_DATABASE'],
                                                  autocommit=True)
    stats_summary_metadata_cursor = stats_summary_metadata_conn.cursor()
    try:
        stats_summary_metadata_cursor.execute(update_statement)
    except Exception as e:
        log.error("Error updating stats_summary_metadata %s" % str(e))
        return None
    return "Success"


def fail_and_exit(proc_output, notify=True):
    run_log_writer._log_trf_loaded_status(options.VERTICA_DOMAIN_NAME, config['PROCESS_NAME'], table_name,
                                          p_utc_hr, 'FAILED', platform, n_p_minutes_string)

    if notify:
        notify_error("Direct load for table %s failed with error [%s]" % (table_name, str(proc_output)))

        if config['LOAD_INTERVAL'] == 'minutes':
            # Ensure we use the right batch when sending notification
            batch = n_p_minutes_string
        else:
            batch = p_utc_hr
        if config['USE_DATADOG'] is True:
            # Sends error event to Yamas
            # Users can create alerts off of these events if desired
            api.Event.create(title='SAF Load Failure; Table: %s; Platform: %s; Batch: %s' %
                                   (table_name, platform, batch),
                             text='Error Output: %s' % str(proc_output),
                             tags=['platform:%s' % platform, 'table:%s' % table_name, 'environment:%s' % env],
                             alert_type='error')

    sys.exit(1)


def execute_query(vertica_command):

    proc_output = saf.run_cmd(vertica_command)

    try:
        rejected_row_count = proc_output.stdout.rstrip()

        if config['LOAD_INTERVAL'] == 'minutes':
            # Ensure we use the right batch when sending notification
            rejected_batch = n_p_minutes_string
        else:
            rejected_batch = p_utc_hr

        if int(rejected_row_count) > 0:
            log.error('Rejected Rows: %s' % rejected_row_count)

            notify_error("Table = %s \n"
                         "Number of rejected rows = %s \n"
                         "Batch = %s" % (table_name, str(rejected_row_count), rejected_batch))
            if config['USE_DATADOG'] is True:
                try:
                    api.Event.create(title='SAF Rejected Rows for %s' % platform,
                                     text='Table = %s; Number of Rejected Rows = %s; Batch = %s' %
                                    (table_name, str(rejected_row_count), rejected_batch),
                                     tags=['platform:%s' % platform, 'table:%s' % table_name, 'environment:%s' % env],
                                     alert_type='error')
                except Exception as e:
                    log.error('Datadog/Yamas Event failed: %s' % str(e))
        else:
            log.info('Rejected Rows: %s' % rejected_row_count)
    except ValueError:
        log.warning('Rejected rows not calculated')

    if proc_output.returncode != 0:
        if 'staging' in env or env == 'dimensions':
            # Check if the AWS error occurred (common)
            if 'The AWS Access Key Id you provided does not exist in our records' in proc_output.stderr \
                    or 'Error calling process() in User Function UDSource' in proc_output.stderr or \
                    'Error calling plan() in User Function S3SourceFactory' in proc_output.stderr:
                log.error('Error running the Insert Query. ERROR : %s' % proc_output.stderr)
                proc_output = saf.run_cmd(vertica_command)
                if proc_output.returncode != 0:
                    log.error('Error running the Insert Query. ERROR : %s' % proc_output.stderr)
                    os.remove(load_lock_file)
                    log.info('Removing lock file %s' % load_lock_file)

                    fail_and_exit(proc_output)
            else:
                # If an actual failure occurs then fail and exit
                log.error('Load failed for table: %s' % table_name)
                log.error('Error running the Insert Query. ERROR : %s' % proc_output.stderr)
                fail_and_exit(proc_output)
        else:
            log.error('Error running the Insert Query. ERROR : %s' % proc_output.stderr)
            os.remove(load_lock_file)
            log.info('Removing lock file %s' % load_lock_file)

            if 'SSL connect error' in proc_output.stderr or \
               'Error calling process() in User Function UDSource at [src/S3.cpp' in proc_output.stderr or \
                    'Error calling plan() in User Function S3SourceFactory' in proc_output.stderr:
                # Failures will be fixed with new Vertica Version
                # We don't want to send failure to slack channel
                fail_and_exit(proc_output, notify=False)
            else:
                if config['LOAD_INTERVAL'] == 'minutes':
                    # Ensure we use the right batch when sending notification
                    batch = n_p_minutes_string
                else:
                    batch = p_utc_hr

                if config['USE_DATADOG'] is True:
                    # Users can create alerts off of these events if desired
                    api.Event.create(title='Load Failure; Table: %s; Platform: %s; Batch: %s' %
                                           (table_name, platform, batch),
                                     text='Error Output: %s' % proc_output.stderr,
                                     tags=['platform:%s' % platform, 'table:%s' % table_name, 'environment:%s' % env],
                                     alert_type='error')
                fail_and_exit(proc_output)

    log.info("Insert statement completed successfully")


def get_row_count(db_name, table, epoch=None):

    if epoch is None:
        """
        :param db_name: Name of database to query
        :return: The number of rows in the Staging table
        """
        row_count = 'select count(*) from %s.%s' % (db_name, table)
    else:
        # This will get row count for load_epoch_ts that was used
        row_count = 'select count(*) from %s.%s where load_epoch_ts = %s' % (db_name, table, epoch)

    vertica_command = '%s -A -t -h %s -U %s -w \'%s\' -c "%s"' % (config['VSQL_CMD'], options.VERTICA_DOMAIN_NAME,
                                                                  config['VERTICA_USER'], config['VERTICA_PSWD'],
                                                                  row_count)

    log.info('Row Count Query: %s' % vertica_command)
    proc_output = saf.run_cmd(vertica_command)

    if proc_output.returncode != 0:
        log.error('Staging table for %s does not equal zero' % table_name)

        try:
            os.remove(load_lock_file)
        except OSError:
            log.warn('No lock file found')
        fail_and_exit(proc_output)

    try:
        rows = int(proc_output.stdout)
        return rows
    except ValueError:
        log.error('Could not get row count from staging table')
        try:
            os.remove(load_lock_file)
        except OSError:
            log.warn('No lock file found')
        fail_and_exit(proc_output)


def is_data_complete(dep_table=None):

    if config['OUTPUT_LOCATION'] == 'HDFS':
        hdfs_loc = config['S3_LOC'] + path_of_hr + '/' + table_name + '/'
        hdfs_res = saf.hdfs_check(hdfs_loc)

        if hdfs_res is True:
            log.info('HDFS SUCCESS File Found!!!')
            return True
        else:
            if hdfs_res is None:
                log.info('HDFS SUCCESS File Not Found')


    if platform != 'test_metadata':

        if dep_table is not None:
            # This checks to see if dependent table has Data before loading the parent
            s3_manifest = config['S3_LOC'] + path_of_hr + '/' + dependent_table + '/'
            proc_output = saf.s3_check_for_manifest_and_validate(s3_manifest, config['S3_PROFILE'], log)

            if proc_output:
                log.info('All files located in the Dependent MANIFEST!')
                log.info('Data is Available on S3 for hour %s for dependent table %s' % (p_utc_hr, dep_table))
                proc = saf.s3_ls(s3_manifest, config['S3_PROFILE'])
                number_of_files = len(proc.stdout.strip().split('\n'))
                log.info('Number of S3 Files = %s' % number_of_files)

                # Check if parent table has all the data
                s3_manifest = config['S3_LOC'] + path_of_hr + '/' + table_name + '/'
                proc_output_main = saf.s3_check_for_manifest_and_validate(s3_manifest, config['S3_PROFILE'], log)

                if proc_output_main:
                    log.info('All files located in the MANIFEST!')
                    log.info('Data is Available on S3 for hour %s' % p_utc_hr)
                    proc = saf.s3_ls(s3_manifest, config['S3_PROFILE'])
                    number_of_files = len(proc.stdout.strip().split('\n'))
                    log.info('Number of S3 Files = %s' % number_of_files)
                    return True
                else:
                    log.info('Manifest File Not Found or Complete - Exiting')
                    sys.exit(1)

            else:
                log.info('Manifest File Not Found or Complete for Dependent Table - Exiting')
                sys.exit(1)
        else:
            s3_manifest = config['S3_LOC'] + path_of_hr + '/' + table_name + '/'
            proc_output = saf.s3_check_for_manifest_and_validate(s3_manifest, config['S3_PROFILE'], log)

            if proc_output:
                log.info('All files located in the MANIFEST!')
                log.info('Data is Available on S3 for hour %s' % p_utc_hr)
                proc = saf.s3_ls(s3_manifest, config['S3_PROFILE'])
                number_of_files = len(proc.stdout.strip().split('\n'))
                log.info('Number of S3 Files = %s' % number_of_files)
                return True
            else:
                log.info('Manifest File Not Found or Complete - Exiting')
                sys.exit(1)
    else:

        s3_data = config['S3_LOC'] + path_of_hr + '/'
        s3_files = saf.s3_check_and_validate(s3_data, config['S3_PROFILE'])
        if s3_files:
            log.info('Files available in S3')
            return True
        else:
            log.info('Files not found on s3 - Exiting')
            sys.exit(1)


def direct_load():
    log.info("Starting direct load for table %s" % table_name)
    log.info("Got hour to be processed %d" % p_utc_hr)

    local_schema_dir = config['RUN_DIR'] + '/' + table_name + '/'

    if is_data_complete(dependent_table):

        if use_local_schema:
            # Just use local schema
            local_schema_dir = config['RUN_SCHEMA'] + '/'
            log.info('Using local schema: %s' % local_schema_dir)
        else:
            ##############################################################
            # This section gets the schema from S3 and copies it locally #
            ##############################################################

            s3_file = config['S3_LOC'] + path_of_hr + '/' + table_name + '/' + config['SCHEMA_FILE_NAME']
            log.info('S3 File Path: %s' % s3_file)

            if config['OUTPUT_LOCATION'] == 'HDFS':
                proc = saf.hdfs_get_schema(s3_file, local_schema_dir)
            else:
                proc = saf.s3_cp(s3_file, local_schema_dir, config['S3_PROFILE'])

            if proc.returncode != 0:
                log.error("Could not download schema file from s3")

                # If Schema isn't copied from S3 then we use local copy
                local_schema_dir = config['RUN_SCHEMA'] + '/'

    with open(local_schema_dir + config['SCHEMA_FILE_NAME'], 'r') as schema:
        schema_output = schema.read()

    try:
        # Parses schema using parse_schema utility
        columns_string, db_name, db_name_stg, load_table, version, \
            delimiter, dimension_dict, join_sql, file_dict, load_query_options = \
            keys_metrics.get_keys_and_metrics_from_json(local_schema_dir + config['SCHEMA_FILE_NAME'], table_name)

        file_format = file_dict['file_format']
        file_compression = file_dict['compression']
        error_handling = load_query_options['add_to_vertica_query']
        query_options = load_query_options['additional_query_params']

    except TypeError as e:
        # If we fail to parse the schema the script exits
        log.error('Exception: Parsing schema failed')
        log.error('Exception: %s' % str(e))
        fail_and_exit(str(e))

    log.info('Schema Version = %s' % version)

    # Does not write dimensions to trf_load_to_db_log
    # Data is available hence starting the load process
    run_log_writer._log_trf_loaded_status(options.VERTICA_DOMAIN_NAME, config['PROCESS_NAME'], table_name, p_utc_hr,
                                          'STARTED', platform, n_p_minutes_string)

    # Check keys/metrics for reporting/staging. Only check keys for dimensions
    if (columns_string is None) or (columns_string == ''):
        log.error("Could not get keys or metrics for table %s" & table_name)
        fail_and_exit('Could not get key or metrics')

    log.info("Got keys and/or metrics successfully from parser.")

    if dimension_dict['is_dimension'] is False:
        if (platform == 'test_mysql') or (platform == 'test_data'):
            # Adding load_epoch_ts for mysql/data
            load_epoch_ts = str(int(time.time()))
            columns_string = columns_string + ',load_epoch_ts as ' + load_epoch_ts
    else:
        load_epoch_ts = None

    s3_source = config['S3_LOC'] + path_of_hr + '/' + table_name + '/*.gz'

    if (db_name_stg is not None) and (skip_staging_table is False):
        # This flow will load to a staging table then to insert to production

        # Get staging table row count
        staging_table_count = get_row_count(db_name_stg, table_name)

        # Checking if staging table row count is 0
        if staging_table_count > 0:
            log.error('Staging table for %s is not empty ---> %s rows' %
                      (table_name, staging_table_count))
            notify_error('Staging table for %s is not empty --> %s rows' %
                         (table_name, staging_table_count))
            sys.exit(1)

        if load_query_options['write_rejects_to_table'] is True:
            log.info('Attempting to write bad records to logs')
            # Specifies the name of the rejected data table
            rejected_data_table = ' REJECTED DATA as TABLE %s.%s_rejected_data' % (db_name_stg, table_name)
            log.info('Possible rejected data can be found here: %s.%s_rejected_data' % (db_name_stg, table_name))

            # Adding rejected_data_table to the error_handling statement
            # Example: REJECTMAX 10 REJECTED DATA as TABLE staging.fact_table_rejected_data
            error_handling = error_handling + rejected_data_table

        if config['OUTPUT_LOCATION'] == 'HDFS':
            # This path is for copying from HDFS

            # Data is copied to staging table before being moved to main table in Vertica
            insert_statement = "COPY %s.%s(%s) FROM '%s' TRAILING NULLCOLS %s DIRECT DELIMITER '%s' %s ; " \
                               "select get_num_rejected_rows();" \
                               % (db_name_stg, table_name, columns_string, s3_source, error_handling, delimiter,
                                  query_options)
        else:
            # This path is for copying from S3

            # Data is copied to staging table before being moved to main table in Vertica
            insert_statement = "ALTER SESSION SET UDPARAMETER FOR awslib aws_id='%s'; " \
                               "ALTER SESSION SET UDPARAMETER FOR awslib aws_secret='%s'; " \
                               "COPY %s.%s(%s) SOURCE S3(bucket='%s') FILTER GZip() " \
                               "TRAILING NULLCOLS %s DIRECT DELIMITER '%s' %s ; select get_num_rejected_rows();" \
                               % (config['AWS_ID'], config['AWS_SECRET'], db_name_stg, table_name,
                                  columns_string, s3_source, error_handling, delimiter, query_options)

        if dimension_dict['is_dimension'] is True:
            if dimension_dict['operation_column'] is None:
                # Fail if Merge is True but merge_column is None
                log.error("Dimension is set to True but operation column is None for Table %s" & table_name)
                fail_and_exit("Dimension is set to True but operation column is None for Table %s" % table_name)

            if dimension_dict['operation'] == 'Insert':
                if dimension_dict['column_string_with_identity'] is not None:
                    columns_string = dimension_dict['column_string_with_identity']

                # Insert from staging to dimensions
                operation_insert_statement = 'INSERT /*+ DIRECT */ INTO %s.%s (%s) SELECT %s FROM %s.%s ' \
                                             'WHERE NOT EXISTS (SELECT 1 FROM %s.%s where %s.%s.%s = %s.%s.%s); ' \
                                             'COMMIT;' % (db_name, table_name, columns_string, columns_string,
                                                          db_name_stg, table_name, db_name, table_name, db_name_stg,
                                                          table_name, dimension_dict['operation_column'], db_name,
                                                          table_name, dimension_dict['operation_column'])
                log.info('Operation Insert Statement: %s' % operation_insert_statement)

        else:
            if join_sql is not None:
                # This will use a custom join sql for inserting into main table
                insert_statement_main = "INSERT /*+ DIRECT */ INTO %s.%s (%s) SELECT %s FROM %s.%s JOIN %s ON " \
                                        "%s.%s = %s.%s.%s; COMMIT;" % (db_name, table_name, columns_string,
                                                                       join_sql['columns'], db_name_stg,
                                                                       table_name, join_sql['join_table'],
                                                                       join_sql['join_table'], join_sql['join_key'],
                                                                       db_name_stg, table_name, join_sql['join_key'])
            else:
                # Standard flow
                insert_statement_main = "INSERT INTO %s.%s SELECT * FROM %s.%s DIRECT; COMMIT;" % \
                                        (db_name, table_name, db_name_stg, table_name)

    else:
        # This flow will copy directly to the production table

        if load_table is not None:
            # This will allow us to load from two output sources into a single table
            options.table_name = load_table

        if file_format == 'orc':
            if dimension_dict['truncate_before_load']:
                # We will DELETE, COPY, then COMMIT. The goal is to not go without data in the main table
                insert_statement = "BEGIN; DELETE /*+ direct */ from %s.%s;" \
                                   "ALTER SESSION SET UDPARAMETER FOR awslib aws_id='%s'; " \
                                   "ALTER SESSION SET UDPARAMETER FOR awslib aws_secret='%s'; " \
                                   "COPY %s.%s(%s) FROM '%s0*' ORC %s DIRECT " \
                                   "NO COMMIT; COMMIT;" % (db_name,
                                                           options.table_name,
                                                           config['AWS_ID'],
                                                           config['AWS_SECRET'],
                                                           db_name,
                                                           options.table_name,
                                                           columns_string, s3_source, error_handling)
            else:
                insert_statement = "ALTER SESSION SET UDPARAMETER FOR awslib aws_id='%s'; " \
                                   "ALTER SESSION SET UDPARAMETER FOR awslib aws_secret='%s'; " \
                                   "COPY %s.%s(%s) FROM '%s0*' ORC %s DIRECT;" % (config['AWS_ID'],
                                                                                              config['AWS_SECRET'],
                                                                                              db_name,
                                                                                              options.table_name,
                                                                                              columns_string,
                                                                                              s3_source, error_handling)
        else:
            # This path is for loading GZip files
            if load_query_options['write_rejects_to_table'] is True:

                log.info('Attempting to write bad records to logs')
                # Specifies the name of the rejected data table
                rejected_data_table = ' REJECTED DATA as TABLE %s.%s_rejected_data' % (db_name_stg, table_name)
                log.info('Possible rejected data can be found here: %s.%s_rejected_data' % (db_name_stg, table_name))

                # Adding rejected_data_table to the error_handling statement
                # Example: REJECTMAX 10 REJECTED DATA as TABLE staging.fact_table_rejected_data
                error_handling = error_handling + rejected_data_table

            if config['OUTPUT_LOCATION'] == 'HDFS':
                # Set aws key and secret key for the SESSION.
                insert_statement = "COPY %s.%s(%s) FROM '%s' TRAILING NULLCOLS %s DIRECT DELIMITER '%s' %s; select "\
                                   "get_num_rejected_rows();" \
                                   % (db_name, options.table_name, columns_string, s3_source, error_handling,
                                      delimiter, query_options)
            else:
                # Set aws key and secret key for the SESSION.
                insert_statement = "ALTER SESSION SET UDPARAMETER FOR awslib aws_id='%s'; " \
                                   "ALTER SESSION SET UDPARAMETER FOR awslib aws_secret='%s'; " \
                                   "COPY %s.%s(%s) SOURCE S3(bucket='%s') FILTER GZip() " \
                                   "TRAILING NULLCOLS %s DIRECT DELIMITER '%s' %s; select get_num_rejected_rows();" \
                                   % (config['AWS_ID'], config['AWS_SECRET'], db_name, options.table_name,
                                      columns_string, s3_source, error_handling, delimiter, query_options)

    log.info("Copy statement is %s" % insert_statement)

    if fact_table:
        # Not setting locks for dimensions
        if os.path.isfile(load_lock_file):
            log.error('Lock file found for previous load: %s' % load_lock_file)
            fail_and_exit("Trying to load data from files which is already loaded. Having lock %s " % load_lock_file)
        else:
            f = open(load_lock_file, 'w+')
            f.close()

    # Run Insert Query for the table
    vertica_command = '%s -A -t -h %s -U %s -w \'%s\' -c "%s"' % (config['VSQL_CMD'], options.VERTICA_DOMAIN_NAME,
                                                                  config['VERTICA_USER'], config['VERTICA_PSWD'],
                                                                  insert_statement)
    log.info("Vertica insert command is %s" % vertica_command)

    start_time = datetime.now()

    execute_query(vertica_command)

    if (db_name_stg is not None) and (skip_staging_table is False):
        # Insert from staging table to production table
        # Once insert is complete, truncate staging table
        try:
            stg_count = get_row_count(db_name_stg, table_name)
            log.info('Staging Rows to be inserted = %s' % stg_count)

            if ('staging' in env) and (dimension_dict['is_dimension'] is False):
                # No need to check dimensions in staging. Sometimes they are 0
                if stg_count == 0:
                    # For staging only if zero rows are inserted this points to a potential error
                    # We will exit with error
                    log.error('Staging Rows to be inserted equals zero for %s' % table_name)
                    sys.exit(1)

            if dimension_dict['is_dimension'] is True:
                # This will run the merge command if Merge is set to True

                try:
                    # Vertica command for merge from staging into dimensions
                    vertica_merge = "%s -h %s -U %s -w \'%s\' -c '%s'" % (config['VSQL_CMD'],
                                                                      options.VERTICA_DOMAIN_NAME,
                                                                      config['VERTICA_USER'],
                                                                      config['VERTICA_PSWD'],
                                                                      operation_insert_statement)

                    log.info("Vertica operation insert statement command is %s" % vertica_merge)
                    proc_output_merge = saf.run_cmd(vertica_merge)

                    # Query for truncating staging table
                    truncate_staging = 'TRUNCATE TABLE %s.%s;' % (db_name_stg, table_name)

                    # Command for truncating staging table
                    vertica_truncate = "%s -h %s -U %s -w \'%s\' -c '%s'" % (config['VSQL_CMD'],
                                                                         options.VERTICA_DOMAIN_NAME,
                                                                         config['VERTICA_USER'],
                                                                         config['VERTICA_PSWD'],
                                                                         truncate_staging)
                    log.info("Truncate Query: %s" % truncate_staging)

                    if proc_output_merge.returncode != 0:
                        log.error('Merge query failed: %s' % proc_output_merge.stderr)

                        # Run truncate query
                        proc_output = saf.run_cmd(vertica_truncate)
                        if proc_output.returncode != 0:
                            # We will not exit here if the truncation fails.
                            # We still want the latest_loaded_date updated
                            log.error('Failed to Truncate Staging table')
                            log.error('Truncate Output: %s' % str(proc_output))
                        else:
                            log.info('Truncate successful for %s' % table_name)

                            # Removing lock file as we will need to reload
                            os.remove(load_lock_file)
                            fail_and_exit(proc_output_merge)
                    else:
                        # Run truncate query if insert fails
                        proc_output = saf.run_cmd(vertica_truncate)

                        if proc_output.returncode != 0:
                            # We will not exit here if the truncation fails.
                            # We still want the latest_loaded_date updated
                            log.error('Failed to Truncate Staging table')
                            log.error('Truncate Output: %s' % str(proc_output))

                        else:
                            log.info('Truncate successful for %s' % table_name)

                except Exception as e:
                    logging.error('Merge Failed Exception: %s' % str(e))
                    fail_and_exit('Merge Failed for Table %s' % table_name)
            else:
                vertica_insert = "%s -h %s -U %s -w \'%s\' -c '%s'" % (
                                config['VSQL_CMD'], options.VERTICA_DOMAIN_NAME,
                                config['VERTICA_USER'], config['VERTICA_PSWD'], insert_statement_main)
                log.info("Vertica insert into main table command is %s" % vertica_insert)

                # Running insert command
                proc_output_insert = saf.run_cmd(vertica_insert)

                # Query for truncating staging table
                truncate_staging = 'TRUNCATE TABLE %s.%s;' % (db_name_stg, table_name)

                # Command for truncating staging table
                vertica_truncate = "%s -h %s -U %s -w \'%s\' -c '%s'" % (
                    config['VSQL_CMD'], options.VERTICA_DOMAIN_NAME,
                    config['VERTICA_USER'], config['VERTICA_PSWD'], truncate_staging)
                log.info("Truncate Query: %s" % truncate_staging)

                if proc_output_insert.returncode != 0:
                    log.error('Error running the Insert Query. ERROR : %s' % proc_output_insert.stderr)

                    # Run truncate query if insert fails
                    proc_output = saf.run_cmd(vertica_truncate)

                    if proc_output.returncode != 0:
                        # We will not exit here if the truncation fails.
                        # We still want the latest_loaded_date updated
                        log.error('Failed to Truncate Staging table')
                        log.error('Truncate Output: %s' % str(proc_output))

                        # Removing lock file as we will need to reload
                        os.remove(load_lock_file)
                        fail_and_exit(proc_output_insert)
                    else:
                        log.info('Truncate successful for %s' % table_name)

                        # Removing lock file as we will need to reload
                        os.remove(load_lock_file)
                        fail_and_exit(proc_output_insert)
                else:
                    # Run truncate query if insert fails
                    proc_output = saf.run_cmd(vertica_truncate)

                    if proc_output.returncode != 0:
                        # We will not exit here if the truncation fails.
                        # We still want the latest_loaded_date updated
                        log.error('Failed to Truncate Staging table')
                        log.error('Truncate Output: %s' % str(proc_output))

                    else:
                        log.info('Truncate successful for %s' % table_name)

        except Exception as e:
            logging.error('Vertica Insert into Main Table Failed: %s' % str(e))
            sys.exit(1)

    else:

        if load_epoch_ts is not None:
            # We are only querying when we have a load_epoch_ts
            if load_table is not None:
                row_count = get_row_count(db_name, load_table, epoch=load_epoch_ts)
            else:
                row_count = get_row_count(db_name, table_name, epoch=load_epoch_ts)

            if 'staging' in env:
                if row_count == 0:
                    # For staging only if zero rows are inserted this points to a potential error
                    # We will exit with error
                    log.error('Rows inserted equals zero for %s' % table_name)
                    sys.exit(1)

            logging.info('Rows inserted equals %s' % row_count)

    # insert Success and last loaded date
    if config['LOAD_INTERVAL'] == 'minutes':
        # Inputs latest_loaded_date with hour and minute
        n_p_minutes_string_time = datetime.strptime(n_p_minutes_string, '%Y%m%d%H%M')
        latest_loaded_date_pt = pytz.utc.localize(n_p_minutes_string_time).astimezone(timezone('US/Pacific'))
        log.info("Trying to update latest_loaded_date to %s for table %s" % (latest_loaded_date_pt, table_name))

        if config['USE_DATADOG'] is True:
            try:
                # Sending event to Datadog detailing last current successful run for given table
                api.Event.create(title='%s Loaded Successfully --> %s' % (table_name, n_p_minutes_string_time),
                                 text='SCHEMA: %s' % schema_output,
                                 alert_type='success',
                                 tags=['platform:%s' % platform, 'table:%s' % table_name, 'environment:%s' % env])
            except Exception as e:
                log.error('Datadog/Yamas Event Creation Failed: %s' % str(e))
    else:
        # Inputs latest_loaded_date with hour only
        latest_loaded_date_utc = datetime.utcfromtimestamp(int(p_utc_hr) * 3600)
        latest_loaded_date_utc = pytz.utc.localize(latest_loaded_date_utc)
        latest_loaded_date_pt = latest_loaded_date_utc.astimezone(timezone('US/Pacific'))
        log.info("Trying to update latest_loaded_date to %s for table %s" % (latest_loaded_date_pt, table_name))

        if config['USE_DATADOG'] is True:
            try:
                # Sending event to Yamas detailing last current successful run for given table
                api.Event.create(title='%s Loaded Successfully --> %s' % (table_name, path_of_hr),
                                 text='SCHEMA: %s' % schema_output,
                                 alert_type='success',
                                 tags=['platform:%s' % platform, 'table:%s' % table_name, 'environment:%s' % env])
            except Exception as e:
                log.error('Datadog Event Creation Failed: %s' % str(e))

    run_time = (datetime.now() - start_time).total_seconds() / 60
    log.info('Table Insert Time: %s' % str(run_time))

    if config['USE_DATADOG'] is True:
        try:
            # Sends to Yamas how long it took to load data into the table
            api.Metric.send(metric='direct_load.run_time',
                            points=run_time,
                            tags=['table:%s' % table_name,
                                  'vertica_cluster:%s' % options.VERTICA_DOMAIN_NAME,
                                  'env:%s' % env])
        except Exception as e:
            log.error('Datadog Failed: %s' % str(e))

    if fact_table is True or ('staging' not in env):
        # For staging we can run this script twice for the same utc_hour.
        # Hence we do not want to put completed in the trf_load_to_db_log table

        try:
            update_log = run_log_writer._log_trf_loaded_status(options.VERTICA_DOMAIN_NAME, config['PROCESS_NAME'],
                                                               table_name, p_utc_hr, 'COMPLETED', platform,
                                                               n_p_minutes_string)

            # Dimension loader will update latest_load_date for dimensions
            update_statement = "UPDATE summary_tables SET latest_loaded_date = IF (latest_loaded_date is NULL, '%s'," \
                               " IF(latest_loaded_date > '%s', latest_loaded_date, '%s')) " \
                               "WHERE table_name = '%s' AND reporting_database_server_id IN " \
                               "(SELECT r.id FROM reporting_database_servers r WHERE host_name = '%s')" %\
                               (latest_loaded_date_pt,latest_loaded_date_pt,latest_loaded_date_pt, table_name,
                                options.VERTICA_DOMAIN_NAME)
            execute_output = execute_update_statement(update_statement)
            log.info("Updated latest_loaded_date to %s for table %s" % (latest_loaded_date_pt, table_name))

        except Exception as e:
            log.error("Exception while writing to db: %s" % str(e))

    # remove the lock file
    if 'staging' in env:
        # Do not remove the lock file for prod. This will ensure no duplicate loads
        os.remove(load_lock_file)


if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option('-o', '--VERTICA_DOMAIN_NAME', action='store', dest='VERTICA_DOMAIN_NAME',  help='Domain name of vertica Example: vertica-b-test.us-west-1.elb.amazonaws.com')
    parser.add_option('-t', '--table_name', action='store', dest='table_name',  help='Table to be processed')
    parser.add_option('-p', '--process_hour', action='store', dest='process_hour',  help='Hour to be processed in utc')
    parser.add_option('-m', '--process_minute', action='store', dest='process_minute', default=None, help='Minutes to be processed')
    parser.add_option('-r', '--reload_dim', action='store_true', dest='reload_dim', help='Reload Dimension Table')
    parser.add_option('-e', '--env', type=str, dest='env', default=None, action='store', help="Which environment to use: Prod, Staging, Dev")
    parser.add_option('-q', '--platform', type=str, dest='platform', default=None, action='store', help="Which platform to use: test_mysql, test_data")
    parser.add_option('--fact_table', dest='fact_table', action='store_true', default=True, help="If set will assume fact table is being loaded")
    parser.add_option('--load_from_s3', dest='load_from_s3', action='store_true', default=False,
                      help="Will load directly from s3")
    parser.add_option('--use_local_schema', dest='use_local_schema', action='store_true', default=False, help="Use local schema. Do not use schema from output")
    parser.add_option('-s', '--s3', type=str, dest='s3_load_path', default=None, action='store', help="S3 Load Location")
    parser.add_option('--skip_staging_table', dest='skip_staging_table', action='store_true', default=False,
                      help="Skip loading to the staging table. This will load directly into prod table")
    parser.add_option('-d', type=str, dest='dependent_table', default=None, action='store',
                      help="Checks if dependent table data is ready")

    options, args = parser.parse_args()

    # Which environment to load to: Prod, Staging, Dev
    env = options.env

    # Which platform we are using: test_mysql
    platform = options.platform

    # True = fact table, False = dimension
    fact_table = options.fact_table

    # If true we will load from s3 instead of loading from Vertica node
    load_from_s3 = options.load_from_s3
    s3_load_path = options.s3_load_path
    use_local_schema = options.use_local_schema

    # If set to true we will not load to staging. Instead we will load directly into the prod table
    skip_staging_table = options.skip_staging_table

    table_name = options.table_name
    dependent_table = options.dependent_table

    # Script exits if command line args are not set properly
    if table_name is None or options.VERTICA_DOMAIN_NAME is None or env is None or platform is None:
        usage()
        sys.exit(1)

    init()

    global p_utc_hr
    global next_processed_minutes
    global processed_minutes_string
    global n_p_minutes_string
    global load_epoch_ts

    load_epoch_ts = None
    n_p_minutes_string = None

    # Get hour to be processed
    if options.process_hour is not None:
        p_utc_hr = int(options.process_hour)

        if options.process_minute is not None:
            processed_minutes_string = options.process_minute

            # Not adding any time since this is ran manually
            next_processed_minutes = (datetime.strptime(str(processed_minutes_string), '%Y%m%d%H%M') +
                                      timedelta(minutes=0)).strftime('%M')

            # Not adding time delta since we are specifying which minute to run
            n_p_minutes_string = (datetime.strptime(str(processed_minutes_string), '%Y%m%d%H%M') +
                                  timedelta(minutes=0)).strftime('%Y%m%d%H%M')

    elif options.process_minute is not None:
        processed_minutes_string = options.process_minute

        # Not adding any time since this is ran manually
        next_processed_minutes = (datetime.strptime(str(processed_minutes_string), '%Y%m%d%H%M') +
                                  timedelta(minutes=0)).strftime('%M')

        # Not adding time delta since we are specifying which minute to run
        n_p_minutes_string = (datetime.strptime(str(processed_minutes_string), '%Y%m%d%H%M') +
                              timedelta(minutes=0)).strftime('%Y%m%d%H%M')

        # Derive utc hour from processed_minutes_string
        tz_utc = pytz.utc
        t = datetime.strptime(processed_minutes_string, '%Y%m%d%H%M')
        time_to_be_processed = tz_utc.localize(t) - timedelta(days=0, hours=-0)
        find_utc_hour = int((time_to_be_processed - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds() / 3600)

        p_utc_hr = int(find_utc_hour)

    else:

        # Get minute interval to be processed
        if config['LOAD_INTERVAL'] == 'minutes':
            """
            Workflow for loading by minute intervals
            """
            # Get next hour to load by getting last_load_hour
            p_utc_hr = run_log_writer.get_last_load_hour(options.VERTICA_DOMAIN_NAME, table_name)

            # Get last processed minute string
            processed_minutes_string = run_log_writer.get_last_load_minute(options.VERTICA_DOMAIN_NAME,
                                                                           table_name, p_utc_hr)

            # If processed_minutes_string is not obtained then exit
            if processed_minutes_string is None:
                log.error('Last Processed Minute is None....Exiting')
                sys.exit(1)

            # Increase the processed_minutes_string by LOAD_INTERVAL_TIME and get Minute value only
            next_processed_minutes = (datetime.strptime(str(processed_minutes_string), '%Y%m%d%H%M') +
                                      timedelta(minutes=int(config['LOAD_INTERVAL_TIME']))).strftime('%M')

            # The next minute interval is the top of the hour
            # We want the process_utc_hour to increase by 1
            if next_processed_minutes == '00':

                # This way we log the correct utc_hour for the minute interval
                p_utc_hr = p_utc_hr + 1

            n_p_minutes_string = (datetime.strptime(str(processed_minutes_string), '%Y%m%d%H%M') +
                                  timedelta(minutes=int(config['LOAD_INTERVAL_TIME']))).strftime('%Y%m%d%H%M')

            log.info('Minute file to process --------------> %s' % n_p_minutes_string)

        # Get hour to be processed
        else:
            """
            Workflow for loading by hour (utc_hour)
            """

            # Essentially utc_hour + 1
            p_utc_hr = int(run_log_writer.get_last_load_hour(
                options.VERTICA_DOMAIN_NAME, table_name)) + int(config['LOAD_INTERVAL_TIME'])

    if p_utc_hr == 0:
        log.info("No data available to be loaded. Hence exiting")
        sys.exit(1)

    if config['LOAD_INTERVAL'] == 'hourly':
        if 'staging' in env:
            path_of_hr = datetime.utcfromtimestamp(int(p_utc_hr) * 3600).strftime('%Y%m%d%H00')
        else:
            path_of_hr = datetime.utcfromtimestamp(int(p_utc_hr) * 3600).strftime('%Y%m%d%H')
    elif config['LOAD_INTERVAL'] == 'daily':
        if platform == 'test_mysql_daily':
            path_of_hr = datetime.utcfromtimestamp(int(p_utc_hr) * 3600).strftime('date=%Y-%m-%d')
    else:
        # Format = '%Y%m%d%H%M'
        path_of_hr = n_p_minutes_string

    # Creates a location to keep json schema downloaded from s3
    saf.mkdir(config['RUN_DIR'] + '/' + table_name + '/')

    if config['LOAD_INTERVAL'] == 'minutes':
        load_lock_file_name = config["PROCESS_NAME"] + "_" + table_name + "_" + n_p_minutes_string + ".lock"
    else:
        load_lock_file_name = config["PROCESS_NAME"] + "_" + table_name + "_" + str(p_utc_hr) + ".lock"

    load_lock_file = os.path.join(config['RUN_DIR'] + '/' + options.VERTICA_DOMAIN_NAME + '/lock_files/%s/' %
                                  table_name, load_lock_file_name)

    if s3_load_path is not None:
        config['S3_LOC'] = s3_load_path

    if 'staging' in env:
        try:
            # Removes any existing lock file for staging runs
            os.remove(load_lock_file)
        except OSError:
            pass

    # Check if another instance of this job is running
    log.info("Checking if another instance of this program is running")
    lock_file = config["PROCESS_NAME"] + "_" + table_name + ".lock"
    direct_load_lock = os.path.join(config['RUN_DIR'], lock_file)
    saf._lock_and_execute_with_given_lock(direct_load, direct_load_lock, log)
