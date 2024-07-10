import subprocess
import shlex
import ConfigParser
import dw_tools as dw
import boto3
import os
import threading
import Queue
import dimension_db as dim_db
import cmd_args as cmd_arg
import email_utils as mail
import json
from datetime import datetime
import pytz
import dim_logging as logging
import run_log_writer
import time
import sys
import random
import oracle_db


def get_pwd(db_path):
    f = open(db_path, 'r')
    pwd = f.read().strip()
    f.close()
    return pwd


# Setup
config = ConfigParser.ConfigParser()
config.read(cmd_arg.arg_opt['config_file'])

PROCESS_ID = int(time.time())
JOB_ID = 170

log_config = dict()
log_config['LOG_HOST'] = config.get('config', 'log_host')
log_config['LOG_USERNAME'] = config.get('config', 'log_user')
log_config['LOG_DATABASE'] = "log"
log_config['LOG_PASSWORD'] = 'local'

run_log_writer._init(log_config, "LOG")

# Delimiter to use for dumped data
delimiter = config.get('config', 'delimiter')

project = config.get('config', 'project')
s3_bucket = config.get('config', 's3bucket')

enclosure_queue = Queue.Queue()

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

mysql_user = config.get('config', 'mysql_user_ro')
aws_profile = config.get('config', 'aws_profile')

version = config.get('config', 'version')
data_dir = config.get('config', 'data_dir')

# Set to True if we are trying to rerun a failed run_time from log.dimension_load_log
rerun = cmd_arg.arg_opt['rerun_dimension']

use_schema_file = config.getboolean('config', 'use_schema_file')
db_type = config.get('config', 'db_type')

if version != 'test_mysql':
    if rerun:
        logging.initLogging(file_name='dimension_%s_rerun' % version)
    else:
        logging.initLogging(file_name='dimension_%s' % version)

if use_schema_file is True:
    db_schema_file = ConfigParser.ConfigParser()
    db_schema_file.read('%s_config_schema.ini' % version)

current_script = os.path.basename(__file__)
# Get time from wrapper

tz_utc = pytz.utc

if version != 'test_mysql':

    try:
        frequency = config.get('config', 'frequency')
    except ConfigParser.NoOptionError as e:
        frequency = None

    if frequency == 'hourly':
        timestamp = datetime.utcnow().strftime('%Y%m%d%H')
    else:
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M')
    logging.print_to_log('INFO', current_script, 'Current Folder Time: %s' % timestamp)
    now = timestamp
else:
    # This data is written by dimension_wrapper before executing dimensions_to_s3
    # Ensure the hour does not change between the time wrapper starts and the time dumper is called
    with open('date_string', 'r') as ds:
        now = ds.read()

if rerun is True:

    # Get failed hour from log.dimension_load_log
    failed_time = dim_db.dimension_load_failed_times(version, now)

    if failed_time is None:
        # If no failed_time present then we exit the script
        logging.print_to_log('INFO', current_script, 'No failed hours to retry')
        sys.exit()

    logging.print_to_log('INFO', current_script, 'Failed Time to Retry: %s' % failed_time)

    # S3 location will be the failed_time taken from log.dimension_load_log
    s3_location = s3_bucket + str(failed_time) + '/'
else:
    # Rerun is false thus we proceed as normal
    if cmd_arg.arg_opt['custom_date'] is None:
        s3_location = s3_bucket + now + '/'

    else:
        s3_location = s3_bucket + cmd_arg.arg_opt['custom_date'] + '/'

# List of all tables
tables_list = []

# List of tables with errors
table_errors = []

# List of Successful tables
successful_tables = []

# Dictionary with tables and reload_status
tables_dict = {}

# List of tables where the delta is none
not_updated = []

dump_full_table = cmd_arg.arg_opt['full_table']

manifest_dict = dict()
manifest_dict["entries"] = []

email_address = config.get('config', 'email_address')
from_address = config.get('config', 'from_address')

# This will create a database dictionary based upon db_names to be used in download_tables function
db_dict = dict()
for values in config.options('db_names'):
    db_name = config.get('db_names', values)
    db_dict[db_name] = {}
    db_dict[db_name]['host'] = config.get('db_hosts', values)
    db_dict[db_name]['pass'] = dim_db.get_pwd(config.get('config', 'conf_dir') + config.get('db_pass', values))


def write_run_log(status, error_msg="", file_name=""):
    return_code = run_log_writer._insert_run_log_with_filename(PROCESS_ID, status, JOB_ID, error_msg, filename=file_name)
    if return_code <= 0:
        logging.print_to_log('ERROR', current_script, 'Error writing to log databases')


def download_tables(que, database_name):
    while True:
        try:

            tables, database_name, delta, reload_table, view_name, always_reload = que.get()

            if version == 'no_delta_test':
                # 100% of the data. No deltas
                always_reload = 1

            # A dumpfile is created locally in order to make format changes to the data set before uploading to s3
            # Locally the dumpfile is written over each run. This is ok since we load each dumpfile to s3

            if rerun is True:
                logfile = '%s_rerun/%s.csv' % (data_dir, tables)
            else:
                logfile = '%s/%s.csv' % (data_dir, tables)

            logging.print_to_log('INFO', current_script, 'database_name = %s' % database_name)

            db_host = db_dict[database_name]['host']

            # Randomly select which host we are dumping from if there is more than one
            if ',' in db_host:
                host_list = db_host.split(',')
                final_db_host = random.choice(host_list)
            else:
                final_db_host = db_host

            if db_type == 'MYSQL':
                mysql_base = 'mysql -h %s -u%s -p%s %s -N -e' % (final_db_host,
                                                                 mysql_user,
                                                                 db_dict[database_name]['pass'],
                                                                 database_name)
                logging.print_to_log('INFO', current_script, 'Using %s Database' % database_name)

            with open(logfile, 'w+b') as f:

                if use_schema_file:
                    # To get columns from a config file and not DB
                    if db_schema_file.has_option(tables, 'columns_mysql'):
                        columns = db_schema_file.get(tables, 'columns_mysql')
                    else:
                        columns = db_schema_file.get(tables, 'columns')
                else:
                    columns = dim_db.get_columns(tables)

                if db_type == 'MYSQL':
                    if delta == 0:

                        if view_name != '':
                            command = mysql_base + '" select %s from %s "' % (columns, view_name)
                        else:
                            command = mysql_base + '" select %s from %s "' % (columns, tables)
                    else:

                        if version == 'test_mysql':
                            # Get last time the table was successfully dumped
                            last_updated_time = dim_db.get_dimension_update(tables)

                            # Write timestamp to log.dimensions table to note when dump was taken
                            dim_db.write_to_dimension(tables)
                        else:
                            last_updated_time = None

                        if last_updated_time is None:
                            # Mysql command will dump entire table since last_updated_time is None
                            if view_name != '':
                                command = mysql_base + '" select %s from %s "' % (columns, view_name)
                            else:
                                command = mysql_base + '" select %s from %s "' % (columns, tables)
                        else:
                            if view_name != '':
                                if dump_full_table is True or reload_table == 1 or always_reload == 1:
                                    # This will dump the entire data
                                    command = mysql_base + '" select %s from %s "' % (columns, view_name)
                                else:
                                    # Mysql command will dump delta since last dump using updated_at timestamp
                                    command = mysql_base + '" select %s from %s where updated_at > %s"' % \
                                              (columns, view_name, last_updated_time)
                            else:
                                if dump_full_table is True or reload_table == 1 or always_reload == 1:
                                    command = mysql_base + '" select %s from %s "' % (columns, tables)
                                else:
                                    # Mysql command will dump delta since last dump using updated_at timestamp
                                    command = mysql_base + '" select %s from %s where updated_at > %s"' % \
                                              (columns, tables, last_updated_time)

                    logging.print_to_log('INFO', current_script, 'Mysql Command: %s' % command)

                start_time = datetime.now()

                try:
                    if db_type == 'ORCL':

                        # Only used for dumping from oracle databases
                        p = subprocess.Popen(shlex.split('python oracle_db.py -t%s -c oracle_config.ini' % tables),
                                             shell=False, stderr=subprocess.PIPE)

                        logging.print_to_log('INFO', current_script, '****** Starting Query - %s ******' % tables)
                        p.communicate()

                    else:
                        # Dumping from MYSQL databases
                        p = subprocess.Popen(shlex.split(command), shell=False, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
                        logging.print_to_log('INFO', current_script, '****** Starting Query - %s ******' % tables)

                        # stdout will be used for writing mysql data to file

                        stdout = p.stdout.read()
                        output_store = stdout

                        if version == 'test_mysql':
                            # Replaces all | with -
                            output_store = output_store.replace('|', '-')
                            # Vertica does accept 0000-00-00 00:00:00 as a value.
                            output_store = output_store.replace('0000-00-00 00:00:00', '1970-01-01 12:00:00')
                        else:
                            output_store = output_store.replace('NULL', '')
                            output_store = output_store.replace('\r', '')

                            # Temp for buyer_seats
                            output_store = output_store.replace('4294967295', '0')

                        # Changing the delimiter: Replaces tab with delimiter
                        if version != 'test_mysql':
                            output_store = output_store.replace('\t', delimiter.decode('hex'))
                        else:
                            output_store = output_store.replace('\t', delimiter)

                        p.communicate()

                        f.write(output_store)
                        f.close()

                        return_code = p.returncode

                        logging.print_to_log('INFO', current_script, 'Subprocess Return Code %s: %s' %
                                             (tables, str(return_code)))
                        if return_code != 0:
                            table_errors.append(tables)
                            # Kills current table job
                            raise Exception('Subprocess Return %s' % tables)

                except Exception as e:
                    logging.print_to_log('ERROR', current_script, 'Exception1 %s: %s' % (tables, e))
                    raise Exception('Failed to query %s' % tables)

                try:
                    logging.print_to_log('WARN', current_script, 'SIZE!!!! %s' % os.path.getsize(logfile))

                    if version != 'test_mysql':
                        if os.path.getsize(logfile) == 0:
                            # Ensure the file size does not equal zero
                            raise Exception('CSV file was empty')

                    logging.print_to_log('INFO', current_script, 'Starting Gzip of %s' % tables)

                    p1 = subprocess.Popen('gzip -f --fast ' + logfile, shell=True, stdout=subprocess.PIPE)
                    p1.communicate()
                    logging.print_to_log('INFO', current_script, 'Dump File Location: %s' % logfile)
                except Exception as d:
                    raise Exception('Failed to Gzip %s ---> %s' % (tables, str(d)))

                if version != 'test_mysql':
                    if version == 'test_schema_file':
                        if db_schema_file.has_option(tables, 'columns_mysql'):
                            columns = db_schema_file.get(tables, 'columns')
                    # Split columns into list
                    columns_list = columns.split(',')

                    # Remove list brackets, keeping quotes around each column name
                    column_string = str(columns_list)[1:-1].replace(' ', '')

                    # Get data-types from mysql database
                    if version == 'test_data':
                        schema_db = dim_db.generate_schema(view_name, column_string, database_name)
                    elif version == 'oracle':
                        schema_db = oracle_db.get_schema(tables, columns)
                    else:
                        schema_db = dim_db.generate_schema(tables, column_string, database_name)

                    location = s3_location + tables + '.csv.gz'
                    location_schema = s3_location + tables + '.schema'

                    schema_file = tables + '.schema'

                    schema_file_location = data_dir + '/' + schema_file

                    try:
                        # Write schema file for table
                        with open(schema_file_location, 'w') as schema:
                            logging.print_to_log('INFO', current_script, 'Writing Schema File For: %s' % tables)

                            # Write initial header
                            schema.write('%s - - - - - - format - -\n' % tables)

                            # Values to change to string
                            change_to_string = ['enum', 'varchar', 'datetime', 'timestamp', 'text', 'date', 'char',
                                                'longblob', 'mediumtext', 'longtext', 'set', 'blob', 'tinyblob',
                                                'VARCHAR2', 'DATE']

                            # Values to change to double
                            change_to_double = ['decimal', 'float', 'FLOAT']

                            # Values to change to int
                            change_to_int = ['tinyint', 'smallint', 'mediumint', 'bit', 'NUMBER']

                            # Values to change to long
                            change_to_long = ['bigint']

                            for line in schema_db:
                                # Making list by '-'
                                split_to_list = line[0].split('-')

                                # Getting the data-type
                                data_type = split_to_list[5].replace(' ', '')

                                # If data-type matches we replace it with appropriate value
                                if data_type in change_to_string:
                                    line = line[0].replace('- ' + data_type + ' -', '- string -')
                                elif data_type in change_to_double:
                                    line = line[0].replace('- ' + data_type + ' -', '- double -')
                                elif data_type in change_to_int:
                                    line = line[0].replace('- ' + data_type + ' -', '- int -')
                                elif data_type in change_to_long:
                                    line = line[0].replace('- ' + data_type + ' -', '- long -')
                                else:
                                    line = line[0]

                                # Replace view name with table name in the schema file
                                line = line.replace(split_to_list[0].split(' ')[0], tables)

                                # Writes table name, column name and data-type
                                schema.write(line + '\n')
                    except Exception as e:
                        # Append table to table_errors in order to be reran
                        table_errors.append(tables)
                        logging.print_to_log('ERROR', current_script, 'Failed to write schema for %s' % tables)
                        logging.print_to_log('ERROR', current_script, 'Schema File Exception: %s' % str(e))
                        raise Exception('Schema File Creation Failed')

                    try:
                        # Upload schema file to s3
                        res = dw.s3_cp(schema_file_location, s3_location, aws_profile)
                        if res.returncode > 0:
                            raise Exception('Error Copying to S3 %s: %s' % (tables, str(res)))

                        if tables in table_errors:
                            # If rerun is successful then remove table from table_errors list
                            table_errors.remove(tables)

                    except Exception as e:
                        logging.print_to_log('ERROR', current_script, 'Exception while moving schema to s3 %s: %s'
                                             % (tables, e))

                        table_errors.append(tables)
                        raise Exception('Copy to s3 failed %s: %s' % (tables, str(e)))

                    # Copy Dumpfile to s3
                    try:
                        # Write data file to S3
                        res = dw.s3_cp(logfile + '.gz', s3_location, aws_profile)
                        if res.returncode > 0:
                            raise Exception('Error Copying to S3 %s: %s' % (tables, str(res)))
                        else:

                            # Add to MANIFEST file
                            url_string = '{"url":"%s"}' % location
                            manifest_dict["entries"].append(url_string)
                            s3_url = '{"url":"%s"}' % location_schema
                            manifest_dict["entries"].append(s3_url)

                            # Remove data files. This helps with memory management
                            os.remove(logfile + '.gz')
                    except Exception as e:
                        logging.print_to_log('ERROR', current_script, 'Exception while moving to s3: %s' % e)
                        table_errors.append(tables)
                        raise Exception('Copy to s3 failed %s: %s' % (tables, str(e)))
                else:
                    # Version is test_mysql
                    # Copy Dumpfile to s3
                    try:
                        res = dw.s3_cp(logfile + '.gz', s3_location + tables + '/', aws_profile)
                        if res.returncode > 0:
                            raise Exception('Error Copying to S3 %s: %s' % (tables, str(res)))
                    except Exception as e:
                        logging.print_to_log('ERROR', current_script, 'Exception while moving to s3: %s' % e)
                        table_errors.append(tables)
                        raise Exception('Copy to s3 failed %s: %s' % (tables, str(e)))

                # Append tables that were successfully dumped. This list is returned to wrapper script
                successful_tables.append(tables)

                # Assuming this table was retried. Since retry was successful remove it from table_errors
                if tables in table_errors:
                    table_errors.remove(tables)

                if always_reload == 1:
                    tables_dict[tables] = always_reload
                else:
                    tables_dict[tables] = reload_table

                run_time_table = (datetime.now() - start_time).total_seconds() / 60

                logging.print_to_log('INFO', current_script, 'RUN_TIME -> %s: %s' % (tables, run_time_table))

                # End job for this table
                enclosure_queue.task_done()
                logging.print_to_log('INFO', current_script, 'Queue: Task Done -> %s' % tables)
                logging.print_to_log('INFO', current_script, 'Queue Size: %s' % str(enclosure_queue.qsize()))

        except Exception as e:
            table_errors.append(tables)
            logging.print_to_log('ERROR', current_script, 'Exception: %s' % e)
            logging.print_to_log('ERROR', current_script, 'Table: %s' % tables)
            logging.print_to_log('ERROR', current_script, 'Database Name: %s' % database_name)
            enclosure_queue.task_done()


# Controls how many parallel threads can run at the same time. Set in dimension_config.ini
thread_count = int(config.get('config', 'threads_dumper'))

for i in range(thread_count):
    worker = threading.Thread(target=download_tables, args=(enclosure_queue, i))
    worker.setDaemon(True)
    worker.start()


def send_to_queue():

    if use_schema_file:
        schema_file_tables = db_schema_file.sections()
    else:
        # Gets all dimensions from stats_fact_summary_tables
        new_tables = dim_db.get_tables(version)

    if cmd_arg.arg_opt['tables'] is not None:
        if use_schema_file:
            database_name = config.get('db_names', 'dim_db_1')
            delta = 1
            reload_table = 1
            view_name = ''
            always_reload = 1
            enclosure_queue.put(
            [cmd_arg.arg_opt['tables'], database_name, delta, reload_table, view_name, always_reload])
        else:
            database_name, delta, reload_table, view_name, always_reload = dim_db.get_mysql_db(cmd_arg.arg_opt['tables'])
            # Dumps a single table. Option passed via command line by using -t
            logging.print_to_log('INFO', current_script, 'Command Line Table: %s' % cmd_arg.arg_opt['tables'])
            # Put job/table in queue
            enclosure_queue.put(
                [cmd_arg.arg_opt['tables'], database_name, delta, reload_table, view_name, always_reload])
        logging.print_to_log('INFO', current_script, 'Queue Size: %s' % str(enclosure_queue.qsize()))

    elif len(table_errors) > 0:

        if use_schema_file:
            for row in schema_file_tables:
                table_name = row

                if table_name in table_errors:
                    logging.print_to_log('INFO', current_script, 'Retrying table: %s' % table_name)
                    table_name = row
                    database_name = config.get('db_names', 'dim_db_1')
                    delta = 0
                    reload_table = 1
                    view_name = ''
                    always_reload = 1
                    enclosure_queue.put([table_name, database_name, delta, reload_table, view_name, always_reload])
        else:
            for row in new_tables:
                table_name = row[0]

                # Only retry those tables that are in the table_errors list
                if table_name in table_errors:
                    logging.print_to_log('INFO', current_script, 'Retrying table: %s' % table_name)
                    database_name = row[1]
                    delta = row[2]
                    reload_table = row[3]
                    view_name = row[4]
                    always_reload = row[5]
                    enclosure_queue.put([table_name, database_name, delta, reload_table, view_name, always_reload])
                    logging.print_to_log('INFO', current_script, 'Queue Size: %s' % str(enclosure_queue.qsize()))
    else:
        if use_schema_file:
            for row in schema_file_tables:
                table_name = row
                database_name = config.get('db_names', 'dim_db_1')
                delta = 0
                reload_table = 1
                view_name = ''
                always_reload = 1
                enclosure_queue.put([table_name, database_name, delta, reload_table, view_name, always_reload])

        else:
            # Dumps all tables found in stats_fact_summary_tables
            for row in new_tables:
                table_name = row[0]
                database_name = row[1]
                delta = row[2]
                reload_table = row[3]
                view_name = row[4]
                always_reload = row[5]
                enclosure_queue.put([table_name, database_name, delta, reload_table, view_name, always_reload])
                logging.print_to_log('INFO', current_script, 'Queue Size: %s' % str(enclosure_queue.qsize()))


def main(custom_table=None, full_dump=None):

    if custom_table is not None:
        # Allows to dump a single table from dimension_controller
        cmd_arg.arg_opt['tables'] = custom_table

        if full_dump is not None:
            cmd_arg.arg_opt['full_table'] = full_dump

    send_to_queue()
    enclosure_queue.join()

    if len(table_errors) > 0:
        # If there are tables that failed, they will be sent back to the queue
        send_to_queue()
        enclosure_queue.join()

    # This ensures we only write the MANIFEST file when there are no failures
    if len(table_errors) == 0:
        if version != 'test_mysql':
            # Write MANIFEST File
            manifest = json.dumps(manifest_dict).replace('\\', '').replace('""', '"').replace('"{', '{').replace('}"', '}')
            with open('MANIFEST', 'w') as mani:
                mani.write(manifest)
                mani.close()

            # Upload MANIFEST to s3
            res = dw.s3_cp('MANIFEST', s3_location, aws_profile)

            if res.returncode > 0:
                logging.print_to_log('ERROR', current_script, 'Manifest to S3 Failed')
    else:
        if len(table_errors) > 0:
            mail.send_email_alert(from_address, email_address, 'Dimension Dumper Failed',
                                  msg='Dimension Dumper failed to dump/upload tables:\n%s' %
                                      ','.join(set(table_errors)))

    return successful_tables, s3_location, table_errors, not_updated, tables_dict


if __name__ == '__main__':
    start_time = datetime.now()

    # Write to run_log
    write_run_log('started', error_msg="", file_name="dimensions_to_s3_%s" % version)

    if version != 'test_mysql':
        if rerun is False:
            dim_db.write_to_dimension_load(now, 'STARTED', version)

    # Start main function
    main()

    if len(table_errors) > 0:
        logging.print_to_log('ERROR', current_script, 'Failed tables: %s' % list(set(table_errors)))
        write_run_log('error', error_msg="Tables Failed: %s" % ','.join(set(table_errors)),
                      file_name="dimensions_to_s3_%s" % version)

        if version != 'test_mysql':
            if rerun is True:
                dim_db.write_to_dimension_load(failed_time, 'FAILED', version, update=True)
            else:
                dim_db.write_to_dimension_load(now, 'FAILED', version, update=True)

        mail.send_email_alert(from_address, email_address, 'Dimension Dumper Failed',
                              msg='Dimension Dumper failed to dump/upload tables:\n%s' % ','.join(set(table_errors)))
    else:
        logging.print_to_log('INFO', current_script, 'Failed Tables: None')
        write_run_log('completed', error_msg="", file_name="dimensions_to_s3_%s" % version)

        if version != 'test_mysql':
            if rerun is True:
                dim_db.write_to_dimension_load(failed_time, 'COMPLETED', version, update=True)
            else:
                dim_db.write_to_dimension_load(now, 'COMPLETED', version, update=True)

    logging.print_to_log('INFO', current_script, 'Successful tables: %s' % successful_tables)
    run_time = (datetime.now() - start_time).total_seconds() / 60

    logging.print_to_log('INFO', current_script, 'Run Time ---------------------------------> %s' % run_time)
    logging.print_to_log('INFO', current_script, 'Dumper script complete')
