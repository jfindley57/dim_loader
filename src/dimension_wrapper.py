
import subprocess
import dim_logging as logging
import ConfigParser
import dw_tools as dw
import os
import get_schema as schema
from datetime import datetime
import pytz
import sys
import dimension_db as dim_db
import email_utils as mail
import threading
import Queue
from datadog import initialize, api
import pid_check
import cmd_args as cmd_arg


def get_pwd(db_path):
    f = open(db_path, 'r')
    pwd = f.read().strip()
    f.close()
    return pwd


custom_table = cmd_arg.arg_opt['tables']

logging.initLogging()
current_script = os.path.basename(__file__)
tz_utc = pytz.utc
timestamp = datetime.utcnow()
time_to_be_processed = tz_utc.localize(timestamp)
hour_to_be_processed = int((time_to_be_processed - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds() / 3600)
date_string = datetime.utcfromtimestamp(hour_to_be_processed * 3600).strftime('%Y/%m/%d/%H')
now = date_string

# Write now to file
# This will be used by dimension_dumper
# This is to ensure that we have the correct hour given a time change occurs
with open('date_string', 'w') as ds:
    ds.write(now)

# Data comes from dimension_config.ini
config = ConfigParser.ConfigParser()
config.read(cmd_arg.arg_opt['config_file'])
conf_dir = config.get('config', 'conf_dir')
datadog_api = get_pwd(conf_dir + config.get('config', 'datadog_api'))
datadog_app = get_pwd(conf_dir + config.get('config', 'datadog_app'))

options = {
        "api_key": datadog_api,
        "app_key": datadog_app
    }

initialize(**options)

startTime = datetime.now()
s3_bucket = config.get('config', 'metadata')
s3_location = s3_bucket + now + '/'
ssh_user = config.get('config', 'ssh_user')
dbload_path = config.get('config', 'dbload_path')
vertica_ip = config.get('config', 'vertica_ip')
vertica_url = config.get('config', 'vertica_url')
aws_profile = config.get('config', 'aws_profile')
email_address = config.get('config', 'email_address')
from_address = config.get('config', 'from_address')

# List to populate successful/failed dimensions
successful_dimensions = []
failed_dimensions = []

enclosure_queue = Queue.Queue()

# Make list from config file
vertica_ip = vertica_ip.split(',')
vertica_url = vertica_url.split(',')
# Create a dictionary from two lists
vertica_dict = dict(zip(vertica_ip, vertica_url))


def remove_locks(table):
    # Function to remove locks as occasionally direct_load fails to remove locks when direct_load fails
    lock_file = '/home/dimension/var/run/LOAD/LOAD_%s_%s.lock' % (table, hour_to_be_processed)
    try:
        os.remove(lock_file)
        logging.print_to_log('INFO', current_script, 'Lock file was removed')
    except Exception as e:
        pass


def run_dumper(custom_table=None, full_table=False):
    results = dumper.main(custom_table, full_table)

    return results


def download_schema():
    # Attempt to get schema from get_schema.py
    if schema.get_ssm_schema() is True:
        try:
            logging.print_to_log('INFO', current_script, 'Schema dump successful')

            # Move schema file to S3
            dw.s3_cp('schema.json', s3_location + 'schema/', aws_profile)

            # Data Max UTC Hour file
            data_max_file = 'part-00000-data_max_utc_hour.csv'
            with open(data_max_file, 'w') as data_max_utc_hr:
                # Write current hour to be processed
                data_max_utc_hr.write(str(hour_to_be_processed))
                data_max_utc_hr.close()

            # Copy data_max_file to S3
            dw.s3_cp(data_max_file, s3_location + 'data_max_utc_hour/', aws_profile)

            return True

        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception Occurred: %s' % e)
            mail.send_email_alert(from_address, email_address, 'Dimension Wrapper Failed to Move Schema to S3',
                                  msg='Exception: %s' % str(e))
            sys.exit(1)
    else:
        logging.print_to_log('ERROR', current_script, 'Schema NOT UPDATED')
        logging.print_to_log('ERROR', current_script, 'System Exiting')
        mail.send_email_alert(from_address, email_address, 'Dimension Wrapper Failed to Get Schema',
                              msg='Dimension Wrapper Failed to generate schema')
        sys.exit(1)


def sync_and_move(tables):
    # This function will sync from s3 to vertica file-server
    preinput = '%s/preinput/' % dbload_path
    input_dir = '%s/input/' % dbload_path
    data_dir = now + '/data/'
    metadata_dir = now + '/metadata/schema/'
    data_max_dir = now + '/metadata/data_max_utc_hour/'
    error_dir = '%s/error/' % dbload_path
    logging.print_to_log('INFO', current_script, 'Move Table: %s' % str(tables))

    for servers in vertica_dict:
        # Iterates through vertica ip-addresses

        # Clean up just in case
        remove_input(tables, servers, directory='in_progress')

        try:
            # Sync schema file from s3 to node - schema directory
            logging.print_to_log('INFO', current_script, 'Attempting to Sync Table: %s' % str(tables))
            dw_proc = dw.s3_sync(ssh_user, servers, s3_location + 'schema/', preinput +
                                 tables + '/' + metadata_dir, aws_profile)
            if dw_proc.returncode != 0:
                failed_dimensions.append([tables, servers])
                logging.print_to_log('ERROR', current_script, 'S3_Sync Failed for Schema: %s' % dw_proc.returncode)
                return False
            else:
                logging.print_to_log('INFO', current_script, 'Sync Return Code: %s | Server: %s ' % (
                    dw_proc.returncode, servers))

        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception on dw.s3_sync: %s' % str(e))
            mail.send_email_alert(from_address, email_address, 'Dimension Wrapper Failed to Sync Schema',
                                  msg='Exception: %s\n'
                                      'Table: %s\n'
                                      'Host: %s' % (str(e), tables, servers))

        try:
            # Sync data_max_utc file from s3 to node - data_max_utc_hour directory
            dw_proc = dw.s3_sync(ssh_user, servers, s3_location + 'data_max_utc_hour/', preinput +
                                 tables + '/' + data_max_dir, aws_profile)
            if dw_proc.returncode != 0:
                logging.print_to_log('ERROR', current_script, 'S3_Sync Failed for Data Max: %s' % dw_proc.returncode)
                return False

        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception on dw.s3_sync: %s' % str(e))

        try:
            # Sync dump-files from s3 to node - data directory
            dw_proc = dw.s3_sync(ssh_user, servers, dimension_dumper[1] + tables + '/',
                                 preinput + tables + '/' + data_dir, aws_profile)
            if dw_proc.returncode != 0:
                logging.print_to_log('ERROR', current_script, 'S3_Sync Failed for Dimension %s with Error: %s' % (
                    tables, dw_proc.returncode))
                return False

        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception on dw.s3_sync: %s' % str(e))
            mail.send_email_alert(from_address, email_address, 'Dimension Wrapper Failed to Sync Dump File',
                                  msg='Exception: %s\n'
                                      'Table: %s\n'
                                      'Host: %s' % (str(e), tables, servers))

        try:
            # Move preinput files to input directory
            logging.print_to_log('INFO', current_script, 'Attempting to Move Table: %s' % str(tables))
            dw_move = dw.ssh_mv(ssh_user, servers, preinput + tables + '/', input_dir)

            if dw_move.returncode != 0:
                logging.print_to_log('ERROR', current_script, 'DW Move Failed: %s' % dw_move.returncode)
                dw_move = dw.ssh_mv(ssh_user, servers, preinput + '*', error_dir)
                mail.send_email_alert(from_address, email_address, 'Dimension Loader Failed for %s' % tables)
                return False
            else:
                logging.print_to_log('INFO', current_script, 'Move to Input Complete | Server: %s ' % servers)

        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception on dw.s3_mv: %s' % str(e))
            enclosure_queue.task_done()
            remove_locks(tables)
            mail.send_email_alert(from_address, email_address, 'Dimension Wrapper Failed to Move to Input',
                                  msg='Exception: %s\n'
                                      'Table: %s\n'
                                      'Host: %s' % (str(e), tables, servers))
    return True


def remove_input(tables, ssh_server, directory='input'):
    # In case of error, remove input/in_progress directories.
    if directory != 'input':
        in_progress = dbload_path + '/in_progress/' + tables
        in_progress_output = dw.ssh_call(ssh_user, ssh_server, 'rm -rf ' + in_progress)
        logging.print_to_log('INFO', current_script, 'Removing Dimension In Progress Directory: %s' % str(tables))
        logging.print_to_log('INFO', current_script, 'Removing Dimension Dir Return Code: %s' % str(
            in_progress_output.returncode))
    input_dir = dbload_path + '/input/' + tables
    logging.print_to_log('INFO', current_script, 'Removing Dimension Input Directory: %s' % str(tables))
    proc_output = dw.ssh_call(ssh_user, ssh_server, 'rm -rf ' + input_dir)
    logging.print_to_log('INFO', current_script, 'Removing Dimension Dir Return Code: %s' % str(proc_output.returncode))


def s3_transfer_to_node(que, job_id):
    while True:
        tables, reload_table = que.get()
        logging.print_to_log('INFO', current_script, 'Queue Size: %s' % str(enclosure_queue.qsize()))

        if load_to_vertica(tables, reload_table) is True:
            # Dimension will be set to complete if all vertica clusters have been successfully updated
            dim_db.write_to_dimension(tables, True)
            successful_dimensions.append(tables)
            dim_db.set_latest_loaded(tables)
            enclosure_queue.task_done()
            logging.print_to_log('INFO', current_script, 'Dimension %s Task is Done' % tables)
        else:
            logging.print_to_log('ERROR', current_script, 'Dimension Load Failed for %s' % tables)
            remove_locks(tables)
        logging.print_to_log('INFO', current_script, 'Failed Dimensions: %s' % failed_dimensions)


# Number of threads to run in parallel. Set in dimension_config.ini
thread_count = int(config.get('config', 'threads_wrapper'))

for i in range(thread_count):
    worker = threading.Thread(target=s3_transfer_to_node, args=(enclosure_queue, i))
    worker.setDaemon(True)
    worker.start()


def sent_to_queue():
    # Getting tables that were successfully dumped from get_schema.py
    get_tables = dimension_dumper[4]
    for tables in get_tables.iterkeys():
        reload_status = get_tables[tables]
        # Putting tables into Queue
        enclosure_queue.put([tables, reload_status])


def load_to_vertica(tables, reload_tables):
    # load_result is being used to judge if any table failed to load accross all vertica clusters
    load_result = True

    for ip, url in vertica_dict.iteritems():
        logging.print_to_log('INFO', current_script, 'Attempting to Load Dimension: %s' % tables)

        # Call direct_load script to load
        if reload_tables == 1 or cmd_arg.arg_opt['full_table'] is True:
            # For this direct_load call we will delete the dimension.{table} database.
            vertica_query = 'python vertica_load.py  -l %s -n "ANY NODE" -f %s  -d %s  -v %s  -o %s -t %s -p %s ' \
                            '-e dimensions -r' % (ip, ip, dbload_path, dbload_path, url, tables, hour_to_be_processed)
        else:
            # For this direct_load call we will merge from staging to dimensions
            vertica_query = 'python vertica_load.py  -l %s -n "ANY NODE" -f %s  -d %s  -v %s  -o %s -t %s -p %s ' \
                            '-e dimensions -q test_mysql --load_from_s3' % (ip, ip, dbload_path, dbload_path, url, tables, hour_to_be_processed)

        logging.print_to_log('INFO', current_script, '********Vertica Query: %s**********' % str(vertica_query))
        vertica_load = subprocess.Popen(vertica_query, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stderr = vertica_load.stderr.read()
        vertica_load.communicate()
        logging.print_to_log('INFO', current_script, 'Vertica Load Return Code: %s' % str(vertica_load.returncode))

        if vertica_load.returncode == 0:
            logging.print_to_log('INFO', current_script, 'Dimension %s loaded successfully' % tables)
            remove_locks(tables)
        else:
            logging.print_to_log('ERROR', current_script, 'Vertica Load Return ERROR Code: %s' % str(
                vertica_load.returncode))
            logging.print_to_log('ERROR', current_script, 'Vertica Load Failed')
            failed_dimensions.append([tables, ip])

            remove_locks(tables)
            load_result = False
            enclosure_queue.task_done()
            mail.send_email_alert(from_address, email_address, 'Dimension Wrapper Failed to Load Dimension',
                                  msg='Table: %s\n'
                                      'Host: %s\n'
                                      'Exception: %s' % (tables, url, stderr))

    if load_result is True:
        return True
    else:
        return False


if __name__ == '__main__':
    import dimensions_to_s3 as dumper

    startTime = datetime.now()

    # Check if Dimension Loader is still running
    pid_check.check_pid()

    if download_schema() is True:
        dimension_dumper = run_dumper()
        sent_to_queue()
        enclosure_queue.join()
        logging.print_to_log('INFO', current_script, 'Successful Dimensions: %s' % successful_dimensions)
        logging.print_to_log('INFO', current_script, 'Failed Dimensions: %s' % failed_dimensions)
        logging.print_to_log('INFO', current_script, 'Dimensions with No New Data: %s' % dimension_dumper[3])
        run_time = (datetime.now() - startTime).total_seconds() / 60
        logging.print_to_log('INFO', current_script, 'Run Time: %s' % run_time)
        pid_check.remove_pid()
    else:
        logging.print_to_log('ERROR', current_script, 'Dumper and load has failed to run')
        run_time = (datetime.now() - startTime).total_seconds() / 60
        logging.print_to_log('INFO', current_script, 'Run Time: %s' % run_time)
        pid_check.remove_pid()

    res = api.Metric.send(metric='dimension_loader.run_time', points=run_time)