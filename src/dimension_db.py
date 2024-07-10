import pymysql
import dim_logging as logging
import os
import ConfigParser
import cmd_args as cmd_arg
import random

current_script = os.path.basename(__file__)
config = ConfigParser.ConfigParser()
config.read(cmd_arg.arg_opt['config_file'])


def get_pwd(db_path):
    f = open(db_path, 'r')
    pwd = f.read().strip()
    f.close()
    return pwd


logging_table = config.get('config', 'logging_table')


log_host = config.get('config', 'log_host')
log_user = config.get('config', 'log_user')
log_pwd = 'local'
user = config.get('config', 'mysql_user_ro')

version = config.get('config', 'version')

if (version == 'test_mysql') or (version == 'test_data'):
    config_host_list = config.get('db_hosts', 'dim_db_1').split(',')
    config_host = random.choice(config_host_list)
    config_pass_file = config.get('db_pass', 'dim_db_1')
    config_pass = get_pwd(config.get('config', 'conf_dir') + config_pass_file)

    billing_host_list = config.get('db_hosts', 'dim_db_2').split(',')
    billing_host = random.choice(billing_host_list)
    billing_pass_file = config.get('db_pass', 'dim_db_2')
    billing_pass = get_pwd(config.get('config', 'conf_dir') + billing_pass_file)

    dim_load_table = config.get('config', 'dim_load_table')
    dimensions_load_table_host = config.get('config', 'dimensions_load_table_host')
    dimensions_load_table_host_user = config.get('config', 'dimensions_load_table_host_user')
    dimensions_load_table_pass_file = config.get('config', 'dimensions_load_table_pass')
    dimensions_load_table_pass = get_pwd(config.get('config', 'conf_dir') + dimensions_load_table_pass_file)

else:
    generic_host = config.get('db_hosts', 'dim_db_1')
    core_pass_file = config.get('db_pass', 'dim_db_1')
    core_pass = get_pwd(config.get('config', 'conf_dir') + core_pass_file)


def dimension_init():
    con = pymysql.connect(host=log_host,
                          user=log_user,
                          passwd=log_pwd,
                          db="log",
                          autocommit=True)
    return con


def dimension_ssm_init():
    con = pymysql.connect(host=dimensions_load_table_host,
                          user=dimensions_load_table_host_user,
                          passwd=dimensions_load_table_pass,
                          db="stats_summary_metadata",
                          autocommit=True)
    return con


def dimension_config_init():
    con = pymysql.connect(host=config_host,
                          user=user,
                          passwd=config_pass,
                          db="config",
                          autocommit=True)
    return con


def dimension_billing_init():
    con = pymysql.connect(host=billing_host,
                          user=user,
                          passwd=billing_pass,
                          db="billing",
                          autocommit=True)
    return con


def dimension_partner_app_init():
    con = pymysql.connect(host=billing_host,
                          user=user,
                          passwd=billing_pass,
                          db="partner_apps",
                          autocommit=True)
    return con


def generic_init(db):
    con = pymysql.connect(host=generic_host,
                          user=user,
                          passwd=core_pass,
                          db=db,
                          autocommit=True)
    return con


def get_dimension_update(table):
    # Gets data from log.dimension_update_log
    con = dimension_init()
    logging.print_to_log('INFO', current_script, 'Getting dimension update')
    query = "select update_date from " + logging_table + " where table_name = '" + table + "' and uploaded_to_vertica " \
            "= 'complete' order by update_date desc limit 1"
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            cur.execute(query)
            row = cur.fetchone()
            if row is None:
                return None
            for data in row:
                logging.print_to_log('INFO', current_script, 'Update Time: %s' % data)
                data = data.strftime('%Y-%m-%d %H:%M:%S')
                data = "'" + data + "'"
                logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
                return data
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def write_to_dimension(table, update=False):
    # Writes data to dimension_update_log
    con = dimension_init()
    if update is True:
        # Set table to complete when dimension loading was successful
        query = "Update " + logging_table + " SET uploaded_to_vertica = 'complete' where table_name = '" + table +\
                "' and  uploaded_to_vertica = 'downloaded' order by update_date desc limit 1"
    else:
        # Inserts new dimension entry when dumping from prod db
        query = 'Insert INTO ' + logging_table + ' (table_name, uploaded_to_vertica) values ("' + table + '", ' \
            '"downloaded")'
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return data
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def write_to_dimension_load(hour, status, platform, update=False):
    # Writes data to dimension_load_log
    con = dimension_init()
    if update is True:
        # Set table to complete when dimension loading was successful
        query = "UPDATE dimension_load_log SET status = '%s' where run_time = %s and platform = '%s'" % (status, hour, platform)
    else:
        # Inserts new dimension entry when dumping from prod db
        query = "INSERT INTO dimension_load_log (run_time, status, platform) values (%s,'%s','%s')" % (hour, status, platform)
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return data
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def dimension_load_failed_times(platform, now_time):
    # Gets failed run_hour from dimension_load_log
    con = dimension_init()

    # Change time_subtraction based upon frequency
    try:
        frequency = config.get('config', 'frequency')
    except ConfigParser.NoOptionError as e:
        frequency = None

    if frequency == 'hourly':
        time_subtraction = 1
    else:
        time_subtraction = 15

    # Select run_time from dimension_load_log
    # Adding status in 'Started'. Sometimes the script fails without putting Failed status
    query = "SELECT run_time from dimension_load_log where status in ('FAILED', 'STARTED') and platform = '%s' " \
            "and run_time < (%s - %s)" % (platform, now_time, time_subtraction)
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)

            # get failed time from dimension_load_log
            failed_hour = cur.fetchone()
            if failed_hour is None:
                return None
            else:
                logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
                return failed_hour[0]
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def get_columns(table):
    con = dimension_ssm_init()
    query_mysql_keys = "select mysql_keys from %s where table_name = '%s'" % (dim_load_table, table)
    query = "select stats_keys from %s where table_name = '%s'" % (dim_load_table, table)
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query_mysql_keys)
            data_mysql_keys = cur.fetchone()

            if data_mysql_keys[0] is None:
                data = cur.execute(query)
                data_stats_keys = cur.fetchone()
                logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
                return data_stats_keys[0]
            else:
                return data_mysql_keys[0]

        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def get_tables(version='test_mysql'):
    con = dimension_ssm_init()
    if version == 'test_mysql':
        query = "select table_name, mysql_db, is_delta, reload_table, view_name, always_reload " \
                "from %s where  database_name = 'dimensions' and is_loading_enabled = 1" % dim_load_table
    else:
        query = "select table_name, mysql_db, is_delta, reload_table, view_name, always_reload " \
                "from %s where  database_name = 'dimensions' and status = 'LIVE'" % dim_load_table
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            data1 = cur.fetchall()
            table_list = []
            for rows in data1:
                table_list.append((rows[0], rows[1], rows[2], rows[3], rows[4], rows[5]))
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return table_list
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def get_mysql_db(table):
    con = dimension_ssm_init()
    query = "select mysql_db, is_delta, reload_table, view_name, always_reload " \
            "from %s where table_name = '%s'" % (dim_load_table, table)
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            data_string = cur.fetchone()
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return data_string[0], data_string[1], data_string[2], data_string[3], data_string[4]
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def set_latest_loaded(table):
    con = dimension_ssm_init()
    query = "UPDATE %s SET latest_loaded_date = now(), reload_table = 0 " \
            "where table_name = '%s'" % (dim_load_table, table)
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return data
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def set_reload_table(table):
    con = dimension_ssm_init()
    query = "UPDATE %s SET reload_table = 1 where table_name in ('%s')" % (dim_load_table, table)
    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return data
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))


def generate_schema(table, columns, database):

    if database == 'config':
        con = dimension_config_init()

    elif database == 'billing':
        con = dimension_billing_init()

    elif database == 'partner_apps':
        con = dimension_partner_app_init()

    else:
        con = generic_init(database)

    query = "SELECT CONCAT(TABLE_NAME , ' ' ,  COLUMN_NAME,  ' - - - - - ' , DATA_TYPE , " \
            "' - \"ends \\\'\\\\001\\\'\"\'  ) FROM information_schema.columns where TABLE_NAME = '%s' and " \
            "COLUMN_NAME in (%s) order by field(column_name, %s);" % (table, columns, columns)

    with con:
        try:
            logging.print_to_log('DEBUG', current_script, 'Query: ' + query)
            cur = con.cursor()
            data = cur.execute(query)
            data_res = cur.fetchall()
            logging.print_to_log('DEBUG', current_script, 'Data: ' + str(data))
            return data_res
        except Exception as e:
            logging.print_to_log('ERROR', current_script, 'Exception has occurred: ' + str(e))
