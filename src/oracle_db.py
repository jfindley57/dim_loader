import cx_Oracle
import sys
import dim_logging as logging
import os
import ConfigParser
import cmd_args as cmd_arg
from fastavro import writer, parse_schema, schemaless_writer


def get_pwd(db_path):
    f = open(db_path, 'r')
    pwd = f.read().strip()
    f.close()
    return pwd


current_script = os.path.basename(__file__)
config = ConfigParser.ConfigParser()
config.read(cmd_arg.arg_opt['config_file'])
version = config.get('config', 'version')
one_mobile = ConfigParser.ConfigParser()
one_mobile.read('%s_config_schema.ini' % version)

db_host = config.get('db_hosts', 'dim_db_1')
db_name = config.get('db_names', 'dim_db_1')
db_user = config.get('config', 'mysql_user_ro')
db_pass = get_pwd(config.get('config', 'conf_dir') + config.get('db_pass', 'dim_db_1'))
table = cmd_arg.arg_opt['tables']
data_dir = config.get('config', 'data_dir')
delimiter = config.get('config', 'delimiter')


def query_orcl(oracle_table):

    query = one_mobile.get(oracle_table, 'columns')
    dsn_tns = cx_Oracle.makedsn(db_host, 1521, db_name)

    connection = cx_Oracle.connect(db_user, db_pass, dsn=dsn_tns)

    curs = connection.cursor()

    orcl_query = "select %s from %s" % (query, oracle_table)

    logging.print_to_log('INFO', current_script, 'ORCL Query: %s' % orcl_query)

    try:
        curs.arraysize = 100000
        curs.setinputsizes()
        curs.execute(orcl_query)

        schema = {}
        parsed_schema = parse_schema(schema)
        with open('%s/%s.csv' % (data_dir, table), 'wt', buffering=1) as f:
            while True:
    
                rows = curs.fetchmany()

                if not rows:
                    break
    
                for row in rows:
    
                    row_list = list(row)
    
                    # Replace None with empty string
                    replace_none = ['' if v is None else v for v in row_list]

                    # Changes the delimiter to ctrl-A
                    final_string = delimiter.decode('hex').join(str(x) for x in replace_none) + '\n'

                    # Removing return
                    final_string.replace('\r', '')

                    #f.write(final_string)
                    schemaless_writer(f, parsed_schema, final_string)
                    f.flush()

    except Exception as e:
        logging.print_to_log('ERROR', current_script, 'Exception(query_orcl): %s' % str(e))
        return None


def get_schema(table, query):

    dsn_tns = cx_Oracle.makedsn(db_host, 1521, db_name)

    connection = cx_Oracle.connect(db_user, db_pass, dsn=dsn_tns)

    try:
        curs = connection.cursor()
        query_decode = query.split(',')

        query_decode1 = '"{0}"'.format('","'.join(query_decode))
        query_decode1.split(',')

        query_string = (",".join("'" + item.replace(' ', '') + "'" for item in query_decode))

        index_list = []

        for item in query_decode:
            # Make a list of the query_decode index
            index_list.append(query_decode.index(item) + 1)

        query_decode_with_index = zip(query_decode, index_list)

        # The goal of this code is to have the output order as the column_name in statement
        query_decode_with_index_list = []
        for val in query_decode_with_index:
            # Removes whitespace that causes error
            query_decode_with_index_list.append(val[0].replace(' ', ''))
            query_decode_with_index_list.append(val[1])

        # Makes into string with a mix of str and int
        final_query_string = (",".join(repr(e) for e in query_decode_with_index_list))

        orcl_query = "SELECT CONCAT(TABLE_NAME , concat(' ' , concat(COLUMN_NAME, concat(' - - - - - ', " \
                     "concat(DATA_TYPE,' - \"ends ''\\001\''\"\'))))) FROM all_tab_columns where TABLE_NAME = '%s' and " \
                     "COLUMN_NAME in (%s) order by decode(column_name, %s)" % (table, query_string, final_query_string)

        logging.print_to_log('INFO', current_script, 'ORCL Query: %s' % orcl_query)

        curs.execute(orcl_query)

        return curs.fetchall()
    except Exception as e:
        logging.print_to_log('ERROR', current_script, 'Exception(get_schema): %s' % str(e))


if __name__ == '__main__':
    query_orcl(table)