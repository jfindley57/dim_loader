import json
import logging
import saf_utils as saf


def init():

    global log

    log = logging.getLogger()


def get_keys_and_metrics_from_json(json_file, table_name, fact_table=True):

    init()

    json_file_cat = saf.run_cmd('cat %s' % json_file)

    if json_file_cat.returncode != 0:
        log.error('Could not get schema file')
        return False
    else:
        json_data = json.loads(json_file_cat.stdout)

    if fact_table is False:
        keys = []
        metrics = []
        db_name = []
        for js in json_data['tables']:
            if js["name"] == table_name:
                keys = js["keys"]
                metrics = js["metrics"]
                db_name = js["db_name"]
        return keys, metrics, db_name

    else:
        keys = []
        metrics = []
        db_name = ""
        db_name_stg = ""
        staging_table = None
        key_string = ""
        metric_string = ""
        column_string = ""
        column_string_insert = ""
        column_string_with_identity = None
        load_table = ""
        version = json_data['version']
        sql = False
        dimension_dict = {}
        join_sql = {}
        file_dict = {}

        for js in json_data['groups']:
            # Get table names for all groups
            for js_nest in js['tables']:
                # Iterate through all tables
                if js_nest["name"] == table_name:
                    try:
                        # Get Delimiter
                        delimiter = js_nest['delimiter']
                    except KeyError:
                        # If delimiter field is not found we will set as Vertical Bar
                        # This is standard for Vertica loading
                        delimiter = '|'

                    try:
                        # Get File Type
                        file_dict['file_format'] = js_nest['file_format']
                    except KeyError:
                        file_dict['file_format'] = None

                    try:
                        # Get File Type
                        file_dict['compression'] = js_nest['compression']
                        if file_dict['compression'] == '':
                            file_dict['compression'] = None
                    except KeyError:
                        file_dict['compression'] = None

                    try:
                        # If schema contains a Spark SQL query then we will process the key/metric in the
                        # order they appear in the schema
                        is_sql = js_nest['sql']
                        log.info('Processing keys/metrics as SQL Schema')
                        log.info('Sql Query: %s' % is_sql)
                        sql = True
                        columns_list = js_nest['columns']
                        for column in columns_list:
                            column_name = column['output_name']
                            column_string += column_name + ","

                            # This will be needed if joins are being used
                            # Creates a string of table.column
                            column_string_insert += js_nest['name'] + '.' + column_name + ','
                    except KeyError:

                        # If no Spark SQL is present then we will group the keys and metrics separately
                        log.info('Processing keys/metrics as non-SQL')
                        columns_list = js_nest['columns']
                        for column in columns_list:
                            field_type = column['field_type']
                            column_name = column['output_name']
                            if field_type == 'KEY':
                                key_string += column_name + ","
                            elif field_type == 'METRIC':
                                metric_string += column_name + ","
                            else:
                                log.error('Incorrect field_type: Not Key or Metric')

                    db_name = js_nest["db_name"]

                    # Get db_name_stg if there is one
                    try:
                        db_name_stg = js_nest['db_name_stg']
                    except KeyError:
                        log.warning('No Staging DB in Schema. Setting to None')
                        db_name_stg = None

                    try:
                        staging_table = js_nest['staging_table']
                        log.info('Staging Table: %s' % staging_table)
                    except KeyError:
                        pass

                    try:
                        # Check to see if there is a load_table option set
                        load_table = js_nest['load_table']
                    except KeyError:
                        log.warning('Load table set to None')
                        load_table = None

                    try:
                        dimension = js_nest['dimension']

                        # Setting is_dimension to True
                        dimension_dict['is_dimension'] = True

                        log.info('Treating %s as a dimension' % table_name)

                        try:
                            # Do we use merge statement for this dimension
                            if dimension['operation'] == 'Insert':
                                # Setting merge to True (python does not parse json as boolean)
                                dimension_dict['operation'] = 'Insert'
                            elif dimension['operation'] == 'Merge':
                                dimension_dict['operation'] = 'Merge'
                            else:
                                dimension_dict['operation'] = None
                        except KeyError:
                            dimension_dict['operation'] = None

                        try:
                            # Specifies merge column
                            if dimension['operation_column'] == '':
                                # Log error when Merge is True but no merge_column is specified
                                log.error('Merge set to True but no merge_column specified in schema')
                            else:
                                dimension_dict['operation_column'] = dimension['operation_column']

                        except KeyError:
                            dimension_dict['operation_column'] = None

                        try:
                            # We are making a string ending with the identity column
                            column_string_with_identity = column_string + dimension['identity_column']
                            dimension_dict['column_string_with_identity'] = column_string_with_identity
                        except KeyError:
                            # Setting column_string_with_identity to None
                            dimension_dict['column_string_with_identity'] = column_string_with_identity

                        try:
                            if dimension['truncate_before_load'] == 'true':
                                dimension_dict['truncate_before_load'] = True
                            else:
                                dimension_dict['truncate_before_load'] = False
                        except KeyError:
                            dimension_dict['truncate_before_load'] = False

                    except KeyError:
                        dimension_dict['is_dimension'] = False
                        dimension_dict['truncate_before_load'] = False
                        log.info('Not a dimension')

                    try:
                        # This section checks if there is a custom insert sql section of the schema
                        custom_insert_sql = js_nest['custom_insert_sql']
                        custom_sql = custom_insert_sql['replace_keys']

                        # Table to join with
                        custom_sql_table = custom_insert_sql['join_table']

                        # Key to join on
                        join_key = custom_insert_sql['join_key']

                        if column_string_insert.endswith(','):

                            column_string_insert = column_string_insert[:-1]
                            column_string_insert = column_string_insert.replace(custom_sql['unwanted_key'],
                                                                                custom_sql['wanted_key'])
                            join_sql['columns'] = column_string_insert
                            join_sql['join_table'] = custom_sql_table
                            join_sql['join_key'] = join_key

                    except KeyError as e:
                        join_sql = None

        if sql is False:

            if key_string.endswith(','):
                # Removes trailing ','
                key_string = key_string[:-1]

            if metric_string.endswith(','):
                # Removes trailing ','
                metric_string = metric_string[:-1]

            keys.append(key_string)
            metrics.append(metric_string)

            if metric_string != '':
                # Adding the key and metric strings together
                column_string += key_string + ',' + metric_string
            else:
                column_string += key_string
        else:
            if column_string.endswith(','):
                # Removes trailing ','
                column_string = column_string[:-1]

        if column_string == '':
            column_string = None
        else:
            log.info('Key/Metric String: %s' % column_string)

        return column_string, db_name, db_name_stg, staging_table, load_table, version, delimiter, dimension_dict, join_sql, file_dict
