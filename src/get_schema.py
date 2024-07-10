import logging
import json
import collections
import ConfigParser
import dimension_db as dim_db
import dim_logging as logging
import os

logging.initLogging()
current_script = os.path.basename(__file__)

json_object = {}

config = ConfigParser.ConfigParser()
config.read('dimension_config.ini')


def get_ssm_schema():
    try:
        con = dim_db.dimension_ssm_init()

        # We version will hit the stats_summary_metadata.dimensions_load_table since we cannot add dimensions to the...
        # ... production schema at this time. Spark will try to aggregate whatever is in schema.json.
        con_query = "SELECT  DISTINCT table_name, stats_keys, measures, database_name  FROM \
        ( \
        SELECT 'all_keys_and_measures' AS table_name ,MAX(stats_keys) AS stats_keys, MAX(measures) AS measures, database_name\
        FROM (      \
             SELECT GROUP_CONCAT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(t.stats_keys, ',', n.n), ',', -1))  stats_keys , '' as measures, '' as database_name \
                 FROM    \
                        (\
                         SELECT DISTINCT  stats_keys \
                         FROM metadata.dimensions_load_table s\
                         WHERE s.is_loading_enabled = 1\
                        ) t\
                CROSS JOIN  \
                 (\
                   SELECT a.N + b.N * 10 + c.N * 100 + 1 n\
                     FROM \
                     (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a\
                   ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b\
                   ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c\
                    ORDER BY n\
                ) n\
                 WHERE n.n <=  (length(stats_keys )-length(replace(stats_keys ,',',''))) + 1 \
              UNION ALL\
                SELECT DISTINCT '' as stats_keys,GROUP_CONCAT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(t.measures, ',', n.n), ',', -1)) measures, '' as database_name\
                FROM    \
                        (\
                         SELECT DISTINCT measures \
                          FROM stats_summary_metadata.dimensions_load_table s\
                           WHERE s.is_loading_enabled = 1  \
                        ) t\
               CROSS JOIN  \
                (\
                   SELECT a.N + b.N * 10 + c.N * 100 + 1 n\
                     FROM \
                    (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a\
                   ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b\
                   ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c\
                    ORDER BY n\
                ) n\
                 WHERE n.n <=  (length(measures )-length(replace(measures ,',',''))) + 1  \
        UNION ALL\
            SELECT DISTINCT '' as stats_keys , '' as measures, database_name \
                FROM \
                        (\
                         select distinct database_name\
                         FROM stats_summary_metadata.dimensions_load_table s\
                         WHERE s.is_loading_enabled = 1\
                        ) t\
                CROSS JOIN  \
                  (\
                   SELECT a.N + b.N * 10 + c.N * 100 + 1 n\
                     FROM \
                     (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a\
                   ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b\
                   ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c\
                    ORDER BY n\
                 ) n\
              ) k_m\
        UNION ALL\
        SELECT DISTINCT table_name, stats_keys, measures, database_name  \
         FROM dimensions_load_table \
        WHERE is_loading_enabled = 1 \
         ) a           \
          ORDER BY table_name  DESC    ;"

        with con:
            cur = con.cursor()
            cur.execute(con_query)
            result = cur.fetchall()
            rowarray_list = []
        for row in result:
            d = collections.OrderedDict()
            d['name'] = row[0]
            d['keys'] = row[1].split(',')
            d['metrics'] = row[2]
            d['db_name'] = row[3]

            # direct_load.py will fail if all metrics = null in the schema.json file. If dimensions are added to
            # stats_fact_summary_table then this will no longer be an issue
            if d['metrics'] is None:
                d['metrics'] = ''

            rowarray_list.append(d)
        json_object['tables'] = rowarray_list
        json_data = json.dumps(json_object)
        logging.print_to_log('INFO', current_script, json_data)
        with open('schema.json', 'w') as f:
            f.write(json_data)
            return True
    except Exception as e:
        logging.print_to_log('ERROR', current_script, 'Exception: %s' % str(e))
        return False

if __name__ == '__main__':
    get_ssm_schema()
