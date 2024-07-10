import logging
import ConfigParser
import datetime

config = ConfigParser.ConfigParser()
config.read('dimension_config.ini')


logFormat = '%(asctime)s - %(levelname)s: %(message)s'
dateFormat = '%m/%d/%Y %I:%M:%S %p'
log_dir = config.get('config', 'log_dir')


def initLogging(location="", level=logging.INFO, file_name=None):

    current_time = datetime.datetime.now()

    if file_name is not None:

        if 'rerun' in file_name:
            # Write the rerun checks to a daily file
            filepath = log_dir + '%s_%s.log' % (file_name, current_time.strftime('%Y.%m.%d'))
        else:
            filepath = log_dir + '%s_%s.log' % (file_name, current_time.strftime('%Y.%m.%d-%H_%M'))
    else:
        filepath = log_dir + 'dimension_loader_%s.log' % current_time.strftime('%Y.%m.%d-%H_%M')

    logging.basicConfig(filename=filepath,
                        level=level,
                        format=logFormat,
                        datefmt=dateFormat)


def print_to_log(severity, source, message):
    if severity == "DEBUG":
        logging.debug("%s - %s" % (source, message))
    if severity == "INFO":
        logging.info("%s - %s" % (source, message))
    if severity == "WARNING":
        logging.warning("%s - %s" % (source, message))
    if severity == "ERROR":
        logging.error("**** %s - %s ****\n" % (source, message))
    if severity == "CRITICAL":
        logging.critical("%s - %s" % (source, message))