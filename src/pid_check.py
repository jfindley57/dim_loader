import os
import sys
import dim_logging as logging
import subprocess
import datetime

logging.initLogging()
current_script = os.path.basename(__file__)

file_name = '/opt/dimension/var/run/dimension_loader/dimension_loader.pid'

pid_exist = os.path.isfile(file_name)

pid = str(os.getpid())
logging.print_to_log('INFO', current_script, 'Current PID is %s' % pid)


def create_pid(pid):
    logging.print_to_log('INFO', current_script, 'Writing to PID file')
    file(str(file_name), 'w').write(pid)


def remove_pid():
    logging.print_to_log("INFO", current_script, "Removing PID File: " + file_name)
    os.remove(file_name)


def kill_if_running():
    try:
        logging.print_to_log("INFO", current_script, "Attempting to kill process")
        f = open(file_name, 'r')
        # Getting pid number in file
        old_pid = f.read()
        if os.path.getsize(file_name) > 0:
            # Killing old process
            logging.print_to_log("INFO", current_script, "OLD PID: %s" % old_pid)
            os.kill(int(old_pid), 9)
            logging.print_to_log("INFO", current_script, "Process killed")
    except OSError:
            # No process found
            logging.print_to_log("INFO", current_script, "No Process Found")


def check_pid():
    if pid_exist is True:
        try:
            logging.print_to_log("INFO", current_script, "PID file already Exists!!")
            with open(file_name, 'r') as f:

                for lines in f:
                    old_pid = lines

            # Subprocess to get elapsed time of process based upon pid
            proc = subprocess.Popen(['ps', '-p', old_pid, '-o', 'etime'], stdout=subprocess.PIPE)

            # proc returns a string in a 00:00 format which changes to 00:00:00.
            # Using awk to format that data into seconds
            seconds = subprocess.check_output(['./seconds.awk'], stdin=proc.stdout)
            proc.wait()

            statbuf = os.stat(file_name)
            file_modified = statbuf.st_mtime
            since_change = datetime.datetime.fromtimestamp(file_modified)
            current_time = datetime.datetime.now()
            time_diff = current_time - since_change
            diff_in_minutes = time_diff.total_seconds() / 60

            if diff_in_minutes > 60:
                logging.print_to_log("INFO", current_script, "PID file was modified %s minutes ago" % diff_in_minutes)
                kill_if_running()
                remove_pid()
                create_pid(pid)
            else:

                if len(seconds) > 0:
                    minutes = int(seconds) / 60
                    logging.print_to_log("INFO", current_script, "PID running for %s minutes" % minutes)

                    if minutes > 60:
                        logging.print_to_log("INFO", current_script, "PID older than 1 hour. Going to kill")
                        kill_if_running()
                        remove_pid()
                        create_pid(pid)
                    else:
                        logging.print_to_log("INFO", current_script, "Loader is still running. Exiting!")
                        sys.exit(0)
                else:
                    remove_pid()
                    create_pid(pid)

        except Exception as e:
            logging.print_to_log("INFO", current_script, "Exception: %s" % e)
    else:
        # There is no prior pid file so one is created
        create_pid(pid)
    return True


if __name__ == '__main__':
    create_pid()