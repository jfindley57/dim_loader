#!/usr/bin/python

import os
import errno
import subprocess
import shlex
import fnmatch
import fcntl
import time
import smtplib
import socket
import sys
import logging
from collections import namedtuple

SSH_OPTS = "-o 'StrictHostKeyChecking no' -o 'ConnectTimeout 30' -o 'PasswordAuthentication no' -o 'ServerAliveInterval 300' -o 'ServerAliveCountMax 2'"

def run_cmd(cmd_str, print_cmd=True):

    if print_cmd:
        print cmd_str
    cmd = shlex.split(cmd_str)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    ProcOutput = namedtuple("ProcOutput", "stdout stderr returncode")
    proc_output = ProcOutput(stdout, stderr, proc.returncode)
    return proc_output


def run_cmd_without_waiting(cmd_str, print_cmd=True):

   if print_cmd:
       print cmd_str
   cmd = shlex.split(cmd_str)
   subprocess.Popen(cmd)


def ssh_call(user, server, ssh_cmd,  print_cmd=True):

    proc_output = run_cmd("ssh %s %s@%s '%s'" % (SSH_OPTS, user, server, ssh_cmd), print_cmd)
    return proc_output


def s3_call(s3_cmd, profile_name, print_cmd=True):

    if print_cmd:
        print s3_cmd
    proc_output = run_cmd("/usr/local/bin/aws s3 %s --profile %s" % (s3_cmd, profile_name))
    return proc_output


def ssh_mv(user, server, source, target):

    mv_cmd = 'mv %s %s' % (source, target)
    proc_output = ssh_call(user, server, mv_cmd)
    return proc_output


def s3_sync(user, server, source_bucket, target_bucket, profile_name, print_cmd=True):

    sync_cmd = 'aws s3 sync %s %s --profile %s' % (source_bucket, target_bucket, profile_name)
    proc_output = ssh_call(user, server, sync_cmd)
    return proc_output


def s3_mv(source_bucket, target_bucket, profile_name):

    mv_cmd = 'mv %s %s' % (source_bucket, target_bucket)
    proc_output = s3_call(mv_cmd, profile_name)
    return proc_output


def s3_cp(source_file, target_bucket, profile_name):
    # copies file to s3 bucket
    cp_cmd = 'cp %s %s ' % (source_file, target_bucket)
    proc_output = s3_call(cp_cmd, profile_name)
    return proc_output


def ssh_cp(user, server, source, target):

    cp_cmd = 'cp -r %s %s' % (source, target)
    proc_output = ssh_call(user, server, cp_cmd)
    return proc_output


def ssh_rm(user, server, directory):

    rm_cmd = 'rm -rf %s' % (directory)
    proc_output = ssh_call(user, server, rm_cmd)
    return proc_output


def ssh_ls(user, server, directory):

    ls_cmd = 'ls -A %s | sort' % (directory)
    proc_output = ssh_call(user, server, ls_cmd)
    return proc_output


def s3_ls(s3bucket,profile_name):
    ls_cmd = 'ls %s ' %(s3bucket)
    proc_output = s3_call(ls_cmd,profile_name)
    return proc_output


def ssh_rsync_pull(ssh_user, src_server, src_dir, target_server, target_dir):

    rsync_cmd = 'rsync -avzp --progress --timeout=1800 rsync://%s%s %s' % (src_server, src_dir, target_dir)
    proc_output = ssh_call(ssh_user, target_server, rsync_cmd)
    return proc_output


def mkdir(directory):

    try:
        os.makedirs(directory)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            return False
        raise
    return True


def lock_and_execute(func, *args, **kwargs):

    """Tries to obtain a file lock before running function. If file lock cannot be obtained, logs it and exits"""
    lockfile = os.path.join(RUN_DIR, "daily_aggregator.lock")
    fp = open(lockfile, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        _log.info("Another instance of the script is running. Could not execute %s." % (func.__name__))
        sys.exit(1)
    func(*args, **kwargs)


def set_config(config, filename, silent):

    d = {}
    try:
        execfile(filename, d)
    except IOError as e:
        if silent and e.errno in (errno.ENOENT, errno.EISDIR):
            return False
        raise

    for key in d:
        if key.isupper():
            config[key] = d[key]
    return True


def _lock_and_execute_with_given_lock(func, lock_path, _log, *args, **kwargs):
    """Tries to obtain a file lock before running function. If file lock cannot be obtained, logs it and exits"""
    fp = open(lock_path, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        _log.info("Another instance of the script is running. Could not execute %s." % (func.__name__))
        sys.exit(1)

    func(*args, **kwargs)


def _rm(filename):
    try:
        os.unlink(filename)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return False
        raise
    return True


def _cleanup_directory(directory, pattern, num_days_archive, _log):
    _log.info('Cleaning up directory %s' % (directory))
    for f in os.listdir(directory):
        filename = os.path.join(directory, f)
        if fnmatch.fnmatch(f, pattern) and os.path.isfile(filename) and os.stat(filename).st_mtime < time.time() - (num_days_archive * 86400):
            _log.info('Deleting file %s' % (filename))
            _rm(filename)
    _log.info('Done cleaning up directory %s' % (directory))


def _notify_error(error_msg, from_add, to_add, _log):
    _log.error(error_msg)
    _smtp = smtplib.SMTP('localhost')
    email_message = "Subject: [%s] Error on %s\n%s" % (os.path.basename(__file__),
                                                       socket.gethostname(),
                                                       error_msg)
    try:
        _smtp.sendmail(from_add, to_add, email_message)
    except:
        _log.error("Error sending email:", sys.exc_info()[0])
    finally:
        _smtp.quit()


def get_logger(log_prefix):

    FORMAT = "%(asctime)-15s |%(levelname)s| %(message)s"

    script_dir_path = os.path.dirname(os.path.abspath(__file__))
    dir_name = os.path.basename(script_dir_path)

    log_dir = os.path.join(script_dir_path, "../var/log/", dir_name)
    logfile = os.path.join(log_dir, "%s.%s.log" % (log_prefix, time.strftime("%Y-%m-%d")))
    log_symlink = os.path.join(log_dir, "%s.log" % log_prefix)

    try:
        os.makedirs(log_dir)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise e

    logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename=logfile)

    try:
        os.symlink(logfile, log_symlink)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise e
        else:
            os.unlink(log_symlink)
            os.symlink(logfile, log_symlink)

    return logging.getLogger()


#### Test functions ####
def test_ssh_ls():

    print "Test function"


if __name__ == '__main__':

    test_ssh_ls()
