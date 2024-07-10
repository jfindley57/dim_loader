from smtplib import SMTP
from email.mime.text import MIMEText
from email.Utils import formatdate
import dim_logging as logging
import os

current_script = os.path.basename(__file__)


def send_email_alert(notify_from, notify_to, subject, msg="---", notify_server='localhost'):
    logging.print_to_log('INFO', current_script, 'From: %s' % notify_from)
    logging.print_to_log('INFO', current_script, 'To: %s' % notify_to)
    logging.print_to_log('INFO', current_script, 'Subject: %s' % subject)
    logging.print_to_log('INFO', current_script, 'Message: %s' % msg)
    logging.print_to_log('INFO', current_script, 'Email Server: %s' % notify_server)
    message = MIMEText(msg, 'plain')
    message['Subject'] = subject
    message['Date'] = formatdate(localtime=True)
    email_server = SMTP(notify_server)
    email_server.sendmail(notify_from, notify_to, message.as_string())
    email_server.quit()





