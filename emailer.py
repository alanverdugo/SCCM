#!/usr/bin/env python
'''
    Usage:
        import emailer
        or
        import send_email from emailer

    Description:
        This program is intended to be used as a Python module. The send_email 
        function should be used by different programs that need to send 
        notification emails (E.g. csr_checker.py and sccm_error_log_emailer.py).
        The fact that this is an independent function allows to have a better 
        control and easier maintenance while editing this code (instead of 
        having the same function replicated in several .py files).

    Autor:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2017-05-09 (?)

    Modification list:
        CCYY-MM-DD  Autor           Description
        2017-06-23  Alan Verdugo    Improved path string concatenation.
                                    Implemented logging functionality.
                                    Minor improvements.
'''

# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# For reading the dist, list.
import json

# Email modules we will need.
from email.mime.text import MIMEText

# The actual email-sending functionality.
import smtplib

# Handle output.
import logging


# Home of the SCCM installation.
sccm_home = os.path.join("/opt", "ibm", "sccm")

# The path where this script and the dist. list are.
binary_home = os.path.join(sccm_home, "bin", "custom")

# The full path of the actual distribution list file.
mail_list_file = os.path.join(binary_home, "mailList.json")

# The hostname of the SMTP server.
smtp_server = "localhost"

# Logging configuration.
log = logging.getLogger("emailer")
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
log.setLevel(logging.INFO)


def send_email(distribution_group, email_subject, email_from, results_message):
    try:
        mail_list = json.load(open(mail_list_file, "r+"))
        for email_groups in mail_list["groups"]:
            if email_groups["name"] == distribution_group:
                email_to = email_groups["members"]
    except Exception as exception:
        log.error("Cannot read email recipients list. {0}".format(exception))
        sys.exit(1)
    msg = MIMEText(results_message,"plain")
    msg["Subject"] = email_subject
    s = smtplib.SMTP(smtp_server)
    try:
        s.sendmail(email_from, email_to, msg.as_string())
        s.quit()
    except Exception as exception:
        log.error("Unable to send notification email. {0}".format(exception))
        sys.exit(1)
    else:
        log.info("Notification email sent to {0}".format(email_to))
        exit (0)
