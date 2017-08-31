#!/usr/bin/env python
'''
    Usage:
        import emailer
        or
        import send_email from emailer

        usage: emailer.py [-h] -r DISTRIBUTION_GROUP -s EMAIL_SUBJECT 
            -f EMAIL_FROM -m MESSAGE_BODY [-a ATTACHMENT [ATTACHMENT ...]]

        Examples:
            python emailer.py -s "This is a test subject" -f test@example.com 
                -m "This is a test body" -r Job_failures

            python emailer.py -s "This is a test subject" -f test@example.com 
                -m @/tmp/message.txt -r Job_failures -a /tmp/attachment1.txt 
                /tmp/attachment2.txt

        Arguments:
            -h, --help            show this help message and exit.
            -r DISTRIBUTION_GROUP, --recipients DISTRIBUTION_GROUP
                    The distribution group name (from the 
                    mailList,json file) (required).
            -s EMAIL_SUBJECT, --subject EMAIL_SUBJECT
                    The subject of the email (required).
            -f EMAIL_FROM, --sender EMAIL_FROM
                    The name or email address of the sender of 
                    the email (required.
            -m MESSAGE_BODY, --message MESSAGE_BODY
                    The message that will be contained in the 
                    body of the email (required).
            -a N [N ...], --attach N [N ...]
                    A file or a list of files (using absolute paths and
                    separated by spaces) that will be attached to the
                    email. (optional).

    Description:
        This program is intended to be used as a Python module or to be called 
        directly from the command line or a shell script. The send_email 
        function should be used by different programs that need to send 
        notification emails (E.g. csr_checker.py and sccm_error_log_emailer.py).
        The fact that this is an independent function allows to have a better 
        control and easier maintenance while editing this code (instead of 
        having the same function replicated in several .py files).

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2017-05-09 (?)

    Modification list:
        CCYY-MM-DD  Author           Description
        2017-06-23  Alan Verdugo    Improved path string concatenation.
                                    Implemented logging functionality.
                                    Minor improvements.
        2017-08-25  Alan Verdugo    This script can now be called independently
                                    (for example, from the CLI or a shellscript)
        2017-08-29  Alan Verdugo    Separated the send_email() function into 
                                    build_email() and send_email().
                                    Added the ability to add attachments.
'''

# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# For reading the dist, list.
import json

# Email modules we will need.
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase

# The actual email-sending functionality.
import smtplib

# Handle output.
import logging

# Handling arguments.
import argparse


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


def get_args(arguments):
    '''
        This function will get and parse the arguments (in case this script is 
        executed directly, i.e. not as a module)
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--recipients",
        help = "The distribution group name (from the mailList.json file).",
        dest = "distribution_group",
        type = str,
        required = True)
    parser.add_argument("-s", "--subject",
        help = "The subject of the email.",
        dest = "email_subject",
        type = str,
        required = True)
    parser.add_argument("-f", "--sender",
        help = "The name or email address of the sender of the email.",
        dest = "email_from",
        required = True)
    parser.add_argument("-m", "--message",
        help = "The message that will be contained in the body of the email.",
        dest = "message_body",
        required = True)
    parser.add_argument("-a", "--attach",
        help = "A file or a list of files (using absolute paths and separated "\
            "by spaces) that will be attached to the email.",
        dest = "attachments",
        metavar = "N",
        type = str,
        nargs = "+",
        required = False)
    args = parser.parse_args()

    build_email(args.distribution_group, args.email_subject, args.email_from,
        args.message_body, args.attachments)


def send_email(email_from, email_to, message):
    '''
        This function will (finally) attempt to deliver the email that was 
        built by the rest of the process.
    '''
    try:
        s = smtplib.SMTP(smtp_server)
        s.sendmail(email_from, email_to, message)
        s.quit()
    except Exception as exception:
        log.error("Unable to send notification email. {0}".format(exception))
        sys.exit(1)
    else:
        log.info("Notification email sent to {0}".format(email_to))
        exit(0)


def build_email(distribution_group, email_subject, email_from, results_message, 
    attachments):
    '''
        This function will build the email with all its parts and then it 
        will attempt to send it using the send_email function.
    '''
    # Open and parse the JSON file containing the recipients of the email.
    try:
        mail_list = json.load(open(mail_list_file, "r+"))
        for email_groups in mail_list["groups"]:
            if email_groups["name"] == distribution_group:
                email_to = email_groups["members"]
    except Exception as exception:
        log.error("Cannot read email recipients list. {0}".format(exception))
        sys.exit(2)

    # The whole email will consist of two parts: The text/body and the 
    # attachments. We will need to "attach" the two parts to the message.
    # I know this may be confusing.
    msg = MIMEMultipart()
    msg["Subject"] = email_subject

    # A list of filenames that could not be properly attached (if any).
    failed_attachments = []

    # If any attachments were specified, open the file(s) and attach them to 
    # the message.
    if attachments != None:
        for attachment in attachments:
            try:
                with open(attachment) as fp:
                    part1 = MIMEBase("application", "octet-stream")
                    part1.set_payload(fp.read())
                # After the file is closed
                part1.add_header("Content-Disposition", "attachment", 
                    filename=os.path.basename(attachment))
                msg.attach(part1)
            except Exception as exception:
                # If there is a problem while attaching a file, let's continue 
                # with the remaining files (if any). Note: In this way, we may 
                # end up sending the email without any attachments at all.
                log.error("Unable to attach file {0}. Exception:"\
                    " {1}".format(attachment, exception))
                failed_attachments.append(attachment)

    # Check if the email body is read from a file or it is just a simple text.
    if results_message.startswith("@"):
        body_file = results_message.split("@")[1]
        # Try to open and read the contents of the file, which will be the 
        # body of the email.
        try:
            with open(body_file) as fp:
                results_message = fp.read()
        except Exception as exception:
            log.error("Unable to read file: {0} Exception: "\
                "{1}".format(body_file, exception))
            sys.exit(3)

    # If there were problem while attaching files, let's add a note to the 
    # email's body.
    if failed_attachments != None:
        for failed_attachment in failed_attachments:
            results_message = results_message + "\n\rThe file {0} was unable "\
            "to be attached to this email.".format(failed_attachment)

    part2 = MIMEText(results_message, "plain")
    msg.attach(part2)

    # Finally, attempt to send the email by calling the send_email function.
    send_email(email_from, email_to, msg.as_string())


if __name__ == "__main__":
    # Parse arguments from the CLI.
    get_args(sys.argv[1:])
