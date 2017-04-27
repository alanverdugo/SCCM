#!/usr/bin/env python
'''
    Usage:
        python sccm_error_log_emailer.py job1 job2 job3...

    Description:
        This program will accept a list of SCCM job names as arguments.
        It will look for the newest .xml logs in the corresponding directories 
        for those jobs and parse them looking for errors.
        If any errors are found, they will be sent to the email distribution 
        list.

    Autor:
        Alan Verdugo

    Modification list:
        CCYY-MM-DD  Autor                   Description
        2016-07-25  Alan Verdugo           The job_names are not necessarily 
                                            equal to the pathnames where the 
                                            log files are created.
                                            I had to open the actual job_files 
                                            in order to get the correct name of 
                                            each job.
                                            Also improved some error checking.
        2016-07-28  Alan Verdugo           Now we look for errors in the 
                                            entire XML log.
        2017-04-18  Alan Verdugo           Converted identation to spaces.
                                            Some changes in readability.
                                            Organized everything into functions.
'''

import os                                   # Needed for system and environment information.
import sys                                  # Needed for system and environment information.
import socket                               # Needed for system and environment information.
import glob                                 # Needed to get the newest log files.
import xml.etree.ElementTree as ET          # XML navigation.
import smtplib                              # For the actual email-sending functionality.
from email.mime.text import MIMEText        # Email modules we will need.
from datetime import datetime               # For timestamp information in the email subject.
import json                                 # For reading the dist, list.
import argparse                             # Handling arguments.

smtp_server = "localhost"                   # The hostname of the SMTP server.
sccm_home = "/opt/ibm/sccm/"               # Home of the SCCM installation.
binary_home = sccm_home + "bin/custom/"     # The path where this script and the dist. list are.
mail_list_file = binary_home + "mailList.json"
job_file_dir = sccm_home + "jobfiles/"      # Location path of SCCM jobfiles.
log_path = sccm_home + "logs/jobrunner/"    # The root location of the collectors log files.
list_of_job_names = []                      # A list of the jobnames.
list_of_log_files = []                      # A list of the found logs.
newest_log = ""                             # Placeholder for the log filenames.
hostname = socket.gethostname()             # The hostname where this is running.
email_from = "SCCM@" + hostname             # Email sender address.
mail_list = ""                              # JSON Object of mail_list_file.
distribution_group = "Job_failures"         # Email distribution group.


def send_email(distribution_group, email_subject, email_from, results_message):
    try:
        mail_list = json.load(open(mail_list_file, "r+"))
        for email_groups in mail_list["groups"]:
            if email_groups["name"] == distribution_group:
                email_to = email_groups["members"]
    except Exception as exception:
        print "ERROR: Cannot read email recipients list.", exception
        sys.exit(1)
    msg = MIMEText(results_message,"plain")
    msg["Subject"] = email_subject
    s = smtplib.SMTP(smtp_server)
    try:
        s.sendmail(email_from, email_to, msg.as_string())
        s.quit()
    except Exception as exception:
        print "ERROR: Unable to send notification email.", exception
        sys.exit(1)
    print "INFO: Notification email sent to", email_to


def main(arguments):
    # Get the list of arguments (i.e. the jobs to check).
    for argument in arguments:
        job_file = job_file_dir + argument + ".xml"
        # Validate that the jobfile exists and its readable.
        if not os.path.exists(job_file):
            print "ERROR: Jobfile", job_file, "does not exist or is not readable. Verify the arguments."
            sys.exit(1)
        # Parse the jobfile to get the actual job name(s).
        # If the XML is malformed, that could mean that we have problems.
        # (Sometimes the jobs fail and are unable to create a proper XML log)
        try:
            tree = ET.parse(job_file)
            root = tree.getroot()
            # SCCM job_files use namespaces, hence they needed to be added here.
            # TODO: Get only the jobs that have an "active" tag on them.
            for job in root.findall("{http://www.ibm.com/TUAMJobs.xsd}Job"):
                # Look for the content of the "Job id" tag.
                job_name = job.get("id")
                list_of_job_names.append(job_name)
        except Exception as exception:
            email_subject = "ERROR: Malformed XML log."
            send_email(email_subject, email_to, email_from, 
                "ERROR: The file" + job_file + " contains malformed XML. " + exception)

        for job_name in list_of_job_names:
            # Check if any of the log paths are invalid directories.
            if os.path.exists(log_path + job_name):
                # Validate that we have at least one log file to work with.
                if len(glob.glob(log_path + job_name + "/*.xml")) > 0:
                    # Get the newest log in the directory
                    newest_log = max(glob.iglob(log_path + job_name + "/*.xml"), key=os.path.getctime)
                    # Make a list with all the valid log files.
                    list_of_log_files.append(newest_log)
                else:
                    print "ERROR: No XML logs found in " + log_path + job_name
            else:
                print "ERROR: The path " + log_path + job_name + " does not exist. Verify the arguments."

    for log_file in list_of_log_files:
        error_message = ""
        error_found = False
        tree = ET.parse(log_file)
        root = tree.getroot()

        # Get the name of the failed job (from the log file).
        job_name = root.find("./Job").get("name")

        # Get the error output messages.
        # Since the error messages could be in any part of the XML structure,
        # we use the // notation for finding any ocurrence of them
        # (.// Selects all subelements, on all levels beneath the 
        # current element.)
        for message in root.findall(".//message"):
            if message.get("type").strip() == "ERROR":
                error_message += "Timestamp: " + message.get("time") + " Message: " + message.text + "\r"
                error_found = True

        # Send an email to the distribution list.
        if error_found:
            print "INFO: Sending notification email..."
            email_subject = "The " + job_name + " job failed in the server " + hostname + " at " + str(datetime.now().time())
            send_email(distribution_group, email_subject, email_from, error_message)


def get_args(argv):
    parser = argparse.ArgumentParser(description="Analyze SCCM logs.")
    parser.add_argument("arguments", metavar="N", type=str, nargs="+",
        help="The job name.")
    args = parser.parse_args()
    main(args.arguments)


if __name__ == "__main__":
    # Parse arguments from the CLI.
    get_args(sys.argv[1:])
exit(0)
