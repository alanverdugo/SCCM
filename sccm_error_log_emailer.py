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
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2016-06-01 (?)

    Modification list:
        CCYY-MM-DD  Autor                   Description
        2016-07-25  alanvemu@mx1.ibm.com    The job_names are not necessarily 
                                            equal to the pathnames where the 
                                            log files are created.
                                            I had to open the actual job_files 
                                            in order to get the correct name of 
                                            each job.
                                            Also improved some error checking.
        2016-07-28  alanvemu@mx1.ibm.com    Now we look for errors in the 
                                            entire XML log.
        2017-04-18  alanvemu@mx1.ibm.com    Converted identation to spaces.
                                            Some changes in readability.
                                            Organized everything into functions.
        2017-05-10  alanvemu@mx1.ibm.com    Separated the send_email function 
                                            into an independent module.
                                            Revised the import and variables 
                                            sections for better legibility.
                                            Added minor improvements.
        2017-08-08  alanvemu@mx1.ibm.com    Improved readability.
                                            Changed lines larger than 80 chars.
                                            Other small improvements.
        2017-08-10  alanvemu@mx1.ibm.com    Validate the age of the newest log 
                                            file and send a warning if a log 
                                            was not created for the previous 
                                            job execution.
        2017-08-11  alanvemu@mx1.ibm.com    Created and assigned the 
                                            MAX_AGE_OF_LAST_LOG_FILE global 
                                            constant.
        2017-08-11  alanvemu@mx1.ibm.com    Improved the error message that is 
                                            sent when there is not recent log 
                                            file created by startJobRunner.sh
'''

# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# Needed for system and environment information.
import socket

# Needed to get the newest log files.
import glob

# XML navigation.
import xml.etree.ElementTree as ET

# For timestamp information in the email subject.
from datetime import datetime

# Handling arguments.
import argparse

# Custom module for email sending (refer to emailer.py)
import emailer

# Handle logging.
import logging

# To get current seconds since Epoch.
import time


# Home of the SCCM installation.
sccm_home = "/opt/ibm/sccm/"

# The path where this script and the distribution list are located.
binary_home = os.path.join(sccm_home, "bin/custom/")

# Location path of SCCM jobfiles.
job_file_dir = os.path.join(sccm_home, "jobfiles/")

# The root location of the collectors log files.
log_path = os.path.join(sccm_home, "logs/jobrunner/")

# A list of the jobnames.
list_of_job_names = []

# A list of the found logs.
list_of_log_files = []

# Placeholder for the log filenames.
newest_log = ""

# The max age we are willing to accept for the creation of the newest 
# log file (in seconds). This is a global constant.
MAX_AGE_OF_LAST_LOG_FILE = 600

# The hostname where this is running.
hostname = socket.gethostname()

# Email sender address.
email_from = "SCCM_" + hostname + "@" + hostname

# JSON Object of mail_list_file.
mail_list = ""

# Email distribution group.
distribution_group = "Job_failures"

# Logging configuration.
log = logging.getLogger("sccm_error_log_emailer")
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')


def main(arguments):
    # Get the list of arguments (i.e. the jobs to check).
    for argument in arguments:
        job_file = os.path.join(job_file_dir, argument) + ".xml"
        # Validate that the jobfile exists and its readable.
        if not os.path.exists(job_file):
            log.error("Jobfile {0} does not exist or is not readable. Verify "\
                "the arguments.".format(job_file))
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
                list_of_job_names.append(job.get("id"))
        except Exception as exception:
            email_subject = "ERROR: Malformed XML job file."
            error_body = "The file {0} contains malformed XML: "\
                "{1}".format(job_file, exception)
            log.error(error_body)
            emailer.send_email(distribution_group, email_subject, email_from, 
                error_body)

        for job_name in list_of_job_names:
            absolute_job_file = os.path.join(log_path, job_name)
            # Check if any of the log paths are invalid directories.
            if os.path.exists(absolute_job_file):
                # Validate that we have at least one log file to work with.
                if len(glob.glob(absolute_job_file + "/*.xml")) > 0:
                    # Get the newest log in the directory
                    newest_log = max(glob.iglob(absolute_job_file + "/*.xml"),
                        key=os.path.getctime)
                    # If the newest log file was created more than, say, 10 
                    # minutes ago, we will assume the startJobRunner.sh script 
                    # failed and did not create a proper log file, in which 
                    # case we may be missing data, so let's send a notification 
                    # email.
                    if ((time.time() - os.path.getctime(newest_log)) >
                        MAX_AGE_OF_LAST_LOG_FILE):
                        # The newest file is older than 10 minutes.
                        email_subject = "ERROR: Missing {0} log file in "\
                            "{1}".format(job_name, hostname)
                        error_body = "The previous run of the {0} job (at "\
                            "{1}) did not generate a log file.\n\rThis may "\
                            "indicate a malfunction in startJobRunner.sh."\
                            "\n\rCheck the console logs located in "\
                            "/tmp/logs/sccm/.".format(job_name, datetime.now())
                        log.error(error_body)
                        emailer.send_email(distribution_group, email_subject, 
                            email_from, error_body)
                        sys.exit(2)

                    # Make a list with all the valid log files.
                    list_of_log_files.append(newest_log)
                else:
                    log.error("No XML logs found in "\
                        "{0}".format(absolute_job_file))
            else:
                log.error("The path {0} does not exist. Verify the "\
                    "arguments.".format(absolute_job_file))

    for log_file in list_of_log_files:
        error_message = []
        error_found = False
        try:
            tree = ET.parse(log_file)
            root = tree.getroot()
        except Exception as exception:
            email_subject = "ERROR: Malformed XML log."
            error_body = "The file {0} contains malformed XML: "\
                "{1}".format(log_file, exception)
            log.error(error_body)
            emailer.send_email(distribution_group, email_subject, email_from, 
                error_body)

        # Get the name of the failed job (from the log file).
        job_name = root.find("./Job").get("name")

        # Get the error output messages.
        # Since the error messages could be in any part of the XML structure,
        # we use the // notation for finding any ocurrence of them
        # (.// Selects all subelements, on all levels beneath the 
        # current element.)
        for message in root.findall(".//message"):
            if message.get("type").strip() == "ERROR":
                error_message.append("Timestamp: {0} Message: "\
                    "{1}".format(message.get("time"), message.text))
                error_found = True

        # Send an email to the distribution list.
        if error_found:
            log.info("Sending notification email...")
            email_subject = "The {0} job failed in the server {1} at "\
                "{2}".format(job_name, hostname, str(datetime.now().time()))
            error_message_string = "\n\r".join(error_message)
            emailer.send_email(distribution_group, email_subject, email_from, 
                error_message_string)


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
