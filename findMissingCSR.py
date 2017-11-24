#!/usr/bin/env python
'''
    Usage:
        findMissingCSR.py [-h] [-v]
            [-y YEAR]
            [-m {01,02,03,04,05,06,07,08,09,10,11,12}]
            [-d {01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,
                21,22,23,24,25,26,27,28,29,30,31}]

    Optional arguments:
        -h, --help
            show this help message and exit
        -v, --verbose
            Will print INFO, WARNING, and ERROR messages to the stdout or stderr
        -y YEAR, --year YEAR  The year in CCYY format. 
                Defaults to the current year.
        -m {01,02,03,04,05,06,07,08,09,10,11,12},
        --month {01,02,03,04,05,06,07,08,09,10,11,12}
                The two-digit number of the month (01-12). Defaults to
                the current month.
        -d {01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31}, 
        --day {01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31}
                The two-digit number of the day. Defaults to the current day.
        -a, --allday
            Check all 24 hours of the day (00:00:00 to 23:00:00).
        -u, --uptonow
            Check all hours of the day up to 'now' (00:00:00 to XX:00:00).

    Description:
        This program will check the CSR files from MCS looking for missing data.
        It will make a list of the "missing hours" and notify the appropriate 
        people (i.e. the people listed under the MCS_checker group in the 
        distribution list JSON file.)
        Not to be confused with csr_checker.py

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2016-11-21

    Modification list:
        CCYY-MM-DD  Author                  Description
        2017-11-21  Alan Verdugo            Improved error checking.
        2017-11-23  Alan Verdugo            All time-handling is now done in 
                                            UTC.
                                            We now also check if the metadata 
                                            fields are present in every CSR 
                                            record.
                                            Minor improvements.
'''

# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# Needed for system and environment information.
import socket

# For timestamp information in the email subject.
from datetime import datetime

# Handling arguments.
import argparse

# Custom module for email sending (refer to emailer.py)
import emailer

# Handle logging.
import logging

# to read the CSR file(s).
import csv

# To get the providers from its "JSON" file.
import json


# Home of the SCCM installation.
sccm_home = os.path.join(os.sep, "opt", "ibm", "sccm")

# The path where this script and the distribution list are located.
binary_home = os.path.join(sccm_home, "bin", "custom")

# Location path of SCCM collector logs.
COLLECTOR_LOGS = os.path.join(sccm_home, "samples", "logs", "collectors")

# Path where MCS data resides.
mcs_home = os.path.join(sccm_home,'wlp', 'usr', 'servers', 'mcs')

# MCS configuration file of providers.
provider_file = os.path.join(mcs_home, 'data', 'providers.json')

# Logs home directory.
log_dir = os.path.join(os.sep, "tmp", "logs", "sccm")
log_filename = "checkMCS_" + str(datetime.now().strftime("%Y%m%d")) + ".log"
full_log_file_name = os.path.join(log_dir, log_filename)

# The hostname where this is running.
hostname = socket.gethostname()

# Email sender address.
email_from = "SCCM_" + hostname + "@" + hostname

# List of metadata fields that should be present in every CSR record.
metadata = ["ActionInProgress", "NetworkZone", "TemplateName"]

# JSON Object of mail_list_file.
mail_list = ""

# Email distribution group.
distribution_group = "MCS_checker"

# Error list initialization (just in case we need it).
errors = []
errors_found = False

# Logging configuration.
log = logging.getLogger("findMissingCSR")
logging.basicConfig(filemode = 'a')
fh = logging.FileHandler(full_log_file_name)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)


def handle_error(error):
    '''
        To avoid duplication of code, this function will take an error message 
        and append it to the "errors" list (which then will be used to build 
        the notification email). It will also log the error and set the 
        boolean variable "errors_found" to True.
    '''
    global errors_found
    errors_found = True
    errors.append(error)
    log.error(error)


def get_providers():
    '''
        (Based on Scott's get_providers.py)
        The providers file, for ridiculous and mysterious reason stores one JSON 
        object per line, so we cannot parse the file in the normal way. We have 
        to parse each line as if it were a complete JSON file.
    '''
    try:
        providers = []
        with open(provider_file, "rb") as file_handle:
            for row in file_handle:
                provider_line = json.loads(row)
                providers.append(provider_line['provider_name'])
        return providers
    except Exception as exception:
        handle_error("Error reading providers file {0} \nException: "\
            "{1}".format(provider_file, exception))


def search_metadata(row):
    '''
        This function will receive a CSR record and look for all the value of 
        the metadata list in it. If all the values are found in the record, it 
        will return True, otherwise it will return False (so, if any metadata
        field is missing, we will get an alert).
    '''
    found_fields = 0
    for field in metadata:
        if field in row:
            found_fields += 1
    if found_fields == len(metadata):
        return True
    else:
        return False


def main(full_date, all_day, up_to_now):
    # Initializing the errors list with a "header" value.
    errors.append("The following errors were found while verifying the MCS "\
        "data for {0} in {1}:\n".format(full_date, hostname))

    # Get a list of providers using the get_providers() function.
    providers = get_providers()
    if providers is None:
        handle_error("No active MCS providers were found in {0}"\
            .format(provider_file))
    else:
        # Infer the "process" from the provider name.
        for provider in providers:
            if provider.endswith("_nova"):
                process = "nova_compute"
            elif provider.endswith("_cinder"):
                # per Charlotte Despres, ICO's OpenStack cinder does not support 
                # additional volumes with VMware, so we expect no records.
                # Let's just silently ignore any cinder providers with "VMWARE" 
                # on their names.
                if "VMWARE" in provider:
                    log.info("Ignoring provider {0} (Currently, there is no "\
                        "support for VMware cinder).".format(provider))
                    continue
                else:
                    process = "cinder_volume"
            else:
                handle_error("The provider {0} is not valid.".format(provider))
            feed = provider

            log.info("Checking CSR files for {0}...\n\tIn feed: {1}\n\tIn "\
                "process: {2}".format(full_date, feed, process))

            # Build the full path and filename of the input file.
            input_file = full_date + ".txt"
            input_file_path = os.path.join(COLLECTOR_LOGS, process, feed)
            full_input_file = os.path.join(input_file_path, input_file)

            # Ensure the input file exist.
            if os.path.exists(input_file_path):
                if os.path.isfile(full_input_file):
                    log.info("Now checking {0}".format(full_input_file))
                else:
                    handle_error("The file {0} does not exist or is not a "\
                        "valid file.".format(full_input_file))
            else:
                handle_error("The directory {0} does not exist or is not a "\
                    "valid directory.".format(input_file_path))

            # Read the CSR file and get the unique contents of the fourth 
            # column (which is the start time of the MCS entry with format 
            # HH:MM:SS).
            file_hours = []
            try:
                with open(full_input_file, "rb") as file_handle:
                    reader = csv.reader(file_handle)
                    for row in reader:
                        if row[3] not in file_hours:
                            file_hours.append(row[3])
                        # Search for metadata fields in the record.
                        if search_metadata(row) == False:
                            handle_error("Missing metadata field(s) in the "\
                                "following record:\n{0}\n".format(row))
            except Exception as exception:
                handle_error("Error reading CSR input file {0} \nException: "\
                    "{1}".format(full_input_file, exception))

            # Get the current timestamp and remove the minutes and seconds.
            # RabbitMQ events use UTC timestamps so all timestamps in this code 
            # should be handled in UTC.
            rounded_current_hour = datetime.utcnow().strftime("%H")
            rounded_current_time = datetime.utcnow().replace(minute=0, 
                second=0).strftime("%H:%M:%S")

            # According to what the user specified, build list of hours for 
            # comparison against the content of the CSR file.
            comparison_hours = []
            if all_day == True:
                # Build a list with all the hours of the day.
                log.info("Now checking entries from 00:00:00 to 23:00:00...")
                for i in range(0, 24):
                    # (range() is not inclusive so we need to add 1 for 
                    # convenience)
                    hour = datetime.now().replace(hour=i, minute=0, second=0)\
                        .strftime("%H:%M:%S")
                    if hour not in comparison_hours:
                        comparison_hours.append(hour)
            elif up_to_now == True:
                # Build a list with all the hours up until now.
                log.info("Now checking entries from 00:00:00 to {0}..."\
                    .format(rounded_current_time))
                for i in range(0, int(rounded_current_hour)+1):
                    # (range() is not inclusive so we need to add 1 for 
                    # convenience)
                    hour = datetime.now().replace(hour=i, minute=0, second=0)\
                        .strftime("%H:%M:%S")
                    if hour not in comparison_hours:
                        comparison_hours.append(hour)
            else:
                # We will check only the current hour.
                log.info("Now checking entries for {0}..."\
                    .format(rounded_current_time))
                comparison_hours.append(rounded_current_time)

            # Any missing entries/hours in the CSR should be reported.
            for hour in comparison_hours:
                if hour not in file_hours:
                    handle_error("Missing MCS entries for {0} (process: "\
                        "{1}, feed: {2})".format(hour, process, feed))
                else:
                    log.info("Entries found for {0}".format(hour))

    # If there are missing entries/hours, notify the heroic billing team.
    if errors_found == True:
        attachments = []
        attachments.append(full_log_file_name)
        errors.append("\nFor more information, refer to the logfile {0}, "\
            "(which is attached to this email) or check the actual CSR files "\
            "in {1}.\n".format(full_log_file_name, COLLECTOR_LOGS))
        error_message_string = "\n".join(errors)
        # Send an email informing of any problems found.
        emailer.build_email(distribution_group,
            "ERROR: MCS collection missing CSR records in {0}".format(hostname), 
            email_from,
            error_message_string, 
            attachments)


def get_args(argv):
    '''
        Get, validate and parse arguments.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose",
        help = "Will print INFO, WARNING, and ERROR messages to the stdout "\
            "or stderr.",
        dest = "verbose",
        default = False,
        action = "store_true")
    parser.add_argument("-y","--year",
        help = "The year in CCYY format. Defaults to the current year (in UTC)",
        dest = "year",
        default = datetime.utcnow().strftime("%Y"))
    parser.add_argument("-m","--month",
        help = "The two-digit number of the month (01-12). Defaults to the "\
            "current month (in UTC).",
        dest = "month",
        default = datetime.utcnow().strftime("%m"),
        choices = ['01','02','03','04','05','06','07','08','09','10','11',
            '12'])
    parser.add_argument("-d","--day",
        help = "The two-digit number of the day. Defaults to the current day.",
        dest = "day",
        default = datetime.utcnow().strftime("%d"),
        choices = ['01','02','03','04','05','06','07','08','09','10',
            '11','12','13','14','15','16','17','18','19','20',
            '21','22','23','24','25','26','27','28','29','30','31'])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-a", "--allday",
        help = "Check all 24 hours of the day (00:00:00 to 23:00:00).",
        dest = "all_day",
        default = False,
        action = "store_true")
    group.add_argument("-u", "--uptonow",
        help = "Check all hours of the day up to 'now' (from 00:00:00 to "\
            "XX:00:00) in UTC.",
        dest = "up_to_now",
        default = False,
        action = "store_true")
    args = parser.parse_args()

    # Ensure that we have a valid date.
    try:
        full_date = datetime(year=int(args.year),
            month=int(args.month),
            day=int(args.day)).strftime("%Y%m%d")
    except Exception as exception:
        logging.error("Provided date is invalid. {0}".format(exception))
        exit(3)

    # Set logging level.
    if args.verbose:
        log.setLevel(logging.INFO)

    # Call the main function with the appropriate mode.
    main(full_date, args.all_day, args.up_to_now)


if __name__ == "__main__":
    # Parse arguments from the CLI.
    get_args(sys.argv[1:])
exit(0)
