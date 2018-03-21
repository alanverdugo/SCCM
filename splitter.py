#!/usr/bin/env python
'''
    Usage:
        python splitter.py [-h] [-v | --verbose] 
            -l | --log-date         LOG_DATE
            -i | --input-file       INPUT_FILE
            -p | --non-prod-file    PROD_OUTPUT_FILE
            -n | --non-prod-file    NON_PROD_OUTPUT_FILE

    Arguments:
        -h, --help      Show this help message and exit.
        -v, --verbose   Will print INFO, WARNING, and ERROR messages to the
                            stdout or stderr.
        -l LOG_DATE, --log-date LOG_DATE
                        The yyyymmdd date, which will be used to compare with 
                        the config file's list of regions' production dates.
        -i INPUT_FILE, --input-file INPUT_FILE
                        The CSR file used as input.
        -p PROD_OUTPUT_FILE, --prod-file PROD_OUTPUT_FILE
                        The output file for production data.
        -n NON_PROD_OUTPUT_FILE, --non-prod-file NON_PROD_OUTPUT_FILE
                        The output file for NON-production data.


    Description:
        This program will read a JSON configuration file and parse it, looking 
        for lists of prod and non-prod regions.
        Based on this information, it will read the CSR file for the current 
        day and separate the records into either PROD or NON-PROD.
        Once separated, the records will be written to two different CSR files.

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2018-03-16

    Modification list:
        CCYY-MM-DD  Author          Description
'''
# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# For reading the configuration file.
import json

# Handling arguments.
import argparse

# Handle logging.
import logging

# To get TODAYs date.
from datetime import date, datetime


upload_path = os.path.join(os.sep, "home", "ftpuser", "upload")

# The configuration file.
dir_path = os.path.dirname(os.path.realpath(__file__))
config_file = os.path.join(dir_path, "regions.json")

# Logging configuration.
log = logging.getLogger("splitter")
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')



def main(log_date, input_file, prod_output_file, non_prod_output_file):
    '''
        Main driver of the program logic.
    '''
    # List for storing the names of the prod regions (from the config file).
    prod_list = []
    # List for storing the names of the NON-prod regions (from the config file).
    non_prod_list = []

    # Open and parse the configuration file.
    try:
        log.info("Parsing config file {0}".format(config_file))
        config = json.load(open(config_file, "r+"))

        # Read every record and classify it for prod or non-prod 
        # (according to the lists in the config file).
        for region in config["regions"]:
            if region["type"] == "prod":
                for subregion in region["regions"]:
                    # If "date" is before TODAY, send the region to the 
                    # non-prod list. Otherwise, it means it is prod and it is 
                    # active now, so let's add it to the prod list.
                    if datetime.strptime(subregion["date"], '%Y-%m-%d').date() \
                        <= log_date:
                        prod_list.append(subregion["name"])
                    else:
                        non_prod_list.append(subregion["name"])
            else:
                for subregion in region["regions"]:
                    non_prod_list.append(subregion["name"])

    except Exception as exception:
        log.exception("Error parsing configuration file {0} \nException: "\
            "{1}".format(config_file, exception))
        raise SystemExit(1)

    # Open PROD output file.
    try:
        prod_output_file = open(prod_output_file, "w")
        log.info("Opening PROD output file {0}".format(prod_output_file))
    except Exception as exception:
        log.exception("Unable to open file.{0} \nException: "\
            "{1}".format(prod_output_file, exception))
        raise SystemExit(1)

    # Open NON-PROD output file.
    try:
        non_prod_output_file = open(non_prod_output_file, "w")
        log.info("Opening NON-PROD output file {0}"\
            .format(non_prod_output_file))
    except Exception as exception:
        log.exception("Unable to open file.{0} \nException: "\
            "{1}".format(non_prod_output_file, exception))
        raise SystemExit(1)

    try:
        # Read every CSR record, looking for any of the "regions" 
        # names. If found, that means there is a match and we can 
        # classify that record as prod/non-prod.
        log.info("Opening input file {0}".format(input_file))
        with open(input_file, "r+") as csr_file:
            for record in csr_file:
                found_in_record = False
                # Note: Be careful changing code here, the flow, 
                # the logic and the syntax makes it easy to miss 
                # any false positives or false negatives. I 
                # recommend much testing here.
                for prod_item in prod_list:
                    if (found_in_record == False) and (prod_item in record):
                        found_in_record = True

                # Write the records to their respective files.
                try:
                    if found_in_record:
                        prod_output_file.write(record)
                    else:
                        non_prod_output_file.write(record)
                except Exception as exception:
                    log.exception("Unable to write to output file(s). \
                        \nException: {1}".format(exception))
                    raise SystemExit(1)
    except Exception as exception:
        log.exception("Problem processing data. \n{0}".format(exception))
        raise SystemExit(1)
    finally:
        # Close output files.
        log.info("Writing finished. Closing output files.")
        prod_output_file.close()
        non_prod_output_file.close()


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
    parser.add_argument("-l", "--log-date",
        help = "The yyyymmdd date, which will be used to compare with the "\
            "config file's list of regions' production dates.",
        dest = "log_date",
        required = True)
    parser.add_argument("-i", "--input-file",
        help = "The CSR file used as input.",
        dest = "input_file",
        required = True)
    parser.add_argument("-p", "--prod-file",
        help = "The output file for production data.",
        dest = "prod_output_file",
        required = True)
    parser.add_argument("-n", "--non-prod-file",
        help = "The output file for NON-production data.",
        dest = "non_prod_output_file",
        required = True)
    args = parser.parse_args()

    # Set logging level.
    if args.verbose:
        log.setLevel(logging.INFO)

    # Ensure log-date is a valid date (I.e. not 2018-02-31 or 2018-31-12 or 
    # 01/01/2018) because we need to compare this --logdate to the list of 
    # regions' production dates.
    try:
        log_date = datetime.strptime(args.log_date, "%Y%m%d").date()
    except Exception as exception:
        log.exception("{0} is not a valid date.\nException: "\
            "{1}".format(args.log_date, exception))
        raise SystemExit(1)

    # Ensure the input file exists.
    if not os.path.exists(args.input_file):
        log.exception("Jobfile {0} does not exist or is not readable. Verify "\
            "the arguments.".format(args.input_file))
        raise SystemExit(1)

    # Call the main function.
    main(log_date, args.input_file, args.prod_output_file, 
        args.non_prod_output_file)


if __name__ == "__main__":
    # Parse arguments from the CLI.
    get_args(sys.argv[1:])
