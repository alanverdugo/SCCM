#!/usr/bin/env python
'''
    Usage:
        python query_runner.py [-h] [-v]

    Optional arguments:
        -h, --help     show this help message and exit.
        -v, --verbose  Will print INFO, WARNING, and ERROR messages to the 
            stdout or stderr.

    Description:
        This program will execute an SQL query in a DB2 database and then 
        export the resulting data into a .csv file.
        This program is supposed to be run as the first part of the SCCM EOM 
        process.
        Due to the nature of the tables and their data, it is easier to write 
        the data manually into the CSV file instead of using the Python CSV 
        module.

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2018-02-28

    Modification list:
        CCYY-MM-DD  Author                  Description

'''

# Needed for system information.
import os
import sys

# Arguments handling.
import argparse

# IBM DB2 driver.
import ibm_db

# Handle logging.
import logging

# Date formating.
import datetime

# To read the configuration file.
import json


# Configuration files.
curr_dir = os.path.dirname(os.path.realpath(__file__))
config_file = os.path.join(curr_dir, "connection_parameters.json")
sql_input_file = os.path.join(curr_dir, "input_file.sql")
output_csv_file = os.path.join(curr_dir, "output_file.csv")

# Logging configuration.
log = logging.getLogger("query_runner")
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    '''
        This main function will do everything (for now).
        TODO: Separate this big function into smaller functions.
    '''
    # Read the connection parameters from a file.
    try:
        log.info("Reading connection parameters from {0}".format(config_file))
        config = json.load(open(config_file, "r+"))
        host = config["hostname"]
        db = config["database"]
        user = config["username"]
        password = config["password"]
        port = config["port"]
        protocol = config["protocol"]
    except Exception as exception:
        log.error("File cannot be opened: {0}\n{1}"\
            .format(config_file, exception))
        raise SystemExit(1)

    # Read the input SQL file containing the query to run.
    try:
        log.info("Reading SQL input file {0}". format(sql_input_file))
        query = open(sql_input_file, "r").read()
    except Exception as exception:
        log.error("File cannot be opened: {0}\n{1}"\
            .format(sql_input_file, exception))
        raise SystemExit(2)

    # Connect to the DB.
    try:
        conn = ibm_db.connect("DATABASE={0};"\
            "HOSTNAME={1};"\
            "PORT={2};"\
            "PROTOCOL={3};"\
            "UID={4};"\
            "PWD={5};".format(db, host, port, protocol, user, password), "", "")
        log.info("Connecting to DB {0} in {1}". format(db, host))
    except Exception as exception:
        log.error("Error connecting to the database: {0}".format(exception))
        raise SystemExit(3)

    # Open and truncate the output file (opening it in write mode automatically 
    # truncates it).
    try:
        log.info("Opening output file {0}". format(output_csv_file))
        output_file_handle = open(output_csv_file, "wb")
    except Exception as exception:
        log.error("Unable to open output file: {0}\n{1}"\
            .format(output_csv_file, exception))
        raise SystemExit(4)

    # Execute the query.
    try:
        log.info("Executing SQL query...")
        statement = ibm_db.exec_immediate(conn, query)

        # ibm_db.fetch_tuple() returns a tuple, indexed by column position, 
        # representing a row in a result set. The columns are 0-indexed.
        result = ibm_db.fetch_tuple(statement)

        while result:
            # Attempting to use the CSV module to write to a file adds quotes 
            # to the quotes already present in the query results (which are 
            # needed for the rest of the process), otherwise, it escapes them. 
            # For that reason, I need to iterate over the result tuple and add 
            # the values separately to every row which will be added to the 
            # final output file.
            row = ""

            # A list to append all the str values in each row.
            row_list = []

            for value in result:
                # Detect datetime values and format accordingly (CCYYMMDD)
                if type(value) is datetime.date:
                    value = value.strftime("%Y%m%d")
                row_list.append(str(value))
            row = ",".join(row_list)

            # Finally, write the row to the actual file.
            output_file_handle.write(row+"\n")

            # Get the next result.
            result = ibm_db.fetch_tuple(statement)
    except Exception as exception:
        log.error("Error executing query: {0}".format(exception))
        raise SystemExit(5)
    finally:
        # If it is still open, close the DB connection.
        if conn is not None:
            log.info("Closing DB connection to {0} in {1}".format(db, host))
            ibm_db.close(conn)
        # Close output file
        log.info("Closing output file {0}".format(output_csv_file))
        output_file_handle.close()


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
    args = parser.parse_args()

    # Set logging level.
    if args.verbose:
        log.setLevel(logging.INFO)

    # Call the main function.
    main()


if __name__ == "__main__":
    # Parse arguments from the CLI.
    get_args(sys.argv[1:])
sys.exit(0)
