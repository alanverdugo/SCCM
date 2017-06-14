#!/usr/bin/python
'''

    Description:
        This script checks if all the CSR files were successfully collected 
        in a specific date or month. If not, it will collect them.
        This was customized from the CSR checker that we use in the TUAM EOM 
        process.
 
    Usage:
        python csr_checker.py -y|--year YYYY -m|--month CURMON|PREMON|MM (01-12) -d|--day DD (01-31)

        csr_checker.py [-h] [-y YEAR] -m
                            {01,02,03,04,05,06,07,08,09,10,11,12,PREMON,CURMON}
                            [-d {01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,
                                16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}]
                            [-v]

        optional arguments:
            -h, --help          show this help message and exit
            -y YEAR, --year YEAR  The year in CCYY format.
            -m/--month {01,02,03,04,05,06,07,08,09,10,11,12,PREMON,CURMON}
                                The two-digit number of the month (01-12).
                                PREMON will check the previous month.
                                CURMON the current month.
            -d/--day {01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,
                16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}
                                The two-digit number of the day.
            -v, --verbose       Using -v/--verbose will print INFO, WARNING, 
                                and ERROR messages to the stdout or stderr.


        Examples:
            Checking all the files in a specific date (February 20th, 2015):
                python csr_checker.py -y 2015 -m 02 -d 20

            Checking all the files for a month (March 2015, in this case):
                python csr_checker.py -y 2015 -m 3

            Checking all the files in the previous month:
                python csr_checker.py -m PREMON

            Checking all the files in the current month up to yesterday:
                python csr_checker.py -m CURMON

    Return codes:
        0 - Everything went fine. No missing files.
        1 - At least one CSR file is missing. Notification email was sent.
        2 - The user provided an invalidad date to check (E.g. 2017-02-31).

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2015-11-24

    Revision history:
        CCYY-MM-DD  Author          Description
        2016-10-31  Alan Verdugo    Adapted it from TUAM to SCCM.
        2017-02-28  Alan Verdugo    Fixed a bug that only happens on the 1st 
                                    of the month.
        2017-04-21  Alan Verdugo    Changed indentation to spaces.
                                    Some minor changes in style.
                                    Changed send_email function to read the
                                    email recipients from a JSON file.
        2017-05-10  Alan Verdugo    Separated the send_email function 
                                    into an independent module.
                                    Revised the import and variables 
                                    sections for better legibility.
                                    Added minor improvements.
        2017-05-30  Alan Verdugo    Improved single digit date handling.
        2017-06-13  Alan Verdugo    Added the -v/--verbose flag.
                                    Error messages are now sent to stderr.
                                    Other minor improvements.
'''

# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# Needed for checking dates.
import calendar

# Needed for checking timestamps.
import datetime

# To get the latest modified files.
import glob

# Proper handling of arguments.
import argparse

# Copying of files.
from shutil import copy2

# Needed for system and environment information.
import socket

# Custom module for email sending (refer to emailer.py)
import emailer


## Environmental variables.

# The hostname where this is running.
hostname = socket.gethostname()

# Root path for the CSR files.
csrPath = "/home/ftpuser/upload/"

# SCCM main installation path.
sccm_home = "/opt/ibm/sccm/"

# Collector's logs path.
destPath = sccm_home + "samples/logs/collectors/"

# The path where this script and the distribution list are located.
binary_home = sccm_home + "bin/custom/"

# Sender address.
email_from = "SCCM@" + hostname

# Error checking.
error_message = ""
error_found = False

# Email distribution group.
distribution_group = "CSR_checker"


def getArgs(argv):
    year = None
    month = None
    day = None
    global curmon
    curmon = None
    global verbose

    parser = argparse.ArgumentParser()
    parser.add_argument("-y","--year",
        help = "The year in CCYY format.",
        dest = "year",
        type = int)
    parser.add_argument("-m","--month",
        help = "The two-digit number of the month (01-12). PREMON will check \
            the previous month. CURMON the current month",
        dest = "month",
        default = "PREMON",
        choices = ['01','02','03','04','05','06','07','08','09','10','11',
            '12','PREMON','CURMON'],
        required = True)
    parser.add_argument("-d","--day",
        help = "The two-digit number of the day.",
        dest = "day",
        choices = ['01','02','03','04','05','06','07','08','09','10',
            '11','12','13','14','15','16','17','18','19','20',
            '21','22','23','24','25','26','27','28','29','30','31'])
    parser.add_argument("-v","--verbose",
        help = "Using -v/--verbose will print INFO, WARNING, and ERROR \
            messages to the stdout or stderr.",
        dest = "verbose",
        default = False,
        required = False,
        action = "store_true")
    args = parser.parse_args()

    # If the month argument is "PREMON", get the current month and year and 
    # calculate the previous month.
    if args.month == "PREMON":
        lastMonth = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
        args.month = lastMonth.strftime("%m")
        args.year = lastMonth.strftime("%Y")
    elif args.month == "CURMON":
        curmon = True
        args.month = datetime.date.today().strftime("%m")
        args.year = datetime.date.today().strftime("%Y")

    # The "day" argument is not required (in case we want to check a full 
    # month) but if we do receive it, make sure it is a valid date 
    # (e.g. not 2016-02-31)
    if args.day:
        # Validate that we have a valid date.
        try:
            datetime.datetime(year=int(args.year),
                month=int(args.month),
                day=int(args.day))
        except:
            sys.stderr.write("ERROR: Provided date is invalid.\n")
            exit(2)
    verbose = args.verbose
    main(args.year, args.month, args.day)


def checkDay(year, month, day):
    global error_message
    global error_found

    # Find the files for the specified date.
    for jobName in ["consolidation_backups",
        "consolidation_cinder_volume",
        "consolidation_nova_compute"]:
        # Check inside every directory inside the jobNames 
        # (there is one dir for every satellite server).
        # Check if any of the arguments are invalid directories.
        if os.path.exists(csrPath + jobName):
            # Read the contents of the path and see if there are any 
            # directories inside.
            for dirname, dirnames, filenames in os.walk(csrPath+jobName):
                # print path to all subdirectories first.
                for subdirname in dirnames:

                    # This next line will ensure the month and day are 
                    # zero-padded in case they are single digit numbers 
                    # (E.g. "3" will become "03")
                    filename = datetime.date(int(year), int(month), int(day)).strftime("%Y%m%d")+".txt"

                    # If we do find directories inside, see if they have all 
                    # the expected files in them.
                    if not (os.path.isfile(csrPath+jobName+"/"+subdirname+"/"+filename)):
                        error_message += "WARNING: File not found: "\
                            + csrPath + jobName + "/" + subdirname +\
                            "/" + filename + "\n"
                        error_found = True
                    else:
                        if verbose:
                            print "INFO: The file", filename, "is present in "\
                                + str(csrPath) + str(jobName) + "/" + \
                                str(subdirname)
                            print "INFO: Copying file to final destination..."
                        # Copy the file to the desired location.
                        try:
                            copy2(csrPath + jobName + "/" + subdirname + "/" + filename, 
                                destPath + jobName + "/" + subdirname + "/" + filename)
                            if verbose:
                                print "INFO: Copy completed!"
                        except IOError as exception:
                            error_message += "ERROR: Unable to copy file. "\
                                + str(exception) + "\n"
                            error_found = True
        else:
            error_message += "ERROR: The path " + csrPath + str(jobName) + \
                " does not exist. Verify the arguments.\n"
            error_found = True


def checkMonth(year, month):
    # If we are checking the current month, we need to stop checking 
    # until "yesterday", unless it is the 1st day of the month, (we will 
    # check the whole previous month if that is the case).
    if curmon:
        daysOfTheMonth = datetime.date.today() - datetime.timedelta(days=1)
        daysOfTheMonth = daysOfTheMonth.strftime("%d")
        if (datetime.date.today().strftime("%d") == "01"):
            # If we made it here, it means it is the 1st day of the month.
            # So we need to check all the days of the previous month.
            # Essentially, it would be like using the PREMON argument.
            lastMonth = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
            month = lastMonth.strftime("%m")
            year = lastMonth.strftime("%Y")
    else:
        # Calculate how many days the specified month has.
        daysOfTheMonth = calendar.monthrange(int(year),int(month))[1]
    if verbose:
        print "INFO: Checking month", month, "of the year", year,\
            " (", daysOfTheMonth, ") files."

    # Check that we have all the files for the specified month.
    day=01
    while day <= int(daysOfTheMonth):
        # Call the checkDay function as many times as it is needed.
        checkDay(year, month, day)
        day=int(day)+1
    

def main(year, month, day):
    # Check if we are going to check (?) a whole month or an individual day.
    if year and month and day:
        # Check an specific day.
        checkDay(year, month, day)
    elif year and month and not(day):
        # Check all the days in a month.
        checkMonth(year, month)
    if error_found:
        # Print error message(s) to stderr.
        sys.stderr.write(error_message)
        # Send the notification email.
        emailer.send_email(distribution_group, "SCCM CSR processing error", 
            email_from, error_message)
        exit (1)


if __name__ == "__main__": 
    # Get and verify the arguments are well-formed and we got a valid date.
    getArgs(sys.argv[1:])
