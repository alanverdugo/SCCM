#!/usr/bin/python

'''
    This program prints all the 1-hour intervals between
    two user-provided datetimes.

    usage: date_range.py [-h] -s START_DATETIME -e END_DATETIME

    Arguments:
      -h, --help            show this help message and exit
      -s START_DATETIME, --start START_DATETIME
                            The start date in CCYYMMDDHHMM format.
      -e END_DATETIME, --end END_DATETIME
                            The end datetime in CCYYMMDDHHMM format.
'''

import sys
import datetime
import argparse

def getArgs(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--start",
        help = "The start date in CCYYMMDDHHMM format.",
        dest = "start_datetime",
        required=True,
        type = str)
    parser.add_argument("-e","--end",
        help = "The end datetime in CCYYMMDDHHMM format.",
        dest = "end_datetime",
        required=True,
        type = str)
    args = parser.parse_args()

    # Validate start date.
    try:
        args.start_datetime = datetime.datetime.strptime(args.start_datetime, "%Y%m%d%H%M")
    except Exception as exception:
        print "ERROR: Please provide a valid start datetime.", exception
        exit(1)

    # Validate end date.
    try:
        args.end_datetime = datetime.datetime.strptime(args.end_datetime, "%Y%m%d%H%M")
    except Exception as exception:
        print "ERROR: Please provide a valid end datetime.", exception
        exit(2)

    # Validate that the End datetime is actually greater than Start datetime.
    if args.start_datetime >= args.end_datetime:
        print "ERROR: End datetime must be greater than Start datetime!"
        exit(3)

    main(args.start_datetime, args.end_datetime)


def main(start_datetime, end_datetime):
    interval = datetime.timedelta(hours = 1)
    while start_datetime <= end_datetime:
        print start_datetime
        start_datetime += interval

if __name__ == "__main__": 
    # Get and verify that the arguments are well formed and we got valid dates.
    getArgs(sys.argv[1:])
