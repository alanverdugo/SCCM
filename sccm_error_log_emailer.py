#!/usr/bin/python
###############################################################################
#
# Copyright (C) IBM Corporation.
#
# Description:
#		This script checks if all the CSR files were successfully collected in a
#		specific date or month. If not, it will collect them.
#		This was customized from the CSR checker that we use in the TUAM EOM 
#		process.
# 
# Usage:
#		python csr_checker.py -y|--year YYYY -m|--month CURMON|PREMON|MM (01-12) -d|--day DD (01-31)
#
#		Examples:
#			Checking all the files in a specific date (February 20th, 2015):
#				python csr_checker.py -y 2015 -m 02 -d 20
#
#			Checking all the files for a month (March 2015, in this case):
#				python csr_checker.py -y 2015 -m 3
#
#			Checking all the files in the previous month:
#				python csr_checker.py -m PREMON
#
#			Checking all the files in the current month up to yesterday:
#				python csr_checker.py -m CURMON
#
# Author:
#		Alan Verdugo (alanvemu@mx1.ibm.com)
#
# Creation date:
#		2015-11-24
#
# Revision history:
#		Author          Date		Notes
#		Alan Verdugo	2016-10-31	Adapted it from TUAM to SCCM.
#		Alan Verdugo	2017-02-28	Fixed a bug that only happens on the 1st of the month.
#
###############################################################################

import sys
import calendar
import datetime
import getopt
import glob
import os
import argparse
from shutil import copy2
import socket                               # Needed for system and environment information.
import smtplib								# Import smtplib for the actual sending function.
from email.mime.text import MIMEText		# Import the email modules we'll need.

## Environmental variables.
hostname = socket.gethostname()             # The hostname where this is running.
csrPath = "/home/ftpuser/upload/"
sccm_home = "/opt/ibm/sccm/"
destPath = sccm_home + "samples/logs/collectors/"
binary_home = sccm_home + "bin/custom/"			# The path where this script and the dist. list are.
email_from = "SCCM@" + hostname				# Sender address.
smtp_server = "localhost"					# The hostname of the SMTP server.
error_message = ""
error_found = False


def send_email(emailSubject, emailFrom, resultsMessage):
	try:
		email_file = open(binary_home + "mailList.txt", "r+")   # Distribution list file, one address per line.
		email_to = email_file.readlines()
	except Exception as exception:
		print "ERROR: Unable to open email recipients file.", exception
		sys.exit(1)
	email_file.close()
	msg = MIMEText(resultsMessage,"plain")
	msg["Subject"] = emailSubject
	s = smtplib.SMTP(smtp_server)
	try:
		s.sendmail(emailFrom, email_to, msg.as_string())
		s.quit()
	except Exception as exception:
		print "ERROR: Unable to send notification email.", exception
		sys.exit(1)
	print "Sent notification email to ", email_to


def getArgs(argv):
	year = None
	month = None
	day = None
	global curmon
	curmon = None

	parser = argparse.ArgumentParser()
	parser.add_argument("-y","--year",
		help = "The year in CCYY format.",
		dest = "year",
		type = int)
	parser.add_argument("-m","--month",
		help = "The two-digit number of the month (01-12). PREMON will check the previous month. CURMON the current month",
		dest = "month",
		default = "PREMON",
		choices = ['01','02','03','04','05','06','07','08','09','10','11','12','PREMON','CURMON'],
		required = True)
	parser.add_argument("-d","--day",
		help = "The two-digit number of the day.",
		dest = "day",
		choices = ['01','02','03','04','05','06','07','08','09','10',
			'11','12','13','14','15','16','17','18','19','20',
			'21','22','23','24','25','26','27','28','29','30','31'])
	args = parser.parse_args()

	# If the month argument is "PREMON", get the current month and year and calculate the previous month.
	if args.month == "PREMON":
		lastMonth = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
		args.month = lastMonth.strftime("%m")
		args.year = lastMonth.strftime("%Y")
	elif args.month == "CURMON":
		curmon = True
		args.month = datetime.date.today().strftime("%m")
		args.year = datetime.date.today().strftime("%Y")

	# The "day" argument is not required (in case we want to check a full month)
	# but if we do receive it, make sure it is a valid date (e.g. not 2016-02-31)
	if args.day is not None:
		# Validate that we have a valid date.
		try:
			datetime.datetime(year=int(args.year),month=int(args.month),day=int(args.day))
		except:
			print "ERROR: Please provide a valid date."
			exit(3)
	main(args.year, args.month, args.day)


def checkDay(year, month, day):
	global error_message
	global error_found
	# Find the files for the specified date.
	for jobName in ["consolidation_backups","consolidation_cinder_volume","consolidation_nova_compute"]:
		# Check inside every directory inside the jobNames (there is one dir for every satellite server).
		# Check if any of the arguments are invalid directories.
		if os.path.exists(csrPath+jobName):
			# Read the contents of the path and see if there are any directories inside.
			for dirname, dirnames, filenames in os.walk(csrPath+jobName):
    			# print path to all subdirectories first.
				for subdirname in dirnames:
					if (len(str(month)) < 2):
						month = "0"+str(month)
					if (len(str(day)) < 2):
						day = "0"+str(day)
					filename = str(year)+str(month)+str(day)+".txt"
					# If we do find directories inside, see if they have all the expected files in them.
					if not (os.path.isfile(csrPath+jobName+"/"+subdirname+"/"+filename)):
						error_message += "\tWARNING: File not found: "+csrPath+jobName+"/"+subdirname+"/"+str(year)+str(month)+str(day)+".txt\n\r"
						error_found = True
					else:
						# The requested file is present, print a notification (we may want to remove this).
						print "The file",filename,"is present in "+str(csrPath)+str(jobName)+"/"+str(subdirname)
						print "INFO: Copying file to final destination..."
						# Copy the file to the desired location.
						try:
							copy2(csrPath+jobName+"/"+subdirname+"/"+filename, destPath+jobName+"/"+subdirname+"/"+filename)
							print "INFO: Copy completed!"
						except IOError as exception:
							error_message += "ERROR: Unable to copy file. "+str(exception)+"\n\r"
							error_found = True
		else:
			error_message += "ERROR: The path",csrPath+jobName,"does not exist. Verify the arguments."
			error_found = True


def checkMonth(year, month):
	# If we are checking the current month, we need to stop checking until "yesterday",
	# unless it is the 1st day of the month, (we will check the whole previous 
	# month if that is the case).
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
	print "INFO: Checking month",month,"of the year",year,"(",daysOfTheMonth,") files."

	# Check that we have all the files for the specified month.
	day=01
	while day <= int(daysOfTheMonth):
		# Call the checkDay function as many times as it is needed.
		checkDay(year, month, day)
		day=int(day)+1
	

def main(year, month, day):
	global error_found
	global error_message
	# Check if we are going to check (?) a whole month or an individual day.
	if year and month and day:
		# Check an specific day.
		checkDay(year, month, day)
	elif year and month and not(day):
		# Check all the days in a month.
		checkMonth(year, month)
	if error_found:
		send_email("SCCM CSR processing error", email_from, error_message)
		exit (1)


if __name__ == "__main__": 
	# Get and verify that the arguments are well formed and we got a valid date.
	getArgs(sys.argv[1:])
