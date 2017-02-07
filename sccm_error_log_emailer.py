#!/usr/bin/env python
'''
	Usage:
		python sccm_error_log_emailer.py job1 job2 job3...

	Description:
		This program will accept a list of SCCM job names as arguments.
		It will look for the newest .xml logs in the corresponding directories for those jobs 
		and parse them looking for errors.
		If any errors are found, they will be sent to the email distribution list.

	Autor:
		Alan Verdugo (alanvemu@mx1.ibm.com)

	Modification list:
		CCYY-MM-DD	Autor					Description
		2016-07-25	alanvemu@mx1.ibm.com	The job_names are not necessarily equal to the
											pathnames where the log files are created.
											I had to open the actual job_files in order to 
											get the correct name of each job.
											Also improved some error checking.
		2016-07-28	alanvemu@mx1.ibm.com	Now we look for errors in the entire XML log.
'''

import os 									# Needed for system and environment information.
import sys 									# Needed for system and environment information.
import socket								# Needed for system and environment information.
import glob									# Needed to get the newest log files.
import xml.etree.ElementTree as ET 			# XML navigation.
import smtplib								# Import smtplib for the actual sending function.
from email.mime.text import MIMEText		# Import the email modules we'll need.
from datetime import datetime				# For timestamp information in the email subject.

email_from = "SCCM@localhost"				# Sender address.
smtp_server = "localhost"					# The hostname of the SMTP server.
log_path = "/opt/ibm/sccm/logs/jobrunner/"	# The root location of the collectors log files.
sccm_home = "/opt/ibm/sccm/"
binary_home = sccm_home + "bin/"			# The path where this script and the dist. list are.
job_file_dir = sccm_home + "jobfiles/"
list_of_job_names = []						# A list of the jobnames.
list_of_log_files = []						# A list of the found logs.
newest_log = ""								# Placeholder for the log filenames.
hostname = socket.gethostname()				# The hostname where this is running.

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
	sys.exit(0)


# Get the list of arguments (i.e. the jobs to check).
for argument in sys.argv[1:len(sys.argv)]:
	job_file = job_file_dir + argument + ".xml"
	# Validate that the jobfile exists and its readable.
	if not os.path.exists(job_file):
		print "jobfile", job_file, "does not exist or is not readable. Verify the arguments."
		sys.exit(1)
	# Parse the jobfile to get the actual job name(s).
	# If the XML is malformed, that could mean that we have problems.
	# (Sometimes the jobs fail and are unable to create a proper XML log)
	try:
		tree = ET.parse(job_file)
		root = tree.getroot()
		# SCCM job_files use namespaces, hence they needed to be added here.
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
		if os.path.exists(log_path+job_name):
			# Validate that we have at least one log file to work with.
			if len(glob.glob(log_path+job_name+"/*.xml")) > 0:
				# Get the newest log in the directory
				newest_log = max(glob.iglob(log_path+job_name+"/*.xml"), key=os.path.getctime)
				# Make a list with all the valid log files.
				list_of_log_files.append(newest_log)
			else:
				print "ERROR: No XML logs found in "+log_path+job_name
		else:
			print "ERROR: The path "+log_path+job_name+" does not exist. Verify the arguments."

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
	# (.// Selects all subelements, on all levels beneath the current element.)
	for message in root.findall(".//message"):
		if message.get("type").strip() == "ERROR":
			error_message += "Timestamp: "+message.get("time")+" Message: "+message.text+"\r"
			error_found = True

	# Send an email to the distribution list.
	if error_found:
		print "sending email"
		email_subject = "The "+job_name+" job failed in the server "+hostname+" at "+str(datetime.now().time())
		send_email(email_subject, email_from, error_message)
