#!/usr/bin/env python
'''
    Usage:
        deploy.py [-h] [-v] [--git-repo GIT_REPO] {satellite,consolidation}

        Positional arguments:
          consolidation         To deploy to a consolidation server.
          satellite             To deploy to a satellite server.

        Optional arguments:
          -h, --help            show this help message and exit
          -v, --verbose         Will print INFO, WARNING, and ERROR messages to 
                                the stdout or stderr.
          --git-repo GIT_REPO   Absolute location of the git repository.
          -e {GA,staging}, --environment {GA,staging}
                                Environment where the current server is being
                                configured. Either 'GA' or 'staging'.
                                Defaults to 'staging'.

    Description:
        This program will deploy files from a git repository clone directory 
        into the appropriate locations.
        It will also schedule cronjobs accordingly.
        The program will act slightly different according to which argument 
        is specified (either -c|--consolidation or -s|--satellite). The 
        behavior is the same in both cases, but the files and cronjobs differ.
        There is also a common configuration that applies to both satellite 
        and consolidation servers.

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2017-09-12

    Modification list:
        CCYY-MM-DD  Author          Description
        2017-12-20  Alan Verdugo    We now accept the --environment parameter, 
                                    which will set the appropriate destination 
                                    consolidation server for csr_sender.sh
        2017-12-28  Alan Verdugo    The --environment parameter is only 
                                    accepted when a satellite deployment is 
                                    being executed. Hence, the csr_sender 
                                    configuration file is left intact when a 
                                    consolidation deployment occurs.
'''
# Needed for system and environment information.
import os

# Needed for system and environment information.
import sys

# For reading the configuration file.
import json

# Handling arguments.
import argparse

# Recursive copying of files (distutils.dir_util.copy_tree).
from distutils import dir_util
from distutils import log as dirs_log

# Handle output.
import logging

# Cron jobs management (refer to cron_management.py).
import cron_management


# Home of the SCCM installation.
sccm_home = os.path.join(os.sep, "opt", "ibm", "sccm")

# Location of the csr_sender.config file.
csr_sender_config_file = os.path.join(sccm_home, "bin", "custom", \
    "csr_sender.config")


# Logging configuration.
log = logging.getLogger("deployment")
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')



def deploy_files(git_repo, files_to_deploy):
    '''
        This function will recursively copy the files from "source" to 
        "destination".
        The argument it receives is a list of source-destination pair(s) in 
        the form of:
        [
            {
                'source': '/path/to/source0', 
                'destination': '/path/to/destination0', 
            }, 
            {
                'source': '/path/to/source1', 
                'destination': '/path/to/destination1', 
            }
        ]
    '''

    # Check if the copying should be verbose or not.
    if logging.getLevelName(log.getEffectiveLevel()) == "INFO":
        verbose_copying = 1
        # The following lines are to specify that the created directories and 
        # copied files are to be printed to stdout and/or stderr.
        dirs_log.set_verbosity(dirs_log.INFO)
        dirs_log.set_threshold(dirs_log.INFO)
    else:
        verbose_copying = 0

    try:
        # If the destination path does not exist, it will be automatically 
        # created. If preserve_mode is true (the default), the file's mode 
        # (type and permission bits, or whatever is analogous on the current 
        # platform) is copied. If preserve_times is true (the default), the 
        # last-modified and last-access times are copied as well. If update is 
        # true, src will only be copied if dst does not exist, or if dst does 
        # exist but is older than src. (Per our discussions, it will be False).
        dir_util.copy_tree( 
            os.path.join(git_repo, files_to_deploy["source"]), 
            files_to_deploy["destination"], 
            preserve_mode = 1, 
            preserve_times = 1, 
            preserve_symlinks = 0, 
            update = 0, 
            verbose = verbose_copying )
    except Exception as exception:
        log.error("Unable to deploy files. Exception: {0}".format(exception))
        sys.exit(1)


def modify_csr_sender_config(consolidation_server):
    '''
        This function will open the csr_sender.config plain text file and set 
        the appropiate value in the "ConsolidationServer" environment variable. 
        This will eventually result in sending the CSR files to the appropiate 
        consolidation server. The default behavior of this process is to send 
        the data to the staging consolidation server.
    '''
    log.info("Configuring {0}".format(csr_sender_config_file))

    try:
        # Read the lines in the configuration file.
        lines = open(csr_sender_config_file).read().splitlines()

        # Find and replace the ConsolidationServer environment variable.
        # To keep it simple, do this regardless of the current setting.
        line_counter = 0
        for line in lines:
            if line.startswith("export ConsolidationServer="):
                lines[line_counter] = 'export ConsolidationServer="{0}"'\
                    .format(consolidation_server)
            line_counter+=1

        # Write the file out again.
        open(csr_sender_config_file,'w').write('\n'.join(lines))
    except Exception as exception:
        log.error("Error modifying {0}\nException: {1}"\
            .format(csr_sender_config_file, exception))
        sys.exit(1)
    else:
        log.info("Successfully configured {0}".format(csr_sender_config_file))


def main(git_repo, mode, environment):
    '''
        Main driver of the program logic.
    '''
    # A JSON configuration file.
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, "deploy_config.json")

    # A list of cronjobs, taken from the configuration file.
    cronjobs = []

    # A list of files to be deployed, taken from the configuration file.
    deployments = []

    log.info("Now deploying configuration for a {0} server...".format(mode))

    # Open and parse the configuration file.
    try:
        config = json.load(open(config_file, "r+"))
        # Parse the common/generic configurations that should apply to all the
        # server types.
        for cronjob in config["common"]["cronjobs"]:
            cronjobs.append(cronjob)
        for deployment in config["common"]["deployments"]:
            deployments.append(deployment)
        # Parse the specific configuration (either satellite or consolidation).
        for cronjob in config[mode]["cronjobs"]:
            cronjobs.append(cronjob)
        for deployment in config[mode]["deployments"]:
            deployments.append(deployment)
    except Exception as exception:
        log.error("Error reading configuration file {0} \nException: "\
            "{1}".format(config_file, exception))
        sys.exit(1)

    # Deploy files by calling the deploy_file() function.
    for deployment in deployments:
        deploy_files(git_repo, deployment)

    # Add the missing crontab entries by calling the add_cron_entry() function.
    for cronjob in cronjobs:
        cron_management.add_cron_entry(cronjob["entry"])

    # Get the appropiate consolidation server FQDN and execute the 
    # modify_csr_sender_config function accordingly.
    try:
        if mode == "satellite":
            if environment == "GA":
                consolidation_server = config["common"]\
                    ["GA_consolidation_server"]
            else:
                consolidation_server = config["common"]\
                    ["staging_consolidation_server"]
            # Modify the csr_sender.config file with the appropiate FQDN of the 
            # consolidation server.
            modify_csr_sender_config(consolidation_server)
    except Exception as exception:
        log.error("Unable to set consolidation server settings.\nException: "\
            "{0}".format(exception))
        sys.exit(1)


def get_args(argv):
    '''
        Get, validate and parse arguments.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument(dest = "mode",
        choices = ["satellite", "consolidation"],
        default = None)
    parser.add_argument("-v", "--verbose",
        help = "Will print INFO, WARNING, and ERROR messages to the stdout "\
            "or stderr.",
        dest = "verbose",
        default = False,
        action = "store_true")
    parser.add_argument("--git-repo",
        help = "Absolute location of the git repository. (Optional)",
        dest = "git_repo",
        required = False,
        default = os.path.join("/root","metering","gitRepo","SCCM"))
    if "satellite" in sys.argv:
        parser.add_argument("-e", "--environment",
            help = "Environment where the current server is being configured. "\
                "Either 'GA' or 'staging'. Defaults to 'staging'.",
            choices = ["GA", "staging"],
            dest = "environment",
            required = "satellite" in sys.argv,
            default = "staging")
    elif "consolidation" in sys.argv and \
        ("--environment" in sys.argv or "-e" in sys.argv):
        log.warning("--environment | -e parameter is ignored while deploying "\
            "on a consolidation server.")
    args = parser.parse_args()

    # Validate that git_repo is a valid directory.
    if os.path.isdir(args.git_repo) == False:
        log.error("{0} is not a valid directory.".format(args.git_repo))
        sys.exit(1)

    # Set logging level.
    if args.verbose:
        log.setLevel(logging.INFO)

    # Call the main function with the appropriate mode.
    main(args.git_repo, args.mode, args.environment)


if __name__ == "__main__":
    # Parse arguments from the CLI.
    get_args(sys.argv[1:])
exit(0)
