#!/usr/bin/env python
'''
    Usage:
        python add_ssh_keys.py [-h] -v

    Optional arguments:
        -h, --help     show this help message and exit
        -v, --verbose  Will print INFO, WARNING, and ERROR messages to the 
                stdout or stderr.

    Description:
        This program will add any missing SSH keys to ~/.ssh/authorized_keys

    Author:
        Alan Verdugo (alanvemu@mx1.ibm.com)

    Creation date:
        2018-02-21

    Modification list:
        CCYY-MM-DD  Author                  Description

'''

# For path navigation.
import os

# Needed for system and environment information.
import sys

# Handling arguments.
import argparse

# Handle logging.
import logging

# JSON library to read configuration file.
import json

# For a fancy and cross-platform way of getting the home directory for the user.
from os.path import expanduser


# The JSON file containing the keys.
curr_dir = os.path.dirname(os.path.realpath(__file__))
keys_file = os.path.join(curr_dir, "sccm_ssh_keys.json")

# Authenticated_keys file.
home = expanduser("~")
authorized_keys_file = os.path.join(home, ".ssh", "authorized_keys")


# Logging configuration.
log = logging.getLogger("ssh_keys_handling")
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    '''
        This function will read the SCCM SSH keys from a JSON file and then 
        check ~/.ssh/authorized_keys looking for them, if any keys are missing, 
        they will be appended.
    '''
    # A list to store the SSH keys.
    key_list = []

    # Open and read the JSON file containing the keys.
    try:
        keys_json_data = json.load(open(keys_file, "r+"))
        # Add the SSH keys to a list.
        for item in keys_json_data["keys"]:
            key_list.append(item["key"])
    except Exception as exception:
        log.error("Error reading SSH keys from file. Exception: "\
            "{0}".format(exception))
        exit(1)

    # Look for every SSH key and add it to authorized_keys if it is not 
    # there already.
    try:
        for key in key_list:
            # Open the authorized_keys file in ~/.ssh/
            with open(authorized_keys_file, "a+") as authorized_keys:
                if not any(key == line.rstrip("\n") for line in authorized_keys):
                    log.info("Adding key: {0}".format(key))
                    # Append key (And newlines, for aesthetic purposes only).
                    authorized_keys.write("\n" + key + "\n")
    except Exception as exception:
        log.error("Unable to add key to authorized_keys file. Exception: "\
            "{0}".format(exception))
        exit(2)


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
exit(0)
