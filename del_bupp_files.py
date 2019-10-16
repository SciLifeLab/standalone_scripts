#!/usr/bin/python
"""A script to clean up among files which will be backed up on tape.

slightly adapted from the original, kindly supplied by it-support@scilifelab.se.
"""

import glob
import datetime
import os
import argparse
import sys

def main(args):
    files = glob.glob("/home/bupp/other/*")
    for f in files:
        bn = os.path.basename(f)
        file_date = None
        if args.mode == 'github' and 'github' in f:
            file_date = datetime.datetime.strptime(bn[13:23], "%Y-%m-%d")
            
        if args.mode == 'zendesk' and 'github' not in f:
            file_date = datetime.datetime.strptime(bn[0:10], "%Y-%m-%d")

        if file_date is None:
            continue

        # Save backups from April, August, December
        # Remove others older than 90 days
        if ((datetime.datetime.now() - file_date).days > 90 and file_date.month % 4):
            if args.danger:
                os.remove(f)
            else:
                sys.stderr.write("Would have removed {}".format(f))

        # Remove everything older than approx 2 years
        if (datetime.datetime.now() - file_date).days > 600:
            if args.danger:
                os.remove(f)
            else:
                sys.stderr.write("Would have removed {}".format(f))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=['github', 'zendesk'])
    parser.add_argument("--danger", action="store_true", help="Without this, no files will be deleted")
    args = parser.parse_args()

    main(args)
