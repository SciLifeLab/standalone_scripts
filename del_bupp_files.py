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
            # Typical file name: githubbackup_2019-09-09T15:42:39.980130.tar.gz
            try:
                file_date = datetime.datetime.strptime(bn[13:23], "%Y-%m-%d")
            except ValueError as e:
                print(e, file=sys.stderr)
                continue

        if args.mode == 'zendesk' and 'github' not in f:
            # Typical file name: 2019-09-09_16-33.bckp.json
            try:
                file_date = datetime.datetime.strptime(bn[0:10], "%Y-%m-%d")
            except ValueError as e:
                sys.stderr.write(e)
                print(e, file=sys.stderr)
                continue

        if file_date is None:
            continue

        remove = False
        # Keep all backups from the last 20 days
        if (datetime.datetime.now() - file_date).days > 20:

            # Remove all but a single last one of the weekly backups
            # for April, August and December.
            if ((file_date.month == 4 and file_date.day < 24) or
                (file_date.month in [8,12] and file_date.day < 25)):
                remove = True

            # Remove all other weekly backups
            if file_date.month % 4:
                remove = True

            # Remove everything older than approx 2 years
            if (datetime.datetime.now() - file_date).days > 600:
                remove = True

            if remove:
                if args.danger:
                    os.remove(f)
                else:
                    sys.stderr.write("Would have removed {}\n".format(f))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=['github', 'zendesk'])
    parser.add_argument("--danger", action="store_true", help="Without this, no files will be deleted")
    args = parser.parse_args()

    main(args)
