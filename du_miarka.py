#!/usr/bin/python3
"""A faster version of `du` for our cluster Miarka, using ceph supplied space consumption.

Author: @alneberg
"""


import argparse
import glob
import os
import subprocess
import sys


def sizeof_fmt(num, suffix="B"):
    """Human readable format for file sizes

    From https://stackoverflow.com/a/1094933/
    """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def print_file_size(path, bytes, raw_bytes=False):
    if raw_bytes:
        print(f"{bytes}\t{path}")
    else:
        print(f"{sizeof_fmt(bytes)}\t{path}")


def main(input_paths, raw_bytes=False, size_sorted=False, depth=0):
    paths = []
    # Do the depth traversing
    for path in input_paths:
        paths.append(path)
        for depth in range(depth):
            path += "/*"
            depth_paths = glob.glob(path)
            if not depth_paths:
                break  # reached the tip of the branch
            paths += depth_paths

    # Append files and sizes to a list so that it can be sorted in the end
    filesizes = []
    for path in paths:
        # Check if path is not a directory
        if not os.path.isdir(path):
            # getfattr didn't work for files, so we use os.stat
            try:
                statinfo = os.stat(path)
            except OSError as e:
                # This happens ƒor example with broken links
                print(str(e), file=sys.stderr)
            bytes = statinfo.st_size
            filesizes.append((path, bytes))
        else:
            # .run Requires python 3.5 or higher
            result = subprocess.run(
                ["getfattr", "-n", "ceph.dir.rbytes", path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if result.returncode != 0:
                print(result.stderr.decode("utf-8"), file=sys.stderr)
                continue

            # Typical output from getfattr:
            #
            #  # file: proj/ngi2016004/private/johannes/pipelines
            #  ceph.dir.rbytes="3699513252"
            #

            lines = result.stdout.decode("utf-8").splitlines()
            for line in lines:
                if line.startswith("# file:"):
                    filename = line.split(" ")[2]
                    filename = filename.strip()
                elif line.startswith("ceph.dir.rbytes"):
                    bytes = int((line.split("=")[1]).strip('"')
                    filesizes.append((filename, bytes))

    if size_sorted:
        filesizes.sort(key=lambda x: x[1], reverse=False)

    for filename, bytes in filesizes:
        print_file_size(filename, bytes, raw_bytes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A faster version of `du` for our cluster Miarka, using ceph supplied space consumption."
    )
    parser.add_argument("paths", nargs="*", help="The paths that should be looked into")
    parser.add_argument(
        "-r",
        "--raw-bytes",
        action="store_true",
        help="Print sizes in bytes instead of human readable format (e.g. 1K 234M 2G)",
    )
    parser.add_argument("--sort", action="store_true", help="Sort by size")
    parser.add_argument(
        "-d",
        "--depth",
        default=0,
        type=int,
        help="The number of levels to go down to inside the given directory",
    )
    args = parser.parse_args()

    main(args.paths, args.raw_bytes, args.sort, args.depth)
