#!/usr/bin/python3

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


def print_file_size(path, bytes, non_human_readable=False):
    if non_human_readable:
        print(f"{bytes}\t{path}")
    else:
        print(f"{sizeof_fmt(bytes)}\t{path}")


def main(input_paths, non_human_readable=False, size_sorted=False, depth=0):
    paths = []
    new_paths = []
    for path in input_paths:
        paths.append(path)
        for depth in range(depth):
            path += '/*'
            depth_paths = glob.glob(path)
            if not depth_paths:
                break
            paths += depth_paths

    filesizes = []
    for path in paths:
        # Check if path is not a directory
        if not os.path.isdir(path):
            try:
                statinfo = os.stat(path)
            except OSError as e:
                print(str(e), file=sys.stderr)
            bytes = statinfo.st_size
            filesizes.append((path, bytes))
        else:  # Directory
            # Requires python 3.5 or higher
            result = subprocess.run(
                ["getfattr", "-n", "ceph.dir.rbytes", path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if result.returncode != 0:
                print(result.stderr.decode("utf-8"), file=sys.stderr)
                continue
            lines = result.stdout.decode("utf-8").splitlines()
            for line in lines:
                if line.startswith("# file:"):
                    filename = line.split(" ")[2]
                    filename = filename.strip()
                if line.startswith("ceph.dir.rbytes"):
                    bytes = int((line.split("=")[1]).replace('"', ""))
                    filesizes.append((filename, bytes))
                    bytes_readable = sizeof_fmt(bytes)

    if size_sorted:
        filesizes.sort(key=lambda x: x[1], reverse=False)

    for filename, bytes in filesizes:
        print_file_size(filename, bytes, non_human_readable)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A faster version of `du` for our cluster Miarka, using ceph supplied space consumption."
    )
    parser.add_argument("paths", nargs="*", help="The path that should be looked into")
    parser.add_argument(
        "-n",
        "--non-human-readable",
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

    main(args.paths, args.non_human_readable, args.sort, args.depth)
