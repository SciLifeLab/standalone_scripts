#!/usr/bin/python3
"""
Checks if a repo is dirty.
"""
import argparse
import sys
from git import Repo

def check_if_dirty(repo_path):
    repo = Repo(repo_path)
    if repo.is_dirty():
        print("Repo is dirty.", file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='check if dirty')
    parser.add_argument('repo_path', metavar='P', type=str, help='path to repository to check')
    args = parser.parse_args()

    check_if_dirty(args.repo_path)
