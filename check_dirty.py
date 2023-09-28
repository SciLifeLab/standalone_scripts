#!/usr/bin/python3
"""
Checks if one or several repos are dirty and/or contain untracked files.
"""
import argparse
import sys
from git import Repo

def check_if_dirty(repo_paths):
    for path in repo_paths:
        repo = Repo(path)
        if repo.is_dirty():
            print("Repo is dirty: ", path, file=sys.stderr)
        if repo.untracked_files:
            print("Repo has untracked files: ", path, repo.untracked_files, file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='check if dirty')
    parser.add_argument('repo_paths', nargs='+', help='one or more paths to repositories to check')
    args = parser.parse_args()

    check_if_dirty(args.repo_paths)
