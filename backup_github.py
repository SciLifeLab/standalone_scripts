#!/usr/bin/env python

import argparse
import os
import sys
import logbook

from subprocess import check_call
from subprocess import CalledProcessError

from pygithub3 import Github

LOG = logbook.Logger('GitHub Backup')


track_all_branches = """
for branch in `git branch -a | grep remotes | grep -v HEAD | grep -v master`; do
    git branch --track ${branch##*/} $branch
done
"""


class cd(object):
    """Changes the current working directory to the one specified
    """

    def __init__(self, path):
        self.original_dir = os.getcwd()
        self.dir = path

    def __enter__(self):
        os.chdir(self.dir)

    def __exit__(self, type, value, tb):
        os.chdir(self.original_dir)


def backup(user, password, dest):
    """Performs a backup of all the public repos in user's GitHub account on dest
    """
    gh = Github(login=user, user=user, password=password)
    repos = gh.repos.list(type='all')
    for repo in repos.all():
        repo_path = os.path.join(dest, repo.name)
        LOG.info("Backing up repository {}".format(repo.name))
        #If the repository is present on destination, update all branches
        if os.path.exists(repo_path):
            LOG.info("The repository {} already exists on destination. Pulling " \
                     "all branches".format(repo.name))
            with cd(repo_path):
                try:
                    cl = ['git', 'up']
                    check_call(cl)
                except CalledProcessError:
                    LOG.error("There was an error fetching the branches from " \
                              "the repository {}, skipping it".format(repo.name))
                    pass
        #Otherwise clone the repository and fetch all branches
        else:
            LOG.info("The repository {} does not exist on destination. " \
                     "Cloning it. ".format(repo.name))
            try:
                cl1 = ['git', 'clone', '-q', repo.clone_url, repo_path]
                check_call(cl1)
                with cd(repo_path):
                    check_call(track_all_branches, shell=True)
            except CalledProcessError:
                LOG.error('Error cloning repository {}, skipping it'.format(repo.name))
                pass

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Clones all the public " \
            "repositories from a GitHub account")
    parser.add_argument("user", type=str, help="GitHub username")
    parser.add_argument("password", type=str, help="GitHub password")
    parser.add_argument("-d", type=str, help="Destination of the copy")
    args = parser.parse_args()

    user = args.user
    password = args.password
    dest = os.getcwd() if not args.d else args.d

    print user, dest
    backup(user, password, dest)
