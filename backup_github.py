#!/usr/bin/env python
"""Clones all the repositories from the specified GitHub organizations.

   After cloning to the destination specified, an archive is created and moved
   to a final destination which is backed up. Credentials needs to be given,
   and private repositories can only be cloned if the user has access to those.
   uses config file .githubbackup_config.yaml if no user/pw is provided.
"""

import argparse
import os
import sys
import logging
import datetime
import tarfile
import shutil
import yaml
from itertools import chain

from subprocess import CalledProcessError, PIPE, STDOUT, check_call
from github import Github

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


def credentials():
    config_file = os.path.join(os.environ.get("HOME"),
                               ".githubbackup_creds.yaml")
    if not os.path.exists(config_file):
        config_file = os.path.join(os.environ.get("GITHUBBACKUP_CREDS"))
    with open(config_file) as f:
        conf = yaml.load(f, Loader=yaml.SafeLoader)
    return conf


def backup(user, password, organizations, dest):
    """Performs a backup of all the accessible repos in given organizations
    """
    if password is None or user is None:
        logger.error("No valid github credentials provided. Exiting!")
        sys.exit(-1)
    if password is not None:
        gh = Github(user, password)
        repos_l = []  # list of repos iterators
        for org in organizations:
            gh_org = gh.get_organization(org)

            repos_l.append(gh_org.get_repos(type='all'))

    for repo in chain(*repos_l):
        if password is not None and repo.private is True:
            source = repo.clone_url.replace(
                                "https://",
                                "https://{}:{}@".format(user, password)
                                )
        else:
            source = repo.clone_url

        repo_path = os.path.join(dest, repo.name)
        logger.info("Backing up repository {}".format(repo.name))
        # If the repository is present on destination, update all branches
        if os.path.exists(repo_path):
            logger.info("The repository {} already exists on destination. "
                        "Pulling all branches".format(repo.name))
            with cd(repo_path):
                try:
                    # These stdout and stderr flush out the normal github
                    # output the alternative of using -q doesn't always work
                    check_call(['git', 'stash'], stdout=PIPE, stderr=STDOUT)
                    check_call(['git', 'pull'], stdout=PIPE, stderr=STDOUT)
                # General exception to better catch errors
                except CalledProcessError:
                    logger.error("There was an error fetching the branches "
                                 "from the repository {}, "
                                 "skipping it".format(repo.name))
                    pass
            logger.info("Finished copying repo {}".format(repo.name))
        # Otherwise clone the repository and fetch all branches
        else:
            logger.info("The repository {} isn't cloned at {}, cloning instead"
                        " of updating...".format(repo.name, repo_path))
            try:
                check_call(['git', 'clone', source, repo_path],
                           stdout=PIPE, stderr=STDOUT)
                logger.info("Cloning {}".format(repo.name))
            except CalledProcessError:
                logger.error("ERROR: Error cloning repository {}, "
                             "skipping it".format(repo.name))
            pass
            try:
                with cd(repo_path):
                    check_call(track_all_branches, shell=True,
                               stdout=PIPE, stderr=STDOUT)
                    logger.info("Fetching branches for {}".format(repo.name))
            except CalledProcessError:
                logger.error("ERROR: Problem fetching branches for "
                             "repository {}, skipping it".format(repo.name))
                pass


def compressAndMove(source, final_dest):
    stamp = datetime.datetime.now().isoformat()
    try:
        with tarfile.open("githubbackup_{}.tar.gz".format(stamp), "w:gz") as tar:
            tar.add(source, arcname=os.path.basename(source))
        tar.close()
    except Exception:
        logger.error("Unable to compress backup into archive.")
        pass
    # Moves output to backup folder
    try:
        shutil.move("{}/githubbackup_{}.tar.gz".format(os.getcwd(), stamp),
                    final_dest)
    except Exception:
        logger.error("Unable to move backup archive.")


if __name__ == "__main__":
    logfile = 'githubbackup.log'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", nargs='?', type=str, help="GitHub username")
    parser.add_argument("--password", nargs='?', type=str,
                        help="GitHub password")
    parser.add_argument("--dest", type=str,
                        help="Destination of the uncompressed copy")
    parser.add_argument('--final_dest', type=str, help="Final destination of "
                        "the copy, typically a directory that will "
                        "be backed up", required=True)
    parser.add_argument('--organizations', nargs='*', required=True,
                        help="Github organizations that should be backed up")
    args = parser.parse_args()

    # Command line flags take priority. Otherwise use config
    user = args.user
    password = args.password
    config = credentials()
    if user is None or password is None:
        user = config.get("github_username")
        password = config.get("github_password")

    dest = os.getcwd() if not args.dest else args.dest

    logging.basicConfig(
        filename=logfile, level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    logger = logging.getLogger(__name__)

    # Handler that will log warnings or worse to stderr
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.WARNING)
    logger.addHandler(stderr_handler)

    logger.info("Creating backup at {}, for organizations {}".format(
                                            dest,
                                            ", ".join(args.organizations)
                                            ))
    backup(user, password, args.organizations, dest)
    compressAndMove(dest, args.final_dest)
