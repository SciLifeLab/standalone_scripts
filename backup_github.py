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


def backup(user, password, access_token, organizations, dest):
    """Performs a backup of all the accessible repos in given organizations
    """
    if password is None or user is None:
        logger.error("No valid github credentials provided. Exiting!")
        sys.exit(-1)
    if password is not None:
        github_instance = Github(access_token)
        repositories = []  # list of repository *iterators*
        for organization in organizations:
            logger.info("Github API rate limit: {}".format(github_instance.get_rate_limit()))
            github_organization = github_instance.get_organization(organization)

            repositories.append(github_organization.get_repos(type='all'))

            # Check that destination directories are set up
            organization_destination_path = os.path.join(dest, organization)
            if not os.path.exists(organization_destination_path):
                os.mkdir(organization_destination_path)

    for repository in chain(*repositories):
        logger.info("Github API rate limit: {}".format(github_instance.get_rate_limit()))
        if password is not None and repository.private is True:
            source = repository.clone_url.replace(
                                "https://",
                                "https://{}:{}@".format(user, password)
                                )
        else:
            source = repository.clone_url

        repository_path = os.path.join(dest, repository.organization.login,
                                       repository.name)
        logger.info("Backing up repository {}".format(repository.name))
        # If the repository is present on destination, update all branches
        if os.path.exists(repository_path):
            logger.info("The repository {} already exists on destination. "
                        "Pulling all branches".format(repository.name))
            with cd(repository_path):
                try:
                    # These stdout and stderr flush out the normal github
                    # output the alternative of using -q doesn't always work
                    check_call(['git', 'stash'], stdout=PIPE, stderr=STDOUT)
                    check_call(['git', 'pull'], stdout=PIPE, stderr=STDOUT)
                # General exception to better catch errors
                except CalledProcessError:
                    logger.error("There was an error fetching the branches "
                                 "from the repository {}, "
                                 "skipping it".format(repository.name))
                    pass
            logger.info("Finished copying repo {}".format(repository.name))
        # Otherwise clone the repository and fetch all branches
        else:
            logger.info("The repository {} isn't cloned at {}, cloning instead"
                        " of updating...".format(repository.name,
                                                 repository_path))
            try:
                check_call(['git', 'clone', source, repository_path],
                           stdout=PIPE, stderr=STDOUT)
                logger.info("Cloning {}".format(repository.name))
            except CalledProcessError as e:
                logger.error("ERROR: Error cloning repository {}, "
                             "skipping it".format(repository.name))
                logger.error(str(e))
                pass
            try:
                with cd(repository_path):
                    check_call(track_all_branches, shell=True,
                               stdout=PIPE, stderr=STDOUT)
                    logger.info("Fetching branches for {}".format(
                                                            repository.name
                                                            ))
            except CalledProcessError as e:
                logger.error("ERROR: Problem fetching branches for "
                             "repository {}, skipping it".format(
                                                            repository.name
                                                            ))
                logger.error(str(e))
                pass


def compress_and_move(source, final_dest):
    stamp = datetime.datetime.now().isoformat()
    try:
        with tarfile.open("githubbackup_{}.tar.gz".format(stamp), "w:gz") as tar:
            tar.add(source, arcname=os.path.basename(source))
        tar.close()
    except Exception as e:
        logger.error("Unable to compress backup into archive.")
        raise e
    # Moves output to backup folder
    try:
        shutil.move("{}/githubbackup_{}.tar.gz".format(os.getcwd(), stamp),
                    final_dest)
    except Exception as e:
        logger.error("Unable to move backup archive.")
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=str,
                        help="Destination of the uncompressed copy")
    parser.add_argument('--final_dest', type=str, help="Final destination of "
                        "the copy, typically a directory that will "
                        "be backed up", required=True)
    parser.add_argument('--organizations', nargs='*', required=True,
                        help="Github organizations that should be backed up")
    parser.add_argument('--logfile', type=str, default='githubbackup.log',
                        help="File to append the log to.")
    args = parser.parse_args()

    dest = os.getcwd() if not args.dest else args.dest

    # Need to check if the directory exists for the given log file
    logfile_directory = os.path.dirname(os.path.abspath(args.logfile))
    if not os.path.exists(logfile_directory):
        logging.error("The directory for the specified log file does not exist. Aborting")
        sys.exit(-1)

    logging.basicConfig(
        filename=args.logfile, level=logging.INFO,
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

    config = credentials()
    user = config.get("github_username")
    password = config.get("github_password")
    access_token = config.get("access_token")

    if user is None:
        logger.error("Missing user from the .githubcredentials file. Exiting!")
        sys.exit(-1)
    if password is None:
        logger.error("Missing password from the .githubcredentials file. Exiting!")
        sys.exit(-1)
    if access_token is None:
        logger.error("Missing access_token from the .githubcredentials file. Exiting!")
        sys.exit(-1)

    backup(user, password, access_token, args.organizations, dest)
    compress_and_move(dest, args.final_dest)
