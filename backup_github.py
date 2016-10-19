#!/usr/bin/env python

import argparse
import os
import sys
import logging
import datetime
import tarfile
import shutil

from subprocess import CalledProcessError, PIPE, STDOUT, check_call
from pygithub3 import Github

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
    #logfile = open(logfile, "w")
    if not password is None:
        gh = Github(login=user, user=user, password=password)
        repos = gh.repos.list(type='all')
    else:
        gh = Github()
        repos = gh.repos.list(type='all', user=user)
    for repo in repos.all():
        if password is not None and repo.private is True:
            source = repo.clone_url.replace("https://", "https://{}:{}@".format(user, password))
        else:
            source = repo.clone_url
            
        repo_path = os.path.join(dest, repo.name)
        logging.info("Backing up repository {}".format(repo.name))
        #If the repository is present on destination, update all branches
        if os.path.exists(repo_path):
            logging.info("The repository {} already exists on destination. Pulling " \
                     "all branches".format(repo.name))
            with cd(repo_path):
                try:
                    #These stdout and stderr flush out the normal github output
                    #the alternative of using -q doesn't always work
                    check_call(['git', 'stash'], stdout=PIPE, stderr=STDOUT)
                    check_call(['git', 'pull'], stdout=PIPE, stderr=STDOUT)
		#General exception to better catch errors
                except CalledProcessError:
		    print "ERROR: There was an error fetching the branches from " \
                              "the repository {}, skipping it".format(repo.name)
                    logging.error("There was an error fetching the branches from " \
                              "the repository {}, skipping it".format(repo.name))
                    pass
        #Otherwise clone the repository and fetch all branches
        else:
            logging.info("The repository {} does not exist on destination".format(repo.name))
        try:
            check_call(['git', 'clone', source, repo_path], stdout=PIPE, stderr=STDOUT)
            logging.info("Cloning {}".format(repo.name))
        except CalledProcessError:
            print 'ERROR: Problem cloning repository {}, skipping it'.format(repo.name)
            logging.error('Error cloning repository {}, skipping it'.format(repo.name))
            pass
        try:
            with cd(repo_path):
                check_call(track_all_branches, shell=True, stdout=PIPE, stderr=STDOUT)
                logging.info("Fetching branches for {}".format(repo.name))
        except CalledProcessError:
            print 'ERROR: Problem fetching branches for repository {}, skipping it'.format(repo.name)
            logging.error('Problem fetching branches for repository {}, skipping it'.format(repo.name))
            pass

def compressAndMove(source):
    stamp = datetime.datetime.now().isoformat()
    try:
        with tarfile.open("githubbackup_{}.tar.gz".format(stamp), "w:gz") as tar:
            tar.add(source, arcname=os.path.basename(source))
        tar.close()
    except Exception:
    	print "ERROR: Unable to compress backup into archive."
    	logging.error("Unable to compress backup into archive.")
    	pass
    #Moves output to backup folder
    try:
       shutil.move("{}/githubbackup_{}.tar.gz".format(os.getcwd(), stamp), "/home/bupp/scilifelab_github/archives/")
    except Exception:
        print "ERROR: Unable to move backup archive."
        logging.error("Unable to move backup archive.")

if __name__=="__main__":
    logfile = 'githubbackup.log'
    parser = argparse.ArgumentParser(description="Clones all the " \
            "repositories from a GitHub account." \
            "Restricted to public ones if no password is given")
    parser.add_argument("user", type=str, help="GitHub username")
    parser.add_argument("password", nargs='?', type=str, help="GitHub password")
    parser.add_argument("-d", type=str, help="Destination of the copy")
    args = parser.parse_args()

    user = args.user
    password = args.password
    dest = os.getcwd() if not args.d else args.d
    
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info("Pulling backup into {} by using credentials for {}".format(dest, user))

    backup(user, password, dest)
    compressAndMove(dest)

