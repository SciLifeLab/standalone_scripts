# Standalone scripts

Repository to store standalone scripts that do not belong to any bigger package or repository.

## Contents

#### backup_gihtub.py
Performs a backup of all the repositories in user's GitHub account.

*Dependencies*

* couchdb
* logbook
* pprint
* pygithub3

#### data_to_ftp.py
Used to transfer data to user's ftp server maintaing the directory tree structure. Main intention
is to get the data to user outside Sweden.

*Dependencies*

* ConfigParser
* ftplib
* other modules such as os,sys,argparse
