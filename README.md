# Standalone scripts

Repository to store standalone scripts that do not belong to any bigger package or repository.

## Contents

#### backup_gihtub.py
Performs a backup of all the repositories in user's GitHub account.

#### quota_log.py
Returns a summary of quota usage in Uppmax

#### db_sync.sh
Script used to mirror (completely) Clarity LIMS database from production to staging server

*Dependencies*

* couchdb
* logbook
* pprint
* pygithub3

#### data_to_ftp.py
Used to transfer data to user's ftp server maintaing the directory tree structure. Main intention
is to get the data to user outside Sweden.

