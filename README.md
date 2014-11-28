# Standalone scripts

Repository to store standalone scripts that do not belong to any bigger package or repository.

## Contents

#### backup_gihtub.py
Performs a backup of all the repositories in user's GitHub account.

*Dependencies*

* logbook
* pygithub3

#### data_to_ftp.py
Used to transfer data to user's ftp server maintaing the directory tree structure. Main intention
is to get the data to user outside Sweden.

#### db_sync.sh
Script used to mirror (completely) Clarity LIMS database from production to staging server

#### quota_log.py
Returns a summary of quota usage in Uppmax

*Dependencies*

* couchdb
* pprint
