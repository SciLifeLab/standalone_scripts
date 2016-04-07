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
###### DO NOT USE THIS SCRIPT! Use `taca server_status uppmax` instead!
Returns a summary of quota usage in Uppmax

*Dependencies*

* couchdb
* pprint

#### couchdb_replication.py
handles the replication of the couchdb instance

*Dependencies*

* couchdb
* logbook
* pycrypto
* yaml

#### ZenDesk Attachments Backup
Takes a ZenDesk XML dump backup file and searches for attachment URLs that match specified
filename patterns. These are then downloaded to a local directory.

*Dependencies*

* argparse
* os
* urllib2
* re

#### repooler.py
Calculates a decent way to re-pool samples in the case that the amount of clusters from each
sample doesn't reach the required threshold due to mismeasurements in concentration.

*Dependencies*

* couchdb
* click
* Genologics: lims, config, entities

