# Standalone scripts

Repository to store standalone scripts that do not belong to any bigger package or repository.

## backup_github.py
Performs a backup of all the repositories in user's GitHub account.

###### Dependencies

* logbook
* pygithub3

## data_to_ftp.py
Used to transfer data to user's ftp server maintaing the directory tree structure. Main intention
is to get the data to user outside Sweden.

## db_sync.sh
Script used to mirror (completely) Clarity LIMS database from production to staging server

## quota_log.py
> **DO NOT USE THIS SCRIPT!**
>
> Use `taca server_status uppmax` instead!

Returns a summary of quota usage in Uppmax

###### Dependencies

* couchdb
* pprint

## couchdb_replication.py
handles the replication of the couchdb instance

*Dependencies*

* couchdb
* logbook
* pycrypto
* yaml

## ZenDesk Attachments Backup
Takes a ZenDesk XML dump backup file and searches for attachment
URLs that match specified filename patterns. These are then
downloaded to a local directory.

This script should be run manually on tools when the manual
ZenDesk backup zip files are saved.

#### Usage
Run with a typical ZenDesk backup zip file (will look for `tickets.xml`
inside the zip file):
```
zendesk_attachment_backup.py -i xml-export-yyyy-mm-dd-tttt-xml.zip
```

Alternatively, run directly on `tickets.xml`:
```
zendesk_attachment_backup.py -i ngisweden-yyyymmdd/tickets.xml
```

###### Usage
If you're using this on `tools` for the first time, you'll need to set up conda.
`tools` only has v2.6 of Python installed by default, which is old and not
compatible with this script

These instructions get a copy of Python 2.7 for you. You only need to do this once:

1. Download & install Miniconda
```
wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```
2. Tell the installer to prepend itself to your `.bashrc` file
3. Log out and log in again, check that `conda` is in your path
4. Create an environment for Python 2.7
```
conda create --name tools_py2.7 python pip
```
5. Add it to your `.bashrc` file so it always loads
```
echo source activate tools_py2.7 >> .bashrc
```

Now Python 2.7 is installed, the zendesk attachment backup script should work.
You can run it by going to the Zendesk backup directory and running it on
any new downloads:
```
zendesk_attachment_backup.py <latest_backup>.zip
```


###### Dependencies
* argparse
* os
* urllib2
* re
* sys
* zipfile

## repooler.py
Calculates a decent way to re-pool samples in the case that the amount of clusters from each
sample doesn't reach the required threshold due to mismeasurements in concentration.

###### Dependencies

* couchdb
* click
* Genologics: lims, config, entities

#### set_bioinforesponsible.py
Calls up the genologics LIMS directly in order to more quickly set a bioinformatics responsible. 

*Dependencies*

* Genologics: lims, config

## Backup Zendesk Tickets 
### backup_zendesk_tickets.py
Retirives tickets from Zendesk via Zendesk API and saves it on the filesystem as json file.

###### Dependencies
* click
* zendesk
* yaml

### Requires config file!!
Config file should be in `yaml` format and should contain the following mandatory parameters:
```
url: https://organization.zendesk.com
username: name.last_name@organization.com
token: token_which_you_generate_via_zendesk_web_interface
output_path: /home/user/zendesk_backup
```

### Usage
```
Usage: backup_zendesk_tickets.py [OPTIONS]

Options:
  --config-file PATH  Path to the config file  [required]
  --days INTEGER      Since how many days ago to backup tickets
  --help              Show this message and exit.
```
