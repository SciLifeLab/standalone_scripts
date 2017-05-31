# Standalone scripts

Repository to store standalone scripts that do not belong to any bigger package or repository.


### compute_undet_index_stats.py
used to fetch stats about undermined indexes.
This scripts queries statusdb x_flowcell_db  and fetch informaiton about runs.
The following operations are supported:

 - check_undet_index: given a specific index checks all FCs and prints all FC and lanes where the indx appears as undetermined
 - most_undet: outputs a summary about undetermiend indexes, printing the most 20 most occurring indexes for each instrument type
  - single_sample_lanes: prints stats about HiSeqX lanes run with a single sample in it
  - workset_undet: prints for each workset the FC, lanes and samples where the specified index has been found in undet. For each sample the plate position is printed.

#### Usage
Examples:
 
  - compute for each workset the FC that contain a lane with index CTTGTAAT present in undet at least 0.5M times:
    -  `python compute_undet_index_stats.py --config couch_db.yaml --index CTTGTAAT --mode workset_undet --min_occurences 500000` 
 - Compute a list of the most occurring undetemriend indexes for HiSeqX runs:
    - `python compute_undet_index_stats.py --config couch_db.yaml -- mode most_undet --instrument-type HiSeqX`




### DupRateTrends_from_charon.py
Used to fetch stats from charon about duplication rate trends and number of sequenced human genomes

#### Usage
Example: `DupRateTrends_from_charon.py`

```
Usage: DupRateTrends_from_charon.py

Options:
  -h, --help            show this help message and exit
  -t TOKEN, --token TOKEN
                        Charon API Token. Will be read from the env variable
                        CHARON_API_TOKEN if not provided
  -u URL, --url URL     Charon base url. Will be read from the env variable
                        CHARON_BASE_URL if not provided
```


### compute_production_stats.py
This scripts queries statusdb x_flowcelldb and project database and fetches informations about what organism have been sequenced. More in detail:

- reports total number of lanes sequenced per year
- reports total number of Human lanes and of Non-Human lanes sequenced (divided per instrument)
- other stats...

##### Usage
Example: `compute_production_stats.py --config couchdb.yaml`
```
Usage: compute_production_stats.py --config couchdb.yam

Options:
    --config CONFIG  configuration file
```
#### Configuration
Requires a config file to access statusdb
```
statusdb:
    url: path_to_tool
    username: Username
    password: *********
    port: port_number
```


### backup_zendesk_tickets.py
Used to automatically back up tickets from zendesk

#### Usage
Example: `backup_zendesk_tickets.py --config-file ~/config_files/backup_zendesk_tickets.yaml --days 30`

```
Usage: backup_zendesk_tickets.py [OPTIONS]

Options:
  --config-file PATH  Path to the config file  [required]
  --days INTEGER      Since how many days ago to backup tickets
  --help              Show this message and exit.

```

#### Dependencies
* zendesk
* click
* yaml
* requests

#### Configuration
Requires a config file:

```
url: https://ngisweden.zendesk.com
username: mattias.ormestad@scilifelab.se
token: <ask Mattias to get token>
output_path: /Users/kate/Dropbox/dropbox_work/zendesk/output
```


### backup_github.py
Performs a backup of all the repositories in user's GitHub account.

###### Dependencies

* logbook
* pygithub3

### couchdb_replication.py
handles the replication of the couchdb instance

###### Dependencies

* couchdb
* logbook
* pycrypto
* yaml

### data_to_ftp.py
Used to transfer data to user's ftp server maintaing the directory tree structure. Main intention
is to get the data to user outside Sweden.

### db_sync.sh
Script used to mirror (completely) Clarity LIMS database from production to staging server

### get_sample_names.py
Prints a list of analyzed samples with user_id and ngi_id
#### Usage:
```
get_sample_names.py P1234
```

### index_fixer.py
Takes in a SampleSheet.csv and generates a new one with swapped or reverse complimented indexes.

###### Dependencies

* click
* Flowcell_Parser: SampleSheetParser

### merge_and_rename_NGI_fastq_files.py
 Merges all fastq_files from a sample into one file.
```
merge_and_rename_NGI_fastq_files.py path/to/dir/with/inputfiles/ path/to/output/directory
```



### project_status_extended.py
Collects information about specified project from the filesystem of irma.
Without any arguments prints statistics for each sample, such as:
* Number of reads
* Coverage
* Duplication rate
* Mapping rate

#### Usage
`python project_status_extended.py P4601`

To remove headers from the output, use option `--skip-header`

The script can take additional arguments:
```
--sequenced           List of all the sequenced samples
--resequenced         List of samples that have been sequenced more than
once, and flowcells
--organized           List of all the organized flowcells
--to-organize         List of all the not-organized flowcells
--analyzed            List of all the analysed samples
--to-analyze          List of samples that are ready to be analyzed
--analysis-failed     List of all the samples with failed analysis
--under-analysis      List of the samples under analysis
--under-qc            List of samples under qc. Use for projects
without BP
--incoherent          Project-status but only for samples which have
incoherent number of sequenced/organized/analyzed
--low-coverage        List of analyzed samples with coverage below 28.5X
--undetermined        List of the samples which use undetermined  
--low-mapping         List of all the samples with mapping below 97 percent
--flowcells           List of flowcells where each sample has been sequenced
```

### repooler.py
Calculates a decent way to re-pool samples in the case that the amount of clusters from each
sample doesn't reach the required threshold due to mismeasurements in concentration.

###### Dependencies

* couchdb
* click
* Genologics: lims, config, entities

### quota_log.py
> **DO NOT USE THIS SCRIPT!**
>
> Use `taca server_status uppmax` instead!

Returns a summary of quota usage in Uppmax

###### Dependencies

* couchdb
* pprint


### Samplesheet_converter.py
For the purpose of converting Illumina samplesheet that contains Chromium 10X indexes for demultiplexing. Headers and lines with ordinary indexes will be passed without any change. Lines with Chromium 10X indexes will be expanded into 4 lines, with 1 index in each line, and suffix 'Sx' will be added at the end of sample names.
#### Usage
`python main.py -i <inputfile> -o <outputfile> -x <indexlibrary>`


### set_bioinforesponsible.py
Calls up the genologics LIMS directly in order to more quickly set a bioinformatics responsible.

###### Dependencies

* Genologics: lims, config

### use_undetermined.sh
Creates softlinks of undetermined for specified flowcell and lane to be used in the analysis.
To be run on irma.
#### Usage
Usage: `use_undetermined.sh  <flowcell> <lane> <sample>`  
Example:  `use_undetermined.sh 160901_ST-E00214_0087_BH33GHALXX 1 P4601_273`
#### Important
After running the script, don't forget to (re-)**ORGANIZE FLOWCELL**.
And then analysis can be started.

### ZenDesk Attachments Backup
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
