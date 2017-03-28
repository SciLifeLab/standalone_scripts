import os
import glob
import re
import sys
import argparse
import random
import requests
import json
import datetime
import shutil
from datetime import  date



def main(args):
    """Fetch all project and start to loop over them
    """
    token = args.token
    url   = args.url
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    duplications_per_date = {}
    for project in  session.get(url+'/api/v1/projects', headers=headers).json()['projects']:
        if project['sequencing_facility'] != 'NGI-S':
            continue
        pid = project['projectid'] #project id
        for sample in session.get(url+'/api/v1/samples/{}'.format(pid), headers=headers).json()['samples']:
            if sample.get('analysis_status') != 'ANALYZED':
                continue
            if 'duplication_pc' not in sample:
                continue
            if sample['duplication_pc'] == 0:
                continue
            #now fetch sample runs
            sid = sample['sampleid']
            oldest_run_date =  date.today() # no run can be older than today and being analysed
            for sample_run in session.get(url+ '/api/v1/seqruns/{}/{}'.format(pid, sid), headers=headers).json()['seqruns']:
                rid = sample_run['seqrunid']
                sequencing_start_date = rid.split("_")[0] #first 6 digit are the date
                year  = int(sequencing_start_date[0:2])
                month = int(sequencing_start_date[2:4])
                day   = int(sequencing_start_date[4:6])
                if oldest_run_date > datetime.date(year, month, day):
                    oldest_run_date = datetime.date(year, month, day)
            #at this point I have the older run date
            if oldest_run_date not in duplications_per_date:
                duplications_per_date[oldest_run_date] = []
            duplications_per_date[oldest_run_date].append(sample['duplication_pc'])
        if len(duplications_per_date) >0:
            import pdb
            pdb.set_trace()





if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This scripts connects to charon and fetches information about duplciation rate for all human sample we are able to find. For each sample approaximates the sequencing data to the most recent sequencing run.
        """)
    # general options
    parser.add_argument('--facility', help="facility sequencing the project (stockholm, uppsala)", type=str, default="stockholm",
        choices=("stockholm", "uppsala"))
    parser.add_argument("-t", "--token", dest="token", default=os.environ.get('CHARON_API_TOKEN'),
            help="Charon API Token. Will be read from the env variable CHARON_API_TOKEN if not provided")
    parser.add_argument("-u", "--url", dest="url", default=os.environ.get('CHARON_BASE_URL'),
            help="Charon base url. Will be read from the env variable CHARON_BASE_URL if not provided")
    args = parser.parse_args()
    if not args.token :
        print( "No valid token found in arg or in environment. Exiting.")
        sys.exit(-1)
    if not args.url:
        print( "No valid url found in arg or in environment. Exiting.")
        sys.exit(-1)
    main(args)



