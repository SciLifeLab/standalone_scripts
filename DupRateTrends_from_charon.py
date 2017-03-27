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


logger = logging.getLogger(__name__)

def main(args):
    """Fetch all project and start to loop over them
    """
    token = args.token,
    url   = args.url
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    import pdb
    pdb.set_trace()
    projects =  session.get(url+'/api/v1/projects', headers=headers).json()['projects']]





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
    if not args.token :
        print( "No valid token found in arg or in environment. Exiting.")
        sys.exit(-1)
    if not args.url:
        print( "No valid url found in arg or in environment. Exiting.")
        sys.exit(-1)
    args = parser.parse_args()
    main(args)



