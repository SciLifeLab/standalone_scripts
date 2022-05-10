#!/usr/bin/env python

import sys
import os
from taca.utils.statusdb import ProjectSummaryConnection
from taca.utils.config import load_config

if len(sys.argv) == 1:
    sys.exit('Please provide a project name')
prj = sys.argv[1]

statusdb_config = os.getenv('STATUS_DB_CONFIG')
conf = load_config(statusdb_config)
conf = conf.get('statusdb')

pcon = ProjectSummaryConnection(config=conf)
prj_obj = pcon.get_entry(prj)
prj_samples = prj_obj.get('samples',{})

print("NGI_id\tUser_id")
for sample in sorted(prj_samples.keys()):
    user_name = prj_samples[sample].get('customer_name','')
    print("{}\t{}".format(sample, user_name))
