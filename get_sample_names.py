#!/usr/bin/env python

import sys
from statusdb.db import connections as statusdb

if len(sys.argv) == 1:
    sys.exit('Please provide a project name')
prj = sys.argv[1]

pcon = statusdb.ProjectSummaryConnection()
prj_obj = pcon.get_entry(prj)
prj_samples = prj_obj.get('samples',{})

print "NGI_id\tUser_id"
for sample in sorted(prj_samples.keys()):
    user_name = prj_samples[sample].get('customer_name','')
    print "{}\t{}".format(sample, user_name)