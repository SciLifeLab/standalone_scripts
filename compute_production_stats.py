import os
import glob
import re
import sys
import socket
import couchdb
import logging
import argparse
import ConfigParser
import yaml
import json
import distance
import operator
import time
from datetime import  date

CONFIG = {}

logger = logging.getLogger(__name__)






def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)



def load_yaml_config(config_file):
    """Load YAML config file

    :param str config_file: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    if type(config_file) is file:
        CONFIG.update(yaml.load(config_file) or {})
        return CONFIG
    else:
        try:
            with open(config_file, 'r') as f:
                content = yaml.load(f)
                CONFIG.update(content)
                return content
        except IOError as e:
            e.message = "Could not open configuration file \"{}\".".format(config_file)
            raise e

def get_FC_type(FCid):
    FC_type = ""
    if "ST-" in FCid:
        FC_type = "HiSeqX"
    elif "000000000-" in FCid:
        FC_type = "MiSeq"
    else:
        FC_type = "HiSeq2500"
    return FC_type




def parse_flowcell_db():
    couch       = setupServer(CONFIG)
    #fetch info about proejcts (reference type)
    projectsDB = couch["projects"]
    project_summary = projectsDB.view("project/summary")
    projects = {}
    for row in project_summary:
        if "project_name" not in row.value:
            print "somehting is wrong here... I guess I am going to fail"
        if "reference_genome" in row.value:
            projects[row.value["project_name"]] = row.value["reference_genome"]
        else:
            projects[row.value["project_name"]] = "None"

    flowcell_db = couch["x_flowcells"]
    flowcells   = {}
    
    instrument_types = ["HiSeqX", "MiSeq", "HiSeq2500"]
    for instrument_type in instrument_types:
        flowcells[instrument_type] = {}

    for fc_doc in flowcell_db:
        try:
            samplesheet_csv = flowcell_db[fc_doc]["samplesheet_csv"]
        except KeyError:
            if "RunInfo" in flowcell_db[fc_doc]:
                print "{}".format(flowcell_db[fc_doc]["RunInfo"]["Id"])
            continue
        flowcell_id     = flowcell_db[fc_doc]["RunInfo"]["Id"]
        instrument_type = get_FC_type(flowcell_id)
        if flowcell_id not in flowcells[instrument_type]:
            flowcells[instrument_type][flowcell_id] = {}

        for sample_lane in samplesheet_csv:
            if instrument_type == "MiSeq":
                lane = "1"
            else:
                lane = sample_lane["Lane"]
            if lane not in flowcells[instrument_type][flowcell_id]:
                flowcells[instrument_type][flowcell_id][lane] = {}
            if "Sample_Project"  in sample_lane:
                project = sample_lane["Sample_Project"].strip()
            elif "Project" in sample_lane:
                project = sample_lane["Project"].strip()
            else:
                print "WRONG"
            # now correct the project
            if "." not in project:
                project = project.replace("__", "_").replace("_", ".", 1)
            if "." in project:
                project_pieaces = project.split(".")
                project = "{}.{}".format(project_pieaces[0].upper(), project_pieaces[1])

            if project not in projects:
                print "{} not found in projects".format(project)
                continue
            if project in flowcells[instrument_type][flowcell_id][lane]:
                # cehck that it is the same
                if projects[project] != flowcells[instrument_type][flowcell_id][lane][project]:
                    print "stange found same  project but different organisms"
            else:
                flowcells[instrument_type][flowcell_id][lane][project] = projects[project]

    #now in flowcell I have a full list of instrument/FCid/lanes/Project and organism
    #now i can pull all the stats I want
    date_low_limit = date(16,1,1)
    date_upper_limit = date(17,1,1)
    total_number_FC    = 0
    total_number_lanes = 0
    total_number_human_lanes     = 0
    total_number_non_human_lanes = 0
    total_number_mixed_lanes     = 0
    for instrument_type in flowcells:
        instrument_number_FC    = 0
        instrument_number_lanes = 0
        instrument_number_human_lanes     = 0
        instrument_number_non_human_lanes = 0
        instrument_number_mixed_lanes     = 0
        for flowcell_id in flowcells[instrument_type]:
            start_date_string = flowcell_id
            year = start_date_string[0:2]
            month = start_date_string[2:4]
            day = start_date_string[4:6]
            fc_date = date(int(year), int(month), int(day))
            if fc_date >= date_low_limit and fc_date < date_upper_limit:
                total_number_FC += 1
                instrument_number_FC += 1
                for lane in flowcells[instrument_type][flowcell_id]:
                    total_number_lanes += 1
                    instrument_number_lanes += 1
                    lane_type = ""
                    for project in flowcells[instrument_type][flowcell_id][lane]:
                        if lane_type == "":
                            lane_type = flowcells[instrument_type][flowcell_id][lane][project]
                        elif lane_type !=  flowcells[instrument_type][flowcell_id][lane][project]:
                            lane_type = "mixed"
                        else:
                            lane_type = flowcells[instrument_type][flowcell_id][lane][project]
                    if lane_type == "mixed":
                        total_number_mixed_lanes += 1
                        instrument_number_mixed_lanes += 1
                    elif lane_type == "hg19":
                        total_number_human_lanes += 1
                        instrument_number_human_lanes += 1
                    else:
                        total_number_non_human_lanes += 1
                        instrument_number_non_human_lanes += 1
            else:
                continue
        print "{}".format(instrument_type)
        print "\tNumber of FC: {}".format(instrument_number_FC)
        print "\tNumber of lanes: {}".format(instrument_number_lanes)
        print "\tNumber of Human lanes: {}".format(instrument_number_human_lanes)
        print "\tNumber of Non-Human lanes: {}".format(instrument_number_non_human_lanes)
        print "\tNumber of Mixed lanes: {}".format(instrument_number_mixed_lanes)
    print "TOTAL"
    print "\tNumber of FC: {}".format(total_number_FC)
    print "\tNumber of lanes: {}".format(total_number_lanes)
    print "\tNumber of Human lanes: {}".format(total_number_human_lanes)
    print "\tNumber of Non-Human lanes: {}".format(total_number_non_human_lanes)
    print "\tNumber of Mixed lanes: {}".format(total_number_mixed_lanes)


            
    
def instrument_usage():
    couch       = setupServer(CONFIG)
    #fetch info about proejcts (reference type)
    projectsDB = couch["projects"]
    project_summary = projectsDB.view("project/summary")
    projects = {}
    instruments = {}
    for row in project_summary:
        if "close_date" not in row.value:
            continue
        if 'aborted' in row.value['details'] and row.value['details']['aborted']:
            continue
        year_close_date = int(row.value["close_date"].split("-")[0])
        if year_close_date >= 2015:
            if 'sequencing_platform' not in  row.value['details']:
                continue
            else:
                instrument = row.value['details']['sequencing_platform']
            if 'sequencing_setup' not in row.value['details']:
                continue
            else:
                sequencing_setup = row.value['details']['sequencing_setup']
            if sequencing_setup == "special" or  sequencing_setup == "Special":
                continue
            pattern = re.compile("^[0-9]+x[0-9]+")
            if not pattern.match(sequencing_setup):
                continue
            if instrument not in instruments:
                instruments[instrument] = {'number':1, 'setup': {sequencing_setup : 1}}
            else:
                instruments[instrument]['number'] += 1
                if sequencing_setup not in instruments[instrument]['setup']:
                    instruments[instrument]['setup'][sequencing_setup] = 1
                else:
                    instruments[instrument]['setup'][sequencing_setup] += 1
            project_name = row.value["project_name"]
            projects[project_name] = {'sequencing_platform': instrument,
                                        'sequencing_setup' :  sequencing_setup,
                                        'samples_sequenced': set(),
                                        'lanes': 0,
                                        'sequencers' : set()
                                        }
    flowcell_db = couch["x_flowcells"]
    project_sequenced = {}
    for fc_doc in flowcell_db:
        if 'RunInfo' not in flowcell_db[fc_doc]:
            continue
        instrument = flowcell_db[fc_doc]["RunInfo"]['Instrument']
        if 'illumina' not in flowcell_db[fc_doc]:
            print "Not illumina field found in doc"
            continue
        if 'Demultiplex_Stats' not in  flowcell_db[fc_doc]['illumina']:
            print "Not Demultiplex_Stats field found in doc"
            continue
        if 'Barcode_lane_statistics' not in flowcell_db[fc_doc]['illumina']['Demultiplex_Stats']:
            print "Not Barcode_lane_statistics field found in doc"
            continue
        projects_in_lanes = {}
        for sample_lane in flowcell_db[fc_doc]['illumina']['Demultiplex_Stats']['Barcode_lane_statistics']:
            if sample_lane['Sample'] == 'unknown':
                continue
            if "Sample_Project"  in sample_lane:
                project = sample_lane["Sample_Project"].strip()
            elif "Project" in sample_lane:
                project = sample_lane["Project"].strip()
            else:
                print "WRONG"
            # now correct the project
            if "." not in project:
                project = project.replace("__", "_").replace("_", ".", 1)
            if "." in project:
                project_pieaces = project.split(".")
                project = "{}.{}".format(project_pieaces[0].upper(), project_pieaces[1])

            if project not in projects:
                print "{} not found in projects".format(project)
                continue
            projects[project]['samples_sequenced'].update(sample_lane['Sample'])
            lane = sample_lane['Lane']
            if lane not in projects_in_lanes:
                projects_in_lanes[lane] = {}
            if project not in projects_in_lanes[lane] :
                projects_in_lanes[lane][project] = 1
            else:
                projects_in_lanes[lane][project] += 1
        for lane in projects_in_lanes:
            for project in projects_in_lanes[lane]:
                projects[project]['lanes'] += 1
        projects[project]['sequencers'].update(instrument)

    import pdb
    pdb.set_trace()


    
def main(args):
    configuration_file = args.config
    load_yaml_config(configuration_file)
    configuration_file = args.config
    load_yaml_config(configuration_file)

    if args.mode == 'production-stats':
        projects = parse_flowcell_db()
    
    if args.mode == 'instrument-usage':
        instrument_usage()
    
    






if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This scripts queries statusdb x_flowcelldb and project database and fetches informations about what organisms have been sequenced. It can be run in the following modes:
         - production-stats: for each instrument type it prints number of FCs, number of lanes, etc. It then prints a summary of all stats
         - instrument-usage: for each instrument type and year it prints different run set-ups and samples run with that set-up
        """)
    parser.add_argument('--config', help="configuration file", type=str,  required=True)
    parser.add_argument('--mode', help="define what action needs to be executed", type=str, required=True, choices=('production-stats', 'instrument-usage'))

    args = parser.parse_args()
    main(args)



