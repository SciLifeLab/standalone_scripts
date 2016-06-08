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

CONFIG = {}

logger = logging.getLogger(__name__)



def check_delivered_projects():
    couch        = setupServer(CONFIG)
    projects_db  = couch['projects']
    applications = {}
    for project_id in projects_db:
        if "creation_time" not in projects_db[project_id]:
            continue
        application  = "unkonwn"
        if "application" in projects_db[project_id]:
            application = projects_db[project_id]["application"]
        if application in applications:
            applications[application] += 1
        else:
            applications[application] = 1
    for application in applications:
        print "{} {}".format(application, applications[application])



def list_LibraryMethods(method_of_interest=None):
    couch        = setupServer(CONFIG)
    projects_db  = couch['projects']
    libraryMethods = {}
    for project_id in projects_db:
        if "creation_time" not in projects_db[project_id]:
            continue
        library  = "unkonwn"
        if "details" in projects_db[project_id]:
            if "library_construction_method" in projects_db[project_id]["details"]:
                method = projects_db[project_id]["details"]["library_construction_method"]
        if method in libraryMethods:
            libraryMethods[method] += 1
        else:
            libraryMethods[method] = 1
    methods_of_interest = []
    for method in libraryMethods:
        print "{} {}".format(method, libraryMethods[method])
        if method_of_interest is not None:
            if method_of_interest in method:
                methods_of_interest.append(method)
    return methods_of_interest

def list_Reference():
    couch        = setupServer(CONFIG)
    projects_db  = couch['projects']
    references_genome = {}
    for project_id in projects_db:
        if "creation_time" not in projects_db[project_id]:
            continue
        reference_genome = "NaR"
        if "reference_genome" in projects_db[project_id]:
            reference_genome = projects_db[project_id]["reference_genome"]
        if reference_genome in references_genome:
            references_genome[reference_genome] += 1
        else:
            references_genome[reference_genome] = 1
    for reference_genome in references_genome:
        print "{} {}".format(reference_genome, references_genome[reference_genome])

def parse_FCdbs():
    couch        = setupServer(CONFIG)
    flowcell_db  = couch['x_flowcells']
    flowcell_projects = {}
    for fc_doc in flowcell_db:
        try:
            sample_stats = flowcell_db[fc_doc]["illumina"]["Demultiplex_Stats"]["Barcode_lane_statistics"]
        except KeyError:
            continue
        for sample in sample_stats:
            project_id = sample["Sample"].replace("Sample_" , "").split("_")[0]
            sample_run_yield = int(sample["Yield (Mbases)"].replace(",", ""))
            if project_id not in flowcell_projects:
                flowcell_projects[project_id] = 0
            flowcell_projects[project_id] += sample_run_yield
    flowcell_db  = couch['flowcells']
    flowcell_projects_TMP = {}
    for fc_doc in flowcell_db:
        try:
            sample_stats = flowcell_db[fc_doc]["illumina"]["Demultiplex_Stats"]["Barcode_lane_statistics"]
        except KeyError:
            continue
        for sample in sample_stats:
            project_id = sample["Sample ID"].replace("Sample_" , "").split("_")[0]
            sample_run_yield = int(sample["Yield (Mbases)"].replace(",", ""))
            if project_id not in flowcell_projects_TMP:
                flowcell_projects_TMP[project_id] = 0
            flowcell_projects_TMP[project_id] += sample_run_yield
    #now merge the DB
    for project in flowcell_projects_TMP:
        if project not in flowcell_projects and project.startswith("P"):
            flowcell_projects[project] = flowcell_projects_TMP[project]
    return flowcell_projects




def compute_delivery_footprint():
    projects = parse_FCdbs()
    couch        = setupServer(CONFIG)
    projects_db  = couch['projects']
    
    #for project_id in projects_db:
    #    if "creation_time" not in projects_db[project_id]:
    #        continue
    #    if projects_db[project_id]["project_id"] not in projects:
    #        continue
    #    if projects_db[project_id]["creation_time"].startswith("2015"):
    #       if "uppnex_id" not in projects_db[project_id]:
    #            uppnex_id = "null"
    #        else:
    #            uppnex_id = projects_db[project_id]["uppnex_id"].lower()
    #        if "eliver" in uppnex_id:
    #            uppnex_id = "null"
    #        print "{} {} {} ".format(uppnex_id.lstrip().rstrip().replace("/INBOX", ""),
    #                projects[projects_db[project_id]["project_id"]],
    #                projects_db[project_id]["project_id"]
    #                )
    #return
    bp = {}
    for project_id in projects_db:
        if "creation_time" not in projects_db[project_id]: # project is too old
            continue
        if projects_db[project_id]["project_id"] not in projects: # project has not been sequenced or the sequencing info is not stored
            continue
        delivered = "True"
        if "close_date" not in projects_db[project_id]:
            delivered = "False"
        if "uppnex_id" not in projects_db[project_id]:
            uppnex_id = "null"
        else:
            uppnex_id = projects_db[project_id]["uppnex_id"].lower()
        if "eliver" in uppnex_id:
            uppnex_id = "null"
        #take away all the shit that can be attached to uppnex_id
        uppnex_id = uppnex_id.split(",")[0].lstrip().rstrip().replace("/inbox", "")
        if uppnex_id == "" or "snic" in uppnex_id or "hard" in uppnex_id:
            uppnex_id = "null"
        if uppnex_id[0].isdigit():
            uppnex_id = "b{}".format(uppnex_id)
        sensitive = "False"
        if "reference_genome" in projects_db[project_id] and projects_db[project_id]["reference_genome"] == "hg19":
            sensitive = "True"
        best_practice_bioinformatics = "False"
        if "details" in projects_db[project_id] and "best_practice_bioinformatics"  in projects_db[project_id]["details"]:
            if projects_db[project_id]["details"]["best_practice_bioinformatics"] == "Yes":
                best_practice_bioinformatics = "True"
        if "close_date" not in projects_db[project_id]:
            date = projects_db[project_id]["creation_time"].split("T")[0]
        else:
            date = projects_db[project_id]["close_date"]
        print "{} {} {} {} {} {} {}".format(uppnex_id,
                    date,
                    projects[projects_db[project_id]["project_id"]],
                    projects_db[project_id]["project_id"],
                    sensitive,
                    best_practice_bioinformatics,
                    delivered)


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



def undermined_stats(configuration_file):
    load_yaml_config(configuration_file)
    couch=setupServer(CONFIG)
    flowcell_db = couch["x_flowcells"]
    flowcell_docs = {}
    
    for fc_doc in flowcell_db:
        try:
            undetermined = flowcell_db[fc_doc]["Undetermined"]
        except KeyError:
            continue
        flowcell_docs[flowcell_db[fc_doc]["RunInfo"]["Id"]] = fc_doc
    time_line = []
    
    for FCid in sorted(flowcell_docs):
        # first check that I have all necessary info to extract information
        fc_doc = flowcell_docs[FCid]
        FC_type = ""
        if "ST-" in FCid:
            FC_type = "HiSeqX"
        elif "000000000-" in FCid:
            FC_type = "MiSeq"
        else:
            FC_type = "HiSeq2500"
        if FC_type is "HiSeqX":
            undetermined = flowcell_db[fc_doc]["Undetermined"]
            lanes_undet = [FCid, []]
            for lane in ['1','2','3','4','5','6','7','8']:
                if lane not in undetermined:
                    continue
                total = 0
                if 'CTTGTAAT' in undetermined[lane]:
                    total = undetermined[lane]['CTTGTAAT']
                lanes_undet[1].append(total)
            time_line.append(lanes_undet)
    for FC in time_line:
        FCid = FC[0]
        lane_num = 1
        for lane in FC[1]:
            print "{}_{} {}".format(FCid, lane_num, lane)
            lane_num += 1




def main(args):
    configuration_file = args.config
    load_yaml_config(configuration_file)
    couch=setupServer(CONFIG)
    #import pdb
    #pdb.set_trace()
    #list_Reference()
    compute_delivery_footprint()





if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This scripts queries statusdb x_flowcell_db  and fetch informaiton about runs""")
    parser.add_argument('--config', help="configuration file", type=str,  required=True)
    args = parser.parse_args()
    main(args)



