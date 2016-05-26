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



def list_applications():
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


class Indexes:
    
    #indexes_by_kit looks like:
    #Kit_name:
    #   i7_index1:
    #       index_name: index_seq
    #       ...
    #   i5_index2:
    indexes_by_kit = {}
    #indexes looks like:
    #index_seq: ((index_name, index_type, kit_name), ....)
    indexes = {}

    def __init__(self, indexes_file):
        try:
            with open(indexes_file, 'r') as f:
                self.indexes_by_kit = yaml.load(f)
        except IOError as e:
                e.message = "Could not open configuration file \"{}\".".format(indexes_file)
                raise e
        #now create a more index centric object
        for kit_type in self.indexes_by_kit: #for each kit type
            if kit_type not in self.indexes_by_kit:
                print "file {} badly fomatted".format(indexes_file)
                return
            for index_type in self.indexes_by_kit[kit_type]: # for each type of indexes
                for index_name, index_seq in self.indexes_by_kit[kit_type][index_type].iteritems():
                    if not self.is_index(index_seq):
                        self.indexes[index_seq] = []
                    self.indexes[index_seq].append({'name': index_name,
                                                'index_type': index_type,
                                                'kit_type': kit_type,
                                               })

    def reverse_complement(self, index):
        for base in index:
            if base not in 'ATCGatcg':
                print "Error: NOT a DNA sequence"
                return None
        seq1 = 'ATCGTAGCatcgtagc'
        seq_dict = { seq1[i]:seq1[i+4] for i in range(16) if i < 4 or 8<=i<12 }
        return "".join([seq_dict[base] for base in reversed(index)])
    
    def is_index(self, index):
        if index not in self.indexes or self.reverse_complement(index) not in self.indexes:
            return False
        else:
            return True
    

    def return_kits(self):
        kits = []
        for kit_type in self.indexes_by_kit:
            kits.append(kit_type)
        return kits

    def check_left_shift_conflicts(self):
        #checks if indexes from the same library after a left shift are conflicting
        for kit_type in self.indexes_by_kit: #for each lib kit type
            for index_type in self.indexes_by_kit[kit_type]: # for each type of indexes
                for index_name, index_seq in self.indexes_by_kit[kit_type][index_type].iteritems():
                    fake_index = index_seq[1:] + "A"
                    for index_name_check, index_seq_check in self.indexes_by_kit[kit_type][index_type].iteritems():
                        hamming_dist = distance.hamming(index_seq_check, fake_index)
                        if hamming_dist <= 2:
                            print "{} {} {} {} {}".format(index_seq, index_seq_check, fake_index, hamming_dist, kit_type)



def fetch_undermined_stats(configuration_file, INDEXES):
    load_yaml_config(configuration_file)
    couch=setupServer(CONFIG)
    flowcell_db = couch["x_flowcells"]
    
    MostOccurringUndetIndexes = {}
    FC_num = 0
    lanes_num = 0
    MostOccurringUndetIndexes["Total"] = {}

    FC_XTen_num = 0
    lanes_Xten_num = 0
    MostOccurringUndetIndexes["HiSeqX"] = {}

    FC_MiSeq_num = 0
    lanes_MiSeq_num = 0
    MostOccurringUndetIndexes["MiSeq"] = {}

    FC_HiSeq_num = 0
    lanes_HiSeq_num = 0
    MostOccurringUndetIndexes["HiSeq2500"] = {}

    #if "ST-" in flowcell_db[fc_doc]["RunInfo"]["Id"]: XTEN specific check
    for fc_doc in sorted(flowcell_db):
        # first check that I have all necessary info to extract information
        try:
            undetermined = flowcell_db[fc_doc]["Undetermined"]
        except KeyError:
            continue
        FCid = flowcell_db[fc_doc]["RunInfo"]["Id"]
        FC_type = ""
        if "ST-" in FCid:
            FC_XTen_num += 1
            FC_type = "HiSeqX"
        elif "000000000-" in FCid:
            FC_MiSeq_num += 1
            FC_type = "MiSeq"
        else:
            FC_HiSeq_num += 1
            FC_type = "HiSeq2500"
        FC_num += 1
        #we can use the illumina Demultiplex_Stats Barcode_lane_statistics to fetch info about indexes
        #to do: most commonly occurring undet index
       
        for lane in undetermined:
        #for each lane
            if len(undetermined[lane]) > 1: # if there are elements (there is the NoIndex case)
                if 'TOTAL' in undetermined[lane]:
                    del undetermined[lane]['TOTAL']
                most_occuring_undet = sorted(undetermined[lane].items(), key=operator.itemgetter(1), reverse=True)[0]
                lanes_num += 1
                if FC_type is "HiSeqX":
                    lanes_Xten_num += 1
                elif FC_type is "HiSeq2500":
                    lanes_HiSeq_num += 1
                elif FC_type is "MiSeq":
                    lanes_MiSeq_num += 1
                
                if most_occuring_undet[0] not in MostOccurringUndetIndexes[FC_type]:
                    MostOccurringUndetIndexes[FC_type][most_occuring_undet[0]] = 0
                MostOccurringUndetIndexes[FC_type][most_occuring_undet[0]] += 1
                if most_occuring_undet[0] not in MostOccurringUndetIndexes["Total"]:
                    MostOccurringUndetIndexes["Total"][most_occuring_undet[0]] = 0
                MostOccurringUndetIndexes["Total"][most_occuring_undet[0]] += 1
    
    # how often I see the left index shift

    #
    print "Flowcells: {}".format(FC_num)
    print "HiSeqX: {}".format(FC_XTen_num)
    print "HiSeq2500: {}".format(FC_HiSeq_num)
    print "MiSeq: {}".format(FC_MiSeq_num)

    print "Most occuring undetermined (seen in #lanes)"
    print "All Flowcells:"
    for twenty_most_occuring_undet in sorted(MostOccurringUndetIndexes["Total"].items(), key=operator.itemgetter(1), reverse=True)[0:10]:
        print "{}\t{}\t{}".format(twenty_most_occuring_undet[0], twenty_most_occuring_undet[1], twenty_most_occuring_undet[1]/float(lanes_num))
    print "All HiSeqX:"
    for twenty_most_occuring_undet in sorted(MostOccurringUndetIndexes["HiSeqX"].items(), key=operator.itemgetter(1), reverse=True)[0:10]:
        print "{}\t{}\t{}".format(twenty_most_occuring_undet[0], twenty_most_occuring_undet[1], twenty_most_occuring_undet[1]/float(lanes_Xten_num))
    print "All HiSeq2500:"
    for twenty_most_occuring_undet in sorted(MostOccurringUndetIndexes["HiSeq2500"].items(), key=operator.itemgetter(1), reverse=True)[0:10]:
        print "{}\t{}\t{}".format(twenty_most_occuring_undet[0], twenty_most_occuring_undet[1], twenty_most_occuring_undet[1]/float(lanes_HiSeq_num))
    print "All MiSeq:"
    for twenty_most_occuring_undet in sorted(MostOccurringUndetIndexes["MiSeq"].items(), key=operator.itemgetter(1), reverse=True)[0:10]:
        print "{}\t{}\t{}".format(twenty_most_occuring_undet[0], twenty_most_occuring_undet[1], twenty_most_occuring_undet[1]/float(lanes_MiSeq_num))








def main(args):
    configuration_file = args.config
    indexes_file = args.indexes
    INDEXES = Indexes(indexes_file)
    #INDEXES.check_left_shift_conflicts()
    
    load_yaml_config(configuration_file)
    couch=setupServer(CONFIG)
    flowcell_db = couch["x_flowcells"]
    import pdb
    pdb.set_trace()
    fetch_undermined_stats(configuration_file, INDEXES)




if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This scripts queries statusdb x_flowcell_db  and fetch informaiton about runs""")
    parser.add_argument('--config', help="configuration file", type=str,  required=True)
    parser.add_argument('--indexes', help="yamls file containing indexes we want to analyse", type=str, required=True)
    args = parser.parse_args()
    main(args)



