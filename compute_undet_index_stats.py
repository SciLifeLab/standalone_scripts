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
                    index_obj = {'name': index_name, 'index_type': index_type, 'kit_type': kit_type}
                    self._add_index(index_seq, index_obj)

    #computes reverse complement
    def _reverse_complement(self, index):
        for base in index:
            if base not in 'ATCGNatcgn':
                print "Error: NOT a DNA sequence"
                return None
        complement_dict = {"A":"T", "C":"G", "G":"C", "T":"A", "N":"N", "a":"t", "c":"g", "g":"c", "t":"a", "n":"n" }
        return "".join([complement_dict[base] for base in reversed(index)])

    #check if index exists in the  indexes list
    def is_index(self, index):
        if index in self.indexes or self._reverse_complement(index) in self.indexes:
            return True
        else:
            return False

    def _add_index(self, index_seq, index_obj):
        index_to_modify = ""
        if index_seq in self.indexes:
            index_to_modify = index_seq
        elif self._reverse_complement(index_seq) in self.indexes:
            index_to_modify = self._reverse_complement(index_seq)
        else:
            index_to_modify = index_seq
            self.indexes[index_to_modify] = []
        #add the information
        self.indexes[index_to_modify].append(index_obj)
        



    #returns all kits
    def return_kits(self):
        kits = []
        for kit_type in self.indexes_by_kit:
            kits.append(kit_type)
        return kits



    #still to be defined
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




def check_index(configuration_file, INDEXES, index):
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
    time_line = {}
    projects  = {}
    projects["M_Nister_16_01"] = {}
    projects["U_Gyllensten_16_01"] = {}
    projects["K_Kindberg_15_01"] = {}
    projects["A_Wedell_16_01"] = {}
    projects["M_Lundberg_16_01"] = {}
    projects["L_Raberg_16_01"] = {}
    projects["L_Aaltonen_16_01"] = {}
    
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
            date = FCid.split("_")[0]
            date_nice = "{}/{}/{}".format(date[0:2],date[2:4],date[4:6])
            if date_nice not in time_line:
                time_line[date_nice] = 0
            undetermined = flowcell_db[fc_doc]["Undetermined"]
            for lane in undetermined:
                projects_in_lane =  [sample["Project"] for sample in flowcell_db[fc_doc]["samplesheet_csv"] if sample["Lane"]== lane]
                for project in projects_in_lane:
                    if project in projects:
                        projects[project][FCid] = 0 #initialiase
            for lane in undetermined:
                if 'TOTAL' in undetermined[lane]:
                    del undetermined[lane]['TOTAL']
                for undet_sequence in undetermined[lane]:
                    index_cont = index
                    if len(undet_sequence) < len(index):
                        index_cont = index[0:len(index)]
                    index_to_check = undet_sequence
                    if len(undet_sequence) > len(index):
                        index_to_check = index[0:len(undet_sequence)]
                    if index_to_check == index_cont:
                        time_line[date_nice] += 1
                        projects_in_lane =  [sample["Project"] for sample in flowcell_db[fc_doc]["samplesheet_csv"] if sample["Lane"]== lane]
                        for project in projects_in_lane:
                            if project in projects:
                                projects[project][FCid] = 1
                            
    for date in sorted(time_line):
        print "{} {}".format(date, time_line[date])



def find_undetermined_index_over_time(index_to_be_searched, instrument_type):
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
        #if a instrument type is specifed process only FCs run on that instrument
        if instrument_type is not None:
            if instrument_type != FC_type:
                continue
        undetermined = flowcell_db[fc_doc]["Undetermined"]
        lanes_undet = [FCid, []]
        for lane in ['1','2','3','4','5','6','7','8']:
            if lane not in undetermined:
                continue
            index_to_be_searched_count = 0
            for undetermined_index in undetermined[lane]:
                if index_to_be_searched in undetermined_index:
                    index_to_be_searched_count = undetermined[lane][undetermined_index]
            lanes_undet[1].append([lane, index_to_be_searched_count])
        if len(lanes_undet[1]) > 0:
            time_line.append(lanes_undet)

    for FC in time_line:
        FCid = FC[0]
        for lane in FC[1]:
            print "{}_{} {}".format(FCid, lane[0], lane[1])






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



##indexes_file = args.indexes
##INDEXES = Indexes(indexes_file)


def main(args):
    configuration_file = args.config
    load_yaml_config(configuration_file)
    
    if args.mode == 'check_undet_index':
        if args.index is None:
            sys.exit("in this mode --index must be specified")
        find_undetermined_index_over_time(args.index, args.instrument_type)
    #fetch_undermined_stats(configuration_file, INDEXES)
    #check_index(configuration_file, INDEXES, "CTTGTAAT")





if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This scripts queries statusdb x_flowcell_db  and fetch informaiton about runs.
    The following operations are supported:
        - check_undet_index: given a specific index checks all FCs and prints all FC and lanes where the indx appears as undetermined
        """)
    parser.add_argument('--config', help="configuration file", type=str,  required=True)
    parser.add_argument('--indexes', help="yamls file containing indexes we want to analyse", type=str)
    
    parser.add_argument('--mode', help="define what action needs to be executed", type=str, required=True, choices=('check_undet_index', 'undet'))
    
    
    parser.add_argument('--index', help="a specifc index (e.g., CTTGTAAT) to be searched across lanes and FCs", type=str)
    parser.add_argument('--instrument-type', help="type of instrument", type=str, default=None, choices=('HiSeqX', 'MiSeq', 'HiSeq2500'))
    args = parser.parse_args()
    main(args)



