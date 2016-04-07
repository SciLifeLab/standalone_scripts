#!/usr/bin/env python2.7

import couchdb
import re
import math
from collections import defaultdict, Counter, OrderedDict
import unicodedata
import csv
import copy
import click
import sys

from time import time
from datetime import datetime

from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.lims import Lims
from genologics.entities import Process

#Assumes ind. sample conc measurements have failed. As such it relies on changing relative volume on already normalized samples and structure
#Structure are retained as conc measurements failure means there's no way to know conc. delta between samples from seperate poolss
def connection():
    user = ''
    pw = ''
    couch = couchdb.Server('http://' + user + ':' + pw + '@tools.scilifelab.se:5984')
    try:
        couch.version()
    except:
        sys.exit("Can't connect to couch server. Most likely username or password are incorrect.")
    return couch

#Fetches the structure of a project
def proj_struct(couch, project, target_clusters):
    db = couch['x_flowcells']
    view = db.view('names/project_ids_list')
    fc_track = defaultdict(set)
    
    #Adds flowcells to ALL projects. Due to intractions its easier to just get FCs for ALL projects
    for rec in view.rows:
        fc = ''.join(rec.key)
        fc = unicodedata.normalize('NFKD', fc).encode('ascii','ignore')
        id = ''.join(rec.id)
        id = unicodedata.normalize('NFKD', id).encode('ascii','ignore')
        for projs in rec.value:
            projs = ''.join(projs)
            projs = unicodedata.normalize('NFKD', projs).encode('ascii','ignore')
            if fc_track[projs] == set([]):
                fc_track[projs] = dict()
            fc_track[projs][fc] = id
            
    #Adds lanes and samples to flowcells, includes samples from other projects if they share lane
    if fc_track[project] == set([]):
        raise Exception('Error: Project not logged in x_flowcells database!')
    for fc, id in fc_track[project].items():
        try: 
            entry = db[id]['illumina']
        except KeyError:
            print "Error: Illumina table for db entry" , id, "doesn't exist!"
        entry = db[id]['illumina']['Demultiplex_Stats']['Barcode_lane_statistics']
        for index in xrange(0, len(entry)):
            lane = entry[index]['Lane']
            sample = entry[index]['Sample']
            if 'Clusters' in entry[index]:
                clusters = entry[index]['Clusters']
            else: 
                clusters = entry[index]['PF Clusters']
            clusters = int(re.sub(r",", "", clusters))
            
            
            if not isinstance(fc_track[project][fc], dict):
                fc_track[project][fc] = dict()
            if not lane in fc_track[project][fc]:
                fc_track[project][fc][lane] = dict()
            #Only counts samples for the given project, other samples are "auto-filled"
            if project in sample:
                fc_track[project][fc][lane][sample] = clusters
            else:
                fc_track[project][fc][lane][sample] = target_clusters
    #Removes any lanes that don't have any part project samples
    for fc, lanes in fc_track[project].items():
        for lane,sample in lanes.items():
            if not any(project in s for s in sample.keys()):
                   del fc_track[project][fc][lane]
    return fc_track[project]

def parse_indata(struct, target_clusters):
    clusters_rem = dict()
    clusters_expr = dict()
    lane_maps = dict()
    counter = 1
    
    #Takes in data and finds unique lane structure, clusters per sample and lane division
    #Output could probably be sent as a nested hash. 
    #Clusters_rem AND clusters_expr may seem redundant, but it saves some calculatin
    for fc, lanes in struct.items():
        for lane, samples in lanes.items():
            
            #Concatinate structure into a set of unique structures
            mapping = sorted(samples.keys(), reverse=True)
            if not mapping in lane_maps.values():
                lane_maps[counter] = mapping 
                counter +=1
                
            #Calculate clusters read per sample
            for sample, value in samples.items():
                if not sample in clusters_rem:
                    clusters_rem[sample] = target_clusters
                    clusters_expr[sample] = 0
                clusters_rem[sample] -= value
                clusters_expr[sample] += value   
                
    return [lane_maps, clusters_rem, clusters_expr] 

#Creates a set where every sample uniquely appears once and only once
def simple_unique_set(lane_maps):
    unique_lane_maps = dict()
    for keyz, valz in lane_maps.items():
        #Fetch what lanes inclusion of given lane excludes
        excluded = list()
        for sample in valz:
            for k, v in lane_maps.items():
                for dupe in v:
                    if dupe == sample and keyz != k and sample != 'Undetermined':
                        excluded.append(k)
                        break
        #Check that none of the excluded lanes have uniquely present samples
        acceptable = True
        for key in excluded:
            total_duplicates = list()
            for values in lane_maps[key]:
                duplicate = 0
                for k, v in lane_maps.items():
                    for dupe in v:
                        if dupe == values and k != key and sample != 'Undetermined':
                            duplicate +=1
                            break
                total_duplicates.append(duplicate)
            if 0 in total_duplicates:
                acceptable = False
                break
        if acceptable:
            #Check that the lane doesn't have sample dupes in the accepted set already
            for entries in valz:
                for kuyz, vulz in unique_lane_maps.items():
                    for things in vulz:
                        if things == entries and entries != 'Undetermined':
                            acceptable = False
                            break
            if acceptable:        
                unique_lane_maps[keyz] = valz
            
    lane_maps = unique_lane_maps
    
    #ALL SAMPLES PRESENT CHECK
    # summap = []
    # for k in lane_maps.keys():
    #     summap += lane_maps[k]
    # print len(set(summap))   

    validate_template_struct(lane_maps)
    
def aggregator(lane_maps,clusters_rem,clusters_per_lane):
#Iterate
    #Find all samples that are also expressed in another struct
    #Sort those structs by duplication
    #Fill them to floor(dups); unless mod % 1 > some_number; then ceil(dups)
    #Note the remaining necessary
#End
    #Use the remaining structs
    #Ceil(dups) those babies
    raise Exception('Error: Not yet implemented!')
    

#Gives how many percent of the lane should be allocated to a specific sample    
def sample_distributor(lane_maps, clusters_rem, clusters_per_lane):
    ideal_ratios = dict()
    req_lanes = dict()
    
    for index in lane_maps:
        summ = 0
        for entry in lane_maps[index]:
            if clusters_rem[entry] > 0:
                summ += clusters_rem[entry]
        for entry in lane_maps[index]:
            if not index in ideal_ratios:
                ideal_ratios[index] = list()
            if clusters_rem[entry] > 0:
                ideal_ratios[index].append(clusters_rem[entry]/float(summ))
            else: 
                ideal_ratios[index].append(0.0)
        #Minimal number of required lanes per pool
        req_lanes[index] = summ/float(clusters_per_lane)
    #Have to be rounded up, rounding down when only using duplicates makes no sense
    total_lanes = map(math.ceil, req_lanes.values())
    
    return [ideal_ratios, req_lanes, total_lanes]

#Crude way to check that no samples are in different TYPES of lanes
def validate_template_struct(lane_maps): 
    tempList = list()
    
    for k, v in lane_maps.items():
        for index in xrange(1,len(v)):
            if not v[index] == 'Undetermined':
                tempList.append(v[index])
    counter = Counter(tempList)
    for values in counter.itervalues():
        if values > 1: 
            raise Exception('Error: This app does NOT handle situations where a sample' 
            'is present in lanes/well with differing structure!')

#Corrects volumes since conc is non-constant
#Also normalizes the numbers
#Finally translates float -> int without underexpressing anything
def correct_numbers(lane_maps, clusters_expr, ideal_ratios, req_lanes, total_lanes):
    # Since some samples are strong and some weaksauce
    # 10% in ideal_ratios does not mean 10% of lane volume
    # As such, ideal_ratios need to be divided by actual_reads/expected_reads
    # Ignores undetermined clusters in calculation
    # Assumes sample conc cant be altered; aka only volume is modified
    
    for ind in xrange(1, len(lane_maps.keys())+1):
        #Bases w/o sample are not expected
        if len(lane_maps[ind]) != 1:
            exp = 1/float(len(lane_maps[ind])-1)
        else:
            exp = 1
        laneTypeExpr = 0
        counter = 0
        for sample in lane_maps[ind]:
            if not sample == 'Undetermined':
                laneTypeExpr += clusters_expr[sample]
        for sample in lane_maps[ind]:
            act = clusters_expr[sample]/float(laneTypeExpr)
            ideal_ratios[ind][counter] = ideal_ratios[ind][counter]*(exp/act)
            counter += 1
                   
    #Normalizes numbers
    
    for index in xrange(1, len(ideal_ratios.keys())+1):
        curSum = sum(ideal_ratios[index])    
        for sample in xrange(0, len(ideal_ratios[index])):
            if curSum == 0:
                ideal_ratios[index][sample] = 0
            else:
                ideal_ratios[index][sample] = (ideal_ratios[index][sample]/curSum)*100
            
    
    # Iteratively rounds to whole percent (min pipette for volume) to reach 100%
    # ideal_ratio * req_lanes.values() = needed
    # acc_ratio * total_lanes = current
    # means a sample can take any whole number between the two
    
    acc_ratios = copy.deepcopy(ideal_ratios)
    for index in xrange(1, len(ideal_ratios.keys())+1):
        for sample in xrange(0, len(ideal_ratios[index])):
            acc_ratios[index][sample] = math.ceil(ideal_ratios[index][sample])
        if sum(acc_ratios[index]) == 100:
            break
        else:
            while sum(acc_ratios[index]) > 100:
                stuck = True
                for sample in xrange(1, len(ideal_ratios[index])):
                    need = ideal_ratios[index][sample]*req_lanes.values()[index-1]
                    cur = (acc_ratios[index][sample] - 1)*total_lanes[index-1]
                    if sum(acc_ratios[index]) > 100 and cur >= need:
                        acc_ratios[index][sample] -= 1
                        stuck = False
                    if sum(acc_ratios[index])== 100:
                        break
                if(stuck):
                    total_lanes[index-1] += 1
                    
    return acc_ratios

def generate_output(project, destid, total_lanes, req_lanes, lane_maps, acc_ratios):
    #Gathers the container id and well name for all samples in project
    #Cred to Denis for providing a base epp
    location = dict()
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    allProjects = lims.get_projects()
    for proj in allProjects:
        if proj.id == project:
            projName = proj.name 
            break

    #All normalization processes for project
    norms=['Library Normalization (MiSeq) 4.0', 'Library Normalization (Illumina SBS) 4.0','Library Normalization (HiSeq X) 1.0']
    pros=lims.get_processes(type=norms, projectname=projName)
    #For all processes
    for p in pros:
        #For all artifacts in process
        for o in p.all_outputs():
            #If artifact is analyte type and has project name in sample
            if o.type=="Analyte" and project in o.name:
                location[o.name.split()[0]] = list()
                location[o.name.split()[0]].append(o.location[0].id)
                location[o.name.split()[0]].append(o.location[1])
                
    #PRINT section
    #Print stats including duplicates
    timestamp = datetime.fromtimestamp(time()).strftime('%Y-%m-%d_%H:%M')
    sumName = projName,  "_summary_", timestamp,".txt"
    sumName = ''.join(sumName)
    with open(sumName, "w") as summary:
        if sum(req_lanes.values()) != 0:
            OPT = sum(total_lanes)/sum(req_lanes.values())
        else: 
            OPT = 0
        output = "Ideal lanes (same schema): ", str(sum(req_lanes.values())) , ", Total lanes: ", str(sum(total_lanes)), ", OPT: ", str(round(OPT,3)),'\n'
        output = ''.join(output)
        summary.write( output )
        output = "Unique pools: ", str(len(total_lanes)), ", Average pool duplication: ", str(sum(total_lanes)/float(len(total_lanes))) ,'\n'
        output = ''.join(output)
        summary.write( output )
        
        bin = 0
        for index in xrange(1, len(lane_maps)+1):
            bin  += 1
            summary.write('\n')
            output = "Wells ", str(bin) , '-' , str(bin+int(total_lanes[index-1])-1),':','\n'
            output = ''.join(output)
            summary.write( output )
            bin += int(total_lanes[index-1]-1)
            for counter in xrange(1, len(lane_maps[index])):
                output = str(lane_maps[index][counter]),' ', str(acc_ratios[index][counter]), "%",'\n'
                output = ''.join(output)
                summary.write( output )

    
    #Creates csv   
    name = projName,"_repool_",timestamp,".csv"
    name = ''.join(name)
    wells = ['Empty','A','B','C','D','E','F','G','H']
    #Index 0 is number, index 1 is Letter
    wellIndex = [1, 1]
    destNo = 0
    
    with open(name, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for index in xrange(1, len(lane_maps)+1):
            for dupes in xrange(1, int(total_lanes[index-1])+1):
                if lane_maps[index] == 0:
                    raise Exception('Error: Project not logged in x_flowcells database!')
                
                for counter in xrange(1, len(lane_maps[index])):
                    #<source plate ID>,<source well>,<volume>,<destination plate ID>,<destination well>
                    #Destination well 200 microL, minimum pipette 2 microL; acc_ratios multiplied by 2.
                    sample = lane_maps[index][counter]
                    position = wells[wellIndex[1]],':',str(wellIndex[0])
                    position = ''.join(position)
                    try:
                        output = location[sample][0],location[sample][1],str(int(acc_ratios[index][counter]*2)),str(destid[destNo]),position
                    except KeyError:
                        print "Error: Samples incorrectly parsed into database, thus causing sample name conflicts!"
                    if not acc_ratios[index][counter] == 0:
                        writer.writerow(output)
                #Increment wellsindex
                if not acc_ratios[index][counter] == 0:
                    if not wellIndex[1] >= 8:
                        wellIndex[1] += 1
                    else:
                        wellIndex[1] = 1
                        if not wellIndex[0] >= 12:
                            wellIndex[0] += 1
                        else:
                            wellIndex[0] = 1
                            destNo += 1
                            try:
                                destid[destNo]
                            except IndexError:
                                print "Critical error; not enough destination plates provided"
                      
@click.command()
@click.option('--project_id', required=True,help='REQUIRED: ID of project to repool. Examples:P2652, P1312 etc.')
@click.option('--dest_plate_list', default=['dp_1','dp_2','dp_3','dp_4','dp_5'], 
              help='List of destination plates for the robot\'s csv file. Include too many rather than too few; excess will be unused Default:[dp_1,dp_2,dp_3,dp_4,dp_5]') 
@click.option('--target_clusters', default=320*1000000, help='Threshold of clusters per sample. \nDefault:320*1000000')
@click.option('--clusters_per_lane', default=380*1000000, help='Expected clusters generated by a single lane/well. \nDefault:380*1000000')  
@click.option('--allow_non_dupl_struct', is_flag=True, help='Allow for samples to be present in different types of flowcells')           

def main(target_clusters, clusters_per_lane, project_id, dest_plate_list, allow_non_dupl_struct):
    """Application that calculates samples under threshold for a project, then calculate the optimal composition for reaching the threshold
    without altering concentrations nor the structure of the pools. Outputs both a summary as well as a functional csv file."""    
    couch = connection()
    structure = proj_struct(couch, project_id, target_clusters)
    [lane_maps, clusters_rem, clusters_expr] = parse_indata(structure, target_clusters)
    if allow_non_dupl_struct:
        sys.warn("WARN: Allow_non_dupl_struct is experimental at best. Use with a MASSIVE grain of salt")
        aggregator(lane_maps,clusters_rem,clusters_per_lane)
    else:
        simple_unique_set(lane_maps)
        sys.warn("WARN: Output from repooler is experimental. Remember to review all numbers before re-sequencing.")
    [ideal_ratios, req_lanes, total_lanes] = sample_distributor(lane_maps, clusters_rem, clusters_per_lane)
    acc_ratios = correct_numbers(lane_maps, clusters_expr, ideal_ratios, req_lanes, total_lanes)
    generate_output(project_id, dest_plate_list, total_lanes, req_lanes, lane_maps, acc_ratios)    

if __name__ == '__main__':
    main()
