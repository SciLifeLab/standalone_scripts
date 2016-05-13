#!/usr/bin/env python2.7

import couchdb
import re
import math
from collections import defaultdict, Counter, OrderedDict
import numpy
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
    user = 'isak'
    pw = 'Purpleplant89'
    tools_server = 'tools.scilifelab.se:5984'
    print("Database server used: http://{}".format(tools_server))
    print("LIMS server used: " + BASEURI)
    couch = couchdb.Server('http://{}:{}@{}'.format(user, pw, tools_server))
    try:
        print "Connecting to statusDB..."
        couch.version()
    except:
        sys.exit("Can't connect to couch server. Username & Password is incorrect, or network is inaccessible.")
    return couch


def proj_struct(couch, project, target_clusters):
    """"Fetches the structure of a project"""
    db = couch['x_flowcells']
    view = db.view('names/project_ids_list')
    fc_track = defaultdict(set)
    
    #Adds flowcells to ALL projects. Due to interactions its easier to just get FCs for ALL projects
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
    """Takes in data and finds unique lane structure, clusters per sample and lane division"""
    clusters_rem = dict() #Total remaining clusters
    lane_maps = dict() #All possible structs. Sample-aggregate is a string, values are average expression
    sample_struct = dict() #All possible structs. Each sample is a value.
    copies = dict() #Keeps track of duplicates of each structure
    #Changed counter from 1 to 0 for lane_maps; uh-oh
    
    #TODO: LANES IN SAMPLE_STRUCT IS DISPLAYED TWICE
    
    for fc, lanes in struct.items():
        for lane, samples in lanes.items():
            #Concats the structure into a set of unique structures
            mapp = sorted(samples, reverse=True)
            map_names = str(mapp)
            map_values = [value for (key, value) in sorted(samples.items(), reverse=True)]

            if not map_names in lane_maps:
                sample_struct[map_names] = mapp
                lane_maps[map_names] = map_values
                copies[map_names] = 1
            else:
                copies[map_names] = copies[map_names]+1
                
            #Calculate remaining clusters for each sample
            for sample, value in samples.items():
                if not sample in clusters_rem:
                    clusters_rem[sample] = target_clusters
                clusters_rem[sample] -= value
                if clusters_rem[sample] < 0:
                    clusters_rem[sample] = 0
    
    #Calculate average output of sample for each structure type
    for names, values in lane_maps.items():
        values = numpy.array(values)
        lane_maps[names] = values/copies[names]
        #Manually overwrites undetermined to exclude them
        lane_maps[names][0] = target_clusters

    return [lane_maps, clusters_rem, sample_struct] 


def simple_unique_set(sample_struct, clusters_rem, target_clusters):
    """Creates a set where every sample uniquely appears once and only once
    Prioritizes lanes with most unsequenced samples first"""


    #Inline function to calculate number of unfinished samples per lane
    def f(x):
        count = 0
        for sample in x:
            if clusters_rem[sample] < target_clusters:
                count = count + 1
        print count
        return count

    #Best = Most samples in need of sequencing = End of lists
    best_cands = OrderedDict(sorted(sample_struct.items(), key=lambda t: f(t[1])))
    
    unique_sample_structs = dict()
    
    #pop the best candidate
    prime = best_cands.pop()
    #List which lanes are excluded from primes inclusion (unique requirement)
    for name, dupe in prime:
        excluded = list()
        for str, lis in best_cands:
            for sample in lis:
                if dupe == sample and name != str and sample != 'Undetermined':
                    excluded.append(str)
                    break
                
    #CONTINUE FROM HERE!!"
    #Check that none of the excluded lanes have uniquely present samples
    for str in excluded:
        for samples in sample_struct[str]:
            
    
    
    
    acceptable = False
    for key in excluded:
        total_duplicates = list()
        for values in lane_maps[key]:
            duplicate = 0
            for k, v in lane_maps.items():
                for dupe in v:
                    if dupe == values and k != key and sample != 'Undetermined':
                        acceptable = True
                        break
            total_duplicates.append(duplicate)
            
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
                

    validate_samples_unique(unique_lane_maps)
    
    
    import pdb
    pdb.set_trace()
    
    unique_lane_maps = dict()
    for keyz, valz in lane_maps.items():
        #Fetch what lanes inclusion of given lane excludes
        excluded = list()
        for sample in lane_maps:
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
                

    validate_samples_unique(unique_lane_maps)
    #validate_all_samples_present(lane_maps)
    
    return unique_lane_maps
    
def validate_all_samples_present(lane_maps):    
    summap = []
    for k in lane_maps.keys():
        summap += lane_maps[k]
    print len(set(summap))   
    
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

def sample_distributor(lane_maps, clusters_rem, clusters_per_lane):
    """Gives how many percent of the lane should be allocated to a specific sample"""    
    ratios_equal_conc = dict()
    req_lanes = dict()
    
    for index in lane_maps:
        summ = 0
        for entry in lane_maps[index]:
            if clusters_rem[entry] > 0:
                summ += clusters_rem[entry]
        for entry in lane_maps[index]:
            if not index in ratios_equal_conc:
                ratios_equal_conc[index] = list()
            if clusters_rem[entry] > 0:
                ratios_equal_conc[index].append(clusters_rem[entry]/float(summ))
            else: 
                ratios_equal_conc[index].append(0.0)
        #Minimal number of required lanes per pool
        req_lanes[index] = summ/float(clusters_per_lane)
    #Have to be rounded up, rounding down when only using duplicates makes no sense
    total_lanes = map(math.ceil, req_lanes.values())
    
    return [ratios_equal_conc, req_lanes, total_lanes]


def validate_samples_unique(lane_maps): 
    """Crude way to check that no samples are in different TYPES of lanes"""
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


def fix_conc_offset(lane_maps, clusters_expr, ratios_equal_conc):
    """Since some samples are strong and some weaksauce
    10% in ratios_equal_conc does not mean 10% of lane volume
    As such, ratios_equal_conc need to be divided by actual_reads/expected_reads
    Ignores undetermined clusters in calculation
    Assumes sample conc cant be altered; aka only volume is modified"""
    #THIS IS BROKEN, SINCE SAMPLES MAY HAVE BEEN SEQUENCED IN DIFFERENT POOLS
    
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
            ratios_equal_conc[ind][counter] = ratios_equal_conc[ind][counter]*(exp/act)
            counter += 1
                   
    #Normalizes numbers
    for index in xrange(1, len(ratios_equal_conc.keys())+1):
        curSum = sum(ratios_equal_conc[index])    
        for sample in xrange(0, len(ratios_equal_conc[index])):
            if curSum == 0:
                ratios_equal_conc[index][sample] = 0
            else:
                ratios_equal_conc[index][sample] = (ratios_equal_conc[index][sample]/curSum)*100

def realize_numbers(lane_maps, clusters_expr, ratios_equal_conc, req_lanes, total_lanes):
    """Actual numbers need to be offset to: 
    A) Work with a pipette of min 2uL 
    B) Work with a pipette with an increase of 0.1*x uL
    C) Downsize under 100%"""
    added=0
    
    #Fixes ratios_equal_conc
    fix_conc_offset(lane_maps, clusters_expr, ratios_equal_conc)
    acc_ratios = copy.deepcopy(ratios_equal_conc)

    ##DOUBLECHECK ROUNDING
    #Set A
    for i in ratios_equal_conc:
        dupes=total_lanes[i-1]
        minTres=round(200/(float(dupes*5)), 2)
        minAdd= round(200/(float(dupes*5*20)), 2)
        for sample in ratios_equal_conc[i]:
            sampleIndex = acc_ratios[i].index(sample)
            if sample < minTres and not sample == 0.0:
                acc_ratios[i][sampleIndex] = minTres
            #Sets B
            elif not round(sample, 2) % minAdd == 0 and not sample == 0.0:
                acc_ratios[i][sampleIndex] = (ratios_equal_conc[i][sampleIndex] 
                                              - (sample % minAdd) + minAdd)
            #Silly float fix
            acc_ratios[i][sampleIndex] = round(acc_ratios[i][sampleIndex], 2)

    #Sets C
    for i in acc_ratios:
        while not sum(acc_ratios[i]) <= 100.0:
            stuck = True
            for sample in acc_ratios[i]:
                sampleIndex = acc_ratios[i].index(sample)
                dupes=total_lanes[i-1]
                minAdd=round(200/(float(dupes*5*20)), 2)
                minTres=round(200/(float(dupes*5)), 2)
                
                #Bit dumb, should find the one with biggest diff and remove from it, instead of just ordered
                if sum(acc_ratios[i]) <= 100.0:
                    break
                elif (not acc_ratios[i][sampleIndex] == 0.0 and not acc_ratios[i][sampleIndex]-minAdd < minTres and
                (acc_ratios[i][sampleIndex]- minAdd)*total_lanes[i-1]  > ratios_equal_conc[i][sampleIndex]*req_lanes[i]):
                    acc_ratios[i][sampleIndex] = round(acc_ratios[i][sampleIndex]-minAdd, 2)
                    stuck = False
            if stuck:
                total_lanes[i-1] = total_lanes[i-1] + 1
                added = added + 1
    print "Lanes added due to  pipette limitations: {}".format(added)
    #SPECIAL CODE:
    #Upscales all results to 100%. Leaves a miniature pool of sample left, but whatever.
    #Make sure to fix csv file after this
    for i in acc_ratios:
        if sum(acc_ratios[i]) < 100:
            total = sum(acc_ratios[i])/100 
            for sample in acc_ratios[i]:
                acc_ratios[i][acc_ratios[i].index(sample)] = round(acc_ratios[i][acc_ratios[i].index(sample)]/float(total), 2)     
    return acc_ratios, added 
     
def round_whole(lane_maps, clusters_expr, ratios_equal_conc, req_lanes, total_lanes):
    """Old rounding that made some faulty assumptions about pool sizes
    
    Translates float -> int without underexpressing anything
    Iteratively rounds to whole percent (min pipette for volume) to reach 100%
    ideal_ratio * req_lanes.values() = needed
    acc_ratio * total_lanes = current
    means a sample can take any whole number between the two"""
    
    added = 0
    #Fixes ratios_equal_conc
    fix_conc_offset(lane_maps, clusters_expr, ratios_equal_conc)
    acc_ratios = copy.deepcopy(ratios_equal_conc)
            
    for index in xrange(1, len(ratios_equal_conc.keys())+1):
        for sample in xrange(0, len(ratios_equal_conc[index])):
            acc_ratios[index][sample] = math.ceil(ratios_equal_conc[index][sample])
        if sum(acc_ratios[index]) == 100:
            break
        else:
            while sum(acc_ratios[index]) > 100:
                stuck = True
                for sample in xrange(1, len(ratios_equal_conc[index])):
                    need = ratios_equal_conc[index][sample]*req_lanes.values()[index-1]
                    cur = (acc_ratios[index][sample] - 1)*total_lanes[index-1]
                    if sum(acc_ratios[index]) > 100 and cur >= need:
                        acc_ratios[index][sample] -= 1
                        stuck = False
                    if sum(acc_ratios[index])== 100:
                        break
                if(stuck):
                    total_lanes[index-1] += 1
                    added = added + 1
            
    print "Lanes added due to pipette limitations: {}".format(added)                
    return acc_ratios

def generate_output(project, destid, total_lanes, req_lanes, lane_maps, acc_ratios, target_clusters, clusters_per_lane, extra_lanes,lane_volume,pool_excess):
    """"Gathers the container id and well name for all samples in project"""
    timestamp = datetime.fromtimestamp(time()).strftime('%Y-%m-%d_%H:%M')
    
    #Cred to Denis for providing a base epp
    location = dict()
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    allProjects = lims.get_projects()
    for proj in allProjects:
        if proj.id == project:
            projName = proj.name 
            break
    
    #Sets up source id    
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
   
    generate_summary(projName, timestamp, project, destid, total_lanes, req_lanes, 
                     lane_maps, acc_ratios, target_clusters, clusters_per_lane, extra_lanes)
    generate_csv(projName, timestamp, location, destid, total_lanes, lane_maps, acc_ratios, lane_volume, pool_excess)
    
def generate_summary(projName, timestamp, project, destid, total_lanes, req_lanes, lane_maps, acc_ratios, 
                     target_clusters, clusters_per_lane, extra_lanes):                
    """Print stats including duplicates"""
    
    sumName = '{}_summary_{}.txt'.format(projName, timestamp)
    with open(sumName, "w") as summary:
        if sum(req_lanes.values()) != 0:
            OPT = sum(total_lanes)/sum(req_lanes.values())
        else: 
            OPT = "ERROR"
        
        if OPT > 1.5:
            print("Warning, OPT exceeds 1.5x! Generated solution is so poor a better one can likely be done by hand!")
            
        output = 'Target clusters per sample: {}, Expected clusters per lane: {}\n'.format(str(target_clusters), str(clusters_per_lane))    
        output = output + 'Project ID: {}, Allow non-duplicate structures: {}\n'.format(project, str(allow_non_dupl_struct))
        output = output + 'Destination plate name list: {}\n'.format(str(destid))
        output = (output + 'Ideal lanes (same schema): {}, Total lanes: {}, Expression over theoretical ideal (OPT): {}x\n'
                  .format(str(round(sum(req_lanes.values()),3)), str(sum(total_lanes)), str(round(OPT,3))))
        output = output + 'Lanes added to prevent sum of lane > 100%: {} (this can be mitigated with bigger pools).\n'.format(extra_lanes)
        output = output + 'Unique pools: {}, Average pool duplication: {}\n'.format(str(len(total_lanes)), str(round(sum(total_lanes)/float(len(total_lanes)),3)))
        summary.write( output )
        
        bin = 0
        for index in xrange(1, len(lane_maps)+1):
            bin  += 1
            output = '\nLane {} to {}:\n'.format(str(bin), str(bin+int(total_lanes[index-1])-1))
            summary.write( output )
            bin += int(total_lanes[index-1]-1)
            for counter in xrange(1, len(lane_maps[index])):
                output = '{} {}%\n'.format(str(lane_maps[index][counter]), str(acc_ratios[index][counter]))
                summary.write( output )
                
def generate_csv(projName, timestamp, location, destid, total_lanes, lane_maps, acc_ratios, lane_volume, pool_excess):
    """Creates the output csv file"""
    
    name = '{}_repool_{}.csv'.format(projName, timestamp)
    wells = ['Empty','A','B','C','D','E','F','G','H']
    #Index 0 is number, index 1 is Letter
    wellIndex = [1, 1]
    destNo = 0
    
    with open(name, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for index in xrange(1, len(lane_maps)+1):
            try:
                destid[destNo]
            except IndexError:
                destid.append ('dp_{}'.format(str(destNo+1)))
                #print "Critical error; not enough destination plates provided"
            
            #Ugly constants
            maxDupes = 200/5
            dupes = int(total_lanes[index-1])
            
            if lane_maps[index] == 0:
                raise Exception('Error: Project not logged in x_flowcells database!')  
            if dupes > maxDupes:
                raise Exception("Error: A pool has been requested that can't be fit into a single well!")

            for counter in xrange(1, len(lane_maps[index])):
                #<source plate ID>,<source well>,<volume>,<destination plate ID>,<destination well>
                sample = lane_maps[index][counter]
                position = '{}:{}'.format(wells[wellIndex[1]], str(wellIndex[0]))
                
                try:
                    #out_pool = acc_ratios[index][counter]/100*(float(lane_volume)*dupes+pool_excess)
                    out_pool = acc_ratios[index][counter]/100*(float(lane_volume)*dupes)
                    output = location[sample][0],location[sample][1],str(out_pool),str(destid[destNo]),position
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
                      
@click.command()
@click.option('--project_id', required=True,help='ID of project to repool. Examples:P2652, P1312 etc.')
@click.option('--dest_plate_list', default=['dp_1'], 
              help='List of destination plates for the robot\'s csv file. Include too many rather than too few; excess will be unused. Default: [dp_1]') 
@click.option('--target_clusters', default=320*1000000, help='Threshold of clusters per sample. \nDefault:320*1000000')
@click.option('--clusters_per_lane', default=380*1000000, help='Expected clusters generated by a single lane/well. \nDefault:380*1000000')  
@click.option('--lane_volume', default=5, help='Lane volume. \nDefault:5 (uL)') 
@click.option('--pool_excess', default=2, help='Excess pool volume when creating a pool. \nDefault:2 (uL)') 
@click.option('--min_pipette', default=1, help='Minimum pipette volume. \nDefault:1 (uL)')          

def main(target_clusters, clusters_per_lane, project_id, dest_plate_list, lane_volume, pool_excess, min_pipette):
    """Application that calculates samples under threshold for a project, then calculate the optimal composition for reaching the threshold
    without altering concentrations nor the structure of the pools. Outputs both a summary as well as a functional csv file."""  
    couch = connection()
    structure = proj_struct(couch, project_id, target_clusters)
    [all_lane_maps, clusters_rem, sample_struct] = parse_indata(structure, target_clusters)
    lane_maps = simple_unique_set(sample_struct, clusters_rem, target_clusters)
    [ratios_equal_conc, req_lanes, total_lanes] = sample_distributor(lane_maps, clusters_rem, clusters_per_lane)
    acc_ratios, extra_lanes = realize_numbers(lane_maps, lane_maps, ratios_equal_conc, req_lanes, total_lanes)
    generate_output(project_id, dest_plate_list, total_lanes, req_lanes, lane_maps, acc_ratios, 
                    target_clusters, clusters_per_lane, extra_lanes,lane_volume,pool_excess)    

    print("WARN: Output from repooler is experimental. Remember to review all numbers before re-sequencing.")
if __name__ == '__main__':
    main()
