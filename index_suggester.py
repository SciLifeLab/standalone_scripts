"""Determines the ideal set of adapters for a number of samples.
Determination is done by finding the set with the latest, and least impactful collisions
Impact is determined by most frequent base pair
Note: Currently only checks the most freq nuc for a given BP and disregards the secondnd most freq (etc)

REQUIRES A JSON KEY FOR THE GOOGLE DOCUMENT FETCH TO WORK

By: Isak Sylvin 
Email: isak.sylvin@scilifelab.se"""

import re
import json
import gspread
import sys
from oauth2client.client import SignedJwtAssertionCredentials as GCredentials
import operator
from operator import itemgetter
from decimal import *
import math

getcontext().prec = 2
sys.setrecursionlimit(1500)

"""Read sequence and adapters name from
   https://docs.google.com/spreadsheets/d/1jMM8062GxMh9FZdy7oi8WFVv3AYyCRPdOG6jwej0mOo/edit#gid=0
"""
def setup_gdocs(document, sheet):
    #Create credentials at https://console.developers.google.com
    #Share document with client e-mail (see key)
    json_key = json.load(open('adapters-for-neoprep-gdocs-key.json'))
    credentials = GCredentials(json_key['client_email'], json_key['private_key'], \
                               'https://spreadsheets.google.com/feeds')
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open(document)
    worksheet = spreadsheet.worksheet(sheet)
    
    return worksheet

"""Takes in a worksheet and a list of numbers.
    Returns a list of dicts where each dict contains pools equal to the number in the list
"""
def extract_info(worksheet, samplelist):
    #Extract info
    adapters = list()
    names = list()
    alpha_names = worksheet.range('G1:G100')
    alpha_adapters = worksheet.range('F1:F100')
    #Gather adapter sequences
    for cell in alpha_adapters:
        #Discard if empty or not only contain nucleotide
        if not re.search('[^ATCG]+',cell.value) and not cell.value == "":
            adapters.append(cell.value)
            #Adds associated name
            name_index = alpha_adapters.index(cell)
            names.append(alpha_names[name_index].value)
            
    #Forms the output
    #Can be optimized by copying pools of equal size
    setinfo = list()
    for samples_in_pool in samplelist:
        sets = len(adapters) - samples_in_pool + 1
        poolinfo = dict()        
        for thisset in range(0, sets):
            #Store set info. Adaptable.
            poolinfo['Set {}'.format(thisset+1)] = dict()
            poolinfo['Set {}'.format(thisset+1)]['names'] = names[thisset:thisset + samples_in_pool]
            poolinfo['Set {}'.format(thisset+1)]['adapters'] = adapters[thisset:thisset + samples_in_pool]
        setinfo.append(poolinfo)
    return setinfo  

"""Takes in setinfo (dict with subdicts 'names' and 'adapters')
    Calculates each A T C G % per base position for each set.
    returns stucture spreadlist[index][setname][position]
"""
def nucleotide_spread(setinfo):
    spreadlist = list()
    for pool in setinfo:
        spread = dict()
        for key, value in pool.items():
            spread[key] = list()
            
            sett = pool[key]['adapters']
            adapter_len = len(sett[0])
            
            for n in range(0, adapter_len):
                templist = list()
                
                #All nucleotides in that position
                currpos = map(itemgetter(n), sett)
                for bases in ['A','T','C','G']:
                    templist.append(currpos.count(bases)/Decimal(len(currpos)))
                spread[key].append(templist)
        spreadlist.append(spread)
    return spreadlist

"""Finds the most equal nucleotide spread and returns the set.
    Depending on input inequalities in the index are weighted differently.
    Returns dict with scores. Lower score is better
"""
def scorer(spreadlist):
    #ONLY SCORES PAST THE ROOF!
    
    sorted_scorelist=list()

    #Roof inf. low
    
    #HOW SHOULD SCORES BE SET???
    #This function never checks the second worst (needs roof for that).
    for spread in spreadlist:
        scores = dict()
        for key, value in spread.items():
            setscore = 0
            for nucvalue in value:
                setscore = setscore + max(nucvalue)
            scores[key] = setscore  
        sorted_scores = sorted(scores.items(), key=operator.itemgetter(1))
        sorted_scorelist.append(sorted_scores)
    
    #Horizontal roof
    
    #Echelon left roof
    
    #Logarithmic roof
    
    return sorted_scorelist

#We assume that adapters have sufficient sequence dissimilarily

def filter_and_combine(setinfo, scorelist):
    namesets = list()
    viablesets = list()
    
    import pdb
    pdb.set_trace()
    for pool in setinfo.items():
        print "pop"
    
    
    #Create setlists
    ##for pool in scorelist:
     #   for key, value in pool:
     #       #Set 1
     #       namesets.append( setinfo[scorelist.index(pool)][key]['names'] )
     #       import pdb
     #       pdb.set_trace()
    
    #return combinefiltered

def outputter(setinfo, scored_sets):
    import pdb
    pdb.set_trace() 
    #Minimal score is ideal
    print "\n-------------------------------------------"
    print "Sets ordered by score:"
    print "-------------------------------------------\n"
    for key, value in scored_sets:
        samples = len(setinfo[key]['names'])
        taljare = math.ceil(samples/float(4)) 
        #Review ideal score once proper score functions come into play
        print "{}: Score {}. Ideal score: ({}/{})*{} = {}".format(key, value, int(taljare) , samples, samples, taljare/samples*samples)
        print "{}".format(', '.join(setinfo[key]['names']))
        print "Indexes:"
        print "{}\n".format('\n'.join(setinfo[key]['adapters']))
    import pdb
    pdb.set_trace()
    
        
#____________________MAIN__________________________________________

doc = "Adapters for NeoPrep"
sheet = "Sheet1"
samplelist = list()

print "--- Index Suggester ---"
pools = input("Enter number of pools (1+): ")
if pools == 1:
    num_samples = input("Enter number of samples to assign indexes to (2+): ")
    samplelist.append(num_samples)
else:
    for poolindex in range(1, pools + 1):
        num_samples = input("Enter number of samples for pool number {} (2+): ".format(poolindex))
        samplelist.append(num_samples)
        
print "Fetching information from sheet '{}' of document '{}'.".format(sheet, doc)

worksheet = setup_gdocs(doc, sheet)
setinfo = extract_info(worksheet, samplelist)
spread = nucleotide_spread(setinfo)
scored_sets = scorer(spread)
filtered_scores = filter_and_combine(setinfo, scored_sets)
outputter(setinfo, scored_sets)


#Output set
#Output metrics

###LEGACY CODE 
"""#Calculate set with optimal distance from eachother
[ideal_set, setinfo] = max_distance(num_samples, adapters, names)
results_out(ideal_set, setinfo)"""

""" Finds the most abundant nucleotide at each position for a set.
    returns the set with the lowest string of "most abundant nucleotide" (e.g. 0123553)
"""
"""def max_distance(num_samples, adapters, names):
    setinfo = dict()
    collision_weights=dict()
    sets = len(adapters) - num_samples + 1
    adapter_len = len(adapters[0])
    
    #Figure out where collisions are
    
    #For every possible set
    for thisset in range(0, sets):
        collision_weights['Set {}'.format(thisset+1)] = ""
        #Store set info. Adaptable.
        setinfo['Set {}'.format(thisset+1)] = list()
        setinfo['Set {}'.format(thisset+1)].append(names[thisset:thisset+num_samples])
        setinfo['Set {}'.format(thisset+1)].append(adapters[thisset:thisset+num_samples])
        #Compare nucleotide n
        for n in range(0, adapter_len):
            nucleotides = [nuc[n] for nuc in adapters[thisset:thisset+num_samples]]
            
            #If nucs has collision
            if not len(nucleotides) == len(set(nucleotides)):
                #Add freq of most abundant base
                most_freq = max(set(nucleotides), key=nucleotides.count)
                collision_weights['Set {}'.format(thisset+1)] += str(nucleotides.count(most_freq))
            else:
                #Add a zero if allequal
                collision_weights['Set {}'.format(thisset+1)] += str(0) 
        setinfo['Set {}'.format(thisset+1)].append(collision_weights['Set {}'.format(thisset+1)])

    #Pick subset with smallest collision
    ideal_set = smallest_collision(collision_weights)
    return ideal_set, setinfo

def smallest_collision(collision_weights):
    least_colls = list()
    smallest_coll_string = min(collision_weights.itervalues())
    
    for key, value in collision_weights.items():
        if value == smallest_coll_string:
            least_colls.append(key)
    
    return least_colls
    
def results_out(ideal_set, setinfo):
    print ""
    if len(ideal_set) > 1:
        print "Multiple equivalent sets found. Outputting in random order."

    #Collision string is the same, so just picking the first
    print "Nucleotide collision string in adapter(s) is '{}'.\n".format(setinfo[ideal_set[0]][2])
    #Unpacking vars
    for thisset in ideal_set:
        adapters = ""
        names = ""
        for name in setinfo[thisset][0]:
            names += "'" + name + "' "
        for adapter in setinfo[thisset][1]:
            adapters += "'" + adapter + "' "
        #Writing output
        print "Suggesting {} consisting of: \nNames: {} \nIndexes: {}\n" \
        .format(thisset, names, adapters)"""