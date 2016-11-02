"""Determines the ideal set of adapters for a number of samples.
Determination is done by finding the set with the latest, and least impactful collisions
Impact is determined by most frequent base pair
Note: Currently only checks the most freq nuc for a given BP and disregards the secondnd most freq (etc)

By: Isak Sylvin 
Email: isak.sylvin@scilifelab.se"""

import re
import json
import gspread
import sys
from oauth2client.client import SignedJwtAssertionCredentials as GCredentials

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

def extract_info(worksheet):
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
            
    return [adapters, names]

def max_distance(num_samples, adapters, names):
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
        .format(thisset, names, adapters)
  
        
#_MAIN__________________________________________

doc = "Adapters for NeoPrep"
sheet = "Sheet1"
print "--- Index Suggester ---"
num_samples = input("Enter number of samples to assign indexes to (2+): ")
print "Fetching information from sheet '{}' of document '{}'.".format(sheet, doc)

worksheet = setup_gdocs(doc, sheet)
[adapters, names] = extract_info(worksheet)
#Calculate set with optimal distance from eachother
[ideal_set, setinfo] = max_distance(num_samples, adapters, names)
results_out(ideal_set, setinfo)

#Output set
#Output metrics