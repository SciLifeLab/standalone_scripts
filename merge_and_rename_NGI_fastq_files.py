#!/usr/bin/env/python
# Merges all fastq_files from each samples into one file. 
# ARGV 1 Base directory with files to be merged
# ARGV 2 Destination dir of the merged output files

import re
import os
import sys
import shutil
def merge_files(dest_dir, fastq_files):
    #Match NGI sample number from flowcell
    sample_pattern=re.compile("^(.+)_S[0-9]+_.+_R([1-2])_")
    #Remove files that already have the right name (i.e have been merged already)
    matches=[]
    for fastq in fastq_files:
        try:
            match=sample_pattern.search(fastq).group(1)
            if match:
                matches.append(fastq)
        except AttributeError: 
            continue
    fastq_files=matches
    
    while fastq_files:
        tomerge=[]
        try:
            first=fastq_files[0]
            fq_bn=os.path.basename(first)
            sample_name=sample_pattern.match(fq_bn).group(1)
            read_nb=sample_pattern.match(fq_bn).group(2)
        #Avoid crashing if no match
        except AttributeError: 
            continue
        for fq in fastq_files:
            #If fq belongs to the same sample and read, add it to tomerge then remove from file list
            if sample_name in os.path.basename(fq) and "_R{}_".format(read_nb) in os.path.basename(fq):
                tomerge.append(fq)
        for fq in tomerge:
            fastq_files.remove(fq)
        #Save under Pname, i.e P001_001_R1.fastq.gz
        outfile=os.path.join(dest_dir, "{}_R{}.fastq.gz".format(sample_name, read_nb))
        with open(outfile, 'wb') as wfp:
            for fn in tomerge:
                with open(fn, 'rb') as rfp:
                    shutil.copyfileobj(rfp, wfp)

def main():
   input_dir=sys.argv[1]
   destination_dir = sys.argv[2] 
    
   fastq_files=[]
   #gather all fastq files in inputdir and subdirs
   for subdir, dirs, files in os.walk(input_dir):
       for fastq in  files:
           if fastq.endswith('.fastq.gz'):
               fastq_files.append(os.path.join(subdir, fastq))
   
   fastq_files.sort()
   merge_files(destination_dir,fastq_files)

if __name__ == "__main__":
    main()
