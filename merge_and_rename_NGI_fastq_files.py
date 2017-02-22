#!/usr/bin/env/python
# Merges all fastq_files from each samples into one file. 
# ARGV 1 Base directory with files to be merged
# ARGV 2 Destination dir of the merged output files

import re
import os
import sys
import shutil
import argparse
import collections
def merge_files(input_dir, dest_dir):
   #Gather all fastq files in inputdir and its subdirs
   fastq_files=[]
   for subdir, dirs, files in os.walk(input_dir):
       for fastq in  files:
           if fastq.endswith('.fastq.gz'):
               fastq_files.append(os.path.join(subdir, fastq))
   
    
    #Match NGI sample number from flowcell
    sample_pattern=re.compile("^(P[0-9]+_[0-9]+)_S[0-9]+_.+_R([1-2])_")
    #Remove files that already have the right name (i.e have been merged already)
    matches=[]
    for fastq_file in fastq_files:
        try:
            match=sample_pattern.search(fastq_file).group(1)
            if match:
                matches.append(fastq_file)
        except AttributeError: 
            continue
    fastq_files=matches
   
    ## Sort on basename to avoid collison
    
    while fastq_files:
        tomerge=[]
        
        #grab one sample to work on
        first=fastq_files[0]
        fq_bn=os.path.basename(first)
        sample_name=sample_pattern.match(fq_bn).group(1)
        fastq_files_read1=[]
        fastq_files_read2=[]
        def getKey(item):
                return item[1]
        #make a list of lists with abs path and basename
        for fq in fastq_files:
            if sample_name in os.path.basename(fq) and "_R1_" in os.path.basename(fq):
                fastq_files_read1.append([fq, os.path.basename(fq)])
                
            if sample_name in os.path.basename(fq) and "_R2_" in os.path.basename(fq):
                fastq_files_read2.append([fq, os.path.basename(fq)])
        #sort both lists on the basename.  
        fastq_files_read1.sort(key=getKey)
        fastq_files_read2.sort(key=getKey)
        #Split the lists
        fastq_files_read1, throw_away = zip(*fastq_files_read1)
        fastq_files_read2, throw_away = zip(*fastq_files_read2)

        actual_merging(sample_name,read_nb, fastq_files_read1)
        actual_merging(sample_name,read_nb, fastq_files_read2)
        #need to give only the abspaths  to the merge function. 
    def actual_merging(sample_name,read_nb, tomerge):
        outfile=os.path.join(dest_dir, "{}_R{}.fastq.gz".format(sample_name, read_nb))
        print "Merging the following files:"
        for x in tomerge:
            print x
            print "as {}".format(outfile) 
        with open(outfile, 'wb') as wfp:
            for fn in tomerge:
                with open(fn, 'rb') as rfp:
                    shutil.copyfileobj(rfp, wfp)
        
        for fq in tomerge:
            fastq_files.remove(fq)

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description=' Merges all fastq-files from each samples into one file. Looks through the given dir and subdirs')
   parser.add_argument("input_dir", metavar='<Path/to/input/files>', nargs='?', default='.'
                                   help="Base directory for the fastq files that should be merged. ")
   parser.add_argument("dest_dir", metavar='<Path/to/destination/dir>', nargs='?', default='.'
                                   help="Path path to where the merged files should be outputed. ")
   kwargs = vars(parser.parse_args()) 
   merge_files((**kwargs)
