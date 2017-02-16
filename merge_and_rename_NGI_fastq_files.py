#!/usr/bin/env/python
# Merges all fastq_files from each samples into one file. 
# ARGV 1 Base directory with files to be merged
# ARGV 2 Destionation dir of the merged output files

import re
import os
import sys
import shutil
import glob
def merge_files(dest_dir, fastq_files):
    sample_pattern=re.compile("^(.+)_S[0-9]+_.+_R([1-2])_")
    #Below is used for BCBP above for Irma style projects.
    #sample_pattern=re.compile("^[0-9]_[0-9]+_[A-Z0-9]+_(P[0-9]+_[0-9]+)_([1-2])")
    while fastq_files:
        tomerge=[]
        first=fastq_files[0]
        fq_bn=os.path.basename(first)
        sample_name=sample_pattern.match(fq_bn).group(1)
        read_nb=sample_pattern.match(fq_bn).group(2)
        for fq in fastq_files:
            if sample_name in os.path.basename(fq) and "_R{}_".format(read_nb) in os.path.basename(fq):
                tomerge.append(fq)
        for fq in tomerge:
            fastq_files.remove(fq)

        import pdb
        pdb.set_trace()
        outfile=os.path.join(dest_dir, "{}_R{}.fastq.gz".format(sample_name, read_nb))
        with open(outfile, 'wb') as wfp:
            for fn in tomerge:
                with open(fn, 'rb') as rfp:
                    shutil.copyfileobj(rfp, wfp)

def main():
    #os.path.abspath
   input_dir=sys.argv[1]
   destination_dir = sys.argv[2] 
    
   fastq_files=[]
   
   for subdir, dirs, files in os.walk(input_dir):
       for x in files:
           fastq_files.append(os.path.join(subdir, x))
   fastq_files.sort()
   merge_files(destination_dir,fastq_files)

if __name__ == "__main__":
    main()
