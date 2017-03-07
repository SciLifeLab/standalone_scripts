# These scripts are for the purpose of converting Illumina samplesheet that contains Chromium 10X indexes for demultiplexing.
# Headers and lines with ordinary indexes will be passed without any change. Lines with Chromium 10X indexes will be expanded into 4 lines, with 1 index in each line, and suffix 'Sx' will be added at the end of sample names.
# Example:
# ------------------------------------------------------------------------------------------------------
# Script: python main.py -i <inputfile> -o <outputfile> -x <indexlibrary>
# ------------------------------------------------------------------------------------------------------
# Original samplesheet:
# [Header]
# Date,None
# Investigator Name,Chuan Wang
# Experiment Name,Project_001
# [Reads]
# 151
# 151
# [Data]
# Lane,SampleID,SampleName,SamplePlate,SampleWell,index,index2,Project,Description
# 1,Sample_101,101,HGWT5ALXX,1:1,SI-GA-A1,,Project_001,
# ------------------------------------------------------------------------------------------------------
# Modified samplesheet:
# [Header]
# Date,None
# Investigator Name,Chuan Wang
# Experiment Name,Project_001
# [Reads]
# 151
# 151
# [Data]
# Lane,SampleID,SampleName,SamplePlate,SampleWell,index,index2,Project,Description
# 1,Sample_101_S1,101_S1,HGWT5ALXX,1:1,GGTTTACT,,Project_001,
# 1,Sample_101_S2,101_S2,HGWT5ALXX,1:1,CTAAACGG,,Project_001,
# 1,Sample_101_S3,101_S3,HGWT5ALXX,1:1,TCGGCGTC,,Project_001,
# 1,Sample_101_S4,101_S4,HGWT5ALXX,1:1,AACCGTAA,,Project_001,
# ------------------------------------------------------------------------------------------------------

# Samplesheet_convert_v1.0
# Written by Chuan Wang (chuan-wang@github), 2017-03-06

#!/usr/bin/python

import sys, getopt
import csv

# Read index library
def read_index_library(indexlibrary):
    with open(indexlibrary,mode='r') as input_file:
        indexes = csv.reader(input_file)
        index_library = {rows[0]:[rows[1],rows[2],rows[3],rows[4]] for rows in indexes}
    return index_library

# Modify samplesheet
def modify_samplesheet(inputfile,indexlibrary):
    index_library=read_index_library(indexlibrary)
    modified_samplesheet=[]
    with open(inputfile,mode='r') as org:
        samplesheet = csv.reader(org)
        for row in samplesheet:
            if len(row) != 9:
                modified_samplesheet.append(row)
            elif row[5] not in index_library:
                modified_samplesheet.append(row)
            else:
                index_count = 1
                org_row = row[:]
                for index in index_library[row[5]]:
                    row[1] += '_S'
                    row[1] += str(index_count)
                    row[2] += '_S'
                    row[2] += str(index_count)
                    row[5] = index
                    modified_samplesheet.append(row)
                    index_count += 1
                    row = org_row[:]
    return modified_samplesheet

# Write new samplesheet
def write_new_samplesheet(inputfile,outputfile,indexlibrary):
    modified_samplesheet=modify_samplesheet(inputfile,indexlibrary)
    with open(outputfile,mode='w') as new_samplesheet:
        writer=csv.writer(new_samplesheet)
        for row in modified_samplesheet:
            writer.writerow(row)

# Main
def main(argv):
    inputfile = ''
    outputfile = ''
    indexlibrary = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:x:",["ifile=","ofile=","indexlib="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('python main.py -i <inputfile> -o <outputfile> -x <indexlibrary>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
            if outputfile == '':
                outputfile = inputfile
        elif opt in ("-x", "--indexlib"):
            indexlibrary = arg
    write_new_samplesheet(inputfile,outputfile,indexlibrary)
if __name__ == "__main__":
    main(sys.argv[1:])
