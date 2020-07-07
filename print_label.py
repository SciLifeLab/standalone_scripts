#!/usr/bin/env python

#####
# Script to print barcodes for regular NGI Projects
# Originally written by Sverker Lundin
# 2014: Moved to https://github.com/SciLifeLab/scilifelab/ (@alneberg)
# 2020: Moved to https://github.com/SciLifeLab/standalone_scripts (@ewels)
# 2020: Rewritten to work with Python 3 (@ewels)

import argparse

def construct(start, end, plates):
    start = int(start)
    end = int(end)
    plates = int(plates)
    for i in range(start, end):
        PID = "P"+str(i)
        makeProjectBarcode(PID,plates)

def makeProjectBarcode(PID,plates):
    print ("^XA") # start of label
    print ("^DFFORMAT^FS") # download and store format, name of format, end of field data (FS = field stop)
    print ("^LH0,0") # label home position (label home = LH)
    print ("^FO360,30^AFN,60,20^FN1^FS") # AF = assign font F, field number 1 (FN1), print text at position field origin (FO) rel. to home
    print ("^FO80,10^BCN,70,N,N^FN2^FS") # BC=barcode 128, field number 2, Normal orientation, height 70, no interpreation line.
    print ("^XZ") # end format

    for i in range (1,plates +1):
        PlateID = "P"+str(i)
        plateCode = PID+PlateID
        print ("^XA") #start of label format
        print ("^XFFORMAT^FS") #label home posision
        print ("^FN1^FD"+plateCode+"^FS") #this is readable
        print ("^FN2^FD"+plateCode+"^FS") #this is the barcode
        print ("^XZ")

def getArgs():
    ''' Options '''
    parser = argparse.ArgumentParser(
        description = "Tool for constructing barcode labels for NGI Genomics Projects",
        usage = '--start <start project ID> --end <end project id>'
    )
    parser.add_argument(
        '-s', '--start',
        type = int,
        help = 'the starting project ID (numeric, e.g. 123)'
    )
    parser.add_argument(
        '-e', '--end',
        type = int,
        help = 'the last project ID (numeric, default --start + 1)'
    )
    parser.add_argument(
        '-p', '--plates',
        type = int,
        default = 5,
        help = 'the number of plates (numeric, default 5)'
    )
    return parser

def main():
    parser = getArgs()
    args = vars(parser.parse_args())
    if not args['start']:
        parser.error('--start is required')
    if not args['end']:
        args['end'] = args['start'] +1
    if args['start'] >= args['end']:
        parser.error('End value has to be > start value')
    try:
        construct(args['start'], args['end'], args['plates'])
    except KeyboardInterrupt:
        parser.error('Interrupted!')

if __name__ == '__main__':
    main()
