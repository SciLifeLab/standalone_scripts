#!/usr/bin/env python

#####
# Script to print barcodes for regular NGI Projects
# Originally written by Sverker Lundin
# 2014: Moved to https://github.com/SciLifeLab/scilifelab/ (@alneberg)
# 2020: Moved to https://github.com/SciLifeLab/standalone_scripts (@ewels)
# 2020: Rewritten to work with Python 3 (@ewels)



import sys

def construct(*args, **kwargs):
    start = int(kwargs.get('start'))
    end = int(kwargs.get('end'))
    plates = int(kwargs.get('plates'))

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
    from optparse import OptionParser
    ''' Options '''
    parser = OptionParser(
        description="""Tool for constructing barcode labels for NGI Genomics Projects""",
        usage='-s <start project ID> -e <end project id>',
        version="%prog V0.02 sverker.lundin@scilifelab.se")
    parser.add_option('-s', '--start', type=int,
        help='the starting project ID (numeric, e.g. 123)')
    parser.add_option('-e', '--end', type=int,
        help='the last project ID (numeric, e.g. 234)')
    parser.add_option('-p', '--plates', type=int,
        help='the number of plates (numeric, e.g. 5)')
    return parser

def main():
    parser = getArgs()
    (options, args) = parser.parse_args()
    if not (options.start):
        print (sys.stderr, 'Usage: %s %s' % \
            (parser.get_prog_name(), parser.usage), file=sys.stderr)
        sys.exit()

    if not (options.plates):
        options.plates = 5
    if not (options.end):
        options.end = options.start +1
    if options.start >= options.end:
        print ('end value has to be > start value', file=sys.stderr)
        sys.exit()
    try:
        construct(start=options.start, end=options.end, plates=options.plates)
    except KeyboardInterrupt:
        print ('Interupted!', file=sys.stderr)
        quit()

if __name__ == '__main__':
    main()
