import sys
import re
import os

import click
from flowcell_parser.classes import SampleSheetParser

def generate_samplesheet(ss_reader):
    """Will generate a 'clean' samplesheet, : the given fields will be removed. if rename_samples is True, samples prepended with 'Sample_'
    are renamed to match the sample name"""
    output=""
    #Header
    output+="[Header]{}".format(os.linesep)
    for field in ss_reader.header:
        output+="{},{}".format(field.rstrip(), ss_reader.header[field].rstrip())
        output+=os.linesep
    #Data
    output+="[Data]{}".format(os.linesep)
    datafields=[]
    for field in ss_reader.datafields:
        datafields.append(field)
    output+=",".join(datafields)
    output+=os.linesep
    for line in ss_reader.data:
        line_ar=[]
        for field in datafields:
            value = line[field]
            line_ar.append(value)

        output+=",".join(line_ar)
        output+=os.linesep
    return output



def nuc_compliment(nuc):
    if nuc == 'A':
        return 'T'
    elif nuc == 'T':
        return 'A'
    elif nuc == 'C':
        return'G'
    elif nuc == 'G':
        return 'C'
    else:
        sys.exit("Critical error. Unknown nucleotide found: {}.".format(nuc))


if sys.version_info[0] == 3:
    ss_type = (str, str)
elif sys.version_info[0] == 2:
    ss_type = (unicode, unicode)
@click.command()
@click.option('--path', required=True,help='Path to the Samplesheet. E.g. ~/fc/161111_M01320_0095_000000000-AWE6P.csv')
@click.option('--project', required=False,help='Project ID, e.g. P10001. Only the indexes of samples with this specific project ID will be changed')
@click.option('--swap', is_flag=True,help='Swaps index 1 with 2 and vice versa.')
@click.option('--rc1', is_flag=True,help='Exchanges index 1 for its reverse compliment.')
@click.option('--rc2', is_flag=True,help='Exchanges index 2 for its reverse compliment.')
@click.option('--platform', required=True, type=click.Choice(['miseq', 'novaseq', 'nextseq']), help="Run platform ('miseq', 'novaseq', 'nextseq')")

def main(path, project, swap, rc1, rc2, platform):
    ss_reader=SampleSheetParser(path)
    ss_data=ss_reader.data
    single = True

    # Check whether both indexes are available
    index1 = 'index'
    index2 = 'index2'
    if index2 in ss_data[0]:
        single = False

    if single:
        #Sanity check
        if rc2 or swap:
            sys.exit("Single index. Cannot change index 2, nor swap indexes")

        #Reverse compliment
        if rc1:
            for row in ss_data:
                sample_id = row['Sample_ID']
                if (not project) or (project in sample_id):
                    index_in = re.match('([ATCG]{4,12})', row[index1])
                    if index_in:
                        if rc1:
                            rc = ""
                            for nuc in index_in.group(1)[::-1]:
                                rc = rc + nuc_compliment(nuc)
                            row[index1] = '{}'.format(rc)

    if not single:
        #Reverse Compliment
        if rc1 or rc2:
            for row in ss_data:
                sample_id = row['Sample_ID']
                if (not project) or (project in sample_id):
                    if platform == "miseq":
                        if rc1:
                            rc = ""
                            for nuc in row['index'][::-1]:
                                rc = rc + nuc_compliment(nuc)
                            row['index'] = rc
                            row['I7_Index_ID'] = rc
                        if rc2:
                            rc = ""
                            for nuc in row['index2'][::-1]:
                                rc = rc + nuc_compliment(nuc)
                            row['index2'] = rc
                            row['I5_Index_ID'] = rc
                    elif platform == "novaseq" or platform == "nextseq":
                        if rc1:
                            rc = ""
                            for nuc in row['index'][::-1]:
                                rc = rc + nuc_compliment(nuc)
                            row['index'] = rc
                        if rc2:
                            rc = ""
                            for nuc in row['index2'][::-1]:
                                rc = rc + nuc_compliment(nuc)
                            row['index2'] = rc
        #Swap indexes
        if swap:
            for row in ss_data:
                sample_id = row['Sample_ID']
                if (not project) or (project in sample_id):
                    if platform == "miseq":
                        storage = row['index']
                        row['index'] = row['index2']
                        row['I7_Index_ID'] = row['index2']
                        row['index2'] = storage
                        row['I5_Index_ID'] = storage
                    elif platform == "novaseq" or platform == "nextseq":
                        storage = row['index']
                        row['index'] = row['index2']
                        row['index2'] = storage

    redemux_ss = generate_samplesheet(ss_reader)
    if platform == "novaseq" or platform == "nextseq":
        filename = re.search('\/(\w+).csv$', path).group(1)
    else:
        filename = "SampleSheet"

    with open('{}_redemux.csv'.format(filename), 'w') as fh_out:
        fh_out.write(redemux_ss)

if __name__ == '__main__':
    main()
