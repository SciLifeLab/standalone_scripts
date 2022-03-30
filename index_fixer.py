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
@click.option('--swap', is_flag=True,help='Swaps index 1 with 2 and vice versa.')
@click.option('--rc1', is_flag=True,help='Exchanges index 1 for its reverse compliment.')
@click.option('--rc2', is_flag=True,help='Exchanges index 2 for its reverse compliment.')
@click.option('--platform', required=True, type=click.Choice(['hiseq', 'miseq', 'hiseqx']), help="Run platform ('hiseq', 'miseq', 'hiseqx')")
@click.option('--ss', multiple=True, type=ss_type, help='Swap index between sample pairs. Use one --ss per pair.')

def main(path, swap, rc1, rc2, platform, ss):
    ss_reader=SampleSheetParser(path)
    ss_data=ss_reader.data
    single = True

    if platform == "hiseq":
        index1 = 'Index'
        if re.search('[-+]', (ss_data[0][index1])):
            single = False

    elif platform == "miseq":
        index1 = 'index'
        index2 = 'index2'
        if index2 in ss_data[0]:
            single = False

    elif platform == "hiseqx":
        index1 = 'index1'
        index2 = 'index2'
        single = False

    if single:
        #Sanity check
        if rc2 or swap:
            sys.exit("Single index. Cannot change index 2, nor swap indexes")

        #Reverse compliment
        if rc1:
            for row in ss_data:
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
                if platform == "hiseq":
                    index_in = re.match('([ATCG]{4,12})[-+]([ATCG]{4,12})', row[index1])
                    if rc1:
                        rc = ""
                        for nuc in index_in.group(1)[::-1]:
                            rc = rc + nuc_compliment(nuc)
                        row[index1] = '{}-{}'.format(rc, index_in.group(2))
                    if rc2:
                        rc = ""
                        for nuc in index_in.group(2)[::-1]:
                            rc = rc + nuc_compliment(nuc)
                        row[index1] = '{}-{}'.format(index_in.group(1), rc)

                elif platform == "miseq" or platform == "hiseqx":
                    if rc1:
                        rc = ""
                        for nuc in row['index1'][::-1]:
                            rc = rc + nuc_compliment(nuc)
                        row['index1'] = rc
                    if rc2:
                        rc = ""
                        for nuc in row['index2'][::-1]:
                            rc = rc + nuc_compliment(nuc)
                        row['index2'] = rc
        #Swap indexes
        if swap:
            for row in ss_data:
                if platform == "hiseq":
                    index_in = re.match('([ATCG]{4,12})[-+]([ATCG]{4,12})', row[index1])
                    row[index1] = '{}-{}'.format(index_in.group(2), index_in.group(1))

                elif platform == "miseq" or platform == "hiseqx":
                    storage = row['index1']
                    row['index1'] = row['index2']
                    row['index2'] = storage

    #Rearrange samples
    if ss:
        #Need to catch all samples in a list prior to writing, then dump them in corrected order
        sys.exit("Sample Swap isn't implemented yet.")

    #redemux_ss = ss_reader.generate_clean_samplesheet()
    redemux_ss = generate_samplesheet(ss_reader)
    if platform == "hiseq" or platform == "hiseqx":
        filename = re.search('\/(\w+).csv$', path).group(1)
    else:
        filename = "SampleSheet"

    with open('{}_redemux.csv'.format(filename), 'w') as fh_out:
        fh_out.write(redemux_ss)

if __name__ == '__main__':
    main()
