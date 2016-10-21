import sys, os, glob
import argparse
from operator import itemgetter
import subprocess

def init_sample_hash_emtry():
    empty_sample_result = {
        '#Archived_runs' : 0,
        '#Data_runs': 0,
        '#Analysis_runs': 0,
        '#Reads':0,
        'RowCov':0,
        '#AlignedReads':0,
        '%AlignedReads':0,
        'AlignCov':0,
        '%Dup':0,
        'MedianInsertSize':0,
        'GCpercentage':0,
        'Delivered':False
    }
    return empty_sample_result

def find_samples_from_archive(roots, project, samples, stockholm):
    """given a project (e.g. P1775 or OB-0726) finds all samples sequenced for that specif project
    it assumes that we never delete the folder stucture, but only fastq files
    returns an hash with one sample name as key and number of seq runs that contain that sample
     """
    for root in roots:
        for dir in os.listdir(root):
            if "_ST-" in dir:
                #must be an X FC
                run_dir     = os.path.join(root, dir)
                sample_dirs = glob.glob("{}/Demultiplexing/*/Sample_*".format(run_dir))
                for sample in sample_dirs:
                    if stockholm:
                        sample_name = sample.split("/")[-1].replace("Sample_", "")
                        if not sample_name.startswith(project):
                            continue
                    else:#uppsala case
                        current_project = sample.split("/")[-2]
                        if project != current_project:
                            continue
                        sample_name = sample.split("/")[-1].replace("Sample_", "")
                    if not sample_name in samples:
                        samples[sample_name] = init_sample_hash_emtry()
                    archived_runs = len(glob.glob("{}/{}*L0*R1*fastq.gz".format(sample,sample_name)))
                    if archived_runs == 0: #stockholm case
                        sampe_name_hyphen = sample_name.replace("_", "-")
                        archived_runs = len(glob.glob("{}/{}*L00*R1*fastq.gz".format(sample,sampe_name_hyphen)))
                    samples[sample_name]["#Archived_runs"] += archived_runs


def find_sample_from_DATA(root, project, samples ):
    """given a project (e.g. P1775) finds all samples tranfered to DATA folder
    returns an hash with one sample name as key and number of seq runs (or lanes runs)
    """
    if not os.path.exists(os.path.join(root,project)):
        return samples

    for sample in os.listdir(os.path.join(root,project)):
        #DATA/SAMPLE/LIB_PREPS/RUNS
        if sample.startswith("."):
            continue
        sample_data_dir = os.path.join(root, project, sample)
        sample_runs     = glob.glob("{}/*/*/{}*L0*_R1*fastq.gz".format(sample_data_dir,sample)) #if sample splitted in multiple lanes there will be an entry per lane
        if not sample in samples:
            samples[sample] = init_sample_hash_emtry()
        samples[sample]['#Data_runs'] = len(sample_runs)


def find_sample_from_ANALYSIS(root, project, samples):
    """given a project (e.g. P1775) finds all samples in ANALYSIS folder
       returns an hash with one sample name as key and various stats on the sample
       It does this by looking at the bam.out files that is present in the 01_raw_alignments folder
       A sample is counted here if it is found in 01_raw_alignments
    """
    raw_alignments_dir = os.path.join(root, project, "piper_ngi", "01_raw_alignments")
    for sample_run in glob.glob("{}/*.out".format(raw_alignments_dir)):
        sample_run_algn = sample_run.split("/")[-1] # this looks like P1775_102.AH2T7GCCXX.P1775_102.1.bam.out
        sample_name = sample_run_algn.split(".")[0]
        sample_lane = int(sample_run_algn.split(".")[3])
        if not sample_name in samples:
            samples[sample_name] = init_sample_hash_emtry()
        samples[sample_name]['#Analysis_runs']  += 1

    # now check if I can retrive other informaiton about  this sample
    for sample, sample_entry in samples.items():
        genome_results_file = os.path.join(root, project, "piper_ngi", "06_final_alignment_qc",
                                           "{}.clean.dedup.qc".format(sample),
                                           "genome_results.txt")

        if os.path.isfile(genome_results_file) and sample_entry['#Analysis_runs'] == 0:
            sample_entry['#Analysis_runs'] = 1 # at least one is present

        if sample_entry['#Analysis_runs'] > 0:
            #if i have run some analysis on this sample fetch info about sequenced reads and coverage
            picard_duplication_metrics = os.path.join(root, project, "piper_ngi", "05_processed_alignments",
                                               "{}.metrics".format(sample))

            if os.path.isfile(genome_results_file):
                #store informations
                parse_qualimap(genome_results_file, sample_entry)


            if os.path.isfile(picard_duplication_metrics) and sample_entry['#Reads'] > 0:
                # if picard file exists and bamqc has been parsed with success
                parse_bamtools_markdup(picard_duplication_metrics, sample_entry)


def find_sample_from_DELIVERY(root, project, samples):
    """given a project (e.g. P1775) finds all samples in DELIVERED folder
       returns an hash with one sample name as key the key delivered set as true or false
    """
    project_delivery_dir = os.path.join(root,project)
    if not os.path.exists(project_delivery_dir):
        return None

    for sample in os.listdir(project_delivery_dir):
        if os.path.isdir(os.path.join(project_delivery_dir, sample)) and sample != "00-Reports":
            if not sample in samples:
                samples[sample] = init_sample_hash_emtry()
            samples[sample]['Delivered'] = True


def parse_bamtools_markdup(picard_duplication_metrics, sample):
    duplication = 0
    with open(picard_duplication_metrics, 'r') as f:
        for line in f:
            line.strip()
            if line.startswith("## METRICS CLASS"):
                line = f.next() # this is the header
                line = f.next().strip() # thisis the one I am intrested
                duplicate_stats= line.split()
                UNPAIRED_READ_DUPLICATES = int(duplicate_stats[4])
                READ_PAIR_DUPLICATES     = int(duplicate_stats[5])
                PERCENT_DUPLICATION      = float(duplicate_stats[7].replace(",", "."))# some times a comma is used
                sample['%Dup']           = PERCENT_DUPLICATION





def parse_qualimap(genome_results_file, sample):
    reference_size         = 0
    number_of_reads        = 0
    number_of_mapped_reads = 0
    coverage_mapped        = 0
    coverage_raw           = 0
    GCpercentage           = 0
    MedianInsertSize       = 0
    autosomal_cov_length   = 0
    autosomal_cov_bases    = 0

    reference_section = False
    global_section    = False
    coverage_section  = False
    coverage_section  = False
    coverage_per_contig_section = False
    insertSize_section= False
    with open(genome_results_file, 'r') as f:
        for line in f:
            if line.startswith('>>>>>>> Reference'):
                reference_section = True
                continue
            if line.startswith('>>>>>>> Globals'):
                reference_section = False
                global_section    = True
                continue
            if line.startswith('>>>>>>> Insert'):
                global_section    = False
                insertSize_section= True
                continue
            if line.startswith('>>>>>>> Coverage per contig'):
                coverage_section     = False
                coverage_per_contig_section = True
                continue
            if line.startswith('>>>>>>> Coverage'):
                coverage_section   = True
                insertSize_section = False
                continue


            if reference_section:
                line = line.strip()
                if "number of bases" in line:
                    reference_size = int(line.split()[4].replace(",", ""))
                    reference_section = False
            if global_section:
                line = line.strip()
                if "number of reads" in line:
                    number_of_reads = int(line.split()[4].replace(",", ""))
                if "number of mapped reads" in line:
                    number_of_mapped_reads = int(line.split()[5].replace(",", ""))
            if insertSize_section:
                line = line.strip()
                if "median insert size" in line:
                    MedianInsertSize = int(line.split()[4])
            if coverage_section:
                line = line.strip()
                if "mean coverageData" in line:
                    coverage_mapped = float(line.split()[3].replace("X", ""))
            if coverage_per_contig_section:
                line = line.strip()
                if line:
                    sections = line.split()
                    if sections[0].isdigit() and int(sections[0]) <= 22:
                        autosomal_cov_length += float(sections[1])
                        autosomal_cov_bases += float(sections[2])

    sample['#Reads'] = number_of_reads
    sample['RowCov'] = (number_of_reads*150)/float(reference_size)
    sample['#AlignedReads'] = number_of_mapped_reads
    sample['%AlignedReads'] = (float(number_of_mapped_reads)/number_of_reads)*100
    sample['AlignCov'] = coverage_mapped
    sample['MedianInsertSize'] = MedianInsertSize
    sample['AutosomalCoverage'] = autosomal_cov_bases / autosomal_cov_length


def main(args):
    uppmax_id    = args.uppmax_project
    stockholm    = args.stockholm
    raw_data     = "/proj/{}/nobackup/NGI/DATA/".format(uppmax_id)
    analysis_dir = "/proj/{}/nobackup/NGI/ANALYSIS/".format(uppmax_id)
    delivery_dir = "/proj/{}/nobackup/NGI/DELIVERY/".format(uppmax_id)
    archive      = ("/proj/{}/archive/".format(uppmax_id), "/proj/{}/incoming/".format(uppmax_id))
    samples      = {}


    projects     = args.projects #[item for sublist in args.projects for item in sublist]

    if args.project_status:
        if len(projects) != 1:
            print "WARNING: only one project when project-status specified\n"
            return

    for project in projects:
        #find all samples sequenced for a project present in archive -- this assumes that fastq files will be deleted but not the folder structure
        find_samples_from_archive(archive, project, samples, stockholm)
        #now find samples that are stored in DATA
        find_sample_from_DATA(raw_data, project, samples)
        find_sample_from_ANALYSIS(analysis_dir, project, samples)
        find_sample_from_DELIVERY(delivery_dir, project, samples)

    if args.project_status:
        sequenced_samples = 0
        delivered_samples = 0
        print "SAMPLE\tARCHIVE_SEQ_RUN\tDATA_SEQ_RUN\tANALYSIS_SEQ_RUN"
        for sample, sample_entry in samples.items():
            sequenced_samples +=1
            if  sample_entry['Delivered']:
                delivered_samples += 1
            print "{}\t{}\t{}\t{}".format(
                sample,
                sample_entry['#Archived_runs'],
                sample_entry['#Data_runs'],
                sample_entry['#Analysis_runs']
            )
        print "PROJECT SUMMARY:"
        print "  SAMPLES_SEQUENCED: {}".format(sequenced_samples)
        print "  SAMPLES_DELIVERED: {}".format(delivered_samples)


    else:
        for sample, sample_entry in samples.items():
            skip_print = 0;
            if sample_entry['#Archived_runs'] != sample_entry['#Data_runs']:
                skip_print = 0
            if sample_entry['#Archived_runs'] != sample_entry['#Analysis_runs']:
                skip_print = 0
            if sample_entry['#Analysis_runs'] == 0:
                skip_print = 1 # no problem here as might have demux runs

            if skip_print == 1:
                print "WARNING: Sample {} has incoherent numbers of runs: ({} {} {})".format(sample,
                                                sample_entry['#Archived_runs'],
                                                sample_entry['#Data_runs'],
                                                sample_entry['#Analysis_runs']
                                                )
            samples[sample]["skip print"] = skip_print

        if not args.skip_header:
            print "sample_name\t#Reads\tRaw_coverage\t#Aligned_reads\t%Aligned_reads\tAlign_cov\tAutosomalCoverage\t%Dup\tMedianInsertSize"

        for sample, sample_entry in samples.items():
            if sample_entry["skip print"] == 0:
                print "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
                    sample,
                    sample_entry['#Reads'],
                    sample_entry['RowCov'],
                    sample_entry['#AlignedReads'],
                    sample_entry['%AlignedReads'],
                    sample_entry['AlignCov'],
                    sample_entry['AutosomalCoverage'],
                    sample_entry['%Dup'],
                    sample_entry['MedianInsertSize']
                )

def get_low_coverage(project):
    # run same script again, to parse the output
    # because all the functions above don't return anything, but just print the result
    script_path = os.path.realpath(__file__)
    command = """python {} {} | awk -v x=28.5 '$7<x' | awk '{{ print $1 " " $7}}' """.format(script_path, project)
    try:
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    except Exception, e:
        print "Command failed: {}".format(command)
        raise e
    output = p.communicate()[0]

    result = {}
    for line in output.split('\n'):
        try:
            sample, coverage = line.split()
        except:
            continue
        else:
            result[sample] = coverage
    return result

def get_project_status(project):
    # run same script again, to parse the output
    # because all the functions above don't return anything, but just print the result
    script_path = os.path.realpath(__file__)
    command = """python {} {} --project-status --skip-header""".format(script_path, project)
    try:
        p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception, e:
        print "Command failed: {}".format(command)
        raise e
    output, error = p.communicate()
    if error:
        print error
        exit(1)

    result = {
        'sequenced': [],
        'resequenced': [],
        'organized': [],
        'not_organized': [],
        'analyzed': [],
        'to_analyze': [],
    }

    for line in output.split('\n'):
        try:
            sample, sequenced, organized, analysed = line.split()
            if project not in sample:
                continue
        except Exception,e: continue
        result['sequenced'].append(sample)
        if sequenced > '1':
            result['resequenced'].append(sample)
        if organized != '0':
            result['organized'].append(sample)
        if organized < sequenced:
            result['not_organized'].append(sample)
        if analysed != '0':
            result['analyzed'].append(sample)
        if analysed < sequenced:
            result['to_analyze'].append(sample)
    return result

def get_samples_with_undetermined(data_dir, project):
    """ get all fastq_files from DATA directory
        check which ones named 'Undetermined'
        then add sample and flowcell to the list
    """
    result = {}
    # get list of fastq_files in DATA directory
    project_path = os.path.join(data_dir, project, '*/*/*/*.fastq*')
    fastq_files = glob.glob(project_path)
    for file_path in fastq_files:
        # check which files are named 'Undetermined'
        filename = os.path.basename(file_path)
        if 'Undetermined' in filename:
            # get sample and flowcell id
            sample = filename.split('_Undetermined')[0]
            flowcell = file_path.split('/')[-2]
            # update result list
            if sample not in result:
                result[sample] = [flowcell]
            elif flowcell not in result[sample]:
                result[sample].append(flowcell)
    return result

def get_samples_under_analysis(project):
    result = []
    command = "jobinfo | grep piper | grep {}".format(project)
    try:
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
    except Exception, e:
        print "Cannot execute command: {}".format(command)
        raise e

    output = output[0].split('\n')
    for line in output:
        # skip empty lines
        if line.strip() != '':
            sample = line.split('piper_{}-'.format(project))[-1].split('-')[0]
            if sample not in result:
                result.append(sample)
    return result

def get_samples_under_qc(project):
    result = []
    command = "jobinfo | grep qc_{}".format(project)
    try:
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
    except Exception, e:
        print "Cannot execute command {}".format(command)
        raise e
    output = output[0].split('\n')
    for line in output:
        # skip empty lines
        if line.strip() != '':
            sample = line.split('qc_{}-'.format(project))[-1]
            if sample not in result:
                result.append(sample)
    return result

def get_samples_with_failed_analysis(project, analysis_dir):
    log_path = os.path.join(analysis_dir, '{}/piper_ngi/logs/{}-*.exit'.format(project, project))
    under_analysis = get_samples_under_analysis(project)
    exit_files = glob.glob(log_path)
    result = {}
    for path in exit_files:
        with open(path, 'r') as exit_file:
            exit_code = exit_file.read().strip()
            # P4603-P4603_189-merge_process_variantcall.exit
            sample = os.path.basename(path).replace('{}-'.format(project), '').split('-')[0]
            if exit_code == '' and sample not in under_analysis:
                if sample in result:
                    result[sample] = [result[sample]]
                    result[sample].append('Empty exit code, but sample is not under analysis')
                else:
                    result[sample] = 'Empty exit code, but sample is not under analysis'
            elif exit_code.strip() != '0' and exit_code != '':
                if sample in result:
                    result[sample] = [result[sample]]
                    result[sample].append('Exit code: {}'.format(exit_code))
                else:
                    result[sample] = 'Exit code: {}'.format(exit_code)
    return result

def get_incoherent_samples(project):

    # run same script again, to parse the output
    # because all the functions above don't return anything, but just print the result
    script_path = os.path.realpath(__file__)
    command = """python {} {} --project-status --skip-header""".format(script_path, project)
    try:
        p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception, e:
        print "Command failed: {}".format(command)
        raise e
    output, error = p.communicate()
    if error:
        print error
        exit(1)
    result = {}
    for line in output.split('\n'):
        try:
            sample, sequenced, organized, analyzed = line.split()
        except Exception, e:
            continue
        # skipping header
        if sample == "SAMPLE":
            continue
        if sequenced != organized or organized != analyzed:
            result[sample] = [sequenced, organized, analyzed]
    return result



if __name__ == '__main__':
    parser = argparse.ArgumentParser("""Process one or more project and report basic statistiscs for it """)
    parser.add_argument('projects', metavar='project', type=str, nargs='+', help='Projects we want to have statistics for, in stockholm case P1000, uppsala NK-0191')
    parser.add_argument('--uppmax-project', help="uppmax project where analysis have been run", type=str, default='ngi2016003')
    parser.add_argument('--project-status', help="reports number of samples, of samples-runs, analysed samples and delivered samples (work only if a single project is specified)", action='store_true')

    parser.add_argument('--skip-header', help="skip header", action='store_true')
    parser.add_argument('--stockholm', help="assume stocholm project format, otherwise uppsala", action='store_true', default=True)

    # added by Kate
    parser.add_argument('--sequenced', help="List of all the sequenced samples", action="store_true")
    parser.add_argument('--low-coverage', help="List of analyzed samples with coverage below 28.5X", action="store_true")
    parser.add_argument('--organized', help="List of all the organized flowcells", action="store_true")
    parser.add_argument('--not-organized', help="List of all the not-organized flowcells", action="store_true")
    parser.add_argument('--analyzed', help="List of all the analysed samples", action="store_true")
    parser.add_argument('--resequenced', help="List of samples that have been sequenced more than once", action="store_true")
    parser.add_argument('--undetermined', help="List of the samples which use undetermined", action="store_true")
    parser.add_argument('--under-analysis', help="List of the samples under analysis", action="store_true")
    parser.add_argument('--to-analyze', help="List of samples that are ready to be analyzed", action="store_true")
    parser.add_argument('--under-qc', help="List of samples under qc. Use only for projects without BP", action="store_true")
    parser.add_argument('--analysis-failed', help="List of all the samples with failed analysis", action="store_true")
    parser.add_argument('--short', help="Project-status but only for samples which have incoherent number of sequenced/organized/analyzed", action="store_true")

    # todo
    parser.add_argument('--all-samples', help="List of all the samples of project (sequenced and not sequenced). Not implemented yet", action="store_true")
    parser.add_argument('--not-sequenced', help="List of the samples that are not sequenced AT ALL on ANY flowcells or lanes. Not implemented yet", action="store_true")
    parser.add_argument('--qc-done', help="List of samples with completed QC. Not implemented yet", action="store_true")
    parser.add_argument('--sample', '-s', help="Statistics for the specified sample. Not implemented yet", type=str)

    args = parser.parse_args()
    if not args.projects:
        print "ERROR: projects must be specified"
        sys.exit()

    # parse arguments
    project = args.projects[0]
    uppmax_id = args.uppmax_project
    data_dir = "/proj/{}/nobackup/NGI/DATA/".format(uppmax_id)
    analysis_dir = "/proj/{}/nobackup/NGI/ANALYSIS/".format(uppmax_id)

    # output the result
    if args.low_coverage:
        samples = get_low_coverage(project)
        if samples:
            if not args.skip_header:
                print "Coverage below 28.5X:"
            for sample in sorted(samples.keys()):
                print "{}   {}".format(sample, samples[sample])
        else:
            print 'All samples are above 28.5X'

    elif args.resequenced:
        samples = get_project_status(project)
        if samples['resequenced']:
            if not args.skip_header:
                print 'Resequenced samples:' # todo: add flowcells
            for sample in sorted(samples['resequenced']):
                print sample
        else:
            print 'No resequenced samples'

    elif args.sequenced:
        samples = get_project_status(project)
        if samples['sequenced']:
            if not args.skip_header:
                print 'Sequenced samples:'
            for sample in sorted(samples['sequenced']):
                print sample
        else:
            print 'No sequenced samples'

    elif args.organized:
        samples = get_project_status(project)
        if samples['organized']:
            if not args.skip_header:
                print 'Organized samples:'
            for sample in sorted(samples['organized']):
                print sample
        else:
            print 'No organized samples'

    elif args.not_organized:
        samples = get_project_status(project)
        if samples['not_organized']:
            if not args.skip_header:
                print 'Not organized samples:'
            for sample in sorted(samples['not_organized']):
                print sample # todo: add flowcell list
        else:
            print 'All samples organized'

    elif args.analyzed:
        samples = get_project_status(project)
        if samples['analyzed']:
            if not args.skip_header:
                print 'Analyzed samples:'
            for sample in sorted(samples['analyzed']):
                print sample
        else:
            print 'No analyzed samples'

    elif args.undetermined:
        samples = get_project_status(project)
        result = get_samples_with_undetermined(data_dir, project)
        if result:
            if not args.skip_header:
                print 'Organized with undetermined:'
            for sample in sorted(result.keys()):
                print "{}: {}".format(sample, ", ".join(fc for fc in result[sample]))
        else:
            print 'No undetermined used'

    elif args.under_analysis:
        result = get_samples_under_analysis(project)
        if result:
            if not args.skip_header:
                print 'Samples under analysis:'
            for sample in sorted(result):
                print sample
        else:
            'No samples are being analyzed'

    elif args.to_analyze:
        samples = get_project_status(project)
        to_analyze = samples['to_analyze']
        if to_analyze:
            print 'something'
            if not args.skip_header:
                print 'Samples ready to be analyzed:'
            for sample in sorted(to_analyze):
                print sample
        else:
            print 'No samples ready to be analyzed'

    elif args.all_samples:
        print "--all-samples has not been implemented yet"

    elif args.not_sequenced:
        print "--not-sequenced has not been implemented yet"

    elif args.under_qc:
        result = get_samples_under_qc(project)
        if result:
            if not args.skip_header:
                print 'Samples under QC:'
            for sample in sorted(result):
                print sample
        else:
            print 'No samples under QC'

    elif args.analysis_failed:
        result = get_samples_with_failed_analysis(project, analysis_dir)
        if result:
            if not args.skip_header:
                print 'Samples with failed analysis:'
            for sample in sorted(result):
                print sample, result[sample]
        else:
            print 'No analysis failed'

    elif args.short:
        result = get_incoherent_samples(project)
        if result:
            if not args.skip_header:
                print "Samples with incoherent runs:"
            for sample in sorted(result.keys()):
                numbers = result[sample]
                print "{}\t{}\t{}\t{}".format(sample, numbers[0], numbers[1], numbers[2])
        else:
            print "No samples with incoherent runs"

    else:
        main(args)