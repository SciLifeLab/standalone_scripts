import sys
import os
import re
import subprocess
import shutil

import click
import yaml
import vcf
import pyexcel_xlsx

from ngi_pipeline.database.classes import CharonSession, CharonError


CONFIG = "/lupus/ngi/production/v1.5/conf//irma_ngi_config_sthlm.yaml"

@click.group()
@click.pass_context
@click.option('--config', '-c', default=CONFIG, help='Path to a config file', required=True, type=click.Path())
def cli(context, config):
    if config == CONFIG:
        click.echo("Using default config file: {}".format(CONFIG))
    else:
        click.echo('Config file: {}'.format(os.path.abspath(config)))
    if not os.path.exists(config):
        click.echo('Config file does not exist!')
        exit(1)
    with open(config, 'r') as config_file:
        config = yaml.load(config_file) or {}
    context.obj = config

@cli.command()
@click.pass_context
def parse_xl_files(context):
    config = context.obj
    # checking config
    XL_FILES_PATH = config.get('XL_FILES_PATH')
    if XL_FILES_PATH is None:
        click.echo("config file missing XL_FILES_PATH argument")
        click.echo('Do you want to enter path to excel files? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter path to excel files')
            XL_FILES_PATH = raw_input()
        else:
            click.echo('No XL_FILES_PATH given. Terminating')
            exit(0)
    if not os.path.exists(XL_FILES_PATH):
        click.echo("Path to excel files does not exist! Path: {}".format(XL_FILES_PATH))
        click.echo('Terminating')
        exit(0)
    #
    ANALYSIS_PATH = config.get('ANALYSIS_PATH')
    if ANALYSIS_PATH is None:
        click.echo("config file missing ANALYSIS_PATH")
        click.echo('Do you want to enter analysis path? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter the output path')
            ANALYSIS_PATH = raw_input()
        else:
            exit(0)
    if not os.path.exists(ANALYSIS_PATH):
        click.echo('Analysis path does not exist! Path: {}'.format(ANALYSIS_PATH))
        click.echo('Terminating')
        exit(0)

    XL_FILES_ARCHIVED = config.get('XL_FILES_ARCHIVED')
    if XL_FILES_ARCHIVED is None:
        click.echo('config file missing XL_FILES_ARCHIVED')
        click.echo('Do you want to enter path where to archive excel files? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            XL_FILES_ARCHIVED = raw_input()
        else:
            click.echo('Please enter where to archive excel files')
            XL_FILES_ARCHIVED = raw_input()
    if not os.path.exists(XL_FILES_ARCHIVED):
        click.echo('Path does not exist! Path: {}'.format(XL_FILES_ARCHIVED))
        click.echo('Terminating')
        exit(0)

    click.echo('Looking for .xlsx files in {}'.format(XL_FILES_PATH))
    xl_files = [os.path.join(XL_FILES_PATH, filename) for filename in os.listdir(XL_FILES_PATH) if '.xlsx' in filename]
    if not xl_files:
        click.echo('No .xlsx files found! Terminating')
        exit(0)

    click.echo('Following files are found: {}'.format(', '.join([os.path.basename(xl_file) for xl_file in xl_files])))

    # parsing snps file
    snps_data = parse_maf_snps_file(config)

    # parsing xl files
    genotype_data = {}
    for xl_file in xl_files:
        click.echo("Parsing file: {}".format(os.path.basename(xl_file)))
        data = pyexcel_xlsx.get_data(xl_file)
        data = data.get('HaploView_ped_0') # sheet name
        # getting list of lists
        header = data[0]
        data = data[1:]
        for row in data:
            # row[1] is always sample name. If doesn't match NGI format - skip.
            if not re.match(r"^ID\d+-P\d+_\d+", row[1]):
                continue
            # sample has format ID22-P2655_176
            sample_id = row[1].split('-')[-1]
            # if the same sample occurs twice in the same file, will be overwriten
            if sample_id not in genotype_data:
                genotype_data[sample_id] = {}
            else:
                click.echo('Sample {} has been already parsed from another (or this) file. Overwriting'.format(sample_id))
            # rs positions start from 9 element. hopefully the format won't change
            for rs_id in header[9:]:
                rs_index = header.index(rs_id)
                allele1, allele2 = row[rs_index].split()
                genotype_data[sample_id][rs_id] = [allele1, allele2]

    output_files = []
    for sample_id in genotype_data:
        project_id = sample_id.split('_')[0]
        # create .gt file for each sample
        output_path = os.path.join(ANALYSIS_PATH, project_id, 'piper_ngi/03_genotype_concordance')
        if not os.path.exists(output_path):
            click.echo('Output path does not exist! Path will be created: {}'.format(output_path))
            os.makedirs(output_path)
        output_dir = os.path.join(output_path, sample_id)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        filename = os.path.join(output_dir, '{}.maf.gt'.format(sample_id))
        if os.path.exists(filename):
            click.echo('File {} already exists. Overwriting'.format(filename))
        with open(filename, 'w+') as output_file:
            for rs, alleles in genotype_data[sample_id].items():
                # checking if snps_data contains such position:
                if rs not in snps_data:
                    click.echo('rs_position {} not found in snps_file!!'.format(rs))
                    click.echo('Skipping')
                    continue
                chromosome, position, rs_position, reference, alternative = snps_data.get(rs)
                output_file.write("{} {} {} {} {} {} {}\n".format(chromosome, position, rs, reference, alternative, alleles[0], alleles[1]))
            output_files.append(filename)
    click.echo('{} files have been created in {}/<project>/piper_ngi/03_genotype_concordance'.format(len(output_files), ANALYSIS_PATH))

    # Updating Charon
    for sample in genotype_data:
        update_charon(sample=sample, status='AVAILABLE')

    # archiving files
    archived = []
    for xl_file in xl_files:
        try:
            shutil.move(xl_file, XL_FILES_ARCHIVED)
        except Exception, e:
            click.echo('Cannot move file {} to {}'.format(xl_file, XL_FILES_ARCHIVED))
            click.echo(str(e))
        else:
            archived.append(xl_file)
    click.echo('{} .xlsx files have been archived in {}'.format(len(archived), XL_FILES_ARCHIVED))


def parse_maf_snps_file(config):
    SNPS_FILE = config.get('SNPS_FILE')
    if SNPS_FILE is None:
        click.echo('config file missing SNPS_FILE')
        click.echo('Do you want to enter path to snps file? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter the path to snps file')
            SNPS_FILE = raw_input()
        else:
            exit(0)
    if not os.path.exists(SNPS_FILE):
        click.echo('SNPS file does not exist! Path: {}'.format(SNPS_FILE))
        click.echo('Terminating')
        exit(0)
    snps_data = {}
    with open(SNPS_FILE) as snps_file:
        lines = snps_file.readlines()
        for line in lines:
            chromosome, position, rs_position, reference, alternative = line.split()
            snps_data[rs_position] = [chromosome, position, rs_position, reference, alternative]
    return snps_data


@cli.command()
@click.argument('sample')
@click.pass_context
def genotype_sample(context, sample):
    # check that config file contains all required parameters
    if is_config_file_ok():
        # if check fails, will throw a warning and terminate
        # otherwise - continue here
        config = context.obj
        project = sample.split('_')[0]
        output_path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance')

        # check if gt file exists
        gt_file = os.path.join(output_path, '{}.gt'.format(sample))
        if not os.path.exists(gt_file):
            click.echo('gt file does not exist! Path: {}'.format(gt_file))
            click.echo('To create .gt file run the command: gt_concordance parse_xl_files')
            exit(0)

        # create output path if not exist
        if not os.path.exists(output_path):
            click.echo('Output path does not exist! Will be created. Path: {}'.format(output_path))
            os.makedirs(output_path)

        # check if gatk needs to be run
        vcf_file = os.path.join(output_path, "{}.vcf".format(sample))
        to_run_gatk = False
        if os.path.exists(vcf_file):
            click.echo('.vcf file already exists: {}'.format(vcf_file))
            click.echo('overwrite? Y/n')
            overwrite = raw_input()
            if overwrite.lower() == 'y':
                to_run_gatk = True
        else:
            to_run_gatk = True
        # run gatk if needed
        if to_run_gatk:
            vcf_file = run_gatk(sample, config)
            if vcf_file is None:
                click.echo('GATK completed with ERROR!')
                click.echo('Terminating')
                # update charon
                update_charon(sample=sample, status='FAILED')
                exit(0)

        # check concordance
        vcf_data = parse_vcf_file(sample, config)
        gt_data = parse_gt_file(sample, config)
        if len(vcf_data) != len(gt_data):
            click.echo('VCF file and GT file contain differenct number of positions!! ({}, {})'.format(len(vcf_data), len(gt_data)))
        concordance = check_concordance(sample, vcf_data, gt_data, config)

        # output the results
        click.echo('Files created:')
        click.echo(' Concordance results: {}'.format(os.path.join(output_path, '{}.conc'.format(sample))))
        click.echo(' Header: {}'.format(os.path.join(output_path, '{}.conc.header'.format(sample))))

        click.echo('Concordance: {} %'.format(concordance))
        # update Charon
        update_charon(sample, 'DONE', concordance)

@click.pass_context
def is_config_file_ok(context):
    config = context.obj
    # check that required variables are present in config file
    ANALYSIS_PATH = config.get('ANALYSIS_PATH')
    if ANALYSIS_PATH is None:
        click.echo("config file missing ANALYSIS_PATH")
        click.echo('Do you want to enter analysis path? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter the analysis path')
            ANALYSIS_PATH = raw_input()
        else:
            exit(0)
    if not os.path.exists(ANALYSIS_PATH):
        click.echo('Analysis path does not exist! Path: {}'.format(ANALYSIS_PATH))
        click.echo('Terminating')
        exit(0)
    config.update({'ANALYSIS_PATH': ANALYSIS_PATH})

    GATK_PATH = config.get('GATK_PATH')
    if GATK_PATH is None:
        click.echo("config file missing GATK_PATH")
        click.echo('Do you want to enter path to GATK? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter path to GATK')
            GATK_PATH = raw_input()
        else:
            exit(0)
    if not os.path.exists(GATK_PATH):
        click.echo('GATK file does not exist! Path: {}'.format(GATK_PATH))
        click.echo('Terminating')
        exit(0)
    config.update({'GATK_PATH': GATK_PATH})

    GATK_REF_FILE = config.get('GATK_REF_FILE')
    if GATK_REF_FILE is None:
        click.echo("config file missing GATK_REF_FILE")
        click.echo('Do you want to enter path to reference file? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter path to reference file')
            GATK_REF_FILE = raw_input()
        else:
            exit(0)
    if not os.path.exists(GATK_REF_FILE):
        click.echo('Reference file does not exist! Path: {}'.format(GATK_REF_FILE))
        click.echo('Terminating')
        exit(0)
    config.update({'GATK_REF_FILE': GATK_REF_FILE})

    GATK_VAR_FILE = config.get('GATK_VAR_FILE')
    if GATK_VAR_FILE is None:
        click.echo("config file missing GATK_VAR_FILE")
        click.echo('Do you want to enter path to GATK variant file? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter the path to GATK variant file')
            GATK_VAR_FILE = raw_input()
        else:
            exit(0)
    if not os.path.exists(GATK_VAR_FILE):
        click.echo('GATK variant file does not exist! Path: {}'.format(GATK_VAR_FILE))
        click.echo('Terminating')
        exit(0)
    config.update({'GATK_VAR_FILE': GATK_VAR_FILE})

    INTERVAL_FILE = config.get('INTERVAL_FILE')
    if INTERVAL_FILE is None:
        click.echo('config file missing INTERVAL_FILE')
        click.echo('Do you want to enter path to interval file? Y/n')
        to_enter = raw_input()
        if to_enter.lower() == 'y':
            click.echo('Please enter the path to interval file')
            INTERVAL_FILE = raw_input()
        else:
            exit(0)
    if not os.path.exists(INTERVAL_FILE):
        click.echo('Interval file does not exist! Path: {}'.format(INTERVAL_FILE))
        click.echo('Terminating')
        exit(0)
    config.update({'INTERVAL_FILE': INTERVAL_FILE})

    return True

def parse_vcf_file(sample, config):
    project = sample.split('_')[0]
    path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance', '{}.vcf'.format(sample))
    vcf_data = {}
    vcf_file = vcf.Reader(open(path, 'r'))
    for record in vcf_file:
        reference = str(record.REF[0])
        alternative = str(record.ALT[0])
        chromosome = str(record.CHROM)
        position = str(record.POS)

        genotype = str(record.genotype(sample)['GT'])
        a1, a2 = genotype.split('/')
        # 0 means no variant (using reference), 1 means variant (using alternative)
        a1 = reference if a1.strip() == '0' else alternative
        a2 = reference if a2.strip() == '0' else alternative
        vcf_data['{} {}'.format(chromosome, position)] = {
            'chromosome': chromosome,
            'position': position,
            'a1': a1,
            'a2': a2 }
    return vcf_data

def parse_gt_file(sample, config):
    project = sample.split('_')[0]
    path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance', '{}.gt'.format(sample))
    gt_data = {}
    with open(path, 'r') as gt_file:
        lines = gt_file.readlines()
        for line in lines:
            chromosome, position, rs_position, reference, alternative, a1, a2 = line.strip().split()
            a1 = reference if a1.strip() == '0' else alternative
            a2 = reference if a2.strip() == '0' else alternative
            gt_data['{} {}'.format(chromosome, position)] = {
                'chromosome': chromosome,
                'position': position,
                'a1': a1,
                'a2': a2 }

    return gt_data

def check_concordance(sample, vcf_data, gt_data, config):
    project = sample.split('_')[0]

    result = ''
    header = 'Sample Chrom Pos A1_seq A2_seq A1_maf A2_maf'

    matches = []
    mismatches = []
    snps_number = 0
    lost = 0
    for chromosome_position in vcf_data.keys():
        chromosome, position = chromosome_position.split()
        vcf_a1 = vcf_data[chromosome_position]['a1']
        vcf_a2 = vcf_data[chromosome_position]['a2']


        gt_a1 = gt_data[chromosome_position]['a1']
        gt_a2 = gt_data[chromosome_position]['a2']
        concordance = set([gt_a1, gt_a2]) == set([vcf_a1, vcf_a2])
        if concordance:
            matches.append('{} {} {} {} {} {} {}'.format(sample, chromosome, position, vcf_a1, vcf_a2, gt_a1, gt_a2))
            snps_number += 1
        else:
            mismatches.append('{} {} {} {} {} {} {}'.format(sample, chromosome, position, vcf_a1, vcf_a2, gt_a1, gt_a2))
            if gt_a1 != '0' and gt_a2 != '0':
                snps_number += 1
            else:
                lost += 1
    # recording results
    result = 'Matches:\n'
    result += '\n'.join(matches)
    result += '\nMismatches:\n'
    result += '\n'.join(mismatches)
    percent_matches=(float(len(matches))/float(snps_number))*100
    result += '\n'
    result += '\nLost snps: {}\n'.format(lost)
    result += 'Total number of matches: {} / {}\n'.format(len(matches), snps_number)
    result += 'Percent matches {}%\n'.format(percent_matches)

    output_path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance')
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    with open(os.path.join(output_path, '{}.conc'.format(sample)), 'w+') as conc_file:
        conc_file.write(result)
    with open(os.path.join(output_path, '{}.conc.header'.format(sample)), 'w+') as header_file:
        header_file.write(header)

    if len(matches) + len(mismatches) != len(vcf_data):
        click.echo('CHECK RESULTS!! Numbers are incoherent. Total number: {}, matches: {}, mismatches: {}'.format(len(vcf_data), len(matches), len(mismatches)))

    return percent_matches


def run_gatk(sample, config):
    project = sample.split('_')[0]
    bamfile = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/05_processed_alignments/{}.clean.dedup.bam'.format(sample))
    if not os.path.exists(bamfile):
        click.echo('bamfile does not exist! {}'.format(bamfile))
        return None
    project = sample.split('_')[0]
    # the path has been already checked
    output_file = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance', "{sample}.vcf".format(sample=sample))
    options = """-T UnifiedGenotyper  -I {bamfile} -R {gatk_ref_file} -o {sample}  -D {gatk_var_file} -L {interval_file} -out_mode EMIT_ALL_SITES """.format(
            bamfile=bamfile,
            sample=output_file,
            interval_file=config.get('INTERVAL_FILE'),
            gatk_ref_file=config.get('GATK_REF_FILE'),
            gatk_var_file=config.get('GATK_VAR_FILE'))
    full_command = 'java -Xmx6g -jar {} {}'.format(config.get('GATK_PATH'), options)
    try:
        subprocess.call(full_command.split())
    except:
        return None
    else:
        return output_file

def update_charon(sample, status, concordance=None):
    charon_session = CharonSession()
    project_id = sample.split('_')[0]
    try:
        charon_session.sample_update(projectid=project_id, sampleid=sample,genotype_status=status, genotype_concordance=concordance)
    except CharonError as e:
        print("Problem updating Charon: error says '{}'".format(e))
        exit()



@cli.command()
@click.argument('project')
@click.option('--force', '-f', is_flag=True, default=False, help='If not specified, will keep existing vcf files and use them to check concordance. Otherwise overwrite')
@click.pass_context
def genotype_project(context, project, force):
    config = context.obj
    if is_config_file_ok():
        output_path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance')
        if not os.path.exists(output_path):
            click.echo('Path does not exist! {}'.format(output_path))
            exit(0)
        list_of_gt_files = [file for file in os.listdir(output_path) if '.gt' in file]
        if not list_of_gt_files:
            click.echo('No .gt files found in {}'.format(output_path))
            click.echo('Generate .gt files first! Run the command: gt_concordance parse_xl_files')
            exit(0)
        click.echo('{} .gt files found in {}'.format(len(list_of_gt_files), output_path))

        # genotype sample for each found gt_file
        conc_files = []
        for gt_file in list_of_gt_files:
            sample = gt_file.split('.')[0]

            # run gatk if needed
            to_run_gatk = False
            vcf_file = os.path.join(output_path, "{}.vcf".format(sample))
            if os.path.exists(vcf_file) and force:
                click.echo('.vcf file will be overwriten: {}'.format(vcf_file))
                to_run_gatk = True
            elif not os.path.exists(vcf_file):
                click.echo('No {}.vcf file found. Running GATK'.format(sample))
                to_run_gatk = True
            if to_run_gatk:
                vcf_file = run_gatk(sample, config)
                # todo: UPDATE CHARON
                if vcf_file is None:
                    click.echo('GATK failed. Continue with the next sample')
                    update_charon(sample, 'FAILED')
                    continue

            # check concordance
            click.echo('Checking sample {}'.format(sample))
            vcf_data = parse_vcf_file(sample, config)
            gt_data = parse_gt_file(sample, config)
            if len(vcf_data) != len(gt_data):
                click.echo('VCF file and GT file contain differenct number of positions!! ({}, {})'.format(len(vcf_data), len(gt_data)))
            #  will write the results
            concordance = check_concordance(sample, vcf_data, gt_data, config)
            click.echo('Sample: {}, concordance: {} %'.format(sample, concordance))
            # update charon
            update_charon(sample=sample, status='DONE', concordance=concordance)

            conc_files.append(os.path.join(output_path, '{}.conc'.format(sample)))
        click.echo('{} files have been created in {}'.format(len(conc_files), output_path))


if __name__ == '__main__':
    cli()
