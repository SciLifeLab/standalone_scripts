#!/usr/bin/env python
"""
Validates the user submitted sample information sheet
so that no errors are propagated downstream.
"""
from __future__ import print_function
import argparse
from openpyxl import load_workbook
import coloredlogs
import logging
import re
import couchdb

# Global variables to store general assumptions
SHEET_NAME = 'Sample information'
FIRST_LINE = 20  # First line where user submitted data is located
SAMPLE_NAME_COL = 'O'
A_RATIO_COL = 'R'  # A260:A280 ratio
CONC_COL = 'S'
VOL_COL = 'T'
RIN_COL = 'V'
SAMPLE_TYPE = 'O8'
PLATE_ID = 'M6'
PROJECT_NAME_USER_SHEET = 'M3'
sample_recommendation_sheet ='./Samplesheet_converter/Sample_requirements.xlsx'

# Set up a logger with colored output
logger = logging.getLogger(__name__)
logger.propagate = False  # Otherwise the messages appeared twice
coloredlogs.install(level='INFO', logger=logger,
                    fmt='%(asctime)s %(levelname)s %(message)s')


class NumericValidator(object):
    """NumericValidator

    Checks whether cell is numeric or not.
    Takes optional params:
       decimal_comma - Checks whether swedish comma is used or not.
                       [default False]
    """

    def validate_conc(self, cell, min_conc, max_conc):
        """Checks whether value is numeric or not."""
        logging.debug(cell.value, cell.data_type)
        if cell.data_type != 'n':
            try:
                float(cell.value.replace(",", "."))
                logger.error(
                    'Cell {} with value \"{}\" is not numeric due to decimal point/comma clash.'\
                    .format(cell.coordinate, cell.value)
                    )
                return False
            except ValueError:
                logger.error(
                'Cell {} with value \"'.format(cell.coordinate)+ cell.value + '\" is not numeric'
                )
            return False

        try:
            float(cell.value)
            if(cell.value < min_conc) \
            or (cell.value > max_conc):
                logger.warning('Sample concentration ({}ng/ul) in cell {} is out of specifications: {}-{}ng/ul'\
               .format(cell.value,cell.coordinate, min_conc, max_conc))
            return True
        except ValueError:
            logger.error(
                'Cell {} is numeric but cannot be '
                'transformed to float'.format(cell.coordinate)
                )
            return False
        except TypeError:
            if cell.value is None:
                logger.error(
                'Cell {} is numeric but empty'.format(cell.coordinate)
                )
                return False
            else:
                raise

    def validate_vol(self, cell, vol):
        """Checks entry for volume"""
        if cell.value is None:
            logger.error('No sample volume given in cell {}'.format(cell.coordinate))
            return False
        elif(cell.value < vol):
            logger.warning('Sample volume ({}ul) in cell {} is lower than required: {}ul'\
            .format(cell.value,cell.coordinate, vol))
            return True
        else:
            return True

    def validate_rin(self,cell):
        """Checks entry for RIN in RNA samples only"""
        if cell.value is None:
            logger.error(
                'No RIN value given in cell {}'.format(cell.coordinate)
            )
            return False
        elif cell.value <8:
            logger.warning(
                'RIN value in cell {} is below recommendation'.format(cell.coordinate)
            )
            return True
        else:
            return True

    def validate_project(self, sheet, name_user_letter, plate_id, project_info):
        project_DB_full = re.search('project_name\': \'.*?\'' , project_info)
        project_name_DB = re.split('\'', project_DB_full.group(0))[2]
        project_name_user = re.split('-', sheet[name_user_letter].value)[0]
        if(project_name_DB == project_name_user):
            logger.info('plateID {} correct.'.format(plate_id))
        else:
            logger.error(
                'Wrong PLATE ID! Your given plate ID {} does not match your project. '
                'If this plate ID is correct, please contact your Project coordinator'.format(plate_id)
                )
            quit()

def validate_column(sheet, prep_stand, concentration_letter, volume_letter, rin_letter, sample_rowID, validator, validator_attr={}):
    """Validates all rows with a sample ID

    First checks for existence and correctness of a plate ID and if user changed the default.
    Then, given the column letter and which rows to validate:
    - Initiates the given validators for concentration, volume and RIN (RNA
    samples only) with the optional attributes.
    - Loops through all the given cells and validates them individually.
    """
    validator = NumericValidator(**validator_attr)
    passes = 0
    total = 0
    #validator.validate_project(sheet, name_user, sheet[plate_id].value, project, project_info, prep_stand)

    for row_nr in sample_rowID:
        total += 1
        cell_id_conc = "{col}{row_nr}".format(col=concentration_letter, row_nr=row_nr)
        cell_id_vol = "{col}{row_nr}".format(col=volume_letter, row_nr=row_nr)
        cell_id_rin = "{col}{row_nr}".format(col=rin_letter, row_nr=row_nr)
        result_conc = validator.validate_conc(sheet[cell_id_conc], prep_stand[0], prep_stand[1])
        result_vol = validator.validate_vol(sheet[cell_id_vol], prep_stand[2])
        if (prep_stand[5] == 'Bioanalyzer (RIN ≥8)'):
            result_rin = validator.validate_rin(sheet[cell_id_rin])
            if result_conc and result_vol and result_rin:  # Test passed
                passes += 1
        else:
            if result_conc and result_vol:  # Test passed
                passes += 1
    if (prep_stand[5] == 'Bioanalyzer (RIN ≥8)'):

        logger.info(
        'Sample processing prerequisit: submission of {} data'.format(prep_stand[5])
        )
        logger.info(
            'Checked entry in sample concentration, volume and quality control. {}/{} pass'\
            .format(passes, total)
            )
    else:
        testing_string = "test"
        if(prep_stand[5] != None):
            logger.info(
            'Sample processing prerequisit: submission of {} data'.format(prep_stand[5])
            )
        if(prep_stand[6] != None):
            logger.info(
            'Sample QC recommendation: submission of {} data'.format(prep_stand[6])
            )
        logger.info(
            'Checked entry in sample concentration and volume. {}/{} pass'\
            .format(passes, total)
            )

def project_information(user_sheet, name_user_letter, plate_id_user, username_couchDB, password_couchDB, validator, validator_attr={}):
    ''' Gets all available project information from couchDB based on the plateID provided by user'''
    plate_id = user_sheet[plate_id_user].value
    project_id_user = plate_id[:6]

    url_string = "http://"+username_couchDB+":"+password_couchDB+"@tools-dev.scilifelab.se:5984"
    connection = couchdb.Server(url=url_string)
    db = connection["projects"]
    pdoc = None
    for prow in db.view("project/project_id", reduce=False):
        if prow.key == project_id_user:
            pdoc = db.get(prow.id)
    if pdoc == None:
        logger.error(
             'Project not found, please check your entry for the PlateID, it should have the format'
             'PxxxxxPx, where x are numbers. If your Plate ID is correct, contact your project coordinator.'
             )
        quit()
    readable_doc = str(pdoc)

    validator.validate_project(user_sheet, name_user_letter, plate_id_user, readable_doc)

    return(readable_doc)
    #m = re.search('(?<=library_construction_method\':.).............' , readable_doc)

def prep_standards(recommendations, project_information):
    ''' gets the sample requirements from the sample requirement excel sheet based
        on the given sample prep type. '''

    m = re.search('library_construction_method\': \'.*?\'' , project_information)
    m2 = re.split('\'', m.group(0))

    prep = m2[2].strip()
    prep_recs = [None,None,None,None,None,None,None,None]
    prep_type_found = False
    for row in range(2, 15):
        cellID_prep = "A{row}".format(row=row)
        if(recommendations[cellID_prep].value == prep):
            prep_row= row
            prep_recs = [\
            recommendations["C{min_conc}".format(min_conc=prep_row)].value, \
            recommendations["D{max_conc}".format(max_conc=prep_row)].value, \
            recommendations["E{min_vol}".format(min_vol=prep_row)].value, \
            recommendations["F{rec_ng}".format(rec_ng=prep_row)].value, \
            recommendations["G{min_ng}".format(min_ng=prep_row)].value, \
            recommendations["H{qual_req}".format(qual_req=prep_row)].value, \
            recommendations["I{qual_rec}".format(qual_rec=prep_row)].value]
            prep_type_found = True
    if(prep_type_found == False):
        logger.error(
            'Preparation type \"{}\" not found'.format(prep)
            )
        quit()
    return(prep_recs)

def sample_number(sheet, sample_letter, row_start):
    """ identifies the all rows containing a sample name, discards rows without entry.
    Rows containing whitespace only trigger a warning and are discarded for subsequent
    tests """
    real = 1
    cellID_withSample =list()
    cellID_noSample =list()
    for i in range(row_start, row_start+96):
        cell_id = "{col}{row_itter}".format(col=sample_letter,row_itter=i)
        cell_value = str(sheet[cell_id].value)
        if(cell_value.isspace()):
            logger.warning(
                'Cell {} contains empty spaces only. Remove content.'.format(cell_id)
               )
        elif(sheet[cell_id].value != None):
            cellID_withSample.append(i)
        else:
            cellID_noSample.append(cell_id)# TODO check here that these rows do really not contain information
    return(cellID_withSample)

def main(input_sheet, username_couchDB, password_couchDB):
    wb = load_workbook(input_sheet, read_only=True, data_only=True)
    ws = wb[SHEET_NAME]
    wb_sample_recommendations = load_workbook(sample_recommendation_sheet, read_only=True, data_only=True)
    ws_recommendations = wb_sample_recommendations['Numeric']
    validator=NumericValidator()
    project_info = project_information(ws, PROJECT_NAME_USER_SHEET, PLATE_ID, username_couchDB, password_couchDB, validator)
    prep_stand = prep_standards(ws_recommendations, project_info)
    nr_sample_pos = sample_number(ws, SAMPLE_NAME_COL, FIRST_LINE)
    validate_column(ws, prep_stand, CONC_COL, VOL_COL, RIN_COL, nr_sample_pos, validator)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sample_info_sheet',
                        help="Completed sample info sent to NGI by the user.")
    parser.add_argument('username', default=None,
                        help="Username for couchDB")
    parser.add_argument('password', default=None,
                        help="Password for couchDB")
    args = parser.parse_args()

    main(args.sample_info_sheet,  args.username, args.password)
