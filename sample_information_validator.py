#!/usr/bin/env python

# load libraries
from __future__ import print_function
import argparse
from openpyxl import load_workbook
import coloredlogs
import logging
import re
import couchdb
import numbers
import json

#global variable
WARNINGS = 0


# Set up a logger with colored output
logger = logging.getLogger(__name__)
logger.propagate = False  # Otherwise the messages appeared twice
coloredlogs.install(level='INFO', logger=logger,
                    fmt='%(asctime)s %(levelname)s %(message)s')


class ProjectSheet:
#Class Attributes
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

    # Initializer / Instance attributes
    def __init__(self, sample_info_sheet):
        self.sample_info_sheet = sample_info_sheet
        self.work_sheet = None
        self.sample_rec = None

    # instance methods
    def getAccessUserSheet(self):
        if self.work_sheet is None:
            wb = load_workbook(self.sample_info_sheet, read_only=True, data_only=True)
            ws = wb[ProjectSheet.SHEET_NAME]
            self.work_sheet = ws

    def projectID(self):
        self.getAccessUserSheet()
        plate_id = self.work_sheet[ProjectSheet.PLATE_ID].value
        project_id_user = re.findall('P[0-9]+', plate_id)[0]
        return(project_id_user)

    def getSamples(self):
        """ identifies the all rows containing a sample name, discards rows without entry.
        Rows containing whitespace only trigger a warning and are discarded for subsequent
        tests """
        cellID_withSample = []
        cellID_noSample = []
        for i in range(ProjectSheet.FIRST_LINE, ProjectSheet.FIRST_LINE+96):
            cell_id = "{col}{row_iter}".format(col=ProjectSheet.SAMPLE_NAME_COL,row_iter=i)
            cell_value = str(self.work_sheet[cell_id].value)
            if(cell_value.isspace()):
                logger.warning(
                    'Cell {} contains empty spaces only. Remove content.'.format(cell_id)
                   )
                global WARNINGS
                WARNINGS += 1
            elif(self.work_sheet[cell_id].value != None):
                cellID_withSample.append(i)
            else:
                cellID_noSample.append(cell_id)# TODO check here that these rows do really not contain information
        return(cellID_withSample)

    def ProjectInfo(self, username_couchDB, password_couchDB, url):
        url_string = "http://"+username_couchDB+":"+password_couchDB+url
        connection = couchdb.Server(url=url_string)
        db = connection["projects"]
        # check the existence of the project number in couchDB
        project_id_found = db.view("project/project_id", key=self.projectID())
        prow=project_id_found.rows
        # Project not found
        if len(prow) == 0:
            logger.error(
            'Project not found, please check your entry for the PlateID, it should have the format'
            'PxxxxxPx, where x are numbers. If your Plate ID is correct, contact your project coordinator.'
            )
            quit()
        # more than one project found
        elif len(prow) > 1:
            logger.error(
            'Project ID not unique, please check your entry for the PlateID, it should have the format'
            'PxxxxxPx, where x are numbers. If your Plate ID is correct, contact your project coordinator.'
            )
            quit()
        else:
        # puts the Document of the identified project in a new variable "pdoc"
            pdoc = db.get(prow[0].id)
            return(pdoc)

    def requirements(self, path):
        if self.sample_rec is None:
            wb_sample_recommendations = load_workbook(path, read_only=True, data_only=True)
            self.sample_rec = wb_sample_recommendations['Numeric']

    def prep_standards(self, info, recom_path):
        ''' gets the sample requirements from the sample requirement excel sheet based
            on the given sample prep type. '''
        with open(recom_path) as ifh: #ifh = input file handler
            recom_info = json.load(ifh)
        prep = info['details']['library_construction_method']
        prep_recs = [None,None,None,None,None,None,None]
        if prep in recom_info:

            prep_recs = [\
            recom_info[prep]['Concentration']['Minimum'],\
            recom_info[prep]['Concentration']['Maximum'],\
            recom_info[prep]['Volume']['Minimum'],\
            recom_info[prep]['Amount']['Recommended'],\
            recom_info[prep]['Amount']['Minimum'],\
            recom_info[prep]['Quality requirement']['Method'],\
            recom_info[prep]['QC recommendation'],]
            if 'RIN' in recom_info[prep]['Quality requirement']:
                prep_recs.append(recom_info[prep]['Quality requirement']['RIN'])
            else:
                prep_recs.append(None)

        else:
            logger.error(
                'Preparation type \"{}\" not found'.format(prep)
                )
            quit()
        return(prep_recs)

    def validate_project_Name(self, info):
        project_name_DB = info['project_name']
        project_name_user = self.work_sheet[ProjectSheet.PROJECT_NAME_USER_SHEET].value.split('-')[0]
        if(project_name_DB == project_name_user):
            logger.info('plateID {} correct.'.format(self.work_sheet[ProjectSheet.PLATE_ID].value))
        else:
            logger.error(
                'Wrong PLATE ID! Your given plate ID {} does not match your project. '
                'If this plate ID is correct, please contact your Project coordinator'.format(ProjectSheet.PLATE_ID)
                )
            quit()

    def validate(self, info, recom_path):
        """Validates all rows with a sample ID

        First checks for existence and correctness of a plate ID and if user changed the default.
        Then, given the column letter and which rows to validate:
        - Initiates the given validators for concentration, volume and RIN (RNA
        samples only) with the optional attributes.
        - Loops through all the given cells and validates them individually.
        """
        prep_recs = self.prep_standards(info, recom_path)
        passes = 0
        total = 0

        for row_nr in self.getSamples():
            total += 1

            cell_id_conc = "{col}{row_nr}".format(col=ProjectSheet.CONC_COL, row_nr=row_nr)
            cell_id_vol = "{col}{row_nr}".format(col=ProjectSheet.VOL_COL, row_nr=row_nr)
            cell_id_rin = "{col}{row_nr}".format(col=ProjectSheet.RIN_COL, row_nr=row_nr)

            validator = Validator(self.work_sheet,cell_id_conc,cell_id_vol, cell_id_rin)
            validator.validate_numeric()
            result_conc = validator.validate_conc(prep_recs[0], prep_recs[1])
            result_vol = validator.validate_vol(prep_recs[2])
            result_rin = validator.validate_rin(prep_recs[7])

            if prep_recs[7] is not None: #most common mis-spellings
                if result_conc and result_vol and result_rin:  # Test passed
                    passes += 1
            else:
                if result_conc and result_vol:  # Test passed
                    passes += 1
        if (prep_recs[5] == 'Bioanalyzer (RIN ≥8)'):

            logger.info(
            'Sample processing prerequisit: submission of {} data'.format(prep_recs[5])
            )
            logger.info(
                'Checked entry in sample concentration, volume and quality control. {}/{} pass'\
                .format(passes, total)
                )
        else:
            if(prep_recs[5] != None):
                logger.info(
                'Sample processing prerequisit: submission of {} data'.format(prep_recs[5])
                )
            if(prep_recs[6] != None):
                logger.info(
                'Sample QC recommendation: submission of {} data'.format(prep_recs[6])
                )
            logger.info(
                'Checked entry in sample concentration and volume. {}/{} pass, {} warning(s).'\
                .format(passes, total, WARNINGS)
                )


class Validator(object):
    # Initializer / Instance attributes
    def __init__(self, access_sample_info_sheet, concentrationID, volumeID, rinID):
        self.access_sample_info_sheet = access_sample_info_sheet
        self.concentrationID = concentrationID
        self.volumeID = volumeID
        self.rinID = rinID

    # instance methods
    def validate_numeric(self):
        """Checks whether value is numeric or not."""
        for checkNumbers in [self.concentrationID, self.volumeID, self.rinID]:
            if not isinstance(self.access_sample_info_sheet[checkNumbers].value, numbers.Number):
                try:
                    float(self.access_sample_info_sheet[checkNumbers].value.replace(",", "."))
                    logger.error(
                        'Cell {} with value \"{}\" is not numeric due to decimal point/comma clash.'\
                        .format(self.access_sample_info_sheet[checkNumbers].coordinate, self.access_sample_info_sheet[checkNumbers].value)
                        )
                    return False
                except ValueError:
                    logger.error(
                    'Cell {0} with value \"{1}\" is not numeric'.format(\
                    self.access_sample_info_sheet[checkNumbers].coordinate, \
                    self.access_sample_info_sheet[checkNumbers].value)
                    )
                except TypeError:
                    if self.access_sample_info_sheet[checkNumbers].value is None:
                        logger.error(
                        'Cell {} is numeric but empty'.format(self.access_sample_info_sheet[checkNumbers].coordinate)
                        )
                        return False
                    else:
                        raise
            return False

    def validate_conc(self, min_conc, max_conc):
        if(self.access_sample_info_sheet[self.concentrationID].value < min_conc) \
        or (self.access_sample_info_sheet[self.concentrationID].value > max_conc):
            global WARNINGS
            WARNINGS += 1

            logger.warning('Sample concentration ({}ng/ul) in cell {} is out of specifications: {}-{}ng/ul'\
            .format(self.access_sample_info_sheet[self.concentrationID].value,self.concentrationID, min_conc, max_conc))
        return True

    def validate_vol(self, vol):
        """Checks entry for volume"""
        if(self.access_sample_info_sheet[self.volumeID].value < vol):
            logger.warning('Sample volume ({}ul) in cell {} is lower than required: {}ul'\
            .format(self.access_sample_info_sheet[self.volumeID].value,self.access_sample_info_sheet[self.volumeID].coordinate, vol))
            global WARNINGS
            WARNINGS += 1
            return True
        else:
            return True

    def validate_rin(self, rin):
        """Checks entry for RIN in RNA samples only"""
        if self.access_sample_info_sheet[self.rinID].value < rin:
            logger.warning(
                'RIN value in cell {} is below recommendation'.format(self.access_sample_info_sheet[self.rinID].coordinate)
            )
            global WARNINGS
            WARNINGS += 1
            return True
        else:
            return True


def main(input_sheet, username_couchDB, password_couchDB, url_couchDB, recom_path):
    # Instantiate the ProjectSheet object
    sheetOI = ProjectSheet(input_sheet)
    # get Project Information from couchDB
    Project_Information = sheetOI.ProjectInfo(username_couchDB, password_couchDB, url_couchDB)
    # validate the project name to ensure correct identification in couchDB
    sheetOI.validate_project_Name(Project_Information)
    # validate all entries
    sheetOI.validate(Project_Information, recom_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sampleInfoSheet',
                        help="Completed sample info sent to NGI by the user.")
    parser.add_argument('username', default=None,
                        help="Username for couchDB")
    parser.add_argument('password', default=None,
                        help="Password for couchDB")
    parser.add_argument('url_couchDB', default=None,
                        help="server address for couchDB i.e. \"@tools-dev.scilifelab.se:5984\"")
    parser.add_argument('path_recom_sheet', default=None,
                        help="path to the sample recommendation excel file")
    args = parser.parse_args()

    main(args.sampleInfoSheet,  args.username, args.password, args.url_couchDB,\
    args.path_recom_sheet)
