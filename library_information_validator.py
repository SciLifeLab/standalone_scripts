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
import decimal
import yaml
from numpy import setdiff1d
from collections import Counter

#global variable
WARNINGS = 0


# Set up a logger with colored output
logger = logging.getLogger(__name__)
logger.propagate = False  # Otherwise the messages appeared twice
coloredlogs.install(level='INFO', logger=logger,
                    fmt='%(asctime)s %(levelname)s %(message)s')


class LibrarySheet:
#Class Attributes
    SHEET_NAME = 'Sample information'
    INDEX_SHEET_NAME = 'Index'
    FIRST_LINE = 20  # First line where user submitted data is located
    SAMPLE_NAME_COL = 'P'
    POOL_NAME_COL = 'V'
    POOL_NAME_SAMPLE_COL = "O"
    LENGTH_COL = 'Y'  # average fragment length
    MOLARITY_COL = 'AA'  # molarity of the pool
    SINDEX_COL = 'S' # sample index (automated)
    CINDEX_COL = 'T' # custom index
    SAMPLE_TYPE = 'P8' # from drop down menue ("Finished library" or "Amplicon with adapters (low diversity)")
    PLATE_ID = 'N6'
    PROJECT_NAME_USER_SHEET = 'N3'

    # Initializer / Instance attributes
    def __init__(self, library_info_sheet):
        self.library_info_sheet = library_info_sheet
        self.library_sheet = None
        self.sample_rec = None

    # instance methods
    def getAccessUserSheet(self):
        if self.library_sheet == None:
            wb = load_workbook(self.library_info_sheet, read_only=True, data_only=True)
            ws = wb[LibrarySheet.SHEET_NAME]
            self.library_sheet = ws

    def projectID(self):
        """retrieves the project and plate ID from the excel sheet and checks the
        correctness of the plate ID format."""
        self.getAccessUserSheet()
        plate_id = self.library_sheet[LibrarySheet.PLATE_ID].value
        if(len(re.findall('P\d+P\d+', plate_id))>0):
            project_id_user = re.findall('P\d+', plate_id)[0]
        else:
            logger.error(
                'Given plate ID ({}) in cell {} has the wrong format. It should be in the format'
                ' PxxxxxPx, where x are numbers. If your Plate ID is correct, contact your project coordinator.'\
                .format(plate_id, LibrarySheet.PLATE_ID)
                )
            quit()
        return([project_id_user, plate_id])

    def getRows(self, column):
        """ identifies the all rows containing a sample name, discards rows without entry.
        Rows containing whitespace only trigger a warning and are discarded for subsequent
        tests """
        cellID_withSample = list()
        cellID_noSample = list()
        for i in range(LibrarySheet.FIRST_LINE, LibrarySheet.FIRST_LINE+96):
            cell_id = "{col}{row_iter}".format(col=column,row_iter=i)
            cell_value = str(self.library_sheet[cell_id].value)
            if(cell_value.isspace()):
                logger.warning(
                    'Cell {} contains empty spaces only. Remove content.'.format(cell_id)
                    )
                global WARNINGS
                WARNINGS += 1
            elif(self.library_sheet[cell_id].value != None):
                cellID_withSample.append(i)
            else:
                cellID_noSample.append(cell_id)
        return(cellID_withSample)

    def getSamples(self):
        """ identifies the all rows containing a sample name, discards rows without entry.
        Rows containing whitespace only trigger a warning and are discarded for subsequent
        tests """
        cellID_withSample = list()
        cellID_noSample = list()
        for i in range(LibrarySheet.FIRST_LINE, LibrarySheet.FIRST_LINE+96):
            cell_id = "{col}{row_iter}".format(col=LibrarySheet.SAMPLE_NAME_COL,row_iter=i)
            cell_value = str(self.library_sheet[cell_id].value)
            if(cell_value.isspace()):
                logger.warning(
                    'Cell {} contains empty spaces only. Remove content.'.format(cell_id)
                   )
                global WARNINGS
                WARNINGS += 1
            elif(self.library_sheet[cell_id].value != None):
                cellID_withSample.append(i)
            else:
                cellID_noSample.append(cell_id)
        return(cellID_withSample)

    def getPool(self):

        cellID_withSample = list()
        cellID_noSample = list()
        for i in range(LibrarySheet.FIRST_LINE, LibrarySheet.FIRST_LINE+96):
            cell_id = "{col}{row_iter}".format(col=LibrarySheet.POOL_NAME_COL,row_iter=i)
            cell_value = str(self.library_sheet[cell_id].value)
            if(cell_value.isspace()):
                logger.warning(
                    'Cell {} contains empty spaces only. Remove content.'.format(cell_id)
                   )
                global WARNINGS
                WARNINGS += 1
            elif(self.library_sheet[cell_id].value != None):
                cellID_withSample.append(i)
            else:
                cellID_noSample.append(cell_id)# TODO check here that these rows do really not contain information
        return(cellID_withSample)

    def ProjectInfo(self, config):
        """
        Retrieves the project information from couchDB, checks that the project exists in
        couchDB and is unique. Returns the information and the full project plateID.
        """
        with open(config) as settings_file:
            server_settings = yaml.load(settings_file, Loader=yaml.FullLoader)
        couch = couchdb.Server(server_settings.get("couch_server", None))
        db = couch["projects"]
        # check the existence of the project number in couchDB
        project_plate_ID = self.projectID()
        project_id_found = db.view("project/project_id", key=project_plate_ID[0])
        prow = project_id_found.rows
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
            return pdoc, project_plate_ID[1]

    def validate_project_Name(self, info, project_plate_ID):
        """
        Prints the identified project name based on the user supplied Plate/Project ID for
        control purposes by the project coordinator. Further checks that the
        plate number is not already in couchDB.
        """
        project_name_DB = info['project_name']
        samples = info['samples'].keys()
        plate ='P{}_{}'.format(project_plate_ID.split("P")[1],project_plate_ID.split("P")[2])
        found_plate = [s for s in samples if plate in s]
        if(len(found_plate)>0):
            new_plate_no = int(project_plate_ID.split("P")[2])
            new_plate_no += 1
            new_plate_ID = 'P{}P{}'.format(project_plate_ID.split("P")[1], new_plate_no)
            logger.warning(
                'Plate number {} is already used. Please increase the plate number to {}.'.format(project_plate_ID, new_plate_ID))
            global WARNINGS
            WARNINGS += 1
        logger.info('identified project name: {}'.format(project_name_DB))

    def validate(self):
        """Validates all rows with a sample ID
        Given the column letter and which rows to validate:
        - Initiates the given validators for cell content (numeric), concentration,
        volume and RIN (RNA samples only) with the optional attributes.
        - Loops through all the given cells and validates them individually.
        - prints summaries of the warnings and of the Excel file check.
        """
        #        print(self.getRows(LibrarySheet.POOL_NAME_SAMPLE_COL))

        for row_nr in self.getPool():
            cell_id_mol = "{col}{row_nr}".format(col=LibrarySheet.MOLARITY_COL, row_nr=row_nr)
            validator = Validator(self.library_sheet,cell_id_mol)
            result_numeric = validator.validate_numeric()

        current_pool_rows = []
        pool_values = []
        for row_nr in self.getRows(LibrarySheet.SAMPLE_NAME_COL):
            current_cell_id_pool ="{col}{row_nr}".format(col=LibrarySheet.POOL_NAME_SAMPLE_COL, row_nr=row_nr)
            current_cell_value_pool = self.library_sheet[current_cell_id_pool].value
            pool_values.append(current_cell_value_pool)

        poolIDs = list(dict.fromkeys(pool_values))
        cell_rowid_sample = self.getRows(LibrarySheet.SAMPLE_NAME_COL)
        cell_rowid_pool = self.getRows(LibrarySheet.POOL_NAME_SAMPLE_COL)

        if(len(cell_rowid_sample) > len(cell_rowid_pool)):
            missing_pool_rowid_list = setdiff1d(cell_rowid_sample, cell_rowid_pool)
            for missing_pool_rowid in missing_pool_rowid_list:
                logger.error(
                    'Missing pool definition in {}{}'.format(LibrarySheet.POOL_NAME_SAMPLE_COL, missing_pool_rowid))
            quit()
        for pool in poolIDs:
            for nrow_nr in cell_rowid_sample:
                current_cell_id_pool ="{col}{row_nr}".format(col=LibrarySheet.POOL_NAME_SAMPLE_COL, row_nr=nrow_nr)
                current_cell_value_pool = self.library_sheet[current_cell_id_pool].value
                if(current_cell_value_pool == pool):
                    current_pool_rows.append(nrow_nr)
            validator = Validator(self.library_sheet, None)
            result_index, sindex = validator.select_index(current_pool_rows, pool)
            validator.validate_index(result_index, pool, sindex)
            current_pool_rows =[]

class Validator(object):
    # Initializer / Instance attributes
    def __init__(self, access_sample_info_sheet, molarityID):
        self.access_sample_info_sheet = access_sample_info_sheet
        self.molarityID = molarityID


    # instance methods
    def validate_numeric(self):
        """Checks whether value is numeric or not."""
        for checkNumbers in [self.molarityID]:
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
                    'Cell {} with value \"'.format(self.access_sample_info_sheet[checkNumbers].coordinate)+ self.access_sample_info_sheet[checkNumbers].value + '\" is not numeric'
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

    def validate_mol(self, min_mol, max_mol):
        if(self.access_sample_info_sheet[self.concentrationID].value < min_mol) \
        or (self.access_sample_info_sheet[self.concentrationID].value > max_mol):
            global WARNINGS
            WARNINGS += 1

            logger.warning('Sample molarity ({}ng/ul) in cell {} is out of specifications: {}-{}ng/ul'\
            .format(self.access_sample_info_sheet[self.molarityID].value,self.molarityID, min_mol, max_mol))
        return True

    def select_index(self, pool_rows,pool):
        sindex_list = []
        sindex_absent = []
        cell_id_standard_index = []
        cell_id_custom_index = []
        sindex_chosen = True
        for row_nr in pool_rows:
           cell_id_standard_index.append("{col}{row_nr}".format(col  =LibrarySheet.SINDEX_COL, row_nr = row_nr))
           cell_id_custom_index.append("{col}{row_nr}".format(col = LibrarySheet.CINDEX_COL, row_nr = row_nr))

        for sindex in cell_id_standard_index:
            if (self.access_sample_info_sheet[sindex].value is None):
                sindex_absent.append(sindex)
            else:
                index_only = re.split('\(|\)', self.access_sample_info_sheet[sindex].value)[1]
                index_only = re.sub('-', '', index_only)
                sindex_list.append(index_only)
                #print(sindex_list)
                if (self.access_sample_info_sheet["{col}{row_nr}".format(col=LibrarySheet.CINDEX_COL, row_nr=sindex.split(LibrarySheet.SINDEX_COL)[1])].value is not None):
                    logger.warning('Custom and Standard Index selected for the sample in fields {}{} and {}{}. Please clarify which of the two indexes was used.'\
                    .format(LibrarySheet.SINDEX_COL, sindex.split(LibrarySheet.SINDEX_COL)[1], LibrarySheet.CINDEX_COL, sindex.split(LibrarySheet.SINDEX_COL)[1]))

        cindex_list = []
        cindex_absent = []
        for cindex in cell_id_custom_index:
            if (self.access_sample_info_sheet[cindex].value is None):
                cindex_absent.append(cindex)
            else:
                index_only = re.sub('-','', self.access_sample_info_sheet[cindex].value)
                cindex_list.append(index_only)

        if(len(cindex_absent) == len(pool_rows)):
            sel_index = sindex_list
            if(len(sindex_list) != len(pool_rows)):
                for absent_index in sindex_absent:
                    logger.error("missing standard index in {}".format(absent_index))

        elif(len(sindex_absent) == len(pool_rows)):
            sel_index = cindex_list
            sindex_chosen = False
            if(len(cindex_list) != len(pool_rows)):
                for absent_index in cindex_absent:
                    logger.error("missing custom index in {}".format(absent_index))
        else:
            sindex_chosen = False

            sel_index = sindex_list + cindex_list
            if(len(sel_index) != len(pool_rows)):

                rowID_cindex_absent = [re.sub("\D","",x) for x in cindex_absent]
                rowID_sindex_absent = [re.sub("\D","",x) for x in sindex_absent]

                missing_indexes = list(set(rowID_cindex_absent).intersection(rowID_sindex_absent))
                for mIndex in missing_indexes:
                    logger.error('missing index in row {}'.format(mIndex))

            logger.warning('mix between custom and standard indexes in pool {}.'\
            .format(pool))
        return(sel_index, sindex_chosen)

    def validate_index(self, index_seq, pool_name, sindex):
        c = Counter(index_seq)
        for index, index_count in c.most_common():
            if(index_count>1):
                logger.error('The index sequence \"{}\" in pool {} is not unique for this pool.'\
                .format(index, pool_name))
            else:
                break

        for index in index_seq: ###TODO currently, standard indexes contain other characters!
            charRE = re.compile(r'[^ATCGatcg\-.]')
            index_search = charRE.search(index)
            if(bool(index_search)):
                logger.warning('The index sequence \"{}\" in pool {} contains invalid characters.'\
                .format(index, pool_name))

        if(not sindex):
            index_length = []
            for index in index_seq:
                index_length.append(len(index))
            count_length = Counter(index_length)

            if(len(count_length) > 1):
                logger.warning('There are {} different index lengths in pool {}, please double check the sequences.'\
                .format(len(count_length),pool_name))

            max_length = sorted(count_length.keys())[-1]
            min_length = sorted(count_length.keys())[0]
            index_list_colour = []
            for index in index_seq:
                index_colour = index.replace('T','G').replace('A','R').replace('C', 'R')
                index_list_colour.append(list(index_colour))

            for row_nr in range(0,max_length):
                column = []
                for row in index_list_colour:
                    try:
                        column.append(row[row_nr])
                    except IndexError:
                        pass
                count_colour = Counter(column)
            #    print(count_colour.values()[1])
                if(len(count_colour)<2 and sum(count_colour.values()) > 1):
                    logger.warning('Indexes in pool {} unbalanced at position {}'\
                    .format(pool_name, row_nr+1))
    #        a = get_row(index_list_colour,1)
    #        print(a)
        # check that index length is the same in all getSamples

#def get_row(dic, row_nr):
#    column = []
#    for row in dic:
#        column.append(row[row_nr])
#    return(column)


def main(input_sheet, config_statusDB):
    # Instantiate the LibrarySheet object
    sheetOI = LibrarySheet(input_sheet)
    # get Project Information from couchDB
    Project_Information, project_plate_ID = sheetOI.ProjectInfo(config_statusDB)
    # validate the project name to ensure correct identification in couchDB
    sheetOI.validate_project_Name(Project_Information, project_plate_ID)
    # get info about prep type
#    prep_recommendations = sheetOI.prep_standards(Project_Information, recom_path)
    # validate all entries
    sheetOI.validate()#Project_Information, config_statusDB)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('libraryInfoSheet',
                        help="Completed sample info sent to NGI by the user.")
    parser.add_argument('config_statusDB',
                        help="settings file in yaml format to access statusDB \
                        in the format \"couch_server: http://<username>:<password>@tools.scilifelab.se:5984\"")
    args = parser.parse_args()

    main(args.libraryInfoSheet,  args.config_statusDB)
