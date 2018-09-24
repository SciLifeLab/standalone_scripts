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

    def validate_conc(self, cell):
        """Checks whether value is numeric or not."""
        logging.debug(cell.value, cell.data_type)
        if cell.data_type != 'n':
            try:
                cell.value = float(cell.value.replace(",", ".")) # not sure how to change this in original file, would have to be done here
                return True
            except ValueError:
                logger.error(
                    'Cell {} with value \"'.format(cell.coordinate)+ cell.value + '\" is not numeric'
                    )
                return False

        try:
            float(cell.value)
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

    def validate_vol(self,cell): # TODO different volumes for different input types! requires changes in data sheet
        """Checks entry for volume"""
        if cell.value is None:
            logger.error(
                'No sample volume given in cell {}'.format(cell.coordinate)
            )
            return False
        else:
            return True

    def validate_rin(self,cell, sample_type):
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

def validate_column(sheet, plate_id, concentration_letter, volume_letter, rin_letter, cell_sample_type, sample_rowID,
                    validator, validator_attr={}):
    """Validates all rows with a sample ID

    First checks for existence of a plate ID and if user changed the default.
    Then, given the column letter and which rows to validate:
    Initiates the given validators for concentration, volumne and RIN (RNA samples only) with the optional attributes
    Loops through all the given cells
    validates them individually.
    """
    if(sheet[plate_id].value == None or sheet[plate_id].value == 'P####P#'):
        logger.error(
            'Missing PLATE ID'
            )
    validator = NumericValidator(**validator_attr)
    passes = 0
    total = 0
    for row_nr in sample_rowID:
        total += 1
        cell_id_conc = "{col}{row_nr}".format(col=concentration_letter, row_nr=row_nr)
        cell_id_vol = "{col}{row_nr}".format(col=volume_letter, row_nr=row_nr)
        cell_id_rin = "{col}{row_nr}".format(col=rin_letter, row_nr=row_nr)
        result_conc = validator.validate_conc(sheet[cell_id_conc])
        result_vol = validator.validate_vol(sheet[cell_id_vol])
        if (sheet[cell_sample_type].value == 'Total RNA' or sheet[cell_sample_type].value == "mRNA" or sheet[cell_sample_type].value == 'Small RNA'):
            result_rin = validator.validate_rin(sheet[cell_id_rin], sheet[cell_sample_type])
            if result_conc and result_vol and result_rin:  # Test passed
                passes += 1
        else:
            if result_conc and result_vol:  # Test passed
                passes += 1
    if (sheet[cell_sample_type].value == 'Total RNA' or sheet[cell_sample_type].value == "mRNA" or sheet[cell_sample_type].value == 'Small RNA'):
        logger.info(
            'Checked columns {}, {} and {}. {}/{} passes'.format(concentration_letter, volume_letter, rin_letter, passes, total)
            )
    else:
        logger.info(
            'Checked columns {} and {}. {}/{} passes'.format(concentration_letter, volume_letter, passes, total)
            )


def sample_number(sheet, sample_letter, row_start):
    """ identifies the all rows containing a sample name"""
    real = 1
    cellID_withSample =list()
    cellID_noSample =list()
    for i in range(row_start, row_start+96):
        cell_id = "{col}{row_itter}".format(col=sample_letter,row_itter=i)
        if(sheet[cell_id].value != None):
            cellID_withSample.append(i)
        else:
            cellID_noSample.append(cell_id)# TODO check here that these rows do really not contain information
    return(cellID_withSample)

def main(input_sheet):
    wb = load_workbook(input_sheet, read_only=True, data_only=True)
    ws = wb[SHEET_NAME]
    nr_sample_pos = sample_number(ws, SAMPLE_NAME_COL, FIRST_LINE)
    validate_column(ws, PLATE_ID, CONC_COL, VOL_COL, RIN_COL, SAMPLE_TYPE, nr_sample_pos, NumericValidator)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sample_info_sheet',
                        help="Completed sample info sent to NGI by the user.")
    args = parser.parse_args()

    main(args.sample_info_sheet)
