#!/usr/bin/env python
"""
Validates the user submitted sample information sheet
so that no errors are propagated downstream.
"""
from __future__ import print_function
import argparse
from openpyxl import load_workbook
import logging

# Global variables to store general assumptions
SHEET_NAME = 'Sample information'
FIRST_LINE = 20  # First line where user submitted data is located
SAMPLE_NAME_COL = 'O'
A_RATIO_COL = 'R'  # A260:A280 ratio
CONC_COL = 'S'


class NumericValidator(object):
    """NumericValidator

    Checks whether cell is numeric or not.
    Takes optional params:
       decimal_comma - Checks whether swedish comma is used or not.
                       [default False]
    """

    def validate(self, cell):
        """Checks whether value is numeric or not."""
        logging.debug(cell.value, cell.data_type)
        if cell.data_type != 'n':
            logging.error(
                    'Cell {} is not numeric'.format(cell.coordinate)
                    )
            return False
        try:
            float(cell.value)
            return True
        except ValueError:
            logging.error(
                    'Cell {} is numeric but cannot be '
                    'transformed to float'.format(cell.coordinate)
            )
            return False
        except TypeError:
            if cell.value is None:
                logging.warning(
                    'Cell {} is numeric but empty'.format(cell.coordinate)
                )
            else:
                raise


def validate_column(sheet, column_letter, row_start,
                    row_end, validator, validator_attr={}):
    """Validates a section of a column

    Given the column letter and which rows to validate:
    Initiates the given validator with the optional attributes
    Loops through all the given cells
    validates them individually.
    """

    validator = NumericValidator(**validator_attr)
    passes = 0
    total = 0
    for row_nr in range(row_start, row_end):
        total += 1
        cell_id = "{col}{row_nr}".format(col=column_letter, row_nr=row_nr)
        result = validator.validate(sheet[cell_id])
        if result:  # Test passed
            passes += 1

    logging.info(
        'Checked column {}. {}/{} passes'.format(column_letter, passes, total)
        )


def main(input_sheet):
    wb = load_workbook(input_sheet, read_only=True, data_only=True)
    ws = wb[SHEET_NAME]
    nr_samples = 8  # TODO
    final_row = FIRST_LINE + nr_samples
    validate_column(ws, CONC_COL, FIRST_LINE, final_row, NumericValidator)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sample_info_sheet',
                        help="Completed sample info sent to NGI by the user.")
    args = parser.parse_args()
    main(args.sample_info_sheet)
